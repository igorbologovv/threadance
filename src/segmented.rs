//! Single-producer, worker-affine queue topologies.
//! A topology owns its memory layout and routing; the core only starts workers
//! and delivers the destination index to an addressed wake policy.

use std::{
    io,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, Builder, JoinHandle, Thread},
    time::{Duration, Instant},
};

use crate::{ExecuteError, QueuePop, Task, TaskConsumer, WaitResult};

/// One producer; successful submission returns the owner of the published task.
///
/// Implementations may block for capacity. Taking &mut self deliberately
/// precludes concurrent submissions without a caller-provided synchronization
/// layer. There is no producer mutex inside this pool.
pub trait SegmentedProducer: Send + 'static {
    fn push(&mut self, task: Task) -> Result<usize, Task>;
}

/// Builds one writer and exactly one reader for each worker.
///
/// A concrete implementation can place all SPSC segments in one allocation,
/// use raw pointers, and implement its own gate/routing policy. None of those
/// implementation choices leak into the worker pool.
pub trait SegmentedTopology: Send + 'static {
    type Producer: SegmentedProducer;
    type Consumer: TaskConsumer;

    fn build(
        self,
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> (Self::Producer, Vec<Self::Consumer>);
}

/// Wake contract for work that can only be consumed by one specific worker.
pub trait SegmentedWakePolicy<C: TaskConsumer>: Send + Sync + 'static {
    fn wait(&self, worker: usize, consumer: &C) -> WaitResult;
    fn wake(&self, worker: usize);
    fn wake_all(&self);
}

/// Idle behavior of the built-in addressed wake implementation.
#[derive(Debug, Clone, Copy)]
pub enum SegmentedIdle {
    SpinThenPark(Duration),
    AgaveBackoff,
}

/// Per-worker park/unpark tokens, without a global bitmap or shared wait queue.
///
/// Call new with the same worker count passed to SegmentedThreadPool::new.
/// The worker registers its own Thread handle *before* a final empty check.
/// Work published earlier is observed by that check; a later unpark stores
/// a token until park, preventing lost wakeups.
pub struct SegmentedWake {
    threads: Box<[OnceLock<Thread>]>,
    idle: SegmentedIdle,
    shutdown: AtomicBool,
}

impl SegmentedWake {
    pub fn new(workers: NonZeroUsize, idle: SegmentedIdle) -> Self {
        Self {
            threads: (0..workers.get()).map(|_| OnceLock::new()).collect(),
            idle,
            shutdown: AtomicBool::new(false),
        }
    }

    #[inline]
    fn probe<C: TaskConsumer>(&self, consumer: &C) -> Option<WaitResult> {
        if self.shutdown.load(Ordering::Acquire) {
            return Some(WaitResult::Interrupted);
        }
        match consumer.try_pop() {
            QueuePop::Item(task) => Some(WaitResult::Task(task)),
            QueuePop::Disconnected => Some(WaitResult::Disconnected),
            QueuePop::Empty => None,
        }
    }
}

impl<C: TaskConsumer> SegmentedWakePolicy<C> for SegmentedWake {
    fn wait(&self, worker: usize, consumer: &C) -> WaitResult {
        self.threads[worker].get_or_init(thread::current);

        if let Some(result) = self.probe(consumer) {
            return result;
        }
        match self.idle {
            SegmentedIdle::SpinThenPark(duration) => {
                if !duration.is_zero() {
                    let start = Instant::now();
                    while start.elapsed() < duration {
                        std::hint::spin_loop();
                        if let Some(result) = self.probe(consumer) {
                            return result;
                        }
                    }
                }
            }
            SegmentedIdle::AgaveBackoff => {
                for step in 0..=10u32 {
                    if step <= 6 {
                        for _ in 0..(1u32 << step) {
                            std::hint::spin_loop();
                        }
                    } else {
                        thread::yield_now();
                    }
                    if let Some(result) = self.probe(consumer) {
                        return result;
                    }
                }
            }
        }
        loop {
            if let Some(result) = self.probe(consumer) {
                return result;
            }
            thread::park();
        }
    }

    #[inline]
    fn wake(&self, worker: usize) {
        // An unregistered worker will observe the task in its first pop.
        if let Some(thread) = self.threads[worker].get() {
            thread.unpark();
        }
    }

    fn wake_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        for thread in &self.threads {
            if let Some(thread) = thread.get() {
                thread.unpark();
            }
        }
    }
}

struct PoolState {
    poisoned: AtomicBool,
}

/// Specialized single-producer pool. Queue and wait policy remain statically
/// dispatched; only the chosen segment number crosses their boundary.
///
/// Unlike the MPMC ThreadPool, submit takes &mut self. Workers execute in
/// parallel while the producer continues submitting.
pub struct SegmentedThreadPool<Q, W>
where
    Q: SegmentedTopology,
    W: SegmentedWakePolicy<Q::Consumer>,
{
    producer: Option<Q::Producer>,
    wake: Arc<W>,
    state: Arc<PoolState>,
    handles: Vec<JoinHandle<()>>,
}

impl<Q, W> SegmentedThreadPool<Q, W>
where
    Q: SegmentedTopology,
    W: SegmentedWakePolicy<Q::Consumer>,
{
    pub fn new(
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        topology: Q,
        wake: W,
    ) -> io::Result<Self> {
        let (producer, consumers) = topology.build(workers, queue_capacity);
        if consumers.len() != workers.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segmented topology must produce exactly one consumer per worker",
            ));
        }

        let wake = Arc::new(wake);
        let state = Arc::new(PoolState {
            poisoned: AtomicBool::new(false),
        });
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(workers.get());
        let mut producer = Some(producer);

        for (index, consumer) in consumers.into_iter().enumerate() {
            let worker_wake = Arc::clone(&wake);
            let worker_state = Arc::clone(&state);
            let result = Builder::new()
                .name(format!("threadance-{index:02}"))
                .spawn(move || worker_loop(index, consumer, worker_wake, worker_state));
            match result {
                Ok(handle) => handles.push(handle),
                Err(err) => {
                    drop(producer.take());
                    wake.wake_all();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(err);
                }
            }
        }
        Ok(Self {
            producer,
            wake,
            state,
            handles,
        })
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.poisoned.load(Ordering::Acquire)
    }

    /// Enqueue and notify precisely the worker selected by the topology.
    pub fn execute<F>(&mut self, task: F) -> Result<(), ExecuteError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_poisoned() {
            return Err(ExecuteError::Poisoned);
        }
        let producer = self.producer.as_mut().ok_or(ExecuteError::Disconnected)?;
        let submitted = producer.push(Box::new(task));
        let worker = match submitted {
            Ok(worker) => worker,
            Err(_) => {
                return Err(if self.is_poisoned() {
                    ExecuteError::Poisoned
                } else {
                    ExecuteError::Disconnected
                });
            }
        };
        assert!(
            worker < self.handles.len(),
            "invalid segmented worker index"
        );
        self.wake.wake(worker);
        if self.is_poisoned() {
            return Err(ExecuteError::Poisoned);
        }
        Ok(())
    }
}

fn worker_loop<C, W>(index: usize, consumer: C, wake: Arc<W>, state: Arc<PoolState>)
where
    C: TaskConsumer,
    W: SegmentedWakePolicy<C>,
{
    loop {
        if state.poisoned.load(Ordering::Acquire) {
            return;
        }
        let task = match consumer.try_pop() {
            QueuePop::Item(task) => task,
            QueuePop::Disconnected => return,
            QueuePop::Empty => match wake.wait(index, &consumer) {
                WaitResult::Task(task) => task,
                WaitResult::Disconnected => return,
                WaitResult::Interrupted => continue,
            },
        };
        if state.poisoned.load(Ordering::Acquire) {
            return;
        }
        if catch_unwind(AssertUnwindSafe(task)).is_err() {
            state.poisoned.store(true, Ordering::Release);
            wake.wake_all();
            return;
        }
    }
}

impl<Q, W> Drop for SegmentedThreadPool<Q, W>
where
    Q: SegmentedTopology,
    W: SegmentedWakePolicy<Q::Consumer>,
{
    fn drop(&mut self) {
        drop(self.producer.take());
        self.wake.wake_all();
        let current = thread::current().id();
        for handle in self.handles.drain(..) {
            if handle.thread().id() == current {
                drop(handle);
            } else {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    // Test fixture only: a bounded channel per worker, with a rotating
    // single producer. The actual gate-ring will live in the harness.
    struct RoundRobin;
    struct Producer {
        senders: Vec<Sender<Task>>,
        next: usize,
    }
    struct Consumer(Receiver<Task>);

    impl SegmentedProducer for Producer {
        fn push(&mut self, task: Task) -> Result<usize, Task> {
            let id = self.next;
            self.senders[id].send(task).map_err(|err| err.0)?;
            self.next = (id + 1) % self.senders.len();
            Ok(id)
        }
    }

    impl TaskConsumer for Consumer {
        fn try_pop(&self) -> QueuePop {
            match self.0.try_recv() {
                Ok(task) => QueuePop::Item(task),
                Err(TryRecvError::Empty) => QueuePop::Empty,
                Err(TryRecvError::Disconnected) => QueuePop::Disconnected,
            }
        }
    }

    impl SegmentedTopology for RoundRobin {
        type Producer = Producer;
        type Consumer = Consumer;

        fn build(self, workers: NonZeroUsize, capacity: NonZeroUsize) -> (Producer, Vec<Consumer>) {
            let (senders, consumers): (Vec<_>, Vec<_>) = (0..workers.get())
                .map(|_| {
                    let (sender, receiver) = bounded(capacity.get());
                    (sender, Consumer(receiver))
                })
                .unzip();
            (Producer { senders, next: 0 }, consumers)
        }
    }

    fn pool(workers: usize, idle: SegmentedIdle) -> SegmentedThreadPool<RoundRobin, SegmentedWake> {
        let workers = NonZeroUsize::new(workers).unwrap();
        SegmentedThreadPool::new(
            workers,
            NonZeroUsize::new(8).unwrap(),
            RoundRobin,
            SegmentedWake::new(workers, idle),
        )
        .unwrap()
    }

    #[test]
    fn tasks_reach_only_their_segment_owner() {
        let mut pool = pool(3, SegmentedIdle::SpinThenPark(Duration::ZERO));
        let (tx, rx) = mpsc::channel();
        for index in 0..90 {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send((index, thread::current().name().unwrap().to_owned()))
                    .unwrap();
            })
            .unwrap();
        }
        drop(pool);
        let names: Vec<_> = rx.try_iter().collect();
        assert_eq!(names.len(), 90);
        for (index, name) in names {
            assert_eq!(name, format!("threadance-{:02}", index % 3));
        }
    }

    #[test]
    fn no_lost_wake_across_repeated_sleep_cycles() {
        for idle in [
            SegmentedIdle::SpinThenPark(Duration::ZERO),
            SegmentedIdle::AgaveBackoff,
        ] {
            let mut pool = pool(2, idle);
            let (tx, rx) = mpsc::channel();
            for index in 0..200 {
                let tx = tx.clone();
                pool.execute(move || tx.send(index).unwrap()).unwrap();
                assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), index);
                thread::yield_now();
            }
        }
    }

    #[test]
    fn drop_drains_work() {
        let mut pool = pool(4, SegmentedIdle::SpinThenPark(Duration::ZERO));
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..160 {
            let done = Arc::clone(&done);
            pool.execute(move || {
                done.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }
        drop(pool);
        assert_eq!(done.load(Ordering::Relaxed), 160);
    }

    #[test]
    fn task_panic_poisons_pool_and_wakes_all() {
        let mut pool = pool(2, SegmentedIdle::SpinThenPark(Duration::ZERO));
        pool.execute(|| panic!("intentional")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !pool.is_poisoned() {
            assert!(Instant::now() < deadline, "pool was not poisoned");
            thread::yield_now();
        }
        assert_eq!(pool.execute(|| {}), Err(ExecuteError::Poisoned));
    }

    #[test]
    fn wrong_consumer_count_is_rejected() {
        struct Missing;
        impl SegmentedTopology for Missing {
            type Producer = Producer;
            type Consumer = Consumer;

            fn build(
                self,
                _workers: NonZeroUsize,
                _capacity: NonZeroUsize,
            ) -> (Producer, Vec<Consumer>) {
                (
                    Producer {
                        senders: Vec::new(),
                        next: 0,
                    },
                    Vec::new(),
                )
            }
        }

        let workers = NonZeroUsize::new(2).unwrap();
        assert!(
            SegmentedThreadPool::new(
                workers,
                NonZeroUsize::new(2).unwrap(),
                Missing,
                SegmentedWake::new(workers, SegmentedIdle::SpinThenPark(Duration::ZERO)),
            )
            .is_err()
        );
    }
}
