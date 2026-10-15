mod queue;
mod wake;

use std::{
    fmt, io,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, Builder, JoinHandle},
    time::Instant,
};

pub use queue::{CrossbeamMpmc, QueuePop, QueueTopology, TaskConsumer, TaskProducer};
pub use wake::{AgaveBackoff, CrossbeamWait, SpinThenPark, WaitResult, WakePolicy};

pub type Task = Box<dyn FnOnce() + Send + 'static>;

static MONOTONIC_ORIGIN: OnceLock<Instant> = OnceLock::new();

pub fn monotonic_now_ns() -> u64 {
    MONOTONIC_ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WaitStatsSnapshot {
    pub empty_observations: u64,
    pub last_empty_ns: u64,
    pub sleeping_waiters: usize,
}

#[derive(Default)]
struct WaitStats {
    enabled: AtomicBool,
    empty_observations: AtomicU64,
    last_empty_ns: AtomicU64,
}

impl WaitStats {
    fn reset(&self) {
        self.empty_observations.store(0, Ordering::Relaxed);
        self.last_empty_ns.store(0, Ordering::Relaxed);
    }

    fn record_empty(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            let now_ns = monotonic_now_ns();
            self.last_empty_ns.store(now_ns, Ordering::Relaxed);
            self.empty_observations.fetch_add(1, Ordering::Release);
        }
    }
}

#[derive(Default)]
struct Shared {
    stats: WaitStats,
    poisoned: AtomicBool,
}

pub struct ThreadPoolBuilder<Q = CrossbeamMpmc, W = CrossbeamWait> {
    workers: NonZeroUsize,
    queue_capacity: NonZeroUsize,
    queue: Q,
    wake_policy: W,
}

impl ThreadPoolBuilder<CrossbeamMpmc, CrossbeamWait> {
    pub fn new(workers: NonZeroUsize, queue_capacity: NonZeroUsize) -> Self {
        Self {
            workers,
            queue_capacity,
            queue: CrossbeamMpmc,
            wake_policy: CrossbeamWait::default(),
        }
    }
}

impl<Q, W> ThreadPoolBuilder<Q, W> {
    pub fn queue<Q2>(self, queue: Q2) -> ThreadPoolBuilder<Q2, W> {
        ThreadPoolBuilder {
            workers: self.workers,
            queue_capacity: self.queue_capacity,
            queue,
            wake_policy: self.wake_policy,
        }
    }

    pub fn wake_policy<W2>(self, wake_policy: W2) -> ThreadPoolBuilder<Q, W2> {
        ThreadPoolBuilder {
            workers: self.workers,
            queue_capacity: self.queue_capacity,
            queue: self.queue,
            wake_policy,
        }
    }

    pub fn build(self) -> io::Result<ThreadPool<Q, W>>
    where
        Q: QueueTopology,
        W: WakePolicy<Q::Consumer>,
    {
        ThreadPool::from_parts(
            self.workers,
            self.queue_capacity,
            self.queue,
            self.wake_policy,
        )
    }
}

pub struct ThreadPool<Q = CrossbeamMpmc, W = CrossbeamWait>
where
    Q: QueueTopology,
    W: WakePolicy<Q::Consumer>,
{
    producer: Option<Q::Producer>,
    shared: Arc<Shared>,
    wake_policy: Arc<W>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl ThreadPool<CrossbeamMpmc, CrossbeamWait> {
    pub fn builder(
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> ThreadPoolBuilder<CrossbeamMpmc, CrossbeamWait> {
        ThreadPoolBuilder::new(workers, queue_capacity)
    }

    pub fn new(workers: NonZeroUsize, queue_capacity: NonZeroUsize) -> io::Result<Self> {
        Self::builder(workers, queue_capacity).build()
    }
}

impl<Q, W> ThreadPool<Q, W>
where
    Q: QueueTopology,
    W: WakePolicy<Q::Consumer>,
{
    fn from_parts(
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        queue: Q,
        wake_policy: W,
    ) -> io::Result<Self> {
        let (producer, consumers) = queue.build(workers, queue_capacity);
        if consumers.len() != workers.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue topology must create exactly one consumer per worker",
            ));
        }

        let wake_policy = Arc::new(wake_policy);
        let shared = Arc::new(Shared::default());
        let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(workers.get());
        let mut producer = Some(producer);

        for (index, consumer) in consumers.into_iter().enumerate() {
            let worker_wake_policy = Arc::clone(&wake_policy);
            let shared = Arc::clone(&shared);

            let handle = match Builder::new()
                .name(format!("threadance-{index:02}"))
                .spawn(move || worker_loop(consumer, worker_wake_policy, shared))
            {
                Ok(handle) => handle,
                Err(error) => {
                    drop(producer.take());
                    wake_policy.wake_all();

                    for handle in worker_handles {
                        let _ = handle.join();
                    }

                    return Err(error);
                }
            };

            worker_handles.push(handle);
        }

        Ok(Self {
            producer,
            shared,
            wake_policy,
            worker_handles,
        })
    }

    pub fn set_wait_instrumentation(&self, enabled: bool) {
        self.shared.stats.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn reset_wait_stats(&self) {
        self.shared.stats.reset();
    }

    pub fn wait_stats(&self) -> WaitStatsSnapshot {
        WaitStatsSnapshot {
            empty_observations: self.shared.stats.empty_observations.load(Ordering::Acquire),
            last_empty_ns: self.shared.stats.last_empty_ns.load(Ordering::Relaxed),
            sleeping_waiters: self.wake_policy.sleeping_waiters(),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.shared.poisoned.load(Ordering::Acquire)
    }

    pub fn execute<F>(&self, task: F) -> Result<(), ExecuteError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_poisoned() {
            return Err(ExecuteError::Poisoned);
        }

        let producer = self.producer.as_ref().ok_or(ExecuteError::Disconnected)?;
        if producer.push(Box::new(task)).is_err() {
            return Err(if self.is_poisoned() {
                ExecuteError::Poisoned
            } else {
                ExecuteError::Disconnected
            });
        }

        if self.is_poisoned() {
            return Err(ExecuteError::Poisoned);
        }

        self.wake_policy.wake_one();
        Ok(())
    }
}

#[inline]
fn run_task<C, W>(task: Task, wake_policy: &W, shared: &Shared) -> bool
where
    C: TaskConsumer,
    W: WakePolicy<C>,
{
    if catch_unwind(AssertUnwindSafe(task)).is_ok() {
        return true;
    }

    shared.poisoned.store(true, Ordering::Release);
    wake_policy.wake_all();
    false
}

#[inline]
fn worker_loop<C, W>(consumer: C, wake_policy: Arc<W>, shared: Arc<Shared>)
where
    C: TaskConsumer,
    W: WakePolicy<C>,
{
    loop {
        if shared.poisoned.load(Ordering::Acquire) {
            return;
        }

        match consumer.try_pop() {
            QueuePop::Item(task) => {
                if !run_task::<C, W>(task, wake_policy.as_ref(), shared.as_ref()) {
                    return;
                }
            }
            QueuePop::Disconnected => return,
            QueuePop::Empty => {
                shared.stats.record_empty();
                match wake_policy.wait(&consumer) {
                    WaitResult::Task(task) => {
                        if shared.poisoned.load(Ordering::Acquire) {
                            return;
                        }
                        if !run_task::<C, W>(task, wake_policy.as_ref(), shared.as_ref()) {
                            return;
                        }
                    }
                    WaitResult::Disconnected => return,
                    WaitResult::Interrupted => continue,
                }
            }
        }
    }
}

impl<Q, W> Drop for ThreadPool<Q, W>
where
    Q: QueueTopology,
    W: WakePolicy<Q::Consumer>,
{
    fn drop(&mut self) {
        drop(self.producer.take());
        self.wake_policy.wake_all();

        let current_thread = thread::current().id();
        for handle in self.worker_handles.drain(..) {
            if handle.thread().id() == current_thread {
                // The last Arc<ThreadPool> may be dropped from inside a submitted
                // task. Joining that worker here would attempt to join the current
                // thread, so detach its handle and let the worker return normally.
                drop(handle);
            } else {
                let _ = handle.join();
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteError {
    Disconnected,
    Poisoned,
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => f.write_str("thread pool is disconnected"),
            Self::Poisoned => f.write_str("thread pool is poisoned because a task panicked"),
        }
    }
}

impl std::error::Error for ExecuteError {}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use super::{
        AgaveBackoff, CrossbeamMpmc, CrossbeamWait, ExecuteError, QueuePop, SpinThenPark,
        TaskConsumer, ThreadPool, WaitResult, WakePolicy,
    };

    fn assert_executes<W>(wake_policy: W)
    where
        W: WakePolicy<<CrossbeamMpmc as super::QueueTopology>::Consumer>,
    {
        let pool = ThreadPool::builder(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        )
        .wake_policy(wake_policy)
        .build()
        .unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }

        drop(pool);
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn crossbeam_wait_executes_submitted_jobs() {
        assert_executes(CrossbeamWait::default());
    }

    #[test]
    fn spin_then_park_executes_submitted_jobs() {
        assert_executes(SpinThenPark::new(Duration::from_micros(50)));
    }

    #[test]
    fn agave_backoff_executes_submitted_jobs() {
        assert_executes(AgaveBackoff::default());
    }

    #[derive(Default)]
    struct YieldUntilReady {
        shutdown: AtomicBool,
    }

    impl<C: TaskConsumer> WakePolicy<C> for YieldUntilReady {
        fn wait(&self, consumer: &C) -> WaitResult {
            loop {
                if self.shutdown.load(Ordering::Acquire) {
                    return WaitResult::Interrupted;
                }

                match consumer.try_pop() {
                    QueuePop::Item(task) => return WaitResult::Task(task),
                    QueuePop::Disconnected => return WaitResult::Disconnected,
                    QueuePop::Empty => thread::yield_now(),
                }
            }
        }

        fn wake_one(&self) {}

        fn wake_all(&self) {
            self.shutdown.store(true, Ordering::Release);
        }
    }

    #[test]
    fn custom_wake_policy_can_be_supplied() {
        assert_executes(YieldUntilReady::default());
    }

    #[test]
    fn parked_worker_is_woken_for_new_work() {
        let pool =
            ThreadPool::builder(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(4).unwrap())
                .wake_policy(SpinThenPark::new(Duration::ZERO))
                .build()
                .unwrap();
        pool.set_wait_instrumentation(true);
        pool.reset_wait_stats();

        let deadline = Instant::now() + Duration::from_secs(1);
        while pool.wait_stats().sleeping_waiters == 0 {
            assert!(Instant::now() < deadline, "worker never entered sleep path");
            thread::yield_now();
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        pool.execute(move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while counter.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline, "sleeping worker was not woken");
            thread::yield_now();
        }
    }

    fn assert_task_panic_poisons_pool<W>(wake_policy: W)
    where
        W: WakePolicy<<CrossbeamMpmc as super::QueueTopology>::Consumer>,
    {
        let pool = ThreadPool::builder(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        )
        .wake_policy(wake_policy)
        .build()
        .unwrap();

        pool.execute(|| panic!("intentional task panic")).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while !pool.is_poisoned() {
            assert!(
                Instant::now() < deadline,
                "pool was not poisoned after task panic"
            );
            thread::yield_now();
        }

        assert_eq!(pool.execute(|| {}), Err(ExecuteError::Poisoned));
        drop(pool);
    }

    #[test]
    fn task_panic_poisons_crossbeam_wait_pool() {
        assert_task_panic_poisons_pool(CrossbeamWait::default());
    }

    #[test]
    fn task_panic_poisons_spin_then_park_pool() {
        assert_task_panic_poisons_pool(SpinThenPark::new(Duration::ZERO));
    }

    #[test]
    fn task_panic_poisons_agave_backoff_pool() {
        assert_task_panic_poisons_pool(AgaveBackoff::default());
    }
}
