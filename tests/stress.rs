use std::{
    io,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use threadance::{
    AgaveBackoff, CrossbeamMpmc, CrossbeamWait, QueuePop, QueueTopology, SpinThenPark, Task,
    TaskConsumer, TaskProducer, ThreadPool, WaitResult, WakePolicy,
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "{message}");
        thread::yield_now();
    }
}

fn assert_lost_wakeup_stress<W>(wake_policy: W)
where
    W: WakePolicy<<CrossbeamMpmc as QueueTopology>::Consumer>,
{
    let pool = ThreadPool::builder(nz(1), nz(4))
        .wake_policy(wake_policy)
        .build()
        .unwrap();
    pool.set_wait_instrumentation(true);

    for _ in 0..200 {
        wait_until(
            || pool.wait_stats().sleeping_waiters == 1,
            "worker never entered the blocking wait path",
        );

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        pool.execute(move || done_tx.send(()).unwrap()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("submitted task was lost while waking a sleeping worker");
    }
}

#[test]
fn no_lost_wakeups_under_repeated_sleep_submit_cycles() {
    assert_lost_wakeup_stress(SpinThenPark::new(Duration::ZERO));
    assert_lost_wakeup_stress(AgaveBackoff::default());
}

fn assert_drop_returns<W>(wake_policy: W, can_observe_sleep: bool)
where
    W: WakePolicy<<CrossbeamMpmc as QueueTopology>::Consumer>,
{
    let pool = ThreadPool::builder(nz(4), nz(8))
        .wake_policy(wake_policy)
        .build()
        .unwrap();
    pool.set_wait_instrumentation(true);

    if can_observe_sleep {
        wait_until(
            || pool.wait_stats().sleeping_waiters == 4,
            "workers never all entered the blocking wait path",
        );
    } else {
        thread::sleep(Duration::from_millis(20));
    }

    let (dropped_tx, dropped_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        drop(pool);
        dropped_tx.send(()).unwrap();
    });

    dropped_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dropping a pool with idle workers hung");
}

#[test]
fn drop_wakes_idle_workers_for_all_builtin_policies() {
    assert_drop_returns(CrossbeamWait::default(), false);
    assert_drop_returns(SpinThenPark::new(Duration::ZERO), true);
    assert_drop_returns(AgaveBackoff::default(), true);
}

#[test]
fn pool_can_be_dropped_from_its_own_worker() {
    let pool = Arc::new(ThreadPool::new(nz(1), nz(4)).unwrap());
    let worker_pool = Arc::clone(&pool);
    let (done_tx, done_rx) = mpsc::sync_channel(1);

    pool.execute(move || {
        drop(worker_pool);
        done_tx.send(()).unwrap();
    })
    .unwrap();

    drop(pool);
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dropping the last pool reference from a worker did not return");
}

fn assert_repeated_lifecycle<F, W>(make_policy: F)
where
    F: Fn() -> W,
    W: WakePolicy<<CrossbeamMpmc as QueueTopology>::Consumer>,
{
    for _ in 0..50 {
        let pool = ThreadPool::builder(nz(2), nz(4))
            .wake_policy(make_policy())
            .build()
            .unwrap();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        pool.execute(move || done_tx.send(()).unwrap()).unwrap();
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(pool);
    }
}

#[test]
fn repeated_create_execute_drop_is_stable() {
    assert_repeated_lifecycle(CrossbeamWait::default);
    assert_repeated_lifecycle(|| SpinThenPark::new(Duration::ZERO));
    assert_repeated_lifecycle(AgaveBackoff::default);
}

#[test]
fn concurrent_producers_execute_every_task_exactly_once() {
    const PRODUCERS: usize = 8;
    const TASKS_PER_PRODUCER: usize = 250;
    const TOTAL: usize = PRODUCERS * TASKS_PER_PRODUCER;

    let pool = Arc::new(ThreadPool::new(nz(4), nz(32)).unwrap());
    let executions = Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (done_tx, done_rx) = mpsc::channel();

    let mut producers = Vec::new();
    for producer in 0..PRODUCERS {
        let pool = Arc::clone(&pool);
        let executions = Arc::clone(&executions);
        let done_tx = done_tx.clone();
        producers.push(thread::spawn(move || {
            for offset in 0..TASKS_PER_PRODUCER {
                let index = producer * TASKS_PER_PRODUCER + offset;
                let executions = Arc::clone(&executions);
                let done_tx = done_tx.clone();
                pool.execute(move || {
                    executions[index].fetch_add(1, Ordering::Relaxed);
                    done_tx.send(()).unwrap();
                })
                .unwrap();
            }
        }));
    }
    drop(done_tx);

    for producer in producers {
        producer.join().unwrap();
    }
    for _ in 0..TOTAL {
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("not all concurrently submitted tasks completed");
    }

    assert!(
        executions
            .iter()
            .all(|count| count.load(Ordering::Relaxed) == 1),
        "every submitted task must execute exactly once"
    );
}

#[test]
fn bounded_queue_applies_backpressure_and_unblocks_after_capacity_frees() {
    let pool = Arc::new(ThreadPool::new(nz(1), nz(1)).unwrap());
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);

    pool.execute(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })
    .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    pool.execute(|| {}).unwrap();

    let blocked_pool = Arc::clone(&pool);
    let (returned_tx, returned_rx) = mpsc::sync_channel(1);
    let (third_done_tx, third_done_rx) = mpsc::sync_channel(1);
    let submitter = thread::spawn(move || {
        let result = blocked_pool.execute(move || third_done_tx.send(()).unwrap());
        returned_tx.send(result).unwrap();
    });

    assert!(
        matches!(
            returned_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "execute returned while the bounded queue was still full"
    );

    release_tx.send(()).unwrap();
    assert_eq!(
        returned_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(())
    );
    third_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    submitter.join().unwrap();
}

#[test]
fn poison_wakes_all_sleeping_workers() {
    let pool = ThreadPool::builder(nz(4), nz(8))
        .wake_policy(SpinThenPark::new(Duration::ZERO))
        .build()
        .unwrap();
    pool.set_wait_instrumentation(true);

    wait_until(
        || pool.wait_stats().sleeping_waiters == 4,
        "workers never all entered sleep before poison test",
    );
    pool.execute(|| panic!("intentional poison")).unwrap();
    wait_until(|| pool.is_poisoned(), "pool was not poisoned");
    wait_until(
        || pool.wait_stats().sleeping_waiters == 0,
        "poison did not wake all sleeping workers",
    );
}

struct EmptyTopology;
struct DummyProducer;
struct DummyConsumer;
struct GenericWake;

impl TaskProducer for DummyProducer {
    fn push(&self, task: Task) -> Result<(), Task> {
        Err(task)
    }
}

impl TaskConsumer for DummyConsumer {
    fn try_pop(&self) -> QueuePop {
        QueuePop::Disconnected
    }
}

impl QueueTopology for EmptyTopology {
    type Producer = DummyProducer;
    type Consumer = DummyConsumer;

    fn build(
        &self,
        _workers: NonZeroUsize,
        _queue_capacity: NonZeroUsize,
    ) -> (Self::Producer, Vec<Self::Consumer>) {
        (DummyProducer, Vec::new())
    }
}

impl<C: TaskConsumer> WakePolicy<C> for GenericWake {
    fn wait(&self, _consumer: &C) -> WaitResult {
        WaitResult::Interrupted
    }

    fn wake_one(&self) {}

    fn wake_all(&self) {}
}

#[test]
fn topology_with_wrong_consumer_count_is_rejected() {
    let result = ThreadPool::builder(nz(2), nz(4))
        .queue(EmptyTopology)
        .wake_policy(GenericWake)
        .build();

    match result {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidInput),
        Ok(_) => panic!("invalid topology unexpectedly built a pool"),
    }
}
