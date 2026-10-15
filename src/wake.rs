use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering, fence},
    },
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender, bounded, select};

use crate::{
    Task,
    queue::{CrossbeamConsumer, QueuePop, TaskConsumer},
};

// The Agave-style backoff and WakeEvent/Waiter protocol below are adapted from
// Anza Agave. Alessandro Decina introduced the futex wake path in commit
// 0fbaa230531807f4af65b92e24b44d69d86f3cc3; Dmitry Adamushko later extracted
// it into `agave-wake-channel` in commit
// 25f128d0fcb1735e8b1f31b5f2083fdd28c11df8 without changing behavior.
// Original work is Apache-2.0 licensed. See ../THIRD_PARTY_NOTICES.md.
// Threadance decouples the protocol from Agave's channel wrapper through
// TaskConsumer/WakePolicy and adds shutdown handling and wait instrumentation.
//
// crossbeam_utils::Backoff 0.8.23 constants used by Agave wake-channel.
const AGAVE_BACKOFF_SPIN_LIMIT: u32 = 6;
const AGAVE_BACKOFF_YIELD_LIMIT: u32 = 10;

/// Result of a wake policy waiting through one complete idle episode.
pub enum WaitResult {
    Task(Task),
    Disconnected,
    Interrupted,
}

/// Complete idle/wake algorithm for a worker pool.
///
/// A policy owns both sides of the protocol: what a worker does after observing
/// an empty queue and how task submission notifies workers that may be blocked.
/// The queue topology is separate; policies interact with it only through the
/// worker-side consumer supplied by the pool.
pub trait WakePolicy<C: TaskConsumer>: Send + Sync + 'static {
    /// Called after the worker has already observed its queue empty. This method
    /// owns the whole idle episode and returns when a task is available, the
    /// queue disconnects, or pool shutdown interrupts the wait.
    ///
    /// Implementations must avoid lost wakeups with their corresponding
    /// [`wake_one`](WakePolicy::wake_one) implementation.
    fn wait(&self, consumer: &C) -> WaitResult;

    /// Called after a task has been pushed successfully.
    ///
    /// If workers may be blocked, this must make at least one eligible waiter
    /// able to observe the newly available work without requiring another task
    /// submission.
    fn wake_one(&self);

    /// Permanently interrupts all current and future waits during pool shutdown.
    ///
    /// This method must release every worker blocked inside [`wait`](WakePolicy::wait)
    /// and make future waits return promptly. Workers may continue draining tasks
    /// already accepted by the queue before the producer disconnected.
    fn wake_all(&self);

    /// Number of workers currently sleeping in the policy's blocking path.
    fn sleeping_waiters(&self) -> usize {
        0
    }
}

/// Delegate task blocking to Crossbeam while retaining an explicit shutdown path.
pub struct CrossbeamWait {
    shutdown_sender: Mutex<Option<Sender<()>>>,
    shutdown_receiver: Receiver<()>,
}

impl Default for CrossbeamWait {
    fn default() -> Self {
        let (shutdown_sender, shutdown_receiver) = bounded(0);
        Self {
            shutdown_sender: Mutex::new(Some(shutdown_sender)),
            shutdown_receiver,
        }
    }
}

impl WakePolicy<CrossbeamConsumer> for CrossbeamWait {
    #[inline]
    fn wait(&self, consumer: &CrossbeamConsumer) -> WaitResult {
        select! {
            recv(consumer.receiver()) -> result => match result {
                Ok(task) => WaitResult::Task(task),
                Err(_) => WaitResult::Disconnected,
            },
            recv(self.shutdown_receiver) -> _ => WaitResult::Interrupted,
        }
    }

    #[inline]
    fn wake_one(&self) {}

    fn wake_all(&self) {
        drop(
            self.shutdown_sender
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take(),
        );
    }
}

/// Spin for a fixed duration, then park on a shared atomic wake event.
pub struct SpinThenPark {
    spin_duration: Duration,
    shutdown: AtomicBool,
    event: WakeEvent,
}

impl SpinThenPark {
    pub fn new(spin_duration: Duration) -> Self {
        Self {
            spin_duration,
            shutdown: AtomicBool::new(false),
            event: WakeEvent::default(),
        }
    }

    pub fn spin_duration(&self) -> Duration {
        self.spin_duration
    }

    #[cold]
    #[inline(never)]
    fn wait_parked<C: TaskConsumer>(&self, consumer: &C) -> WaitResult {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return WaitResult::Interrupted;
            }

            let waiter = self.event.register_waiter();
            match consumer.try_pop() {
                QueuePop::Item(task) => return WaitResult::Task(task),
                QueuePop::Disconnected => return WaitResult::Disconnected,
                QueuePop::Empty if self.shutdown.load(Ordering::Acquire) => {
                    return WaitResult::Interrupted;
                }
                QueuePop::Empty => waiter.wait(),
            }
        }
    }
}

impl<C: TaskConsumer> WakePolicy<C> for SpinThenPark {
    fn wait(&self, consumer: &C) -> WaitResult {
        if self.shutdown.load(Ordering::Acquire) {
            return WaitResult::Interrupted;
        }

        if !self.spin_duration.is_zero() {
            let started = Instant::now();

            loop {
                if self.shutdown.load(Ordering::Acquire) {
                    return WaitResult::Interrupted;
                }

                std::hint::spin_loop();

                match consumer.try_pop() {
                    QueuePop::Item(task) => return WaitResult::Task(task),
                    QueuePop::Disconnected => return WaitResult::Disconnected,
                    QueuePop::Empty if started.elapsed() >= self.spin_duration => break,
                    QueuePop::Empty => {}
                }
            }
        }

        self.wait_parked(consumer)
    }

    #[inline]
    fn wake_one(&self) {
        self.event.wake_one();
    }

    fn wake_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.event.wake_all();
    }

    fn sleeping_waiters(&self) -> usize {
        self.event.sleeping_waiters()
    }
}

/// Agave-style fixed spin/yield backoff followed by atomic blocking wake-up.
#[derive(Default)]
pub struct AgaveBackoff {
    shutdown: AtomicBool,
    event: WakeEvent,
}

impl<C: TaskConsumer> WakePolicy<C> for AgaveBackoff {
    fn wait(&self, consumer: &C) -> WaitResult {
        for step in 0..=AGAVE_BACKOFF_YIELD_LIMIT {
            if self.shutdown.load(Ordering::Acquire) {
                return WaitResult::Interrupted;
            }

            if step <= AGAVE_BACKOFF_SPIN_LIMIT {
                for _ in 0..(1u32 << step) {
                    std::hint::spin_loop();
                }
            } else {
                std::thread::yield_now();
            }

            match consumer.try_pop() {
                QueuePop::Item(task) => return WaitResult::Task(task),
                QueuePop::Disconnected => return WaitResult::Disconnected,
                QueuePop::Empty => {}
            }
        }

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return WaitResult::Interrupted;
            }

            let waiter = self.event.register_waiter();
            match consumer.try_pop() {
                QueuePop::Item(task) => return WaitResult::Task(task),
                QueuePop::Disconnected => return WaitResult::Disconnected,
                QueuePop::Empty if self.shutdown.load(Ordering::Acquire) => {
                    return WaitResult::Interrupted;
                }
                QueuePop::Empty => waiter.wait(),
            }
        }
    }

    #[inline]
    fn wake_one(&self) {
        self.event.wake_one();
    }

    fn wake_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.event.wake_all();
    }

    fn sleeping_waiters(&self) -> usize {
        self.event.sleeping_waiters()
    }
}

#[derive(Default)]
struct WakeEvent {
    cookie: AtomicU32,
    waiters: AtomicUsize,
    sleeping_waiters: AtomicUsize,
}

impl WakeEvent {
    #[inline]
    fn register_waiter(&self) -> WakeWaiter<'_> {
        let cookie = self.cookie.load(Ordering::Relaxed);
        self.waiters.fetch_add(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        WakeWaiter {
            event: self,
            cookie,
        }
    }

    #[inline]
    fn wake_one(&self) {
        fence(Ordering::SeqCst);

        if self.waiters.load(Ordering::Relaxed) != 0 {
            self.cookie.fetch_add(1, Ordering::Relaxed);
            atomic_wait::wake_one(&self.cookie);
        }
    }

    #[cold]
    #[inline(never)]
    fn wake_all(&self) {
        fence(Ordering::SeqCst);

        if self.waiters.load(Ordering::Relaxed) != 0 {
            self.cookie.fetch_add(1, Ordering::Relaxed);
            atomic_wait::wake_all(&self.cookie);
        }
    }

    fn sleeping_waiters(&self) -> usize {
        self.sleeping_waiters.load(Ordering::Relaxed)
    }
}

struct WakeWaiter<'a> {
    event: &'a WakeEvent,
    cookie: u32,
}

impl WakeWaiter<'_> {
    #[cold]
    #[inline(never)]
    fn wait(self) {
        if self.event.cookie.load(Ordering::Relaxed) != self.cookie {
            return;
        }

        self.event.sleeping_waiters.fetch_add(1, Ordering::Relaxed);
        atomic_wait::wait(&self.event.cookie, self.cookie);
        self.event.sleeping_waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for WakeWaiter<'_> {
    fn drop(&mut self) {
        self.event.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}
