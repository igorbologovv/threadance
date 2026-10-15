use std::num::NonZeroUsize;

use crossbeam_channel::{TryRecvError, bounded};

use crate::Task;

/// Result of a non-blocking task-queue probe.
pub enum QueuePop {
    Item(Task),
    Empty,
    Disconnected,
}

/// Producer endpoint created by a [`QueueTopology`].
pub trait TaskProducer: Send + Sync + 'static {
    /// Submits one task to the topology.
    ///
    /// Implementations may block while capacity is unavailable. Returning `Ok(())`
    /// means the task was accepted exactly once. Returning `Err(task)` means the
    /// task was not accepted and ownership is returned to the caller.
    fn push(&self, task: Task) -> Result<(), Task>;
}

/// Worker-side endpoint created by a [`QueueTopology`].
pub trait TaskConsumer: Send + 'static {
    /// Performs one non-blocking queue probe.
    ///
    /// Across all consumers produced by a topology, each accepted task must be
    /// yielded at most once. Once every producer is gone and all accepted work
    /// has been drained, consumers must eventually report [`QueuePop::Disconnected`].
    fn try_pop(&self) -> QueuePop;
}

/// Defines how submitted tasks are routed to workers.
///
/// A topology owns the number and kind of channels. It may create one global
/// MPMC queue, per-worker queues, a dispatcher, or another routing structure
/// without changing the thread-pool or wake-policy code.
pub trait QueueTopology: Send + Sync + 'static {
    type Producer: TaskProducer;
    type Consumer: TaskConsumer;

    /// Builds the producer endpoint and exactly one consumer per worker.
    ///
    /// [`ThreadPool`](crate::ThreadPool) rejects a topology that returns a
    /// different number of consumers. The interpretation of `queue_capacity`
    /// is topology-specific, but implementations should preserve the configured
    /// bounded-capacity behavior rather than silently creating an unbounded path.
    fn build(
        &self,
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> (Self::Producer, Vec<Self::Consumer>);
}

/// One bounded Crossbeam MPMC queue shared by all producers and workers.
#[derive(Debug, Clone, Copy, Default)]
pub struct CrossbeamMpmc;

#[doc(hidden)]
pub struct CrossbeamProducer {
    inner: crossbeam_channel::Sender<Task>,
}

#[doc(hidden)]
pub struct CrossbeamConsumer {
    inner: crossbeam_channel::Receiver<Task>,
}

impl CrossbeamConsumer {
    pub(crate) fn receiver(&self) -> &crossbeam_channel::Receiver<Task> {
        &self.inner
    }
}

impl TaskProducer for CrossbeamProducer {
    fn push(&self, task: Task) -> Result<(), Task> {
        self.inner.send(task).map_err(|error| error.0)
    }
}

impl TaskConsumer for CrossbeamConsumer {
    #[inline]
    fn try_pop(&self) -> QueuePop {
        match self.inner.try_recv() {
            Ok(task) => QueuePop::Item(task),
            Err(TryRecvError::Empty) => QueuePop::Empty,
            Err(TryRecvError::Disconnected) => QueuePop::Disconnected,
        }
    }
}

impl QueueTopology for CrossbeamMpmc {
    type Producer = CrossbeamProducer;
    type Consumer = CrossbeamConsumer;

    fn build(
        &self,
        workers: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> (Self::Producer, Vec<Self::Consumer>) {
        let (sender, receiver) = bounded(queue_capacity.get());
        let consumers = (0..workers.get())
            .map(|_| CrossbeamConsumer {
                inner: receiver.clone(),
            })
            .collect();

        (CrossbeamProducer { inner: sender }, consumers)
    }
}
