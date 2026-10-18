# Threadance

A configurable Rust thread pool for experimenting with how work is delivered and how idle workers wait and wake.

Threadance started while I was working on replacing Rayon in Agave's shred sigverify path ([PR #14641](https://github.com/anza-xyz/agave/pull/14641)). In narrow CPU-bound pipelines like that, a dedicated worker design can sometimes beat a general-purpose scheduler by a very large margin. That made me want a small thread pool where the interesting choices are explicit configuration instead of hard-coded implementation details.

The project is still early. Right now it mostly lets me combine existing queue and wake ideas cleanly; more queue topologies and policies are planned as the research progresses.

## Design

Threadance separates two decisions:

- `QueueTopology` — where tasks are stored, how producers submit them, and how workers obtain them.
- `WakePolicy<C>` — what a worker does after an immediate queue probe finds no work, and how new work or shutdown wakes waiting workers.

For example:

```rust
use std::{error::Error, num::NonZeroUsize};
use threadance::{AgaveBackoff, ThreadPool};

fn main() -> Result<(), Box<dyn Error>> {
    let worker_count =
        NonZeroUsize::new(4).expect("the example uses a non-zero worker count");
    let queue_capacity =
        NonZeroUsize::new(256).expect("the example uses a non-zero queue capacity");

    let pool = ThreadPool::builder(worker_count, queue_capacity)
        .wake_policy(AgaveBackoff::default())
        .build()?;

    pool.execute(|| {
        // CPU-bound work
    })?;

    Ok(())
}
```

The two builder arguments are the number of worker threads and the capacity of the bounded task queue. `NonZeroUsize` makes zero an invalid configuration at the type level. The literals above are known to be non-zero; applications using runtime configuration should validate those values instead of calling `expect` on unchecked input.

`build()` returns an error if the pool cannot be constructed, for example if a worker thread cannot be spawned. `execute()` returns an error if the pool is disconnected or has been poisoned by a task panic.

`execute()` currently uses blocking submission for bounded queues. If the queue is full, the calling thread waits until capacity becomes available. A non-blocking submission API may be added separately later.

Dropping a healthy pool is graceful: the producer is disconnected, already accepted tasks are drained, and idle workers are woken so they can observe shutdown. A task panic is different and poisons the pool immediately.

The concrete pool type contains the selected components, so normal use relies on static dispatch rather than a strategy `match` in the worker hot path.

## Compatibility is explicit

Threadance tries to be flexible, not magical. Queue topology and wake behavior are separate configuration axes, but not every possible pair is meaningful or correct.

A wake protocol may rely on properties that a particular consumer does not provide. A shared MPMC queue, for example, can usually wake any worker because every worker can consume the same task. A per-worker queue design may instead need to wake the specific worker whose queue received the task.

Compatibility is therefore expressed through Rust trait bounds. The core relationship is:

```rust
W: WakePolicy<Q::Consumer>
```

In other words, the selected wake policy must explicitly support the consumer type produced by the selected queue topology. Unsupported combinations should fail at compile time rather than becoming a runtime configuration error.

The trait system can enforce API compatibility; implementations are still responsible for the concurrency semantics and memory-ordering guarantees required by their protocol.

## Experimental single-producer segmented topologies

The experimental segmented API lives alongside the existing MPMC API; it does
not change ThreadPool, TaskProducer, or WakePolicy.

A segmented topology implements SegmentedTopology and returns one
SegmentedProducer plus one TaskConsumer for each worker. The producer owns
its cursor and returns the destination worker index when it accepts a task.
SegmentedThreadPool forwards that index to SegmentedWakePolicy::wake(index),
rather than guessing which sleeping worker owns the task. A built-in
SegmentedWake supports addressed SpinThenPark and AgaveBackoff waiting.

SegmentedThreadPool::execute takes &mut self, intentionally enforcing a
single active producer without a global submission mutex. Task execution
still happens concurrently on worker threads. Applications that need
concurrent producers should choose the MPMC API or explicitly add their
own serialization; the segmented API does not secretly provide MPSC.

The pool does not allocate or manage queue slots: a concrete topology
can use a single contiguous buffer divided into per-worker SPSC rings,
with raw pointers, gate routing, and its own backpressure policy. That
hot-first algorithm belongs in the benchmark harness initially, **not**
in the reusable Threadance API. The library still boxes each task using
the existing Task type; the segmented infrastructure does not add a
second per-task wrapper.

**Lifecycle contract:** dropping the producer must make consumers
eventually return Disconnected after accepted work has been drained.
The provided SegmentedWake must be constructed with the same worker
count as the pool; its per-worker park/unpark handles are registered
before the final queue probe, avoiding lost wakeups during startup.


## Current components

| Queue topology | `CrossbeamWait` | `SpinThenPark` | `AgaveBackoff` |
| --- | ---: | ---: | ---: |
| `CrossbeamMpmc` | yes | yes | yes |

`CrossbeamMpmc` is currently the only queue topology. Adding a second topology is one of the next steps.

### Wake policies

- `CrossbeamWait` delegates blocking task receive to Crossbeam and serves as the native baseline.
- `SpinThenPark` spins for a configurable duration and then blocks on an atomic wake event.
- `AgaveBackoff` uses an Agave-style fixed spin/yield backoff followed by an atomic wait/wake path.

## Agave inspiration and attribution

One of the ideas I specifically wanted to make selectable was the wake path introduced by Alessandro Decina in Agave. His change replaced Crossbeam's blocking receive/waker path with a small futex-style `WakeEvent` in order to avoid contention around Crossbeam's `SyncWaker`:

- [Alessandro Decina — `replay: replace crossbeam blocking receive with plain futex`](https://github.com/anza-xyz/agave/commit/0fbaa230531807f4af65b92e24b44d69d86f3cc3)

Dmitry Adamushko later extracted that implementation, without changing its behavior, into the standalone `agave-wake-channel` crate:

- [`wake-channel: extract the ledger futex channel wrapper`](https://github.com/anza-xyz/agave/commit/25f128d0fcb1735e8b1f31b5f2083fdd28c11df8)
- [Agave `wake-channel/src/lib.rs` at that commit](https://github.com/anza-xyz/agave/blob/25f128d0fcb1735e8b1f31b5f2083fdd28c11df8/wake-channel/src/lib.rs)

Threadance adapts that wake-event/backoff idea so the wake policy is not tied directly to Agave's channel wrapper and can operate through Threadance's `TaskConsumer` abstraction. Threadance also adds its own shutdown handling and wait instrumentation.

See [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) for provenance and licensing details.

## Panic behavior

Threadance is fail-fast. If a submitted task panics, the pool is poisoned, all workers are shut down, and later submissions return `ExecuteError::Poisoned`.

A task panic is treated as a failure of the pool, not as permission to silently continue with fewer workers.

## Status

Threadance requires Rust 1.85 or newer and uses the Rust 2024 edition.

Threadance is experimental and its API is not stable yet. The initial `0.0.x` releases are intended to support the accompanying thread-pool research while the configuration model is still being validated.

## License

Threadance is licensed under the [Apache License 2.0](LICENSE).
