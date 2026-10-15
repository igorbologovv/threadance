# Third-party notices

## Anza Agave wake-channel

Parts of Threadance's Agave-style wake policy and `WakeEvent` / waiter protocol are adapted from the Anza Agave project.

Original implementation:

- Project: Anza Agave
- Author of the futex wake-event change: Alessandro Decina
- Commit: `0fbaa230531807f4af65b92e24b44d69d86f3cc3`
- Commit URL: https://github.com/anza-xyz/agave/commit/0fbaa230531807f4af65b92e24b44d69d86f3cc3
- Original location: `ledger/src/thread_pool.rs`

The implementation was later extracted without behavioral changes into `agave-wake-channel`:

- Extraction author: Dmitry Adamushko
- Commit: `25f128d0fcb1735e8b1f31b5f2083fdd28c11df8`
- Commit URL: https://github.com/anza-xyz/agave/commit/25f128d0fcb1735e8b1f31b5f2083fdd28c11df8
- Source file: https://github.com/anza-xyz/agave/blob/25f128d0fcb1735e8b1f31b5f2083fdd28c11df8/wake-channel/src/lib.rs

Agave is licensed under the Apache License, Version 2.0.

Threadance-specific changes include:

- decoupling queue access from the wake algorithm through `TaskConsumer`;
- integrating the algorithm as a selectable `WakePolicy`;
- adding pool shutdown handling and sleeping-waiter instrumentation used by the benchmark harness.

The adapted code remains distributed under the Apache License, Version 2.0. See `LICENSE`.
