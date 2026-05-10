# Changelog

## [Unreleased]

## [0.9.2] - 2026-05-10

### Added

- New `cancellation_safety` module — `check_cancel_safe(name, cancel_at, fut, assert_safe)` drives a future to a deadline, drops it, and runs the caller's assertion to verify observable state remains consistent. Verdicts: `Pass` + `cancellation_safe`, `Fail (Critical)` + `cancellation_unsafe` + `regression`, or `Skip` if the future completed before cancellation.
- `BlockingAsyncProducer::with_runtime_builder(configure, factory)` — owned-runtime constructor that lets the caller customize the `tokio::runtime::Builder` (worker thread count, thread names, stack size, etc.) before the runtime is built.

[0.9.2]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.2

## [0.9.1] - 2026-05-09

### Added

- `BlockingAsyncProducer::with_new_runtime(factory)` constructor — owns a fresh multi-thread `tokio::runtime::Runtime` for the lifetime of the producer.
- `BlockingAsyncProducer::with_current_thread_runtime(factory)` constructor — owns a `current_thread` runtime; lighter-weight for tests and single-threaded harnesses.
- The existing `BlockingAsyncProducer::new(handle, factory)` is unchanged. New constructors return `io::Result<Self>` because runtime construction can fail on resource exhaustion.

### Fixed

- Broken intra-doc link `[`blocking`]` in the crate-level docstring would warn under `cargo doc` when the `block-detect` feature is disabled. The link is now a plain code span.

[0.9.1]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.1

## [0.9.0] - 2026-05-08

### Added

#### Adoption of dev-report 0.9

- Bumped `dev-report` dep to `0.9`.
- Every `CheckResult` from `run_with_timeout` and `join_all_with_timeout` now carries the `async` tag and numeric `Evidence` for `timeout_ms` (always) and `elapsed_ms` (Pass paths). Failures additionally carry `timeout` / `task_panicked` / `regression` tags.

#### Deadlock detection (v0.2 milestone)

- `dev_async::deadlock` module.
- `try_mutex_lock_with_timeout`, `try_rwlock_read_with_timeout`, `try_rwlock_write_with_timeout` for `tokio::sync::{Mutex, RwLock}`.
- Timeout -> `Fail (Error)` with `deadlock_suspected` + `regression` tags.

#### Task tracking (v0.3 milestone)

- `dev_async::tasks::TrackedTaskGroup`.
- `spawn` records each task; `finalize(grace)` joins all and emits one aggregate `CheckResult`.
- Panic -> `Fail (Critical)` + `task_panicked`. Leak -> `Fail (Error)` + `task_leak`.

#### Graceful shutdown (v0.4 milestone)

- `dev_async::shutdown::ShutdownProbe` and `ShutdownComponent`.
- Polls each component's drain predicate on a configurable interval until deadline.
- Emits one `CheckResult` per component plus an `aggregate`-tagged summary.

#### Blocking-call detection (v0.5 milestone, opt-in)

- `block-detect` feature flag (off by default; pulls `pin-project-lite`).
- `dev_async::blocking::detect_blocking` wraps a future and tracks longest non-yielding poll duration.
- Exceeds threshold -> `Warn (Warning)` + `blocking_suspected`.

#### Producer integration

- `AsyncProducer` trait for async harnesses.
- `BlockingAsyncProducer<F, Fut>` adapter implementing `dev_report::Producer` via `tokio::runtime::Handle::block_on`.
- `AsyncCheck` trait retained from v0.1.

### Documentation

- All public items have rustdoc with at least one example.
- REPS.md expanded: §4 (required evidence/tags), §6 (deadlock helpers), §7 (task tracking), §8 (shutdown), §9 (block-detect), §10 (producer integration).

[0.9.0]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.0

## [0.1.0] - 2026-05-07

### Added

- Initial crate skeleton.
- `run_with_timeout`: hard-timeout wrapper for a single future,
  produces a `dev-report::CheckResult`.
- `join_all_with_timeout`: collects verdicts for a vector of
  `JoinHandle`s with timeout enforcement.
- `AsyncCheck` trait for harness integration.
- Smoke tests covering pass, fail-by-timeout, and multi-task paths.

### Note

Name-claim release. Deadlock detection, task tracking, and shutdown
verification land in `0.2.x` and beyond.

[Unreleased]: https://github.com/jamesgober/dev-async/compare/v0.9.2...HEAD
[0.1.0]: https://github.com/jamesgober/dev-async/releases/tag/v0.1.0
