# Changelog

## [Unreleased]

## [0.9.5] - 2026-10-09

Timeout and shutdown fixes from a review pass, plus the MSRV rollback
to Rust 1.75. The rollback follows `dev-fixtures` 0.9.5 swapping
`tempfile` for `mod-tempdir` 1.0, which removed the
`getrandom 0.4.2 -> edition2024` chain that held the dev-* collection
at 1.85. This crate's own runtime dependencies have always been
1.75-compatible.

### Added

- `VERSION` constant with the crate version as compiled, so tools that
  bundle this crate can report what is actually linked.

### Fixed

- `BlockingAsyncProducer` built with `with_current_thread_runtime` hung
  forever on any sleep or timeout, because `Handle::block_on` cannot
  drive a `current_thread` runtime's timer. A producer that owns its
  runtime now drives it with `Runtime::block_on`.
- `ShutdownProbe::run` marked a component listed after a slow one as
  not drained without ever running its check, hung forever on a drain
  check that never resolved, and could sleep past the deadline with a
  long poll interval. Every check now runs at least once, is bounded by
  the deadline, and gets a final check at the deadline. Very large
  deadlines no longer panic.

### Changed

- `join_all_with_timeout` and `TrackedTaskGroup::finalize` gave each
  task its own timeout in turn, so N hung tasks took N times the
  timeout, and timed-out tasks kept running in the background. All
  tasks are now joined at once against one shared deadline, the call
  returns within about one timeout, and tasks still running at the
  deadline are aborted (REPS section 5).
- `rust-version` lowered from `1.85` to `1.75`. CI's MSRV job now
  builds on 1.75 against an MSRV-compatible lockfile; it was still
  pinned to 1.85.
- `clippy::result_large_err` is allowed on the three public
  lock-timeout helpers. Boxing the error would change their signatures.

### Documentation

- The module list and `deadlock` docs named `try_lock_with_timeout`,
  which does not exist; they now list the real helpers.
- `TrackedTaskGroup` docs no longer claim that dropping the group
  reports leaks.
- `BlockingAsyncProducer` docs say calling it inside a runtime panics
  (not deadlocks), and note the `current_thread` limit of `new()` and
  the `enable_all()` requirement of `with_runtime_builder`.
- README: feature snippet version, MSRV section and badge say 1.75.

[0.9.5]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.5

## [0.9.4] - 2026-05-12

Documentation and SEO pass. No code changes.

### Changed

- README header standardized to match the collection-wide template: Rust logo image, MSRV badge between CI and docs.rs, copyright block at bottom.
- Subtitle now reads `ASYNC RUNTIME VERIFICATION FOR RUST` (was `ASYNC-SPECIFIC VALIDATION FOR RUST`). Specific to what the crate actually verifies; search-tighter.
- Tagline rewritten to lead with what the crate detects (timeouts, deadlocks, task tracking, hung shutdown) instead of the part-of-suite framing.
- `## What it does` rewritten so the consumer story is CI/release-pipeline-first; AI assistants demoted to one of several consumers.
- `## The dev-* suite` block added with the full 14-crate map.
- `Cargo.toml` description rewritten: leads with the failure modes detected.
- `Cargo.toml` keywords retuned: dropped `verification` and `ai-tools`, added `timeout` and `shutdown` for crates.io search.

### Added

- "Part of the `dev-*` verification collection" block on the README, under the intro, linking the umbrella `dev-tools` crate.

[0.9.4]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.4

## [0.9.3] - 2026-05-12

### Added

- `examples/run_with_timeout.rs` — runnable demonstration of `run_with_timeout` against two futures (one fast, one hung), showing the `Pass` and `Fail` verdicts plus the `timeout` tag on the hung case.

### Changed

- CI: `actions/checkout` bumped from `v4` to `v5` (removes Node 20 deprecation warnings).

[0.9.3]: https://github.com/jamesgober/dev-async/releases/tag/v0.9.3

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

[Unreleased]: https://github.com/jamesgober/dev-async/compare/v0.9.3...HEAD
[0.1.0]: https://github.com/jamesgober/dev-async/releases/tag/v0.1.0
