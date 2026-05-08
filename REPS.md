# dev-async — Project Specification (REPS)

> Rust Engineering Project Specification.
> Normative language follows RFC 2119.

## 1. Purpose

`dev-async` MUST detect async-specific failure modes that synchronous
tests cannot catch: hung futures, task leaks, blocking inside async,
shutdown that never completes. Output MUST be `dev-report::CheckResult`.

## 2. Scope

This crate MUST provide:

- A timeout wrapper for a single future.
- A timeout-aware joiner for a vector of `JoinHandle`s.
- An `AsyncCheck` trait for harness integration.

This crate SHOULD provide (later versions):

- Deadlock detection wrappers (timeout-based, best-effort).
- Task tracking primitives.
- Graceful shutdown verification.
- Blocking-call detection inside async (feature-gated, best-effort).

This crate MUST NOT:

- Replace `tokio-test` for unit-test scaffolding.
- Run synchronous benchmarks (`dev-bench`).
- Inject failures (`dev-chaos`).

## 3. Runtime requirements

This crate REQUIRES tokio. Other runtimes MAY be added behind feature
flags later, but tokio is the minimum. The `rt`, `rt-multi-thread`,
`sync`, `time`, and `macros` features are required.

## 4. Timeout contract

- A future that completes before the timeout MUST result in `Pass`.
- A future that does not complete MUST result in `Fail` with severity
  `Error`.
- A spawned task that panics MUST result in `Fail` with severity
  `Critical`.
- The check MUST capture and report duration in milliseconds.

### 4.1 Required evidence and tags

Every `CheckResult` from this crate MUST carry the `async` tag.
Numeric `Evidence` MUST include:

- `timeout_ms` — the configured timeout.
- `elapsed_ms` — actual elapsed wall-clock (on Pass paths).

Failure-flagged checks MUST additionally carry the `regression` tag,
plus a per-cause tag:

- `timeout` — future or task did not complete in time.
- `task_panicked` — joinhandle reported a panic.
- `deadlock_suspected` — lock acquisition timed out.
- `task_leak` — `TrackedTaskGroup` finalize observed an unfinished task.
- `not_drained` — `ShutdownProbe` found a component still running.
- `blocking_suspected` — `block-detect` observed a long non-yielding poll.

## 5. Cancellation

When a timeout expires, the underlying future MUST be cancelled
(dropped). The crate MUST NOT return until the cancellation is
observable from the caller's perspective.

## 6. Deadlock helpers

`dev_async::deadlock::try_*_with_timeout` wraps `tokio::sync` lock
acquisitions with a hard deadline. On timeout:

- Verdict is `Fail (Error)`.
- Tags include `deadlock_suspected`.

This is timeout-based detection only. Real lock-graph cycle detection
is out of scope for `0.x`.

## 7. Task tracking

`dev_async::tasks::TrackedTaskGroup` records every task it spawns and
joins them with a configurable grace period at finalize:

- All complete cleanly -> `Pass`.
- Any task panics -> `Fail (Critical)` + `task_panicked`.
- Any task does not finish in grace -> `Fail (Error)` + `task_leak`.

## 8. Graceful shutdown

`dev_async::shutdown::ShutdownProbe` polls a set of named components
until each drains or the deadline elapses.

- One `CheckResult` per component, plus an aggregate.
- Per-component drained -> `Pass`. Not drained -> `Fail (Error)` + `not_drained`.
- Aggregate -> `Pass` only if every component drained.

## 9. Blocking-call detection (opt-in)

Available with the `block-detect` feature.

`dev_async::blocking::detect_blocking` wraps a future and tracks the
longest single poll's wall-clock duration. If any poll exceeds
`max_no_yield`, the verdict is `Warn (Warning)` with
`blocking_suspected` tag.

This is heuristic. A long pure-CPU section also looks like blocking
from this detector's perspective. Treat the verdict as a signal to
investigate, not a proof.

## 10. Producer integration

`dev-report::Producer` is synchronous, which doesn't fit async
harnesses. This crate provides:

- `AsyncCheck` trait for async producers (returns a future).
- `AsyncProducer` trait for async harnesses building a `Report`.
- `BlockingAsyncProducer<F, Fut>` adapter implementing
  `dev_report::Producer` by calling
  `tokio::runtime::Handle::current().block_on(...)` internally.
  MUST be invoked from a sync context.

## 11. Safety guarantees

- No global state.
- Re-entrant: multiple concurrent invocations of `run_with_timeout`
  in the same runtime MUST not interfere.
- No blocking calls in this crate's own code (per DIRECTIVES § 5).
