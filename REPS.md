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

## 5. Cancellation

When a timeout expires, the underlying future MUST be cancelled
(dropped). The crate MUST NOT return until the cancellation is
observable from the caller's perspective.

## 6. Safety guarantees

- No global state.
- Re-entrant: multiple concurrent invocations of `run_with_timeout`
  in the same runtime MUST not interfere.
- No blocking calls in this crate's own code.
