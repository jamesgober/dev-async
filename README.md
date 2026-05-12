<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <strong>dev-async</strong>
    <br>
    <sup><sub>ASYNC RUNTIME VERIFICATION FOR RUST</sub></sup>
</h1>
<p align="center">
    <a href="https://crates.io/crates/dev-async"><img alt="crates.io" src="https://img.shields.io/crates/v/dev-async.svg"></a>
    <a href="https://crates.io/crates/dev-async"><img alt="downloads" src="https://img.shields.io/crates/d/dev-async.svg"></a>
    <a href="https://github.com/jamesgober/dev-async/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamesgober/dev-async/actions/workflows/ci.yml/badge.svg"></a>
    <img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue.svg?style=flat-square" title="Rust Version">
    <a href="https://docs.rs/dev-async"><img alt="docs.rs" src="https://docs.rs/dev-async/badge.svg"></a>
</p>

<p align="center">
    <strong>Timeouts, deadlock detection, task tracking, hung-shutdown probes.</strong> The async failure modes that synchronous tests can't catch.
</p>

<br>

<div align="center">
    <strong>Part of the <a href="https://crates.io/crates/dev-tools"><code>dev-*</code></a> verification collection.</strong><br>
    <sub>Also available as the <code>async</code> feature of the <a href="https://crates.io/crates/dev-tools"><code>dev-tools</code></a> umbrella crate &mdash; one dependency, every verification layer.</sub>
</div>

<br>

---

## What it does

Catches the async-specific failure modes that synchronous tests miss:

- Futures that never complete
- Tasks that get dropped without cleanup
- Shutdown sequences that hang
- Blocking calls inside async paths
- Unbounded task growth

All output flows through `dev-report` so CI gates, release pipelines,
and AI assistants can act on it without scraping logs.

## Quick start

```toml
[dependencies]
dev-async = "0.9.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Opt-in features:

```toml
[dependencies]
dev-async = { version = "0.9.4", features = ["block-detect"] }
```

```rust
use dev_async::{run_with_timeout, join_all_with_timeout};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Hard-timeout a single future.
    let _check = run_with_timeout(
        "user_login",
        Duration::from_secs(2),
        async {
            // your async code here
        }
    ).await;

    // Verify all spawned tasks finish in time.
    let h1 = tokio::spawn(async { 1 });
    let h2 = tokio::spawn(async { 2 });
    let _checks = join_all_with_timeout(
        "worker_pool",
        Duration::from_secs(5),
        vec![h1, h2]
    ).await;
}
```

The returned `CheckResult` carries the `async` tag, plus
`timeout`/`task_panicked`/`regression` tags on failure paths, plus
numeric `Evidence` for `timeout_ms` and (on Pass) `elapsed_ms`.

## Deadlock detection

```rust
use dev_async::deadlock::try_mutex_lock_with_timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

# async fn ex() {
let m = Arc::new(Mutex::new(0));
match try_mutex_lock_with_timeout("counter", &m, Duration::from_millis(50)).await {
    Ok((_check, mut guard)) => *guard += 1,
    Err(check) => {
        assert!(check.has_tag("deadlock_suspected"));
    }
};
# }
```

## Task leak detection

```rust
use dev_async::tasks::TrackedTaskGroup;
use std::time::Duration;

# async fn ex() {
let mut group = TrackedTaskGroup::new("workers");
group.spawn(async { /* work */ });
group.spawn(async { /* work */ });

// Joins all with a grace period; any unfinished task -> task_leak tag.
let _check = group.finalize(Duration::from_millis(200)).await;
# }
```

## Graceful shutdown

```rust
use dev_async::shutdown::{ShutdownComponent, ShutdownProbe};
use std::time::Duration;

# async fn ex() {
let probe = ShutdownProbe::new("system")
    .deadline(Duration::from_secs(5))
    .poll_interval(Duration::from_millis(50))
    .with_component(ShutdownComponent::new("workers", || async {
        // return true once drained
        true
    }))
    .with_component(ShutdownComponent::new("connections", || async { true }));

let checks = probe.run().await;
// Last entry is the aggregate verdict.
let _aggregate = checks.last().unwrap();
# }
```

## Blocking-call detection (opt-in)

```rust,ignore
use dev_async::blocking::detect_blocking;
use std::time::Duration;

# async fn ex() {
let (_check, value) = detect_blocking(
    "user_op",
    Duration::from_millis(50),
    async {
        // possibly-blocking code
        42
    },
).await;
# }
```

The detector flags `Warn` with a `blocking_suspected` tag if any
single poll exceeds the threshold. Heuristic: a long pure-CPU
section also looks like blocking from this detector's perspective.

## Producer trait

`dev-report::Producer` is sync. For async harnesses, this crate
provides the `AsyncProducer` trait and a `BlockingAsyncProducer`
adapter that satisfies `Producer` by calling `block_on` from a
sync context.

```rust,no_run
use dev_async::{run_with_timeout, BlockingAsyncProducer};
use dev_report::{Producer, Report};
use std::time::Duration;

let rt = tokio::runtime::Runtime::new().unwrap();
let handle = rt.handle().clone();
let producer = BlockingAsyncProducer::new(handle, || async {
    let check = run_with_timeout("op", Duration::from_millis(50), async {}).await;
    let mut r = Report::new("crate", "0.1.0").with_producer("dev-async");
    r.push(check);
    r.finish();
    r
});
let _report = producer.produce();
```

## The `dev-*` collection

`dev-async` ships independently and is also re-exported by the
[`dev-tools`](https://crates.io/crates/dev-tools) umbrella crate as
the `async` feature. Sister crates cover the other verification
dimensions:

- [`dev-report`](https://crates.io/crates/dev-report) &mdash; report schema everything emits
- [`dev-fixtures`](https://crates.io/crates/dev-fixtures) &mdash; deterministic test fixtures
- [`dev-bench`](https://crates.io/crates/dev-bench) &mdash; performance and regression detection
- [`dev-stress`](https://crates.io/crates/dev-stress) &mdash; stress and soak workloads
- [`dev-chaos`](https://crates.io/crates/dev-chaos) &mdash; fault injection and recovery testing
- [`dev-coverage`](https://crates.io/crates/dev-coverage) &mdash; code coverage with regression gates
- [`dev-security`](https://crates.io/crates/dev-security) &mdash; CVE / license / banned-crate audit
- [`dev-deps`](https://crates.io/crates/dev-deps) &mdash; unused / outdated dep detection
- [`dev-ci`](https://crates.io/crates/dev-ci) &mdash; GitHub Actions workflow generator
- [`dev-fuzz`](https://crates.io/crates/dev-fuzz) &mdash; fuzz testing workflow
- [`dev-flaky`](https://crates.io/crates/dev-flaky) &mdash; flaky-test detection
- [`dev-mutate`](https://crates.io/crates/dev-mutate) &mdash; mutation testing

## Status

`v0.9.x` is the pre-1.0 stabilization line. APIs are expected to be
near-final; minor adjustments may still happen ahead of `1.0`. The
timeout / cancellation contract (REPS § 4-5) will not change.

## Minimum supported Rust version

`1.85` — pinned in `Cargo.toml` via `rust-version` and verified by
the MSRV job in CI. (Bumped from 1.75 to align with the suite's
shared MSRV after sibling crates picked up dependencies that require
`edition2024`.)

## License

Apache-2.0. See [LICENSE](LICENSE).




<!-- COPYRIGHT
---------------------------------->
<div align="center">
    <br>
    <h2></h2>
    Copyright &copy; 2026 James Gober.
</div>
