<h1 align="center">
    <strong>dev-async</strong>
    <br>
    <sup><sub>ASYNC-SPECIFIC VALIDATION FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/dev-async"><img alt="crates.io" src="https://img.shields.io/crates/v/dev-async.svg"></a>
    <a href="https://docs.rs/dev-async"><img alt="docs.rs" src="https://docs.rs/dev-async/badge.svg"></a>
    <a href="https://github.com/jamesgober/dev-async/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
</p>

<p align="center">
    Deadlocks, task leaks, hung futures, graceful shutdown.<br>
    Part of the <code>dev-*</code> verification suite.
</p>

---

## What it does

Catches the async-specific failure modes that synchronous tests miss:

- Futures that never complete
- Tasks that get dropped without cleanup
- Shutdown sequences that hang
- Blocking calls inside async paths
- Unbounded task growth

All output flows through `dev-report` so AI agents and CI gates can act
on it without parsing logs.

## Quick start

```toml
[dependencies]
dev-async = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use dev_async::{run_with_timeout, join_all_with_timeout};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Hard-timeout a single future.
    let check = run_with_timeout(
        "user_login",
        Duration::from_secs(2),
        async {
            // your async code here
        }
    ).await;

    // Verify all spawned tasks finish in time.
    let h1 = tokio::spawn(async { 1 });
    let h2 = tokio::spawn(async { 2 });
    let checks = join_all_with_timeout(
        "worker_pool",
        Duration::from_secs(5),
        vec![h1, h2]
    ).await;
}
```

## What's planned

- Deadlock detection wrapper around `parking_lot` / `Mutex` types.
- Task tracking: detect tasks that outlive their parent scope.
- Shutdown verification: confirm a system reaches a clean stopped
  state within a deadline.
- Blocking-call detection inside async contexts (best-effort, behind
  a feature flag).

## License

Apache-2.0. See [LICENSE](LICENSE).
