//! Run two futures under a hard timeout — one that finishes in time, one
//! that does not — and print the resulting `CheckResult`s.
//!
//! ```text
//! cargo run --example run_with_timeout
//! ```
//!
//! Demonstrates the headline `dev_async::run_with_timeout` helper:
//! finished futures pass with elapsed-time evidence, hung futures get a
//! `Fail (Error)` verdict with a "timeout" tag — the calling task never
//! hangs.

use std::time::Duration;

use dev_async::run_with_timeout;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let fast = run_with_timeout("fast_op", Duration::from_millis(500), async {
        tokio::time::sleep(Duration::from_millis(10)).await;
    })
    .await;
    println!(
        "fast:    {:?}  tags={:?}  duration={:?}ms",
        fast.verdict, fast.tags, fast.duration_ms
    );

    let hung = run_with_timeout("hung_op", Duration::from_millis(50), async {
        tokio::time::sleep(Duration::from_secs(60)).await;
    })
    .await;
    println!(
        "hung:    {:?}  tags={:?}  severity={:?}",
        hung.verdict, hung.tags, hung.severity
    );
    if let Some(detail) = &hung.detail {
        println!("         detail: {}", detail);
    }
}
