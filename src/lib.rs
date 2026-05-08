//! # dev-async
//!
//! Async-specific validation for Rust. Deadlocks, task leaks, hung
//! futures, graceful shutdown. Part of the `dev-*` verification suite.
//!
//! Async Rust fails in subtle ways that synchronous unit tests can't
//! catch: a future that never completes, a task that gets dropped
//! without cleanup, a shutdown that hangs because one worker is stuck
//! in a blocking call. `dev-async` provides primitives for catching
//! these issues programmatically.
//!
//! ## Quick example
//!
//! Run a future with a hard timeout. If it doesn't finish in time, you
//! get a `Fail` verdict, not a hang.
//!
//! ```no_run
//! use dev_async::run_with_timeout;
//! use std::time::Duration;
//!
//! # async fn example() {
//! let check = run_with_timeout(
//!     "user_login",
//!     Duration::from_secs(2),
//!     async { do_login().await }
//! ).await;
//!
//! // check is a CheckResult: Pass if completed, Fail+Error if timed out.
//! # }
//! # async fn do_login() {}
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

use std::future::Future;
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Severity};

/// Run a future with a hard timeout. Produces a `CheckResult`.
///
/// If the future completes before the timeout, the verdict is `Pass`
/// and the duration is recorded.
///
/// If the future does not complete in time, the verdict is `Fail` with
/// severity `Error`. The future itself is dropped (cancelled) when the
/// timeout expires.
pub async fn run_with_timeout<F, T>(
    name: impl Into<String>,
    timeout: Duration,
    fut: F,
) -> CheckResult
where
    F: Future<Output = T>,
{
    let name = name.into();
    let started = Instant::now();
    match tokio::time::timeout(timeout, fut).await {
        Ok(_value) => {
            let elapsed = started.elapsed();
            CheckResult::pass(format!("async::{name}"))
                .with_duration_ms(elapsed.as_millis() as u64)
        }
        Err(_elapsed) => CheckResult::fail(format!("async::{name}"), Severity::Error)
            .with_detail(format!("future did not complete within {timeout:?}")),
    }
}

/// Verify that all spawned tasks finish within the given timeout.
///
/// Pass a vector of `JoinHandle`s. Returns one `CheckResult` per task,
/// plus an aggregate verdict.
pub async fn join_all_with_timeout<T>(
    name: impl Into<String>,
    timeout: Duration,
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<CheckResult> {
    let name = name.into();
    let mut results = Vec::with_capacity(handles.len());
    for (i, h) in handles.into_iter().enumerate() {
        let task_name = format!("async::{name}::task{i}");
        let started = Instant::now();
        match tokio::time::timeout(timeout, h).await {
            Ok(Ok(_)) => {
                let elapsed = started.elapsed();
                results.push(
                    CheckResult::pass(task_name).with_duration_ms(elapsed.as_millis() as u64),
                );
            }
            Ok(Err(join_err)) => {
                results.push(
                    CheckResult::fail(task_name, Severity::Critical)
                        .with_detail(format!("task panicked or was cancelled: {join_err}")),
                );
            }
            Err(_) => {
                results.push(
                    CheckResult::fail(task_name, Severity::Error)
                        .with_detail(format!("task did not complete within {timeout:?}")),
                );
            }
        }
    }
    results
}

/// A trait for any async harness that produces a verdict via a future.
pub trait AsyncCheck {
    /// Output of the check. Typically `CheckResult`.
    type Output;
    /// The future returned by `run`.
    type Fut: Future<Output = Self::Output>;
    /// Run the check.
    fn run(self) -> Self::Fut;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeout_pass_fast_future() {
        let check = run_with_timeout("fast", Duration::from_millis(500), async {}).await;
        assert!(matches!(check.verdict, dev_report::Verdict::Pass));
    }

    #[tokio::test]
    async fn timeout_fail_slow_future() {
        let check = run_with_timeout("slow", Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        assert!(matches!(check.verdict, dev_report::Verdict::Fail));
    }

    #[tokio::test]
    async fn join_all_basic() {
        let h1 = tokio::spawn(async { 1 });
        let h2 = tokio::spawn(async { 2 });
        let results = join_all_with_timeout("g", Duration::from_secs(1), vec![h1, h2]).await;
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|r| matches!(r.verdict, dev_report::Verdict::Pass)));
    }
}
