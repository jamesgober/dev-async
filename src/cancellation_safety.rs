//! Cancellation-safety verification.
//!
//! A future is *cancellation-safe* (per the tokio definition) if
//! dropping it mid-poll leaves observable state unchanged compared
//! to never having polled it. Cancellation-unsafety is a common
//! source of data corruption: a `select!` arm that wins drops the
//! losing arms, and if those losing arms had already partially
//! completed visible work (writing bytes, advancing a cursor,
//! holding a lock), the system is now in an inconsistent state.
//!
//! [`check_cancel_safe`] runs a future to a fixed deadline,
//! captures its in-flight state, then drops it and asks the caller
//! to verify state. The verdict reports whether the post-cancel
//! state matches a "safe" predicate.
//!
//! ## What this catches
//!
//! - Futures that buffer writes but flush mid-await.
//! - Futures that consume from a stream and acknowledge before yielding.
//! - State machines that advance internally then await on the next stage.
//!
//! ## What this does NOT catch
//!
//! - Cancellation issues that only manifest under specific schedules.
//! - Issues in nested futures the test doesn't directly inspect.
//!
//! Treat the verdict as a strong signal, not a proof.

use std::future::Future;
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Evidence, Severity};

/// Drive `fut` for at most `cancel_at` duration, then drop it. After
/// the drop, run `assert_safe` to ask the caller whether the
/// observable state is still consistent. Emit a [`CheckResult`].
///
/// Verdicts:
/// - `assert_safe()` returns `true` -> `Pass` with `cancellation_safe`.
/// - `assert_safe()` returns `false` -> `Fail (Critical)` with
///   `cancellation_unsafe` + `regression` tags. State has been
///   corrupted by the cancellation.
/// - `fut` completes before `cancel_at` -> `Skip` with detail
///   ("future completed before cancellation"). The check did not
///   exercise cancellation; tighten `cancel_at` or run a slower
///   future.
///
/// `assert_safe` is invoked synchronously after the future has been
/// dropped. It must not panic.
///
/// # Example
///
/// ```no_run
/// use dev_async::cancellation_safety::check_cancel_safe;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// # async fn ex() {
/// let counter = Arc::new(AtomicUsize::new(0));
/// let c2 = counter.clone();
///
/// let check = check_cancel_safe(
///     "buffered_write",
///     Duration::from_millis(20),
///     async move {
///         c2.fetch_add(1, Ordering::SeqCst);
///         tokio::time::sleep(Duration::from_secs(1)).await;
///         c2.fetch_add(1, Ordering::SeqCst); // never reached if cancelled
///     },
///     || counter.load(Ordering::SeqCst) <= 1,
/// ).await;
///
/// assert!(check.has_tag("async"));
/// # }
/// ```
pub async fn check_cancel_safe<F, Fut, AssertFn>(
    name: impl Into<String>,
    cancel_at: Duration,
    fut: Fut,
    assert_safe: AssertFn,
) -> CheckResult
where
    Fut: Future<Output = F>,
    AssertFn: FnOnce() -> bool,
{
    let name = name.into();
    let started = Instant::now();
    let result = tokio::time::timeout(cancel_at, fut).await;
    let elapsed = started.elapsed();

    let evidence_base = vec![
        Evidence::numeric("cancel_at_ms", cancel_at.as_millis() as f64),
        Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
    ];

    match result {
        Ok(_completed) => {
            let mut c = CheckResult::skip(format!("async::{name}")).with_detail(
                "future completed before cancellation; check did not exercise drop path",
            );
            c.tags = vec!["async".to_string(), "cancellation_check".to_string()];
            c.evidence = evidence_base;
            c
        }
        Err(_elapsed) => {
            // The future was dropped. Now assess state.
            let safe = assert_safe();
            if safe {
                let mut c = CheckResult::pass(format!("async::{name}"))
                    .with_duration_ms(elapsed.as_millis() as u64)
                    .with_detail("future cancelled at deadline; state predicate held");
                c.tags = vec!["async".to_string(), "cancellation_safe".to_string()];
                c.evidence = evidence_base;
                c
            } else {
                let mut c = CheckResult::fail(format!("async::{name}"), Severity::Critical)
                    .with_detail("state predicate failed after future was cancelled mid-poll");
                c.tags = vec![
                    "async".to_string(),
                    "cancellation_unsafe".to_string(),
                    "regression".to_string(),
                ];
                c.evidence = evidence_base;
                c
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn future_that_completes_yields_skip() {
        let check = check_cancel_safe("fast", Duration::from_secs(1), async {}, || true).await;
        assert_eq!(check.verdict, Verdict::Skip);
        assert!(check.has_tag("cancellation_check"));
    }

    #[tokio::test]
    async fn cancellation_with_safe_state_passes() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = counter.clone();
        let check = check_cancel_safe(
            "buffered_write",
            Duration::from_millis(20),
            async move {
                // Buffered work increments counter only on completion.
                tokio::time::sleep(Duration::from_secs(1)).await;
                c2.fetch_add(1, Ordering::SeqCst);
            },
            || counter.load(Ordering::SeqCst) == 0,
        )
        .await;
        assert_eq!(check.verdict, Verdict::Pass);
        assert!(check.has_tag("cancellation_safe"));
    }

    #[tokio::test]
    async fn cancellation_with_unsafe_state_fails() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = counter.clone();
        let check = check_cancel_safe(
            "early_commit",
            Duration::from_millis(20),
            async move {
                // BAD: increments counter before the await — visible
                // even if the future is cancelled.
                c2.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(1)).await;
            },
            || counter.load(Ordering::SeqCst) == 0,
        )
        .await;
        assert_eq!(check.verdict, Verdict::Fail);
        assert_eq!(check.severity, Some(Severity::Critical));
        assert!(check.has_tag("cancellation_unsafe"));
        assert!(check.has_tag("regression"));
    }

    #[tokio::test]
    async fn evidence_includes_cancel_at_and_elapsed() {
        let check = check_cancel_safe(
            "x",
            Duration::from_millis(50),
            async {
                tokio::time::sleep(Duration::from_secs(1)).await;
            },
            || true,
        )
        .await;
        let labels: Vec<&str> = check.evidence.iter().map(|e| e.label.as_str()).collect();
        assert!(labels.contains(&"cancel_at_ms"));
        assert!(labels.contains(&"elapsed_ms"));
    }
}
