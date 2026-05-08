//! Heuristic blocking-call detection inside async tasks.
//!
//! Available with the `block-detect` feature.
//!
//! When an async task contains a synchronous I/O call (e.g.
//! `std::fs::read` instead of `tokio::fs::read`), the runtime cannot
//! preempt it, and other tasks on the same worker stall. This module
//! provides a heuristic detector: wrap an async block, and if it
//! takes more than `max_no_yield` real time without yielding, flag it.
//!
//! ## Heuristic, not deterministic
//!
//! This cannot perfectly distinguish "blocking call" from "long
//! synchronous work between awaits". A long pure-CPU section also
//! looks like blocking from this detector's perspective. Treat the
//! verdict as a signal to investigate, not a proof.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Evidence, Severity};

/// Wrap a future and emit a [`CheckResult`] flagging suspected
/// blocking calls inside it.
///
/// The wrapper measures the wall-clock duration between every poll's
/// entry and the immediate-following return. If any single poll
/// exceeds `max_no_yield`, the verdict is `Warn (Warning)` with a
/// `blocking_suspected` tag.
///
/// # Example
///
/// ```no_run
/// use dev_async::blocking::detect_blocking;
/// use std::time::Duration;
///
/// # async fn ex() {
/// let (check, _value) = detect_blocking(
///     "user_op",
///     Duration::from_millis(50),
///     async {
///         // ... possibly-blocking code ...
///         42
///     },
/// ).await;
/// assert!(check.has_tag("async"));
/// # }
/// ```
pub async fn detect_blocking<F, T>(
    name: impl Into<String>,
    max_no_yield: Duration,
    fut: F,
) -> (CheckResult, T)
where
    F: Future<Output = T>,
{
    let name = name.into();
    let started = Instant::now();
    let monitor = BlockingMonitor::new(fut, max_no_yield);
    tokio::pin!(monitor);
    let value = monitor.as_mut().await;
    let elapsed = started.elapsed();
    let max_observed = monitor.max_observed_no_yield();

    let evidence = vec![
        Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
        Evidence::numeric("max_no_yield_ms", max_observed.as_millis() as f64),
        Evidence::numeric("threshold_ms", max_no_yield.as_millis() as f64),
    ];

    let check = if max_observed > max_no_yield {
        let mut c =
            CheckResult::warn(format!("async::{name}"), Severity::Warning).with_detail(format!(
                "longest non-yielding poll was {:?}, exceeds threshold {:?}",
                max_observed, max_no_yield
            ));
        c.tags = vec!["async".to_string(), "blocking_suspected".to_string()];
        c.evidence = evidence;
        c
    } else {
        let mut c = CheckResult::pass(format!("async::{name}"))
            .with_duration_ms(elapsed.as_millis() as u64);
        c.tags = vec!["async".to_string()];
        c.evidence = evidence;
        c
    };
    (check, value)
}

pin_project_lite::pin_project! {
    struct BlockingMonitor<F: Future> {
        #[pin]
        inner: F,
        threshold: Duration,
        max_observed: Duration,
    }
}

impl<F: Future> BlockingMonitor<F> {
    fn new(inner: F, threshold: Duration) -> Self {
        Self {
            inner,
            threshold,
            max_observed: Duration::ZERO,
        }
    }

    fn max_observed_no_yield(self: Pin<&mut Self>) -> Duration {
        *self.project().max_observed
    }
}

impl<F: Future> Future for BlockingMonitor<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let started = Instant::now();
        let result = this.inner.poll(cx);
        let elapsed = started.elapsed();
        if elapsed > *this.max_observed {
            *this.max_observed = elapsed;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;

    #[tokio::test]
    async fn fast_future_passes() {
        let (check, v) = detect_blocking("op", Duration::from_millis(50), async { 42 }).await;
        assert_eq!(check.verdict, Verdict::Pass);
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn long_blocking_section_warns() {
        let (check, _) = detect_blocking("op", Duration::from_millis(5), async {
            // Synchronous sleep simulates blocking work inside the poll.
            std::thread::sleep(Duration::from_millis(20));
        })
        .await;
        assert_eq!(check.verdict, Verdict::Warn);
        assert!(check.has_tag("blocking_suspected"));
    }

    #[tokio::test]
    async fn evidence_includes_max_no_yield() {
        let (check, _) = detect_blocking("op", Duration::from_millis(50), async {}).await;
        let labels: Vec<&str> = check.evidence.iter().map(|e| e.label.as_str()).collect();
        assert!(labels.contains(&"max_no_yield_ms"));
        assert!(labels.contains(&"threshold_ms"));
    }
}
