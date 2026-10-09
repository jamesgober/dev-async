//! Task tracking for leak detection.
//!
//! [`TrackedTaskGroup`] records every task spawned through it and
//! reports any tasks that are still running when the group is
//! finalized.
//!
//! Dropping a group without calling
//! [`finalize`](TrackedTaskGroup::finalize) reports nothing; the tasks
//! keep running detached, as with any dropped `JoinHandle`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dev_report::{CheckResult, Evidence, Severity};
use tokio::task::JoinHandle;

/// A group of spawned tasks whose lifecycle is tracked.
///
/// When [`finalize`](TrackedTaskGroup::finalize) is called, the group
/// joins every handle within the grace period and reports any tasks
/// still running as leaks.
///
/// # Example
///
/// ```no_run
/// use dev_async::tasks::TrackedTaskGroup;
/// use std::time::Duration;
///
/// # async fn ex() {
/// let mut group = TrackedTaskGroup::new("workers");
/// group.spawn(async { tokio::time::sleep(Duration::from_millis(5)).await; });
/// group.spawn(async { tokio::time::sleep(Duration::from_millis(5)).await; });
///
/// let check = group.finalize(Duration::from_millis(50)).await;
/// assert!(check.has_tag("async"));
/// # }
/// ```
pub struct TrackedTaskGroup {
    name: String,
    handles: Vec<JoinHandle<()>>,
    spawned: Arc<AtomicUsize>,
}

impl TrackedTaskGroup {
    /// Create a new group with a stable name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            handles: Vec::new(),
            spawned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Spawn a future and track its handle.
    ///
    /// The future MUST resolve to `()`. If you need a result, capture
    /// it via shared state (e.g. `tokio::sync::oneshot::Sender`).
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like `tokio::spawn`.
    pub fn spawn<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.spawned.fetch_add(1, Ordering::Relaxed);
        self.handles.push(tokio::spawn(fut));
    }

    /// How many tasks have been spawned through this group so far.
    pub fn spawned_count(&self) -> usize {
        self.spawned.load(Ordering::Relaxed)
    }

    /// Join all tracked tasks within a grace period and emit a
    /// [`CheckResult`].
    ///
    /// Every task gets the same grace period, measured from the call.
    /// The tasks are joined concurrently, so `finalize` returns within
    /// about `grace` no matter how many tasks leak. Tasks still running
    /// when the grace period ends are aborted, so a leaked task does not
    /// keep running after the check.
    ///
    /// Verdicts:
    /// - All tasks completed cleanly -> `Pass`.
    /// - One or more tasks panicked -> `Fail (Critical)` with
    ///   `task_panicked` tag.
    /// - One or more tasks did not finish in time (leak suspected) ->
    ///   `Fail (Error)` with `task_leak` tag.
    pub async fn finalize(self, grace: Duration) -> CheckResult {
        let name = format!("async::tasks::{}", self.name);
        let total = self.handles.len();
        let mut completed = 0usize;
        let mut panicked = 0usize;
        let mut leaked = 0usize;

        for outcome in crate::join_with_deadline(self.handles, grace).await {
            match outcome {
                crate::JoinOutcome::Finished(_) => completed += 1,
                crate::JoinOutcome::Failed(_) => panicked += 1,
                crate::JoinOutcome::TimedOut => leaked += 1,
            }
        }

        let evidence = vec![
            Evidence::numeric("spawned", total as f64),
            Evidence::numeric("completed", completed as f64),
            Evidence::numeric("panicked", panicked as f64),
            Evidence::numeric("leaked", leaked as f64),
            Evidence::numeric("grace_ms", grace.as_millis() as f64),
        ];

        let detail = format!(
            "spawned={} completed={} panicked={} leaked={}",
            total, completed, panicked, leaked
        );

        if panicked > 0 {
            let mut c = CheckResult::fail(name, Severity::Critical).with_detail(detail);
            c.tags = vec![
                "async".to_string(),
                "tasks".to_string(),
                "task_panicked".to_string(),
                "regression".to_string(),
            ];
            c.evidence = evidence;
            return c;
        }

        if leaked > 0 {
            let mut c = CheckResult::fail(name, Severity::Error).with_detail(detail);
            c.tags = vec![
                "async".to_string(),
                "tasks".to_string(),
                "task_leak".to_string(),
                "regression".to_string(),
            ];
            c.evidence = evidence;
            return c;
        }

        let mut c = CheckResult::pass(name).with_detail(detail);
        c.tags = vec!["async".to_string(), "tasks".to_string()];
        c.evidence = evidence;
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;

    #[tokio::test]
    async fn empty_group_passes() {
        let g = TrackedTaskGroup::new("empty");
        let c = g.finalize(Duration::from_millis(10)).await;
        assert_eq!(c.verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn all_complete_passes() {
        let mut g = TrackedTaskGroup::new("clean");
        for _ in 0..3 {
            g.spawn(async {
                tokio::time::sleep(Duration::from_millis(2)).await;
            });
        }
        let c = g.finalize(Duration::from_millis(100)).await;
        assert_eq!(c.verdict, Verdict::Pass);
        assert!(c.has_tag("tasks"));
        assert!(!c.has_tag("regression"));
    }

    #[tokio::test]
    async fn panic_yields_critical() {
        let mut g = TrackedTaskGroup::new("panicky");
        g.spawn(async {
            panic!("oops");
        });
        let c = g.finalize(Duration::from_millis(50)).await;
        assert_eq!(c.verdict, Verdict::Fail);
        assert_eq!(c.severity, Some(Severity::Critical));
        assert!(c.has_tag("task_panicked"));
    }

    #[tokio::test]
    async fn leak_yields_error_with_task_leak_tag() {
        let mut g = TrackedTaskGroup::new("leaky");
        g.spawn(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        });
        let c = g.finalize(Duration::from_millis(20)).await;
        assert_eq!(c.verdict, Verdict::Fail);
        assert_eq!(c.severity, Some(Severity::Error));
        assert!(c.has_tag("task_leak"));
    }

    #[tokio::test]
    async fn many_leaks_finalize_within_one_grace_period() {
        // Each leaked handle used to get its own grace period in turn,
        // so 10 leaks with a 200ms grace took about 2s.
        let mut g = TrackedTaskGroup::new("many");
        for _ in 0..10 {
            g.spawn(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
        }
        let started = std::time::Instant::now();
        let c = g.finalize(Duration::from_millis(200)).await;
        let elapsed = started.elapsed();
        assert!(c.has_tag("task_leak"));
        assert!(
            elapsed < Duration::from_secs(1),
            "finalize took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn leaked_tasks_are_aborted() {
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = finished.clone();
        let mut g = TrackedTaskGroup::new("abort");
        g.spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flag.store(true, Ordering::SeqCst);
        });
        let c = g.finalize(Duration::from_millis(10)).await;
        assert!(c.has_tag("task_leak"));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn counts_mixed_outcomes() {
        let mut g = TrackedTaskGroup::new("mixed");
        g.spawn(async {});
        g.spawn(async {
            panic!("boom");
        });
        g.spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let c = g.finalize(Duration::from_millis(50)).await;
        // A panic outranks a leak.
        assert_eq!(c.severity, Some(Severity::Critical));
        assert_eq!(
            c.detail.as_deref(),
            Some("spawned=3 completed=1 panicked=1 leaked=1")
        );
    }

    #[tokio::test]
    async fn spawned_count_tracks_calls() {
        let mut g = TrackedTaskGroup::new("count");
        for _ in 0..4 {
            g.spawn(async {});
        }
        assert_eq!(g.spawned_count(), 4);
        let _ = g.finalize(Duration::from_millis(50)).await;
    }
}
