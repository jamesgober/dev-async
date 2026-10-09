//! Graceful-shutdown verification.
//!
//! [`ShutdownProbe`] watches a set of components and emits one
//! [`CheckResult`] per component plus an aggregate. A component is
//! considered "drained" when its predicate returns `true`.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Evidence, Severity};

/// A predicate that returns `true` once the named component has
/// reached a clean stopped state.
///
/// The closure may be async (return a future). The probe polls it on
/// a configurable interval until the predicate returns `true` or the
/// deadline elapses.
pub type DrainCheck = Box<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// A component to drain.
pub struct ShutdownComponent {
    name: String,
    drain_check: DrainCheck,
}

impl ShutdownComponent {
    /// Build a component with the given name and drain predicate.
    ///
    /// The predicate MUST return `true` once the component is fully
    /// drained.
    pub fn new<F, Fut>(name: impl Into<String>, drain_check: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let drain_check: DrainCheck = Box::new(move || Box::pin(drain_check()));
        Self {
            name: name.into(),
            drain_check,
        }
    }
}

/// Polls a set of [`ShutdownComponent`]s until they all drain or the
/// deadline elapses.
///
/// # Example
///
/// ```no_run
/// use dev_async::shutdown::{ShutdownComponent, ShutdownProbe};
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// # async fn ex() {
/// let drained = Arc::new(AtomicBool::new(true));
/// let comp = {
///     let drained = drained.clone();
///     ShutdownComponent::new("worker", move || {
///         let d = drained.clone();
///         async move { d.load(Ordering::Relaxed) }
///     })
/// };
///
/// let probe = ShutdownProbe::new("system")
///     .deadline(Duration::from_millis(200))
///     .poll_interval(Duration::from_millis(10))
///     .with_component(comp);
///
/// let checks = probe.run().await;
/// assert!(!checks.is_empty());
/// # }
/// ```
pub struct ShutdownProbe {
    name: String,
    components: Vec<ShutdownComponent>,
    deadline: Duration,
    poll_interval: Duration,
}

impl ShutdownProbe {
    /// Begin building a probe with a stable name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            components: Vec::new(),
            deadline: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
        }
    }

    /// Maximum time to wait for the system to drain.
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = d;
        self
    }

    /// How often to re-evaluate each component's drain predicate.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Add a component to the probe.
    pub fn with_component(mut self, component: ShutdownComponent) -> Self {
        self.components.push(component);
        self
    }

    /// Run the probe and return one [`CheckResult`] per component plus
    /// an aggregate.
    ///
    /// Components are checked in the order they were added, against one
    /// shared deadline measured from the start of `run`. Each predicate
    /// is evaluated at least once, even if an earlier component used up
    /// the deadline, and once more at the deadline itself. A predicate
    /// future that has not resolved by the deadline counts as "not
    /// drained", so a hung predicate cannot hang the probe.
    ///
    /// Per-component verdicts:
    /// - Drained before deadline -> `Pass` with `elapsed_ms` evidence.
    /// - Did not drain in time -> `Fail (Error)` with `not_drained` tag.
    ///
    /// The aggregate verdict is `Fail` if any component failed,
    /// otherwise `Pass`. It is the last entry in the returned vector
    /// and tagged `aggregate`.
    pub async fn run(self) -> Vec<CheckResult> {
        let group = self.name;
        let deadline = self.deadline;
        let interval = self.poll_interval;
        let started = Instant::now();
        // Deadline on tokio's clock so sleeps and the deadline agree.
        // `None` when the deadline is too large to represent: no limit.
        let deadline_at = tokio::time::Instant::now().checked_add(deadline);
        let mut results = Vec::with_capacity(self.components.len() + 1);
        let mut failed_any = false;

        for comp in self.components {
            let comp_name = format!("async::shutdown::{group}::{}", comp.name);
            let comp_started = Instant::now();
            let mut drained = false;
            loop {
                let check = (comp.drain_check)();
                let ok = match deadline_at {
                    // Polled at least once even when the deadline has passed.
                    Some(at) => tokio::time::timeout_at(at, check).await.unwrap_or(false),
                    None => check.await,
                };
                if ok {
                    drained = true;
                    break;
                }
                let now = tokio::time::Instant::now();
                match deadline_at {
                    Some(at) if now >= at => break,
                    Some(at) => {
                        // Never sleep past the deadline; the loop then
                        // makes one last check at the deadline.
                        let wake = now.checked_add(interval).map_or(at, |t| t.min(at));
                        tokio::time::sleep_until(wake).await;
                    }
                    None => tokio::time::sleep(interval).await,
                }
            }
            let elapsed = comp_started.elapsed();
            let evidence = vec![
                Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
                Evidence::numeric("deadline_ms", deadline.as_millis() as f64),
                Evidence::numeric("poll_interval_ms", interval.as_millis() as f64),
            ];
            let mut check = if drained {
                let mut c = CheckResult::pass(comp_name)
                    .with_duration_ms(elapsed.as_millis() as u64)
                    .with_detail(format!("drained in {elapsed:?}"));
                c.tags = vec!["async".to_string(), "shutdown".to_string()];
                c
            } else {
                failed_any = true;
                let mut c = CheckResult::fail(comp_name, Severity::Error)
                    .with_detail(format!("did not drain within {deadline:?}"));
                c.tags = vec![
                    "async".to_string(),
                    "shutdown".to_string(),
                    "not_drained".to_string(),
                    "regression".to_string(),
                ];
                c
            };
            check.evidence = evidence;
            results.push(check);
        }

        let total_elapsed = started.elapsed();
        let aggregate_name = format!("async::shutdown::{group}");
        let evidence = vec![
            Evidence::numeric("components", results.len() as f64),
            Evidence::numeric("elapsed_ms", total_elapsed.as_millis() as f64),
            Evidence::numeric("deadline_ms", deadline.as_millis() as f64),
        ];
        let mut aggregate = if failed_any {
            let mut c = CheckResult::fail(aggregate_name, Severity::Error)
                .with_detail("one or more components did not drain");
            c.tags = vec![
                "async".to_string(),
                "shutdown".to_string(),
                "aggregate".to_string(),
                "regression".to_string(),
            ];
            c
        } else {
            let mut c = CheckResult::pass(aggregate_name)
                .with_duration_ms(total_elapsed.as_millis() as u64)
                .with_detail("all components drained");
            c.tags = vec![
                "async".to_string(),
                "shutdown".to_string(),
                "aggregate".to_string(),
            ];
            c
        };
        aggregate.evidence = evidence;
        results.push(aggregate);
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn already_drained_component_passes() {
        let comp = ShutdownComponent::new("done", || async { true });
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(100))
            .poll_interval(Duration::from_millis(5))
            .with_component(comp);
        let results = probe.run().await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].verdict, Verdict::Pass);
        assert_eq!(results[1].verdict, Verdict::Pass);
        assert!(results[1].has_tag("aggregate"));
    }

    #[tokio::test]
    async fn never_draining_component_fails() {
        let comp = ShutdownComponent::new("hung", || async { false });
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(50))
            .poll_interval(Duration::from_millis(5))
            .with_component(comp);
        let results = probe.run().await;
        assert_eq!(results[0].verdict, Verdict::Fail);
        assert!(results[0].has_tag("not_drained"));
        assert_eq!(results[1].verdict, Verdict::Fail);
    }

    #[tokio::test]
    async fn component_drains_eventually() {
        let flag = Arc::new(AtomicBool::new(false));
        // Trigger the flag after 30ms.
        let f2 = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            f2.store(true, Ordering::Relaxed);
        });
        let f3 = flag.clone();
        let comp = ShutdownComponent::new("delayed", move || {
            let f = f3.clone();
            async move { f.load(Ordering::Relaxed) }
        });
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(200))
            .poll_interval(Duration::from_millis(5))
            .with_component(comp);
        let results = probe.run().await;
        assert_eq!(results[0].verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn drained_component_after_slow_one_still_passes() {
        // "hung" uses up the whole deadline. "done" was drained all
        // along and used to be reported as not drained without its
        // predicate ever being called.
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(40))
            .poll_interval(Duration::from_millis(5))
            .with_component(ShutdownComponent::new("hung", || async { false }))
            .with_component(ShutdownComponent::new("done", || async { true }));
        let results = probe.run().await;
        assert_eq!(results[0].verdict, Verdict::Fail);
        assert_eq!(results[1].verdict, Verdict::Pass);
        assert_eq!(results[2].verdict, Verdict::Fail); // aggregate
    }

    #[tokio::test]
    async fn hung_predicate_does_not_hang_the_probe() {
        let comp = ShutdownComponent::new("stuck", || async {
            std::future::pending::<()>().await;
            true
        });
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(50))
            .poll_interval(Duration::from_millis(5))
            .with_component(comp);
        let started = Instant::now();
        let results = tokio::time::timeout(Duration::from_secs(5), probe.run())
            .await
            .expect("probe hung on a predicate that never resolves");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(results[0].verdict, Verdict::Fail);
        assert!(results[0].has_tag("not_drained"));
    }

    #[tokio::test]
    async fn long_poll_interval_does_not_overrun_deadline() {
        // A 10s poll interval with a 50ms deadline used to sleep the
        // full 10s before giving up.
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(50))
            .poll_interval(Duration::from_secs(10))
            .with_component(ShutdownComponent::new("hung", || async { false }));
        let started = Instant::now();
        let results = probe.run().await;
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(results[0].verdict, Verdict::Fail);
    }

    #[tokio::test]
    async fn checks_again_at_the_deadline() {
        // Drains at ~30ms; the next regular poll would be at 1s, past
        // the 60ms deadline. The final check at the deadline sees it.
        let flag = Arc::new(AtomicBool::new(false));
        let f2 = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            f2.store(true, Ordering::SeqCst);
        });
        let f3 = flag.clone();
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::from_millis(60))
            .poll_interval(Duration::from_secs(1))
            .with_component(ShutdownComponent::new("late", move || {
                let f = f3.clone();
                async move { f.load(Ordering::SeqCst) }
            }));
        let results = probe.run().await;
        assert_eq!(results[0].verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn huge_deadline_does_not_panic() {
        let probe = ShutdownProbe::new("sys")
            .deadline(Duration::MAX)
            .poll_interval(Duration::MAX)
            .with_component(ShutdownComponent::new("a", || async { true }));
        let results = probe.run().await;
        assert_eq!(results[0].verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn aggregate_includes_component_evidence_count() {
        let probe = ShutdownProbe::new("multi")
            .deadline(Duration::from_millis(50))
            .poll_interval(Duration::from_millis(5))
            .with_component(ShutdownComponent::new("a", || async { true }))
            .with_component(ShutdownComponent::new("b", || async { true }));
        let results = probe.run().await;
        assert_eq!(results.len(), 3); // 2 components + aggregate
        let agg = results.last().unwrap();
        let comps = agg
            .evidence
            .iter()
            .find(|e| e.label == "components")
            .unwrap();
        if let dev_report::EvidenceData::Numeric(n) = comps.data {
            assert_eq!(n, 2.0);
        } else {
            panic!("expected numeric");
        }
    }
}
