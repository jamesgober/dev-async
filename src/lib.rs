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
//! let _check = run_with_timeout(
//!     "user_login",
//!     Duration::from_secs(2),
//!     async { do_login().await }
//! ).await;
//! # }
//! # async fn do_login() {}
//! ```
//!
//! ## Modules
//!
//! - [`deadlock`] — `try_mutex_lock_with_timeout`,
//!   `try_rwlock_read_with_timeout` and `try_rwlock_write_with_timeout`.
//! - [`tasks`] — `TrackedTaskGroup` for leak detection.
//! - [`shutdown`] — `ShutdownProbe` for graceful-shutdown verification.
//! - [`cancellation_safety`] — `check_cancel_safe` for verifying that
//!   futures dropped mid-poll leave observable state consistent.
//! - `blocking` (feature `block-detect`) — heuristic blocking-call
//!   detection inside async tasks (visible in rustdoc when the
//!   feature is enabled).

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

/// Version of this crate as compiled, taken from its `Cargo.toml`.
///
/// Lets tools that bundle this crate, such as the `dev` CLI in
/// `dev-tools`, report the version that is actually linked.
///
/// # Example
///
/// ```
/// assert!(!dev_async::VERSION.is_empty());
/// ```
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::future::Future;
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Evidence, Producer, Report, Severity};

pub mod cancellation_safety;
pub mod deadlock;
pub mod shutdown;
pub mod tasks;

#[cfg(feature = "block-detect")]
#[cfg_attr(docsrs, doc(cfg(feature = "block-detect")))]
pub mod blocking;

/// Run a future with a hard timeout. Produces a [`CheckResult`] tagged
/// `async`.
///
/// If the future completes before the timeout, the verdict is `Pass`
/// and the duration is recorded.
///
/// If the future does not complete in time, the verdict is `Fail` with
/// severity `Error`. The future itself is dropped (cancelled) when the
/// timeout expires.
///
/// The returned `CheckResult` carries numeric `Evidence` for
/// `timeout_ms` and (on the pass path) `elapsed_ms`.
///
/// # Example
///
/// ```no_run
/// use dev_async::run_with_timeout;
/// use std::time::Duration;
///
/// # async fn ex() {
/// let check = run_with_timeout("op", Duration::from_millis(50), async {}).await;
/// assert!(check.has_tag("async"));
/// # }
/// ```
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
            let mut c = CheckResult::pass(format!("async::{name}"))
                .with_duration_ms(elapsed.as_millis() as u64);
            c.tags = vec!["async".to_string()];
            c.evidence = vec![
                Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
                Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
            ];
            c
        }
        Err(_elapsed) => {
            let mut c = CheckResult::fail(format!("async::{name}"), Severity::Error)
                .with_detail(format!("future did not complete within {timeout:?}"));
            c.tags = vec![
                "async".to_string(),
                "timeout".to_string(),
                "regression".to_string(),
            ];
            c.evidence = vec![Evidence::numeric("timeout_ms", timeout.as_millis() as f64)];
            c
        }
    }
}

/// Verify that all spawned tasks finish within the given timeout.
///
/// Pass a vector of `JoinHandle`s. Returns one [`CheckResult`] per task,
/// in the same order as `handles`, each tagged `async` with numeric
/// `Evidence` for the index and timeout / elapsed.
///
/// All tasks share one deadline, `timeout` after the call starts, and
/// are joined concurrently, so the call returns within about `timeout`
/// no matter how many tasks hang. `elapsed_ms` is the time from the
/// call until that task was seen to finish.
///
/// Verdicts:
/// - Task completed -> `Pass`, with `elapsed_ms` evidence.
/// - Task panicked or was cancelled -> `Fail (Critical)`, with
///   `task_panicked` tag.
/// - Task did not finish in time -> `Fail (Error)`, with `timeout` tag.
///   The task is aborted so it does not keep running after the check.
pub async fn join_all_with_timeout<T>(
    name: impl Into<String>,
    timeout: Duration,
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<CheckResult> {
    let name = name.into();
    let outcomes = join_with_deadline(handles, timeout).await;
    let mut results = Vec::with_capacity(outcomes.len());
    for (i, outcome) in outcomes.into_iter().enumerate() {
        let task_name = format!("async::{name}::task{i}");
        let evidence_base = vec![
            Evidence::numeric("task_index", i as f64),
            Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
        ];
        let result = match outcome {
            JoinOutcome::Finished(elapsed) => {
                let mut c =
                    CheckResult::pass(task_name).with_duration_ms(elapsed.as_millis() as u64);
                c.tags = vec!["async".to_string()];
                c.evidence = {
                    let mut e = evidence_base;
                    e.push(Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64));
                    e
                };
                c
            }
            JoinOutcome::Failed(join_err) => {
                let mut c = CheckResult::fail(task_name, Severity::Critical)
                    .with_detail(format!("task panicked or was cancelled: {join_err}"));
                c.tags = vec![
                    "async".to_string(),
                    "task_panicked".to_string(),
                    "regression".to_string(),
                ];
                c.evidence = evidence_base;
                c
            }
            JoinOutcome::TimedOut => {
                let mut c = CheckResult::fail(task_name, Severity::Error)
                    .with_detail(format!("task did not complete within {timeout:?}"));
                c.tags = vec![
                    "async".to_string(),
                    "timeout".to_string(),
                    "regression".to_string(),
                ];
                c.evidence = evidence_base;
                c
            }
        };
        results.push(result);
    }
    results
}

/// How one task ended when joined by [`join_with_deadline`].
pub(crate) enum JoinOutcome {
    /// Finished normally; time from the start of the join until it was
    /// seen to finish.
    Finished(Duration),
    /// Panicked or was cancelled.
    Failed(tokio::task::JoinError),
    /// Still running at the deadline. The task has been aborted.
    TimedOut,
}

/// Join every handle concurrently against one shared deadline,
/// `timeout` from now. Tasks still running at the deadline are aborted
/// so they do not outlive the check. Outcomes are returned in the order
/// of `handles`.
pub(crate) async fn join_with_deadline<T>(
    handles: Vec<tokio::task::JoinHandle<T>>,
    timeout: Duration,
) -> Vec<JoinOutcome> {
    let started = Instant::now();
    // `None` when `timeout` is too large to represent: wait without a deadline.
    let deadline = tokio::time::Instant::now().checked_add(timeout);
    let mut pending: Vec<Option<tokio::task::JoinHandle<T>>> =
        handles.into_iter().map(Some).collect();
    let mut outcomes: Vec<Option<JoinOutcome>> = pending.iter().map(|_| None).collect();
    {
        let all_done = std::future::poll_fn(|cx| {
            let mut still_running = false;
            for (slot, outcome) in pending.iter_mut().zip(outcomes.iter_mut()) {
                if let Some(handle) = slot {
                    match std::pin::Pin::new(handle).poll(cx) {
                        std::task::Poll::Ready(res) => {
                            *outcome = Some(match res {
                                Ok(_) => JoinOutcome::Finished(started.elapsed()),
                                Err(e) => JoinOutcome::Failed(e),
                            });
                            *slot = None;
                        }
                        std::task::Poll::Pending => still_running = true,
                    }
                }
            }
            if still_running {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        });
        match deadline {
            Some(at) => {
                let _ = tokio::time::timeout_at(at, all_done).await;
            }
            None => all_done.await,
        }
    }
    pending
        .into_iter()
        .zip(outcomes)
        .map(|(slot, outcome)| match outcome {
            Some(o) => o,
            None => {
                if let Some(handle) = slot {
                    handle.abort();
                }
                JoinOutcome::TimedOut
            }
        })
        .collect()
}

/// A trait for any async harness that produces a verdict via a future.
///
/// `dev-report::Producer` is synchronous, which doesn't fit async
/// harnesses. `AsyncCheck` is the async equivalent.
///
/// # Example
///
/// ```no_run
/// use dev_async::AsyncCheck;
/// use dev_report::CheckResult;
/// use std::future::Future;
/// use std::pin::Pin;
///
/// struct PingCheck;
/// impl AsyncCheck for PingCheck {
///     type Output = CheckResult;
///     type Fut = Pin<Box<dyn Future<Output = CheckResult> + Send>>;
///     fn run(self) -> Self::Fut {
///         Box::pin(async move { CheckResult::pass("ping") })
///     }
/// }
/// ```
pub trait AsyncCheck {
    /// Output of the check. Typically `CheckResult`.
    type Output;
    /// The future returned by `run`.
    type Fut: Future<Output = Self::Output>;
    /// Run the check.
    fn run(self) -> Self::Fut;
}

/// An async producer that builds a `Report`.
///
/// `dev-report::Producer` is synchronous. `AsyncProducer` is the
/// async equivalent that returns a future. Bridge to a sync
/// `Producer` via [`BlockingAsyncProducer`].
pub trait AsyncProducer {
    /// The future returned by `produce`.
    type Fut: Future<Output = Report>;
    /// Run the producer and return a finalized [`Report`].
    fn produce(self) -> Self::Fut;
}

/// Adapter that wraps an `async fn` returning a [`Report`] and
/// implements `dev_report::Producer` by blocking on it.
///
/// When the adapter owns its runtime (built with
/// [`with_new_runtime`](Self::with_new_runtime),
/// [`with_current_thread_runtime`](Self::with_current_thread_runtime) or
/// [`with_runtime_builder`](Self::with_runtime_builder)), `produce` drives
/// the future with that runtime's `block_on`, so timers and IO work on
/// both runtime flavors. When it borrows a handle via [`new`](Self::new),
/// `produce` calls `Handle::block_on`; see that constructor for the
/// `current_thread` caveat.
///
/// MUST be invoked from a sync context. Calling `produce` from inside
/// an async runtime panics (tokio refuses to block a runtime thread);
/// in async code, use [`AsyncProducer`] directly without going through
/// `Producer`.
///
/// # Example
///
/// ```no_run
/// use dev_async::{run_with_timeout, BlockingAsyncProducer};
/// use dev_report::{Producer, Report};
/// use std::time::Duration;
///
/// fn build_report() -> impl std::future::Future<Output = Report> {
///     async {
///         let check = run_with_timeout("op", Duration::from_millis(50), async {}).await;
///         let mut r = Report::new("crate", "0.1.0").with_producer("dev-async");
///         r.push(check);
///         r.finish();
///         r
///     }
/// }
///
/// // From a sync test or main:
/// // let rt = tokio::runtime::Runtime::new().unwrap();
/// // let handle = rt.handle().clone();
/// // let producer = BlockingAsyncProducer::new(handle, build_report);
/// // let report = producer.produce();
/// ```
pub struct BlockingAsyncProducer<F, Fut>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Report>,
{
    handle: tokio::runtime::Handle,
    /// Owned runtime, when constructed via `with_new_runtime` /
    /// `with_current_thread_runtime`. `None` when borrowing an
    /// externally-supplied handle. Kept alive for the lifetime of the
    /// producer so `block_on` always has a valid runtime.
    _owned_runtime: Option<tokio::runtime::Runtime>,
    factory: F,
}

impl<F, Fut> BlockingAsyncProducer<F, Fut>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Report>,
{
    /// Build a new adapter bound to an externally-supplied `handle`.
    ///
    /// `factory` is invoked once per `produce()` call and must return
    /// a fresh future each time.
    ///
    /// Use this when you already have a `tokio::runtime::Handle`
    /// (e.g. from a long-lived runtime in your test harness). For the
    /// common case of "I just want to drive an async producer from a
    /// sync test", prefer [`with_new_runtime`](Self::with_new_runtime).
    ///
    /// `produce` uses `Handle::block_on`. On a `current_thread` runtime
    /// that call cannot drive the timer or IO drivers, so a future that
    /// sleeps or times out (including [`run_with_timeout`]) never wakes
    /// unless another thread is inside that runtime's `Runtime::block_on`
    /// at the same time. Pass a handle to a multi-thread runtime, or use
    /// one of the owning constructors, which do not have this limit.
    pub fn new(handle: tokio::runtime::Handle, factory: F) -> Self {
        Self {
            handle,
            _owned_runtime: None,
            factory,
        }
    }

    /// Build a new adapter that owns a fresh multi-thread `tokio::runtime::Runtime`.
    ///
    /// The runtime lives for the lifetime of the producer and is
    /// dropped (along with all its workers) when the producer is
    /// dropped. Use this when you don't already have a runtime and
    /// just want to drive an async producer from sync code.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dev_async::{run_with_timeout, BlockingAsyncProducer};
    /// use dev_report::{Producer, Report};
    /// use std::time::Duration;
    ///
    /// let producer = BlockingAsyncProducer::with_new_runtime(|| async {
    ///     let check = run_with_timeout("op", Duration::from_millis(50), async {}).await;
    ///     let mut r = Report::new("crate", "0.1.0").with_producer("dev-async");
    ///     r.push(check);
    ///     r.finish();
    ///     r
    /// })
    /// .expect("build runtime");
    /// let _report = producer.produce();
    /// ```
    pub fn with_new_runtime(factory: F) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let handle = rt.handle().clone();
        Ok(Self {
            handle,
            _owned_runtime: Some(rt),
            factory,
        })
    }

    /// Build a new adapter that owns a fresh `current_thread`
    /// `tokio::runtime::Runtime`.
    ///
    /// Lighter-weight than [`with_new_runtime`](Self::with_new_runtime):
    /// no worker threads are spawned. Suitable for tests and
    /// single-threaded harnesses.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dev_async::BlockingAsyncProducer;
    /// use dev_report::{Producer, Report};
    ///
    /// let producer = BlockingAsyncProducer::with_current_thread_runtime(|| async {
    ///     Report::new("c", "0.1.0").with_producer("dev-async")
    /// })
    /// .expect("build runtime");
    /// let _r = producer.produce();
    /// ```
    pub fn with_current_thread_runtime(factory: F) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let handle = rt.handle().clone();
        Ok(Self {
            handle,
            _owned_runtime: Some(rt),
            factory,
        })
    }

    /// Build a new adapter with a runtime configured by the caller.
    ///
    /// `configure` receives a `tokio::runtime::Builder` that the
    /// caller can customize (worker thread count, thread name,
    /// stack size, IO/time enablement, etc.) before it is built.
    /// The resulting runtime is owned by the producer.
    ///
    /// The builder starts with timers and IO disabled, as tokio's
    /// builders do. Call `enable_all()` (or at least `enable_time()`)
    /// if the produced future uses [`run_with_timeout`] or any other
    /// timer; otherwise tokio panics when the timer is created.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dev_async::BlockingAsyncProducer;
    /// use dev_report::{Producer, Report};
    ///
    /// let producer = BlockingAsyncProducer::with_runtime_builder(
    ///     |b| {
    ///         b.worker_threads(2);
    ///         b.thread_name("my-async-test");
    ///         b.enable_all();
    ///         b
    ///     },
    ///     || async { Report::new("c", "0.1.0").with_producer("dev-async") },
    /// )
    /// .expect("build runtime");
    /// let _r = producer.produce();
    /// ```
    pub fn with_runtime_builder<C>(configure: C, factory: F) -> std::io::Result<Self>
    where
        C: FnOnce(&mut tokio::runtime::Builder) -> &mut tokio::runtime::Builder,
    {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        configure(&mut builder);
        let rt = builder.build()?;
        let handle = rt.handle().clone();
        Ok(Self {
            handle,
            _owned_runtime: Some(rt),
            factory,
        })
    }
}

impl<F, Fut> Producer for BlockingAsyncProducer<F, Fut>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Report>,
{
    fn produce(&self) -> Report {
        let fut = (self.factory)();
        // An owned runtime is driven with `Runtime::block_on`: unlike
        // `Handle::block_on`, it also drives the timer and IO drivers of
        // a `current_thread` runtime, so sleeps and timeouts complete.
        match &self._owned_runtime {
            Some(rt) => rt.block_on(fut),
            None => self.handle.block_on(fut),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;

    #[tokio::test]
    async fn timeout_pass_fast_future() {
        let check = run_with_timeout("fast", Duration::from_millis(500), async {}).await;
        assert_eq!(check.verdict, Verdict::Pass);
        assert!(check.has_tag("async"));
        let labels: Vec<&str> = check.evidence.iter().map(|e| e.label.as_str()).collect();
        assert!(labels.contains(&"elapsed_ms"));
        assert!(labels.contains(&"timeout_ms"));
    }

    #[tokio::test]
    async fn timeout_fail_slow_future() {
        let check = run_with_timeout("slow", Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        assert_eq!(check.verdict, Verdict::Fail);
        assert!(check.has_tag("timeout"));
        assert!(check.has_tag("regression"));
    }

    #[tokio::test]
    async fn join_all_basic() {
        let h1 = tokio::spawn(async { 1 });
        let h2 = tokio::spawn(async { 2 });
        let results = join_all_with_timeout("g", Duration::from_secs(1), vec![h1, h2]).await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.verdict == Verdict::Pass));
        assert!(results.iter().all(|r| r.has_tag("async")));
    }

    #[tokio::test]
    async fn join_all_panic_is_critical() {
        let h = tokio::spawn(async { panic!("oops") });
        let results = join_all_with_timeout("g", Duration::from_secs(1), vec![h]).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].verdict, Verdict::Fail);
        assert_eq!(results[0].severity, Some(Severity::Critical));
        assert!(results[0].has_tag("task_panicked"));
    }

    #[tokio::test]
    async fn join_all_timeout_is_error() {
        let h = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        });
        let results = join_all_with_timeout("g", Duration::from_millis(20), vec![h]).await;
        assert_eq!(results[0].verdict, Verdict::Fail);
        assert_eq!(results[0].severity, Some(Severity::Error));
        assert!(results[0].has_tag("timeout"));
    }

    #[tokio::test]
    async fn join_all_shares_one_deadline_across_hung_tasks() {
        // Ten hung tasks with a 200ms timeout used to take 10 x 200ms
        // because each handle got its own timeout in turn.
        let handles: Vec<_> = (0..10)
            .map(|_| {
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                })
            })
            .collect();
        let started = Instant::now();
        let results = join_all_with_timeout("g", Duration::from_millis(200), handles).await;
        let elapsed = started.elapsed();
        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.has_tag("timeout")));
        assert!(
            elapsed < Duration::from_secs(1),
            "join took {elapsed:?}, expected about one timeout"
        );
    }

    // Paused clock so the 40ms task always finishes before the 80ms
    // deadline fires, however loaded the machine is.
    #[tokio::test(start_paused = true)]
    async fn join_all_fails_task_that_finishes_after_the_shared_deadline() {
        // Task 0 finishes at ~40ms, task 1 at ~120ms. With a 80ms
        // timeout, task 1 is late even though it finishes within 80ms
        // of task 0.
        let h0 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(40)).await;
        });
        let h1 = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(400)).await;
        });
        let results = join_all_with_timeout("g", Duration::from_millis(80), vec![h0, h1]).await;
        assert_eq!(results[0].verdict, Verdict::Pass);
        assert_eq!(results[1].verdict, Verdict::Fail);
        assert!(results[1].has_tag("timeout"));
    }

    #[tokio::test]
    async fn join_all_aborts_timed_out_tasks() {
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = finished.clone();
        let h = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let results = join_all_with_timeout("g", Duration::from_millis(10), vec![h]).await;
        assert!(results[0].has_tag("timeout"));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !finished.load(std::sync::atomic::Ordering::SeqCst),
            "timed-out task kept running after the check"
        );
    }

    #[tokio::test]
    async fn join_all_reports_results_in_handle_order() {
        let slow = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
        });
        let panics = tokio::spawn(async { panic!("boom") });
        let fast = tokio::spawn(async {});
        let results =
            join_all_with_timeout("g", Duration::from_secs(2), vec![slow, panics, fast]).await;
        assert_eq!(results[0].name, "async::g::task0");
        assert_eq!(results[0].verdict, Verdict::Pass);
        assert!(results[1].has_tag("task_panicked"));
        assert_eq!(results[2].verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn join_all_with_huge_timeout_does_not_panic() {
        let h = tokio::spawn(async {});
        let results = join_all_with_timeout("g", Duration::MAX, vec![h]).await;
        assert_eq!(results[0].verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn join_all_empty_is_empty() {
        let results = join_all_with_timeout::<()>("g", Duration::from_millis(10), Vec::new()).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn check_evidence_includes_timeout() {
        let check = run_with_timeout("x", Duration::from_millis(50), async {}).await;
        let timeout_evidence = check
            .evidence
            .iter()
            .find(|e| e.label == "timeout_ms")
            .expect("timeout_ms evidence present");
        // The exact match isn't crucial; just ensure shape is numeric.
        if let dev_report::EvidenceData::Numeric(n) = timeout_evidence.data {
            assert_eq!(n, 50.0);
        } else {
            panic!("expected numeric");
        }
    }

    #[test]
    fn blocking_async_producer_with_new_runtime() {
        let producer = BlockingAsyncProducer::with_new_runtime(|| async {
            let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
            r.push(dev_report::CheckResult::pass("x"));
            r.finish();
            r
        })
        .expect("build runtime");
        let report = producer.produce();
        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.overall_verdict(), Verdict::Pass);
    }

    #[test]
    fn blocking_async_producer_with_current_thread_runtime() {
        let producer = BlockingAsyncProducer::with_current_thread_runtime(|| async {
            let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
            r.push(dev_report::CheckResult::pass("y"));
            r.finish();
            r
        })
        .expect("build runtime");
        let report = producer.produce();
        assert_eq!(report.checks.len(), 1);
    }

    #[test]
    fn blocking_async_producer_can_drive_run_with_timeout() {
        let producer = BlockingAsyncProducer::with_current_thread_runtime(|| async {
            let check = run_with_timeout("op", Duration::from_millis(50), async {}).await;
            let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
            r.push(check);
            r.finish();
            r
        })
        .expect("build runtime");
        let report = producer.produce();
        assert!(matches!(report.overall_verdict(), Verdict::Pass));
    }

    /// Run `f` on a helper thread and fail if it does not return in time,
    /// so a regression shows up as a test failure instead of a hang.
    fn finishes_within<R: Send + 'static>(
        limit: Duration,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> R {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(limit)
            .expect("producer did not finish; block_on is not driving timers")
    }

    #[test]
    fn current_thread_producer_drives_timers() {
        // `Handle::block_on` cannot drive a current_thread runtime's
        // timer, so this used to hang forever.
        let report = finishes_within(Duration::from_secs(10), || {
            let producer = BlockingAsyncProducer::with_current_thread_runtime(|| async {
                let check = run_with_timeout("sleepy", Duration::from_secs(5), async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                })
                .await;
                let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
                r.push(check);
                r.finish();
                r
            })
            .expect("build runtime");
            producer.produce()
        });
        assert_eq!(report.overall_verdict(), Verdict::Pass);
    }

    #[test]
    fn current_thread_producer_reports_timeout() {
        let report = finishes_within(Duration::from_secs(10), || {
            let producer = BlockingAsyncProducer::with_current_thread_runtime(|| async {
                let check = run_with_timeout("hung", Duration::from_millis(20), async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                })
                .await;
                let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
                r.push(check);
                r.finish();
                r
            })
            .expect("build runtime");
            producer.produce()
        });
        assert_eq!(report.overall_verdict(), Verdict::Fail);
    }

    #[test]
    fn blocking_async_producer_with_runtime_builder() {
        let producer = BlockingAsyncProducer::with_runtime_builder(
            |b| {
                b.worker_threads(1);
                b.enable_all();
                b
            },
            || async {
                let mut r = Report::new("c", "0.1.0").with_producer("dev-async");
                r.push(dev_report::CheckResult::pass("custom-rt"));
                r.finish();
                r
            },
        )
        .expect("build runtime");
        let report = producer.produce();
        assert_eq!(report.checks.len(), 1);
    }
}
