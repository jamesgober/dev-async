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
//! - [`deadlock`] — `try_lock_with_timeout` helpers.
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
/// each tagged `async` with numeric `Evidence` for the index and
/// timeout / elapsed.
///
/// Verdicts:
/// - Task completed -> `Pass`, with `elapsed_ms` evidence.
/// - Task panicked or was cancelled -> `Fail (Critical)`, with
///   `task_panicked` tag.
/// - Task did not finish in time -> `Fail (Error)`, with `timeout` tag.
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
        let evidence_base = vec![
            Evidence::numeric("task_index", i as f64),
            Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
        ];
        let result = match tokio::time::timeout(timeout, h).await {
            Ok(Ok(_)) => {
                let elapsed = started.elapsed();
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
            Ok(Err(join_err)) => {
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
            Err(_) => {
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
/// implements `dev_report::Producer` by calling
/// `tokio::runtime::Handle::current().block_on(...)`.
///
/// MUST be invoked from a sync context that *is not* itself running
/// inside a `current_thread` runtime. Calling `block_on` from inside
/// an async runtime would deadlock; if you need that, use
/// [`AsyncProducer`] directly without going through `Producer`.
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
        self.handle.block_on(fut)
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
