//! Timeout-based deadlock detection helpers.
//!
//! `try_lock_with_timeout` wraps an async lock acquisition with a hard
//! deadline. If the lock cannot be acquired in time, the result is
//! [`Verdict::Fail`] with a `deadlock_suspected` tag.
//!
//! ## What this catches
//!
//! - Real deadlock cycles (lock A waits for B, B waits for A).
//! - Single-lock starvation (one holder never releases).
//! - Long contention periods that look like deadlocks from the caller's
//!   perspective.
//!
//! ## What this does NOT catch
//!
//! Timeout-based detection cannot distinguish a true deadlock from a
//! slow-but-eventually-completing operation. For deterministic cycle
//! detection you'd need a lock-graph tracker, which is out of scope
//! for `0.x`.
//!
//! [`Verdict::Fail`]: dev_report::Verdict::Fail

use std::sync::Arc;
use std::time::{Duration, Instant};

use dev_report::{CheckResult, Evidence, Severity};
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Acquire a `tokio::sync::Mutex` lock or fail with a deadlock-suspected
/// verdict.
///
/// On success, returns `Ok((CheckResult::pass, MutexGuard))`. On
/// timeout, returns `Err(CheckResult::fail)` and the lock is left
/// alone.
///
/// # Example
///
/// ```no_run
/// use dev_async::deadlock::try_mutex_lock_with_timeout;
/// use std::sync::Arc;
/// use std::time::Duration;
/// use tokio::sync::Mutex;
///
/// # async fn ex() {
/// let m = Arc::new(Mutex::new(0));
/// match try_mutex_lock_with_timeout("counter", &m, Duration::from_millis(50)).await {
///     Ok((check, mut guard)) => {
///         *guard += 1;
///         drop(guard);
///         assert!(check.has_tag("async"));
///     }
///     Err(check) => {
///         assert!(check.has_tag("deadlock_suspected"));
///     }
/// };
/// # }
/// ```
pub async fn try_mutex_lock_with_timeout<'a, T>(
    name: impl Into<String>,
    lock: &'a Arc<Mutex<T>>,
    timeout: Duration,
) -> Result<(CheckResult, MutexGuard<'a, T>), CheckResult> {
    let name = name.into();
    let started = Instant::now();
    match tokio::time::timeout(timeout, lock.lock()).await {
        Ok(guard) => {
            let elapsed = started.elapsed();
            let mut c = CheckResult::pass(format!("async::lock::{name}"))
                .with_duration_ms(elapsed.as_millis() as u64);
            c.tags = vec!["async".to_string(), "lock".to_string()];
            c.evidence = vec![
                Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
                Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
            ];
            Ok((c, guard))
        }
        Err(_) => {
            let mut c = CheckResult::fail(format!("async::lock::{name}"), Severity::Error)
                .with_detail(format!("could not acquire lock within {timeout:?}"));
            c.tags = vec![
                "async".to_string(),
                "lock".to_string(),
                "deadlock_suspected".to_string(),
                "regression".to_string(),
            ];
            c.evidence = vec![Evidence::numeric("timeout_ms", timeout.as_millis() as f64)];
            Err(c)
        }
    }
}

/// Acquire a `tokio::sync::RwLock` read lock or fail.
pub async fn try_rwlock_read_with_timeout<'a, T>(
    name: impl Into<String>,
    lock: &'a Arc<RwLock<T>>,
    timeout: Duration,
) -> Result<(CheckResult, RwLockReadGuard<'a, T>), CheckResult> {
    let name = name.into();
    let started = Instant::now();
    match tokio::time::timeout(timeout, lock.read()).await {
        Ok(guard) => {
            let elapsed = started.elapsed();
            let mut c = CheckResult::pass(format!("async::lock::{name}::read"))
                .with_duration_ms(elapsed.as_millis() as u64);
            c.tags = vec!["async".to_string(), "lock".to_string()];
            c.evidence = vec![
                Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
                Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
            ];
            Ok((c, guard))
        }
        Err(_) => {
            let mut c = CheckResult::fail(format!("async::lock::{name}::read"), Severity::Error)
                .with_detail(format!("could not acquire read lock within {timeout:?}"));
            c.tags = vec![
                "async".to_string(),
                "lock".to_string(),
                "deadlock_suspected".to_string(),
                "regression".to_string(),
            ];
            c.evidence = vec![Evidence::numeric("timeout_ms", timeout.as_millis() as f64)];
            Err(c)
        }
    }
}

/// Acquire a `tokio::sync::RwLock` write lock or fail.
pub async fn try_rwlock_write_with_timeout<'a, T>(
    name: impl Into<String>,
    lock: &'a Arc<RwLock<T>>,
    timeout: Duration,
) -> Result<(CheckResult, RwLockWriteGuard<'a, T>), CheckResult> {
    let name = name.into();
    let started = Instant::now();
    match tokio::time::timeout(timeout, lock.write()).await {
        Ok(guard) => {
            let elapsed = started.elapsed();
            let mut c = CheckResult::pass(format!("async::lock::{name}::write"))
                .with_duration_ms(elapsed.as_millis() as u64);
            c.tags = vec!["async".to_string(), "lock".to_string()];
            c.evidence = vec![
                Evidence::numeric("elapsed_ms", elapsed.as_millis() as f64),
                Evidence::numeric("timeout_ms", timeout.as_millis() as f64),
            ];
            Ok((c, guard))
        }
        Err(_) => {
            let mut c = CheckResult::fail(format!("async::lock::{name}::write"), Severity::Error)
                .with_detail(format!("could not acquire write lock within {timeout:?}"));
            c.tags = vec![
                "async".to_string(),
                "lock".to_string(),
                "deadlock_suspected".to_string(),
                "regression".to_string(),
            ];
            c.evidence = vec![Evidence::numeric("timeout_ms", timeout.as_millis() as f64)];
            Err(c)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dev_report::Verdict;

    #[tokio::test]
    async fn mutex_lock_acquires_under_timeout() {
        let m = Arc::new(Mutex::new(0));
        let (check, _g) = try_mutex_lock_with_timeout("a", &m, Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(check.verdict, Verdict::Pass);
        assert!(check.has_tag("lock"));
    }

    #[tokio::test]
    async fn mutex_lock_times_out_when_held() {
        let m = Arc::new(Mutex::new(0));
        let _held = m.lock().await;
        let err = try_mutex_lock_with_timeout("a", &m, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.verdict, Verdict::Fail);
        assert!(err.has_tag("deadlock_suspected"));
        assert!(err.has_tag("regression"));
    }

    #[tokio::test]
    async fn rwlock_read_under_timeout() {
        let l = Arc::new(RwLock::new(0));
        let (check, _g) = try_rwlock_read_with_timeout("a", &l, Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(check.verdict, Verdict::Pass);
    }

    #[tokio::test]
    async fn rwlock_write_times_out_when_held() {
        let l = Arc::new(RwLock::new(0));
        let _held = l.write().await;
        let err = try_rwlock_write_with_timeout("a", &l, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err.verdict, Verdict::Fail);
    }
}
