use dev_async::{
    deadlock::try_mutex_lock_with_timeout,
    join_all_with_timeout, run_with_timeout,
    shutdown::{ShutdownComponent, ShutdownProbe},
    tasks::TrackedTaskGroup,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn smoke_pass_fast_future() {
    let c = run_with_timeout("fast", Duration::from_millis(500), async {}).await;
    assert!(matches!(c.verdict, dev_report::Verdict::Pass));
    assert!(c.has_tag("async"));
}

#[tokio::test]
async fn smoke_fail_slow_future() {
    let c = run_with_timeout("slow", Duration::from_millis(10), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    assert!(matches!(c.verdict, dev_report::Verdict::Fail));
    assert!(c.has_tag("timeout"));
    assert!(c.has_tag("regression"));
}

#[tokio::test]
async fn smoke_join_all() {
    let h1 = tokio::spawn(async { 1 });
    let h2 = tokio::spawn(async { 2 });
    let results = join_all_with_timeout("g", Duration::from_secs(1), vec![h1, h2]).await;
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|r| matches!(r.verdict, dev_report::Verdict::Pass)));
}

#[tokio::test]
async fn smoke_check_carries_numeric_evidence() {
    let c = run_with_timeout("op", Duration::from_millis(50), async {}).await;
    let labels: Vec<&str> = c.evidence.iter().map(|e| e.label.as_str()).collect();
    assert!(labels.contains(&"timeout_ms"));
    assert!(labels.contains(&"elapsed_ms"));
}

#[tokio::test]
async fn smoke_mutex_lock_under_timeout_passes() {
    let m = Arc::new(tokio::sync::Mutex::new(0));
    let r = try_mutex_lock_with_timeout("a", &m, Duration::from_millis(50)).await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn smoke_mutex_lock_times_out_when_held() {
    let m = Arc::new(tokio::sync::Mutex::new(0));
    let _held = m.lock().await;
    let err = try_mutex_lock_with_timeout("a", &m, Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(err.has_tag("deadlock_suspected"));
}

#[tokio::test]
async fn smoke_tracked_task_group_clean() {
    let mut g = TrackedTaskGroup::new("clean");
    g.spawn(async {});
    g.spawn(async {});
    let c = g.finalize(Duration::from_millis(50)).await;
    assert!(matches!(c.verdict, dev_report::Verdict::Pass));
}

#[tokio::test]
async fn smoke_tracked_task_group_leak() {
    let mut g = TrackedTaskGroup::new("leaky");
    g.spawn(async {
        tokio::time::sleep(Duration::from_millis(500)).await;
    });
    let c = g.finalize(Duration::from_millis(20)).await;
    assert!(matches!(c.verdict, dev_report::Verdict::Fail));
    assert!(c.has_tag("task_leak"));
}

#[tokio::test]
async fn smoke_shutdown_probe_drained() {
    let probe = ShutdownProbe::new("sys")
        .deadline(Duration::from_millis(50))
        .poll_interval(Duration::from_millis(5))
        .with_component(ShutdownComponent::new("a", || async { true }));
    let results = probe.run().await;
    assert_eq!(results.len(), 2); // 1 component + aggregate
    assert!(matches!(
        results.last().unwrap().verdict,
        dev_report::Verdict::Pass
    ));
}
