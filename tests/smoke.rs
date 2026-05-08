use dev_async::{join_all_with_timeout, run_with_timeout};
use std::time::Duration;

#[tokio::test]
async fn smoke_pass_fast_future() {
    let c = run_with_timeout("fast", Duration::from_millis(500), async {}).await;
    assert!(matches!(c.verdict, dev_report::Verdict::Pass));
}

#[tokio::test]
async fn smoke_fail_slow_future() {
    let c = run_with_timeout("slow", Duration::from_millis(10), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    assert!(matches!(c.verdict, dev_report::Verdict::Fail));
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
