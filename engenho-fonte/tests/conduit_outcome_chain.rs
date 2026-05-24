//! Tests for Conduit's optional OutcomeChainRecorder integration.

use engenho_fonte::{
    Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockOutcomeChain, MockPublisher,
    MockWatcher, mock_system_controller,
};
use std::sync::Arc;

fn build_conduit_with_chain() -> (Arc<MockWatcher>, Arc<MockOutcomeChain>, Conduit) {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let chain = Arc::new(MockOutcomeChain::new());
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    )
    .with_outcome_chain(chain.clone());
    (watcher, chain, conduit)
}

#[tokio::test]
async fn no_chain_attached_no_recording() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;
    let outcome = conduit.tick().await.unwrap().expect("outcome");
    assert_eq!(outcome.revision, 1);
    // No chain attached → no recording. The test's purpose is the
    // compile-time + no-side-effect contract.
}

#[tokio::test]
async fn outcome_chain_records_each_tick() {
    let (watcher, chain, conduit) = build_conduit_with_chain();
    for i in 1..=3u64 {
        watcher
            .push(Change {
                source: "x".into(),
                kind: ChangeKind::Initial,
                source_text: r#"{"name":"x","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
                revision: i,
            })
            .await;
    }
    let outcomes = conduit.drain().await.unwrap();
    assert_eq!(outcomes.len(), 3);
    // Chain has the SAME outcomes the Publisher saw.
    let chained = chain.outcomes();
    assert_eq!(chained.len(), 3);
    for (i, c) in chained.iter().enumerate() {
        assert_eq!(c.revision, (i + 1) as u64);
    }
}

#[tokio::test]
async fn chain_does_not_record_failed_ticks() {
    // Use an evaluator that always fails — chain shouldn't record
    // outcomes for ticks that never reached publish.
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let chain = Arc::new(MockOutcomeChain::new());
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    )
    .with_outcome_chain(chain.clone());

    // Push malformed JSON — evaluator will reject.
    watcher
        .push(Change {
            source: "broken".into(),
            kind: ChangeKind::Initial,
            source_text: "not json {".into(),
            revision: 1,
        })
        .await;
    let result = conduit.tick().await;
    assert!(result.is_err());
    // No outcome reached the chain.
    assert!(chain.outcomes().is_empty());
}

#[tokio::test]
#[cfg(feature = "with-tameshi")]
async fn real_tameshi_outcome_chain_records_per_tick() {
    use engenho_fonte::TameshiOutcomeChain;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let chain: Arc<dyn engenho_fonte::OutcomeChainRecorder> =
        Arc::new(TameshiOutcomeChain::new("e2e", "0.1.0"));

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    )
    .with_outcome_chain(chain);

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;
    let outcome = conduit.tick().await.unwrap().expect("outcome");
    assert_eq!(outcome.revision, 1);
    // The TameshiOutcomeChain's own chain() accessor is on the
    // concrete type; this test confirms the dyn-dispatch boundary
    // works.
}
