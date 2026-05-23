//! End-to-end tests: drive the [`Conduit`] through changes,
//! assert each downstream stage observes them in order with chained
//! provenance.

use engenho_fonte::{
    Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockProposer, MockPublisher,
    MockWatcher,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;

fn build_conduit() -> (
    Arc<MockWatcher>,
    Arc<MockProposer>,
    Arc<MockAttester>,
    Arc<MockPublisher>,
    Conduit,
) {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let proposer = Arc::new(MockProposer::new());
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator.clone(),
        proposer.clone(),
        attester.clone(),
        publisher.clone(),
    );
    (watcher, proposer, attester, publisher, conduit)
}

#[tokio::test]
async fn single_change_flows_end_to_end() {
    let (watcher, _proposer, attester, publisher, conduit) = build_conduit();
    watcher
        .push(Change {
            source: "sistema/rio.lisp".into(),
            kind: ChangeKind::Initial,
            source_text: "{\"name\": \"rio\", \"replicas\": 3}".into(),
            revision: 1,
        })
        .await;

    let outcome = conduit.tick().await.unwrap().expect("expected outcome");
    assert_eq!(outcome.revision, 1);
    assert_eq!(outcome.proposal_id, 0);
    assert_eq!(publisher.outcomes().len(), 1);
    assert_eq!(attester.chain().len(), 1);
}

#[tokio::test]
async fn empty_watcher_returns_none_no_side_effects() {
    let (_watcher, _proposer, attester, publisher, conduit) = build_conduit();
    let out = conduit.tick().await.unwrap();
    assert!(out.is_none());
    assert_eq!(attester.chain().len(), 0);
    assert_eq!(publisher.outcomes().len(), 0);
}

#[tokio::test]
async fn out_of_order_changes_are_dropped() {
    let (watcher, _proposer, attester, publisher, conduit) = build_conduit();
    for rev in [1u64, 3, 2, 4] {
        watcher
            .push(Change {
                source: "x".into(),
                kind: ChangeKind::Modified,
                source_text: "null".into(),
                revision: rev,
            })
            .await;
    }
    let outcomes = conduit.drain().await.unwrap();
    // Stale (rev=2) should be dropped because we already saw rev=3.
    let revs: Vec<u64> = outcomes.iter().map(|o| o.revision).collect();
    assert_eq!(revs, vec![1, 3, 4]);
    assert_eq!(attester.chain().len(), 3);
    assert_eq!(publisher.outcomes().len(), 3);
}

#[tokio::test]
async fn malformed_source_surfaces_typed_eval_error() {
    let (watcher, _proposer, _attester, _publisher, conduit) = build_conduit();
    watcher
        .push(Change {
            source: "broken".into(),
            kind: ChangeKind::Initial,
            source_text: "this is not json {".into(),
            revision: 1,
        })
        .await;
    let err = conduit.tick().await.unwrap_err();
    // The evaluator's typed error must propagate as FonteError::Eval(...).
    let msg = format!("{err}");
    assert!(msg.contains("fonte/eval"), "got: {msg}");
}

proptest_with_env! {
    /// N changes in monotone order all produce exactly N outcomes
    /// with monotone proposal ids 0..N.
    #[test]
    fn n_changes_produce_n_outcomes_monotone(n in 1usize..32) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let outcomes = rt.block_on(async {
            let (watcher, _p, _a, publisher, conduit) = build_conduit();
            for i in 0..n {
                watcher.push(Change {
                    source: "x".into(),
                    kind: ChangeKind::Modified,
                    source_text: format!("{{\"i\": {i}}}").into(),
                    revision: (i + 1) as u64,
                }).await;
            }
            let out = conduit.drain().await.unwrap();
            (out, publisher.outcomes())
        });
        prop_assert_eq!(outcomes.0.len(), n);
        prop_assert_eq!(outcomes.1.len(), n);
        for (i, o) in outcomes.0.iter().enumerate() {
            prop_assert_eq!(o.proposal_id, i as u64);
            prop_assert_eq!(o.revision, (i + 1) as u64);
        }
    }

    /// The attester chain length equals the published outcome count
    /// for every drain — no orphaned receipts, no orphaned publishes.
    #[test]
    fn attest_chain_aligns_with_published_outcomes(n in 1usize..16) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (chain_len, pub_len) = rt.block_on(async {
            let (watcher, _p, attester, publisher, conduit) = build_conduit();
            for i in 0..n {
                watcher.push(Change {
                    source: "x".into(),
                    kind: ChangeKind::Initial,
                    source_text: format!("{{\"i\": {i}}}").into(),
                    revision: (i + 1) as u64,
                }).await;
            }
            conduit.drain().await.unwrap();
            (attester.chain().len(), publisher.outcomes().len())
        });
        prop_assert_eq!(chain_len, n);
        prop_assert_eq!(pub_len, n);
    }
}
