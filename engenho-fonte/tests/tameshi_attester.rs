//! Tests for the TameshiAttester — proves Conduit attestations land
//! in tameshi's HeartbeatChain with cryptographic linkage.

#![cfg(feature = "with-tameshi")]

use engenho_fonte::{Attester, Change, ChangeKind, Decision, TameshiAttester};
use engenho_sui_typescape::TypescapeValue;

fn sample_decision(revision: u64) -> Decision {
    Decision {
        change: Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: "{}".into(),
            revision,
        },
        typed: TypescapeValue::attrs([("name", TypescapeValue::string("rio"))]),
    }
}

#[tokio::test]
async fn attest_appends_one_entry() {
    let a = TameshiAttester::new("test-instance", "0.1.0");
    let r = a.attest(&sample_decision(1), 0).await.unwrap();
    assert_eq!(r.proposal_id, 0);
    assert!(!r.id.is_empty());
    let chain = a.chain();
    let entries = chain.entries();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn n_attests_produce_chained_n_entries() {
    let a = TameshiAttester::new("inst", "0.1.0");
    for i in 1..=5u64 {
        a.attest(&sample_decision(i), i - 1).await.unwrap();
    }
    let entries = a.chain().entries();
    assert_eq!(entries.len(), 5);
    // Chain integrity: each entry's previous_hash equals the
    // prior entry's entry_hash (BLAKE3 linkage).
    for w in entries.windows(2) {
        assert_eq!(
            w[1].previous_hash, w[0].entry_hash,
            "BLAKE3 chain must be unbroken"
        );
    }
}

#[tokio::test]
async fn attester_plugs_into_conduit_end_to_end() {
    use engenho_fonte::{
        Conduit, MockEvaluator, MockPublisher, MockWatcher, mock_system_controller,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let attester = Arc::new(TameshiAttester::new("e2e", "0.1.0"));
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        Arc::new(ctrl),
        attester.clone(),
        publisher,
    );

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text:
                r#"{"name":"rio","apps":[],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#
                    .into(),
            revision: 1,
        })
        .await;

    conduit.tick().await.unwrap();
    assert_eq!(attester.chain().entries().len(), 1);
}
