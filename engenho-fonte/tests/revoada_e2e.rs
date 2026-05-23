//! Integration test for the `with-revoada` feature — pipes a
//! Sistema-shape change through the Conduit using a real
//! [`PureRaftFace`](engenho_revoada::PureRaftFace) as the Proposer's
//! backend.

#![cfg(feature = "with-revoada")]

use engenho_fonte::{
    Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockPublisher, MockWatcher,
    RevoadaProposer,
};
use engenho_revoada::face::Face;
use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
use std::sync::Arc;

#[tokio::test]
async fn sistema_change_lands_in_revoada_face() {
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "fonte".into(),
        kind: FaceKind::PureRaft,
    })
    .expect("face construction");
    face.start().expect("face start");

    let face_arc: Arc<dyn Face> = Arc::new(face);
    let proposer = Arc::new(RevoadaProposer::new(face_arc.clone()));

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer.clone(),
        attester.clone(),
        publisher.clone(),
    );

    // A minimal Sistema-shape JSON change.
    let json = r#"{
        "name": "rio",
        "apps": [{"name": "podinfo", "version": null}],
        "infra": [],
        "promises": [],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;
    watcher
        .push(Change {
            source: "sistemas/rio".into(),
            kind: ChangeKind::Initial,
            source_text: json.into(),
            revision: 1,
        })
        .await;

    let outcome = conduit
        .tick()
        .await
        .expect("conduit tick")
        .expect("outcome");
    assert_eq!(outcome.proposal_id, 0);
    assert_eq!(attester.chain().len(), 1);
    assert_eq!(publisher.outcomes().len(), 1);

    // Face received the resource — count is now 1.
    assert_eq!(
        face_arc.resource_count(),
        1,
        "revoada face should hold one resource after fonte proposed it"
    );

    face_arc.shutdown().expect("face shutdown");
}

#[tokio::test]
async fn n_changes_become_n_resources_in_revoada() {
    let face = PureRaftFace::from_declaration(&FabricFace {
        name: "fonte-bulk".into(),
        kind: FaceKind::PureRaft,
    })
    .unwrap();
    face.start().unwrap();
    let face_arc: Arc<dyn Face> = Arc::new(face);
    let proposer = Arc::new(RevoadaProposer::new(face_arc.clone()));

    let watcher = Arc::new(MockWatcher::new());
    let conduit = Conduit::new(
        watcher.clone(),
        Arc::new(MockEvaluator::new()),
        proposer,
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    for i in 1..=5_u64 {
        watcher
            .push(Change {
                source: format!("s/{i}").into(),
                kind: ChangeKind::Initial,
                source_text: format!(
                    r#"{{"name": "s{i}", "apps": [], "infra": [], "promises": [], "topology": {{"strategy": "solo", "nodes": 1}}}}"#
                )
                .into(),
                revision: i,
            })
            .await;
    }
    let outcomes = conduit.drain().await.unwrap();
    assert_eq!(outcomes.len(), 5);
    assert_eq!(face_arc.resource_count(), 5);

    face_arc.shutdown().unwrap();
}
