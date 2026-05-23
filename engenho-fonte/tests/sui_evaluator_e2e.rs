//! Real SuiEvaluator integration: feed a Nix-shaped Sistema
//! declaration to the Conduit, assert it lands as a typed Sistema
//! through the four sub-reconcilers.

#![cfg(feature = "with-sui-eval")]

use engenho_fonte::{
    Change, ChangeKind, Conduit, MockAttester, MockPublisher, MockWatcher, Sistema, SuiEvaluator,
    mock_system_controller,
};
use std::sync::Arc;

#[tokio::test]
async fn nix_sistema_declaration_flows_through_conduit() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(SuiEvaluator::new());
    let (apps, infra, _promises, topology, ctrl) = mock_system_controller();
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer.clone(),
        attester.clone(),
        publisher.clone(),
    );

    // Nix-shape Sistema (not JSON). Sui's evaluator handles this.
    let nix_sistema = r#"{
        name = "rio";
        apps = [
            { name = "podinfo"; version = null; }
            { name = "lilitu";  version = "1.0"; }
        ];
        infra = [
            { name = "rio-net"; backend = "magma"; }
        ];
        promises = [];
        topology = { strategy = "quorum_3m"; nodes = 3; };
    }"#;

    watcher
        .push(Change {
            source: "rio.nix".into(),
            kind: ChangeKind::Initial,
            source_text: nix_sistema.into(),
            revision: 1,
        })
        .await;

    let outcome = conduit.tick().await.expect("tick").expect("outcome");
    assert_eq!(outcome.revision, 1);
    assert_eq!(apps.log().len(), 2);
    assert_eq!(infra.log().len(), 1);
    assert_eq!(topology.log().len(), 1);

    let snap: Sistema = proposer.last_applied().expect("snapshot");
    assert_eq!(&*snap.name, "rio");
    assert_eq!(snap.apps.len(), 2);
    assert_eq!(&*snap.topology.strategy, "quorum_3m");
}

#[tokio::test]
async fn malformed_nix_surfaces_typed_eval_error() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(SuiEvaluator::new());
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
            source: "broken.nix".into(),
            kind: ChangeKind::Initial,
            source_text: "this is not a Nix expression {{".into(),
            revision: 1,
        })
        .await;

    let err = conduit.tick().await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("fonte/eval"), "got: {msg}");
}
