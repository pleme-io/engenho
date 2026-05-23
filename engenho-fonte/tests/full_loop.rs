//! End-to-end: a `(defsistema …)` JSON-shape change flows through
//! the conduit, fans out via the SystemController to four mock
//! sub-reconcilers, attests the transition, and publishes the
//! outcome. Proves the destination loop wired-and-working end-to-end
//! with mocks.

use engenho_fonte::{
    Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockPublisher, MockWatcher, Sistema,
    mock_system_controller,
};
use std::sync::Arc;

#[tokio::test]
async fn defsistema_change_propagates_to_every_sub_reconciler() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (apps, infra, promises, topology, ctrl) = mock_system_controller();
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

    // Sistema declaring 3 apps, 2 infra units, 2 promises, 1 topology.
    let sistema_json = r#"{
        "name": "rio-cluster",
        "apps": [
            {"name": "podinfo", "version": "6.4.1"},
            {"name": "lilitu",  "version": null},
            {"name": "tend",    "version": null}
        ],
        "infra": [
            {"name": "rio-network", "backend": "magma"},
            {"name": "rio-dns",     "backend": "pangea"}
        ],
        "promises": [
            {"name": "sla",  "kind": "availability", "target": 99.99},
            {"name": "cost", "kind": "budget",       "target": 5000.0}
        ],
        "topology": {"strategy": "quorum_3m", "nodes": 3}
    }"#;

    watcher
        .push(Change {
            source: "sistemas/rio.lisp".into(),
            kind: ChangeKind::Initial,
            source_text: sistema_json.into(),
            revision: 1,
        })
        .await;

    let outcome = conduit.tick().await.unwrap().expect("expected outcome");
    assert_eq!(outcome.revision, 1);
    assert_eq!(outcome.proposal_id, 0);

    // Every sub-reconciler was invoked with the right cardinality.
    assert_eq!(apps.log().len(), 3, "3 apps must reconcile");
    assert_eq!(infra.log().len(), 2, "2 infra units must reconcile");
    assert_eq!(promises.log().len(), 2, "2 promises must reconcile");
    assert_eq!(topology.log().len(), 1, "1 topology must reconcile");

    // The published outcome is chained: 1 attestation entry, 1 publish.
    assert_eq!(attester.chain().len(), 1);
    assert_eq!(publisher.outcomes().len(), 1);

    // The controller's last_applied snapshot equals the declared sistema.
    let snap = proposer.last_applied().expect("controller has snapshot");
    assert_eq!(&*snap.name, "rio-cluster");
    assert_eq!(snap.apps.len(), 3);
}

#[tokio::test]
async fn two_changes_fan_out_independently() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (apps, _infra, _promises, _topology, ctrl) = mock_system_controller();
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

    let small = r#"{
        "name": "a", "apps": [{"name": "x", "version": null}],
        "infra": [], "promises": [],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;
    let large = r#"{
        "name": "a", "apps": [
            {"name": "x", "version": null},
            {"name": "y", "version": null}
        ],
        "infra": [], "promises": [],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;

    watcher
        .push(Change {
            source: "x".into(),
            kind: ChangeKind::Initial,
            source_text: small.into(),
            revision: 1,
        })
        .await;
    watcher
        .push(Change {
            source: "x".into(),
            kind: ChangeKind::Modified,
            source_text: large.into(),
            revision: 2,
        })
        .await;

    let outcomes = conduit.drain().await.unwrap();
    assert_eq!(outcomes.len(), 2);
    // 1 app in change 1 + 2 apps in change 2 = 3 reconciles total.
    assert_eq!(apps.log().len(), 3);
}

#[tokio::test]
async fn malformed_sistema_surfaces_typed_eval_error() {
    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_a, _i, _p, _t, ctrl) = mock_system_controller();
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer,
        attester.clone(),
        publisher.clone(),
    );

    // Missing required `apps` field.
    watcher
        .push(Change {
            source: "broken".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name": "broken"}"#.into(),
            revision: 1,
        })
        .await;

    let err = conduit.tick().await.unwrap_err();
    let msg = format!("{err}");
    // SystemController returns a TypescapeError(MissingAttr) which the
    // Proposer trait's signature propagates as FonteError::Propose.
    // Either category is acceptable; the load-bearing assertion is:
    // no attestation chained, no publish.
    assert!(
        msg.contains("fonte/eval") || msg.contains("fonte/propose"),
        "got: {msg}"
    );
    assert_eq!(attester.chain().len(), 0);
    assert_eq!(publisher.outcomes().len(), 0);
}

/// Sanity: the Sistema type can be constructed in Rust directly +
/// the round-trip via the controller works without any tlisp/JSON
/// step (proves the Rust API is usable for tests + future direct
/// callers).
#[tokio::test]
async fn direct_sistema_construction_round_trips_through_controller() {
    use engenho_fonte::{AppRef, InfraBackend, InfraRef, PromessaKind, PromessaRef, TopologyRef};
    use engenho_sui_typescape::Typescape;

    let s = Sistema {
        name: "direct".into(),
        apps: vec![AppRef {
            name: "podinfo".into(),
            version: None,
        }],
        infra: vec![InfraRef {
            name: "net".into(),
            backend: InfraBackend::Magma,
        }],
        promises: vec![PromessaRef {
            name: "uptime".into(),
            kind: PromessaKind::Availability,
            target: 99.5,
        }],
        topology: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
    };
    let typed = s.to_typescape_value();
    let back = Sistema::from_typescape_value(&typed).unwrap();
    assert_eq!(back, s);
}
