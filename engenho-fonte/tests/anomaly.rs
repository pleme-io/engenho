//! Tests for the typed AnomalyChain — drift detection + the
//! BLAKE3-linked log.

use engenho_fonte::{
    AnomalyChain, AnomalyEvent, AppRef, InfraBackend, InfraRef, MockAnomalyChain, PromessaKind,
    PromessaRef, Sistema, TopologyRef,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

fn solo(name: &str) -> Sistema {
    Sistema {
        name: name.into(),
        apps: Vec::new(),
        infra: Vec::new(),
        promises: Vec::new(),
        topology: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
    }
}

#[test]
fn identical_sistemas_diff_to_empty() {
    let s = solo("a");
    assert!(AnomalyEvent::diff(&s, &s).is_empty());
}

#[test]
fn added_app_surfaces_typed_event() {
    let prev = solo("a");
    let mut next = prev.clone();
    next.apps.push(AppRef {
        name: "podinfo".into(),
        version: None,
    });
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 1);
    matches!(&d[0], AnomalyEvent::AppAdded(_));
}

#[test]
fn removed_app_surfaces_typed_event() {
    let mut prev = solo("a");
    prev.apps.push(AppRef {
        name: "podinfo".into(),
        version: None,
    });
    let next = solo("a");
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 1);
    matches!(&d[0], AnomalyEvent::AppRemoved(_));
}

#[test]
fn version_change_surfaces_typed_event() {
    let mut prev = solo("a");
    prev.apps.push(AppRef {
        name: "podinfo".into(),
        version: Some("1.0".into()),
    });
    let mut next = solo("a");
    next.apps.push(AppRef {
        name: "podinfo".into(),
        version: Some("2.0".into()),
    });
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 1);
    match &d[0] {
        AnomalyEvent::AppVersionChanged { from, to, .. } => {
            assert_eq!(from.as_ref().unwrap().as_ref(), "1.0");
            assert_eq!(to.as_ref().unwrap().as_ref(), "2.0");
        }
        other => panic!("expected AppVersionChanged, got {other:?}"),
    }
}

#[test]
fn topology_shift_surfaces_typed_event() {
    let prev = solo("a");
    let mut next = prev.clone();
    next.topology.nodes = 3;
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 1);
    matches!(&d[0], AnomalyEvent::TopologyChanged { .. });
}

#[test]
fn infra_add_remove_surfaces_typed_events() {
    let prev = solo("a");
    let mut next = prev.clone();
    next.infra.push(InfraRef {
        name: "net".into(),
        backend: InfraBackend::Magma,
    });
    next.infra.push(InfraRef {
        name: "dns".into(),
        backend: InfraBackend::Pangea,
    });
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 2);
    assert!(d.iter().all(|e| matches!(e, AnomalyEvent::InfraAdded(_))));
}

#[test]
fn promessa_target_shift_surfaces_typed_event() {
    let mut prev = solo("a");
    prev.promises.push(PromessaRef {
        name: "sla".into(),
        kind: PromessaKind::Availability,
        target: 99.0,
    });
    let mut next = solo("a");
    next.promises.push(PromessaRef {
        name: "sla".into(),
        kind: PromessaKind::Availability,
        target: 99.99,
    });
    let d = AnomalyEvent::diff(&prev, &next);
    assert_eq!(d.len(), 1);
    match &d[0] {
        AnomalyEvent::PromessaTargetChanged { from, to, .. } => {
            assert!((from - 99.0).abs() < f64::EPSILON);
            assert!((to - 99.99).abs() < f64::EPSILON);
        }
        other => panic!("expected PromessaTargetChanged, got {other:?}"),
    }
}

#[tokio::test]
async fn chain_records_each_event_with_link() {
    let chain = MockAnomalyChain::new();
    chain
        .record(
            1,
            vec![
                AnomalyEvent::AppAdded(AppRef {
                    name: "a".into(),
                    version: None,
                }),
                AnomalyEvent::AppAdded(AppRef {
                    name: "b".into(),
                    version: None,
                }),
            ],
        )
        .await
        .unwrap();
    chain
        .record(
            2,
            vec![AnomalyEvent::AppRemoved(AppRef {
                name: "a".into(),
                version: None,
            })],
        )
        .await
        .unwrap();
    let entries = chain.entries();
    assert_eq!(entries.len(), 3);
    assert!(chain.validate_chain(), "BLAKE3 chain must be unbroken");
    assert_eq!(entries[0].revision, 1);
    assert_eq!(entries[1].revision, 1);
    assert_eq!(entries[2].revision, 2);
}

#[tokio::test]
async fn chain_empty_record_returns_none() {
    let chain = MockAnomalyChain::new();
    let id = chain.record(1, vec![]).await.unwrap();
    assert!(id.is_none());
    assert!(chain.entries().is_empty());
}

// ── Integration test: SystemController + AnomalyChain ────────────

#[tokio::test]
async fn system_controller_records_drift_across_two_reconciles() {
    use engenho_fonte::{
        Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockPublisher, MockWatcher,
        mock_system_controller,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let (_apps, _infra, _promises, _topology, ctrl) = mock_system_controller();
    let chain = Arc::new(MockAnomalyChain::new());
    let ctrl = ctrl.with_anomaly_chain(chain.clone());
    let proposer = Arc::new(ctrl);
    let attester = Arc::new(MockAttester::new());
    let publisher = Arc::new(MockPublisher::new());

    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer.clone(),
        attester,
        publisher,
    );

    // First reconcile: 1 app.
    let first = r#"{
        "name": "a",
        "apps": [{"name": "x", "version": null}],
        "infra": [], "promises": [],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;
    watcher
        .push(Change {
            source: "s".into(),
            kind: ChangeKind::Initial,
            source_text: first.into(),
            revision: 1,
        })
        .await;
    conduit.tick().await.unwrap();
    // First reconcile: diff vs synthetic empty Sistema (topology
    // nodes=0) → 1 AppAdded + 1 TopologyChanged = 2 events. The
    // synthetic empty is intentional: it surfaces the topology
    // declaration as a first-class drift event, mirroring the way
    // viggy would alert on initial topology choice in production.
    assert_eq!(chain.entries().len(), 2);

    // Second reconcile: 2 apps + 1 promessa added.
    let second = r#"{
        "name": "a",
        "apps": [
            {"name": "x", "version": null},
            {"name": "y", "version": null}
        ],
        "infra": [],
        "promises": [{"name": "sla", "kind": "availability", "target": 99.99}],
        "topology": {"strategy": "solo", "nodes": 1}
    }"#;
    watcher
        .push(Change {
            source: "s".into(),
            kind: ChangeKind::Modified,
            source_text: second.into(),
            revision: 2,
        })
        .await;
    conduit.tick().await.unwrap();
    // Second reconcile diff vs first: y AppAdded + sla PromessaAdded
    // = 2 additional events → total 4.
    assert_eq!(chain.entries().len(), 4);
    assert!(
        chain.validate_chain(),
        "chain must remain unbroken across reconciles"
    );
}

// ── Property tests ───────────────────────────────────────────────

proptest_with_env! {
    /// For every Sistema with N apps + an empty prev, exactly N
    /// AppAdded events surface.
    #[test]
    fn n_apps_added_yields_n_events(n in 0usize..16) {
        let prev = solo("x");
        let mut next = prev.clone();
        for i in 0..n {
            next.apps.push(AppRef {
                name: format!("app-{i}").into(),
                version: None,
            });
        }
        let d = AnomalyEvent::diff(&prev, &next);
        prop_assert_eq!(d.len(), n);
        for e in &d {
            prop_assert!(matches!(e, AnomalyEvent::AppAdded(_)));
        }
    }

    /// diff is anti-symmetric on add/remove: prev→next adds N apps =
    /// next→prev removes N apps.
    #[test]
    fn diff_is_anti_symmetric_on_add_remove(n in 0usize..16) {
        let prev = solo("x");
        let mut next = prev.clone();
        for i in 0..n {
            next.apps.push(AppRef {
                name: format!("app-{i}").into(),
                version: None,
            });
        }
        let forward = AnomalyEvent::diff(&prev, &next);
        let backward = AnomalyEvent::diff(&next, &prev);
        prop_assert_eq!(forward.len(), backward.len());
        for (f, b) in forward.iter().zip(backward.iter()) {
            match (f, b) {
                (AnomalyEvent::AppAdded(a), AnomalyEvent::AppRemoved(b)) => {
                    prop_assert_eq!(&a.name, &b.name);
                }
                _ => prop_assert!(false, "expected add↔remove pair"),
            }
        }
    }
}
