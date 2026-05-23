//! Tests for the PromessaTargetReconciler — proves the bridge from
//! fonte's PromessaRef to promessa-types' canonical
//! PromessaTargetKind.

#![cfg(feature = "with-promessa")]

use engenho_fonte::{
    PromessaIntent, PromessaKind, PromessaReconciler, PromessaRef, PromessaTargetReconciler,
    promessa_kind_to_target,
};
use promessa_types::PromessaTargetKind;

#[test]
fn every_fonte_kind_maps_to_canonical_target() {
    assert_eq!(
        promessa_kind_to_target(PromessaKind::Availability),
        PromessaTargetKind::Sla
    );
    assert_eq!(
        promessa_kind_to_target(PromessaKind::Budget),
        PromessaTargetKind::CostBudget
    );
    assert_eq!(
        promessa_kind_to_target(PromessaKind::Compliance),
        PromessaTargetKind::Compliance
    );
    assert_eq!(
        promessa_kind_to_target(PromessaKind::Security),
        PromessaTargetKind::Security
    );
    assert_eq!(
        promessa_kind_to_target(PromessaKind::CustomerKpi),
        PromessaTargetKind::CustomerKpi
    );
}

#[tokio::test]
async fn reconcile_records_typed_intent() {
    let r = PromessaTargetReconciler::new();
    r.reconcile_promessa(&PromessaRef {
        name: "sla".into(),
        kind: PromessaKind::Availability,
        target: 99.99,
    })
    .await
    .unwrap();
    let intents = r.intents();
    assert_eq!(intents.len(), 1);
    assert_eq!(&*intents[0].name, "sla");
    assert_eq!(intents[0].kind, PromessaTargetKind::Sla);
    assert!((intents[0].target - 99.99).abs() < f64::EPSILON);
}

#[tokio::test]
async fn n_promises_produce_n_intents() {
    let r = PromessaTargetReconciler::new();
    let refs = vec![
        PromessaRef {
            name: "sla".into(),
            kind: PromessaKind::Availability,
            target: 99.0,
        },
        PromessaRef {
            name: "cost".into(),
            kind: PromessaKind::Budget,
            target: 5000.0,
        },
        PromessaRef {
            name: "fed".into(),
            kind: PromessaKind::Compliance,
            target: 1.0,
        },
    ];
    for p in &refs {
        r.reconcile_promessa(p).await.unwrap();
    }
    let intents = r.intents();
    assert_eq!(intents.len(), 3);
    let kinds: Vec<PromessaTargetKind> = intents.iter().map(|i| i.kind).collect();
    assert_eq!(
        kinds,
        vec![
            PromessaTargetKind::Sla,
            PromessaTargetKind::CostBudget,
            PromessaTargetKind::Compliance,
        ]
    );
}

#[tokio::test]
async fn plug_into_system_controller_end_to_end() {
    use engenho_fonte::{
        AppRef, Change, ChangeKind, Conduit, MockAppReconciler, MockAttester, MockEvaluator,
        MockInfraReconciler, MockPublisher, MockTopologyReconciler, MockWatcher, SystemController,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let apps = Arc::new(MockAppReconciler::new());
    let infra = Arc::new(MockInfraReconciler::new());
    let promises = Arc::new(PromessaTargetReconciler::new());
    let topology = Arc::new(MockTopologyReconciler::new());
    let ctrl = SystemController::new(
        apps.clone(),
        infra.clone(),
        promises.clone(),
        topology.clone(),
    );
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
            source_text: r#"{
                "name": "rio", "apps": [],
                "infra": [],
                "promises": [
                    {"name":"sla","kind":"availability","target":99.99},
                    {"name":"cost","kind":"budget","target":5000.0}
                ],
                "topology": {"strategy":"solo","nodes":1}
            }"#
            .into(),
            revision: 1,
        })
        .await;

    conduit.tick().await.unwrap();
    let intents = promises.intents();
    assert_eq!(intents.len(), 2);
    let names: Vec<&str> = intents.iter().map(|i| i.name.as_ref()).collect();
    assert!(names.contains(&"sla"));
    assert!(names.contains(&"cost"));

    // Suppress unused AppRef import warning if it leaks
    let _: AppRef = AppRef {
        name: "noop".into(),
        version: None,
    };
}
