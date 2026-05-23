//! Tests for ShigotoProvisioningController — typed DAG-based
//! provisioning with parallel waves.

#![cfg(feature = "with-shigoto")]

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, MockProvisioningStage, ShigotoProvisioningController, Sistema,
    StageDag, StageDagBuilder, StageKind, TopologyRef,
};
use std::collections::BTreeMap;
use std::sync::Arc;

fn sample(name: &str) -> Sistema {
    Sistema {
        name: name.into(),
        apps: vec![AppRef {
            name: "podinfo".into(),
            version: None,
        }],
        infra: vec![InfraRef {
            name: "net".into(),
            backend: InfraBackend::Magma,
        }],
        promises: vec![],
        topology: TopologyRef {
            strategy: "solo".into(),
            nodes: 1,
        },
    }
}

#[test]
fn canonical_linear_dag_has_six_sequential_waves() {
    let dag = StageDag::canonical_linear();
    let waves = dag.waves().unwrap();
    assert_eq!(waves.len(), 6, "linear DAG → 6 sequential waves");
    for w in &waves {
        assert_eq!(
            w.len(),
            1,
            "each wave has exactly one stage in linear shape"
        );
    }
    let kinds: Vec<StageKind> = waves.iter().flat_map(|w| w.iter().copied()).collect();
    assert_eq!(
        kinds,
        vec![
            StageKind::Cloud,
            StageKind::Networking,
            StageKind::EngenhoInstall,
            StageKind::CaixaBoot,
            StageKind::PromessaRegister,
            StageKind::FederationJoin,
        ]
    );
}

#[test]
fn parallel_cloud_and_networking_collapses_to_two_stages_wave() {
    // Operator-declared DAG where Cloud + Networking have NO
    // dependency on each other (both depend on nothing in this
    // skeleton). Both fire in wave 0.
    let dag = StageDagBuilder::new()
        .stage(StageKind::Cloud)
        .stage(StageKind::Networking)
        .depends_on(StageKind::EngenhoInstall, StageKind::Cloud)
        .depends_on(StageKind::EngenhoInstall, StageKind::Networking)
        .build();
    let waves = dag.waves().unwrap();
    assert_eq!(waves.len(), 2);
    assert_eq!(
        waves[0].len(),
        2,
        "wave 0 has Cloud + Networking in parallel"
    );
    assert_eq!(waves[1].len(), 1, "wave 1 has EngenhoInstall");
    let set: std::collections::HashSet<StageKind> = waves[0].iter().copied().collect();
    assert!(set.contains(&StageKind::Cloud));
    assert!(set.contains(&StageKind::Networking));
}

#[tokio::test]
async fn linear_dag_runs_all_six_stages_in_order() {
    let dag = StageDag::canonical_linear();
    let cloud = Arc::new(MockProvisioningStage::new(StageKind::Cloud));
    let networking = Arc::new(MockProvisioningStage::new(StageKind::Networking));
    let engenho = Arc::new(MockProvisioningStage::new(StageKind::EngenhoInstall));
    let caixa = Arc::new(MockProvisioningStage::new(StageKind::CaixaBoot));
    let promessa = Arc::new(MockProvisioningStage::new(StageKind::PromessaRegister));
    let fed = Arc::new(MockProvisioningStage::new(StageKind::FederationJoin));

    let mut stages: BTreeMap<StageKind, Arc<dyn engenho_fonte::ProvisioningStage>> =
        BTreeMap::new();
    stages.insert(StageKind::Cloud, cloud.clone());
    stages.insert(StageKind::Networking, networking.clone());
    stages.insert(StageKind::EngenhoInstall, engenho.clone());
    stages.insert(StageKind::CaixaBoot, caixa.clone());
    stages.insert(StageKind::PromessaRegister, promessa.clone());
    stages.insert(StageKind::FederationJoin, fed.clone());

    let ctrl = ShigotoProvisioningController::new(dag, stages);
    let report = ctrl.provision_cluster(&sample("rio")).await.unwrap();
    assert!(report.stage_failed.is_none());
    assert_eq!(report.stages_completed.len(), 6);
    for stage in [&cloud, &networking, &engenho, &caixa, &promessa, &fed] {
        assert_eq!(stage.log().len(), 1);
    }
}

#[tokio::test]
async fn failure_in_a_wave_halts_subsequent_waves() {
    let dag = StageDag::canonical_linear();
    let cloud = Arc::new(MockProvisioningStage::new(StageKind::Cloud));
    let networking = Arc::new(MockProvisioningStage::new(StageKind::Networking));
    let engenho = Arc::new(MockProvisioningStage::new(StageKind::EngenhoInstall));
    let caixa = Arc::new(MockProvisioningStage::new(StageKind::CaixaBoot));
    let promessa = Arc::new(MockProvisioningStage::new(StageKind::PromessaRegister));
    let fed = Arc::new(MockProvisioningStage::new(StageKind::FederationJoin));

    engenho.fail_next();

    let mut stages: BTreeMap<StageKind, Arc<dyn engenho_fonte::ProvisioningStage>> =
        BTreeMap::new();
    stages.insert(StageKind::Cloud, cloud.clone());
    stages.insert(StageKind::Networking, networking.clone());
    stages.insert(StageKind::EngenhoInstall, engenho.clone());
    stages.insert(StageKind::CaixaBoot, caixa.clone());
    stages.insert(StageKind::PromessaRegister, promessa.clone());
    stages.insert(StageKind::FederationJoin, fed.clone());

    let ctrl = ShigotoProvisioningController::new(dag, stages);
    let report = ctrl.provision_cluster(&sample("rio")).await.unwrap();
    assert!(report.stage_failed.is_some());
    let (kind, _msg) = report.stage_failed.unwrap();
    assert_eq!(kind, StageKind::EngenhoInstall);
    // Earlier waves DID fire.
    assert_eq!(cloud.log().len(), 1);
    assert_eq!(networking.log().len(), 1);
    // Later waves did NOT fire.
    assert!(caixa.log().is_empty());
    assert!(promessa.log().is_empty());
    assert!(fed.log().is_empty());
}
