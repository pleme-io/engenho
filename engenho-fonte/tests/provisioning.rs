//! Tests for the ProvisioningController — declarative cluster
//! bootstrap walking the canonical 6-stage DAG.

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, ProvisioningStage, Sistema, StageKind, TopologyRef,
    mock_provisioning_controller,
};

fn sample_sistema(name: &str) -> Sistema {
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

#[tokio::test]
async fn happy_path_provisions_all_six_stages_in_order() {
    let (cloud, networking, engenho, caixa, promessa, fed, ctrl) = mock_provisioning_controller();
    let s = sample_sistema("rio");

    let report = ctrl.provision_cluster(&s).await.unwrap();
    assert!(report.stage_failed.is_none());
    assert_eq!(
        report.stages_completed,
        vec![
            StageKind::Cloud,
            StageKind::Networking,
            StageKind::EngenhoInstall,
            StageKind::CaixaBoot,
            StageKind::PromessaRegister,
            StageKind::FederationJoin,
        ]
    );
    // Every stage saw the same Sistema name.
    for stage in [&cloud, &networking, &engenho, &caixa, &promessa, &fed] {
        let log = stage.log();
        assert_eq!(log.len(), 1);
        assert_eq!(&*log[0], "rio");
    }
}

#[tokio::test]
async fn failure_halts_the_chain_at_the_failing_stage() {
    let (cloud, networking, engenho, caixa, promessa, fed, ctrl) = mock_provisioning_controller();
    let s = sample_sistema("rio");

    // Make EngenhoInstall fail on its next call.
    engenho.fail_next();
    let report = ctrl.provision_cluster(&s).await.unwrap();
    assert_eq!(
        report.stages_completed,
        vec![StageKind::Cloud, StageKind::Networking]
    );
    assert!(report.stage_failed.is_some());
    let (kind, _msg) = report.stage_failed.unwrap();
    assert_eq!(kind, StageKind::EngenhoInstall);

    // Later stages did NOT run.
    assert!(caixa.log().is_empty());
    assert!(promessa.log().is_empty());
    assert!(fed.log().is_empty());

    // Cloud + Networking DID run.
    assert_eq!(cloud.log().len(), 1);
    assert_eq!(networking.log().len(), 1);
}

#[tokio::test]
async fn n_clusters_all_provision_independently() {
    let (cloud, _n, _e, _c, _p, _f, ctrl) = mock_provisioning_controller();
    for name in &["rio", "sao", "minas"] {
        let r = ctrl.provision_cluster(&sample_sistema(name)).await.unwrap();
        assert!(r.stage_failed.is_none());
    }
    let log = cloud.log();
    let names: Vec<&str> = log.iter().map(|n| n.as_ref()).collect();
    assert_eq!(names, vec!["rio", "sao", "minas"]);
}

#[test]
fn canonical_order_covers_every_stage_kind() {
    let order = StageKind::canonical_order();
    assert_eq!(order.len(), 6);
    let mut all_kinds = std::collections::HashSet::new();
    for k in order {
        all_kinds.insert(k);
    }
    assert_eq!(
        all_kinds.len(),
        6,
        "canonical_order must contain each StageKind exactly once"
    );
}

#[test]
fn provisioning_stage_kind_method_matches_declared_kind() {
    let stage = engenho_fonte::MockProvisioningStage::new(StageKind::CaixaBoot);
    assert_eq!(stage.kind(), StageKind::CaixaBoot);
}
