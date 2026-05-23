//! Tests for SubprocessStage — typed Command construction for
//! magma + pangea + arbitrary external binaries.

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, ProvisioningStage, Sistema, StageKind, SubprocessStage,
    TopologyRef, magma_cloud_stage, pangea_networking_stage,
};

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

#[tokio::test]
async fn dry_run_records_invocation_without_spawning() {
    let stage = SubprocessStage::new(StageKind::Cloud, "magma", |s| {
        vec!["apply".into(), s.name.to_string()]
    })
    .dry_run();
    stage.provision(&sample("rio")).await.unwrap();
    let invocations = stage.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].sistema_name, "rio");
    assert_eq!(
        invocations[0].argv,
        vec!["apply".to_string(), "rio".to_string()]
    );
    assert_eq!(invocations[0].exit_code, Some(0));
}

#[tokio::test]
async fn live_run_against_real_binary_succeeds() {
    // Use /bin/echo as the binary — always available on macOS + Linux.
    let stage = SubprocessStage::new(StageKind::Cloud, "/bin/echo", |s| {
        vec!["hello".into(), s.name.to_string()]
    });
    stage.provision(&sample("rio")).await.unwrap();
    let invocations = stage.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].exit_code, Some(0));
    assert!(
        invocations[0].stdout.contains("hello rio"),
        "stdout was: {}",
        invocations[0].stdout
    );
}

#[tokio::test]
async fn nonzero_exit_surfaces_typed_propose_error() {
    let stage = SubprocessStage::new(StageKind::Cloud, "/usr/bin/false", |_| vec![]);
    let err = stage.provision(&sample("rio")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("subprocess-stage Cloud"), "got: {msg}");
}

#[tokio::test]
async fn missing_binary_surfaces_typed_propose_error() {
    let stage = SubprocessStage::new(StageKind::Cloud, "/no/such/binary/anywhere", |_| vec![]);
    let err = stage.provision(&sample("rio")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("subprocess-stage Cloud spawn"), "got: {msg}");
}

#[tokio::test]
async fn argv_closure_depends_on_sistema() {
    let stage = SubprocessStage::new(StageKind::EngenhoInstall, "echo", |sistema| {
        vec![
            "cluster".into(),
            sistema.name.to_string(),
            "nodes".into(),
            sistema.topology.nodes.to_string(),
        ]
    })
    .dry_run();
    stage
        .provision(&Sistema {
            topology: TopologyRef {
                strategy: "quorum_3m".into(),
                nodes: 3,
            },
            ..sample("rio")
        })
        .await
        .unwrap();
    let inv = &stage.invocations()[0];
    assert_eq!(inv.argv, vec!["cluster", "rio", "nodes", "3"]);
}

#[tokio::test]
async fn magma_cloud_stage_builds_argv_correctly() {
    let stage = magma_cloud_stage();
    assert_eq!(stage.kind(), StageKind::Cloud);
    // Use dry_run to avoid needing magma installed.
    let stage = magma_cloud_stage().dry_run();
    stage.provision(&sample("rio")).await.unwrap();
    let inv = &stage.invocations()[0];
    assert_eq!(
        inv.argv,
        vec!["apply", "--workspace", "rio", "--auto-approve"]
    );
}

#[tokio::test]
async fn pangea_networking_stage_builds_argv_correctly() {
    let stage = pangea_networking_stage();
    assert_eq!(stage.kind(), StageKind::Networking);
    let stage = pangea_networking_stage().dry_run();
    stage.provision(&sample("rio")).await.unwrap();
    let inv = &stage.invocations()[0];
    assert_eq!(inv.argv, vec!["deploy", "--cluster=rio"]);
}

#[tokio::test]
async fn plug_into_provisioning_controller_with_dry_run_stages() {
    use engenho_fonte::{MockProvisioningStage, ProvisioningController};
    use std::sync::Arc;

    let cloud = Arc::new(magma_cloud_stage().dry_run());
    let networking = Arc::new(pangea_networking_stage().dry_run());
    let engenho = Arc::new(MockProvisioningStage::new(StageKind::EngenhoInstall));
    let caixa = Arc::new(MockProvisioningStage::new(StageKind::CaixaBoot));
    let promessa = Arc::new(MockProvisioningStage::new(StageKind::PromessaRegister));
    let fed = Arc::new(MockProvisioningStage::new(StageKind::FederationJoin));

    let ctrl = ProvisioningController::new(
        cloud.clone(),
        networking.clone(),
        engenho,
        caixa,
        promessa,
        fed,
    );
    let report = ctrl.provision_cluster(&sample("rio")).await.unwrap();
    assert!(report.stage_failed.is_none());
    assert_eq!(report.stages_completed.len(), 6);

    // Magma + pangea each recorded one invocation.
    assert_eq!(cloud.invocations().len(), 1);
    assert_eq!(networking.invocations().len(), 1);
}
