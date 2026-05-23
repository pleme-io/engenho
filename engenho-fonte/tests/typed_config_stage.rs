//! Tests for TypedConfigStage — JSON-config-driven concrete
//! ProvisioningStage suitable for real magma/pangea/engenho-install
//! integration via per-Sistema typed configs.

use engenho_fonte::{
    AppRef, InfraBackend, InfraRef, ProvisioningStage, Sistema, StageKind, TopologyRef,
    TypedConfigStage,
};
use engenho_sui_typescape::TypescapeValue;

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
async fn constant_json_evaluates_to_typed_value() {
    let s = TypedConfigStage::from_json(StageKind::Cloud, r#"{"region":"us-east-1"}"#);
    s.provision(&sample("rio")).await.unwrap();
    let eval = s.evaluated();
    assert_eq!(eval.len(), 1);
    let attrs = eval[0].as_attrs().unwrap();
    assert_eq!(
        attrs.get("region"),
        Some(&TypescapeValue::string("us-east-1"))
    );
}

#[tokio::test]
async fn per_sistema_config_depends_on_sistema() {
    let s = TypedConfigStage::per_sistema(StageKind::EngenhoInstall, |sistema| {
        format!(
            r#"{{"cluster":"{}","nodes":{}}}"#,
            sistema.name, sistema.topology.nodes
        )
    });
    s.provision(&sample("rio")).await.unwrap();
    s.provision(&Sistema {
        name: "sao".into(),
        topology: TopologyRef {
            strategy: "quorum_3m".into(),
            nodes: 3,
        },
        ..sample("rio")
    })
    .await
    .unwrap();
    let eval = s.evaluated();
    assert_eq!(eval.len(), 2);
    let attrs0 = eval[0].as_attrs().unwrap();
    assert_eq!(attrs0.get("cluster"), Some(&TypescapeValue::string("rio")));
    let attrs1 = eval[1].as_attrs().unwrap();
    assert_eq!(attrs1.get("cluster"), Some(&TypescapeValue::string("sao")));
    assert_eq!(attrs1.get("nodes"), Some(&TypescapeValue::int(3)));
}

#[tokio::test]
async fn malformed_json_surfaces_typed_propose_error() {
    let s = TypedConfigStage::from_json(StageKind::Cloud, "not json {");
    let err = s.provision(&sample("rio")).await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("typed-config-stage"), "got: {msg}");
}

#[tokio::test]
async fn plugs_into_provisioning_controller() {
    use engenho_fonte::ProvisioningController;
    use std::sync::Arc;

    let cloud = Arc::new(TypedConfigStage::per_sistema(StageKind::Cloud, |s| {
        format!(r#"{{"cluster":"{}"}}"#, s.name)
    }));
    let networking = Arc::new(TypedConfigStage::from_json(
        StageKind::Networking,
        r#"{"dns":"cloudflare"}"#,
    ));
    let engenho = Arc::new(TypedConfigStage::from_json(StageKind::EngenhoInstall, "{}"));
    let caixa = Arc::new(TypedConfigStage::from_json(StageKind::CaixaBoot, "{}"));
    let promessa = Arc::new(TypedConfigStage::from_json(
        StageKind::PromessaRegister,
        "{}",
    ));
    let fed = Arc::new(TypedConfigStage::from_json(StageKind::FederationJoin, "{}"));

    let ctrl =
        ProvisioningController::new(cloud.clone(), networking, engenho, caixa, promessa, fed);
    let report = ctrl.provision_cluster(&sample("rio")).await.unwrap();
    assert!(report.stage_failed.is_none());
    assert_eq!(report.stages_completed.len(), 6);

    // Cloud stage evaluated its per-Sistema config with the right cluster name.
    let cloud_eval = cloud.evaluated();
    assert_eq!(cloud_eval.len(), 1);
    let cloud_attrs = cloud_eval[0].as_attrs().unwrap();
    assert_eq!(
        cloud_attrs.get("cluster"),
        Some(&TypescapeValue::string("rio"))
    );
}
