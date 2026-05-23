//! Tests for KubeAppReconciler — translates AppRef → typed
//! engenho-types Deployment.

use engenho_fonte::{AppReconciler, AppRef, KubeAppReconciler};

#[tokio::test]
async fn translate_app_ref_with_version_produces_typed_deployment() {
    let r = KubeAppReconciler::new();
    let app = AppRef {
        name: "podinfo".into(),
        version: Some("6.4.1".into()),
    };
    let dep = r.translate(&app);
    assert_eq!(&dep.metadata.name, "podinfo");
    assert_eq!(dep.metadata.namespace.as_deref(), Some("default"));
    assert_eq!(dep.spec.replicas, Some(1));
    assert_eq!(dep.spec.template.spec.containers.len(), 1);
    let c = &dep.spec.template.spec.containers[0];
    assert_eq!(c.name, "podinfo");
    assert_eq!(c.image, "podinfo:6.4.1");
}

#[tokio::test]
async fn no_version_falls_back_to_latest_tag() {
    let r = KubeAppReconciler::new();
    let dep = r.translate(&AppRef {
        name: "x".into(),
        version: None,
    });
    let c = &dep.spec.template.spec.containers[0];
    assert_eq!(c.image, "x:latest");
}

#[tokio::test]
async fn label_invariants_hold() {
    let r = KubeAppReconciler::new();
    let dep = r.translate(&AppRef {
        name: "y".into(),
        version: None,
    });
    // app label
    assert_eq!(
        dep.metadata.labels.get("app").map(String::as_str),
        Some("y")
    );
    // sistema-managed label (lets a future GC reconciler find every
    // resource this loop owns)
    assert_eq!(
        dep.metadata
            .labels
            .get("pleme.io/sistema-managed")
            .map(String::as_str),
        Some("true")
    );
    // selector + template labels include `app`
    assert_eq!(
        dep.spec
            .selector
            .match_labels
            .get("app")
            .map(String::as_str),
        Some("y")
    );
    assert_eq!(
        dep.spec
            .template
            .metadata
            .labels
            .get("app")
            .map(String::as_str),
        Some("y")
    );
}

#[tokio::test]
async fn custom_namespace_propagates() {
    let r = KubeAppReconciler::default_with_namespace("rio");
    let dep = r.translate(&AppRef {
        name: "n".into(),
        version: None,
    });
    assert_eq!(dep.metadata.namespace.as_deref(), Some("rio"));
}

#[tokio::test]
async fn reconcile_records_manifest_in_audit_log() {
    let r = KubeAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "first".into(),
        version: None,
    })
    .await
    .unwrap();
    r.reconcile_app(&AppRef {
        name: "second".into(),
        version: Some("1.0".into()),
    })
    .await
    .unwrap();
    let emitted = r.emitted();
    assert_eq!(emitted.len(), 2);
    assert_eq!(&emitted[0].metadata.name, "first");
    assert_eq!(&emitted[1].metadata.name, "second");
    assert_eq!(
        emitted[1].spec.template.spec.containers[0].image,
        "second:1.0"
    );
}

#[tokio::test]
async fn translation_is_pure_and_deterministic() {
    let r1 = KubeAppReconciler::new();
    let r2 = KubeAppReconciler::new();
    let app = AppRef {
        name: "det".into(),
        version: Some("42".into()),
    };
    let d1 = r1.translate(&app);
    let d2 = r2.translate(&app);
    assert_eq!(d1, d2, "pure translation must produce equal Deployments");
}

// ── Integration with SystemController ───────────────────────────

#[tokio::test]
async fn kube_reconciler_plugs_into_system_controller() {
    use engenho_fonte::{
        Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockInfraReconciler,
        MockPromessaReconciler, MockPublisher, MockTopologyReconciler, MockWatcher,
        SystemController,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let apps = Arc::new(KubeAppReconciler::new());
    let infra = Arc::new(MockInfraReconciler::new());
    let promises = Arc::new(MockPromessaReconciler::new());
    let topology = Arc::new(MockTopologyReconciler::new());
    let ctrl = SystemController::new(
        apps.clone(),
        infra.clone(),
        promises.clone(),
        topology.clone(),
    );
    let proposer = Arc::new(ctrl);
    let conduit = Conduit::new(
        watcher.clone(),
        evaluator,
        proposer,
        Arc::new(MockAttester::new()),
        Arc::new(MockPublisher::new()),
    );

    watcher
        .push(Change {
            source: "rio".into(),
            kind: ChangeKind::Initial,
            source_text: r#"{"name":"rio","apps":[{"name":"podinfo","version":"6.4.1"}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;

    conduit.tick().await.unwrap();
    let emitted = apps.emitted();
    assert_eq!(emitted.len(), 1);
    assert_eq!(&emitted[0].metadata.name, "podinfo");
    assert_eq!(
        emitted[0].spec.template.spec.containers[0].image,
        "podinfo:6.4.1"
    );
}
