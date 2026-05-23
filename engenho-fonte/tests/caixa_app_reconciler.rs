//! Tests for CaixaAppReconciler — proves caixa-helm renders a typed
//! chart from a typed AppRef.

#![cfg(feature = "with-caixa")]

use engenho_fonte::{AppReconciler, AppRef, CaixaAppReconciler};

#[tokio::test]
async fn reconcile_renders_chart_per_app() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "podinfo".into(),
        version: Some("6.4.1".into()),
    })
    .await
    .unwrap();
    let rendered = r.rendered();
    assert_eq!(rendered.len(), 1);
    let chart = &rendered[0];
    assert_eq!(chart.name, "lareira-podinfo");
    assert_eq!(chart.files.len(), 3);
    // Chart.yaml, values.yaml, README.md
    let paths: Vec<String> = chart
        .files
        .iter()
        .map(|f| f.path.display().to_string())
        .collect();
    assert!(paths.contains(&"Chart.yaml".to_string()));
    assert!(paths.contains(&"values.yaml".to_string()));
    assert!(paths.contains(&"README.md".to_string()));
}

#[tokio::test]
async fn n_apps_render_n_distinct_charts() {
    let r = CaixaAppReconciler::new();
    for name in &["app-a", "app-b", "app-c"] {
        r.reconcile_app(&AppRef {
            name: (*name).into(),
            version: None,
        })
        .await
        .unwrap();
    }
    let rendered = r.rendered();
    assert_eq!(rendered.len(), 3);
    let names: Vec<&str> = rendered.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["lareira-app-a", "lareira-app-b", "lareira-app-c"]
    );
}

#[tokio::test]
async fn version_propagates_into_chart_yaml() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "podinfo".into(),
        version: Some("9.9.9".into()),
    })
    .await
    .unwrap();
    let chart = &r.rendered()[0];
    let chart_yaml = chart
        .files
        .iter()
        .find(|f| f.path.display().to_string() == "Chart.yaml")
        .unwrap();
    assert!(
        chart_yaml.contents.contains("9.9.9"),
        "Chart.yaml should contain the AppRef version: {}",
        chart_yaml.contents
    );
}

// ── Integration with SystemController ───────────────────────────

#[tokio::test]
async fn plugs_into_system_controller() {
    use engenho_fonte::{
        Change, ChangeKind, Conduit, MockAttester, MockEvaluator, MockInfraReconciler,
        MockPromessaReconciler, MockPublisher, MockTopologyReconciler, MockWatcher,
        SystemController,
    };
    use std::sync::Arc;

    let watcher = Arc::new(MockWatcher::new());
    let evaluator = Arc::new(MockEvaluator::new());
    let apps = Arc::new(CaixaAppReconciler::new());
    let infra = Arc::new(MockInfraReconciler::new());
    let promises = Arc::new(MockPromessaReconciler::new());
    let topology = Arc::new(MockTopologyReconciler::new());
    let ctrl = SystemController::new(apps.clone(), infra, promises, topology);
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
            source_text: r#"{"name":"rio","apps":[{"name":"podinfo","version":"6.4.1"}],"infra":[],"promises":[],"topology":{"strategy":"solo","nodes":1}}"#.into(),
            revision: 1,
        })
        .await;

    conduit.tick().await.unwrap();
    let rendered = apps.rendered();
    assert_eq!(rendered.len(), 1);
    assert_eq!(rendered[0].name, "lareira-podinfo");
}
