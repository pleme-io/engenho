//! Tests for CaixaHelmInstaller — bridges rendered ChartDirs to
//! the cluster via `helm install/upgrade` subprocess.

#![cfg(feature = "with-caixa")]

use engenho_fonte::{AppReconciler, AppRef, CaixaAppReconciler, CaixaHelmInstaller};

#[tokio::test]
async fn dry_run_records_intended_argv() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "podinfo".into(),
        version: Some("6.4.1".into()),
    })
    .await
    .unwrap();
    let chart = &r.rendered()[0];

    let installer = CaixaHelmInstaller::new().dry_run();
    let record = installer.install(chart).await.unwrap();
    assert_eq!(record.release_name, "lareira-podinfo");
    assert_eq!(record.exit_code, Some(0));
    assert!(record.stdout.contains("DRY RUN"));
    assert!(record.stdout.contains("upgrade"));
    assert!(record.stdout.contains("--install"));
    assert!(record.stdout.contains("--create-namespace"));
    assert!(record.stdout.contains("lareira-podinfo"));
}

#[tokio::test]
async fn dry_run_with_custom_namespace_appears_in_argv() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "x".into(),
        version: None,
    })
    .await
    .unwrap();
    let chart = &r.rendered()[0];

    let installer = CaixaHelmInstaller::new().namespace("rio").dry_run();
    let record = installer.install(chart).await.unwrap();
    assert!(record.stdout.contains("--namespace rio"));
}

#[tokio::test]
async fn dry_run_with_kubeconfig_path_appears_in_argv() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "x".into(),
        version: None,
    })
    .await
    .unwrap();
    let chart = &r.rendered()[0];

    let installer = CaixaHelmInstaller::new()
        .kubeconfig("/tmp/test-kubeconfig.yaml")
        .dry_run();
    let record = installer.install(chart).await.unwrap();
    assert!(record.stdout.contains("--kubeconfig"));
    assert!(record.stdout.contains("/tmp/test-kubeconfig.yaml"));
}

#[tokio::test]
async fn n_charts_install_independently_in_dry_run() {
    let r = CaixaAppReconciler::new();
    for name in &["a", "b", "c"] {
        r.reconcile_app(&AppRef {
            name: (*name).into(),
            version: None,
        })
        .await
        .unwrap();
    }
    let installer = CaixaHelmInstaller::new().dry_run();
    for chart in &r.rendered() {
        installer.install(chart).await.unwrap();
    }
    let invocations = installer.invocations();
    assert_eq!(invocations.len(), 3);
    let names: Vec<&str> = invocations
        .iter()
        .map(|i| i.release_name.as_str())
        .collect();
    assert_eq!(names, vec!["lareira-a", "lareira-b", "lareira-c"]);
}

#[tokio::test]
async fn install_writes_chart_to_disk_in_dry_run() {
    let r = CaixaAppReconciler::new();
    r.reconcile_app(&AppRef {
        name: "podinfo".into(),
        version: None,
    })
    .await
    .unwrap();
    let chart = &r.rendered()[0];

    let installer = CaixaHelmInstaller::new().dry_run();
    let record = installer.install(chart).await.unwrap();
    // The chart was written to a tempdir; dry_run records the path
    // but the tempdir is cleaned up after install() returns.
    // We assert the path was non-empty + matches the chart name.
    let path_str = record.chart_path.display().to_string();
    assert!(path_str.contains("lareira-podinfo"), "got: {path_str}");
}
