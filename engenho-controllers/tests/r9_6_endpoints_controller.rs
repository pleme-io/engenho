//! R9.6 integration — EndpointsController materializing Endpoints
//! from Service selector + matching ready Pods.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, EndpointsController};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("controllers-r96").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put_service(store: &StoreMesh, name: &str, selector: Value) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Service", "default", name),
            value: json!({
                "kind": "Service",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "selector": selector,
                    "ports": [{ "port": 80, "targetPort": 9898 }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn put_ready_pod(store: &StoreMesh, name: &str, labels: Value, pod_ip: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name, "labels": labels },
                "spec": { "containers": [{"name": "main", "image": "podinfo:6"}] },
                "status": {
                    "podIP": pod_ip,
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn endpoint_ips(store: &StoreMesh, svc_name: &str) -> Vec<String> {
    let key = ResourceKey::namespaced("", "v1", "Endpoints", "default", svc_name);
    let ep = store.get(&key).await;
    let Some(ep) = ep else {
        return Vec::new();
    };
    let Some(subsets) = ep.get("subsets").and_then(|s| s.as_array()) else {
        return Vec::new();
    };
    let mut out: Vec<String> = subsets
        .iter()
        .flat_map(|s| {
            s.get("addresses")
                .and_then(|a| a.as_array())
                .into_iter()
                .flatten()
        })
        .filter_map(|a| a.get("ip").and_then(|i| i.as_str()).map(String::from))
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn endpoints_materialize_for_matching_ready_pods() {
    let store = boot_store().await;
    put_service(&store, "podinfo", json!({"app": "podinfo"})).await;
    put_ready_pod(&store, "p1", json!({"app": "podinfo"}), "10.0.0.1").await;
    put_ready_pod(&store, "p2", json!({"app": "podinfo"}), "10.0.0.2").await;

    let ctrl = EndpointsController::new(store.clone(), Some("default".into()));
    let report = ctrl.tick().await.unwrap();
    assert_eq!(report.objects_examined, 1);
    assert_eq!(report.objects_changed, 1);

    let ips = endpoint_ips(&store, "podinfo").await;
    assert_eq!(ips, vec!["10.0.0.1", "10.0.0.2"]);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn endpoints_exclude_non_matching_pods() {
    let store = boot_store().await;
    put_service(&store, "podinfo", json!({"app": "podinfo"})).await;
    put_ready_pod(&store, "p1", json!({"app": "podinfo"}), "10.0.0.1").await;
    put_ready_pod(&store, "other", json!({"app": "other"}), "10.0.0.99").await;

    let ctrl = EndpointsController::new(store.clone(), Some("default".into()));
    ctrl.tick().await.unwrap();

    assert_eq!(endpoint_ips(&store, "podinfo").await, vec!["10.0.0.1"]);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn endpoints_exclude_unready_pods() {
    let store = boot_store().await;
    put_service(&store, "podinfo", json!({"app": "podinfo"})).await;
    put_ready_pod(&store, "p1", json!({"app": "podinfo"}), "10.0.0.1").await;
    // unready pod
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "p2"),
            value: json!({
                "metadata": {"name": "p2", "labels": {"app": "podinfo"}},
                "status": {
                    "podIP": "10.0.0.2",
                    "conditions": [{"type": "Ready", "status": "False"}]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ctrl = EndpointsController::new(store.clone(), Some("default".into()));
    ctrl.tick().await.unwrap();

    assert_eq!(endpoint_ips(&store, "podinfo").await, vec!["10.0.0.1"]);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn endpoints_idempotent_at_steady_state() {
    let store = boot_store().await;
    put_service(&store, "podinfo", json!({"app": "podinfo"})).await;
    put_ready_pod(&store, "p1", json!({"app": "podinfo"}), "10.0.0.1").await;

    let ctrl = EndpointsController::new(store.clone(), Some("default".into()));
    ctrl.tick().await.unwrap();
    let report2 = ctrl.tick().await.unwrap();
    assert_eq!(report2.objects_changed, 0);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn endpoints_update_when_pod_added() {
    let store = boot_store().await;
    put_service(&store, "podinfo", json!({"app": "podinfo"})).await;
    put_ready_pod(&store, "p1", json!({"app": "podinfo"}), "10.0.0.1").await;

    let ctrl = EndpointsController::new(store.clone(), Some("default".into()));
    ctrl.tick().await.unwrap();
    assert_eq!(endpoint_ips(&store, "podinfo").await, vec!["10.0.0.1"]);

    // Add a second pod.
    put_ready_pod(&store, "p2", json!({"app": "podinfo"}), "10.0.0.2").await;
    let report = ctrl.tick().await.unwrap();
    assert_eq!(report.objects_changed, 1);
    assert_eq!(
        endpoint_ips(&store, "podinfo").await,
        vec!["10.0.0.1", "10.0.0.2"]
    );

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
