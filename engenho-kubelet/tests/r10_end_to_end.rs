//! R10 integration — Kubelet picks up bound Pods, starts containers
//! via FakeBackend, patches Pod.status.podIP + Ready=True.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    command::{Reason, ResourceCommand},
    default_config, InProcessRouter, ResourceKey, StoreMesh,
};
use serde_json::json;

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("kubelet-r10").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put_pod(store: &StoreMesh, name: &str, image: &str, node_name: Option<&str>) {
    let mut value = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": {
            "containers": [{
                "name": "main",
                "image": image,
            }]
        }
    });
    if let Some(node) = node_name {
        value["spec"]["nodeName"] = json!(node);
    }
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn kubelet_starts_container_for_bound_pod() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "podinfo:6", Some("node-A")).await;

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_examined, 1);
    assert_eq!(report.objects_changed, 1);

    // Backend received exactly one Start event.
    let events = backend.events().await;
    assert_eq!(events.len(), 1);
    assert_eq!(backend.running_count().await, 1);

    // Pod status got patched.
    let key = ResourceKey::namespaced("", "v1", "Pod", "default", "p1");
    let pod = store.get(&key).await.unwrap();
    let status = pod.get("status").unwrap();
    assert_eq!(status.get("phase").unwrap(), "Running");
    assert!(status.get("podIP").is_some());
    let conditions = status.get("conditions").unwrap().as_array().unwrap();
    assert!(conditions.iter().any(|c| {
        c.get("type").unwrap() == "Ready" && c.get("status").unwrap() == "True"
    }));

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kubelet_ignores_pods_bound_to_other_nodes() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "mine", "img", Some("node-A")).await;
    put_pod(&store, "yours", "img", Some("node-B")).await;
    put_pod(&store, "unbound", "img", None).await;

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_examined, 1, "only node-A's pod examined");
    assert_eq!(report.objects_changed, 1);
    assert_eq!(backend.running_count().await, 1);

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kubelet_is_idempotent_at_steady_state() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p", "img", Some("node-A")).await;

    // First tick: starts the container.
    kubelet.tick().await.unwrap();
    assert_eq!(backend.running_count().await, 1);

    // Second tick: pod is now phase=Running; should NOT re-start.
    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_changed, 0);
    assert_eq!(backend.running_count().await, 1);
    // Backend still saw only one Start event.
    assert_eq!(backend.events().await.len(), 1);

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kubelet_skips_pods_with_invalid_manifest() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // Pod has nodeName but no containers — kubelet should log + skip.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "broken"),
            value: json!({
                "metadata": {"name": "broken"},
                "spec": {"nodeName": "node-A"}
                // No containers field
            }),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_examined, 1);
    assert_eq!(report.objects_skipped, 1);
    assert_eq!(report.objects_changed, 0);
    assert_eq!(backend.running_count().await, 0);

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kubelet_multiple_pods_each_get_a_container() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    for i in 0..5 {
        put_pod(&store, &format!("p{i}"), "img", Some("node-A")).await;
    }

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_examined, 5);
    assert_eq!(report.objects_changed, 5);
    assert_eq!(backend.running_count().await, 5);

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn kubelet_implements_controller_trait() {
    // Compile-time check that Kubelet is registerable on a
    // ControllerRuntime + driveable by WatchDriver. Verifies the
    // trait impl exists; runtime behavior is in the other tests.
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend, "n");
    let trait_obj: &dyn Controller = &kubelet;
    assert_eq!(trait_obj.name(), "kubelet");

    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
