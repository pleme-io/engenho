use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_controllers::event_recorder::{CollectingEventSink, Reason as EventReason};
use engenho_kubelet::cgroup::{NotEnforced, ResourceEnforcement};
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn node(
    name: &str,
    enforcement: ResourceEnforcement,
) -> (
    Arc<StoreMesh>,
    Arc<FakeBackend>,
    Arc<CollectingEventSink>,
    Kubelet,
) {
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config(name).unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let backend = Arc::new(FakeBackend::new().with_resource_enforcement(enforcement));
    let sink = Arc::new(CollectingEventSink::new());
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_event_sink(sink.clone());
    (store, backend, sink, kubelet)
}

async fn put_pod(store: &StoreMesh, name: &str, resources: &Value) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name, "namespace": "default" },
                "spec": {
                    "nodeName": "node-A",
                    "containers": [ { "name": "app", "image": "busybox", "resources": resources } ]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn running(backend: &FakeBackend) -> usize {
    backend
        .containers()
        .await
        .into_iter()
        .filter(|(_, s)| s.is_running())
        .count()
}

const UNENFORCED: ResourceEnforcement =
    ResourceEnforcement::NotEnforced(NotEnforced::PlatformUnsupported);

#[tokio::test]
async fn a_pod_whose_limits_this_node_does_not_enforce_runs_and_says_so() {
    let (store, backend, sink, kubelet) = node("unenforced", UNENFORCED).await;
    put_pod(
        &store,
        "bounded",
        &json!({ "limits": { "memory": "64Mi" } }),
    )
    .await;
    kubelet.tick().await.unwrap();

    assert_eq!(running(&backend).await, 1, "the pod is not stranded");
    let events = sink.drain();
    let warning = events
        .iter()
        .find(|e| e.reason == EventReason::ResourcesNotEnforced)
        .unwrap_or_else(|| panic!("a Warning says the limit is not enforced: {events:?}"));
    assert_eq!(warning.involved.name, "bounded");
    assert_eq!(warning.involved.namespace.as_deref(), Some("default"));
    assert!(warning.message.contains("app"), "{}", warning.message);
    assert!(warning.message.contains("macOS"), "{}", warning.message);
}

#[tokio::test]
async fn a_pod_declaring_nothing_or_a_node_that_enforces_says_nothing() {
    let (store, backend, sink, kubelet) = node("unbounded", UNENFORCED).await;
    put_pod(&store, "unbounded", &json!({})).await;
    kubelet.tick().await.unwrap();
    assert_eq!(running(&backend).await, 1);
    assert!(!sink.reasons().contains(&EventReason::ResourcesNotEnforced));

    let (store, backend, sink, kubelet) = node("enforced", ResourceEnforcement::Enforced).await;
    put_pod(
        &store,
        "bounded",
        &json!({ "limits": { "memory": "64Mi" } }),
    )
    .await;
    kubelet.tick().await.unwrap();
    assert_eq!(running(&backend).await, 1);
    assert!(!sink.reasons().contains(&EventReason::ResourcesNotEnforced));
}
