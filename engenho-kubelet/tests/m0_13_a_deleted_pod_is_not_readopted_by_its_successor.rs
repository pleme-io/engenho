use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::backend::FakeEvent;
use engenho_kubelet::{FakeBackend, Kubelet, Readoption};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

const CONTAINER: &str = "default_p_app";

async fn boot_store(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn pod_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", "p")
}

async fn put_pod(store: &StoreMesh) -> String {
    store
        .propose(ResourceCommand::Put {
            key: pod_key(),
            value: json!({
                "kind": "Pod", "apiVersion": "v1",
                "metadata": {"name": "p"},
                "spec": {"nodeName": "node-A", "restartPolicy": "Always",
                         "containers": [{"name": "app", "image": "app"}]},
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    uid(&pod(store).await)
}

async fn delete_pod(store: &StoreMesh) {
    store
        .propose(ResourceCommand::delete(pod_key(), Reason::Operator))
        .await
        .unwrap();
}

async fn pod(store: &StoreMesh) -> Value {
    store.get(&pod_key()).await.expect("pod present")
}

fn uid(p: &Value) -> String {
    p.pointer("/metadata/uid")
        .and_then(Value::as_str)
        .expect("the store stamps a uid")
        .to_string()
}

fn container_id(p: &Value) -> String {
    p.pointer("/status/containerStatuses/0/containerID")
        .and_then(Value::as_str)
        .expect("a started container has an id")
        .to_string()
}

async fn running(backend: &FakeBackend, id: &str) -> bool {
    backend
        .containers()
        .await
        .iter()
        .any(|(held, status)| held == id && status.is_running())
}

async fn started_for(backend: &FakeBackend, id: &str) -> String {
    backend
        .spec_of(id)
        .await
        .expect("a live container has a spec")
        .pod
        .uid
}

#[tokio::test]
async fn a_pod_recreated_under_the_same_name_stops_the_old_process_and_starts_a_new_one() {
    let store = boot_store("recreated-same-name").await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    let uid_a = put_pod(&store).await;
    kubelet.tick().await.unwrap();
    let a = container_id(&pod(&store).await);
    assert!(running(&backend, &a).await, "premise: pod A runs");
    assert_eq!(started_for(&backend, &a).await, uid_a);

    delete_pod(&store).await;
    let uid_b = put_pod(&store).await;
    assert_ne!(uid_a, uid_b, "premise: a new pod under the same name");

    kubelet.tick().await.unwrap();

    let events = backend.events().await;
    assert!(
        events.contains(&FakeEvent::Stop(a.clone()))
            && events.contains(&FakeEvent::Remove(a.clone())),
        "the deleted pod's container is stopped and removed: {events:?}"
    );
    assert!(!running(&backend, &a).await, "pod A's process is gone");
    assert_eq!(
        backend.start_attempts(CONTAINER).await,
        2,
        "pod B gets a start of its own: {events:?}"
    );
    let b = container_id(&pod(&store).await);
    assert_ne!(b, a, "pod B is not observed through pod A's container");
    assert!(running(&backend, &b).await, "pod B's process runs");
    assert_eq!(started_for(&backend, &b).await, uid_b);
    assert_eq!(backend.running_count().await, 1, "never two copies");
}

#[tokio::test]
async fn the_new_pod_waits_until_the_deleted_pods_process_is_reaped() {
    let store = boot_store("recreated-waits").await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store).await;
    kubelet.tick().await.unwrap();
    let a = container_id(&pod(&store).await);
    backend.hold_unreaped(&a).await;

    delete_pod(&store).await;
    let uid_b = put_pod(&store).await;
    kubelet.tick().await.unwrap();
    assert_eq!(
        backend.start_attempts(CONTAINER).await,
        1,
        "nothing starts beside a process still inside its grace period"
    );
    assert!(
        pod(&store)
            .await
            .pointer("/status/containerStatuses/0/containerID")
            .and_then(Value::as_str)
            != Some(a.as_str()),
        "pod B never reports pod A's container"
    );

    backend.reap(&a).await;
    kubelet.tick().await.unwrap();
    assert_eq!(backend.start_attempts(CONTAINER).await, 2);
    let b = container_id(&pod(&store).await);
    assert_ne!(b, a);
    assert_eq!(started_for(&backend, &b).await, uid_b);
}

#[tokio::test]
async fn a_daemon_restart_readopts_the_running_container_of_the_same_pod_uid() {
    let store = boot_store("restart-readopts").await;
    let backend = Arc::new(FakeBackend::new().with_readoption(Readoption::AdoptsRunning));

    let uid_a = put_pod(&store).await;
    Kubelet::new(store.clone(), backend.clone(), "node-A")
        .tick()
        .await
        .unwrap();
    let a = container_id(&pod(&store).await);

    let restarted = Kubelet::new(store.clone(), backend.clone(), "node-A");
    restarted.tick().await.unwrap();

    let events = backend.events().await;
    assert!(
        events.contains(&FakeEvent::Adopt(CONTAINER.to_string())),
        "the restarted daemon hands the same pod its running container: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, FakeEvent::Stop(_) | FakeEvent::Remove(_))),
        "nothing is stopped: {events:?}"
    );
    assert_eq!(uid(&pod(&store).await), uid_a);
    assert_eq!(container_id(&pod(&store).await), a);
    assert!(running(&backend, &a).await);
    assert_eq!(backend.running_count().await, 1);
}
