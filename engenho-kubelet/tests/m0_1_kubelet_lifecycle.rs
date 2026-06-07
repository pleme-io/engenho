//! M0.1 item 9 — kubelet pod-lifecycle close.
//!
//! Proves the reconcile-diff: delete-cleanup, unbind-cleanup,
//! exit→Succeeded/Failed mapping, and the anti-latch + anti-restart
//! invariants. All FakeBackend-only — no real podman, no network.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::{ContainerRuntime, FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("kubelet-m0_1").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
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
            key: pod_key(name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn delete_pod(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::delete(pod_key(name), Reason::Operator))
        .await
        .unwrap();
}

/// Patch a Pod's spec.nodeName (rebind / unbind without deletion).
async fn rebind_pod(store: &StoreMesh, name: &str, node_name: &str) {
    store
        .propose(ResourceCommand::patch(
            pod_key(name),
            json!({ "spec": { "nodeName": node_name } }),
            Reason::Scheduler,
        ))
        .await
        .unwrap();
}

/// The first FakeBackend container_id (the started pod's handle).
async fn first_container_id(backend: &FakeBackend) -> String {
    backend
        .containers()
        .await
        .into_iter()
        .next()
        .expect("at least one container started")
        .0
}

fn pod_phase(pod: &Value) -> Option<String> {
    pod.get("status")
        .and_then(|s| s.get("phase"))
        .and_then(|p| p.as_str())
        .map(String::from)
}

fn pod_ready_is_true(pod: &Value) -> bool {
    pod.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
}

fn count_starts(events: &[engenho_kubelet::backend::FakeEvent]) -> usize {
    use engenho_kubelet::backend::FakeEvent;
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(_)))
        .count()
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

// ── Test 1 — delete → stop THEN remove, local cleared ────────────────────

#[tokio::test]
async fn delete_managed_pod_stops_and_removes() {
    use engenho_kubelet::backend::FakeEvent;

    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    assert_eq!(backend.running_count().await, 1);
    let cid = first_container_id(&backend).await;

    // Hard-delete: the key leaves the store entirely.
    delete_pod(&store, "p1").await;
    assert!(store.get(&pod_key("p1")).await.is_none());

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_changed, 1, "one pod cleaned up");

    let events = backend.events().await;
    let stop_idx = events
        .iter()
        .position(|e| matches!(e, FakeEvent::Stop(id) if *id == cid))
        .expect("Stop event fired");
    let remove_idx = events
        .iter()
        .position(|e| matches!(e, FakeEvent::Remove(id) if *id == cid))
        .expect("Remove event fired");
    assert!(stop_idx < remove_idx, "stop must precede remove");

    assert_eq!(backend.running_count().await, 0);
    assert!(backend.containers().await.is_empty());

    // local no longer tracks it: a second tick drives zero backend calls.
    let before = backend.events().await.len();
    let report2 = kubelet.tick().await.unwrap();
    assert_eq!(report2.objects_changed, 0);
    assert_eq!(backend.events().await.len(), before, "no new backend calls");

    teardown(store, kubelet).await;
}

// ── Test 2 — unbind (spec.nodeName moved) → same cleanup path ─────────────

#[tokio::test]
async fn unbound_pod_is_cleaned_up() {
    use engenho_kubelet::backend::FakeEvent;

    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    assert_eq!(backend.running_count().await, 1);
    let cid = first_container_id(&backend).await;

    // Pod stays in the store but is rebound to node-B — no longer bound
    // here. Same cleanup as a delete, keyed on bound-set membership.
    rebind_pod(&store, "p1", "node-B").await;
    assert!(store.get(&pod_key("p1")).await.is_some());

    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_changed, 1);

    let events = backend.events().await;
    let stop_idx = events
        .iter()
        .position(|e| matches!(e, FakeEvent::Stop(id) if *id == cid))
        .expect("Stop fired");
    let remove_idx = events
        .iter()
        .position(|e| matches!(e, FakeEvent::Remove(id) if *id == cid))
        .expect("Remove fired");
    assert!(stop_idx < remove_idx);
    assert_eq!(backend.running_count().await, 0);

    // local cleared: second tick is a no-op (pod isn't bound here anymore).
    let before = backend.events().await.len();
    let report2 = kubelet.tick().await.unwrap();
    assert_eq!(report2.objects_changed, 0);
    assert_eq!(backend.events().await.len(), before);

    teardown(store, kubelet).await;
}

// ── Test 3 — exit 0 → Succeeded, no restart, idempotent ──────────────────

#[tokio::test]
async fn exit_zero_drives_succeeded() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    assert_eq!(pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(), Some("Running"));

    // Container exits cleanly.
    backend.set_exit(&cid, 0).await;

    let report = kubelet.tick().await.unwrap();
    assert!(report.objects_changed >= 1, "terminal transition is a write");

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Succeeded"));
    assert!(!pod_ready_is_true(&pod), "Ready must not be True for a terminal pod");
    let term = &pod["status"]["containerStatuses"][0]["state"]["terminated"];
    assert_eq!(term["exitCode"], 0);

    // No new Start: exactly one Start across the lifetime.
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(backend.running_count().await, 0);

    // Third tick: idempotent-skip → no restart, phase still Succeeded.
    let report3 = kubelet.tick().await.unwrap();
    assert_eq!(report3.objects_changed, 0, "terminal pod is steady-state NoChange");
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Succeeded")
    );

    teardown(store, kubelet).await;
}

// ── Test 4 — exit nonzero → Failed ───────────────────────────────────────

#[tokio::test]
async fn exit_nonzero_drives_failed() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    backend.set_exit(&cid, 137).await;
    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));
    let term = &pod["status"]["containerStatuses"][0]["state"]["terminated"];
    assert_eq!(term["exitCode"], 137);

    assert_eq!(count_starts(&backend.events().await), 1, "exactly one Start ever");
    assert_eq!(backend.running_count().await, 0);

    teardown(store, kubelet).await;
}

// ── Test 5 — still-running stays running, no spurious restart ─────────────

#[tokio::test]
async fn still_running_stays_running_no_extra_start() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Running")
    );
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(backend.running_count().await, 1);

    // Two more ticks while the container keeps running.
    for _ in 0..2 {
        let report = kubelet.tick().await.unwrap();
        // write_status_cas NoChange → no store write, no watch storm.
        assert_eq!(report.objects_changed, 0, "steady-state running tick is a no-op");
        assert_eq!(
            pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
            Some("Running")
        );
        // The anti-restart proof: still exactly one Start.
        assert_eq!(count_starts(&backend.events().await), 1);
        assert_eq!(backend.running_count().await, 1);
    }

    teardown(store, kubelet).await;
}

// ── Test 6 — vanished container is re-created next tick ───────────────────

#[tokio::test]
async fn vanished_container_is_recreated() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    assert_eq!(count_starts(&backend.events().await), 1);

    // Container vanishes out-of-band (manual podman rm / host reboot):
    // remove drops the backend record so status() returns None.
    backend.remove(&cid).await.unwrap();
    assert!(backend.status(&cid).await.unwrap().is_none());

    // Tick: kubelet observes the gap, clears local. Pod stays bound.
    kubelet.tick().await.unwrap();

    // Next tick re-creates the container → a managed bound pod converges
    // back to running.
    let report = kubelet.tick().await.unwrap();
    assert!(report.objects_changed >= 1);
    assert_eq!(count_starts(&backend.events().await), 2, "container re-created");
    assert_eq!(backend.running_count().await, 1);
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Running")
    );

    teardown(store, kubelet).await;
}
