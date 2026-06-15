//! M0.8 — kubelet INIT-CONTAINER I/O driver.
//!
//! Proves the kubelet sequentially runs `spec.initContainers[]` (one at a time,
//! in order; each must exit 0 before the next starts), holds app containers
//! until every init container Succeeds, renders `status.initContainerStatuses`
//! + the `Initialized` condition, and Fails the pod when an init container
//! exits non-zero under restartPolicy:Never. All FakeBackend-only — no real
//! podman, no network. Consumes the ALREADY-LANDED pure interpreter
//! (`next_init_action` / `reconcile_pod_phase_with_init`) — this file exercises
//! the I/O shell on top of it.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::backend::FakeEvent;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("kubelet-m0_8").unwrap();
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

/// Put a Pod with the given init + app containers + restartPolicy, bound to a
/// node. `inits` and `apps` are `(name, image)` pairs.
async fn put_pod_with_init(
    store: &StoreMesh,
    name: &str,
    node_name: &str,
    restart_policy: &str,
    inits: &[(&str, &str)],
    apps: &[(&str, &str)],
) {
    let init_arr: Vec<Value> = inits
        .iter()
        .map(|(n, i)| json!({ "name": n, "image": i }))
        .collect();
    let app_arr: Vec<Value> = apps
        .iter()
        .map(|(n, i)| json!({ "name": n, "image": i }))
        .collect();
    let mut spec = json!({
        "nodeName": node_name,
        "restartPolicy": restart_policy,
        "containers": app_arr,
    });
    if !init_arr.is_empty() {
        spec["initContainers"] = Value::Array(init_arr);
    }
    let value = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": spec,
    });
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

fn pod_phase(pod: &Value) -> Option<String> {
    pod.get("status")
        .and_then(|s| s.get("phase"))
        .and_then(|p| p.as_str())
        .map(String::from)
}

/// The status of a named pod condition (e.g. "Initialized", "Ready").
fn condition_status(pod: &Value, ty: &str) -> Option<String> {
    pod.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .and_then(|conds| {
            conds.iter().find(|c| c.get("type").and_then(|t| t.as_str()) == Some(ty))
        })
        .and_then(|c| c.get("status").and_then(|s| s.as_str()))
        .map(String::from)
}

fn init_container_statuses(pod: &Value) -> Vec<Value> {
    pod.get("status")
        .and_then(|s| s.get("initContainerStatuses"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
}

fn container_statuses(pod: &Value) -> Vec<Value> {
    pod.get("status")
        .and_then(|s| s.get("containerStatuses"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
}

fn count_starts_named(events: &[FakeEvent], name: &str) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(n) if n == name))
        .count()
}

/// The FakeBackend container_id started under the given spec NAME, by scanning
/// the live containers' specs.
async fn id_for_spec_name(backend: &FakeBackend, spec_name: &str) -> Option<String> {
    for (id, _status) in backend.containers().await {
        if let Some(spec) = backend.spec_of(&id).await {
            if spec.name == spec_name {
                return Some(id);
            }
        }
    }
    None
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

// ── Test 1 — one init container exits 0 → app starts → Running ───────────

#[tokio::test]
async fn one_init_succeeds_then_app_runs() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // Pod with one init container (`setup`) + one app container (`web`).
    // restartPolicy:Always (the K8s default) — the init exits 0 so policy is
    // irrelevant to the happy path.
    put_pod_with_init(
        &store,
        "p1",
        "node-A",
        "Always",
        &[("setup", "img-setup")],
        &[("web", "img-web")],
    )
    .await;

    // Tick 1: the init container starts; the app container does NOT.
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p1_init-setup"),
        1,
        "init container started (with init- prefix on the backend name)"
    );
    assert_eq!(
        count_starts_named(&ev, "default_p1_web"),
        0,
        "app container must NOT start while init runs"
    );

    // Status: Pending, Initialized=False, initContainerStatuses shows the init
    // container Running (not yet exited).
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Pending"));
    assert_eq!(condition_status(&pod, "Initialized").as_deref(), Some("False"));
    let ics = init_container_statuses(&pod);
    assert_eq!(ics.len(), 1);
    assert_eq!(ics[0]["name"], "setup");
    assert!(ics[0]["state"]["running"].is_object());
    // No app container has started yet → empty/absent containerStatuses entries.
    assert!(
        container_statuses(&pod).is_empty(),
        "no app container statuses while init runs"
    );

    // The init container exits 0.
    let init_id = id_for_spec_name(&backend, "default_p1_init-setup")
        .await
        .expect("init container tracked");
    backend.set_exit(&init_id, 0).await;

    // Tick 2: kubelet observes init Succeeded → init_complete → starts the app
    // container. Pod becomes Running; Initialized=True.
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p1_web"),
        1,
        "app container starts after init Succeeds"
    );

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert_eq!(condition_status(&pod, "Initialized").as_deref(), Some("True"));
    // initContainerStatuses: the init Terminated exit 0.
    let ics = init_container_statuses(&pod);
    assert_eq!(ics.len(), 1);
    assert_eq!(ics[0]["state"]["terminated"]["exitCode"], 0);
    // App containerStatuses: web Running.
    let cs = container_statuses(&pod);
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0]["name"], "web");
    assert!(cs[0]["state"]["running"].is_object());

    teardown(store, kubelet).await;
}

// ── Test 2 — init exits non-zero under restartPolicy:Never → pod Failed ──

#[tokio::test]
async fn init_failure_under_never_fails_pod_app_never_starts() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod_with_init(
        &store,
        "p2",
        "node-A",
        "Never",
        &[("setup", "img-setup")],
        &[("web", "img-web")],
    )
    .await;

    // Tick 1: init starts.
    kubelet.tick().await.unwrap();
    let init_id = id_for_spec_name(&backend, "default_p2_init-setup")
        .await
        .expect("init container tracked");

    // The init container exits non-zero.
    backend.set_exit(&init_id, 7).await;

    // Tick 2: under Never, the failed init is terminal → pod Failed; app never
    // starts.
    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("p2")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));
    assert_eq!(condition_status(&pod, "Initialized").as_deref(), Some("False"));
    let ics = init_container_statuses(&pod);
    assert_eq!(ics[0]["state"]["terminated"]["exitCode"], 7);

    // The app container NEVER started — not now, not ever.
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p2_web"),
        0,
        "app container never starts when init fails terminally"
    );

    // Steady-state: a further tick does not start the app + stays Failed.
    kubelet.tick().await.unwrap();
    let pod = store.get(&pod_key("p2")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));
    assert_eq!(
        count_starts_named(&backend.events().await, "default_p2_web"),
        0
    );

    teardown(store, kubelet).await;
}

// ── Test 3 — TWO init containers run in order, then app starts ───────────

#[tokio::test]
async fn two_init_containers_run_in_order_then_app() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // Two init containers (init-0, init-1) + one app (web).
    put_pod_with_init(
        &store,
        "p3",
        "node-A",
        "Always",
        &[("init-0", "img-0"), ("init-1", "img-1")],
        &[("web", "img-web")],
    )
    .await;

    // Tick 1: ONLY init-0 starts (init-1 must NOT start concurrently).
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(count_starts_named(&ev, "default_p3_init-init-0"), 1);
    assert_eq!(
        count_starts_named(&ev, "default_p3_init-init-1"),
        0,
        "init[1] must not start until init[0] Succeeds"
    );
    assert_eq!(count_starts_named(&ev, "default_p3_web"), 0);

    // init-0 exits 0.
    let id0 = id_for_spec_name(&backend, "default_p3_init-init-0")
        .await
        .unwrap();
    backend.set_exit(&id0, 0).await;

    // Tick 2: init-0 Succeeded → init-1 starts; app still NOT started.
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p3_init-init-1"),
        1,
        "init[1] starts after init[0] Succeeds"
    );
    assert_eq!(count_starts_named(&ev, "default_p3_web"), 0);

    // Status mid-sequence: Pending, Initialized=False, two init statuses
    // (init-0 Terminated 0, init-1 Running).
    let pod = store.get(&pod_key("p3")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Pending"));
    assert_eq!(condition_status(&pod, "Initialized").as_deref(), Some("False"));
    let ics = init_container_statuses(&pod);
    assert_eq!(ics.len(), 2);
    assert_eq!(ics[0]["name"], "init-0");
    assert_eq!(ics[0]["state"]["terminated"]["exitCode"], 0);
    assert_eq!(ics[1]["name"], "init-1");
    assert!(ics[1]["state"]["running"].is_object());

    // init-1 exits 0.
    let id1 = id_for_spec_name(&backend, "default_p3_init-init-1")
        .await
        .unwrap();
    backend.set_exit(&id1, 0).await;

    // Tick 3: all init Succeeded → app starts → Running + Initialized=True.
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p3_web"),
        1,
        "app starts after both init containers Succeed"
    );
    let pod = store.get(&pod_key("p3")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert_eq!(condition_status(&pod, "Initialized").as_deref(), Some("True"));
    let ics = init_container_statuses(&pod);
    assert_eq!(ics.len(), 2);
    assert!(ics.iter().all(|s| s["state"]["terminated"]["exitCode"] == 0));

    teardown(store, kubelet).await;
}

// ── Test 4 — REGRESSION: no-init pod behaves exactly as before ───────────

#[tokio::test]
async fn no_init_pod_starts_app_immediately_initialized_true() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // A pod with NO init containers — the behavior-preserving common case.
    put_pod_with_init(
        &store,
        "p4",
        "node-A",
        "Always",
        &[],
        &[("web", "img-web")],
    )
    .await;

    // Tick 1: the app container starts immediately (no init gate).
    kubelet.tick().await.unwrap();
    let ev = backend.events().await;
    assert_eq!(
        count_starts_named(&ev, "default_p4_web"),
        1,
        "no-init pod starts the app container immediately"
    );

    let pod = store.get(&pod_key("p4")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    // App container Running.
    let cs = container_statuses(&pod);
    assert_eq!(cs.len(), 1);
    assert!(cs[0]["state"]["running"].is_object());

    // BYTE-IDENTICAL no-init render: NO initContainerStatuses, NO Initialized
    // condition (only ContainersReady + Ready, in that order). This is the
    // behavior-preserving guarantee — the no-init status must match the
    // pre-init-brick render exactly.
    assert!(
        pod.get("status").and_then(|s| s.get("initContainerStatuses")).is_none(),
        "no-init pod has NO initContainerStatuses field"
    );
    let conds = pod["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 2, "no-init pod has exactly 2 conditions");
    assert_eq!(conds[0]["type"], "ContainersReady");
    assert_eq!(conds[1]["type"], "Ready");
    assert!(
        condition_status(&pod, "Initialized").is_none(),
        "no-init pod has NO Initialized condition"
    );
    // Ready True (all app containers running).
    assert_eq!(condition_status(&pod, "Ready").as_deref(), Some("True"));

    teardown(store, kubelet).await;
}
