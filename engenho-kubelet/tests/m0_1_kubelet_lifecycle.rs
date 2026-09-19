//! M0.1 item 9 — kubelet pod-lifecycle close.
//!
//! Proves the reconcile-diff: delete-cleanup, unbind-cleanup,
//! exit→Succeeded/Failed mapping, and the anti-latch + anti-restart
//! invariants. All FakeBackend-only — no real podman, no network.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::{ContainerRuntime, FakeBackend, Kubelet, LogOptions, Readoption};
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
    // Default helper: restartPolicy:Never so a `set_exit` reaches the terminal
    // latch (Succeeded/Failed). Under the K8s default (Always) an exited
    // container is restarted + the pod stays Running — covered by the
    // restartPolicy:Always tests below; the terminal-path tests use Never.
    put_pod_with_policy(store, name, image, node_name, Some("Never")).await;
}

/// Put a Pod with an explicit `spec.restartPolicy` (or absent when `None`).
async fn put_pod_with_policy(
    store: &StoreMesh,
    name: &str,
    image: &str,
    node_name: Option<&str>,
    restart_policy: Option<&str>,
) {
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
    if let Some(policy) = restart_policy {
        value["spec"]["restartPolicy"] = json!(policy);
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

/// Put a 2-container Pod (`web` + `sidecar`) bound to a node, with an explicit
/// `spec.restartPolicy`. Used by the multi-container tests.
async fn put_multi_pod(store: &StoreMesh, name: &str, node_name: &str, restart_policy: &str) {
    let value = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": {
            "nodeName": node_name,
            "restartPolicy": restart_policy,
            "containers": [
                { "name": "web", "image": "img-web" },
                { "name": "sidecar", "image": "img-sidecar" },
            ]
        }
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

/// Count `Start(spec_name)` events whose spec name equals `name`.
fn count_starts_named(events: &[engenho_kubelet::backend::FakeEvent], name: &str) -> usize {
    use engenho_kubelet::backend::FakeEvent;
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(n) if n == name))
        .count()
}

fn count_stops(events: &[engenho_kubelet::backend::FakeEvent]) -> usize {
    use engenho_kubelet::backend::FakeEvent;
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Stop(_)))
        .count()
}

fn count_removes(events: &[engenho_kubelet::backend::FakeEvent]) -> usize {
    use engenho_kubelet::backend::FakeEvent;
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Remove(_)))
        .count()
}

/// The pod's `status.containerStatuses` array.
fn container_statuses(pod: &Value) -> Vec<Value> {
    pod.get("status")
        .and_then(|s| s.get("containerStatuses"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
}

/// FakeBackend container_id assigned to the spec named `spec_name`, by
/// scanning the events for the Start + correlating to the live container set.
/// Simpler: the FakeBackend assigns ids in start order, so the Nth Start's id
/// is the Nth tracked container. For these tests we resolve by querying the
/// backend's live containers (ids are opaque but stable).
async fn container_id_count(backend: &FakeBackend) -> usize {
    backend.containers().await.len()
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
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Running")
    );

    // Container exits cleanly.
    backend.set_exit(&cid, 0).await;

    let report = kubelet.tick().await.unwrap();
    assert!(
        report.objects_changed >= 1,
        "terminal transition is a write"
    );

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Succeeded"));
    assert!(
        !pod_ready_is_true(&pod),
        "Ready must not be True for a terminal pod"
    );
    let term = &pod["status"]["containerStatuses"][0]["state"]["terminated"];
    assert_eq!(term["exitCode"], 0);
    assert_eq!(term["reason"], "Completed");

    // No new Start: exactly one Start across the lifetime.
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(backend.running_count().await, 0);

    // Third tick: idempotent-skip → no restart, phase still Succeeded.
    let report3 = kubelet.tick().await.unwrap();
    assert_eq!(
        report3.objects_changed, 0,
        "terminal pod is steady-state NoChange"
    );
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

    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "exactly one Start ever"
    );
    assert_eq!(backend.running_count().await, 0);

    teardown(store, kubelet).await;
}

// ── T1.2 — how a container ended, carried to the Pod phase ───────────────

/// ★ Live on ryn before T1.2: a `SIGKILL`ed `restartPolicy: Never` container
/// has no exit code, the kubelet read the absence as 0, and the pod was
/// published `Succeeded` — which JobController then counted as a completion.
#[tokio::test]
async fn a_sigkilled_never_pod_is_failed_not_succeeded() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    backend
        .set_run_state(&cid, RunState::Exited(ExitDisposition::Signal(9)))
        .await;
    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(
        pod_phase(&pod).as_deref(),
        Some("Failed"),
        "a killed container is not a successful one"
    );
    let term = &pod["status"]["containerStatuses"][0]["state"]["terminated"];
    assert_eq!(term["exitCode"], 137, "SIGKILL renders as 128+9");
    assert_eq!(term["reason"], "Error");
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "Never: no restart"
    );

    teardown(store, kubelet).await;
}

/// An exit nobody observed: upstream renders it terminated / 137 /
/// `ContainerStatusUnknown`, and under `Never` the pod is Failed.
#[tokio::test]
async fn an_unobserved_exit_under_never_is_failed_as_container_status_unknown() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    backend.set_run_state(&cid, RunState::Unknown).await;
    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));
    let term = &pod["status"]["containerStatuses"][0]["state"]["terminated"];
    assert_eq!(term["exitCode"], 137);
    assert_eq!(term["reason"], "ContainerStatusUnknown");
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "Never: no restart"
    );

    teardown(store, kubelet).await;
}

/// Under `OnFailure` an unobserved exit and a signal death are failures, so
/// both are restarted rather than latched `Succeeded`.
#[tokio::test]
async fn under_on_failure_a_signal_or_unobserved_exit_is_restarted() {
    for run_state in [
        RunState::Exited(ExitDisposition::Signal(9)),
        RunState::Unknown,
    ] {
        let store = boot_store().await;
        let backend = Arc::new(FakeBackend::new());
        let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

        put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("OnFailure")).await;
        kubelet.tick().await.unwrap();
        let cid = first_container_id(&backend).await;

        backend.set_run_state(&cid, run_state).await;
        kubelet.tick().await.unwrap();

        assert_eq!(
            count_starts(&backend.events().await),
            2,
            "{run_state:?} under OnFailure must be restarted"
        );
        assert_eq!(
            pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
            Some("Running"),
            "{run_state:?}"
        );

        teardown(store, kubelet).await;
    }
}

// ── T1.2 c2 — re-adoption: no local record is not "never started" ───────
//
// A kubelet restart forgets every pod. A pod whose STORED status shows a
// container up, on a runtime that cannot hand back what the previous process
// started, is an exit nobody observed — not a pod that has yet to run.

/// Simulate a kubelet restart: a fresh `Kubelet` over the same store and the
/// same runtime, with no memory of what the old one started.
fn restarted_kubelet(store: &Arc<StoreMesh>, backend: &Arc<FakeBackend>) -> Kubelet {
    Kubelet::new(store.clone(), backend.clone(), "node-A")
}

/// ★ THE ryn DEFECT. A running `restartPolicy: Never` pod survived every
/// kubelet restart by being started again from scratch — a Job pod run twice,
/// in place, with restartCount 0 and nothing in its status to say so.
#[tokio::test]
async fn after_a_restart_a_running_never_pod_is_not_rerun_in_place() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    assert_eq!(
        backend.readoption(),
        Readoption::Cannot,
        "control: this runtime cannot hand back a container"
    );
    let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod(&store, "job-1", "img", Some("node-A")).await;
    first.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    let before = store.get(&pod_key("job-1")).await.unwrap();
    assert_eq!(pod_phase(&before).as_deref(), Some("Running"));
    drop(first);

    let kubelet = restarted_kubelet(&store, &backend);
    kubelet.tick().await.unwrap();
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "a Never pod that already ran must not be started a second time"
    );
    let pod = store.get(&pod_key("job-1")).await.unwrap();
    assert_eq!(
        pod_phase(&pod).as_deref(),
        Some("Failed"),
        "it cannot succeed: how its container ended was never observed"
    );
    let status = &container_statuses(&pod)[0];
    assert_eq!(
        status["state"]["terminated"]["reason"],
        "ContainerStatusUnknown"
    );
    assert_eq!(status["state"]["terminated"]["exitCode"], 137);
    assert_eq!(
        status["containerID"].as_str(),
        Some(cid.as_str()),
        "the run it reports is the one that happened"
    );

    teardown(store, kubelet).await;
}

/// ★ On podman (CLI) and CRI the container OUTLIVES the kubelet. Publishing
/// the lost run `terminated` while it still runs is a false status, and the
/// replacement JobController creates would run beside it — the double run,
/// made concurrent. The lost run is stopped and removed BEFORE the pod is
/// published Failed.
#[tokio::test]
async fn after_a_restart_a_lost_never_run_is_torn_down_before_the_pod_is_failed() {
    use engenho_kubelet::backend::FakeEvent;
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod(&store, "job-1", "img", Some("node-A")).await;
    first.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    drop(first);
    assert!(
        backend.status(&cid).await.unwrap().unwrap().is_running(),
        "control: the runtime still runs what the old kubelet started"
    );

    let kubelet = restarted_kubelet(&store, &backend);
    kubelet.tick().await.unwrap();

    assert_eq!(
        backend.status(&cid).await.unwrap(),
        None,
        "the lost run is not left running with no record anywhere"
    );
    let events = backend.events().await;
    let stop = events
        .iter()
        .position(|e| *e == FakeEvent::Stop(cid.clone()));
    let remove = events
        .iter()
        .position(|e| *e == FakeEvent::Remove(cid.clone()));
    assert!(
        matches!((stop, remove), (Some(s), Some(r)) if s < r),
        "stop THEN remove: {events:?}"
    );
    assert_eq!(count_starts(&events), 1, "and nothing started in its place");
    let pod = store.get(&pod_key("job-1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));

    teardown(store, kubelet).await;
}

/// A lost run the runtime cannot be ASKED about, or will not stop, holds the
/// pod: no `Failed` claiming it ended, and no second copy started beside it.
/// When the runtime answers again the run is torn down and the pod settles.
#[tokio::test]
async fn a_lost_run_that_cannot_be_torn_down_holds_the_pod() {
    #[derive(Clone, Copy, Debug)]
    enum Fault {
        Poll,
        Stop,
    }
    let cases = [Some("Never"), None]
        .into_iter()
        .flat_map(|policy| [(policy, Fault::Stop), (policy, Fault::Poll)]);
    for (policy, fault) in cases {
        let store = boot_store().await;
        let backend = Arc::new(FakeBackend::new());
        let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
        put_pod_with_policy(&store, "p", "img", Some("node-A"), policy).await;
        first.tick().await.unwrap();
        let cid = first_container_id(&backend).await;
        let before = store.get(&pod_key("p")).await.unwrap();
        drop(first);

        match fault {
            Fault::Poll => {
                backend
                    .seed_status_fault(&cid, "podman socket: connection refused")
                    .await;
            }
            Fault::Stop => {
                backend
                    .seed_stop_fault(&cid, "podman stop: timed out")
                    .await
            }
        }
        let kubelet = restarted_kubelet(&store, &backend);
        kubelet.tick().await.unwrap();
        kubelet.tick().await.unwrap();

        let case = (policy, fault);
        assert_eq!(
            count_starts(&backend.events().await),
            1,
            "{case:?}: nothing starts beside a run that may still be up"
        );
        let pod = store.get(&pod_key("p")).await.unwrap();
        assert_eq!(
            pod["status"], before["status"],
            "{case:?}: no status claims the run ended"
        );
        match fault {
            Fault::Poll => backend.clear_status_fault(&cid).await,
            Fault::Stop => backend.clear_stop_fault(&cid).await,
        }
        assert!(
            backend.status(&cid).await.unwrap().unwrap().is_running(),
            "{case:?}: control: the lost run is still up"
        );

        kubelet.tick().await.unwrap();

        assert_eq!(
            backend.status(&cid).await.unwrap(),
            None,
            "{case:?}: torn down once the runtime answers"
        );
        let pod = store.get(&pod_key("p")).await.unwrap();
        let (phase, starts, restarts) = match policy {
            Some("Never") => ("Failed", 1, 0),
            _ => ("Running", 2, 1),
        };
        assert_eq!(pod_phase(&pod).as_deref(), Some(phase), "{case:?}");
        assert_eq!(count_starts(&backend.events().await), starts, "{case:?}");
        assert_eq!(
            container_statuses(&pod)[0]["restartCount"],
            restarts,
            "{case:?}"
        );

        teardown(store, kubelet).await;
    }
}

/// Under a restarting policy the lost pod is started again — and the
/// restart is counted, not reset to a first start.
#[tokio::test]
async fn after_a_restart_an_always_pod_is_restarted_and_the_restart_counted() {
    // The second runtime shape is ryn's: it cannot re-adopt, AND it derives a
    // container's id from its name, so the fresh process has the lost run's
    // id. An id match there is not an adoption, and the restart still counts.
    // (A fresh fake per case: `FakeBackend::clone` shares its state.)
    let cases = [None, Some("OnFailure")]
        .into_iter()
        .flat_map(|policy| [(policy, "counter ids"), (policy, "name-derived ids")]);
    for (policy, ids) in cases {
        let runtime = match ids {
            "name-derived ids" => FakeBackend::new().with_ids_from_names(),
            _ => FakeBackend::new(),
        };
        let store = boot_store().await;
        let backend = Arc::new(runtime);
        let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
        put_pod_with_policy(&store, "web", "img", Some("node-A"), policy).await;
        first.tick().await.unwrap();
        drop(first);

        let kubelet = restarted_kubelet(&store, &backend);
        kubelet.tick().await.unwrap();

        assert_eq!(
            count_starts(&backend.events().await),
            2,
            "{policy:?} / {ids}"
        );
        let pod = store.get(&pod_key("web")).await.unwrap();
        assert_eq!(
            pod_phase(&pod).as_deref(),
            Some("Running"),
            "{policy:?} / {ids}"
        );
        assert_eq!(
            container_statuses(&pod)[0]["restartCount"],
            1,
            "{policy:?} / {ids}: the run the old kubelet lost is a restart"
        );
        assert_eq!(
            container_id_count(&backend).await,
            1,
            "{policy:?} / {ids}: the lost run is torn down, not left running beside its restart"
        );

        teardown(store, kubelet).await;
    }
}

/// A pod that never got a container up before the restart is still just a
/// pod to start: the rule reads the stored status, it does not refuse pods.
#[tokio::test]
async fn after_a_restart_a_pod_that_never_started_is_started_normally() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure("default_job-2_main", "image not known")
        .await;
    let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod(&store, "job-2", "img", Some("node-A")).await;
    first.tick().await.unwrap();
    assert_eq!(
        pod_phase(&store.get(&pod_key("job-2")).await.unwrap()).as_deref(),
        Some("Pending"),
        "control: nothing ever started"
    );
    drop(first);

    let fixed = Arc::new(FakeBackend::new());
    let kubelet = restarted_kubelet(&store, &fixed);
    kubelet.tick().await.unwrap();

    assert_eq!(count_starts(&fixed.events().await), 1);
    let pod = store.get(&pod_key("job-2")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert_eq!(container_statuses(&pod)[0]["restartCount"], 0);

    teardown(store, kubelet).await;
}

/// On a runtime that CAN hand back a running container, the restarted
/// kubelet adopts it: no second copy, no failure, and the restart count the
/// container had earned is kept.
#[tokio::test]
async fn after_a_restart_an_adopting_runtime_keeps_the_running_container_and_its_count() {
    use engenho_kubelet::backend::FakeEvent;
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new().with_readoption(Readoption::AdoptsRunning));
    let first = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod_with_policy(&store, "web", "img", Some("node-A"), None).await;
    first.tick().await.unwrap();
    // Earn one restart before the kubelet goes away.
    let cid = first_container_id(&backend).await;
    backend.set_exit(&cid, 1).await;
    first.tick().await.unwrap();
    let pod = store.get(&pod_key("web")).await.unwrap();
    assert_eq!(container_statuses(&pod)[0]["restartCount"], 1, "control");
    let live_id = container_statuses(&pod)[0]["containerID"].clone();
    drop(first);

    let kubelet = restarted_kubelet(&store, &backend);
    kubelet.tick().await.unwrap();

    let events = backend.events().await;
    assert_eq!(count_starts(&events), 2, "no third copy was started");
    assert!(events.contains(&FakeEvent::Adopt("default_web_main".into())));
    let pod = store.get(&pod_key("web")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    let status = &container_statuses(&pod)[0];
    assert_eq!(status["containerID"], live_id, "the same container");
    assert_eq!(
        status["restartCount"], 1,
        "adopting a container is not restarting it, and does not reset it"
    );

    teardown(store, kubelet).await;
}

// ── T1.2 c2 — a poll that failed is not a container that never started ───

/// ★ A status poll that errors used to render the container
/// `Waiting{ContainerCreating}` with no id — a running pod published
/// `Pending` on every podman hiccup. The write is withheld instead: the last
/// published status stands until a poll answers.
#[tokio::test]
async fn a_failed_status_poll_withholds_the_write_instead_of_reporting_never_started() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod_with_policy(&store, "web", "img", Some("node-A"), None).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    let before = store.get(&pod_key("web")).await.unwrap();
    assert_eq!(pod_phase(&before).as_deref(), Some("Running"), "control");

    backend
        .seed_status_fault(&cid, "podman socket: connection refused")
        .await;
    kubelet.tick().await.unwrap();

    let pod = store.get(&pod_key("web")).await.unwrap();
    assert_eq!(
        pod["status"], before["status"],
        "a tick that could not see the container publishes nothing"
    );
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert!(
        container_statuses(&pod)[0]["state"]
            .get("waiting")
            .is_none(),
        "a running container is never re-reported as not yet started"
    );
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "and not restarted"
    );

    // The runtime answers again: reconciliation resumes where it was.
    backend.clear_status_fault(&cid).await;
    kubelet.tick().await.unwrap();
    let pod = store.get(&pod_key("web")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert_eq!(count_starts(&backend.events().await), 1);

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
        assert_eq!(
            report.objects_changed, 0,
            "steady-state running tick is a no-op"
        );
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

// ── Test 6 — a vanished container is an exit nobody observed ─────────────
//
// It used to clear the pod's whole local record so the next tick "re-created"
// it — which re-ran a restartPolicy:Never pod in place. A container the
// runtime lost is down with an Unknown exit, decided like any other exit.

/// Under a restarting policy the lost container is restarted in place, and
/// the restart is counted.
#[tokio::test]
async fn a_vanished_container_is_restarted_and_counted_under_always() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod_with_policy(&store, "p1", "img", Some("node-A"), None).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    assert_eq!(count_starts(&backend.events().await), 1);

    // Container vanishes out-of-band (manual podman rm / host reboot):
    // remove drops the backend record so status() returns None.
    backend.remove(&cid).await.unwrap();
    assert!(backend.status(&cid).await.unwrap().is_none());

    kubelet.tick().await.unwrap();
    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "container re-created"
    );
    assert_eq!(backend.running_count().await, 1);
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    assert_eq!(
        container_statuses(&pod)[0]["restartCount"],
        1,
        "the lost run is a restart, and it is counted"
    );

    teardown(store, kubelet).await;
}

/// Under `Never` a lost container is not run a second time: the pod is
/// Failed, and says the container's status could not be determined.
#[tokio::test]
async fn a_vanished_never_container_fails_the_pod_instead_of_rerunning_it() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    backend.remove(&cid).await.unwrap();

    kubelet.tick().await.unwrap();
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "Never: a lost container is not re-run"
    );
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));
    let term = &container_statuses(&pod)[0]["state"]["terminated"];
    assert_eq!(term["reason"], "ContainerStatusUnknown");
    assert_eq!(term["exitCode"], 137);

    teardown(store, kubelet).await;
}

/// ★ An exit once observed is not forgotten. A `Never` pod whose container
/// exited 0 is Succeeded, and the runtime later losing the container (a
/// manual `podman rm`, a host clean-up) or reporting it UNKNOWN does not
/// make it fail. The kubelet read each poll alone, so the exit became
/// 137 / ContainerStatusUnknown and the pod Failed: a completed Job pod
/// counted as a failure. Oracle row: `status/vanished, previous status
/// Terminated: old status carried verbatim`.
#[tokio::test]
async fn a_completed_container_the_runtime_then_loses_stays_completed() {
    for lost in ["removed", "reported unknown"] {
        let store = boot_store().await;
        let backend = Arc::new(FakeBackend::new());
        let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

        put_pod(&store, "p1", "img", Some("node-A")).await;
        kubelet.tick().await.unwrap();
        let cid = first_container_id(&backend).await;
        backend.set_exit(&cid, 0).await;
        kubelet.tick().await.unwrap();
        let pod = store.get(&pod_key("p1")).await.unwrap();
        assert_eq!(pod_phase(&pod).as_deref(), Some("Succeeded"), "{lost}");

        if lost == "removed" {
            backend.remove(&cid).await.unwrap();
        } else {
            backend.set_run_state(&cid, RunState::Unknown).await;
        }
        kubelet.tick().await.unwrap();
        kubelet.tick().await.unwrap();

        let pod = store.get(&pod_key("p1")).await.unwrap();
        let term = &container_statuses(&pod)[0]["state"]["terminated"];
        assert_eq!(
            term["exitCode"], 0,
            "{lost}: the observed exit stands: {pod}"
        );
        assert_eq!(term["reason"], "Completed", "{lost}: {pod}");
        assert_eq!(pod_phase(&pod).as_deref(), Some("Succeeded"), "{lost}");
        assert_eq!(
            count_starts(&backend.events().await),
            1,
            "{lost}: not re-run"
        );

        teardown(store, kubelet).await;
    }
}

/// ★ A terminal phase is never left. The pod's stored status shows it
/// Succeeded or Failed (the API is the record: an eviction, a controller,
/// an operator wrote it) while this kubelet still holds its container; the
/// kubelet wrote the phase its fold computed over it, `Running`. Upstream
/// forces a phase the apiserver shows terminal back, whatever the
/// containers say. Oracle row: `phase/apiserver phase Succeeded is sticky
/// even though containers render unknown with restartCount 1`.
#[tokio::test]
async fn a_pod_published_terminal_is_never_moved_out_of_it() {
    for terminal in ["Succeeded", "Failed"] {
        let store = boot_store().await;
        let backend = Arc::new(FakeBackend::new());
        let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

        put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("Always")).await;
        kubelet.tick().await.unwrap();
        let mut pod = store.get(&pod_key("p1")).await.unwrap();
        assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));

        pod["status"]["phase"] = json!(terminal);
        store
            .propose(ResourceCommand::Put {
                key: pod_key("p1"),
                value: pod,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        kubelet.tick().await.unwrap();
        kubelet.tick().await.unwrap();

        let pod = store.get(&pod_key("p1")).await.unwrap();
        assert_eq!(pod_phase(&pod).as_deref(), Some(terminal), "{pod}");

        teardown(store, kubelet).await;
    }
}

// ── Test 7 — MULTI-CONTAINER: N containers → N starts, all running ───────

#[tokio::test]
async fn multi_container_pod_starts_every_container() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // 2-container pod (web + sidecar), restartPolicy:Always.
    put_multi_pod(&store, "mp", "node-A", "Always").await;
    kubelet.tick().await.unwrap();

    // TWO starts (one per container), named <ns>_<pod>_<cname>.
    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "2 containers → 2 starts"
    );
    assert_eq!(
        count_starts_named(&backend.events().await, "default_mp_web"),
        1
    );
    assert_eq!(
        count_starts_named(&backend.events().await, "default_mp_sidecar"),
        1
    );
    assert_eq!(backend.running_count().await, 2);

    // Pod status: phase Running + a 2-element containerStatuses array, both
    // running, both with containerIDs.
    let pod = store.get(&pod_key("mp")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Running"));
    let cs = container_statuses(&pod);
    assert_eq!(cs.len(), 2, "two containerStatuses");
    assert!(cs.iter().all(|c| c["state"]["running"].is_object()));
    assert!(cs.iter().all(|c| c["containerID"].is_string()));
    let names: Vec<&str> = cs.iter().filter_map(|c| c["name"].as_str()).collect();
    assert!(names.contains(&"web") && names.contains(&"sidecar"));
    assert!(
        pod_ready_is_true(&pod),
        "all-running multi-container pod is Ready"
    );

    teardown(store, kubelet).await;
}

// ── Test 8 — MULTI-CONTAINER delete → N stops + N removes ────────────────

#[tokio::test]
async fn multi_container_delete_stops_and_removes_all() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_multi_pod(&store, "mp", "node-A", "Never").await;
    kubelet.tick().await.unwrap();
    assert_eq!(backend.running_count().await, 2);

    // Hard-delete the pod → kubelet stops THEN removes BOTH containers.
    delete_pod(&store, "mp").await;
    let report = kubelet.tick().await.unwrap();
    assert_eq!(report.objects_changed, 1, "one pod cleaned up");

    let events = backend.events().await;
    assert_eq!(count_stops(&events), 2, "2 containers → 2 stops");
    assert_eq!(count_removes(&events), 2, "2 containers → 2 removes");
    assert_eq!(backend.running_count().await, 0);
    assert!(backend.containers().await.is_empty());

    // local cleared: a second tick drives zero new backend calls.
    let before = backend.events().await.len();
    kubelet.tick().await.unwrap();
    assert_eq!(backend.events().await.len(), before, "no new backend calls");

    teardown(store, kubelet).await;
}

// ── Test 9 — restartPolicy:Always re-starts an exited container ──────────

#[tokio::test]
async fn restart_policy_always_restarts_exited_container() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    // Single-container pod, restartPolicy:Always.
    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("Always")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Running")
    );

    // The container self-exits (exit 0). Under Always, the kubelet restarts
    // it on the next tick → a SECOND Start for the same container name; the
    // pod stays Running.
    backend.set_exit(&cid, 0).await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts_named(&backend.events().await, "default_p1_main"),
        2,
        "Always → container restarted (a second Start)"
    );
    assert_eq!(
        backend.running_count().await,
        1,
        "the restarted container runs"
    );
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(
        pod_phase(&pod).as_deref(),
        Some("Running"),
        "Always pod stays Running across an exit"
    );
    // restartCount bumped to 1 on the single container.
    let cs = container_statuses(&pod);
    assert_eq!(cs[0]["restartCount"], 1);

    teardown(store, kubelet).await;
}

// ── Test 10 — restartPolicy:Never latches terminal (no restart) ──────────

#[tokio::test]
async fn restart_policy_never_latches_terminal() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("Never")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    // Exit nonzero → Failed (Never never restarts).
    backend.set_exit(&cid, 5).await;
    kubelet.tick().await.unwrap();
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "Never → exactly one Start ever"
    );
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod_phase(&pod).as_deref(), Some("Failed"));

    // Steady-state: a further tick does NOT restart + stays Failed.
    kubelet.tick().await.unwrap();
    assert_eq!(count_starts(&backend.events().await), 1);
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Failed")
    );

    teardown(store, kubelet).await;
}

// ── Test 11 — restartPolicy:OnFailure restarts nonzero, latches on zero ──

#[tokio::test]
async fn restart_policy_on_failure_restarts_only_nonzero() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("OnFailure")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    // Nonzero exit under OnFailure → restart, pod stays Running.
    backend.set_exit(&cid, 1).await;
    kubelet.tick().await.unwrap();
    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "OnFailure restarts nonzero"
    );
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Running")
    );

    // The restarted container now exits ZERO → no restart → Succeeded.
    let cid2 = first_container_id(&backend).await;
    backend.set_exit(&cid2, 0).await;
    kubelet.tick().await.unwrap();
    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "OnFailure does NOT restart zero exit"
    );
    assert_eq!(
        pod_phase(&store.get(&pod_key("p1")).await.unwrap()).as_deref(),
        Some("Succeeded")
    );

    teardown(store, kubelet).await;
}

// ── Test 12 — kubectl-logs path returns the per-container fake buffer ─────

#[tokio::test]
async fn container_logs_returns_seeded_buffer() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    // Seed the exact stdout the started container will report.
    backend.seed_log("default_p1_main", "hello-engenho\n").await;
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("Never")).await;
    kubelet.tick().await.unwrap();

    // The kubelet's in-process logs path resolves the container id from local
    // bookkeeping + asks the backend.
    let logs = kubelet
        .container_logs("default", "p1", None, &LogOptions::all())
        .await
        .unwrap();
    assert_eq!(logs, "hello-engenho\n");

    // -c selecting the (only) container by name also works.
    let logs_c = kubelet
        .container_logs("default", "p1", Some("main"), &LogOptions::all())
        .await
        .unwrap();
    assert_eq!(logs_c, "hello-engenho\n");

    // A non-existent container → typed error (never empty-Ok).
    let err = kubelet
        .container_logs("default", "p1", Some("nope"), &LogOptions::all())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), "invalid_pod");

    // A pod not running on this node → typed error.
    let err2 = kubelet
        .container_logs("default", "ghost", None, &LogOptions::all())
        .await
        .unwrap_err();
    assert_eq!(err2.kind(), "invalid_pod");

    teardown(store, kubelet).await;
}

// ── Test 13 — multi-container partial start: Pending until all up ────────

#[tokio::test]
async fn multi_container_logs_select_per_container() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    backend.seed_log("default_mp_web", "from-web\n").await;
    backend
        .seed_log("default_mp_sidecar", "from-sidecar\n")
        .await;
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_multi_pod(&store, "mp", "node-A", "Always").await;
    kubelet.tick().await.unwrap();
    assert_eq!(container_id_count(&backend).await, 2);

    // -c web vs -c sidecar select the right buffers.
    let web = kubelet
        .container_logs("default", "mp", Some("web"), &LogOptions::all())
        .await
        .unwrap();
    assert_eq!(web, "from-web\n");
    let side = kubelet
        .container_logs("default", "mp", Some("sidecar"), &LogOptions::all())
        .await
        .unwrap();
    assert_eq!(side, "from-sidecar\n");

    teardown(store, kubelet).await;
}

/// **A pod whose containers ALL fail to start must still get a status.**
///
/// Regression test for a defect measured on the live daemon 2026-08-28. The
/// status reconcile was guarded on `started_any`, so when every container
/// failed to start the kubelet wrote NO status at all — no `phase`, no
/// conditions, no `containerStatuses`. The API served a pod with a `nodeName`
/// and literally nothing else.
///
/// That is worse than a wrong status: a permanent, total failure became
/// **indistinguishable from "not yet processed"** to every client. On the
/// operator's machine the real cause was the launchd agent having no `podman`
/// on PATH, so every start returned `spawn: No such file or directory` on
/// every tick for hours — and k9s showed an empty screen with no error
/// anywhere. Upstream ALWAYS reports such a pod as `Pending` with each
/// container `Waiting`.
///
/// The `FakeBackend::seed_start_failure` seam this test needs did not exist
/// either, which is precisely why the class shipped: a fake that can only
/// succeed cannot prove what happens when the runtime refuses.
#[tokio::test]
async fn total_start_failure_still_writes_pending_status() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    // The kubelet names containers `<ns>_<pod>_<cname>`; seed the failure on
    // that backend-facing name, the same convention `seed_log` uses.
    backend
        .seed_start_failure(
            "default_p1_main",
            "podman network exists spawn: No such file or directory (os error 2)",
        )
        .await;
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");

    put_pod(&store, "p1", "img", Some("node-A")).await;
    kubelet.tick().await.unwrap();

    // Nothing started — that is the premise, not the assertion.
    assert_eq!(
        backend.running_count().await,
        0,
        "premise: the seeded failure means no container started"
    );

    let pod = store.get(&pod_key("p1")).await.expect("pod still stored");
    let status = pod
        .get("status")
        .unwrap_or_else(|| panic!("pod MUST carry a status after a failed start, got: {pod}"));

    assert_eq!(
        status.get("phase").and_then(Value::as_str),
        Some("Pending"),
        "a pod whose containers cannot start is Pending, never status-less: {status}"
    );

    let cs = status
        .get("containerStatuses")
        .and_then(Value::as_array)
        .expect("containerStatuses present");
    assert_eq!(cs.len(), 1, "one container declared, one status reported");
    assert!(
        cs[0].get("state").and_then(|s| s.get("waiting")).is_some(),
        "the un-started container reports Waiting: {:#}",
        cs[0]
    );
    assert_eq!(
        cs[0].get("ready").and_then(Value::as_bool),
        Some(false),
        "an un-started container is not ready"
    );
}

// ── T2.10 — a restart reads its stop and remove results ─────────────────
//
// `restart_container` discarded both with `let _`, then started the
// replacement regardless: beside an old container the runtime had refused to
// stop, or — on the native backend — beside a process still inside its grace
// period. Each is two copies of one workload.

/// A replacement is never started beside an old container the runtime
/// refused to stop. Once the runtime stops it, the restart goes ahead.
#[tokio::test]
async fn a_restart_never_starts_a_replacement_beside_a_container_that_would_not_stop() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("OnFailure")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    backend.set_exit(&cid, 1).await;
    backend
        .seed_stop_fault(&cid, "podman stop: timed out")
        .await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "no replacement beside a container the runtime would not stop"
    );
    assert!(
        backend.status(&cid).await.unwrap().is_some(),
        "the old container was not removed out from under its failed stop"
    );

    backend.clear_stop_fault(&cid).await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "control: once it stops, the restart goes ahead"
    );
    assert_eq!(backend.status(&cid).await.unwrap(), None, "old one removed");
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(container_statuses(&pod)[0]["restartCount"], 1);

    teardown(store, kubelet).await;
}

/// A restart waits for the old process to be REAPED — the native backend
/// refuses to drop it while it is inside its grace period — and asks to be
/// re-ticked soon rather than on the next sweep.
///
/// `Always`, because the fake's `stop` rewrites the container's exit to 0;
/// the restart path under test is the same one every policy takes.
#[tokio::test]
async fn a_restart_waits_for_the_old_process_to_be_reaped() {
    use engenho_controllers::ReconcileResult;

    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");
    put_pod_with_policy(&store, "p1", "img", Some("node-A"), Some("Always")).await;
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;

    backend.set_exit(&cid, 1).await;
    backend.hold_unreaped(&cid).await;
    let outcome = kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "no replacement while the old process is unreaped"
    );
    assert!(
        matches!(
            outcome.result,
            ReconcileResult::Requeue(_) | ReconcileResult::RequeueWithProgress(_)
        ),
        "the kubelet must come back soon, got {:?}",
        outcome.result
    );

    backend.reap(&cid).await;
    kubelet.tick().await.unwrap();

    assert_eq!(
        count_starts(&backend.events().await),
        2,
        "restarted once reaped"
    );

    teardown(store, kubelet).await;
}

/// The pod's `terminationGracePeriodSeconds` reaches the runtime with the
/// container it governs — the native backend escalates on exactly this value.
#[tokio::test]
async fn the_pods_termination_grace_reaches_the_runtime() {
    let store = boot_store().await;
    let backend = Arc::new(FakeBackend::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A");
    store
        .propose(ResourceCommand::Put {
            key: pod_key("pg"),
            value: json!({
                "kind": "Pod", "apiVersion": "v1",
                "metadata": { "name": "pg" },
                "spec": {
                    "nodeName": "node-A",
                    "terminationGracePeriodSeconds": 90,
                    "containers": [{ "name": "main", "image": "img" }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    kubelet.tick().await.unwrap();
    let cid = first_container_id(&backend).await;
    let spec = backend.spec_of(&cid).await.expect("started");
    assert_eq!(spec.termination_grace.duration(), Duration::from_secs(90));

    teardown(store, kubelet).await;
}
