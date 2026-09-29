//! A start's backoff belongs to the POD, not to its name — and a held start
//! says so in the pod's status.
//!
//! Measured on a native node while recovering from a garbage-collected image:
//! the stale pods were deleted by hand, their `DaemonSet`s recreated pods
//! under the SAME names (`{ds}-{node}`), six started at once, and two sat
//! `ContainerCreating` with no log line for about four and a half minutes
//! before starting together. The start curve was keyed by `namespace/name`,
//! so each brand-new pod inherited the dead pod's thousands of consecutive
//! failures, and its first start waited out the curve's five-minute cap.
//!
//! Upstream keys container backoff by pod UID and container name. A pod
//! recreated under the same name is a new pod with a new UID, and its first
//! start is immediate.
//!
//! The second half: a start held on the curve rendered as a bare
//! `ContainerCreating` and logged only at debug, so nothing told an operator
//! that the kubelet was waiting on purpose, or for how long. Upstream reports
//! a held container as `Waiting{CrashLoopBackOff}` with a
//! `back-off <duration> restarting failed container=<c> pod=<p>` message.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

const CONTAINER: &str = "default_home_app";

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
    ResourceKey::namespaced("", "v1", "Pod", "default", "home")
}

async fn put_pod(store: &StoreMesh) {
    store
        .propose(ResourceCommand::Put {
            key: pod_key(),
            value: json!({
                "kind": "Pod", "apiVersion": "v1",
                "metadata": {"name": "home"},
                "spec": {"nodeName": "node-A", "restartPolicy": "Always",
                         "containers": [{"name": "app", "image": "app"}]},
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn pod(store: &StoreMesh) -> Value {
    store.get(&pod_key()).await.expect("pod present")
}

fn waiting(p: &Value) -> (Option<String>, Option<String>) {
    let w = p.pointer("/status/containerStatuses/0/state/waiting");
    (
        w.and_then(|w| w.get("reason"))
            .and_then(Value::as_str)
            .map(String::from),
        w.and_then(|w| w.get("message"))
            .and_then(Value::as_str)
            .map(String::from),
    )
}

/// Fail the pod's start for ten virtual minutes: several failures, and a
/// curve that now owes minutes.
async fn fail_for_ten_minutes(kubelet: &Kubelet, clock: &TestClock) {
    for _ in 0..600 {
        kubelet.tick().await.unwrap();
        clock.advance(Duration::from_secs(1));
    }
}

#[tokio::test]
async fn a_pod_recreated_under_the_same_name_starts_without_the_old_pods_backoff() {
    let store = boot_store("backoff-per-uid").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure(CONTAINER, "closure is not in the store")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store).await;
    fail_for_ten_minutes(&kubelet, &clock).await;
    let failed_attempts = backend.start_attempts(CONTAINER).await;
    assert!(
        failed_attempts >= 5,
        "premise: a long streak ({failed_attempts})"
    );

    // The cause is gone, and the pod is deleted and recreated under the same
    // name before the kubelet ticks again — what a DaemonSet does.
    backend.clear_start_failure(CONTAINER).await;
    let old_uid = pod(&store).await.pointer("/metadata/uid").cloned();
    store
        .propose(ResourceCommand::delete(pod_key(), Reason::Operator))
        .await
        .unwrap();
    put_pod(&store).await;
    assert_ne!(
        pod(&store).await.pointer("/metadata/uid").cloned(),
        old_uid,
        "premise: a new pod"
    );

    kubelet.tick().await.unwrap();
    assert_eq!(
        backend.start_attempts(CONTAINER).await,
        failed_attempts + 1,
        "the new pod's first start is immediate: it owes nothing"
    );
    let p = pod(&store).await;
    assert!(
        p.pointer("/status/containerStatuses/0/state/running")
            .is_some(),
        "{p}"
    );
}

#[tokio::test]
async fn a_held_start_says_it_is_backing_off_and_for_how_long() {
    let store = boot_store("backoff-says-so").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure(CONTAINER, "exec format error")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store).await;

    kubelet.tick().await.unwrap(); // the first start fails
    kubelet.tick().await.unwrap(); // held: the curve owes 10s
    let p = pod(&store).await;
    let (reason, message) = waiting(&p);
    assert_eq!(reason.as_deref(), Some("CrashLoopBackOff"), "{p}");
    let message = message.unwrap_or_default();
    assert!(
        message.starts_with("back-off 10s restarting failed container=app pod=home"),
        "the wait and whose it is: {message:?}"
    );
    assert_eq!(
        p.pointer("/status/phase").and_then(Value::as_str),
        Some("Pending"),
        "a container that never started keeps the pod Pending"
    );
}
