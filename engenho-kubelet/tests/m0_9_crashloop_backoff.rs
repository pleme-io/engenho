//! CRASHLOOP BACKOFF — proving the DECISION is wired into the LOOP.
//!
//! ★ WHY THIS FILE EXISTS SEPARATELY FROM `backoff.rs`'s unit tests.
//! `backoff::decide` was correct and fully tested from the day it landed,
//! and the kubelet restarted a failing container on EVERY tick anyway,
//! because nothing called it. A tested pure function that no loop consults
//! is not a feature — it is a well-tested opinion. These tests exercise the
//! reconcile path, so they fail if the call is ever removed.
//!
//! This is the class behind the pod measured at 149 restarts on cid with
//! nothing in the cluster able to explain why.
//!
//! Invariants:
//!   B1 the FIRST restart is immediate — backoff answers repetition, not one exit
//!   B2 the SECOND exit is HELD, and the container is not restarted
//!   B3 the hold ends exactly when the owed delay elapses
//!   B4 a held container publishes `CrashLoopBackOff`, and the pod stays
//!      Running — not Pending, which would read as "still pulling the image"
//!   B5 a container that stayed up past RESET_AFTER restarts immediately,
//!      never inheriting an old crash's penalty
//!   B6 restartPolicy:Never never restarts and never backs off

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::backend::FakeEvent;
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

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

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

async fn put_pod(store: &StoreMesh, name: &str, restart_policy: &str) {
    let value = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": {
            "nodeName": "node-A",
            "restartPolicy": restart_policy,
            "containers": [ { "name": "app", "image": "busybox" } ]
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

fn starts(events: &[FakeEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(_)))
        .count()
}

async fn pod(store: &StoreMesh, name: &str) -> Value {
    store.get(&pod_key(name)).await.expect("pod present")
}

fn phase(p: &Value) -> Option<String> {
    p.get("status")?.get("phase")?.as_str().map(String::from)
}

fn waiting_reason(p: &Value) -> Option<String> {
    p.get("status")?
        .get("containerStatuses")?
        .as_array()?
        .first()?
        .get("state")?
        .get("waiting")?
        .get("reason")?
        .as_str()
        .map(String::from)
}

/// The live container id, so a test can crash whatever is currently up.
async fn live_id(backend: &FakeBackend) -> String {
    backend
        .containers()
        .await
        .into_iter()
        .find(|(_, s)| s.running)
        .map(|(id, _)| id)
        .expect("a running container")
}

/// Crash the running container and reconcile once.
async fn crash_and_tick(backend: &FakeBackend, kubelet: &Kubelet) {
    let id = live_id(backend).await;
    backend.set_exit(&id, 1).await;
    kubelet.tick().await.unwrap();
}

#[tokio::test]
async fn b1_b2_b3_the_first_restart_is_immediate_and_the_second_is_held() {
    let store = boot_store("backoff-curve").await;
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    let _key = pod_key("crasher");
    put_pod(&store, "crasher", "Always").await;

    // Start.
    kubelet.tick().await.unwrap();
    assert_eq!(starts(&backend.events().await), 1, "initial start");

    // B1 — first exit restarts immediately (restart_count was 0).
    crash_and_tick(&backend, &kubelet).await;
    assert_eq!(
        starts(&backend.events().await),
        2,
        "the FIRST restart is immediate: backoff answers repetition, not a single exit"
    );

    // B2 — second exit is held. Ticking repeatedly must not start anything:
    // this is precisely the hot loop the wiring exists to stop.
    crash_and_tick(&backend, &kubelet).await;
    for _ in 0..5 {
        kubelet.tick().await.unwrap();
    }
    assert_eq!(
        starts(&backend.events().await),
        2,
        "five ticks inside the hold started nothing"
    );

    // B3 — the hold ends when the owed delay (10s after 1 restart) elapses.
    clock.advance(Duration::from_secs(9));
    kubelet.tick().await.unwrap();
    assert_eq!(starts(&backend.events().await), 2, "still held at 9s");

    clock.advance(Duration::from_secs(2));
    kubelet.tick().await.unwrap();
    assert_eq!(
        starts(&backend.events().await),
        3,
        "restarted once the 10s delay elapsed"
    );

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn b4_a_held_container_says_crashloopbackoff_and_the_pod_stays_running() {
    let store = boot_store("backoff-status").await;
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    let _key = pod_key("crasher");
    put_pod(&store, "crasher", "Always").await;

    kubelet.tick().await.unwrap();
    crash_and_tick(&backend, &kubelet).await; // immediate restart
    crash_and_tick(&backend, &kubelet).await; // now held

    let p = pod(&store, "crasher").await;
    assert_eq!(
        waiting_reason(&p).as_deref(),
        Some("CrashLoopBackOff"),
        "the exact upstream string every alerting rule matches: {p}"
    );
    // ★ Running, NOT Pending. A crash-looping pod reported Pending is
    // indistinguishable from one still pulling its image — the opposite
    // diagnosis, from the same screen.
    assert_eq!(phase(&p).as_deref(), Some("Running"), "{p}");

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn b5_a_container_that_stayed_up_long_enough_restarts_immediately() {
    let store = boot_store("backoff-reset").await;
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    let _key = pod_key("crasher");
    put_pod(&store, "crasher", "Always").await;

    kubelet.tick().await.unwrap();
    crash_and_tick(&backend, &kubelet).await; // restart 1, immediate
    assert_eq!(starts(&backend.events().await), 2);

    // The replacement stays up well past RESET_AFTER (600s), then dies.
    clock.advance(Duration::from_secs(700));
    crash_and_tick(&backend, &kubelet).await;

    assert_eq!(
        starts(&backend.events().await),
        3,
        "a container that earned a clean slate is not penalised for an old crash"
    );

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn b6_restart_policy_never_neither_restarts_nor_backs_off() {
    let store = boot_store("backoff-never").await;
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    let _key = pod_key("oneshot");
    put_pod(&store, "oneshot", "Never").await;

    kubelet.tick().await.unwrap();
    crash_and_tick(&backend, &kubelet).await;
    kubelet.tick().await.unwrap();

    assert_eq!(starts(&backend.events().await), 1, "never restarted");
    let p = pod(&store, "oneshot").await;
    // Terminal, and NOT wearing a backoff label — a pod that will never be
    // retried must not look like one that is merely waiting its turn.
    assert_ne!(
        waiting_reason(&p).as_deref(),
        Some("CrashLoopBackOff"),
        "{p}"
    );
    assert_eq!(phase(&p).as_deref(), Some("Failed"), "{p}");

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
