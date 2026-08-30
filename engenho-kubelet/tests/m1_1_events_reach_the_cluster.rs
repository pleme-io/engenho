//! EVENTS — proving they reach the STORE, not just a collector.
//!
//! ★ WHY THE COLLECTOR TESTS WERE NOT ENOUGH. `Reason`, `Severity`,
//! `EventRecord` and `EventSink` all shipped fully tested against
//! `CollectingEventSink`, and no event ever reached a cluster, because
//! nothing constructed a sink that wrote to one and nothing called the
//! recorder from a lifecycle path. That is why diagnosing a pod stuck at
//! 149 restarts had to bypass the cluster entirely and read podman
//! directly. A cluster that cannot explain itself makes every operator an
//! engenho developer.
//!
//! Invariants:
//!   E1 starting a container emits `Started`, readable as a v1.Event
//!   E2 a backoff hold emits `BackOff` — the line that would have explained
//!      the 149 restarts — and it is a Warning, not Normal
//!   E3 the event's involvedObject names the pod, so `kubectl describe pod`
//!      can find it

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_controllers::event_recorder::{EventStore, StoreEventSink};
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

struct MeshEventStore {
    store: Arc<StoreMesh>,
}

#[async_trait::async_trait]
impl EventStore for MeshEventStore {
    async fn put_event(&self, key: ResourceKey, value: Value) -> Result<(), String> {
        self.store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Controller,
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

async fn boot(name: &str) -> (Arc<StoreMesh>, Arc<FakeBackend>, Arc<Kubelet>) {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let backend = Arc::new(FakeBackend::new());
    let sink = Arc::new(StoreEventSink::new(Arc::new(MeshEventStore {
        store: store.clone(),
    })));
    let kubelet =
        Arc::new(Kubelet::new(store.clone(), backend.clone(), "node-A").with_event_sink(sink));
    (store, backend, kubelet)
}

async fn put_pod(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "nodeName": "node-A",
                    "restartPolicy": "Always",
                    "containers": [ { "name": "app", "image": "busybox" } ]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// Every Event object in the store.
async fn events(store: &StoreMesh) -> Vec<Value> {
    store
        .list("", "v1", "Event", Some("default"))
        .await
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

fn reasons(evs: &[Value]) -> Vec<String> {
    evs.iter()
        .filter_map(|e| e["reason"].as_str().map(String::from))
        .collect()
}

#[tokio::test]
async fn e1_e2_e3_lifecycle_events_land_in_the_store() {
    let (store, backend, kubelet) = boot("events").await;
    put_pod(&store, "crasher").await;

    // E1 — start.
    kubelet.tick().await.unwrap();
    let evs = events(&store).await;
    assert!(
        reasons(&evs).contains(&"Started".to_string()),
        "a start is announced: {:?}",
        reasons(&evs)
    );

    // E3 — the involvedObject is what `kubectl describe pod` matches on.
    let started = evs
        .iter()
        .find(|e| e["reason"] == "Started")
        .expect("Started event");
    assert_eq!(started["involvedObject"]["kind"], "Pod");
    assert_eq!(started["involvedObject"]["name"], "crasher");
    assert_eq!(started["involvedObject"]["namespace"], "default");
    assert_eq!(started["apiVersion"], "v1", "a real v1.Event: {started}");

    // Crash twice: the first restart is immediate, the second is held.
    for _ in 0..2 {
        let id = backend
            .containers()
            .await
            .into_iter()
            .find(|(_, s)| s.running)
            .map(|(id, _)| id)
            .expect("a running container");
        backend.set_exit(&id, 1).await;
        kubelet.tick().await.unwrap();
    }

    // E2 — the line that would have explained 149 restarts.
    let evs = events(&store).await;
    let backoff = evs
        .iter()
        .find(|e| e["reason"] == "BackOff")
        .unwrap_or_else(|| panic!("a BackOff event: {:?}", reasons(&evs)));
    assert_eq!(
        backoff["type"], "Warning",
        "severity is derived from the reason, never passed per call site: {backoff}"
    );
    let msg = backoff["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("Back-off") && msg.contains("app"),
        "the message names the container: {msg}"
    );

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

// ── NODE LEASE ────────────────────────────────────────────────────────
//
// ★ Same shape as the events gap. `node_lease` defined the key, the object,
// the renew interval, the grace period AND the readiness derivation — and
// nothing ever WROTE a lease, so the derivation had no input and a node's
// Ready condition could never become Unknown. A Ready that cannot change is
// not a health signal, it is a constant.
//
// Invariants:
//   L1 a tick writes this node's lease into kube-node-lease
//   L2 the object is a real coordination.k8s.io/v1 Lease naming this node
//   L3 a fresh lease derives Ready, a stale one NotReady, and one that was
//      NEVER written derives Unknown — three states, not two

#[tokio::test]
async fn l1_l2_l3_the_kubelet_writes_a_lease_that_drives_readiness() {
    use engenho_kubelet::node_lease::{GRACE_PERIOD, NodeReadiness, lease_key, readiness};
    use std::time::Duration as StdDuration;

    let (store, _backend, kubelet) = boot("node-lease").await;
    kubelet.tick().await.unwrap();

    // L1 — the lease exists, at upstream's key.
    let lease = store
        .get(&lease_key("node-A"))
        .await
        .expect("the kubelet wrote its lease");

    // L2 — a real Lease, not a bag of fields.
    assert_eq!(lease["kind"], "Lease", "{lease}");
    assert_eq!(lease["apiVersion"], "coordination.k8s.io/v1", "{lease}");
    assert_eq!(lease["spec"]["holderIdentity"], "node-A", "{lease}");
    assert!(
        lease["spec"]["renewTime"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "a heartbeat carries a time: {lease}"
    );

    // L3 — the derivation this write finally feeds, and the reason the
    // write had to exist: with no lease ever written, EVERY node sat on the
    // `None` arm forever, so the other two arms were unreachable code.
    //
    // Three states, not two. `Unknown` is never-heartbeat (mid-registration)
    // and `NotReady` is heartbeat-then-stopped (failed) — collapsing them
    // makes a booting node look like a dying one, and every autoscaler
    // treats those differently.
    assert_eq!(readiness(Some(StdDuration::ZERO)), NodeReadiness::Ready);
    assert_eq!(
        readiness(Some(GRACE_PERIOD + StdDuration::from_secs(1))),
        NodeReadiness::NotReady,
        "a stale heartbeat is a FAILED node, not an unregistered one"
    );
    assert_eq!(
        readiness(None),
        NodeReadiness::Unknown,
        "never heartbeat is the mid-registration state"
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
async fn l4_the_lease_renews_on_its_interval_not_on_every_tick() {
    // ★ THIS IS A REGRESSION TEST FOR A REAL BREAK, not a hypothetical. The
    // first version of the lease wiring wrote on every tick, so every idle
    // reconcile became a store write and the revision advanced forever —
    // defeating the idempotent-skip hot-loop defense the rest of this
    // controller is built on. `deployment_status_converges_then_reconcile_
    // is_bounded` caught it, which is exactly what that tripwire is for.
    //
    // Upstream renews on RENEW_INTERVAL rather than per sync loop for the
    // same reason. The cadence is the fix; the write is not the problem.
    use engenho_kubelet::node_lease::{RENEW_INTERVAL, lease_key};
    use std::time::Duration as StdDuration;

    let router = InProcessRouter::new();
    let cfg = default_config("lease-cadence").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let sink = Arc::new(StoreEventSink::new(Arc::new(MeshEventStore {
        store: store.clone(),
    })));
    let kubelet = Arc::new(
        Kubelet::new(store.clone(), backend, "node-A")
            .with_event_sink(sink)
            .with_clock(clock.as_clock()),
    );

    kubelet.tick().await.unwrap();
    let first = store
        .get(&lease_key("node-A"))
        .await
        .expect("the first tick always renews");
    let rev_after_first = store.current_catalog().await.revision();

    // Five more ticks INSIDE the interval must write nothing at all.
    for _ in 0..5 {
        kubelet.tick().await.unwrap();
    }
    assert_eq!(
        store.current_catalog().await.revision(),
        rev_after_first,
        "ticks inside RENEW_INTERVAL advance no revision — this is the \
         idempotent-skip invariant the first version broke"
    );

    // Past the interval, it renews again.
    //
    // Judged by the REVISION, not by renewTime: that field is wall-clock
    // (an Instant has no calendar meaning, so an operator-facing timestamp
    // cannot come from the test clock), and both writes land inside the
    // same real second — so comparing timestamps would report "no write"
    // for a write that definitely happened. The revision is the honest
    // observable for "did the store change".
    clock.advance(RENEW_INTERVAL + StdDuration::from_secs(1));
    kubelet.tick().await.unwrap();
    assert!(
        store.current_catalog().await.revision() > rev_after_first,
        "past the interval the heartbeat actually beats"
    );
    assert!(
        store.get(&lease_key("node-A")).await.is_some(),
        "and the lease is still there: {first}"
    );

    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
