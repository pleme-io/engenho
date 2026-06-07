//! M0.1 item 10 — resource-fit predicate integration tests against a
//! real `StoreMesh`. The Filter stage runs in FRONT of the RoundRobin
//! Score stage: a Pod is bound only to a node that fits its cpu/memory
//! requests, never overcommitting (across-pod OR within a single tick),
//! and a Pod that fits nowhere stays Pending with a typed
//! `PodScheduled=False / reason=Unschedulable` status.
//!
//! Boot pattern copied from `r8_end_to_end.rs` (real openraft single-node
//! + real ResourceCatalog + real RoundRobinStrategy).

use std::sync::Arc;
use std::time::Duration;

use engenho_scheduler::{RoundRobinStrategy, Scheduler};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("scheduler-fit").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

/// Put a Ready, schedulable node with the given cpu/memory allocatable.
async fn put_sized_node(store: &StoreMesh, name: &str, cpu: &str, memory: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", name),
            value: json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": { "unschedulable": false },
                "status": {
                    "capacity": { "cpu": cpu, "memory": memory },
                    "allocatable": { "cpu": cpu, "memory": memory },
                    "conditions": [{ "type": "Ready", "status": "True" }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// Put a pending pod requesting `cpu`/`memory` on a single container.
async fn put_requesting_pod(store: &StoreMesh, name: &str, cpu: &str, memory: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "containers": [{
                        "name": "main",
                        "image": "podinfo:6",
                        "resources": { "requests": { "cpu": cpu, "memory": memory } }
                    }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// Put a pod already bound to `node` requesting `cpu`/`memory`.
async fn put_bound_pod(store: &StoreMesh, name: &str, node: &str, cpu: &str, memory: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "nodeName": node,
                    "containers": [{
                        "name": "main",
                        "image": "podinfo:6",
                        "resources": { "requests": { "cpu": cpu, "memory": memory } }
                    }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn get_pod(store: &StoreMesh, name: &str) -> Option<Value> {
    let key = ResourceKey::namespaced("", "v1", "Pod", "default", name);
    store.get(&key).await
}

fn node_name_of_pod(pod: &Value) -> Option<String> {
    pod.get("spec")
        .and_then(|s| s.get("nodeName"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// True if the pod carries a `PodScheduled=False / reason=Unschedulable`
/// status condition.
fn is_unschedulable(pod: &Value) -> bool {
    pod.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("PodScheduled")
                    && c.get("status").and_then(|s| s.as_str()) == Some("False")
                    && c.get("reason").and_then(|r| r.as_str()) == Some("Unschedulable")
            })
        })
}

async fn teardown(store: Arc<StoreMesh>, sched: Scheduler) {
    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn over_request_pod_stays_pending_with_unschedulable_status() {
    let store = boot_store().await;
    // Single node: 1 core, 1Gi.
    put_sized_node(&store, "node-1", "1", "1Gi").await;
    // Pod requests 4 cores — cannot fit.
    put_requesting_pod(&store, "greedy", "4", "256Mi").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();

    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 0, "over-request pod must NOT bind");
    assert_eq!(report.unschedulable_no_fit, 1);
    assert_eq!(
        report.skipped_no_node, 0,
        "node exists, it just doesn't fit"
    );

    let pod = get_pod(&store, "greedy").await.unwrap();
    assert!(
        node_name_of_pod(&pod).is_none(),
        "greedy pod's spec.nodeName must stay absent"
    );
    assert!(
        is_unschedulable(&pod),
        "greedy pod must carry PodScheduled=False/Unschedulable; got {pod:#}"
    );

    teardown(store, sched).await;
}

#[tokio::test]
async fn pod_binds_only_to_a_fitting_node() {
    // node-small (500m) is filtered OUT by the predicate; only node-big
    // fits a 2-core request. The fitting candidate set handed to the
    // strategy is therefore `[node-big]` (size 1), so the round-robin
    // cursor (whatever its value) lands on node-big — cursor luck cannot
    // produce a wrong answer because the only fitting node is unique. We
    // additionally seed the node set in BOTH orderings to prove insertion
    // order doesn't matter either.
    for (a, ca, ma, b, cb, mb) in [
        ("node-small", "500m", "8Gi", "node-big", "4", "8Gi"),
        ("node-big", "4", "8Gi", "node-small", "500m", "8Gi"),
    ] {
        let store = boot_store().await;
        put_sized_node(&store, a, ca, ma).await;
        put_sized_node(&store, b, cb, mb).await;
        put_requesting_pod(&store, "needs-two-cores", "2", "256Mi").await;

        let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
        let report = sched.tick().await.unwrap();
        assert_eq!(report.bound.len(), 1, "exactly one bind");
        assert_eq!(
            report.bound[0].node_name, "node-big",
            "pod requesting 2 cores must land on node-big, never node-small"
        );

        let pod = get_pod(&store, "needs-two-cores").await.unwrap();
        assert_eq!(node_name_of_pod(&pod).as_deref(), Some("node-big"));

        teardown(store, sched).await;
    }
}

#[tokio::test]
async fn full_node_is_skipped_pod_goes_elsewhere() {
    let store = boot_store().await;
    // node-A: 1 core, already fully consumed by a bound pod (free = 0).
    put_sized_node(&store, "node-A", "1", "2Gi").await;
    put_bound_pod(&store, "occupant", "node-A", "1", "512Mi").await;
    // node-B: 1 core, free.
    put_sized_node(&store, "node-B", "1", "2Gi").await;
    // New pending pod needs a full core.
    put_requesting_pod(&store, "newcomer", "1", "256Mi").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();

    assert_eq!(report.bound.len(), 1);
    assert_eq!(
        report.bound[0].node_name, "node-B",
        "newcomer must bind to node-B (node-A is full), not overcommit node-A"
    );

    let pod = get_pod(&store, "newcomer").await.unwrap();
    assert_eq!(node_name_of_pod(&pod).as_deref(), Some("node-B"));
    // occupant is untouched.
    let occ = get_pod(&store, "occupant").await.unwrap();
    assert_eq!(node_name_of_pod(&occ).as_deref(), Some("node-A"));

    teardown(store, sched).await;
}

#[tokio::test]
async fn no_within_tick_overcommit_of_a_single_node() {
    let store = boot_store().await;
    // One node, exactly 1 core of room.
    put_sized_node(&store, "node-1", "1", "4Gi").await;
    // TWO pending pods, each needing a full core — only ONE can fit.
    put_requesting_pod(&store, "pod-a", "1", "256Mi").await;
    put_requesting_pod(&store, "pod-b", "1", "256Mi").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();

    assert_eq!(report.pending_pods, 2);
    assert_eq!(
        report.bound.len(),
        1,
        "exactly ONE pod binds; the running free-map must decrement within the tick"
    );
    assert_eq!(
        report.unschedulable_no_fit, 1,
        "the other pod is Unschedulable (no room left after the first bind)"
    );

    // Exactly one of the two pods is bound; the other is Unschedulable.
    let a = get_pod(&store, "pod-a").await.unwrap();
    let b = get_pod(&store, "pod-b").await.unwrap();
    let a_bound = node_name_of_pod(&a).is_some();
    let b_bound = node_name_of_pod(&b).is_some();
    assert!(
        a_bound ^ b_bound,
        "exactly one pod bound; a_bound={a_bound} b_bound={b_bound}"
    );
    // The unbound one carries the typed Unschedulable status.
    let unbound = if a_bound { &b } else { &a };
    assert!(
        is_unschedulable(unbound),
        "the unbound pod must carry PodScheduled=False/Unschedulable"
    );

    teardown(store, sched).await;
}
