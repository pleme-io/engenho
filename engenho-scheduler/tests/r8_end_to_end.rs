//! R8 integration test — bootstrap a real StoreMesh, write Nodes +
//! pending Pods, run scheduler tick, assert bindings landed.
//!
//! Real openraft single-node + real ResourceCatalog + real
//! RoundRobinStrategy. The test is the architectural proof that
//! the engenho substrate hosts production K8s controllers.

mod common;

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
    let cfg = default_config("scheduler-r8").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

/// A heartbeating node that stores `Ready=True`.
async fn put_node(store: &StoreMesh, name: &str) {
    put_node_storing(store, name, json!([{ "type": "Ready", "status": "True" }])).await;
    common::put_fresh_lease(store, name).await;
}

/// Write only the Node object, with `conditions` as its stored
/// `status.conditions` (`Value::Null` omits the field). No Lease.
async fn put_node_storing(store: &StoreMesh, name: &str, conditions: Value) {
    // Nodes now advertise status.allocatable (M0.1 item 10): the
    // resource-fit predicate treats absent allocatable as zero-free, so a
    // realistic node carries cpu/memory. (The pods these tests bind
    // request NOTHING, so they'd fit even a zero-sized node — but a sized
    // node is the truthful shape + guards the predicate doesn't reject a
    // zero-request pod against a sized node.)
    let mut status = json!({
        "capacity": { "cpu": "4", "memory": "8Gi" },
        "allocatable": { "cpu": "4", "memory": "8Gi" },
    });
    if !conditions.is_null() {
        status["conditions"] = conditions;
    }
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", name),
            value: json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": { "unschedulable": false },
                "status": status,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn put_pending_pod(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": {
                    "containers": [{ "name": "main", "image": "podinfo:6" }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn pod_node_name(store: &StoreMesh, name: &str) -> Option<String> {
    let key = ResourceKey::namespaced("", "v1", "Pod", "default", name);
    let pod = store.get(&key).await?;
    pod.get("spec")
        .and_then(|s| s.get("nodeName"))
        .and_then(|n| n.as_str())
        .map(String::from)
}

#[tokio::test]
async fn scheduler_binds_single_pending_pod_to_only_available_node() {
    let store = boot_store().await;
    put_node(&store, "node-1").await;
    put_pending_pod(&store, "pending-pod").await;

    assert!(pod_node_name(&store, "pending-pod").await.is_none());

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 1);
    assert_eq!(report.unschedulable, 0);
    assert_eq!(report.bound[0].node_name, "node-1");

    assert_eq!(
        pod_node_name(&store, "pending-pod").await.as_deref(),
        Some("node-1")
    );

    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn scheduler_round_robins_across_multiple_pending_pods() {
    let store = boot_store().await;
    for n in ["node-a", "node-b", "node-c"] {
        put_node(&store, n).await;
    }
    for p in ["pod-1", "pod-2", "pod-3"] {
        put_pending_pod(&store, p).await;
    }

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    assert_eq!(report.pending_pods, 3);
    assert_eq!(report.bound.len(), 3);

    // Each pod landed on a distinct node — round-robin distribution.
    let mut node_assignments: Vec<String> = Vec::new();
    for p in ["pod-1", "pod-2", "pod-3"] {
        let n = pod_node_name(&store, p).await.expect("bound");
        node_assignments.push(n);
    }
    let mut sorted = node_assignments.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        3,
        "expected 3 distinct nodes; got {node_assignments:?}"
    );

    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn scheduler_skips_already_bound_pods() {
    let store = boot_store().await;
    put_node(&store, "node-1").await;

    // Pre-bind a pod manually.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "already-bound"),
            value: json!({
                "metadata": { "name": "already-bound" },
                "spec": { "nodeName": "node-original", "containers": [] }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Add a pending one too.
    put_pending_pod(&store, "p2").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    // Only p2 is pending; already-bound is skipped.
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 1);
    assert_eq!(report.bound[0].pod_key.name, "p2");

    // already-bound's nodeName is unchanged.
    assert_eq!(
        pod_node_name(&store, "already-bound").await.as_deref(),
        Some("node-original")
    );

    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

/// The message of the pod's `PodScheduled=False / Unschedulable` condition.
async fn unschedulable_message(store: &StoreMesh, name: &str) -> Option<String> {
    let pod = store
        .get(&ResourceKey::namespaced("", "v1", "Pod", "default", name))
        .await?;
    pod.pointer("/status/conditions")
        .and_then(Value::as_array)?
        .iter()
        .find(|c| {
            c.get("type").and_then(Value::as_str) == Some("PodScheduled")
                && c.get("reason").and_then(Value::as_str) == Some("Unschedulable")
        })?
        .get("message")
        .and_then(Value::as_str)
        .map(String::from)
}

#[tokio::test]
async fn a_pod_whose_only_node_is_cordoned_is_marked_unschedulable() {
    let store = boot_store().await;
    // Add an unschedulable node.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", "cordoned"),
            value: json!({
                "metadata": { "name": "cordoned" },
                "spec": { "unschedulable": true },
                "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    // Heartbeating, so the CORDON is what keeps the pod off it.
    common::put_fresh_lease(&store, "cordoned").await;
    put_pending_pod(&store, "lonely").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 0);
    assert_eq!(report.unschedulable, 1);

    // Pod is still unbound, and says why.
    assert!(pod_node_name(&store, "lonely").await.is_none());
    assert_eq!(
        unschedulable_message(&store, "lonely").await.as_deref(),
        Some("0/1 nodes are available: 1 node(s) were unschedulable.")
    );

    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn scheduler_namespace_filter_works() {
    let store = boot_store().await;
    put_node(&store, "node-1").await;
    put_pending_pod(&store, "default-pod").await; // namespace = "default"

    // Pod in a different namespace.
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "kube-system", "system-pod"),
            value: json!({"metadata": {"name": "system-pod"}, "spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Scheduler scoped to "default" namespace only.
    let sched = Scheduler::new(
        store.clone(),
        RoundRobinStrategy::new(),
        Some("default".into()),
    );
    let report = sched.tick().await.unwrap();
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 1);
    assert_eq!(
        report.bound[0].pod_key.namespace.as_deref(),
        Some("default")
    );

    // The kube-system pod stays pending — out of namespace.
    let system_key = ResourceKey::namespaced("", "v1", "Pod", "kube-system", "system-pod");
    let system_pod = store.get(&system_key).await.unwrap();
    assert!(system_pod.get("spec").unwrap().get("nodeName").is_none());

    drop(sched);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

// =================================================================
// T1.3d: the scheduler reads readiness through the Lease projection
// =================================================================

/// Tick once over one pending pod and return (report, where it landed).
async fn schedule_one(store: &Arc<StoreMesh>) -> (engenho_scheduler::TickReport, Option<String>) {
    put_pending_pod(store, "p").await;
    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    drop(sched);
    (report, pod_node_name(store, "p").await)
}

async fn teardown(store: Arc<StoreMesh>) {
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn a_node_whose_lease_went_stale_gets_no_pod_though_storage_says_ready() {
    // The rio failure: the kubelet wedged and the Node kept its last
    // published Ready=True. The apiserver serves Unknown for it; the
    // scheduler must not place onto it either.
    let store = boot_store().await;
    put_node_storing(
        &store,
        "wedged",
        json!([{ "type": "Ready", "status": "True" }]),
    )
    .await;
    common::put_lease(&store, "wedged", common::STALE_RENEW_TIME).await;

    let (report, landed) = schedule_one(&store).await;
    assert_eq!(landed, None, "a pod was bound to a node with a stale lease");
    assert_eq!(report.bound.len(), 0);
    assert_eq!(report.unschedulable, 1);
    assert_eq!(
        unschedulable_message(&store, "p").await.as_deref(),
        Some("0/1 nodes are available: 1 node(s) were not ready.")
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_node_with_no_conditions_and_no_lease_gets_no_pod() {
    // Was "no status yet, assume schedulable". Nobody has heard from this
    // node, so its Ready is Unknown.
    let store = boot_store().await;
    put_node_storing(&store, "silent", Value::Null).await;

    let (report, landed) = schedule_one(&store).await;
    assert_eq!(landed, None, "a pod was bound to a node never heard from");
    assert_eq!(report.unschedulable, 1);
    assert_eq!(
        unschedulable_message(&store, "p").await.as_deref(),
        Some("0/1 nodes are available: 1 node(s) were not ready.")
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_node_with_no_ready_condition_and_no_lease_gets_no_pod() {
    // Was "no Ready condition yet, assume schedulable".
    let store = boot_store().await;
    put_node_storing(
        &store,
        "silent",
        json!([{ "type": "MemoryPressure", "status": "False" }]),
    )
    .await;

    let (report, landed) = schedule_one(&store).await;
    assert_eq!(landed, None, "a pod was bound to a node never heard from");
    assert_eq!(report.unschedulable, 1);
    assert_eq!(
        unschedulable_message(&store, "p").await.as_deref(),
        Some("0/1 nodes are available: 1 node(s) were not ready.")
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_fresh_lease_makes_a_node_stored_unknown_schedulable() {
    // The shape a node registers with (Ready=Unknown, never updated). Its
    // first heartbeat makes it schedulable, whether or not the kubelet has
    // republished the Node yet: readiness is derived, not stored.
    let store = boot_store().await;
    put_node_storing(
        &store,
        "booting",
        json!([{ "type": "Ready", "status": "Unknown", "reason": "NodeStatusNeverUpdated" }]),
    )
    .await;
    common::put_fresh_lease(&store, "booting").await;

    let (report, landed) = schedule_one(&store).await;
    assert_eq!(landed.as_deref(), Some("booting"), "{report:?}");
    teardown(store).await;
}

#[tokio::test]
async fn with_a_live_node_available_the_pod_never_lands_on_the_wedged_one() {
    // Sorted first, so a scheduler reading storage would pick it.
    let store = boot_store().await;
    put_node_storing(
        &store,
        "a-wedged",
        json!([{ "type": "Ready", "status": "True" }]),
    )
    .await;
    common::put_lease(&store, "a-wedged", common::STALE_RENEW_TIME).await;
    put_node(&store, "b-live").await;

    let (report, landed) = schedule_one(&store).await;
    assert_eq!(landed.as_deref(), Some("b-live"), "{report:?}");
    teardown(store).await;
}
