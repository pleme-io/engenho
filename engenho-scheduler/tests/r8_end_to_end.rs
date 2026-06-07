//! R8 integration test — bootstrap a real StoreMesh, write Nodes +
//! pending Pods, run scheduler tick, assert bindings landed.
//!
//! Real openraft single-node + real ResourceCatalog + real
//! RoundRobinStrategy. The test is the architectural proof that
//! the engenho substrate hosts production K8s controllers.

use std::sync::Arc;
use std::time::Duration;

use engenho_scheduler::{RoundRobinStrategy, Scheduler};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

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

async fn put_node(store: &StoreMesh, name: &str) {
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", name),
            value: json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": { "unschedulable": false },
                "status": {
                    "conditions": [{ "type": "Ready", "status": "True" }]
                }
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
    assert_eq!(report.skipped_no_node, 0);
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

#[tokio::test]
async fn scheduler_reports_skipped_when_no_schedulable_nodes() {
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
    put_pending_pod(&store, "lonely").await;

    let sched = Scheduler::new(store.clone(), RoundRobinStrategy::new(), None);
    let report = sched.tick().await.unwrap();
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.bound.len(), 0);
    assert_eq!(report.skipped_no_node, 1);

    // Pod is still unbound.
    assert!(pod_node_name(&store, "lonely").await.is_none());

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
