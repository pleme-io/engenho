//! T5.7 — the Filter stage's plugins, against a real `StoreMesh`.
//!
//! Before T5.7 the node selector and taint predicates had tests and no
//! caller: a pod with `nodeSelector: {gpu: "true"}` was bound to a
//! `gpu=false` node carrying a `NoSchedule` taint (measured 2026-09-18).
//! These tests drive `Scheduler::tick` end to end, so they fail if any plugin
//! is dropped from the Filter stage, whatever the unit tests say.

mod common;

use std::sync::Arc;
use std::time::Duration;

use engenho_scheduler::{RoundRobinStrategy, Scheduler, TickReport};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("scheduler-filter").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn teardown(store: Arc<StoreMesh>) {
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

/// A heartbeating, sized node with the given labels and taints.
async fn put_node(store: &StoreMesh, name: &str, labels: Value, taints: Value) {
    common::put_fresh_lease(store, name).await;
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::cluster_scoped("", "v1", "Node", name),
            value: json!({
                "kind": "Node",
                "apiVersion": "v1",
                "metadata": { "name": name, "labels": labels },
                "spec": { "unschedulable": false, "taints": taints },
                "status": {
                    "capacity": { "cpu": "4", "memory": "8Gi" },
                    "allocatable": { "cpu": "4", "memory": "8Gi" },
                    "conditions": [{ "type": "Ready", "status": "True" }]
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// A pending pod whose `spec` carries `extra` beside one container.
async fn put_pod(store: &StoreMesh, name: &str, extra: Value) {
    let mut spec = json!({ "containers": [{ "name": "main", "image": "podinfo:6" }] });
    if let (Some(s), Some(e)) = (spec.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            s.insert(k.clone(), v.clone());
        }
    }
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": spec,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn get_pod(store: &StoreMesh, name: &str) -> Value {
    store
        .get(&ResourceKey::namespaced("", "v1", "Pod", "default", name))
        .await
        .expect("pod exists")
}

fn bound_node(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The message of the pod's `PodScheduled=False / Unschedulable` condition.
fn unschedulable_message(pod: &Value) -> Option<&str> {
    pod.pointer("/status/conditions")
        .and_then(Value::as_array)?
        .iter()
        .find(|c| {
            c.get("type").and_then(Value::as_str) == Some("PodScheduled")
                && c.get("status").and_then(Value::as_str) == Some("False")
                && c.get("reason").and_then(Value::as_str) == Some("Unschedulable")
        })?
        .get("message")
        .and_then(Value::as_str)
}

async fn tick(store: &Arc<StoreMesh>, strategy: RoundRobinStrategy) -> TickReport {
    Scheduler::new(store.clone(), strategy, None)
        .tick()
        .await
        .unwrap()
}

fn gpu_selector() -> Value {
    json!({ "nodeSelector": { "gpu": "true" } })
}

fn no_schedule(key: &str) -> Value {
    json!([{ "key": key, "value": "reserved", "effect": "NoSchedule" }])
}

#[tokio::test]
async fn a_gpu_pod_is_not_bound_to_a_gpu_false_node() {
    let store = boot_store().await;
    put_node(&store, "plain", json!({ "gpu": "false" }), json!([])).await;
    put_pod(&store, "trainer", gpu_selector()).await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "trainer").await;
    assert_eq!(
        bound_node(&pod),
        None,
        "gpu=true pod bound to a gpu=false node"
    );
    assert!(report.bound.is_empty(), "{report:?}");
    assert_eq!(report.unschedulable, 1);
    assert_eq!(
        unschedulable_message(&pod),
        Some("0/1 nodes are available: 1 node(s) didn't match Pod's node selector."),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_gpu_pod_lands_on_the_gpu_node_whichever_is_listed_first() {
    // Positive control, and cursor-independent: the gpu node is the only
    // feasible one, so round-robin cannot land anywhere else.
    for (first, second) in [("a-plain", "b-gpu"), ("a-gpu", "b-plain")] {
        let store = boot_store().await;
        for name in [first, second] {
            let gpu = if name.ends_with("gpu") {
                "true"
            } else {
                "false"
            };
            put_node(&store, name, json!({ "gpu": gpu }), json!([])).await;
        }
        put_pod(&store, "trainer", gpu_selector()).await;

        let report = tick(&store, RoundRobinStrategy::new()).await;
        let want = if first.ends_with("gpu") {
            first
        } else {
            second
        };
        assert_eq!(report.bound.len(), 1, "{report:?}");
        assert_eq!(bound_node(&get_pod(&store, "trainer").await), Some(want));
        teardown(store).await;
    }
}

#[tokio::test]
async fn a_no_schedule_taint_without_a_toleration_blocks_placement() {
    let store = boot_store().await;
    put_node(&store, "db-only", json!({}), no_schedule("dedicated")).await;
    put_pod(&store, "web", json!({})).await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "web").await;
    assert_eq!(
        bound_node(&pod),
        None,
        "pod bound past an untolerated NoSchedule taint"
    );
    assert_eq!(report.unschedulable, 1, "{report:?}");
    assert_eq!(
        unschedulable_message(&pod),
        Some("0/1 nodes are available: 1 node(s) had untolerated taint {dedicated}."),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn a_matching_toleration_admits_the_pod_onto_the_tainted_node() {
    let store = boot_store().await;
    put_node(&store, "db-only", json!({}), no_schedule("dedicated")).await;
    put_pod(
        &store,
        "db",
        json!({ "tolerations": [
            { "key": "dedicated", "operator": "Equal", "value": "reserved", "effect": "NoSchedule" }
        ] }),
    )
    .await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    assert_eq!(report.bound.len(), 1, "{report:?}");
    assert_eq!(bound_node(&get_pod(&store, "db").await), Some("db-only"));
    teardown(store).await;
}

#[tokio::test]
async fn the_measured_incident_a_gpu_pod_and_a_tainted_gpu_false_node() {
    // 2026-09-18: gpu=true bound to a gpu=false node carrying NoSchedule.
    // Either plugin alone excludes it; the selector is reported, being first.
    let store = boot_store().await;
    put_node(
        &store,
        "cpu-box",
        json!({ "gpu": "false" }),
        no_schedule("batch"),
    )
    .await;
    put_pod(&store, "trainer", gpu_selector()).await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let pod = get_pod(&store, "trainer").await;
    assert_eq!(bound_node(&pod), None, "{report:?}");
    assert!(
        unschedulable_message(&pod).is_some_and(|m| m.contains("node selector")),
        "{pod:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn with_no_node_observed_nothing_is_written() {
    // A cluster before its kubelet registers: there is no reason to report,
    // only an absence, so the pod is not touched.
    let store = boot_store().await;
    put_pod(&store, "early", json!({})).await;
    let before = get_pod(&store, "early").await;

    let report = tick(&store, RoundRobinStrategy::new()).await;
    let after = get_pod(&store, "early").await;
    assert_eq!(report.pending_pods, 1);
    assert_eq!(report.no_nodes_observed, 1, "{report:?}");
    assert_eq!(report.unschedulable, 0, "{report:?}");
    assert_eq!(
        after, before,
        "a pod was written with no node observed: {after:#}"
    );
    teardown(store).await;
}

#[tokio::test]
async fn an_unschedulable_pod_is_not_rewritten_while_its_reason_is_unchanged() {
    // The scheduler watches Pods: rewriting an identical condition each tick
    // would wake it on its own write.
    let store = boot_store().await;
    put_node(&store, "plain", json!({ "gpu": "false" }), json!([])).await;
    put_pod(&store, "trainer", gpu_selector()).await;

    let first = tick(&store, RoundRobinStrategy::new()).await;
    assert_eq!(first.unschedulable_written, 1, "{first:?}");
    let marked = get_pod(&store, "trainer").await;

    let second = tick(&store, RoundRobinStrategy::new()).await;
    assert_eq!(second.unschedulable, 1, "{second:?}");
    assert_eq!(second.unschedulable_written, 0, "{second:?}");
    assert_eq!(
        get_pod(&store, "trainer").await,
        marked,
        "the identical condition was written again"
    );

    // A changed reason is written: a second node, also rejected.
    put_node(
        &store,
        "tainted",
        json!({ "gpu": "true" }),
        no_schedule("dedicated"),
    )
    .await;
    let third = tick(&store, RoundRobinStrategy::new()).await;
    assert_eq!(third.unschedulable_written, 1, "{third:?}");
    assert_eq!(
        unschedulable_message(&get_pod(&store, "trainer").await),
        Some(
            "0/2 nodes are available: 1 node(s) didn't match Pod's node selector, \
             1 node(s) had untolerated taint {dedicated}."
        )
    );
    teardown(store).await;
}
