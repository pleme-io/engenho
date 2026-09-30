//! Fixtures shared by the scheduler's store-backed integration tests.
//!
//! The scheduler derives a Node's `Ready` from its Lease, so a node a test
//! means to be schedulable needs a heartbeat as well as a Node object.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::node_lease::{lease_key, lease_value};
use engenho_scheduler::{RoundRobinStrategy, Scheduler, TickReport};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

/// A `renewTime` far past the grace period: the node stopped heartbeating.
#[allow(dead_code)] // Each test binary compiles this module; not all use every item.
pub const STALE_RENEW_TIME: &str = "2020-01-01T00:00:00Z";

/// Write `node`'s Lease with the given `renewTime`.
pub async fn put_lease(store: &StoreMesh, node: &str, renew_time: &str) {
    store
        .propose(ResourceCommand::Put {
            key: lease_key(node),
            value: lease_value(node, renew_time, 0),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// Write `node`'s Lease renewed just now.
pub async fn put_fresh_lease(store: &StoreMesh, node: &str) {
    put_lease(store, node, &engenho_types::time::now_rfc3339_utc()).await;
}

pub async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("scheduler-store-fixture").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

pub async fn teardown(store: Arc<StoreMesh>) {
    let mesh = Arc::try_unwrap(store).ok().expect("only owner left");
    mesh.terminate().await.unwrap();
}

/// A heartbeating, sized node with the given labels and taints.
pub async fn put_node(store: &StoreMesh, name: &str, labels: Value, taints: Value) {
    put_fresh_lease(store, name).await;
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

pub async fn get_pod(store: &StoreMesh, name: &str) -> Value {
    store
        .get(&ResourceKey::namespaced("", "v1", "Pod", "default", name))
        .await
        .expect("pod exists")
}

pub fn bound_node(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The message of the pod's `PodScheduled=False / Unschedulable` condition.
pub fn unschedulable_message(pod: &Value) -> Option<&str> {
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

pub async fn tick(store: &Arc<StoreMesh>, strategy: RoundRobinStrategy) -> TickReport {
    Scheduler::new(store.clone(), strategy, None)
        .tick()
        .await
        .unwrap()
}
