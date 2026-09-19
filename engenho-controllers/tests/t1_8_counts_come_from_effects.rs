//! T1.8 — a reported change is a write that landed.
//!
//! Before T1.8 every count site proposed a command, `?`-ed the transport
//! error and added one, without reading the `ResourceOp` the store returned.
//! These tests drive a real in-memory store and datapath doubles to the two
//! cases that difference is about:
//!
//! * **E1** a write the store answers `NoOp` (nothing changed) is not a change;
//! * **E2** a datapath removal the backend refused is not a change.
//!
//! Each has a positive control in the same test, so a controller that stops
//! counting altogether fails too.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_controllers::network_policy::{
    Direction, FakeNetworkPolicyEnforcer, NetworkPolicyEnforcer, NetworkPolicyError,
    NetworkPolicyRule, PolicyDatapath,
};
use engenho_controllers::network_policy_controller::NetworkPolicyController;
use engenho_controllers::{Controller, GcController, OwnerReference, set_owner_reference};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(tag: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(tag).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// A pod controlled by a ReplicaSet that was never stored: an orphan.
fn orphan_pod(name: &str, finalizers: &[&str]) -> Value {
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name, "finalizers": finalizers },
        "spec": { "containers": [{ "name": "c", "image": "x" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "gone".into(),
            uid: "uid-never-stored".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    pod
}

/// E1. gc deletes orphans with no clock. For an orphan that carries a
/// finalizer the store has no deletionTimestamp to stamp, so it leaves the
/// object exactly as it was and answers `NoOp`. That delete used to count
/// as a change, on every tick, for as long as the finalizer stayed.
#[tokio::test]
async fn a_write_the_store_answered_noop_is_not_a_change() {
    let store = boot("t1-8-gc-noop").await;
    let held = ResourceKey::namespaced("", "v1", "Pod", "ns", "held");
    let free = ResourceKey::namespaced("", "v1", "Pod", "ns", "free");
    put(&store, &held, orphan_pod("held", &["example.com/hold"])).await;
    put(&store, &free, orphan_pod("free", &[])).await;
    let held_before = store.get(&held).await.expect("held");

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(outcome.report.objects_examined, 2);
    assert!(
        store.get(&free).await.is_none(),
        "positive control: the finalizer-free orphan was deleted"
    );
    assert_eq!(
        store.get(&held).await.expect("held survives"),
        held_before,
        "the store changed nothing on the finalizer-bearing orphan"
    );
    assert_eq!(
        outcome.report.objects_changed, 1,
        "only the delete that landed is a change; the NoOp is not"
    );

    // And the next tick, with only the held orphan left, reports nothing.
    let again = gc.tick().await.expect("gc tick");
    assert_eq!(
        again.report.objects_changed, 0,
        "a NoOp repeated every tick is not a change every tick"
    );
}

fn stale_rule() -> NetworkPolicyRule {
    NetworkPolicyRule {
        policy_id: "ns/deleted#0".into(),
        pod_selector: std::collections::BTreeMap::new(),
        direction: Direction::Ingress,
        allowed_peers: Vec::new(),
        allowed_ports: Vec::new(),
    }
}

/// An enforcer still holding a deleted policy's rule, which refuses to
/// remove it.
struct Stuck;

#[async_trait]
impl NetworkPolicyEnforcer for Stuck {
    fn name(&self) -> &'static str {
        "stuck"
    }
    fn datapath(&self) -> PolicyDatapath {
        PolicyDatapath::Computed
    }
    async fn upsert(&self, _rule: &NetworkPolicyRule) -> Result<(), NetworkPolicyError> {
        Ok(())
    }
    async fn remove(&self, _policy_id: &str) -> Result<(), NetworkPolicyError> {
        Err(NetworkPolicyError::Backend("rule busy".into()))
    }
    async fn list(&self) -> Result<Vec<NetworkPolicyRule>, NetworkPolicyError> {
        Ok(vec![stale_rule()])
    }
}

/// E2. The reap of a deleted policy's rule used to be
/// `let _ = remove(..); reaped += 1`: a removal the enforcer refused was
/// reported as a change, while the filter stayed installed.
#[tokio::test]
async fn a_backend_removal_that_failed_is_not_a_change() {
    let store = boot("t1-8-np-reap").await;

    // Positive control: an enforcer that does remove the stale rule.
    let working = Arc::new(FakeNetworkPolicyEnforcer::new());
    working.upsert(&stale_rule()).await.unwrap();
    let c = NetworkPolicyController::new(store.clone(), working.clone());
    let reaped = c.tick().await.expect("tick");
    assert_eq!(working.rule_count().await, 0, "the stale rule was removed");
    assert_eq!(
        reaped.report.objects_changed, 1,
        "a removal that landed is a change"
    );

    // The enforcer refuses: the rule is still there, and nothing changed.
    let c = NetworkPolicyController::new(store.clone(), Arc::new(Stuck));
    let refused = c
        .tick()
        .await
        .expect("a refused removal does not fail the tick");
    assert_eq!(
        refused.report.objects_changed, 0,
        "a removal the enforcer refused is not a change"
    );
}
