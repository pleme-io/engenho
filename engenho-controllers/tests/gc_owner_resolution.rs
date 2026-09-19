//! The gc controller resolves a dependent's owner by that owner's OWN
//! apiVersion + kind, and fails closed.
//!
//! Measured on ryn 2026-09-18: `pitr-lab/mysql-0`, owned by a StatefulSet,
//! was deleted ~10 times per SECOND for days, because gc built its set of
//! live owner UIDs from two hardcoded kinds — Deployment and ReplicaSet —
//! and read "uid not in my set" as "orphan". The statefulset controller
//! recreated the pod, the scheduler bound it, gc deleted it again. Three
//! controllers ran flat out, each reporting `changed=1` every tick, and
//! the resulting event storm overflowed the kubelet's watch buffer.
//!
//! These tests pin the rule in both directions: an owner gc was never
//! taught about keeps its dependent, and a genuinely absent owner still
//! loses it.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, GcController, OwnerReference, set_owner_reference};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store(tag: &str) -> Arc<StoreMesh> {
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

async fn put(store: &StoreMesh, key: &ResourceKey, value: Value) -> Value {
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store.get(key).await.expect("stored")
}

fn uid_of(v: &Value) -> String {
    v.get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("uid assigned")
        .to_string()
}

/// A StatefulSet is a kind gc never enumerated. Its pod must survive.
#[tokio::test]
async fn a_pod_owned_by_a_statefulset_is_not_an_orphan() {
    let store = boot_store("gc-sts").await;

    let sts_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "pitr-lab", "mysql");
    let sts = put(
        &store,
        &sts_key,
        json!({
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": { "name": "mysql" },
            "spec": { "replicas": 1 }
        }),
    )
    .await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "mysql-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "mysql-0" },
        "spec": { "containers": [{ "name": "mysql", "image": "mysql:8.0" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "mysql".into(),
            uid: uid_of(&sts),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 0,
        "gc deleted a StatefulSet's pod as an orphan; that is the ryn busy-loop"
    );
    assert!(
        store.get(&pod_key).await.is_some(),
        "mysql-0 must survive a gc tick while its StatefulSet exists"
    );
}

/// The other direction, so the guard above is not vacuous: an owner that
/// is genuinely gone still orphans its dependent.
#[tokio::test]
async fn a_pod_whose_owner_does_not_exist_is_still_collected() {
    let store = boot_store("gc-absent").await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "ghost-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "ghost-0" },
        "spec": { "containers": [{ "name": "c", "image": "x" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "never-existed".into(),
            uid: "uid-that-was-never-stored".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 1,
        "an absent owner must still orphan its dependent"
    );
}

/// An owner recreated under the same name is a different object, and the
/// dependent belongs to the generation that died.
#[tokio::test]
async fn a_uid_mismatch_on_a_live_name_still_orphans() {
    let store = boot_store("gc-uid").await;

    let sts_key = ResourceKey::namespaced("apps", "v1", "StatefulSet", "pitr-lab", "mysql");
    put(
        &store,
        &sts_key,
        json!({
            "kind": "StatefulSet",
            "apiVersion": "apps/v1",
            "metadata": { "name": "mysql" },
            "spec": { "replicas": 1 }
        }),
    )
    .await;

    let pod_key = ResourceKey::namespaced("", "v1", "Pod", "pitr-lab", "mysql-0");
    let mut pod = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": "mysql-0" },
        "spec": { "containers": [{ "name": "mysql", "image": "mysql:8.0" }] }
    });
    set_owner_reference(
        &mut pod,
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "StatefulSet".into(),
            name: "mysql".into(),
            uid: "uid-of-a-previous-incarnation".into(),
            controller: true,
            block_owner_deletion: true,
        },
    )
    .unwrap();
    put(&store, &pod_key, pod).await;

    let gc = GcController::new(store.clone(), None);
    let outcome = gc.tick().await.expect("gc tick");

    assert_eq!(
        outcome.report.objects_changed, 1,
        "a live name under a different uid is a dead generation's dependent"
    );
}
