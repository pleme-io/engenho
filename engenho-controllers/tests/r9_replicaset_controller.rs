//! R9 integration tests — real StoreMesh + real ReplicaSetController
//! reconciling Pod counts via owner references.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, ReplicaSetController, is_owned_by};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("controllers-r9").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

/// Write a ReplicaSet with `replicas` desired count + a simple
/// pod template. The store auto-fills metadata.uid on the first
/// Put — we read it back to use as the owner reference uid.
async fn put_replicaset(store: &StoreMesh, name: &str, replicas: i64) -> String {
    let key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", name);
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value: json!({
                "kind": "ReplicaSet",
                "apiVersion": "apps/v1",
                "metadata": { "name": name },
                "spec": {
                    "replicas": replicas,
                    "selector": { "matchLabels": { "app": name } },
                    "template": {
                        "metadata": { "labels": { "app": name } },
                        "spec": { "containers": [{ "name": "main", "image": "podinfo:6" }] }
                    }
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    // Read back the uid the state machine assigned.
    let rs = store.get(&key).await.expect("rs stored");
    rs.get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

async fn owned_pod_count(store: &StoreMesh, owner_uid: &str) -> usize {
    let pods = store.list("", "v1", "Pod", Some("default")).await;
    pods.iter()
        .filter(|(_, p)| is_owned_by(p, owner_uid))
        .count()
}

#[tokio::test]
async fn rs_controller_creates_missing_pods_to_meet_replicas() {
    let store = boot_store().await;
    let uid = put_replicaset(&store, "podinfo", 3).await;
    assert_eq!(owned_pod_count(&store, &uid).await, 0);

    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));
    let report = ctrl.tick().await.unwrap();
    assert_eq!(report.objects_examined, 1);
    // 3 Pods created + 1 .status write (item-8).
    assert_eq!(report.objects_changed, 4);
    assert_eq!(owned_pod_count(&store, &uid).await, 3);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn rs_controller_is_idempotent_at_steady_state() {
    let store = boot_store().await;
    let uid = put_replicaset(&store, "stable", 2).await;
    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));

    // First tick creates 2 pods.
    ctrl.tick().await.unwrap();
    assert_eq!(owned_pod_count(&store, &uid).await, 2);

    // Second tick is a no-op.
    let report = ctrl.tick().await.unwrap();
    assert_eq!(report.objects_changed, 0);
    assert_eq!(owned_pod_count(&store, &uid).await, 2);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn rs_controller_evicts_excess_when_replicas_decreases() {
    let store = boot_store().await;
    let uid = put_replicaset(&store, "shrink", 4).await;
    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));

    // Initial: 4 pods.
    ctrl.tick().await.unwrap();
    assert_eq!(owned_pod_count(&store, &uid).await, 4);

    // Patch RS to replicas=1.
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "shrink");
    store
        .propose(ResourceCommand::patch(
            rs_key,
            json!({"spec": {"replicas": 1}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    let report = ctrl.tick().await.unwrap();
    // 3 deletions + 1 .status write (replicas 4 → 1).
    assert_eq!(report.objects_changed, 4);
    assert_eq!(owned_pod_count(&store, &uid).await, 1);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn rs_controller_scales_up_after_increasing_replicas() {
    let store = boot_store().await;
    let uid = put_replicaset(&store, "grow", 1).await;
    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));

    ctrl.tick().await.unwrap();
    assert_eq!(owned_pod_count(&store, &uid).await, 1);

    // Scale to 5.
    let rs_key = ResourceKey::namespaced("apps", "v1", "ReplicaSet", "default", "grow");
    store
        .propose(ResourceCommand::patch(
            rs_key,
            json!({"spec": {"replicas": 5}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    let report = ctrl.tick().await.unwrap();
    // 4 new pods + 1 .status write (replicas 1 → 5).
    assert_eq!(report.objects_changed, 5);
    assert_eq!(owned_pod_count(&store, &uid).await, 5);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn rs_controller_handles_two_replicasets_independently() {
    let store = boot_store().await;
    let uid_a = put_replicaset(&store, "rs-a", 2).await;
    let uid_b = put_replicaset(&store, "rs-b", 3).await;
    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));

    let report = ctrl.tick().await.unwrap();
    assert_eq!(report.objects_examined, 2);
    // (2 pods + 1 status) for rs-a + (3 pods + 1 status) for rs-b = 7.
    assert_eq!(report.objects_changed, 7);
    assert_eq!(owned_pod_count(&store, &uid_a).await, 2);
    assert_eq!(owned_pod_count(&store, &uid_b).await, 3);

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

/// Pod template's containers + labels survive into the created pods.
#[tokio::test]
async fn rs_controller_pods_inherit_template_spec_and_labels() {
    let store = boot_store().await;
    let _uid = put_replicaset(&store, "labeled", 1).await;
    let ctrl = ReplicaSetController::new(store.clone(), Some("default".into()));
    ctrl.tick().await.unwrap();

    let pods = store.list("", "v1", "Pod", Some("default")).await;
    assert_eq!(pods.len(), 1);
    let (_, pod) = &pods[0];
    // Template's labels carried through.
    let labels = pod
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .expect("labels carried");
    assert_eq!(labels.get("app").unwrap(), "labeled");
    // Containers carried through.
    let containers = pod
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.as_array())
        .expect("containers carried");
    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0].get("image").unwrap(), "podinfo:6");
    // Pod has no nodeName yet — scheduler binds it.
    assert!(pod.get("spec").unwrap().get("nodeName").is_none());

    drop(ctrl);
    let mesh = Arc::try_unwrap(store).ok().expect("only owner");
    mesh.terminate().await.unwrap();
}

fn _unused_value() -> Value {
    Value::Null
}
