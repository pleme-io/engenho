//! R9.5 integration — DeploymentController + ReplicaSetController
//! together orchestrating the full Deployment → ReplicaSet → Pod
//! ownership graph. Plus R9.7 GcController orphan cleanup.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{
    Controller, DeploymentController, GcController, ReplicaSetController, is_owned_by,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

async fn boot_store() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("controllers-r95").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put_deployment(store: &StoreMesh, name: &str, replicas: i64, image: &str) -> String {
    let key = ResourceKey::namespaced("apps", "v1", "Deployment", "default", name);
    store
        .propose(ResourceCommand::Put {
            key: key.clone(),
            value: json!({
                "kind": "Deployment",
                "apiVersion": "apps/v1",
                "metadata": { "name": name },
                "spec": {
                    "replicas": replicas,
                    "selector": { "matchLabels": { "app": name } },
                    "template": {
                        "metadata": { "labels": { "app": name } },
                        "spec": { "containers": [{ "name": "main", "image": image }] }
                    }
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let d = store.get(&key).await.unwrap();
    d.get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .into()
}

async fn replicaset_count_owned_by(store: &StoreMesh, owner_uid: &str) -> usize {
    let rs = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    rs.iter().filter(|(_, r)| is_owned_by(r, owner_uid)).count()
}

async fn pod_count_owned_by(store: &StoreMesh, owner_uid: &str) -> usize {
    let pods = store.list("", "v1", "Pod", Some("default")).await;
    pods.iter()
        .filter(|(_, p)| is_owned_by(p, owner_uid))
        .count()
}

#[tokio::test]
async fn deployment_creates_replicaset_with_correct_replicas() {
    let store = boot_store().await;
    let dep_uid = put_deployment(&store, "podinfo", 3, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));

    let report = dc.tick().await.unwrap();
    assert_eq!(report.objects_changed, 1); // 1 RS created
    assert_eq!(replicaset_count_owned_by(&store, &dep_uid).await, 1);

    // The RS has the desired replicas.
    let rs = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let (_, rs_value) = rs
        .iter()
        .find(|(_, r)| is_owned_by(r, &dep_uid))
        .expect("RS exists");
    assert_eq!(rs_value.get("spec").unwrap().get("replicas").unwrap(), 3);

    drop(dc);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn deployment_then_replicaset_then_pods_full_chain() {
    let store = boot_store().await;
    let dep_uid = put_deployment(&store, "podinfo", 2, "podinfo:6").await;

    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));

    // Tick D — creates RS.
    dc.tick().await.unwrap();
    assert_eq!(replicaset_count_owned_by(&store, &dep_uid).await, 1);

    // Find the RS uid.
    let rs_list = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let (_, rs_value) = rs_list
        .iter()
        .find(|(_, r)| is_owned_by(r, &dep_uid))
        .expect("RS exists");
    let rs_uid = rs_value
        .get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();

    // Tick RS — creates Pods owned by the RS.
    rc.tick().await.unwrap();
    assert_eq!(pod_count_owned_by(&store, &rs_uid).await, 2);

    drop((dc, rc));
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn deployment_rollout_creates_new_replicaset_for_template_change() {
    let store = boot_store().await;
    let dep_uid = put_deployment(&store, "podinfo", 2, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));

    // Initial rollout — creates one RS.
    dc.tick().await.unwrap();
    assert_eq!(replicaset_count_owned_by(&store, &dep_uid).await, 1);

    // Patch the Deployment to a new image.
    let dep_key = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "podinfo");
    store
        .propose(ResourceCommand::Patch {
            key: dep_key,
            patch: json!({
                "spec": {
                    "template": {
                        "metadata": { "labels": { "app": "podinfo" } },
                        "spec": { "containers": [{ "name": "main", "image": "podinfo:7" }] }
                    }
                }
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Tick — should create a NEW RS for the new template + scale
    // the OLD RS to 0.
    let report = dc.tick().await.unwrap();
    assert!(report.objects_changed >= 2); // new RS + scale old to 0

    let owned_rs_count = replicaset_count_owned_by(&store, &dep_uid).await;
    assert_eq!(owned_rs_count, 2); // revision history retained

    // Verify: one RS has replicas=2 (current), the other has 0.
    let rs_list = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let owned: Vec<i64> = rs_list
        .iter()
        .filter(|(_, r)| is_owned_by(r, &dep_uid))
        .filter_map(|(_, r)| {
            r.get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|n| n.as_i64())
        })
        .collect();
    let mut sorted = owned.clone();
    sorted.sort();
    assert_eq!(sorted, vec![0, 2], "expected one current + one stale RS");

    drop(dc);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn deployment_is_idempotent_at_steady_state() {
    let store = boot_store().await;
    let _dep_uid = put_deployment(&store, "podinfo", 2, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));

    dc.tick().await.unwrap();
    let report2 = dc.tick().await.unwrap();
    assert_eq!(
        report2.objects_changed, 0,
        "steady state should not change anything"
    );

    drop(dc);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn gc_deletes_orphan_replicaset_when_deployment_is_removed() {
    let store = boot_store().await;
    let dep_uid = put_deployment(&store, "doomed", 1, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let gc = GcController::new(store.clone(), Some("default".into()));

    // D → RS exists.
    dc.tick().await.unwrap();
    assert_eq!(replicaset_count_owned_by(&store, &dep_uid).await, 1);

    // Delete the Deployment.
    let dep_key = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "doomed");
    store
        .propose(ResourceCommand::Delete {
            key: dep_key,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // GC tick — orphan RS gets deleted.
    let report = gc.tick().await.unwrap();
    assert!(report.objects_changed >= 1);
    assert_eq!(replicaset_count_owned_by(&store, &dep_uid).await, 0);

    drop((dc, gc));
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn gc_deletes_orphan_pods_when_replicaset_is_removed() {
    let store = boot_store().await;
    let dep_uid = put_deployment(&store, "p", 3, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));
    let gc = GcController::new(store.clone(), Some("default".into()));

    dc.tick().await.unwrap();
    rc.tick().await.unwrap();

    let rs_list = store
        .list("apps", "v1", "ReplicaSet", Some("default"))
        .await;
    let (rs_key, rs_value) = rs_list
        .iter()
        .find(|(_, r)| is_owned_by(r, &dep_uid))
        .expect("RS exists");
    let rs_uid = rs_value
        .get("metadata")
        .unwrap()
        .get("uid")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(pod_count_owned_by(&store, &rs_uid).await, 3);

    // Delete the RS directly.
    store
        .propose(ResourceCommand::Delete {
            key: rs_key.clone(),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // GC tick — orphan pods get deleted.
    let report = gc.tick().await.unwrap();
    assert!(report.objects_changed >= 3);
    assert_eq!(pod_count_owned_by(&store, &rs_uid).await, 0);

    drop((dc, rc, gc));
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn gc_leaves_non_orphans_alone() {
    let store = boot_store().await;
    let _dep_uid = put_deployment(&store, "alive", 2, "podinfo:6").await;
    let dc = DeploymentController::new(store.clone(), Some("default".into()));
    let rc = ReplicaSetController::new(store.clone(), Some("default".into()));
    let gc = GcController::new(store.clone(), Some("default".into()));

    dc.tick().await.unwrap();
    rc.tick().await.unwrap();

    let pods_before = store.list("", "v1", "Pod", Some("default")).await.len();
    let report = gc.tick().await.unwrap();
    assert_eq!(report.objects_changed, 0, "no orphans expected");

    let pods_after = store.list("", "v1", "Pod", Some("default")).await.len();
    assert_eq!(pods_before, pods_after);

    drop((dc, rc, gc));
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
