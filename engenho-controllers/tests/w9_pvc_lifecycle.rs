//! W9 — pvc-protection, then Released and reclaim, composed.
//!
//! The two controllers never call each other; they meet only in the store.
//! pvc-protection holds a claim a pod uses, the binder treats a held claim
//! as alive, and the volume is reclaimed only once both have let go. Run
//! here end to end, one tick of each at a time, the way two `WatchDriver`s
//! would interleave them.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::meta::ObjectMeta;
use engenho_controllers::{
    Controller, FakeProvisionerEnv, PVC_PROTECTION_FINALIZER, PvBinderController,
    PvcProtectionController,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

const ROOT: &str = "/data/local-path";

async fn boot() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("w9-pvc-lifecycle").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn claim_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "PersistentVolumeClaim", "app", "data")
}

fn pod_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "app", "web")
}

fn volume_key() -> ResourceKey {
    ResourceKey::cluster_scoped("", "v1", "PersistentVolume", "pvc-uid-data")
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::put(key, value, Reason::Operator))
        .await
        .unwrap();
}

async fn delete(store: &StoreMesh, key: ResourceKey) {
    store
        .propose(ResourceCommand::delete(key, Reason::Operator))
        .await
        .unwrap();
}

/// One tick of each controller, protection first.
async fn settle(protection: &PvcProtectionController, binder: &PvBinderController) {
    protection.tick().await.unwrap();
    binder.tick().await.unwrap();
}

/// ★ The whole lifecycle. A pod mounts a dynamically provisioned claim;
/// the claim is deleted while the pod runs. The claim and its volume must
/// both outlive that delete for as long as the pod exists, and once it is
/// gone the claim goes, its volume goes Released, and — under the class's
/// `Delete` — its directory and PV are removed.
#[tokio::test]
async fn a_claim_in_use_keeps_its_volume_until_its_pod_is_gone_then_both_are_reclaimed() {
    let store = boot().await;
    put(
        &store,
        ResourceKey::cluster_scoped("storage.k8s.io", "v1", "StorageClass", "local"),
        json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
               "metadata": {"name": "local", "annotations":
                   {"storageclass.kubernetes.io/is-default-class": "true"}},
               "provisioner": "engenho.io/local-path", "reclaimPolicy": "Delete",
               "volumeBindingMode": "Immediate"}),
    )
    .await;
    put(
        &store,
        claim_key(),
        json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
               "metadata": {"name": "data", "namespace": "app", "uid": "uid-data"},
               "spec": {"accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}}}}),
    )
    .await;
    put(
        &store,
        pod_key(),
        json!({"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": "web", "namespace": "app", "uid": "uid-web"},
               "spec": {"nodeName": "node-a", "volumes": [
                   {"name": "data", "persistentVolumeClaim": {"claimName": "data"}}]}}),
    )
    .await;
    let env = Arc::new(FakeProvisionerEnv::new());
    let protection = PvcProtectionController::new(store.clone(), None);
    let binder = PvBinderController::with_env(store.clone(), None, ROOT, env.clone());
    let dir = [ROOT, "/pvc-uid-data_app_data"].concat();

    settle(&protection, &binder).await;
    let claim = store.get(&claim_key()).await.unwrap();
    assert_eq!(claim["status"]["phase"], "Bound");
    assert!(
        claim.has_finalizer(PVC_PROTECTION_FINALIZER),
        "the bind kept the finalizer"
    );
    assert_eq!(env.ensured_dirs(), vec![dir.clone()]);

    // Deleted while the pod runs: nothing lets go.
    delete(&store, claim_key()).await;
    for _ in 0..3 {
        settle(&protection, &binder).await;
    }
    let claim = store
        .get(&claim_key())
        .await
        .expect("the claim outlives its delete");
    assert!(claim.is_terminating());
    let volume = store
        .get(&volume_key())
        .await
        .expect("and so does its volume");
    assert_eq!(volume["status"]["phase"], "Bound");
    assert!(env.removed_dirs().is_empty(), "{:?}", env.removed_dirs());

    // The pod goes: the claim goes, then its volume.
    delete(&store, pod_key()).await;
    protection.tick().await.unwrap();
    assert!(store.get(&claim_key()).await.is_none(), "the claim is gone");
    binder.tick().await.unwrap();
    assert_eq!(
        store.get(&volume_key()).await.unwrap()["status"]["phase"],
        "Released"
    );
    for _ in 0..2 {
        settle(&protection, &binder).await;
    }
    assert!(
        store.get(&volume_key()).await.is_none(),
        "the PV is deleted"
    );
    assert_eq!(env.removed_dirs(), vec![dir], "and exactly its directory");
}
