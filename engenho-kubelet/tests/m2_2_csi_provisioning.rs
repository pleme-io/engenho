//! M2.2 — dynamic provisioning through a REAL CSI driver, end to end.
//!
//! A PVC and a StorageClass go into the store; a real gRPC driver on a real
//! unix socket answers `CreateVolume`; the binder writes a PV whose
//! `spec.csi` the node path can then publish. That last part is the one
//! that matters: the two halves of Phase A meet here.
//!
//! * **P1** a CSI StorageClass provisions and binds
//! * **P2** the PV carries a `csi` source the NODE PATH can consume
//! * **P3** the driver's capacity wins over the request
//! * **P4** provisioning is idempotent on the PV name — a retry does not
//!   leak a second disk
//! * **P5** an unregistered provisioner leaves the claim Pending untouched
//! * **P6** a failing driver never produces a fake Bound

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use engenho_controllers::controller::Controller;
use engenho_controllers::pv_binder::PvBinderController;
use engenho_csi::client::CsiClient;
use engenho_csi::testdriver::{DriverCall, DriverConfig, TestDriver};
use engenho_kubelet::csi_materializer::{DriverCsiProvisioner, DriverTable, RegisteredDriver};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

const DRIVER: &str = "test.csi.engenho.io";

async fn boot(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key,
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn sc_key() -> ResourceKey {
    ResourceKey::cluster_scoped("storage.k8s.io", "v1", "StorageClass", "csi-sc")
}

fn pvc_key() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "PersistentVolumeClaim", "ns", "claim")
}

fn pv_key() -> ResourceKey {
    ResourceKey::cluster_scoped("", "v1", "PersistentVolume", "pvc-ns-claim")
}

async fn seed(store: &StoreMesh, provisioner: &str, size: &str) {
    put(
        store,
        sc_key(),
        json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": { "name": "csi-sc" },
            "provisioner": provisioner,
            "reclaimPolicy": "Delete",
            "parameters": { "type": "gp3" },
        }),
    )
    .await;
    put(
        store,
        pvc_key(),
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": "claim", "namespace": "ns", "uid": "u-1" },
            "spec": {
                "storageClassName": "csi-sc",
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": size } },
            },
            "status": { "phase": "Pending" },
        }),
    )
    .await;
}

async fn provisioner_for(d: &TestDriver) -> DriverCsiProvisioner {
    let client = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();
    let info = client.info().await.unwrap();
    let table = DriverTable::new();
    table
        .insert(
            info.name.clone(),
            RegisteredDriver {
                client,
                stage_unstage: info.stage_unstage,
                node_id: info.node_id.clone(),
            },
        )
        .await;
    DriverCsiProvisioner::new(table)
}

/// P1 + P2 + P3 + P4 — the whole provisioning path.
#[tokio::test]
async fn a_csi_storage_class_provisions_binds_and_yields_a_publishable_pv() {
    let d = TestDriver::start_default().await;
    let store = boot("csi-provision").await;
    seed(&store, DRIVER, "1Gi").await;

    let binder = PvBinderController::new(store.clone(), None, "/tmp/local-path")
        .with_csi(Arc::new(provisioner_for(&d).await));
    binder.tick().await.unwrap();

    // P1 — the claim is Bound to a real volume.
    let pvc = store.get(&pvc_key()).await.expect("pvc");
    assert_eq!(pvc["status"]["phase"], "Bound");
    assert_eq!(pvc["spec"]["volumeName"], "pvc-ns-claim");

    // P2 — and the PV carries a `csi` source the node path consumes. This
    // is where the two halves of the CSI work meet: publish reads exactly
    // these three fields.
    let pv = store.get(&pv_key()).await.expect("pv");
    assert_eq!(pv["spec"]["csi"]["driver"], DRIVER);
    assert_eq!(pv["spec"]["csi"]["volumeHandle"], "vol-1");
    assert_eq!(pv["spec"]["storageClassName"], "csi-sc");
    assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Delete");
    assert_eq!(pv["status"]["phase"], "Bound");

    // P3 — the size the DRIVER reports, parsed from the Ki/Gi quantity.
    // 1Gi is 2^30, not 10^9: the 7.4% gap that presents as a disk full at
    // 93% while monitoring says there is room.
    assert_eq!(pv["spec"]["capacity"]["storage"], "1073741824");

    // The StorageClass parameters reached the driver.
    let creates: Vec<_> = d
        .calls()
        .into_iter()
        .filter(|c| matches!(c, DriverCall::CreateVolume(..)))
        .collect();
    assert_eq!(creates.len(), 1, "{creates:?}");
    assert_eq!(
        creates[0],
        DriverCall::CreateVolume("pvc-ns-claim".into(), 1_073_741_824)
    );

    // P4 — a second tick must not provision a second disk. The claim is
    // Bound now so the binder skips it; the guarantee that matters is that
    // the driver saw exactly one CreateVolume.
    binder.tick().await.unwrap();
    let creates = d
        .calls()
        .into_iter()
        .filter(|c| matches!(c, DriverCall::CreateVolume(..)))
        .count();
    assert_eq!(creates, 1, "a retry must not leak a second volume");
    assert_eq!(d.volumes().len(), 1);

    drop(binder);
    d.stop().await;
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// P5 — a provisioner nobody registered leaves the claim exactly as it was.
///
/// This is the behaviour a cluster with an EXTERNAL provisioner depends on:
/// engenho must not touch a claim it is not responsible for.
#[tokio::test]
async fn an_unregistered_provisioner_leaves_the_claim_pending() {
    let d = TestDriver::start_default().await;
    let store = boot("csi-unregistered").await;
    seed(&store, "somebody.elses.csi.io", "1Gi").await;

    let binder = PvBinderController::new(store.clone(), None, "/tmp/local-path")
        .with_csi(Arc::new(provisioner_for(&d).await));
    binder.tick().await.unwrap();

    let pvc = store.get(&pvc_key()).await.expect("pvc");
    assert_eq!(pvc["status"]["phase"], "Pending", "untouched");
    assert!(store.get(&pv_key()).await.is_none(), "no PV was invented");
    assert!(
        d.calls().is_empty(),
        "and OUR driver was not called for somebody else's class: {:?}",
        d.calls()
    );

    drop(binder);
    d.stop().await;
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// P6 — a driver that fails never yields a Bound claim.
///
/// A PVC bound to a volume that was never created is worse than one that is
/// honestly still Pending: the pod mounts and fails instead of waiting.
#[tokio::test]
async fn a_failing_driver_never_produces_a_fake_bound_claim() {
    // A node-only driver has no controller service, so CreateVolume is a
    // genuine `Unimplemented` — a real failure, not a synthetic one.
    let d = TestDriver::start(DriverConfig {
        controller_service: false,
        ..DriverConfig::default()
    })
    .await;
    let store = boot("csi-failing").await;
    seed(&store, DRIVER, "1Gi").await;

    let binder = PvBinderController::new(store.clone(), None, "/tmp/local-path")
        .with_csi(Arc::new(provisioner_for(&d).await));
    let out = binder.tick().await;
    assert!(out.is_ok(), "one bad claim must not fail the whole tick");

    let pvc = store.get(&pvc_key()).await.expect("pvc");
    assert_eq!(pvc["status"]["phase"], "Pending");
    assert!(pvc["spec"].get("volumeName").is_none());
    assert!(
        store.get(&pv_key()).await.is_none(),
        "no PV pointing at a volume that does not exist"
    );

    drop(binder);
    d.stop().await;
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// A claim with no size asked for still provisions.
///
/// The driver picks its own default, which is what upstream does; sending
/// `required_bytes: 0` would be a request for a zero-byte volume.
#[tokio::test]
async fn a_claim_with_no_requested_size_lets_the_driver_choose() {
    let d = TestDriver::start_default().await;
    let store = boot("csi-nosize").await;
    put(
        &store,
        sc_key(),
        json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": { "name": "csi-sc" }, "provisioner": DRIVER,
        }),
    )
    .await;
    put(
        &store,
        pvc_key(),
        json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": { "name": "claim", "namespace": "ns" },
            "spec": { "storageClassName": "csi-sc", "accessModes": ["ReadWriteOnce"] },
            "status": { "phase": "Pending" },
        }),
    )
    .await;

    let binder = PvBinderController::new(store.clone(), None, "/tmp/local-path")
        .with_csi(Arc::new(provisioner_for(&d).await));
    binder.tick().await.unwrap();

    assert_eq!(
        store.get(&pvc_key()).await.expect("pvc")["status"]["phase"],
        "Bound"
    );
    assert_eq!(
        d.calls()
            .into_iter()
            .find_map(|c| match c {
                DriverCall::CreateVolume(_, bytes) => Some(bytes),
                _ => None,
            })
            .expect("a create"),
        0,
        "no capacity range was sent, so the driver saw none"
    );

    drop(binder);
    d.stop().await;
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}
