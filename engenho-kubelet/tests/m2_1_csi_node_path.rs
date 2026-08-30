//! M2.1 — a pod mounts a CSI volume, end to end, against a REAL driver.
//!
//! This is the test that says the CSI node path actually works: a Pod with
//! a PVC, a PVC bound to a PV with a `csi` source, and a real gRPC driver
//! on a real unix socket that records exactly which RPCs it was asked for.
//!
//! * **V1** a CSI PV publishes and yields a mountable host path
//! * **V2** the stage/publish ORDER, and the handle and attributes reaching
//!   the driver unchanged
//! * **V3** a driver that does NOT want staging is not staged
//! * **V4** read-only from EITHER the PV or the pod reaches the driver
//! * **V5** teardown unpublishes and unstages
//! * **V6** a driver that fails leaves the pod Pending, never a fake mount

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Value, json};

use engenho_csi::client::CsiClient;
use engenho_csi::testdriver::{DriverCall, DriverConfig, TestDriver};
use engenho_kubelet::csi_materializer::{CsiVolumeMaterializer, DriverTable, RegisteredDriver};
use engenho_kubelet::pod_volume::{
    FakeVolumeMaterializer, MountSource, VolumeMaterializer, resolve_pod_volumes,
};

const DRIVER: &str = "test.csi.engenho.io";

fn pod_with_pvc(read_only: bool) -> Value {
    json!({
        "spec": { "volumes": [{
            "name": "data",
            "persistentVolumeClaim": { "claimName": "c", "readOnly": read_only }
        }]}
    })
}

fn objects(pv_csi: Value) -> impl Fn(&str, &str) -> Option<Value> {
    move |kind: &str, name: &str| match (kind, name) {
        ("PersistentVolumeClaim", "c") => Some(json!({
            "spec": { "volumeName": "pv-1" }, "status": { "phase": "Bound" }
        })),
        ("PersistentVolume", "pv-1") => Some(pv_csi.clone()),
        _ => None,
    }
}

fn csi_pv(read_only: bool) -> Value {
    json!({ "spec": { "csi": {
        "driver": DRIVER,
        "volumeHandle": "vol-handle-42",
        "fsType": "ext4",
        "readOnly": read_only,
        "volumeAttributes": { "backend": "pool-a" },
    }}})
}

async fn materializer_for(d: &TestDriver, root: &std::path::Path) -> CsiVolumeMaterializer {
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
    CsiVolumeMaterializer::new(Arc::new(FakeVolumeMaterializer::new()), table, root)
}

/// V1 + V2 — the whole node path, and the fields that must survive it.
#[tokio::test]
async fn a_csi_pv_is_staged_then_published_and_becomes_a_mountable_path() {
    let d = TestDriver::start_default().await;
    let root = tempfile::tempdir().unwrap();
    let m = materializer_for(&d, root.path()).await;

    let resolved = resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .expect("a registered driver must publish");

    // V1 — the container runtime gets an ordinary host directory, so
    // nothing downstream needs a CSI-shaped case.
    let source = resolved.get("data").expect("the volume resolved");
    let MountSource::PvcHostDir { path, read_only } = source else {
        panic!("expected a host-dir mount, got {source:?}");
    };
    assert!(!read_only);
    assert!(
        path.ends_with("volumes/kubernetes.io~csi/data/mount"),
        "upstream's layout, which drivers and e2e suites look at: {}",
        path.display()
    );
    assert!(
        path.is_dir(),
        "the kubelet mkdirs the target, not the driver"
    );

    // V2 — order, and the handle the driver keys on.
    let calls = d.calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    let DriverCall::NodeStage(id, staging) = &calls[0] else {
        panic!("staging must come FIRST for a stage_unstage driver: {calls:?}");
    };
    assert_eq!(id, "vol-handle-42", "the PV's handle, not the PV name");
    let DriverCall::NodePublish(id, target, ro) = &calls[1] else {
        panic!("publish must follow: {calls:?}");
    };
    assert_eq!(id, "vol-handle-42");
    assert!(!ro);
    assert_eq!(target, &path.display().to_string());
    assert!(
        !staging.contains("pods"),
        "staging is per-NODE, not per-pod: {staging}"
    );

    d.stop().await;
}

/// V3 — a driver that does not declare `STAGE_UNSTAGE_VOLUME` must not be
/// staged. Staging it anyway fails the mount in a way that reads as a
/// broken volume rather than a protocol mistake.
#[tokio::test]
async fn a_driver_that_does_not_want_staging_is_not_staged() {
    let d = TestDriver::start(DriverConfig {
        stage_unstage: false,
        ..DriverConfig::default()
    })
    .await;
    let root = tempfile::tempdir().unwrap();
    let m = materializer_for(&d, root.path()).await;

    resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .unwrap();

    let calls = d.calls();
    assert_eq!(calls.len(), 1, "publish only: {calls:?}");
    assert!(matches!(calls[0], DriverCall::NodePublish(..)), "{calls:?}");

    d.stop().await;
}

/// V4 — read-only from EITHER side forces it.
///
/// A volume the CLUSTER declared read-only must not become writable because
/// the pod did not repeat the claim.
#[tokio::test]
async fn read_only_from_either_the_pv_or_the_pod_reaches_the_driver() {
    for (pv_ro, pod_ro, why) in [
        (true, false, "the PV alone"),
        (false, true, "the pod alone"),
        (true, true, "both"),
    ] {
        let d = TestDriver::start_default().await;
        let root = tempfile::tempdir().unwrap();
        let m = materializer_for(&d, root.path()).await;

        resolve_pod_volumes(
            &pod_with_pvc(pod_ro),
            "ns",
            "pod1",
            objects(csi_pv(pv_ro)),
            &m,
        )
        .await
        .unwrap();

        let published = d
            .calls()
            .into_iter()
            .find_map(|c| match c {
                DriverCall::NodePublish(_, _, ro) => Some(ro),
                _ => None,
            })
            .expect("a publish call");
        assert!(published, "read-only via {why} must reach the driver");

        d.stop().await;
    }

    // And the negative direction, so the assertion above is not vacuous.
    let d = TestDriver::start_default().await;
    let root = tempfile::tempdir().unwrap();
    let m = materializer_for(&d, root.path()).await;
    resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .unwrap();
    let published = d
        .calls()
        .into_iter()
        .find_map(|c| match c {
            DriverCall::NodePublish(_, _, ro) => Some(ro),
            _ => None,
        })
        .unwrap();
    assert!(!published, "neither side asked for read-only");
    d.stop().await;
}

/// V5 — teardown undoes exactly what was published.
#[tokio::test]
async fn teardown_unpublishes_and_unstages() {
    let d = TestDriver::start_default().await;
    let root = tempfile::tempdir().unwrap();
    let m = materializer_for(&d, root.path()).await;

    resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .unwrap();
    m.unpublish_csi("ns", "pod1", "data").await.unwrap();

    let calls = d.calls();
    assert_eq!(
        calls.len(),
        4,
        "stage, publish, unpublish, unstage: {calls:?}"
    );
    assert!(
        matches!(calls[2], DriverCall::NodeUnpublish(..)),
        "{calls:?}"
    );
    assert!(matches!(calls[3], DriverCall::NodeUnstage(..)), "{calls:?}");

    // Idempotent: a second teardown is a no-op, not an error, because
    // deletion may retry after a partial failure.
    m.unpublish_csi("ns", "pod1", "data").await.unwrap();
    assert_eq!(d.calls().len(), 4, "no duplicate teardown");

    d.stop().await;
}

/// V6 — a failing driver keeps the pod Pending. Never a fake mount, and
/// never an empty directory presented as the volume: silently-empty
/// storage is data loss that looks like an application bug.
#[tokio::test]
async fn a_failing_driver_leaves_the_pod_pending_with_a_readable_reason() {
    let d = TestDriver::start(DriverConfig {
        fail_node_rpcs: Some("backend unreachable".into()),
        ..DriverConfig::default()
    })
    .await;
    let root = tempfile::tempdir().unwrap();
    let m = materializer_for(&d, root.path()).await;

    let err = resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("backend unreachable"), "{msg}");
    assert!(msg.contains("NodeStageVolume"), "names the RPC: {msg}");

    d.stop().await;
}

/// The attributes bag reaches the driver verbatim.
///
/// `volumeAttributes` is what `CreateVolume` returned and the driver
/// expects back unchanged; dropping it is how a mount succeeds against the
/// wrong backend path.
#[tokio::test]
async fn volume_attributes_are_carried_to_the_driver_unchanged() {
    // Asserted through the public resolver rather than the wire, because
    // the reference driver does not echo context — what IS provable here is
    // that the typed request carries them, which is the half engenho owns.
    use engenho_kubelet::pod_volume::CsiPublishRequest;

    struct Capture(Arc<std::sync::Mutex<Option<CsiPublishRequest>>>);
    #[async_trait::async_trait]
    impl VolumeMaterializer for Capture {
        fn name(&self) -> &'static str {
            "capture"
        }
        async fn materialize_files(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &BTreeMap<String, Vec<u8>>,
        ) -> Result<MountSource, engenho_kubelet::pod_volume::VolumeResolveError> {
            unreachable!()
        }
        async fn ensure_empty_dir(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<MountSource, engenho_kubelet::pod_volume::VolumeResolveError> {
            unreachable!()
        }
        async fn remove_empty_dir(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<(), engenho_kubelet::pod_volume::VolumeResolveError> {
            Ok(())
        }
        async fn publish_csi(
            &self,
            _: &str,
            _: &str,
            _: &str,
            req: &CsiPublishRequest,
        ) -> Result<MountSource, engenho_kubelet::pod_volume::VolumeResolveError> {
            *self.0.lock().unwrap() = Some(req.clone());
            Ok(MountSource::PvcHostDir {
                path: "/tmp/x".into(),
                read_only: req.read_only,
            })
        }
    }

    let seen = Arc::new(std::sync::Mutex::new(None));
    let m = Capture(seen.clone());
    resolve_pod_volumes(
        &pod_with_pvc(false),
        "ns",
        "pod1",
        objects(csi_pv(false)),
        &m,
    )
    .await
    .unwrap();

    let req = seen.lock().unwrap().clone().expect("publish was called");
    assert_eq!(req.driver, DRIVER);
    assert_eq!(req.volume_handle, "vol-handle-42");
    assert_eq!(req.fs_type.as_deref(), Some("ext4"));
    assert_eq!(
        req.volume_attributes.get("backend").map(String::as_str),
        Some("pool-a")
    );
}
