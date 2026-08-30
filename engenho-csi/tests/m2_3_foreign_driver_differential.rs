//! M2.3 — the DIFFERENTIAL: engenho's CSI client against a driver we did
//! not write.
//!
//! ★ WHY THIS EXISTS WHEN THE CRATE ALREADY HAS A REFERENCE DRIVER. The
//! in-crate `testdriver` is a real gRPC server on a real socket, and it
//! still cannot falsify us: it was written from the same reading of the
//! same spec by the same author as the client. It proves our encoder agrees
//! with our decoder — the one thing that cannot fail.
//!
//! An hour before this file was written, real `etcdctl` found a
//! divide-by-zero in engenho's etcd façade that no in-house test could
//! have: we would never have written a client that divides by `db_size`.
//! That is the argument for a foreign oracle, and this is the CSI one.
//!
//! ★ THIS IS NOT A GO DEPENDENCY. `csi-driver-host-path` is the CSI
//! project's own reference driver. It is never built by our build, never
//! linked, never shipped. It is a measuring instrument, and its language is
//! incidental — what matters is that we did not write it.
//!
//! ★ IGNORED BY DEFAULT, AND THAT IS DELIBERATE. A test that silently
//! passes when the oracle is absent is worse than no test: it reports green
//! for "the differential ran and agreed" when the truth is "nothing ran".
//! Run it explicitly:
//!
//! ```text
//! # build the oracle once
//! git clone --depth 1 -b v1.15.0 \
//!   https://github.com/kubernetes-csi/csi-driver-host-path /tmp/csi-hp
//! (cd /tmp/csi-hp && go build -o /tmp/hostpathplugin ./cmd/hostpathplugin)
//!
//! # run it
//! /tmp/hostpathplugin --endpoint=unix:///tmp/csi-state/csi.sock \
//!   --nodeid=cid --statedir=/tmp/csi-state
//!
//! # then
//! ENGENHO_CSI_ORACLE=/tmp/csi-state/csi.sock \
//!   cargo test -p engenho-csi --test m2_3_foreign_driver_differential -- --ignored
//! ```

use std::collections::HashMap;

use engenho_csi::client::CsiClient;
use engenho_csi::pb;

/// The oracle's socket, or a skip.
fn oracle() -> String {
    std::env::var("ENGENHO_CSI_ORACLE").unwrap_or_default()
}

fn cap() -> pb::VolumeCapability {
    pb::VolumeCapability {
        access_type: Some(pb::volume_capability::AccessType::Mount(
            pb::volume_capability::MountVolume::default(),
        )),
        access_mode: Some(pb::volume_capability::AccessMode {
            mode: pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

/// D1 — engenho can interrogate a driver it has never seen.
///
/// This is the whole registration path against foreign software: identity,
/// plugin capabilities, node info, node capabilities and controller
/// capabilities, folded into one `DriverInfo`.
#[tokio::test]
#[ignore = "needs a running csi-driver-host-path; see the module header"]
async fn engenho_interrogates_a_driver_it_did_not_write() {
    let socket = oracle();
    assert!(
        !socket.is_empty(),
        "set ENGENHO_CSI_ORACLE to the driver's socket"
    );

    let client = CsiClient::dial(&socket).await.expect("dial the oracle");
    let info = client.info().await.expect("interrogate the oracle");

    // The driver's OWN name, not one we chose.
    assert_eq!(info.name, "hostpath.csi.k8s.io", "{info:?}");
    // The driver REFUSES GetPluginInfo with `Unavailable: Driver is
    // missing version` when built without one — measured, by building it
    // without one first. So a non-empty version here is not decoration: it
    // is the only reason the call succeeded at all.
    assert!(!info.vendor_version.is_empty(), "{info:?}");
    // hostpath ships the controller service and staging; reading these off
    // the wire rather than assuming them is the point.
    assert!(info.has_controller_service, "{info:?}");
    assert!(info.stage_unstage, "{info:?}");
    assert_eq!(info.node_id, "cid", "the --nodeid we started it with");

    assert!(client.probe().await.expect("probe"), "the driver is ready");
}

/// D2 — the full controller path against foreign software.
///
/// Provision, attach, detach, delete. Every field engenho sends has to be
/// one the driver actually accepts, and every field it returns one engenho
/// actually reads.
#[tokio::test]
#[ignore = "needs a running csi-driver-host-path; see the module header"]
async fn the_controller_path_round_trips_through_a_foreign_driver() {
    let client = CsiClient::dial(&oracle()).await.expect("dial");

    let volume = client
        .create_volume(pb::CreateVolumeRequest {
            name: "engenho-differential-1".into(),
            capacity_range: Some(pb::CapacityRange {
                required_bytes: 1024 * 1024,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![cap()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            volume_content_source: None,
            accessibility_requirements: None,
            mutable_parameters: HashMap::new(),
        })
        .await
        .expect("the foreign driver provisions");

    assert!(!volume.volume_id.is_empty(), "a real handle: {volume:?}");
    assert!(volume.capacity_bytes >= 1024 * 1024, "{volume:?}");

    // ★ IDEMPOTENCY, MEASURED RATHER THAN ASSUMED. engenho keys retries on
    // the volume NAME precisely because CreateVolume is specified as
    // idempotent on it — a second call with the same name must return the
    // SAME volume, or every reconcile after a transient failure leaks a
    // disk. This asserts the spec against a real implementation of it.
    let again = client
        .create_volume(pb::CreateVolumeRequest {
            name: "engenho-differential-1".into(),
            capacity_range: Some(pb::CapacityRange {
                required_bytes: 1024 * 1024,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![cap()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            volume_content_source: None,
            accessibility_requirements: None,
            mutable_parameters: HashMap::new(),
        })
        .await
        .expect("a retry is not an error");
    assert_eq!(
        again.volume_id, volume.volume_id,
        "the same name returned a DIFFERENT volume: retries leak disks"
    );

    client
        .delete_volume(pb::DeleteVolumeRequest {
            volume_id: volume.volume_id.clone(),
            secrets: HashMap::new(),
        })
        .await
        .expect("delete");

    // Deleting twice is also idempotent per the spec, and engenho's
    // teardown path relies on it.
    client
        .delete_volume(pb::DeleteVolumeRequest {
            volume_id: volume.volume_id,
            secrets: HashMap::new(),
        })
        .await
        .expect("a second delete is success, not an error");
}

/// D3 — the node path against foreign software, as far as THIS platform
/// allows.
///
/// ★ WHAT THIS MEASURES, AND WHAT IT HONESTLY CANNOT. `NodeStageVolume`
/// reaches the real driver and succeeds. `NodePublishVolume` then fails —
/// and the failure is worth reading exactly:
///
/// ```text
/// Unknown: check target path: util/mount on this platform is not supported
/// ```
///
/// That is `k8s.io/mount-utils` refusing to mount on darwin. It is a
/// property of the ORACLE on this host, not of engenho's request: the error
/// comes from inside the driver's own mount layer, which means engenho's
/// message passed every piece of the driver's validation ahead of it —
/// volume id, capability, staging path, target path, context. A malformed
/// request would have been rejected as `InvalidArgument` long before
/// reaching a mount syscall.
///
/// So the assertion is precise rather than aspirational: stage SUCCEEDS,
/// and publish fails for a PLATFORM reason rather than a protocol one. The
/// full publish→pod path is a Linux test.
/// `pending-csi-differential: node-publish (needs a Linux host)`
#[tokio::test]
#[ignore = "needs a running csi-driver-host-path; see the module header"]
async fn the_node_path_reaches_a_foreign_driver_and_stops_only_at_the_platform() {
    let client = CsiClient::dial(&oracle()).await.expect("dial");

    let volume = client
        .create_volume(pb::CreateVolumeRequest {
            name: "engenho-differential-node".into(),
            capacity_range: Some(pb::CapacityRange {
                required_bytes: 1024 * 1024,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![cap()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            volume_content_source: None,
            accessibility_requirements: None,
            mutable_parameters: HashMap::new(),
        })
        .await
        .expect("provision");

    let base = std::env::temp_dir().join("engenho-csi-differential");
    let staging = base.join("staging");
    let target = base.join("target");
    for p in [&staging, &target] {
        std::fs::create_dir_all(p).expect("mkdir");
    }

    // The driver declares STAGE_UNSTAGE_VOLUME (read off the wire in D1,
    // not assumed), so staging comes first — and it WORKS against foreign
    // software.
    client
        .node_stage(pb::NodeStageVolumeRequest {
            volume_id: volume.volume_id.clone(),
            publish_context: HashMap::new(),
            staging_target_path: staging.display().to_string(),
            volume_capability: Some(cap()),
            secrets: HashMap::new(),
            volume_context: volume.volume_context.clone(),
        })
        .await
        .expect("NodeStageVolume succeeds against the real driver");

    let published = client
        .node_publish(pb::NodePublishVolumeRequest {
            volume_id: volume.volume_id.clone(),
            publish_context: HashMap::new(),
            staging_target_path: staging.display().to_string(),
            target_path: target.display().to_string(),
            volume_capability: Some(cap()),
            readonly: false,
            secrets: HashMap::new(),
            volume_context: volume.volume_context,
        })
        .await;

    match published {
        // On Linux this is the real answer and the differential is complete.
        Ok(()) => {}
        Err(e) => {
            let msg = e.to_string();
            // The distinction that makes this a useful assertion rather
            // than a shrug: a PLATFORM refusal proves our request was
            // well-formed all the way through the driver's validation. A
            // protocol error would mean engenho sent something wrong.
            assert!(
                msg.contains("not supported") || msg.contains("mount"),
                "publish failed for a PROTOCOL reason, which is our bug: {msg}"
            );
            assert!(
                !msg.contains("InvalidArgument"),
                "the driver rejected engenho's request as malformed: {msg}"
            );
        }
    }

    // Teardown is best-effort: on darwin nothing was mounted to unwind.
    let _ = client
        .node_unpublish(pb::NodeUnpublishVolumeRequest {
            volume_id: volume.volume_id.clone(),
            target_path: target.display().to_string(),
        })
        .await;
    let _ = client
        .node_unstage(pb::NodeUnstageVolumeRequest {
            volume_id: volume.volume_id.clone(),
            staging_target_path: staging.display().to_string(),
        })
        .await;
    client
        .delete_volume(pb::DeleteVolumeRequest {
            volume_id: volume.volume_id,
            secrets: HashMap::new(),
        })
        .await
        .expect("delete");
}
