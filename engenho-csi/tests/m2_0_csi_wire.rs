//! M2.0 — the CSI wire, against a REAL gRPC driver on a REAL unix socket.
//!
//! Not a mock. Every test here goes through the generated tonic server, the
//! real protobuf encoding and an actual `UnixStream`. A CSI client tested
//! against a hand-written double only proves our encoder agrees with our
//! decoder.
//!
//! * **C1** capabilities are READ, not assumed
//! * **C2** a node-only driver does not fail the probe
//! * **C3** `Probe` with an absent `ready` means READY
//! * **C4** the node path: stage → publish → unpublish → unstage
//! * **C5** the controller path: create → attach → detach → delete
//! * **C6** an RPC failure names the RPC
//! * **R1** registration performs the full four-step handshake
//! * **R2** the status callback fires on SUCCESS
//! * **R3** the status callback fires on FAILURE too, carrying the reason
//! * **R4** a non-CSI plugin is refused by type, not dialed as storage
//! * **R5** a version engenho cannot speak is refused by name
//! * **R6** one broken plugin does not hide the working ones

use std::collections::HashMap;

use engenho_csi::client::CsiClient;
use engenho_csi::pb;
use engenho_csi::registry::{PluginRegistry, RegistrationError};
use engenho_csi::testdriver::{DriverCall, DriverConfig, TestDriver};

fn cap_value() -> pb::VolumeCapability {
    pb::VolumeCapability {
        access_type: Some(pb::volume_capability::AccessType::Mount(
            pb::volume_capability::MountVolume::default(),
        )),
        access_mode: Some(pb::volume_capability::AccessMode {
            mode: pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

// The proto field IS an `Option`, so returning one here is the shape every
// call site wants; unwrapping and re-wrapping at eight sites would be worse.
#[allow(clippy::unnecessary_wraps)]
fn cap() -> Option<pb::VolumeCapability> {
    Some(cap_value())
}

/// C1 — the flags come off the wire.
///
/// Assuming them is not a harmless simplification: calling
/// `ControllerPublishVolume` on a driver that does not declare
/// `PUBLISH_UNPUBLISH_VOLUME` returns `Unimplemented`, which a naive caller
/// retries forever.
#[tokio::test]
async fn driver_capabilities_are_read_from_the_driver() {
    let d = TestDriver::start_default().await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();

    let info = c.info().await.unwrap();
    assert_eq!(info.name, "test.csi.engenho.io");
    assert_eq!(info.node_id, "node-1");
    assert!(info.has_controller_service);
    assert!(info.stage_unstage);
    assert!(info.publish_unpublish);

    d.stop().await;
}

/// C2 — a node-only driver is a normal deployment, not an error.
///
/// It genuinely FAILS `ControllerGetCapabilities` with `Unimplemented`. A
/// client that treats that as fatal rejects a whole class of shipping
/// drivers (every local-storage one).
#[tokio::test]
async fn a_node_only_driver_probes_cleanly_with_the_controller_half_off() {
    let d = TestDriver::start(DriverConfig::node_only("local.csi.engenho.io")).await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();

    let info = c
        .info()
        .await
        .expect("a node-only driver must not fail the probe");
    assert_eq!(info.name, "local.csi.engenho.io");
    assert!(!info.has_controller_service);
    assert!(
        !info.stage_unstage,
        "no staging: publish is called directly"
    );
    assert!(!info.publish_unpublish);

    d.stop().await;
}

/// C3 — `ready` is a `BoolValue` so a driver can decline to answer, and the
/// spec defines no-answer as READY. Defaulting the other way leaves every
/// such driver permanently unready.
#[tokio::test]
async fn a_driver_that_declines_to_answer_probe_is_ready() {
    let d = TestDriver::start(DriverConfig {
        ready: None,
        ..DriverConfig::default()
    })
    .await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();
    assert!(c.probe().await.unwrap(), "an absent `ready` means ready");
    d.stop().await;

    let d = TestDriver::start(DriverConfig {
        ready: Some(false),
        ..DriverConfig::default()
    })
    .await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();
    assert!(
        !c.probe().await.unwrap(),
        "an explicit false is still false"
    );
    d.stop().await;
}

/// C4 — the node path, in the order the spec fixes.
#[tokio::test]
async fn the_node_path_stages_publishes_and_unwinds_in_order() {
    let d = TestDriver::start_default().await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();

    c.node_stage(pb::NodeStageVolumeRequest {
        volume_id: "vol-1".into(),
        publish_context: HashMap::new(),
        staging_target_path: "/stage/vol-1".into(),
        volume_capability: cap(),
        secrets: HashMap::new(),
        volume_context: HashMap::new(),
    })
    .await
    .unwrap();

    c.node_publish(pb::NodePublishVolumeRequest {
        volume_id: "vol-1".into(),
        publish_context: HashMap::new(),
        staging_target_path: "/stage/vol-1".into(),
        target_path: "/pods/p/volumes/vol-1".into(),
        volume_capability: cap(),
        readonly: true,
        secrets: HashMap::new(),
        volume_context: HashMap::new(),
    })
    .await
    .unwrap();

    c.node_unpublish(pb::NodeUnpublishVolumeRequest {
        volume_id: "vol-1".into(),
        target_path: "/pods/p/volumes/vol-1".into(),
    })
    .await
    .unwrap();

    c.node_unstage(pb::NodeUnstageVolumeRequest {
        volume_id: "vol-1".into(),
        staging_target_path: "/stage/vol-1".into(),
    })
    .await
    .unwrap();

    assert_eq!(
        d.calls(),
        vec![
            DriverCall::NodeStage("vol-1".into(), "/stage/vol-1".into()),
            DriverCall::NodePublish("vol-1".into(), "/pods/p/volumes/vol-1".into(), true),
            DriverCall::NodeUnpublish("vol-1".into(), "/pods/p/volumes/vol-1".into()),
            DriverCall::NodeUnstage("vol-1".into(), "/stage/vol-1".into()),
        ],
        "the driver saw exactly the four calls, in order, with readonly carried"
    );

    d.stop().await;
}

/// C5 — the controller path.
#[tokio::test]
async fn the_controller_path_provisions_attaches_detaches_and_deletes() {
    let d = TestDriver::start_default().await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();

    let vol = c
        .create_volume(pb::CreateVolumeRequest {
            name: "pvc-abc".into(),
            capacity_range: Some(pb::CapacityRange {
                required_bytes: 1024 * 1024 * 1024,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![cap_value()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            volume_content_source: None,
            accessibility_requirements: None,
            mutable_parameters: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(vol.volume_id, "vol-1");
    assert_eq!(vol.capacity_bytes, 1024 * 1024 * 1024);
    assert_eq!(d.volumes(), vec!["vol-1"]);

    let ctx = c
        .controller_publish(pb::ControllerPublishVolumeRequest {
            volume_id: vol.volume_id.clone(),
            node_id: "node-1".into(),
            volume_capability: cap(),
            readonly: false,
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        })
        .await
        .unwrap();
    // The publish context is not decoration: it is how the node stage call
    // learns which device to mount.
    assert_eq!(
        ctx.get("devicePath").map(String::as_str),
        Some("/dev/test0")
    );

    c.controller_unpublish(pb::ControllerUnpublishVolumeRequest {
        volume_id: vol.volume_id.clone(),
        node_id: "node-1".into(),
        secrets: HashMap::new(),
    })
    .await
    .unwrap();

    c.delete_volume(pb::DeleteVolumeRequest {
        volume_id: vol.volume_id.clone(),
        secrets: HashMap::new(),
    })
    .await
    .unwrap();
    assert!(d.volumes().is_empty(), "the volume is gone");

    d.stop().await;
}

/// C6 — a failure names the RPC, because "CSI failed" in a log is useless.
#[tokio::test]
async fn an_rpc_failure_names_which_rpc_broke() {
    let d = TestDriver::start(DriverConfig {
        fail_node_rpcs: Some("disk on fire".into()),
        ..DriverConfig::default()
    })
    .await;
    let c = CsiClient::dial(&d.driver_socket().display().to_string())
        .await
        .unwrap();

    let e = c
        .node_publish(pb::NodePublishVolumeRequest {
            volume_id: "vol-1".into(),
            target_path: "/t".into(),
            volume_capability: cap(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("NodePublishVolume"), "{msg}");
    assert!(
        msg.contains("disk on fire"),
        "the driver's reason survives: {msg}"
    );

    d.stop().await;
}

/// R1 + R2 — the four-step handshake, including the callback.
#[tokio::test]
async fn registration_completes_the_handshake_and_reports_success() {
    let d = TestDriver::start_default().await;
    let registry = PluginRegistry::new(d.dir());

    let plugin = registry
        .register_one(&d.registration_socket())
        .await
        .expect("registration");

    assert_eq!(plugin.info.name, "test.csi.engenho.io");
    // The whole reason for two sockets: GetInfo redirected us to the
    // DRIVER socket, and that is what we interrogated.
    assert_eq!(plugin.endpoint, d.driver_socket().display().to_string());
    assert_ne!(
        plugin.endpoint,
        d.registration_socket().display().to_string()
    );

    // R2 — a registrar that never hears back re-registers in a loop, so
    // this callback is what stops a healthy driver from appearing to flap.
    assert!(
        d.calls()
            .contains(&DriverCall::RegistrationStatus(true, String::new())),
        "got {:?}",
        d.calls()
    );

    d.stop().await;
}

/// R3 + R4 — a device plugin is refused BY TYPE, and told why.
#[tokio::test]
async fn a_non_csi_plugin_is_refused_by_type_and_told_the_reason() {
    let d = TestDriver::start(DriverConfig {
        plugin_type: "DevicePlugin".into(),
        ..DriverConfig::default()
    })
    .await;
    let registry = PluginRegistry::new(d.dir());

    let e = registry
        .register_one(&d.registration_socket())
        .await
        .unwrap_err();
    assert!(
        matches!(e, RegistrationError::NotCsi { .. }),
        "a GPU plugin must not be dialed as storage: {e:?}"
    );

    // R3 — the failure callback carries the reason, which is how
    // `kubectl logs` on the registrar can explain a rejection.
    let statuses: Vec<_> = d
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            DriverCall::RegistrationStatus(ok, err) => Some((ok, err)),
            _ => None,
        })
        .collect();
    assert_eq!(statuses.len(), 1, "got {statuses:?}");
    assert!(!statuses[0].0, "reported as NOT registered");
    assert!(statuses[0].1.contains("DevicePlugin"), "{}", statuses[0].1);

    d.stop().await;
}

/// R5 — an unspeakable version is refused by name.
#[tokio::test]
async fn a_version_engenho_cannot_speak_is_refused() {
    let d = TestDriver::start(DriverConfig {
        supported_versions: vec!["0.3.0".into()],
        ..DriverConfig::default()
    })
    .await;
    let e = PluginRegistry::new(d.dir())
        .register_one(&d.registration_socket())
        .await
        .unwrap_err();
    assert!(
        matches!(e, RegistrationError::VersionMismatch { .. }),
        "{e:?}"
    );
    d.stop().await;
}

/// R6 — one broken plugin must not hide the working ones.
///
/// A silently-skipped driver is how a storage outage becomes
/// unexplainable, so `scan` returns BOTH halves.
#[tokio::test]
async fn a_broken_plugin_does_not_hide_a_working_one() {
    let good = TestDriver::start(DriverConfig {
        name: "good.csi.engenho.io".into(),
        ..DriverConfig::default()
    })
    .await;
    let bad = TestDriver::start(DriverConfig {
        name: "bad.csi.engenho.io".into(),
        plugin_type: "DevicePlugin".into(),
        ..DriverConfig::default()
    })
    .await;

    // One registry directory holding both registration sockets, which is
    // the real deployment shape.
    let dir = tempfile::tempdir().unwrap();
    for (d, link) in [(&good, "good-reg.sock"), (&bad, "bad-reg.sock")] {
        std::os::unix::fs::symlink(d.registration_socket(), dir.path().join(link)).unwrap();
    }

    let (found, failed) = PluginRegistry::new(dir.path()).scan().await.unwrap();
    assert_eq!(
        found.keys().collect::<Vec<_>>(),
        vec!["good.csi.engenho.io"],
        "the working driver registered"
    );
    assert_eq!(
        failed.len(),
        1,
        "and the broken one is REPORTED, not dropped"
    );

    good.stop().await;
    bad.stop().await;
}
