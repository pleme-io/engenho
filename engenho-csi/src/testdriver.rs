//! A REAL CSI driver, in-process, on a real unix socket.
//!
//! ★ WHY THIS IS NOT A MOCK, AND WHY THAT DISTINCTION IS THE POINT. A CSI
//! client tested against a hand-written double proves that our encoder
//! agrees with our decoder — the one thing that cannot fail. This is a
//! generated `tonic` server speaking the generated wire format over a real
//! `UnixStream`, so a test against it exercises the transport, the
//! protobuf encoding, the gRPC framing and the capability negotiation
//! exactly as a vendor's driver would.
//!
//! It is behind `#[cfg(any(test, feature = "test-driver"))]` so it does not
//! ship in a production build, and it is deliberately IN THE CRATE rather
//! than in `tests/` so the integration tests and any downstream crate's
//! tests share ONE reference driver instead of each growing their own,
//! divergent, idea of how a driver behaves.
//!
//! ★ IT IS CONFIGURABLE IN THE WAYS REAL DRIVERS DIFFER, because those
//! differences are exactly what breaks a naive client: a node-only driver
//! with no controller service, a driver that does not want staging, a
//! driver that reports `ready=false` while it warms up. Each is a
//! deployment shape that ships today, not a hypothetical.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::pb;
use crate::reg;

/// Every call the driver received, in order — the assertion surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverCall {
    /// `NodeStageVolume(volume_id, staging_target_path)`
    NodeStage(String, String),
    /// `NodeUnstageVolume(volume_id, staging_target_path)`
    NodeUnstage(String, String),
    /// `NodePublishVolume(volume_id, target_path, readonly)`
    NodePublish(String, String, bool),
    /// `NodeUnpublishVolume(volume_id, target_path)`
    NodeUnpublish(String, String),
    /// `CreateVolume(name, capacity_bytes)`
    CreateVolume(String, i64),
    /// `DeleteVolume(volume_id)`
    DeleteVolume(String),
    /// `ControllerPublishVolume(volume_id, node_id)`
    ControllerPublish(String, String),
    /// `ControllerUnpublishVolume(volume_id, node_id)`
    ControllerUnpublish(String, String),
    /// `NotifyRegistrationStatus(registered, error)`
    RegistrationStatus(bool, String),
}

/// How this driver should behave — the axes real drivers differ on.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// The driver's name.
    pub name: String,
    /// The node id it reports for this node.
    pub node_id: String,
    /// Declare the controller service (provision / attach).
    pub controller_service: bool,
    /// Declare `STAGE_UNSTAGE_VOLUME`.
    pub stage_unstage: bool,
    /// Declare `PUBLISH_UNPUBLISH_VOLUME`.
    pub publish_unpublish: bool,
    /// `Probe` answer: `None` means the driver declines to say, which the
    /// spec defines as READY and which a client must not read as unready.
    pub ready: Option<bool>,
    /// Registration `type` — overridable so a test can present a device
    /// plugin and prove engenho refuses to dial it as storage.
    pub plugin_type: String,
    /// Versions offered at registration.
    pub supported_versions: Vec<String>,
    /// Fail every Node RPC with this message, to exercise the error path.
    pub fail_node_rpcs: Option<String>,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            name: "test.csi.engenho.io".into(),
            node_id: "node-1".into(),
            controller_service: true,
            stage_unstage: true,
            publish_unpublish: true,
            ready: Some(true),
            plugin_type: crate::registry::CSI_PLUGIN_TYPE.into(),
            supported_versions: vec![crate::registry::SUPPORTED_CSI_VERSION.into()],
            fail_node_rpcs: None,
        }
    }
}

impl DriverConfig {
    /// A node-only driver: no controller service, no staging. This is the
    /// shape a naive client breaks on, so it has a constructor.
    #[must_use]
    pub fn node_only(name: &str) -> Self {
        Self {
            name: name.into(),
            controller_service: false,
            stage_unstage: false,
            publish_unpublish: false,
            ..Self::default()
        }
    }
}

/// Shared driver state.
#[derive(Debug, Default)]
struct State {
    calls: Vec<DriverCall>,
    volumes: HashMap<String, i64>,
    next_id: u64,
}

/// A running in-process CSI driver.
#[derive(Debug)]
pub struct TestDriver {
    config: DriverConfig,
    state: Arc<Mutex<State>>,
    dir: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    joined: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Clone)]
struct Svc {
    config: DriverConfig,
    state: Arc<Mutex<State>>,
    driver_socket: PathBuf,
}

impl Svc {
    fn record(&self, call: DriverCall) {
        self.state.lock().expect("driver state").calls.push(call);
    }

    fn node_guard(&self) -> Result<(), Status> {
        match &self.config.fail_node_rpcs {
            Some(msg) => Err(Status::internal(msg.clone())),
            None => Ok(()),
        }
    }
}

#[tonic::async_trait]
impl pb::identity_server::Identity for Svc {
    async fn get_plugin_info(
        &self,
        _r: Request<pb::GetPluginInfoRequest>,
    ) -> Result<Response<pb::GetPluginInfoResponse>, Status> {
        Ok(Response::new(pb::GetPluginInfoResponse {
            name: self.config.name.clone(),
            vendor_version: "0.1.0-test".into(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _r: Request<pb::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<pb::GetPluginCapabilitiesResponse>, Status> {
        let mut capabilities = Vec::new();
        if self.config.controller_service {
            capabilities.push(pb::PluginCapability {
                r#type: Some(pb::plugin_capability::Type::Service(
                    pb::plugin_capability::Service {
                        r#type: pb::plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            });
        }
        Ok(Response::new(pb::GetPluginCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn probe(
        &self,
        _r: Request<pb::ProbeRequest>,
    ) -> Result<Response<pb::ProbeResponse>, Status> {
        Ok(Response::new(pb::ProbeResponse {
            ready: self.config.ready,
        }))
    }
}

#[tonic::async_trait]
impl pb::node_server::Node for Svc {
    async fn node_stage_volume(
        &self,
        r: Request<pb::NodeStageVolumeRequest>,
    ) -> Result<Response<pb::NodeStageVolumeResponse>, Status> {
        self.node_guard()?;
        let r = r.into_inner();
        self.record(DriverCall::NodeStage(r.volume_id, r.staging_target_path));
        Ok(Response::new(pb::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        r: Request<pb::NodeUnstageVolumeRequest>,
    ) -> Result<Response<pb::NodeUnstageVolumeResponse>, Status> {
        self.node_guard()?;
        let r = r.into_inner();
        self.record(DriverCall::NodeUnstage(r.volume_id, r.staging_target_path));
        Ok(Response::new(pb::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        r: Request<pb::NodePublishVolumeRequest>,
    ) -> Result<Response<pb::NodePublishVolumeResponse>, Status> {
        self.node_guard()?;
        let r = r.into_inner();
        self.record(DriverCall::NodePublish(
            r.volume_id,
            r.target_path,
            r.readonly,
        ));
        Ok(Response::new(pb::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        r: Request<pb::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<pb::NodeUnpublishVolumeResponse>, Status> {
        self.node_guard()?;
        let r = r.into_inner();
        self.record(DriverCall::NodeUnpublish(r.volume_id, r.target_path));
        Ok(Response::new(pb::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        _r: Request<pb::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<pb::NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("NodeGetVolumeStats"))
    }

    async fn node_expand_volume(
        &self,
        _r: Request<pb::NodeExpandVolumeRequest>,
    ) -> Result<Response<pb::NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("NodeExpandVolume"))
    }

    async fn node_get_capabilities(
        &self,
        _r: Request<pb::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<pb::NodeGetCapabilitiesResponse>, Status> {
        let mut capabilities = Vec::new();
        if self.config.stage_unstage {
            capabilities.push(pb::NodeServiceCapability {
                r#type: Some(pb::node_service_capability::Type::Rpc(
                    pb::node_service_capability::Rpc {
                        r#type: pb::node_service_capability::rpc::Type::StageUnstageVolume as i32,
                    },
                )),
            });
        }
        Ok(Response::new(pb::NodeGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn node_get_info(
        &self,
        _r: Request<pb::NodeGetInfoRequest>,
    ) -> Result<Response<pb::NodeGetInfoResponse>, Status> {
        Ok(Response::new(pb::NodeGetInfoResponse {
            node_id: self.config.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }
}

#[tonic::async_trait]
impl pb::controller_server::Controller for Svc {
    async fn create_volume(
        &self,
        r: Request<pb::CreateVolumeRequest>,
    ) -> Result<Response<pb::CreateVolumeResponse>, Status> {
        if !self.config.controller_service {
            return Err(Status::unimplemented("no controller service"));
        }
        let r = r.into_inner();
        let bytes = r.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
        self.record(DriverCall::CreateVolume(r.name.clone(), bytes));
        let mut state = self.state.lock().expect("driver state");
        state.next_id += 1;
        let volume_id = format!("vol-{}", state.next_id);
        state.volumes.insert(volume_id.clone(), bytes);
        Ok(Response::new(pb::CreateVolumeResponse {
            volume: Some(pb::Volume {
                capacity_bytes: bytes,
                volume_id,
                volume_context: HashMap::new(),
                content_source: None,
                accessible_topology: Vec::new(),
            }),
        }))
    }

    async fn delete_volume(
        &self,
        r: Request<pb::DeleteVolumeRequest>,
    ) -> Result<Response<pb::DeleteVolumeResponse>, Status> {
        if !self.config.controller_service {
            return Err(Status::unimplemented("no controller service"));
        }
        let r = r.into_inner();
        self.state
            .lock()
            .expect("driver state")
            .volumes
            .remove(&r.volume_id);
        self.record(DriverCall::DeleteVolume(r.volume_id));
        Ok(Response::new(pb::DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        r: Request<pb::ControllerPublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerPublishVolumeResponse>, Status> {
        if !self.config.publish_unpublish {
            return Err(Status::unimplemented("PUBLISH_UNPUBLISH_VOLUME"));
        }
        let r = r.into_inner();
        self.record(DriverCall::ControllerPublish(
            r.volume_id.clone(),
            r.node_id.clone(),
        ));
        Ok(Response::new(pb::ControllerPublishVolumeResponse {
            publish_context: HashMap::from([("devicePath".into(), "/dev/test0".into())]),
        }))
    }

    async fn controller_unpublish_volume(
        &self,
        r: Request<pb::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerUnpublishVolumeResponse>, Status> {
        if !self.config.publish_unpublish {
            return Err(Status::unimplemented("PUBLISH_UNPUBLISH_VOLUME"));
        }
        let r = r.into_inner();
        self.record(DriverCall::ControllerUnpublish(r.volume_id, r.node_id));
        Ok(Response::new(pb::ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        _r: Request<pb::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<pb::ValidateVolumeCapabilitiesResponse>, Status> {
        Err(Status::unimplemented("ValidateVolumeCapabilities"))
    }

    async fn list_volumes(
        &self,
        _r: Request<pb::ListVolumesRequest>,
    ) -> Result<Response<pb::ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes"))
    }

    async fn get_capacity(
        &self,
        _r: Request<pb::GetCapacityRequest>,
    ) -> Result<Response<pb::GetCapacityResponse>, Status> {
        Err(Status::unimplemented("GetCapacity"))
    }

    async fn controller_get_capabilities(
        &self,
        _r: Request<pb::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<pb::ControllerGetCapabilitiesResponse>, Status> {
        if !self.config.controller_service {
            // A node-only driver genuinely fails this RPC. The client must
            // degrade rather than treat it as fatal, and this is what makes
            // that path testable.
            return Err(Status::unimplemented("no controller service"));
        }
        let mut capabilities = vec![pb::ControllerServiceCapability {
            r#type: Some(pb::controller_service_capability::Type::Rpc(
                pb::controller_service_capability::Rpc {
                    r#type: pb::controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                },
            )),
        }];
        if self.config.publish_unpublish {
            capabilities.push(pb::ControllerServiceCapability {
                r#type: Some(pb::controller_service_capability::Type::Rpc(
                    pb::controller_service_capability::Rpc {
                        r#type: pb::controller_service_capability::rpc::Type::PublishUnpublishVolume
                            as i32,
                    },
                )),
            });
        }
        Ok(Response::new(pb::ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn create_snapshot(
        &self,
        _r: Request<pb::CreateSnapshotRequest>,
    ) -> Result<Response<pb::CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("CreateSnapshot"))
    }

    async fn delete_snapshot(
        &self,
        _r: Request<pb::DeleteSnapshotRequest>,
    ) -> Result<Response<pb::DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("DeleteSnapshot"))
    }

    async fn list_snapshots(
        &self,
        _r: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots"))
    }

    async fn controller_expand_volume(
        &self,
        _r: Request<pb::ControllerExpandVolumeRequest>,
    ) -> Result<Response<pb::ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerExpandVolume"))
    }

    async fn controller_get_volume(
        &self,
        _r: Request<pb::ControllerGetVolumeRequest>,
    ) -> Result<Response<pb::ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume"))
    }

    async fn controller_modify_volume(
        &self,
        _r: Request<pb::ControllerModifyVolumeRequest>,
    ) -> Result<Response<pb::ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerModifyVolume"))
    }
}

#[tonic::async_trait]
impl reg::registration_server::Registration for Svc {
    async fn get_info(
        &self,
        _r: Request<reg::InfoRequest>,
    ) -> Result<Response<reg::PluginInfo>, Status> {
        Ok(Response::new(reg::PluginInfo {
            r#type: self.config.plugin_type.clone(),
            name: self.config.name.clone(),
            // The whole point of the two-socket dance: this points at the
            // DRIVER socket, not the registration socket the caller dialed.
            endpoint: self.driver_socket.display().to_string(),
            supported_versions: self.config.supported_versions.clone(),
        }))
    }

    async fn notify_registration_status(
        &self,
        r: Request<reg::RegistrationStatus>,
    ) -> Result<Response<reg::RegistrationStatusResponse>, Status> {
        let r = r.into_inner();
        self.record(DriverCall::RegistrationStatus(r.plugin_registered, r.error));
        Ok(Response::new(reg::RegistrationStatusResponse {}))
    }
}

impl TestDriver {
    /// Start a driver, serving BOTH sockets, in a fresh temp directory.
    ///
    /// Both services are served on both sockets. That is not laziness — a
    /// single-socket driver is a legal deployment, and serving both means
    /// one harness covers the two-socket and one-socket shapes.
    ///
    /// # Panics
    /// If the sockets cannot be bound.
    pub async fn start(config: DriverConfig) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let driver_socket = dir.path().join("csi.sock");
        let registration_socket = dir.path().join("driver-reg.sock");

        let state = Arc::new(Mutex::new(State::default()));
        let svc = Svc {
            config: config.clone(),
            state: state.clone(),
            driver_socket: driver_socket.clone(),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let listeners = vec![
            tokio::net::UnixListener::bind(&driver_socket).expect("bind driver socket"),
            tokio::net::UnixListener::bind(&registration_socket).expect("bind registration socket"),
        ];

        let joined = tokio::spawn(async move {
            let mut shutdown = rx;
            let mut servers = Vec::new();
            for listener in listeners {
                let svc = svc.clone();
                servers.push(tokio::spawn(async move {
                    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
                    let _ = tonic::transport::Server::builder()
                        .add_service(pb::identity_server::IdentityServer::new(svc.clone()))
                        .add_service(pb::node_server::NodeServer::new(svc.clone()))
                        .add_service(pb::controller_server::ControllerServer::new(svc.clone()))
                        .add_service(reg::registration_server::RegistrationServer::new(svc))
                        .serve_with_incoming(incoming)
                        .await;
                }));
            }
            let _ = (&mut shutdown).await;
            for s in servers {
                s.abort();
            }
        });

        // Wait for both sockets to accept, rather than sleeping: a fixed
        // sleep is either flaky under load or slow always.
        for path in [&driver_socket, &registration_socket] {
            for _ in 0..200 {
                if tokio::net::UnixStream::connect(path).await.is_ok() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        Self {
            config,
            state,
            dir,
            shutdown: Some(tx),
            joined: Some(joined),
        }
    }

    /// Start with the default configuration.
    #[must_use]
    pub async fn start_default() -> Self {
        Self::start(DriverConfig::default()).await
    }

    /// The driver's own socket.
    #[must_use]
    pub fn driver_socket(&self) -> PathBuf {
        self.dir.path().join("csi.sock")
    }

    /// The registration socket.
    #[must_use]
    pub fn registration_socket(&self) -> PathBuf {
        self.dir.path().join("driver-reg.sock")
    }

    /// The temp directory, usable as a `plugins_registry` for the registry
    /// scanner. Note it also contains `csi.sock`, which is exactly the
    /// messy real-world case worth covering.
    #[must_use]
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// The configured name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Every call received, in order.
    ///
    /// # Panics
    /// If the state mutex was poisoned by a panic inside the driver — which
    /// is a test bug worth surfacing loudly, not recovering from.
    #[must_use]
    pub fn calls(&self) -> Vec<DriverCall> {
        self.state.lock().expect("driver state").calls.clone()
    }

    /// Volume ids the driver currently holds.
    ///
    /// # Panics
    /// On a poisoned state mutex; see [`Self::calls`].
    #[must_use]
    pub fn volumes(&self) -> Vec<String> {
        let mut v: Vec<_> = self
            .state
            .lock()
            .expect("driver state")
            .volumes
            .keys()
            .cloned()
            .collect();
        v.sort();
        v
    }

    /// Stop the driver.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.joined.take() {
            let _ = j.await;
        }
    }
}
