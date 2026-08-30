//! `LocalPathDriver` — engenho's own CSI driver, in Rust.
//!
//! ★ THE NATURALIZED ARTIFACT. Not a test double: a conformant CSI driver
//! any runtime can register — kubelet, engenho, anything that speaks the
//! plugin-registration protocol. It backs volumes with node-local
//! directories, which is what `rancher.io/local-path` and
//! `csi-driver-host-path` do, and what engenho's own `PvBinder` already
//! does behind a private code path.
//!
//! ★ WHY THIS EXISTS WHEN THE BINDER ALREADY PROVISIONS LOCAL PATHS. The
//! binder's local-path branch is engenho talking to itself: a private
//! shape, invisible to anything outside the process, untestable by any
//! external tool. The same capability behind the CSI contract is
//! addressable by every piece of storage tooling in the ecosystem, and —
//! the part that matters — it becomes measurable against
//! `csi-driver-host-path`, because both now answer the same RPCs. A
//! capability we cannot compare is a capability we cannot claim.
//!
//! ★ WHAT IT DELIBERATELY DOES NOT DO. No bind mounts. `NodePublishVolume`
//! makes the volume's data appear at the target path by SYMLINK, not by
//! `mount(2)`. That is why this driver runs on darwin where
//! `csi-driver-host-path` cannot — its `k8s.io/mount-utils` refuses the
//! platform — and it is an honest limitation rather than a hidden one: a
//! symlink is visible to a process in the same mount namespace and is NOT
//! equivalent to a bind mount for a container with its own. The Linux path
//! that needs a real mount is named, not faked.
//! `pending-localpath-driver: bind-mount on Linux`
//!
//! ★ STATE IS ON DISK, NOT IN MEMORY. A driver that forgets its volumes on
//! restart hands the runtime a `NodePublishVolume` for a volume it has
//! never heard of, and the pod fails in a way that looks like corruption.
//! One JSON file per volume beside the data.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::pb;

/// The driver name engenho registers under.
///
/// A reverse-DNS name we own, because a driver name is a cluster-wide key:
/// two drivers sharing one would each see the other's volumes.
pub const DRIVER_NAME: &str = "localpath.csi.engenho.io";

/// What the driver records about one volume, so a restart does not lose it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeRecord {
    /// The driver's id for the volume.
    pub volume_id: String,
    /// The name the runtime asked for — the idempotency key.
    pub name: String,
    /// Requested capacity in bytes.
    pub capacity_bytes: i64,
    /// Where the data lives.
    pub data_path: PathBuf,
}

/// A node-local, directory-backed CSI driver.
#[derive(Debug, Clone)]
pub struct LocalPathDriver {
    root: PathBuf,
    node_id: String,
}

impl LocalPathDriver {
    /// New driver rooted at `root`, reporting `node_id`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, node_id: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            node_id: node_id.into(),
        }
    }

    /// Where volumes and their records live.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn volumes_dir(&self) -> PathBuf {
        self.root.join("volumes")
    }

    fn record_path(&self, volume_id: &str) -> PathBuf {
        self.volumes_dir().join(format!("{volume_id}.json"))
    }

    /// A volume id derived from the requested NAME.
    ///
    /// ★ DERIVED, NOT RANDOM, AND THAT IS THE IDEMPOTENCY GUARANTEE.
    /// `CreateVolume` is specified idempotent on the name: a runtime
    /// retrying after a timeout must get the same volume back. A random id
    /// would provision a second directory on every retry and nothing would
    /// ever reclaim the first.
    #[must_use]
    pub fn volume_id_for(name: &str) -> String {
        // Sanitised so the id is safe as a filename. A name is operator
        // input and can contain anything.
        let safe: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        format!("pvc-{safe}")
    }

    /// Read a volume's record.
    #[must_use]
    pub fn record(&self, volume_id: &str) -> Option<VolumeRecord> {
        let bytes = std::fs::read(self.record_path(volume_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Every volume this driver holds.
    #[must_use]
    pub fn volumes(&self) -> Vec<VolumeRecord> {
        let Ok(entries) = std::fs::read_dir(self.volumes_dir()) else {
            return Vec::new();
        };
        let mut out: Vec<VolumeRecord> = entries
            .filter_map(Result::ok)
            .filter_map(|e| std::fs::read(e.path()).ok())
            .filter_map(|b| serde_json::from_slice(&b).ok())
            .collect();
        out.sort_by(|a: &VolumeRecord, b| a.volume_id.cmp(&b.volume_id));
        out
    }

    fn write_record(&self, record: &VolumeRecord) -> Result<(), Status> {
        std::fs::create_dir_all(self.volumes_dir())
            .map_err(|e| Status::internal(format!("state dir: {e}")))?;
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|e| Status::internal(format!("encode record: {e}")))?;
        std::fs::write(self.record_path(&record.volume_id), bytes)
            .map_err(|e| Status::internal(format!("write record: {e}")))
    }
}

#[tonic::async_trait]
impl pb::identity_server::Identity for LocalPathDriver {
    async fn get_plugin_info(
        &self,
        _r: Request<pb::GetPluginInfoRequest>,
    ) -> Result<Response<pb::GetPluginInfoResponse>, Status> {
        Ok(Response::new(pb::GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            // ★ NEVER EMPTY. csi-driver-host-path refuses GetPluginInfo
            // with `Unavailable: Driver is missing version` when built
            // without one — measured 2026-08-30 by building it that way.
            // A driver that will not identify itself cannot be registered.
            vendor_version: env!("CARGO_PKG_VERSION").to_string(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _r: Request<pb::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<pb::GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(pb::GetPluginCapabilitiesResponse {
            capabilities: vec![pb::PluginCapability {
                r#type: Some(pb::plugin_capability::Type::Service(
                    pb::plugin_capability::Service {
                        r#type: pb::plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            }],
        }))
    }

    async fn probe(
        &self,
        _r: Request<pb::ProbeRequest>,
    ) -> Result<Response<pb::ProbeResponse>, Status> {
        // Ready once the root is writable — a driver that reports ready and
        // then fails every CreateVolume is worse than one that says no.
        let ready = std::fs::create_dir_all(&self.root).is_ok();
        Ok(Response::new(pb::ProbeResponse { ready: Some(ready) }))
    }
}

#[tonic::async_trait]
impl pb::controller_server::Controller for LocalPathDriver {
    async fn create_volume(
        &self,
        r: Request<pb::CreateVolumeRequest>,
    ) -> Result<Response<pb::CreateVolumeResponse>, Status> {
        let r = r.into_inner();
        if r.name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        let volume_id = Self::volume_id_for(&r.name);
        let requested = r.capacity_range.as_ref().map_or(0, |c| c.required_bytes);

        // Idempotent on the name. A retry after a timeout returns the SAME
        // volume rather than provisioning a second directory nobody
        // reclaims.
        if let Some(existing) = self.record(&volume_id) {
            // ★ AND IT MUST REFUSE A CONFLICTING RE-REQUEST. The spec says
            // ALREADY_EXISTS when a volume of this name exists with
            // incompatible parameters. Silently returning the smaller
            // volume would give a workload less storage than it asked for
            // with no error anywhere.
            if requested > existing.capacity_bytes {
                return Err(Status::already_exists(format!(
                    "volume {} exists with {} bytes, {} requested",
                    r.name, existing.capacity_bytes, requested
                )));
            }
            return Ok(Response::new(pb::CreateVolumeResponse {
                volume: Some(pb::Volume {
                    capacity_bytes: existing.capacity_bytes,
                    volume_id: existing.volume_id,
                    volume_context: HashMap::new(),
                    content_source: None,
                    accessible_topology: Vec::new(),
                }),
            }));
        }

        let data_path = self.root.join("data").join(&volume_id);
        std::fs::create_dir_all(&data_path)
            .map_err(|e| Status::internal(format!("create {}: {e}", data_path.display())))?;

        let record = VolumeRecord {
            volume_id: volume_id.clone(),
            name: r.name,
            capacity_bytes: requested,
            data_path: data_path.clone(),
        };
        self.write_record(&record)?;

        Ok(Response::new(pb::CreateVolumeResponse {
            volume: Some(pb::Volume {
                capacity_bytes: requested,
                volume_id,
                volume_context: HashMap::from([(
                    "dataPath".to_string(),
                    data_path.display().to_string(),
                )]),
                content_source: None,
                accessible_topology: Vec::new(),
            }),
        }))
    }

    async fn delete_volume(
        &self,
        r: Request<pb::DeleteVolumeRequest>,
    ) -> Result<Response<pb::DeleteVolumeResponse>, Status> {
        let r = r.into_inner();
        // Deleting an unknown volume is SUCCESS per the spec, and engenho's
        // own teardown relies on it: a retry after a partial failure must
        // not wedge.
        if let Some(record) = self.record(&r.volume_id) {
            let _ = std::fs::remove_dir_all(&record.data_path);
            let _ = std::fs::remove_file(self.record_path(&r.volume_id));
        }
        Ok(Response::new(pb::DeleteVolumeResponse {}))
    }

    async fn controller_get_capabilities(
        &self,
        _r: Request<pb::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<pb::ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(pb::ControllerGetCapabilitiesResponse {
            // CREATE_DELETE only. No PUBLISH_UNPUBLISH: this driver's
            // volumes are node-local and cannot move between nodes, so
            // declaring attach would invite a controller to call an RPC
            // that can never mean anything here.
            capabilities: vec![pb::ControllerServiceCapability {
                r#type: Some(pb::controller_service_capability::Type::Rpc(
                    pb::controller_service_capability::Rpc {
                        r#type: pb::controller_service_capability::rpc::Type::CreateDeleteVolume
                            as i32,
                    },
                )),
            }],
        }))
    }

    async fn controller_publish_volume(
        &self,
        _r: Request<pb::ControllerPublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "local-path volumes are node-local and never attach",
        ))
    }

    async fn controller_unpublish_volume(
        &self,
        _r: Request<pb::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<pb::ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "local-path volumes are node-local and never attach",
        ))
    }

    async fn validate_volume_capabilities(
        &self,
        r: Request<pb::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<pb::ValidateVolumeCapabilitiesResponse>, Status> {
        let r = r.into_inner();
        if self.record(&r.volume_id).is_none() {
            return Err(Status::not_found(format!("no volume {}", r.volume_id)));
        }
        // ★ MULTI-NODE MODES ARE REFUSED BY OMISSION, WHICH IS HOW THE SPEC
        // SAYS NO. A confirmed empty `confirmed` field means "not
        // supported"; returning the capabilities unconditionally would tell
        // a scheduler it may place two writers on two nodes against a
        // volume that exists on one.
        let single_node = r.volume_capabilities.iter().all(|c| {
            c.access_mode.as_ref().is_none_or(|m| {
                m.mode == pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32
                    || m.mode
                        == pb::volume_capability::access_mode::Mode::SingleNodeReaderOnly as i32
            })
        });
        Ok(Response::new(pb::ValidateVolumeCapabilitiesResponse {
            confirmed: single_node.then(|| pb::validate_volume_capabilities_response::Confirmed {
                volume_context: HashMap::new(),
                volume_capabilities: r.volume_capabilities,
                parameters: HashMap::new(),
                mutable_parameters: HashMap::new(),
            }),
            message: if single_node {
                String::new()
            } else {
                "local-path volumes exist on one node and support single-node access only".into()
            },
        }))
    }

    async fn list_volumes(
        &self,
        _r: Request<pb::ListVolumesRequest>,
    ) -> Result<Response<pb::ListVolumesResponse>, Status> {
        Ok(Response::new(pb::ListVolumesResponse {
            entries: self
                .volumes()
                .into_iter()
                .map(|v| pb::list_volumes_response::Entry {
                    volume: Some(pb::Volume {
                        capacity_bytes: v.capacity_bytes,
                        volume_id: v.volume_id,
                        volume_context: HashMap::new(),
                        content_source: None,
                        accessible_topology: Vec::new(),
                    }),
                    status: None,
                })
                .collect(),
            next_token: String::new(),
        }))
    }

    async fn get_capacity(
        &self,
        _r: Request<pb::GetCapacityRequest>,
    ) -> Result<Response<pb::GetCapacityResponse>, Status> {
        Err(Status::unimplemented("GetCapacity"))
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
impl pb::node_server::Node for LocalPathDriver {
    async fn node_get_info(
        &self,
        _r: Request<pb::NodeGetInfoRequest>,
    ) -> Result<Response<pb::NodeGetInfoResponse>, Status> {
        Ok(Response::new(pb::NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }

    async fn node_get_capabilities(
        &self,
        _r: Request<pb::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<pb::NodeGetCapabilitiesResponse>, Status> {
        // No STAGE_UNSTAGE: there is nothing to stage once per node when
        // the volume is already a directory on it. Declaring it would make
        // every runtime issue a stage call with nothing to do — and, worse,
        // make a runtime that skipped staging look wrong.
        Ok(Response::new(pb::NodeGetCapabilitiesResponse {
            capabilities: Vec::new(),
        }))
    }

    async fn node_publish_volume(
        &self,
        r: Request<pb::NodePublishVolumeRequest>,
    ) -> Result<Response<pb::NodePublishVolumeResponse>, Status> {
        let r = r.into_inner();
        let Some(record) = self.record(&r.volume_id) else {
            return Err(Status::not_found(format!("no volume {}", r.volume_id)));
        };
        if r.target_path.is_empty() {
            return Err(Status::invalid_argument("target_path is required"));
        }
        let target = PathBuf::from(&r.target_path);

        // Idempotent: publishing an already-published volume is success.
        // A runtime retrying after a timeout must not get an error for
        // work that already happened.
        if target.exists() {
            let already = std::fs::read_link(&target)
                .map(|dest| dest == record.data_path)
                .unwrap_or(false);
            if already {
                return Ok(Response::new(pb::NodePublishVolumeResponse {}));
            }
            // A pre-existing EMPTY directory is what the kubelet creates
            // before calling us, so it is removed rather than treated as a
            // conflict. A non-empty one is somebody else's data.
            let empty = std::fs::read_dir(&target).is_ok_and(|mut d| d.next().is_none());
            if !empty {
                return Err(Status::already_exists(format!(
                    "{} exists and is not this volume",
                    target.display()
                )));
            }
            std::fs::remove_dir(&target)
                .map_err(|e| Status::internal(format!("clear target: {e}")))?;
        }

        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Status::internal(format!("target parent: {e}")))?;
        }
        // A SYMLINK, not a bind mount — see the module header. Honest and
        // portable; not equivalent for a container in its own mount
        // namespace, which is why the Linux bind-mount path is named as
        // pending rather than quietly assumed.
        #[cfg(unix)]
        std::os::unix::fs::symlink(&record.data_path, &target)
            .map_err(|e| Status::internal(format!("publish {}: {e}", target.display())))?;

        Ok(Response::new(pb::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        r: Request<pb::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<pb::NodeUnpublishVolumeResponse>, Status> {
        let r = r.into_inner();
        // Unpublishing what was never published is SUCCESS: teardown runs
        // on a path that may already be half-complete.
        let target = PathBuf::from(&r.target_path);
        if target.symlink_metadata().is_ok() {
            let _ = std::fs::remove_file(&target);
        }
        Ok(Response::new(pb::NodeUnpublishVolumeResponse {}))
    }

    async fn node_stage_volume(
        &self,
        _r: Request<pb::NodeStageVolumeRequest>,
    ) -> Result<Response<pb::NodeStageVolumeResponse>, Status> {
        // Not declared in NodeGetCapabilities, so a conformant runtime
        // never calls this. Refusing rather than silently succeeding keeps
        // a runtime that DOES call it honest about the mismatch.
        Err(Status::unimplemented(
            "this driver does not declare STAGE_UNSTAGE_VOLUME",
        ))
    }

    async fn node_unstage_volume(
        &self,
        _r: Request<pb::NodeUnstageVolumeRequest>,
    ) -> Result<Response<pb::NodeUnstageVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "this driver does not declare STAGE_UNSTAGE_VOLUME",
        ))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_volume_id_is_derived_from_the_name_so_retries_are_idempotent() {
        // A random id would provision a second directory on every retry and
        // nothing would ever reclaim the first.
        assert_eq!(
            LocalPathDriver::volume_id_for("pvc-ns-claim"),
            LocalPathDriver::volume_id_for("pvc-ns-claim")
        );
        assert_ne!(
            LocalPathDriver::volume_id_for("a"),
            LocalPathDriver::volume_id_for("b")
        );
    }

    #[test]
    fn a_name_with_path_characters_cannot_escape_the_volumes_directory() {
        // A volume name is operator input and becomes a filename.
        let id = LocalPathDriver::volume_id_for("../../etc/passwd");
        assert!(!id.contains('/'), "{id}");
        assert!(!id.contains(".."), "{id}");
    }
}
