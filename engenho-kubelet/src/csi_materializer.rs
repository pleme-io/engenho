//! `CsiVolumeMaterializer` — the CSI node path, wired into the kubelet.
//!
//! ★ THIS IS A DECORATOR, NOT A REPLACEMENT. It wraps whatever materializer
//! the kubelet already has and adds exactly the two methods CSI needs,
//! delegating configMap / secret / emptyDir untouched. A second full
//! materializer would fork the volume path in two, and the two copies would
//! drift on the details (read-only defaults, subPath, cleanup order) that
//! took the original one several passes to get right.
//!
//! ★ THE STAGE/PUBLISH SPLIT IS THE DRIVER'S DECISION, NOT OURS.
//! `NodeStageVolume` mounts a volume ONCE per node, at a staging path;
//! `NodePublishVolume` then bind-mounts it per pod. A driver that declares
//! `STAGE_UNSTAGE_VOLUME` requires both, in that order. A driver that does
//! NOT declare it expects publish to be called directly, and staging it
//! first fails the mount in a way that reads as a broken volume rather than
//! a protocol error. So the flag is read off the driver
//! (`DriverInfo::stage_unstage`) and never assumed.
//!
//! ★ THE PATHS ARE UPSTREAM'S, AND THAT IS NOT COSMETIC. A driver may write
//! bookkeeping beside its target path, and the CSI e2e suites assert on the
//! `.../pods/<uid>/volumes/kubernetes.io~csi/<vol>/mount` shape. Choosing
//! our own layout would work right up until a driver or a test suite looked.
//!
//! ★ TEARDOWN IS BEST-EFFORT AND SAYS WHY. `unpublish` on a volume that was
//! never published, or whose driver has since gone, is SUCCESS: teardown
//! runs on a path that may already be half-complete, and erroring would
//! wedge pod deletion behind a volume that is already gone. A genuine
//! driver error is logged — losing the reason entirely is how an
//! undeletable pod becomes unexplainable.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use engenho_csi::client::CsiClient;
use engenho_csi::pb;

use crate::pod_volume::{CsiPublishRequest, MountSource, VolumeMaterializer, VolumeResolveError};

/// Where a driver is reached, and what it can do.
///
/// The kubelet holds these by driver NAME because that is the only key a PV
/// carries (`spec.csi.driver`).
#[derive(Clone)]
pub struct RegisteredDriver {
    /// A connected client.
    pub client: CsiClient,
    /// Whether the driver requires `NodeStageVolume` first.
    pub stage_unstage: bool,
    /// The driver's own id for this node.
    pub node_id: String,
}

impl std::fmt::Debug for RegisteredDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredDriver")
            .field("endpoint", &self.client.endpoint())
            .field("stage_unstage", &self.stage_unstage)
            .field("node_id", &self.node_id)
            .finish()
    }
}

/// The per-node CSI driver table.
///
/// Separate from the materializer so registration (a background scan) and
/// mounting (a pod-start path) can touch it without one owning the other.
#[derive(Clone, Default)]
pub struct DriverTable {
    inner: Arc<Mutex<BTreeMap<String, RegisteredDriver>>>,
}

impl std::fmt::Debug for DriverTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverTable").finish_non_exhaustive()
    }
}

impl DriverTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a driver.
    pub async fn insert(&self, name: impl Into<String>, driver: RegisteredDriver) {
        self.inner.lock().await.insert(name.into(), driver);
    }

    /// Remove a driver (its registrar vanished).
    pub async fn remove(&self, name: &str) {
        self.inner.lock().await.remove(name);
    }

    /// Look one up.
    pub async fn get(&self, name: &str) -> Option<RegisteredDriver> {
        self.inner.lock().await.get(name).cloned()
    }

    /// Every registered driver name, sorted.
    pub async fn names(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }
}

/// The staging path for one volume on this node.
///
/// Upstream's layout. Per-VOLUME, not per-pod: staging happens once per
/// node no matter how many pods mount it, which is the entire point of the
/// stage/publish split.
#[must_use]
pub fn staging_path(root: &Path, driver: &str, volume_handle: &str) -> PathBuf {
    root.join("plugins")
        .join(driver)
        .join("volumeDevices")
        .join("staging")
        .join(volume_handle)
}

/// The per-pod publish target for one volume.
///
/// Upstream's layout, including the `kubernetes.io~csi` segment and the
/// trailing `mount`. Drivers and e2e suites both look at this shape.
#[must_use]
pub fn target_path(root: &Path, namespace: &str, pod: &str, volume: &str) -> PathBuf {
    root.join("pods")
        .join(format!("{namespace}_{pod}"))
        .join("volumes")
        .join("kubernetes.io~csi")
        .join(volume)
        .join("mount")
}

/// What was published, so teardown can undo exactly it.
///
/// Recorded rather than recomputed at delete time: the PV may already be
/// gone by then, and recomputing from a vanished object is how a teardown
/// silently unpublishes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Published {
    driver: String,
    volume_handle: String,
    target: PathBuf,
    staging: Option<PathBuf>,
}

/// A materializer that adds the CSI node path to a base materializer.
pub struct CsiVolumeMaterializer {
    base: Arc<dyn VolumeMaterializer>,
    drivers: DriverTable,
    root: PathBuf,
    published: Arc<Mutex<BTreeMap<String, Published>>>,
}

impl std::fmt::Debug for CsiVolumeMaterializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsiVolumeMaterializer")
            .field("base", &self.base.name())
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl CsiVolumeMaterializer {
    /// Wrap `base`, resolving drivers through `drivers`, with kubelet root
    /// `root`.
    #[must_use]
    pub fn new(
        base: Arc<dyn VolumeMaterializer>,
        drivers: DriverTable,
        root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            base,
            drivers,
            root: root.into(),
            published: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The driver table, so a registrar can populate it.
    #[must_use]
    pub fn drivers(&self) -> DriverTable {
        self.drivers.clone()
    }

    fn book_key(namespace: &str, pod: &str, volume: &str) -> String {
        format!("{namespace}/{pod}/{volume}")
    }

    /// The volume capability engenho asks for.
    ///
    /// `SINGLE_NODE_WRITER` even for a read-only mount: the access MODE
    /// describes how many nodes may write, while read-only-ness of THIS
    /// mount is the separate `readonly` field. Conflating them makes a
    /// `ReadWriteOnce` PVC mounted read-only by one pod look like a
    /// `ReadOnlyMany` volume to the driver, which changes its locking.
    fn capability(fs_type: Option<&str>) -> pb::VolumeCapability {
        pb::VolumeCapability {
            access_type: Some(pb::volume_capability::AccessType::Mount(
                pb::volume_capability::MountVolume {
                    fs_type: fs_type.unwrap_or_default().to_string(),
                    ..Default::default()
                },
            )),
            access_mode: Some(pb::volume_capability::AccessMode {
                mode: pb::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
            }),
        }
    }
}

fn driver_err(volume: &str, e: &engenho_csi::client::CsiError) -> VolumeResolveError {
    VolumeResolveError::Materialize(format!("CSI publish of volume {volume}: {e}"))
}

#[async_trait]
impl VolumeMaterializer for CsiVolumeMaterializer {
    fn name(&self) -> &'static str {
        "csi"
    }

    async fn materialize_files(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<MountSource, VolumeResolveError> {
        self.base
            .materialize_files(namespace, pod, volume, files)
            .await
    }

    async fn ensure_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<MountSource, VolumeResolveError> {
        self.base.ensure_empty_dir(namespace, pod, volume).await
    }

    async fn remove_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError> {
        self.base.remove_empty_dir(namespace, pod, volume).await
    }

    async fn publish_csi(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
        req: &CsiPublishRequest,
    ) -> Result<MountSource, VolumeResolveError> {
        // A PV naming a driver that never registered is a typed Pending,
        // not a mount of something else. The pod waits and an operator can
        // read which driver is missing.
        let Some(driver) = self.drivers.get(&req.driver).await else {
            return Err(VolumeResolveError::CsiUnavailable {
                vol: volume.to_string(),
                driver: req.driver.clone(),
            });
        };

        let target = target_path(&self.root, namespace, pod, volume);
        // The driver mounts AT this path and does not create it; upstream's
        // kubelet is the one that mkdirs. A driver handed a nonexistent
        // target fails with an errno that reads as a driver bug.
        std::fs::create_dir_all(&target).map_err(|e| {
            VolumeResolveError::Materialize(format!(
                "creating CSI target {}: {e}",
                target.display()
            ))
        })?;

        let capability = Some(Self::capability(req.fs_type.as_deref()));
        let volume_context: HashMap<String, String> = req
            .volume_attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Stage FIRST, and only when the driver asked for it.
        let staging = if driver.stage_unstage {
            let staging = staging_path(&self.root, &req.driver, &req.volume_handle);
            std::fs::create_dir_all(&staging).map_err(|e| {
                VolumeResolveError::Materialize(format!(
                    "creating CSI staging dir {}: {e}",
                    staging.display()
                ))
            })?;
            driver
                .client
                .node_stage(pb::NodeStageVolumeRequest {
                    volume_id: req.volume_handle.clone(),
                    publish_context: HashMap::new(),
                    staging_target_path: staging.display().to_string(),
                    volume_capability: capability.clone(),
                    secrets: HashMap::new(),
                    volume_context: volume_context.clone(),
                })
                .await
                .map_err(|e| driver_err(volume, &e))?;
            Some(staging)
        } else {
            None
        };

        driver
            .client
            .node_publish(pb::NodePublishVolumeRequest {
                volume_id: req.volume_handle.clone(),
                publish_context: HashMap::new(),
                staging_target_path: staging
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                target_path: target.display().to_string(),
                volume_capability: capability,
                readonly: req.read_only,
                secrets: HashMap::new(),
                volume_context,
            })
            .await
            .map_err(|e| driver_err(volume, &e))?;

        self.published.lock().await.insert(
            Self::book_key(namespace, pod, volume),
            Published {
                driver: req.driver.clone(),
                volume_handle: req.volume_handle.clone(),
                target: target.clone(),
                staging,
            },
        );

        // A published CSI volume is, from the container runtime's point of
        // view, just a host directory. `PvcHostDir` carries the read-only
        // flag the argv builder already knows how to honour, so nothing
        // downstream needs a CSI-shaped case.
        Ok(MountSource::PvcHostDir {
            path: target,
            read_only: req.read_only,
        })
    }

    async fn unpublish_csi(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError> {
        let key = Self::book_key(namespace, pod, volume);
        // Never published, or already torn down: SUCCESS. Teardown runs on
        // a path that may already be half-complete.
        let Some(record) = self.published.lock().await.remove(&key) else {
            return Ok(());
        };
        let Some(driver) = self.drivers.get(&record.driver).await else {
            // The driver is gone. There is nothing to call, and refusing to
            // delete the pod over it would strand the pod forever.
            tracing::warn!(
                driver = %record.driver,
                volume,
                "CSI driver gone at teardown; the mount may be left behind"
            );
            return Ok(());
        };

        if let Err(e) = driver
            .client
            .node_unpublish(pb::NodeUnpublishVolumeRequest {
                volume_id: record.volume_handle.clone(),
                target_path: record.target.display().to_string(),
            })
            .await
        {
            tracing::warn!(volume, error = %e, "NodeUnpublishVolume failed");
        }

        if let Some(staging) = record.staging {
            // Unstage is per-NODE, so it is only correct once the last pod
            // using the volume is gone. engenho publishes one pod per
            // volume today (a PVC with ReadWriteOnce), so unstaging here is
            // right; a shared-volume future needs a refcount, and this is
            // the line that will need it.
            if let Err(e) = driver
                .client
                .node_unstage(pb::NodeUnstageVolumeRequest {
                    volume_id: record.volume_handle,
                    staging_target_path: staging.display().to_string(),
                })
                .await
            {
                tracing::warn!(volume, error = %e, "NodeUnstageVolume failed");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn the_target_path_is_upstreams_layout_exactly() {
        // Not cosmetic: drivers write bookkeeping beside this path and the
        // CSI e2e suites assert on its shape.
        let p = target_path(Path::new("/var/lib/kubelet"), "ns", "pod1", "data");
        assert_eq!(
            p,
            PathBuf::from("/var/lib/kubelet/pods/ns_pod1/volumes/kubernetes.io~csi/data/mount")
        );
    }

    #[test]
    fn the_staging_path_is_per_volume_not_per_pod() {
        // The entire point of the stage/publish split: staging happens once
        // per node however many pods mount the volume. A per-pod staging
        // path would stage the same volume N times and unstage it on the
        // first pod's deletion, breaking the others.
        let a = staging_path(Path::new("/root"), "d.csi", "vol-1");
        let b = staging_path(Path::new("/root"), "d.csi", "vol-1");
        assert_eq!(a, b);
        assert!(!a.display().to_string().contains("pods"), "{}", a.display());
    }

    #[tokio::test]
    async fn an_unregistered_driver_is_a_typed_pending_not_a_wrong_mount() {
        let m = CsiVolumeMaterializer::new(
            Arc::new(crate::pod_volume::FakeVolumeMaterializer::new()),
            DriverTable::new(),
            "/tmp/engenho-csi-test",
        );
        let err = m
            .publish_csi(
                "ns",
                "pod1",
                "data",
                &CsiPublishRequest {
                    driver: "absent.csi.io".into(),
                    volume_handle: "vol-1".into(),
                    fs_type: None,
                    volume_attributes: BTreeMap::new(),
                    read_only: false,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "CsiUnavailable");
        assert!(err.to_string().contains("absent.csi.io"), "{err}");
    }

    #[tokio::test]
    async fn unpublishing_something_never_published_succeeds() {
        // Teardown runs on a path that may already be half-complete;
        // erroring here wedges pod deletion behind a volume already gone.
        let m = CsiVolumeMaterializer::new(
            Arc::new(crate::pod_volume::FakeVolumeMaterializer::new()),
            DriverTable::new(),
            "/tmp/engenho-csi-test",
        );
        assert!(m.unpublish_csi("ns", "pod1", "data").await.is_ok());
    }
}

// =====================================================================
// THE PROVISIONING PRODUCER
// =====================================================================

/// [`engenho_controllers::csi_provisioner::CsiProvisioner`] backed by the
/// node's registered drivers.
///
/// Lives HERE rather than in `engenho-controllers` because that crate must
/// not depend on `engenho-csi` — `engenho-kubelet` already depends on
/// `engenho-controllers`, so the arrow cannot be reversed. The controller
/// declares the verbs; this supplies the transport.
#[derive(Clone, Debug)]
pub struct DriverCsiProvisioner {
    drivers: DriverTable,
}

impl DriverCsiProvisioner {
    /// New provisioner over `drivers`.
    #[must_use]
    pub fn new(drivers: DriverTable) -> Self {
        Self { drivers }
    }
}

#[async_trait]
impl engenho_controllers::csi_provisioner::CsiProvisioner for DriverCsiProvisioner {
    async fn can_provision(&self, driver: &str) -> bool {
        self.drivers.get(driver).await.is_some()
    }

    async fn create_volume(
        &self,
        req: &engenho_controllers::csi_provisioner::CsiCreateRequest,
    ) -> Result<engenho_controllers::csi_provisioner::CsiCreatedVolume, String> {
        let driver = self
            .drivers
            .get(&req.driver)
            .await
            .ok_or_else(|| format!("CSI driver {} is not registered", req.driver))?;

        // The access mode the driver is told about is the CLAIM's, because
        // it changes how the driver locks the volume. Defaulting everything
        // to SingleNodeWriter would make a ReadWriteMany PVC silently
        // single-writer, which presents as one pod's writes vanishing.
        let mode = if req.multi_node {
            pb::volume_capability::access_mode::Mode::MultiNodeMultiWriter
        } else {
            pb::volume_capability::access_mode::Mode::SingleNodeWriter
        };

        let volume = driver
            .client
            .create_volume(pb::CreateVolumeRequest {
                name: req.name.clone(),
                capacity_range: if req.capacity_bytes > 0 {
                    Some(pb::CapacityRange {
                        required_bytes: req.capacity_bytes,
                        limit_bytes: 0,
                    })
                } else {
                    // No size asked for: send NO capacity range rather than
                    // a zero one. A `required_bytes: 0` is a request for a
                    // zero-byte volume, which drivers variously reject or
                    // honour; omitting the field lets the driver pick its
                    // own default, which is what upstream does.
                    None
                },
                volume_capabilities: vec![pb::VolumeCapability {
                    access_type: Some(pb::volume_capability::AccessType::Mount(
                        pb::volume_capability::MountVolume::default(),
                    )),
                    access_mode: Some(pb::volume_capability::AccessMode { mode: mode as i32 }),
                }],
                parameters: req
                    .parameters
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                secrets: HashMap::new(),
                volume_content_source: None,
                accessibility_requirements: None,
                mutable_parameters: HashMap::new(),
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(engenho_controllers::csi_provisioner::CsiCreatedVolume {
            volume_handle: volume.volume_id,
            // The driver's answer, not the request: a driver rounds up to
            // its allocation unit, and a zero here (a driver that declines
            // to report) falls back to what was asked for rather than
            // recording a zero-capacity PV.
            capacity_bytes: if volume.capacity_bytes > 0 {
                volume.capacity_bytes
            } else {
                req.capacity_bytes
            },
            volume_attributes: volume.volume_context.into_iter().collect(),
        })
    }

    async fn delete_volume(&self, driver: &str, volume_handle: &str) -> Result<(), String> {
        let driver = self
            .drivers
            .get(driver)
            .await
            .ok_or_else(|| format!("CSI driver {driver} is not registered"))?;
        driver
            .client
            .delete_volume(pb::DeleteVolumeRequest {
                volume_id: volume_handle.to_string(),
                secrets: HashMap::new(),
            })
            .await
            .map_err(|e| e.to_string())
    }
}

// =====================================================================
// THE REGISTRATION PRODUCER
// =====================================================================

/// Scans the kubelet's `plugins_registry` directory each tick and keeps
/// [`DriverTable`] in sync with what is actually there.
///
/// ★ A DIRECTORY SCAN RATHER THAN A WATCH, DELIBERATELY. Registration is a
/// startup event measured in seconds, not a hot path, and an `inotify` /
/// `FSEvents` watcher would add a second platform-specific code path for a
/// problem that does not need one. Named here so the absence reads as a
/// decision rather than an omission.
///
/// ★ IT ALSO DEREGISTERS. A driver whose registration socket has vanished
/// is removed from the table, so a PV naming it fails with a typed
/// `CsiUnavailable` instead of hanging on a dead socket — and so the
/// annotation an operator reads matches what is actually dialable.
pub struct CsiRegistrarController {
    registry: engenho_csi::registry::PluginRegistry,
    drivers: DriverTable,
}

impl CsiRegistrarController {
    /// New registrar over `<kubelet-root>/plugins_registry`.
    #[must_use]
    pub fn new(root: impl AsRef<std::path::Path>, drivers: DriverTable) -> Self {
        Self {
            registry: engenho_csi::registry::PluginRegistry::under_kubelet_root(root),
            drivers,
        }
    }

    /// The directory being scanned.
    #[must_use]
    pub fn dir(&self) -> &std::path::Path {
        self.registry.dir()
    }
}

#[async_trait]
impl engenho_controllers::Controller for CsiRegistrarController {
    fn name(&self) -> &'static str {
        "csi-registrar"
    }

    async fn tick(
        &self,
    ) -> Result<
        engenho_controllers::controller::ReconcileOutcome,
        engenho_controllers::error::ControllerError,
    > {
        let mut report = engenho_controllers::controller::ReconcileReport::default();

        // A missing directory is an empty scan, not an error: a node with
        // no CSI driver deployed is completely normal.
        let (found, failed) = match self.registry.scan().await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "CSI plugin registry scan failed");
                return Ok(engenho_controllers::controller::ReconcileOutcome::from(
                    report,
                ));
            }
        };
        report.objects_examined = found.len() + failed.len();

        for (socket, err) in &failed {
            // One broken plugin must not hide three working ones, and a
            // silently-skipped driver is how a storage outage becomes
            // unexplainable.
            tracing::warn!(socket = %socket.display(), error = %err, "CSI plugin registration failed");
            report.objects_skipped += 1;
        }

        let known = self.drivers.names().await;
        for (name, plugin) in &found {
            if known.contains(name) {
                continue;
            }
            let Ok(client) = engenho_csi::client::CsiClient::dial(&plugin.endpoint).await else {
                report.objects_skipped += 1;
                continue;
            };
            tracing::info!(
                driver = %name,
                endpoint = %plugin.endpoint,
                stage_unstage = plugin.info.stage_unstage,
                "CSI driver registered"
            );
            self.drivers
                .insert(
                    name.clone(),
                    RegisteredDriver {
                        client,
                        stage_unstage: plugin.info.stage_unstage,
                        node_id: plugin.info.node_id.clone(),
                    },
                )
                .await;
            report.objects_changed += 1;
        }

        // Deregister what is gone, or a PV naming it hangs on a dead socket.
        for name in known {
            if !found.contains_key(&name) {
                tracing::info!(driver = %name, "CSI driver deregistered: its socket is gone");
                self.drivers.remove(&name).await;
                report.objects_changed += 1;
            }
        }

        Ok(engenho_controllers::controller::ReconcileOutcome::from(
            report,
        ))
    }
}
