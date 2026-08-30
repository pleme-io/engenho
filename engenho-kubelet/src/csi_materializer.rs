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
