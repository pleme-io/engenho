//! R13 — Volume runtime (CSI-style storage plugin).
//!
//! Same shape as [`crate::backend::ContainerRuntime`] — a typed
//! trait the kubelet calls to materialize storage on the host;
//! pluggable backends for tests + production.
//!
//! ## Surface
//!
//! ```text
//! ContainerSpec.volumes (future) →
//!     VolumeRuntime::mount(spec) → MountedVolume
//!         FakeVolumeBackend (tests)
//!         HostPathVolumeBackend (homelab dev — bind-mount host dirs)
//!         CsiVolumeBackend (R13b — gRPC to CSI plugins)
//! ```
//!
//! ## Why a new trait vs reusing ContainerRuntime
//!
//! Storage has independent lifecycle from containers: a PVC
//! survives container restart; CSI plugins are out-of-process;
//! mount can fail differently (NoCapacity, AlreadyMounted). The
//! sibling trait keeps the kubelet's container path uncluttered
//! by volume concerns.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

/// Spec describing what volume the kubelet wants mounted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeSpec {
    /// Logical volume name (typically `pvc_namespace/pvc_name`).
    pub name: String,
    /// PVC's storage class — opaque string; backends route on it.
    pub storage_class: String,
    /// Requested size in MiB. Backends may reject if below capacity.
    pub size_mib: u64,
    /// Access mode: ReadWriteOnce / ReadOnlyMany / ReadWriteMany.
    pub access_mode: AccessMode,
    /// Backend-specific parameters (storage_class.parameters).
    pub parameters: BTreeMap<String, String>,
}

/// PVC access modes (matches K8s convention).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum AccessMode {
    /// Mounted read-write on a single node.
    ReadWriteOnce,
    /// Mounted read-only on many nodes.
    ReadOnlyMany,
    /// Mounted read-write on many nodes.
    ReadWriteMany,
}

/// What a successful mount returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedVolume {
    /// Backend handle for unmount.
    pub volume_id: String,
    /// Host-filesystem path where the volume is mounted.
    pub mount_path: PathBuf,
    /// Backend that produced this mount (telemetry).
    pub backend: String,
}

/// Volume backend errors. Stable `.kind()` for telemetry.
#[derive(Debug, Clone, Error)]
pub enum VolumeError {
    /// Backend rejected the spec (bad params, OOM, etc).
    #[error("backend: {0}")]
    Backend(String),
    /// Storage class requested by spec isn't served by this backend.
    #[error("unsupported storage_class: {0}")]
    UnsupportedStorageClass(String),
    /// Volume already exists with conflicting params.
    #[error("volume {0} already exists with different spec")]
    Conflict(String),
}

engenho_substrate::impl_error_kind! {
    VolumeError {
        (Backend(_)) => "backend",
        (UnsupportedStorageClass(_)) => "unsupported_storage_class",
        (Conflict(_)) => "conflict",
    }
}

/// Pluggable volume backend trait. Sibling to [`crate::backend::ContainerRuntime`].
#[async_trait]
pub trait VolumeRuntime: Send + Sync {
    /// Backend name for telemetry.
    fn name(&self) -> &'static str;

    /// Mount + return a typed [`MountedVolume`]. Idempotent: if
    /// `spec.name` already exists with compatible params, return
    /// the existing mount.
    ///
    /// # Errors
    ///
    /// - [`VolumeError::UnsupportedStorageClass`] if backend doesn't serve the class
    /// - [`VolumeError::Conflict`] if name exists with different params
    /// - [`VolumeError::Backend`] on filesystem / driver failure
    async fn mount(&self, spec: &VolumeSpec) -> Result<MountedVolume, VolumeError>;

    /// Unmount + release backing storage.
    ///
    /// # Errors
    /// [`VolumeError::Backend`] on backend failure.
    async fn unmount(&self, volume_id: &str) -> Result<(), VolumeError>;

    /// Currently-mounted volumes (telemetry + reconcile diffing).
    ///
    /// # Errors
    /// [`VolumeError::Backend`] on inspection failure.
    async fn list(&self) -> Result<Vec<MountedVolume>, VolumeError>;
}

// =================================================================
// FakeVolumeBackend — in-memory deterministic backend for tests
// =================================================================

/// In-memory backend. Tracks mounts in a BTreeMap + records every
/// mount/unmount call for test assertions.
#[derive(Default, Clone)]
pub struct FakeVolumeBackend {
    inner: Arc<Mutex<FakeVolState>>,
}

#[derive(Default)]
struct FakeVolState {
    next_id: u64,
    by_name: BTreeMap<String, MountedVolume>,
    by_id: BTreeMap<String, VolumeSpec>,
    /// Operation log for test assertion.
    events: Vec<FakeVolumeEvent>,
}

/// Per-call event log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FakeVolumeEvent {
    /// `mount(spec.name)` was invoked.
    Mount(String),
    /// `unmount(volume_id)` was invoked.
    Unmount(String),
}

impl FakeVolumeBackend {
    /// Fresh empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of recorded events.
    pub async fn events(&self) -> Vec<FakeVolumeEvent> {
        self.inner.lock().await.events.clone()
    }

    /// Count of mounted volumes.
    pub async fn mounted_count(&self) -> usize {
        self.inner.lock().await.by_name.len()
    }
}

#[async_trait]
impl VolumeRuntime for FakeVolumeBackend {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn mount(&self, spec: &VolumeSpec) -> Result<MountedVolume, VolumeError> {
        let mut state = self.inner.lock().await;
        // Idempotency: if name already mounted with same spec → return existing.
        if let Some(existing) = state.by_name.get(&spec.name).cloned() {
            if state.by_id.get(&existing.volume_id) == Some(spec) {
                return Ok(existing);
            }
            return Err(VolumeError::Conflict(spec.name.clone()));
        }
        state.next_id += 1;
        let id = format!("fake-vol-{:08x}", state.next_id);
        let mount = MountedVolume {
            volume_id: id.clone(),
            mount_path: PathBuf::from(format!("/var/lib/engenho/volumes/{id}")),
            backend: "fake".to_string(),
        };
        state.by_name.insert(spec.name.clone(), mount.clone());
        state.by_id.insert(id, spec.clone());
        state.events.push(FakeVolumeEvent::Mount(spec.name.clone()));
        Ok(mount)
    }

    async fn unmount(&self, volume_id: &str) -> Result<(), VolumeError> {
        let mut state = self.inner.lock().await;
        state.by_name.retain(|_, m| m.volume_id != volume_id);
        state.by_id.remove(volume_id);
        state
            .events
            .push(FakeVolumeEvent::Unmount(volume_id.to_string()));
        Ok(())
    }

    async fn list(&self) -> Result<Vec<MountedVolume>, VolumeError> {
        Ok(self.inner.lock().await.by_name.values().cloned().collect())
    }
}

// =================================================================
// HostPathVolumeBackend — homelab dev backend (bind-mount host dirs)
// =================================================================

/// Mounts volumes as host directories under a configurable root.
/// Suitable for single-node homelab clusters; storage_class must
/// be "hostpath".
#[derive(Clone)]
pub struct HostPathVolumeBackend {
    root: PathBuf,
    inner: Arc<Mutex<HostPathState>>,
}

#[derive(Default)]
struct HostPathState {
    by_name: BTreeMap<String, MountedVolume>,
}

impl HostPathVolumeBackend {
    /// New backend rooted at `root`. The directory is created on first mount.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            inner: Arc::new(Mutex::new(HostPathState::default())),
        }
    }
}

#[async_trait]
impl VolumeRuntime for HostPathVolumeBackend {
    fn name(&self) -> &'static str {
        "hostpath"
    }

    async fn mount(&self, spec: &VolumeSpec) -> Result<MountedVolume, VolumeError> {
        if spec.storage_class != "hostpath" {
            return Err(VolumeError::UnsupportedStorageClass(
                spec.storage_class.clone(),
            ));
        }
        let mut state = self.inner.lock().await;
        if let Some(existing) = state.by_name.get(&spec.name).cloned() {
            return Ok(existing);
        }
        // Sanitize name → directory-safe form.
        let safe_name: String = spec
            .name
            .chars()
            .map(|c| if c == '/' { '_' } else { c })
            .collect();
        let mount_path = self.root.join(&safe_name);
        std::fs::create_dir_all(&mount_path).map_err(|e| {
            VolumeError::Backend(format!("mkdir {}: {e}", mount_path.display()))
        })?;
        let mount = MountedVolume {
            volume_id: safe_name.clone(),
            mount_path,
            backend: "hostpath".to_string(),
        };
        state.by_name.insert(spec.name.clone(), mount.clone());
        Ok(mount)
    }

    async fn unmount(&self, volume_id: &str) -> Result<(), VolumeError> {
        let mut state = self.inner.lock().await;
        state.by_name.retain(|_, m| m.volume_id != volume_id);
        // Note: we don't delete the directory — operator's choice
        // whether storage is reclaimable or persistent.
        Ok(())
    }

    async fn list(&self) -> Result<Vec<MountedVolume>, VolumeError> {
        Ok(self.inner.lock().await.by_name.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec(name: &str) -> VolumeSpec {
        VolumeSpec {
            name: name.to_string(),
            storage_class: "hostpath".to_string(),
            size_mib: 1024,
            access_mode: AccessMode::ReadWriteOnce,
            parameters: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn fake_backend_mount_assigns_id_and_path() {
        let b = FakeVolumeBackend::new();
        let m = b.mount(&sample_spec("default/pvc1")).await.unwrap();
        assert!(m.volume_id.starts_with("fake-vol-"));
        assert_eq!(m.backend, "fake");
        assert!(m.mount_path.to_string_lossy().contains(&m.volume_id));
        assert_eq!(b.mounted_count().await, 1);
    }

    #[tokio::test]
    async fn fake_backend_mount_is_idempotent_for_same_spec() {
        let b = FakeVolumeBackend::new();
        let spec = sample_spec("p");
        let m1 = b.mount(&spec).await.unwrap();
        let m2 = b.mount(&spec).await.unwrap();
        assert_eq!(m1.volume_id, m2.volume_id);
        assert_eq!(b.mounted_count().await, 1);
    }

    #[tokio::test]
    async fn fake_backend_mount_rejects_conflicting_spec() {
        let b = FakeVolumeBackend::new();
        let mut spec = sample_spec("p");
        b.mount(&spec).await.unwrap();
        spec.size_mib = 2048;  // different params
        let err = b.mount(&spec).await.unwrap_err();
        assert_eq!(err.kind(), "conflict");
    }

    #[tokio::test]
    async fn fake_backend_unmount_clears_state() {
        let b = FakeVolumeBackend::new();
        let m = b.mount(&sample_spec("p")).await.unwrap();
        b.unmount(&m.volume_id).await.unwrap();
        assert_eq!(b.mounted_count().await, 0);
    }

    #[tokio::test]
    async fn fake_backend_events_record_in_order() {
        let b = FakeVolumeBackend::new();
        let m = b.mount(&sample_spec("a")).await.unwrap();
        b.unmount(&m.volume_id).await.unwrap();
        let events = b.events().await;
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], FakeVolumeEvent::Mount(n) if n == "a"));
        assert!(matches!(&events[1], FakeVolumeEvent::Unmount(_)));
    }

    #[tokio::test]
    async fn fake_backend_list_returns_mounted() {
        let b = FakeVolumeBackend::new();
        b.mount(&sample_spec("a")).await.unwrap();
        b.mount(&sample_spec("b")).await.unwrap();
        let list = b.list().await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn hostpath_backend_creates_directory() {
        let root = std::env::temp_dir().join(format!(
            "engenho-vol-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let b = HostPathVolumeBackend::new(&root);
        let m = b.mount(&sample_spec("default/pvc1")).await.unwrap();
        assert!(m.mount_path.exists());
        // Name was sanitized: "default/pvc1" → "default_pvc1".
        assert!(m.volume_id.contains("default_pvc1"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn hostpath_backend_rejects_non_hostpath_class() {
        let b = HostPathVolumeBackend::new(std::env::temp_dir().join("nope"));
        let mut spec = sample_spec("x");
        spec.storage_class = "fast-ssd".to_string();
        let err = b.mount(&spec).await.unwrap_err();
        assert_eq!(err.kind(), "unsupported_storage_class");
    }

    #[tokio::test]
    async fn hostpath_backend_is_idempotent() {
        let root = std::env::temp_dir().join(format!(
            "engenho-vol-idem-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let b = HostPathVolumeBackend::new(&root);
        let m1 = b.mount(&sample_spec("p")).await.unwrap();
        let m2 = b.mount(&sample_spec("p")).await.unwrap();
        assert_eq!(m1.volume_id, m2.volume_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn access_mode_serializes_pascal_case() {
        let json = serde_json::to_string(&AccessMode::ReadWriteOnce).unwrap();
        assert_eq!(json, "\"ReadWriteOnce\"");
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(VolumeError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            VolumeError::UnsupportedStorageClass("x".into()).kind(),
            "unsupported_storage_class"
        );
        assert_eq!(VolumeError::Conflict("x".into()).kind(), "conflict");
    }

    #[test]
    fn backend_names_are_stable() {
        assert_eq!(FakeVolumeBackend::new().name(), "fake");
        assert_eq!(
            HostPathVolumeBackend::new(std::env::temp_dir()).name(),
            "hostpath"
        );
    }
}
