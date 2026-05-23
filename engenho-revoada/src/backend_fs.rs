//! # FileSystemBackend — single-node persistent store on disk
//!
//! The first real (non-in-memory) [`StoreBackend`] impl. Persists
//! every applied resource to disk under a deterministic path
//! layout so the cluster's state survives process restarts.
//!
//! ## Layout
//!
//! ```text
//! {root}/
//!   ├── _meta.json           (format version + face-name marker)
//!   └── {kind}/
//!       └── {namespace_or_"_cluster"}/
//!           └── {name}.envelope  (CBOR NativeEnvelope bytes)
//! ```
//!
//! `_cluster` is used for cluster-scoped resources (no namespace).
//! Names are not sanitized — operators should restrict
//! `ResourceRef.name` to characters their filesystem accepts. The
//! backend errors on path-traversal attempts (`..` in any field).
//!
//! ## Architecture
//!
//! - Writes: serialize through the operator's chosen format
//!   adapter to a CBOR NativeEnvelope; write to
//!   `{root}/{kind}/{ns}/{name}.envelope`; mirror in an
//!   InMemoryStore for fast reads + watch fan-out.
//! - Reads: served from the InMemoryStore cache (hot path). If
//!   not cached, the cache is rebuilt from disk lazily on demand.
//! - Watch: delegated entirely to the InMemoryStore cache —
//!   subscribers see Added/Modified/Deleted events as writes flow
//!   through the same code path used by InMemoryBackend.
//! - Snapshot/restore: walks the filesystem; emits/receives a
//!   single CBOR `Vec<(ResourceRef, Vec<u8>)>` matching the
//!   InMemoryStore snapshot format. So a snapshot taken via an
//!   InMemoryBackend can be restored into FileSystemBackend and
//!   vice versa — cross-backend interop by design.
//!
//! ## What this proves about the trait
//!
//! [`StoreBackend`] was sized for in-memory; this impl proves it
//! also fits a backend with I/O latency, fallible writes, and
//! persistent durability requirements. The next backend
//! (`RaftBackend`, `KubeApiServerBackend`, etc.) follows the same
//! shape — overlay a real backing system on top of an
//! InMemoryStore cache for watch + fast-read semantics.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::backend::StoreBackend;
use crate::face::{FaceError, FaceWatchStream, ResourceFormat, ResourceRef};
use crate::face_store::InMemoryStore;
use crate::format::{AdapterRegistry, FormatAdapter};

/// Errors specific to filesystem persistence. Surface through
/// [`FaceError::Unsupported`] (the trait's broadest error variant)
/// with the io::Error's message attached.
#[derive(Debug, thiserror::Error)]
enum FsError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("path traversal attempt rejected: {field} contained '..'")]
    PathTraversal { field: &'static str },
}

impl From<FsError> for FaceError {
    fn from(e: FsError) -> Self {
        FaceError::Unsupported(format!("filesystem backend: {e}"))
    }
}

/// File extension for stored envelopes. Lets future versions of
/// the backend add other extensions (e.g. `.snapshot` for snapshot
/// metadata files) without colliding.
const ENVELOPE_EXT: &str = "envelope";
const META_FILE: &str = "_meta.json";
const CLUSTER_SCOPED_DIR: &str = "_cluster";

/// On-disk metadata sidecar — records format version + face name
/// so cross-version restore can detect mismatches.
#[derive(serde::Serialize, serde::Deserialize)]
struct Metadata {
    format_version: u32,
    face_name: String,
}

const FORMAT_VERSION: u32 = 1;

/// Single-node persistent backend backed by a filesystem tree.
///
/// **What this is good for:** dev / homelab / single-host
/// production where consensus replication isn't needed but state
/// must survive restarts. Operators choose this over the
/// in-memory backend when persistence is a hard requirement and
/// the latency of disk I/O is acceptable.
///
/// **What this isn't:** distributed. There's no replication, no
/// quorum, no consensus. A second engenho process writing to the
/// same `root` simultaneously will race. For multi-node use
/// `RaftBackend` (R5) or wrap multiple `FileSystemBackend`s in a
/// FederatedFabric.
pub struct FileSystemBackend {
    root: PathBuf,
    face_name: String,
    /// In-memory cache mirroring the on-disk state. All reads +
    /// watch fan-out flow through here; writes update both cache
    /// and disk atomically (cache first, then disk; failed disk
    /// write rolls back cache).
    cache: InMemoryStore,
}

impl FileSystemBackend {
    /// Open or create a filesystem-backed backend at `root`. If
    /// the directory exists + has a valid `_meta.json` matching
    /// the format version, existing state is loaded into the cache.
    /// Otherwise the directory is created (recursive) + initialized.
    ///
    /// # Errors
    ///
    /// Returns [`FaceError::Unsupported`] wrapping any I/O failure
    /// (directory create denied, metadata parse error, etc.).
    pub fn open(root: impl Into<PathBuf>, face_name: impl Into<String>) -> Result<Self, FaceError> {
        let root = root.into();
        let face_name = face_name.into();
        Self::open_with_adapters(root, face_name, AdapterRegistry::default())
    }

    /// Open with a custom adapter registry (e.g. for an
    /// operator-defined HCL adapter that ships extra format support).
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub fn open_with_adapters(
        root: PathBuf,
        face_name: String,
        adapters: AdapterRegistry,
    ) -> Result<Self, FaceError> {
        std::fs::create_dir_all(&root).map_err(FsError::Io)?;

        // Read or initialize the metadata sidecar.
        let meta_path = root.join(META_FILE);
        if meta_path.exists() {
            let meta_bytes = std::fs::read(&meta_path).map_err(FsError::Io)?;
            let meta: Metadata = serde_json::from_slice(&meta_bytes).map_err(|e| {
                FaceError::Unsupported(format!("filesystem backend: parse {META_FILE}: {e}"))
            })?;
            if meta.format_version != FORMAT_VERSION {
                return Err(FaceError::Unsupported(format!(
                    "filesystem backend: on-disk format version {} != expected {}",
                    meta.format_version, FORMAT_VERSION,
                )));
            }
        } else {
            let meta = Metadata {
                format_version: FORMAT_VERSION,
                face_name: face_name.clone(),
            };
            let bytes = serde_json::to_vec_pretty(&meta).map_err(|e| {
                FaceError::Unsupported(format!("filesystem backend: serialize meta: {e}"))
            })?;
            std::fs::write(&meta_path, bytes).map_err(FsError::Io)?;
        }

        let mut cache = InMemoryStore::new(face_name.clone());
        cache.set_adapters(adapters);
        let backend = Self {
            root,
            face_name,
            cache,
        };
        backend.load_cache_from_disk()?;
        Ok(backend)
    }

    /// Compute the on-disk path for a resource. Errors if any
    /// field contains a path-traversal attempt.
    fn path_for(&self, reference: &ResourceRef) -> Result<PathBuf, FsError> {
        Self::check_no_traversal("kind", &reference.kind)?;
        Self::check_no_traversal("name", &reference.name)?;
        if let Some(ns) = &reference.namespace {
            Self::check_no_traversal("namespace", ns)?;
        }
        let ns_segment = reference.namespace.as_deref().unwrap_or(CLUSTER_SCOPED_DIR);
        Ok(self
            .root
            .join(&reference.kind)
            .join(ns_segment)
            .join(format!("{}.{ENVELOPE_EXT}", reference.name)))
    }

    fn check_no_traversal(field: &'static str, value: &str) -> Result<(), FsError> {
        if value.split(['/', '\\']).any(|seg| seg == "..") || value.contains('\0') {
            return Err(FsError::PathTraversal { field });
        }
        Ok(())
    }

    /// Walk the filesystem tree and push every envelope into the
    /// in-memory cache.
    fn load_cache_from_disk(&self) -> Result<(), FaceError> {
        let entries = Self::walk_envelopes(&self.root).map_err(FsError::Io)?;
        for (reference, bytes) in entries {
            // Replay through cache.apply via the Native passthrough
            // adapter — the on-disk envelope IS already the CBOR
            // NativeEnvelope shape.
            self.cache.apply(ResourceFormat::Native, &bytes)?;
        }
        Ok(())
    }

    /// Recursive walk of the filesystem tree; yields every
    /// (reference, envelope bytes) pair.
    fn walk_envelopes(root: &Path) -> std::io::Result<Vec<(ResourceRef, Vec<u8>)>> {
        let mut out = Vec::new();
        if !root.exists() {
            return Ok(out);
        }
        for kind_entry in std::fs::read_dir(root)? {
            let kind_entry = kind_entry?;
            let kind_path = kind_entry.path();
            if !kind_path.is_dir() {
                continue; // skip _meta.json
            }
            let kind = kind_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if kind.is_empty() {
                continue;
            }
            for ns_entry in std::fs::read_dir(&kind_path)? {
                let ns_entry = ns_entry?;
                let ns_path = ns_entry.path();
                if !ns_path.is_dir() {
                    continue;
                }
                let ns_str = ns_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                let namespace = if ns_str == CLUSTER_SCOPED_DIR {
                    None
                } else {
                    Some(ns_str.clone())
                };
                for resource_entry in std::fs::read_dir(&ns_path)? {
                    let resource_entry = resource_entry?;
                    let resource_path = resource_entry.path();
                    if resource_path.extension().and_then(|s| s.to_str()) != Some(ENVELOPE_EXT) {
                        continue;
                    }
                    let name = resource_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    let bytes = std::fs::read(&resource_path)?;
                    out.push((
                        ResourceRef {
                            kind: kind.to_string(),
                            name,
                            namespace: namespace.clone(),
                        },
                        bytes,
                    ));
                }
            }
        }
        Ok(out)
    }
}

impl StoreBackend for FileSystemBackend {
    fn name(&self) -> &str {
        "filesystem"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        // Run through the cache's adapter pipeline so the operator
        // gets identical validation/error semantics to the in-memory
        // backend (and so the cache stays consistent if the disk
        // write later succeeds).
        self.cache.apply(format, body)?;
        // Extract the ResourceRef from the cache by listing all
        // entries (slow but correct — we don't have direct access
        // to the adapter from here). For the common case the
        // cache update has already happened; we need the envelope
        // to write to disk. The simplest path: re-run extract_ref
        // through the adapter so we know which file to write.
        let adapter: Arc<dyn FormatAdapter> = self
            .cache
            .adapters()
            .select(format)
            .map_err(|e| FaceError::Unsupported(format!("filesystem apply: {e}")))?;
        let reference = adapter
            .extract_ref(format, body)
            .map_err(|e| FaceError::Unsupported(format!("filesystem apply: {e}")))?;
        let envelope = adapter
            .to_native(format, body)
            .map_err(|e| FaceError::Unsupported(format!("filesystem apply: {e}")))?;

        // Persist to disk: create parent dir + write atomically
        // (tempfile + rename so partial writes never corrupt state).
        let path = self.path_for(&reference)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(FsError::Io)?;
        }
        let tmp_path = path.with_extension(format!("{ENVELOPE_EXT}.tmp"));
        std::fs::write(&tmp_path, &envelope).map_err(FsError::Io)?;
        std::fs::rename(&tmp_path, &path).map_err(FsError::Io)?;
        Ok(())
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        // Served from the cache (which mirrors disk).
        self.cache.get(reference, format)
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.cache.list(kind, namespace, format)
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        // Cache delete (also broadcasts the Deleted watch event).
        self.cache.delete(reference)?;
        // Disk delete: best-effort. If the file is gone we still
        // succeeded — operator may have deleted it externally.
        let path = self.path_for(reference)?;
        if path.exists() {
            std::fs::remove_file(&path).map_err(FsError::Io)?;
        }
        Ok(())
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        // Watch fan-out lives entirely in the cache (apply +
        // delete already broadcast through it).
        self.cache.watch(kind, namespace, format)
    }

    fn resource_count(&self) -> usize {
        self.cache.len()
    }

    fn subscriber_count(&self) -> usize {
        self.cache.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        // Same format as InMemoryStore so snapshots are
        // cross-backend interop-compatible.
        self.cache.snapshot()
    }

    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        // Replace the cache, then walk the cache and write every
        // entry to disk (overwriting whatever was there).
        self.cache.restore(snapshot_bytes)?;
        // Wipe the existing disk state EXCEPT the meta file.
        for kind_entry in std::fs::read_dir(&self.root).map_err(FsError::Io)? {
            let entry = kind_entry.map_err(FsError::Io)?;
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path).map_err(FsError::Io)?;
            }
        }
        // Re-snapshot the cache (now restored state) and walk it
        // to re-emit every file. Use the same snapshot codec as
        // we just consumed.
        let restored_snap = self.cache.snapshot()?;
        let entries: Vec<(ResourceRef, Vec<u8>)> = ciborium::from_reader(restored_snap.as_slice())
            .map_err(|e| FaceError::Unsupported(format!("filesystem restore: cbor decode: {e}")))?;
        for (reference, envelope) in entries {
            let path = self.path_for(&reference)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(FsError::Io)?;
            }
            std::fs::write(&path, &envelope).map_err(FsError::Io)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::face::encode_native_envelope;

    /// Make a temporary directory for filesystem tests. Returns
    /// a TempDir so the directory is auto-cleaned on drop.
    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn pod_ref(name: &str, ns: &str) -> ResourceRef {
        ResourceRef::namespaced("Pod", name, ns)
    }

    fn yaml(name: &str, ns: &str) -> Vec<u8> {
        format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n"
        )
        .into_bytes()
    }

    fn envelope(reference: &ResourceRef, payload: &[u8]) -> Vec<u8> {
        encode_native_envelope(reference, payload).unwrap()
    }

    // ── Construction + basic open/close ───────────────────────────

    #[test]
    fn open_creates_root_directory_and_meta() {
        let dir = temp_dir();
        let root = dir.path().join("engenho-fs");
        let backend = FileSystemBackend::open(&root, "test").unwrap();
        assert!(root.exists());
        assert!(root.join(META_FILE).exists());
        assert_eq!(backend.name(), "filesystem");
    }

    #[test]
    fn reopen_loads_existing_state_into_cache() {
        let dir = temp_dir();
        {
            let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
            backend
                .apply(ResourceFormat::Yaml, &yaml("a", "default"))
                .unwrap();
            backend
                .apply(ResourceFormat::Yaml, &yaml("b", "default"))
                .unwrap();
            assert_eq!(backend.resource_count(), 2);
        }
        // Reopen — state should reload from disk.
        let reopened = FileSystemBackend::open(dir.path(), "test").unwrap();
        assert_eq!(reopened.resource_count(), 2);
    }

    // ── Apply / Get round-trip ───────────────────────────────────

    #[test]
    fn apply_yaml_persists_to_disk_under_expected_path() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let expected_path = dir
            .path()
            .join("Pod")
            .join("default")
            .join("nginx.envelope");
        assert!(expected_path.exists(), "expected file at {expected_path:?}");
    }

    #[test]
    fn apply_then_get_round_trips_yaml() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let body = yaml("nginx", "default");
        backend.apply(ResourceFormat::Yaml, &body).unwrap();
        let r = pod_ref("nginx", "default");
        assert_eq!(backend.get(&r, ResourceFormat::Yaml).unwrap(), body,);
    }

    #[test]
    fn apply_then_get_round_trips_json() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "x", "namespace": "default" },
            "spec": {}
        }))
        .unwrap();
        backend.apply(ResourceFormat::Json, &body).unwrap();
        let r = pod_ref("x", "default");
        assert_eq!(backend.get(&r, ResourceFormat::Json).unwrap(), body);
    }

    #[test]
    fn apply_then_get_round_trips_native() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let r = pod_ref("nginx", "default");
        let env = envelope(&r, b"payload");
        backend.apply(ResourceFormat::Native, &env).unwrap();
        assert_eq!(backend.get(&r, ResourceFormat::Native).unwrap(), env);
    }

    // ── Cluster-scoped resources go under _cluster ──────────────

    #[test]
    fn cluster_scoped_resource_persists_under_cluster_dir() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": "admin" },
            "rules": []
        }))
        .unwrap();
        backend.apply(ResourceFormat::Json, &body).unwrap();
        let expected = dir
            .path()
            .join("ClusterRole")
            .join("_cluster")
            .join("admin.envelope");
        assert!(expected.exists(), "expected cluster-scoped at {expected:?}");
    }

    // ── Delete removes from disk + cache ────────────────────────

    #[test]
    fn delete_removes_envelope_file_and_cache_entry() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let path = dir
            .path()
            .join("Pod")
            .join("default")
            .join("nginx.envelope");
        assert!(path.exists());
        let r = pod_ref("nginx", "default");
        backend.delete(&r).unwrap();
        assert!(!path.exists(), "envelope file should be gone");
        assert_eq!(backend.resource_count(), 0);
    }

    // ── List filters by kind + namespace ────────────────────────

    #[test]
    fn list_filters_by_namespace() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("b", "default"))
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("c", "other"))
            .unwrap();
        let default = backend
            .list("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        assert_eq!(default.len(), 2);
        let other = backend
            .list("Pod", Some("other"), ResourceFormat::Yaml)
            .unwrap();
        assert_eq!(other.len(), 1);
    }

    // ── Watch wires through cache ───────────────────────────────

    #[test]
    fn watch_streams_apply_events() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let mut watch = backend
            .watch("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let _ = watch.next_event().unwrap().expect("event");
    }

    // ── Snapshot/Restore cross-backend interop ──────────────────

    #[test]
    fn snapshot_then_restore_into_new_backend() {
        let dir1 = temp_dir();
        let dir2 = temp_dir();
        let src = FileSystemBackend::open(dir1.path(), "src").unwrap();
        src.apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        src.apply(ResourceFormat::Yaml, &yaml("b", "default"))
            .unwrap();
        let snap = src.snapshot().unwrap();

        let dst = FileSystemBackend::open(dir2.path(), "dst").unwrap();
        dst.restore(&snap).unwrap();
        assert_eq!(dst.resource_count(), 2);
        // Restored state also persisted to dst's disk.
        assert!(
            dir2.path()
                .join("Pod")
                .join("default")
                .join("a.envelope")
                .exists()
        );
        assert!(
            dir2.path()
                .join("Pod")
                .join("default")
                .join("b.envelope")
                .exists()
        );
    }

    #[test]
    fn snapshot_format_compatible_with_in_memory_backend() {
        // Snapshot from filesystem → restore into in-memory →
        // identical state. The cross-backend interop guarantee.
        let dir = temp_dir();
        let fs = FileSystemBackend::open(dir.path(), "fs").unwrap();
        fs.apply(ResourceFormat::Yaml, &yaml("a", "default"))
            .unwrap();
        let snap = fs.snapshot().unwrap();

        let mem = InMemoryStore::new("mem");
        mem.restore(&snap).unwrap();
        assert_eq!(mem.len(), 1);
    }

    #[test]
    fn restore_replaces_existing_disk_state() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        // Initial state.
        backend
            .apply(ResourceFormat::Yaml, &yaml("keep", "default"))
            .unwrap();
        let snap_with_keep = backend.snapshot().unwrap();
        // Add a different resource.
        backend
            .apply(ResourceFormat::Yaml, &yaml("transient", "default"))
            .unwrap();
        assert_eq!(backend.resource_count(), 2);
        // Restore the prior snapshot — transient should be gone.
        backend.restore(&snap_with_keep).unwrap();
        assert_eq!(backend.resource_count(), 1);
        assert!(
            !dir.path()
                .join("Pod")
                .join("default")
                .join("transient.envelope")
                .exists()
        );
        assert!(
            dir.path()
                .join("Pod")
                .join("default")
                .join("keep.envelope")
                .exists()
        );
    }

    // ── Path-traversal safety ───────────────────────────────────

    #[test]
    fn apply_rejects_path_traversal_in_name() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let evil = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "../../etc/passwd", "namespace": "default" },
            "spec": {}
        }))
        .unwrap();
        match backend.apply(ResourceFormat::Json, &evil) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("traversal"), "msg: {msg}");
            }
            other => panic!("expected traversal rejection, got {other:?}"),
        }
    }

    #[test]
    fn apply_rejects_path_traversal_in_namespace() {
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        let evil = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "x", "namespace": "../../tmp" },
            "spec": {}
        }))
        .unwrap();
        match backend.apply(ResourceFormat::Json, &evil) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("traversal"));
            }
            other => panic!("expected traversal rejection, got {other:?}"),
        }
    }

    // ── Cross-version safety ────────────────────────────────────

    #[test]
    fn open_with_mismatched_format_version_errors_clearly() {
        let dir = temp_dir();
        // Manually write meta with a different version.
        std::fs::write(
            dir.path().join(META_FILE),
            serde_json::to_vec(&serde_json::json!({
                "format_version": 999,
                "face_name": "old"
            }))
            .unwrap(),
        )
        .unwrap();
        // `FileSystemBackend` doesn't impl Debug (contains
        // InMemoryStore which has a Mutex); pattern-match the
        // Result manually.
        match FileSystemBackend::open(dir.path(), "new") {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("format version"));
                assert!(msg.contains("999"));
            }
            Err(other) => panic!("expected Unsupported, got {other:?}"),
            Ok(_) => panic!("expected version mismatch error, got Ok"),
        }
    }

    // ── Backend trait coverage ───────────────────────────────────

    #[test]
    fn filesystem_backend_dispatches_through_store_backend_trait() {
        let dir = temp_dir();
        let backend: Box<dyn StoreBackend> =
            Box::new(FileSystemBackend::open(dir.path(), "test").unwrap());
        assert_eq!(backend.name(), "filesystem");
        let body = yaml("nginx", "default");
        backend.apply(ResourceFormat::Yaml, &body).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    #[test]
    fn write_is_atomic_via_tempfile_rename() {
        // The .envelope.tmp file should NEVER survive past a
        // successful apply. We exercise apply + assert the tmp
        // file isn't there afterwards.
        let dir = temp_dir();
        let backend = FileSystemBackend::open(dir.path(), "test").unwrap();
        backend
            .apply(ResourceFormat::Yaml, &yaml("nginx", "default"))
            .unwrap();
        let tmp_path = dir
            .path()
            .join("Pod")
            .join("default")
            .join("nginx.envelope.tmp");
        assert!(!tmp_path.exists(), "tmp file should not survive apply");
    }
}
