//! # StoreBackend — pluggable verb backend trait
//!
//! Every face's 5-verb contract dispatches through a
//! [`StoreBackend`] trait object. The substrate ships one default
//! impl ([`crate::face_store::InMemoryStore`]); R6+ operators add
//! their own backend impls that wire to real systems:
//!
//! - `RaftBackend` (engenho-store openraft-replicated)
//! - `KubeApiServerBackend` (engenho-kube-client → real apiserver)
//! - `NomadHttpBackend` (Nomad jobs over HTTP)
//! - `SystemdDbusBackend` (unit-file render + dbus daemon-reload)
//! - `SupervisedSystemdBackend` (orchestrated containers)
//!
//! Each lives in the operator's own crate (or in
//! `engenho-revoada-backends-*` siblings). The trait stays here so
//! the substrate's typed shape carries the contract; operator code
//! just implements the trait.
//!
//! ## Why a trait, not concrete-in-face?
//!
//! Per prime-directive: the third-site rule applies twice over —
//! five faces each could have their own backend type, OR all five
//! share one trait + N backend impls. The trait is the right
//! lift because:
//!
//! 1. Swappable at runtime: operators choose "real raft" for prod,
//!    "in-memory" for tests, via the same `with_backend` builder.
//! 2. Backend impls live OUTSIDE the substrate. The substrate
//!    crate doesn't need to depend on openraft / kube-rs / dbus —
//!    those deps live in the operator's backend crate.
//! 3. Cross-face uniform: the trait's shape is fixed; every face
//!    composes with any backend that honors it.

use crate::face::{FaceError, FaceWatchStream, ResourceFormat, ResourceRef};

/// The contract every store backend honors. Mirrors the 5-verb
/// Face contract + snapshot/restore + observability accessors.
///
/// **Object-safe by design** — `Send + Sync + 'static` so faces
/// can hold `Box<dyn StoreBackend>` and swap implementations
/// behind a single trait object.
pub trait StoreBackend: Send + Sync + 'static {
    /// Stable name for diagnostics + telemetry (e.g.
    /// `"in-memory"`, `"openraft"`, `"kube-apiserver"`).
    fn name(&self) -> &str;

    /// Apply (create-or-update) a resource. See [`crate::Face::apply_resource`].
    ///
    /// # Errors
    ///
    /// Returns backend-specific errors via [`FaceError`].
    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError>;

    /// Get a single resource. See [`crate::Face::get_resource`].
    ///
    /// # Errors
    ///
    /// Returns backend-specific errors via [`FaceError`].
    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError>;

    /// List resources of a given kind in an optional namespace.
    /// See [`crate::Face::list_resources`].
    ///
    /// # Errors
    ///
    /// Returns backend-specific errors via [`FaceError`].
    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError>;

    /// Delete a single resource. See [`crate::Face::delete_resource`].
    ///
    /// # Errors
    ///
    /// Returns backend-specific errors via [`FaceError`].
    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError>;

    /// Open a watch stream. See [`crate::Face::watch_resources`].
    ///
    /// # Errors
    ///
    /// Returns backend-specific errors via [`FaceError`].
    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError>;

    /// Resource count for observability. Default: 0 (backends with
    /// no local cache opt out).
    fn resource_count(&self) -> usize {
        0
    }

    /// Active watch subscriber count. Default: 0.
    fn subscriber_count(&self) -> usize {
        0
    }

    /// Capture state as a typed snapshot. Default: Unsupported.
    ///
    /// # Errors
    ///
    /// Default returns `Err(FaceError::Unsupported)`. Backends with
    /// a readable state override.
    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "snapshot not supported by {} backend",
            self.name()
        )))
    }

    /// Restore from a typed snapshot. Default: Unsupported.
    ///
    /// # Errors
    ///
    /// Default returns `Err(FaceError::Unsupported)`. Backends with
    /// a writable state override.
    fn restore(&self, _snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        Err(FaceError::Unsupported(format!(
            "restore not supported by {} backend",
            self.name()
        )))
    }
}

/// Blanket impl: an in-memory store implements the trait without
/// any glue — its method signatures align.
impl StoreBackend for crate::face_store::InMemoryStore {
    fn name(&self) -> &str {
        "in-memory"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.apply(format, body)
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        self.get(reference, format)
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.list(kind, namespace, format)
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.delete(reference)
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.watch(kind, namespace, format)
    }

    fn resource_count(&self) -> usize {
        self.len()
    }

    fn subscriber_count(&self) -> usize {
        self.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.snapshot()
    }

    fn restore(&self, bytes: &[u8]) -> Result<(), FaceError> {
        self.restore(bytes)
    }
}

// ─────────────────────────────────────────────────────────────────
// StubBackend — testing + reference backend for trait shape
// ─────────────────────────────────────────────────────────────────

/// A stub backend that returns `Unsupported` for every verb. Used
/// in tests + as a reference for what "no real implementation yet"
/// looks like under the trait. R6 operators replace with their
/// real backend impl.
///
/// **Use case:** an operator wants to wire `KubernetesFace` to a
/// real kube-apiserver but the integration isn't done yet — they
/// install `StubBackend` to make the face boot cleanly, with
/// every verb erroring loudly until the real backend lands.
pub struct StubBackend {
    backend_name: String,
}

impl StubBackend {
    /// Build a stub named after the backend it's standing in for
    /// (e.g. `"raft-coming-soon"`, `"kube-apiserver-tbd"`).
    pub fn new(backend_name: impl Into<String>) -> Self {
        Self {
            backend_name: backend_name.into(),
        }
    }
}

impl StoreBackend for StubBackend {
    fn name(&self) -> &str {
        &self.backend_name
    }

    fn apply(&self, _format: ResourceFormat, _body: &[u8]) -> Result<(), FaceError> {
        Err(FaceError::Unsupported(format!(
            "apply: {} stub backend (real impl pending)",
            self.backend_name
        )))
    }

    fn get(&self, _reference: &ResourceRef, _format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "get: {} stub backend (real impl pending)",
            self.backend_name
        )))
    }

    fn list(
        &self,
        _kind: &str,
        _namespace: Option<&str>,
        _format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "list: {} stub backend (real impl pending)",
            self.backend_name
        )))
    }

    fn delete(&self, _reference: &ResourceRef) -> Result<(), FaceError> {
        Err(FaceError::Unsupported(format!(
            "delete: {} stub backend (real impl pending)",
            self.backend_name
        )))
    }

    fn watch(
        &self,
        _kind: &str,
        _namespace: Option<&str>,
        _format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "watch: {} stub backend (real impl pending)",
            self.backend_name
        )))
    }
}

// ─────────────────────────────────────────────────────────────────
// Named backend stubs — one per planned R6 integration
// ─────────────────────────────────────────────────────────────────

/// Convenience constructors naming each R6 backend the substrate
/// expects to land. Operators use these to scaffold a face with
/// the right error message while the real backend integration is
/// underway.

/// Stub for the openraft-replicated store landing in R5
/// (engenho-store / engenho-revoada::consensus).
#[must_use]
pub fn raft_stub() -> Box<dyn StoreBackend> {
    Box::new(StubBackend::new("openraft-coming-in-R5"))
}

/// Stub for the kube-apiserver bridge landing in R6
/// (engenho-kube-client).
#[must_use]
pub fn kube_apiserver_stub() -> Box<dyn StoreBackend> {
    Box::new(StubBackend::new("kube-apiserver-coming-in-R6"))
}

/// Stub for the Nomad HTTP client landing in R6.
#[must_use]
pub fn nomad_http_stub() -> Box<dyn StoreBackend> {
    Box::new(StubBackend::new("nomad-http-coming-in-R6"))
}

/// Stub for the systemd dbus + unit-file renderer landing in R6.
#[must_use]
pub fn systemd_dbus_stub() -> Box<dyn StoreBackend> {
    Box::new(StubBackend::new("systemd-dbus-coming-in-R6"))
}

/// Stub for the bare-metal supervised-systemd-container backend
/// landing in R6.
#[must_use]
pub fn supervised_systemd_stub() -> Box<dyn StoreBackend> {
    Box::new(StubBackend::new("supervised-systemd-coming-in-R6"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod_ref() -> ResourceRef {
        ResourceRef::namespaced("Pod", "nginx", "default")
    }

    // ── Trait is object-safe + dyn-compat ────────────────────────

    #[test]
    fn store_backend_is_send_sync_static_dyn_compat() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<Box<dyn StoreBackend>>();
    }

    #[test]
    fn in_memory_store_impls_store_backend() {
        let store = crate::face_store::InMemoryStore::new("test");
        let backend: Box<dyn StoreBackend> = Box::new(store);
        assert_eq!(backend.name(), "in-memory");
    }

    // ── StubBackend errors loudly for every verb ────────────────

    #[test]
    fn stub_apply_errors_with_named_backend() {
        let stub = StubBackend::new("raft-coming-soon");
        match stub.apply(ResourceFormat::Native, b"") {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("raft-coming-soon"), "msg: {msg}");
                assert!(msg.contains("apply"), "msg: {msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn stub_get_errors_with_named_backend() {
        let stub = StubBackend::new("kube-tbd");
        match stub.get(&pod_ref(), ResourceFormat::Yaml) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("kube-tbd"));
                assert!(msg.contains("get"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn stub_list_errors_with_named_backend() {
        let stub = StubBackend::new("nomad-tbd");
        match stub.list("Job", None, ResourceFormat::Hcl) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("nomad-tbd"));
                assert!(msg.contains("list"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn stub_delete_errors_with_named_backend() {
        let stub = StubBackend::new("systemd-tbd");
        match stub.delete(&pod_ref()) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("systemd-tbd"));
                assert!(msg.contains("delete"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn stub_watch_errors_with_named_backend() {
        let stub = StubBackend::new("bms-tbd");
        match stub.watch("Pod", None, ResourceFormat::Yaml) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("bms-tbd"));
                assert!(msg.contains("watch"));
            }
            Err(other) => panic!("expected Unsupported, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[test]
    fn stub_snapshot_default_errors() {
        let stub = StubBackend::new("any");
        match stub.snapshot() {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("snapshot"), "msg: {msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // ── Named stub helpers carry the R5/R6 milestone in the name ─

    #[test]
    fn raft_stub_names_R5_milestone() {
        let s = raft_stub();
        assert!(s.name().contains("R5"), "name: {}", s.name());
        assert!(s.name().contains("openraft"));
    }

    #[test]
    fn kube_apiserver_stub_names_R6_milestone() {
        let s = kube_apiserver_stub();
        assert!(s.name().contains("R6"));
        assert!(s.name().contains("kube-apiserver"));
    }

    #[test]
    fn nomad_http_stub_names_R6_milestone() {
        let s = nomad_http_stub();
        assert!(s.name().contains("R6"));
        assert!(s.name().contains("nomad"));
    }

    #[test]
    fn systemd_dbus_stub_names_R6_milestone() {
        let s = systemd_dbus_stub();
        assert!(s.name().contains("R6"));
        assert!(s.name().contains("systemd"));
    }

    #[test]
    fn supervised_systemd_stub_names_R6_milestone() {
        let s = supervised_systemd_stub();
        assert!(s.name().contains("R6"));
        assert!(s.name().contains("supervised"));
    }

    // ── Heterogeneous Vec — multiple backends behind one type ────

    #[test]
    fn heterogeneous_backend_vec_compiles_and_iterates() {
        let backends: Vec<Box<dyn StoreBackend>> = vec![
            Box::new(crate::face_store::InMemoryStore::new("mem")),
            raft_stub(),
            kube_apiserver_stub(),
            nomad_http_stub(),
            systemd_dbus_stub(),
            supervised_systemd_stub(),
        ];
        assert_eq!(backends.len(), 6);
        // Every backend exposes a name; the in-memory one returns
        // its canonical name regardless of constructor args.
        assert_eq!(backends[0].name(), "in-memory");
        // The 5 stubs name their planned R5/R6 milestone.
        assert!(
            backends[1..]
                .iter()
                .all(|b| { b.name().contains("R5") || b.name().contains("R6") })
        );
    }

    #[test]
    fn in_memory_backend_via_trait_object_round_trips_apply_get() {
        // Trait dispatch works end-to-end: apply through Box<dyn StoreBackend>
        // → get through Box<dyn StoreBackend> → original bytes.
        let backend: Box<dyn StoreBackend> =
            Box::new(crate::face_store::InMemoryStore::new("test"));
        let env = crate::face::encode_native_envelope(&pod_ref(), b"payload").unwrap();
        backend.apply(ResourceFormat::Native, &env).unwrap();
        let got = backend.get(&pod_ref(), ResourceFormat::Native).unwrap();
        assert_eq!(got, env);
        assert_eq!(backend.resource_count(), 1);
    }
}
