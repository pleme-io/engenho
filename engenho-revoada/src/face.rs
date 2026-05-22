//! # Face trait — the contract every fabric renderer satisfies
//!
//! [`crate::fabric::FabricFace`] is the typed **declaration** an
//! operator authors: name + protocol kind. This module ships the
//! **trait** every concrete implementation honors — the lifecycle +
//! identity surface the engenho runtime calls into.
//!
//! Two impls ship in-tree to prove the abstraction holds (per the
//! prime-directive "third site" rule, single-impl abstractions are
//! overengineering; two impls are the minimum proof that the shape
//! generalizes):
//!
//! - [`PureRaftFace`] — the no-rendering face. Exposes the raw
//!   raft state machine via the internal openraft RPC + iroh
//!   content; no external protocol translation. Used when operators
//!   run pleme-io-native tooling and don't want kube-apiserver
//!   overhead.
//! - [`KubernetesFace`] — the current default. Translates the fabric
//!   vocabulary to/from the K8s API (kubectl / CRI / etcd-v3 wire
//!   compat). Delegates the actual reader/writer work to
//!   `engenho-kube-client` + the in-tree kube-apiserver bridge.
//!
//! Future faces (Nomad / Systemd / BareMetalSupervisor) land as
//! additional impls. The trait stays stable; each face is a Reader+
//! Writer pair against the same engenho-types vocabulary.

use std::sync::Mutex;

use crate::fabric::{FabricFace, FaceKind};

/// Errors a face can surface during its lifecycle.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FaceError {
    #[error("face already started")]
    AlreadyStarted,
    #[error("face not started")]
    NotStarted,
    #[error("face start failed: {0}")]
    StartFailed(String),
    #[error("face shutdown failed: {0}")]
    ShutdownFailed(String),
    #[error("face does not support operation: {0}")]
    Unsupported(String),
}

/// The contract every fabric face honors.
///
/// **Lifecycle:** every face is constructed disabled, transitions to
/// running via [`Face::start`], and transitions back to stopped via
/// [`Face::shutdown`]. Operators can swap faces at runtime by
/// shutting down the active face and starting another; the fabric
/// vocabulary stays addressable across the swap.
///
/// **Object-safe by design** — `Send + Sync + 'static` so the
/// runtime can carry `Box<dyn Face>` in its state machine and swap
/// implementations behind a single trait object. The synchronous
/// lifecycle methods are deliberate (per-face complexity belongs in
/// the impl, not in the trait) — async wiring lands in the
/// implementation when each face needs it.
pub trait Face: Send + Sync + 'static {
    /// Operator-facing name from the [`FabricFace`] declaration that
    /// constructed this face. Appears in audit logs + telemetry.
    fn name(&self) -> &str;

    /// The face's protocol kind — what external API does this face
    /// speak? Matches a [`FaceKind`] variant from the declaration.
    fn kind(&self) -> FaceKind;

    /// Transition the face from stopped → running. Idempotent failure:
    /// returns `Err(FaceError::AlreadyStarted)` if called twice
    /// without an intervening shutdown.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the underlying renderer can't initialize
    /// (e.g. K8s face can't bind to its api-server port, Nomad face
    /// can't reach its server). Already-started returns
    /// `AlreadyStarted` so callers can distinguish "you forgot to
    /// shutdown" from "the backend rejected start".
    fn start(&self) -> Result<(), FaceError>;

    /// Transition the face from running → stopped. Returns
    /// `Err(FaceError::NotStarted)` if the face was never started.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the underlying renderer can't gracefully
    /// stop (e.g. K8s face can't drain pending watch streams within
    /// the grace period).
    fn shutdown(&self) -> Result<(), FaceError>;

    /// True iff [`Face::start`] succeeded and [`Face::shutdown`] has
    /// not yet been called. The default implementation reads from
    /// each face's internal state; faces that need richer status
    /// override this.
    fn is_running(&self) -> bool;

    // ── Resource verbs — operator-facing CRUDW contract ─────────
    //
    // Every face exposes the same five verbs over the fabric
    // vocabulary. The default impl is `Err(FaceError::Unsupported)`
    // so faces opt in as they ship their renderer (R6+ for most
    // faces today). Naming the contract uniformly now means
    // operators get a single mental model regardless of which face
    // is active, and the runtime can dispatch generically without
    // peeking at concrete types.
    //
    // The verbs are byte-buffer-oriented at the trait level: each
    // face owns the format negotiation (yaml/json/hcl/native).
    // ResourceFormat + ResourceRef are the typed protocol shapes
    // every face speaks; concrete content stays in the buffer.

    /// Apply (create-or-update) a resource. The body bytes are in
    /// `format` — the face translates to its native protocol.
    ///
    /// # Errors
    ///
    /// Default: `Err(FaceError::Unsupported)`. Faces override as
    /// they ship.
    fn apply_resource(&self, _format: ResourceFormat, _body: &[u8]) -> Result<(), FaceError> {
        Err(FaceError::Unsupported(format!(
            "apply_resource not yet implemented for {}",
            self.name()
        )))
    }

    /// Get a single resource by typed reference. Returns the
    /// resource serialized in `format`.
    ///
    /// # Errors
    ///
    /// Default: `Err(FaceError::Unsupported)`. Faces override as
    /// they ship.
    fn get_resource(
        &self,
        _reference: &ResourceRef,
        _format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "get_resource not yet implemented for {}",
            self.name()
        )))
    }

    /// List all resources of `kind` in `namespace` (or cluster-wide
    /// when `None`). Each entry is serialized in `format`.
    ///
    /// # Errors
    ///
    /// Default: `Err(FaceError::Unsupported)`. Faces override as
    /// they ship.
    fn list_resources(
        &self,
        _kind: &str,
        _namespace: Option<&str>,
        _format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "list_resources not yet implemented for {}",
            self.name()
        )))
    }

    /// Delete a resource by typed reference.
    ///
    /// # Errors
    ///
    /// Default: `Err(FaceError::Unsupported)`. Faces override as
    /// they ship.
    fn delete_resource(&self, _reference: &ResourceRef) -> Result<(), FaceError> {
        Err(FaceError::Unsupported(format!(
            "delete_resource not yet implemented for {}",
            self.name()
        )))
    }

    /// Open a watch on `kind` (in `namespace` or cluster-wide).
    /// Returns a stream of byte-encoded events in `format`.
    ///
    /// The returned trait object owns the watch lifecycle —
    /// dropping it cancels the subscription.
    ///
    /// # Errors
    ///
    /// Default: `Err(FaceError::Unsupported)`. Faces override as
    /// they ship.
    fn watch_resources(
        &self,
        _kind: &str,
        _namespace: Option<&str>,
        _format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        Err(FaceError::Unsupported(format!(
            "watch_resources not yet implemented for {}",
            self.name()
        )))
    }
}

/// Format the face speaks for its resource verbs. Each face owns
/// the set of formats it supports; passing an unsupported format
/// is `Err(FaceError::Unsupported)` from the verb.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceFormat {
    /// Kubernetes-style YAML.
    Yaml,
    /// JSON — universal.
    Json,
    /// HashiCorp Nomad HCL.
    Hcl,
    /// engenho-types native (CBOR-encoded TypedResource — internal).
    Native,
}

/// Typed reference to a single resource. Faces translate this to
/// their native addressing scheme:
///   * K8s: `/api/v1/namespaces/{ns}/{kind}/{name}`
///   * Nomad: `/v1/job/{name}` (namespace mapped to job namespace)
///   * PureRaft: raft key `{kind}/{ns}/{name}`
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourceRef {
    /// Kind name (e.g. "Pod", "Service", "Job"). Face-specific
    /// catalog resolves to the typed engenho-types kind.
    pub kind: String,
    /// Resource name within the namespace.
    pub name: String,
    /// Namespace (e.g. "default" for K8s, "" for cluster-scoped,
    /// `None` for faces that don't model namespaces — Nomad uses
    /// `namespace` for region, PureRaft uses it as a prefix).
    pub namespace: Option<String>,
}

impl ResourceRef {
    /// Convenience constructor for cluster-scoped resources (no
    /// namespace, e.g. K8s Namespace / Node / ClusterRole).
    #[must_use]
    pub fn cluster_scoped(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            namespace: None,
        }
    }

    /// Convenience constructor for namespaced resources (e.g. Pod,
    /// Service, Deployment).
    #[must_use]
    pub fn namespaced(
        kind: impl Into<String>,
        name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
            namespace: Some(namespace.into()),
        }
    }
}

/// The cancellable handle returned by `Face::watch_resources`.
/// Dropping cancels the subscription.
pub trait FaceWatchStream: Send + 'static {
    /// Pull the next event from the stream. Returns `Ok(None)` on
    /// stream end (face shutdown, network drop, etc.); `Ok(Some)`
    /// on each delivered event; `Err` on transient failures.
    ///
    /// Sync API for now — async lands once the runtime picks an
    /// async story for face dispatch.
    ///
    /// # Errors
    ///
    /// Returns transport errors from the underlying watch
    /// connection (network failure, decode failure, etc.).
    fn next_event(&mut self) -> Result<Option<FaceWatchEvent>, FaceError>;
}

/// Single event delivered through a face watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaceWatchEvent {
    pub kind: FaceWatchEventKind,
    /// Resource body in the requested format from `watch_resources`.
    pub body: Vec<u8>,
}

/// Event taxonomy — every face emits events in these categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaceWatchEventKind {
    Added,
    Modified,
    Deleted,
    /// The watch was reset — the consumer should re-fetch state
    /// from scratch. K8s emits this on bookmark gap; Nomad on
    /// index reset; PureRaft on raft snapshot install.
    Reset,
}

// ─────────────────────────────────────────────────────────────────
// PureRaftFace — the no-rendering face
// ─────────────────────────────────────────────────────────────────

/// The pleme-io-native face: no external protocol translation.
///
/// The raft state machine IS the addressable surface. Operators
/// using `engenho-mcp` / `engenho-cli` (future) talk directly to
/// the raft RPC + iroh content layer. No kube-apiserver overhead,
/// no etcd-v3 emulation, no CRI handshake.
///
/// **Why this face matters for the abstraction:** if `Face` were
/// shaped around K8s assumptions, this face wouldn't fit. The fact
/// that it does — lifecycle methods + identity + no
/// renderer-specific knobs — is the structural proof that the trait
/// is face-agnostic.
pub struct PureRaftFace {
    name: String,
    state: Mutex<FaceState>,
    /// Shared verb-impl backend. R5+ replaces this with a raft-
    /// backed store; today it's an in-memory HashMap. The verb
    /// signatures stay byte-identical — the swap is internal.
    store: crate::face_store::InMemoryStore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaceState {
    Stopped,
    Running,
}

impl PureRaftFace {
    /// Construct from a [`FabricFace`] declaration. Returns `None`
    /// if the declaration's kind isn't `PureRaft` (the typed shape
    /// owns dispatch — wrong-kind declarations don't construct).
    ///
    /// The default [`crate::format::AdapterRegistry`] ships with
    /// Native + Json + Yaml adapters — operators can immediately
    /// call `face.apply_resource(Yaml, k8s_manifest)`. To use a
    /// custom registry (e.g. registering an HCL adapter), use
    /// [`Self::with_adapters`] after construction.
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        if decl.kind != FaceKind::PureRaft {
            return None;
        }
        Some(Self {
            name: decl.name.clone(),
            state: Mutex::new(FaceState::Stopped),
            store: crate::face_store::InMemoryStore::new(decl.name.clone()),
        })
    }

    /// Replace the format adapter registry. Useful for tests that
    /// want to inject a stub adapter, or operators registering
    /// custom format families.
    #[must_use]
    pub fn with_adapters(mut self, adapters: crate::format::AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }
}

impl Face for PureRaftFace {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FaceKind {
        FaceKind::PureRaft
    }

    fn start(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Running {
            return Err(FaceError::AlreadyStarted);
        }
        // Pure-raft has no external port to bind — the raft layer
        // is already running underneath. Transitioning state is the
        // entirety of "start" for this face.
        *state = FaceState::Running;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Stopped {
            return Err(FaceError::NotStarted);
        }
        *state = FaceState::Stopped;
        Ok(())
    }

    fn is_running(&self) -> bool {
        let state = self.state.lock().expect("face state mutex poisoned");
        *state == FaceState::Running
    }

    // ── Resource verbs — first concrete impl ──────────────────────
    //
    // PureRaftFace stores raw native-format bytes in an in-memory
    // HashMap keyed by ResourceRef. This is the proof-of-concept
    // contract impl — every other face follows the same shape but
    // routes through its own backend (kube-apiserver, nomad HTTP,
    // systemd dbus, bare-metal supervisor).

    fn apply_resource(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }

    fn get_resource(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }

    fn list_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }

    fn delete_resource(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }

    fn watch_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
}

/// CBOR-encoded envelope used by `PureRaftFace::apply_resource` —
/// carries the reference + payload in one wire shape. This is the
/// `ResourceFormat::Native` for PureRaftFace; other faces define
/// their own native shape.
#[derive(serde::Serialize, serde::Deserialize)]
struct NativeEnvelope {
    #[serde(rename = "ref")]
    reference: ResourceRef,
    payload: Vec<u8>,
}

impl serde::Serialize for ResourceRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = s.serialize_struct("ResourceRef", 3)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("namespace", &self.namespace)?;
        state.end()
    }
}

impl<'de> serde::Deserialize<'de> for ResourceRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Wire {
            kind: String,
            name: String,
            namespace: Option<String>,
        }
        let w = Wire::deserialize(d)?;
        Ok(Self {
            kind: w.kind,
            name: w.name,
            namespace: w.namespace,
        })
    }
}

/// `Sync` `mpsc::Receiver`-backed watch stream. PureRaftFace
/// fans events to channels; this stream pulls from one channel.
struct MpscWatchStream {
    rx: std::sync::mpsc::Receiver<FaceWatchEvent>,
}

impl FaceWatchStream for MpscWatchStream {
    fn next_event(&mut self) -> Result<Option<FaceWatchEvent>, FaceError> {
        match self.rx.recv() {
            Ok(event) => Ok(Some(event)),
            // Sender side dropped (face shutdown / GC) — stream end.
            Err(_) => Ok(None),
        }
    }
}

/// Construct a `NativeEnvelope`-encoded body for use with
/// `PureRaftFace::apply_resource`. Convenience for callers that
/// don't want to depend on ciborium directly.
///
/// # Errors
///
/// Returns the underlying serialization error if encoding fails.
pub fn encode_native_envelope(reference: &ResourceRef, payload: &[u8]) -> Result<Vec<u8>, String> {
    let env = NativeEnvelope {
        reference: reference.clone(),
        payload: payload.to_vec(),
    };
    let mut out = Vec::new();
    ciborium::into_writer(&env, &mut out).map_err(|e| e.to_string())?;
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────
// KubernetesFace — the current default
// ─────────────────────────────────────────────────────────────────

/// The Kubernetes API face — kubectl / CRI / etcd-v3 wire compat.
///
/// Skeleton in this release: lifecycle hooks land here; the actual
/// API-server + watch-stream wiring delegates to
/// `engenho-kube-client` + the in-tree kube-apiserver bridge as it
/// stabilizes through the M0–M4 phases (CNCF conformance target).
///
/// The version string + CNCF-certified flag are carried verbatim
/// from the [`FabricFace`] declaration so audit + telemetry can
/// distinguish faces by their wire version.
pub struct KubernetesFace {
    name: String,
    version: String,
    certified_cncf: bool,
    state: Mutex<FaceState>,
    /// Shared verb-impl backend. R6 swaps this for an actual
    /// kube-apiserver bridge; today the in-memory store covers
    /// the contract end-to-end.
    store: crate::face_store::InMemoryStore,
}

impl KubernetesFace {
    /// Construct from a [`FabricFace`] declaration. Returns `None`
    /// for non-Kubernetes kinds.
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        let (version, certified_cncf) = match &decl.kind {
            FaceKind::Kubernetes {
                version,
                certified_cncf,
            } => (version.clone(), *certified_cncf),
            _ => return None,
        };
        Some(Self {
            name: decl.name.clone(),
            version,
            certified_cncf,
            state: Mutex::new(FaceState::Stopped),
            store: crate::face_store::InMemoryStore::new(decl.name.clone()),
        })
    }

    /// API version this face speaks (e.g. `"1.34"`).
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// True iff this face's `(version)` passed the CNCF Certified
    /// Kubernetes Software Conformance suite at release time.
    #[must_use]
    pub fn is_cncf_certified(&self) -> bool {
        self.certified_cncf
    }

    /// Replace the format adapter registry.
    #[must_use]
    pub fn with_adapters(mut self, adapters: crate::format::AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }
}

impl Face for KubernetesFace {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FaceKind {
        FaceKind::Kubernetes {
            version: self.version.clone(),
            certified_cncf: self.certified_cncf,
        }
    }

    fn start(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Running {
            return Err(FaceError::AlreadyStarted);
        }
        // M0–M4 wiring lands here: bind to api-server port; spawn
        // the watch-stream pump; register with the CRI/CNI bridges.
        // Current release: lifecycle bookkeeping only.
        *state = FaceState::Running;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Stopped {
            return Err(FaceError::NotStarted);
        }
        *state = FaceState::Stopped;
        Ok(())
    }

    fn is_running(&self) -> bool {
        let state = self.state.lock().expect("face state mutex poisoned");
        *state == FaceState::Running
    }

    // Verb delegates — shared in-memory backend (R6 swaps for the
    // real kube-apiserver bridge).
    fn apply_resource(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get_resource(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete_resource(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
}

// ─────────────────────────────────────────────────────────────────
// NomadFace — the third impl (Nomad jobs)
// ─────────────────────────────────────────────────────────────────

/// HashiCorp Nomad face — renders the fabric vocabulary to Nomad
/// job specifications.
///
/// **Why this face matters for the abstraction:** with two impls
/// (PureRaft + Kubernetes) the [`Face`] trait could still be
/// secretly K8s-shaped — PureRaft is the "no-rendering" degenerate
/// case. A genuine *third* renderer that translates to a different
/// non-Kubernetes external API (Nomad jobs, allocations, deployments,
/// task groups — a wholly different resource ontology) is the
/// structural proof that [`Face`] abstracts over arbitrary
/// fabric-to-external-API translations.
///
/// Skeleton in this release: lifecycle hooks land here; the actual
/// `nomad-client` (Rust nomad-api crate) + fabric-to-Nomad
/// translation lands at engenho-revoada R6 as the typed catalog
/// stabilizes. The version string is carried verbatim from the
/// [`FabricFace`] declaration.
pub struct NomadFace {
    name: String,
    version: String,
    state: Mutex<FaceState>,
    /// Shared verb-impl backend; R6 swaps for the real nomad HTTP
    /// client.
    store: crate::face_store::InMemoryStore,
}

impl NomadFace {
    /// Construct from a [`FabricFace`] declaration. Returns `None`
    /// for non-Nomad kinds.
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        let version = match &decl.kind {
            FaceKind::Nomad { version } => version.clone(),
            _ => return None,
        };
        Some(Self {
            name: decl.name.clone(),
            version,
            state: Mutex::new(FaceState::Stopped),
            store: crate::face_store::InMemoryStore::new(decl.name.clone()),
        })
    }

    /// Nomad API version this face speaks (e.g. `"1.7"`).
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Replace the format adapter registry.
    #[must_use]
    pub fn with_adapters(mut self, adapters: crate::format::AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }
}

impl Face for NomadFace {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FaceKind {
        FaceKind::Nomad {
            version: self.version.clone(),
        }
    }

    fn start(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Running {
            return Err(FaceError::AlreadyStarted);
        }
        // R6 wiring lands here: open the nomad HTTP client; subscribe
        // to the /v1/event/stream endpoint; register the
        // fabric-to-Nomad translator. Current release: lifecycle
        // bookkeeping only.
        *state = FaceState::Running;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Stopped {
            return Err(FaceError::NotStarted);
        }
        *state = FaceState::Stopped;
        Ok(())
    }

    fn is_running(&self) -> bool {
        let state = self.state.lock().expect("face state mutex poisoned");
        *state == FaceState::Running
    }

    // Verb delegates — shared in-memory backend (R6 swaps for nomad HTTP).
    fn apply_resource(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get_resource(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete_resource(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
}

// ─────────────────────────────────────────────────────────────────
// SystemdFace — the fourth impl (single-node or supervised-VM)
// ─────────────────────────────────────────────────────────────────

/// systemd face — renders the fabric vocabulary to systemd unit
/// files for non-clustered single-node or supervised-VM deployments.
///
/// **Why this face matters for the abstraction:** with three impls
/// already proving generalization (PureRaft / Kubernetes / Nomad),
/// SystemdFace strengthens the proof in a fourth dimension —
/// generating *files* (unit files on disk) instead of issuing *API
/// calls* (kube-apiserver / nomad-http / raft RPC). The Face trait
/// abstracts equally over both interaction shapes; if it had hidden
/// API-call assumptions, the file-emitting case wouldn't fit. The
/// clean fit IS the structural proof for the second axis.
///
/// `user_units = true` emits units under `~/.config/systemd/user/`
/// (rootless); `false` emits under `/etc/systemd/system/` (system).
/// Skeleton in this release: lifecycle hooks land here; the actual
/// unit-file renderer + dbus-API client land at engenho-revoada R6.
pub struct SystemdFace {
    name: String,
    user_units: bool,
    state: Mutex<FaceState>,
    /// Shared verb-impl backend; R6 swaps for unit-file render +
    /// dbus daemon-reload.
    store: crate::face_store::InMemoryStore,
}

impl SystemdFace {
    /// Construct from a [`FabricFace`] declaration. Returns `None`
    /// for non-Systemd kinds.
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        let user_units = match &decl.kind {
            FaceKind::Systemd { user_units } => *user_units,
            _ => return None,
        };
        Some(Self {
            name: decl.name.clone(),
            user_units,
            state: Mutex::new(FaceState::Stopped),
            store: crate::face_store::InMemoryStore::new(decl.name.clone()),
        })
    }

    /// True iff units are emitted to the user-systemd path
    /// (`~/.config/systemd/user/`) rather than the system path
    /// (`/etc/systemd/system/`).
    #[must_use]
    pub fn is_user_units(&self) -> bool {
        self.user_units
    }

    /// Replace the format adapter registry.
    #[must_use]
    pub fn with_adapters(mut self, adapters: crate::format::AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }
}

impl Face for SystemdFace {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FaceKind {
        FaceKind::Systemd {
            user_units: self.user_units,
        }
    }

    fn start(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Running {
            return Err(FaceError::AlreadyStarted);
        }
        // R6 wiring lands here: render unit files; daemon-reload via
        // dbus; subscribe to unit state-change events. Current
        // release: lifecycle bookkeeping only.
        *state = FaceState::Running;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Stopped {
            return Err(FaceError::NotStarted);
        }
        *state = FaceState::Stopped;
        Ok(())
    }

    fn is_running(&self) -> bool {
        let state = self.state.lock().expect("face state mutex poisoned");
        *state == FaceState::Running
    }

    // Verb delegates — shared in-memory backend (R6 swaps for unit-file render + dbus).
    fn apply_resource(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get_resource(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete_resource(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
}

// ─────────────────────────────────────────────────────────────────
// BareMetalSupervisorFace — the fifth impl, completes the enum
// ─────────────────────────────────────────────────────────────────

/// Bare-metal supervisor face — systemd-orchestrated containers
/// without a Kubernetes apiserver.
///
/// **Why this face matters for the abstraction:** with four impls
/// already (PureRaft / Kubernetes / Nomad / Systemd) the Face
/// trait generalizes across interaction shapes + ontology. The
/// fifth impl completes the FaceKind enumeration so
/// `instantiate()` has no `Unsupported` arm — every typed face
/// declaration is now constructible. The "what if someone declares
/// X?" question is gone; the type system has answers for the full
/// surface.
///
/// Skeleton in this release: lifecycle hooks land here; the actual
/// supervised-container renderer (drop systemd units onto a host;
/// supervise via the host's existing systemd; no apiserver, no
/// raft cluster) lands at engenho-revoada R6.
pub struct BareMetalSupervisorFace {
    name: String,
    state: Mutex<FaceState>,
    /// Shared verb-impl backend; R6 swaps for systemd-orchestrated
    /// container supervision.
    store: crate::face_store::InMemoryStore,
}

impl BareMetalSupervisorFace {
    /// Construct from a [`FabricFace`] declaration. Returns `None`
    /// for non-BareMetalSupervisor kinds.
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        if decl.kind != FaceKind::BareMetalSupervisor {
            return None;
        }
        Some(Self {
            name: decl.name.clone(),
            state: Mutex::new(FaceState::Stopped),
            store: crate::face_store::InMemoryStore::new(decl.name.clone()),
        })
    }

    /// Replace the format adapter registry.
    #[must_use]
    pub fn with_adapters(mut self, adapters: crate::format::AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }
}

impl Face for BareMetalSupervisorFace {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> FaceKind {
        FaceKind::BareMetalSupervisor
    }

    fn start(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Running {
            return Err(FaceError::AlreadyStarted);
        }
        // R6 wiring lands here: render systemd units; supervise via
        // the host's existing systemd; expose status via a small
        // local HTTP socket. Current release: lifecycle bookkeeping
        // only.
        *state = FaceState::Running;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), FaceError> {
        let mut state = self.state.lock().expect("face state mutex poisoned");
        if *state == FaceState::Stopped {
            return Err(FaceError::NotStarted);
        }
        *state = FaceState::Stopped;
        Ok(())
    }

    fn is_running(&self) -> bool {
        let state = self.state.lock().expect("face state mutex poisoned");
        *state == FaceState::Running
    }

    // Verb delegates — shared in-memory backend (R6 swaps for supervised containers).
    fn apply_resource(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get_resource(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete_resource(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch_resources(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
}

// ─────────────────────────────────────────────────────────────────
// Construct any face from a typed declaration
// ─────────────────────────────────────────────────────────────────

/// Build a concrete [`Face`] from a typed [`FabricFace`] declaration.
/// Future faces (Nomad / Systemd / BareMetalSupervisor) add arms
/// here as they ship.
///
/// # Errors
///
/// Returns `Err(FaceError::Unsupported)` for face kinds that don't
/// have a concrete impl yet (Nomad / Systemd / BareMetalSupervisor
/// at present).
pub fn instantiate(decl: &FabricFace) -> Result<Box<dyn Face>, FaceError> {
    match &decl.kind {
        FaceKind::PureRaft => Ok(Box::new(
            PureRaftFace::from_declaration(decl)
                .expect("kind matched, declaration must construct"),
        )),
        FaceKind::Kubernetes { .. } => Ok(Box::new(
            KubernetesFace::from_declaration(decl)
                .expect("kind matched, declaration must construct"),
        )),
        FaceKind::Nomad { .. } => Ok(Box::new(
            NomadFace::from_declaration(decl)
                .expect("kind matched, declaration must construct"),
        )),
        FaceKind::Systemd { .. } => Ok(Box::new(
            SystemdFace::from_declaration(decl)
                .expect("kind matched, declaration must construct"),
        )),
        FaceKind::BareMetalSupervisor => Ok(Box::new(
            BareMetalSupervisorFace::from_declaration(decl)
                .expect("kind matched, declaration must construct"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raft_decl() -> FabricFace {
        FabricFace {
            name: "pure-raft-test".into(),
            kind: FaceKind::PureRaft,
        }
    }

    fn k8s_decl() -> FabricFace {
        FabricFace::prescribed_kubernetes_v1_34()
    }

    // ── PureRaftFace ──────────────────────────────────────────────

    #[test]
    fn pure_raft_constructs_from_matching_declaration() {
        let face = PureRaftFace::from_declaration(&raft_decl());
        assert!(face.is_some());
    }

    #[test]
    fn pure_raft_rejects_non_matching_declaration() {
        let face = PureRaftFace::from_declaration(&k8s_decl());
        assert!(face.is_none());
    }

    #[test]
    fn pure_raft_lifecycle_starts_then_stops() {
        let face = PureRaftFace::from_declaration(&raft_decl()).unwrap();
        assert!(!face.is_running());
        assert_eq!(face.start(), Ok(()));
        assert!(face.is_running());
        assert_eq!(face.shutdown(), Ok(()));
        assert!(!face.is_running());
    }

    #[test]
    fn pure_raft_double_start_errors() {
        let face = PureRaftFace::from_declaration(&raft_decl()).unwrap();
        face.start().unwrap();
        assert_eq!(face.start(), Err(FaceError::AlreadyStarted));
    }

    #[test]
    fn pure_raft_shutdown_without_start_errors() {
        let face = PureRaftFace::from_declaration(&raft_decl()).unwrap();
        assert_eq!(face.shutdown(), Err(FaceError::NotStarted));
    }

    // ── KubernetesFace ────────────────────────────────────────────

    #[test]
    fn kubernetes_constructs_from_matching_declaration() {
        let face = KubernetesFace::from_declaration(&k8s_decl()).unwrap();
        assert_eq!(face.version(), "1.34");
        assert!(face.is_cncf_certified());
    }

    #[test]
    fn kubernetes_rejects_non_matching_declaration() {
        let face = KubernetesFace::from_declaration(&raft_decl());
        assert!(face.is_none());
    }

    #[test]
    fn kubernetes_lifecycle_starts_then_stops() {
        let face = KubernetesFace::from_declaration(&k8s_decl()).unwrap();
        assert!(!face.is_running());
        face.start().unwrap();
        assert!(face.is_running());
        face.shutdown().unwrap();
        assert!(!face.is_running());
    }

    #[test]
    fn kubernetes_double_start_errors() {
        let face = KubernetesFace::from_declaration(&k8s_decl()).unwrap();
        face.start().unwrap();
        assert_eq!(face.start(), Err(FaceError::AlreadyStarted));
    }

    // ── The trait abstracts cleanly ───────────────────────────────

    #[test]
    fn face_trait_is_object_safe_send_sync_static() {
        // Compile-time check: if Face isn't Send + Sync + 'static,
        // this won't typecheck. Runtime assertions are trivially
        // true; the value is the type-level constraint.
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<Box<dyn Face>>();
    }

    #[test]
    fn instantiate_pure_raft_returns_running_face() {
        let face = instantiate(&raft_decl()).unwrap();
        assert_eq!(face.name(), "pure-raft-test");
        assert_eq!(face.kind(), FaceKind::PureRaft);
        face.start().unwrap();
        assert!(face.is_running());
    }

    #[test]
    fn instantiate_kubernetes_returns_typed_face() {
        let face = instantiate(&k8s_decl()).unwrap();
        match face.kind() {
            FaceKind::Kubernetes {
                version,
                certified_cncf,
            } => {
                assert_eq!(version, "1.34");
                assert!(certified_cncf);
            }
            other => panic!("expected Kubernetes face, got {other:?}"),
        }
    }

    fn nomad_decl() -> FabricFace {
        FabricFace {
            name: "nomad-1.7".into(),
            kind: FaceKind::Nomad {
                version: "1.7".into(),
            },
        }
    }

    // ── NomadFace ─────────────────────────────────────────────────

    #[test]
    fn nomad_constructs_from_matching_declaration() {
        let face = NomadFace::from_declaration(&nomad_decl()).unwrap();
        assert_eq!(face.version(), "1.7");
        assert_eq!(face.name(), "nomad-1.7");
    }

    #[test]
    fn nomad_rejects_non_matching_declaration() {
        let face = NomadFace::from_declaration(&k8s_decl());
        assert!(face.is_none());
        let face = NomadFace::from_declaration(&raft_decl());
        assert!(face.is_none());
    }

    #[test]
    fn nomad_lifecycle_starts_then_stops() {
        let face = NomadFace::from_declaration(&nomad_decl()).unwrap();
        assert!(!face.is_running());
        face.start().unwrap();
        assert!(face.is_running());
        face.shutdown().unwrap();
        assert!(!face.is_running());
    }

    #[test]
    fn nomad_double_start_errors() {
        let face = NomadFace::from_declaration(&nomad_decl()).unwrap();
        face.start().unwrap();
        assert_eq!(face.start(), Err(FaceError::AlreadyStarted));
    }

    #[test]
    fn instantiate_nomad_returns_running_face() {
        match instantiate(&nomad_decl()) {
            Ok(face) => {
                assert_eq!(face.name(), "nomad-1.7");
                match face.kind() {
                    FaceKind::Nomad { version } => assert_eq!(version, "1.7"),
                    other => panic!("expected Nomad face, got {other:?}"),
                }
            }
            Err(e) => panic!("Nomad face should construct, got error {e}"),
        }
    }

    fn systemd_decl(user_units: bool) -> FabricFace {
        FabricFace {
            name: if user_units { "systemd-user" } else { "systemd-system" }.into(),
            kind: FaceKind::Systemd { user_units },
        }
    }

    // ── SystemdFace ───────────────────────────────────────────────

    #[test]
    fn systemd_constructs_from_matching_declaration() {
        let face = SystemdFace::from_declaration(&systemd_decl(true)).unwrap();
        assert_eq!(face.name(), "systemd-user");
        assert!(face.is_user_units());
    }

    #[test]
    fn systemd_carries_user_vs_system_distinction() {
        let user = SystemdFace::from_declaration(&systemd_decl(true)).unwrap();
        let system = SystemdFace::from_declaration(&systemd_decl(false)).unwrap();
        assert!(user.is_user_units());
        assert!(!system.is_user_units());
    }

    #[test]
    fn systemd_rejects_non_matching_declaration() {
        assert!(SystemdFace::from_declaration(&k8s_decl()).is_none());
        assert!(SystemdFace::from_declaration(&raft_decl()).is_none());
        assert!(SystemdFace::from_declaration(&nomad_decl()).is_none());
    }

    #[test]
    fn systemd_lifecycle_starts_then_stops() {
        let face = SystemdFace::from_declaration(&systemd_decl(false)).unwrap();
        face.start().unwrap();
        assert!(face.is_running());
        face.shutdown().unwrap();
        assert!(!face.is_running());
    }

    #[test]
    fn instantiate_systemd_returns_running_face() {
        match instantiate(&systemd_decl(false)) {
            Ok(face) => {
                assert_eq!(face.name(), "systemd-system");
                match face.kind() {
                    FaceKind::Systemd { user_units } => assert!(!user_units),
                    other => panic!("expected Systemd face, got {other:?}"),
                }
            }
            Err(e) => panic!("Systemd face should construct, got error {e}"),
        }
    }

    fn bms_decl() -> FabricFace {
        FabricFace {
            name: "bms-test".into(),
            kind: FaceKind::BareMetalSupervisor,
        }
    }

    // ── BareMetalSupervisorFace ───────────────────────────────────

    #[test]
    fn bms_constructs_from_matching_declaration() {
        let face = BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap();
        assert_eq!(face.name(), "bms-test");
        assert_eq!(face.kind(), FaceKind::BareMetalSupervisor);
    }

    #[test]
    fn bms_rejects_non_matching_declaration() {
        assert!(BareMetalSupervisorFace::from_declaration(&k8s_decl()).is_none());
        assert!(BareMetalSupervisorFace::from_declaration(&raft_decl()).is_none());
        assert!(BareMetalSupervisorFace::from_declaration(&nomad_decl()).is_none());
    }

    #[test]
    fn bms_lifecycle_starts_then_stops() {
        let face = BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap();
        face.start().unwrap();
        assert!(face.is_running());
        face.shutdown().unwrap();
        assert!(!face.is_running());
    }

    #[test]
    fn instantiate_bare_metal_supervisor_now_returns_running_face() {
        match instantiate(&bms_decl()) {
            Ok(face) => {
                assert_eq!(face.name(), "bms-test");
                assert_eq!(face.kind(), FaceKind::BareMetalSupervisor);
            }
            Err(e) => panic!("BMS face should construct, got error {e}"),
        }
    }

    #[test]
    fn instantiate_covers_every_face_kind_with_no_unsupported_arm() {
        // The full enumeration: every FaceKind variant is now
        // constructible through instantiate(). The "what if someone
        // declares X?" question is gone — the type system has
        // answers for the full surface.
        let kinds = vec![
            FaceKind::PureRaft,
            FaceKind::Kubernetes {
                version: "1.34".into(),
                certified_cncf: true,
            },
            FaceKind::Nomad { version: "1.7".into() },
            FaceKind::Systemd { user_units: false },
            FaceKind::BareMetalSupervisor,
        ];
        for kind in kinds {
            let decl = FabricFace {
                name: format!("{kind:?}"),
                kind,
            };
            match instantiate(&decl) {
                Ok(_) => {}
                Err(e) => panic!("FaceKind {:?} failed to instantiate: {e}", decl.kind),
            }
        }
    }

    #[test]
    fn five_faces_compose_in_a_single_vec() {
        // FIVE impls now — the complete enumeration of the
        // FabricFace::FaceKind variants. Vec<Box<dyn Face>>
        // composes all of them; each is a renderer extension
        // around the SAME engenho-types vocabulary.
        let faces: Vec<Box<dyn Face>> = vec![
            Box::new(PureRaftFace::from_declaration(&raft_decl()).unwrap()),
            Box::new(KubernetesFace::from_declaration(&k8s_decl()).unwrap()),
            Box::new(NomadFace::from_declaration(&nomad_decl()).unwrap()),
            Box::new(SystemdFace::from_declaration(&systemd_decl(false)).unwrap()),
            Box::new(BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap()),
        ];
        assert_eq!(faces.len(), 5);
        for face in &faces {
            assert!(!face.is_running());
        }
    }

    // ── Resource verbs — default Unsupported behavior ─────────────

    // NOTE: All 5 faces now share the InMemoryStore verb backend
    // (face_store::InMemoryStore). The verbs work uniformly on
    // every face today; per-face R6 backends replace the store
    // without changing the operator-facing contract.

    fn yaml_manifest(name: &str, ns: &str) -> Vec<u8> {
        format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec:\n  containers:\n    - name: c\n      image: nginx\n"
        )
        .into_bytes()
    }

    #[test]
    fn kubernetes_face_apply_get_yaml_round_trips() {
        let face = KubernetesFace::from_declaration(&k8s_decl()).unwrap();
        let yaml = yaml_manifest("nginx", "default");
        face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let back = face.get_resource(&r, ResourceFormat::Yaml).unwrap();
        assert_eq!(back, yaml);
    }

    #[test]
    fn nomad_face_apply_get_json_round_trips() {
        let face = NomadFace::from_declaration(&nomad_decl()).unwrap();
        let json = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "nomad.io/v1",
            "kind": "Job",
            "metadata": { "name": "web", "namespace": "global" }
        }))
        .unwrap();
        face.apply_resource(ResourceFormat::Json, &json).unwrap();
        let r = ResourceRef::namespaced("Job", "web", "global");
        let back = face.get_resource(&r, ResourceFormat::Json).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn systemd_face_list_aggregates_applied_resources() {
        let face = SystemdFace::from_declaration(&systemd_decl(false)).unwrap();
        let y1 = yaml_manifest("a", "default");
        let y2 = yaml_manifest("b", "default");
        face.apply_resource(ResourceFormat::Yaml, &y1).unwrap();
        face.apply_resource(ResourceFormat::Yaml, &y2).unwrap();
        let listed = face
            .list_resources("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        assert_eq!(listed.len(), 2);
    }

    #[test]
    fn bms_face_watch_streams_events() {
        let face = BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap();
        let mut watch = face
            .watch_resources("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        let yaml = yaml_manifest("nginx", "default");
        face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
        let ev = watch.next_event().unwrap().expect("event");
        assert_eq!(ev.kind, FaceWatchEventKind::Added);
    }

    #[test]
    fn delete_resource_missing_errors_uniformly_across_faces() {
        let r = ResourceRef::namespaced("Pod", "missing", "default");
        let k = KubernetesFace::from_declaration(&k8s_decl()).unwrap();
        let n = NomadFace::from_declaration(&nomad_decl()).unwrap();
        let s = SystemdFace::from_declaration(&systemd_decl(false)).unwrap();
        let b = BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap();
        let p = PureRaftFace::from_declaration(&raft_decl()).unwrap();
        for (label, result) in [
            ("k8s", k.delete_resource(&r)),
            ("nomad", n.delete_resource(&r)),
            ("systemd", s.delete_resource(&r)),
            ("bms", b.delete_resource(&r)),
            ("pure-raft", p.delete_resource(&r)),
        ] {
            match result {
                Err(FaceError::Unsupported(msg)) => {
                    assert!(msg.contains("no resource"), "{label}: msg: {msg}");
                }
                other => panic!("{label}: expected Unsupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn all_five_faces_apply_get_uniform_across_yaml() {
        // The canonical "across all 5 faces" cross-face test —
        // operator semantics are identical regardless of which
        // face is active.
        let faces: Vec<Box<dyn Face>> = vec![
            Box::new(PureRaftFace::from_declaration(&raft_decl()).unwrap()),
            Box::new(KubernetesFace::from_declaration(&k8s_decl()).unwrap()),
            Box::new(NomadFace::from_declaration(&nomad_decl()).unwrap()),
            Box::new(SystemdFace::from_declaration(&systemd_decl(false)).unwrap()),
            Box::new(BareMetalSupervisorFace::from_declaration(&bms_decl()).unwrap()),
        ];
        let yaml = yaml_manifest("nginx", "default");
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        for face in &faces {
            face.apply_resource(ResourceFormat::Yaml, &yaml).unwrap();
            let back = face.get_resource(&r, ResourceFormat::Yaml).unwrap();
            assert_eq!(back, yaml, "face {} should round-trip YAML", face.name());
        }
    }

    // ── PureRaftFace verb impls — first concrete face ─────────────

    fn raft_face() -> PureRaftFace {
        PureRaftFace::from_declaration(&raft_decl()).unwrap()
    }

    fn pod_ref(name: &str, ns: &str) -> ResourceRef {
        ResourceRef::namespaced("Pod", name, ns)
    }

    fn envelope(reference: &ResourceRef, payload: &[u8]) -> Vec<u8> {
        encode_native_envelope(reference, payload).expect("envelope encode")
    }

    #[test]
    fn pure_raft_apply_then_get_round_trips_envelope() {
        // New adapter contract: Native is symmetric pass-through.
        // Apply takes a CBOR envelope; get returns the same CBOR
        // envelope. Operators who want the payload-only shape use
        // a YAML/JSON adapter (those round-trip via the operator's
        // chosen format).
        let face = raft_face();
        let r = pod_ref("nginx", "default");
        let env = envelope(&r, b"my-payload");
        face.apply_resource(ResourceFormat::Native, &env).unwrap();
        let got = face
            .get_resource(&r, ResourceFormat::Native)
            .expect("get after apply");
        assert_eq!(got, env);
    }

    #[test]
    fn pure_raft_apply_yaml_now_works_via_adapter_registry() {
        // The default AdapterRegistry includes K8sYamlAdapter, so
        // operators can apply real K8s YAML manifests directly.
        // The face extracts metadata.name/namespace and stores
        // the envelope; get with format=Yaml returns the original
        // operator bytes.
        let face = raft_face();
        let yaml = b"apiVersion: v1\nkind: Pod\nmetadata:\n  name: nginx\n  namespace: default\nspec:\n  containers:\n    - name: c\n      image: nginx\n";
        face.apply_resource(ResourceFormat::Yaml, yaml)
            .expect("YAML apply should succeed via K8sYamlAdapter");
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let back = face
            .get_resource(&r, ResourceFormat::Yaml)
            .expect("YAML get should succeed");
        assert_eq!(back, yaml);
    }

    #[test]
    fn pure_raft_apply_json_works_via_adapter_registry() {
        let face = raft_face();
        let json = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "nginx", "namespace": "default" },
            "spec": { "containers": [{"name": "c", "image": "nginx"}] }
        }))
        .unwrap();
        face.apply_resource(ResourceFormat::Json, &json)
            .expect("JSON apply should succeed");
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let back = face
            .get_resource(&r, ResourceFormat::Json)
            .expect("JSON get should succeed");
        assert_eq!(back, json);
    }

    #[test]
    fn pure_raft_with_custom_adapter_registry_overrides_default() {
        // Build a face that ONLY accepts Native (custom registry
        // explicitly without YAML/JSON adapters). Confirms the
        // builder hook works + the unsupported-format error
        // surfaces cleanly.
        let face = PureRaftFace::from_declaration(&raft_decl())
            .unwrap()
            .with_adapters({
                let mut r = crate::format::AdapterRegistry::empty();
                r.register(std::sync::Arc::new(
                    crate::format::NativePassthroughAdapter,
                ));
                r
            });
        let r = pod_ref("nginx", "default");
        // YAML now rejected because no YAML adapter is registered.
        match face.apply_resource(ResourceFormat::Yaml, b"apiVersion: v1\nkind: Pod\nmetadata: {name: x}\n") {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("Yaml") || msg.contains("UnsupportedFormat"), "msg: {msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
        // Native still works.
        face.apply_resource(ResourceFormat::Native, &envelope(&r, b"x"))
            .expect("Native still works");
    }

    #[test]
    fn pure_raft_invalid_yaml_apply_returns_clear_parse_error() {
        let face = raft_face();
        // Missing required kind field.
        let yaml = b"apiVersion: v1\nmetadata:\n  name: nginx\n";
        match face.apply_resource(ResourceFormat::Yaml, yaml) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("kind"), "msg should mention missing kind: {msg}");
            }
            other => panic!("expected Unsupported (MissingField kind), got {other:?}"),
        }
    }

    #[test]
    fn pure_raft_list_yaml_returns_each_envelope_as_yaml() {
        let face = raft_face();
        let yaml_a = b"apiVersion: v1\nkind: Pod\nmetadata:\n  name: a\n  namespace: default\n";
        let yaml_b = b"apiVersion: v1\nkind: Pod\nmetadata:\n  name: b\n  namespace: default\n";
        face.apply_resource(ResourceFormat::Yaml, yaml_a).unwrap();
        face.apply_resource(ResourceFormat::Yaml, yaml_b).unwrap();
        let listed = face
            .list_resources("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        assert_eq!(listed.len(), 2);
        let mut got: Vec<&[u8]> = listed.iter().map(Vec::as_slice).collect();
        got.sort();
        let mut want = vec![yaml_a.as_slice(), yaml_b.as_slice()];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn pure_raft_get_missing_resource_errors() {
        let face = raft_face();
        let r = pod_ref("does-not-exist", "default");
        match face.get_resource(&r, ResourceFormat::Native) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("no resource"), "msg: {msg}");
            }
            other => panic!("expected Unsupported (no resource), got {other:?}"),
        }
    }

    #[test]
    fn pure_raft_apply_updates_existing_with_modified_event() {
        let face = raft_face();
        let r = pod_ref("nginx", "default");
        let v1 = envelope(&r, b"v1");
        let v2 = envelope(&r, b"v2");
        face.apply_resource(ResourceFormat::Native, &v1).unwrap();
        face.apply_resource(ResourceFormat::Native, &v2).unwrap();
        let got = face.get_resource(&r, ResourceFormat::Native).unwrap();
        assert_eq!(got, v2);
    }

    #[test]
    fn pure_raft_list_returns_all_of_kind_in_namespace() {
        let face = raft_face();
        let env_a = envelope(&pod_ref("a", "default"), b"A");
        let env_b = envelope(&pod_ref("b", "default"), b"B");
        let env_c = envelope(&pod_ref("c", "other"), b"C");
        face.apply_resource(ResourceFormat::Native, &env_a).unwrap();
        face.apply_resource(ResourceFormat::Native, &env_b).unwrap();
        face.apply_resource(ResourceFormat::Native, &env_c).unwrap();
        let in_default = face
            .list_resources("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        assert_eq!(in_default.len(), 2);
        let mut got: Vec<Vec<u8>> = in_default;
        got.sort();
        let mut want = vec![env_a, env_b];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn pure_raft_delete_removes_then_get_errors() {
        let face = raft_face();
        let r = pod_ref("nginx", "default");
        face.apply_resource(ResourceFormat::Native, &envelope(&r, b"x"))
            .unwrap();
        face.delete_resource(&r).unwrap();
        match face.get_resource(&r, ResourceFormat::Native) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("no resource"), "msg: {msg}");
            }
            other => panic!("expected Unsupported after delete, got {other:?}"),
        }
    }

    #[test]
    fn pure_raft_delete_missing_resource_errors() {
        let face = raft_face();
        let r = pod_ref("missing", "default");
        match face.delete_resource(&r) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("no resource"), "msg: {msg}");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn pure_raft_watch_replays_existing_state_as_added() {
        let face = raft_face();
        let env_a = envelope(&pod_ref("a", "default"), b"A");
        let env_b = envelope(&pod_ref("b", "default"), b"B");
        face.apply_resource(ResourceFormat::Native, &env_a).unwrap();
        face.apply_resource(ResourceFormat::Native, &env_b).unwrap();
        let mut watch = face
            .watch_resources("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        // Drain two Added events (replay of existing state).
        // Watch emits raw envelope bytes (Native shape) — see
        // the watch_resources comment in face.rs explaining why
        // watch doesn't run from_native at fan-out time.
        let mut got: Vec<Vec<u8>> = Vec::new();
        for _ in 0..2 {
            let ev = watch.next_event().unwrap().expect("event");
            assert_eq!(ev.kind, FaceWatchEventKind::Added);
            got.push(ev.body);
        }
        got.sort();
        let mut want = vec![env_a, env_b];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn pure_raft_watch_streams_modified_then_deleted() {
        use std::sync::Arc;
        use std::thread;
        let face = Arc::new(raft_face());
        let r = pod_ref("nginx", "default");
        let v1 = envelope(&r, b"v1");
        let v2 = envelope(&r, b"v2");
        face.apply_resource(ResourceFormat::Native, &v1).unwrap();
        let mut watch = face
            .watch_resources("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        // Drain the replay of v1 (Added).
        let replay = watch.next_event().unwrap().expect("replay");
        assert_eq!(replay.kind, FaceWatchEventKind::Added);
        assert_eq!(replay.body, v1);
        // Mutate on another thread to exercise the cross-thread fan-out.
        let face2 = Arc::clone(&face);
        let r2 = r.clone();
        let v2_clone = v2.clone();
        let writer = thread::spawn(move || {
            face2
                .apply_resource(ResourceFormat::Native, &v2_clone)
                .unwrap();
            face2.delete_resource(&r2).unwrap();
        });
        let mod_ev = watch.next_event().unwrap().expect("mod");
        assert_eq!(mod_ev.kind, FaceWatchEventKind::Modified);
        assert_eq!(mod_ev.body, v2);
        let del_ev = watch.next_event().unwrap().expect("del");
        assert_eq!(del_ev.kind, FaceWatchEventKind::Deleted);
        assert_eq!(del_ev.body, v2);
        writer.join().unwrap();
    }

    #[test]
    fn pure_raft_watch_filters_by_kind() {
        let face = raft_face();
        let mut pod_watch = face
            .watch_resources("Pod", None, ResourceFormat::Native)
            .unwrap();
        // Apply a Service — should NOT reach the Pod watch.
        face.apply_resource(
            ResourceFormat::Native,
            &envelope(
                &ResourceRef::namespaced("Service", "frontend", "default"),
                b"S",
            ),
        )
        .unwrap();
        // Apply a Pod — SHOULD reach the watch.
        let pod_env = envelope(&pod_ref("nginx", "default"), b"P");
        face.apply_resource(ResourceFormat::Native, &pod_env).unwrap();
        let ev = pod_watch.next_event().unwrap().expect("pod event");
        assert_eq!(ev.body, pod_env);
    }

    #[test]
    fn pure_raft_watch_filters_by_namespace() {
        let face = raft_face();
        let mut watch = face
            .watch_resources("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        face.apply_resource(
            ResourceFormat::Native,
            &envelope(&pod_ref("a", "other"), b"O"),
        )
        .unwrap();
        let default_env = envelope(&pod_ref("b", "default"), b"D");
        face.apply_resource(ResourceFormat::Native, &default_env).unwrap();
        let ev = watch.next_event().unwrap().expect("event");
        // Only the "default" namespace event arrives.
        assert_eq!(ev.body, default_env);
    }

    #[test]
    fn pure_raft_watch_multiple_subscribers_all_receive_events() {
        let face = raft_face();
        let mut w1 = face
            .watch_resources("Pod", None, ResourceFormat::Native)
            .unwrap();
        let mut w2 = face
            .watch_resources("Pod", None, ResourceFormat::Native)
            .unwrap();
        let pod_env = envelope(&pod_ref("nginx", "default"), b"x");
        face.apply_resource(ResourceFormat::Native, &pod_env).unwrap();
        let e1 = w1.next_event().unwrap().expect("w1 event");
        let e2 = w2.next_event().unwrap().expect("w2 event");
        assert_eq!(e1.body, pod_env);
        assert_eq!(e2.body, pod_env);
    }

    #[test]
    fn encode_native_envelope_round_trips_through_apply() {
        // Operator-facing helper produces bytes that apply accepts.
        let face = raft_face();
        let r = pod_ref("nginx", "default");
        let env = encode_native_envelope(&r, b"payload").unwrap();
        face.apply_resource(ResourceFormat::Native, &env).unwrap();
        let got = face.get_resource(&r, ResourceFormat::Native).unwrap();
        // get returns the full envelope under the new adapter
        // contract (Native is symmetric pass-through).
        assert_eq!(got, env);
    }

    // ── ResourceRef typed constructors ───────────────────────────

    #[test]
    fn resource_ref_cluster_scoped_has_no_namespace() {
        let r = ResourceRef::cluster_scoped("Namespace", "default");
        assert_eq!(r.kind, "Namespace");
        assert_eq!(r.name, "default");
        assert_eq!(r.namespace, None);
    }

    #[test]
    fn resource_ref_namespaced_carries_namespace() {
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        assert_eq!(r.kind, "Pod");
        assert_eq!(r.name, "nginx");
        assert_eq!(r.namespace, Some("default".into()));
    }

    #[test]
    fn resource_ref_is_hashable() {
        // Faces will use ResourceRef as HashMap keys for in-memory
        // caches; verify the hash impl exists.
        use std::collections::HashSet;
        let mut s: HashSet<ResourceRef> = HashSet::new();
        s.insert(ResourceRef::namespaced("Pod", "a", "default"));
        s.insert(ResourceRef::namespaced("Pod", "a", "default"));
        s.insert(ResourceRef::namespaced("Pod", "b", "default"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn four_faces_compose_in_a_single_vec() {
        // FOUR impls now in a single Vec<Box<dyn Face>>:
        //   PureRaft  — no rendering at all
        //   Kubernetes — kube-apiserver API calls
        //   Nomad      — nomad HTTP API calls
        //   Systemd    — unit-file emission (file-based, not API!)
        // The fourth impl proves generalization across a SECOND
        // axis: the trait abstracts over both "API call" and "file
        // emission" interaction shapes. If it had hidden API-call
        // assumptions, SystemdFace wouldn't fit cleanly.
        let faces: Vec<Box<dyn Face>> = vec![
            Box::new(PureRaftFace::from_declaration(&raft_decl()).unwrap()),
            Box::new(KubernetesFace::from_declaration(&k8s_decl()).unwrap()),
            Box::new(NomadFace::from_declaration(&nomad_decl()).unwrap()),
            Box::new(SystemdFace::from_declaration(&systemd_decl(false)).unwrap()),
        ];
        assert_eq!(faces.len(), 4);
        let kinds: Vec<&'static str> = faces.iter().map(|f| match f.kind() {
            FaceKind::PureRaft => "PureRaft",
            FaceKind::Kubernetes { .. } => "Kubernetes",
            FaceKind::Nomad { .. } => "Nomad",
            FaceKind::Systemd { .. } => "Systemd",
            FaceKind::BareMetalSupervisor => "BareMetalSupervisor",
        }).collect();
        assert_eq!(
            kinds,
            vec!["PureRaft", "Kubernetes", "Nomad", "Systemd"],
        );
    }

    #[test]
    fn three_faces_compose_in_a_single_vec() {
        // The third-impl proof: Vec<Box<dyn Face>> carries
        // PureRaft (no rendering) + Kubernetes (api-server)
        // + Nomad (job-based, non-K8s ontology) uniformly.
        // If Face were K8s-shaped under the covers, NomadFace
        // wouldn't fit cleanly — its job/allocation vocabulary
        // is foreign to the K8s catalog. The clean fit IS the
        // proof.
        let faces: Vec<Box<dyn Face>> = vec![
            Box::new(PureRaftFace::from_declaration(&raft_decl()).unwrap()),
            Box::new(KubernetesFace::from_declaration(&k8s_decl()).unwrap()),
            Box::new(NomadFace::from_declaration(&nomad_decl()).unwrap()),
        ];
        assert_eq!(faces.len(), 3);
        let names: Vec<&str> = faces.iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec!["pure-raft-test", "k8s-v1.34", "nomad-1.7"],
        );
        for face in &faces {
            assert!(!face.is_running());
        }
    }

    #[test]
    fn faces_can_be_carried_as_boxed_trait_objects() {
        // The whole point of the trait: heterogeneous faces live in
        // one Vec<Box<dyn Face>> the runtime swaps between.
        let faces: Vec<Box<dyn Face>> = vec![
            Box::new(PureRaftFace::from_declaration(&raft_decl()).unwrap()),
            Box::new(KubernetesFace::from_declaration(&k8s_decl()).unwrap()),
        ];
        assert_eq!(faces.len(), 2);
        // Each face honors the same trait API.
        for face in &faces {
            assert!(!face.is_running());
        }
    }

    #[test]
    fn round_trip_declaration_through_face_and_back() {
        // Declaration → face → kind() must round-trip exactly so
        // the runtime can serialize back the active face for
        // observability without losing information.
        let decl = k8s_decl();
        let face = instantiate(&decl).unwrap();
        assert_eq!(face.kind(), decl.kind);
    }
}
