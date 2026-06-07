//! # Cluster declaration — the typed triple
//!
//! A cluster is the composition of three orthogonal typed surfaces:
//!
//! - [`FabricStrategy`] — HOW the fabric converges (consensus +
//!   placement + cadence).
//! - [`FabricFace`] — WHICH external API the fabric speaks (K8s /
//!   Nomad / Systemd / PureRaft / BareMetalSupervisor).
//! - [`Box<dyn TopologyStrategy>`] — WHAT SHAPE the cluster takes
//!   (Solo / Pair / Quorum3M / Cluster3MNW / MeshAllPeers / Phalanx).
//!
//! Each surface alone is well-typed. The interesting failure mode is
//! the *combination*: declaring a Solo (1-node) topology alongside a
//! consensus strategy that needs a 3-quorum is well-formed in
//! isolation but **incoherent as a cluster** — the cluster will boot
//! but immediately fail liveness because no quorum can ever form.
//!
//! [`ClusterDeclaration`] is the typed shield. Construction runs
//! every cross-check between the three surfaces; if any fires, the
//! declaration cannot exist. The runtime takes `ClusterDeclaration`
//! and trusts the typed witness — no runtime coherence detection
//! needed, because the configuration that would allow incoherence
//! cannot be expressed.
//!
//! This is the "by construction" form of the engenho-as-fabric
//! invariant chain:
//!
//! 1. [`ReconciliationCadence::new`] rejects zero ticks.
//! 2. [`FabricStrategy::prove_liveness`] rejects incoherent strategy
//!    self-fields.
//! 3. [`ClusterDeclaration::new`] rejects incoherent
//!    strategy + face + topology combinations.
//!
//! Three layers, each catching a strictly larger class of errors at
//! a strictly earlier moment.

use crate::fabric::{ConsensusKind, FabricFace, FabricStrategy, FaceKind};
use crate::face::{self, Face, FaceError, FaceWatchStream, ResourceFormat, ResourceRef};
use crate::topology::TopologyStrategy;

/// Errors that surface when a [`ClusterDeclaration`] is constructed
/// from incoherent inputs. Each variant names exactly which cross-
/// surface invariant the inputs violated.
#[derive(Debug, thiserror::Error)]
pub enum ClusterCoherenceError {
    #[error("strategy self-incoherent: {0}")]
    StrategyError(#[from] crate::fabric::FabricStrategyError),

    #[error(
        "topology {topology:?} requires only {topology_min} nodes minimum but strategy consensus quorum is {quorum_size} — the cluster will boot but immediately fail liveness because no quorum can ever form"
    )]
    TopologyTooSmallForQuorum {
        topology: String,
        topology_min: usize,
        quorum_size: usize,
    },

    #[error(
        "face {face:?} ({face_kind}) requires operator signatures on every attestation block, but the strategy does not require operator signatures — this exposes the fabric to operator-impersonation attacks since the face surface promises something the strategy doesn't enforce"
    )]
    FaceSignaturePromiseUnmet {
        face: String,
        face_kind: &'static str,
    },
}

engenho_substrate::impl_error_kind! {
    ClusterCoherenceError {
        (StrategyError(_)) => "strategy_error",
        { TopologyTooSmallForQuorum { .. } } => "topology_too_small_for_quorum",
        { FaceSignaturePromiseUnmet { .. } } => "face_signature_promise_unmet",
    }
}

/// A fully-coherent declaration of a cluster. The only way to obtain
/// one is via [`ClusterDeclaration::new`], which runs every cross-
/// surface check before constructing.
///
/// Once constructed, the runtime can pass this value around and
/// trust that every invariant in [`ClusterCoherenceError`] holds.
/// No runtime re-verification needed; the typed witness IS the
/// proof carrier.
#[must_use = "ClusterDeclaration carries proofs; consume it via the runtime entry point"]
pub struct ClusterDeclaration {
    strategy: FabricStrategy,
    face: FabricFace,
    topology: Box<dyn TopologyStrategy>,
}

impl ClusterDeclaration {
    /// Construct from the three surfaces. Runs every cross-coherence
    /// check; returns `Err` naming the first violated invariant.
    ///
    /// # Errors
    ///
    /// - [`ClusterCoherenceError::StrategyError`] when the strategy
    ///   fails its own [`FabricStrategy::prove_liveness`] check.
    /// - [`ClusterCoherenceError::TopologyTooSmallForQuorum`] when
    ///   the topology can't satisfy the consensus quorum size.
    /// - [`ClusterCoherenceError::FaceSignaturePromiseUnmet`] when
    ///   the face promises operator signatures but the strategy
    ///   doesn't enforce them.
    pub fn new(
        strategy: FabricStrategy,
        face: FabricFace,
        topology: Box<dyn TopologyStrategy>,
    ) -> Result<Self, ClusterCoherenceError> {
        // Check #1: strategy is internally coherent.
        strategy.prove_liveness()?;

        // Check #2: topology can satisfy the consensus quorum.
        let quorum_size = match strategy.consensus.kind {
            ConsensusKind::OpenRaft { quorum_size, .. } => quorum_size as usize,
        };
        if topology.min_nodes() < quorum_size {
            return Err(ClusterCoherenceError::TopologyTooSmallForQuorum {
                topology: topology.name().to_string(),
                topology_min: topology.min_nodes(),
                quorum_size,
            });
        }

        // Check #3: face attestation promises match strategy
        // attestation enforcement. The Kubernetes face publishes its
        // audit log externally; if the face's contract implies
        // operator-attested events but the strategy doesn't enforce
        // operator-signed seals, the audit log shows attestations
        // the strategy never actually validated.
        //
        // The current Face variants don't expose a
        // "requires-operator-signature" flag — every face is content
        // with whatever the strategy provides. This check is
        // reserved for future face variants that publish their
        // operator-attestation guarantee externally (e.g. a
        // future FedRAMP-mode face) so the cross-check shape is
        // already in place when that variant lands.
        let _ = &face;

        Ok(Self {
            strategy,
            face,
            topology,
        })
    }

    /// Borrow the strategy.
    #[must_use]
    pub fn strategy(&self) -> &FabricStrategy {
        &self.strategy
    }

    /// Borrow the face.
    #[must_use]
    pub fn face(&self) -> &FabricFace {
        &self.face
    }

    /// Borrow the topology trait object.
    #[must_use]
    pub fn topology(&self) -> &dyn TopologyStrategy {
        self.topology.as_ref()
    }

    /// Stable identifier for telemetry: `"<face>/<strategy>/<topology>"`.
    #[must_use]
    pub fn id(&self) -> String {
        format!(
            "{}/{}/{}",
            self.face.name,
            self.strategy.name,
            self.topology.name()
        )
    }
}

impl std::fmt::Debug for ClusterDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterDeclaration")
            .field("strategy", &self.strategy.name)
            .field("face", &self.face.name)
            .field("face_kind", &face_kind_str(&self.face.kind))
            .field("topology", &self.topology.name())
            .finish()
    }
}

fn face_kind_str(kind: &FaceKind) -> &'static str {
    match kind {
        FaceKind::Kubernetes { .. } => "Kubernetes",
        FaceKind::Nomad { .. } => "Nomad",
        FaceKind::Systemd { .. } => "Systemd",
        FaceKind::PureRaft => "PureRaft",
        FaceKind::BareMetalSupervisor => "BareMetalSupervisor",
    }
}

// ─────────────────────────────────────────────────────────────────
// Cluster — the running composition (consumes ClusterDeclaration)
// ─────────────────────────────────────────────────────────────────

/// Errors a [`Cluster`] surfaces during construction or lifecycle.
#[derive(Debug, thiserror::Error)]
pub enum ClusterRuntimeError {
    #[error("face instantiation failed: {0}")]
    Face(#[from] FaceError),
    #[error("cluster already started")]
    AlreadyStarted,
    #[error("cluster not started")]
    NotStarted,
}

engenho_substrate::impl_error_kind! {
    ClusterRuntimeError {
        (Face(_)) => "face",
        AlreadyStarted => "already_started",
        NotStarted => "not_started",
    }
}

/// The **running** composition of strategy + face + topology.
///
/// Where [`ClusterDeclaration`] is the typed *authored* witness
/// (declared but no resources allocated), `Cluster` is the typed
/// *running* witness (face started, layers initialized).
///
/// **The load-bearing wire:** `Cluster` can only be constructed
/// from a `ClusterDeclaration`. The runtime cannot exist without
/// the typed coherence proof. Misconfigured clusters never reach
/// the `Cluster::start` call site because the declaration never
/// constructed in the first place.
///
/// **What ships today:** the face lifecycle is wired through
/// (start propagates to `face.start()`, shutdown to
/// `face.shutdown()`). The other four layers (membership /
/// consensus / content / attestation) are reserved for R3+
/// wiring as each module's runtime entry stabilizes. The skeleton
/// is in place; consumers can already pass `Cluster` around as
/// "the running fabric handle" and trust the typed witness chain.
#[must_use = "Cluster is the runtime handle — start it via .start()"]
pub struct Cluster {
    declaration: ClusterDeclaration,
    face: Box<dyn Face>,
    state: std::sync::Mutex<ClusterState>,
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("declaration", &self.declaration)
            .field("face_kind", &self.face.kind())
            .field("state", &self.state.lock().ok())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClusterState {
    Constructed,
    Running,
    Stopped,
}

impl Cluster {
    /// Construct from a [`ClusterDeclaration`]. Instantiates the
    /// face via [`face::instantiate`]; the face starts in its
    /// stopped state and is brought online via [`Cluster::start`].
    ///
    /// # Errors
    ///
    /// Returns [`ClusterRuntimeError::Face`] if the face couldn't
    /// be instantiated (e.g. Systemd / BareMetalSupervisor today
    /// — those return `Unsupported`).
    pub fn from_declaration(declaration: ClusterDeclaration) -> Result<Self, ClusterRuntimeError> {
        let face = face::instantiate(declaration.face())?;
        Ok(Self {
            declaration,
            face,
            state: std::sync::Mutex::new(ClusterState::Constructed),
        })
    }

    /// Borrow the declaration this cluster was constructed from.
    /// The declaration carries the typed coherence witness; the
    /// runtime trusts it without re-verifying.
    #[must_use]
    pub fn declaration(&self) -> &ClusterDeclaration {
        &self.declaration
    }

    /// Borrow the face the runtime is using.
    #[must_use]
    pub fn face(&self) -> &dyn Face {
        self.face.as_ref()
    }

    /// Bring the cluster online: face → start; future layers wire
    /// in as they stabilize.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterRuntimeError::AlreadyStarted`] if called
    /// twice without an intervening shutdown; propagates any
    /// underlying [`FaceError`] from the face's own start.
    pub fn start(&self) -> Result<(), ClusterRuntimeError> {
        let mut state = self.state.lock().expect("cluster state mutex poisoned");
        if *state == ClusterState::Running {
            return Err(ClusterRuntimeError::AlreadyStarted);
        }
        self.face.start()?;
        // Future R3+ layer starts land here:
        //   - membership: start chitchat gossip
        //   - consensus: start openraft + join cluster
        //   - content: start iroh content sync
        //   - attestation: start tameshi chain sealer
        *state = ClusterState::Running;
        Ok(())
    }

    /// Bring the cluster offline gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterRuntimeError::NotStarted`] if shutdown is
    /// called before start.
    pub fn shutdown(&self) -> Result<(), ClusterRuntimeError> {
        let mut state = self.state.lock().expect("cluster state mutex poisoned");
        if *state != ClusterState::Running {
            return Err(ClusterRuntimeError::NotStarted);
        }
        // Reverse order of start: layers down first, face last.
        // Future R3+ shutdowns land here in reverse start order.
        self.face.shutdown()?;
        *state = ClusterState::Stopped;
        Ok(())
    }

    /// True iff [`Cluster::start`] succeeded and the cluster hasn't
    /// been shut down yet.
    #[must_use]
    pub fn is_running(&self) -> bool {
        let state = self.state.lock().expect("cluster state mutex poisoned");
        *state == ClusterState::Running
    }

    /// Stable identifier — delegates to the declaration so the
    /// running cluster has the same telemetry identity as its
    /// declaration.
    #[must_use]
    pub fn id(&self) -> String {
        self.declaration.id()
    }

    /// Aggregated typed status across the running cluster — the
    /// single observability surface every operator needs. Reaches
    /// into face + store to assemble in one allocation.
    #[must_use]
    pub fn health(&self) -> ClusterHealth {
        ClusterHealth {
            name: self.declaration.face().name.clone(),
            kind: self.declaration.face().kind.clone(),
            face_running: self.face.is_running(),
            cluster_running: self.is_running(),
            resource_count: self.face.resource_count(),
            subscriber_count: self.face.subscriber_count(),
            strategy_name: self.declaration.strategy().name.clone(),
            topology_name: self.declaration.topology().name().to_string(),
        }
    }

    /// Capture a typed snapshot of the face's resource state.
    /// Delegates to [`Face::snapshot`]; returns CBOR bytes the
    /// operator can persist + later feed back to [`Self::restore`].
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's snapshot impl
    /// (e.g. `Unsupported` for faces without a backing store).
    pub fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.face.snapshot()
    }

    /// Replay a snapshot into the face. Replaces all face state.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's restore impl.
    pub fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.face.restore(snapshot_bytes)
    }

    // ── One-shot construction + auto-start ────────────────────────

    /// Build + start in one call: equivalent to
    /// `let c = Cluster::from_declaration(d)?; c.start()?;`. The
    /// most operator-ergonomic entry path — `let cluster =
    /// Cluster::start(decl)?;` is all you need.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterRuntimeError::Face`] on face instantiation
    /// failure (currently only legacy unsupported variants — all 5
    /// face kinds construct as of e8e67af). Propagates start
    /// failures from the face.
    pub fn start_with(declaration: ClusterDeclaration) -> Result<Self, ClusterRuntimeError> {
        let cluster = Self::from_declaration(declaration)?;
        cluster.start()?;
        Ok(cluster)
    }

    // ── Operator-facing verb delegates ────────────────────────────
    //
    // The operator-facing API: cluster.apply / cluster.get /
    // cluster.list / cluster.delete / cluster.watch. Each delegates
    // to the underlying face via the Face trait verbs — operators
    // never need to reach in to `cluster.face()` for the common
    // CRUDW path. The cluster handle IS the verb surface.

    /// Apply (create-or-update) a resource through the face.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's
    /// [`Face::apply_resource`].
    pub fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.face.apply_resource(format, body)
    }

    /// Get a single resource through the face.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's
    /// [`Face::get_resource`].
    pub fn get(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        self.face.get_resource(reference, format)
    }

    /// List resources through the face.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's
    /// [`Face::list_resources`].
    pub fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.face.list_resources(kind, namespace, format)
    }

    /// Delete a resource through the face.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's
    /// [`Face::delete_resource`].
    pub fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.face.delete_resource(reference)
    }

    /// Watch resources through the face.
    ///
    /// # Errors
    ///
    /// Propagates any [`FaceError`] from the face's
    /// [`Face::watch_resources`].
    pub fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.face.watch_resources(kind, namespace, format)
    }
}

// ─────────────────────────────────────────────────────────────────
// ClusterHealth — aggregated typed status
// ─────────────────────────────────────────────────────────────────

/// Aggregated status for a running [`Cluster`]. Composed from the
/// declaration (identity), face (lifecycle + resource counts), and
/// runtime (lifecycle). One value, all observability surfaces.
///
/// Suitable for serializing into operator-facing telemetry, health-
/// check endpoints, log fields, etc.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ClusterHealth {
    pub name: String,
    pub kind: FaceKind,
    pub face_running: bool,
    pub cluster_running: bool,
    pub resource_count: usize,
    pub subscriber_count: usize,
    pub strategy_name: String,
    pub topology_name: String,
}

impl ClusterHealth {
    /// `Ok` iff both `cluster_running` and `face_running` are true
    /// AND the resource count is non-negative (always true; this
    /// is a typed sanity check). Returns the first failed
    /// invariant on `Err` so health checks can log specifically.
    pub fn check(&self) -> Result<(), &'static str> {
        if !self.cluster_running {
            return Err("cluster not running");
        }
        if !self.face_running {
            return Err("face not running");
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────
// RAII — leak-free lifecycle
// ─────────────────────────────────────────────────────────────────

impl Drop for Cluster {
    /// If the cluster is still running when it's dropped, shut it
    /// down. Resources allocated through the face (network
    /// listeners, watch channels, raft handles) are released.
    /// Errors during the implicit shutdown are swallowed — Drop
    /// can't return — but the lifecycle transitions through the
    /// shutdown path correctly.
    ///
    /// Operators who want explicit shutdown for error handling
    /// should call [`Cluster::shutdown`] before the cluster drops.
    fn drop(&mut self) {
        let state_snapshot = self.state.lock().ok().map(|g| *g);
        if state_snapshot == Some(ClusterState::Running) {
            // Best-effort: shutdown the face. Errors here would
            // typically be transient (a watch channel that was
            // already closed); we swallow them since Drop can't
            // propagate.
            let _ = self.face.shutdown();
            if let Ok(mut state) = self.state.lock() {
                *state = ClusterState::Stopped;
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// ClusterBuilder — fluent typed construction
// ─────────────────────────────────────────────────────────────────

/// Fluent builder for [`Cluster`]. Collects the three surfaces +
/// face declaration over a chain of typed calls, then dispatches
/// through [`ClusterDeclaration::new`] + [`Cluster::start_with`]
/// in one terminal call.
///
/// Operator-facing pattern:
///
/// ```rust,ignore
/// use engenho_revoada::{Cluster, FabricStrategy};
/// use engenho_revoada::topology::Quorum3M;
///
/// let cluster = Cluster::builder()
///     .strategy(FabricStrategy::prescribed_homelab())
///     .face_pure_raft("homelab")
///     .topology(Quorum3M)
///     .start()?;
///
/// // Verbs work directly on the cluster handle.
/// cluster.apply(ResourceFormat::Native, &envelope)?;
///
/// // Auto-shutdown when `cluster` drops — no manual cleanup.
/// ```
///
/// Each field starts as `None`; [`Self::start`] errors with
/// [`ClusterBuilderError::Missing`] for the first unset field.
#[derive(Default)]
#[must_use = "ClusterBuilder does nothing until .start() (or .build()) is called"]
pub struct ClusterBuilder {
    strategy: Option<FabricStrategy>,
    face: Option<FabricFace>,
    topology: Option<Box<dyn TopologyStrategy>>,
}

/// Errors a [`ClusterBuilder`] surfaces when terminal construction
/// fails — missing field or downstream declaration/runtime error.
#[derive(Debug, thiserror::Error)]
pub enum ClusterBuilderError {
    #[error("ClusterBuilder missing required field: {0}")]
    Missing(&'static str),
    #[error("declaration coherence failed: {0}")]
    Coherence(#[from] ClusterCoherenceError),
    #[error("runtime construction failed: {0}")]
    Runtime(#[from] ClusterRuntimeError),
}

engenho_substrate::impl_error_kind! {
    ClusterBuilderError {
        (Missing(_)) => "missing",
        (Coherence(_)) => "coherence",
        (Runtime(_)) => "runtime",
    }
}

impl ClusterBuilder {
    /// Set the [`FabricStrategy`].
    pub fn strategy(mut self, s: FabricStrategy) -> Self {
        self.strategy = Some(s);
        self
    }

    /// Set the [`FabricFace`] directly.
    pub fn face(mut self, f: FabricFace) -> Self {
        self.face = Some(f);
        self
    }

    /// Convenience: declare a PureRaft face with the given name.
    pub fn face_pure_raft(mut self, name: impl Into<String>) -> Self {
        self.face = Some(FabricFace {
            name: name.into(),
            kind: FaceKind::PureRaft,
        });
        self
    }

    /// Convenience: declare the prescribed CNCF-certified K8s v1.34
    /// face.
    pub fn face_kubernetes_prescribed(mut self) -> Self {
        self.face = Some(FabricFace::prescribed_kubernetes_v1_34());
        self
    }

    /// Set the topology strategy (boxed so any TopologyStrategy
    /// impl works).
    pub fn topology<T: TopologyStrategy + 'static>(mut self, t: T) -> Self {
        self.topology = Some(Box::new(t));
        self
    }

    /// Build the typed [`ClusterDeclaration`] without starting.
    /// Useful for tests that want to inspect the witness before
    /// allocating runtime resources.
    ///
    /// # Errors
    ///
    /// [`ClusterBuilderError::Missing`] if any of the three
    /// surfaces wasn't set; [`ClusterBuilderError::Coherence`] on
    /// cross-surface check failure.
    pub fn build(self) -> Result<ClusterDeclaration, ClusterBuilderError> {
        let strategy = self
            .strategy
            .ok_or(ClusterBuilderError::Missing("strategy"))?;
        let face = self.face.ok_or(ClusterBuilderError::Missing("face"))?;
        let topology = self
            .topology
            .ok_or(ClusterBuilderError::Missing("topology"))?;
        let decl = ClusterDeclaration::new(strategy, face, topology)?;
        Ok(decl)
    }

    /// Build + start in one call. The operator-ergonomic path.
    ///
    /// # Errors
    ///
    /// See [`Self::build`] + propagates runtime errors from
    /// [`Cluster::start_with`].
    pub fn start(self) -> Result<Cluster, ClusterBuilderError> {
        let decl = self.build()?;
        Ok(Cluster::start_with(decl)?)
    }
}

impl Cluster {
    /// Start a fluent [`ClusterBuilder`] chain.
    #[must_use]
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use crate::fabric::{FabricStrategy, FaceKind};
    use crate::topology::Quorum3M;

    fn ok_cluster() -> Cluster {
        let decl = ClusterDeclaration::new(
            FabricStrategy::prescribed_homelab(),
            FabricFace {
                name: "pure-raft".into(),
                kind: FaceKind::PureRaft,
            },
            Box::new(Quorum3M),
        )
        .unwrap();
        Cluster::from_declaration(decl).unwrap()
    }

    #[test]
    fn constructed_cluster_starts_in_stopped_state() {
        let cluster = ok_cluster();
        assert!(!cluster.is_running());
    }

    #[test]
    fn start_brings_face_to_running() {
        let cluster = ok_cluster();
        cluster.start().unwrap();
        assert!(cluster.is_running());
        assert!(cluster.face().is_running());
    }

    #[test]
    fn shutdown_returns_face_to_stopped() {
        let cluster = ok_cluster();
        cluster.start().unwrap();
        cluster.shutdown().unwrap();
        assert!(!cluster.is_running());
        assert!(!cluster.face().is_running());
    }

    #[test]
    fn double_start_errors() {
        let cluster = ok_cluster();
        cluster.start().unwrap();
        match cluster.start() {
            Err(ClusterRuntimeError::AlreadyStarted) => {}
            other => panic!("expected AlreadyStarted, got {other:?}"),
        }
    }

    #[test]
    fn shutdown_without_start_errors() {
        let cluster = ok_cluster();
        match cluster.shutdown() {
            Err(ClusterRuntimeError::NotStarted) => {}
            other => panic!("expected NotStarted, got {other:?}"),
        }
    }

    #[test]
    fn id_delegates_to_declaration() {
        let cluster = ok_cluster();
        assert_eq!(cluster.id(), cluster.declaration().id());
    }

    // ── ClusterBuilder fluent surface ─────────────────────────────

    #[test]
    fn builder_fluent_chain_yields_running_cluster() {
        let cluster = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_pure_raft("homelab")
            .topology(Quorum3M)
            .start()
            .expect("builder.start() should produce a running cluster");
        assert!(cluster.is_running());
        assert_eq!(cluster.face().kind(), FaceKind::PureRaft);
    }

    #[test]
    fn builder_face_kubernetes_prescribed_convenience() {
        let cluster = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_kubernetes_prescribed()
            .topology(Quorum3M)
            .start()
            .expect("k8s prescribed builder");
        match cluster.face().kind() {
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

    #[test]
    fn builder_missing_strategy_errors_with_named_field() {
        let err = Cluster::builder()
            .face_pure_raft("x")
            .topology(Quorum3M)
            .start()
            .expect_err("missing strategy must error");
        match err {
            ClusterBuilderError::Missing("strategy") => {}
            other => panic!("expected Missing(\"strategy\"), got {other:?}"),
        }
    }

    #[test]
    fn builder_missing_face_errors_with_named_field() {
        let err = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .topology(Quorum3M)
            .start()
            .expect_err("missing face must error");
        match err {
            ClusterBuilderError::Missing("face") => {}
            other => panic!("expected Missing(\"face\"), got {other:?}"),
        }
    }

    #[test]
    fn builder_missing_topology_errors_with_named_field() {
        let err = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_pure_raft("x")
            .start()
            .expect_err("missing topology must error");
        match err {
            ClusterBuilderError::Missing("topology") => {}
            other => panic!("expected Missing(\"topology\"), got {other:?}"),
        }
    }

    #[test]
    fn builder_propagates_coherence_failure() {
        // Solo topology + 3-quorum consensus = incoherent. The
        // builder surfaces the coherence error rather than panicking.
        use crate::topology::Solo;
        let err = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_pure_raft("x")
            .topology(Solo)
            .build()
            .expect_err("Solo + 3-quorum must fail coherence");
        match err {
            ClusterBuilderError::Coherence(ClusterCoherenceError::TopologyTooSmallForQuorum {
                ..
            }) => {}
            other => panic!("expected TopologyTooSmallForQuorum, got {other:?}"),
        }
    }

    #[test]
    fn builder_build_returns_declaration_without_starting() {
        // Operators that want to inspect the declaration without
        // allocating runtime resources use .build() instead of
        // .start(). The declaration is the typed witness; no face
        // gets started.
        let decl = Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_pure_raft("inspect-only")
            .topology(Quorum3M)
            .build()
            .expect("build coherent decl");
        assert_eq!(decl.strategy().name, "homelab-3node");
        assert_eq!(decl.face().name, "inspect-only");
    }

    // ── Cluster::start_with one-shot constructor ─────────────────

    #[test]
    fn start_with_constructs_and_starts_in_one_call() {
        let decl = ok_decl();
        let cluster = Cluster::start_with(decl).expect("one-shot start");
        assert!(cluster.is_running());
        assert!(cluster.face().is_running());
    }

    // ── Drop / RAII — leak-free lifecycle ─────────────────────────

    #[test]
    fn dropping_running_cluster_shuts_face_down() {
        // Capture a reference to the face's lifecycle state via a
        // separate cluster handle that wraps the same face — we
        // can't (and shouldn't) capture the inner Box<dyn Face>
        // directly. Instead: build a cluster, start it, drop it,
        // then build a NEW cluster and verify its face starts
        // cleanly (would fail if a prior cluster left state lingering
        // in a way that affects new construction).
        //
        // The more direct assertion: the Drop impl runs without
        // panicking + without double-shutdown errors. This test
        // verifies that path executes.
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        assert!(cluster.is_running());
        drop(cluster); // Drop runs face.shutdown(); should not panic.
        // If shutdown ran twice or on a stopped face,
        // FaceError::NotStarted would surface — but
        // Drop swallows errors. We confirm no panic.
    }

    #[test]
    fn dropping_stopped_cluster_is_safe() {
        // Explicit shutdown before drop — the Drop impl should
        // detect non-running state and skip the shutdown call.
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        cluster.shutdown().unwrap();
        assert!(!cluster.is_running());
        drop(cluster); // Drop sees Stopped, no-ops.
    }

    #[test]
    fn dropping_never_started_cluster_is_safe() {
        // Drop before start — should not call face.shutdown.
        let cluster = Cluster::from_declaration(ok_decl()).unwrap();
        assert!(!cluster.is_running());
        drop(cluster);
    }

    // ── Verb delegates — cluster IS the verb surface ──────────────

    fn pod_envelope(name: &str, payload: &[u8]) -> Vec<u8> {
        use crate::face::encode_native_envelope;
        let r = ResourceRef::namespaced("Pod", name, "default");
        encode_native_envelope(&r, payload).unwrap()
    }

    #[test]
    fn cluster_apply_delegates_to_face_apply_resource() {
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        let env = pod_envelope("nginx", b"x");
        cluster
            .apply(ResourceFormat::Native, &env)
            .expect("apply through cluster");
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let got = cluster
            .get(&r, ResourceFormat::Native)
            .expect("get through cluster");
        // Native is now symmetric pass-through — get returns the
        // full envelope (the same shape apply received).
        assert_eq!(got, env);
    }

    #[test]
    fn cluster_list_delegates_to_face_list_resources() {
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        cluster
            .apply(ResourceFormat::Native, &pod_envelope("a", b"A"))
            .unwrap();
        cluster
            .apply(ResourceFormat::Native, &pod_envelope("b", b"B"))
            .unwrap();
        let listed = cluster
            .list("Pod", Some("default"), ResourceFormat::Native)
            .expect("list through cluster");
        assert_eq!(listed.len(), 2);
    }

    #[test]
    fn cluster_delete_delegates_to_face_delete_resource() {
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        cluster
            .apply(ResourceFormat::Native, &pod_envelope("nginx", b"x"))
            .unwrap();
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        cluster.delete(&r).expect("delete through cluster");
        match cluster.get(&r, ResourceFormat::Native) {
            Err(FaceError::Unsupported(msg)) => assert!(msg.contains("no resource"), "msg: {msg}"),
            other => panic!("expected Unsupported after delete, got {other:?}"),
        }
    }

    #[test]
    fn cluster_watch_delegates_to_face_watch_resources() {
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        let mut watch = cluster
            .watch("Pod", Some("default"), ResourceFormat::Native)
            .expect("watch through cluster");
        let env = pod_envelope("nginx", b"x");
        cluster.apply(ResourceFormat::Native, &env).unwrap();
        let ev = watch.next_event().unwrap().expect("event");
        // Watch emits envelope bytes (Native shape).
        assert_eq!(ev.body, env);
    }

    #[test]
    fn cluster_apply_propagates_face_unsupported_format() {
        let cluster = Cluster::start_with(ok_decl()).unwrap();
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let env = {
            use crate::face::encode_native_envelope;
            encode_native_envelope(&r, b"x").unwrap()
        };
        match cluster.apply(ResourceFormat::Yaml, &env) {
            Err(FaceError::Unsupported(_)) => {}
            other => panic!("expected face's format-unsupported, got {other:?}"),
        }
    }

    // ── Helpers ───────────────────────────────────────────────────

    fn ok_decl() -> ClusterDeclaration {
        ClusterDeclaration::new(
            FabricStrategy::prescribed_homelab(),
            FabricFace {
                name: "pure-raft".into(),
                kind: FaceKind::PureRaft,
            },
            Box::new(Quorum3M),
        )
        .unwrap()
    }

    #[test]
    fn bare_metal_supervisor_now_lifecycles_through_runtime() {
        // BareMetalSupervisor was the LAST Unsupported face. Now
        // that BareMetalSupervisorFace ships, every FaceKind
        // variant constructs through the runtime cleanly. The
        // type system covers the full surface — no Unsupported
        // arm in instantiate(), no runtime panic path for
        // declared-but-unimplemented faces.
        let decl = ClusterDeclaration::new(
            FabricStrategy::prescribed_homelab(),
            FabricFace {
                name: "bms-test".into(),
                kind: FaceKind::BareMetalSupervisor,
            },
            Box::new(Quorum3M),
        )
        .expect("declaration coherent");
        let cluster = Cluster::from_declaration(decl)
            .expect("BMS face is now supported via BareMetalSupervisorFace impl");
        cluster.start().unwrap();
        assert!(cluster.is_running());
        cluster.shutdown().unwrap();
    }

    #[test]
    fn systemd_face_now_lifecycles_through_runtime() {
        // Systemd is now supported (4th impl); the runtime should
        // construct cleanly and start without surfacing an
        // Unsupported error.
        let decl = ClusterDeclaration::new(
            FabricStrategy::prescribed_homelab(),
            FabricFace {
                name: "systemd-test".into(),
                kind: FaceKind::Systemd { user_units: false },
            },
            Box::new(Quorum3M),
        )
        .expect("declaration coherent");
        let cluster = Cluster::from_declaration(decl)
            .expect("Systemd face is now supported via SystemdFace impl");
        cluster.start().unwrap();
        assert!(cluster.is_running());
        cluster.shutdown().unwrap();
    }

    #[test]
    fn cluster_carries_typed_witness_across_lifecycle() {
        // The declaration is borrowed identically before + after
        // start, so any consumer that holds a &Cluster gets the
        // typed witness for free at any point in the lifecycle.
        let cluster = ok_cluster();
        let id_before = cluster.id();
        cluster.start().unwrap();
        let id_after_start = cluster.id();
        cluster.shutdown().unwrap();
        let id_after_shutdown = cluster.id();
        assert_eq!(id_before, id_after_start);
        assert_eq!(id_after_start, id_after_shutdown);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric::{ConsensusConfig, FabricStrategy, ReconciliationCadence};
    use crate::topology::{Cluster3MNW, Phalanx, Quorum3M, Solo};

    fn prescribed_strategy() -> FabricStrategy {
        FabricStrategy::prescribed_homelab()
    }

    fn raft_face() -> FabricFace {
        FabricFace {
            name: "pure-raft".into(),
            kind: FaceKind::PureRaft,
        }
    }

    fn k8s_face() -> FabricFace {
        FabricFace::prescribed_kubernetes_v1_34()
    }

    // ── Happy path ────────────────────────────────────────────────

    #[test]
    fn quorum3m_topology_works_with_3node_consensus() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), raft_face(), Box::new(Quorum3M));
        assert!(
            cluster.is_ok(),
            "Quorum3M + OpenRaft{{3}} should be coherent"
        );
    }

    #[test]
    fn cluster3mnw_topology_works_with_3node_consensus() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), k8s_face(), Box::new(Cluster3MNW));
        assert!(
            cluster.is_ok(),
            "Cluster3MNW + OpenRaft{{3}} should be coherent"
        );
    }

    #[test]
    fn phalanx_topology_works_with_3node_consensus() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), raft_face(), Box::new(Phalanx));
        // Phalanx min_nodes is configured by Phalanx itself; if it
        // satisfies the 3-quorum it must succeed here.
        if Phalanx.min_nodes() >= 3 {
            assert!(cluster.is_ok(), "Phalanx min >= 3 should be coherent");
        } else {
            assert!(cluster.is_err(), "Phalanx min < 3 should fail coherence");
        }
    }

    // ── Cross-check fires correctly ───────────────────────────────

    #[test]
    fn solo_topology_fails_against_3node_consensus() {
        // Solo.min_nodes() == 1, OpenRaft quorum == 3 → coherence
        // failure. The cluster cannot boot to a state where any
        // write is committed.
        let err = ClusterDeclaration::new(prescribed_strategy(), raft_face(), Box::new(Solo))
            .unwrap_err();
        match err {
            ClusterCoherenceError::TopologyTooSmallForQuorum {
                topology,
                topology_min,
                quorum_size,
            } => {
                assert_eq!(topology, "solo");
                assert_eq!(topology_min, 1);
                assert_eq!(quorum_size, 3);
            }
            other => panic!("expected TopologyTooSmallForQuorum, got {other:?}"),
        }
    }

    #[test]
    fn invalid_strategy_propagates_through_cluster_construction() {
        // A strategy that fails prove_liveness must surface up
        // through ClusterDeclaration::new rather than slipping past.
        let mut s = prescribed_strategy();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 4, // even quorum — should fail prove_liveness
            election_timeout_ms: 150,
            snapshot_interval_entries: 1000,
        };
        let err = ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).unwrap_err();
        assert!(matches!(err, ClusterCoherenceError::StrategyError(_)));
    }

    #[test]
    fn detector_outpaces_reconciliation_caught_via_cluster() {
        let mut s = prescribed_strategy();
        s.membership.failure_detector_timeout_ms = s.reconciliation.millis() + 1;
        let err = ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).unwrap_err();
        match err {
            ClusterCoherenceError::StrategyError(inner) => {
                assert!(matches!(
                    inner,
                    crate::fabric::FabricStrategyError::DetectorOutpacesReconciliation { .. }
                ));
            }
            other => panic!("expected nested StrategyError, got {other:?}"),
        }
    }

    // ── Accessors + observability ────────────────────────────────

    #[test]
    fn cluster_id_format_is_stable() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), k8s_face(), Box::new(Quorum3M)).unwrap();
        let id = cluster.id();
        assert!(id.contains("k8s-v1.34"));
        assert!(id.contains("homelab-3node"));
        assert!(id.contains(Quorum3M.name()));
    }

    #[test]
    fn accessors_return_the_constructed_surfaces() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), k8s_face(), Box::new(Cluster3MNW))
                .unwrap();
        assert_eq!(cluster.strategy().name, "homelab-3node");
        assert_eq!(cluster.face().name, "k8s-v1.34");
        assert_eq!(cluster.topology().name(), Cluster3MNW.name());
    }

    #[test]
    fn debug_format_names_all_three_surfaces() {
        let cluster =
            ClusterDeclaration::new(prescribed_strategy(), raft_face(), Box::new(Quorum3M))
                .unwrap();
        let dbg = format!("{cluster:?}");
        assert!(dbg.contains("homelab-3node"));
        assert!(dbg.contains("pure-raft"));
        assert!(dbg.contains("PureRaft"));
        assert!(dbg.contains(Quorum3M.name()));
    }

    #[test]
    fn nonzero_reconciliation_with_huge_election_timeout_still_works() {
        // Sanity: prove_liveness only enforces the cross-field
        // invariants we declared; arbitrary other knob values are
        // allowed. This test pins the "minimal-rule, not
        // exhaustive" surface so future maintainers don't add
        // accidental rules.
        let mut s = prescribed_strategy();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 3,
            election_timeout_ms: 60_000, // 1 minute — extreme but valid
            snapshot_interval_entries: 1,
        };
        assert!(ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).is_ok());
        let _ = ConsensusConfig {
            kind: ConsensusKind::OpenRaft {
                quorum_size: 3,
                election_timeout_ms: 1,
                snapshot_interval_entries: 1,
            },
        };
        // Confirm the imports compile cleanly.
        let _ = ReconciliationCadence::new(1).unwrap();
    }
}
