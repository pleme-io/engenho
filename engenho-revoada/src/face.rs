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
    #[must_use]
    pub fn from_declaration(decl: &FabricFace) -> Option<Self> {
        if decl.kind != FaceKind::PureRaft {
            return None;
        }
        Some(Self {
            name: decl.name.clone(),
            state: Mutex::new(FaceState::Stopped),
        })
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
        })
    }

    /// Nomad API version this face speaks (e.g. `"1.7"`).
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
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
        FaceKind::Systemd { .. } => Err(FaceError::Unsupported(
            "SystemdFace impl lands at engenho-revoada R6+".into(),
        )),
        FaceKind::BareMetalSupervisor => Err(FaceError::Unsupported(
            "BareMetalSupervisorFace impl lands at engenho-revoada R6+".into(),
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

    #[test]
    fn instantiate_systemd_still_unsupported_until_r6() {
        let decl = FabricFace {
            name: "systemd-test".into(),
            kind: FaceKind::Systemd { user_units: false },
        };
        match instantiate(&decl) {
            Err(FaceError::Unsupported(msg)) => {
                assert!(msg.contains("R6"), "msg: {msg}");
            }
            Err(other) => panic!("expected Unsupported, got {other:?}"),
            Ok(_) => panic!("expected Err for Systemd face, got Ok"),
        }
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
