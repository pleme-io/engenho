//! `with-revoada` — real [`Proposer`] backed by a revoada
//! [`Face`](engenho_revoada::face::Face).
//!
//! Pulls in `engenho-revoada` only when the `with-revoada` Cargo
//! feature is enabled. Consumers wire:
//!
//! ```ignore
//! use engenho_fonte::{Conduit, RevoadaProposer, Sistema};
//! use engenho_revoada::{FabricFace, FaceKind, PureRaftFace};
//! use std::sync::Arc;
//!
//! let face = PureRaftFace::from_declaration(&FabricFace {
//!     name: "fonte".into(),
//!     kind: FaceKind::PureRaft,
//! })
//! .unwrap();
//! face.start().unwrap();
//!
//! let proposer = Arc::new(RevoadaProposer::new(Arc::new(face)));
//! // ... pass proposer to Conduit::new(...)
//! ```
//!
//! The Proposer serializes each Decision's typed value as YAML +
//! synthesizes a Pod-shaped envelope so the face's wire-format
//! adapter ingests it like any operator-applied resource. (M1.3 will
//! emit a `(defsistema …)` envelope kind once revoada's
//! FormatAdapter learns it; until then, YAML+Pod is the bridge.)

use crate::{Decision, FonteError, FonteResult, GossipBroker, ProposalId, Proposer};
use async_trait::async_trait;
use engenho_revoada::face::{Face, ResourceFormat};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Real Proposer backed by a revoada [`Face`].
pub struct RevoadaProposer {
    face: Arc<dyn Face>,
    next_id: AtomicU64,
}

impl RevoadaProposer {
    /// Construct from an already-`start()`-ed face. Construction
    /// does not call `start()` itself — operators control face
    /// lifecycle (a face that starts implicitly on construction
    /// can't be re-started after shutdown).
    #[must_use]
    pub fn new(face: Arc<dyn Face>) -> Self {
        Self {
            face,
            next_id: AtomicU64::new(0),
        }
    }

    /// Borrow the underlying face for assertions in integration tests.
    #[must_use]
    pub fn face(&self) -> &Arc<dyn Face> {
        &self.face
    }
}

#[async_trait]
impl Proposer for RevoadaProposer {
    async fn propose(&self, decision: &Decision) -> FonteResult<ProposalId> {
        // Synthesize a Pod-shaped YAML envelope carrying the
        // serialized Sistema in the annotation `pleme.io/sistema`.
        // This is the M1.3 bridge — revoada's FormatAdapter only
        // recognizes Pod/Deployment shapes today; a future
        // (defsistema) envelope kind will replace this.
        let name = decision.change.source.replace('/', "-");
        let payload = serde_json::to_string(&decision.typed)
            .map_err(|e| FonteError::Propose(format!("serialize: {e}")))?;
        let yaml = sistema_envelope(&name, &payload);
        self.face
            .apply_resource(ResourceFormat::Yaml, yaml.as_bytes())
            .map_err(|e| FonteError::Propose(format!("face apply_resource: {e}")))?;
        Ok(self.next_id.fetch_add(1, Ordering::SeqCst))
    }
}

/// Synthesize a Pod-shaped YAML envelope carrying the typed Sistema
/// payload as an annotation. Shared by [`RevoadaProposer`] +
/// [`FaceGossipBroker`].
fn sistema_envelope(name: &str, payload: &str) -> String {
    format!(
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {}\n  namespace: fonte\n  \
         annotations:\n    pleme.io/sistema: |\n      {}\nspec: {{}}\n",
        name,
        payload.replace('\n', "\n      "),
    )
}

/// `GossipBroker` backed by a revoada [`Face`]. announce() sends a
/// typed Sistema envelope through the face's apply_resource verb;
/// peer subscribers see it via the face's watch stream (built
/// separately — see `FaceWatcher` below).
///
/// Real cross-cluster gossip — every peer registers against the
/// same Face (e.g. a shared PureRaftFace cluster) and broadcasts
/// to every other peer via the face's typed protocol.
pub struct FaceGossipBroker {
    face: Arc<dyn Face>,
    rev: AtomicU64,
}

impl FaceGossipBroker {
    /// New broker — operator passes an already-`start()`-ed face.
    #[must_use]
    pub fn new(face: Arc<dyn Face>) -> Self {
        Self {
            face,
            rev: AtomicU64::new(0),
        }
    }

    /// Borrow the underlying face for assertions in tests.
    #[must_use]
    pub fn face(&self) -> &Arc<dyn Face> {
        &self.face
    }
}

#[async_trait]
impl GossipBroker for FaceGossipBroker {
    async fn announce(&self, source: Arc<str>, source_text: Arc<str>) -> FonteResult<u64> {
        let name = source.replace('/', "-");
        let yaml = sistema_envelope(&name, source_text.as_ref());
        self.face
            .apply_resource(ResourceFormat::Yaml, yaml.as_bytes())
            .map_err(|e| FonteError::Propose(format!("face apply_resource: {e}")))?;
        Ok(self.rev.fetch_add(1, Ordering::SeqCst))
    }
}
