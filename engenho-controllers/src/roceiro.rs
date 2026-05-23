//! Roceiro — the materializer trait.
//!
//! Wraps a [`BuildBackend`] (existing) + a [`Verifier`] (existing)
//! into one typed surface the [`PlantioController`] dispatches:
//! "materialize this Stage on this node, run its verifiers,
//! return a typed MaterializationReceipt."
//!
//! ## Trait shape
//!
//! Pluggable so consumers can swap in:
//!   * `FakeRoceiro` — deterministic for tests
//!   * `BuildBackendRoceiro` — production: drives a BuildBackend
//!      + a Verifier + ingests into a DerivationCacheBackend
//!   * `SuiRoceiro` (future) — production: invokes sui directly
//!
//! ## Composition
//!
//! ```text
//! PlantioController.tick():
//!     for each ready stage in topo order:
//!         for each placement target:
//!             roceiro.materialize(stage, target) → MaterializationReceipt
//!             ledger.ingest(stage.id, threshold, &receipt)
//!             if ledger says Reached: mark stage Confirmed, unblock deps
//!             if ledger says Dissent: emit Anomaly
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use engenho_substrate::{MaterializationReceipt, NodeId, ReceiptKind, Stage, StageId};
use thiserror::Error;
use tokio::sync::Mutex;

/// Errors the materializer surfaces.
#[derive(Debug, Clone, Error)]
pub enum RoceiroError {
    /// Backend (build / verifier / cache) failed.
    #[error("backend: {0}")]
    Backend(String),
    /// Stage is malformed for this materializer (e.g. shape
    /// unsupported by the backend).
    #[error("unsupported stage {stage}: {detail}")]
    UnsupportedStage {
        /// The stage that couldn't be materialized.
        stage: StageId,
        /// Why.
        detail: String,
    },
    /// Verification produced a typed denial.
    #[error("verification denied for stage {stage}: {detail}")]
    VerificationDenied {
        /// The stage that failed verification.
        stage: StageId,
        /// Verifier's reason.
        detail: String,
    },
}

engenho_substrate::impl_error_kind! {
    RoceiroError {
        (Backend(_)) => "backend",
        { UnsupportedStage { .. } } => "unsupported_stage",
        { VerificationDenied { .. } } => "verification_denied",
    }
}

/// The materializer trait.
#[async_trait]
pub trait Roceiro: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Materialize `stage` on `node`. Returns the typed receipt the
    /// PlantioController feeds into the ledger.
    ///
    /// # Errors
    /// [`RoceiroError::Backend`] for build / cache failures;
    /// [`RoceiroError::UnsupportedStage`] for shape mismatches;
    /// [`RoceiroError::VerificationDenied`] if a verifier denied.
    async fn materialize(
        &self,
        stage: &Stage,
        node: NodeId,
    ) -> Result<MaterializationReceipt, RoceiroError>;
}

// =================================================================
// FakeRoceiro — deterministic in-memory materializer for tests
// =================================================================

/// In-memory materializer. Records every materialize call;
/// produces synthetic receipts whose evidence is keyed by
/// (stage_id, shape) — all nodes agree on the same bytes for a
/// given stage. Use `dissent_on()` to inject byzantine-emitter
/// behavior where each node reports a different evidence hash.
#[derive(Default, Clone)]
pub struct FakeRoceiro {
    inner: Arc<Mutex<FakeRoceiroState>>,
}

#[derive(Default)]
struct FakeRoceiroState {
    /// Stage IDs whose materialize() should fail with Backend.
    backend_fail: std::collections::BTreeSet<StageId>,
    /// Stage IDs whose materialize() should deny verification.
    deny_verify: std::collections::BTreeSet<StageId>,
    /// Stage IDs whose evidence should diverge per node (byzantine).
    dissent: std::collections::BTreeSet<StageId>,
    /// Call log for test assertion.
    calls: Vec<(StageId, NodeId)>,
}

impl FakeRoceiro {
    /// Fresh roceiro — every materialize succeeds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `stage_id` to fail with Backend on next + subsequent calls.
    pub async fn fail_backend(&self, stage_id: StageId) {
        self.inner.lock().await.backend_fail.insert(stage_id);
    }

    /// Mark `stage_id` to deny verification on next + subsequent calls.
    pub async fn deny_verification(&self, stage_id: StageId) {
        self.inner.lock().await.deny_verify.insert(stage_id);
    }

    /// Make `stage_id` produce divergent evidence per node — the
    /// QuorumTracker will report Dissent. Simulates byzantine
    /// emitter behavior for testing the dissent path.
    pub async fn dissent_on(&self, stage_id: StageId) {
        self.inner.lock().await.dissent.insert(stage_id);
    }

    /// Snapshot of materialize calls.
    pub async fn calls(&self) -> Vec<(StageId, NodeId)> {
        self.inner.lock().await.calls.clone()
    }

    /// Count of materialize calls.
    pub async fn call_count(&self) -> usize {
        self.inner.lock().await.calls.len()
    }
}

#[async_trait]
impl Roceiro for FakeRoceiro {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn materialize(
        &self,
        stage: &Stage,
        node: NodeId,
    ) -> Result<MaterializationReceipt, RoceiroError> {
        let mut state = self.inner.lock().await;
        state.calls.push((stage.id.clone(), node));
        if state.backend_fail.contains(&stage.id) {
            return Err(RoceiroError::Backend(format!(
                "fake backend failure for {}",
                stage.id
            )));
        }
        if state.deny_verify.contains(&stage.id) {
            return Err(RoceiroError::VerificationDenied {
                stage: stage.id.clone(),
                detail: "fake denial".into(),
            });
        }
        let dissent = state.dissent.contains(&stage.id);
        drop(state);
        // Synthetic subject = BLAKE3(stage_id). Evidence is
        // BLAKE3(stage_id + shape_tag) so faithful nodes agree.
        // For dissent simulation, include the node id so each
        // node reports a different evidence hash.
        let subject = *blake3::hash(stage.id.as_str().as_bytes()).as_bytes();
        let mut composed = stage.id.as_str().as_bytes().to_vec();
        composed.extend_from_slice(stage.shape.tag().as_bytes());
        if dissent {
            composed.extend_from_slice(&node.0);
        }
        let evidence = *blake3::hash(&composed).as_bytes();
        Ok(MaterializationReceipt::new(
            ReceiptKind::Shape(stage.shape.tag()),
            subject,
            node,
            42, // synthetic deterministic timestamp
            evidence,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::{Placement, WorkloadShape};

    fn n(b: u8) -> NodeId {
        NodeId::new([b; 32])
    }

    fn stage(id: &str) -> Stage {
        Stage::pinned(id, WorkloadShape::OciImage, n(1))
    }

    #[tokio::test]
    async fn materialize_default_succeeds_and_records_call() {
        let r = FakeRoceiro::new();
        let receipt = r.materialize(&stage("build"), n(7)).await.unwrap();
        assert_eq!(r.call_count().await, 1);
        match receipt.kind {
            ReceiptKind::Shape(tag) => assert_eq!(tag, "oci_image"),
            _ => panic!("expected Shape kind"),
        }
        assert_eq!(receipt.emitter, n(7));
    }

    #[tokio::test]
    async fn receipt_subject_is_deterministic_per_stage_id() {
        let r = FakeRoceiro::new();
        let r1 = r.materialize(&stage("x"), n(1)).await.unwrap();
        let r2 = r.materialize(&stage("x"), n(2)).await.unwrap();
        // Same subject (stage), different emitter.
        assert_eq!(r1.subject, r2.subject);
        assert_ne!(r1.emitter, r2.emitter);
    }

    #[tokio::test]
    async fn faithful_nodes_agree_on_evidence_for_same_stage() {
        let r = FakeRoceiro::new();
        let r1 = r.materialize(&stage("x"), n(1)).await.unwrap();
        let r2 = r.materialize(&stage("x"), n(2)).await.unwrap();
        // Default: nodes produce the SAME evidence (faithful
        // materialization). Quorum reaches Reached, not Dissent.
        assert_eq!(r1.evidence_hash, r2.evidence_hash);
        assert_ne!(r1.emitter, r2.emitter);
    }

    #[tokio::test]
    async fn dissent_on_makes_nodes_disagree() {
        let r = FakeRoceiro::new();
        r.dissent_on(StageId::new("byz")).await;
        let r1 = r.materialize(&stage("byz"), n(1)).await.unwrap();
        let r2 = r.materialize(&stage("byz"), n(2)).await.unwrap();
        // With dissent_on, nodes report divergent evidence.
        assert_ne!(r1.evidence_hash, r2.evidence_hash);
    }

    #[tokio::test]
    async fn fail_backend_marks_stage_to_fail() {
        let r = FakeRoceiro::new();
        r.fail_backend(StageId::new("bad")).await;
        let err = r.materialize(&stage("bad"), n(1)).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn deny_verification_marks_stage_to_deny() {
        let r = FakeRoceiro::new();
        r.deny_verification(StageId::new("nope")).await;
        let err = r.materialize(&stage("nope"), n(1)).await.unwrap_err();
        assert_eq!(err.kind(), "verification_denied");
    }

    #[tokio::test]
    async fn unmarked_stages_succeed_even_when_others_marked() {
        let r = FakeRoceiro::new();
        r.fail_backend(StageId::new("bad")).await;
        let ok = r.materialize(&stage("good"), n(1)).await;
        assert!(ok.is_ok());
    }

    #[tokio::test]
    async fn calls_log_records_each_invocation() {
        let r = FakeRoceiro::new();
        r.materialize(&stage("a"), n(1)).await.unwrap();
        r.materialize(&stage("b"), n(2)).await.unwrap();
        let calls = r.calls().await;
        assert_eq!(
            calls,
            vec![(StageId::new("a"), n(1)), (StageId::new("b"), n(2)),]
        );
    }

    #[tokio::test]
    async fn shape_tag_passes_through_to_receipt() {
        let r = FakeRoceiro::new();
        let mut st = stage("static");
        st.shape = WorkloadShape::StaticBinary {
            triple: "x86_64-unknown-linux-musl".into(),
        };
        // Make sure pinned-defaults aren't surprising.
        st.placement = Placement::Pinned { node: n(1) };
        let receipt = r.materialize(&st, n(1)).await.unwrap();
        match receipt.kind {
            ReceiptKind::Shape(tag) => {
                assert_eq!(tag, "static_binary:x86_64-unknown-linux-musl");
            }
            _ => panic!("expected Shape kind"),
        }
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(RoceiroError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            RoceiroError::UnsupportedStage {
                stage: StageId::new("a"),
                detail: "x".into(),
            }
            .kind(),
            "unsupported_stage"
        );
        assert_eq!(
            RoceiroError::VerificationDenied {
                stage: StageId::new("a"),
                detail: "x".into(),
            }
            .kind(),
            "verification_denied"
        );
    }

    #[tokio::test]
    async fn backend_name_is_stable() {
        assert_eq!(FakeRoceiro::new().name(), "fake");
    }
}
