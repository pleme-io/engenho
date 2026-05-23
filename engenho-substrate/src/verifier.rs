//! Verification contract for the roça layer.
//!
//! `Verificacao` is the typed predicate every Stage must satisfy
//! before the DAG advances. Each variant produces a typed
//! `VerificationReceipt` (BLAKE3-addressed, ed25519-signable —
//! signing wiring lands when tameshi integrates).
//!
//! Stages compose multiple `Verificacao` entries with AND
//! semantics: every entry must produce its receipt before the
//! stage transitions to `Confirmed`.
//!
//! ## Composition with existing receipts
//!
//! `VerificationReceipt` IS a `MaterializationReceipt` — the
//! verification subject is the materialized artifact's
//! evidence_hash; the receipt's evidence_hash is the verifier's
//! proof (re-derived BLAKE3 / tameshi chain head / signature
//! bytes). Verification receipts gossip + accumulate in
//! `QuorumTracker` exactly like materialization receipts.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::derivation::NarHash;
use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};

/// One verification predicate. Stages hold a `Vec<Verificacao>`
/// evaluated with AND semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Verificacao {
    /// Recomputed NAR hash must equal the expected.
    HashEquality {
        /// Expected NAR hash.
        expected: NarHash,
    },
    /// K-of-N nodes must report the same materialization receipt.
    CrossNodeAgreement {
        /// Quorum threshold.
        quorum: usize,
    },
    /// Artifact must be signed by a known tameshi attestation chain.
    TameshiSigned {
        /// Stable identifier of the chain whose signature is required.
        signer: String,
    },
    /// Independent re-derivation on a second backend must match.
    Independent {
        /// Stable identifier of the backend to use for re-derivation.
        backend: String,
    },
    /// Post-materialize smoke test — a drv whose successful build
    /// is itself the verification.
    SmokeTest {
        /// Hex string of the smoke-test drv hash.
        drv_hash_hex: String,
    },
}

/// What kind of verifier evaluated this predicate (telemetry +
/// dispatch). Stable string instead of enum so backends can ship
/// independently.
pub type VerifierId = String;

/// One verifier's proof that one Verificacao was satisfied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReceipt {
    /// Predicate this receipt is proof of.
    pub verificacao: Verificacao,
    /// Underlying MaterializationReceipt — kind+subject+emitter+
    /// timestamp+evidence_hash semantics inherited.
    pub receipt: MaterializationReceipt,
    /// Verifier that produced this receipt.
    pub verifier: VerifierId,
}

impl VerificationReceipt {
    /// Construct from typed parts.
    #[must_use]
    pub fn new(
        verificacao: Verificacao,
        receipt: MaterializationReceipt,
        verifier: VerifierId,
    ) -> Self {
        Self {
            verificacao,
            receipt,
            verifier,
        }
    }
}

/// Verifier errors.
#[derive(Debug, Clone, Error)]
pub enum VerifyError {
    /// Backend failure (re-derivation crash / signer unreachable / etc.).
    #[error("backend: {0}")]
    Backend(String),
    /// Verifier doesn't implement this Verificacao variant.
    #[error("unsupported verificacao: {0}")]
    Unsupported(String),
    /// Verification negative — the predicate was checked and FAILED.
    /// Distinct from Backend (process error vs proof of negation).
    #[error("failed: {0}")]
    Failed(String),
}

impl VerifyError {
    /// Stable identifier for telemetry / SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
            Self::Unsupported(_) => "unsupported",
            Self::Failed(_) => "failed",
        }
    }
}

/// Pluggable verifier trait.
#[async_trait]
pub trait Verifier: Send + Sync {
    /// Verifier identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Evaluate `verificacao` against the materialized artifact
    /// identified by `subject_hash`. Returns the proof receipt on
    /// success; typed error otherwise.
    ///
    /// # Errors
    /// [`VerifyError::Backend`] for process failures;
    /// [`VerifyError::Unsupported`] if the verifier doesn't handle
    /// the variant;
    /// [`VerifyError::Failed`] if the predicate was checked + denied.
    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError>;
}

// =================================================================
// FakeVerifier — deterministic backend for tests
// =================================================================

/// Test verifier — operator pins per-Verificacao outcomes.
#[derive(Default, Clone)]
pub struct FakeVerifier {
    inner: Arc<Mutex<FakeVerifierState>>,
}

#[derive(Default)]
struct FakeVerifierState {
    /// If set, the next verify call fails with this typed error.
    fail_next: Option<VerifyError>,
    /// Per-variant policy: true=pass, false=fail. Default true.
    policies: std::collections::BTreeMap<String, bool>,
    /// Call log for test assertion.
    calls: Vec<String>,
}

impl FakeVerifier {
    /// Fresh verifier — every Verificacao passes by default.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin the next verify() to return the given typed error.
    pub async fn fail_next(&self, err: VerifyError) {
        self.inner.lock().await.fail_next = Some(err);
    }

    /// Set per-variant pass/fail policy. The variant key is the
    /// Verificacao's serde tag (e.g. "hash_equality").
    pub async fn set_policy(&self, variant_tag: &str, pass: bool) {
        self.inner
            .lock()
            .await
            .policies
            .insert(variant_tag.to_string(), pass);
    }

    /// Snapshot of verify call labels.
    pub async fn calls(&self) -> Vec<String> {
        self.inner.lock().await.calls.clone()
    }

    /// Stable tag for a Verificacao variant (mirrors serde rename).
    #[must_use]
    pub fn tag_of(v: &Verificacao) -> &'static str {
        match v {
            Verificacao::HashEquality { .. } => "hash_equality",
            Verificacao::CrossNodeAgreement { .. } => "cross_node_agreement",
            Verificacao::TameshiSigned { .. } => "tameshi_signed",
            Verificacao::Independent { .. } => "independent",
            Verificacao::SmokeTest { .. } => "smoke_test",
        }
    }
}

#[async_trait]
impl Verifier for FakeVerifier {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError> {
        let tag = Self::tag_of(verificacao);
        let mut state = self.inner.lock().await;
        state.calls.push(tag.to_string());
        if let Some(err) = state.fail_next.take() {
            return Err(err);
        }
        let pass = state.policies.get(tag).copied().unwrap_or(true);
        drop(state);
        if !pass {
            return Err(VerifyError::Failed(format!(
                "fake verifier denied {tag}"
            )));
        }
        // Synthetic evidence: BLAKE3 over (tag, subject_hash).
        let mut composed = tag.as_bytes().to_vec();
        composed.extend_from_slice(&subject_hash);
        let evidence_hash = *blake3::hash(&composed).as_bytes();
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape(format!("verify:{tag}")),
            subject_hash,
            emitter,
            emitted_at,
            evidence_hash,
        );
        Ok(VerificationReceipt::new(
            verificacao.clone(),
            receipt,
            "fake".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_hash_eq() -> Verificacao {
        Verificacao::HashEquality {
            expected: NarHash::from_bytes(b"x"),
        }
    }

    fn sample_subject() -> [u8; 32] {
        *blake3::hash(b"subject").as_bytes()
    }

    fn sample_emitter() -> NodeId {
        NodeId::from_bytes(b"node-a")
    }

    #[tokio::test]
    async fn fake_verifier_default_passes_any_variant() {
        let v = FakeVerifier::new();
        let r = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap();
        assert_eq!(r.verifier, "fake");
        assert_eq!(r.verificacao, sample_hash_eq());
    }

    #[tokio::test]
    async fn fake_verifier_policy_can_deny() {
        let v = FakeVerifier::new();
        v.set_policy("hash_equality", false).await;
        let err = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "failed");
    }

    #[tokio::test]
    async fn fail_next_overrides_policy() {
        let v = FakeVerifier::new();
        v.fail_next(VerifyError::Backend("kaboom".into())).await;
        let err = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn fail_next_is_consumed_after_one_call() {
        let v = FakeVerifier::new();
        v.fail_next(VerifyError::Backend("first only".into())).await;
        let _ = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await;
        let r = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn calls_log_records_tag_per_call() {
        let v = FakeVerifier::new();
        let _ = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await;
        let _ = v
            .verify(
                &Verificacao::CrossNodeAgreement { quorum: 2 },
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await;
        let calls = v.calls().await;
        assert_eq!(calls, vec!["hash_equality", "cross_node_agreement"]);
    }

    #[tokio::test]
    async fn verify_receipt_has_verify_prefixed_shape_tag() {
        let v = FakeVerifier::new();
        let r = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap();
        match r.receipt.kind {
            ReceiptKind::Shape(tag) => assert_eq!(tag, "verify:hash_equality"),
            _ => panic!("expected Shape variant"),
        }
    }

    #[test]
    fn tag_of_covers_every_variant() {
        for (v, expected) in [
            (sample_hash_eq(), "hash_equality"),
            (Verificacao::CrossNodeAgreement { quorum: 1 }, "cross_node_agreement"),
            (
                Verificacao::TameshiSigned {
                    signer: "x".into(),
                },
                "tameshi_signed",
            ),
            (
                Verificacao::Independent {
                    backend: "x".into(),
                },
                "independent",
            ),
            (
                Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                "smoke_test",
            ),
        ] {
            assert_eq!(FakeVerifier::tag_of(&v), expected);
        }
    }

    #[test]
    fn verificacao_serializes_with_snake_case_tag() {
        let json = serde_json::to_string(&sample_hash_eq()).unwrap();
        assert!(json.contains("\"kind\":\"hash_equality\""));
    }

    #[test]
    fn verify_error_kinds_are_stable() {
        assert_eq!(VerifyError::Backend("x".into()).kind(), "backend");
        assert_eq!(VerifyError::Unsupported("x".into()).kind(), "unsupported");
        assert_eq!(VerifyError::Failed("x".into()).kind(), "failed");
    }

    #[test]
    fn verification_receipt_round_trips_via_serde() {
        let r = VerificationReceipt::new(
            sample_hash_eq(),
            MaterializationReceipt::for_drv(
                [1u8; 32],
                NodeId::from_bytes(b"x"),
                100,
                [2u8; 32],
            ),
            "fake".to_string(),
        );
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: VerificationReceipt = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
    }

    #[tokio::test]
    async fn fake_verifier_name_is_stable() {
        let v = FakeVerifier::new();
        assert_eq!(v.name(), "fake");
    }
}
