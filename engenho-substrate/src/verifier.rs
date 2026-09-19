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

use std::collections::BTreeMap;
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

/// The closed set of [`Verificacao`] kinds, one per variant, without the
/// variant's payload. A verifier keys per-predicate behaviour by this type
/// rather than by the serde tag string, so a misspelt key is a compile error
/// instead of a key that silently matches nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VerificacaoKind {
    /// [`Verificacao::HashEquality`].
    HashEquality,
    /// [`Verificacao::CrossNodeAgreement`].
    CrossNodeAgreement,
    /// [`Verificacao::TameshiSigned`].
    TameshiSigned,
    /// [`Verificacao::Independent`].
    Independent,
    /// [`Verificacao::SmokeTest`].
    SmokeTest,
}

impl VerificacaoKind {
    /// Every kind, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::HashEquality,
        Self::CrossNodeAgreement,
        Self::TameshiSigned,
        Self::Independent,
        Self::SmokeTest,
    ];

    /// The serde tag this kind carries on the wire (`"hash_equality"`, ...).
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::HashEquality => "hash_equality",
            Self::CrossNodeAgreement => "cross_node_agreement",
            Self::TameshiSigned => "tameshi_signed",
            Self::Independent => "independent",
            Self::SmokeTest => "smoke_test",
        }
    }
}

impl Verificacao {
    /// This predicate's kind, payload dropped.
    #[must_use]
    pub const fn kind(&self) -> VerificacaoKind {
        match self {
            Self::HashEquality { .. } => VerificacaoKind::HashEquality,
            Self::CrossNodeAgreement { .. } => VerificacaoKind::CrossNodeAgreement,
            Self::TameshiSigned { .. } => VerificacaoKind::TameshiSigned,
            Self::Independent { .. } => VerificacaoKind::Independent,
            Self::SmokeTest { .. } => VerificacaoKind::SmokeTest,
        }
    }
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

crate::impl_error_kind! {
    VerifyError {
        (Backend(_)) => "backend",
        (Unsupported(_)) => "unsupported",
        (Failed(_)) => "failed",
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

/// What a [`FakeVerifier`] answers for one [`VerificacaoKind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FakeOutcome {
    /// Issue a receipt: the predicate is treated as checked and satisfied.
    Pass,
    /// Refuse with [`VerifyError::Failed`]: checked and denied.
    Deny,
}

/// Test verifier — the caller pins an outcome per [`VerificacaoKind`].
///
/// A kind with no pinned outcome is refused with
/// [`VerifyError::Unsupported`]. The fake never issues a receipt for a
/// predicate it was not told how to answer: a PASS receipt for a check that
/// never ran has no code path here. A test that wants receipts says which
/// kinds pass, with [`FakeVerifier::passing`] or [`FakeVerifier::pin`].
#[derive(Default, Clone)]
pub struct FakeVerifier {
    inner: Arc<Mutex<FakeVerifierState>>,
}

#[derive(Default)]
struct FakeVerifierState {
    /// If set, the next verify call fails with this typed error.
    fail_next: Option<VerifyError>,
    /// Pinned outcomes. A kind absent from this map is refused.
    outcomes: BTreeMap<VerificacaoKind, FakeOutcome>,
    /// Call log for test assertion.
    calls: Vec<String>,
}

impl FakeVerifier {
    /// A verifier with nothing pinned: it refuses every predicate.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A verifier that passes exactly `kinds` and refuses every other kind.
    #[must_use]
    pub fn passing(kinds: impl IntoIterator<Item = VerificacaoKind>) -> Self {
        let outcomes = kinds
            .into_iter()
            .map(|kind| (kind, FakeOutcome::Pass))
            .collect();
        Self {
            inner: Arc::new(Mutex::new(FakeVerifierState {
                outcomes,
                ..FakeVerifierState::default()
            })),
        }
    }

    /// Pin the next `verify` to return the given typed error.
    pub async fn fail_next(&self, err: VerifyError) {
        self.inner.lock().await.fail_next = Some(err);
    }

    /// Pin what every later `verify` of `kind` answers.
    pub async fn pin(&self, kind: VerificacaoKind, outcome: FakeOutcome) {
        self.inner.lock().await.outcomes.insert(kind, outcome);
    }

    /// Snapshot of verify call labels.
    pub async fn calls(&self) -> Vec<String> {
        self.inner.lock().await.calls.clone()
    }

    /// Stable tag for a Verificacao variant (mirrors serde rename).
    #[must_use]
    pub fn tag_of(v: &Verificacao) -> &'static str {
        v.kind().tag()
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
        let kind = verificacao.kind();
        let tag = kind.tag();
        let mut state = self.inner.lock().await;
        state.calls.push(tag.to_string());
        if let Some(err) = state.fail_next.take() {
            return Err(err);
        }
        let outcome = state.outcomes.get(&kind).copied();
        drop(state);
        match outcome {
            // Nothing pinned: the check did not run, so there is no receipt.
            None => return Err(VerifyError::Unsupported(tag.to_owned())),
            Some(FakeOutcome::Deny) => return Err(VerifyError::Failed(tag.to_owned())),
            Some(FakeOutcome::Pass) => {}
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

    fn one_of_each() -> [Verificacao; 5] {
        [
            sample_hash_eq(),
            Verificacao::CrossNodeAgreement { quorum: 2 },
            Verificacao::TameshiSigned {
                signer: "engenho-pki".into(),
            },
            Verificacao::Independent {
                backend: "second".into(),
            },
            Verificacao::SmokeTest {
                drv_hash_hex: "abc".into(),
            },
        ]
    }

    /// ★ T5.6: the fake issued a PASS receipt for any predicate nobody had
    /// configured. With nothing pinned, every kind is refused and no receipt
    /// exists to be counted.
    #[tokio::test]
    async fn fake_verifier_with_nothing_pinned_refuses_every_kind() {
        let v = FakeVerifier::new();
        for p in one_of_each() {
            let err = v
                .verify(&p, sample_subject(), sample_emitter(), 100)
                .await
                .expect_err("an unpinned predicate must not produce a receipt");
            assert_eq!(err.kind(), "unsupported", "{p:?}");
        }
        assert_eq!(v.calls().await.len(), 5, "every call is still logged");
    }

    #[tokio::test]
    async fn default_fake_verifier_refuses_like_new() {
        let v = FakeVerifier::default();
        let err = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported");
    }

    #[tokio::test]
    async fn passing_passes_exactly_the_named_kinds() {
        let v = FakeVerifier::passing([VerificacaoKind::HashEquality]);
        let r = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap();
        assert_eq!(r.verifier, "fake");
        assert_eq!(r.verificacao, sample_hash_eq());
        for p in one_of_each()
            .into_iter()
            .filter(|p| p.kind() != VerificacaoKind::HashEquality)
        {
            let err = v
                .verify(&p, sample_subject(), sample_emitter(), 100)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), "unsupported", "{p:?}");
        }
    }

    #[tokio::test]
    async fn pinned_pass_issues_a_receipt_for_every_kind() {
        let v = FakeVerifier::passing(VerificacaoKind::ALL);
        for p in one_of_each() {
            let r = v
                .verify(&p, sample_subject(), sample_emitter(), 100)
                .await
                .unwrap();
            assert_eq!(r.verificacao, p);
        }
    }

    #[tokio::test]
    async fn fake_verifier_pin_can_deny() {
        let v = FakeVerifier::passing(VerificacaoKind::ALL);
        v.pin(VerificacaoKind::HashEquality, FakeOutcome::Deny)
            .await;
        let err = v
            .verify(&sample_hash_eq(), sample_subject(), sample_emitter(), 100)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "failed");
    }

    #[test]
    fn verificacao_kind_tag_is_the_serde_tag_for_every_kind() {
        for p in one_of_each() {
            let json = serde_json::to_value(&p).unwrap();
            assert_eq!(json["kind"], p.kind().tag(), "{p:?}");
        }
        let kinds: Vec<VerificacaoKind> = one_of_each().iter().map(Verificacao::kind).collect();
        assert_eq!(kinds, VerificacaoKind::ALL);
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
        let v = FakeVerifier::passing([VerificacaoKind::HashEquality]);
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
        let v = FakeVerifier::passing([VerificacaoKind::HashEquality]);
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
            (
                Verificacao::CrossNodeAgreement { quorum: 1 },
                "cross_node_agreement",
            ),
            (
                Verificacao::TameshiSigned { signer: "x".into() },
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
            MaterializationReceipt::for_drv([1u8; 32], NodeId::from_bytes(b"x"), 100, [2u8; 32]),
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
