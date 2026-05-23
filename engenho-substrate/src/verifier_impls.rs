//! Concrete `Verifier` impls for the roça verification layer.
//!
//! Three implementations land here, each pluggable + composable
//! through `ChainedVerifier`:
//!
//!   * [`HashEqualityVerifier`] — recomputes BLAKE3 over given
//!     bytes; compares to the `Verificacao::HashEquality.expected`
//!     value. The substrate's cryptographic equality check.
//!
//!   * [`SmokeTestVerifier`] — runs a smoke-test drv through a
//!     pluggable `BuildBackend`-shape function; success of the
//!     build itself is the verification proof.
//!
//!   * [`IndependentVerifier`] — runs a second build path
//!     (operator-supplied async closure that returns NAR bytes)
//!     + emits a receipt whose evidence is the BLAKE3 of those
//!     bytes. Downstream `QuorumTracker` surfaces a Dissent
//!     outcome if the independent rebuild produces a different
//!     hash from the primary materializer's claim.

use std::sync::Arc;

use async_trait::async_trait;

use crate::derivation::NarHash;
use crate::receipt::{MaterializationReceipt, NodeId, ReceiptKind};
use crate::verifier::{Verificacao, VerificationReceipt, VerifyError, Verifier};

// =================================================================
// HashEqualityVerifier
// =================================================================

/// Operator-supplied accessor that returns the bytes the verifier
/// should hash for `subject_hash`. Production wires this to the
/// `DerivationCacheBackend::get_nar` path; tests pin a synthetic
/// byte-source.
pub type BytesAccessor = Arc<
    dyn Fn([u8; 32]) -> futures::future::BoxFuture<'static, Result<Option<Vec<u8>>, VerifyError>>
        + Send
        + Sync,
>;

/// Recomputes BLAKE3 of the bytes the accessor returns + compares
/// to the predicate's `expected` NAR hash. Only implements the
/// `HashEquality` variant; other Verificacao kinds return
/// `Unsupported`.
pub struct HashEqualityVerifier {
    name: &'static str,
    accessor: BytesAccessor,
}

impl HashEqualityVerifier {
    /// New verifier with telemetry name + bytes accessor.
    #[must_use]
    pub fn new(name: &'static str, accessor: BytesAccessor) -> Self {
        Self { name, accessor }
    }

    /// Convenience constructor with name `"hash-equality"`.
    #[must_use]
    pub fn default_named(accessor: BytesAccessor) -> Self {
        Self::new("hash-equality", accessor)
    }
}

#[async_trait]
impl Verifier for HashEqualityVerifier {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError> {
        let expected = match verificacao {
            Verificacao::HashEquality { expected } => expected.clone(),
            _ => {
                return Err(VerifyError::Unsupported(
                    "HashEqualityVerifier only handles HashEquality".into(),
                ));
            }
        };
        let bytes = match (self.accessor)(subject_hash).await? {
            Some(b) => b,
            None => {
                return Err(VerifyError::Failed(format!(
                    "no bytes available for subject {}",
                    hex_encode(&subject_hash)
                )));
            }
        };
        let actual = NarHash::from_bytes(&bytes);
        if actual != expected {
            return Err(VerifyError::Failed(format!(
                "hash mismatch: expected {} got {}",
                expected, actual
            )));
        }
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape("verify:hash_equality".into()),
            subject_hash,
            emitter,
            emitted_at,
            actual.0,
        );
        Ok(VerificationReceipt::new(
            verificacao.clone(),
            receipt,
            self.name.to_string(),
        ))
    }
}

// =================================================================
// SmokeTestVerifier
// =================================================================

/// Operator-supplied function: given a `drv_hash_hex`, attempts a
/// smoke-test build + returns Ok on success / Err on build failure.
/// Production wires this to a BuildBackend-shape adapter; tests pin
/// a deterministic outcome.
pub type SmokeBuilder = Arc<
    dyn Fn(String) -> futures::future::BoxFuture<'static, Result<(), VerifyError>>
        + Send
        + Sync,
>;

/// Runs a smoke-test drv via an operator-supplied builder; success
/// of the build IS the verification proof.
pub struct SmokeTestVerifier {
    name: &'static str,
    builder: SmokeBuilder,
}

impl SmokeTestVerifier {
    /// New verifier with telemetry name + builder.
    #[must_use]
    pub fn new(name: &'static str, builder: SmokeBuilder) -> Self {
        Self { name, builder }
    }

    /// Convenience: name `"smoke-test"`.
    #[must_use]
    pub fn default_named(builder: SmokeBuilder) -> Self {
        Self::new("smoke-test", builder)
    }
}

#[async_trait]
impl Verifier for SmokeTestVerifier {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError> {
        let drv_hex = match verificacao {
            Verificacao::SmokeTest { drv_hash_hex } => drv_hash_hex.clone(),
            _ => {
                return Err(VerifyError::Unsupported(
                    "SmokeTestVerifier only handles SmokeTest".into(),
                ));
            }
        };
        (self.builder)(drv_hex.clone()).await?;
        // Evidence: BLAKE3 over (smoke-test drv hash hex + subject_hash hex).
        // Same smoke test on different nodes produces same evidence
        // → QuorumTracker reaches Reached.
        let mut composed = drv_hex.into_bytes();
        composed.extend_from_slice(&subject_hash);
        let evidence = *blake3::hash(&composed).as_bytes();
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape("verify:smoke_test".into()),
            subject_hash,
            emitter,
            emitted_at,
            evidence,
        );
        Ok(VerificationReceipt::new(
            verificacao.clone(),
            receipt,
            self.name.to_string(),
        ))
    }
}

// =================================================================
// IndependentVerifier
// =================================================================

/// Operator-supplied independent rebuild: given the subject hash,
/// runs an alternate build path + returns the NAR bytes the
/// rebuild produced.
pub type IndependentRebuild = Arc<
    dyn Fn([u8; 32]) -> futures::future::BoxFuture<'static, Result<Vec<u8>, VerifyError>>
        + Send
        + Sync,
>;

/// Re-derives via a second backend; emits a receipt whose evidence
/// is BLAKE3 over the rebuilt bytes. Downstream `QuorumTracker`
/// surfaces Dissent if this evidence differs from the primary
/// materializer's claim.
pub struct IndependentVerifier {
    name: &'static str,
    rebuild: IndependentRebuild,
}

impl IndependentVerifier {
    /// New verifier with telemetry name + rebuild function.
    #[must_use]
    pub fn new(name: &'static str, rebuild: IndependentRebuild) -> Self {
        Self { name, rebuild }
    }

    /// Convenience: name `"independent"`.
    #[must_use]
    pub fn default_named(rebuild: IndependentRebuild) -> Self {
        Self::new("independent", rebuild)
    }
}

#[async_trait]
impl Verifier for IndependentVerifier {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError> {
        match verificacao {
            Verificacao::Independent { .. } => {}
            _ => {
                return Err(VerifyError::Unsupported(
                    "IndependentVerifier only handles Independent".into(),
                ));
            }
        }
        let bytes = (self.rebuild)(subject_hash).await?;
        let evidence = *blake3::hash(&bytes).as_bytes();
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape("verify:independent".into()),
            subject_hash,
            emitter,
            emitted_at,
            evidence,
        );
        Ok(VerificationReceipt::new(
            verificacao.clone(),
            receipt,
            self.name.to_string(),
        ))
    }
}

// =================================================================
// TameshiVerifier
// =================================================================

/// Operator-supplied signature check: given a `signer` chain id +
/// the subject hash, returns the typed evidence (typically the
/// signature bytes) or a typed error. Production wires this to the
/// cosign / tameshi PKI integration; tests pin a deterministic
/// outcome.
pub type SignerCheck = Arc<
    dyn Fn(String, [u8; 32]) -> futures::future::BoxFuture<'static, Result<Vec<u8>, VerifyError>>
        + Send
        + Sync,
>;

/// Verifies cosign-style signatures via an operator-supplied check.
/// Evidence is BLAKE3 over the returned signature bytes — faithful
/// nodes presenting the same signature for the same subject produce
/// identical evidence (QuorumTracker reaches; tampered nodes
/// produce a divergent signature → Dissent).
pub struct TameshiVerifier {
    name: &'static str,
    check: SignerCheck,
}

impl TameshiVerifier {
    /// New verifier with telemetry name + signature check.
    #[must_use]
    pub fn new(name: &'static str, check: SignerCheck) -> Self {
        Self { name, check }
    }

    /// Convenience: name `"tameshi"`.
    #[must_use]
    pub fn default_named(check: SignerCheck) -> Self {
        Self::new("tameshi", check)
    }
}

#[async_trait]
impl Verifier for TameshiVerifier {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn verify(
        &self,
        verificacao: &Verificacao,
        subject_hash: [u8; 32],
        emitter: NodeId,
        emitted_at: u64,
    ) -> Result<VerificationReceipt, VerifyError> {
        let signer = match verificacao {
            Verificacao::TameshiSigned { signer } => signer.clone(),
            _ => {
                return Err(VerifyError::Unsupported(
                    "TameshiVerifier only handles TameshiSigned".into(),
                ));
            }
        };
        let sig_bytes = (self.check)(signer, subject_hash).await?;
        let evidence = *blake3::hash(&sig_bytes).as_bytes();
        let receipt = MaterializationReceipt::new(
            ReceiptKind::Shape("verify:tameshi_signed".into()),
            subject_hash,
            emitter,
            emitted_at,
            evidence,
        );
        Ok(VerificationReceipt::new(
            verificacao.clone(),
            receipt,
            self.name.to_string(),
        ))
    }
}

// =================================================================
// hex helper (shared)
// =================================================================

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn subj() -> [u8; 32] {
        *blake3::hash(b"subj").as_bytes()
    }

    fn emit() -> NodeId {
        NodeId::from_bytes(b"n")
    }

    // ── HashEqualityVerifier ───────────────────────────────────

    fn bytes_accessor_returning(bytes: Vec<u8>) -> BytesAccessor {
        Arc::new(move |_hash| {
            let b = bytes.clone();
            async move { Ok(Some(b)) }.boxed()
        })
    }

    fn bytes_accessor_none() -> BytesAccessor {
        Arc::new(|_hash| async move { Ok(None) }.boxed())
    }

    #[tokio::test]
    async fn hash_equality_passes_when_bytes_match() {
        let bytes = b"hello-nar".to_vec();
        let expected = NarHash::from_bytes(&bytes);
        let v = HashEqualityVerifier::default_named(bytes_accessor_returning(bytes));
        let r = v
            .verify(
                &Verificacao::HashEquality { expected: expected.clone() },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        assert_eq!(r.receipt.evidence_hash, expected.0);
    }

    #[tokio::test]
    async fn hash_equality_fails_when_bytes_differ() {
        let v = HashEqualityVerifier::default_named(bytes_accessor_returning(b"actual".to_vec()));
        let err = v
            .verify(
                &Verificacao::HashEquality {
                    expected: NarHash::from_bytes(b"different"),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "failed");
    }

    #[tokio::test]
    async fn hash_equality_fails_when_no_bytes_available() {
        let v = HashEqualityVerifier::default_named(bytes_accessor_none());
        let err = v
            .verify(
                &Verificacao::HashEquality {
                    expected: NarHash::from_bytes(b"x"),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "failed");
    }

    #[tokio::test]
    async fn hash_equality_rejects_wrong_variant() {
        let v = HashEqualityVerifier::default_named(bytes_accessor_returning(vec![]));
        let err = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported");
    }

    #[tokio::test]
    async fn hash_equality_name_is_configurable() {
        let v = HashEqualityVerifier::new(
            "custom",
            bytes_accessor_returning(vec![]),
        );
        assert_eq!(v.name(), "custom");
    }

    #[tokio::test]
    async fn hash_equality_default_name() {
        let v = HashEqualityVerifier::default_named(bytes_accessor_returning(vec![]));
        assert_eq!(v.name(), "hash-equality");
    }

    // ── SmokeTestVerifier ──────────────────────────────────────

    fn smoke_succeeds() -> SmokeBuilder {
        Arc::new(|_| async move { Ok(()) }.boxed())
    }

    fn smoke_fails(msg: &'static str) -> SmokeBuilder {
        Arc::new(move |_| async move { Err(VerifyError::Backend(msg.into())) }.boxed())
    }

    #[tokio::test]
    async fn smoke_test_passes_when_build_succeeds() {
        let v = SmokeTestVerifier::default_named(smoke_succeeds());
        let r = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        assert_eq!(r.verifier, "smoke-test");
    }

    #[tokio::test]
    async fn smoke_test_fails_when_build_fails() {
        let v = SmokeTestVerifier::default_named(smoke_fails("nope"));
        let err = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn smoke_test_rejects_wrong_variant() {
        let v = SmokeTestVerifier::default_named(smoke_succeeds());
        let err = v
            .verify(
                &Verificacao::HashEquality {
                    expected: NarHash::from_bytes(b"x"),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported");
    }

    #[tokio::test]
    async fn smoke_test_evidence_is_deterministic_for_same_drv() {
        let v = SmokeTestVerifier::default_named(smoke_succeeds());
        let r1 = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        let r2 = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "abc".into(),
                },
                subj(),
                NodeId::from_bytes(b"different-node"),
                100,  // different timestamp
            )
            .await
            .unwrap();
        // Faithful smoke test → identical evidence across nodes.
        assert_eq!(r1.receipt.evidence_hash, r2.receipt.evidence_hash);
    }

    #[tokio::test]
    async fn smoke_test_default_name() {
        let v = SmokeTestVerifier::default_named(smoke_succeeds());
        assert_eq!(v.name(), "smoke-test");
    }

    // ── IndependentVerifier ────────────────────────────────────

    fn rebuild_returning(bytes: Vec<u8>) -> IndependentRebuild {
        Arc::new(move |_subject| {
            let b = bytes.clone();
            async move { Ok(b) }.boxed()
        })
    }

    fn rebuild_failing(msg: &'static str) -> IndependentRebuild {
        Arc::new(move |_| async move { Err(VerifyError::Backend(msg.into())) }.boxed())
    }

    #[tokio::test]
    async fn independent_passes_with_evidence_from_rebuild() {
        let bytes = b"alt-nar".to_vec();
        let expected_evidence = *blake3::hash(&bytes).as_bytes();
        let v = IndependentVerifier::default_named(rebuild_returning(bytes));
        let r = v
            .verify(
                &Verificacao::Independent {
                    backend: "alt".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        assert_eq!(r.receipt.evidence_hash, expected_evidence);
    }

    #[tokio::test]
    async fn independent_propagates_rebuild_failure() {
        let v = IndependentVerifier::default_named(rebuild_failing("rebuild died"));
        let err = v
            .verify(
                &Verificacao::Independent {
                    backend: "alt".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn independent_rejects_wrong_variant() {
        let v = IndependentVerifier::default_named(rebuild_returning(vec![]));
        let err = v
            .verify(
                &Verificacao::SmokeTest {
                    drv_hash_hex: "x".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported");
    }

    #[tokio::test]
    async fn independent_default_name() {
        let v = IndependentVerifier::default_named(rebuild_returning(vec![]));
        assert_eq!(v.name(), "independent");
    }

    // ── TameshiVerifier ────────────────────────────────────────

    fn sig_returning(bytes: Vec<u8>) -> SignerCheck {
        Arc::new(move |_signer, _subject| {
            let b = bytes.clone();
            async move { Ok(b) }.boxed()
        })
    }

    fn sig_failing(msg: &'static str) -> SignerCheck {
        Arc::new(move |_, _| async move { Err(VerifyError::Backend(msg.into())) }.boxed())
    }

    #[tokio::test]
    async fn tameshi_passes_when_signer_returns_signature() {
        let v = TameshiVerifier::default_named(sig_returning(b"sig-bytes".to_vec()));
        let r = v
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        assert_eq!(r.verifier, "tameshi");
        assert_eq!(r.receipt.evidence_hash, *blake3::hash(b"sig-bytes").as_bytes());
    }

    #[tokio::test]
    async fn tameshi_propagates_signer_failure() {
        let v = TameshiVerifier::default_named(sig_failing("untrusted"));
        let err = v
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn tameshi_rejects_wrong_variant() {
        let v = TameshiVerifier::default_named(sig_returning(vec![]));
        let err = v
            .verify(
                &Verificacao::HashEquality {
                    expected: NarHash::from_bytes(b"x"),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "unsupported");
    }

    #[tokio::test]
    async fn tameshi_faithful_nodes_agree_on_evidence() {
        let v = TameshiVerifier::default_named(sig_returning(b"sig".to_vec()));
        let r1 = v
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                NodeId::from_bytes(b"node-1"),
                42,
            )
            .await
            .unwrap();
        let r2 = v
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                NodeId::from_bytes(b"node-2"),
                100,
            )
            .await
            .unwrap();
        // Same signature bytes → same evidence (QuorumTracker reaches).
        assert_eq!(r1.receipt.evidence_hash, r2.receipt.evidence_hash);
    }

    #[tokio::test]
    async fn tameshi_byzantine_node_diverges() {
        let v_real = TameshiVerifier::default_named(sig_returning(b"valid".to_vec()));
        let v_byz = TameshiVerifier::default_named(sig_returning(b"forged".to_vec()));
        let r_real = v_real
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        let r_byz = v_byz
            .verify(
                &Verificacao::TameshiSigned {
                    signer: "engenho-pki".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        // Different signature bytes → different evidence → Dissent
        // when QuorumTracker sees both.
        assert_ne!(r_real.receipt.evidence_hash, r_byz.receipt.evidence_hash);
    }

    #[tokio::test]
    async fn tameshi_default_name() {
        let v = TameshiVerifier::default_named(sig_returning(vec![]));
        assert_eq!(v.name(), "tameshi");
    }

    #[tokio::test]
    async fn tameshi_custom_name() {
        let v = TameshiVerifier::new("cosign-chain-prod", sig_returning(vec![]));
        assert_eq!(v.name(), "cosign-chain-prod");
    }

    #[tokio::test]
    async fn independent_disagrees_when_rebuild_differs_from_primary() {
        // The independent verifier emits evidence = BLAKE3(alt bytes).
        // If the alt path produces DIFFERENT bytes than the primary
        // materializer's claim, downstream QuorumTracker would
        // observe two distinct evidence hashes → Dissent.
        let v_primary_matching = IndependentVerifier::default_named(rebuild_returning(b"X".to_vec()));
        let v_primary_diff = IndependentVerifier::default_named(rebuild_returning(b"Y".to_vec()));
        let r1 = v_primary_matching
            .verify(
                &Verificacao::Independent {
                    backend: "alt".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        let r2 = v_primary_diff
            .verify(
                &Verificacao::Independent {
                    backend: "alt".into(),
                },
                subj(),
                emit(),
                42,
            )
            .await
            .unwrap();
        assert_ne!(r1.receipt.evidence_hash, r2.receipt.evidence_hash);
    }
}
