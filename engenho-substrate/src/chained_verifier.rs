//! ChainedVerifier — composes N `Verifier` impls AND-style.
//!
//! Stage.verify is `Vec<Verificacao>` evaluated with AND semantics
//! (all predicates must pass). This wrapper does the same on the
//! verifier side: hand it a list of typed verifiers, each entry
//! checked in order; first failure short-circuits.
//!
//! ## When to use
//!
//!   * Stage requires multiple verifications (e.g.
//!     HashEquality + TameshiSigned + SmokeTest) — wire one
//!     ChainedVerifier holding three specialized backends.
//!   * Production deployments combining cosign + reproducer +
//!     attestation chain into one dispatchable surface.
//!
//! ## Returns
//!
//! On success, returns the LAST verifier's receipt (callers can
//! audit the chain via the embedded `verifier` field). On failure,
//! short-circuits with the failing verifier's error wrapped in
//! `VerifyError::Failed("<verifier_name>: <detail>")`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::receipt::NodeId;
use crate::verifier::{Verificacao, VerificationReceipt, Verifier, VerifyError};

/// Composes N verifiers; first failure denies.
pub struct ChainedVerifier {
    name: &'static str,
    verifiers: Vec<Arc<dyn Verifier>>,
}

impl ChainedVerifier {
    /// New chain with a stable telemetry name + verifiers in
    /// evaluation order.
    #[must_use]
    pub fn new(name: &'static str, verifiers: Vec<Arc<dyn Verifier>>) -> Self {
        Self { name, verifiers }
    }

    /// New chain with the default name "chained".
    #[must_use]
    pub fn default_named(verifiers: Vec<Arc<dyn Verifier>>) -> Self {
        Self::new("chained", verifiers)
    }

    /// Count of verifiers in the chain.
    #[must_use]
    pub fn len(&self) -> usize {
        self.verifiers.len()
    }

    /// True if no verifiers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.verifiers.is_empty()
    }

    /// Observable snapshot — name + verifier count. Pattern #2
    /// (SSC v0.91) — returns canonical
    /// [`crate::mirante::ChildCountSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> crate::mirante::ChildCountSnapshot {
        crate::mirante::ChildCountSnapshot {
            name: self.name,
            child_count: self.len(),
        }
    }
}

crate::impl_named_field!(ChainedVerifier);

crate::impl_observable!(ChainedVerifier, crate::mirante::ChildCountSnapshot);

#[async_trait]
impl Verifier for ChainedVerifier {
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
        if self.verifiers.is_empty() {
            return Err(VerifyError::Backend("empty verifier chain".into()));
        }
        let mut last_receipt: Option<VerificationReceipt> = None;
        for v in &self.verifiers {
            match v
                .verify(verificacao, subject_hash, emitter, emitted_at)
                .await
            {
                Ok(receipt) => {
                    last_receipt = Some(receipt);
                }
                Err(e) => {
                    return Err(VerifyError::Failed(format!("{}: {e}", v.name())));
                }
            }
        }
        Ok(last_receipt.expect("non-empty chain produces a receipt"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derivation::NarHash;
    use crate::verifier::FakeVerifier;

    fn sample_verificacao() -> Verificacao {
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
    async fn empty_chain_returns_backend_error() {
        let c = ChainedVerifier::default_named(vec![]);
        let err = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn single_passing_verifier_returns_its_receipt() {
        let v: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
        let c = ChainedVerifier::default_named(vec![v]);
        let receipt = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap();
        assert_eq!(receipt.verifier, "fake");
    }

    #[tokio::test]
    async fn all_passing_verifiers_returns_last_receipt() {
        let v1: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
        let v2: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
        let c = ChainedVerifier::default_named(vec![v1, v2]);
        let receipt = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap();
        // Both verifiers ran; receipt is from the last (whichever).
        assert_eq!(receipt.verifier, "fake");
    }

    #[tokio::test]
    async fn first_failure_short_circuits() {
        let v1 = Arc::new(FakeVerifier::new());
        v1.set_policy("hash_equality", false).await;
        let v2 = Arc::new(FakeVerifier::new());
        let c = ChainedVerifier::default_named(vec![
            v1.clone() as Arc<dyn Verifier>,
            v2.clone() as Arc<dyn Verifier>,
        ]);
        let err = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap_err();
        // Failure → Failed kind.
        assert_eq!(err.kind(), "failed");
        // v2 should NOT have been called.
        assert_eq!(v2.calls().await.len(), 0);
    }

    #[tokio::test]
    async fn middle_failure_short_circuits_and_skips_rest() {
        let v1 = Arc::new(FakeVerifier::new());
        let v2 = Arc::new(FakeVerifier::new());
        v2.set_policy("hash_equality", false).await;
        let v3 = Arc::new(FakeVerifier::new());
        let c = ChainedVerifier::default_named(vec![
            v1.clone() as Arc<dyn Verifier>,
            v2.clone() as Arc<dyn Verifier>,
            v3.clone() as Arc<dyn Verifier>,
        ]);
        let err = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "failed");
        assert_eq!(v1.calls().await.len(), 1);
        assert_eq!(v2.calls().await.len(), 1);
        assert_eq!(v3.calls().await.len(), 0);
    }

    #[tokio::test]
    async fn failure_message_includes_offending_verifier_name() {
        let v = Arc::new(FakeVerifier::new());
        v.set_policy("hash_equality", false).await;
        let c = ChainedVerifier::default_named(vec![v as Arc<dyn Verifier>]);
        let err = c
            .verify(
                &sample_verificacao(),
                sample_subject(),
                sample_emitter(),
                100,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("fake"));
    }

    #[tokio::test]
    async fn chain_metadata_helpers() {
        let v1: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
        let v2: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
        let c = ChainedVerifier::default_named(vec![v1, v2]);
        assert_eq!(c.len(), 2);
        assert!(!c.is_empty());
        let empty: ChainedVerifier = ChainedVerifier::default_named(vec![]);
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn chained_verifier_name_is_configurable() {
        let c = ChainedVerifier::new("custom-name", vec![Arc::new(FakeVerifier::new())]);
        assert_eq!(c.name(), "custom-name");
    }
}
