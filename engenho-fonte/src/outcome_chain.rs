//! Typed OutcomeChain — every Conduit Outcome chained.
//!
//! v1.30's TameshiAttester chains per-attest entries (one entry per
//! Conduit tick attest() call). This module adds a SECOND typed
//! chain — OutcomeChain — that records the terminal Outcome value
//! (revision + proposal_id + receipt_id + finalized_at_ms) per tick.
//!
//! Two separate chains so auditors can prove different things:
//!   * Attestation chain: "was this decision attested?"
//!   * Outcome chain: "did this decision become the cluster's reality?"
//!
//! The OutcomeChainRecorder trait + MockOutcomeChain are always-on
//! so the Conduit can accept any recorder without feature gating.
//! TameshiOutcomeChain (real BLAKE3-chained backend) ships behind
//! `with-tameshi`.

use crate::{FonteResult, Outcome};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::Mutex;

/// Records every Conduit Outcome.
#[async_trait]
pub trait OutcomeChainRecorder: Send + Sync {
    /// Append the given Outcome to the chain. Returns the
    /// chain-assigned receipt id.
    async fn record(&self, outcome: &Outcome) -> FonteResult<Arc<str>>;
}

/// In-memory mock OutcomeChain. Records outcomes in a Vec; the
/// receipt id is the BLAKE3 hex of the canonical-JSON of the
/// outcome, abridged to 16 chars (same shape as the real chain's
/// id format).
#[derive(Debug, Default)]
pub struct MockOutcomeChain {
    log: Mutex<Vec<Outcome>>,
}

impl MockOutcomeChain {
    /// New empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the recorded outcomes for tests + audit.
    pub fn outcomes(&self) -> Vec<Outcome> {
        self.log
            .lock()
            .expect("mock outcome chain poisoned")
            .clone()
    }
}

#[async_trait]
impl OutcomeChainRecorder for MockOutcomeChain {
    async fn record(&self, outcome: &Outcome) -> FonteResult<Arc<str>> {
        let canonical = serde_json::to_string(outcome)
            .map_err(|e| crate::FonteError::Attest(format!("outcome serialize: {e}")))?;
        let hash = blake3::hash(canonical.as_bytes()).to_hex();
        let id: Arc<str> = Arc::from(&hash[..16]);
        self.log
            .lock()
            .expect("mock outcome chain poisoned")
            .push(outcome.clone());
        Ok(id)
    }
}

// ── Real TameshiOutcomeChain (gated `with-tameshi`) ──────────────

#[cfg(feature = "with-tameshi")]
mod tameshi_impl {
    use super::OutcomeChainRecorder;
    use crate::{FonteResult, Outcome};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tameshi::hash::Blake3Hash;
    use tameshi::heartbeat::{
        HeartbeatChain, HeartbeatEvent, VerificationOutcome, VerifierIdentity,
    };

    /// Real OutcomeChain backed by tameshi's HeartbeatChain.
    pub struct TameshiOutcomeChain {
        chain: Arc<HeartbeatChain>,
        identity: VerifierIdentity,
    }

    impl TameshiOutcomeChain {
        /// New chain with a fresh HeartbeatChain + the given identity.
        #[must_use]
        pub fn new(instance: impl Into<String>, version: impl Into<String>) -> Self {
            Self {
                chain: Arc::new(HeartbeatChain::new()),
                identity: VerifierIdentity::new(
                    "engenho-fonte/outcome",
                    &instance.into(),
                    &version.into(),
                ),
            }
        }

        /// Reuse an existing chain.
        #[must_use]
        pub fn with_chain(
            chain: Arc<HeartbeatChain>,
            instance: impl Into<String>,
            version: impl Into<String>,
        ) -> Self {
            Self {
                chain,
                identity: VerifierIdentity::new(
                    "engenho-fonte/outcome",
                    &instance.into(),
                    &version.into(),
                ),
            }
        }

        /// Borrow the chain for proof queries.
        #[must_use]
        pub fn chain(&self) -> Arc<HeartbeatChain> {
            self.chain.clone()
        }
    }

    #[async_trait]
    impl OutcomeChainRecorder for TameshiOutcomeChain {
        async fn record(&self, outcome: &Outcome) -> FonteResult<Arc<str>> {
            let canonical = serde_json::to_string(outcome)
                .map_err(|e| crate::FonteError::Attest(format!("outcome serialize: {e}")))?;
            let signature = Blake3Hash::from(*blake3::hash(canonical.as_bytes()).as_bytes());
            let resource = format!(
                "outcome:rev={}/proposal={}",
                outcome.revision, outcome.proposal_id
            );
            let entry = self.chain.append(
                self.identity.clone(),
                HeartbeatEvent::ComplianceRecheck,
                VerificationOutcome::Allowed,
                &resource,
                signature,
            );
            let id_hex = entry.entry_hash.to_hex();
            Ok(Arc::from(&id_hex[..16]))
        }
    }
}

#[cfg(feature = "with-tameshi")]
pub use tameshi_impl::TameshiOutcomeChain;
