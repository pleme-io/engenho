//! Typed OutcomeChain — every Conduit Outcome chained cryptographically.
//!
//! v1.30's TameshiAttester chains per-attest entries (one entry per
//! Conduit tick attest() call). This module adds a SECOND typed
//! chain — OutcomeChain — that records the terminal Outcome value
//! (revision + proposal_id + receipt_id + finalized_at_ms) per tick
//! into its own tameshi HeartbeatChain. Operators get:
//!
//!   - Attestation chain (v1.30) — proves "this typed decision was
//!     evaluated AND admitted"
//!   - Outcome chain (v1.44) — proves "this typed decision was
//!     COMMITTED + finalized — the cluster's typed state of record"
//!
//! Two separate chains so auditors can prove different things:
//! "Was this decision attested?" vs "Did this decision become the
//! cluster's reality?"
//!
//! Gated `with-tameshi`.

use crate::{FonteResult, Outcome};
use async_trait::async_trait;
use std::sync::Arc;
use tameshi::hash::Blake3Hash;
use tameshi::heartbeat::{HeartbeatChain, HeartbeatEvent, VerificationOutcome, VerifierIdentity};

/// Records every Conduit Outcome into a typed HeartbeatChain.
/// Mirrors [`crate::TameshiAttester`] but for Outcomes, not
/// per-attest events.
#[async_trait]
pub trait OutcomeChainRecorder: Send + Sync {
    /// Append the given Outcome to the chain. Returns the
    /// chain-assigned receipt id.
    async fn record(&self, outcome: &Outcome) -> FonteResult<Arc<str>>;
}

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

    /// Reuse an existing chain — operators wire one chain across
    /// the attester + outcome recorder if they want a unified
    /// audit log.
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
        // signature_checked: BLAKE3 of the typed Outcome's canonical
        // JSON. Any tamper after the chain-write breaks BLAKE3.
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
