//! `with-tameshi` — real Attester wrapping tameshi's HeartbeatChain.
//!
//! Each Conduit attest() call appends a HeartbeatEntry to the chain
//! with:
//!   - VerifierIdentity { component: "engenho-fonte", instance,
//!     version }
//!   - HeartbeatEvent::ComplianceRecheck (closest semantic match for
//!     "this Sistema decision is recorded as the cluster's typed
//!     state of record")
//!   - VerificationOutcome::Allowed (the SystemController already
//!     passed every sub-reconcile; the chain records success)
//!   - resource = decision.change.source (which Sistema declaration
//!     this attests)
//!   - signature_checked = BLAKE3 of canonical-JSON of the typed
//!     decision
//!
//! Real cryptographic chain — entries link via previous_hash field,
//! each entry's hash is BLAKE3 over the canonical serialization
//! (including previous_hash). Chain integrity validates via the
//! standard tameshi proof APIs.

use crate::{Attester, Decision, FonteResult, ProposalId, Receipt};
use async_trait::async_trait;
use std::sync::Arc;
use tameshi::hash::Blake3Hash;
use tameshi::heartbeat::{HeartbeatChain, HeartbeatEvent, VerificationOutcome, VerifierIdentity};

/// Attester wrapping a tameshi HeartbeatChain. Each attest() call
/// appends one entry; the chain's BLAKE3 link integrity is
/// cryptographic.
pub struct TameshiAttester {
    chain: Arc<HeartbeatChain>,
    identity: VerifierIdentity,
}

impl TameshiAttester {
    /// New attester with a fresh chain + an identity for telemetry.
    /// `instance` typically the hostname or pod name; `version` the
    /// crate's package version.
    #[must_use]
    pub fn new(instance: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            chain: Arc::new(HeartbeatChain::new()),
            identity: VerifierIdentity::new("engenho-fonte", &instance.into(), &version.into()),
        }
    }

    /// Use a pre-existing chain (e.g. shared across multiple
    /// attesters in a multi-controller setup).
    #[must_use]
    pub fn with_chain(
        chain: Arc<HeartbeatChain>,
        instance: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            chain,
            identity: VerifierIdentity::new("engenho-fonte", &instance.into(), &version.into()),
        }
    }

    /// Borrow the underlying chain — operators iterate entries +
    /// run proof_of / consistency_proof for compliance auditing.
    #[must_use]
    pub fn chain(&self) -> Arc<HeartbeatChain> {
        self.chain.clone()
    }
}

#[async_trait]
impl Attester for TameshiAttester {
    async fn attest(&self, decision: &Decision, proposal: ProposalId) -> FonteResult<Receipt> {
        // signature_checked: BLAKE3 of the typed decision's
        // canonical-JSON. This is the typed value the chain claims
        // was attested — anyone can recompute it from the source.
        let canonical = serde_json::to_string(&decision.typed)
            .map_err(|e| crate::FonteError::Attest(format!("decision serialize: {e}")))?;
        let signature_hash = Blake3Hash::from(*blake3::hash(canonical.as_bytes()).as_bytes());
        let resource = decision.change.source.to_string();

        let entry = self.chain.append(
            self.identity.clone(),
            HeartbeatEvent::ComplianceRecheck,
            VerificationOutcome::Allowed,
            &resource,
            signature_hash,
        );

        // Tameshi's HeartbeatEntry has entry_hash (Blake3Hash) +
        // sequence + timestamp (DateTime<Utc>). Map onto fonte's
        // Receipt shape.
        let id_hex = entry.entry_hash.to_hex();
        let sealed_at_ms = u64::try_from(entry.timestamp.timestamp_millis()).unwrap_or(0);
        let receipt = Receipt {
            id: Arc::from(&id_hex[..16]),
            proposal_id: proposal,
            sealed_at_ms,
        };
        Ok(receipt)
    }
}
