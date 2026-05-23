//! The `Attester` role — chains a BLAKE3 receipt for the committed
//! transition.

use crate::{Decision, FonteResult, ProposalId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

/// Typed attestation receipt. The mock returns a `Receipt` whose
/// `id` is the BLAKE3 hex of the decision + proposal_id; real impls
/// (M1.4+) hand this to tameshi+sekiban+kensa for chain insertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    /// Stable identifier (mock: 16-char BLAKE3 hex prefix; real:
    /// full BLAKE3 of the chain entry).
    pub id: Arc<str>,
    /// The proposal this receipt attests.
    pub proposal_id: ProposalId,
    /// Wall-clock ms-since-epoch when the receipt was sealed.
    pub sealed_at_ms: u64,
}

/// Typed Attester. Seals a `(decision, proposal_id)` pair into a
/// chained receipt.
#[async_trait]
pub trait Attester: Send + Sync {
    /// Compute + chain a receipt.
    async fn attest(&self, decision: &Decision, proposal: ProposalId) -> FonteResult<Receipt>;
}

// ── Mock impl (always available) ─────────────────────────────────

/// Mock attester that BLAKE3-hashes the canonical-JSON of (decision,
/// proposal_id) and chains entries in memory. Suitable for testing
/// the pipeline's wiring + the BLAKE3 chain invariant (each entry's
/// `prev` is the previous entry's `id`).
#[derive(Debug)]
pub struct MockAttester {
    state: Mutex<MockAttesterState>,
}

#[derive(Debug, Default)]
struct MockAttesterState {
    last_id: Option<Arc<str>>,
    chain: Vec<Receipt>,
    /// Deterministic clock: mock seals at monotone ms-since-construction
    /// rather than wall-clock so tests are byte-deterministic.
    next_ms: u64,
}

impl Default for MockAttester {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAttester {
    /// New empty attester chain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MockAttesterState::default()),
        }
    }

    /// Borrow the chain for assertion in tests.
    pub fn chain(&self) -> Vec<Receipt> {
        self.state
            .lock()
            .expect("mock attester poisoned")
            .chain
            .clone()
    }
}

#[async_trait]
impl Attester for MockAttester {
    async fn attest(&self, decision: &Decision, proposal: ProposalId) -> FonteResult<Receipt> {
        let mut state = self.state.lock().expect("mock attester poisoned");
        // Hash inputs deterministically. Chain link via `prev` =
        // last_id; if absent use the all-zero sentinel.
        let prev_hex = state
            .last_id
            .clone()
            .unwrap_or_else(|| Arc::from(ZERO_PREV));
        let canonical = serde_json::json!({
            "prev": prev_hex.as_ref(),
            "proposal": proposal,
            "revision": decision.change.revision,
            "source": decision.change.source.as_ref(),
            "typed": &decision.typed,
        })
        .to_string();
        let hash = blake3::hash(canonical.as_bytes()).to_hex();
        let id: Arc<str> = Arc::from(&hash[..16]);
        let sealed_at_ms = state.next_ms;
        state.next_ms += 1;
        let receipt = Receipt {
            id: id.clone(),
            proposal_id: proposal,
            sealed_at_ms,
        };
        state.chain.push(receipt.clone());
        state.last_id = Some(id);
        Ok(receipt)
    }
}

const ZERO_PREV: &str = "0000000000000000";
