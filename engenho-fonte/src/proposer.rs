//! The `Proposer` role — commits a typed `Decision` to consensus.

use crate::{Decision, FonteResult};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotone identifier returned by the Proposer for every committed
/// decision. Real impls (M1.3+) map this to revoada's Raft log index;
/// the mock generates a per-instance fetch_add counter.
pub type ProposalId = u64;

/// Typed Proposer. Submits a [`Decision`] to consensus and returns
/// the typed commit identifier.
#[async_trait]
pub trait Proposer: Send + Sync {
    /// Propose the decision; block until consensus accepts.
    async fn propose(&self, decision: &Decision) -> FonteResult<ProposalId>;
}

// ── Mock impl (always available) ─────────────────────────────────

/// In-memory `Proposer` that hands out monotone integers.
#[derive(Debug, Default)]
pub struct MockProposer {
    next_id: AtomicU64,
}

impl MockProposer {
    /// New mock starting at proposal id 0.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Proposer for MockProposer {
    async fn propose(&self, _decision: &Decision) -> FonteResult<ProposalId> {
        Ok(self.next_id.fetch_add(1, Ordering::SeqCst))
    }
}
