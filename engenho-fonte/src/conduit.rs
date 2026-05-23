//! The `Conduit` supervisor — runs the seven-beat Viggy convergence
//! tick across the five typed roles.

use crate::{
    Attester, Change, Decision, Evaluator, FonteError, FonteResult, Outcome, Proposer, Publisher,
    Watcher,
};
use std::sync::Arc;

/// Supervisor wiring the five typed roles into the seven-beat
/// convergence tick.
///
/// ```text
/// Observe   ← Watcher::next()
/// Diff      ← compare change.revision against last applied
/// Classify  ← inspect ChangeKind
/// Decide    ← Evaluator::evaluate()
/// Act       ← Proposer::propose()
/// Attest    ← Attester::attest()
/// Tick      ← Publisher::publish() + advance internal cursor
/// ```
///
/// Holds each role behind `Arc<dyn Trait>` so a real wiring +
/// a mock wiring share the type.
pub struct Conduit {
    watcher: Arc<dyn Watcher>,
    evaluator: Arc<dyn Evaluator>,
    proposer: Arc<dyn Proposer>,
    attester: Arc<dyn Attester>,
    publisher: Arc<dyn Publisher>,
    last_revision: std::sync::Mutex<Option<u64>>,
}

impl Conduit {
    /// Build a conduit from the five typed roles. Each is `Arc<dyn …>`
    /// so the same conduit type accepts mock + real impls.
    #[must_use]
    pub fn new(
        watcher: Arc<dyn Watcher>,
        evaluator: Arc<dyn Evaluator>,
        proposer: Arc<dyn Proposer>,
        attester: Arc<dyn Attester>,
        publisher: Arc<dyn Publisher>,
    ) -> Self {
        Self {
            watcher,
            evaluator,
            proposer,
            attester,
            publisher,
            last_revision: std::sync::Mutex::new(None),
        }
    }

    /// Run one full convergence tick: pull changes off the watcher
    /// until one is fresh (revision strictly greater than the last
    /// applied); drive it through evaluate → propose → attest →
    /// publish.
    ///
    /// Returns `Ok(Some(Outcome))` when one change was processed,
    /// `Ok(None)` only when the watcher's stream is empty. Stale
    /// changes (revision ≤ last applied) are silently dropped within
    /// this call so `tick()` is a *valid-progress* primitive — never
    /// a noop when the watcher still has work pending.
    pub async fn tick(&self) -> FonteResult<Option<Outcome>> {
        loop {
            let Some(change) = self.watcher.next().await? else {
                return Ok(None);
            };
            {
                let mut last = self.last_revision.lock().expect("conduit poisoned");
                if let Some(prev) = *last
                    && change.revision <= prev
                {
                    continue;
                }
                *last = Some(change.revision);
            }
            return Ok(Some(self.drive(change).await?));
        }
    }

    /// Run ticks back-to-back until the watcher returns `None`.
    /// Useful for batch tests; production wiring loops with backoff
    /// instead.
    pub async fn drain(&self) -> FonteResult<Vec<Outcome>> {
        let mut out = Vec::new();
        while let Some(o) = self.tick().await? {
            out.push(o);
        }
        Ok(out)
    }

    async fn drive(&self, change: Change) -> FonteResult<Outcome> {
        let revision = change.revision;
        let decision: Decision = self.evaluator.evaluate(change).await?;
        let proposal_id = self.proposer.propose(&decision).await?;
        let receipt = self.attester.attest(&decision, proposal_id).await?;
        let outcome = Outcome {
            revision,
            proposal_id,
            receipt_id: receipt.id,
            finalized_at_ms: receipt.sealed_at_ms,
        };
        self.publisher.publish(&outcome).await?;
        Ok(outcome)
    }
}

// ── Compile-time assertion: Conduit is Send+Sync ──────────────────
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Conduit>();
    assert_send_sync::<FonteError>();
};
