//! ShigotoRetryConduit — Conduit wrapper with typed retry policy +
//! per-tick budget.
//!
//! Each tick:
//!   1. Run Conduit::tick()
//!   2. On Ok: return outcome
//!   3. On Err: record FailureRecord; consult RetryPolicy.decide()
//!      - Retry { after } → tokio::sleep + tick again
//!      - Deadletter → return the typed error
//!
//! Budget integration (typed BudgetSpec) caps concurrency + failure
//! rate at the substrate layer. For the single-Conduit case here,
//! the budget surface is a single global counter; operators with
//! multiple Conduits wire a shared BudgetTree.
//!
//! Gated `with-shigoto`.

use crate::{Conduit, FonteError, FonteResult, Outcome};
use shigoto_retry::{FailureRecord, RetryDecision, RetryPolicy};
use shigoto_types::FailureKind;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::sleep;

/// Conduit wrapper with typed RetryPolicy.
pub struct ShigotoRetryConduit {
    inner: Arc<Conduit>,
    policy: RetryPolicy,
    history: Mutex<Vec<FailureRecord>>,
    max_attempts: u32,
}

impl ShigotoRetryConduit {
    /// Wrap a Conduit with a typed RetryPolicy. `max_attempts`
    /// caps the retry loop independent of the policy's own
    /// attempt count (defense-in-depth: a misconfigured
    /// Exponential(attempts: u32::MAX) won't loop forever here).
    #[must_use]
    pub fn new(inner: Arc<Conduit>, policy: RetryPolicy, max_attempts: u32) -> Self {
        Self {
            inner,
            policy,
            history: Mutex::new(Vec::new()),
            max_attempts: max_attempts.max(1),
        }
    }

    /// Run one tick through the retry policy.
    pub async fn tick(&self) -> FonteResult<Option<Outcome>> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.inner.tick().await {
                Ok(outcome) => return Ok(outcome),
                Err(e) => {
                    let kind = classify_fonte_error(&e);
                    let record = FailureRecord {
                        attempt,
                        at_ms: 0,
                        error: format!("{e}"),
                        kind,
                    };
                    {
                        let mut h = self.history.lock().expect("retry history poisoned");
                        h.push(record.clone());
                    }
                    let history = self.history.lock().expect("retry history poisoned").clone();
                    if attempt >= self.max_attempts {
                        return Err(e);
                    }
                    match self.policy.decide(attempt, &history) {
                        RetryDecision::Retry { after } => {
                            sleep(after).await;
                            continue;
                        }
                        RetryDecision::Deadletter => return Err(e),
                    }
                }
            }
        }
    }

    /// Read the failure history for tests + audit.
    pub fn history(&self) -> Vec<FailureRecord> {
        self.history.lock().expect("retry history poisoned").clone()
    }
}

/// Classify a FonteError as a typed FailureKind for the
/// retry decider. Watch/Eval/Propose/Attest/Publish are all
/// transient by default — operators override via custom
/// RetryDeciders for declarative classification.
fn classify_fonte_error(err: &FonteError) -> FailureKind {
    match err {
        // Eval errors that are typed Invariant (decoded structurally
        // wrong) are declarative — the operator's source is broken,
        // retrying won't help.
        FonteError::Eval(_) => FailureKind::Declarative,
        // Watch / Propose / Attest / Publish / Budget / Abort are
        // typically transient (network blip, channel hiccup, etc.)
        _ => FailureKind::Transient,
    }
}
