//! `with-sui-eval` — real [`Evaluator`] that parses each Change's
//! source_text as Nix via sui-eval and converts the result through
//! [`engenho_sui_typescape::from_sui_value`].
//!
//! Wired in two lines once the typescape adapter exists (v1.19).
//! No further plumbing needed — the typed Decision flows downstream
//! through the same Conduit pipeline that the JSON-driven
//! MockEvaluator already proved.

use crate::{Change, Decision, Evaluator, FonteResult};
use async_trait::async_trait;
use engenho_sui_typescape::eval_nix_str;

/// Evaluator backed by sui's Nix bytecode VM. Parses each Change's
/// `source_text` as a Nix expression, forces the result via
/// [`from_sui_value`].
#[derive(Debug, Default)]
pub struct SuiEvaluator;

impl SuiEvaluator {
    /// New SuiEvaluator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Evaluator for SuiEvaluator {
    async fn evaluate(&self, change: Change) -> FonteResult<Decision> {
        // eval_nix_str is synchronous; for typed Sistema declarations
        // (tens of KB max) it evaluates in microseconds.
        let typed = eval_nix_str(change.source_text.as_ref())?;
        Ok(Decision { change, typed })
    }
}
