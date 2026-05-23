//! The `Evaluator` role — parses + type-checks a source `Change`
//! into a typed `Decision`.

use crate::{Change, Decision, FonteError, FonteResult};
use async_trait::async_trait;
use engenho_sui_typescape::TypescapeValue;

/// Typed evaluator. Takes a [`Change`] and produces a [`Decision`]
/// whose `typed` field is a `TypescapeValue`. Real impls (M1.2+) wrap
/// sui's evaluator behind `with-sui-eval`; the mock parses a tiny
/// JSON subset that's enough to test the pipeline end-to-end.
#[async_trait]
pub trait Evaluator: Send + Sync {
    /// Evaluate a change, producing a typed decision.
    async fn evaluate(&self, change: Change) -> FonteResult<Decision>;
}

// ── Mock impl (always available) ─────────────────────────────────

/// Mock evaluator that parses the change's `source_text` as JSON and
/// converts the resulting `serde_json::Value` into a `TypescapeValue`.
///
/// This is deliberately small: real `(defsistema …)` tlisp parsing
/// is the M1.2 deliverable behind `with-sui-eval`. For mock-driven
/// tests, an operator hands the watcher a JSON snippet like
/// `{"name": "rio", "replicas": 3}` and the convergence pipeline
/// flows the typed value end-to-end.
#[derive(Debug, Default)]
pub struct MockEvaluator;

impl MockEvaluator {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Evaluator for MockEvaluator {
    async fn evaluate(&self, change: Change) -> FonteResult<Decision> {
        let raw: serde_json::Value =
            serde_json::from_str(change.source_text.as_ref()).map_err(|e| {
                FonteError::Eval(engenho_sui_typescape::TypescapeError::Invariant {
                    location: change.source.to_string(),
                    reason: format!("invalid JSON: {e}"),
                })
            })?;
        let typed = json_to_typescape(raw);
        Ok(Decision { change, typed })
    }
}

fn json_to_typescape(j: serde_json::Value) -> TypescapeValue {
    use serde_json::Value as J;
    match j {
        J::Null => TypescapeValue::null(),
        J::Bool(b) => TypescapeValue::bool(b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                TypescapeValue::int(i)
            } else if let Some(f) = n.as_f64() {
                TypescapeValue::float(f)
            } else {
                TypescapeValue::null()
            }
        }
        J::String(s) => TypescapeValue::string(s.as_str()),
        J::Array(a) => TypescapeValue::list(a.into_iter().map(json_to_typescape)),
        J::Object(m) => {
            TypescapeValue::attrs(m.into_iter().map(|(k, v)| (k, json_to_typescape(v))))
        }
    }
}
