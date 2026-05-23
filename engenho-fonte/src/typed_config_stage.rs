//! TypedConfigStage — concrete ProvisioningStage that evaluates a
//! stage's typed JSON configuration.
//!
//! Always-on (no Cargo feature flag). Pattern for real
//! magma/pangea/engenho-install stages: each one's configuration
//! lives as a TypescapeValue (JSON-backed) attached to the Sistema;
//! the stage parses + types the value, dispatches to the real
//! backend (when wired behind future feature flags), records the
//! typed config for audit otherwise.
//!
//! See `typed_nix_stage.rs` for the Nix-source variant
//! (gated `with-sui-eval`, currently waiting on a sui-bytecode
//! upstream fix for the new `HeapObject::BigInt` variant — once
//! that lands, the two variants converge into one).

use crate::{FonteResult, ProvisioningStage, Sistema, StageKind};
use async_trait::async_trait;
use engenho_sui_typescape::TypescapeValue;
use serde_json::Value as JsonValue;
use std::sync::Mutex;

/// ProvisioningStage that takes a JSON configuration per Sistema
/// and records the typed result. The closure `config_fn` emits a
/// JSON string per Sistema being provisioned; the stage parses it
/// into a TypescapeValue + logs it.
pub struct TypedConfigStage {
    kind: StageKind,
    config_fn: Box<dyn Fn(&Sistema) -> String + Send + Sync>,
    evaluated: Mutex<Vec<TypescapeValue>>,
}

impl TypedConfigStage {
    /// Stage with a constant JSON config.
    pub fn from_json(kind: StageKind, json: impl Into<String>) -> Self {
        let json = json.into();
        Self {
            kind,
            config_fn: Box::new(move |_| json.clone()),
            evaluated: Mutex::new(Vec::new()),
        }
    }

    /// Stage with a per-Sistema JSON config — the typical real case
    /// (e.g. emits `{"cluster":"rio","nodes":3}` from the Sistema's
    /// name + topology).
    pub fn per_sistema<F>(kind: StageKind, config_fn: F) -> Self
    where
        F: Fn(&Sistema) -> String + Send + Sync + 'static,
    {
        Self {
            kind,
            config_fn: Box::new(config_fn),
            evaluated: Mutex::new(Vec::new()),
        }
    }

    /// Read the log of evaluated TypescapeValues for tests / audit.
    pub fn evaluated(&self) -> Vec<TypescapeValue> {
        self.evaluated.lock().expect("stage poisoned").clone()
    }
}

#[async_trait]
impl ProvisioningStage for TypedConfigStage {
    fn kind(&self) -> StageKind {
        self.kind
    }

    async fn provision(&self, sistema: &Sistema) -> FonteResult<()> {
        let raw = (self.config_fn)(sistema);
        let json: JsonValue = serde_json::from_str(&raw).map_err(|e| {
            crate::FonteError::Propose(format!(
                "typed-config-stage {:?} JSON parse: {e}",
                self.kind
            ))
        })?;
        let typed = json_to_typescape(json);
        self.evaluated.lock().expect("stage poisoned").push(typed);
        Ok(())
    }
}

fn json_to_typescape(j: JsonValue) -> TypescapeValue {
    use JsonValue::*;
    match j {
        Null => TypescapeValue::null(),
        Bool(b) => TypescapeValue::bool(b),
        Number(n) => {
            if let Some(i) = n.as_i64() {
                TypescapeValue::int(i)
            } else if let Some(f) = n.as_f64() {
                TypescapeValue::float(f)
            } else {
                TypescapeValue::null()
            }
        }
        String(s) => TypescapeValue::string(s.as_str()),
        Array(a) => TypescapeValue::list(a.into_iter().map(json_to_typescape)),
        Object(m) => TypescapeValue::attrs(m.into_iter().map(|(k, v)| (k, json_to_typescape(v)))),
    }
}
