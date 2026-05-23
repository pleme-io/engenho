//! TypedNixStage — concrete ProvisioningStage that evaluates a
//! stage's Nix configuration via sui-typescape.
//!
//! Pattern for real magma/pangea/engenho-install stages: each one's
//! configuration lives as a Nix expression in the Sistema's repo;
//! the stage evaluates the expression at provision time, types the
//! result, and (in production with the relevant backend feature
//! flag) dispatches to the real provisioner. The dry-run records
//! the typed config for inspection.
//!
//! Gated `with-sui-eval` — requires the Nix VM to evaluate.

#![cfg(feature = "with-sui-eval")]

use crate::{FonteResult, ProvisioningStage, Sistema, StageKind};
use async_trait::async_trait;
use engenho_sui_typescape::{TypescapeValue, eval_nix_str};
use std::sync::Mutex;

/// ProvisioningStage that evaluates a Nix-shaped configuration per
/// Sistema. Records the typed result for audit.
///
/// Two construction modes:
///   * `TypedNixStage::from_nix(kind, nix_expr)` — eval a literal
///     Nix expression at construction (fast; suitable for tests
///     and homogeneous stages).
///   * `TypedNixStage::per_sistema(kind, |sistema| nix_expr)` — eval
///     a Nix expression that depends on the Sistema being
///     provisioned (operator-shaped — the typical real case).
pub struct TypedNixStage {
    kind: StageKind,
    expr_fn: Box<dyn Fn(&Sistema) -> String + Send + Sync>,
    evaluated: Mutex<Vec<TypescapeValue>>,
}

impl TypedNixStage {
    /// Stage with a constant Nix expression.
    pub fn from_nix(kind: StageKind, expr: impl Into<String>) -> Self {
        let expr = expr.into();
        Self {
            kind,
            expr_fn: Box::new(move |_| expr.clone()),
            evaluated: Mutex::new(Vec::new()),
        }
    }

    /// Stage with a per-Sistema Nix expression — the closure
    /// produces the Nix source per invocation (typical: emits
    /// `(defmagmastage { cluster = "rio"; nodes = 3; })` from a
    /// Sistema's name + topology).
    pub fn per_sistema<F>(kind: StageKind, expr_fn: F) -> Self
    where
        F: Fn(&Sistema) -> String + Send + Sync + 'static,
    {
        Self {
            kind,
            expr_fn: Box::new(expr_fn),
            evaluated: Mutex::new(Vec::new()),
        }
    }

    /// Read the log of evaluated TypescapeValues for tests / audit.
    pub fn evaluated(&self) -> Vec<TypescapeValue> {
        self.evaluated.lock().expect("stage poisoned").clone()
    }
}

#[async_trait]
impl ProvisioningStage for TypedNixStage {
    fn kind(&self) -> StageKind {
        self.kind
    }

    async fn provision(&self, sistema: &Sistema) -> FonteResult<()> {
        let expr = (self.expr_fn)(sistema);
        let typed = eval_nix_str(&expr).map_err(|e| {
            crate::FonteError::Propose(format!("typed-nix-stage {:?} failed eval: {e}", self.kind))
        })?;
        self.evaluated.lock().expect("stage poisoned").push(typed);
        Ok(())
    }
}
