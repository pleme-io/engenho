//! `with-promessa` — bridge fonte's [`PromessaRef`] to promessa-types'
//! [`PromessaTargetKind`].
//!
//! Fonte's `PromessaRef { name, kind, target }` is a *reference* — a
//! typed pointer to a promessa controlled by the upstream promessa
//! ecosystem. Promessa's `TargetController` is the actual per-kind
//! controller with associated types (Spec / Snapshot / Drift) — too
//! heavy for fonte to instantiate without the per-domain Spec types
//! engenho-promessa-controllers ships.
//!
//! [`PromessaTargetReconciler`] is the bridge: each reconcile_promessa
//! call translates fonte's PromessaKind into the upstream's
//! PromessaTargetKind, logs the typed intent, and (in production with
//! engenho-promessa-controllers wired) dispatches to the registered
//! per-kind controller.
//!
//! This crate stops at the typed-bridge layer — actually firing a
//! TargetController per call needs a typed registry (which lives in
//! engenho-promessa-controllers, NOT here). The bridge records WHAT
//! the operator declared; the upstream chain does the per-kind work.

use crate::{FonteResult, PromessaKind, PromessaReconciler, PromessaRef};
use async_trait::async_trait;
use promessa_types::PromessaTargetKind;
use std::sync::Arc;
use std::sync::Mutex;

/// Translate fonte's local PromessaKind to promessa's canonical kind.
#[must_use]
pub fn promessa_kind_to_target(kind: PromessaKind) -> PromessaTargetKind {
    match kind {
        PromessaKind::Availability => PromessaTargetKind::Sla,
        PromessaKind::Budget => PromessaTargetKind::CostBudget,
        PromessaKind::Compliance => PromessaTargetKind::Compliance,
        PromessaKind::Security => PromessaTargetKind::Security,
        PromessaKind::CustomerKpi => PromessaTargetKind::CustomerKpi,
    }
}

/// Recorded intent — what fonte wants to delegate to the upstream
/// promessa ecosystem.
#[derive(Debug, Clone, PartialEq)]
pub struct PromessaIntent {
    /// Promessa name.
    pub name: Arc<str>,
    /// Typed upstream kind (the kebab-case slug the upstream
    /// controller registers under).
    pub kind: PromessaTargetKind,
    /// The target value the controller chases.
    pub target: f64,
}

/// Real `PromessaReconciler` that records typed intents. Operators
/// in production wire this alongside an engenho-promessa-controllers
/// runtime that picks up the recorded intents + drives the actual
/// per-kind TargetController.
#[derive(Debug, Default)]
pub struct PromessaTargetReconciler {
    intents: Mutex<Vec<PromessaIntent>>,
}

impl PromessaTargetReconciler {
    /// New empty reconciler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the recorded intent log.
    pub fn intents(&self) -> Vec<PromessaIntent> {
        self.intents
            .lock()
            .expect("promessa reconciler poisoned")
            .clone()
    }
}

#[async_trait]
impl PromessaReconciler for PromessaTargetReconciler {
    async fn reconcile_promessa(&self, promessa: &PromessaRef) -> FonteResult<()> {
        let intent = PromessaIntent {
            name: promessa.name.clone(),
            kind: promessa_kind_to_target(promessa.kind),
            target: promessa.target,
        };
        self.intents
            .lock()
            .expect("promessa reconciler poisoned")
            .push(intent);
        Ok(())
    }
}
