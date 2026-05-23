//! Typed remediation policy — routes [`AnomalyEvent`]s to typed
//! responses.
//!
//! Maps 1:1 to the Viggy Method's RemediationPolicy enum
//! (`pleme-io/theory/CONTINUOUS-SOLUTION-MACHINE.md` §III.5): every
//! drift event the AnomalyChain records flows through an
//! [`AnomalyRouter`] that picks a typed [`RemediationPolicy`] based
//! on the event kind + asks the routed handler to act.
//!
//! Mock-driven by default ([`MockAnomalyRouter`]); real wiring to
//! viggy's AnomalyController + EscalationLadder lands in M3.5+ behind
//! the `with-viggy` feature flag (not yet drafted).
//!
//! ## Routing rule semantics
//!
//! - **NoOp** — drift is ignored. The chain still records it
//!   (auditability never sacrificed) but no action runs.
//! - **Alert** — drift triggers a typed alert; the handler is
//!   responsible for fan-out (e.g. ntfy, opsgenie, mirante channel
//!   bump). The chain still records.
//! - **AutoCorrect** — drift triggers an automatic correction;
//!   typically a re-reconcile against `last_applied` or a typed
//!   roll-forward. Used by SLA / CostBudget promessas.
//! - **RequireApproval** — drift halts the convergence; an operator
//!   must approve (or the Conduit times out + escalates).
//! - **Escalate** — drift fires the EscalationLadder (typed series
//!   of progressively-broader notifications + page-outs).

use crate::{AnomalyEvent, FonteResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

/// Typed remediation policy. One per AnomalyEvent kind (per routing
/// rule); the policy is the typed response the router selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemediationPolicy {
    /// Ignore the drift (still chained for audit).
    NoOp,
    /// Surface the drift as a typed alert.
    Alert,
    /// Drive an automatic correction.
    AutoCorrect,
    /// Halt convergence until operator approves.
    RequireApproval,
    /// Fire the escalation ladder.
    Escalate,
}

impl RemediationPolicy {
    /// Default routing per anomaly kind. Operators override per-cluster
    /// via a typed policy table; these defaults match the Viggy
    /// Method's documented "safe-by-default" choices.
    #[must_use]
    pub fn default_for(event: &AnomalyEvent) -> Self {
        match event {
            // Additions are typed declarative growth — the operator
            // wanted them. Auto-correct (reconcile against new
            // desired).
            AnomalyEvent::AppAdded(_)
            | AnomalyEvent::InfraAdded(_)
            | AnomalyEvent::PromessaAdded(_) => Self::AutoCorrect,
            // Removals are typed destructive intent — surface as
            // alert + auto-correct. Real operators sometimes want
            // RequireApproval; per-cluster override available.
            AnomalyEvent::AppRemoved(_)
            | AnomalyEvent::InfraRemoved(_)
            | AnomalyEvent::PromessaRemoved(_) => Self::AutoCorrect,
            // Version changes — small move; auto-correct.
            AnomalyEvent::AppVersionChanged { .. } => Self::AutoCorrect,
            // Target shift — auto-correct (controller chases new
            // target).
            AnomalyEvent::PromessaTargetChanged { .. } => Self::AutoCorrect,
            // Topology shift — bigger move; alert. Operator wires
            // AutoCorrect explicitly for cluster-elastic systems.
            AnomalyEvent::TopologyChanged { .. } => Self::Alert,
        }
    }
}

/// Async handler called by the AnomalyRouter for each routed event.
/// Real wiring (M3.5+) bridges per-policy to the cluster's
/// AlertManager / OpsAutomation / approval-queue surfaces. Mock impl
/// records (event, policy) tuples for assertion.
#[async_trait]
pub trait AnomalyHandler: Send + Sync {
    /// Act on the routed (event, policy) pair.
    async fn handle(&self, event: &AnomalyEvent, policy: RemediationPolicy) -> FonteResult<()>;
}

/// Routes anomaly events to typed handlers per policy. Holds an
/// `Arc<dyn AnomalyHandler>` per policy variant — consumers register
/// what they want for each policy. Unregistered policies are a no-op
/// (the event is logged but no handler runs).
pub struct AnomalyRouter {
    no_op: Arc<dyn AnomalyHandler>,
    alert: Arc<dyn AnomalyHandler>,
    auto_correct: Arc<dyn AnomalyHandler>,
    require_approval: Arc<dyn AnomalyHandler>,
    escalate: Arc<dyn AnomalyHandler>,
    rule: Arc<dyn Fn(&AnomalyEvent) -> RemediationPolicy + Send + Sync>,
}

impl AnomalyRouter {
    /// Build a router with one handler per policy. The default routing
    /// rule is [`RemediationPolicy::default_for`]; override via
    /// [`Self::with_routing_rule`].
    #[must_use]
    pub fn new(
        no_op: Arc<dyn AnomalyHandler>,
        alert: Arc<dyn AnomalyHandler>,
        auto_correct: Arc<dyn AnomalyHandler>,
        require_approval: Arc<dyn AnomalyHandler>,
        escalate: Arc<dyn AnomalyHandler>,
    ) -> Self {
        Self {
            no_op,
            alert,
            auto_correct,
            require_approval,
            escalate,
            rule: Arc::new(RemediationPolicy::default_for),
        }
    }

    /// Override the default routing rule.
    #[must_use]
    pub fn with_routing_rule<F>(mut self, rule: F) -> Self
    where
        F: Fn(&AnomalyEvent) -> RemediationPolicy + Send + Sync + 'static,
    {
        self.rule = Arc::new(rule);
        self
    }

    /// Dispatch one event through the router. Returns the policy
    /// selected so callers can assert + audit.
    pub async fn route(&self, event: &AnomalyEvent) -> FonteResult<RemediationPolicy> {
        let policy = (self.rule)(event);
        let handler = match policy {
            RemediationPolicy::NoOp => &self.no_op,
            RemediationPolicy::Alert => &self.alert,
            RemediationPolicy::AutoCorrect => &self.auto_correct,
            RemediationPolicy::RequireApproval => &self.require_approval,
            RemediationPolicy::Escalate => &self.escalate,
        };
        handler.handle(event, policy).await?;
        Ok(policy)
    }
}

/// Mock handler that records every (event, policy) tuple.
#[derive(Debug, Default)]
pub struct MockAnomalyHandler {
    log: Mutex<Vec<(AnomalyEvent, RemediationPolicy)>>,
}

impl MockAnomalyHandler {
    /// New mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Read the log of routed (event, policy) pairs.
    pub fn log(&self) -> Vec<(AnomalyEvent, RemediationPolicy)> {
        self.log.lock().expect("mock handler poisoned").clone()
    }
}

#[async_trait]
impl AnomalyHandler for MockAnomalyHandler {
    async fn handle(&self, event: &AnomalyEvent, policy: RemediationPolicy) -> FonteResult<()> {
        self.log
            .lock()
            .expect("mock handler poisoned")
            .push((event.clone(), policy));
        Ok(())
    }
}

/// Convenience: build a router where every policy routes to ONE
/// shared mock handler. Tests can read `handler.log()` to assert
/// what was routed where.
#[must_use]
pub fn mock_anomaly_router() -> (Arc<MockAnomalyHandler>, AnomalyRouter) {
    let h = Arc::new(MockAnomalyHandler::new());
    let router = AnomalyRouter::new(h.clone(), h.clone(), h.clone(), h.clone(), h.clone());
    (h, router)
}
