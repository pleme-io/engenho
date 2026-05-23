//! R20 — Admission webhook trait.
//!
//! Lets the apiserver call out to a pluggable validator before
//! committing any `ResourceCommand`. Backends can:
//!   * Inspect the proposed value (validation)
//!   * Mutate the proposed value (mutating webhook)
//!   * Reject the request with a typed reason
//!
//! ## Use-cases
//!
//! * Defaulting (mutating) — set missing `replicas`, inject sidecars
//! * Validation — reject Pods without resource requests, enforce
//!   network-policy labels, gate compliance overlays
//! * Policy — enforce admission rules from a Cilium/OPA/Kyverno
//!   compatibility layer
//! * Tameshi attestation — verify image signatures before admission
//!
//! ## Decision shape
//!
//! Each webhook returns an [`AdmissionDecision`]:
//!   * `Allow` — accept the request as-is
//!   * `Mutate(new_value)` — accept with a transformed value
//!   * `Deny(reason)` — reject; reason surfaces to the operator
//!
//! Multiple webhooks compose: the apiserver feeds the (possibly
//! mutated) value through every registered webhook in order. First
//! `Deny` short-circuits.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::resource::ResourceKey;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;

/// What an admission webhook says about a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Accept the request, optionally with a mutated value.
    Allow,
    /// Accept the request with the value replaced by `new`.
    Mutate(Value),
    /// Reject the request with a human-readable reason.
    Deny(String),
}

/// What action the request is performing (mirrors `ResourceCommand`'s
/// variants but as a flat enum the webhook can pattern-match on).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionAction {
    /// `Put` of a new or existing resource.
    Put,
    /// `Patch` — partial update.
    Patch,
    /// `Delete` of an existing resource.
    Delete,
}

/// The complete admission request handed to a webhook.
#[derive(Clone, Debug)]
pub struct AdmissionRequest {
    /// What's being done.
    pub action: AdmissionAction,
    /// The target resource.
    pub key: ResourceKey,
    /// The proposed value (for Put / Patch). None for Delete.
    pub value: Option<Value>,
    /// The current stored value, if the resource already exists.
    pub current: Option<Value>,
}

/// Admission errors.
#[derive(Debug, Clone, Error)]
pub enum AdmissionError {
    /// Webhook backend (HTTP / OPA / Kyverno) failed.
    #[error("backend: {0}")]
    Backend(String),
}

impl AdmissionError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
        }
    }
}

/// Pluggable admission webhook trait.
#[async_trait]
pub trait AdmissionWebhook: Send + Sync {
    /// Webhook identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Inspect / mutate / reject `request`.
    ///
    /// # Errors
    /// [`AdmissionError::Backend`] on backend failure (caller
    /// decides whether to fail-open or fail-closed via the
    /// [`AdmissionChain`] mode).
    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError>;
}

// =================================================================
// AdmissionChain — runs N webhooks in order, fan-out + short-circuit
// =================================================================

/// Composition policy when a webhook returns an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionMode {
    /// Treat backend errors as `Allow` (fail-open). Production
    /// default for non-critical webhooks (e.g. metrics injectors).
    FailOpen,
    /// Treat backend errors as `Deny` (fail-closed). Production
    /// default for compliance/security webhooks.
    FailClosed,
}

/// Chain of webhooks evaluated in order. Each webhook's mutation is
/// fed forward to the next; first Deny short-circuits.
pub struct AdmissionChain {
    hooks: Vec<Arc<dyn AdmissionWebhook>>,
    mode: AdmissionMode,
}

impl AdmissionChain {
    /// New chain with the given mode.
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn AdmissionWebhook>>, mode: AdmissionMode) -> Self {
        Self { hooks, mode }
    }

    /// Evaluate the chain. Returns the final value (possibly
    /// mutated) on Allow; the Deny reason on rejection.
    pub async fn review(
        &self,
        mut request: AdmissionRequest,
    ) -> AdmissionDecision {
        for hook in &self.hooks {
            let decision = match hook.review(&request).await {
                Ok(d) => d,
                Err(_) => match self.mode {
                    AdmissionMode::FailOpen => AdmissionDecision::Allow,
                    AdmissionMode::FailClosed => {
                        return AdmissionDecision::Deny(format!(
                            "{}: backend failure (fail-closed)",
                            hook.name()
                        ));
                    }
                },
            };
            match decision {
                AdmissionDecision::Allow => continue,
                AdmissionDecision::Mutate(new) => {
                    request.value = Some(new);
                }
                AdmissionDecision::Deny(reason) => {
                    return AdmissionDecision::Deny(format!(
                        "{}: {reason}",
                        hook.name()
                    ));
                }
            }
        }
        // Either the value is unchanged (Allow) or it was mutated
        // through the chain — both surface as Allow/Mutate.
        match request.value {
            Some(v) => AdmissionDecision::Mutate(v),
            None => AdmissionDecision::Allow,
        }
    }

    /// Count of hooks in the chain.
    #[must_use]
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    /// True if no hooks are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

// =================================================================
// FakeAdmissionWebhook — deterministic for tests
// =================================================================

/// Test webhook with explicit decisions per-key. Records review calls.
#[derive(Clone, Default)]
pub struct FakeAdmissionWebhook {
    inner: Arc<Mutex<FakeAdmissionState>>,
    backend_name: &'static str,
}

#[derive(Default)]
struct FakeAdmissionState {
    decisions: BTreeMap<String, AdmissionDecision>,
    fail_with: Option<String>,
    call_log: Vec<(AdmissionAction, String)>,
}

impl FakeAdmissionWebhook {
    /// New webhook with the given telemetry name.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeAdmissionState::default())),
            backend_name: name,
        }
    }

    /// Pin a decision for a specific resource (by `key.label()`).
    pub async fn set_decision(&self, key: &str, d: AdmissionDecision) {
        self.inner.lock().await.decisions.insert(key.to_string(), d);
    }

    /// Make `review` return `AdmissionError::Backend` for the next call.
    pub async fn fail_next(&self, msg: impl Into<String>) {
        self.inner.lock().await.fail_with = Some(msg.into());
    }

    /// Snapshot of the call log.
    pub async fn calls(&self) -> Vec<(AdmissionAction, String)> {
        self.inner.lock().await.call_log.clone()
    }
}

#[async_trait]
impl AdmissionWebhook for FakeAdmissionWebhook {
    fn name(&self) -> &'static str {
        self.backend_name
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        let mut state = self.inner.lock().await;
        state
            .call_log
            .push((request.action, request.key.label()));
        if let Some(msg) = state.fail_with.take() {
            return Err(AdmissionError::Backend(msg));
        }
        Ok(state
            .decisions
            .get(&request.key.label())
            .cloned()
            .unwrap_or(AdmissionDecision::Allow))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(action: AdmissionAction, name: &str, value: Value) -> AdmissionRequest {
        AdmissionRequest {
            action,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: Some(value),
            current: None,
        }
    }

    fn label_for(name: &str) -> String {
        ResourceKey::namespaced("", "v1", "Pod", "default", name).label()
    }

    #[tokio::test]
    async fn allow_passes_through_unchanged() {
        let hook = Arc::new(FakeAdmissionWebhook::new("test"));
        let chain = AdmissionChain::new(vec![hook], AdmissionMode::FailClosed);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({"spec": {}})))
            .await;
        // value was set → returns Mutate of unchanged value
        assert!(matches!(r, AdmissionDecision::Mutate(_)));
    }

    #[tokio::test]
    async fn deny_short_circuits_chain() {
        let h1 = Arc::new(FakeAdmissionWebhook::new("h1"));
        let h2 = Arc::new(FakeAdmissionWebhook::new("h2"));
        h1.set_decision(&label_for("x"), AdmissionDecision::Deny("no".into()))
            .await;
        let chain =
            AdmissionChain::new(vec![h1.clone(), h2.clone()], AdmissionMode::FailClosed);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({})))
            .await;
        assert!(matches!(r, AdmissionDecision::Deny(reason) if reason.contains("h1") && reason.contains("no")));
        // h2 should NOT have been called.
        assert_eq!(h2.calls().await.len(), 0);
    }

    #[tokio::test]
    async fn mutate_feeds_forward() {
        let h1 = Arc::new(FakeAdmissionWebhook::new("defaulter"));
        let h2 = Arc::new(FakeAdmissionWebhook::new("validator"));
        h1.set_decision(
            &label_for("x"),
            AdmissionDecision::Mutate(json!({"spec": {"replicas": 3}})),
        )
        .await;
        // h2 sees the mutated value: read the call log
        let chain =
            AdmissionChain::new(vec![h1.clone(), h2.clone()], AdmissionMode::FailClosed);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({"spec": {}})))
            .await;
        match r {
            AdmissionDecision::Mutate(v) => {
                assert_eq!(v.get("spec").unwrap().get("replicas").unwrap(), 3);
            }
            other => panic!("expected Mutate, got {other:?}"),
        }
        assert_eq!(h2.calls().await.len(), 1);
    }

    #[tokio::test]
    async fn fail_open_treats_error_as_allow() {
        let h1 = Arc::new(FakeAdmissionWebhook::new("h1"));
        h1.fail_next("oops").await;
        let chain = AdmissionChain::new(vec![h1], AdmissionMode::FailOpen);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({})))
            .await;
        // Failure swallowed; value still flows through as Mutate(unchanged).
        assert!(matches!(r, AdmissionDecision::Mutate(_)));
    }

    #[tokio::test]
    async fn fail_closed_treats_error_as_deny() {
        let h1 = Arc::new(FakeAdmissionWebhook::new("compliance"));
        h1.fail_next("oops").await;
        let chain = AdmissionChain::new(vec![h1], AdmissionMode::FailClosed);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({})))
            .await;
        match r {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("compliance"));
                assert!(reason.contains("fail-closed"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_chain_allows_anything() {
        let chain = AdmissionChain::new(vec![], AdmissionMode::FailClosed);
        let r = chain
            .review(req(AdmissionAction::Put, "x", json!({})))
            .await;
        assert!(matches!(r, AdmissionDecision::Mutate(_)));
    }

    #[tokio::test]
    async fn chain_metadata_helpers() {
        let h1 = Arc::new(FakeAdmissionWebhook::new("a"));
        let h2 = Arc::new(FakeAdmissionWebhook::new("b"));
        let chain = AdmissionChain::new(vec![h1, h2], AdmissionMode::FailOpen);
        assert_eq!(chain.len(), 2);
        assert!(!chain.is_empty());
        let empty = AdmissionChain::new(vec![], AdmissionMode::FailOpen);
        assert!(empty.is_empty());
    }

    #[test]
    fn admission_error_kind_is_stable() {
        assert_eq!(AdmissionError::Backend("x".into()).kind(), "backend");
    }
}
