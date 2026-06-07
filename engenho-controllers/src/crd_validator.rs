//! CRD validation webhook — typed-shape validation of CR writes.
//!
//! Performs typed-shape validation on every Put / Patch:
//!   * The kind must be registered (or be in the core "always-allowed"
//!     set: Pod / Service / Endpoints / etc.)
//!   * Scope match — Namespaced kinds must have a namespace; Cluster-scoped
//!     kinds must NOT.
//!   * Required fields exist (shallow check via the entry's
//!     `schema.required`, when present).
//!
//! ## Status: DEFERRED + UNWIRED
//!
//! This webhook is NOT wired into the runtime admission chain today —
//! structural-schema validation of CR instances is a DEFERRED surface (per
//! the CRD-serving scope boundary: CR creates are accepted as opaque JSON
//! via `kubectl apply --validate=false`; we never CLAIM validation, so
//! there is no false-accept of a malformed object). Full JSON Schema Draft 7
//! validation requires a schema-compiler crate; future R22b lands it as an
//! optional cargo feature so the substrate stays light.
//!
//! It carries its own read-only [`ValidationRegistry`] snapshot rather than
//! re-introducing a parallel routing surface — routing now lives ONLY in the
//! apiserver's `RouterState` (driven by `CrdController` via
//! [`crate::DynamicHandlerSink`]). When validation is wired, the source of
//! the registered-kinds snapshot will be the controller's bookkeeping (or a
//! read view of the router table), NOT a second authoritative registry.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;

use async_trait::async_trait;

use crate::admission::{
    AdmissionAction, AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};
use crate::crd::{CrdEntry, CrdScope};

/// Read-only snapshot of registered CRD entries the validation webhook
/// consults. Keyed by the canonical `{group}/{version}/{kind}` string.
///
/// Deliberately NOT a routing surface — it is a validation-only view. Built
/// once from a `Vec<CrdEntry>` (e.g. the `CrdController`'s bookkeeping when
/// validation is wired); not mutated concurrently with reads, so a plain
/// `BTreeMap` (no lock) suffices.
#[derive(Clone, Default, Debug)]
pub struct ValidationRegistry {
    by_key: BTreeMap<String, CrdEntry>,
}

impl ValidationRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from a snapshot of entries.
    #[must_use]
    pub fn from_entries(entries: Vec<CrdEntry>) -> Self {
        let mut by_key = BTreeMap::new();
        for e in entries {
            by_key.insert(Self::key_of(&e), e);
        }
        Self { by_key }
    }

    /// Canonical `{group-or-v1}/{version}/{kind}` key.
    fn key_of(e: &CrdEntry) -> String {
        let g = if e.group.is_empty() {
            "v1"
        } else {
            e.group.as_str()
        };
        let mut s = String::with_capacity(g.len() + e.version.len() + e.kind.len() + 2);
        s.push_str(g);
        s.push('/');
        s.push_str(&e.version);
        s.push('/');
        s.push_str(&e.kind);
        s
    }

    /// Look up an entry by canonical key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&CrdEntry> {
        self.by_key.get(key)
    }
}

/// Core kinds the apiserver knows without an explicit CRD. Mirrors
/// engenho-types's static catalog. Any kind not in this set must be
/// registered in the [`ValidationRegistry`] before admission.
fn core_kinds() -> BTreeSet<&'static str> {
    [
        "Pod",
        "Service",
        "Endpoints",
        "ConfigMap",
        "Secret",
        "Namespace",
        "Node",
        "PersistentVolume",
        "PersistentVolumeClaim",
        "ReplicaSet",
        "Deployment",
        "StatefulSet",
        "DaemonSet",
        "Job",
        "CronJob",
        "Ingress",
        "NetworkPolicy",
        "HorizontalPodAutoscaler",
        "PodDisruptionBudget",
        "ServiceAccount",
        "Role",
        "RoleBinding",
        "ClusterRole",
        "ClusterRoleBinding",
        "CustomResourceDefinition",
    ]
    .into_iter()
    .collect()
}

/// Webhook that gates writes on kind registration + scope match.
pub struct CrdValidationWebhook {
    registry: ValidationRegistry,
    core: BTreeSet<&'static str>,
}

impl CrdValidationWebhook {
    /// New webhook backed by `registry`.
    #[must_use]
    pub fn new(registry: ValidationRegistry) -> Self {
        Self {
            registry,
            core: core_kinds(),
        }
    }

    /// True if `kind` is a substrate-known core kind.
    pub fn is_core_kind(&self, kind: &str) -> bool {
        self.core.contains(kind)
    }

    /// Validate required fields exist on the value (shallow: only top-level
    /// keys named in `schema.required` are checked). Returns the first
    /// missing-required field name, or None.
    #[must_use]
    pub fn first_missing_required<'a>(schema: &'a Value, value: &Value) -> Option<&'a str> {
        let required = schema.get("required")?.as_array()?;
        for r in required {
            let Some(name) = r.as_str() else { continue };
            if value.get(name).is_none() {
                return Some(name);
            }
        }
        None
    }
}

#[async_trait]
impl AdmissionWebhook for CrdValidationWebhook {
    fn name(&self) -> &'static str {
        "crd-validation"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        // Delete is allowed — we never block teardown for missing CRDs.
        if request.action == AdmissionAction::Delete {
            return Ok(AdmissionDecision::Allow);
        }
        // Core kinds bypass CRD lookup.
        if self.is_core_kind(&request.key.kind) {
            return Ok(AdmissionDecision::Allow);
        }
        // Compose the canonical registry key.
        let g = if request.key.group.is_empty() {
            "v1"
        } else {
            request.key.group.as_str()
        };
        let registry_key = format!("{g}/{}/{}", request.key.version, request.key.kind);
        let entry = match self.registry.get(&registry_key) {
            Some(e) => e,
            None => {
                return Ok(AdmissionDecision::Deny(format!(
                    "kind {registry_key} is not registered"
                )));
            }
        };
        // Scope check.
        match (entry.scope, request.key.namespace.as_deref()) {
            (CrdScope::Cluster, Some(ns)) if !ns.is_empty() => {
                return Ok(AdmissionDecision::Deny(format!(
                    "kind {registry_key} is cluster-scoped; rejecting namespace={ns}"
                )));
            }
            (CrdScope::Namespaced, None) => {
                return Ok(AdmissionDecision::Deny(format!(
                    "kind {registry_key} is namespaced; namespace required"
                )));
            }
            _ => {}
        }
        // Shallow required-fields check (Put only — Patch can omit required
        // fields as long as the merged result has them).
        if request.action == AdmissionAction::Put {
            if let Some(value) = &request.value {
                if let Some(missing) = Self::first_missing_required(&entry.schema, value) {
                    return Ok(AdmissionDecision::Deny(format!(
                        "required field {missing} missing on {registry_key}"
                    )));
                }
            }
        }
        Ok(AdmissionDecision::Allow)
    }
}

/// Convenience constructor.
#[must_use]
pub fn crd_validation_webhook(registry: ValidationRegistry) -> Arc<dyn AdmissionWebhook> {
    Arc::new(CrdValidationWebhook::new(registry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::resource::ResourceKey;
    use serde_json::json;

    fn ns_request(
        action: AdmissionAction,
        kind: &str,
        ns: Option<&str>,
        value: Value,
    ) -> AdmissionRequest {
        let key = match ns {
            Some(n) => ResourceKey::namespaced("engenho.io", "v1", kind, n, "x"),
            None => ResourceKey::cluster_scoped("engenho.io", "v1", kind, "x"),
        };
        AdmissionRequest {
            action,
            key,
            value: Some(value),
            current: None,
        }
    }

    /// Build a `CrdEntry` for `kind` with the given scope + schema.
    fn entry(kind: &str, scope: CrdScope, schema: Value) -> CrdEntry {
        CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: kind.into(),
            list_kind: format!("{kind}List"),
            plural: format!("{}s", kind.to_lowercase()),
            singular: kind.to_lowercase(),
            short_names: Vec::new(),
            categories: Vec::new(),
            scope,
            schema,
        }
    }

    fn webhook_with(entries: Vec<CrdEntry>) -> CrdValidationWebhook {
        CrdValidationWebhook::new(ValidationRegistry::from_entries(entries))
    }

    #[tokio::test]
    async fn delete_always_allowed() {
        let w = webhook_with(vec![]);
        let req = ns_request(
            AdmissionAction::Delete,
            "Unknown",
            Some("default"),
            json!({}),
        );
        assert_eq!(w.review(&req).await.unwrap(), AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn core_kind_bypasses_registry() {
        let w = webhook_with(vec![]);
        let req = AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
            value: Some(json!({})),
            current: None,
        };
        assert_eq!(w.review(&req).await.unwrap(), AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn unregistered_custom_kind_is_denied() {
        let w = webhook_with(vec![]);
        let req = ns_request(AdmissionAction::Put, "Widget", Some("default"), json!({}));
        let r = w.review(&req).await.unwrap();
        match r {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("Widget"));
                assert!(reason.contains("not registered"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn registered_namespaced_kind_allowed() {
        let w = webhook_with(vec![entry("Widget", CrdScope::Namespaced, json!({}))]);
        let req = ns_request(
            AdmissionAction::Put,
            "Widget",
            Some("default"),
            json!({"spec": {}}),
        );
        assert_eq!(w.review(&req).await.unwrap(), AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn cluster_scoped_kind_with_namespace_denied() {
        let w = webhook_with(vec![entry("ClusterWidget", CrdScope::Cluster, json!({}))]);
        let req = ns_request(
            AdmissionAction::Put,
            "ClusterWidget",
            Some("default"),
            json!({}),
        );
        let r = w.review(&req).await.unwrap();
        assert!(matches!(r, AdmissionDecision::Deny(reason) if reason.contains("cluster-scoped")));
    }

    #[tokio::test]
    async fn namespaced_kind_without_namespace_denied() {
        let w = webhook_with(vec![entry("Widget", CrdScope::Namespaced, json!({}))]);
        let req = ns_request(AdmissionAction::Put, "Widget", None, json!({}));
        let r = w.review(&req).await.unwrap();
        assert!(
            matches!(r, AdmissionDecision::Deny(reason) if reason.contains("namespace required"))
        );
    }

    #[tokio::test]
    async fn missing_required_field_denied() {
        let w = webhook_with(vec![entry(
            "Widget",
            CrdScope::Namespaced,
            json!({"required": ["spec"]}),
        )]);
        let req = ns_request(
            AdmissionAction::Put,
            "Widget",
            Some("default"),
            json!({"metadata": {"name": "x"}}), // spec missing
        );
        let r = w.review(&req).await.unwrap();
        assert!(matches!(r, AdmissionDecision::Deny(reason) if reason.contains("spec")));
    }

    #[tokio::test]
    async fn required_field_present_allowed() {
        let w = webhook_with(vec![entry(
            "Widget",
            CrdScope::Namespaced,
            json!({"required": ["spec"]}),
        )]);
        let req = ns_request(
            AdmissionAction::Put,
            "Widget",
            Some("default"),
            json!({"spec": {}}),
        );
        assert_eq!(w.review(&req).await.unwrap(), AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn patch_doesnt_enforce_required_fields() {
        let w = webhook_with(vec![entry(
            "Widget",
            CrdScope::Namespaced,
            json!({"required": ["spec"]}),
        )]);
        let req = ns_request(
            AdmissionAction::Patch,
            "Widget",
            Some("default"),
            json!({"metadata": {"labels": {"app": "x"}}}), // partial update; no spec
        );
        assert_eq!(w.review(&req).await.unwrap(), AdmissionDecision::Allow);
    }

    #[test]
    fn first_missing_required_finds_first() {
        let schema = json!({"required": ["a", "b", "c"]});
        let value = json!({"a": 1, "c": 3});
        assert_eq!(
            CrdValidationWebhook::first_missing_required(&schema, &value),
            Some("b")
        );
    }

    #[test]
    fn first_missing_required_returns_none_when_schema_has_no_required() {
        let schema = json!({});
        let value = json!({});
        assert_eq!(
            CrdValidationWebhook::first_missing_required(&schema, &value),
            None
        );
    }

    #[tokio::test]
    async fn webhook_name_is_stable() {
        let w = webhook_with(vec![]);
        assert_eq!(w.name(), "crd-validation");
    }
}
