//! CRD validation webhook — adapts `CrdRegistry` to the
//! `AdmissionWebhook` trait.
//!
//! Performs typed-shape validation on every Put / Patch:
//!   * The kind must be registered (or be in the core
//!     "always-allowed" set: Pod / Service / Endpoints / etc.)
//!   * Scope match — Namespaced kinds must have a namespace;
//!     Cluster-scoped kinds must NOT.
//!   * Required fields exist (shallow check via the entry's
//!     schema.required, when present).
//!
//! NOT in this commit: full JSON Schema Draft 7 validation. That
//! requires a schema-compiler crate; future R22b lands it as an
//! optional cargo feature so the substrate stays light.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::admission::{
    AdmissionAction, AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};
use crate::crd::{CrdRegistry, CrdScope};

/// Core kinds the apiserver knows without an explicit CRD. Mirrors
/// engenho-types's static catalog. Any kind not in this set must
/// be registered in the [`CrdRegistry`] before admission.
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
    registry: CrdRegistry,
    core: BTreeSet<&'static str>,
}

impl CrdValidationWebhook {
    /// New webhook backed by `registry`.
    #[must_use]
    pub fn new(registry: CrdRegistry) -> Self {
        Self {
            registry,
            core: core_kinds(),
        }
    }

    /// True if `kind` is a substrate-known core kind.
    pub fn is_core_kind(&self, kind: &str) -> bool {
        self.core.contains(kind)
    }

    /// Validate required fields exist on the value (shallow: only
    /// top-level keys named in `schema.required` are checked).
    /// Returns the first missing-required field name, or None.
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
        let entry = match self.registry.get(&registry_key).await {
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
        // Shallow required-fields check (Put only — Patch can omit
        // required fields as long as the merged result has them).
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
pub fn crd_validation_webhook(registry: CrdRegistry) -> Arc<dyn AdmissionWebhook> {
    Arc::new(CrdValidationWebhook::new(registry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::CrdEntry;
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

    async fn webhook_with(entries: Vec<CrdEntry>) -> CrdValidationWebhook {
        let r = CrdRegistry::new();
        for e in entries {
            r.register(e, false).await.unwrap();
        }
        CrdValidationWebhook::new(r)
    }

    #[tokio::test]
    async fn delete_always_allowed() {
        let w = webhook_with(vec![]).await;
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
        let w = webhook_with(vec![]).await;
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
        let w = webhook_with(vec![]).await;
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
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
            list_kind: "WidgetList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({}),
        };
        let w = webhook_with(vec![entry]).await;
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
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "ClusterWidget".into(),
            list_kind: "ClusterWidgetList".into(),
            scope: CrdScope::Cluster,
            schema: json!({}),
        };
        let w = webhook_with(vec![entry]).await;
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
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
            list_kind: "WidgetList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({}),
        };
        let w = webhook_with(vec![entry]).await;
        let req = ns_request(AdmissionAction::Put, "Widget", None, json!({}));
        let r = w.review(&req).await.unwrap();
        assert!(
            matches!(r, AdmissionDecision::Deny(reason) if reason.contains("namespace required"))
        );
    }

    #[tokio::test]
    async fn missing_required_field_denied() {
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
            list_kind: "WidgetList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({"required": ["spec"]}),
        };
        let w = webhook_with(vec![entry]).await;
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
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
            list_kind: "WidgetList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({"required": ["spec"]}),
        };
        let w = webhook_with(vec![entry]).await;
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
        let entry = CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
            list_kind: "WidgetList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({"required": ["spec"]}),
        };
        let w = webhook_with(vec![entry]).await;
        let req = ns_request(
            AdmissionAction::Patch,
            "Widget",
            Some("default"),
            json!({"metadata": {"labels": {"app": "x"}}}), // partial update; no spec
        );
        // Patch is allowed without requiring all top-level fields.
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
        let w = webhook_with(vec![]).await;
        assert_eq!(w.name(), "crd-validation");
    }
}
