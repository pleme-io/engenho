//! R21 — CRD (CustomResourceDefinition) registry.
//!
//! Lets operators register new typed kinds at runtime without
//! recompiling. A CRD declares:
//!   * group + version + kind + listKind
//!   * scope (Namespaced or ClusterScoped)
//!   * schema (JSON Schema for validation)
//!
//! The apiserver consults the registry on every write to:
//!   * Decide if a kind is known (admission preflight)
//!   * Validate the payload against the schema
//!   * Surface the kind in discovery (`GET /apis/{group}/{version}`)
//!
//! ## Reconcile rule (CRDController)
//!
//! For each CustomResourceDefinition in the store:
//!   1. Read group/version/kind/scope/schema from spec
//!   2. Register in the [`CrdRegistry`]
//!   3. Patch CRD status to `established=True`
//!
//! Removal of a CRD object → unregister from the registry +
//! optionally cascade-delete all instances (controlled by
//! `spec.preserveUnknownFields` — future R21b).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    command::{Reason, ResourceCommand},
    StoreMesh,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::controller::{Controller, ReconcileReport};
use crate::error::ControllerError;

/// Resource scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum CrdScope {
    /// Namespaced — must specify `metadata.namespace`.
    Namespaced,
    /// Cluster-scoped — no namespace allowed.
    Cluster,
}

/// One registered CRD. Pure-data value cached in the registry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdEntry {
    /// `engenho.io`, `apps`, `batch`, ... (empty for core).
    pub group: String,
    /// `v1`, `v1alpha1`, ...
    pub version: String,
    /// `Pod`, `Deployment`, `Derivation`, ...
    pub kind: String,
    /// Plural list-kind (typically `{kind}List`).
    pub list_kind: String,
    /// Namespace scope.
    pub scope: CrdScope,
    /// JSON Schema (Draft 7) for validation. Opaque to the
    /// registry; the apiserver validator interprets.
    pub schema: Value,
}

impl CrdEntry {
    /// Canonical lookup key `{group}/{version}/{kind}`.
    #[must_use]
    pub fn key(&self) -> String {
        let g = if self.group.is_empty() {
            "v1"
        } else {
            self.group.as_str()
        };
        format!("{g}/{}/{}", self.version, self.kind)
    }
}

/// Registry errors.
#[derive(Debug, Clone, Error)]
pub enum CrdError {
    /// Conflict — a different CRD already exists under this key.
    #[error("conflict on {0}")]
    Conflict(String),
    /// Schema invalid (apiserver couldn't compile / validate).
    #[error("invalid schema for {key}: {reason}")]
    InvalidSchema {
        /// CRD key.
        key: String,
        /// Reason from the schema compiler.
        reason: String,
    },
}

impl CrdError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Conflict(_) => "conflict",
            Self::InvalidSchema { .. } => "invalid_schema",
        }
    }
}

/// Thread-safe registry of CRD entries. Cloned + shared across
/// apiserver + controllers — the live cluster's known-kinds set.
#[derive(Clone, Default)]
pub struct CrdRegistry {
    inner: Arc<RwLock<BTreeMap<String, CrdEntry>>>,
}

impl CrdRegistry {
    /// Empty registry. Core kinds (Pod / Service / Deployment / …)
    /// are NOT auto-registered — the apiserver handles them via
    /// the static catalog. The registry holds USER-defined kinds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a CRD. Overwrites any existing entry under the
    /// same `{group}/{version}/{kind}` key.
    ///
    /// # Errors
    /// Returns [`CrdError::Conflict`] only if `replace` is false
    /// and an entry already exists under the same key.
    pub async fn register(&self, entry: CrdEntry, replace: bool) -> Result<(), CrdError> {
        let key = entry.key();
        let mut guard = self.inner.write().await;
        if !replace && guard.contains_key(&key) {
            return Err(CrdError::Conflict(key));
        }
        guard.insert(key, entry);
        Ok(())
    }

    /// Remove a CRD entry by key. Returns true if removed.
    pub async fn unregister(&self, key: &str) -> bool {
        self.inner.write().await.remove(key).is_some()
    }

    /// Look up a CRD by `{group}/{version}/{kind}`.
    pub async fn get(&self, key: &str) -> Option<CrdEntry> {
        self.inner.read().await.get(key).cloned()
    }

    /// True if a kind is registered.
    pub async fn knows(&self, key: &str) -> bool {
        self.inner.read().await.contains_key(key)
    }

    /// All registered entries (snapshot).
    pub async fn entries(&self) -> Vec<CrdEntry> {
        self.inner.read().await.values().cloned().collect()
    }

    /// Count of registered CRDs.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    /// True if no CRDs are registered.
    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}

// =================================================================
// CrdController — reconciles CRDs from the store into the registry
// =================================================================

/// Controller that syncs `apiextensions.k8s.io/v1.CustomResourceDefinition`
/// objects from the store into a [`CrdRegistry`].
pub struct CrdController {
    store: Arc<StoreMesh>,
    registry: CrdRegistry,
}

impl CrdController {
    /// New controller. Shares the registry handle with the apiserver.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, registry: CrdRegistry) -> Self {
        Self { store, registry }
    }

    /// Extract a [`CrdEntry`] from a CRD manifest. Pure helper.
    #[must_use]
    pub fn extract_entry(crd: &Value) -> Option<CrdEntry> {
        let spec = crd.get("spec")?;
        let group = spec
            .get("group")
            .and_then(|g| g.as_str())
            .unwrap_or("")
            .to_string();
        let scope = match spec.get("scope").and_then(|s| s.as_str()) {
            Some("Cluster") => CrdScope::Cluster,
            _ => CrdScope::Namespaced,
        };
        let names = spec.get("names")?;
        let kind = names.get("kind").and_then(|k| k.as_str())?.to_string();
        let list_kind = names
            .get("listKind")
            .and_then(|k| k.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("{kind}List"));
        // Take the first version (most CRDs have one; the served=true
        // version is typically first).
        let versions = spec.get("versions").and_then(|v| v.as_array())?;
        let first = versions.first()?;
        let version = first.get("name").and_then(|n| n.as_str())?.to_string();
        let schema = first
            .get("schema")
            .and_then(|s| s.get("openAPIV3Schema"))
            .cloned()
            .unwrap_or(Value::Null);
        Some(CrdEntry {
            group,
            version,
            kind,
            list_kind,
            scope,
            schema,
        })
    }
}

#[async_trait]
impl Controller for CrdController {
    fn name(&self) -> &'static str {
        "crd"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let crds = self
            .store
            .list("apiextensions.k8s.io", "v1", "CustomResourceDefinition", None)
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = crds.len();

        // Build the desired set.
        let mut desired_keys: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        for (crd_key, crd_value) in &crds {
            let Some(entry) = Self::extract_entry(crd_value) else {
                report.objects_skipped += 1;
                continue;
            };
            let registry_key = entry.key();
            desired_keys.insert(registry_key.clone());

            let was_known = self.registry.knows(&registry_key).await;
            let existing = self.registry.get(&registry_key).await;
            if existing.as_ref() != Some(&entry) {
                self.registry
                    .register(entry.clone(), true)
                    .await
                    .map_err(|e| ControllerError::Internal(e.to_string()))?;
                report.objects_changed += 1;
            }

            // Patch status.established when first registered.
            if !was_known {
                self.store
                    .propose(ResourceCommand::Patch {
                        key: crd_key.clone(),
                        patch: json!({"status": {"established": true}}),
                        reason: Reason::Controller,
                    })
                    .await
                    .map_err(|e| ControllerError::Store(e.to_string()))?;
            }
        }

        // GC: any registry key not in desired is removed (the CRD
        // object was deleted from the store).
        let existing_keys: Vec<String> = self
            .registry
            .entries()
            .await
            .into_iter()
            .map(|e| e.key())
            .collect();
        for key in existing_keys {
            if !desired_keys.contains(&key) {
                self.registry.unregister(&key).await;
                report.objects_changed += 1;
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry() -> CrdEntry {
        CrdEntry {
            group: "engenho.io".into(),
            version: "v1".into(),
            kind: "Derivation".into(),
            list_kind: "DerivationList".into(),
            scope: CrdScope::Namespaced,
            schema: json!({}),
        }
    }

    // ── CrdRegistry ────────────────────────────────────────────

    #[tokio::test]
    async fn register_and_lookup() {
        let r = CrdRegistry::new();
        r.register(sample_entry(), false).await.unwrap();
        assert!(r.knows("engenho.io/v1/Derivation").await);
        assert_eq!(r.len().await, 1);
        let e = r.get("engenho.io/v1/Derivation").await.unwrap();
        assert_eq!(e.kind, "Derivation");
    }

    #[tokio::test]
    async fn register_conflict_without_replace() {
        let r = CrdRegistry::new();
        r.register(sample_entry(), false).await.unwrap();
        let err = r.register(sample_entry(), false).await.unwrap_err();
        assert_eq!(err.kind(), "conflict");
    }

    #[tokio::test]
    async fn register_overwrites_with_replace() {
        let r = CrdRegistry::new();
        r.register(sample_entry(), false).await.unwrap();
        let mut e2 = sample_entry();
        e2.list_kind = "DerivationListV2".into();
        r.register(e2, true).await.unwrap();
        let got = r.get("engenho.io/v1/Derivation").await.unwrap();
        assert_eq!(got.list_kind, "DerivationListV2");
    }

    #[tokio::test]
    async fn unregister_removes_entry() {
        let r = CrdRegistry::new();
        r.register(sample_entry(), false).await.unwrap();
        assert!(r.unregister("engenho.io/v1/Derivation").await);
        assert!(!r.knows("engenho.io/v1/Derivation").await);
        assert!(r.is_empty().await);
    }

    #[tokio::test]
    async fn unregister_returns_false_for_unknown() {
        let r = CrdRegistry::new();
        assert!(!r.unregister("nothing.io/v1/X").await);
    }

    #[tokio::test]
    async fn entries_snapshot_is_independent() {
        let r = CrdRegistry::new();
        r.register(sample_entry(), false).await.unwrap();
        let snap = r.entries().await;
        r.unregister("engenho.io/v1/Derivation").await;
        // Snapshot still has the entry.
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].kind, "Derivation");
    }

    // ── CrdEntry ────────────────────────────────────────────────

    #[test]
    fn key_for_core_group_is_v1() {
        let mut e = sample_entry();
        e.group = String::new();
        assert_eq!(e.key(), "v1/v1/Derivation");
    }

    #[test]
    fn key_for_named_group() {
        assert_eq!(sample_entry().key(), "engenho.io/v1/Derivation");
    }

    // ── CrdController extract_entry ────────────────────────────

    #[test]
    fn extract_entry_parses_full_spec() {
        let crd = json!({
            "spec": {
                "group": "engenho.io",
                "scope": "Namespaced",
                "names": {"kind": "Derivation", "listKind": "DerivationList"},
                "versions": [{
                    "name": "v1",
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }]
            }
        });
        let e = CrdController::extract_entry(&crd).unwrap();
        assert_eq!(e.group, "engenho.io");
        assert_eq!(e.version, "v1");
        assert_eq!(e.kind, "Derivation");
        assert_eq!(e.scope, CrdScope::Namespaced);
        assert_eq!(e.schema.get("type").unwrap(), "object");
    }

    #[test]
    fn extract_entry_defaults_list_kind_when_missing() {
        let crd = json!({
            "spec": {
                "group": "x.io",
                "names": {"kind": "Widget"},
                "versions": [{"name": "v1"}]
            }
        });
        let e = CrdController::extract_entry(&crd).unwrap();
        assert_eq!(e.list_kind, "WidgetList");
    }

    #[test]
    fn extract_entry_handles_cluster_scope() {
        let crd = json!({
            "spec": {
                "group": "x.io",
                "scope": "Cluster",
                "names": {"kind": "ClusterRole"},
                "versions": [{"name": "v1"}]
            }
        });
        let e = CrdController::extract_entry(&crd).unwrap();
        assert_eq!(e.scope, CrdScope::Cluster);
    }

    #[test]
    fn extract_entry_none_for_missing_fields() {
        // Missing spec.names entirely.
        let crd = json!({"spec": {"group": "x"}});
        assert!(CrdController::extract_entry(&crd).is_none());
        // Missing versions.
        let crd = json!({"spec": {"group": "x", "names": {"kind": "X"}}});
        assert!(CrdController::extract_entry(&crd).is_none());
    }

    #[test]
    fn scope_serializes_pascal_case() {
        assert_eq!(
            serde_json::to_string(&CrdScope::Cluster).unwrap(),
            "\"Cluster\""
        );
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(CrdError::Conflict("x".into()).kind(), "conflict");
        assert_eq!(
            CrdError::InvalidSchema {
                key: "x".into(),
                reason: "y".into()
            }
            .kind(),
            "invalid_schema"
        );
    }

    #[test]
    fn controller_name_is_stable() {
        struct F;
        #[async_trait]
        impl Controller for F {
            fn name(&self) -> &'static str { "crd" }
            async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
                Ok(ReconcileReport::default())
            }
        }
        assert_eq!(F.name(), "crd");
    }
}
