//! R21 — CRD (CustomResourceDefinition) serving.
//!
//! Lets operators register new typed kinds at runtime without recompiling.
//! A CRD declares:
//!   * `spec.group`
//!   * `spec.versions[]` — each `{name, served, storage, schema.openAPIV3Schema}`
//!   * `spec.scope` (`Namespaced` | `Cluster`)
//!   * `spec.names` — `{plural, singular, kind, listKind, shortNames[]}`
//!
//! ## How it gets served (REUSE, not fork)
//!
//! A CRD object is stored opaquely via the SAME `StoreBackedHandler` as any
//! other kind (the `apiextensions.k8s.io/v1.CustomResourceDefinition`
//! catalog row builds it automatically). The [`CrdController`] then reacts
//! to CRD writes by registering a `StoreBackedHandler` PER SERVED VERSION
//! into the apiserver's live `RouterState` (via the [`DynamicHandlerSink`]
//! trait the apiserver implements) — so every CR instance verb (CRUD / list
//! / watch) flows through the existing catch-all + `do_*` bodies, and the
//! group + resource auto-appear in discovery (both fold the same live
//! handler snapshot). There is NO parallel CR codepath.
//!
//! ## Reconcile rule (CrdController)
//!
//! For each CustomResourceDefinition in the store:
//!   1. Parse `spec` through the typed [`CrdSpec`] serde border.
//!   2. For EACH served version, `sink.register_crd(...)` under
//!      `(group, version, plural)` + scope + names + shortNames.
//!   3. Patch CRD `status` to `Established=True` / `NamesAccepted=True`.
//!
//! Removal of a CRD object → `sink.unregister_crd(...)` for every version
//! the controller had registered (precise GC via the private
//! [`CrdEntry`]-keyed bookkeeping set). Orphaned CR objects remain in the
//! store unreachable (no cascade-delete — finalizers are a future brick).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
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

impl CrdScope {
    /// `true` for `Namespaced`. The router/handler scope flag is
    /// `namespaced: bool`, so this is the bridge.
    #[must_use]
    pub fn is_namespaced(self) -> bool {
        matches!(self, CrdScope::Namespaced)
    }
}

// ── typed serde border for the CRD spec (TYPED EMISSION) ────────────────
//
// The CRD OBJECT is stored opaquely (StoreBackedHandler is opaque-JSON);
// this typed border is purely how the controller PARSES the few `spec.*`
// paths it acts on — through types, not ad-hoc `.get()` chains. Unknown
// fields are tolerated (CRDs carry far more than we read) so we never
// reject a valid upstream CRD.

/// `spec.names` — how kubectl names the custom resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdNames {
    /// Plural URL segment (`widgets`).
    pub plural: String,
    /// Singular name (`widget`) — discovery `singularName`. Defaults to the
    /// lowercased kind when absent.
    #[serde(default)]
    pub singular: String,
    /// Kind (`Widget`).
    pub kind: String,
    /// List kind (`WidgetList`) — defaults to `{kind}List` when absent.
    #[serde(rename = "listKind", default)]
    pub list_kind: String,
    /// kubectl short-name aliases (`["wd"]`) — discovery `shortNames`.
    #[serde(rename = "shortNames", default)]
    pub short_names: Vec<String>,
    /// kubectl categories (`["all"]`).
    #[serde(default)]
    pub categories: Vec<String>,
}

/// One entry in `spec.versions[]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdVersion {
    /// Version name (`v1`, `v1beta1`).
    pub name: String,
    /// Whether this version is served at the API surface. Only served
    /// versions get a registered handler.
    #[serde(default)]
    pub served: bool,
    /// Whether this version is the storage version. Recorded but not
    /// load-bearing until conversion lands (each served version reads/writes
    /// the same stored object at its own GVK).
    #[serde(default)]
    pub storage: bool,
    /// The OpenAPI v3 schema (`schema.openAPIV3Schema`) — captured opaque;
    /// structural validation against it is DEFERRED (CR creates are accepted
    /// as-is via `--validate=false`).
    #[serde(default)]
    pub schema: Value,
}

/// The typed `spec` of a CustomResourceDefinition — the parse border the
/// controller drives. Unknown fields tolerated (no `deny_unknown_fields`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdSpec {
    /// API group (`example.com`).
    #[serde(default)]
    pub group: String,
    /// Resource scope. Defaults to Namespaced (kube-apiserver default).
    #[serde(default = "default_scope")]
    pub scope: CrdScope,
    /// Names block.
    pub names: CrdNames,
    /// All declared versions. The controller registers a handler PER served
    /// version (widened beyond the old `versions.first()`).
    #[serde(default)]
    pub versions: Vec<CrdVersion>,
}

fn default_scope() -> CrdScope {
    CrdScope::Namespaced
}

/// One registered CRD-version handler descriptor — the controller's PRIVATE
/// bookkeeping of "what I registered" (keyed by `(group, version, plural)`)
/// so unregister-on-delete is precise. This REPLACES the old `CrdRegistry`
/// routing surface; routing now lives in the apiserver's `RouterState` via
/// the [`DynamicHandlerSink`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdEntry {
    /// `example.com` (empty for core, never for a real CRD).
    pub group: String,
    /// The served version (`v1`).
    pub version: String,
    /// `Widget`.
    pub kind: String,
    /// Plural list-kind (`WidgetList`).
    pub list_kind: String,
    /// Plural URL segment (`widgets`).
    pub plural: String,
    /// Singular (`widget`).
    pub singular: String,
    /// kubectl short-name aliases (`["wd"]`).
    pub short_names: Vec<String>,
    /// kubectl categories (`["all"]`).
    pub categories: Vec<String>,
    /// Namespace scope.
    pub scope: CrdScope,
    /// The opaque openAPIV3Schema for this version (validation DEFERRED).
    pub schema: Value,
}

impl CrdEntry {
    /// Canonical routing key `(group, version, plural)` — the SAME triple
    /// the `RouterState` handler map is keyed on.
    #[must_use]
    pub fn route_key(&self) -> (String, String, String) {
        (self.group.clone(), self.version.clone(), self.plural.clone())
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

engenho_substrate::impl_error_kind! {
    CrdError {
        (Conflict(_)) => "conflict",
        { InvalidSchema { .. } } => "invalid_schema",
    }
}

// =================================================================
// DynamicHandlerSink — the apiserver-provided registration surface
// =================================================================

/// Typed sink the apiserver implements so the controller (which must NOT
/// depend on engenho-apiserver — that would be a dependency cycle, since
/// apiserver already depends on controllers for admission) can mutate the
/// live `RouterState` handler table.
///
/// The apiserver impl builds a `StoreBackedHandler::new(store, g, v, kind,
/// plural, namespaced).with_registration_metadata(short_names, singular,
/// categories).with_admission(chain)` and calls `RouterState::register` /
/// `::unregister`. Threading `short_names` + `singular` from `spec.names`
/// into `with_registration_metadata` is the ONE load-bearing wiring detail
/// that makes `kubectl get wd` + `kubectl api-resources` resolve the CR.
pub trait DynamicHandlerSink: Send + Sync {
    /// Register a CR-instance handler under `(group, version, plural)` with
    /// the given scope + discovery metadata. Idempotent (a re-register is a
    /// harmless overwrite of the same GVK).
    fn register_crd(&self, spec: CrdHandlerSpec);

    /// Unregister the CR-instance handler keyed by `(group, version,
    /// plural)`. Returns `true` iff one was present + removed.
    fn unregister_crd(&self, group: &str, version: &str, plural: &str) -> bool;
}

/// The typed argument bundle for [`DynamicHandlerSink::register_crd`] — one
/// served CRD version's full handler shape, carried as owned `String`s
/// (the controller derives them from the typed `CrdSpec`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrdHandlerSpec {
    /// API group (`example.com`).
    pub group: String,
    /// Served version (`v1`).
    pub version: String,
    /// Kind (`Widget`).
    pub kind: String,
    /// Plural (`widgets`).
    pub plural: String,
    /// Singular (`widget`).
    pub singular: String,
    /// Short-name aliases (`["wd"]`).
    pub short_names: Vec<String>,
    /// Categories (`["all"]`).
    pub categories: Vec<String>,
    /// `true` ⇒ namespaced.
    pub namespaced: bool,
}

impl From<&CrdEntry> for CrdHandlerSpec {
    fn from(e: &CrdEntry) -> Self {
        Self {
            group: e.group.clone(),
            version: e.version.clone(),
            kind: e.kind.clone(),
            plural: e.plural.clone(),
            singular: e.singular.clone(),
            short_names: e.short_names.clone(),
            categories: e.categories.clone(),
            namespaced: e.scope.is_namespaced(),
        }
    }
}

// =================================================================
// CrdController — reconciles CRDs from the store into the router table
// =================================================================

/// Controller that syncs `apiextensions.k8s.io/v1.CustomResourceDefinition`
/// objects from the store into the apiserver's live handler table via a
/// [`DynamicHandlerSink`]. Driven by a `WatchDriver` filtered to
/// `["CustomResourceDefinition"]`.
pub struct CrdController {
    store: Arc<StoreMesh>,
    sink: Arc<dyn DynamicHandlerSink>,
    /// What this controller has registered, keyed by `(group, version,
    /// plural)` → entry. PRIVATE bookkeeping (the old `CrdRegistry` collapses
    /// into this), so unregister-on-delete is precise. Behind a `Mutex`
    /// because `tick()` takes `&self` (the `Controller` trait), but the set
    /// is mutated across ticks.
    registered: std::sync::Mutex<BTreeMap<(String, String, String), CrdEntry>>,
}

impl CrdController {
    /// New controller. `sink` is the apiserver-provided handle that mutates
    /// the live `RouterState`.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, sink: Arc<dyn DynamicHandlerSink>) -> Self {
        Self {
            store,
            sink,
            registered: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// Parse a CRD manifest into the typed [`CrdSpec`]. `None` if `spec` is
    /// missing or unparseable (a malformed CRD is skipped, never panics).
    #[must_use]
    pub fn parse_spec(crd: &Value) -> Option<CrdSpec> {
        let spec = crd.get("spec")?;
        serde_json::from_value::<CrdSpec>(spec.clone()).ok()
    }

    /// Extract one [`CrdEntry`] PER SERVED version from a CRD manifest. This
    /// WIDENS the old `extract_entry` (which only read `versions.first()`):
    /// a multi-served-version CRD now yields one entry per served version,
    /// each registered under its own GVK. Returns an empty vec for a CRD
    /// with no served versions (or an unparseable spec).
    ///
    /// `plural` / `singular` / `kind` / `shortNames` / `scope` / `listKind`
    /// all flow from the typed `spec.names` + `spec.scope`.
    #[must_use]
    pub fn extract_entries(crd: &Value) -> Vec<CrdEntry> {
        let Some(spec) = Self::parse_spec(crd) else {
            return Vec::new();
        };
        let kind = &spec.names.kind;
        let singular = if spec.names.singular.is_empty() {
            kind.to_lowercase()
        } else {
            spec.names.singular.clone()
        };
        let list_kind = if spec.names.list_kind.is_empty() {
            let mut lk = kind.clone();
            lk.push_str("List");
            lk
        } else {
            spec.names.list_kind.clone()
        };
        spec.versions
            .iter()
            .filter(|v| v.served)
            .map(|v| CrdEntry {
                group: spec.group.clone(),
                version: v.name.clone(),
                kind: kind.clone(),
                list_kind: list_kind.clone(),
                plural: spec.names.plural.clone(),
                singular: singular.clone(),
                short_names: spec.names.short_names.clone(),
                categories: spec.names.categories.clone(),
                scope: spec.scope,
                schema: v.schema.clone(),
            })
            .collect()
    }

    /// Snapshot of the controller's currently-registered entries (test +
    /// introspection). Order is by `(group, version, plural)`.
    #[must_use]
    pub fn registered_entries(&self) -> Vec<CrdEntry> {
        self.registered
            .lock()
            .expect("registered mutex not poisoned")
            .values()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Controller for CrdController {
    fn name(&self) -> &'static str {
        "crd"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let crds = self
            .store
            .list(
                "apiextensions.k8s.io",
                "v1",
                "CustomResourceDefinition",
                None,
            )
            .await;
        let mut report = ReconcileReport::default();
        report.objects_examined = crds.len();

        // Build the desired set: for every CRD, one entry per served
        // version keyed by (group, version, plural).
        let mut desired: BTreeMap<(String, String, String), CrdEntry> = BTreeMap::new();
        // Track which CRD object (by store ResourceKey) owns each route key,
        // so we patch status on the right object exactly once per CRD when
        // first established.
        let mut crd_for_key: BTreeMap<(String, String, String), engenho_store::ResourceKey> =
            BTreeMap::new();

        for (crd_key, crd_value) in &crds {
            let entries = Self::extract_entries(crd_value);
            if entries.is_empty() {
                report.objects_skipped += 1;
                continue;
            }
            for entry in entries {
                let key = entry.route_key();
                crd_for_key
                    .entry(key.clone())
                    .or_insert_with(|| crd_key.clone());
                desired.insert(key, entry);
            }
        }

        // Register any newly-desired (or changed) route key. Idempotent
        // re-registration is a harmless overwrite of the same GVK.
        let mut crds_to_establish: std::collections::BTreeSet<engenho_store::ResourceKey> =
            std::collections::BTreeSet::new();
        {
            let mut reg = self.registered.lock().expect("registered mutex not poisoned");
            for (key, entry) in &desired {
                let needs_register = reg.get(key) != Some(entry);
                if needs_register {
                    self.sink.register_crd(CrdHandlerSpec::from(entry));
                    reg.insert(key.clone(), entry.clone());
                    report.objects_changed += 1;
                    if let Some(crd_key) = crd_for_key.get(key) {
                        crds_to_establish.insert(crd_key.clone());
                    }
                }
            }

            // GC: any previously-registered route key not in the desired set
            // (its CRD object was deleted, or that version stopped being
            // served) → unregister the handler so subsequent CR access is a
            // typed NotFound.
            let stale: Vec<(String, String, String)> = reg
                .keys()
                .filter(|k| !desired.contains_key(*k))
                .cloned()
                .collect();
            for key in stale {
                self.sink.unregister_crd(&key.0, &key.1, &key.2);
                reg.remove(&key);
                report.objects_changed += 1;
            }
        }

        // Patch status Established=True / NamesAccepted=True on each CRD
        // that gained a fresh registration this tick. Done OUTSIDE the
        // registered-set lock (no .await while holding a std Mutex).
        for crd_key in crds_to_establish {
            self.store
                .propose(ResourceCommand::Patch {
                    key: crd_key,
                    patch: json!({
                        "status": {
                            "conditions": [
                                {
                                    "type": "Established",
                                    "status": "True",
                                    "reason": "InitialNamesAccepted",
                                    "message": "the initial names have been accepted"
                                },
                                {
                                    "type": "NamesAccepted",
                                    "status": "True",
                                    "reason": "NoConflicts",
                                    "message": "no conflicts found"
                                }
                            ]
                        }
                    }),
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
        }

        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // ── recording mock sink ──────────────────────────────────────

    #[derive(Default)]
    struct RecordingSink {
        registered: Mutex<Vec<CrdHandlerSpec>>,
        unregistered: Mutex<Vec<(String, String, String)>>,
    }

    impl DynamicHandlerSink for RecordingSink {
        fn register_crd(&self, spec: CrdHandlerSpec) {
            self.registered.lock().unwrap().push(spec);
        }
        fn unregister_crd(&self, group: &str, version: &str, plural: &str) -> bool {
            self.unregistered.lock().unwrap().push((
                group.to_string(),
                version.to_string(),
                plural.to_string(),
            ));
            true
        }
    }

    fn single_version_crd() -> Value {
        json!({
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {
                    "plural": "widgets",
                    "singular": "widget",
                    "kind": "Widget",
                    "listKind": "WidgetList",
                    "shortNames": ["wd"]
                },
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }]
            }
        })
    }

    fn multi_version_crd() -> Value {
        json!({
            "spec": {
                "group": "example.com",
                "scope": "Cluster",
                "names": {
                    "plural": "gizmos",
                    "kind": "Gizmo",
                    "shortNames": ["gz"]
                },
                "versions": [
                    {"name": "v1", "served": true, "storage": true},
                    {"name": "v2", "served": true, "storage": false},
                    {"name": "v1alpha1", "served": false, "storage": false}
                ]
            }
        })
    }

    // ── parse_spec / extract_entries ─────────────────────────────

    #[test]
    fn extract_entries_single_served_version() {
        let entries = CrdController::extract_entries(&single_version_crd());
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.group, "example.com");
        assert_eq!(e.version, "v1");
        assert_eq!(e.kind, "Widget");
        assert_eq!(e.plural, "widgets");
        assert_eq!(e.singular, "widget");
        assert_eq!(e.short_names, vec!["wd".to_string()]);
        assert_eq!(e.scope, CrdScope::Namespaced);
        assert_eq!(e.schema.get("openAPIV3Schema").unwrap().get("type").unwrap(), "object");
    }

    #[test]
    fn extract_entries_iterates_all_served_versions_only() {
        // The widened behavior: a 3-version CRD with 2 served versions
        // yields exactly 2 entries (the served=false one is skipped); the
        // old `versions.first()` would have yielded 1.
        let entries = CrdController::extract_entries(&multi_version_crd());
        assert_eq!(entries.len(), 2, "one entry per SERVED version");
        let versions: Vec<&str> = entries.iter().map(|e| e.version.as_str()).collect();
        assert!(versions.contains(&"v1"));
        assert!(versions.contains(&"v2"));
        assert!(!versions.contains(&"v1alpha1"), "unserved version skipped");
        // Names/scope flow from spec.names + spec.scope onto every entry.
        for e in &entries {
            assert_eq!(e.kind, "Gizmo");
            assert_eq!(e.plural, "gizmos");
            assert_eq!(e.singular, "gizmo", "singular defaults to lowercased kind");
            assert_eq!(e.list_kind, "GizmoList", "listKind defaults to {{kind}}List");
            assert_eq!(e.short_names, vec!["gz".to_string()]);
            assert_eq!(e.scope, CrdScope::Cluster);
        }
    }

    #[test]
    fn extract_entries_empty_for_missing_names() {
        // No spec.names → unparseable → empty (skipped, never panics).
        let crd = json!({"spec": {"group": "x", "versions": [{"name": "v1", "served": true}]}});
        assert!(CrdController::extract_entries(&crd).is_empty());
    }

    #[test]
    fn extract_entries_empty_for_no_served_versions() {
        let crd = json!({
            "spec": {
                "group": "x.io",
                "names": {"plural": "xs", "kind": "X"},
                "versions": [{"name": "v1", "served": false}]
            }
        });
        assert!(CrdController::extract_entries(&crd).is_empty());
    }

    #[test]
    fn crd_handler_spec_from_entry_threads_metadata() {
        let entries = CrdController::extract_entries(&single_version_crd());
        let spec = CrdHandlerSpec::from(&entries[0]);
        assert_eq!(spec.plural, "widgets");
        assert_eq!(spec.singular, "widget");
        assert_eq!(spec.short_names, vec!["wd".to_string()]);
        assert!(spec.namespaced, "Namespaced scope → namespaced=true");
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
            fn name(&self) -> &'static str {
                "crd"
            }
            async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
                Ok(ReconcileReport::default().into())
            }
        }
        assert_eq!(F.name(), "crd");
    }

    // ── tick() with a recording sink + ephemeral store ───────────

    async fn ephemeral_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        use std::time::Duration;
        let router = InProcessRouter::new();
        let cfg = default_config("crd-ctrl-test").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    #[tokio::test]
    async fn tick_registers_one_handler_per_served_version() {
        use engenho_store::{ResourceKey, command::ResourceCommand};
        let store = ephemeral_store().await;
        let sink = Arc::new(RecordingSink::default());
        let ctrl = CrdController::new(store.clone(), sink.clone());

        // Install a multi-served-version CRD object.
        store
            .propose(ResourceCommand::Put {
                key: ResourceKey::cluster_scoped(
                    "apiextensions.k8s.io",
                    "v1",
                    "CustomResourceDefinition",
                    "gizmos.example.com",
                ),
                value: multi_version_crd(),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();

        ctrl.tick().await.unwrap();

        // Two register_crd calls (v1 + v2), neither for the unserved
        // v1alpha1; both cluster-scoped with the gz short name.
        let regs = sink.registered.lock().unwrap();
        assert_eq!(regs.len(), 2);
        for r in regs.iter() {
            assert_eq!(r.plural, "gizmos");
            assert_eq!(r.short_names, vec!["gz".to_string()]);
            assert!(!r.namespaced, "Cluster scope → namespaced=false");
        }
        let versions: Vec<&str> = regs.iter().map(|r| r.version.as_str()).collect();
        assert!(versions.contains(&"v1") && versions.contains(&"v2"));

        // The controller's bookkeeping mirrors the two registrations.
        assert_eq!(ctrl.registered_entries().len(), 2);
    }

    #[tokio::test]
    async fn tick_is_idempotent_no_duplicate_registers() {
        use engenho_store::{ResourceKey, command::ResourceCommand};
        let store = ephemeral_store().await;
        let sink = Arc::new(RecordingSink::default());
        let ctrl = CrdController::new(store.clone(), sink.clone());
        store
            .propose(ResourceCommand::Put {
                key: ResourceKey::cluster_scoped(
                    "apiextensions.k8s.io",
                    "v1",
                    "CustomResourceDefinition",
                    "widgets.example.com",
                ),
                value: single_version_crd(),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();

        ctrl.tick().await.unwrap();
        ctrl.tick().await.unwrap(); // second tick: nothing new.
        assert_eq!(
            sink.registered.lock().unwrap().len(),
            1,
            "re-tick does not re-register the unchanged CRD"
        );
    }

    #[tokio::test]
    async fn tick_unregisters_on_crd_delete() {
        use engenho_store::{ResourceKey, command::ResourceCommand};
        let store = ephemeral_store().await;
        let sink = Arc::new(RecordingSink::default());
        let ctrl = CrdController::new(store.clone(), sink.clone());
        let crd_key = ResourceKey::cluster_scoped(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            "widgets.example.com",
        );
        store
            .propose(ResourceCommand::Put {
                key: crd_key.clone(),
                value: single_version_crd(),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        ctrl.tick().await.unwrap();
        assert_eq!(ctrl.registered_entries().len(), 1);

        // Delete the CRD object, then tick → the handler is unregistered.
        store
            .propose(ResourceCommand::Delete {
                key: crd_key,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        ctrl.tick().await.unwrap();

        let unregs = sink.unregistered.lock().unwrap();
        assert_eq!(unregs.len(), 1);
        assert_eq!(
            unregs[0],
            ("example.com".to_string(), "v1".to_string(), "widgets".to_string())
        );
        assert!(ctrl.registered_entries().is_empty(), "bookkeeping cleared on GC");
    }
}
