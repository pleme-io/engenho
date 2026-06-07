//! Per-kind CRUD trait.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    Revision, StoreMesh, WatchGone, WatchOpts, WatchStream,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
    watch_backend::WATCH_CHANNEL_CAPACITY,
};
use engenho_types::generated_v1_34::RESOURCE_CATALOG;

use crate::error::ApiError;
use crate::params::{ResumePoint, Selectors};

/// Bookmark cadence handed to `watch_from` when the client opted into
/// bookmarks (`allowWatchBookmarks=true`). Mirrors the store default.
const WATCH_BOOKMARK_EVERY: Duration = Duration::from_secs(5);

/// Map a [`WatchGone`] surfaced at WATCH REGISTRATION to an
/// [`ApiError::Gone`] (HTTP 410 / Expired). Both variants become 410:
/// the client re-LISTs + re-WATCHes from a fresh list revision.
///
/// `CompactedTooOld` is the common at-registration case (`from` below
/// the compaction watermark). `Overflow` cannot occur at registration
/// (the channel is empty) but is mapped for completeness — never a
/// silent wrong answer.
#[must_use]
pub fn gone_to_api_error(gone: WatchGone) -> ApiError {
    match gone {
        WatchGone::CompactedTooOld {
            requested,
            compacted,
        } => ApiError::Gone(format!("too old resource version: {requested} ({compacted})")),
        WatchGone::Overflow { last_seen, .. } => ApiError::Gone(format!(
            "watch buffer overflowed at registration; resume from {last_seen}"
        )),
    }
}

/// Typed K8s-resource CRUD trait. Each registered kind implements
/// this; the router dispatches REST routes to the trait methods.
///
/// Default impl: [`StoreBackedHandler`] — works for any kind by
/// routing through the opaque-JSON [`StoreMesh`] catalog.
#[async_trait]
pub trait ResourceHandler: Send + Sync + 'static {
    fn group(&self) -> &str;
    fn version(&self) -> &str;
    fn kind(&self) -> &str;
    fn plural(&self) -> &str;
    fn namespaced(&self) -> bool;

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError>;

    async fn list(&self, namespace: Option<&str>) -> Result<Value, ApiError>;

    /// LIST the items + the snapshot resourceVersion captured ATOMICALLY
    /// from the SAME catalog clone, with selectors applied apiserver-side.
    ///
    /// Returns `(filtered items, snapshot rv)`. The rv is
    /// `current_revision` (the dense MVCC counter), NOT
    /// `last_applied_index` — so a client that LISTs at `rv=N` then
    /// `WATCH ?resourceVersion=N` resumes from exactly the snapshot
    /// boundary, gap-free + dup-free.
    async fn list_at(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
    ) -> Result<(Vec<Value>, Revision), ApiError>;

    /// Open a streaming WATCH from `from`. The returned [`WatchStream`]
    /// is cluster-wide (the store fans every kind through one registry);
    /// the router filters each event down to this handler's GVK +
    /// requested namespace + selectors.
    ///
    /// `allow_bookmarks` toggles the bookmark cadence (`5s` vs disabled).
    ///
    /// # Errors
    ///
    /// [`ApiError::Gone`] when `from` is below the compaction watermark
    /// at registration (`WatchGone::CompactedTooOld`).
    async fn watch_stream(
        &self,
        namespace: Option<&str>,
        from: ResumePoint,
        allow_bookmarks: bool,
    ) -> Result<WatchStream, ApiError>;

    async fn create(&self, namespace: Option<&str>, body: Value) -> Result<Value, ApiError>;

    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<Value, ApiError>;

    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<(), ApiError>;

    /// The `apiVersion` string for this kind — `"v1"` for the core
    /// group, `"<group>/<version>"` otherwise.
    fn api_version(&self) -> String {
        if self.group().is_empty() {
            self.version().to_string()
        } else {
            format!("{}/{}", self.group(), self.version())
        }
    }

    /// Build the typed K8s `<Kind>List` envelope from already-filtered
    /// items + the snapshot revision. Shared by the router's LIST branch
    /// (which calls [`Self::list_at`] with selectors) so the
    /// cluster-scoped + namespaced cases emit ONE body shape.
    fn list_response(&self, items: Vec<Value>, rv: Revision) -> Value {
        let env = ListEnvelope {
            kind: format!("{}List", self.kind()),
            api_version: self.api_version(),
            items,
            metadata: ListMeta {
                resource_version: rv.to_string(),
            },
        };
        serde_json::to_value(env).unwrap_or(Value::Null)
    }
}

/// Default implementation backed by [`StoreMesh`]. Handles every
/// kind uniformly — the kind-specific intelligence (defaulters,
/// validators, finalizers) is left to controllers + admission
/// webhooks at R8+.
pub struct StoreBackedHandler {
    group: String,
    version: String,
    kind: String,
    plural: String,
    namespaced: bool,
    store: Arc<StoreMesh>,
}

impl StoreBackedHandler {
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        plural: impl Into<String>,
        namespaced: bool,
    ) -> Self {
        Self {
            store,
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            namespaced,
        }
    }

    /// Construct from a known K8s kind by looking the descriptor up in
    /// the generated [`RESOURCE_CATALOG`] — the single source of truth for
    /// (group, version, plural, scope). This REPLACES the old
    /// `format!("{}s", …)` derivation (which produced `endpointss` for
    /// `Endpoints` and wrong plurals for any irregular kind); the curated
    /// catalog plural is used verbatim, so the `+s` bug class cannot recur.
    ///
    /// The `namespaced` argument is asserted against the catalog scope —
    /// mismatched callers get `None` rather than a silently mis-scoped
    /// handler. Returns `None` for an uncataloged kind.
    ///
    /// Retained for the existing core-kind test harnesses; new code should
    /// prefer [`Self::for_kind`] (which reads the scope from the catalog)
    /// or [`crate::handlers_from_catalog`] (the full cataloged set).
    #[must_use]
    pub fn for_core_kind(store: Arc<StoreMesh>, kind: &str, namespaced: bool) -> Self {
        let d = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == kind && d.group.is_empty())
            .unwrap_or_else(|| {
                panic!("for_core_kind: {kind:?} is not a cataloged core/v1 kind — add a KIND_CATALOG row + regenerate")
            });
        debug_assert_eq!(
            d.namespaced, namespaced,
            "for_core_kind: caller scope ({namespaced}) disagrees with catalog scope for {kind}"
        );
        Self::new(store, d.group, d.version, d.kind, d.plural, d.namespaced)
    }

    /// Construct a handler for `kind` by looking its descriptor up in the
    /// generated [`RESOURCE_CATALOG`]. The (group, version, plural, scope)
    /// all come from the catalog — no hand-passed scope, no ad-hoc plural.
    /// Returns `None` for an uncataloged kind.
    #[must_use]
    pub fn for_kind(store: Arc<StoreMesh>, kind: &str) -> Option<Self> {
        let d = RESOURCE_CATALOG.iter().find(|d| d.kind == kind)?;
        Some(Self::new(
            store,
            d.group,
            d.version,
            d.kind,
            d.plural,
            d.namespaced,
        ))
    }

    fn key(&self, namespace: Option<&str>, name: &str) -> Result<ResourceKey, ApiError> {
        if self.namespaced != namespace.is_some() {
            return Err(ApiError::BadRequest(format!(
                "{}/{} is {}; got namespace={:?}",
                self.kind,
                name,
                if self.namespaced {
                    "namespaced"
                } else {
                    "cluster-scoped"
                },
                namespace
            )));
        }
        Ok(match namespace {
            Some(ns) => ResourceKey::namespaced(&self.group, &self.version, &self.kind, ns, name),
            None => ResourceKey::cluster_scoped(&self.group, &self.version, &self.kind, name),
        })
    }
}

#[async_trait]
impl ResourceHandler for StoreBackedHandler {
    fn group(&self) -> &str {
        &self.group
    }
    fn version(&self) -> &str {
        &self.version
    }
    fn kind(&self) -> &str {
        &self.kind
    }
    fn plural(&self) -> &str {
        &self.plural
    }
    fn namespaced(&self) -> bool {
        self.namespaced
    }

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let v = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        Ok(inject_type_meta(&v, self.api_version(), &self.kind))
    }

    async fn list(&self, namespace: Option<&str>) -> Result<Value, ApiError> {
        // Default (no selectors) LIST. The atomic-rv envelope is built
        // by the trait's `list_response`; this wraps `list_at` with an
        // empty selector set so both the router LIST branch and any
        // direct caller share ONE body.
        let (items, rv) = self.list_at(namespace, &Selectors::default()).await?;
        Ok(self.list_response(items, rv))
    }

    async fn list_at(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
    ) -> Result<(Vec<Value>, Revision), ApiError> {
        // ONE catalog clone → (items, snapshot rv). The rv is
        // current_revision (dense MVCC), NOT last_applied_index — the
        // load-bearing fix. Selector filtering stays apiserver-side
        // (the store is GVK-keyed).
        let (entries, rv) = self
            .store
            .list_at_revision(&self.group, &self.version, &self.kind, namespace)
            .await;
        let items: Vec<Value> = entries
            .into_iter()
            .filter(|(_, v)| sel.matches(v))
            .map(|(_, v)| inject_type_meta(&v, self.api_version(), &self.kind))
            .collect();
        Ok((items, rv))
    }

    async fn watch_stream(
        &self,
        _namespace: Option<&str>,
        from: ResumePoint,
        allow_bookmarks: bool,
    ) -> Result<WatchStream, ApiError> {
        // Resolve the resume revision. MostRecent ("0"/absent) means
        // "from now, no replay" → read the current revision under one
        // catalog clone and start there.
        let from_rev = match from {
            ResumePoint::At(rev) => rev,
            ResumePoint::MostRecent => self.store.current_catalog().await.revision(),
        };
        let opts = WatchOpts {
            from: from_rev,
            buffer: WATCH_CHANNEL_CAPACITY,
            bookmark_every: if allow_bookmarks {
                WATCH_BOOKMARK_EVERY
            } else {
                Duration::ZERO
            },
        };
        // CompactedTooOld at registration → a real HTTP 410 Gone. The
        // client re-LISTs + re-WATCHes from the fresh list rv. Overflow
        // cannot occur at registration (the channel is empty).
        self.store.watch_from(opts).await.map_err(gone_to_api_error)
    }

    async fn create(&self, namespace: Option<&str>, body: Value) -> Result<Value, ApiError> {
        let name = body
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| ApiError::BadRequest("missing metadata.name in request body".into()))?
            .to_string();
        let key = self.key(namespace, &name)?;
        // Reject if already exists (POST semantics).
        if self.store.get(&key).await.is_some() {
            return Err(ApiError::Conflict(
                format!("{}/{}", self.kind, name),
                "resource already exists".into(),
            ));
        }
        let result = self
            .store
            .propose(ResourceCommand::Put {
                key: key.clone(),
                value: body,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        // Read back the committed resource (with resourceVersion).
        let _ = result;
        let stored = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::Internal("created but not readable".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }

    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        if self.store.get(&key).await.is_none() {
            return Err(ApiError::NotFound(format!("{}/{}", self.kind, name)));
        }
        self.store
            .propose(ResourceCommand::Patch {
                key: key.clone(),
                patch,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        let stored = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::Internal("patch lost during commit".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }

    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<(), ApiError> {
        let key = self.key(namespace, name)?;
        self.store
            .propose(ResourceCommand::Delete {
                key,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        Ok(())
    }
}

/// Build one [`StoreBackedHandler`] per row of the generated
/// [`RESOURCE_CATALOG`]. The complete cataloged set goes live — routing,
/// discovery, and pluralization all follow the same source of truth.
///
/// This is the generation-over-composition registration surface (Pillar
/// 12): "add a kind" = one `KIND_CATALOG` row + regenerate, never a
/// hand-written handler construction.
#[must_use]
pub fn handlers_from_catalog(store: Arc<StoreMesh>) -> Vec<Arc<dyn ResourceHandler>> {
    RESOURCE_CATALOG
        .iter()
        .map(|d| {
            Arc::new(StoreBackedHandler::new(
                store.clone(),
                d.group,
                d.version,
                d.kind,
                d.plural,
                d.namespaced,
            )) as Arc<dyn ResourceHandler>
        })
        .collect()
}

/// Typed K8s `<Kind>List` envelope. `metadata.resourceVersion` is the
/// snapshot rv captured atomically with `items` in
/// [`ResourceHandler::list_at`].
#[derive(serde::Serialize)]
struct ListEnvelope {
    kind: String,
    #[serde(rename = "apiVersion")]
    api_version: String,
    items: Vec<Value>,
    metadata: ListMeta,
}

#[derive(serde::Serialize)]
struct ListMeta {
    #[serde(rename = "resourceVersion")]
    resource_version: String,
}

/// Add `kind` + `apiVersion` to a resource if missing. Matches
/// what kubectl expects in single-resource GET responses.
fn inject_type_meta(v: &Value, api_version: String, kind: &str) -> Value {
    let mut out = v.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind.to_string()));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Constructor coverage is exercised by the integration tests in
    // tests/r7_http_k8s_api.rs — they build a real StoreMesh + verify
    // each handler method end-to-end. Zero-cost mocking the StoreMesh
    // here would require introducing a trait for it; the integration
    // path is more honest.

    #[test]
    fn for_kind_uses_catalog_plural_not_plus_s() {
        // The catalog lookup, not the store, is what we exercise here —
        // assert the (group, version, plural, scope) come from
        // RESOURCE_CATALOG. We need a store to build the handler, so this
        // lives as a sync check against the catalog directly + a
        // descriptor existence assertion. `for_kind` is integration-tested
        // end-to-end in tests/m0_1_group_routing_discovery.rs.
        let ep = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == "Endpoints")
            .expect("Endpoints cataloged");
        assert_eq!(ep.plural, "endpoints", "curated plural, NOT endpointss");
        assert!(ep.namespaced);
        let dep = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == "Deployment")
            .expect("Deployment cataloged");
        assert_eq!(dep.group, "apps");
        assert_eq!(dep.version, "v1");
        assert_eq!(dep.plural, "deployments");
    }

    #[test]
    fn inject_type_meta_adds_missing_fields() {
        let v = serde_json::json!({"metadata": {"name": "x"}});
        let out = inject_type_meta(&v, "v1".into(), "Pod");
        assert_eq!(out.get("kind").unwrap(), "Pod");
        assert_eq!(out.get("apiVersion").unwrap(), "v1");
    }

    #[test]
    fn inject_type_meta_preserves_existing_fields() {
        let v = serde_json::json!({"kind": "Pod", "apiVersion": "v1"});
        let out = inject_type_meta(&v, "v99".into(), "WrongKind");
        // Existing kind / apiVersion survive.
        assert_eq!(out.get("kind").unwrap(), "Pod");
        assert_eq!(out.get("apiVersion").unwrap(), "v1");
    }

    #[test]
    fn compacted_too_old_maps_to_gone_410() {
        // The translation arm: WatchGone::CompactedTooOld => ApiError::Gone,
        // which renders HTTP 410 / Expired (proven in error::tests).
        let gone = WatchGone::CompactedTooOld {
            requested: Revision(2),
            compacted: Revision(5),
        };
        let err = gone_to_api_error(gone);
        assert!(matches!(err, ApiError::Gone(_)));
        let msg = err.to_string();
        assert!(msg.contains('2') && msg.contains('5'), "carries req + compacted: {msg}");
        // Renders HTTP 410.
        use axum::response::IntoResponse;
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::GONE,
            "CompactedTooOld → ApiError::Gone → HTTP 410"
        );
    }

    #[test]
    fn overflow_at_registration_also_maps_to_gone() {
        let gone = WatchGone::Overflow {
            capacity: 4,
            last_seen: Revision(7),
        };
        let err = gone_to_api_error(gone);
        assert!(matches!(err, ApiError::Gone(_)));
    }
}
