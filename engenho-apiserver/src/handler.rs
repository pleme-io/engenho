//! Per-kind CRUD trait.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::admission::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionRequest,
};
use engenho_store::{
    ContinueToken, Revision, StoreMesh, WatchGone, WatchOpts, WatchStream,
    command::{Reason, ResourceCommand, ResourceOp},
    resource::ResourceKey,
    watch_backend::WATCH_CHANNEL_CAPACITY,
};
use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor, Subresource};

use crate::error::ApiError;
use crate::params::{ResumePoint, Selectors, body_precondition};
use crate::scale::{Scale, project_scale};

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

    /// kubectl short-name aliases for this kind (e.g. `["deploy"]`).
    /// Pure registration metadata flowing into discovery `shortNames`,
    /// which is how `kubectl get deploy` resolves to `deployments`.
    /// Empty default so non-`StoreBacked` handlers are unaffected.
    fn short_names(&self) -> &[&str] {
        &[]
    }

    /// The singular resource name (lowercase kind) — served as discovery
    /// `singularName`. Empty default; `StoreBackedHandler` overrides it
    /// from the catalog.
    fn singular_name(&self) -> &str {
        ""
    }

    /// kubectl resource categories (e.g. `["all"]`) — served as discovery
    /// `categories`. Empty default; `StoreBackedHandler` overrides it.
    fn categories(&self) -> &[&str] {
        &[]
    }

    /// The typed subresources this kind serves (`/status`, `/scale`),
    /// sourced from the generated catalog descriptor. The router dispatch +
    /// discovery fold both read this — advertised == routable. Empty default
    /// (a handler with no descriptor serves no subresource); `StoreBackedHandler`
    /// returns the descriptor's slice.
    fn subresources(&self) -> &[Subresource] {
        &[]
    }

    /// GET the parent object's `/status` view. Default: a typed `NotFound`
    /// (this kind does not serve `/status`) — never a panic. `StoreBackedHandler`
    /// overrides for status-bearing kinds: it reads the live object (the
    /// whole object IS the `/status` GET body in K8s) through the same
    /// `inject_type_meta` path GET uses.
    async fn get_status(&self, _namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        Err(self.no_subresource("status", name))
    }

    /// PUT (full-object replace) the parent's `/status`. Default typed 404.
    /// `StoreBackedHandler` writes ONLY `.status` from the incoming object
    /// (spec/metadata preserved by the RFC7396 merge + generation preserved
    /// by `compute_generation_on_put` on a status-only write).
    async fn put_status(
        &self,
        _namespace: Option<&str>,
        name: &str,
        _incoming: Value,
    ) -> Result<Value, ApiError> {
        Err(self.no_subresource("status", name))
    }

    /// PATCH the parent's `/status`. Default typed 404. `StoreBackedHandler`
    /// scopes the patch to `.status` only (merge/strategic: strip non-status
    /// keys; json-patch: reject ops outside `/status`).
    async fn patch_status(
        &self,
        _namespace: Option<&str>,
        name: &str,
        _patch: Value,
        _patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError> {
        Err(self.no_subresource("status", name))
    }

    /// GET the parent's `/scale` as an `autoscaling/v1` Scale projection.
    /// Default typed 404 (this kind is not scalable). `StoreBackedHandler`
    /// projects `spec.replicas` / `status.replicas` / selector off the
    /// live parent.
    async fn get_scale(&self, _namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        Err(self.no_subresource("scale", name))
    }

    /// PUT (full-object replace) the parent's `/scale`. Default typed 404.
    /// `StoreBackedHandler` deserializes the incoming Scale, takes
    /// `spec.replicas`, and writes ONLY `spec.replicas` back to the parent.
    async fn put_scale(
        &self,
        _namespace: Option<&str>,
        name: &str,
        _incoming: Value,
    ) -> Result<Value, ApiError> {
        Err(self.no_subresource("scale", name))
    }

    /// PATCH the parent's `/scale`. Default typed 404. `StoreBackedHandler`
    /// translates the Scale-shaped patch's `spec.replicas` into a
    /// `{"spec":{"replicas":N}}` merge patch on the parent.
    async fn patch_scale(
        &self,
        _namespace: Option<&str>,
        name: &str,
        _patch: Value,
        _patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError> {
        Err(self.no_subresource("scale", name))
    }

    /// Build the typed `NotFound` a handler returns when asked for a
    /// subresource its kind does not serve — "the server could not find the
    /// requested resource". This is the no-stub-Ok discipline: an
    /// unimplemented subresource on a kind that lacks it is a typed 404 by
    /// construction, never a panic / `todo!()`.
    fn no_subresource(&self, sub: &str, name: &str) -> ApiError {
        ApiError::NotFound(format!(
            "the server could not find the requested resource: {} {:?} does not serve subresource {:?}",
            self.kind(),
            name,
            sub
        ))
    }

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

    /// PAGED LIST — at most `limit` selector-matched items, resuming from
    /// `continue_token` (the cursor of the previous page). The
    /// selector-vs-limit interaction is load-bearing: selectors are
    /// applied apiserver-side, so `limit` counts items AFTER filtering,
    /// and `continue` encodes the last EMITTED key. The handler
    /// over-fetches store pages within the snapshot, selector-filters,
    /// and accumulates until `limit` matches are collected or the
    /// snapshot is exhausted.
    ///
    /// All store pages in one request AND across the continue series read
    /// from the SAME revision (the token's snapshot rev) so the series is
    /// consistent (etcd consistent-list semantics).
    ///
    /// Returns `(items, snapshot_rv, continue, remaining)`:
    ///   * `items` — up to `limit` selector-matched objects.
    ///   * `snapshot_rv` — the page-series consistent revision (baked
    ///     into the next continue token + the LIST envelope's
    ///     `resourceVersion`).
    ///   * `continue` — the opaque next-page token iff more matching
    ///     items remain, else `None`.
    ///   * `remaining` — a lower bound on the still-unreturned matching
    ///     items (the store-side GVK tail count after the last emitted
    ///     key), or `None` when there is no continuation.
    async fn list_page(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
        limit: usize,
        continue_token: Option<ContinueToken>,
    ) -> Result<(Vec<Value>, Revision, Option<String>, Option<u64>), ApiError>;

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

    /// Apply a PATCH. `patch_type` is the typed discriminant of the
    /// request's `Content-Type` (resolved by the router from the media type)
    /// — it travels into the store's [`ResourceCommand::Patch`] so the
    /// deterministic apply path dispatches the correct algorithm (RFC 7396
    /// merge / RFC 6902 json-patch / strategic list-merge). The raw `patch`
    /// `Value` is the decoded body; for json-patch it is the RFC 6902 op
    /// array.
    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
        patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError>;

    /// DELETE a resource. Returns the response BODY as the K8s wire
    /// contract requires — the deleted object when one existed (so kubectl
    /// can decode + discard it), or a `metav1.Status{status:"Success"}`
    /// when the name was already absent (idempotent no-op). NEVER `()`:
    /// an empty body crashes kubectl's `json.Unmarshal([]byte{})`.
    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError>;

    /// DELETE with an optimistic-concurrency precondition
    /// (`Preconditions.resourceVersion`, surfaced as `?resourceVersion=`).
    /// `expected = None` is an unconditional delete (identical to
    /// [`Self::delete`]); `Some(N)` deletes iff the live object's
    /// `mod_revision == N`, else a typed
    /// [`ApiError::ResourceVersionConflict`] (409).
    ///
    /// Returns the response BODY (deleted object, or Status-Success when
    /// the name was absent) — same contract as [`Self::delete`].
    async fn delete_with_precondition(
        &self,
        namespace: Option<&str>,
        name: &str,
        expected: Option<Revision>,
    ) -> Result<Value, ApiError>;

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
    ///
    /// The unpaged path passes `continue_ = None`, `remaining = None`
    /// (both fields omitted from the body). The paged path
    /// ([`Self::list_page`]) passes the next-page token + remaining count.
    fn list_response(
        &self,
        items: Vec<Value>,
        rv: Revision,
        continue_: Option<String>,
        remaining: Option<u64>,
    ) -> Value {
        let env = ListEnvelope {
            kind: format!("{}List", self.kind()),
            api_version: self.api_version(),
            items,
            metadata: ListMeta {
                resource_version: rv.to_string(),
                continue_,
                // K8s `remainingItemCount` is an int64; our store count
                // fits (resource counts are far below i64::MAX).
                remaining_item_count: remaining.map(|r| r as i64),
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
    /// kubectl short-name aliases, singular name, and categories — pure
    /// registration metadata sourced from the generated `RESOURCE_CATALOG`.
    /// `&'static` because the catalog is `&'static`; the empty defaults for
    /// the legacy constructors are `&[]` / `""` (no metadata, identical to
    /// the trait defaults).
    short_names: &'static [&'static str],
    singular: &'static str,
    categories: &'static [&'static str],
    /// The typed subresources this kind serves (`/status`, `/scale`),
    /// sourced from the catalog descriptor. `&[]` for the legacy
    /// constructors + dynamically-registered CRDs (CRs serve no
    /// status/scale at this brick).
    subresources: &'static [Subresource],
    store: Arc<StoreMesh>,
    /// Optional admission chain dispatched on the write path (create /
    /// patch / delete) at the API boundary. `None` = no admission — every
    /// existing constructor produces this, so legacy callers + tests are
    /// unaffected by the new field. Controller writes (`Reason::Controller`)
    /// do NOT flow through a handler, so they never hit admission.
    admission: Option<Arc<AdmissionChain>>,
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
            // Legacy constructor: no registration metadata (identical to
            // the trait defaults). The cataloged registration path uses
            // [`Self::with_registration_metadata`] / [`Self::from_descriptor`].
            short_names: &[],
            singular: "",
            categories: &[],
            // No subresources by default; set from the catalog descriptor in
            // [`Self::from_descriptor`]. A dynamically-registered CR serves
            // none at this brick.
            subresources: &[],
            admission: None,
        }
    }

    /// Attach the per-kind discovery registration metadata (short names,
    /// singular name, categories) sourced from the generated catalog
    /// descriptor. Builder style; the cataloged registration path
    /// ([`crate::handlers_from_catalog`]) is the only production caller.
    #[must_use]
    pub fn with_registration_metadata(
        mut self,
        short_names: &'static [&'static str],
        singular: &'static str,
        categories: &'static [&'static str],
    ) -> Self {
        self.short_names = short_names;
        self.singular = singular;
        self.categories = categories;
        self
    }

    /// Attach the typed subresource set (`/status`, `/scale`) sourced from
    /// the generated catalog descriptor. Builder style; [`Self::from_descriptor`]
    /// is the only production caller, so a cataloged kind serves exactly the
    /// subresources its `KIND_CATALOG` row declares.
    #[must_use]
    pub fn with_subresources(mut self, subresources: &'static [Subresource]) -> Self {
        self.subresources = subresources;
        self
    }

    /// Attach an [`AdmissionChain`] to this handler (builder style). The
    /// chain is dispatched on every create / patch / delete BEFORE the
    /// store proposal; an empty chain is a no-op (admits everything).
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<AdmissionChain>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Run the admission chain for `action` on `key` and return the body
    /// to actually propose (possibly mutated). `None` admission, or an
    /// empty chain, returns `body` unchanged. A `Deny` becomes a typed
    /// [`ApiError::Forbidden`] (HTTP 403).
    ///
    /// For Delete the body is `None`; the chain still runs (so a policy
    /// can block deletes) and the returned value is ignored by the
    /// caller.
    async fn admit(
        &self,
        action: AdmissionAction,
        key: &ResourceKey,
        body: Option<Value>,
    ) -> Result<Option<Value>, ApiError> {
        let Some(chain) = &self.admission else {
            return Ok(body);
        };
        let current = self.store.get(key).await;
        let request = AdmissionRequest {
            action,
            key: key.clone(),
            value: body.clone(),
            current,
        };
        match chain.review(request).await {
            AdmissionDecision::Allow => Ok(body),
            AdmissionDecision::Mutate(v) => Ok(Some(v)),
            AdmissionDecision::Deny(reason) => Err(ApiError::Forbidden(reason)),
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
        Some(Self::from_descriptor(store, d))
    }

    /// Construct a fully-registered handler from a generated catalog
    /// [`ResourceDescriptor`] — (group, version, plural, scope) PLUS the
    /// discovery registration metadata (short names, singular, categories).
    /// This is the single canonical constructor the cataloged registration
    /// path uses so every routable kind also carries the metadata kubectl
    /// needs to resolve short names + run `explain`.
    #[must_use]
    pub fn from_descriptor(store: Arc<StoreMesh>, d: &'static ResourceDescriptor) -> Self {
        Self::new(store, d.group, d.version, d.kind, d.plural, d.namespaced)
            .with_registration_metadata(d.short_names, d.singular, d.categories)
            .with_subresources(d.subresources)
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
    fn short_names(&self) -> &[&str] {
        self.short_names
    }
    fn singular_name(&self) -> &str {
        self.singular
    }
    fn categories(&self) -> &[&str] {
        self.categories
    }

    fn subresources(&self) -> &[Subresource] {
        self.subresources
    }

    // ── subresource scoped writes (reuse ResourceCommand::Patch verbatim) ──
    //
    // Each method reads the LIVE object via `store.get`, builds a SCOPED
    // patch, and proposes a `ResourceCommand::Patch` — no new store command.
    // The scoping is what gives spec/status isolation: a /status write
    // carries only `{"status": …}` so `apply_patch`'s RFC7396 merge preserves
    // spec+metadata and `compute_generation_on_put` preserves generation; a
    // /scale write carries only `{"spec":{"replicas":N}}`. CAS threads
    // `expected` from the incoming object's metadata.resourceVersion (same
    // `body_precondition` the main path uses) so concurrent writes conflict
    // with a typed 409. This is the OPERATOR-facing peer of the
    // controller-facing `write_status_cas`.

    async fn get_status(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        // The whole object IS the /status GET body in K8s — same as GET.
        self.get(namespace, name).await
    }

    async fn put_status(
        &self,
        namespace: Option<&str>,
        name: &str,
        incoming: Value,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        // The object MUST exist (you can't set status on a missing object).
        let live = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        // CAS precondition: from the incoming object's
        // metadata.resourceVersion (kubectl PUT /status round-trips the live
        // rv); absent → fall back to the live rv so a no-rv PUT still
        // conflicts on a concurrent change rather than blindly clobbering.
        let expected = body_precondition(&incoming)?.or_else(|| revision_of(&live));
        // Take ONLY the incoming object's `.status` — a /status PUT may not
        // touch spec/metadata (K8s drops them on the /status endpoint). The
        // scoped `{"status": …}` merge patch leaves spec+metadata intact and
        // preserves generation (status-only writes don't bump it).
        let incoming_status = incoming.get("status").cloned().unwrap_or(Value::Null);
        let scoped = serde_json::json!({ "status": incoming_status });
        self.propose_scoped_patch(&key, name, scoped, expected).await
    }

    async fn patch_status(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
        patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        if self.store.get(&key).await.is_none() {
            return Err(ApiError::NotFound(format!("{}/{}", self.kind, name)));
        }
        // Scope the patch to `.status` per K8s /status semantics:
        //   * merge/strategic: a /status patch that names `spec` is IGNORED —
        //     keep only the `status` key from the patch body.
        //   * json-patch: every op MUST target `/status...`; an op outside is
        //     a typed rejection (no silent spec write through /status).
        let scoped = scope_status_patch(&patch, patch_type)?;
        let expected = body_precondition(&scoped)?;
        self.propose_scoped_patch_typed(&key, name, scoped, patch_type, expected)
            .await
    }

    async fn get_scale(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let live = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        // Project the parent into its autoscaling/v1 Scale view. Serde →
        // Value (typed emission; no format! of the wire).
        let scale = project_scale(&live);
        serde_json::to_value(&scale)
            .map_err(|e| ApiError::Internal(format!("scale projection serialize: {e}")))
    }

    async fn put_scale(
        &self,
        namespace: Option<&str>,
        name: &str,
        incoming: Value,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let live = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        // Deserialize the incoming autoscaling/v1 Scale + take spec.replicas.
        let scale: Scale = serde_json::from_value(incoming)
            .map_err(|e| ApiError::BadRequest(format!("invalid Scale body: {e}")))?;
        // CAS from the incoming Scale's metadata.resourceVersion (which is the
        // parent's rv — see project_scale); absent → the live parent's rv.
        let expected = scale
            .metadata
            .resource_version
            .parse::<u64>()
            .ok()
            .map(engenho_store::Revision)
            .or_else(|| revision_of(&live));
        // Write ONLY spec.replicas. This bumps generation (a real spec
        // change) — correct, scale IS a spec mutation.
        let scoped = serde_json::json!({ "spec": { "replicas": scale.spec.replicas } });
        let _ = self.propose_scoped_patch(&key, name, scoped, expected).await?;
        // Re-project the now-updated parent for the Scale response.
        self.get_scale(namespace, name).await
    }

    async fn patch_scale(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
        _patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        if self.store.get(&key).await.is_none() {
            return Err(ApiError::NotFound(format!("{}/{}", self.kind, name)));
        }
        // Translate the Scale-shaped patch's spec.replicas into a scoped
        // `{"spec":{"replicas":N}}` merge patch on the parent. A scale patch
        // that doesn't carry spec.replicas is a no-op replica change → leave
        // the parent's replicas alone (empty scoped patch).
        let replicas = patch
            .get("spec")
            .and_then(|s| s.get("replicas"))
            .and_then(Value::as_i64);
        let scoped = match replicas {
            Some(n) => serde_json::json!({ "spec": { "replicas": n } }),
            None => serde_json::json!({}),
        };
        let expected = body_precondition(&patch)?;
        let _ = self
            .propose_scoped_patch(&key, name, scoped, expected)
            .await?;
        self.get_scale(namespace, name).await
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
        Ok(self.list_response(items, rv, None, None))
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

    async fn list_page(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
        limit: usize,
        continue_token: Option<ContinueToken>,
    ) -> Result<(Vec<Value>, Revision, Option<String>, Option<u64>), ApiError> {
        // Resolve the page-series snapshot revision + the resume cursor.
        // The FIRST page (no continue token) captures the snapshot rev
        // below; every subsequent page reuses the token's snapshot rev so
        // the whole series is consistent. The cursor is the token's
        // last_key.
        let series_snapshot = continue_token.as_ref().map(|t| t.snapshot_rev);
        let mut cursor: Option<ResourceKey> = continue_token.map(|t| t.last_key);

        // limit == 0 → unbounded. One store page (limit=0 = all after
        // cursor), selector-filter, no continuation. The snapshot rev is
        // captured here (or reused from the token).
        if limit == 0 {
            let (entries, snap, _next, _remaining) = self
                .store
                .list_page_at_revision(
                    &self.group,
                    &self.version,
                    &self.kind,
                    namespace,
                    cursor.as_ref(),
                    0,
                )
                .await;
            let rv = series_snapshot.unwrap_or(snap);
            let items: Vec<Value> = entries
                .into_iter()
                .filter(|(_, v)| sel.matches(v))
                .map(|(_, v)| inject_type_meta(&v, self.api_version(), &self.kind))
                .collect();
            return Ok((items, rv, None, None));
        }

        // Selector-aware paging: `limit` counts items AFTER selector
        // filtering, but the store pages on the GVK-keyed catalog. Loop:
        // over-fetch a store page, selector-filter, accumulate until we
        // have `limit` matches or the GVK set is exhausted. The over-fetch
        // chunk is `limit` (a reasonable default; with no selectors it is
        // exactly one store page per accepted page).
        let mut accepted: Vec<Value> = Vec::with_capacity(limit);
        // `last_emitted_key` is the key of the last ACCEPTED item — the
        // basis for the next continue token (must be the last EMITTED key,
        // not the last SCANNED key).
        let mut last_emitted_key: Option<ResourceKey> = None;
        // The store-side tail count after the last scanned store page —
        // becomes `remainingItemCount` when we stop with a continuation.
        // The `loop` always assigns it before the post-loop read.
        let mut store_tail_remaining: u64;
        // The snapshot rv (captured from the first store fetch if the
        // series didn't already pin one).
        let mut snapshot_rv: Option<Revision> = series_snapshot;
        // The chunk size to over-fetch per store page.
        let chunk = limit.max(1);

        loop {
            let (entries, snap, next, remaining) = self
                .store
                .list_page_at_revision(
                    &self.group,
                    &self.version,
                    &self.kind,
                    namespace,
                    cursor.as_ref(),
                    chunk,
                )
                .await;
            if snapshot_rv.is_none() {
                snapshot_rv = Some(snap);
            }
            store_tail_remaining = remaining;

            let store_page_empty = entries.is_empty();
            for (k, v) in entries {
                if sel.matches(&v) {
                    accepted.push(inject_type_meta(&v, self.api_version(), &self.kind));
                    last_emitted_key = Some(k);
                    if accepted.len() == limit {
                        break;
                    }
                }
            }

            // Done if we filled the page, the GVK set has no more items
            // after this store page (`next` is None), or the store page
            // came back empty (defensive — `next: None` already covers
            // it).
            if accepted.len() == limit || next.is_none() || store_page_empty {
                break;
            }
            // Advance the store cursor to the last SCANNED key of this
            // store page and fetch the next chunk (more matches may lie
            // beyond the selector-rejected items).
            cursor = next;
        }

        let rv = snapshot_rv.unwrap_or_else(Revision::default);

        // Build the continuation. There is a next page iff we stopped
        // because the page filled AND more matching items may remain.
        // After the last emitted key, the remaining matching count is at
        // most the store tail (`store_tail_remaining`) plus any items
        // scanned-but-not-emitted in the final store page — but at M0.1
        // `remainingItemCount` is documented as a lower bound (the store
        // GVK tail after the last store page). We emit a continuation iff
        // the page is full AND (the store reported a tail OR the last
        // store page had a `next`). Conservatively: if the page filled and
        // there's a known last_emitted_key and the store still has a tail,
        // continue.
        let page_full = accepted.len() == limit;
        let (continue_str, remaining_out) = if page_full && store_tail_remaining > 0 {
            // More GVK items remain after the last store page; resume
            // strictly after the last EMITTED key at the series snapshot.
            let token = last_emitted_key
                .map(|k| ContinueToken::new(rv, k).encode());
            (token, Some(store_tail_remaining))
        } else {
            (None, None)
        };

        Ok((accepted, rv, continue_str, remaining_out))
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
        // Admission runs at the API boundary BEFORE any store proposal.
        // A Mutate replaces the body; a Deny short-circuits with 403.
        let body = self
            .admit(AdmissionAction::Put, &key, Some(body))
            .await?
            .expect("admit(Put, Some(_)) preserves Some on Allow/Mutate");
        // Optimistic-concurrency precondition from the (post-admission)
        // body's metadata.resourceVersion. For a CREATE (POST) of a NEW
        // object the rv is absent → None (unconditional create); a
        // re-PUT-style body carrying an rv threads CAS into the proposal.
        let expected = body_precondition(&body)?;
        // Reject if already exists (POST semantics). The no-rv POST keeps
        // its existing already-exists Conflict/"AlreadyExists" path.
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
                expected,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        // A CAS conflict from the deterministic apply path → typed 409
        // "Conflict" (distinct from the already-exists path above).
        if result.op == ResourceOp::Conflict {
            return Err(self.rv_conflict(&name, expected));
        }
        // Read back the committed resource (with resourceVersion).
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
        patch_type: engenho_types::patch::PatchType,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        // Admission runs at the API boundary BEFORE the store proposal.
        let patch = self
            .admit(AdmissionAction::Patch, &key, Some(patch))
            .await?
            .expect("admit(Patch, Some(_)) preserves Some on Allow/Mutate");
        // Precondition from the (post-admission) patch body's
        // metadata.resourceVersion (absent → unconditional). Only a
        // merge/strategic body is an object carrying metadata; a json-patch
        // op array has no precondition (the K8s wire never carries one on a
        // json-patch), so `body_precondition` over a non-object → None.
        let expected = body_precondition(&patch)?;
        if self.store.get(&key).await.is_none() {
            return Err(ApiError::NotFound(format!("{}/{}", self.kind, name)));
        }
        let result = self
            .store
            .propose(ResourceCommand::Patch {
                key: key.clone(),
                patch,
                patch_type,
                expected,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        if result.op == ResourceOp::Conflict {
            return Err(self.rv_conflict(name, expected));
        }
        // The patch interpreter REFUSED the patch (RFC6902 test failure, bad
        // pointer, missing strategic merge-key, bad $patch directive, or the
        // typed-deferred server-side-apply path). Surface the typed Status —
        // 415 for SSA (recognized-but-unimplemented content-type), 422 for a
        // bad-but-valid patch body. NEVER a corrupted object.
        if result.op == ResourceOp::PatchRejected {
            let msg = result
                .patch_error
                .unwrap_or_else(|| "patch rejected".to_string());
            return Err(if msg.contains("server-side apply") {
                ApiError::UnsupportedMediaType(format!(
                    "the server does not yet implement server-side apply: {msg}"
                ))
            } else {
                ApiError::BadRequest(msg)
            });
        }
        let stored = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::Internal("patch lost during commit".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }

    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        // Unconditional delete (no precondition). The precondition path
        // is [`Self::delete_with_precondition`], driven by `?resourceVersion=`.
        self.delete_with_precondition(namespace, name, None).await
    }

    async fn delete_with_precondition(
        &self,
        namespace: Option<&str>,
        name: &str,
        expected: Option<Revision>,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        // Admission runs at the API boundary BEFORE the store proposal so
        // a policy can block deletes. The Delete body is None; any Mutate
        // value is ignored (delete has no body to rewrite).
        let _ = self.admit(AdmissionAction::Delete, &key, None).await?;
        // Read the LIVE object BEFORE proposing the delete — this is the
        // body the K8s DELETE wire returns. The store's `ApplyResult`
        // carries only op+revision (NOT the removed object — see
        // engenho-store type_config.rs), so the apiserver captures the
        // pre-image here. The read-then-delete is two store ops (not atomic
        // with the CAS); a lost race just means we return the last-read
        // object, while the CAS precondition is still enforced inside
        // `propose`. Threading the apply outcome's `Change.prior` back
        // through `ApplyResult` is a larger store-layer change deferred to
        // a later brick.
        let prior = self.store.get(&key).await;
        let result = self
            .store
            .propose(ResourceCommand::Delete {
                key,
                expected,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        if result.op == ResourceOp::Conflict {
            return Err(self.rv_conflict(name, expected));
        }
        // Return the response BODY:
        //   * object existed at delete time → the removed object exactly as
        //     it was, through the SAME `inject_type_meta` path GET uses
        //     (renders cleanly as json OR protobuf — its GVK is in the pool).
        //   * object absent (idempotent no-op) → a typed
        //     `metav1.Status{status:"Success"}`. Never an empty body.
        match prior {
            Some(v) => Ok(inject_type_meta(&v, self.api_version(), &self.kind)),
            None => Ok(crate::error::delete_status_success(name, &self.kind)),
        }
    }
}

impl StoreBackedHandler {
    /// Build the typed optimistic-concurrency 409 ("Conflict") error for
    /// a CAS failure on `name` with the caller's `expected` revision. The
    /// message mirrors kube-apiserver's optimistic-concurrency phrasing.
    fn rv_conflict(&self, name: &str, expected: Option<Revision>) -> ApiError {
        let expected_str = expected
            .map(|r| r.to_string())
            .unwrap_or_else(|| "<none>".to_string());
        ApiError::ResourceVersionConflict(format!(
            "Operation cannot be fulfilled on {} \"{}\": the object has been modified; \
             expected resourceVersion {} did not match the live object",
            self.plural, name, expected_str
        ))
    }

    /// Propose a scoped RFC7396 merge `Patch` (the subresource write shape)
    /// and read back the committed object. REUSES `ResourceCommand::Patch`
    /// + the `patch_apply` interpreter verbatim — no new store command. The
    /// `patch` is already scoped to `{"status":…}` or
    /// `{"spec":{"replicas":N}}` by the caller, so the untouched half of the
    /// object is preserved by the merge. A CAS failure → typed 409; a
    /// rejected patch → the same typed Status the main patch path returns.
    async fn propose_scoped_patch(
        &self,
        key: &ResourceKey,
        name: &str,
        patch: Value,
        expected: Option<Revision>,
    ) -> Result<Value, ApiError> {
        self.propose_scoped_patch_typed(
            key,
            name,
            patch,
            engenho_types::patch::PatchType::Merge,
            expected,
        )
        .await
    }

    /// Like [`Self::propose_scoped_patch`] but with an explicit
    /// [`PatchType`] — used by `/status` PATCH where the request's typed
    /// patch algorithm (merge / strategic / json-patch) flows through to the
    /// interpreter after the body has been scoped to `.status`.
    async fn propose_scoped_patch_typed(
        &self,
        key: &ResourceKey,
        name: &str,
        patch: Value,
        patch_type: engenho_types::patch::PatchType,
        expected: Option<Revision>,
    ) -> Result<Value, ApiError> {
        let result = self
            .store
            .propose(ResourceCommand::Patch {
                key: key.clone(),
                patch,
                patch_type,
                expected,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        if result.op == ResourceOp::Conflict {
            return Err(self.rv_conflict(name, expected));
        }
        if result.op == ResourceOp::PatchRejected {
            let msg = result
                .patch_error
                .unwrap_or_else(|| "patch rejected".to_string());
            return Err(if msg.contains("server-side apply") {
                ApiError::UnsupportedMediaType(format!(
                    "the server does not yet implement server-side apply: {msg}"
                ))
            } else {
                ApiError::BadRequest(msg)
            });
        }
        let stored = self
            .store
            .get(key)
            .await
            .ok_or_else(|| ApiError::Internal("subresource patch lost during commit".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }
}

/// Read a stored object's `metadata.resourceVersion` as a [`Revision`] —
/// the CAS `expected` fallback for a subresource write whose incoming body
/// carried no rv. `None` when absent/unparseable.
fn revision_of(value: &Value) -> Option<Revision> {
    value
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|rv| rv.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Revision)
}

/// Scope a `/status` PATCH body to the `.status` field, per K8s /status
/// subresource semantics:
///
///   * **merge / strategic** — a `/status` patch that names `spec` (or any
///     non-status key) is IGNORED. Keep ONLY the `status` key from the patch
///     body; everything else is dropped so a `/status` write can never touch
///     spec/metadata. Carrying `metadata.resourceVersion` through is allowed
///     (it's the CAS precondition, not a mutated field).
///   * **json-patch** — every RFC6902 op MUST target `/status...`. An op
///     whose `path` does not start with `/status` is a typed `BadRequest`
///     (no silent spec write through `/status`).
///   * **apply** — server-side apply is typed-deferred; the body passes
///     through unscoped and the store returns the typed SSA rejection.
///
/// Returns the scoped patch body to propose.
fn scope_status_patch(
    patch: &Value,
    patch_type: engenho_types::patch::PatchType,
) -> Result<Value, ApiError> {
    use engenho_types::patch::PatchType;
    match patch_type {
        PatchType::Merge | PatchType::Strategic => {
            // Keep only `status` (+ a metadata.resourceVersion precondition if
            // present). Drop spec + any other top-level key.
            let mut out = serde_json::Map::new();
            if let Some(obj) = patch.as_object() {
                if let Some(status) = obj.get("status") {
                    out.insert("status".to_string(), status.clone());
                }
                // Preserve the CAS precondition (resourceVersion) only — never
                // any other metadata field (a /status write can't rename/relabel).
                if let Some(rv) = obj
                    .get("metadata")
                    .and_then(|m| m.get("resourceVersion"))
                {
                    out.insert(
                        "metadata".to_string(),
                        serde_json::json!({ "resourceVersion": rv.clone() }),
                    );
                }
            }
            Ok(Value::Object(out))
        }
        PatchType::Json => {
            // Every op's `path` must be under `/status`.
            let ops = patch.as_array().ok_or_else(|| {
                ApiError::BadRequest("json-patch body must be an array of operations".into())
            })?;
            for op in ops {
                let path = op.get("path").and_then(Value::as_str).unwrap_or("");
                if !(path == "/status" || path.starts_with("/status/")) {
                    return Err(ApiError::BadRequest(format!(
                        "the status subresource patch may only target /status; got op path {path:?}"
                    )));
                }
            }
            Ok(patch.clone())
        }
        PatchType::Apply => Ok(patch.clone()),
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
            Arc::new(StoreBackedHandler::from_descriptor(store.clone(), d))
                as Arc<dyn ResourceHandler>
        })
        .collect()
}

/// Build one admission-dispatching [`StoreBackedHandler`] per row of the
/// generated [`RESOURCE_CATALOG`]. Identical to [`handlers_from_catalog`]
/// except every handler carries the shared `admission` chain — so create
/// / patch / delete flow through admission at the API boundary. The
/// SAME chain `Arc` is cloned into every handler (one chain governs the
/// whole API surface).
#[must_use]
pub fn handlers_from_catalog_with_admission(
    store: Arc<StoreMesh>,
    admission: Arc<AdmissionChain>,
) -> Vec<Arc<dyn ResourceHandler>> {
    RESOURCE_CATALOG
        .iter()
        .map(|d| {
            Arc::new(
                StoreBackedHandler::from_descriptor(store.clone(), d)
                    .with_admission(admission.clone()),
            ) as Arc<dyn ResourceHandler>
        })
        .collect()
}

// ── DynamicHandlerSink — apiserver impl over the live RouterState ──────
//
// The `CrdController` (in engenho-controllers) can't depend on
// engenho-apiserver (that's a cycle — apiserver already depends on
// controllers for admission), so it mutates the router table through the
// typed `DynamicHandlerSink` trait. This is the apiserver-side impl: it
// owns the store + the admission chain + a clone of the live RouterState,
// and on `register_crd` builds the SAME `StoreBackedHandler` the cataloged
// path builds (opaque-JSON CRUD, admission-dispatched) under the CRD's GVK
// + names, then lands it via `RouterState::register`. NO parallel CR
// codepath — a registered CR handler IS a StoreBackedHandler.

use engenho_controllers::{CrdHandlerSpec, DynamicHandlerSink};

/// Apiserver-provided [`DynamicHandlerSink`] over the live
/// [`crate::router::RouterState`]. Built once at boot + shared (as
/// `Arc<dyn DynamicHandlerSink>`) with the `CrdController`.
pub struct RouterHandlerSink {
    store: Arc<StoreMesh>,
    /// The SAME admission chain the cataloged handlers carry, so CR
    /// create/patch/delete flow through admission identically.
    admission: Arc<AdmissionChain>,
    /// A clone of the live RouterState — `register`/`unregister` swap the
    /// shared `ArcSwap`, so the change is visible to in-flight requests.
    router: crate::router::RouterState,
}

impl RouterHandlerSink {
    /// New sink. `router` MUST be a clone of the RouterState the
    /// [`crate::ApiServer`] was started with (same `Arc<ArcSwap<…>>`).
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        admission: Arc<AdmissionChain>,
        router: crate::router::RouterState,
    ) -> Self {
        Self {
            store,
            admission,
            router,
        }
    }

    /// Box as the trait object the controller consumes.
    #[must_use]
    pub fn into_dyn(self) -> Arc<dyn DynamicHandlerSink> {
        Arc::new(self)
    }
}

impl DynamicHandlerSink for RouterHandlerSink {
    fn register_crd(&self, spec: CrdHandlerSpec) {
        // Leak the per-CRD discovery metadata to `&'static` — the
        // `StoreBackedHandler` registration-metadata slots are `&'static`
        // (the cataloged path sources them from the `&'static`
        // RESOURCE_CATALOG). A CRD registration is rare + permanent for the
        // process lifetime (the handler lives until the CRD is deleted, and
        // even an unregister just drops the Arc — the leaked metadata is a
        // bounded one-time-per-served-version cost, not a per-request leak).
        let short_names: &'static [&'static str] = Box::leak(
            spec.short_names
                .iter()
                .map(|s| Box::leak(s.clone().into_boxed_str()) as &'static str)
                .collect::<Vec<&'static str>>()
                .into_boxed_slice(),
        );
        let categories: &'static [&'static str] = Box::leak(
            spec.categories
                .iter()
                .map(|s| Box::leak(s.clone().into_boxed_str()) as &'static str)
                .collect::<Vec<&'static str>>()
                .into_boxed_slice(),
        );
        let singular: &'static str = Box::leak(spec.singular.clone().into_boxed_str());

        let handler: Arc<dyn ResourceHandler> = Arc::new(
            StoreBackedHandler::new(
                self.store.clone(),
                spec.group.clone(),
                spec.version.clone(),
                spec.kind.clone(),
                spec.plural.clone(),
                spec.namespaced,
            )
            .with_registration_metadata(short_names, singular, categories)
            .with_admission(self.admission.clone()),
        );
        self.router.register(handler);
    }

    fn unregister_crd(&self, group: &str, version: &str, plural: &str) -> bool {
        self.router.unregister(group, version, plural)
    }
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
    /// The opaque next-page cursor — present only on a paged LIST that
    /// has more items. Omitted otherwise (K8s contract).
    #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
    continue_: Option<String>,
    /// A lower bound on the still-unreturned matching items — present
    /// only on a paged LIST with a continuation. Omitted otherwise.
    #[serde(rename = "remainingItemCount", skip_serializing_if = "Option::is_none")]
    remaining_item_count: Option<i64>,
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
