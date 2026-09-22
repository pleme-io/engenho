//! Per-kind CRUD trait.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::admission::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionRequest,
};
use engenho_store::{
    ContinueToken, Revision, StoreMesh, WatchOpts,
    command::{Reason, ResourceCommand, ResourceOp},
    resource::ResourceKey,
    watch_backend::WATCH_CHANNEL_CAPACITY,
};
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor, Subresource};

use crate::error::ApiError;
use crate::field_validation::PatchFields;
use crate::object_body::ObjectBody;
use crate::params::{DryRun, ResumePoint, Selectors, body_precondition};
use crate::pod_logs::{LogQuery, PodLogReader};
use crate::scale::{Scale, project_scale};
use crate::watch_start::{WatchRefusal, WatchStart};

mod write_plan;

use write_plan::WriteRequest;

/// Bookmark cadence handed to `watch_from` when the client opted into
/// bookmarks (`allowWatchBookmarks=true`). Mirrors the store default.
const WATCH_BOOKMARK_EVERY: Duration = Duration::from_secs(5);

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

    /// Whether this kind serves the `deletecollection` verb (a `DELETE` on the
    /// collection path with no object name). kube-apiserver exposes it for
    /// every resource whose REST storage implements `rest.CollectionDeleter`
    /// — in the core group that is everything EXCEPT the three cluster-special
    /// kinds `namespaces`, `bindings`, `componentstatuses`. Both the router
    /// dispatch AND the discovery verb fold read this, so advertised ==
    /// routable. Default `true`; `StoreBackedHandler` overrides the exceptions.
    fn supports_delete_collection(&self) -> bool {
        true
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

    /// GET a Pod container's logs (`kubectl logs <pod> [-c <container>]`).
    /// Default: a typed `NotFound` (this kind does not serve `/log`) — never a
    /// panic, never a fake-empty Ok. The Pod handler overrides it: it routes to
    /// the in-process kubelet (single-node) via an installed [`PodLogReader`].
    ///
    /// `query` carries the typed `?container=` + `?tailLines=` knobs. Returns
    /// the raw log text (rendered as `text/plain`).
    async fn logs(
        &self,
        _namespace: Option<&str>,
        name: &str,
        _query: &LogQuery,
    ) -> Result<String, ApiError> {
        Err(self.no_subresource("log", name))
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

    /// The store's current revision, read without reading any object: what a
    /// LIST that names a `resourceVersion` waits on
    /// ([`crate::list_floor::await_revision`]).
    async fn current_revision(&self) -> Revision;

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

    /// Open a streaming WATCH from `from`. A [`WatchStart::Streaming`]
    /// stream is cluster-wide (the store fans every kind through one
    /// registry); the router filters each event down to this handler's GVK +
    /// requested namespace + selectors.
    ///
    /// `allow_bookmarks` toggles the bookmark cadence (`5s` vs disabled).
    ///
    /// A resume point the store cannot serve is [`WatchStart::Refused`], not
    /// an error: ahead of the store's current revision, or below its
    /// compaction floor. The router ends a refused watch in-band with a 410
    /// (see [`crate::watch_start`] for why never an HTTP status).
    ///
    /// # Errors
    ///
    /// An [`ApiError`] only when the handler cannot open a watch at all.
    async fn watch_stream(
        &self,
        namespace: Option<&str>,
        from: ResumePoint,
        allow_bookmarks: bool,
    ) -> Result<WatchStart, ApiError>;

    /// CREATE a resource. `user_info` is the authenticated identity (threaded
    /// from the request's `Extension<UserInfo>`); it travels into the
    /// admission chain's `AdmissionRequest.user_info` so a webhook sees WHO is
    /// creating.
    ///
    /// `body` is an [`ObjectBody`]: a POST body is the object that will be
    /// stored, so it arrives already normalized (null `labels`,
    /// `annotations`, `ownerReferences`, `finalizers` dropped; mis-shaped
    /// metadata refused). There is no way to hand this method raw bytes.
    async fn create(
        &self,
        namespace: Option<&str>,
        body: ObjectBody,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError>;

    /// REPLACE (PUT) the whole object — the kubectl `replace` / update verb.
    ///
    /// The default is UNSUPPORTED (a typed 400 mirroring the pre-M0 router
    /// contract), so a handler that hasn't wired a store-backed replace keeps
    /// the old behavior. [`StoreBackedHandler`] overrides it through the one
    /// write pipeline (`handler/write_plan.rs`) every write verb shares:
    /// existence-required (404 otherwise), every rule a create gets
    /// (defaulting, validation, CRD schema, admission as UPDATE), server-owned
    /// metadata preserved, and a compare-and-swap at the revision it read —
    /// the client's `metadata.resourceVersion` when present, else re-planned
    /// on a lost race. `user_info` is the authenticated identity threaded into
    /// admission. `body` is an [`ObjectBody`] for the same reason as in
    /// [`Self::create`]: a PUT body is the object that will be stored.
    async fn replace(
        &self,
        _namespace: Option<&str>,
        _name: &str,
        _body: ObjectBody,
        _user_info: &UserInfo,
        _dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        Err(ApiError::BadRequest(
            "PUT on the main object is not supported (use POST to create, PATCH to update)"
                .to_string(),
        ))
    }

    /// Apply a PATCH. `patch_type` is the typed discriminant of the
    /// request's `Content-Type` (resolved by the router from the media type)
    /// and selects the algorithm (RFC 7396 merge / RFC 6902 json-patch /
    /// strategic list-merge / server-side apply). The raw `patch` `Value` is
    /// the decoded body; for json-patch it is the RFC 6902 op array.
    ///
    /// [`StoreBackedHandler`] computes the merged object with the store's own
    /// pure algorithm, then runs it through the one write pipeline
    /// (`handler/write_plan.rs`) every write verb shares — defaulting,
    /// validation, CRD schema, admission over the WHOLE object — and commits
    /// it by compare-and-swap, so `dryRun` returns the merged result.
    ///
    /// `apply_opts` is `Some(_)` ONLY when `patch_type ==
    /// PatchType::Apply` (server-side apply) — it carries the validated
    /// `fieldManager` + `force`. For EVERY other patch algorithm it is
    /// `None`. `user_info` is the authenticated identity threaded into
    /// admission.
    ///
    /// ★ `patch` stays a raw `Value`, never an [`ObjectBody`]: in a merge
    /// patch `null` means "delete this field", so normalizing it away would
    /// turn `kubectl label x-` into a no-op (plan edge 9).
    ///
    /// `fields` is the request's `?fieldValidation=` context (T4.6): the
    /// directive and what the router found in the patch's own bytes. For a
    /// merge, strategic or JSON patch, [`StoreBackedHandler`] judges the
    /// PATCHED object's unknown fields against it and leaves the warnings
    /// the response carries in it. A server-side apply configuration was
    /// judged whole at the border, so it is not judged again.
    #[allow(clippy::too_many_arguments)]
    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
        patch_type: engenho_types::patch::PatchType,
        apply_opts: Option<crate::params::ApplyOptions>,
        user_info: &UserInfo,
        dry_run: DryRun,
        fields: &mut PatchFields,
    ) -> Result<Value, ApiError>;

    /// DELETE a resource. Returns the response BODY as the K8s wire
    /// contract requires — the deleted object when one existed (so kubectl
    /// can decode + discard it), or a `metav1.Status{status:"Success"}`
    /// when the name was already absent (idempotent no-op). NEVER `()`:
    /// an empty body crashes kubectl's `json.Unmarshal([]byte{})`.
    /// `user_info` is the authenticated identity threaded into admission.
    async fn delete(
        &self,
        namespace: Option<&str>,
        name: &str,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError>;

    /// DELETE with an optimistic-concurrency precondition
    /// (`Preconditions.resourceVersion`, surfaced as `?resourceVersion=`).
    /// `expected = None` is an unconditional delete (identical to
    /// [`Self::delete`]); `Some(N)` deletes iff the live object's
    /// `mod_revision == N`, else a typed
    /// [`ApiError::ResourceVersionConflict`] (409).
    ///
    /// Returns the response BODY (deleted object, or Status-Success when
    /// the name was absent) — same contract as [`Self::delete`].
    /// `user_info` is the authenticated identity threaded into admission.
    async fn delete_with_precondition(
        &self,
        namespace: Option<&str>,
        name: &str,
        expected: Option<Revision>,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError>;

    /// DELETECOLLECTION — delete every object matching `sel` in `namespace`
    /// (all objects when `sel` is empty). The K8s wire contract returns the
    /// `<Kind>List` of the objects that were selected for deletion (their
    /// pre-delete images), with HTTP 200.
    ///
    /// Composed from the two primitives every store-backed kind already has:
    /// [`Self::list_at`] captures the matched pre-images atomically (with the
    /// snapshot rv that becomes the envelope's `resourceVersion`), then each
    /// is deleted by name through [`Self::delete_with_precondition`] (so every
    /// item passes the same per-object admission a single DELETE does). A kind
    /// that does not serve the verb ([`Self::supports_delete_collection`] ⇒
    /// `false`) is never routed here.
    async fn delete_collection(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        let (items, rv) = self.list_at(namespace, sel).await?;
        for item in &items {
            if let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) {
                self.delete_with_precondition(namespace, name, None, user_info, dry_run)
                    .await?;
            }
        }
        Ok(self.list_response(items, rv, None, None))
    }

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
    /// Whether the NamespaceLifecycle admission rule is enforced — reject a
    /// create into a namespace that does not exist.
    ///
    /// OFF by default and enabled by the assembled Runtime, mirroring
    /// upstream where NamespaceLifecycle is an admission PLUGIN rather than
    /// an unconditional apiserver behaviour. A bare handler under unit test
    /// has no seeded namespaces and is exercising handler mechanics, not
    /// cluster lifecycle; making this unconditional would have made ~59 such
    /// tests assert cluster bootstrap as a side effect of testing pagination.
    namespace_lifecycle: bool,
    /// This kind's `openAPIV3Schema`, for a CRD-served kind. `None` for a
    /// cataloged kind, whose shape is already enforced by its generated Rust
    /// type. A CRD's whole promise is that its schema is enforced, so leaving
    /// this unset for a custom resource means accepting data every downstream
    /// controller was written to assume impossible.
    crd_schema: Option<Arc<serde_json::Value>>,
    /// Optional in-process Pod-log reader (the kubelet adapter). `None` for
    /// every kind except Pod (and for Pod when no kubelet is wired — tests /
    /// apiserver-only). When `Some`, the Pod handler's `logs` override
    /// delegates to it; when `None`, `/log` returns a typed `NotFound` (no
    /// fake-empty log). Installed by the runtime via [`Self::with_log_reader`].
    log_reader: Option<Arc<dyn PodLogReader>>,
    /// How often a watch that asked for bookmarks gets one while the store's
    /// revision advances. [`WATCH_BOOKMARK_EVERY`] unless
    /// [`Self::with_bookmark_every`] set it.
    bookmark_every: Duration,
}

impl StoreBackedHandler {
    /// Whether this handler serves the core `v1 Node` kind.
    fn serves_nodes(&self) -> bool {
        self.group.is_empty() && self.version == "v1" && self.kind == "Node"
    }

    /// Replace a Node's `Ready` condition with the one its Lease implies.
    ///
    /// ── ★ DERIVED AT READ, NOT SERVED FROM STORAGE ────────────────────────
    /// A stored `Ready` can only be corrected by something that is still
    /// running. Upstream has one — the node-lifecycle-controller, on another
    /// machine. engenho is one binary, so being embedded removes the observer
    /// rather than the problem, and a kubelet task that wedges leaves
    /// `Ready=True` standing while this apiserver serves it. Measured on rio:
    /// three days, 2,050 failed reconciles, every pod Pending, node Ready.
    ///
    /// Deriving here means there is no stored copy to go stale. The full
    /// reasoning, and what it deliberately does NOT fix (a derived value
    /// changes by the passage of time, so no WATCH event fires — which is why
    /// the kubelet still writes on transition), is on
    /// [`engenho_controllers::node_lease::project_ready_condition`].
    ///
    /// No recursion: this reads a **Lease**, a different kind, straight from
    /// the store rather than back through a handler.
    async fn project_node_readiness(&self, value: &mut Value, name: &str) {
        if !self.serves_nodes() || name.is_empty() {
            return;
        }
        let lease = self
            .store
            .get(&engenho_controllers::node_lease::lease_key(name))
            .await;
        engenho_controllers::node_lease::project_ready_condition(
            value,
            lease.as_ref(),
            &engenho_types::time::now_rfc3339_utc(),
        );
    }
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
            namespace_lifecycle: false,
            crd_schema: None,
            log_reader: None,
            bookmark_every: WATCH_BOOKMARK_EVERY,
        }
    }

    /// Set how often a watch that asked for bookmarks gets one (builder
    /// style). The runtime keeps [`WATCH_BOOKMARK_EVERY`]; a test that
    /// observes bookmarks shortens it rather than waiting five seconds per
    /// bookmark. A zero cadence would disable bookmarks for every client that
    /// asked for them, so it is refused: the cadence stays unchanged.
    #[must_use]
    pub fn with_bookmark_every(mut self, every: Duration) -> Self {
        if !every.is_zero() {
            self.bookmark_every = every;
        }
        self
    }

    /// Install an in-process [`PodLogReader`] (the kubelet adapter) so this
    /// handler's `/log` subresource serves real container logs. Builder style;
    /// the runtime calls this on the Pod handler ONLY. A handler without a log
    /// reader returns a typed `NotFound` for `/log` (no fake-empty log).
    #[must_use]
    pub fn with_log_reader(mut self, reader: Arc<dyn PodLogReader>) -> Self {
        self.log_reader = Some(reader);
        self
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
    /// chain reviews every write — the whole candidate object of a create or
    /// update, whatever its verb, and every delete — BEFORE the store
    /// proposal; an empty chain is a no-op (admits everything).
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<AdmissionChain>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Enforce NamespaceLifecycle — a create into a non-existent namespace is
    /// rejected 404 rather than silently storing a permanently-orphaned
    /// object. Enabled by the assembled Runtime, where the system namespaces
    /// are seeded at boot.
    #[must_use]
    pub fn with_namespace_lifecycle(mut self) -> Self {
        self.namespace_lifecycle = true;
        self
    }

    /// Enforce a CRD's `openAPIV3Schema` (defaults, then validation) on
    /// every write's candidate object — POST, PUT, PATCH and server-side
    /// apply alike. See
    /// [`crate::schema_validation`] for exactly which keywords are checked —
    /// it is type-checking, not full JSON Schema.
    #[must_use]
    pub fn with_crd_schema(mut self, schema: serde_json::Value) -> Self {
        // An empty or non-object schema carries no constraints; storing it
        // would cost a clone per create for nothing.
        if schema.is_object() {
            self.crd_schema = Some(Arc::new(schema));
        }
        self
    }

    /// Run the admission chain over a DELETE of `key`. A delete has no body,
    /// so the review carries `value: None` and the only outcome that matters
    /// is whether the chain denies: `Deny` → typed [`ApiError::Forbidden`]
    /// (HTTP 403); `Allow` and `Mutate` both admit, and a `Mutate` value is
    /// discarded because there is no body for it to rewrite. The chain always
    /// runs when one is attached, so a policy can block deletes.
    async fn admit_delete(&self, key: &ResourceKey, user_info: &UserInfo) -> Result<(), ApiError> {
        let Some(chain) = &self.admission else {
            return Ok(());
        };
        let current = self.store.get(key).await;
        let request = admission_request(AdmissionAction::Delete, key, None, current, user_info);
        match chain.review(request).await {
            AdmissionDecision::Allow | AdmissionDecision::Mutate(_) => Ok(()),
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
    /// The `namespaced` argument is checked against the catalog scope — a
    /// caller whose scope disagrees gets `None` rather than a handler scoped
    /// by a claim the catalog contradicts. An uncataloged kind, or a kind
    /// that exists only in a named group, is also `None`. Neither case
    /// panics: the answer is a value the caller must match on.
    ///
    /// Retained for the existing core-kind test harnesses; new code should
    /// prefer [`Self::for_kind`] (which reads the scope from the catalog)
    /// or [`crate::handlers_from_catalog`] (the full cataloged set).
    #[must_use]
    pub fn for_core_kind(store: Arc<StoreMesh>, kind: &str, namespaced: bool) -> Option<Self> {
        let d = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == kind && d.group.is_empty() && d.namespaced == namespaced)?;
        Some(Self::new(
            store,
            d.group,
            d.version,
            d.kind,
            d.plural,
            d.namespaced,
        ))
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

    fn supports_delete_collection(&self) -> bool {
        // The core-group kinds whose REST storage has no CollectionDeleter.
        // Everything else (ConfigMap, Pod, Secret, Deployment, …) serves
        // deletecollection.
        !matches!(
            self.kind.as_str(),
            "Namespace" | "Binding" | "ComponentStatus"
        )
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
        self.propose_scoped_patch(&key, name, scoped, expected)
            .await
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
        // Value (typed emission; no format! of the wire). A count the parent
        // declares but that is not an integer is a 500, never the default.
        let scale = project_scale(&live)?;
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
        // The parent must be projectable BEFORE anything is written, as
        // upstream converts the old object to a Scale first: otherwise the
        // write lands and the re-projection below answers 500 for it.
        project_scale(&live)?;
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
        let _ = self
            .propose_scoped_patch(&key, name, scoped, expected)
            .await?;
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
        let live = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        // Projectable before anything is written (see `put_scale`).
        project_scale(&live)?;
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

    async fn logs(
        &self,
        namespace: Option<&str>,
        name: &str,
        query: &LogQuery,
    ) -> Result<String, ApiError> {
        // The Pod `/log` subresource. Delegate to the in-process kubelet log
        // reader when one is installed; without it (apiserver-only / tests),
        // serve a typed NotFound — never a fake-empty log.
        let Some(reader) = &self.log_reader else {
            return Err(self.no_subresource("log", name));
        };
        // A `/log` request is always namespaced (Pod is namespaced); the
        // router already enforces the instance path. Default the namespace to
        // "default" defensively (key() would have rejected a scope mismatch on
        // any other verb).
        let ns = namespace.unwrap_or("default");
        reader.read_pod_logs(ns, name, query).await
    }

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let mut v = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        self.project_node_readiness(&mut v, name).await;
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

    async fn current_revision(&self) -> Revision {
        // A scalar read under the store's lock: no catalog clone (T3.2b).
        self.store.current_revision().await
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
        let mut items: Vec<Value> = entries
            .into_iter()
            .filter(|(_, v)| sel.matches(v))
            .map(|(_, v)| strip_type_meta(&v))
            .collect();
        // Same derivation on the LIST path. Doing it only on GET would make
        // `kubectl get node` and `kubectl get nodes` disagree about the same
        // field, which is a worse failure than either answer alone.
        if self.serves_nodes() {
            for item in &mut items {
                let name = item
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                self.project_node_readiness(item, &name).await;
            }
        }
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
                .map(|(_, v)| strip_type_meta(&v))
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
                    accepted.push(strip_type_meta(&v));
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
            let token = last_emitted_key.map(|k| ContinueToken::new(rv, k).encode());
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
    ) -> Result<WatchStart, ApiError> {
        // One read of the current revision serves both resume points.
        // MostRecent ("0"/absent) means "from now, no replay" and starts
        // there; an explicit `At(rev)` must not be past it.
        //
        // ★ `current_revision()`, NOT `current_catalog().revision()`. The
        // latter reads this one `u64` by deep-cloning the entire catalog —
        // every resource plus the 8192-entry watch-replay ring, whose entries
        // each hold a full post-image AND a full pre-image. Because a watch is
        // registered under the same lock `apply` needs, that clone stalled
        // every concurrent WRITE. Measured on rio while FluxCD (dozens of
        // watches) was reconciling: writes went from ~40ms to 27-51s with the
        // daemon burning ~2 cores in memcpy. MostRecent is the COMMON case —
        // every "watch from now" takes this branch.
        let current = self.store.current_revision().await;
        // A resume point the store has not reached is refused, never served
        // from `current`: that answer would leave the client's cache silently
        // stale after a restore or a replay that renumbered history (T3.9a).
        //
        // Revisions only grow between this read and the registration below,
        // so a point at or below `current` is still servable when
        // `watch_from` registers. The opposite race, a write landing in
        // between, refuses a watch that just became servable, which costs
        // that client one relist. The store also judges "ahead" under its own
        // registration lock (store T3.9a-lock), so a rewind between this read
        // and the registration is refused there, through the same mapping.
        if let Some(refusal) = WatchRefusal::ahead_of(from, current) {
            return Ok(WatchStart::Refused(refusal));
        }
        let from_rev = match from {
            ResumePoint::At(rev) => rev,
            ResumePoint::MostRecent => current,
        };
        let opts = WatchOpts {
            from: from_rev,
            buffer: WATCH_CHANNEL_CAPACITY,
            bookmark_every: if allow_bookmarks {
                self.bookmark_every
            } else {
                Duration::ZERO
            },
        };
        // CompactedTooOld at registration is refused like a point ahead of
        // the store: an in-band 410, after which the client re-LISTs and
        // re-WATCHes from the fresh list rv.
        //
        // Registration fails for nothing else: the store reports a replay
        // that overflows the buffer on the stream, after delivering what
        // fitted, and `crate::watch_end` decides whether that watch ends or
        // resumes. An
        // overflow reported here breaks that contract. It is a storage
        // error, never a 410: a 410 would send the client to relist for a
        // condition that is not a compaction.
        match self.store.watch_from(opts).await {
            Ok(stream) => Ok(WatchStart::Streaming(stream)),
            Err(gone) => match WatchRefusal::from_watch_gone(gone) {
                Ok(refusal) => Ok(WatchStart::Refused(refusal)),
                Err(other) => Err(ApiError::StorageError(other.to_string())),
            },
        }
    }

    async fn create(
        &self,
        namespace: Option<&str>,
        body: ObjectBody,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        // A POST names its object in the body, not the URL. Every other rule
        // — name format, NamespaceLifecycle, defaulting, validation, the CRD
        // schema, admission, the create stamps and the create-only CAS — is
        // the write pipeline's, shared with every other verb.
        let name = body
            .as_value()
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::BadRequest("missing metadata.name in request body".into()))?
            .to_string();
        self.write(
            namespace,
            &name,
            WriteRequest::Create(body),
            user_info,
            dry_run,
            &mut PatchFields::unasked(),
        )
        .await
    }

    async fn replace(
        &self,
        namespace: Option<&str>,
        name: &str,
        body: ObjectBody,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        self.write(
            namespace,
            name,
            WriteRequest::Replace(body),
            user_info,
            dry_run,
            &mut PatchFields::unasked(),
        )
        .await
    }

    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
        patch_type: engenho_types::patch::PatchType,
        apply_opts: Option<crate::params::ApplyOptions>,
        user_info: &UserInfo,
        dry_run: DryRun,
        fields: &mut PatchFields,
    ) -> Result<Value, ApiError> {
        let request = WriteRequest::patch(patch, patch_type, apply_opts)?;
        self.write(namespace, name, request, user_info, dry_run, fields)
            .await
    }

    async fn delete(
        &self,
        namespace: Option<&str>,
        name: &str,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        // Unconditional delete (no precondition). The precondition path
        // is [`Self::delete_with_precondition`], driven by `?resourceVersion=`.
        self.delete_with_precondition(namespace, name, None, user_info, dry_run)
            .await
    }

    async fn delete_with_precondition(
        &self,
        namespace: Option<&str>,
        name: &str,
        expected: Option<Revision>,
        user_info: &UserInfo,
        dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        // Admission runs at the API boundary BEFORE the store proposal so
        // a policy can block deletes. The Delete body is None; any Mutate
        // value is ignored (delete has no body to rewrite). The authenticated
        // identity travels into AdmissionRequest.user_info.
        self.admit_delete(&key, user_info).await?;
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
        // DRY-RUN GATE. THE dangerous one: before this existed a
        // `kubectl delete --dry-run=server` really deleted. The pre-delete
        // image is exactly what a real delete returns, so the dry response is
        // truthful rather than approximated.
        if dry_run.is_dry() {
            return Ok(match &prior {
                Some(obj) => inject_type_meta(obj, self.api_version(), &self.kind),
                None => crate::error::delete_status_success(name, &self.kind),
            });
        }
        // Keep a clone for the DeletionPending re-read (the original is
        // moved into the proposed command).
        let key_for_reread = key.clone();
        // ★ EVERY DELETE CARRIES ITS CLOCK (T3.6). Whether the object is
        // removed or goes Terminating is decided by the store at APPLY time,
        // from the object as it stands then — never from `prior`, which was
        // read at a different moment. A finalizer that landed between that
        // read and this proposal used to reach the store with no timestamp,
        // and the store kept the object live while this DELETE answered
        // success. The clock is read ONCE here, at the boundary, through the
        // typed RFC3339 render (the one non-deterministic input), and frozen
        // into the replicated command so every replica stamps the same bytes.
        // A finalizer-free object ignores it and is removed immediately.
        let result = self
            .store
            .propose(ResourceCommand::delete_at(
                key,
                expected,
                Reason::Operator,
                Some(engenho_types::time::now_rfc3339_utc()),
            ))
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        if result.op == ResourceOp::Conflict {
            return Err(self.rv_conflict(name, expected));
        }
        // Return the response BODY:
        //   * finalizer-blocked delete (DeletionPending) → re-read the LIVE
        //     object so the body carries the freshly-stamped
        //     `metadata.deletionTimestamp` (Terminating), exactly like
        //     upstream returns the still-present object on a finalizer block.
        //   * object existed + removed → the removed object exactly as it
        //     was, through the SAME `inject_type_meta` path GET uses
        //     (renders cleanly as json OR protobuf — its GVK is in the pool).
        //   * object absent (idempotent no-op) → a typed
        //     `metav1.Status{status:"Success"}`. Never an empty body.
        if result.op == ResourceOp::DeletionPending {
            // The store kept the object Terminating; surface the live
            // (stamped) object so kubectl shows the deletionTimestamp.
            if let Some(live) = self.store.get(&key_for_reread).await {
                return Ok(inject_type_meta(&live, self.api_version(), &self.kind));
            }
        }
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
                // Subresource SSA (apply on `/status` or `/scale`) is a
                // separate brick — subresource managedFields tracking isn't
                // wired here, so an apply-typed subresource patch carries no
                // ApplyMeta and the store returns a typed rejection (a 400,
                // never a silent merge). The main-object apply path is the
                // one this brick implements.
                apply: None,
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
            return Err(ApiError::BadRequest(msg));
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
                if let Some(rv) = obj.get("metadata").and_then(|m| m.get("resourceVersion")) {
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
                    .with_admission(admission.clone())
                    // NamespaceLifecycle rides with the admission chain
                    // because upstream models it as an admission PLUGIN, and
                    // this is the assembled-cluster path — the one where the
                    // Runtime has seeded the system namespaces at boot. The
                    // plain `handlers_from_catalog` stays lenient for bare
                    // handlers under unit test, which have no seeded
                    // namespaces and are exercising handler mechanics.
                    .with_namespace_lifecycle(),
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
            // ── ★ A CRD'S DECLARED `/status` IS NOW SERVED ──────────────
            // Dynamically-registered CRs used to serve NO subresource
            // (`subresources: &[]` in `new`), so a controller writing
            // `.status` — which is what every controller does — got
            // `does not serve subresource` and could never record an
            // outcome. Measured against pangea-operator on 2026-08-27:
            // it reconciled correctly and then failed writing status,
            // so the CR sat at `Pending` forever with the real result
            // discarded.
            //
            // No type change was needed. `subresources` is `&'static`
            // because the cataloged path sources it from the static
            // RESOURCE_CATALOG — but the VALUE here is a constant, so
            // the dynamic decision is only WHICH static slice to point
            // at. That keeps CRDs off the metadata-leak path the rest of
            // this function uses for genuinely per-CRD strings.
            .with_subresources(if spec.serves_status {
                const STATUS_ONLY: &[Subresource] = &[Subresource::Status];
                STATUS_ONLY
            } else {
                // A CRD that declares no status subresource must keep
                // 404-ing, exactly as kube does — serving it anyway would
                // let a controller silently write a field the CRD author
                // never opted into.
                &[]
            })
            .with_admission(self.admission.clone())
            // The CRD's own schema, ENFORCED. Threaded from CrdEntry through
            // CrdHandlerSpec so the handler can reject a CR that violates the
            // types its author declared.
            .with_crd_schema(spec.schema.clone())
            // A dynamically-registered CR lives in a namespace like anything
            // else, so the same NamespaceLifecycle rule applies.
            .with_namespace_lifecycle(),
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

/// The ONE [`AdmissionRequest`] shape both admit paths hand the chain — the
/// write pipeline's object review and [`StoreBackedHandler::admit_delete`]:
/// the live object as `current` (absent for a create), the proposed `value`
/// (absent for a delete), and the AUTHENTICATED identity threaded from the
/// request's `Extension<UserInfo>`, so a webhook can decide on WHO is acting
/// as well as WHAT.
fn admission_request(
    action: AdmissionAction,
    key: &ResourceKey,
    value: Option<Value>,
    current: Option<Value>,
    user_info: &UserInfo,
) -> AdmissionRequest {
    AdmissionRequest {
        action,
        key: key.clone(),
        value,
        current,
        user_info: user_info.clone(),
    }
}

/// Remove `kind` + `apiVersion` from a LIST item. The `<Kind>List` envelope
/// carries the GVK (`ConfigMapList` / `v1`); each item is TypeMeta-less on
/// the wire — kube-apiserver's codec clears item-level TypeMeta when encoding
/// a list. A stored object may carry TypeMeta (a create/PUT body includes it),
/// so list emission STRIPS rather than merely skips injection. (Single-object
/// GET keeps TypeMeta via [`inject_type_meta`] — that IS conformant.)
fn strip_type_meta(v: &Value) -> Value {
    let mut out = v.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.remove("kind");
        obj.remove("apiVersion");
    }
    out
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

/// Inject `metadata.creationTimestamp` (if absent) from `now`, the write's
/// ONE boundary clock read (the typed RFC3339 render, frozen before the
/// write is planned). Idempotent: a body that already carries one (a client
/// that set it) is left untouched. The store-apply path never reads a clock,
/// so this boundary stamp is the authoritative value carried into the
/// replicated command, and a server-side apply that creates stamps the same
/// instant its `managedFields` entry records.
fn stamp_creation_timestamp(body: &mut Value, now: &str) {
    if let Some(obj) = body.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            if creation_timestamp_is_unset(meta_obj.get("creationTimestamp")) {
                meta_obj.insert(
                    "creationTimestamp".to_string(),
                    Value::String(now.to_string()),
                );
            }
        }
    }
}

/// Stamp `metadata.namespace` from the request (URL) namespace on a
/// namespaced create. The URL namespace is the routing authority (the
/// `ResourceKey` is already built from it); upstream k8s likewise defaults
/// `metadata.namespace` from the request namespace, so the stored body's
/// `metadata.namespace` always reflects where the object lives. Idempotent:
/// a body already carrying the same namespace is unchanged; a body carrying
/// a DIFFERENT one is corrected to the URL authority (k8s would 400, but
/// correcting-to-authority keeps the key + body in agreement and never
/// strands a workload in the wrong namespace). Pure JSON mutation.
fn stamp_namespace(body: &mut Value, namespace: &str) {
    if let Some(obj) = body.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            meta_obj.insert(
                "namespace".to_string(),
                Value::String(namespace.to_string()),
            );
        }
    }
}

/// Preserve the server-owned metadata of the live object across an UPDATE
/// (PUT, PATCH, server-side apply): the candidate is whatever the client's
/// write produced, and these fields are not the client's to write.
///
///   * `creationTimestamp` and `uid` are assigned once at create. A write
///     that omits or alters them keeps the live values; when the live object
///     lacks one, the candidate's stands (as before).
///   * `deletionTimestamp` is upstream's `rest.BeforeUpdate` rule, "an update
///     can never remove/change a deletion timestamp": only DELETE starts
///     termination, and only emptying the finalizers ends it. The candidate
///     carries exactly the live value — kept when the live object is
///     Terminating, dropped when it is not. Before the one write pipeline a
///     PUT that omitted it, or a merge patch that nulled it, brought a
///     Terminating object back to life; and a patch that SET one on a
///     finalizer-free object deleted it through the store's finalizer
///     release. (Upstream answers that last case 422; engenho corrects the
///     field silently, as it does `uid`.)
///
/// Pure JSON mutation; idempotent.
fn preserve_immutable_meta(body: &mut Value, existing: &Value) {
    let existing_meta = existing.get("metadata");
    if let Some(obj) = body.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            for field in ["creationTimestamp", "uid"] {
                if let Some(v) = existing_meta.and_then(|m| m.get(field)) {
                    meta_obj.insert(field.to_string(), v.clone());
                }
            }
            match existing_meta.and_then(|m| m.get("deletionTimestamp")) {
                Some(live) => {
                    meta_obj.insert("deletionTimestamp".to_string(), live.clone());
                }
                None => {
                    meta_obj.remove("deletionTimestamp");
                }
            }
        }
    }
}

/// For a kind that declares a `/status` subresource, a main-object update
/// (PUT, PATCH, server-side apply) MUST NOT change `.status` — k8s writes
/// status ONLY through `/status` and drops any status a client sends to the
/// base-object endpoint. Preserve the LIVE
/// status into the incoming REPLACE body: copy the live `.status` in when the
/// object has one, otherwise drop whatever status the client sent. The
/// symmetric peer of [`StoreBackedHandler::put_status`] scoping a status write
/// to `.status` only — together they enforce the spec/status write-split in
/// both directions. Pure JSON mutation; idempotent.
fn preserve_status(body: &mut Value, existing: &Value) {
    if let Some(obj) = body.as_object_mut() {
        match existing.get("status") {
            Some(live_status) => {
                obj.insert("status".to_string(), live_status.clone());
            }
            None => {
                obj.remove("status");
            }
        }
    }
}

/// `true` iff the `creationTimestamp` slot is effectively UNSET and should
/// be stamped: absent, JSON `null`, an empty string, OR an EMPTY object
/// `{}`. The empty-object case is load-bearing: the kubectl typed clientset
/// posts a protobuf body whose `metav1.Time` (a message) decodes to `{}`
/// when zero — without treating `{}` as unset, a protobuf `create` would
/// leave `creationTimestamp: {}` and AGE would render `<unknown>`.
fn creation_timestamp_is_unset(slot: Option<&Value>) -> bool {
    match slot {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(Value::Object(m)) => m.is_empty(),
        _ => false,
    }
}

/// At Namespace create, seed the three server-side defaults the
/// namespace-lifecycle admission plugin stamps upstream:
///
///   * `status.phase = "Active"`.
///   * `spec.finalizers += "kubernetes"` — the namespace finalizer lives on
///     the typed `Namespace.spec.finalizers` (the legacy per-namespace
///     finalize mechanism the NamespaceController clears via `/finalize`),
///     NOT on the generic `metadata.finalizers`. Upstream k8s never seeds a
///     `metadata.finalizers` on a Namespace.
///   * `metadata.labels["kubernetes.io/metadata.name"] = <name>` — the
///     auto-label every namespace carries so a field-less selector can pin a
///     namespace by name.
///
/// Idempotent: an existing Active phase / `kubernetes` finalizer / name label
/// is left as-is.
fn stamp_namespace_create_defaults(body: &mut Value) {
    const KUBERNETES_FINALIZER: &str = "kubernetes";
    const NAME_LABEL: &str = "kubernetes.io/metadata.name";
    // The namespace name is `metadata.name` (the create path already
    // extracted + validated it before this stamp runs).
    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(obj) = body.as_object_mut() {
        // status.phase = Active
        let status = obj
            .entry("status".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(status_obj) = status.as_object_mut() {
            status_obj
                .entry("phase".to_string())
                .or_insert_with(|| Value::String("Active".to_string()));
        }
        // spec.finalizers += kubernetes (if not already present)
        let spec = obj
            .entry("spec".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(spec_obj) = spec.as_object_mut() {
            let finalizers = spec_obj
                .entry("finalizers".to_string())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Some(arr) = finalizers.as_array_mut() {
                let present = arr.iter().any(|v| v.as_str() == Some(KUBERNETES_FINALIZER));
                if !present {
                    arr.push(Value::String(KUBERNETES_FINALIZER.to_string()));
                }
            }
        }
        // metadata.labels["kubernetes.io/metadata.name"] = <name>
        if let Some(name) = name {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                let labels = meta_obj
                    .entry("labels".to_string())
                    .or_insert_with(|| serde_json::json!({}));
                if let Some(labels_obj) = labels.as_object_mut() {
                    labels_obj
                        .entry(NAME_LABEL.to_string())
                        .or_insert_with(|| Value::String(name));
                }
            }
        }
    }
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
    fn strip_type_meta_removes_list_item_gvk() {
        // A stored object carrying TypeMeta (a create body includes it) is
        // emitted TypeMeta-less as a list item — the <Kind>List envelope
        // carries the GVK, each item does not.
        let v = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "x"},
            "data": {"k": "v"},
        });
        let out = strip_type_meta(&v);
        assert!(out.get("kind").is_none(), "item kind stripped");
        assert!(out.get("apiVersion").is_none(), "item apiVersion stripped");
        // Everything else survives untouched.
        assert_eq!(
            out.pointer("/metadata/name"),
            Some(&Value::String("x".into()))
        );
        assert_eq!(out.pointer("/data/k"), Some(&Value::String("v".into())));
    }

    // ── metadata.namespace stamp (K8s namespaced-object invariant) ────────

    #[test]
    fn stamp_namespace_sets_from_request_when_absent() {
        // A namespaced create whose body carries NO metadata.namespace must
        // be stamped from the request (URL) namespace — the fix that
        // unblocked controller namespace inheritance.
        let mut body = serde_json::json!({"metadata": {"name": "web"}});
        stamp_namespace(&mut body, "team-x");
        assert_eq!(
            body.get("metadata").unwrap().get("namespace").unwrap(),
            "team-x"
        );
    }

    #[test]
    fn stamp_namespace_creates_metadata_when_missing() {
        let mut body = serde_json::json!({"kind": "Deployment"});
        stamp_namespace(&mut body, "team-x");
        assert_eq!(
            body.get("metadata").unwrap().get("namespace").unwrap(),
            "team-x"
        );
    }

    #[test]
    fn stamp_namespace_corrects_to_url_authority() {
        // The URL is the routing authority (the ResourceKey is built from
        // it); a body claiming a DIFFERENT namespace is corrected so the key
        // + body never disagree (never strands a workload in the wrong ns).
        let mut body = serde_json::json!({"metadata": {"name": "web", "namespace": "wrong"}});
        stamp_namespace(&mut body, "team-x");
        assert_eq!(
            body.get("metadata").unwrap().get("namespace").unwrap(),
            "team-x"
        );
    }

    #[test]
    fn stamp_namespace_defaults_are_conformant() {
        // Namespace server-side defaulting must match kube-apiserver:
        //   * spec.finalizers = ["kubernetes"]  (NOT metadata.finalizers)
        //   * metadata.labels["kubernetes.io/metadata.name"] = <name>
        //   * status.phase = "Active"
        let mut body = serde_json::json!({"metadata": {"name": "team-a"}});
        stamp_namespace_create_defaults(&mut body);
        assert_eq!(
            body.pointer("/spec/finalizers"),
            Some(&serde_json::json!(["kubernetes"])),
            "kubernetes finalizer lives on spec.finalizers"
        );
        assert!(
            body.pointer("/metadata/finalizers").is_none(),
            "no generic metadata.finalizers is seeded on a Namespace"
        );
        assert_eq!(
            body.pointer("/metadata/labels/kubernetes.io~1metadata.name"),
            Some(&Value::String("team-a".to_string())),
            "the name auto-label is present"
        );
        assert_eq!(
            body.pointer("/status/phase"),
            Some(&Value::String("Active".to_string()))
        );
    }

    #[test]
    fn stamp_namespace_defaults_are_idempotent() {
        // A body already carrying the defaults is left byte-identical.
        let mut body = serde_json::json!({
            "metadata": {"name": "team-a", "labels": {"kubernetes.io/metadata.name": "team-a"}},
            "spec": {"finalizers": ["kubernetes"]},
            "status": {"phase": "Active"},
        });
        let before = body.clone();
        stamp_namespace_create_defaults(&mut body);
        assert_eq!(body, before, "defaulting is idempotent");
    }

    #[test]
    fn stamp_creation_timestamp_is_idempotent() {
        // Stamped when absent; never bumped when present.
        let mut body = serde_json::json!({"metadata": {"name": "x"}});
        stamp_creation_timestamp(&mut body, "2026-09-19T00:00:00Z");
        let first = body
            .get("metadata")
            .unwrap()
            .get("creationTimestamp")
            .unwrap()
            .clone();
        assert_eq!(
            first, "2026-09-19T00:00:00Z",
            "stamped from the frozen instant"
        );
        stamp_creation_timestamp(&mut body, "2030-01-01T00:00:00Z");
        let second = body
            .get("metadata")
            .unwrap()
            .get("creationTimestamp")
            .unwrap()
            .clone();
        assert_eq!(first, second, "creationTimestamp must not be bumped");
    }
}
