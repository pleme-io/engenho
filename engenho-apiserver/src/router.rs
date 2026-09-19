//! Axum router that wires K8s REST URL patterns to
//! [`ResourceHandler`] trait methods.
//!
//! The router supports kubectl's canonical URLs across BOTH the core
//! group (`/api/v1/…`) and named groups (`/apis/<group>/<version>/…`)
//! through ONE coords → dispatch path: two catch-all routes select the
//! verb handler, the [`crate::coords::ResourceCoords`] extractor hands it the
//! coordinates the request-info layer classified ONCE (the same value authz
//! judged), and scope-agnostic per-method verb handlers resolve
//! a handler via ONE resolver ([`RouterState::lookup`]) then delegate to
//! the shared per-verb `do_*` bodies. This collapses the ~20 hand-fanned
//! per-scope/per-verb route wrappers (5 verbs × 4 scope/group shapes) into
//! one extractor + four method handlers (GET fans LIST/WATCH vs single-GET
//! internally on `coords.name`).
//!
//! Feeding routes:
//!   * `/api/v1/*rest`               → core group.
//!   * `/apis/:group/:version/*rest` → named group.
//!
//! [`crate::coords::RequestInfo::parse`] decomposes the percent-decoded path
//! into the six K8s resource URL shapes (namespaced/cluster ×
//! collection/instance, + an optional subresource segment); the verb handlers
//! pick list-vs-watch + collection-vs-instance from
//! [`crate::coords::RequestInfo::is_watch`] + `coords.name`. A handler-map
//! key is the full `(group, version, plural)` triple with `group=""`/
//! `version="v1"` as the core sentinel, so the old `lookup_core(p)` is
//! exactly `lookup("", "v1", p)` — the two resolvers fold into one.
//!
//! Discovery (`/api`, `/api/v1`, `/apis`, `/apis/<group>/<version>`) is
//! served by [`crate::discovery`] from the same handler set, so what is
//! advertised is exactly what is routable. Those routes are more-specific
//! (static or a shallower param leaf) than the catch-alls and take
//! precedence in matchit.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, Query, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use engenho_kube_proto::{
    self as kube_proto, CONTENT_TYPE_PROTOBUF, Gvk, is_protobuf_content_type,
    response_wants_protobuf,
};
use engenho_store::{WatchEventKind, WatchGone, WatchSignal, WatchStream};
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::Subresource;
use engenho_types::patch::PatchType;
use utoipa::OpenApi;

use crate::discovery;
use crate::error::ApiError;
use crate::handler::ResourceHandler;
use crate::health;
use crate::openapi::ApiDoc;
use crate::params::{
    DryRun, ListWatchParams, ResumePoint, Selectors, WatchGvk, bookmark_line, event_line,
    gvk_ns_matches, status_410_line,
};

/// The dispatch key for a registered handler: `(group, version, plural)`.
/// `group` is `""` for the core group. Keying on the full triple (not the
/// plural alone) makes cross-group plural collisions impossible to
/// mis-route.
pub type HandlerKey = (String, String, String);

#[derive(Clone)]
pub struct RouterState {
    /// `(group, version, plural)` → handler. Lookup is O(1).
    ///
    /// Wrapped in an [`arc_swap::ArcSwap`] so the table is RUNTIME-MUTABLE:
    /// the hot read path ([`Self::lookup`], [`Self::handler_set`]) loads a
    /// snapshot lock-free (a cheap atomic load returning a guarded
    /// `Arc<HashMap>`), while [`Self::register`] / [`Self::unregister`] do a
    /// rare clone-insert-swap via `rcu` (read-copy-update, lost-write-safe
    /// under concurrent CrdController ticks). This is what lets the
    /// CrdController register a `StoreBackedHandler` for a freshly-installed
    /// CRD's served version at runtime — the swap is visible to in-flight
    /// requests immediately. The outer `Arc` makes `RouterState`
    /// cheaply-cloneable into axum `with_state` while every clone shares the
    /// SAME `ArcSwap`, so a `register()` from the controller task is seen by
    /// every per-request handler clone.
    pub handlers: Arc<arc_swap::ArcSwap<HashMap<HandlerKey, Arc<dyn ResourceHandler>>>>,
    /// The typed authenticator chain (X509 → SA → admin-token → anonymous).
    /// Shared (cheap `Arc` clone) into the authn middleware + the
    /// SelfSubjectReview route. Defaults to the bootstrap chain with NO admin
    /// token (every existing `RouterState::new` caller / test); the runtime
    /// installs the configured admin token via [`Self::with_authenticator`].
    pub authenticator: Arc<crate::authn::ChainAuthenticator>,
    /// The typed RBAC authorizer (Brick B). Shared (cheap `Arc` clone) into the
    /// authz middleware + the three SAR route handlers. Defaults to
    /// [`crate::authz::AllowAllAuthorizer`] (authorize-ALL — byte-identical to
    /// the pre-Brick-B behavior, so every existing `RouterState::new` caller /
    /// test is unchanged); the runtime installs the real
    /// [`crate::authz::RbacAuthorizer`] over the store via
    /// [`Self::with_authorizer`].
    pub authorizer: Arc<dyn crate::authz::Authorizer>,
    /// The ServiceAccount token MINTER, for `POST serviceaccounts/<n>/token`.
    ///
    /// `None` ⇒ the `/token` subresource answers a typed error rather than a
    /// token. That is the honest state for a server with no signing key, and
    /// it is deliberately NOT a fallback that mints an unsigned or
    /// fixed-string token — refusing to issue must never degrade into issuing
    /// something that does not authenticate.
    ///
    /// The verifying mirror of this lives in
    /// [`crate::authn::ServiceAccountTokenAuthenticator`]; the runtime builds
    /// both from ONE `SaKeypair`, so a mintable token is always a verifiable
    /// one.
    pub token_issuer: Option<Arc<crate::sa_token::SaIssuer>>,
    /// The process's rollout-gate ledger: what every gate in `Shadow` has
    /// allowed that `Enforce` would have refused. `/metrics` renders it as
    /// `engenho_would_reject_total{gate,reason}`.
    ///
    /// Defaults to a ledger private to this router that logs through
    /// [`crate::metrics::log_would_reject`]. The runtime installs the ONE
    /// ledger it shares with every other gate-owning component via
    /// [`Self::with_would_reject_ledger`], so one scrape shows every gate.
    pub would_reject: Arc<engenho_substrate::WouldRejectLedger>,
}

impl RouterState {
    #[must_use]
    pub fn new(handlers: Vec<Arc<dyn ResourceHandler>>) -> Self {
        let map: HashMap<HandlerKey, Arc<dyn ResourceHandler>> = handlers
            .into_iter()
            .map(|h| (Self::key_of(&h), h))
            .collect();
        Self {
            handlers: Arc::new(arc_swap::ArcSwap::from_pointee(map)),
            // No admin token by default — an opaque bearer authenticates as
            // anonymous (preserving the existing anonymous-kubeconfig path).
            // The runtime overrides this with the configured admin token.
            authenticator: Arc::new(crate::authn::ChainAuthenticator::bootstrap(None)),
            // Authorize-ALL by default — byte-identical to the pre-Brick-B
            // behavior so every existing caller/test is unchanged. The runtime
            // installs the real RBAC authorizer via `with_authorizer`.
            authorizer: Arc::new(crate::authz::AllowAllAuthorizer),
            // No signing key by default: `/token` answers a typed error until
            // the runtime installs the cluster's keypair. NEVER a stub token.
            token_issuer: None,
            would_reject: Arc::new(engenho_substrate::WouldRejectLedger::new(
                crate::metrics::log_would_reject,
            )),
        }
    }

    /// Install the process-wide rollout-gate ledger. Builder style mirroring
    /// [`Self::with_authorizer`]; the runtime passes the same `Arc` to every
    /// component that owns a gate, so `/metrics` counts all of them.
    #[must_use]
    pub fn with_would_reject_ledger(
        mut self,
        ledger: Arc<engenho_substrate::WouldRejectLedger>,
    ) -> Self {
        self.would_reject = ledger;
        self
    }

    /// Install the ServiceAccount token minter. Builder style mirroring
    /// [`Self::with_authenticator`]; the runtime calls this with the SAME
    /// `SaKeypair` it hands the authenticator, so the server cannot mint a
    /// token it would then refuse.
    #[must_use]
    pub fn with_token_issuer(mut self, issuer: Arc<crate::sa_token::SaIssuer>) -> Self {
        self.token_issuer = Some(issuer);
        self
    }

    /// Install the typed authenticator chain (carrying the configured
    /// bootstrap admin token). Builder style; the runtime calls this so the
    /// admin bearer + admin client cert both resolve to the admin identity,
    /// while every other request path is unchanged.
    #[must_use]
    pub fn with_authenticator(
        mut self,
        authenticator: Arc<crate::authn::ChainAuthenticator>,
    ) -> Self {
        self.authenticator = authenticator;
        self
    }

    /// Install the typed RBAC authorizer (Brick B). Builder style mirroring
    /// [`Self::with_authenticator`]; the runtime calls this with
    /// `RbacAuthorizer::new(StoreRbacEnv::new(store.clone()))` so authz enforces
    /// the seeded bootstrap policy + bound roles. Until installed, the default
    /// [`crate::authz::AllowAllAuthorizer`] keeps every request allowed.
    #[must_use]
    pub fn with_authorizer(mut self, authorizer: Arc<dyn crate::authz::Authorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }

    /// The dispatch key `(group, version, plural)` for a handler.
    fn key_of(h: &Arc<dyn ResourceHandler>) -> HandlerKey {
        (
            h.group().to_string(),
            h.version().to_string(),
            h.plural().to_string(),
        )
    }

    /// Register a handler at runtime (clone-insert-swap via `rcu`). Used by
    /// the apiserver-side [`crate::DynamicHandlerSink`] impl when the
    /// `CrdController` observes a newly-served CRD version: it builds a
    /// `StoreBackedHandler` for `(group, version, plural)` + names and lands
    /// it here. `rcu` (read-copy-update) retries on a concurrent swap, so
    /// two racing register/unregister calls never lose a write. Idempotent:
    /// re-registering the same `(group, version, plural)` overwrites (a
    /// harmless refresh — the same GVK handler).
    pub fn register(&self, h: Arc<dyn ResourceHandler>) {
        let key = Self::key_of(&h);
        self.handlers.rcu(|cur| {
            let mut m = HashMap::clone(cur);
            m.insert(key.clone(), h.clone());
            m
        });
    }

    /// Unregister the handler keyed by `(group, version, plural)` (the CRD
    /// GC path: a deleted CRD ⇒ its CR handler is removed, so subsequent CR
    /// access resolves to a typed `NotFound`). Returns `true` iff a handler
    /// was present + removed. `rcu` makes the remove lost-write-safe under
    /// concurrent ticks.
    pub fn unregister(&self, group: &str, version: &str, plural: &str) -> bool {
        let key: HandlerKey = (group.to_string(), version.to_string(), plural.to_string());
        // Pre-check on a snapshot for the boolean report, then swap. The
        // window between the check + the rcu can only flip present→absent
        // (no concurrent re-register of the SAME deleted CRD key in the same
        // tick), so the reported bool matches the observed-desired transition
        // the CrdController acts on. The rcu itself is the authoritative,
        // lost-write-safe removal.
        let present = self.handlers.load().contains_key(&key);
        self.handlers.rcu(|cur| {
            let mut m = HashMap::clone(cur);
            m.remove(&key);
            m
        });
        present
    }

    /// The registered handlers, for discovery folding. Order is
    /// unspecified (HashMap); discovery sorts for determinism. Returns
    /// OWNED `Arc`s (cloned out of the loaded snapshot) because the
    /// `ArcSwap` guard borrow can't escape the call — discovery's three
    /// builders consume `h.as_ref()` / `h.group()` identically on an owned
    /// `Arc`. The fold reads ONE snapshot, so a CRD-registered handler
    /// appears in discovery AND routing atomically.
    #[must_use]
    pub fn handler_set(&self) -> Vec<Arc<dyn ResourceHandler>> {
        self.handlers.load().values().cloned().collect()
    }

    /// Resolve a CORE-group handler by plural (keyed on `("","v1",plural)`).
    /// A thin `#[inline]` shim over [`Self::lookup`] with the core sentinel
    /// — the canonical statement that `lookup_core(p)` IS `lookup("", "v1",
    /// p)`. The coords path folded both old resolvers onto [`Self::lookup`]
    /// directly, so the only remaining caller is the fold-equivalence test;
    /// `#[cfg(test)]`-gated so the production build has no dead method.
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when no core handler is registered for `plural`.
    #[cfg(test)]
    #[inline]
    fn lookup_core(&self, plural: &str) -> Result<Arc<dyn ResourceHandler>, ApiError> {
        self.lookup("", "v1", plural)
    }

    /// Resolve a handler by the full `(group, version, plural)` triple.
    /// The ONE resolver both the core (`group=""`) and grouped scopes fold
    /// into — the map key is already the full triple with `group=""`/
    /// `version="v1"` as the core sentinel, so this subsumes the old
    /// `lookup_core`.
    ///
    /// Returns an OWNED `Arc<dyn ResourceHandler>` (cloned from the loaded
    /// snapshot) rather than `&Arc<…>`: the `ArcSwap` guard borrow can't
    /// escape, so the resolved handler is cloned out. Cloning an `Arc` is a
    /// single atomic refcount bump — negligible on the hot path, and the
    /// verb handlers already cloned for the watch-unfold path.
    ///
    /// The `NotFound` message text branches on whether `group` is the core
    /// sentinel so the core-plural-miss body stays `unknown core kind
    /// plural: {plural}` (behavior-preserving) while named-group misses
    /// keep `unknown kind: {g}/{v}/{plural}`.
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when no handler is registered for the triple.
    fn lookup(
        &self,
        group: &str,
        version: &str,
        plural: &str,
    ) -> Result<Arc<dyn ResourceHandler>, ApiError> {
        let snap = self.handlers.load();
        snap.get(&(group.to_string(), version.to_string(), plural.to_string()))
            .cloned()
            .ok_or_else(|| {
                if group.is_empty() {
                    // Core-group miss — preserve the exact legacy text so
                    // the rendered Status body is byte-identical.
                    ApiError::NotFound(format!("unknown core kind plural: {plural}"))
                } else {
                    ApiError::NotFound(format!("unknown kind: {group}/{version}/{plural}"))
                }
            })
    }
}

pub fn build(state: RouterState) -> Router {
    // The authn middleware reads the verified client cert (from the TLS
    // acceptor) + the Authorization header into the typed authenticator chain,
    // writes the resolved `UserInfo` into request extensions, and lets the
    // request proceed. Authorize-ALL is retained — it NEVER 401/403s on authn
    // EXCEPT for a typed-bad credential (a structurally-SA bearer this brick
    // can't validate → 401). The SelfSubjectReview route is registered FIRST so
    // the layer wraps it too (kubectl auth whoami authenticates as anonymous
    // and still gets a typed answer).
    let authenticator = state.authenticator.clone();
    let authorizer = state.authorizer.clone();
    let router = build_routes(state);
    // Layer order (axum: the LAST `.layer()` is OUTERMOST), outside in:
    //
    //   1. request-info — classifies the request ONCE (method + decoded path +
    //      `watch`) into `Extension<RequestInfo>`; upstream runs
    //      `WithRequestInfo` before authentication too.
    //   2. authn — populates `Extension<UserInfo>`.
    //   3. authz — judges the stored RequestInfo as that user.
    //
    // All three wrap the WHOLE route table (discovery + SAR + health + the
    // fallback), and every route comes from `route_table`, so no route can be
    // reached without a classification. Dispatch reads the same stored value
    // (the `ResourceCoords` / `RequestInfo` extractors), so what authz judged
    // is what runs. The always-allow check is the first branch inside the
    // authz middleware after the lookup (health/version are pre-authz so they
    // work with an unseeded RBAC store).
    router
        .layer(axum::middleware::from_fn(
            move |req: axum::http::Request<Body>, next: axum::middleware::Next| {
                let authorizer = authorizer.clone();
                async move { authz_middleware(authorizer, req, next).await }
            },
        ))
        .layer(axum::middleware::from_fn(
            move |req: axum::http::Request<Body>, next: axum::middleware::Next| {
                let authenticator = authenticator.clone();
                async move { authn_middleware(authenticator, req, next).await }
            },
        ))
        .layer(axum::middleware::from_fn(request_info_middleware))
}

/// The router built from [`route_table`], without the layers. Split out from
/// [`build`] so the layers wrap the WHOLE table — every route (incl.
/// discovery / health / selfsubjectreviews) is classified and authenticated
/// first.
fn build_routes(state: RouterState) -> Router {
    route_table()
        .into_iter()
        .fold(Router::new(), |router, (path, methods)| {
            router.route(path, methods)
        })
        .with_state(state)
}

/// Every route this server serves, as `(path pattern, methods)`. The ONE
/// source of routes: [`build_routes`] folds it into the router, and the
/// route-coverage test walks it to prove each route is classified before
/// authz. A route added anywhere else would escape both.
fn route_table() -> Vec<(&'static str, axum::routing::MethodRouter<RouterState>)> {
    vec![
        // ── resources: ONE coords→dispatch path, fed by two catch-alls ─
        //
        // The ~20 hand-fanned per-scope/per-verb wrappers collapse into
        // the [`ResourceCoords`] extractor + the scope-agnostic verb
        // handlers (one per HTTP method: GET handles both LIST/WATCH and
        // single-GET, branching on `coords.name`; POST/PATCH/DELETE map to
        // create/patch/delete). Two catch-all routes pick the HANDLER:
        //
        //   * `/api/v1/*rest`               → core group.
        //   * `/apis/:group/:version/*rest` → named group.
        //
        // The route params are never read. The coordinates come from the
        // request-info layer's one classification of the DECODED path (the
        // same value authz judged), so an encoded `/` in any segment moves
        // authz and dispatch together or not at all.
        //
        // Each catch-all registers EXACTLY the methods the K8s wire
        // supports for a resource path; `RequestInfo::is_watch` +
        // `coords.name` pick list-vs-watch + collection-vs-instance.
        // An unrouted method on a matched resource path yields axum's 405
        // — the same terminal semantics the legacy MethodRouter gave (e.g.
        // PUT was never registered, so it 405'd then too). The discovery /
        // openapi / health routes below are MORE-SPECIFIC (static or a
        // shallower param leaf) and take precedence over the catch-alls in
        // matchit (verified: `/api/v1` exact beats `/api/v1/*rest`, and
        // `/apis/:g/:v` coexists with the one-segment-deeper catch-all).
        (
            "/api/v1/*rest",
            get(resource_get_or_list)
                .post(resource_create)
                .put(resource_put)
                .patch(resource_patch)
                .delete(resource_delete),
        ),
        (
            "/apis/:group/:version/*rest",
            get(resource_get_or_list)
                .post(resource_create)
                .put(resource_put)
                .patch(resource_patch)
                .delete(resource_delete),
        ),
        // ── discovery ─────────────────────────────────────────────────
        ("/api", get(discovery::api_versions)),
        ("/api/v1", get(discovery::core_resources)),
        ("/apis", get(discovery::api_groups)),
        ("/apis/:group/:version", get(discovery::group_resources)),
        // ── openapi ───────────────────────────────────────────────────
        // `/openapi.json` keeps the utoipa-derived description of engenho's
        // own REST surface (SDK/codegen consumers). `/openapi/v3` is the
        // K8s OpenAPI-v3 DISCOVERY surface kubectl `apply --validate` +
        // `explain` consume — a typed index + per-group vendored schemas,
        // scoped to exactly the cataloged groups.
        ("/openapi.json", get(openapi_spec)),
        ("/openapi/v3", get(openapi_v3_index)),
        ("/openapi/v3/api/v1", get(openapi_v3_core)),
        ("/openapi/v3/apis/:group/:version", get(openapi_v3_group)),
        // ── authentication.k8s.io SelfSubjectReview (kubectl auth whoami) ──
        // A discovery-light special route (NOT a store-backed kind): it echoes
        // the authenticated identity from `Extension<UserInfo>` back as a typed
        // SelfSubjectReview. POST per the upstream API; the body is ignored
        // (the identity comes from the credential, not the body).
        (
            "/apis/authentication.k8s.io/v1/selfsubjectreviews",
            axum::routing::post(self_subject_review),
        ),
        // ── authorization.k8s.io SubjectAccessReview family (Brick B) ──────
        // Discovery-light special routes (NOT store-backed kinds), modeled on
        // the SelfSubjectReview route above. `kubectl auth can-i` POSTs to
        // these; they build Attributes from the spec (SAR) or the caller
        // (SelfSAR) + call the shared `state.authorizer`. They run AFTER authn
        // (Extension<UserInfo> = the caller) and through the authz layer (a
        // SubjectAccessReview create is itself authorized — granted to
        // system:masters + via the system:basic-user policy for self-reviews).
        (
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            axum::routing::post(crate::authz::sar::subject_access_review),
        ),
        (
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            axum::routing::post(crate::authz::sar::self_subject_access_review),
        ),
        (
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            axum::routing::post(crate::authz::sar::self_subject_rules_review),
        ),
        // ── version + health (no RouterState; kubectl/client-go probe
        //    these before they will trust the server) ──────────────────
        ("/version", get(health::version)),
        ("/readyz", get(health::readyz)),
        ("/livez", get(health::livez)),
        ("/healthz", get(health::healthz)),
        // Prometheus. RBAC already classified this as a non-resource URL
        // before anything served it — authz could authorize a path that
        // did not exist.
        ("/metrics", get(crate::metrics::metrics)),
    ]
}

/// The OpenAPI v3 spec — the central machine-readable description
/// from which gRPC, GraphQL, and downstream SDKs derive. Per the
/// multi-face plan in docs/API-SURFACE.md.
async fn openapi_spec() -> impl IntoResponse {
    Json(ApiDoc::openapi())
}

// ── authentication middleware + the typed UserInfo extractor ───────────────

/// A per-request extractor that yields the authenticated [`UserInfo`] from
/// request extensions (inserted by [`authn_middleware`]). Defaults to
/// [`UserInfo::anonymous`] when absent — so a handler reached WITHOUT the authn
/// layer (a direct-call test path) is robust, never a 500. Infallible.
pub struct ExtractUserInfo(pub UserInfo);

#[axum::async_trait]
impl<S> axum::extract::FromRequestParts<S> for ExtractUserInfo
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let user = parts
            .extensions
            .get::<UserInfo>()
            .cloned()
            .unwrap_or_else(UserInfo::anonymous);
        Ok(ExtractUserInfo(user))
    }
}

/// The authn middleware — the thin axum adapter over the typed
/// [`crate::authn::ChainAuthenticator`]. Extracts the request material into the
/// chain's MOCKABLE [`crate::authn::RequestCreds`] input (the verified client
/// cert from the TLS acceptor's extension + the `Authorization: Bearer` token),
/// runs the chain, and writes the resolved [`UserInfo`] into request
/// extensions for downstream handlers + admission.
///
/// Authorize-ALL is RETAINED: this NEVER 401/403s on authn EXCEPT a typed-bad
/// credential (a structurally-SA bearer this brick can't validate → 401). A
/// no-credential request resolves to anonymous and proceeds.
async fn authn_middleware(
    authenticator: Arc<crate::authn::ChainAuthenticator>,
    mut req: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    let creds = crate::authn::RequestCreds {
        // The TLS acceptor injected the verified peer cert as an extension when
        // the client presented one; absent for token/anonymous clients.
        client_cert: req
            .extensions()
            .get::<crate::pki::VerifiedClientCert>()
            .cloned(),
        bearer: bearer_from_headers(req.headers()),
    };
    match authenticator.authenticate(&creds) {
        Ok(user_info) => {
            req.extensions_mut().insert(user_info);
            next.run(req).await
        }
        // A typed-bad credential (e.g. a structurally-SA bearer this brick
        // can't validate) → a typed 401 K8s Status. This is the ONLY authn
        // rejection; everything else resolves to an identity + proceeds.
        Err(e) => ApiError::Unauthorized(e.to_string()).into_response(),
    }
}

// ── authorization middleware (Brick B) ─────────────────────────────────────

/// The pre-authz TIER-1 always-allow set: `/healthz`, `/livez`, `/readyz`,
/// `/version`. These are wired in [`build_routes`] with NO `RouterState`;
/// kubectl/client-go probe them BEFORE trusting the server, so they MUST work
/// even with an empty/unseeded RBAC store (e.g. during boot before
/// `seed_bootstrap_rbac` lands). This is the load-bearing reason they are a
/// fixed allow-list HERE (in the authz middleware) and NOT a binding.
///
/// Discovery (`/api`, `/apis`, `/openapi/v3`) is DELIBERATELY NOT here — it is
/// TIER-2 (binding-driven via the `system:discovery` + `system:public-info-viewer`
/// bootstrap ClusterRoleBindings), so anonymous discovery resolves THROUGH the
/// authorizer, not pre-authz.
fn is_always_allowed(path: &str) -> bool {
    matches!(path, "/healthz" | "/livez" | "/readyz" | "/version")
        // `/version/...` (e.g. a trailing slash) — kubectl hits `/version`
        // exactly, but be defensive about a trailing segment.
        || path.starts_with("/version/")
}

/// The request-info middleware — the ONE parse. Classifies the request from
/// its method + URI ([`crate::coords::RequestInfo::parse`]: percent-decoded
/// path, `watch` flag) and stores the result in the request extensions, where
/// authz and dispatch both read it. A path that is not UTF-8 once decoded is a
/// typed 400 here, before any identity is resolved.
async fn request_info_middleware(
    mut req: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    match crate::coords::RequestInfo::parse(req.method(), req.uri()) {
        Ok(info) => {
            req.extensions_mut().insert(info);
            next.run(req).await
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// The authz middleware — the thin axum adapter over the typed
/// [`crate::authz::Authorizer`]. Branch order:
///
///   0. Read the [`crate::coords::RequestInfo`] the request-info layer stored.
///      None → a typed 500 ([`crate::coords::RequestInfoError::Missing`]); the
///      path is never parsed here.
///   1. [`is_always_allowed`] (`/healthz`, `/livez`, `/readyz`, `/version`)
///      → proceed (pre-authz; works with an unseeded RBAC store).
///   2. Build [`crate::authz::Attributes`] from that `RequestInfo` + the
///      resolved [`UserInfo`] (inserted by the authn layer).
///   3. `authorizer.authorize(&attrs)` →
///      * [`crate::authz::Decision::Allow`] → `next.run(req)`.
///      * `Deny` / `NoOpinion` → a typed [`ApiError::AuthzForbidden`] 403
///        (the standard RBAC `Status` body via [`crate::error::forbidden_message`]).
async fn authz_middleware(
    authorizer: Arc<dyn crate::authz::Authorizer>,
    req: axum::http::Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    let attrs = match crate::coords::RequestInfo::from_extensions(req.extensions()) {
        Err(e) => return ApiError::from(e).into_response(),
        // TIER 1 — pre-authz always-allow (health/version).
        Ok(info) if info.non_resource_url().is_some_and(is_always_allowed) => None,
        Ok(info) => {
            let user = req
                .extensions()
                .get::<UserInfo>()
                .cloned()
                .unwrap_or_else(UserInfo::anonymous);
            Some(crate::authz::Attributes::for_request(user, info))
        }
    };
    let Some(attrs) = attrs else {
        return next.run(req).await;
    };

    match authorizer.authorize(&attrs).await {
        crate::authz::Decision::Allow => next.run(req).await,
        // NoOpinion (the RBAC default-deny) + an explicit Deny both render the
        // standard RBAC 403 with the typed `forbidden: User ... cannot ...`
        // message (NOT the admission `"admission denied:"` prefix).
        crate::authz::Decision::Deny | crate::authz::Decision::NoOpinion => {
            ApiError::AuthzForbidden(crate::error::forbidden_message(&attrs)).into_response()
        }
    }
}

/// Extract the `Authorization: Bearer <token>` value, if present. Case-
/// insensitive on the `Bearer` scheme per RFC 6750.
fn bearer_from_headers(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let rest = value.strip_prefix("Bearer ").or_else(|| {
        // Case-insensitive scheme match for robustness.
        value
            .split_once(' ')
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, tok)| tok)
    })?;
    let token = rest.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

// ── authentication.k8s.io/v1 SelfSubjectReview (kubectl auth whoami) ────────

/// The typed `authentication.k8s.io/v1.SelfSubjectReview` response — what
/// `kubectl auth whoami` reads. Built with serde (TYPED EMISSION), never a
/// `json!()` of the wire. `status.userInfo` echoes the authenticated identity.
#[derive(serde::Serialize)]
struct SelfSubjectReview {
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    kind: &'static str,
    status: SelfSubjectReviewStatus,
}

/// The `status` block of a [`SelfSubjectReview`] — carries the resolved
/// [`UserInfo`] under `userInfo` (camelCase, matching authentication/v1).
#[derive(serde::Serialize)]
struct SelfSubjectReviewStatus {
    #[serde(rename = "userInfo")]
    user_info: UserInfo,
}

/// `POST /apis/authentication.k8s.io/v1/selfsubjectreviews` → echo the
/// authenticated identity. The body is ignored (the identity comes from the
/// credential, not the request body). Reached AFTER the authn layer, so
/// `Extension<UserInfo>` is the resolved identity (admin / anonymous / cert).
async fn self_subject_review(user_info: ExtractUserInfo) -> Result<Response, ApiError> {
    let review = SelfSubjectReview {
        api_version: "authentication.k8s.io/v1",
        kind: "SelfSubjectReview",
        status: SelfSubjectReviewStatus {
            user_info: user_info.0,
        },
    };
    Ok((StatusCode::CREATED, Json(review)).into_response())
}

// ── K8s OpenAPI-v3 discovery surface (/openapi/v3) ─────────────────────
//
// kubectl's client-side `--validate` path + `kubectl explain` fetch the K8s
// OpenAPI-v3 DISCOVERY document at `/openapi/v3` — a typed index mapping each
// served `(group, version)` to a `serverRelativeURL` — then GET each
// per-group schema document. We serve the BLAKE3-attested vendored bodies
// verbatim (already valid OpenAPI 3.0.0; NEVER round-tripped through utoipa),
// scoped to exactly the cataloged groups so the index advertises only what
// is routable + schema-served (mirroring the discovery invariant).

/// The `/openapi/v3` discovery document — `{ paths: { <key>:
/// { serverRelativeURL } } }`. Typed serde struct per the ★★ TYPED EMISSION
/// rule (NOT `json!()`). `paths` keys are `api/v1` for core and
/// `apis/<group>/<version>` for named groups.
#[derive(serde::Serialize)]
struct OpenApiV3Discovery {
    paths: std::collections::BTreeMap<String, OpenApiV3PathItem>,
}

/// One entry in the [`OpenApiV3Discovery`] index: the relative URL of the
/// per-group schema document, with the vendored BLAKE3 as the `?hash=`
/// cache key (kubectl caches the document keyed on this digest).
#[derive(serde::Serialize)]
struct OpenApiV3PathItem {
    #[serde(rename = "serverRelativeURL")]
    server_relative_url: String,
}

/// `GET /openapi/v3` → the K8s OpenAPI-v3 discovery index, built by
/// iterating the engenho-types `SERVED` table (the single source scoped to
/// the cataloged groups). Each entry's `serverRelativeURL` points at the
/// per-group document endpoint with the attested hash for caching.
async fn openapi_v3_index() -> impl IntoResponse {
    let mut paths = std::collections::BTreeMap::new();
    for d in engenho_types::openapi_v3::SERVED {
        let key = d.index_key();
        // serverRelativeURL = "/openapi/v3/<key>?hash=<blake3>". Built by
        // concatenation (no format! of the URL) — the pieces are all typed.
        let url = ["/openapi/v3/", &key, "?hash=", d.blake3].concat();
        paths.insert(
            key,
            OpenApiV3PathItem {
                server_relative_url: url,
            },
        );
    }
    Json(OpenApiV3Discovery { paths })
}

/// `GET /openapi/v3/api/v1` → the core group's vendored OpenAPI v3 document,
/// served verbatim as `application/json`.
async fn openapi_v3_core() -> Result<Response, ApiError> {
    serve_openapi_v3_document("", "v1")
}

/// `GET /openapi/v3/apis/<group>/<version>` → that group's vendored OpenAPI
/// v3 document verbatim, or a 404 K8s Status for an uncataloged pair.
async fn openapi_v3_group(
    Path((group, version)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    serve_openapi_v3_document(&group, &version)
}

/// Serve the vendored OpenAPI v3 document for `(group, version)` verbatim
/// (Content-Type `application/json`), or a typed 404 for an uncataloged
/// pair. The bytes are already valid OpenAPI 3.0.0 — emitted as-is, never
/// re-serialized.
fn serve_openapi_v3_document(group: &str, version: &str) -> Result<Response, ApiError> {
    match engenho_types::openapi_v3::document_for(group, version) {
        Some(body) => {
            Ok((StatusCode::OK, [(CONTENT_TYPE, "application/json")], body).into_response())
        }
        None => Err(ApiError::NotFound(format!(
            "no OpenAPI v3 document for group/version {group}/{version}"
        ))),
    }
}

// ── content negotiation (the protobuf <-> JSON boundary) ───────────────
//
// kubectl's typed clientset (imperative `kubectl create configmap/secret/
// deployment …`) negotiates `application/vnd.kubernetes.protobuf` ONCE at
// client construction and never renegotiates — a 415 is a TERMINAL error,
// not a fall-back-to-JSON trigger (proven empirically). So the write
// handlers extract the raw body + headers themselves and dispatch on
// Content-Type through the typed `engenho-kube-proto` codec, with a
// proper ApiError-rendered 415 K8s Status for anything else (NEVER axum's
// built-in plain-text JsonRejection). The downstream handler/store/
// admission/read-back pipeline stays serde_json::Value-typed.

/// The codec to use for a RESPONSE body, negotiated from the request
/// `Accept` header. kubectl's typed clientset sends
/// `Accept: application/vnd.kubernetes.protobuf,application/json`
/// (protobuf first); the dynamic/unstructured client sends
/// `Accept: application/json`.
#[derive(Clone, Copy)]
enum ResponseCodec {
    Json,
    Protobuf,
    /// `Accept: application/json;as=Table;v=1;g=meta.k8s.io` — server-side
    /// printing. kubectl and k9s BOTH default to this for list views and let
    /// the server choose the columns; without it they receive a plain List and
    /// cannot draw a table at all.
    Table(crate::table::IncludeObject),
    /// `Accept: …;as=PartialObjectMetadataList;g=meta.k8s.io;v=1` — the
    /// metadata-only projection every controller-runtime METADATA cache asks
    /// for. A client requesting it installs a decoder for exactly that kind,
    /// so answering with the full List is undecodable, not a superset.
    PartialMetadata,
}

impl ResponseCodec {
    /// Negotiate from the request headers' `Accept`. Defaults to JSON
    /// when `Accept` is absent or does not list protobuf.
    /// Negotiate from the request's `Accept`.
    ///
    /// # Errors
    /// [`ApiError::NotAcceptable`] (HTTP 406) when `Accept` is present and
    /// names NO type this server can produce. Measured 2026-08-28:
    /// `Accept: text/html` returned **200 with a JSON body** — the server
    /// ignored the header entirely and sent something the client had
    /// explicitly said it could not read.
    fn from_headers(headers: &HeaderMap) -> Result<Self, ApiError> {
        let accept = headers
            .get(ACCEPT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // An absent or empty Accept means "anything" — default to JSON, never
        // 406. Only an Accept that is PRESENT and names nothing servable is a
        // failed negotiation.
        if !accept.trim().is_empty() && !accept_is_servable(accept) {
            return Err(ApiError::NotAcceptable(
                [
                    "only the following media types are acceptable: application/json, \
                     application/yaml, ",
                    CONTENT_TYPE_PROTOBUF,
                    "; got ",
                    accept,
                ]
                .concat(),
            ));
        }
        // Table is checked BEFORE protobuf because kubectl sends both in one
        // Accept header (`...;as=Table;...,application/json`) and the Table
        // range is the specific request; falling through to protobuf/JSON
        // would silently ignore it.
        if crate::table::accept_wants_table(accept) {
            // `includeObject` is a query parameter, not part of Accept. The
            // upstream default (Metadata) is what kubectl and k9s rely on.
            Ok(ResponseCodec::Table(crate::table::IncludeObject::default()))
        } else if crate::table::accept_wants_partial_metadata(accept) {
            // BEFORE protobuf, for the same reason Table is: the metadata
            // client sends ONE Accept naming both
            // (`application/vnd.kubernetes.protobuf;as=PartialObjectMetadataList;…,application/json`),
            // and the `as=` range is the specific request. Falling through to
            // protobuf answered with a protobuf-encoded FULL list, which is
            // where `invalid character 'k'` came from — the JSON decoder
            // meeting the `k8s\0` protobuf magic.
            //
            // Served as JSON deliberately: the same Accept lists
            // `application/json`, and client-go selects its decoder from the
            // response Content-Type, so this is honest negotiation rather
            // than ignoring the preference.
            Ok(ResponseCodec::PartialMetadata)
        } else if response_wants_protobuf(accept) {
            Ok(ResponseCodec::Protobuf)
        } else {
            Ok(ResponseCodec::Json)
        }
    }
}

/// Whether ANY media range in `accept` names something this server produces.
///
/// Per-range, because `Accept` is a list and one servable range is enough.
/// `*/*` and `application/*` are wildcards every real client sends, and
/// rejecting them would break kubectl before it made a single call.
fn accept_is_servable(accept: &str) -> bool {
    accept.split(',').any(|range| {
        let media = range
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        media == "*/*"
            || media == "application/*"
            || media == "application/json"
            || media == "application/yaml"
            || media == CONTENT_TYPE_PROTOBUF
    })
}

/// The GVK a handler speaks, as the K8s wire `(apiVersion, kind)` —
/// the key the protobuf codec uses to select the per-kind descriptor.
fn handler_gvk(h: &Arc<dyn ResourceHandler>) -> Gvk {
    Gvk::new(h.api_version(), h.kind())
}

/// Decode a write request body into the `serde_json::Value` the handler
/// pipeline expects, dispatching on `Content-Type`:
///
///   * `application/json` (or absent → JSON) → `serde_json::from_slice`.
///   * `application/vnd.kubernetes.protobuf` → the typed
///     `engenho-kube-proto` codec (magic + `runtime.Unknown` + per-kind
///     `DynamicMessage` → Value).
///   * anything else → a typed [`ApiError::UnsupportedMediaType`] (HTTP
///     415, proper K8s `Status` body) — NOT axum's plain-text rejection.
fn decode_write_body(headers: &HeaderMap, raw: &[u8]) -> Result<serde_json::Value, ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media = content_type.split(';').next().unwrap_or("").trim();
    if media.is_empty() || media.eq_ignore_ascii_case("application/json") {
        serde_json::from_slice(raw)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON request body: {e}")))
    } else if is_protobuf_content_type(content_type) {
        Ok(kube_proto::decode_protobuf(raw)?)
    } else {
        Err(ApiError::UnsupportedMediaType(format!(
            "the body of the request was in an unsupported format - \
             accepted media types are application/json, \
             {CONTENT_TYPE_PROTOBUF}; got {media:?}"
        )))
    }
}

/// Decode a PATCH request body AND resolve the typed patch algorithm from the
/// `Content-Type`. The media type is the load-bearing signal: it tells the
/// store which of the four algorithms (merge / strategic / json-patch / apply)
/// to run. The previous `decode_patch_body` discarded it, funnelling all four
/// content-types into one untyped `Value` so every patch ran as RFC7396 merge
/// — the first erasure point this fixes.
///
/// The four K8s patch content-types are all JSON-family and parse as JSON.
/// A missing/empty Content-Type defaults to `Strategic` (kube-apiserver's
/// default patch type). `application/apply-patch+yaml` resolves to
/// [`PatchType::Apply`] (typed-deferred server-side apply — the store returns
/// a typed 415, never a silent strategic/merge fallback). A protobuf
/// full-object replace decodes via the codec (typed as `Merge`, the
/// replace-shaped algorithm). An unrecognized media type is a typed 415.
fn decode_patch(
    headers: &HeaderMap,
    raw: &[u8],
    gvk: &Gvk,
) -> Result<(serde_json::Value, PatchType), ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media = content_type.split(';').next().unwrap_or("").trim();

    // Plain `application/json` is a full-object replace shape — treat it as a
    // merge patch (RFC7396-merge of a full object is an idempotent replace).
    if media.eq_ignore_ascii_case("application/json") {
        let v = serde_json::from_slice(raw)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON patch body: {e}")))?;
        return Ok((v, PatchType::Merge));
    }

    if is_protobuf_content_type(content_type) {
        // A protobuf full-object replace (rare): decode via the codec, typed
        // as a merge (full-object replace).
        let _ = gvk;
        let v = kube_proto::decode_protobuf(raw)?;
        return Ok((v, PatchType::Merge));
    }

    // The four K8s patch content-types (+ the empty default) resolve through
    // the typed inverse of `Patch::content_type`. An unknown media type → 415.
    let Some(patch_type) = PatchType::from_content_type(media) else {
        return Err(ApiError::UnsupportedMediaType(format!(
            "the body of the patch request was in an unsupported format; got {media:?}"
        )));
    };
    let v = serde_json::from_slice(raw)
        .map_err(|e| ApiError::BadRequest(format!("invalid JSON patch body: {e}")))?;
    Ok((v, patch_type))
}

/// Render a handler-returned `serde_json::Value` as the negotiated
/// response codec, with the given HTTP status. JSON → `axum::Json`;
/// protobuf → the typed `engenho-kube-proto` encoder (magic +
/// `runtime.Unknown` + per-kind `DynamicMessage`) with
/// `Content-Type: application/vnd.kubernetes.protobuf`.
/// Render a LIST envelope through the negotiated codec.
///
/// Split from [`render_object`] only because a list's Table conversion carries
/// the LIST's metadata (resourceVersion, continue) rather than an object's —
/// a paging client that lost the continue token would silently stop at the
/// first page.
fn render_list(
    codec: ResponseCodec,
    gvk: &Gvk,
    value: serde_json::Value,
) -> Result<Response, ApiError> {
    // ── ★ A LIST IS ENCODED AS `<Kind>List`, NOT AS `<Kind>` ──────────────
    // The protobuf codec derives its descriptor from this GVK. Passing the
    // ITEM's kind made it deserialize a `SecretList` body against the
    // `Secret` descriptor, and because the codec is deliberately lenient
    // (`deny_unknown_fields(false)`, so a newer apiserver field never breaks
    // an older client) every field of the list — `items` included — was
    // silently DROPPED. The result was a well-formed 200 with the right
    // Content-Type and an essentially empty body.
    //
    // Measured on ryn 2026-09-17, the same LIST two ways: JSON 1196 bytes,
    // protobuf 30. Nothing errored. `helm list` came back empty and
    // `helm upgrade` said "has no deployed releases" for a release it had
    // just written correctly — Helm's Go client negotiates protobuf, so it
    // read zero items and reported that as fact.
    //
    // JSON was unaffected because it renders the value as-is; only the
    // typed codec needs the descriptor, which is why every kubectl check
    // passed while every Go-client tool saw an empty cluster.
    let list_gvk = Gvk {
        api_version: gvk.api_version.clone(),
        kind: {
            let mut k = gvk.kind.clone();
            k.push_str("List");
            k
        },
    };
    render_object(codec, &list_gvk, StatusCode::OK, value)
}

fn render_object(
    codec: ResponseCodec,
    gvk: &Gvk,
    status: StatusCode,
    value: serde_json::Value,
) -> Result<Response, ApiError> {
    match codec {
        ResponseCodec::Json => Ok((status, Json(value)).into_response()),
        ResponseCodec::Table(include) => {
            let table = crate::table::to_table(&value, include);
            Ok((status, Json(table)).into_response())
        }
        ResponseCodec::PartialMetadata => {
            let projected = crate::table::to_partial_object_metadata(&value);
            Ok((status, Json(projected)).into_response())
        }
        ResponseCodec::Protobuf => {
            // The read-back Value carries apiVersion+kind from
            // inject_type_meta; the codec re-derives the per-kind
            // descriptor from `gvk` (the handler's GVK), so the response
            // wraps correctly even if the stored object omitted TypeMeta.
            let bytes = kube_proto::encode_response(gvk, &value)?;
            Ok((status, [(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)], bytes).into_response())
        }
    }
}

// ── shared per-verb bodies (core + grouped wrappers reuse these) ───────
//
// The five helpers below are the ONE implementation of each verb. The
// core routes and the grouped routes are thin wrappers that resolve a
// handler (by core-plural vs full triple) and delegate here — no
// duplicated CRUD/watch logic between the two route families.

async fn do_get(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    codec: ResponseCodec,
) -> Result<Response, ApiError> {
    let v = h.get(ns, name).await?;
    render_object(codec, &handler_gvk(h), StatusCode::OK, v)
}

async fn do_create(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    headers: &HeaderMap,
    raw: &[u8],
    user_info: &UserInfo,
    dry_run: DryRun,
) -> Result<Response, ApiError> {
    let body = decode_write_body(headers, raw)?;
    let v = h.create(ns, body, user_info, dry_run).await?;
    let codec = ResponseCodec::from_headers(headers)?;
    render_object(codec, &handler_gvk(h), StatusCode::CREATED, v)
}

async fn do_replace(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
    user_info: &UserInfo,
    dry_run: DryRun,
) -> Result<Response, ApiError> {
    let body = decode_write_body(headers, raw)?;
    let v = h.replace(ns, name, body, user_info, dry_run).await?;
    let codec = ResponseCodec::from_headers(headers)?;
    render_object(codec, &handler_gvk(h), StatusCode::OK, v)
}

async fn do_patch(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
    apply_params: &crate::params::ApplyParams,
    user_info: &UserInfo,
) -> Result<Response, ApiError> {
    let gvk = handler_gvk(h);
    // Resolve the typed patch algorithm from the Content-Type FIRST — the
    // media type is the load-bearing signal the store dispatches on. Erasing
    // it here (the old `decode_patch_body` did) made every patch a merge.
    let (patch, patch_type) = decode_patch(headers, raw, &gvk)?;
    // Server-side apply (Content-Type application/apply-patch+yaml) → validate
    // the `?fieldManager=`/`?force=` query into typed ApplyOptions. A missing
    // fieldManager is a typed 400 here (matching upstream). For EVERY other
    // patch algorithm `apply_opts` is None — the non-SSA path is UNCHANGED.
    let apply_opts = if patch_type == engenho_types::patch::PatchType::Apply {
        Some(crate::params::ApplyOptions::from_params(apply_params)?)
    } else {
        None
    };
    let v = h
        .patch(
            ns,
            name,
            patch,
            patch_type,
            apply_opts,
            user_info,
            DryRun::parse(apply_params.dry_run.as_deref())?,
        )
        .await?;
    // An apply that CREATES the object returns 201; an apply/patch that
    // updates returns 200. The store reports Created vs Patched; the handler
    // surfaces the status via the response — here we keep 200 for parity with
    // the existing patch path (kubectl apply --server-side accepts both, and
    // the read-back object carries the committed state either way).
    let codec = ResponseCodec::from_headers(headers)?;
    render_object(codec, &gvk, StatusCode::OK, v)
}

async fn do_delete(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    p: &ListWatchParams,
    user_info: &UserInfo,
    dry_run: DryRun,
) -> Result<Response, ApiError> {
    // `?resourceVersion=N` is the K8s DELETE precondition
    // (`Preconditions.resourceVersion`); absent/"0" → unconditional.
    let expected = p.precondition()?;
    // The K8s DELETE wire returns a NON-empty typed body (the deleted
    // object, or a `metav1.Status{status:"Success"}` when the name was
    // absent). An empty 200 crashes kubectl's `json.Unmarshal([]byte{})`
    // ("unexpected end of JSON input"). This is the SAME content-negotiated
    // emission chokepoint create/get use — zero new serialization code.
    let obj = h
        .delete_with_precondition(ns, name, expected, user_info, dry_run)
        .await?;
    let codec = ResponseCodec::from_headers(headers)?;
    // PROTOBUF CAVEAT: the deleted-object branch encodes cleanly (its GVK
    // — Deployment, ConfigMap, … — is in the proto pool). The Status-Success
    // fallback only arises when no object existed; `Status` lives in
    // meta/v1, NOT the core/v1 package the kube-proto map reaches, so
    // encoding it as protobuf would hit `CodecError::UncatalogedKind`.
    // Render that one value as JSON regardless of Accept. This is invisible
    // to conformance: the conformance DELETE always targets an existing
    // object (object path → protobuf works).
    if is_status_value(&obj) {
        return render_object(ResponseCodec::Json, &handler_gvk(h), StatusCode::OK, obj);
    }
    render_object(codec, &handler_gvk(h), StatusCode::OK, obj)
}

/// `true` iff `v` is a `kind: "Status"` envelope — the meta/v1 Status that
/// the DELETE no-object fallback returns. Used by [`do_delete`] to force
/// JSON for that one value (its protobuf descriptor is not reachable
/// through the kube-proto core/v1 package map).
fn is_status_value(v: &serde_json::Value) -> bool {
    v.get("kind").and_then(serde_json::Value::as_str) == Some("Status")
}

/// DELETE on a collection path (no object name) — deletecollection. The K8s
/// wire returns the `<Kind>List` of the objects selected for deletion (their
/// pre-delete images) with HTTP 200. Selectors (`?labelSelector=` /
/// `?fieldSelector=`) narrow the collection just as they do for LIST. The
/// list body is rendered JSON — the same shape [`do_list_or_watch`] emits.
async fn do_delete_collection(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    p: &ListWatchParams,
    user_info: &UserInfo,
    dry_run: DryRun,
) -> Result<Response, ApiError> {
    let sel = p.selectors()?;
    let list = h.delete_collection(ns, &sel, user_info, dry_run).await?;
    Ok(Json(list).into_response())
}

/// The shared LIST/WATCH body for both the core + grouped cases.
///
///   * `watch == false` → the atomic-rv LIST envelope (selectors
///     applied apiserver-side; rv = `current_revision`).
///   * `watch == true`  → the streaming chunked NDJSON WATCH (the K8s
///     list-then-watch contract).
///
/// `watch` is [`crate::coords::RequestInfo::is_watch`] — the verb authz
/// judged — never a second read of the query.
async fn do_list_or_watch(
    h: Arc<dyn ResourceHandler>,
    namespace: Option<String>,
    watch: bool,
    p: ListWatchParams,
    codec: ResponseCodec,
) -> Result<Response, ApiError> {
    let sel = p.selectors()?;
    if watch {
        watch_response(h, namespace, p, sel, codec).await
    } else {
        // Paged path when `limit` or `continue` is present; otherwise the
        // unbounded atomic-rv LIST envelope (back-compat: no continue /
        // remainingItemCount fields emitted).
        let limit = p.limit()?;
        let continue_token = p.continue_token()?;
        if limit > 0 || continue_token.is_some() {
            let (items, rv, cont, remaining) = h
                .list_page(namespace.as_deref(), &sel, limit, continue_token)
                .await?;
            render_list(
                codec,
                &handler_gvk(&h),
                h.list_response(items, rv, cont, remaining),
            )
        } else {
            let (items, rv) = h.list_at(namespace.as_deref(), &sel).await?;
            render_list(
                codec,
                &handler_gvk(&h),
                h.list_response(items, rv, None, None),
            )
        }
    }
}

/// Per-stream state for the watch `unfold` — owns everything the
/// streaming closure needs to filter + encode each `WatchSignal`.
struct WatchStreamState {
    stream: WatchStream,
    handler: Arc<dyn ResourceHandler>,
    namespace: Option<String>,
    selectors: Selectors,
    allow_bookmarks: bool,
    /// Server-side deadline from `?timeoutSeconds=N`. `None` = stream
    /// until the client goes away.
    ///
    /// K8s closes a watch CLEANLY at this point so the client re-LISTs and
    /// re-WATCHes. Without it the field was parsed and honoured by nobody,
    /// so a long-running client fell back on its own read timeout: measured
    /// against a live engenho on 2026-09-06, pangea-operator logged ~28
    /// `hyper::Error(Body, Kind(TimedOut))` per hour across all 12 of its
    /// controllers, each followed by a re-LIST. Reconciliation still
    /// happened, so the failure was invisible except as log noise.
    deadline: Option<tokio::time::Instant>,
    /// Project every emitted object to its metadata (see `watch_response`).
    partial: bool,
}

/// The object a watch line carries: the stored object, or just its metadata
/// when the client negotiated PartialObjectMetadata.
fn project_watch_object(object: &serde_json::Value, partial: bool) -> serde_json::Value {
    if partial {
        crate::table::to_partial_object_metadata(object)
    } else {
        object.clone()
    }
}

/// The TypeMeta stamped on a watch line. A projected object is a
/// `meta.k8s.io/v1 PartialObjectMetadata`, never the resource's own kind —
/// stamping the resource kind onto a stripped object is exactly the
/// mismatch the client refuses to decode.
fn watch_gvk<'a>(gvk: WatchGvk<'a>, partial: bool) -> WatchGvk<'a> {
    if partial {
        WatchGvk {
            api_version: "meta.k8s.io/v1",
            kind: "PartialObjectMetadata",
        }
    } else {
        gvk
    }
}

/// Build the streaming chunked-transfer WATCH response.
///
/// The K8s wire shape is newline-delimited JSON `WatchEvent` lines
/// (NOT a JSON array). HTTP status is 200 the moment the response
/// starts; per-event/terminal status (incl. 410) is carried IN-BAND as
/// Status objects, matching kube-apiserver's long-poll watch behavior.
///
/// GVK-agnostic: the `gvk_ns_matches` filter keys on the handler's full
/// `(group, version, kind)` + requested namespace, so non-core kinds get
/// the same WATCH machinery as core ones with zero duplication.
async fn watch_response(
    h: Arc<dyn ResourceHandler>,
    namespace: Option<String>,
    p: ListWatchParams,
    sel: Selectors,
    codec: ResponseCodec,
) -> Result<Response, ApiError> {
    // A metadata-only client sends ONE Accept for its LIST and its WATCH, so
    // a stream that ignores the `as=` parameter hands a full object to a
    // decoder registered only for PartialObjectMetadata. That does not fail
    // as a bad object: client-go reports `no kind "ConfigMap" is registered
    // for version "v1" in scheme`, drops the watch and re-LISTs forever.
    // Measured on rio 2026-09-15 — Flux's controllers logged it every ~25s
    // while their LIST (already projected) succeeded.
    let partial = matches!(codec, ResponseCodec::PartialMetadata);
    let mut from: ResumePoint = p.resume_point()?;

    // ── streaming lists (K8s 1.27 `sendInitialEvents`) ──
    //
    // The client asked for the current state to arrive AS watch events
    // instead of issuing a separate LIST. Snapshot first, then open the
    // stream AT that snapshot's revision — opening the stream first would
    // leave a window in which a change lands after the stream registers but
    // before the snapshot is taken, and the client would see it twice; the
    // reverse order can only ever REPLAY, never drop.
    let mut prelude: Vec<Bytes> = Vec::new();
    if p.send_initial_events {
        let (items, rv) = h.list_at(namespace.as_deref(), &sel).await?;
        let api_version = h.api_version();
        let gvk = WatchGvk {
            api_version: &api_version,
            kind: h.kind(),
        };
        prelude.reserve(items.len() + 1);
        for item in &items {
            prelude.push(event_line(
                WatchEventKind::Added,
                &project_watch_object(item, partial),
                watch_gvk(gvk, partial),
            ));
        }
        // The terminator. Without this annotation a kube-rs `watcher` /
        // client-go reflector stays in its initializing state forever even
        // though every object above was delivered.
        prelude.push(bookmark_line(rv, gvk, true));
        from = ResumePoint::At(rv);
    }

    // CompactedTooOld AT REGISTRATION → a real HTTP 410 (the client
    // re-LISTs). Once we have a stream, the response is 200 and any
    // later loss is in-band.
    let stream = h
        .watch_stream(namespace.as_deref(), from, p.allow_watch_bookmarks)
        .await?;

    let init = WatchStreamState {
        stream,
        handler: h,
        namespace,
        selectors: sel,
        allow_bookmarks: p.allow_watch_bookmarks,
        deadline: p.timeout()?.map(|d| tokio::time::Instant::now() + d),
        partial,
    };

    let live = futures::stream::unfold(init, |mut st| async move {
        loop {
            // Honour `?timeoutSeconds=N`: at the deadline the server ENDS the
            // stream cleanly (`None`), which is a normal close on the wire, not
            // an error. The client re-LISTs and re-WATCHes — the K8s contract.
            // Racing here rather than wrapping the whole stream keeps the
            // in-band 410 paths below reachable right up to the deadline.
            let next = match st.deadline {
                Some(at) => match tokio::time::timeout_at(at, st.stream.next()).await {
                    Ok(item) => item,
                    Err(_) => return None,
                },
                None => st.stream.next().await,
            };
            match next {
                Some(Ok(WatchSignal::Event(ev))) => {
                    // Filter to this handler's GVK + requested namespace,
                    // then by selectors. A change to another kind / ns /
                    // non-matching object advances the shared revision
                    // but is dropped here.
                    if !gvk_ns_matches(
                        &ev.key,
                        st.handler.group(),
                        st.handler.version(),
                        st.handler.kind(),
                        st.namespace.as_deref(),
                    ) || !st.selectors.matches(&ev.object)
                    {
                        continue;
                    }
                    let api_version = st.handler.api_version();
                    let gvk = watch_gvk(
                        WatchGvk {
                            api_version: &api_version,
                            kind: st.handler.kind(),
                        },
                        st.partial,
                    );
                    let line =
                        event_line(ev.kind, &project_watch_object(&ev.object, st.partial), gvk);
                    return Some((Ok::<Bytes, Infallible>(line), st));
                }
                Some(Ok(WatchSignal::Bookmark(rev))) => {
                    if st.allow_bookmarks {
                        let api_version = st.handler.api_version();
                        let gvk = WatchGvk {
                            api_version: &api_version,
                            kind: st.handler.kind(),
                        };
                        return Some((Ok(bookmark_line(rev, gvk, false)), st));
                    }
                    // Bookmarks not requested → drop + keep streaming.
                    continue;
                }
                Some(Err(WatchGone::CompactedTooOld { compacted, .. })) => {
                    // Mid-stream compaction: emit an in-band 410 Status
                    // carrying the safe resume point, then end. The next
                    // unfold poll sees None (the WatchStream surfaces its
                    // single terminal Err exactly once, then None) — the
                    // 410 line is the final line of the stream.
                    let line = status_410_line(compacted);
                    return Some((Ok(line), st));
                }
                Some(Err(WatchGone::Overflow { last_seen, .. })) => {
                    // Mid-stream loss: emit an in-band 410 Status carrying
                    // last_seen as the safe resume point, then end. The
                    // client re-LISTs.
                    let line = status_410_line(last_seen);
                    return Some((Ok(line), st));
                }
                None => return None, // store dropped / clean close → end.
            }
        }
    });

    // The initial-events replay, then the live stream. `prelude` is empty
    // unless `sendInitialEvents=true`, so the ordinary watch path is
    // byte-for-byte what it was.
    let body = Body::from_stream(futures::StreamExt::chain(
        futures::stream::iter(prelude.into_iter().map(Ok::<Bytes, Infallible>)),
        live,
    ));

    // 200 the instant the response starts. The body is an unbounded
    // stream with no Content-Length, so hyper frames it as HTTP/1.1
    // chunked transfer-encoding automatically — we MUST NOT set
    // `Transfer-Encoding: chunked` by hand (a manual header double-frames
    // the body and the client never sees a complete chunk).
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .map_err(|e| ApiError::Internal(format!("failed to build watch response: {e}")))?;
    Ok(resp)
}

// ── scope-agnostic verb handlers (ONE per verb; both URL families) ─────
//
// Each handler takes the [`ResourceCoords`] extractor (the request-info
// layer's ONE classification, the value authz judged) + does exactly:
// resolve the handler via the SINGLE resolver
// `state.lookup(coords.group_key(), coords.version_key(), &coords.plural)`,
// then delegate to the matching shared `do_*` body. The
// namespaced-vs-cluster scope assertion still lives inside
// `StoreBackedHandler::key()` (typed 400 on mismatch); coords just carries
// `namespace: Option<String>` straight through. No subresource handler
// exists today — a `Some(subresource)` returns a typed `NotFound` (no stub
// Ok), reserved for the status/scale follow-up.

/// A subresource resolved against its kind's catalog, carried TOGETHER with
/// the instance name it addresses. A subresource always targets one object,
/// so the name is part of the resolved value rather than a separate
/// `Option` on [`crate::coords::ResourceCoords`] that every dispatch arm
/// would have to re-check. `name` is a `&str`, not an `Option`, so a target
/// without a name cannot be represented. The type is private to this module
/// and [`resolve_subresource`] is the one place that builds it; that part is
/// convention inside the module, not something the compiler enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubresourceTarget<'a> {
    /// Which catalog-declared subresource the request addresses.
    subresource: Subresource,
    /// The object the subresource belongs to.
    name: &'a str,
}

/// Resolve `coords.subresource` against the resolved handler's catalog-
/// declared subresource set. The router NEVER special-cases a kind by name:
/// the handler's `descriptor.subresources` (sourced from `RESOURCE_CATALOG`)
/// is the single authority for whether a subresource is served.
///
///   * `None`               → `Ok(None)` (the base-object verb path).
///   * `Some("status")` + the kind declares `Subresource::Status`
///                          → `Ok(Some(target))` with `Subresource::Status`.
///   * `Some("scale")`  + the kind declares `Subresource::Scale`
///                          → `Ok(Some(target))` with `Subresource::Scale`.
///   * anything else, or a declared-absent subresource
///                          → a typed K8s `Status` 404 (no stub Ok, no panic).
///
/// `name` is required for a subresource (a subresource always targets an
/// instance) — a collection-path subresource is a typed `BadRequest`. The
/// name is returned inside the [`SubresourceTarget`], so a caller holding a
/// resolved subresource holds its name too.
fn resolve_subresource<'a>(
    coords: &'a crate::coords::ResourceCoords,
    h: &Arc<dyn ResourceHandler>,
) -> Result<Option<SubresourceTarget<'a>>, ApiError> {
    let Some(sub) = coords.subresource.as_deref() else {
        return Ok(None);
    };
    let Some(name) = coords.name.as_deref() else {
        return Err(ApiError::BadRequest(format!(
            "subresource {sub:?} requires a resource name (instance path)"
        )));
    };
    let subresource = match sub {
        "status" if h.subresources().contains(&Subresource::Status) => Subresource::Status,
        "scale" if h.subresources().contains(&Subresource::Scale) => Subresource::Scale,
        "log" if h.subresources().contains(&Subresource::Log) => Subresource::Log,
        "token" if h.subresources().contains(&Subresource::Token) => Subresource::Token,
        other => {
            return Err(ApiError::NotFound(format!(
                "the server could not find the requested resource: {} does not serve subresource {:?}",
                coords.plural, other
            )));
        }
    };
    Ok(Some(SubresourceTarget { subresource, name }))
}

/// Default token lifetime when a `TokenRequest` names none — one hour, the
/// same default kube-apiserver applies.
const TOKEN_LIFETIME_DEFAULT_SECS: i64 = 3600;
/// Floor on a requested lifetime. Upstream refuses anything under 10 minutes;
/// a token that expires faster than a kubelet can rotate it is a crashloop
/// dressed as a credential.
const TOKEN_LIFETIME_MIN_SECS: i64 = 600;
/// Ceiling on a requested lifetime. A caller asking for more is CLAMPED, not
/// refused — and `status.expirationTimestamp` echoes the lifetime actually
/// minted, so a clamped caller can SEE it was clamped rather than believing it
/// holds a token good for a year.
const TOKEN_LIFETIME_MAX_SECS: i64 = 86_400;

/// Clamp a requested token lifetime into the servable band.
///
/// CLAMPS rather than refuses, and the caller is told: `status.expirationTimestamp`
/// is rendered from the clamped value, so a client asking for a year can SEE it
/// holds a day. Refusing instead would break `kubectl create token
/// --duration=...` for values upstream itself accepts.
fn clamp_token_lifetime(requested: i64) -> i64 {
    requested.clamp(TOKEN_LIFETIME_MIN_SECS, TOKEN_LIFETIME_MAX_SECS)
}

/// `POST serviceaccounts/<name>/token` — mint a bound ServiceAccount JWT.
///
/// The one create-shaped subresource. Nothing is persisted: the response IS
/// the product, and the token exists only in it.
///
/// ── ★ WHY THIS CLOSES A REAL HOLE, NOT A MISSING FEATURE ───────────────────
/// The verifying half shipped long ago — `sa_token::verify`, `SaVerifier`, and
/// the runtime's `bootstrap_with_sa` over `pki/sa.key` are all live. Only the
/// ISSUING half was absent, and the consequence was not "SA tokens don't
/// work": it was that a pod had no identity of its own, so every workload
/// needing the API had to mount a kubeconfig carrying ADMIN client-key
/// material. Cluster-admin was distributed to ordinary pods because the
/// cheapest correct credential could not be minted.
///
/// It also made RBAC decorative. The authorizer, the Roles and the bindings
/// all worked; nothing could ever present a non-admin identity to be judged.
///
/// `name` is the service account the resolved [`SubresourceTarget`] addresses —
/// taken from the target rather than re-read from the coordinates, so there
/// is no "token request that names no service account" branch to write.
async fn do_token_request(
    state: &RouterState,
    h: &Arc<dyn ResourceHandler>,
    namespace: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &Bytes,
) -> Result<Response, ApiError> {
    let Some(namespace) = namespace else {
        return Err(ApiError::BadRequest(
            "the token subresource is namespaced; no namespace in the request path".into(),
        ));
    };
    // Refusing to issue must never degrade into issuing something that does
    // not authenticate, so a keyless server says so instead of minting.
    let Some(issuer) = state.token_issuer.as_ref() else {
        return Err(ApiError::Internal(
            "ServiceAccount token issuance is not configured: this apiserver holds no SA \
             signing key, so `/token` cannot mint. (The cluster's key lives at \
             <data_dir>/pki/sa.key and is installed via RouterState::with_token_issuer.)"
                .into(),
        ));
    };

    // The ServiceAccount must EXIST and its uid goes into the claim — a token
    // naming a deleted SA would verify happily while authorizing an identity
    // nobody can revoke. `get` yields the typed 404 when it is absent.
    let sa = h.get(Some(namespace), name).await?;
    let uid = sa
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();

    // An empty body is a legal TokenRequest (every spec field is optional).
    // Decoded through the SHARED write-body decoder, not `serde_json` direct:
    // kubectl negotiates protobuf for core/v1 writes, so a hand-rolled JSON
    // parse here rejects the very client this endpoint exists for — measured,
    // `kubectl create token` failed "expected value at line 1 column 1".
    let req: serde_json::Value = if raw.is_empty() {
        serde_json::json!({})
    } else {
        decode_write_body(headers, raw)?
    };
    let spec = req.get("spec");

    let audiences: Vec<String> = spec
        .and_then(|s| s.get("audiences"))
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .filter(|v: &Vec<String>| !v.is_empty())
        .unwrap_or_else(|| vec![issuer.default_audience.clone()]);

    let requested = spec
        .and_then(|s| s.get("expirationSeconds"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(TOKEN_LIFETIME_DEFAULT_SECS);
    let lifetime = clamp_token_lifetime(requested);

    // Bind to a Pod when asked. Only Pod is honoured: binding to a kind whose
    // lifetime we do not track would be a promise the runtime cannot keep.
    let bound = spec
        .and_then(|s| s.get("boundObjectRef"))
        .filter(|b| b.get("kind").and_then(serde_json::Value::as_str) == Some("Pod"))
        .map(|b| crate::sa_token::NamedUid {
            name: b
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            uid: b
                .get("uid")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });

    // ONE clock read. `exp` and `status.expirationTimestamp` are both derived
    // from it, so the token and the advertised expiry cannot disagree.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| ApiError::Internal(["clock before UNIX epoch: ", &e.to_string()].concat()))?
        .as_secs();
    let now = i64::try_from(now)
        .map_err(|_| ApiError::Internal("clock past the representable range".into()))?;

    let token = crate::sa_token::issue(
        &issuer.signing,
        &issuer.issuer,
        namespace,
        name,
        &uid,
        &audiences,
        bound,
        now,
        lifetime,
    )
    .map_err(|e| ApiError::Internal(["minting the token failed: ", &e.to_string()].concat()))?;

    let expiry = engenho_types::time::epoch_to_rfc3339_utc(now + lifetime).ok_or_else(|| {
        ApiError::Internal("the computed token expiry is not a representable instant".into())
    })?;

    let body = serde_json::json!({
        "kind": "TokenRequest",
        "apiVersion": "authentication.k8s.io/v1",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": engenho_types::time::now_rfc3339_utc(),
        },
        "spec": {
            "audiences": audiences,
            "expirationSeconds": lifetime,
        },
        "status": {
            "token": token,
            "expirationTimestamp": expiry,
        },
    });
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

// ── shared subresource do_* bodies (status/scale; both URL families) ────
//
// Each delegates to the matching `ResourceHandler` scoped-write method (the
// store-side scoping lives there, reusing ResourceCommand::Patch). The verb
// handlers pick the body by the typed `Subresource` resolved from the
// catalog — no kind name is ever matched in the router.

async fn do_get_status(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    codec: ResponseCodec,
) -> Result<Response, ApiError> {
    let v = h.get_status(ns, name).await?;
    render_object(codec, &handler_gvk(h), StatusCode::OK, v)
}

async fn do_put_status(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
) -> Result<Response, ApiError> {
    let body = decode_write_body(headers, raw)?;
    let v = h.put_status(ns, name, body).await?;
    let codec = ResponseCodec::from_headers(headers)?;
    render_object(codec, &handler_gvk(h), StatusCode::OK, v)
}

async fn do_patch_status(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
) -> Result<Response, ApiError> {
    let gvk = handler_gvk(h);
    let (patch, patch_type) = decode_patch(headers, raw, &gvk)?;
    let v = h.patch_status(ns, name, patch, patch_type).await?;
    let codec = ResponseCodec::from_headers(headers)?;
    render_object(codec, &gvk, StatusCode::OK, v)
}

async fn do_get_scale(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
) -> Result<Response, ApiError> {
    // The Scale projection is autoscaling/v1 regardless of the parent's
    // group, so it is rendered as JSON (its GVK is not in the parent's
    // protobuf pool) — the upstream contract. Same as the Status-Success
    // DELETE fallback's JSON-regardless rendering.
    let v = h.get_scale(ns, name).await?;
    Ok((StatusCode::OK, Json(v)).into_response())
}

/// GET `<plural>/<name>/log` — the Pod-logs subresource. Takes the typed
/// [`crate::pod_logs::LogQuery`] (already decoded by the verb handler's
/// `Query` extractor from `?container=` / `?tailLines=` / `?timestamps=`), asks
/// the handler, and renders the log text as `text/plain` (kubectl reads the
/// streamed log body as plain text, NOT a JSON object). A typed
/// `NotFound`/`Internal` from the handler renders as the usual K8s `Status`
/// body via `ApiError::into_response`.
async fn do_get_log(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    query: &crate::pod_logs::LogQuery,
) -> Result<Response, ApiError> {
    let text = h.logs(ns, name, query).await?;
    // text/plain — the log stream body. kubectl reads stdout verbatim.
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; charset=utf-8")],
        text,
    )
        .into_response())
}

async fn do_put_scale(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
) -> Result<Response, ApiError> {
    let body = decode_write_body(headers, raw)?;
    let v = h.put_scale(ns, name, body).await?;
    Ok((StatusCode::OK, Json(v)).into_response())
}

async fn do_patch_scale(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
) -> Result<Response, ApiError> {
    let gvk = handler_gvk(h);
    let (patch, patch_type) = decode_patch(headers, raw, &gvk)?;
    let v = h.patch_scale(ns, name, patch, patch_type).await?;
    Ok((StatusCode::OK, Json(v)).into_response())
}

/// GET on a resource path — LIST/WATCH a collection (no `name`) or GET a
/// single instance (with `name`). The `?watch=` flag + `coords.name` pick
/// the branch, exactly as the legacy list-vs-get split did.
async fn resource_get_or_list(
    State(state): State<RouterState>,
    // The whole classification, not just the coords: list-vs-watch is
    // `info.is_watch()`, the verb authz judged.
    info: crate::coords::RequestInfo,
    headers: HeaderMap,
    // The `/log` subresource reads its typed `?container=&tailLines=&timestamps=`
    // knobs from this extractor. axum parses the WHOLE query string into BOTH
    // `LogQuery` and `ListWatchParams` independently — neither denies unknown
    // fields, so each ignores the other's keys.
    Query(log_query): Query<crate::pod_logs::LogQuery>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let coords = info.resource_coords()?;
    // Resolve the handler FIRST (the subresource is served by the parent
    // kind's handler), then dispatch on the typed subresource resolved from
    // the catalog.
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    if let Some(SubresourceTarget { subresource, name }) = resolve_subresource(&coords, &h)? {
        // A subresource always targets an instance — the resolved target
        // carries that instance's name.
        let codec = ResponseCodec::from_headers(&headers)?;
        return match subresource {
            Subresource::Status => {
                do_get_status(&h, coords.namespace.as_deref(), name, codec).await
            }
            Subresource::Scale => do_get_scale(&h, coords.namespace.as_deref(), name).await,
            // `/log` — read-only; the typed LogQuery is already decoded above.
            Subresource::Log => do_get_log(&h, coords.namespace.as_deref(), name, &log_query).await,
            // `/token` is WRITE-ONLY (POST/create). A GET cannot return the
            // last token because none is stored — the mint is the only copy
            // that ever exists. Typed BadRequest naming the verb, never an
            // empty 200 that reads as "this SA has no token".
            Subresource::Token => Err(ApiError::BadRequest(
                "the token subresource supports create (POST) only; there is no stored token to                  GET — a minted token is returned once, in the POST response, and never                  persisted"
                    .into(),
            )),
        };
    }
    match &coords.name {
        // Collection GET → LIST or WATCH (the shared body branches on the
        // judged verb). `lookup` already returns an owned `Arc` (the
        // ArcSwap-snapshot clone) — exactly what the watch unfold stream
        // needs, no extra `.clone()`.
        None => {
            let codec = ResponseCodec::from_headers(&headers)?;
            do_list_or_watch(h, coords.namespace, info.is_watch(), p, codec).await
        }
        // Instance GET → the shared `do_get` body. Bind the owned `Arc`,
        // pass it by reference (`do_get` takes `&Arc`).
        Some(name) => {
            do_get(
                &h,
                coords.namespace.as_deref(),
                name,
                ResponseCodec::from_headers(&headers)?,
            )
            .await
        }
    }
}

/// POST on a resource path — CREATE into a collection. A POST that carries
/// a `name` (instance path) is not a K8s CREATE shape; `do_create` would
/// have no body slot for it under the legacy routes (POST was only wired on
/// the collection routes), so reject it with a typed `BadRequest` mirroring
/// that the legacy instance routes never accepted POST.
/// Query params common to every WRITE verb. Today it carries `?dryRun=`;
/// it is a struct rather than a bare Option so the next write-time option
/// (`fieldValidation`) lands in ONE place instead of a sixth signature.
#[derive(Debug, Default, Clone, serde::Deserialize)]
#[serde(default)]
pub struct WriteParams {
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
}

async fn resource_create(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    coords: crate::coords::ResourceCoords,
    Query(write): Query<WriteParams>,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let dry_run = DryRun::parse(write.dry_run.as_deref())?;
    // POST on a subresource is not a K8s CREATE shape — status/scale/log are
    // get/patch/update shapes. `/token` is the ONE exception: it is defined as
    // a POST that mints rather than persists, so it is dispatched here rather
    // than being refused with the others. The check stays catalog-driven — a
    // kind that does not declare `Subresource::Token` still falls through to
    // the typed BadRequest below.
    if let Some(sub) = &coords.subresource {
        let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
        if let Ok(Some(SubresourceTarget {
            subresource: Subresource::Token,
            name,
        })) = resolve_subresource(&coords, &h)
        {
            return do_token_request(
                &state,
                &h,
                coords.namespace.as_deref(),
                name,
                &headers,
                &raw,
            )
            .await;
        }
        return Err(ApiError::BadRequest(format!(
            "the {sub:?} subresource does not support create (POST)"
        )));
    }
    if coords.name.is_some() {
        return Err(ApiError::BadRequest(
            "POST is only valid on a collection path (no resource name)".into(),
        ));
    }
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    do_create(
        &h,
        coords.namespace.as_deref(),
        &headers,
        &raw,
        &user_info.0,
        dry_run,
    )
    .await
}

/// PUT on a resource path. PUT is the kubectl full-object-replace verb for
/// `/status` and `/scale` (kubectl writes both via PUT). On the MAIN object
/// path (no subresource) engenho uses POST-create + PATCH, so a PUT there is
/// a typed `BadRequest` mirroring the upstream contract — never a silent
/// stub.
async fn resource_put(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    coords: crate::coords::ResourceCoords,
    Query(write): Query<WriteParams>,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let name = coords.name.as_deref().ok_or_else(|| {
        ApiError::BadRequest("PUT requires a resource name (instance path)".into())
    })?;
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    // The target's name IS `name` (both read `coords.name`); only the variant
    // is needed here because the main-object arm needs the name as well.
    match resolve_subresource(&coords, &h)?.map(|t| t.subresource) {
        Some(Subresource::Status) => {
            do_put_status(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        Some(Subresource::Scale) => {
            do_put_scale(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        // `/token` is create-only; PUT has no meaning on a thing that is
        // minted rather than stored.
        Some(Subresource::Token) => Err(ApiError::BadRequest(
            "the token subresource supports create (POST) only".into(),
        )),
        // `/log` is READ-ONLY (GET only) — PUT is a typed BadRequest, never a
        // silent accept.
        Some(Subresource::Log) => Err(ApiError::BadRequest(
            "the log subresource is read-only (GET only)".into(),
        )),
        // PUT on the main object (no subresource) — the kubectl `replace` /
        // update verb. Store-backed handlers do a real optimistic-concurrency
        // replace; handlers that don't override `replace` keep the typed 400.
        None => {
            do_replace(
                &h,
                coords.namespace.as_deref(),
                name,
                &headers,
                &raw,
                &user_info.0,
                DryRun::parse(write.dry_run.as_deref())?,
            )
            .await
        }
    }
}

/// PATCH on a resource path — PATCH a single instance (requires `name`).
async fn resource_patch(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    coords: crate::coords::ResourceCoords,
    // The `?fieldManager=`/`?force=` server-side-apply query params — decoded
    // for EVERY patch but only consumed when the Content-Type resolves to
    // apply (do_patch validates fieldManager iff patch_type == Apply). axum
    // parses the whole query string; non-apply patches ignore these fields.
    Query(apply_params): Query<crate::params::ApplyParams>,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let name = coords.name.as_deref().ok_or_else(|| {
        ApiError::BadRequest("PATCH requires a resource name (instance path)".into())
    })?;
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    // As in `resource_put`: the target's name is `name`, needed by every arm.
    match resolve_subresource(&coords, &h)?.map(|t| t.subresource) {
        Some(Subresource::Status) => {
            do_patch_status(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        Some(Subresource::Scale) => {
            do_patch_scale(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        // `/token` is create-only; PATCH has no meaning on a thing that is
        // minted rather than stored.
        Some(Subresource::Token) => Err(ApiError::BadRequest(
            "the token subresource supports create (POST) only".into(),
        )),
        // `/log` is READ-ONLY (GET only) — PATCH is a typed BadRequest.
        Some(Subresource::Log) => Err(ApiError::BadRequest(
            "the log subresource is read-only (GET only)".into(),
        )),
        None => {
            do_patch(
                &h,
                coords.namespace.as_deref(),
                name,
                &headers,
                &raw,
                &apply_params,
                &user_info.0,
            )
            .await
        }
    }
}

/// DELETE on a resource path — DELETE a single instance (requires `name`).
async fn resource_delete(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    Query(p): Query<ListWatchParams>,
    Query(write): Query<WriteParams>,
    // DELETE carries its dry-run in a `DeleteOptions` BODY, not the query
    // string — see [`DryRun::for_delete`]. Extracted LAST because a body
    // extractor consumes the request.
    raw: Bytes,
) -> Result<Response, ApiError> {
    let dry_run = DryRun::for_delete(write.dry_run.as_deref(), &raw)?;
    // DELETE on a subresource is invalid — no subresource supports delete
    // (status/scale are get/patch/update only). Typed BadRequest, never a
    // stub Ok.
    if let Some(sub) = &coords.subresource {
        return Err(ApiError::BadRequest(format!(
            "the {sub:?} subresource does not support delete"
        )));
    }
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    // Collection path (no object name) → deletecollection when the kind
    // serves it; otherwise the name-required BadRequest is preserved (a
    // kind like Namespace has no CollectionDeleter).
    let Some(name) = coords.name.as_deref() else {
        if h.supports_delete_collection() {
            return do_delete_collection(
                &h,
                coords.namespace.as_deref(),
                &p,
                &user_info.0,
                dry_run,
            )
            .await;
        }
        return Err(ApiError::BadRequest(
            "DELETE requires a resource name (instance path)".into(),
        ));
    };
    do_delete(
        &h,
        coords.namespace.as_deref(),
        name,
        &headers,
        &p,
        &user_info.0,
        dry_run,
    )
    .await
}

#[cfg(test)]
mod tests {

    /// A metadata-only WATCH must carry PartialObjectMetadata objects under
    /// meta.k8s.io/v1. Serving the stored object instead is what made
    /// client-go drop the stream with `no kind "ConfigMap" is registered`.
    #[test]
    fn watch_projects_objects_when_partial_metadata_was_negotiated() {
        let stored = serde_json::json!({
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {"name": "c", "namespace": "flux-system", "resourceVersion": "7"},
            "data": {"k": "v"}
        });
        let projected = super::project_watch_object(&stored, true);
        assert_eq!(projected.get("kind").unwrap(), "PartialObjectMetadata");
        assert_eq!(projected.get("apiVersion").unwrap(), "meta.k8s.io/v1");
        assert!(
            projected.get("data").is_none(),
            "the body must not survive the projection"
        );
        assert_eq!(
            projected.pointer("/metadata/resourceVersion").unwrap(),
            "7",
            "the resourceVersion is what the client resumes from"
        );

        let gvk = super::watch_gvk(
            crate::params::WatchGvk {
                api_version: "v1",
                kind: "ConfigMap",
            },
            true,
        );
        assert_eq!(gvk.kind, "PartialObjectMetadata");
        assert_eq!(gvk.api_version, "meta.k8s.io/v1");
    }

    /// Negative control for the test above: an ordinary watcher keeps the
    /// whole object and the resource's own TypeMeta.
    #[test]
    fn watch_leaves_objects_whole_when_partial_metadata_was_not_negotiated() {
        let stored = serde_json::json!({
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {"name": "c"},
            "data": {"k": "v"}
        });
        assert_eq!(super::project_watch_object(&stored, false), stored);
        let gvk = super::watch_gvk(
            crate::params::WatchGvk {
                api_version: "v1",
                kind: "ConfigMap",
            },
            false,
        );
        assert_eq!(gvk.kind, "ConfigMap");
        assert_eq!(gvk.api_version, "v1");
    }
    use super::*;

    /// `lookup_core(p)` IS `lookup("", "v1", p)` — the fold the refactor
    /// asserts. Both resolvers key on the SAME `("", "v1", p)` triple AND
    /// render the SAME `Status` body on a miss, so collapsing the two old
    /// route families onto one resolver is byte-identical for the core
    /// case the `endpointss`-style tests exercise. Proven against an empty
    /// handler set (the miss path) — both calls produce the identical
    /// core-miss message; the grouped call produces the distinct grouped
    /// message.
    #[test]
    fn lookup_core_folds_into_lookup() {
        let state = RouterState::new(Vec::new());

        // The Ok type (`Arc<dyn ResourceHandler>`) is not `Debug`, so
        // extract the miss error by match rather than `unwrap_err()`.
        let msg = |r: Result<Arc<dyn ResourceHandler>, ApiError>| match r {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a NotFound miss against the empty handler set"),
        };

        let via_core = msg(state.lookup_core("pods"));
        let via_full = msg(state.lookup("", "v1", "pods"));
        assert_eq!(
            via_core, via_full,
            "lookup_core(p) == lookup(\"\", \"v1\", p): same Status body"
        );
        assert_eq!(
            via_core, "resource not found: unknown core kind plural: pods",
            "core-miss message text is preserved byte-for-byte",
        );

        // The grouped miss keeps its DISTINCT message — the message branch
        // on `group.is_empty()` is what makes the fold behavior-preserving.
        let grouped = msg(state.lookup("apps", "v1", "deployments"));
        assert_eq!(
            grouped,
            "resource not found: unknown kind: apps/v1/deployments",
        );
        assert_ne!(grouped, via_core);
    }

    // ── runtime-mutable dispatch table (ArcSwap register/unregister) ─────
    //
    // A minimal `ResourceHandler` carrying only its GVK + plural. The
    // routing-table assertions exercise ONLY the sync identity methods
    // (`group`/`version`/`plural`/…); the async CRUD/watch methods are
    // never called here, so they return a typed `ApiError` (NOT a panic /
    // `unimplemented!()` — per the no-stub discipline, an unexercised
    // surface returns a typed error, never a silent wrong answer).

    struct FakeHandler {
        group: String,
        version: String,
        kind: String,
        plural: String,
        namespaced: bool,
        short_names: Vec<&'static str>,
        singular: &'static str,
    }

    impl FakeHandler {
        fn arc(
            group: &str,
            version: &str,
            kind: &str,
            plural: &str,
            namespaced: bool,
        ) -> Arc<dyn ResourceHandler> {
            Arc::new(Self {
                group: group.into(),
                version: version.into(),
                kind: kind.into(),
                plural: plural.into(),
                namespaced,
                short_names: Vec::new(),
                singular: "",
            })
        }

        fn arc_with_meta(
            group: &str,
            version: &str,
            kind: &str,
            plural: &str,
            namespaced: bool,
            short_names: Vec<&'static str>,
            singular: &'static str,
        ) -> Arc<dyn ResourceHandler> {
            Arc::new(Self {
                group: group.into(),
                version: version.into(),
                kind: kind.into(),
                plural: plural.into(),
                namespaced,
                short_names,
                singular,
            })
        }
    }

    #[async_trait::async_trait]
    impl ResourceHandler for FakeHandler {
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
            &self.short_names
        }
        fn singular_name(&self) -> &str {
            self.singular
        }
        async fn get(&self, _ns: Option<&str>, _name: &str) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal("fake handler: get not exercised".into()))
        }
        async fn list(&self, _ns: Option<&str>) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal(
                "fake handler: list not exercised".into(),
            ))
        }
        async fn list_at(
            &self,
            _ns: Option<&str>,
            _sel: &crate::params::Selectors,
        ) -> Result<(Vec<serde_json::Value>, engenho_store::Revision), ApiError> {
            Err(ApiError::Internal(
                "fake handler: list_at not exercised".into(),
            ))
        }
        async fn list_page(
            &self,
            _ns: Option<&str>,
            _sel: &crate::params::Selectors,
            _limit: usize,
            _continue_token: Option<engenho_store::ContinueToken>,
        ) -> Result<
            (
                Vec<serde_json::Value>,
                engenho_store::Revision,
                Option<String>,
                Option<u64>,
            ),
            ApiError,
        > {
            Err(ApiError::Internal(
                "fake handler: list_page not exercised".into(),
            ))
        }
        async fn watch_stream(
            &self,
            _ns: Option<&str>,
            _from: crate::params::ResumePoint,
            _allow_bookmarks: bool,
        ) -> Result<engenho_store::WatchStream, ApiError> {
            Err(ApiError::Internal(
                "fake handler: watch_stream not exercised".into(),
            ))
        }
        async fn create(
            &self,
            _ns: Option<&str>,
            _body: serde_json::Value,
            _user_info: &engenho_types::auth::UserInfo,
            _dry_run: crate::params::DryRun,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal(
                "fake handler: create not exercised".into(),
            ))
        }
        async fn patch(
            &self,
            _ns: Option<&str>,
            _name: &str,
            _patch: serde_json::Value,
            _patch_type: engenho_types::patch::PatchType,
            _apply_opts: Option<crate::params::ApplyOptions>,
            _user_info: &engenho_types::auth::UserInfo,
            _dry_run: crate::params::DryRun,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal(
                "fake handler: patch not exercised".into(),
            ))
        }
        async fn delete(
            &self,
            _ns: Option<&str>,
            _name: &str,
            _user_info: &engenho_types::auth::UserInfo,
            _dry_run: crate::params::DryRun,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal(
                "fake handler: delete not exercised".into(),
            ))
        }
        async fn delete_with_precondition(
            &self,
            _ns: Option<&str>,
            _name: &str,
            _expected: Option<engenho_store::Revision>,
            _user_info: &engenho_types::auth::UserInfo,
            _dry_run: crate::params::DryRun,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal(
                "fake handler: delete_with_precondition not exercised".into(),
            ))
        }
    }

    #[test]
    fn register_then_lookup_resolves_the_handler() {
        let state = RouterState::new(Vec::new());
        // Empty table → a NotFound miss.
        assert!(state.lookup("example.com", "v1", "widgets").is_err());

        state.register(FakeHandler::arc(
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
        ));

        let h = state
            .lookup("example.com", "v1", "widgets")
            .expect("registered Widget handler resolves");
        assert_eq!(h.kind(), "Widget");
        assert_eq!(h.plural(), "widgets");
        assert!(h.namespaced());
    }

    #[test]
    fn unregister_then_lookup_is_notfound() {
        let state = RouterState::new(Vec::new());
        state.register(FakeHandler::arc(
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
        ));
        assert!(state.lookup("example.com", "v1", "widgets").is_ok());

        let removed = state.unregister("example.com", "v1", "widgets");
        assert!(removed, "unregister reports the handler was present");

        // Subsequent lookup → typed NotFound with the grouped message. The
        // Ok type (`Arc<dyn ResourceHandler>`) isn't `Debug`, so match
        // rather than `expect_err`.
        match state.lookup("example.com", "v1", "widgets") {
            Err(e) => assert_eq!(
                e.to_string(),
                "resource not found: unknown kind: example.com/v1/widgets"
            ),
            Ok(_) => panic!("unregistered Widget must no longer resolve"),
        }

        // Unregistering again reports false (nothing present).
        assert!(!state.unregister("example.com", "v1", "widgets"));
    }

    #[test]
    fn handler_set_reflects_post_register_snapshot() {
        let state = RouterState::new(Vec::new());
        assert!(state.handler_set().is_empty());
        state.register(FakeHandler::arc(
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
        ));
        let set = state.handler_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].group(), "example.com");
        assert_eq!(set[0].plural(), "widgets");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_register_from_two_tasks_both_land() {
        // rcu (read-copy-update) is lost-write-safe: two tasks each
        // registering a DISTINCT handler must BOTH be present afterward
        // even if their clone-insert-swaps race (the loser of a swap retries
        // against the winner's map). A naive `load → clone → store` would
        // drop one.
        let state = RouterState::new(Vec::new());
        let s1 = state.clone();
        let s2 = state.clone();
        let t1 = tokio::spawn(async move {
            for i in 0..50 {
                let plural = format!("widgets{i}");
                s1.register(FakeHandler::arc(
                    "a.example.com",
                    "v1",
                    "Widget",
                    Box::leak(plural.into_boxed_str()),
                    true,
                ));
            }
        });
        let t2 = tokio::spawn(async move {
            for i in 0..50 {
                let plural = format!("gadgets{i}");
                s2.register(FakeHandler::arc(
                    "b.example.com",
                    "v1",
                    "Gadget",
                    Box::leak(plural.into_boxed_str()),
                    false,
                ));
            }
        });
        t1.await.unwrap();
        t2.await.unwrap();

        // All 100 distinct registrations survived (no lost write).
        assert_eq!(
            state.handler_set().len(),
            100,
            "no lost write under racing rcu"
        );
        assert!(state.lookup("a.example.com", "v1", "widgets0").is_ok());
        assert!(state.lookup("b.example.com", "v1", "gadgets49").is_ok());
    }

    #[test]
    fn registered_handler_carries_metadata_into_handler_set() {
        // The metadata (shortNames / singular) a registered handler carries
        // flows through `handler_set()` for discovery to fold — this is the
        // load-bearing wiring for `kubectl get wd` short-name resolution.
        let state = RouterState::new(Vec::new());
        state.register(FakeHandler::arc_with_meta(
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
            vec!["wd"],
            "widget",
        ));
        let set = state.handler_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].short_names(), &["wd"]);
        assert_eq!(set[0].singular_name(), "widget");
    }

    #[test]
    fn discovery_group_resources_folds_registered_widget() {
        // build_group_resources folds the live ArcSwap snapshot → a
        // dynamically-registered Widget handler appears as a `widgets` row
        // with its shortNames + singular. This is exactly what drives
        // `kubectl api-resources --api-group=example.com`.
        let state = RouterState::new(Vec::new());
        state.register(FakeHandler::arc_with_meta(
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
            vec!["wd"],
            "widget",
        ));
        let list = crate::discovery::build_group_resources(&state, "example.com", "v1")
            .expect("example.com/v1 resource list present after register");
        assert_eq!(list.group_version, "example.com/v1");
        assert_eq!(list.resources.len(), 1);
        let r = &list.resources[0];
        assert_eq!(r.name, "widgets");
        assert_eq!(r.singular_name, "widget");
        assert!(r.namespaced);
        assert_eq!(r.short_names, vec!["wd".to_string()]);
        assert!(r.verbs.iter().any(|v| v == "watch"));

        // And the group shows up in /apis (build_api_groups).
        let groups = crate::discovery::build_api_groups(&state);
        assert!(groups.groups.iter().any(|g| g.name == "example.com"));
    }
}

#[cfg(test)]
mod token_subresource {
    use super::{
        TOKEN_LIFETIME_DEFAULT_SECS, TOKEN_LIFETIME_MAX_SECS, TOKEN_LIFETIME_MIN_SECS,
        clamp_token_lifetime,
    };
    use engenho_types::generated_v1_34::{RESOURCE_CATALOG, Subresource};

    #[test]
    fn a_default_request_is_inside_the_band_untouched() {
        // If the default ever drifted outside the band the clamp would
        // silently rewrite EVERY token's lifetime, which no caller would see.
        assert_eq!(
            clamp_token_lifetime(TOKEN_LIFETIME_DEFAULT_SECS),
            TOKEN_LIFETIME_DEFAULT_SECS
        );
    }

    #[test]
    fn an_absurd_request_is_clamped_not_honoured() {
        // A year. Honouring it would mint a credential that outlives the
        // cluster; the caller reads the real expiry off status.
        assert_eq!(clamp_token_lifetime(31_536_000), TOKEN_LIFETIME_MAX_SECS);
    }

    #[test]
    fn a_too_short_request_is_raised_to_the_floor() {
        // A token that expires faster than a kubelet can rotate it is a
        // crashloop dressed as a credential.
        assert_eq!(clamp_token_lifetime(1), TOKEN_LIFETIME_MIN_SECS);
        assert_eq!(clamp_token_lifetime(0), TOKEN_LIFETIME_MIN_SECS);
    }

    #[test]
    fn a_negative_request_cannot_mint_an_already_expired_token() {
        // `exp = now + lifetime`, so a negative lifetime would mint a token
        // that is expired at issue — refused by the verifier the instant it is
        // used, presenting as an auth bug rather than a bad request.
        assert!(clamp_token_lifetime(-3600) >= TOKEN_LIFETIME_MIN_SECS);
    }

    /// ★ NEGATIVE CONTROL for the band itself. Without this, both bounds could
    /// be set to the same number and every clamp test above would still pass
    /// while the endpoint served exactly one lifetime.
    #[test]
    fn the_band_is_actually_a_band() {
        assert!(
            TOKEN_LIFETIME_MIN_SECS < TOKEN_LIFETIME_MAX_SECS,
            "a collapsed band serves one lifetime and silently ignores every request"
        );
        assert!(TOKEN_LIFETIME_DEFAULT_SECS >= TOKEN_LIFETIME_MIN_SECS);
        assert!(TOKEN_LIFETIME_DEFAULT_SECS <= TOKEN_LIFETIME_MAX_SECS);
    }

    /// The catalog is the single authority for whether a kind serves `/token`;
    /// the router never matches a kind by name. If this row is lost, the mint
    /// endpoint 404s with no other symptom.
    #[test]
    fn serviceaccount_declares_the_token_subresource() {
        let sa = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == "ServiceAccount" && d.group.is_empty())
            .expect("ServiceAccount is cataloged");
        assert!(
            sa.subresources.contains(&Subresource::Token),
            "without this row `kubectl create token` is a 404 and every pod is identity-less"
        );
    }

    /// ★ NEGATIVE CONTROL for the row above: `/token` must NOT be blanket-added
    /// to every kind. A Pod serving `/token` would advertise a mint endpoint
    /// the router cannot satisfy.
    #[test]
    fn pods_do_not_serve_the_token_subresource() {
        let pod = RESOURCE_CATALOG
            .iter()
            .find(|d| d.kind == "Pod" && d.group.is_empty())
            .expect("Pod is cataloged");
        assert!(!pod.subresources.contains(&Subresource::Token));
    }
}

/// T4.1 — every route is classified ONCE, before authz, and authz judges that
/// classification.
#[cfg(test)]
mod request_info_coverage {
    use super::*;

    // ── T4.1: the ONE request classification reaches every route ────────
    //
    // Every test below drives the REAL `build` router (all three layers) with
    // an authorizer that records what it was asked and refuses everything, so
    // no handler body runs and no store is needed. A route reached without a
    // classification answers 500 (authz reads the stored RequestInfo first),
    // so a 403 carrying the expected Attributes proves the route was
    // classified by the request-info layer before authz judged it.

    /// Records every `Attributes` it is handed and returns `NoOpinion`
    /// (RBAC's default-deny), so the request stops at authz with a 403.
    #[derive(Default)]
    struct RecordingDeny {
        seen: std::sync::Mutex<Vec<crate::authz::Attributes>>,
    }

    impl RecordingDeny {
        fn last(&self) -> Option<crate::authz::Attributes> {
            self.seen.lock().expect("recorder mutex").last().cloned()
        }
        fn count(&self) -> usize {
            self.seen.lock().expect("recorder mutex").len()
        }
    }

    #[async_trait::async_trait]
    impl crate::authz::Authorizer for RecordingDeny {
        async fn authorize(&self, attrs: &crate::authz::Attributes) -> crate::authz::Decision {
            self.seen
                .lock()
                .expect("recorder mutex")
                .push(attrs.clone());
            crate::authz::Decision::NoOpinion
        }
    }

    /// The full router over an empty handler set, judged by a [`RecordingDeny`].
    fn recorded_router() -> (Router, Arc<RecordingDeny>) {
        let recorder = Arc::new(RecordingDeny::default());
        let state = RouterState::new(Vec::new()).with_authorizer(recorder.clone());
        (build(state), recorder)
    }

    /// A concrete request path for a route pattern: every `:param` and the
    /// `*rest` tail filled with a plausible resource-shaped value.
    fn concrete_path(pattern: &str) -> String {
        pattern
            .replace(":group", "apps")
            .replace(":version", "v1")
            .replace("*rest", "namespaces/default/pods")
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
    ) -> axum::http::StatusCode {
        use tower::ServiceExt as _;
        let mut req = axum::http::Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let req = req.body(Body::empty()).expect("test request");
        app.clone()
            .oneshot(req)
            .await
            .expect("the router is infallible")
            .status()
    }

    /// What authz must have been handed for `method uri`: the pure
    /// classification of the (already-decoded) path, as the anonymous user.
    fn expected_attrs(method: &str, path: &str, is_watch: bool) -> crate::authz::Attributes {
        crate::authz::Attributes::for_request(
            UserInfo::anonymous(),
            &crate::coords::RequestInfo::from_method_path(method, path, is_watch),
        )
    }

    #[tokio::test]
    async fn every_route_in_the_table_is_classified_before_authz() {
        let (app, recorder) = recorded_router();
        let table = route_table();
        // Positive control: the walk covers the resource catch-alls, not just
        // the static routes around them.
        let patterns: Vec<&str> = table.iter().map(|(p, _)| *p).collect();
        for must in ["/api/v1/*rest", "/apis/:group/:version/*rest", "/healthz"] {
            assert!(patterns.contains(&must), "{must} is in the route table");
        }

        let (mut judged, mut pre_authz) = (0usize, 0usize);
        for pattern in patterns {
            let path = concrete_path(pattern);
            let before = recorder.count();
            let status = send(&app, "GET", &path, &[]).await;
            let info = crate::coords::RequestInfo::from_method_path("GET", &path, false);
            if info.non_resource_url().is_some_and(is_always_allowed) {
                // Pre-authz health/version: authz still READ the stored
                // classification (a missing one is a 500) and let it through.
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{pattern} ({path}) reached its handler"
                );
                assert_eq!(recorder.count(), before, "{pattern} skips the authorizer");
                pre_authz += 1;
            } else {
                assert_eq!(
                    status,
                    StatusCode::FORBIDDEN,
                    "{pattern} ({path}) was judged"
                );
                assert_eq!(
                    recorder.last(),
                    Some(expected_attrs("GET", &path, false)),
                    "{pattern} was judged on the one classification of {path}"
                );
                judged += 1;
            }
        }
        assert_eq!(
            pre_authz, 4,
            "healthz, livez, readyz and version skip authz"
        );
        assert_eq!(judged + pre_authz, table.len(), "every route was walked");
    }

    #[tokio::test]
    async fn watch_requests_are_classified_as_watches_on_both_catch_alls() {
        let (app, recorder) = recorded_router();
        for (uri, path) in [
            (
                "/api/v1/namespaces/default/pods?watch=true",
                "/api/v1/namespaces/default/pods",
            ),
            (
                "/apis/apps/v1/namespaces/default/deployments?watch=1&timeoutSeconds=5",
                "/apis/apps/v1/namespaces/default/deployments",
            ),
            ("/api/v1/nodes?watch=yes", "/api/v1/nodes"),
        ] {
            assert_eq!(send(&app, "GET", uri, &[]).await, StatusCode::FORBIDDEN);
            let judged = recorder.last().expect("authz ran");
            assert_eq!(judged.verb, "watch", "{uri}");
            assert_eq!(judged, expected_attrs("GET", path, true), "{uri}");
        }
    }

    #[tokio::test]
    async fn websocket_upgrades_are_classified_before_authz() {
        // No route serves a WebSocket today (exec/attach/portforward are not
        // implemented), but an upgrade request must still be classified and
        // judged like any other: the layers wrap the upgrade handshake too.
        let (app, recorder) = recorded_router();
        let upgrade = [
            ("connection", "Upgrade"),
            ("upgrade", "websocket"),
            ("sec-websocket-version", "13"),
            ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ];

        let status = send(
            &app,
            "GET",
            "/api/v1/namespaces/default/pods/p1/exec?command=sh&stdin=true",
            &upgrade,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let judged = recorder.last().expect("authz ran");
        assert_eq!(judged.resource, "pods");
        assert_eq!(judged.subresource.as_deref(), Some("exec"));
        assert_eq!(judged.name.as_deref(), Some("p1"));
        assert_eq!(judged.verb, "get");

        // A watch negotiated over a WebSocket (client-go can) is a watch.
        let status = send(
            &app,
            "GET",
            "/api/v1/namespaces/default/pods?watch=true",
            &upgrade,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(recorder.last().map(|a| a.verb).as_deref(), Some("watch"));
    }

    #[tokio::test]
    async fn an_unrouted_path_is_classified_and_judged_too() {
        // The fallback is wrapped by the same layers, so a path no route
        // serves is judged as a non-resource URL rather than slipping past.
        let (app, recorder) = recorded_router();
        assert_eq!(
            send(&app, "GET", "/no/such/route", &[]).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            recorder.last(),
            Some(expected_attrs("GET", "/no/such/route", false))
        );
    }

    #[tokio::test]
    async fn an_encoded_separator_moves_authz_and_routing_together() {
        // `/apis/apps%2Fv1/...` is matched by the router as group `apps/v1`,
        // version `namespaces`. Authz and dispatch both read the DECODED
        // classification instead: deployments in `default`.
        let (app, recorder) = recorded_router();
        let status = send(
            &app,
            "GET",
            "/apis/apps%2Fv1/namespaces/default/deployments",
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let judged = recorder.last().expect("authz ran");
        assert_eq!(judged.group, "apps");
        assert_eq!(judged.version, "v1");
        assert_eq!(judged.resource, "deployments");
        assert_eq!(judged.namespace.as_deref(), Some("default"));
        assert_eq!(judged.verb, "list");
    }

    #[tokio::test]
    async fn a_path_that_is_not_utf8_once_decoded_is_a_typed_400_before_authz() {
        let (app, recorder) = recorded_router();
        assert_eq!(
            send(&app, "GET", "/api/v1/namespaces/default/pods/%FF", &[]).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(recorder.count(), 0, "nothing unclassifiable reaches authz");
    }

    #[tokio::test]
    async fn without_the_request_info_layer_authz_and_dispatch_fail_closed() {
        // Authz alone, no request-info layer: a typed 500, never a re-parse
        // of the path (and never an allow).
        let recorder = Arc::new(RecordingDeny::default());
        let authorizer: Arc<dyn crate::authz::Authorizer> = recorder.clone();
        let authz_only =
            build_routes(RouterState::new(Vec::new())).layer(axum::middleware::from_fn(
                move |req: axum::http::Request<Body>, next: axum::middleware::Next| {
                    let authorizer = authorizer.clone();
                    async move { authz_middleware(authorizer, req, next).await }
                },
            ));
        for uri in ["/healthz", "/api/v1/namespaces/default/pods", "/api"] {
            assert_eq!(
                send(&authz_only, "GET", uri, &[]).await,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{uri}"
            );
        }
        assert_eq!(
            recorder.count(),
            0,
            "an unclassified request is never judged"
        );

        // Dispatch alone: the ResourceCoords / RequestInfo extractors refuse
        // with the same typed 500 rather than reading the route params.
        let bare = build_routes(RouterState::new(Vec::new()));
        for (method, uri) in [
            ("GET", "/api/v1/namespaces/default/pods"),
            (
                "POST",
                "/api/v1/namespaces/default/serviceaccounts/foo/token",
            ),
            ("DELETE", "/apis/apps/v1/namespaces/default/deployments/web"),
        ] {
            assert_eq!(
                send(&bare, method, uri, &[]).await,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method} {uri}"
            );
        }
    }
}

/// T4.8 — a resolved subresource carries the instance name it targets, so no
/// dispatch arm re-reads `coords.name` and asserts it is present.
#[cfg(test)]
mod subresource_target {
    use super::*;
    use crate::coords::ResourceCoords;

    /// The cataloged Pod handler (declares `status` + `log`, not `token`).
    /// `resolve_subresource` reads only the descriptor, so the store is never
    /// touched — it exists because a handler cannot be built without one.
    async fn pod_handler() -> Arc<dyn ResourceHandler> {
        let cfg = engenho_store::default_config("router-subresource-target").expect("store config");
        let store = engenho_store::StoreMesh::start(
            1,
            "in-process://1".into(),
            engenho_store::InProcessRouter::new(),
            cfg,
        )
        .await
        .expect("store starts");
        Arc::new(
            crate::handler::StoreBackedHandler::for_kind(Arc::new(store), "Pod")
                .expect("Pod is cataloged"),
        )
    }

    fn pod_coords(name: Option<&str>, subresource: Option<&str>) -> ResourceCoords {
        ResourceCoords {
            group: None,
            version: Some("v1".into()),
            namespace: Some("default".into()),
            plural: "pods".into(),
            name: name.map(Into::into),
            subresource: subresource.map(Into::into),
        }
    }

    #[tokio::test]
    async fn a_declared_subresource_resolves_together_with_its_instance_name() {
        let h = pod_handler().await;
        let coords = pod_coords(Some("web-0"), Some("status"));
        match resolve_subresource(&coords, &h) {
            Ok(Some(target)) => assert_eq!(
                target,
                SubresourceTarget {
                    subresource: Subresource::Status,
                    name: "web-0",
                }
            ),
            Ok(None) => panic!("pods/web-0/status resolved to the base object"),
            Err(e) => panic!("pods/web-0/status is served: {e:?}"),
        }
        let coords = pod_coords(Some("web-1"), Some("log"));
        assert!(matches!(
            resolve_subresource(&coords, &h),
            Ok(Some(SubresourceTarget {
                subresource: Subresource::Log,
                name: "web-1",
            }))
        ));
    }

    #[tokio::test]
    async fn no_subresource_is_the_base_object_path() {
        let h = pod_handler().await;
        assert!(matches!(
            resolve_subresource(&pod_coords(Some("web-0"), None), &h),
            Ok(None)
        ));
        assert!(matches!(
            resolve_subresource(&pod_coords(None, None), &h),
            Ok(None)
        ));
    }

    /// A subresource with no instance name never becomes a target — this is
    /// the state the GET dispatch's old `expect` assumed away.
    #[tokio::test]
    async fn a_subresource_without_an_instance_name_is_a_bad_request() {
        let h = pod_handler().await;
        let coords = pod_coords(None, Some("status"));
        let resolved = resolve_subresource(&coords, &h);
        assert!(
            matches!(resolved, Err(ApiError::BadRequest(_))),
            "a collection-path subresource is a typed 400: {resolved:?}"
        );
    }

    #[tokio::test]
    async fn a_subresource_the_kind_does_not_declare_is_not_found() {
        let h = pod_handler().await;
        let coords = pod_coords(Some("web-0"), Some("token"));
        let resolved = resolve_subresource(&coords, &h);
        assert!(
            matches!(resolved, Err(ApiError::NotFound(_))),
            "Pod does not serve /token: {resolved:?}"
        );
    }
}
