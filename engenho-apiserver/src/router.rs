//! Axum router that wires K8s REST URL patterns to
//! [`ResourceHandler`] trait methods.
//!
//! The router supports kubectl's canonical URLs across BOTH the core
//! group (`/api/v1/…`) and named groups (`/apis/<group>/<version>/…`)
//! through ONE coords → dispatch path: two catch-all routes feed the
//! [`crate::coords::ResourceCoords`] extractor (the single place URL
//! shapes are parsed), and scope-agnostic per-method verb handlers resolve
//! a handler via ONE resolver ([`RouterState::lookup`]) then delegate to
//! the shared per-verb `do_*` bodies. This collapses the ~20 hand-fanned
//! per-scope/per-verb route wrappers (5 verbs × 4 scope/group shapes) into
//! one extractor + four method handlers (GET fans LIST/WATCH vs single-GET
//! internally on `coords.name`).
//!
//! Feeding routes:
//!   * `/api/v1/*rest`               → core group (extractor synthesizes
//!                                      `group=None, version="v1"`).
//!   * `/apis/:group/:version/*rest` → named group (`group`/`version` from
//!                                      the path).
//!
//! The extractor decomposes the `*rest` tail into the six K8s resource URL
//! shapes (namespaced/cluster × collection/instance, + an optional
//! subresource segment); the verb handlers pick list-vs-watch +
//! collection-vs-instance from `?watch=` + `coords.name`. A handler-map
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
use engenho_store::{WatchGone, WatchSignal, WatchStream};
use engenho_types::generated_v1_34::Subresource;
use engenho_types::patch::PatchType;
use utoipa::OpenApi;

use crate::discovery;
use crate::error::ApiError;
use crate::handler::ResourceHandler;
use crate::health;
use crate::openapi::ApiDoc;
use crate::params::{
    ListWatchParams, ResumePoint, Selectors, bookmark_line, gvk_ns_matches, status_410_line,
    to_k8s_watch_line,
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
        }
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
    Router::new()
        // ── resources: ONE coords→dispatch path, fed by two catch-alls ─
        //
        // The ~20 hand-fanned per-scope/per-verb wrappers collapse into
        // the [`ResourceCoords`] extractor + the scope-agnostic verb
        // handlers (one per HTTP method: GET handles both LIST/WATCH and
        // single-GET, branching on `coords.name`; POST/PATCH/DELETE map to
        // create/patch/delete). Two catch-all routes feed the extractor:
        //
        //   * `/api/v1/*rest`               → core group (the extractor
        //                                      synthesizes group=None,
        //                                      version="v1").
        //   * `/apis/:group/:version/*rest` → named group.
        //
        // Each catch-all registers EXACTLY the methods the K8s wire
        // supports for a resource path; the extractor + `?watch=` flag
        // pick list-vs-watch + collection-vs-instance from `coords.name`.
        // An unrouted method on a matched resource path yields axum's 405
        // — the same terminal semantics the legacy MethodRouter gave (e.g.
        // PUT was never registered, so it 405'd then too). The discovery /
        // openapi / health routes below are MORE-SPECIFIC (static or a
        // shallower param leaf) and take precedence over the catch-alls in
        // matchit (verified: `/api/v1` exact beats `/api/v1/*rest`, and
        // `/apis/:g/:v` coexists with the one-segment-deeper catch-all).
        .route(
            "/api/v1/*rest",
            get(resource_get_or_list)
                .post(resource_create)
                .put(resource_put)
                .patch(resource_patch)
                .delete(resource_delete),
        )
        .route(
            "/apis/:group/:version/*rest",
            get(resource_get_or_list)
                .post(resource_create)
                .put(resource_put)
                .patch(resource_patch)
                .delete(resource_delete),
        )
        // ── discovery ─────────────────────────────────────────────────
        .route("/api", get(discovery::api_versions))
        .route("/api/v1", get(discovery::core_resources))
        .route("/apis", get(discovery::api_groups))
        .route("/apis/:group/:version", get(discovery::group_resources))
        // ── openapi ───────────────────────────────────────────────────
        // `/openapi.json` keeps the utoipa-derived description of engenho's
        // own REST surface (SDK/codegen consumers). `/openapi/v3` is the
        // K8s OpenAPI-v3 DISCOVERY surface kubectl `apply --validate` +
        // `explain` consume — a typed index + per-group vendored schemas,
        // scoped to exactly the cataloged groups.
        .route("/openapi.json", get(openapi_spec))
        .route("/openapi/v3", get(openapi_v3_index))
        .route("/openapi/v3/api/v1", get(openapi_v3_core))
        .route(
            "/openapi/v3/apis/:group/:version",
            get(openapi_v3_group),
        )
        // ── version + health (no RouterState; kubectl/client-go probe
        //    these before they will trust the server) ──────────────────
        .route("/version", get(health::version))
        .route("/readyz", get(health::readyz))
        .route("/livez", get(health::livez))
        .route("/healthz", get(health::healthz))
        .with_state(state)
}

/// The OpenAPI v3 spec — the central machine-readable description
/// from which gRPC, GraphQL, and downstream SDKs derive. Per the
/// multi-face plan in docs/API-SURFACE.md.
async fn openapi_spec() -> impl IntoResponse {
    Json(ApiDoc::openapi())
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
        Some(body) => Ok((
            StatusCode::OK,
            [(CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()),
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
}

impl ResponseCodec {
    /// Negotiate from the request headers' `Accept`. Defaults to JSON
    /// when `Accept` is absent or does not list protobuf.
    fn from_headers(headers: &HeaderMap) -> Self {
        let accept = headers
            .get(ACCEPT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if response_wants_protobuf(accept) {
            ResponseCodec::Protobuf
        } else {
            ResponseCodec::Json
        }
    }
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
fn render_object(
    codec: ResponseCodec,
    gvk: &Gvk,
    status: StatusCode,
    value: serde_json::Value,
) -> Result<Response, ApiError> {
    match codec {
        ResponseCodec::Json => Ok((status, Json(value)).into_response()),
        ResponseCodec::Protobuf => {
            // The read-back Value carries apiVersion+kind from
            // inject_type_meta; the codec re-derives the per-kind
            // descriptor from `gvk` (the handler's GVK), so the response
            // wraps correctly even if the stored object omitted TypeMeta.
            let bytes = kube_proto::encode_response(gvk, &value)?;
            Ok((
                status,
                [(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)],
                bytes,
            )
                .into_response())
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
) -> Result<Response, ApiError> {
    let body = decode_write_body(headers, raw)?;
    let v = h.create(ns, body).await?;
    let codec = ResponseCodec::from_headers(headers);
    render_object(codec, &handler_gvk(h), StatusCode::CREATED, v)
}

async fn do_patch(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    raw: &[u8],
) -> Result<Response, ApiError> {
    let gvk = handler_gvk(h);
    // Resolve the typed patch algorithm from the Content-Type FIRST — the
    // media type is the load-bearing signal the store dispatches on. Erasing
    // it here (the old `decode_patch_body` did) made every patch a merge.
    let (patch, patch_type) = decode_patch(headers, raw, &gvk)?;
    let v = h.patch(ns, name, patch, patch_type).await?;
    let codec = ResponseCodec::from_headers(headers);
    render_object(codec, &gvk, StatusCode::OK, v)
}

async fn do_delete(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    p: &ListWatchParams,
) -> Result<Response, ApiError> {
    // `?resourceVersion=N` is the K8s DELETE precondition
    // (`Preconditions.resourceVersion`); absent/"0" → unconditional.
    let expected = p.precondition()?;
    // The K8s DELETE wire returns a NON-empty typed body (the deleted
    // object, or a `metav1.Status{status:"Success"}` when the name was
    // absent). An empty 200 crashes kubectl's `json.Unmarshal([]byte{})`
    // ("unexpected end of JSON input"). This is the SAME content-negotiated
    // emission chokepoint create/get use — zero new serialization code.
    let obj = h.delete_with_precondition(ns, name, expected).await?;
    let codec = ResponseCodec::from_headers(headers);
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

/// The shared LIST/WATCH body for both the core + grouped cases.
///
///   * `p.watch == false` → the atomic-rv LIST envelope (selectors
///     applied apiserver-side; rv = `current_revision`).
///   * `p.watch == true`  → the streaming chunked NDJSON WATCH (the K8s
///     list-then-watch contract).
async fn do_list_or_watch(
    h: Arc<dyn ResourceHandler>,
    namespace: Option<String>,
    p: ListWatchParams,
) -> Result<Response, ApiError> {
    let sel = p.selectors()?;
    if p.watch {
        watch_response(h, namespace, p, sel).await
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
            Ok(Json(h.list_response(items, rv, cont, remaining)).into_response())
        } else {
            let (items, rv) = h.list_at(namespace.as_deref(), &sel).await?;
            Ok(Json(h.list_response(items, rv, None, None)).into_response())
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
) -> Result<Response, ApiError> {
    let from: ResumePoint = p.resume_point()?;
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
    };

    let body = Body::from_stream(futures::stream::unfold(init, |mut st| async move {
        loop {
            match st.stream.next().await {
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
                    let line = to_k8s_watch_line(&ev);
                    return Some((Ok::<Bytes, Infallible>(line), st));
                }
                Some(Ok(WatchSignal::Bookmark(rev))) => {
                    if st.allow_bookmarks {
                        return Some((Ok(bookmark_line(rev)), st));
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
    }));

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
// Each handler takes the [`ResourceCoords`] extractor (the ONE place URL
// shapes are parsed) + does exactly: resolve the handler via the SINGLE
// resolver `state.lookup(coords.group_key(), coords.version_key(),
// &coords.plural)`, then delegate to the matching shared `do_*` body. The
// namespaced-vs-cluster scope assertion still lives inside
// `StoreBackedHandler::key()` (typed 400 on mismatch); coords just carries
// `namespace: Option<String>` straight through. No subresource handler
// exists today — a `Some(subresource)` returns a typed `NotFound` (no stub
// Ok), reserved for the status/scale follow-up.

/// Resolve `coords.subresource` against the resolved handler's catalog-
/// declared subresource set. The router NEVER special-cases a kind by name:
/// the handler's `descriptor.subresources` (sourced from `RESOURCE_CATALOG`)
/// is the single authority for whether a subresource is served.
///
///   * `None`               → `Ok(None)` (the base-object verb path).
///   * `Some("status")` + the kind declares `Subresource::Status`
///                          → `Ok(Some(Subresource::Status))`.
///   * `Some("scale")`  + the kind declares `Subresource::Scale`
///                          → `Ok(Some(Subresource::Scale))`.
///   * anything else, or a declared-absent subresource
///                          → a typed K8s `Status` 404 (no stub Ok, no panic).
///
/// `name` is required for a subresource (a subresource always targets an
/// instance) — a collection-path subresource is a typed `BadRequest`.
fn resolve_subresource(
    coords: &crate::coords::ResourceCoords,
    h: &Arc<dyn ResourceHandler>,
) -> Result<Option<Subresource>, ApiError> {
    let Some(sub) = coords.subresource.as_deref() else {
        return Ok(None);
    };
    if coords.name.is_none() {
        return Err(ApiError::BadRequest(format!(
            "subresource {sub:?} requires a resource name (instance path)"
        )));
    }
    let parsed = match sub {
        "status" if h.subresources().contains(&Subresource::Status) => Subresource::Status,
        "scale" if h.subresources().contains(&Subresource::Scale) => Subresource::Scale,
        other => {
            return Err(ApiError::NotFound(format!(
                "the server could not find the requested resource: {} does not serve subresource {:?}",
                coords.plural, other
            )));
        }
    };
    Ok(Some(parsed))
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
    let codec = ResponseCodec::from_headers(headers);
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
    let codec = ResponseCodec::from_headers(headers);
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
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    // Resolve the handler FIRST (the subresource is served by the parent
    // kind's handler), then dispatch on the typed subresource resolved from
    // the catalog.
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    if let Some(sub) = resolve_subresource(&coords, &h)? {
        // A subresource always targets an instance — `name` is present
        // (resolve_subresource enforces it).
        let name = coords.name.as_deref().expect("subresource requires a name");
        let codec = ResponseCodec::from_headers(&headers);
        return match sub {
            Subresource::Status => {
                do_get_status(&h, coords.namespace.as_deref(), name, codec).await
            }
            Subresource::Scale => do_get_scale(&h, coords.namespace.as_deref(), name).await,
        };
    }
    match &coords.name {
        // Collection GET → LIST or WATCH (the shared body branches on
        // `p.watch`). `lookup` already returns an owned `Arc` (the
        // ArcSwap-snapshot clone) — exactly what the watch unfold stream
        // needs, no extra `.clone()`.
        None => do_list_or_watch(h, coords.namespace, p).await,
        // Instance GET → the shared `do_get` body. Bind the owned `Arc`,
        // pass it by reference (`do_get` takes `&Arc`).
        Some(name) => {
            do_get(
                &h,
                coords.namespace.as_deref(),
                name,
                ResponseCodec::from_headers(&headers),
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
async fn resource_create(
    State(state): State<RouterState>,
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    // POST on a subresource is not a K8s CREATE shape — no subresource
    // supports create (status/scale are get/patch/update only). Typed
    // BadRequest, never a stub Ok.
    if let Some(sub) = &coords.subresource {
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
    do_create(&h, coords.namespace.as_deref(), &headers, &raw).await
}

/// PUT on a resource path. PUT is the kubectl full-object-replace verb for
/// `/status` and `/scale` (kubectl writes both via PUT). On the MAIN object
/// path (no subresource) engenho uses POST-create + PATCH, so a PUT there is
/// a typed `BadRequest` mirroring the upstream contract — never a silent
/// stub.
async fn resource_put(
    State(state): State<RouterState>,
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let name = coords.name.as_deref().ok_or_else(|| {
        ApiError::BadRequest("PUT requires a resource name (instance path)".into())
    })?;
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    match resolve_subresource(&coords, &h)? {
        Some(Subresource::Status) => {
            do_put_status(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        Some(Subresource::Scale) => {
            do_put_scale(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        // PUT on the main object (no subresource) — engenho uses POST/PATCH;
        // mirror upstream with a typed BadRequest.
        None => Err(ApiError::BadRequest(
            "PUT on the main object is not supported (use POST to create, PATCH to update)".into(),
        )),
    }
}

/// PATCH on a resource path — PATCH a single instance (requires `name`).
async fn resource_patch(
    State(state): State<RouterState>,
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let name = coords.name.as_deref().ok_or_else(|| {
        ApiError::BadRequest("PATCH requires a resource name (instance path)".into())
    })?;
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    match resolve_subresource(&coords, &h)? {
        Some(Subresource::Status) => {
            do_patch_status(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        Some(Subresource::Scale) => {
            do_patch_scale(&h, coords.namespace.as_deref(), name, &headers, &raw).await
        }
        None => do_patch(&h, coords.namespace.as_deref(), name, &headers, &raw).await,
    }
}

/// DELETE on a resource path — DELETE a single instance (requires `name`).
async fn resource_delete(
    State(state): State<RouterState>,
    coords: crate::coords::ResourceCoords,
    headers: HeaderMap,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    // DELETE on a subresource is invalid — no subresource supports delete
    // (status/scale are get/patch/update only). Typed BadRequest, never a
    // stub Ok.
    if let Some(sub) = &coords.subresource {
        return Err(ApiError::BadRequest(format!(
            "the {sub:?} subresource does not support delete"
        )));
    }
    let name = coords.name.as_deref().ok_or_else(|| {
        ApiError::BadRequest("DELETE requires a resource name (instance path)".into())
    })?;
    let h = state.lookup(coords.group_key(), coords.version_key(), &coords.plural)?;
    do_delete(&h, coords.namespace.as_deref(), name, &headers, &p).await
}

#[cfg(test)]
mod tests {
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
            grouped, "resource not found: unknown kind: apps/v1/deployments",
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
            Err(ApiError::Internal("fake handler: list not exercised".into()))
        }
        async fn list_at(
            &self,
            _ns: Option<&str>,
            _sel: &crate::params::Selectors,
        ) -> Result<(Vec<serde_json::Value>, engenho_store::Revision), ApiError> {
            Err(ApiError::Internal("fake handler: list_at not exercised".into()))
        }
        async fn list_page(
            &self,
            _ns: Option<&str>,
            _sel: &crate::params::Selectors,
            _limit: usize,
            _continue_token: Option<engenho_store::ContinueToken>,
        ) -> Result<(Vec<serde_json::Value>, engenho_store::Revision, Option<String>, Option<u64>), ApiError>
        {
            Err(ApiError::Internal("fake handler: list_page not exercised".into()))
        }
        async fn watch_stream(
            &self,
            _ns: Option<&str>,
            _from: crate::params::ResumePoint,
            _allow_bookmarks: bool,
        ) -> Result<engenho_store::WatchStream, ApiError> {
            Err(ApiError::Internal("fake handler: watch_stream not exercised".into()))
        }
        async fn create(
            &self,
            _ns: Option<&str>,
            _body: serde_json::Value,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal("fake handler: create not exercised".into()))
        }
        async fn patch(
            &self,
            _ns: Option<&str>,
            _name: &str,
            _patch: serde_json::Value,
            _patch_type: engenho_types::patch::PatchType,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal("fake handler: patch not exercised".into()))
        }
        async fn delete(
            &self,
            _ns: Option<&str>,
            _name: &str,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal("fake handler: delete not exercised".into()))
        }
        async fn delete_with_precondition(
            &self,
            _ns: Option<&str>,
            _name: &str,
            _expected: Option<engenho_store::Revision>,
        ) -> Result<serde_json::Value, ApiError> {
            Err(ApiError::Internal("fake handler: delete_with_precondition not exercised".into()))
        }
    }

    #[test]
    fn register_then_lookup_resolves_the_handler() {
        let state = RouterState::new(Vec::new());
        // Empty table → a NotFound miss.
        assert!(state.lookup("example.com", "v1", "widgets").is_err());

        state.register(FakeHandler::arc("example.com", "v1", "Widget", "widgets", true));

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
        state.register(FakeHandler::arc("example.com", "v1", "Widget", "widgets", true));
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
        state.register(FakeHandler::arc("example.com", "v1", "Widget", "widgets", true));
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
        assert_eq!(state.handler_set().len(), 100, "no lost write under racing rcu");
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
