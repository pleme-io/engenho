//! Axum router that wires K8s REST URL patterns to
//! [`ResourceHandler`] trait methods.
//!
//! The router supports kubectl's canonical URLs across BOTH the core
//! group (`/api/v1/…`) and named groups (`/apis/<group>/<version>/…`):
//!
//! Core group (`/api/v1`):
//!   * GET    /api/v1/namespaces/{ns}/{plural}/{name}
//!   * GET    /api/v1/namespaces/{ns}/{plural}
//!   * POST   /api/v1/namespaces/{ns}/{plural}
//!   * PATCH  /api/v1/namespaces/{ns}/{plural}/{name}
//!   * DELETE /api/v1/namespaces/{ns}/{plural}/{name}
//!   * GET/POST   /api/v1/{plural}                (cluster-scoped)
//!   * GET/PATCH/DELETE /api/v1/{plural}/{name}   (cluster-scoped)
//!
//! Named groups (`/apis/<group>/<version>`): the same eight shapes,
//! with `(group, version)` extracted from the path and matched against
//! the registered handler set keyed by `(group, version, plural)`.
//!
//! Discovery (`/api`, `/api/v1`, `/apis`, `/apis/<group>/<version>`) is
//! served by [`crate::discovery`] from the same handler set, so what is
//! advertised is exactly what is routable.

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
    pub handlers: Arc<HashMap<HandlerKey, Arc<dyn ResourceHandler>>>,
}

impl RouterState {
    #[must_use]
    pub fn new(handlers: Vec<Arc<dyn ResourceHandler>>) -> Self {
        let map: HashMap<HandlerKey, Arc<dyn ResourceHandler>> = handlers
            .into_iter()
            .map(|h| {
                (
                    (
                        h.group().to_string(),
                        h.version().to_string(),
                        h.plural().to_string(),
                    ),
                    h,
                )
            })
            .collect();
        Self {
            handlers: Arc::new(map),
        }
    }

    /// The registered handlers, for discovery folding. Order is
    /// unspecified (HashMap); discovery sorts for determinism.
    #[must_use]
    pub fn handler_set(&self) -> Vec<&Arc<dyn ResourceHandler>> {
        self.handlers.values().collect()
    }

    /// Resolve a CORE-group handler by plural (keyed on `("","v1",plural)`).
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when no core handler is registered for `plural`.
    fn lookup_core(&self, plural: &str) -> Result<&Arc<dyn ResourceHandler>, ApiError> {
        self.handlers
            .get(&(String::new(), "v1".to_string(), plural.to_string()))
            .ok_or_else(|| ApiError::NotFound(format!("unknown core kind plural: {plural}")))
    }

    /// Resolve a handler by the full `(group, version, plural)` triple.
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when no handler is registered for the triple.
    fn lookup(
        &self,
        group: &str,
        version: &str,
        plural: &str,
    ) -> Result<&Arc<dyn ResourceHandler>, ApiError> {
        self.handlers
            .get(&(group.to_string(), version.to_string(), plural.to_string()))
            .ok_or_else(|| {
                ApiError::NotFound(format!("unknown kind: {group}/{version}/{plural}"))
            })
    }
}

pub fn build(state: RouterState) -> Router {
    Router::new()
        // ── core group (/api/v1) ──────────────────────────────────────
        .route(
            "/api/v1/namespaces/:ns/:plural",
            get(list_namespaced).post(create_namespaced),
        )
        .route(
            "/api/v1/namespaces/:ns/:plural/:name",
            get(get_namespaced)
                .patch(patch_namespaced)
                .delete(delete_namespaced),
        )
        .route(
            "/api/v1/:plural",
            get(list_cluster_scoped).post(create_cluster_scoped),
        )
        .route(
            "/api/v1/:plural/:name",
            get(get_cluster_scoped)
                .patch(patch_cluster_scoped)
                .delete(delete_cluster_scoped),
        )
        // ── named groups (/apis/<group>/<version>) ────────────────────
        .route(
            "/apis/:group/:version/namespaces/:ns/:plural",
            get(list_ns_grouped).post(create_ns_grouped),
        )
        .route(
            "/apis/:group/:version/namespaces/:ns/:plural/:name",
            get(get_ns_grouped)
                .patch(patch_ns_grouped)
                .delete(delete_ns_grouped),
        )
        .route(
            "/apis/:group/:version/:plural",
            get(list_grouped).post(create_grouped),
        )
        .route(
            "/apis/:group/:version/:plural/:name",
            get(get_grouped).patch(patch_grouped).delete(delete_grouped),
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

/// Decode a PATCH request body. The patch Content-Types
/// (strategic/merge/json-patch) are all JSON-family and parse as JSON;
/// only a full-object replace via protobuf goes through the codec. An
/// unknown media type is a typed 415. (kubectl patch always sends a
/// JSON-family patch body, so the JSON branch is the live path.)
fn decode_patch_body(
    headers: &HeaderMap,
    raw: &[u8],
    gvk: &Gvk,
) -> Result<serde_json::Value, ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media = content_type.split(';').next().unwrap_or("").trim();
    // Every JSON-family patch media type parses as JSON. K8s patch
    // content-types: application/json-patch+json,
    // application/merge-patch+json,
    // application/strategic-merge-patch+json, application/apply-patch+yaml
    // (the +json family + the JSON subset of apply-patch). All are
    // JSON-decodable except apply-patch+yaml — which we accept as JSON too
    // (kubectl apply --server-side sends JSON-shaped YAML; full SSA is a
    // later phase). The default kubectl patch (no --type) is
    // strategic-merge-patch+json.
    if media.is_empty()
        || media.eq_ignore_ascii_case("application/json")
        || media.ends_with("+json")
        || media.ends_with("+yaml")
    {
        serde_json::from_slice(raw)
            .map_err(|e| ApiError::BadRequest(format!("invalid JSON patch body: {e}")))
    } else if is_protobuf_content_type(content_type) {
        // A protobuf full-object replace (rare): decode via the codec.
        let _ = gvk;
        Ok(kube_proto::decode_protobuf(raw)?)
    } else {
        Err(ApiError::UnsupportedMediaType(format!(
            "the body of the patch request was in an unsupported format; got {media:?}"
        )))
    }
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
    let patch = decode_patch_body(headers, raw, &gvk)?;
    let v = h.patch(ns, name, patch).await?;
    let codec = ResponseCodec::from_headers(headers);
    render_object(codec, &gvk, StatusCode::OK, v)
}

async fn do_delete(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    p: &ListWatchParams,
) -> Result<Response, ApiError> {
    // `?resourceVersion=N` is the K8s DELETE precondition
    // (`Preconditions.resourceVersion`); absent/"0" → unconditional.
    let expected = p.precondition()?;
    h.delete_with_precondition(ns, name, expected).await?;
    Ok(StatusCode::OK.into_response())
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

// ── core-group route handlers (/api/v1) ────────────────────────────────

async fn get_namespaced(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((ns, plural, name)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_get(h, Some(&ns), &name, ResponseCodec::from_headers(&headers)).await
}

async fn list_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural)): Path<(String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?.clone();
    do_list_or_watch(h, Some(ns), p).await
}

async fn create_namespaced(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((ns, plural)): Path<(String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_create(h, Some(&ns), &headers, &raw).await
}

async fn patch_namespaced(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((ns, plural, name)): Path<(String, String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_patch(h, Some(&ns), &name, &headers, &raw).await
}

async fn delete_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural, name)): Path<(String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_delete(h, Some(&ns), &name, &p).await
}

async fn get_cluster_scoped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((plural, name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_get(h, None, &name, ResponseCodec::from_headers(&headers)).await
}

async fn list_cluster_scoped(
    State(state): State<RouterState>,
    Path(plural): Path<String>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?.clone();
    do_list_or_watch(h, None, p).await
}

async fn create_cluster_scoped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path(plural): Path<String>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_create(h, None, &headers, &raw).await
}

async fn patch_cluster_scoped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((plural, name)): Path<(String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_patch(h, None, &name, &headers, &raw).await
}

async fn delete_cluster_scoped(
    State(state): State<RouterState>,
    Path((plural, name)): Path<(String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_delete(h, None, &name, &p).await
}

// ── named-group route handlers (/apis/<group>/<version>) ───────────────

async fn get_ns_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, ns, plural, name)): Path<(String, String, String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_get(h, Some(&ns), &name, ResponseCodec::from_headers(&headers)).await
}

async fn list_ns_grouped(
    State(state): State<RouterState>,
    Path((group, version, ns, plural)): Path<(String, String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?.clone();
    do_list_or_watch(h, Some(ns), p).await
}

async fn create_ns_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, ns, plural)): Path<(String, String, String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_create(h, Some(&ns), &headers, &raw).await
}

async fn patch_ns_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, ns, plural, name)): Path<(String, String, String, String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_patch(h, Some(&ns), &name, &headers, &raw).await
}

async fn delete_ns_grouped(
    State(state): State<RouterState>,
    Path((group, version, ns, plural, name)): Path<(String, String, String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_delete(h, Some(&ns), &name, &p).await
}

async fn get_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_get(h, None, &name, ResponseCodec::from_headers(&headers)).await
}

async fn list_grouped(
    State(state): State<RouterState>,
    Path((group, version, plural)): Path<(String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?.clone();
    do_list_or_watch(h, None, p).await
}

async fn create_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, plural)): Path<(String, String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_create(h, None, &headers, &raw).await
}

async fn patch_grouped(
    State(state): State<RouterState>,
    headers: HeaderMap,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_patch(h, None, &name, &headers, &raw).await
}

async fn delete_grouped(
    State(state): State<RouterState>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_delete(h, None, &name, &p).await
}
