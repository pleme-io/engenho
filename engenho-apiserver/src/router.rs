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
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use engenho_store::{WatchGone, WatchSignal, WatchStream};
use utoipa::OpenApi;

use crate::discovery;
use crate::error::ApiError;
use crate::handler::ResourceHandler;
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
        .route("/openapi.json", get(openapi_spec))
        .route("/openapi/v3", get(openapi_spec))
        .with_state(state)
}

/// The OpenAPI v3 spec — the central machine-readable description
/// from which gRPC, GraphQL, and downstream SDKs derive. Per the
/// multi-face plan in docs/API-SURFACE.md.
async fn openapi_spec() -> impl IntoResponse {
    Json(ApiDoc::openapi())
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
) -> Result<Response, ApiError> {
    let v = h.get(ns, name).await?;
    Ok(Json(v).into_response())
}

async fn do_create(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    body: serde_json::Value,
) -> Result<Response, ApiError> {
    let v = h.create(ns, body).await?;
    Ok((StatusCode::CREATED, Json(v)).into_response())
}

async fn do_patch(
    h: &Arc<dyn ResourceHandler>,
    ns: Option<&str>,
    name: &str,
    patch: serde_json::Value,
) -> Result<Response, ApiError> {
    let v = h.patch(ns, name, patch).await?;
    Ok(Json(v).into_response())
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
    Path((ns, plural, name)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_get(h, Some(&ns), &name).await
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
    Path((ns, plural)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_create(h, Some(&ns), body).await
}

async fn patch_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural, name)): Path<(String, String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_patch(h, Some(&ns), &name, patch_body).await
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
    Path((plural, name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_get(h, None, &name).await
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
    Path(plural): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_create(h, None, body).await
}

async fn patch_cluster_scoped(
    State(state): State<RouterState>,
    Path((plural, name)): Path<(String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup_core(&plural)?;
    do_patch(h, None, &name, patch_body).await
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
    Path((group, version, ns, plural, name)): Path<(String, String, String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_get(h, Some(&ns), &name).await
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
    Path((group, version, ns, plural)): Path<(String, String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_create(h, Some(&ns), body).await
}

async fn patch_ns_grouped(
    State(state): State<RouterState>,
    Path((group, version, ns, plural, name)): Path<(String, String, String, String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_patch(h, Some(&ns), &name, patch_body).await
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
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_get(h, None, &name).await
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
    Path((group, version, plural)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_create(h, None, body).await
}

async fn patch_grouped(
    State(state): State<RouterState>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_patch(h, None, &name, patch_body).await
}

async fn delete_grouped(
    State(state): State<RouterState>,
    Path((group, version, plural, name)): Path<(String, String, String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&group, &version, &plural)?;
    do_delete(h, None, &name, &p).await
}
