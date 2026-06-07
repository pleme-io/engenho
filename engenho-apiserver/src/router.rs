//! Axum router that wires K8s REST URL patterns to
//! [`ResourceHandler`] trait methods.
//!
//! The router supports kubectl's canonical URLs:
//!
//!   * GET    /api/v1/namespaces/{ns}/{plural}/{name}
//!   * GET    /api/v1/namespaces/{ns}/{plural}
//!   * POST   /api/v1/namespaces/{ns}/{plural}
//!   * PATCH  /api/v1/namespaces/{ns}/{plural}/{name}
//!   * DELETE /api/v1/namespaces/{ns}/{plural}/{name}
//!
//! Future R7.5+ adds the cluster-scoped variants (no /namespaces
//! segment) + the apps/v1 / rbac.authorization.k8s.io/v1 group
//! prefixes.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use engenho_store::{WatchGone, WatchSignal, WatchStream};
use utoipa::OpenApi;

use crate::error::ApiError;
use crate::handler::ResourceHandler;
use crate::openapi::ApiDoc;
use crate::params::{
    ListWatchParams, ResumePoint, Selectors, bookmark_line, gvk_ns_matches, status_410_line,
    to_k8s_watch_line,
};

#[derive(Clone)]
pub struct RouterState {
    /// plural → handler. Lookup is O(1).
    pub handlers: Arc<HashMap<String, Arc<dyn ResourceHandler>>>,
}

impl RouterState {
    pub fn new(handlers: Vec<Arc<dyn ResourceHandler>>) -> Self {
        let map: HashMap<String, Arc<dyn ResourceHandler>> = handlers
            .into_iter()
            .map(|h| (h.plural().to_string(), h))
            .collect();
        Self {
            handlers: Arc::new(map),
        }
    }

    fn lookup(&self, plural: &str) -> Result<&Arc<dyn ResourceHandler>, ApiError> {
        self.handlers
            .get(plural)
            .ok_or_else(|| ApiError::NotFound(format!("unknown kind plural: {plural}")))
    }
}

pub fn build(state: RouterState) -> Router {
    Router::new()
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

/// The shared LIST/WATCH body for the cluster-scoped + namespaced cases.
///
///   * `p.watch == false` → the atomic-rv LIST envelope (selectors
///     applied apiserver-side; rv = `current_revision`).
///   * `p.watch == true`  → the streaming chunked NDJSON WATCH (the K8s
///     list-then-watch contract).
async fn list_or_watch(
    h: Arc<dyn ResourceHandler>,
    namespace: Option<String>,
    p: ListWatchParams,
) -> Result<Response, ApiError> {
    let sel = p.selectors()?;
    if p.watch {
        watch_response(h, namespace, p, sel).await
    } else {
        let (items, rv) = h.list_at(namespace.as_deref(), &sel).await?;
        Ok(Json(h.list_response(items, rv)).into_response())
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

async fn get_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural, name)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.get(Some(&ns), &name).await?;
    Ok(Json(v))
}

async fn list_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural)): Path<(String, String)>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&plural)?.clone();
    list_or_watch(h, Some(ns), p).await
}

async fn create_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.create(Some(&ns), body).await?;
    Ok((StatusCode::CREATED, Json(v)))
}

async fn patch_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural, name)): Path<(String, String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.patch(Some(&ns), &name, patch_body).await?;
    Ok(Json(v))
}

async fn delete_namespaced(
    State(state): State<RouterState>,
    Path((ns, plural, name)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    h.delete(Some(&ns), &name).await?;
    Ok(StatusCode::OK)
}

async fn get_cluster_scoped(
    State(state): State<RouterState>,
    Path((plural, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.get(None, &name).await?;
    Ok(Json(v))
}

async fn list_cluster_scoped(
    State(state): State<RouterState>,
    Path(plural): Path<String>,
    Query(p): Query<ListWatchParams>,
) -> Result<Response, ApiError> {
    let h = state.lookup(&plural)?.clone();
    list_or_watch(h, None, p).await
}

async fn create_cluster_scoped(
    State(state): State<RouterState>,
    Path(plural): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.create(None, body).await?;
    Ok((StatusCode::CREATED, Json(v)))
}

async fn patch_cluster_scoped(
    State(state): State<RouterState>,
    Path((plural, name)): Path<(String, String)>,
    Json(patch_body): Json<serde_json::Value>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.patch(None, &name, patch_body).await?;
    Ok(Json(v))
}

async fn delete_cluster_scoped(
    State(state): State<RouterState>,
    Path((plural, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    h.delete(None, &name).await?;
    Ok(StatusCode::OK)
}
