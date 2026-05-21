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
use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use axum::Router;

use crate::error::ApiError;
use crate::handler::ResourceHandler;

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
        .route("/api/v1/:plural", get(list_cluster_scoped).post(create_cluster_scoped))
        .route(
            "/api/v1/:plural/:name",
            get(get_cluster_scoped)
                .patch(patch_cluster_scoped)
                .delete(delete_cluster_scoped),
        )
        .with_state(state)
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
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.list(Some(&ns)).await?;
    Ok(Json(v))
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
) -> Result<impl IntoResponse, ApiError> {
    let h = state.lookup(&plural)?;
    let v = h.list(None).await?;
    Ok(Json(v))
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
