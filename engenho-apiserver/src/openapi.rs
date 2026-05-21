//! OpenAPI v3 spec emitter — the canonical machine-readable
//! description of engenho-apiserver's REST surface.
//!
//! The spec is generated at compile time via utoipa derive
//! macros + served at /openapi.json. From it, downstream
//! tooling (openapi-generator-cli, swagger-ui, postman) builds
//! SDKs in any target language + interactive docs.
//!
//! Per the multi-face plan (docs/API-SURFACE.md), the OpenAPI
//! spec is the **central structure** all faces derive from:
//!   * REST    — already lives here
//!   * gRPC    — R7.6 will translate the spec to .proto
//!   * GraphQL — R7.7 will derive a schema from the spec
//!   * SDKs    — R7.8 wires openapi-generator-cli in release.yml

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

/// JSON-shaped resource — used in all CRUD payloads. Opaque at
/// R7.5a (matches the StoreMesh's serde_json::Value catalog);
/// per-kind typed schemas via engenho-types come at R7.5c+.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct K8sResource {
    /// API version, e.g. "v1" or "apps/v1".
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Kind name, e.g. "Pod" or "Deployment".
    pub kind: String,
    /// Standard K8s object metadata.
    pub metadata: serde_json::Value,
    /// Opaque spec for now. R7.5c: per-kind typed via engenho-types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<serde_json::Value>,
    /// Opaque status for now. R7.5c: per-kind typed via engenho-types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<serde_json::Value>,
}

/// K8s-style list envelope.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct K8sResourceList {
    /// e.g. "PodList", "DeploymentList".
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// List items.
    pub items: Vec<K8sResource>,
    /// resourceVersion + continue cursor.
    pub metadata: serde_json::Value,
}

/// K8s-style error envelope. Mapped from `ApiError` at the
/// response layer.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct K8sStatus {
    /// Always "Status".
    pub kind: String,
    /// "v1".
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// "Failure" for errors.
    pub status: String,
    pub code: u16,
    pub reason: String,
    pub message: String,
}

/// JSON merge patch payload.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct K8sPatch {
    /// The merge patch body.
    #[serde(flatten)]
    pub patch: serde_json::Value,
}

/// The OpenAPI document for engenho-apiserver's K8s REST surface.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "engenho-apiserver — K8s REST API",
        version = "0.5.1",
        description = "HTTP REST surface for the engenho K8s base layer. Backed by engenho-store (openraft-replicated, BLAKE3+ed25519 attested). Every face — REST, gRPC, GraphQL — derives from this OpenAPI spec; see docs/API-SURFACE.md.",
        license(name = "MIT")
    ),
    paths(
        // Namespaced kinds — Pod et al
        get_namespaced_resource,
        list_namespaced_resources,
        create_namespaced_resource,
        patch_namespaced_resource,
        delete_namespaced_resource,
        // Cluster-scoped kinds — Namespace, Node, ClusterRole, …
        get_cluster_scoped_resource,
        list_cluster_scoped_resources,
        create_cluster_scoped_resource,
        patch_cluster_scoped_resource,
        delete_cluster_scoped_resource,
    ),
    components(schemas(K8sResource, K8sResourceList, K8sStatus, K8sPatch))
)]
pub struct ApiDoc;

// ---------------------------------------------------------------------------
//   Path declarations — these mirror the real Axum routes in router.rs.
//
//   We declare them as utoipa::path attributes pointing at uninstantiated
//   functions whose only purpose is to carry the macro annotations. The
//   real handlers (in router.rs) implement the same shapes; future R7.8
//   tooling (openapi-generator-cli) emits SDKs that consume them.
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{ns}/{plural}/{name}",
    params(
        ("ns" = String, Path, description = "Namespace"),
        ("plural" = String, Path, description = "Resource plural (pods, services, …)"),
        ("name" = String, Path, description = "Resource name"),
    ),
    responses(
        (status = 200, description = "Resource fetched", body = K8sResource),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "namespaced"
)]
pub fn get_namespaced_resource() {}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{ns}/{plural}",
    params(
        ("ns" = String, Path, description = "Namespace"),
        ("plural" = String, Path, description = "Resource plural"),
    ),
    responses(
        (status = 200, description = "List of resources", body = K8sResourceList),
        (status = 404, description = "Unknown kind", body = K8sStatus),
    ),
    tag = "namespaced"
)]
pub fn list_namespaced_resources() {}

#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{ns}/{plural}",
    params(
        ("ns" = String, Path, description = "Namespace"),
        ("plural" = String, Path, description = "Resource plural"),
    ),
    request_body = K8sResource,
    responses(
        (status = 201, description = "Resource created", body = K8sResource),
        (status = 400, description = "Invalid body", body = K8sStatus),
        (status = 409, description = "Resource already exists", body = K8sStatus),
    ),
    tag = "namespaced"
)]
pub fn create_namespaced_resource() {}

#[utoipa::path(
    patch,
    path = "/api/v1/namespaces/{ns}/{plural}/{name}",
    params(
        ("ns" = String, Path, description = "Namespace"),
        ("plural" = String, Path, description = "Resource plural"),
        ("name" = String, Path, description = "Resource name"),
    ),
    request_body = K8sPatch,
    responses(
        (status = 200, description = "Resource patched", body = K8sResource),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "namespaced"
)]
pub fn patch_namespaced_resource() {}

#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{ns}/{plural}/{name}",
    params(
        ("ns" = String, Path, description = "Namespace"),
        ("plural" = String, Path, description = "Resource plural"),
        ("name" = String, Path, description = "Resource name"),
    ),
    responses(
        (status = 200, description = "Resource deleted"),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "namespaced"
)]
pub fn delete_namespaced_resource() {}

#[utoipa::path(
    get,
    path = "/api/v1/{plural}/{name}",
    params(
        ("plural" = String, Path, description = "Cluster-scoped resource plural"),
        ("name" = String, Path, description = "Resource name"),
    ),
    responses(
        (status = 200, description = "Resource fetched", body = K8sResource),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "cluster-scoped"
)]
pub fn get_cluster_scoped_resource() {}

#[utoipa::path(
    get,
    path = "/api/v1/{plural}",
    params(("plural" = String, Path, description = "Cluster-scoped resource plural")),
    responses(
        (status = 200, description = "List of resources", body = K8sResourceList),
        (status = 404, description = "Unknown kind", body = K8sStatus),
    ),
    tag = "cluster-scoped"
)]
pub fn list_cluster_scoped_resources() {}

#[utoipa::path(
    post,
    path = "/api/v1/{plural}",
    params(("plural" = String, Path, description = "Cluster-scoped resource plural")),
    request_body = K8sResource,
    responses(
        (status = 201, description = "Resource created", body = K8sResource),
        (status = 409, description = "Resource already exists", body = K8sStatus),
    ),
    tag = "cluster-scoped"
)]
pub fn create_cluster_scoped_resource() {}

#[utoipa::path(
    patch,
    path = "/api/v1/{plural}/{name}",
    params(
        ("plural" = String, Path, description = "Cluster-scoped resource plural"),
        ("name" = String, Path, description = "Resource name"),
    ),
    request_body = K8sPatch,
    responses(
        (status = 200, description = "Resource patched", body = K8sResource),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "cluster-scoped"
)]
pub fn patch_cluster_scoped_resource() {}

#[utoipa::path(
    delete,
    path = "/api/v1/{plural}/{name}",
    params(
        ("plural" = String, Path, description = "Cluster-scoped resource plural"),
        ("name" = String, Path, description = "Resource name"),
    ),
    responses(
        (status = 200, description = "Resource deleted"),
        (status = 404, description = "Resource not found", body = K8sStatus),
    ),
    tag = "cluster-scoped"
)]
pub fn delete_cluster_scoped_resource() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_doc_emits_valid_v3_spec() {
        let doc = ApiDoc::openapi();
        let json = serde_json::to_value(&doc).unwrap();
        // Top-level version + info + paths must be populated.
        assert!(json.get("openapi").unwrap().as_str().unwrap().starts_with("3."));
        let info = json.get("info").unwrap();
        assert_eq!(info.get("title").unwrap(), "engenho-apiserver — K8s REST API");
        let paths = json.get("paths").unwrap().as_object().unwrap();
        // All 10 routes present.
        assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}/{name}"));
        assert!(paths.contains_key("/api/v1/namespaces/{ns}/{plural}"));
        assert!(paths.contains_key("/api/v1/{plural}/{name}"));
        assert!(paths.contains_key("/api/v1/{plural}"));
    }

    #[test]
    fn openapi_doc_includes_all_schema_components() {
        let doc = ApiDoc::openapi();
        let json = serde_json::to_value(&doc).unwrap();
        let schemas = json
            .get("components")
            .unwrap()
            .get("schemas")
            .unwrap()
            .as_object()
            .unwrap();
        for name in ["K8sResource", "K8sResourceList", "K8sStatus", "K8sPatch"] {
            assert!(schemas.contains_key(name), "missing schema: {name}");
        }
    }
}
