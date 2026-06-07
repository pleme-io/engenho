//! K8s API discovery — `/api`, `/api/v1`, `/apis`, `/apis/<g>/<v>`.
//!
//! Discovery is the machine-readable index kubectl + controllers fetch to
//! learn what kinds the server serves, at what group/version/plural, and
//! whether they are namespaced. Every response is a typed serde struct
//! mirroring `meta/v1` (camelCase via `#[serde(rename)]`) — NEVER
//! `serde_json::json!()` of an ad-hoc map (per the ★★ TYPED EMISSION rule).
//!
//! ## Single source of truth
//!
//! The discovery documents are folded from the SAME registered handler
//! set the router dispatches on — so what is advertised is exactly what
//! is routable. `name` is always the handler's curated `plural`
//! (`Endpoints` → `endpoints`, never `endpointss`); `kind` is the
//! handler's kind; `namespaced` is the handler's scope.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::error::ApiError;
use crate::handler::ResourceHandler;
use crate::router::RouterState;

/// The verbs every cataloged kind supports at M0.1. CRUD + list/watch are
/// all wired through `StoreBackedHandler`; `update` is the PUT alias of the
/// PATCH path (kubectl/client-go advertise both).
const VERBS: &[&str] = &[
    "get", "list", "watch", "create", "update", "patch", "delete",
];

// ── typed meta/v1 discovery structs ────────────────────────────────────

/// `GET /api` — `APIVersions` listing the core legacy versions.
#[derive(Serialize)]
pub struct APIVersions {
    pub kind: &'static str,
    pub versions: Vec<String>,
    #[serde(rename = "serverAddressByClientCIDRs")]
    pub server_address_by_client_cidrs: Vec<ServerAddressByClientCIDR>,
}

/// One `(clientCIDR, serverAddress)` pair. Empty at M0.1 (single-endpoint
/// in-process server) but the field is present for wire-shape fidelity.
#[derive(Serialize)]
pub struct ServerAddressByClientCIDR {
    #[serde(rename = "clientCIDR")]
    pub client_cidr: String,
    #[serde(rename = "serverAddress")]
    pub server_address: String,
}

/// `GET /apis` — `APIGroupList` over every non-core group.
#[derive(Serialize)]
pub struct APIGroupList {
    pub kind: &'static str,
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
    pub groups: Vec<APIGroup>,
}

/// One group in the `APIGroupList`.
#[derive(Serialize)]
pub struct APIGroup {
    pub name: String,
    pub versions: Vec<GroupVersionForDiscovery>,
    #[serde(rename = "preferredVersion")]
    pub preferred_version: GroupVersionForDiscovery,
}

/// `{groupVersion:"<g>/<v>", version:"<v>"}`.
#[derive(Serialize, Clone)]
pub struct GroupVersionForDiscovery {
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub version: String,
}

/// `GET /api/v1` or `GET /apis/<g>/<v>` — `APIResourceList`.
#[derive(Serialize)]
pub struct APIResourceList {
    pub kind: &'static str,
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub resources: Vec<APIResource>,
}

/// One resource row in an `APIResourceList`.
#[derive(Serialize)]
pub struct APIResource {
    /// The plural URL segment (curated; e.g. `endpoints`).
    pub name: String,
    #[serde(rename = "singularName")]
    pub singular_name: String,
    pub namespaced: bool,
    pub kind: String,
    pub verbs: Vec<String>,
}

// ── builders (fold the handler set; deterministic via BTree ordering) ───

/// Project one handler into an `APIResource` discovery row.
fn resource_of(h: &dyn ResourceHandler) -> APIResource {
    APIResource {
        name: h.plural().to_string(),
        singular_name: String::new(),
        namespaced: h.namespaced(),
        kind: h.kind().to_string(),
        verbs: VERBS.iter().map(|v| (*v).to_string()).collect(),
    }
}

/// Build the `/api/v1` `APIResourceList` from the core handlers (group ==
/// "", version == "v1"), sorted by plural for determinism.
fn build_core_resources(state: &RouterState) -> APIResourceList {
    let mut rows: Vec<APIResource> = state
        .handler_set()
        .into_iter()
        .filter(|h| h.group().is_empty() && h.version() == "v1")
        .map(|h| resource_of(h.as_ref()))
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    APIResourceList {
        kind: "APIResourceList",
        api_version: "v1",
        group_version: "v1".to_string(),
        resources: rows,
    }
}

/// Build the `/apis` `APIGroupList`: one `APIGroup` per distinct non-core
/// group, each advertising the single registered `(group, version)` pair.
fn build_api_groups(state: &RouterState) -> APIGroupList {
    // group → set of versions present (BTree for deterministic ordering).
    let mut by_group: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();
    for h in state.handler_set() {
        if h.group().is_empty() {
            continue; // core group is advertised under /api, not /apis.
        }
        by_group
            .entry(h.group().to_string())
            .or_default()
            .insert(h.version().to_string());
    }

    let groups: Vec<APIGroup> = by_group
        .into_iter()
        .map(|(name, versions)| {
            let gvs: Vec<GroupVersionForDiscovery> = versions
                .into_iter()
                .map(|v| GroupVersionForDiscovery {
                    group_version: format!("{name}/{v}"),
                    version: v,
                })
                .collect();
            // preferredVersion = the highest-sorted version present (one
            // version per group at M0.1, so this is unambiguous).
            let preferred = gvs
                .last()
                .cloned()
                .unwrap_or(GroupVersionForDiscovery {
                    group_version: name.clone(),
                    version: String::new(),
                });
            APIGroup {
                name,
                versions: gvs,
                preferred_version: preferred,
            }
        })
        .collect();

    APIGroupList {
        kind: "APIGroupList",
        api_version: "v1",
        groups,
    }
}

/// Build the `/apis/<group>/<version>` `APIResourceList`, or `None` if no
/// handler is registered for that `(group, version)` pair.
fn build_group_resources(
    state: &RouterState,
    group: &str,
    version: &str,
) -> Option<APIResourceList> {
    let mut rows: Vec<APIResource> = state
        .handler_set()
        .into_iter()
        .filter(|h| h.group() == group && h.version() == version)
        .map(|h| resource_of(h.as_ref()))
        .collect();
    if rows.is_empty() {
        return None;
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Some(APIResourceList {
        kind: "APIResourceList",
        api_version: "v1",
        group_version: format!("{group}/{version}"),
        resources: rows,
    })
}

// ── axum route handlers ────────────────────────────────────────────────

/// `GET /api` → the core `APIVersions` (we serve exactly `v1`).
pub async fn api_versions() -> impl IntoResponse {
    Json(APIVersions {
        kind: "APIVersions",
        versions: vec!["v1".to_string()],
        server_address_by_client_cidrs: Vec::new(),
    })
}

/// `GET /api/v1` → the core `APIResourceList`.
pub async fn core_resources(State(state): State<RouterState>) -> impl IntoResponse {
    Json(build_core_resources(&state))
}

/// `GET /apis` → the `APIGroupList` over every non-core group.
pub async fn api_groups(State(state): State<RouterState>) -> impl IntoResponse {
    Json(build_api_groups(&state))
}

/// `GET /apis/<group>/<version>` → that group/version's `APIResourceList`,
/// or 404 if the pair is not registered.
pub async fn group_resources(
    State(state): State<RouterState>,
    Path((group, version)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    match build_group_resources(&state, &group, &version) {
        Some(list) => Ok(Json(list).into_response()),
        None => Err(ApiError::NotFound(format!(
            "no resources for group/version {group}/{version}"
        ))),
    }
}
