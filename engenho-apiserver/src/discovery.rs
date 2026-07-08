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
use engenho_types::generated_v1_34::Subresource;
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

/// The base-row verb list for a handler: the always-served [`VERBS`] plus
/// `deletecollection` when the kind serves it
/// ([`ResourceHandler::supports_delete_collection`]). Keeps discovery's
/// advertised verb set == the router's routable verb set (a
/// deletecollection-serving kind advertises it; `namespaces` / `bindings` /
/// `componentstatuses` do not).
fn base_verbs(h: &dyn ResourceHandler) -> Vec<String> {
    let mut verbs: Vec<String> = VERBS.iter().map(|v| (*v).to_string()).collect();
    if h.supports_delete_collection() {
        verbs.push("deletecollection".to_string());
    }
    verbs
}

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
    /// kubectl short-name aliases (e.g. `["deploy"]`). Omitted when empty
    /// (matches kube-apiserver, which only emits `shortNames` for kinds
    /// that have one). This is what lets `kubectl get deploy` resolve.
    #[serde(rename = "shortNames", skip_serializing_if = "Vec::is_empty")]
    pub short_names: Vec<String>,
    /// Resource categories (e.g. `["all"]`). Omitted when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    /// The API group a subresource row belongs to when it DIFFERS from the
    /// parent's group — `Some("autoscaling")` for a `<plural>/scale` row
    /// (the Scale projection is autoscaling/v1 regardless of the parent's
    /// group). Omitted (None) for the base row + the `/status` row (which
    /// inherit the parent's group/version). This is the K8s `group` field
    /// on `APIResource`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// The API version a subresource row belongs to when it differs from the
    /// parent's — `Some("v1")` for a `<plural>/scale` row. Omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

// ── builders (fold the handler set; deterministic via BTree ordering) ───

/// The verbs a subresource (`/status`, `/scale`) serves — get + the two
/// write verbs (patch + update). NO list/watch/create/delete (a subresource
/// is always an instance projection, never a collection).
const SUBRESOURCE_VERBS: &[&str] = &["get", "patch", "update"];

/// Project one handler into an `APIResource` discovery row. The
/// registration metadata (singularName / shortNames / categories) flows
/// from the handler — which sources it from the generated catalog — so
/// `/api/v1` and `/apis/<g>/<v>` advertise it identically with no
/// per-route wiring. kubectl reads `shortNames` here to resolve
/// `deploy` → `deployments`, etc.
fn resource_of(h: &dyn ResourceHandler) -> APIResource {
    APIResource {
        name: h.plural().to_string(),
        singular_name: h.singular_name().to_string(),
        namespaced: h.namespaced(),
        kind: h.kind().to_string(),
        verbs: base_verbs(h),
        short_names: h.short_names().iter().map(|s| (*s).to_string()).collect(),
        categories: h.categories().iter().map(|c| (*c).to_string()).collect(),
        // The base row inherits its own group/version (the list's
        // groupVersion); these K8s fields are only set when a subresource
        // diverges (the scale row).
        group: None,
        version: None,
    }
}

/// Fold one handler into its discovery rows: the base resource row PLUS one
/// row per catalog-declared subresource (so advertised == routable — the
/// same handler set the router dispatches on drives discovery). The order
/// is base-then-subresources; the caller sorts by `name` so
/// `deployments` < `deployments/scale` < `deployments/status` is stable.
fn rows_of(h: &dyn ResourceHandler) -> Vec<APIResource> {
    let mut rows = vec![resource_of(h)];
    for sub in h.subresources() {
        rows.push(subresource_row(h, *sub));
    }
    rows
}

/// Build the `APIResource` discovery row for one subresource of `h`.
///
///   * **status** — `name: "<plural>/status"`, `singularName: ""`, parent's
///     `kind` + scope, verbs `get/patch/update`, group/version OMITTED
///     (inherits the parent's GV — same list).
///   * **scale**  — `name: "<plural>/scale"`, `kind: "Scale"`,
///     `group: "autoscaling"`, `version: "v1"`, parent's scope, verbs
///     `get/patch/update`.
fn subresource_row(h: &dyn ResourceHandler, sub: Subresource) -> APIResource {
    let verbs = SUBRESOURCE_VERBS.iter().map(|v| (*v).to_string()).collect();
    match sub {
        Subresource::Status => APIResource {
            name: format!("{}/status", h.plural()),
            singular_name: String::new(),
            namespaced: h.namespaced(),
            kind: h.kind().to_string(),
            verbs,
            short_names: Vec::new(),
            categories: Vec::new(),
            group: None,
            version: None,
        },
        Subresource::Scale => APIResource {
            name: format!("{}/scale", h.plural()),
            singular_name: String::new(),
            namespaced: h.namespaced(),
            kind: "Scale".to_string(),
            verbs,
            short_names: Vec::new(),
            categories: Vec::new(),
            group: Some("autoscaling".to_string()),
            version: Some("v1".to_string()),
        },
        // `/log` is READ-ONLY — verbs `get` only (no patch/update). Kind is the
        // parent's (`Pod`); group/version inherit the parent's GV.
        Subresource::Log => APIResource {
            name: format!("{}/log", h.plural()),
            singular_name: String::new(),
            namespaced: h.namespaced(),
            kind: h.kind().to_string(),
            verbs: vec!["get".to_string()],
            short_names: Vec::new(),
            categories: Vec::new(),
            group: None,
            version: None,
        },
    }
}

/// Build the `/api/v1` `APIResourceList` from the core handlers (group ==
/// "", version == "v1"), sorted by plural for determinism.
pub(crate) fn build_core_resources(state: &RouterState) -> APIResourceList {
    let mut rows: Vec<APIResource> = state
        .handler_set()
        .into_iter()
        .filter(|h| h.group().is_empty() && h.version() == "v1")
        .flat_map(|h| rows_of(h.as_ref()))
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
pub(crate) fn build_api_groups(state: &RouterState) -> APIGroupList {
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
pub(crate) fn build_group_resources(
    state: &RouterState,
    group: &str,
    version: &str,
) -> Option<APIResourceList> {
    let mut rows: Vec<APIResource> = state
        .handler_set()
        .into_iter()
        .filter(|h| h.group() == group && h.version() == version)
        .flat_map(|h| rows_of(h.as_ref()))
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
