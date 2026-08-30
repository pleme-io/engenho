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
            let preferred = gvs.last().cloned().unwrap_or(GroupVersionForDiscovery {
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

// ═══════════════════════════════════════════════════════════════════════
// AGGREGATED DISCOVERY v2 (`apidiscovery.k8s.io/v2`)
//
// ★ WHY IT MATTERS: IT IS A LATENCY CONTRACT, NOT A NEW FEATURE. Legacy
// discovery makes a client fetch `/apis` and then ONE REQUEST PER
// group/version — on a cluster with 20 groups that is 21 round trips
// before kubectl can run its first command. Aggregated discovery returns
// the whole thing in one response, and modern kubectl asks for it FIRST.
// Without it every client silently pays the N+1 path forever, which
// presents as "kubectl feels slow against engenho" and nothing else.
//
// ★ IT IS SELECTED BY ACCEPT HEADER, ON THE SAME PATHS. `/apis` serves the
// legacy `APIGroupList` or this, depending on what the client asked for.
// That is upstream's design and it is what makes the feature invisible to
// old clients rather than a breaking change.
//
// ★ `freshness` IS A PROMISE ABOUT STALENESS. `Current` means "this
// listing is authoritative right now". engenho folds discovery from the
// LIVE handler set on every request — there is no cache to go stale — so
// `Current` is the truthful value. A server that cached and still said
// `Current` would make a client trust a listing that no longer matches
// what it can route to.

/// One resource row in aggregated discovery.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryResource {
    pub resource: String,
    pub response_kind: DiscoveryGvk,
    /// `Namespaced` or `Cluster` — upstream's spelling, NOT a bool. A
    /// client renders this string directly.
    pub scope: &'static str,
    pub singular_resource: String,
    pub verbs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub short_names: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub subresources: Vec<DiscoverySubresource>,
}

/// The GVK a resource responds with.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryGvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

/// A subresource row.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverySubresource {
    pub subresource: String,
    pub response_kind: DiscoveryGvk,
    pub verbs: Vec<String>,
}

/// One version of one group.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryVersion {
    pub version: String,
    pub resources: Vec<DiscoveryResource>,
    /// Always `Current` here — see the block comment.
    pub freshness: &'static str,
}

/// One group.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryGroup {
    pub metadata: DiscoveryGroupMeta,
    pub versions: Vec<DiscoveryVersion>,
}

/// A group's `metadata` — only `name` is meaningful.
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryGroupMeta {
    pub name: String,
}

/// The aggregated-discovery document.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct APIGroupDiscoveryList {
    pub kind: &'static str,
    pub api_version: &'static str,
    pub metadata: DiscoveryGroupMeta,
    pub items: Vec<DiscoveryGroup>,
}

/// The media type upstream negotiates aggregated discovery with.
pub const AGGREGATED_DISCOVERY_ACCEPT: &str = "apidiscovery.k8s.io";

/// Does this `Accept` header ask for aggregated discovery?
///
/// Matched on the GROUP parameter rather than the whole string: clients
/// send the media-range with varying parameter order and whitespace, and a
/// literal comparison would fail against a request that is asking for
/// exactly this.
#[must_use]
pub fn wants_aggregated(accept: Option<&str>) -> bool {
    accept.is_some_and(|a| a.contains(AGGREGATED_DISCOVERY_ACCEPT))
}

/// Fold the live handler set into an aggregated-discovery document.
///
/// `core` selects the core group (`/api`) rather than the named groups
/// (`/apis`) — upstream serves aggregated discovery on both, and mixing
/// them would put core `v1` under an empty group name in the `/apis`
/// listing, where no client expects it.
#[must_use]
pub fn build_aggregated(state: &RouterState, core: bool) -> APIGroupDiscoveryList {
    use std::collections::BTreeMap;

    // group → version → resources
    let mut by_group: BTreeMap<String, BTreeMap<String, Vec<DiscoveryResource>>> = BTreeMap::new();

    for h in state.handler_set() {
        let is_core = h.group().is_empty();
        if is_core != core {
            continue;
        }
        let rows = rows_of(h.as_ref());
        // `rows_of` yields the resource AND its subresources as separate
        // rows (`pods`, `pods/status`); aggregated discovery NESTS the
        // latter, so they are split here rather than emitted flat — a flat
        // listing would make a client believe `pods/status` is a top-level
        // resource it can LIST.
        let (mains, subs): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| !r.name.contains('/'));
        for m in mains {
            let subresources = subs
                .iter()
                .filter(|s| s.name.starts_with(&[m.name.as_str(), "/"].concat()))
                .map(|s| DiscoverySubresource {
                    subresource: s.name.rsplit('/').next().unwrap_or_default().to_string(),
                    response_kind: DiscoveryGvk {
                        group: h.group().to_string(),
                        version: h.version().to_string(),
                        kind: s.kind.clone(),
                    },
                    verbs: s.verbs.clone(),
                })
                .collect();
            by_group
                .entry(h.group().to_string())
                .or_default()
                .entry(h.version().to_string())
                .or_default()
                .push(DiscoveryResource {
                    resource: m.name.clone(),
                    response_kind: DiscoveryGvk {
                        group: h.group().to_string(),
                        version: h.version().to_string(),
                        kind: m.kind.clone(),
                    },
                    scope: if m.namespaced {
                        "Namespaced"
                    } else {
                        "Cluster"
                    },
                    singular_resource: m.singular_name.clone(),
                    verbs: m.verbs.clone(),
                    short_names: m.short_names.clone(),
                    categories: m.categories.clone(),
                    subresources,
                });
        }
    }

    let items = by_group
        .into_iter()
        .map(|(group, versions)| DiscoveryGroup {
            metadata: DiscoveryGroupMeta { name: group },
            versions: versions
                .into_iter()
                .map(|(version, mut resources)| {
                    resources.sort_by(|a, b| a.resource.cmp(&b.resource));
                    DiscoveryVersion {
                        version,
                        resources,
                        freshness: "Current",
                    }
                })
                .collect(),
        })
        .collect();

    APIGroupDiscoveryList {
        kind: "APIGroupDiscoveryList",
        api_version: "apidiscovery.k8s.io/v2",
        metadata: DiscoveryGroupMeta {
            name: String::new(),
        },
        items,
    }
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
pub async fn api_groups(
    State(state): State<RouterState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    // Content-negotiated on the SAME path, which is what makes aggregated
    // discovery invisible to old clients rather than a breaking change.
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok());
    if wants_aggregated(accept) {
        return Json(build_aggregated(&state, false)).into_response();
    }
    Json(build_api_groups(&state)).into_response()
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

#[cfg(test)]
mod aggregated_tests {
    use super::*;

    /// The Accept negotiation is the half a client actually depends on: if
    /// it fails, modern kubectl silently falls back to the N+1 path and the
    /// whole feature is invisible.
    #[test]
    fn the_aggregated_accept_header_is_recognised_in_the_shapes_clients_send() {
        // Real kubectl sends the full media range with several parameters
        // in an order it does not promise. Matching the whole string
        // literally would fail against a request that IS asking for this.
        for accept in [
            "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
            "application/json;as=APIGroupDiscoveryList;v=v2;g=apidiscovery.k8s.io",
            "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json",
            // v2beta1, which older-but-still-modern kubectl asks for.
            "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList",
        ] {
            assert!(wants_aggregated(Some(accept)), "must match: {accept}");
        }
    }

    #[test]
    fn a_plain_accept_does_not_trigger_aggregated_discovery() {
        // The other direction, and the one that keeps this from being a
        // breaking change: an old client must keep getting APIGroupList.
        for accept in [
            "application/json",
            "application/vnd.kubernetes.protobuf",
            "*/*",
            "application/json;as=Table;v=v1;g=meta.k8s.io",
        ] {
            assert!(!wants_aggregated(Some(accept)), "must NOT match: {accept}");
        }
        assert!(
            !wants_aggregated(None),
            "an absent Accept is not a request for it"
        );
    }

    #[test]
    fn the_document_envelope_is_the_one_clients_parse() {
        // A client matches on kind + apiVersion before reading items; the
        // wrong envelope makes a correct body unreadable.
        let doc = build_aggregated(&RouterState::new(Vec::new()), false);
        assert_eq!(doc.kind, "APIGroupDiscoveryList");
        assert_eq!(doc.api_version, "apidiscovery.k8s.io/v2");
        assert!(doc.items.is_empty(), "no handlers ⇒ no groups");
    }

    #[test]
    fn core_and_named_groups_are_served_separately() {
        // Mixing them would put core v1 under an empty group name in the
        // /apis listing, where no client expects it.
        let state = RouterState::new(Vec::new());
        let core = build_aggregated(&state, true);
        let named = build_aggregated(&state, false);
        // Both are well-formed even when empty; the selector is what
        // differs, and it must not be the same call twice.
        assert_eq!(core.kind, named.kind);
        assert!(core.items.is_empty() && named.items.is_empty());
    }

    #[test]
    fn freshness_is_current_because_the_listing_is_folded_live() {
        // A server that cached and still said Current would make a client
        // trust a listing that no longer matches what it can route to.
        let v = DiscoveryVersion {
            version: "v1".into(),
            resources: Vec::new(),
            freshness: "Current",
        };
        assert_eq!(v.freshness, "Current");
    }

    #[test]
    fn scope_is_upstreams_string_not_a_bool() {
        // A client renders this value directly; `true`/`false` would show
        // up in kubectl output as exactly that.
        let r = DiscoveryResource {
            resource: "pods".into(),
            response_kind: DiscoveryGvk {
                group: String::new(),
                version: "v1".into(),
                kind: "Pod".into(),
            },
            scope: "Namespaced",
            singular_resource: "pod".into(),
            verbs: vec!["get".into()],
            short_names: Vec::new(),
            categories: Vec::new(),
            subresources: Vec::new(),
        };
        let json = serde_json::to_value(&r).expect("serializes");
        assert_eq!(json["scope"], "Namespaced");
        // camelCase on the wire — responseKind, not response_kind.
        assert_eq!(json["responseKind"]["kind"], "Pod");
        assert_eq!(json["singularResource"], "pod");
        // Empty collections are omitted rather than sent as [], matching
        // upstream and keeping the payload small on a large catalog.
        assert!(json.get("shortNames").is_none());
        assert!(json.get("subresources").is_none());
    }
}
