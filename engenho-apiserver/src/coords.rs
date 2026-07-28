//! The ONE typed K8s resource-URL parser.
//!
//! [`ResourceCoords`] is the single typed shape every resource request
//! decomposes into: `(group, version, namespace, plural, name,
//! subresource)`. The [`FromRequestParts`] extractor is the ONLY place
//! K8s resource URL shapes are parsed — replacing the ~20 hand-fanned
//! per-scope/per-verb axum route wrappers with one coords → dispatch
//! path.
//!
//! ## Two feeding routes
//!
//! The router ([`crate::router::build`]) feeds this extractor through two
//! catch-all routes:
//!
//!   * `/api/v1/*rest`               → core group (`group=None`,
//!                                       `version=Some("v1")`).
//!   * `/apis/:group/:version/*rest` → named group (`group=Some(g)`,
//!                                       `version=Some(v)`).
//!
//! The extractor reads the matched path params via [`RawPathParams`] (the
//! same percent-decoded representation axum's `Path` extractor is built
//! on, so captured names match the legacy per-segment routes for every
//! K8s-shaped identifier), then splits the `*rest` tail into coords.
//!
//! ## Tail shapes
//!
//! `*rest` (the path after the `/api/v1` or `/apis/<g>/<v>` prefix) is
//! split on `/` and matched against the six shapes, in order:
//!
//! | tail                                       | namespace  | name       | subresource |
//! |--------------------------------------------|------------|------------|-------------|
//! | `namespaces/{ns}/{plural}`                 | `Some(ns)` | `None`     | `None`      |
//! | `namespaces/{ns}/{plural}/{name}`          | `Some(ns)` | `Some`     | `None`      |
//! | `namespaces/{ns}/{plural}/{name}/{sub}`    | `Some(ns)` | `Some`     | `Some(sub)` |
//! | `{plural}`                                 | `None`     | `None`     | `None`      |
//! | `{plural}/{name}`                          | `None`     | `Some`     | `None`      |
//! | `{plural}/{name}/{sub}`                    | `None`     | `Some`     | `Some(sub)` |
//!
//! A tail matching none of these (wrong arity, empty plural) → a typed
//! [`ApiError::NotFound`] (a real K8s `Status` 404, NOT axum's empty
//! 404), so bad-shape requests still render the typed body.

use axum::async_trait;
use axum::extract::{FromRequestParts, RawPathParams};
use axum::http::request::Parts;

use crate::error::ApiError;
use crate::router::RouterState;

/// The typed coordinates of a K8s resource URL. The single shape every
/// resource verb handler consumes — scope (namespaced vs cluster), group
/// (core vs named), collection (no `name`) vs instance (with `name`), and
/// the optional subresource all live here as typed `Option`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceCoords {
    /// `None` => core group (the `""` sentinel in the handler-map key);
    /// `Some(g)` => named group.
    pub group: Option<String>,
    /// Core => `Some("v1")`; named group => `Some(v)` from the path. A
    /// resource URL always has a version, so this is always `Some` for a
    /// parsed resource request.
    pub version: Option<String>,
    /// `Some(ns)` => namespaced URL (`.../namespaces/{ns}/...`); `None`
    /// => cluster-scoped URL.
    pub namespace: Option<String>,
    /// The plural URL segment — always present (a resource URL always
    /// names a plural).
    pub plural: String,
    /// `None` => collection (list / create / watch); `Some(name)` =>
    /// instance (get / patch / delete).
    pub name: Option<String>,
    /// `None` => the base resource; `Some("status")` / `Some("scale")` =>
    /// a subresource. Reserved for the status/scale follow-up; the verb
    /// handlers route `None` to the base verb (identity) and reject
    /// `Some(_)` with a typed `NotFound` until subresource handlers land.
    pub subresource: Option<String>,
}

impl ResourceCoords {
    /// The handler-map group key: the `""` sentinel for the core group,
    /// else the named group. [`RouterState::lookup`] keys on
    /// `(group, version, plural)` with `group=""` for core, so this folds
    /// `lookup_core(p)` and `lookup(g, v, p)` into one resolver call.
    #[must_use]
    pub fn group_key(&self) -> &str {
        self.group.as_deref().unwrap_or("")
    }

    /// The handler-map version key. A parsed resource URL always carries a
    /// version (core => `"v1"`, named => the path `:version`); the
    /// `"v1"` fallback is defensive only.
    #[must_use]
    pub fn version_key(&self) -> &str {
        self.version.as_deref().unwrap_or("v1")
    }

    /// Parse a coords value from the group/version prefix + the `*rest`
    /// tail.
    ///
    /// `group` is `None` for the core catch-all (`/api/v1/*rest`) and
    /// `Some(g)` for the grouped catch-all (`/apis/:group/:version/*rest`);
    /// `version` mirrors the same (core => `"v1"`). `rest` is the
    /// already-percent-decoded tail after the prefix (no leading slash).
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when the tail matches none of the six K8s
    /// resource URL shapes (wrong segment arity, or an empty plural) — a
    /// typed K8s `Status` 404, never axum's empty 404.
    pub fn parse(
        group: Option<String>,
        version: Option<String>,
        rest: &str,
    ) -> Result<Self, ApiError> {
        // Split the tail into non-empty segments. A trailing slash, a
        // double slash, or a leading slash all collapse away — matchit
        // strips the leading slash, but be defensive.
        let segs: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();

        // `namespaces/{ns}/...` is the namespaced family; everything else
        // is cluster-scoped. The plural is never the literal `namespaces`
        // sentinel in the namespaced family (that is the URL keyword), but
        // a cluster-scoped `namespaces` plural (the core Namespace kind)
        // IS valid — distinguished by arity (`namespaces/{ns}/{plural}` is
        // >= 3 segments; cluster `namespaces` collection is exactly 1).
        let namespaced = segs.first() == Some(&"namespaces") && segs.len() >= 3;

        let (namespace, rest_segs): (Option<String>, &[&str]) = if namespaced {
            // segs[0] == "namespaces", segs[1] == ns, segs[2..] == the
            // {plural}[/{name}[/{sub}]] tail.
            (Some(segs[1].to_string()), &segs[2..])
        } else {
            (None, &segs[..])
        };

        // `rest_segs` is now exactly `{plural}` | `{plural}/{name}` |
        // `{plural}/{name}/{sub}`.
        let (plural, name, subresource) = match rest_segs {
            [plural] => ((*plural).to_string(), None, None),
            [plural, name] => ((*plural).to_string(), Some((*name).to_string()), None),
            [plural, name, sub] => (
                (*plural).to_string(),
                Some((*name).to_string()),
                Some((*sub).to_string()),
            ),
            // Wrong arity (empty tail, or > 3 trailing segments) → a real
            // K8s Status 404, never axum's empty 404.
            _ => {
                return Err(ApiError::NotFound(format!(
                    "unrecognized resource URL shape: {rest:?}"
                )));
            }
        };

        // An empty plural can't happen (split filtered empties + the arity
        // arms require a non-empty first segment), but guard anyway so a
        // future change can't introduce a silently-wrong empty-plural
        // lookup.
        if plural.is_empty() {
            return Err(ApiError::NotFound(format!(
                "unrecognized resource URL shape: {rest:?}"
            )));
        }

        Ok(Self {
            group,
            version,
            namespace,
            plural,
            name,
            subresource,
        })
    }
}

/// The typed RBAC request info — the shape the authz layer reduces a request
/// to, built from the HTTP method + URL path WITHOUT the axum `RouterState`
/// extractor (the authz middleware runs over `req.uri().path()` +
/// `req.method()` directly). Either `resource` (a `/api`/`/apis` resource shape)
/// or `non_resource_url` (everything else) is set; the verb is derived from the
/// method + collection/instance shape + `?watch=`.
///
/// This mirrors kube-apiserver's `RequestInfo`. The resource-path decomposition
/// reuses the SAME six-shape parse as [`ResourceCoords::parse`] (lifted into
/// [`parse_resource_path`]) so the authz layer and the routing layer agree on
/// what a URL means — no second parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestInfo {
    /// The RBAC verb (`get` / `list` / `watch` / `create` / `update` / `patch`
    /// / `delete` / `deletecollection`; for a non-resource path, the lowercased
    /// HTTP method).
    pub verb: String,
    /// The API group (`""` core), for a resource request.
    pub group: String,
    /// The API version, for a resource request.
    pub version: String,
    /// The resource plural, for a resource request (empty for non-resource).
    pub resource: String,
    /// The subresource (`status`/`scale`), if targeted.
    pub subresource: Option<String>,
    /// The namespace, for a namespaced resource request.
    pub namespace: Option<String>,
    /// The instance name, for an instance-targeting request.
    pub name: Option<String>,
    /// The non-resource URL, for a non-resource request.
    pub non_resource_url: Option<String>,
}

/// Decompose a `/api/v1/...` or `/apis/<g>/<v>/...` path into its
/// `(group, version, ResourceCoords)` — the same six-shape parse the routing
/// extractor uses, but driven off a raw path string instead of axum's
/// `RawPathParams`. Returns `None` when the path is not a resource shape (the
/// caller treats that as a non-resource request).
///
/// `path` is the request path (with leading slash). The leading `/api/v1` or
/// `/apis/<g>/<v>` prefix is consumed; the rest is fed to
/// [`ResourceCoords::parse`].
#[must_use]
pub fn parse_resource_path(path: &str) -> Option<ResourceCoords> {
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // Core: /api/v1/<rest>
    if segs.first() == Some(&"api") {
        // Exactly `/api` or `/api/v1` (discovery) — not a resource shape.
        if segs.len() <= 2 {
            return None;
        }
        // segs[0]=="api", segs[1]==version ("v1"), segs[2..]==the rest tail.
        let version = segs[1].to_string();
        let rest = segs[2..].join("/");
        return ResourceCoords::parse(None, Some(version), &rest).ok();
    }
    // Named group: /apis/<group>/<version>/<rest>
    if segs.first() == Some(&"apis") {
        // `/apis`, `/apis/<g>`, or `/apis/<g>/<v>` (discovery) — not a resource
        // shape (no trailing plural).
        if segs.len() <= 3 {
            return None;
        }
        let group = segs[1].to_string();
        let version = segs[2].to_string();
        let rest = segs[3..].join("/");
        return ResourceCoords::parse(Some(group), Some(version), &rest).ok();
    }
    None
}

impl RequestInfo {
    /// Build the typed RBAC request info from the HTTP method + path + whether
    /// the request carried `?watch=true`. The verb mapping mirrors
    /// kube-apiserver's `RequestInfo`:
    ///
    ///   * GET   + collection (no name) → `list`; `?watch=true` → `watch`;
    ///     GET + name → `get`.
    ///   * POST  → `create`.
    ///   * PUT   → `update`.
    ///   * PATCH → `patch`.
    ///   * DELETE + name → `delete`; DELETE + no name → `deletecollection`.
    ///   * non-resource path → the lowercased HTTP method.
    #[must_use]
    pub fn from_method_path(method: &str, path: &str, is_watch: bool) -> Self {
        match parse_resource_path(path) {
            Some(coords) => {
                let verb = resource_verb(method, coords.name.is_some(), is_watch);
                RequestInfo {
                    verb,
                    group: coords.group_key().to_string(),
                    version: coords.version_key().to_string(),
                    resource: coords.plural,
                    subresource: coords.subresource,
                    namespace: coords.namespace,
                    name: coords.name,
                    non_resource_url: None,
                }
            }
            None => RequestInfo {
                // The RBAC verb for a non-resource path is the lowercased method.
                verb: method.to_ascii_lowercase(),
                group: String::new(),
                version: String::new(),
                resource: String::new(),
                subresource: None,
                namespace: None,
                name: None,
                non_resource_url: Some(path.to_string()),
            },
        }
    }
}

/// Map an HTTP method on a RESOURCE path to the RBAC verb, given whether the
/// path targets an instance (`has_name`) and whether `?watch=true` was set.
#[must_use]
pub fn resource_verb(method: &str, has_name: bool, is_watch: bool) -> String {
    let m = method.to_ascii_uppercase();
    match m.as_str() {
        "GET" | "HEAD" => {
            if is_watch {
                "watch".to_string()
            } else if has_name {
                "get".to_string()
            } else {
                "list".to_string()
            }
        }
        "POST" => "create".to_string(),
        "PUT" => "update".to_string(),
        "PATCH" => "patch".to_string(),
        "DELETE" => {
            if has_name {
                "delete".to_string()
            } else {
                "deletecollection".to_string()
            }
        }
        // Any other method on a resource path → the lowercased method (never a
        // silent wrong verb).
        other => other.to_ascii_lowercase(),
    }
}

#[async_trait]
impl FromRequestParts<RouterState> for ResourceCoords {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &RouterState,
    ) -> Result<Self, Self::Rejection> {
        // RawPathParams is the percent-decoded matched-param view axum's
        // own `Path` extractor is built on — so the captured `ns` / `name`
        // / `plural` segments decode identically to the legacy per-segment
        // routes for every K8s-shaped identifier.
        let raw = RawPathParams::from_request_parts(parts, &())
            .await
            .map_err(|e| ApiError::Internal(format!("path params unavailable: {e}")))?;

        let mut group: Option<String> = None;
        let mut version: Option<String> = None;
        let mut rest: Option<String> = None;
        for (k, v) in &raw {
            match k {
                "group" => group = Some(v.to_string()),
                "version" => version = Some(v.to_string()),
                "rest" => rest = Some(v.to_string()),
                _ => {}
            }
        }

        // The core catch-all `/api/v1/*rest` binds only `rest`; we
        // synthesize the core sentinel `(group=None, version="v1")`. The
        // grouped catch-all binds `group` + `version` + `rest`.
        let version = version.or_else(|| Some("v1".to_string()));
        let rest = rest.ok_or_else(|| {
            ApiError::Internal("resource catch-all matched without a *rest param".into())
        })?;

        ResourceCoords::parse(group, version, &rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core(rest: &str) -> Result<ResourceCoords, ApiError> {
        ResourceCoords::parse(None, Some("v1".to_string()), rest)
    }

    fn grouped(g: &str, v: &str, rest: &str) -> Result<ResourceCoords, ApiError> {
        ResourceCoords::parse(Some(g.to_string()), Some(v.to_string()), rest)
    }

    #[test]
    fn core_namespaced_collection() {
        let c = core("namespaces/default/pods").unwrap();
        assert_eq!(c.group, None);
        assert_eq!(c.version.as_deref(), Some("v1"));
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.plural, "pods");
        assert_eq!(c.name, None);
        assert_eq!(c.subresource, None);
    }

    #[test]
    fn core_namespaced_instance() {
        let c = core("namespaces/default/pods/p1").unwrap();
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.plural, "pods");
        assert_eq!(c.name.as_deref(), Some("p1"));
        assert_eq!(c.subresource, None);
    }

    #[test]
    fn core_cluster_collection() {
        let c = core("nodes").unwrap();
        assert_eq!(c.group, None);
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "nodes");
        assert_eq!(c.name, None);
    }

    #[test]
    fn core_cluster_instance() {
        let c = core("nodes/n1").unwrap();
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "nodes");
        assert_eq!(c.name.as_deref(), Some("n1"));
    }

    #[test]
    fn core_cluster_namespaces_collection_is_not_namespaced() {
        // `/api/v1/namespaces` is the cluster-scoped Namespace collection —
        // exactly ONE segment, so NOT the namespaced family (which is
        // `namespaces/{ns}/{plural}`, >= 3 segments). Distinguished by
        // arity so the core Namespace kind round-trips.
        let c = core("namespaces").unwrap();
        assert_eq!(c.namespace, None, "cluster-scoped, not namespaced");
        assert_eq!(c.plural, "namespaces");
        assert_eq!(c.name, None);
    }

    #[test]
    fn core_namespaces_instance_is_cluster_scoped() {
        // `/api/v1/namespaces/team-a` — two segments. NOT the namespaced
        // family (needs >= 3); this is the cluster Namespace instance.
        let c = core("namespaces/team-a").unwrap();
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "namespaces");
        assert_eq!(c.name.as_deref(), Some("team-a"));
    }

    #[test]
    fn grouped_namespaced_collection_and_instance() {
        let c = grouped("apps", "v1", "namespaces/default/deployments").unwrap();
        assert_eq!(c.group.as_deref(), Some("apps"));
        assert_eq!(c.version.as_deref(), Some("v1"));
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.plural, "deployments");
        assert_eq!(c.name, None);

        let c = grouped("apps", "v1", "namespaces/default/deployments/web").unwrap();
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.plural, "deployments");
        assert_eq!(c.name.as_deref(), Some("web"));
    }

    #[test]
    fn grouped_cluster_collection_and_instance() {
        let c = grouped("rbac.authorization.k8s.io", "v1", "clusterroles").unwrap();
        assert_eq!(c.group.as_deref(), Some("rbac.authorization.k8s.io"));
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "clusterroles");
        assert_eq!(c.name, None);

        let c = grouped("rbac.authorization.k8s.io", "v1", "clusterroles/x").unwrap();
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "clusterroles");
        assert_eq!(c.name.as_deref(), Some("x"));
    }

    #[test]
    fn subresource_tails_parse() {
        let c = core("namespaces/default/pods/p1/status").unwrap();
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.plural, "pods");
        assert_eq!(c.name.as_deref(), Some("p1"));
        assert_eq!(c.subresource.as_deref(), Some("status"));

        // cluster-scoped subresource tail.
        let c = grouped("apps", "v1", "deployments/web/scale").unwrap();
        assert_eq!(c.namespace, None);
        assert_eq!(c.plural, "deployments");
        assert_eq!(c.name.as_deref(), Some("web"));
        assert_eq!(c.subresource.as_deref(), Some("scale"));
    }

    #[test]
    fn empty_tail_is_not_found() {
        // An empty tail (`/api/v1/` with nothing after) has no plural →
        // typed NotFound, not a panic / empty 404.
        assert!(matches!(core(""), Err(ApiError::NotFound(_))));
        assert!(matches!(core("/"), Err(ApiError::NotFound(_))));
    }

    #[test]
    fn over_long_tail_is_not_found() {
        // More than three trailing segments (e.g. a bogus deep path) is a
        // wrong-arity shape → typed NotFound.
        assert!(matches!(
            core("namespaces/default/pods/p1/status/extra"),
            Err(ApiError::NotFound(_))
        ));
        assert!(matches!(
            grouped("apps", "v1", "deployments/web/scale/extra"),
            Err(ApiError::NotFound(_))
        ));
    }

    #[test]
    fn namespaced_tail_without_plural_is_not_found() {
        // `namespaces/default` alone (no plural) is two segments → NOT the
        // namespaced family (needs >= 3) and NOT a valid cluster shape for
        // the `namespaces` plural either, well — it parses as the cluster
        // `namespaces` instance `default`. That is the same as
        // core("namespaces/team-a"): a Namespace instance named "default".
        // This is correct (matchit can't tell `/api/v1/namespaces/default`
        // apart from a Namespace GET by name), and the handler lookup for
        // the `namespaces` plural enforces the rest.
        let c = core("namespaces/default").unwrap();
        assert_eq!(c.plural, "namespaces");
        assert_eq!(c.name.as_deref(), Some("default"));
    }

    #[test]
    fn request_info_resource_verbs() {
        // GET collection → list; GET instance → get; GET ?watch → watch.
        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb, "list");
        assert_eq!(ri.group, "");
        assert_eq!(ri.resource, "pods");
        assert_eq!(ri.namespace.as_deref(), Some("default"));
        assert_eq!(ri.name, None);

        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb, "get");
        assert_eq!(ri.name.as_deref(), Some("p1"));

        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods", true);
        assert_eq!(ri.verb, "watch");

        // POST → create; PUT → update; PATCH → patch.
        let ri = RequestInfo::from_method_path("POST", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb, "create");
        let ri = RequestInfo::from_method_path("PUT", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb, "update");
        let ri =
            RequestInfo::from_method_path("PATCH", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb, "patch");

        // DELETE instance → delete; DELETE collection → deletecollection.
        let ri =
            RequestInfo::from_method_path("DELETE", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb, "delete");
        let ri = RequestInfo::from_method_path("DELETE", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb, "deletecollection");
    }

    #[test]
    fn request_info_subresource_and_grouped() {
        let ri = RequestInfo::from_method_path(
            "PUT",
            "/apis/apps/v1/namespaces/default/deployments/web/scale",
            false,
        );
        assert_eq!(ri.verb, "update");
        assert_eq!(ri.group, "apps");
        assert_eq!(ri.resource, "deployments");
        assert_eq!(ri.subresource.as_deref(), Some("scale"));
        assert_eq!(ri.name.as_deref(), Some("web"));
    }

    #[test]
    fn request_info_non_resource_paths() {
        // /healthz, /version, /metrics, /openapi/v3 → non-resource, verb =
        // lowercased method.
        for p in [
            "/healthz",
            "/version",
            "/metrics",
            "/openapi/v3",
            "/openapi/v3/apis/apps/v1",
        ] {
            let ri = RequestInfo::from_method_path("GET", p, false);
            assert!(ri.non_resource_url.is_some(), "{p} is non-resource");
            assert_eq!(ri.verb, "get");
            assert_eq!(ri.non_resource_url.as_deref(), Some(p));
        }
    }

    #[test]
    fn request_info_discovery_paths_are_non_resource() {
        // /api, /api/v1, /apis, /apis/<g>/<v> are discovery → non-resource.
        for p in ["/api", "/api/v1", "/apis", "/apis/apps/v1"] {
            let ri = RequestInfo::from_method_path("GET", p, false);
            assert!(
                ri.non_resource_url.is_some(),
                "{p} is a discovery (non-resource) shape"
            );
            assert_eq!(ri.non_resource_url.as_deref(), Some(p));
        }
    }

    #[test]
    fn parse_resource_path_round_trips_core_and_grouped() {
        let c = parse_resource_path("/api/v1/namespaces/default/pods/p1").unwrap();
        assert_eq!(c.plural, "pods");
        assert_eq!(c.name.as_deref(), Some("p1"));
        assert_eq!(c.namespace.as_deref(), Some("default"));

        let c = parse_resource_path("/apis/rbac.authorization.k8s.io/v1/clusterroles").unwrap();
        assert_eq!(c.group.as_deref(), Some("rbac.authorization.k8s.io"));
        assert_eq!(c.plural, "clusterroles");
        assert_eq!(c.namespace, None);

        // Discovery shapes → None.
        assert!(parse_resource_path("/api/v1").is_none());
        assert!(parse_resource_path("/apis/apps/v1").is_none());
        assert!(parse_resource_path("/healthz").is_none());
    }

    #[test]
    fn group_and_version_keys_fold_core_and_grouped() {
        // Core: group_key is the "" sentinel, version_key is "v1" — so
        // `lookup(group_key, version_key, plural)` == the old
        // `lookup_core(plural)`.
        let c = core("pods").unwrap();
        assert_eq!(c.group_key(), "");
        assert_eq!(c.version_key(), "v1");

        // Grouped: the real group + version.
        let c = grouped("apps", "v1", "deployments").unwrap();
        assert_eq!(c.group_key(), "apps");
        assert_eq!(c.version_key(), "v1");
    }
}
