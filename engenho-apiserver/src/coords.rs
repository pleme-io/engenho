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
            [plural, name] => (
                (*plural).to_string(),
                Some((*name).to_string()),
                None,
            ),
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
