//! The ONE typed K8s request classification.
//!
//! [`RequestInfo`] is computed ONCE per request, by the request-info layer that
//! [`crate::router::build`] installs outside authn and authz, from the method,
//! the percent-decoded path and the `watch` query flag. It lives in the request
//! extensions, and every consumer reads that one value:
//!
//!   * the authz middleware judges [`RequestInfo::verb`] + [`RequestInfo::target`]
//!     (through `crate::authz::Attributes::for_request`);
//!   * the [`ResourceCoords`] extractor hands dispatch the same target's coords;
//!   * the GET dispatcher branches list-vs-watch on [`RequestInfo::is_watch`].
//!
//! Nothing downstream parses the URL again. A consumer that finds no
//! `RequestInfo` answers a typed 500 ([`RequestInfoError::Missing`]) instead of
//! parsing the path itself, so authz and dispatch cannot disagree about what a
//! request names.
//!
//! They did disagree before T4.1. Authz split the RAW path while dispatch split
//! axum's percent-decoded `*rest`, so `POST .../serviceaccounts/foo%2Ftoken`
//! was judged as a create of a `ServiceAccount` named `foo%2Ftoken` and
//! dispatched as a token minted for `foo`. The `watch` flag was likewise read
//! twice with two truth tables. Upstream's `RequestInfoFactory` also classifies
//! the decoded path (Go's `URL.Path`), so decoding first is the faithful order.
//!
//! ## Tail shapes
//!
//! A resource path is `/api/<v>/<tail>` (core) or `/apis/<g>/<v>/<tail>` (named
//! group). The tail is split on `/` and matched against the six shapes, in
//! order:
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
//! A path matching none of these is a [`RequestTarget::NonResource`]: authz
//! judges it as a non-resource URL, and a resource route that receives one
//! answers a typed [`ApiError::NotFound`] (a real K8s `Status` 404, NOT axum's
//! empty 404).

use axum::async_trait;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use axum::http::{Extensions, Method, Uri};

use crate::error::ApiError;

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
    /// else the named group. `RouterState::lookup` keys on
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

    /// Parse a coords value from the group/version prefix + the tail after
    /// it. Called by [`parse_resource_path`], inside the one classification.
    ///
    /// `group` is `None` for a core path (`/api/<v>/...`) and `Some(g)` for a
    /// named-group path (`/apis/<g>/<v>/...`); `version` is the path's
    /// version either way. `rest` is the already-percent-decoded tail after
    /// the prefix (no leading slash).
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

/// What a request names: one of the six resource shapes, or anything else.
///
/// The two arms are the whole discriminant, so a request cannot be both a
/// resource and a non-resource URL, and a consumer cannot read resource
/// coordinates off a request that has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestTarget {
    /// A `/api/<v>/...` or `/apis/<g>/<v>/...` path in one of the six shapes.
    Resource(ResourceCoords),
    /// Every other path, carried percent-decoded: discovery, health, openapi,
    /// metrics, and any path under a resource prefix that is none of the six
    /// shapes (too deep, or with no plural).
    NonResource(String),
}

/// The typed request classification, mirroring kube-apiserver's
/// `RequestInfo`: the RBAC verb plus the [`RequestTarget`].
///
/// Built only by [`Self::parse`] (the request-info layer) and
/// [`Self::from_method_path`] (the same classification over an
/// already-decoded path). The fields are private, so a value in the request
/// extensions is always one of those two computations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestInfo {
    /// The RBAC verb (`get` / `list` / `watch` / `create` / `update` / `patch`
    /// / `delete` / `deletecollection`; for a non-resource path, the lowercased
    /// HTTP method).
    verb: String,
    target: RequestTarget,
}

/// Why a request could not be classified, or its classification could not be
/// found. One `Display` per cause; the HTTP status is chosen by variant in the
/// `From` impl below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RequestInfoError {
    /// No [`RequestInfo`] in the request extensions: a consumer ran on a route
    /// the request-info layer does not wrap. A wiring defect in this server,
    /// so a 500. It is never answered by parsing the path a second time.
    #[error(
        "the request reached a consumer unclassified: no RequestInfo in the request \
         extensions (the request-info layer does not wrap this route)"
    )]
    Missing,
    /// The path is not UTF-8 once percent-decoded.
    #[error("the request path is not valid UTF-8 once percent-decoded")]
    PathNotUtf8,
    /// The query string could not be decoded into key/value pairs.
    #[error("the request query string could not be decoded")]
    MalformedQuery,
}

impl From<RequestInfoError> for ApiError {
    fn from(e: RequestInfoError) -> Self {
        match e {
            RequestInfoError::Missing => ApiError::Internal(e.to_string()),
            RequestInfoError::PathNotUtf8 | RequestInfoError::MalformedQuery => {
                ApiError::BadRequest(e.to_string())
            }
        }
    }
}

/// Decompose a `/api/v1/...` or `/apis/<g>/<v>/...` path into its
/// [`ResourceCoords`]. Returns `None` when the path is not a resource shape
/// (the caller treats that as a non-resource request).
///
/// `path` is the request path (with leading slash), already percent-decoded.
/// The leading `/api/<v>` or `/apis/<g>/<v>` prefix is consumed; the rest is
/// fed to [`ResourceCoords::parse`].
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
    /// Classify a request from its method and URI. The ONE parse: the
    /// request-info layer calls this and stores the result; nothing else reads
    /// the URI to decide what a request names.
    ///
    /// The path is percent-decoded before it is split, as upstream classifies
    /// Go's decoded `URL.Path`. The `watch` flag is read from the decoded query
    /// with [`crate::params::query_flag`], and only for a resource request.
    ///
    /// # Errors
    ///
    /// [`RequestInfoError::PathNotUtf8`] when the decoded path is not UTF-8;
    /// [`RequestInfoError::MalformedQuery`] when the query of a resource
    /// request does not decode into key/value pairs.
    pub fn parse(method: &Method, uri: &Uri) -> Result<Self, RequestInfoError> {
        let path = percent_decode_path(uri.path())?;
        let target = RequestTarget::of_path(&path);
        let is_watch = match &target {
            RequestTarget::Resource(_) => watch_requested(uri)?,
            RequestTarget::NonResource(_) => false,
        };
        Ok(Self::classify(method.as_str(), target, is_watch))
    }

    /// The same classification as [`Self::parse`], over a path that is ALREADY
    /// percent-decoded and a `watch` flag that is already read. For callers
    /// that hold those pieces rather than a request (tests, tooling).
    #[must_use]
    pub fn from_method_path(method: &str, path: &str, is_watch: bool) -> Self {
        Self::classify(method, RequestTarget::of_path(path), is_watch)
    }

    fn classify(method: &str, target: RequestTarget, is_watch: bool) -> Self {
        let verb = match &target {
            RequestTarget::Resource(c) => resource_verb(method, c.name.is_some(), is_watch),
            // The RBAC verb for a non-resource path is the lowercased method.
            RequestTarget::NonResource(_) => method.to_ascii_lowercase(),
        };
        Self { verb, target }
    }

    /// The classification the request-info layer stored for this request.
    ///
    /// # Errors
    ///
    /// [`RequestInfoError::Missing`] when there is none: the caller runs on a
    /// route the layer does not wrap.
    pub fn from_extensions(extensions: &Extensions) -> Result<&Self, RequestInfoError> {
        extensions.get::<Self>().ok_or(RequestInfoError::Missing)
    }

    /// The RBAC verb.
    #[must_use]
    pub fn verb(&self) -> &str {
        &self.verb
    }

    /// What the request names.
    #[must_use]
    pub fn target(&self) -> &RequestTarget {
        &self.target
    }

    /// `true` iff this is a WATCH: a GET/HEAD on a resource collection with a
    /// truthy `watch` flag. The GET dispatcher streams exactly when this holds,
    /// and authz judged exactly this verb.
    #[must_use]
    pub fn is_watch(&self) -> bool {
        self.verb == "watch"
    }

    /// The decoded non-resource URL, or `None` for a resource request.
    #[must_use]
    pub fn non_resource_url(&self) -> Option<&str> {
        match &self.target {
            RequestTarget::NonResource(path) => Some(path),
            RequestTarget::Resource(_) => None,
        }
    }

    /// The resource coordinates dispatch acts on.
    ///
    /// # Errors
    ///
    /// A typed [`ApiError::NotFound`] carrying the decoded path when the
    /// request is not one of the six resource shapes (a resource route reached
    /// by a path too deep or with no plural).
    pub fn resource_coords(&self) -> Result<ResourceCoords, ApiError> {
        match &self.target {
            RequestTarget::Resource(c) => Ok(c.clone()),
            RequestTarget::NonResource(path) => Err(ApiError::NotFound(path.clone())),
        }
    }
}

impl RequestTarget {
    /// Classify an already-decoded path.
    fn of_path(path: &str) -> Self {
        match parse_resource_path(path) {
            Some(coords) => Self::Resource(coords),
            None => Self::NonResource(path.to_string()),
        }
    }
}

/// Percent-decode a request path, once, with the decoder axum's `Path`
/// extractor uses: `percent_encoding::percent_decode`, then a strict UTF-8
/// check. Each `%XX` (two hex digits, either case) becomes that byte;
/// anything else, including a malformed or truncated `%`, is kept as it is,
/// and `+` stays `+` (it is a space only in a form body).
///
/// It is the same crate function, not a copy of its rules, so the path authz
/// classifies and the path a `Path` extractor dispatches on (discovery's
/// `/apis/{group}/{version}`) cannot decode differently.
///
/// # Errors
///
/// [`RequestInfoError::PathNotUtf8`] when the decoded bytes are not UTF-8.
fn percent_decode_path(path: &str) -> Result<String, RequestInfoError> {
    percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| RequestInfoError::PathNotUtf8)
}

/// The request's `watch` flag, percent-decoded, read with the one truth table
/// every query flag uses ([`crate::params::query_flag`]). The FIRST `watch`
/// value wins, as in upstream's `RequestInfoFactory`.
fn watch_requested(uri: &Uri) -> Result<bool, RequestInfoError> {
    let Query(pairs) = Query::<Vec<(String, String)>>::try_from_uri(uri)
        .map_err(|_| RequestInfoError::MalformedQuery)?;
    Ok(pairs
        .iter()
        .find(|(k, _)| k == "watch")
        .is_some_and(|(_, v)| crate::params::query_flag(v)))
}

/// Map an HTTP method on a RESOURCE path to the RBAC verb, given whether the
/// path targets an instance (`has_name`) and whether the `watch` flag was set.
///
/// As in upstream's `RequestInfoFactory`, `watch` is read only on a collection
/// GET: a GET that names an instance is a `get` whatever its query says,
/// because that is what the dispatcher serves for it.
#[must_use]
pub fn resource_verb(method: &str, has_name: bool, is_watch: bool) -> String {
    let m = method.to_ascii_uppercase();
    match m.as_str() {
        "GET" | "HEAD" => {
            if has_name {
                "get".to_string()
            } else if is_watch {
                "watch".to_string()
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

/// Dispatch's view of the one classification: the resource coordinates the
/// request-info layer stored. No `RawPathParams`, no second split of the path.
#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ResourceCoords {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        RequestInfo::from_extensions(&parts.extensions)?.resource_coords()
    }
}

/// The whole classification, for a handler that needs more than the coords
/// (the GET dispatcher reads [`RequestInfo::is_watch`]).
#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RequestInfo {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(RequestInfo::from_extensions(&parts.extensions)?.clone())
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

    /// The resource coords of `ri`; panics (test-only) on a non-resource one.
    fn coords_of(ri: &RequestInfo) -> &ResourceCoords {
        match ri.target() {
            RequestTarget::Resource(c) => c,
            RequestTarget::NonResource(p) => panic!("expected a resource request, got {p:?}"),
        }
    }

    #[test]
    fn request_info_resource_verbs() {
        // GET collection → list; GET instance → get; GET ?watch → watch.
        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb(), "list");
        let c = coords_of(&ri);
        assert_eq!(c.group_key(), "");
        assert_eq!(c.plural, "pods");
        assert_eq!(c.namespace.as_deref(), Some("default"));
        assert_eq!(c.name, None);

        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb(), "get");
        assert_eq!(coords_of(&ri).name.as_deref(), Some("p1"));

        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods", true);
        assert_eq!(ri.verb(), "watch");
        assert!(ri.is_watch());

        // POST → create; PUT → update; PATCH → patch.
        let ri = RequestInfo::from_method_path("POST", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb(), "create");
        let ri = RequestInfo::from_method_path("PUT", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb(), "update");
        let ri =
            RequestInfo::from_method_path("PATCH", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb(), "patch");

        // DELETE instance → delete; DELETE collection → deletecollection.
        let ri =
            RequestInfo::from_method_path("DELETE", "/api/v1/namespaces/default/pods/p1", false);
        assert_eq!(ri.verb(), "delete");
        let ri = RequestInfo::from_method_path("DELETE", "/api/v1/namespaces/default/pods", false);
        assert_eq!(ri.verb(), "deletecollection");
    }

    #[test]
    fn a_watch_flag_on_an_instance_is_a_get() {
        // The dispatcher serves a single GET for an instance path whatever the
        // query says, so the verb authz judges is `get`, never `watch`.
        let ri = RequestInfo::from_method_path("GET", "/api/v1/namespaces/default/pods/p1", true);
        assert_eq!(ri.verb(), "get");
        assert!(!ri.is_watch());
        // And a watch flag never turns a write into a watch.
        let ri = RequestInfo::from_method_path("POST", "/api/v1/namespaces/default/pods", true);
        assert_eq!(ri.verb(), "create");
    }

    #[test]
    fn request_info_subresource_and_grouped() {
        let ri = RequestInfo::from_method_path(
            "PUT",
            "/apis/apps/v1/namespaces/default/deployments/web/scale",
            false,
        );
        assert_eq!(ri.verb(), "update");
        let c = coords_of(&ri);
        assert_eq!(c.group_key(), "apps");
        assert_eq!(c.plural, "deployments");
        assert_eq!(c.subresource.as_deref(), Some("scale"));
        assert_eq!(c.name.as_deref(), Some("web"));
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
            assert_eq!(ri.verb(), "get");
            assert_eq!(ri.non_resource_url(), Some(p), "{p} is non-resource");
            assert!(ri.resource_coords().is_err(), "{p} has no resource coords");
        }
    }

    #[test]
    fn request_info_discovery_paths_are_non_resource() {
        // /api, /api/v1, /apis, /apis/<g>/<v> are discovery → non-resource.
        for p in ["/api", "/api/v1", "/apis", "/apis/apps/v1"] {
            let ri = RequestInfo::from_method_path("GET", p, false);
            assert_eq!(
                ri.non_resource_url(),
                Some(p),
                "{p} is a discovery (non-resource) shape"
            );
        }
    }

    #[test]
    fn a_path_under_a_resource_prefix_in_no_shape_is_non_resource_and_not_found() {
        // Too deep: authz judges a non-resource URL, and a resource route that
        // receives it answers a typed 404 naming the decoded path.
        let p = "/api/v1/namespaces/default/pods/p1/status/extra";
        let ri = RequestInfo::from_method_path("GET", p, false);
        assert_eq!(ri.non_resource_url(), Some(p));
        match ri.resource_coords() {
            Err(ApiError::NotFound(path)) => assert_eq!(path, p),
            other => panic!("expected a typed NotFound, got {other:?}"),
        }
    }

    fn parse(method: &str, uri: &str) -> Result<RequestInfo, RequestInfoError> {
        let method = Method::from_bytes(method.as_bytes()).expect("test method");
        let uri: Uri = uri.parse().expect("test uri");
        RequestInfo::parse(&method, &uri)
    }

    #[test]
    fn parse_classifies_the_percent_decoded_path() {
        // The encoded slash is a path separator once decoded: this is the
        // token subresource of `foo`, not a ServiceAccount named `foo%2Ftoken`.
        let ri = parse(
            "POST",
            "/api/v1/namespaces/default/serviceaccounts/foo%2Ftoken",
        )
        .expect("classified");
        assert_eq!(ri.verb(), "create");
        let c = coords_of(&ri);
        assert_eq!(c.plural, "serviceaccounts");
        assert_eq!(c.name.as_deref(), Some("foo"));
        assert_eq!(c.subresource.as_deref(), Some("token"));
        assert_eq!(c.namespace.as_deref(), Some("default"));

        // An encoded separator inside the group/version prefix decodes too.
        let ri = parse("GET", "/apis/apps%2Fv1/namespaces/default/deployments").expect("ok");
        let c = coords_of(&ri);
        assert_eq!(c.group_key(), "apps");
        assert_eq!(c.version_key(), "v1");
        assert_eq!(c.plural, "deployments");
        assert_eq!(ri.verb(), "list");

        // Decoding happens ONCE: `%252F` is a literal `%2F` in a name.
        let ri = parse("GET", "/api/v1/namespaces/default/pods/a%252Fb").expect("ok");
        assert_eq!(coords_of(&ri).name.as_deref(), Some("a%2Fb"));
        assert_eq!(coords_of(&ri).subresource, None);

        // A non-resource path is carried decoded.
        let ri = parse("GET", "/openapi/v3/apis/apps%2Fv1").expect("ok");
        assert_eq!(ri.non_resource_url(), Some("/openapi/v3/apis/apps/v1"));
    }

    #[test]
    fn percent_decoding_matches_the_path_decoder_axum_used() {
        let decode = |p: &str| percent_decode_path(p).expect("utf-8");
        assert_eq!(decode("/a%2Fb"), "/a/b");
        assert_eq!(decode("/a%2fb"), "/a/b", "either hex case");
        assert_eq!(decode("/a%252Fb"), "/a%2Fb", "decoded exactly once");
        assert_eq!(decode("/%E2%9C%93"), "/\u{2713}", "multi-byte UTF-8");
        assert_eq!(decode("/a+b"), "/a+b", "`+` is not a space in a path");
        // Malformed or truncated escapes are kept verbatim, not refused.
        assert_eq!(decode("/a%zzb"), "/a%zzb");
        assert_eq!(decode("/a%2"), "/a%2");
        assert_eq!(decode("/a%"), "/a%");
        assert_eq!(decode("/%%41"), "/%A");
        assert_eq!(decode(""), "");
        assert_eq!(
            percent_decode_path("/%C3%28"),
            Err(RequestInfoError::PathNotUtf8),
            "an invalid UTF-8 sequence is refused"
        );
    }

    #[test]
    fn parse_refuses_a_path_that_is_not_utf8_once_decoded() {
        assert_eq!(
            parse("GET", "/api/v1/namespaces/default/pods/%FF"),
            Err(RequestInfoError::PathNotUtf8)
        );
        // A 400, not a 500: the client sent it.
        assert!(matches!(
            ApiError::from(RequestInfoError::PathNotUtf8),
            ApiError::BadRequest(_)
        ));
    }

    #[test]
    fn parse_reads_the_watch_flag_with_the_query_flag_truth_table() {
        let verb = |q: &str| {
            parse("GET", &["/api/v1/namespaces/default/pods", q].concat())
                .expect("ok")
                .verb()
                .to_string()
        };
        assert_eq!(verb("?watch=true"), "watch");
        assert_eq!(verb("?watch=1"), "watch");
        assert_eq!(verb("?watch=yes"), "watch");
        // Decoded before it is read.
        assert_eq!(verb("?watch=%74rue"), "watch");
        assert_eq!(verb("?watch=false"), "list");
        assert_eq!(verb("?watch"), "list");
        assert_eq!(verb("?watch="), "list");
        assert_eq!(verb(""), "list");
        // The first value wins.
        assert_eq!(verb("?watch=true&watch=false"), "watch");
        assert_eq!(verb("?watch=false&watch=true"), "list");
    }

    #[test]
    fn a_missing_classification_is_a_typed_500() {
        let empty = Extensions::new();
        assert_eq!(
            RequestInfo::from_extensions(&empty),
            Err(RequestInfoError::Missing)
        );
        assert!(matches!(
            ApiError::from(RequestInfoError::Missing),
            ApiError::Internal(_)
        ));
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
