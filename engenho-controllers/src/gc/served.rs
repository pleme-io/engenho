//! What the apiserver serves right now, as the garbage collector's `RESTMapper`.
//!
//! Upstream resolves an ownerReference through `restMapper.RESTMapping(
//! groupKind, version)`, pinned to the reference's EXACT group and version
//! (`garbagecollector.go` `apiResource`). A miss is `restMappingError`, and
//! the dependent is requeued, never deleted. [`ServedKinds::resolve`] is that
//! lookup: the compiled-in [`RESOURCE_CATALOG`] plus every served version of
//! every stored `CustomResourceDefinition`, rebuilt at the start of each tick.
//!
//! A reference to a well-formed apiVersion that is not served
//! (`extensions/v1beta1` for a Deployment stored as `apps/v1`) is
//! [`Unresolvable`], never Absent. Reading it as Absent is how a live
//! workload's pods get deleted.

use engenho_types::generated_v1_34::RESOURCE_CATALOG;
use engenho_types::kind::Scope;
use serde_json::Value;
use thiserror::Error;

use crate::crd::CrdController;

/// One kind served at one group/version: a `RESTMapper` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedKind {
    /// API group; `""` for core.
    pub group: String,
    /// API version (`v1`).
    pub version: String,
    /// Canonical kind (`ReplicaSet`).
    pub kind: String,
    /// The resource (plural URL segment, `replicasets`).
    pub plural: String,
    /// Namespaced or cluster-scoped.
    pub scope: Scope,
}

/// Why an owner reference could not be classified. Each one leaves the
/// dependent exactly as it is: not knowing an owner is never evidence that
/// the owner is gone.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Unresolvable {
    /// The reference's apiVersion does not parse to a group and a non-empty
    /// version, or that exact group/version does not serve its kind.
    /// Upstream's `restMappingError`, message included.
    #[error("unable to get REST mapping for {api_version}/{kind}.")]
    NoRestMapping {
        /// The reference's `apiVersion`, verbatim.
        api_version: String,
        /// The reference's `kind`, verbatim.
        kind: String,
    },
    /// A cluster-scoped dependent names an owner of a namespaced kind. An
    /// owner reference has no namespace field, so there is nowhere to look.
    /// Upstream's `namespacedOwnerOfClusterScopedObjectErr`, message included.
    #[error("cluster-scoped objects cannot refer to namespaced owners")]
    NamespacedOwnerOfClusterScoped,
    /// `metadata.ownerReferences` is present and is not a list.
    #[error("metadata.ownerReferences is not a list")]
    NotAList,
    /// One entry of `metadata.ownerReferences` does not read as an owner
    /// reference (a missing `apiVersion`, `kind`, `name` or `uid`).
    #[error("metadata.ownerReferences[{index}] is not an owner reference")]
    Malformed {
        /// The entry's position in the list.
        index: usize,
    },
}

/// `apiVersion` to `(group, version)` the way `schema.ParseGroupVersion`
/// does it: `v1` is core, `g/v` is a group, anything else does not parse.
/// A group-only `apps/` parses to an empty version, which nothing serves.
fn parse_group_version(api_version: &str) -> Option<(&str, &str)> {
    let mut parts = api_version.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(version), None, None) if !version.is_empty() => Some(("", version)),
        (Some(group), Some(version), None) if !version.is_empty() => Some((group, version)),
        _ => None,
    }
}

/// Every kind served right now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServedKinds {
    kinds: Vec<ServedKind>,
}

impl ServedKinds {
    /// The compiled-in catalog: every built-in kind, each at its one version.
    #[must_use]
    pub fn builtin() -> Self {
        Self::from_kinds(RESOURCE_CATALOG.iter().map(|d| ServedKind {
            group: d.group.to_owned(),
            version: d.version.to_owned(),
            kind: d.kind.to_owned(),
            plural: d.plural.to_owned(),
            scope: d.scope(),
        }))
    }

    /// Exactly these kinds, in this order.
    #[must_use]
    pub fn from_kinds(kinds: impl IntoIterator<Item = ServedKind>) -> Self {
        Self {
            kinds: kinds.into_iter().collect(),
        }
    }

    /// Add every served version of each `CustomResourceDefinition`, read the
    /// way the CRD controller registers its handlers
    /// ([`CrdController::extract_entries`]). A CRD whose spec does not parse
    /// serves nothing and adds nothing.
    #[must_use]
    pub fn with_crds<'a>(mut self, crds: impl IntoIterator<Item = &'a Value>) -> Self {
        for crd in crds {
            self.kinds
                .extend(
                    CrdController::extract_entries(crd)
                        .into_iter()
                        .map(|e| ServedKind {
                            group: e.group,
                            version: e.version,
                            kind: e.kind,
                            plural: e.plural,
                            scope: if e.scope.is_namespaced() {
                                Scope::Namespaced
                            } else {
                                Scope::Cluster
                            },
                        }),
                );
        }
        self
    }

    /// Every served kind, in catalog order.
    pub fn iter(&self) -> impl Iterator<Item = &ServedKind> {
        self.kinds.iter()
    }

    /// The served kind at exactly this group, version and canonical kind.
    #[must_use]
    pub fn find(&self, group: &str, version: &str, kind: &str) -> Option<&ServedKind> {
        self.kinds
            .iter()
            .find(|k| k.group == group && k.version == version && k.kind == kind)
    }

    /// Resolve an owner reference's `apiVersion` + `kind`, pinned to that
    /// exact group/version: no fallback to another served version of the
    /// same kind.
    ///
    /// The kind matches exactly, or in its all-lowercase form (client-go's
    /// discovery mapper registers `strings.ToLower(Kind)` too). A mixed-case
    /// spelling matches nothing, as upstream. Upstream's third alias,
    /// `Kind+"List"`, maps to a resource the server does not serve; engenho
    /// does not guess one, so such a reference is [`Unresolvable`].
    ///
    /// # Errors
    ///
    /// [`Unresolvable::NoRestMapping`] when the apiVersion does not parse or
    /// that group/version does not serve the kind.
    pub fn resolve(&self, api_version: &str, kind: &str) -> Result<&ServedKind, Unresolvable> {
        let no_mapping = || Unresolvable::NoRestMapping {
            api_version: api_version.to_owned(),
            kind: kind.to_owned(),
        };
        let (group, version) = parse_group_version(api_version).ok_or_else(no_mapping)?;
        let at_version = |k: &&ServedKind| k.group == group && k.version == version;
        let lowercase = !kind.bytes().any(|b| b.is_ascii_uppercase());
        self.kinds
            .iter()
            .filter(at_version)
            .find(|k| k.kind == kind)
            .or_else(|| {
                self.kinds
                    .iter()
                    .filter(at_version)
                    .find(|k| lowercase && k.kind.eq_ignore_ascii_case(kind))
            })
            .ok_or_else(no_mapping)
    }

    /// Every OTHER served version of `served`'s group and kind.
    ///
    /// engenho keys a stored object by the version it was written at and
    /// does not convert on read, so an owner of a kind served at several
    /// versions may sit under any of them. A lookup that calls an owner
    /// absent must have looked at all of them.
    pub fn other_versions<'a>(
        &'a self,
        served: &'a ServedKind,
    ) -> impl Iterator<Item = &'a ServedKind> + 'a {
        self.kinds.iter().filter(move |k| {
            k.group == served.group && k.kind == served.kind && k.version != served.version
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn served(group: &str, version: &str, kind: &str, plural: &str) -> ServedKind {
        ServedKind {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            scope: Scope::Namespaced,
        }
    }

    #[test]
    fn group_versions_parse_as_upstream_does() {
        assert_eq!(parse_group_version("v1"), Some(("", "v1")));
        assert_eq!(parse_group_version("apps/v1"), Some(("apps", "v1")));
        assert_eq!(parse_group_version("/v1"), Some(("", "v1")));
        for bad in ["", "apps/", "a/b/c", "/"] {
            assert_eq!(parse_group_version(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_kind_resolves_only_at_its_own_group_version() {
        let kinds = ServedKinds::from_kinds([served("apps", "v1", "Deployment", "deployments")]);
        assert_eq!(
            kinds
                .resolve("apps/v1", "Deployment")
                .map(|k| &k.plural[..]),
            Ok("deployments")
        );
        assert_eq!(
            kinds.resolve("extensions/v1beta1", "Deployment"),
            Err(Unresolvable::NoRestMapping {
                api_version: "extensions/v1beta1".into(),
                kind: "Deployment".into(),
            })
        );
        assert!(kinds.resolve("apps/", "Deployment").is_err());
    }

    #[test]
    fn lowercase_resolves_and_mixed_case_does_not() {
        let kinds = ServedKinds::builtin();
        let rc = kinds
            .resolve("v1", "replicationcontroller")
            .expect("lowercase");
        assert_eq!(rc.kind, "ReplicationController");
        assert!(kinds.resolve("v1", "replicationController").is_err());
        assert!(kinds.resolve("v1", "PodList").is_err());
    }

    #[test]
    fn builtin_scope_comes_from_the_catalog() {
        let kinds = ServedKinds::builtin();
        assert_eq!(
            kinds.resolve("v1", "Node").map(|k| k.scope),
            Ok(Scope::Cluster)
        );
        assert_eq!(
            kinds.resolve("apps/v1", "StatefulSet").map(|k| k.scope),
            Ok(Scope::Namespaced)
        );
    }

    #[test]
    fn every_served_crd_version_resolves_and_an_unserved_one_does_not() {
        let crd = json!({"spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"kind": "Widget", "plural": "widgets"},
            "versions": [
                {"name": "v1", "served": true, "storage": true},
                {"name": "v2", "served": true},
                {"name": "v0", "served": false}
            ]
        }});
        let kinds = ServedKinds::builtin().with_crds([&crd]);
        let v2 = kinds
            .resolve("example.com/v2", "Widget")
            .expect("v2 served");
        assert_eq!(v2.scope, Scope::Cluster);
        assert!(kinds.resolve("example.com/v0", "Widget").is_err());
        let others: Vec<&str> = kinds.other_versions(v2).map(|k| &k.version[..]).collect();
        assert_eq!(others, ["v1"]);
    }
}
