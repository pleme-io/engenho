//! The `KubeResource` typed contract.
//!
//! Every Kubernetes resource kind in engenho-types implements this trait.
//! It is the substrate that the rest of engenho (apiserver registry,
//! datastore, controller-manager, scheduler) consumes — type-axis at L0
//! of the test pyramid (theory/ENGENHO.md §V.1).
//!
//! The trait is intentionally narrow. Wire surfaces (JSON / protobuf /
//! Server-Side-Apply / Strategic-Merge-Patch / JSON-Patch) are layered on
//! top via per-kind generated impls; this trait is the minimal common
//! shape every kind agrees to.

use std::borrow::Cow;

/// The Group/Version/Kind triple every typed resource declares.
///
/// Const-fn-eligible so that downstream callers can pattern-match the
/// triple in a `const` context — needed for the apiserver's typed
/// registry (planned M0.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupVersionKind {
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
}

/// The Group/Version/Resource triple (resource = the plural URL segment).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupVersionResource {
    pub group: &'static str,
    pub version: &'static str,
    pub resource: &'static str,
}

/// Whether a kind is namespaced or cluster-scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    Namespaced,
    Cluster,
}

/// The contract every Kubernetes resource type satisfies.
///
/// Implementors are emitted by `forge-gen --backend kube-resource` from
/// upstream OpenAPI v3. Hand-authoring an `impl KubeResource for X` for
/// any K8s upstream kind is a CI-rejected anti-pattern — see
/// `theory/ENGENHO.md` §IV (the non-negotiable rule).
pub trait KubeResource: serde::Serialize + serde::de::DeserializeOwned + Clone {
    /// Group/Version/Kind for this resource.
    const GVK: GroupVersionKind;

    /// Group/Version/Resource for this resource.
    const GVR: GroupVersionResource;

    /// Whether instances of this resource are namespaced or cluster-scoped.
    const SCOPE: Scope;

    /// The `metadata.name` field of this instance (or empty if unset).
    fn name(&self) -> Cow<'_, str>;

    /// The `metadata.namespace` field, if any.
    fn namespace(&self) -> Option<Cow<'_, str>>;

    /// The `metadata.resourceVersion` field, if any.
    /// Used by the watch + SSA machinery to detect concurrent writes.
    fn resource_version(&self) -> Option<Cow<'_, str>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gvk_equality_is_trivial() {
        let a = GroupVersionKind {
            group: "",
            version: "v1",
            kind: "Pod",
        };
        let b = GroupVersionKind {
            group: "",
            version: "v1",
            kind: "Pod",
        };
        assert_eq!(a, b);
    }

    #[test]
    fn scope_round_trips() {
        assert_eq!(Scope::Namespaced, Scope::Namespaced);
        assert_ne!(Scope::Namespaced, Scope::Cluster);
    }
}
