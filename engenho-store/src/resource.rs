//! Typed key for a K8s resource entry in the store.
//!
//! `(group, version, kind, namespace, name)` is the canonical
//! identity. Cluster-scoped kinds use `namespace = None`.
//!
//! The value payload is `serde_json::Value` at R6 — opaque JSON
//! that round-trips faithfully. R6.5+ may type per-kind via
//! engenho-types' catalog so the state machine can enforce
//! schema-level invariants (defaulters, validators, finalizers).

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResourceKey {
    /// API group; empty string for core/v1.
    pub group: String,
    pub version: String,
    pub kind: String,
    /// `None` for cluster-scoped resources (Namespace, Node, PV,
    /// ClusterRole, …); `Some(ns)` for namespaced.
    pub namespace: Option<String>,
    pub name: String,
}

impl ResourceKey {
    #[must_use]
    pub fn namespaced(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }

    #[must_use]
    pub fn cluster_scoped(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            namespace: None,
            name: name.into(),
        }
    }

    /// Stable string identifier — used in error payloads + logs.
    #[must_use]
    pub fn label(&self) -> String {
        let group = if self.group.is_empty() {
            "v1"
        } else {
            self.group.as_str()
        };
        match &self.namespace {
            Some(ns) => format!("{group}/{}/{}/{ns}/{}", self.version, self.kind, self.name),
            None => format!("{group}/{}/{}/{}", self.version, self.kind, self.name),
        }
    }
}

/// Stored value — opaque JSON at R6 (typed per-kind via
/// engenho-types::catalog at R6.5+).
///
/// `metadata.resourceVersion` is materialized by the state machine
/// on every successful apply — clients use it for optimistic
/// concurrency on update / delete. `metadata.uid` is set on first
/// create + preserved across updates.
pub type ResourceValue = serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_key_label_is_stable() {
        let k = ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo");
        assert_eq!(k.label(), "v1/v1/Pod/default/podinfo");
        let c = ResourceKey::cluster_scoped("", "v1", "Namespace", "default");
        assert_eq!(c.label(), "v1/v1/Namespace/default");
        let r = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "podinfo");
        assert_eq!(r.label(), "apps/v1/Deployment/default/podinfo");
    }

    #[test]
    fn resource_key_serde_round_trips() {
        let k = ResourceKey::namespaced("apps", "v1", "Deployment", "kube-system", "coredns");
        let json = serde_json::to_string(&k).unwrap();
        let back: ResourceKey = serde_json::from_str(&json).unwrap();
        assert_eq!(back, k);
    }

    #[test]
    fn resource_keys_are_ordered() {
        let a = ResourceKey::namespaced("", "v1", "Pod", "default", "a");
        let b = ResourceKey::namespaced("", "v1", "Pod", "default", "b");
        let c = ResourceKey::namespaced("apps", "v1", "Deployment", "default", "a");
        let mut v = vec![b.clone(), c.clone(), a.clone()];
        v.sort();
        // group("") sorts before group("apps"); within core/v1, Pods sort by name.
        assert_eq!(v, vec![a, b, c]);
    }
}
