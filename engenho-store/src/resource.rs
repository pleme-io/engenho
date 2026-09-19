//! Typed key for a K8s resource entry in the store.
//!
//! `(group, version, kind, namespace, name)` is the canonical
//! identity. Cluster-scoped kinds use `namespace = None`.
//!
//! The value payload is `serde_json::Value` at R6 — opaque JSON
//! that round-trips faithfully. R6.5+ may type per-kind via
//! engenho-types' catalog so the state machine can enforce
//! schema-level invariants (defaulters, validators, finalizers).

use std::collections::{BTreeMap, btree_map};
use std::ops::Bound;

use serde::{Deserialize, Serialize};

/// ★ FIELD ORDER IS LOAD-BEARING. The derived `Ord` compares the fields in
/// declaration order, and [`ListScope`] relies on it: every key of one GVK,
/// and within it every key of one namespace, sorts into one contiguous run.
/// Reordering the fields breaks every LIST (the `t3_2a_paged_list` tests
/// catch it).
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

/// The keys one LIST addresses: every key of one (group, version, kind),
/// optionally narrowed to one namespace. `namespace: None` spans every
/// namespace AND the cluster-scoped keys of the kind.
///
/// ## Why a scope is one contiguous run of the catalog map
///
/// [`ResourceKey`]'s derived `Ord` compares `group, version, kind,
/// namespace, name` in that order, with `None < Some`. So the keys of one
/// scope sort together, and [`Self::range`] asks the `BTreeMap` for exactly
/// that run: from the least key the scope could hold up to, but excluding,
/// the least key that sorts after all of them. No key outside the scope is
/// visited, so a LIST costs the size of its scope, not of the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListScope<'a> {
    pub group: &'a str,
    pub version: &'a str,
    pub kind: &'a str,
    pub namespace: Option<&'a str>,
}

impl<'a> ListScope<'a> {
    #[must_use]
    pub fn new(
        group: &'a str,
        version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
    ) -> Self {
        Self {
            group,
            version,
            kind,
            namespace,
        }
    }

    /// `true` iff `key` is one of the keys this scope addresses.
    #[must_use]
    pub fn contains(&self, key: &ResourceKey) -> bool {
        key.group == self.group
            && key.version == self.version
            && key.kind == self.kind
            && self
                .namespace
                .is_none_or(|ns| key.namespace.as_deref() == Some(ns))
    }

    /// The entries of `map` in this scope that sort strictly after `after`,
    /// in key order. The range is bounded at both ends, so nothing outside
    /// the scope is visited and no post-filter is needed.
    ///
    /// A cursor is clamped, never trusted: a continue token carries its key
    /// from the client, and `BTreeMap::range` panics on an inverted range. A
    /// cursor below the scope reads it from the start; one at or past its
    /// end reads nothing.
    pub fn range<'m, V>(
        &self,
        map: &'m BTreeMap<ResourceKey, V>,
        after: Option<&ResourceKey>,
    ) -> btree_map::Range<'m, ResourceKey, V> {
        let first = self.first();
        let end = self.end();
        let lower = match after {
            Some(cursor) if *cursor >= end => Bound::Included(&end),
            Some(cursor) if *cursor >= first => Bound::Excluded(cursor),
            _ => Bound::Included(&first),
        };
        map.range::<ResourceKey, _>((lower, Bound::Excluded(&end)))
    }

    /// The least key this scope could hold: `""` is the least name, and for
    /// an all-namespace scope `None` is the least namespace.
    fn first(&self) -> ResourceKey {
        ResourceKey {
            group: self.group.to_owned(),
            version: self.version.to_owned(),
            kind: self.kind.to_owned(),
            namespace: self.namespace.map(str::to_owned),
            name: String::new(),
        }
    }

    /// The least key that sorts after every key of this scope.
    ///
    /// The least string greater than `s` is `s` followed by one NUL: any
    /// greater string either extends `s` (so its next char is at least NUL)
    /// or exceeds it at an earlier position. So the scope ends at its last
    /// narrowing field's successor, with every later field at its least.
    fn end(&self) -> ResourceKey {
        let (kind, namespace) = match self.namespace {
            None => (successor(self.kind), None),
            Some(ns) => (self.kind.to_owned(), Some(successor(ns))),
        };
        ResourceKey {
            group: self.group.to_owned(),
            version: self.version.to_owned(),
            kind,
            namespace,
            name: String::new(),
        }
    }
}

/// The least string that sorts after `s`.
fn successor(s: &str) -> String {
    [s, "\0"].concat()
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
