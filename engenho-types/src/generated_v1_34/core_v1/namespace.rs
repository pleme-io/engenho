//! `Namespace` — M0.0.2 typed expansion #5. First cluster-scoped
//! kind in the catalog.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::namespace_spec::{NamespaceSpec, NamespaceStatus};

/// `Namespace` provides a scope for Names. Use of multiple
/// namespaces is optional.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Namespace {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: NamespaceSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<NamespaceStatus>,
}

impl KubeResource for Namespace {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Namespace",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "namespaces",
    };
    /// **Cluster-scoped** — engenho-kube-client routes requests
    /// without a `/namespaces/<ns>/` URL segment for this kind.
    const SCOPE: Scope = Scope::Cluster;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.metadata.name.as_str())
    }
    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.metadata.namespace.as_deref().map(Cow::Borrowed)
    }
    fn resource_version(&self) -> Option<Cow<'_, str>> {
        if self.metadata.resource_version.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(self.metadata.resource_version.as_str()))
        }
    }
}

fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
fn is_empty_spec(s: &NamespaceSpec) -> bool {
    s == &NamespaceSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::namespace_spec::NamespacePhase;

    #[test]
    fn namespace_round_trips_with_typed_status() {
        let mut ns = Namespace::default();
        ns.metadata.name = "flux-system".into();
        ns.status = Some(NamespaceStatus {
            phase: Some(NamespacePhase::Active),
            ..Default::default()
        });
        let json = serde_json::to_string(&ns).unwrap();
        assert!(json.contains("\"Active\""), "got: {json}");
        // Cluster-scoped: serialized form must not carry a namespace.
        assert!(!json.contains("\"namespace\""), "Namespace must not have its own .namespace field: {json}");
        let back: Namespace = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ns);
    }

    #[test]
    fn namespace_is_cluster_scoped() {
        assert_eq!(Namespace::SCOPE, Scope::Cluster);
        assert_eq!(Namespace::GVK.kind, "Namespace");
        assert_eq!(Namespace::GVR.resource, "namespaces");
    }
}
