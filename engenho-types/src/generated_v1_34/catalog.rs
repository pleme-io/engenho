//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Runtime-iterable resource catalog — one [`ResourceDescriptor`] per
//! generated kind. The apiserver folds [`RESOURCE_CATALOG`] to build
//! handlers, register routes, and serve discovery; the curated plural
//! (`KindEntry.resource`) flows verbatim into every consumer so there is
//! ONE source of truth for pluralization (no `+s` derivation).
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.

use crate::kind::{GroupVersionKind, GroupVersionResource, Scope};

/// One runtime row describing a routable/discoverable Kubernetes kind.
///
/// `plural` is the curated URL segment (`KindEntry.resource`) — irregular
/// plurals (e.g. `Endpoints` → `endpoints`) are correct by construction.
/// `api_version` is `"v1"` for the core group, `"<group>/<version>"`
/// otherwise — precomputed so consumers never re-derive it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResourceDescriptor {
    /// API group (`""` for core/v1).
    pub group: &'static str,
    /// API version (`"v1"`, …).
    pub version: &'static str,
    /// Kind (`"Pod"`, `"Deployment"`, …).
    pub kind: &'static str,
    /// Plural URL segment (`"pods"`, `"endpoints"`, …) — curated, never `+s`.
    pub plural: &'static str,
    /// `true` ⇒ namespaced; `false` ⇒ cluster-scoped.
    pub namespaced: bool,
    /// `"v1"` for core, `"<group>/<version>"` otherwise.
    pub api_version: &'static str,
}

impl ResourceDescriptor {
    /// The Group/Version/Kind triple for this descriptor.
    #[must_use]
    pub const fn to_gvk(&self) -> GroupVersionKind {
        GroupVersionKind {
            group: self.group,
            version: self.version,
            kind: self.kind,
        }
    }

    /// The Group/Version/Resource triple for this descriptor.
    #[must_use]
    pub const fn to_gvr(&self) -> GroupVersionResource {
        GroupVersionResource {
            group: self.group,
            version: self.version,
            resource: self.plural,
        }
    }

    /// The runtime [`Scope`] for this descriptor.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        if self.namespaced {
            Scope::Namespaced
        } else {
            Scope::Cluster
        }
    }
}

/// Every routable/discoverable kind, in `KIND_CATALOG` order.
pub const RESOURCE_CATALOG: &[ResourceDescriptor] = &[
    ResourceDescriptor { group: "", version: "v1", kind: "Pod", plural: "pods", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "Service", plural: "services", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "ConfigMap", plural: "configmaps", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "Secret", plural: "secrets", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "Namespace", plural: "namespaces", namespaced: false, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "ServiceAccount", plural: "serviceaccounts", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "Node", plural: "nodes", namespaced: false, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "PersistentVolume", plural: "persistentvolumes", namespaced: false, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "PersistentVolumeClaim", plural: "persistentvolumeclaims", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "", version: "v1", kind: "Endpoints", plural: "endpoints", namespaced: true, api_version: "v1" },
    ResourceDescriptor { group: "apps", version: "v1", kind: "Deployment", plural: "deployments", namespaced: true, api_version: "apps/v1" },
    ResourceDescriptor { group: "apps", version: "v1", kind: "ReplicaSet", plural: "replicasets", namespaced: true, api_version: "apps/v1" },
    ResourceDescriptor { group: "apps", version: "v1", kind: "StatefulSet", plural: "statefulsets", namespaced: true, api_version: "apps/v1" },
    ResourceDescriptor { group: "apps", version: "v1", kind: "DaemonSet", plural: "daemonsets", namespaced: true, api_version: "apps/v1" },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "Role", plural: "roles", namespaced: true, api_version: "rbac.authorization.k8s.io/v1" },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRole", plural: "clusterroles", namespaced: false, api_version: "rbac.authorization.k8s.io/v1" },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "RoleBinding", plural: "rolebindings", namespaced: true, api_version: "rbac.authorization.k8s.io/v1" },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRoleBinding", plural: "clusterrolebindings", namespaced: false, api_version: "rbac.authorization.k8s.io/v1" },
];
