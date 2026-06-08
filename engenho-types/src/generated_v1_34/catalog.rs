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

/// A K8s subresource a kind serves under `<plural>/<name>/<sub>`.
///
/// Typed — NOT a string — so a bad combination is unrepresentable: the
/// discovery emitter, the router dispatch, and the store-scoping handler all
/// `match` on this enum, and the compiler refuses any value outside the
/// closed set. `Status` is the `/status` subresource (controller-written
/// status, isolated from spec on write); `Scale` is the autoscaling/v1
/// `/scale` subresource (the projected replica view).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Subresource {
    /// `/status` — get/patch/update the `.status` only (spec untouched).
    Status,
    /// `/scale` — get/patch/update the autoscaling/v1 Scale projection
    /// (`spec.replicas` only).
    Scale,
    /// `/log` — GET a Pod container's stdout/stderr (`kubectl logs`). Read-only
    /// (GET only). Pod-only; the router dispatches it to the kubelet
    /// (single-node: in-process) which reads `backend.logs`.
    Log,
}

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
    /// kubectl short-name aliases (e.g. `["deploy"]`). Empty for kinds
    /// with no upstream short name. Pure registration metadata (NOT in the
    /// OpenAPI schema) — flows into discovery `shortNames`.
    pub short_names: &'static [&'static str],
    /// Singular resource name (lowercase kind) — served as discovery
    /// `singularName`.
    pub singular: &'static str,
    /// kubectl resource categories (e.g. `["all"]`). Empty for kinds in
    /// no category. Flows into discovery `categories`.
    pub categories: &'static [&'static str],
    /// `true` ⇒ this kind has NO vendored OpenAPI v3 schema document and NO
    /// typed struct — it is served as opaque JSON via the generic
    /// `StoreBackedHandler` (e.g.
    /// `apiextensions.k8s.io/v1.CustomResourceDefinition`, stored as plain
    /// JSON). Such kinds are routable + discoverable but absent from the
    /// `/openapi/v3` served set, so the ★★ CATALOG-REFLECTION invariant
    /// (`SERVED (group,version) == non-opaque RESOURCE_CATALOG (group,version)`)
    /// excludes them. `kubectl apply` of an opaque kind needs
    /// `--validate=false` (no schema to validate against); `kubectl explain`
    /// is unavailable. Non-opaque kinds are `false`.
    pub opaque: bool,
    /// The subresources this kind serves under `<plural>/<name>/<sub>`
    /// (`/status`, `/scale`). Typed slice — the SINGLE source the router
    /// dispatch, the store-scoping handler, and discovery all read; no kind
    /// is ever special-cased by name. `&[]` for kinds with no subresource.
    pub subresources: &'static [Subresource],
}

impl ResourceDescriptor {
    /// `true` iff this kind serves the `/status` subresource.
    #[must_use]
    pub fn has_status(&self) -> bool {
        self.subresources.contains(&Subresource::Status)
    }

    /// `true` iff this kind serves the autoscaling/v1 `/scale` subresource.
    #[must_use]
    pub fn has_scale(&self) -> bool {
        self.subresources.contains(&Subresource::Scale)
    }

    /// `true` iff this kind serves the `/log` subresource (Pod only).
    #[must_use]
    pub fn has_log(&self) -> bool {
        self.subresources.contains(&Subresource::Log)
    }

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
    ResourceDescriptor { group: "", version: "v1", kind: "Pod", plural: "pods", namespaced: true, api_version: "v1", short_names: &["po"], singular: "pod", categories: &["all"], opaque: false, subresources: &[Subresource::Status, Subresource::Log] },
    ResourceDescriptor { group: "", version: "v1", kind: "Service", plural: "services", namespaced: true, api_version: "v1", short_names: &["svc"], singular: "service", categories: &["all"], opaque: false, subresources: &[Subresource::Status] },
    ResourceDescriptor { group: "", version: "v1", kind: "ConfigMap", plural: "configmaps", namespaced: true, api_version: "v1", short_names: &["cm"], singular: "configmap", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "", version: "v1", kind: "Secret", plural: "secrets", namespaced: true, api_version: "v1", short_names: &[], singular: "secret", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "", version: "v1", kind: "Namespace", plural: "namespaces", namespaced: false, api_version: "v1", short_names: &["ns"], singular: "namespace", categories: &[], opaque: false, subresources: &[Subresource::Status] },
    ResourceDescriptor { group: "", version: "v1", kind: "ServiceAccount", plural: "serviceaccounts", namespaced: true, api_version: "v1", short_names: &["sa"], singular: "serviceaccount", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "", version: "v1", kind: "Node", plural: "nodes", namespaced: false, api_version: "v1", short_names: &["no"], singular: "node", categories: &[], opaque: false, subresources: &[Subresource::Status] },
    ResourceDescriptor { group: "", version: "v1", kind: "PersistentVolume", plural: "persistentvolumes", namespaced: false, api_version: "v1", short_names: &["pv"], singular: "persistentvolume", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "", version: "v1", kind: "PersistentVolumeClaim", plural: "persistentvolumeclaims", namespaced: true, api_version: "v1", short_names: &["pvc"], singular: "persistentvolumeclaim", categories: &[], opaque: false, subresources: &[Subresource::Status] },
    ResourceDescriptor { group: "", version: "v1", kind: "Endpoints", plural: "endpoints", namespaced: true, api_version: "v1", short_names: &["ep"], singular: "endpoints", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "apps", version: "v1", kind: "Deployment", plural: "deployments", namespaced: true, api_version: "apps/v1", short_names: &["deploy"], singular: "deployment", categories: &["all"], opaque: false, subresources: &[Subresource::Status, Subresource::Scale] },
    ResourceDescriptor { group: "apps", version: "v1", kind: "ReplicaSet", plural: "replicasets", namespaced: true, api_version: "apps/v1", short_names: &["rs"], singular: "replicaset", categories: &["all"], opaque: false, subresources: &[Subresource::Status, Subresource::Scale] },
    ResourceDescriptor { group: "apps", version: "v1", kind: "StatefulSet", plural: "statefulsets", namespaced: true, api_version: "apps/v1", short_names: &["sts"], singular: "statefulset", categories: &["all"], opaque: false, subresources: &[Subresource::Status, Subresource::Scale] },
    ResourceDescriptor { group: "apps", version: "v1", kind: "DaemonSet", plural: "daemonsets", namespaced: true, api_version: "apps/v1", short_names: &["ds"], singular: "daemonset", categories: &["all"], opaque: false, subresources: &[Subresource::Status] },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "Role", plural: "roles", namespaced: true, api_version: "rbac.authorization.k8s.io/v1", short_names: &[], singular: "role", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRole", plural: "clusterroles", namespaced: false, api_version: "rbac.authorization.k8s.io/v1", short_names: &[], singular: "clusterrole", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "RoleBinding", plural: "rolebindings", namespaced: true, api_version: "rbac.authorization.k8s.io/v1", short_names: &[], singular: "rolebinding", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRoleBinding", plural: "clusterrolebindings", namespaced: false, api_version: "rbac.authorization.k8s.io/v1", short_names: &[], singular: "clusterrolebinding", categories: &[], opaque: false, subresources: &[] },
    ResourceDescriptor { group: "apiextensions.k8s.io", version: "v1", kind: "CustomResourceDefinition", plural: "customresourcedefinitions", namespaced: false, api_version: "apiextensions.k8s.io/v1", short_names: &["crd", "crds"], singular: "customresourcedefinition", categories: &["api-extensions"], opaque: true, subresources: &[] },
];
