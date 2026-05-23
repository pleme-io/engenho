//! Curated catalog of K8s kinds to generate in the M0.0.1 first pass.
//!
//! Hardcoded because:
//!   1. OpenAPI v3 doesn't carry the plural-resource name (`Pod` → `pods`,
//!      `PodSecurityPolicy` → `podsecuritypolicies`); those live in the
//!      apiserver's discovery doc, not the schema. Hardcoding here is
//!      the simplest path until M0.0.4 wires a fleet-aware lookup.
//!   2. Scope (Namespaced vs Cluster) is similarly not in OpenAPI v3.
//!   3. Curating a known-good subset lets us SHIP the typed surface
//!      while expanding coverage incrementally. Adding a new kind is
//!      one row in this table.
//!
//! Per theory/ENGENHO.md §M0.0.4 the eventual full set is ~150 kinds
//! across ~16 API groups. This file's `KIND_CATALOG` is the M0.0.1
//! subset (well-known kinds every test cluster uses).

/// One entry in the curated catalog: enough info to emit a typed
/// struct + `KubeResource` impl.
pub struct KindEntry {
    /// `Kind` field of the resource (e.g. `Pod`).
    pub kind: &'static str,
    /// OpenAPI definition key (e.g. `io.k8s.api.core.v1.Pod`).
    pub openapi_key: &'static str,
    /// API group (`""` for core/v1, `apps` for apps/v1, …).
    pub group: &'static str,
    /// API version (`v1`, `v1beta1`, …).
    pub version: &'static str,
    /// Plural resource name (the URL segment).
    pub resource: &'static str,
    /// `true` ⇒ cluster-scoped; `false` ⇒ namespaced.
    pub cluster_scoped: bool,
    /// Generated module path under `engenho-types::generated_v1_34::`
    /// (e.g. `core_v1` for core/v1 kinds).
    pub module: &'static str,
}

/// The curated set of M0.0.1 kinds.
///
/// Static array so the generator iterates deterministically — entry
/// order = emission order, which determines `lib.rs` mod declarations.
pub const KIND_CATALOG: &[KindEntry] = &[
    // ── core/v1 ─────────────────────────────────────────────────────
    KindEntry {
        kind: "Pod",
        openapi_key: "io.k8s.api.core.v1.Pod",
        group: "",
        version: "v1",
        resource: "pods",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "Service",
        openapi_key: "io.k8s.api.core.v1.Service",
        group: "",
        version: "v1",
        resource: "services",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "ConfigMap",
        openapi_key: "io.k8s.api.core.v1.ConfigMap",
        group: "",
        version: "v1",
        resource: "configmaps",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "Secret",
        openapi_key: "io.k8s.api.core.v1.Secret",
        group: "",
        version: "v1",
        resource: "secrets",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "Namespace",
        openapi_key: "io.k8s.api.core.v1.Namespace",
        group: "",
        version: "v1",
        resource: "namespaces",
        cluster_scoped: true,
        module: "core_v1",
    },
    KindEntry {
        kind: "ServiceAccount",
        openapi_key: "io.k8s.api.core.v1.ServiceAccount",
        group: "",
        version: "v1",
        resource: "serviceaccounts",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "Node",
        openapi_key: "io.k8s.api.core.v1.Node",
        group: "",
        version: "v1",
        resource: "nodes",
        cluster_scoped: true,
        module: "core_v1",
    },
    KindEntry {
        kind: "PersistentVolume",
        openapi_key: "io.k8s.api.core.v1.PersistentVolume",
        group: "",
        version: "v1",
        resource: "persistentvolumes",
        cluster_scoped: true,
        module: "core_v1",
    },
    KindEntry {
        kind: "PersistentVolumeClaim",
        openapi_key: "io.k8s.api.core.v1.PersistentVolumeClaim",
        group: "",
        version: "v1",
        resource: "persistentvolumeclaims",
        cluster_scoped: false,
        module: "core_v1",
    },
    KindEntry {
        kind: "Endpoints",
        openapi_key: "io.k8s.api.core.v1.Endpoints",
        group: "",
        version: "v1",
        resource: "endpoints",
        cluster_scoped: false,
        module: "core_v1",
    },
    // ── apps/v1 ─────────────────────────────────────────────────────
    KindEntry {
        kind: "Deployment",
        openapi_key: "io.k8s.api.apps.v1.Deployment",
        group: "apps",
        version: "v1",
        resource: "deployments",
        cluster_scoped: false,
        module: "apps_v1",
    },
    KindEntry {
        kind: "ReplicaSet",
        openapi_key: "io.k8s.api.apps.v1.ReplicaSet",
        group: "apps",
        version: "v1",
        resource: "replicasets",
        cluster_scoped: false,
        module: "apps_v1",
    },
    KindEntry {
        kind: "StatefulSet",
        openapi_key: "io.k8s.api.apps.v1.StatefulSet",
        group: "apps",
        version: "v1",
        resource: "statefulsets",
        cluster_scoped: false,
        module: "apps_v1",
    },
    KindEntry {
        kind: "DaemonSet",
        openapi_key: "io.k8s.api.apps.v1.DaemonSet",
        group: "apps",
        version: "v1",
        resource: "daemonsets",
        cluster_scoped: false,
        module: "apps_v1",
    },
    // ── rbac.authorization.k8s.io/v1 ────────────────────────────────
    KindEntry {
        kind: "Role",
        openapi_key: "io.k8s.api.rbac.v1.Role",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "roles",
        cluster_scoped: false,
        module: "rbac_v1",
    },
    KindEntry {
        kind: "ClusterRole",
        openapi_key: "io.k8s.api.rbac.v1.ClusterRole",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "clusterroles",
        cluster_scoped: true,
        module: "rbac_v1",
    },
    KindEntry {
        kind: "RoleBinding",
        openapi_key: "io.k8s.api.rbac.v1.RoleBinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "rolebindings",
        cluster_scoped: false,
        module: "rbac_v1",
    },
    KindEntry {
        kind: "ClusterRoleBinding",
        openapi_key: "io.k8s.api.rbac.v1.ClusterRoleBinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "clusterrolebindings",
        cluster_scoped: true,
        module: "rbac_v1",
    },
];
