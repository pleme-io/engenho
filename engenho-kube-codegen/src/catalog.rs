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
    /// kubectl short-name aliases (e.g. `["deploy"]` for `Deployment`).
    /// PURE registration metadata — NOT in OpenAPI; sourced verbatim from
    /// upstream kube-apiserver Go REST storage. Empty for kinds with no
    /// upstream short name (e.g. `Secret`, every RBAC kind).
    pub short_names: &'static [&'static str],
    /// The singular resource name (the lowercase kind, e.g. `deployment`).
    /// Served as discovery `singularName`; `kubectl` uses it to round-trip
    /// the kind. Always `kind.to_lowercase()`.
    pub singular: &'static str,
    /// kubectl resource categories the kind belongs to (e.g. `["all"]` for
    /// the workload + networking core kinds). Empty for kinds in no
    /// category. PURE registration metadata, like `short_names`.
    pub categories: &'static [&'static str],
    /// `true` ⇒ this kind has NO vendored OpenAPI schema and NO typed
    /// struct — it is served as opaque JSON via the generic
    /// `StoreBackedHandler` and appears ONLY as a `RESOURCE_CATALOG` row
    /// (no `<module>/<kind>.rs`, no `$ref` closure, no schema lookup).
    ///
    /// This is the codegen seam for kinds whose object is stored opaquely
    /// (e.g. `apiextensions.k8s.io/v1.CustomResourceDefinition` — the
    /// apiserver stores the CRD body as plain JSON, never decoding it into
    /// a typed struct). The generator SKIPS such kinds in the per-kind
    /// struct/module emission (no OpenAPI schema is required, so
    /// `openapi_key` / `module` may be empty) while STILL emitting the
    /// catalog descriptor row, so routing + discovery light up exactly as
    /// for a schema-backed kind. Non-opaque kinds set this `false`.
    pub opaque: bool,
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
        short_names: &["po"],
        singular: "pod",
        categories: &["all"],
        opaque: false,
    },
    KindEntry {
        kind: "Service",
        openapi_key: "io.k8s.api.core.v1.Service",
        group: "",
        version: "v1",
        resource: "services",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["svc"],
        singular: "service",
        categories: &["all"],
        opaque: false,
    },
    KindEntry {
        kind: "ConfigMap",
        openapi_key: "io.k8s.api.core.v1.ConfigMap",
        group: "",
        version: "v1",
        resource: "configmaps",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["cm"],
        singular: "configmap",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "Secret",
        openapi_key: "io.k8s.api.core.v1.Secret",
        group: "",
        version: "v1",
        resource: "secrets",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &[],
        singular: "secret",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "Namespace",
        openapi_key: "io.k8s.api.core.v1.Namespace",
        group: "",
        version: "v1",
        resource: "namespaces",
        cluster_scoped: true,
        module: "core_v1",
        short_names: &["ns"],
        singular: "namespace",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "ServiceAccount",
        openapi_key: "io.k8s.api.core.v1.ServiceAccount",
        group: "",
        version: "v1",
        resource: "serviceaccounts",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["sa"],
        singular: "serviceaccount",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "Node",
        openapi_key: "io.k8s.api.core.v1.Node",
        group: "",
        version: "v1",
        resource: "nodes",
        cluster_scoped: true,
        module: "core_v1",
        short_names: &["no"],
        singular: "node",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "PersistentVolume",
        openapi_key: "io.k8s.api.core.v1.PersistentVolume",
        group: "",
        version: "v1",
        resource: "persistentvolumes",
        cluster_scoped: true,
        module: "core_v1",
        short_names: &["pv"],
        singular: "persistentvolume",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "PersistentVolumeClaim",
        openapi_key: "io.k8s.api.core.v1.PersistentVolumeClaim",
        group: "",
        version: "v1",
        resource: "persistentvolumeclaims",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["pvc"],
        singular: "persistentvolumeclaim",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "Endpoints",
        openapi_key: "io.k8s.api.core.v1.Endpoints",
        group: "",
        version: "v1",
        resource: "endpoints",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["ep"],
        singular: "endpoints",
        categories: &[],
        opaque: false,
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
        short_names: &["deploy"],
        singular: "deployment",
        categories: &["all"],
        opaque: false,
    },
    KindEntry {
        kind: "ReplicaSet",
        openapi_key: "io.k8s.api.apps.v1.ReplicaSet",
        group: "apps",
        version: "v1",
        resource: "replicasets",
        cluster_scoped: false,
        module: "apps_v1",
        short_names: &["rs"],
        singular: "replicaset",
        categories: &["all"],
        opaque: false,
    },
    KindEntry {
        kind: "StatefulSet",
        openapi_key: "io.k8s.api.apps.v1.StatefulSet",
        group: "apps",
        version: "v1",
        resource: "statefulsets",
        cluster_scoped: false,
        module: "apps_v1",
        short_names: &["sts"],
        singular: "statefulset",
        categories: &["all"],
        opaque: false,
    },
    KindEntry {
        kind: "DaemonSet",
        openapi_key: "io.k8s.api.apps.v1.DaemonSet",
        group: "apps",
        version: "v1",
        resource: "daemonsets",
        cluster_scoped: false,
        module: "apps_v1",
        short_names: &["ds"],
        singular: "daemonset",
        categories: &["all"],
        opaque: false,
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
        short_names: &[],
        singular: "role",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "ClusterRole",
        openapi_key: "io.k8s.api.rbac.v1.ClusterRole",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "clusterroles",
        cluster_scoped: true,
        module: "rbac_v1",
        short_names: &[],
        singular: "clusterrole",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "RoleBinding",
        openapi_key: "io.k8s.api.rbac.v1.RoleBinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "rolebindings",
        cluster_scoped: false,
        module: "rbac_v1",
        short_names: &[],
        singular: "rolebinding",
        categories: &[],
        opaque: false,
    },
    KindEntry {
        kind: "ClusterRoleBinding",
        openapi_key: "io.k8s.api.rbac.v1.ClusterRoleBinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "clusterrolebindings",
        cluster_scoped: true,
        module: "rbac_v1",
        short_names: &[],
        singular: "clusterrolebinding",
        categories: &[],
        opaque: false,
    },
    // ── apiextensions.k8s.io/v1 ─────────────────────────────────────
    //
    // The CRD kind itself. OPAQUE: the apiserver stores a CRD object as
    // plain JSON (the `StoreBackedHandler` never decodes it into a typed
    // struct; the `CrdController` parses the few `spec.*` paths it needs
    // through a typed serde border in engenho-controllers), so this kind
    // needs NO vendored OpenAPI schema doc + NO generated `<module>/crd.rs`
    // — it appears ONLY as a `RESOURCE_CATALOG` descriptor row. That row is
    // enough: `handlers_from_catalog{,_with_admission}` builds a
    // `StoreBackedHandler` for it, the catch-all routes CRUD/list/watch,
    // and discovery folds it into `/apis` + `/apis/apiextensions.k8s.io/v1`
    // so `kubectl get crd` resolves the `customresourcedefinitions` plural
    // (shortNames crd/crds). Vendoring the apiextensions schema doc + a
    // typed CRD struct is DEFERRED (kubectl `apply -f crd.yaml` works; only
    // `kubectl explain customresourcedefinition` needs the schema doc).
    KindEntry {
        kind: "CustomResourceDefinition",
        openapi_key: "",
        group: "apiextensions.k8s.io",
        version: "v1",
        resource: "customresourcedefinitions",
        cluster_scoped: true,
        module: "",
        short_names: &["crd", "crds"],
        singular: "customresourcedefinition",
        categories: &["api-extensions"],
        opaque: true,
    },
];
