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

/// A K8s subresource a kind serves under `<plural>/<name>/<sub>`.
///
/// Typed — NOT a string — so a bad combination is unrepresentable: the
/// discovery emitter, the router dispatch, and the store-scoping handler
/// all `match` on this enum, and the compiler refuses any value outside the
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
    /// (GET; no put/patch/delete). Pod-only; the router dispatches it to the
    /// kubelet (single-node: in-process) which reads `backend.logs`.
    Log,
}

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
    /// The subresources this kind serves under `<plural>/<name>/<sub>`
    /// (`/status`, `/scale`). Typed — `&[Subresource::Status,
    /// Subresource::Scale]` for the scalable workloads, `&[Subresource::Status]`
    /// for status-only kinds, `&[]` for kinds with no subresource. The
    /// catalog is the SINGLE source: the router dispatch, the store-scoping
    /// handler, and discovery all read this slice — no kind is ever
    /// special-cased by name in the router.
    pub subresources: &'static [Subresource],
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
        subresources: &[Subresource::Status, Subresource::Log],
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
        subresources: &[Subresource::Status],
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
        subresources: &[],
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
        subresources: &[],
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
        subresources: &[Subresource::Status],
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
        subresources: &[],
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
        subresources: &[Subresource::Status],
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
        subresources: &[],
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
        subresources: &[Subresource::Status],
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
        subresources: &[],
    },
    // ── discovery.k8s.io/v1 ─────────────────────────────────────────
    // EndpointSlice — the modern endpoint-publishing kind kube-proxy +
    // many controllers consume (the scalable successor to Endpoints).
    // Served OPAQUE: engenho's EndpointsController emits the slice body as
    // plain JSON alongside the legacy Endpoints object; there is no typed
    // struct closure required for the store-backed CRUD + the controller
    // build path (mirrors how CRD is served opaquely).
    KindEntry {
        kind: "EndpointSlice",
        openapi_key: "",
        group: "discovery.k8s.io",
        version: "v1",
        resource: "endpointslices",
        cluster_scoped: false,
        module: "",
        short_names: &[],
        singular: "endpointslice",
        categories: &[],
        opaque: true,
        subresources: &[],
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
        subresources: &[Subresource::Status, Subresource::Scale],
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
        subresources: &[Subresource::Status, Subresource::Scale],
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
        subresources: &[Subresource::Status, Subresource::Scale],
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
        subresources: &[Subresource::Status],
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
        subresources: &[],
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
        subresources: &[],
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
        subresources: &[],
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
        subresources: &[],
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
        subresources: &[],
    },
    // ── admissionregistration.k8s.io/v1 — dynamic admission config ──────
    //
    // Both webhook-configuration kinds are CLUSTER-SCOPED, opaque JSON (no
    // vendored schema / typed struct today — they are stored + read by the
    // apiserver's webhook-admission plugin as plain JSON). Serving them makes
    // `kubectl apply -f mutatingwebhookconfiguration.yaml` land a config the
    // `WebhookAdmissionPlugin` then dispatches on the write path. Vendoring the
    // admissionregistration schema doc + typed structs is DEFERRED (apply works;
    // only `kubectl explain mutatingwebhookconfiguration` needs the schema doc).
    KindEntry {
        kind: "MutatingWebhookConfiguration",
        openapi_key: "",
        group: "admissionregistration.k8s.io",
        version: "v1",
        resource: "mutatingwebhookconfigurations",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "mutatingwebhookconfiguration",
        categories: &["api-extensions"],
        opaque: true,
        subresources: &[],
    },
    KindEntry {
        kind: "ValidatingWebhookConfiguration",
        openapi_key: "",
        group: "admissionregistration.k8s.io",
        version: "v1",
        resource: "validatingwebhookconfigurations",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "validatingwebhookconfiguration",
        categories: &["api-extensions"],
        opaque: true,
        subresources: &[],
    },
    // ════════════════════════════════════════════════════════════════════
    // CONFORMANCE-BREADTH OPAQUE KINDS (M0.0.4 catalog expansion).
    //
    // Every entry below is served OPAQUE — no vendored OpenAPI schema, no
    // typed struct, no `<module>/<kind>.rs`. Each appears ONLY as a
    // `RESOURCE_CATALOG` descriptor row: `handlers_from_catalog{,_with_admission}`
    // builds a generic `StoreBackedHandler` per row, the catch-all router
    // serves CRUD/list/watch, and discovery folds it into `/api` + `/apis`.
    // `kubectl apply -f <kind>.yaml --validate=false` lands the object;
    // `kubectl get <kind>` resolves via the curated plural + shortNames.
    //
    // These widen the SERVED conformance surface (more kinds = more
    // conformance-suite coverage). Typed expansion (vendoring each group's
    // OpenAPI + emitting a struct) is the M0.0.4 follow-up; several have
    // their OWN reconciler brick deferred (named per group below) — SERVING
    // the kind is this brick's scope, BINDING/reconciling it is later work.
    // ── core/v1 — remaining well-known kinds ────────────────────────────
    KindEntry {
        kind: "ReplicationController",
        openapi_key: "io.k8s.api.core.v1.ReplicationController",
        group: "",
        version: "v1",
        resource: "replicationcontrollers",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["rc"],
        singular: "replicationcontroller",
        categories: &["all"],
        opaque: false,
        subresources: &[Subresource::Status, Subresource::Scale],
    },
    KindEntry {
        kind: "PodTemplate",
        openapi_key: "io.k8s.api.core.v1.PodTemplate",
        group: "",
        version: "v1",
        resource: "podtemplates",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &[],
        singular: "podtemplate",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "LimitRange",
        openapi_key: "io.k8s.api.core.v1.LimitRange",
        group: "",
        version: "v1",
        resource: "limitranges",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["limits"],
        singular: "limitrange",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "ResourceQuota",
        openapi_key: "io.k8s.api.core.v1.ResourceQuota",
        group: "",
        version: "v1",
        resource: "resourcequotas",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["quota"],
        singular: "resourcequota",
        categories: &[],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    KindEntry {
        kind: "Event",
        openapi_key: "io.k8s.api.core.v1.Event",
        group: "",
        version: "v1",
        resource: "events",
        cluster_scoped: false,
        module: "core_v1",
        short_names: &["ev"],
        singular: "event",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── batch/v1 — Job exists ONLY as a serving gap today; add both ─────
    // Job/CronJob reconcilers are a separate brick; serving the kinds is
    // this brick's scope.
    KindEntry {
        kind: "Job",
        openapi_key: "io.k8s.api.batch.v1.Job",
        group: "batch",
        version: "v1",
        resource: "jobs",
        cluster_scoped: false,
        module: "batch_v1",
        short_names: &[],
        singular: "job",
        categories: &["all"],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    KindEntry {
        kind: "CronJob",
        openapi_key: "io.k8s.api.batch.v1.CronJob",
        group: "batch",
        version: "v1",
        resource: "cronjobs",
        cluster_scoped: false,
        module: "batch_v1",
        short_names: &["cj"],
        singular: "cronjob",
        categories: &["all"],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    // ── apps/v1 — ControllerRevision (StatefulSet/DaemonSet history) ────
    KindEntry {
        kind: "ControllerRevision",
        openapi_key: "io.k8s.api.apps.v1.ControllerRevision",
        group: "apps",
        version: "v1",
        resource: "controllerrevisions",
        cluster_scoped: false,
        module: "apps_v1",
        short_names: &[],
        singular: "controllerrevision",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── autoscaling/v2 — HorizontalPodAutoscaler ───────────────────────
    KindEntry {
        kind: "HorizontalPodAutoscaler",
        openapi_key: "io.k8s.api.autoscaling.v2.HorizontalPodAutoscaler",
        group: "autoscaling",
        version: "v2",
        resource: "horizontalpodautoscalers",
        cluster_scoped: false,
        module: "autoscaling_v2",
        short_names: &["hpa"],
        singular: "horizontalpodautoscaler",
        categories: &[],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    // ── policy/v1 — PodDisruptionBudget ─────────────────────────────────
    KindEntry {
        kind: "PodDisruptionBudget",
        openapi_key: "io.k8s.api.policy.v1.PodDisruptionBudget",
        group: "policy",
        version: "v1",
        resource: "poddisruptionbudgets",
        cluster_scoped: false,
        module: "policy_v1",
        short_names: &["pdb"],
        singular: "poddisruptionbudget",
        categories: &[],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    // ── networking.k8s.io/v1 — Ingress / IngressClass / NetworkPolicy ──
    // Ingress controller + NetworkPolicy enforcement are separate bricks;
    // serving the kinds is this brick's scope.
    KindEntry {
        kind: "Ingress",
        openapi_key: "io.k8s.api.networking.v1.Ingress",
        group: "networking.k8s.io",
        version: "v1",
        resource: "ingresses",
        cluster_scoped: false,
        module: "networking_v1",
        short_names: &["ing"],
        singular: "ingress",
        categories: &[],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    KindEntry {
        kind: "IngressClass",
        openapi_key: "io.k8s.api.networking.v1.IngressClass",
        group: "networking.k8s.io",
        version: "v1",
        resource: "ingressclasses",
        cluster_scoped: true,
        module: "networking_v1",
        short_names: &[],
        singular: "ingressclass",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "NetworkPolicy",
        openapi_key: "io.k8s.api.networking.v1.NetworkPolicy",
        group: "networking.k8s.io",
        version: "v1",
        resource: "networkpolicies",
        cluster_scoped: false,
        module: "networking_v1",
        short_names: &["netpol"],
        singular: "networkpolicy",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── storage.k8s.io/v1 — StorageClass / CSI* / VolumeAttachment ──────
    // PV/PVC binder + CSI plumbing are separate bricks; serving the kinds
    // is this brick's scope.
    KindEntry {
        kind: "StorageClass",
        openapi_key: "io.k8s.api.storage.v1.StorageClass",
        group: "storage.k8s.io",
        version: "v1",
        resource: "storageclasses",
        cluster_scoped: true,
        module: "storage_v1",
        short_names: &["sc"],
        singular: "storageclass",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "CSINode",
        openapi_key: "io.k8s.api.storage.v1.CSINode",
        group: "storage.k8s.io",
        version: "v1",
        resource: "csinodes",
        cluster_scoped: true,
        module: "storage_v1",
        short_names: &[],
        singular: "csinode",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "CSIDriver",
        openapi_key: "io.k8s.api.storage.v1.CSIDriver",
        group: "storage.k8s.io",
        version: "v1",
        resource: "csidrivers",
        cluster_scoped: true,
        module: "storage_v1",
        short_names: &[],
        singular: "csidriver",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    KindEntry {
        kind: "VolumeAttachment",
        openapi_key: "io.k8s.api.storage.v1.VolumeAttachment",
        group: "storage.k8s.io",
        version: "v1",
        resource: "volumeattachments",
        cluster_scoped: true,
        module: "storage_v1",
        short_names: &[],
        singular: "volumeattachment",
        categories: &[],
        opaque: false,
        subresources: &[Subresource::Status],
    },
    KindEntry {
        kind: "CSIStorageCapacity",
        openapi_key: "io.k8s.api.storage.v1.CSIStorageCapacity",
        group: "storage.k8s.io",
        version: "v1",
        resource: "csistoragecapacities",
        cluster_scoped: false,
        module: "storage_v1",
        short_names: &[],
        singular: "csistoragecapacity",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── node.k8s.io/v1 — RuntimeClass ───────────────────────────────────
    KindEntry {
        kind: "RuntimeClass",
        openapi_key: "io.k8s.api.node.v1.RuntimeClass",
        group: "node.k8s.io",
        version: "v1",
        resource: "runtimeclasses",
        cluster_scoped: true,
        module: "node_v1",
        short_names: &[],
        singular: "runtimeclass",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── scheduling.k8s.io/v1 — PriorityClass ────────────────────────────
    KindEntry {
        kind: "PriorityClass",
        openapi_key: "io.k8s.api.scheduling.v1.PriorityClass",
        group: "scheduling.k8s.io",
        version: "v1",
        resource: "priorityclasses",
        cluster_scoped: true,
        module: "scheduling_v1",
        short_names: &["pc"],
        singular: "priorityclass",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── coordination.k8s.io/v1 — Lease ──────────────────────────────────
    KindEntry {
        kind: "Lease",
        openapi_key: "io.k8s.api.coordination.v1.Lease",
        group: "coordination.k8s.io",
        version: "v1",
        resource: "leases",
        cluster_scoped: false,
        module: "coordination_v1",
        short_names: &[],
        singular: "lease",
        categories: &[],
        opaque: false,
        subresources: &[],
    },
    // ── authentication.k8s.io/v1 — TokenReview (cluster-scoped, create) ─
    KindEntry {
        kind: "TokenReview",
        openapi_key: "",
        group: "authentication.k8s.io",
        version: "v1",
        resource: "tokenreviews",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "tokenreview",
        categories: &[],
        opaque: true,
        subresources: &[],
    },
    // ── authorization.k8s.io/v1 — *Access/SelfSubject reviews ───────────
    KindEntry {
        kind: "SubjectAccessReview",
        openapi_key: "",
        group: "authorization.k8s.io",
        version: "v1",
        resource: "subjectaccessreviews",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "subjectaccessreview",
        categories: &[],
        opaque: true,
        subresources: &[],
    },
    KindEntry {
        kind: "SelfSubjectAccessReview",
        openapi_key: "",
        group: "authorization.k8s.io",
        version: "v1",
        resource: "selfsubjectaccessreviews",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "selfsubjectaccessreview",
        categories: &[],
        opaque: true,
        subresources: &[],
    },
    KindEntry {
        kind: "SelfSubjectRulesReview",
        openapi_key: "",
        group: "authorization.k8s.io",
        version: "v1",
        resource: "selfsubjectrulesreviews",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "selfsubjectrulesreview",
        categories: &[],
        opaque: true,
        subresources: &[],
    },
    KindEntry {
        kind: "LocalSubjectAccessReview",
        openapi_key: "",
        group: "authorization.k8s.io",
        version: "v1",
        resource: "localsubjectaccessreviews",
        cluster_scoped: false,
        module: "",
        short_names: &[],
        singular: "localsubjectaccessreview",
        categories: &[],
        opaque: true,
        subresources: &[],
    },
    // ── certificates.k8s.io/v1 — CertificateSigningRequest ──────────────
    KindEntry {
        kind: "CertificateSigningRequest",
        openapi_key: "",
        group: "certificates.k8s.io",
        version: "v1",
        resource: "certificatesigningrequests",
        cluster_scoped: true,
        module: "",
        short_names: &["csr"],
        singular: "certificatesigningrequest",
        categories: &[],
        opaque: true,
        subresources: &[Subresource::Status],
    },
    // ── apiregistration.k8s.io/v1 — APIService (aggregation registry) ───
    // Aggregated-API PROXYING needs a real backend (deferred); SERVING the
    // APIService registry object is this brick's scope.
    KindEntry {
        kind: "APIService",
        openapi_key: "",
        group: "apiregistration.k8s.io",
        version: "v1",
        resource: "apiservices",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "apiservice",
        categories: &[],
        opaque: true,
        subresources: &[Subresource::Status],
    },
    // ── flowcontrol.apiserver.k8s.io/v1 — APF schema + priority levels ──
    // The flow-control ENFORCEMENT machinery is deferred; SERVING the
    // config objects is this brick's scope.
    KindEntry {
        kind: "FlowSchema",
        openapi_key: "",
        group: "flowcontrol.apiserver.k8s.io",
        version: "v1",
        resource: "flowschemas",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "flowschema",
        categories: &[],
        opaque: true,
        subresources: &[Subresource::Status],
    },
    KindEntry {
        kind: "PriorityLevelConfiguration",
        openapi_key: "",
        group: "flowcontrol.apiserver.k8s.io",
        version: "v1",
        resource: "prioritylevelconfigurations",
        cluster_scoped: true,
        module: "",
        short_names: &[],
        singular: "prioritylevelconfiguration",
        categories: &[],
        opaque: true,
        subresources: &[Subresource::Status],
    },
];
