//! M0.1 item 6 — invariants on the generated runtime catalog.
//!
//! `RESOURCE_CATALOG` (the runtime-iterable descriptor list the apiserver
//! folds for routing + discovery) MUST agree with the per-type
//! `KubeResource` consts (`GVK` / `GVR` / `SCOPE`). If a future codegen
//! change drifts the catalog from the per-type consts, this test fails —
//! the single-source-of-truth guarantee is mechanical, not by convention.

use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor, Subresource};
use engenho_types::kind::{KubeResource, Scope};

use engenho_types::generated_v1_34::apps_v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use engenho_types::generated_v1_34::core_v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
    Service, ServiceAccount,
};
use engenho_types::generated_v1_34::rbac_v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};

/// Look up a catalog row by kind. Panics with a clear message if the kind
/// is absent — a missing row is a real failure (the per-type const exists
/// but the catalog forgot it).
fn row(kind: &str) -> &'static ResourceDescriptor {
    RESOURCE_CATALOG
        .iter()
        .find(|d| d.kind == kind)
        .unwrap_or_else(|| {
            panic!("kind {kind:?} present as a generated type but absent from RESOURCE_CATALOG")
        })
}

/// Assert one kind's catalog row matches its `KubeResource` consts exactly:
/// plural == GVR.resource, group/version match GVK, namespaced matches SCOPE,
/// and the plural is non-empty.
fn assert_row_matches<K: KubeResource>() {
    let d = row(K::GVK.kind);
    assert!(!d.plural.is_empty(), "{}: plural is non-empty", K::GVK.kind);
    assert_eq!(
        d.plural,
        K::GVR.resource,
        "{}: catalog plural == GVR.resource",
        K::GVK.kind
    );
    assert_eq!(d.group, K::GVK.group, "{}: group", K::GVK.kind);
    assert_eq!(d.version, K::GVK.version, "{}: version", K::GVK.kind);
    let want_namespaced = matches!(K::SCOPE, Scope::Namespaced);
    assert_eq!(
        d.namespaced,
        want_namespaced,
        "{}: namespaced == SCOPE",
        K::GVK.kind
    );
    // api_version is precomputed; cross-check it against the GVK.
    let want_api_version = if K::GVK.group.is_empty() {
        K::GVK.version.to_string()
    } else {
        format!("{}/{}", K::GVK.group, K::GVK.version)
    };
    assert_eq!(
        d.api_version,
        want_api_version,
        "{}: api_version",
        K::GVK.kind
    );
}

#[test]
fn catalog_rows_match_per_type_consts() {
    // core/v1
    assert_row_matches::<Pod>();
    assert_row_matches::<Service>();
    assert_row_matches::<ConfigMap>();
    assert_row_matches::<Secret>();
    assert_row_matches::<Namespace>();
    assert_row_matches::<ServiceAccount>();
    assert_row_matches::<Node>();
    assert_row_matches::<PersistentVolume>();
    assert_row_matches::<PersistentVolumeClaim>();
    assert_row_matches::<Endpoints>();
    // apps/v1
    assert_row_matches::<Deployment>();
    assert_row_matches::<ReplicaSet>();
    assert_row_matches::<StatefulSet>();
    assert_row_matches::<DaemonSet>();
    // rbac.authorization.k8s.io/v1
    assert_row_matches::<Role>();
    assert_row_matches::<ClusterRole>();
    assert_row_matches::<RoleBinding>();
    assert_row_matches::<ClusterRoleBinding>();
}

#[test]
fn every_catalog_plural_is_non_empty() {
    for d in RESOURCE_CATALOG {
        assert!(!d.plural.is_empty(), "{}: plural must be non-empty", d.kind);
    }
}

#[test]
fn endpoints_uses_curated_irregular_plural() {
    let d = row("Endpoints");
    assert_eq!(
        d.plural, "endpoints",
        "Endpoints → endpoints (NOT endpointss)"
    );
    assert_eq!(d.plural, Endpoints::GVR.resource);
}

#[test]
fn catalog_kinds_are_unique() {
    let mut seen = std::collections::BTreeSet::new();
    for d in RESOURCE_CATALOG {
        assert!(seen.insert(d.kind), "duplicate kind in catalog: {}", d.kind);
    }
}

#[test]
fn catalog_routing_keys_are_unique() {
    // The apiserver routing key is (group, version, plural). It MUST be
    // collision-free, else two kinds silently mis-route.
    let mut seen = std::collections::BTreeSet::new();
    for d in RESOURCE_CATALOG {
        let key = (d.group, d.version, d.plural);
        assert!(seen.insert(key), "duplicate routing key: {key:?}");
    }
}

// ── per-kind registration metadata (short names / singular / categories) ──

/// Resolve the kind a short name maps to, by scanning the catalog.
fn kind_for_short_name(short: &str) -> Option<&'static str> {
    RESOURCE_CATALOG
        .iter()
        .find(|d| d.short_names.contains(&short))
        .map(|d| d.kind)
}

#[test]
fn known_short_names_map_to_expected_kinds() {
    // The exact upstream kube-apiserver short-name registrations we vendor.
    // kubectl resolves these against /api/v1 + /apis/<g>/<v> discovery.
    let cases = [
        ("deploy", "Deployment"),
        ("rs", "ReplicaSet"),
        ("po", "Pod"),
        ("svc", "Service"),
        ("cm", "ConfigMap"),
        ("ns", "Namespace"),
        ("sa", "ServiceAccount"),
        ("ep", "Endpoints"),
        ("pv", "PersistentVolume"),
        ("pvc", "PersistentVolumeClaim"),
        ("sts", "StatefulSet"),
        ("ds", "DaemonSet"),
        ("no", "Node"),
    ];
    for (short, kind) in cases {
        assert_eq!(
            kind_for_short_name(short),
            Some(kind),
            "short name {short:?} must resolve to {kind}"
        );
    }
}

#[test]
fn short_names_are_globally_unique() {
    // A short name resolving to two kinds is ambiguous — forbidden.
    let mut seen = std::collections::BTreeSet::new();
    for d in RESOURCE_CATALOG {
        for s in d.short_names {
            assert!(seen.insert(*s), "duplicate short name across kinds: {s:?}");
        }
    }
}

#[test]
fn every_kind_has_singular_equal_to_lowercase_kind() {
    for d in RESOURCE_CATALOG {
        assert!(!d.singular.is_empty(), "{}: singular present", d.kind);
        assert_eq!(
            d.singular,
            d.kind.to_lowercase(),
            "{}: singular == lowercased kind",
            d.kind
        );
    }
}

#[test]
fn secret_and_rbac_kinds_have_no_short_names() {
    // Secret + every RBAC kind have NO upstream short name — empty by
    // construction (we mirror upstream, never invent).
    for kind in [
        "Secret",
        "Role",
        "ClusterRole",
        "RoleBinding",
        "ClusterRoleBinding",
    ] {
        let d = row(kind);
        assert!(
            d.short_names.is_empty(),
            "{kind} has no upstream short name — must be empty"
        );
    }
}

#[test]
fn all_category_is_on_the_workload_kinds() {
    // The "all" category is carried by exactly the workload+networking core
    // kinds we catalog: Pod, Service, Deployment, ReplicaSet, StatefulSet,
    // DaemonSet (ReplicationController is not cataloged).
    let want_all = [
        "Pod",
        "Service",
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "DaemonSet",
    ];
    for kind in want_all {
        assert!(
            row(kind).categories.contains(&"all"),
            "{kind} must be in category 'all'"
        );
    }
    // Secret / Namespace / RBAC are NOT in any category.
    for kind in ["Secret", "Namespace", "Role", "Node"] {
        assert!(
            row(kind).categories.is_empty(),
            "{kind} must be in no category"
        );
    }
}

// ── subresource declarations (status / scale) ─────────────────────────────

#[test]
fn every_subresources_slice_is_a_subset_of_known_subresources() {
    // The typed enum makes a bad value unrepresentable, but pin that no row
    // somehow carries a duplicate or an unexpected combination — every slice
    // is a subset of {Status, Scale, Log}.
    for d in RESOURCE_CATALOG {
        for s in d.subresources {
            assert!(
                matches!(
                    s,
                    Subresource::Status | Subresource::Scale | Subresource::Log
                ),
                "{}: subresource must be Status, Scale, or Log",
                d.kind
            );
        }
        // No duplicates within a row.
        let mut seen = std::collections::HashSet::new();
        for s in d.subresources {
            assert!(
                seen.insert(*s),
                "{}: duplicate subresource in slice",
                d.kind
            );
        }
    }
}

#[test]
fn log_subresource_is_only_on_pod() {
    // /log (kubectl logs) is served by EXACTLY the Pod kind. Any other kind
    // declaring Log is a bug.
    for d in RESOURCE_CATALOG {
        let has_log = d.subresources.contains(&Subresource::Log);
        if d.kind == "Pod" {
            assert!(has_log, "Pod must serve the /log subresource");
        } else {
            assert!(!has_log, "{} must NOT serve /log (Pod-only)", d.kind);
        }
    }
}

#[test]
fn scale_is_only_on_deployment_replicaset_statefulset() {
    // /scale is served by EXACTLY the scalable workloads: the three apps/v1
    // workloads + core/v1 ReplicationController (the legacy scalable kind,
    // cataloged opaque since the M0.0.4 breadth expansion). Any other kind
    // declaring Scale is a bug.
    let want_scale = [
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "ReplicationController",
    ];
    for d in RESOURCE_CATALOG {
        if want_scale.contains(&d.kind) {
            assert!(d.has_scale(), "{} must serve /scale", d.kind);
        } else {
            assert!(!d.has_scale(), "{} must NOT serve /scale", d.kind);
        }
    }
}

#[test]
fn scale_bearing_kinds_also_bear_status() {
    // Every scalable kind also has /status (scale ⊆ status-bearing). A
    // kind with /scale but no /status would be an inconsistent catalog.
    for d in RESOURCE_CATALOG {
        if d.has_scale() {
            assert!(
                d.has_status(),
                "{}: a /scale kind must also serve /status",
                d.kind
            );
        }
    }
}

#[test]
fn status_is_on_the_expected_kinds() {
    // The status-subresource kinds we declare: the workloads + Pod / PVC /
    // Node / Namespace / Service. ConfigMap / Secret / Endpoints / RBAC /
    // ServiceAccount / PersistentVolume / CRD have NO status subresource.
    let want_status = [
        "Pod",
        "Service",
        "Namespace",
        "Node",
        "PersistentVolumeClaim",
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "DaemonSet",
    ];
    let no_status = [
        "ConfigMap",
        "Secret",
        "Endpoints",
        "ServiceAccount",
        "PersistentVolume",
        "Role",
        "ClusterRole",
        "RoleBinding",
        "ClusterRoleBinding",
        "CustomResourceDefinition",
    ];
    for kind in want_status {
        assert!(row(kind).has_status(), "{kind} must serve /status");
    }
    for kind in no_status {
        assert!(!row(kind).has_status(), "{kind} must NOT serve /status");
    }
}

// ── /openapi/v3 served-tuples ↔ catalog invariant ─────────────────────────

#[test]
fn openapi_v3_document_for_apps_is_openapi_3() {
    let doc = engenho_types::openapi_v3::document_for("apps", "v1")
        .expect("apps/v1 OpenAPI v3 document served");
    let parsed: serde_json::Value = serde_json::from_str(doc).expect("apps/v1 doc parses as JSON");
    assert_eq!(
        parsed.get("openapi").and_then(|v| v.as_str()),
        Some("3.0.0"),
        "apps/v1 vendored document is openapi 3.0.0"
    );
}

#[test]
fn served_openapi_tuples_equal_non_opaque_catalog_group_version_pairs() {
    // ★★ CATALOG REFLECTION invariant: the set of (group, version) pairs in
    // the served OpenAPI-v3 table MUST equal the distinct (group, version)
    // pairs of the NON-OPAQUE rows in RESOURCE_CATALOG. Adding a
    // schema-backed group is one catalog change + one vendored file + one
    // SERVED row — never a hand-wired divergence.
    //
    // OPAQUE kinds (no vendored OpenAPI schema — e.g.
    // apiextensions.k8s.io/v1.CustomResourceDefinition, stored as plain
    // JSON) are EXCLUDED by design: they are routable + discoverable but
    // have no `/openapi/v3` schema document (kubectl applies them with
    // `--validate=false`; `kubectl explain` is unavailable). Their presence
    // in RESOURCE_CATALOG without a SERVED row is the intended seam, so the
    // comparison filters them out.
    let catalog_pairs: std::collections::BTreeSet<(&str, &str)> = RESOURCE_CATALOG
        .iter()
        .filter(|d| !d.opaque)
        .map(|d| (d.group, d.version))
        .collect();
    let served_pairs: std::collections::BTreeSet<(&str, &str)> = engenho_types::openapi_v3::SERVED
        .iter()
        .map(|d| (d.group, d.version))
        .collect();
    assert_eq!(
        served_pairs, catalog_pairs,
        "served OpenAPI-v3 (group,version) set must equal the NON-OPAQUE RESOURCE_CATALOG's"
    );

    // And the apiextensions group (the opaque CRD kind) is deliberately
    // ABSENT from the served OpenAPI set — pin that so a future accidental
    // SERVED row for it (without a real vendored schema) is caught.
    assert!(
        !engenho_types::openapi_v3::SERVED
            .iter()
            .any(|d| d.group == "apiextensions.k8s.io"),
        "apiextensions.k8s.io is opaque — must NOT have a served OpenAPI doc"
    );
}

// ── M0.0.4 conformance-breadth catalog expansion ──────────────────────────
//
// The opaque-JSON catalog seam lets a kind go LIVE (routable + discoverable +
// store-backed CRUD) from a single `RESOURCE_CATALOG` row, with no vendored
// OpenAPI schema and no typed struct. The tests below pin the breadth the
// expansion added so a future regression that drops a conformance kind fails
// mechanically.

/// Every group the catalog serves at least one kind in (core advertised as
/// `""`). The conformance surface is "more groups + more kinds served".
fn served_groups() -> std::collections::BTreeSet<&'static str> {
    RESOURCE_CATALOG.iter().map(|d| d.group).collect()
}

#[test]
fn catalog_serves_at_least_fifty_conformance_kinds() {
    // The M0.0.4 breadth expansion took the served catalog from ~22 to 50+
    // kinds (the conformance-relevant set across ~16 API groups). This is the
    // single load-bearing "did the surface grow" gate — a regression that
    // drops kinds below the conformance floor fails here.
    assert!(
        RESOURCE_CATALOG.len() >= 50,
        "served catalog must carry >= 50 conformance kinds, got {}",
        RESOURCE_CATALOG.len()
    );
}

#[test]
fn conformance_relevant_groups_are_all_served() {
    // The ~16 API groups a conformant v1.34 cluster exposes. Each must have at
    // least one cataloged kind so it appears in `/api` or `/apis` discovery.
    let want_groups = [
        "",                             // core/v1
        "apps",                         // Deployment, …, ControllerRevision
        "batch",                        // Job, CronJob
        "autoscaling",                  // HorizontalPodAutoscaler
        "policy",                       // PodDisruptionBudget
        "networking.k8s.io",            // Ingress, IngressClass, NetworkPolicy
        "storage.k8s.io",               // StorageClass, CSI*, VolumeAttachment
        "node.k8s.io",                  // RuntimeClass
        "scheduling.k8s.io",            // PriorityClass
        "coordination.k8s.io",          // Lease
        "rbac.authorization.k8s.io",    // Role, …
        "authentication.k8s.io",        // TokenReview
        "authorization.k8s.io",         // *AccessReview
        "certificates.k8s.io",          // CertificateSigningRequest
        "apiregistration.k8s.io",       // APIService
        "flowcontrol.apiserver.k8s.io", // FlowSchema, PriorityLevelConfiguration
        "admissionregistration.k8s.io", // *WebhookConfiguration
        "apiextensions.k8s.io",         // CustomResourceDefinition
        "discovery.k8s.io",             // EndpointSlice
    ];
    let served = served_groups();
    for g in want_groups {
        assert!(
            served.contains(g),
            "conformance group {g:?} must be served (have >= 1 cataloged kind)"
        );
    }
}

#[test]
fn newly_added_conformance_kinds_carry_correct_metadata() {
    // Spot-check the (kind → plural, namespaced, shortNames) registration for a
    // representative sampling of the breadth-expansion kinds. These are the
    // values discovery folds into `/api` + `/apis`, which is what lets
    // `kubectl get <short>` resolve. Irregular plurals + cluster scope are the
    // failure-prone fields, so they're pinned explicitly.
    //
    // The `opaque` column tracks the M0.0.4 typed-promotion: the conformance
    // groups whose OpenAPI is now vendored (batch / networking / policy /
    // storage / scheduling / coordination / node / autoscaling) flipped from
    // opaque → typed (a `/openapi/v3` schema + `kubectl explain` + validated
    // apply). The groups still without a vendored schema (apiregistration,
    // flowcontrol, …) stay opaque.
    struct Expect {
        kind: &'static str,
        plural: &'static str,
        namespaced: bool,
        short_names: &'static [&'static str],
        opaque: bool,
    }
    let cases = [
        Expect {
            kind: "CronJob",
            plural: "cronjobs",
            namespaced: true,
            short_names: &["cj"],
            opaque: false,
        },
        Expect {
            kind: "HorizontalPodAutoscaler",
            plural: "horizontalpodautoscalers",
            namespaced: true,
            short_names: &["hpa"],
            opaque: false,
        },
        Expect {
            kind: "PodDisruptionBudget",
            plural: "poddisruptionbudgets",
            namespaced: true,
            short_names: &["pdb"],
            opaque: false,
        },
        Expect {
            kind: "Ingress",
            plural: "ingresses",
            namespaced: true,
            short_names: &["ing"],
            opaque: false,
        },
        Expect {
            kind: "NetworkPolicy",
            plural: "networkpolicies",
            namespaced: true,
            short_names: &["netpol"],
            opaque: false,
        },
        Expect {
            kind: "StorageClass",
            plural: "storageclasses",
            namespaced: false,
            short_names: &["sc"],
            opaque: false,
        },
        Expect {
            kind: "CSIStorageCapacity",
            plural: "csistoragecapacities",
            namespaced: true,
            short_names: &[],
            opaque: false,
        },
        Expect {
            kind: "PriorityClass",
            plural: "priorityclasses",
            namespaced: false,
            short_names: &["pc"],
            opaque: false,
        },
        Expect {
            kind: "Lease",
            plural: "leases",
            namespaced: true,
            short_names: &[],
            opaque: false,
        },
        // Still opaque — no vendored OpenAPI for these groups yet.
        Expect {
            kind: "CertificateSigningRequest",
            plural: "certificatesigningrequests",
            namespaced: false,
            short_names: &["csr"],
            opaque: true,
        },
        Expect {
            kind: "APIService",
            plural: "apiservices",
            namespaced: false,
            short_names: &[],
            opaque: true,
        },
        Expect {
            kind: "PriorityLevelConfiguration",
            plural: "prioritylevelconfigurations",
            namespaced: false,
            short_names: &[],
            opaque: true,
        },
    ];
    for e in cases {
        let d = row(e.kind);
        assert_eq!(d.plural, e.plural, "{}: plural", e.kind);
        assert_eq!(d.namespaced, e.namespaced, "{}: namespaced", e.kind);
        assert_eq!(d.short_names, e.short_names, "{}: shortNames", e.kind);
        assert_eq!(
            d.opaque, e.opaque,
            "{}: opaque (typed-promotion state)",
            e.kind
        );
        // singular == lowercased kind (already a global invariant, re-pinned
        // here for the new rows specifically).
        assert_eq!(d.singular, e.kind.to_lowercase(), "{}: singular", e.kind);
    }
}
