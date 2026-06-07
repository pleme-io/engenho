//! M0.1 item 6 — invariants on the generated runtime catalog.
//!
//! `RESOURCE_CATALOG` (the runtime-iterable descriptor list the apiserver
//! folds for routing + discovery) MUST agree with the per-type
//! `KubeResource` consts (`GVK` / `GVR` / `SCOPE`). If a future codegen
//! change drifts the catalog from the per-type consts, this test fails —
//! the single-source-of-truth guarantee is mechanical, not by convention.

use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor};
use engenho_types::kind::{KubeResource, Scope};

use engenho_types::generated_v1_34::apps_v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use engenho_types::generated_v1_34::core_v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret,
    Service, ServiceAccount,
};
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, Role, RoleBinding,
};

/// Look up a catalog row by kind. Panics with a clear message if the kind
/// is absent — a missing row is a real failure (the per-type const exists
/// but the catalog forgot it).
fn row(kind: &str) -> &'static ResourceDescriptor {
    RESOURCE_CATALOG
        .iter()
        .find(|d| d.kind == kind)
        .unwrap_or_else(|| panic!("kind {kind:?} present as a generated type but absent from RESOURCE_CATALOG"))
}

/// Assert one kind's catalog row matches its `KubeResource` consts exactly:
/// plural == GVR.resource, group/version match GVK, namespaced matches SCOPE,
/// and the plural is non-empty.
fn assert_row_matches<K: KubeResource>() {
    let d = row(K::GVK.kind);
    assert!(!d.plural.is_empty(), "{}: plural is non-empty", K::GVK.kind);
    assert_eq!(d.plural, K::GVR.resource, "{}: catalog plural == GVR.resource", K::GVK.kind);
    assert_eq!(d.group, K::GVK.group, "{}: group", K::GVK.kind);
    assert_eq!(d.version, K::GVK.version, "{}: version", K::GVK.kind);
    let want_namespaced = matches!(K::SCOPE, Scope::Namespaced);
    assert_eq!(d.namespaced, want_namespaced, "{}: namespaced == SCOPE", K::GVK.kind);
    // api_version is precomputed; cross-check it against the GVK.
    let want_api_version = if K::GVK.group.is_empty() {
        K::GVK.version.to_string()
    } else {
        format!("{}/{}", K::GVK.group, K::GVK.version)
    };
    assert_eq!(d.api_version, want_api_version, "{}: api_version", K::GVK.kind);
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
    assert_eq!(d.plural, "endpoints", "Endpoints → endpoints (NOT endpointss)");
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
