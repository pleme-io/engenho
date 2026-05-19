//! Property tests on the codegen'd kinds: JSON round-trip identity +
//! KubeResource GVK invariants.
//!
//! Asserts that every kind emitted by engenho-kube-codegen:
//!   1. Round-trips through serde_json identically.
//!   2. Exposes a non-empty GVR resource string.
//!   3. Has `KubeResource::name()` matching `metadata.name`.

use engenho_types::generated_v1_34::core_v1::*;
use engenho_types::generated_v1_34::apps_v1::*;
use engenho_types::generated_v1_34::rbac_v1::*;
use engenho_types::kind::KubeResource;
use engenho_types::meta::ObjectMeta;

use proptest::prelude::*;

fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,32}".prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_meta() -> impl Strategy<Value = ObjectMeta> {
    (arb_name(), proptest::option::of("[a-z][a-z0-9-]{0,30}")).prop_map(
        |(name, ns)| ObjectMeta {
            name,
            namespace: ns,
            ..Default::default()
        },
    )
}

macro_rules! roundtrip_property_test {
    ($name:ident, $type:ty, $expected_group:expr, $expected_kind:expr, $expected_resource:expr) => {
        proptest! {
            #[test]
            fn $name(meta in arb_meta()) {
                let mut obj = <$type>::default();
                obj.metadata = meta;
                let json    = serde_json::to_string(&obj).unwrap();
                let back: $type = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(obj.clone(), back);
                prop_assert_eq!(<$type>::GVK.group,    $expected_group);
                prop_assert_eq!(<$type>::GVK.kind,     $expected_kind);
                prop_assert_eq!(<$type>::GVR.resource, $expected_resource);
                let obj_name = obj.name().into_owned();
                prop_assert_eq!(obj_name, obj.metadata.name.clone());
            }
        }
    };
}

// core/v1
roundtrip_property_test!(pod_roundtrip,                    Pod,                    "", "Pod",                    "pods");
roundtrip_property_test!(service_roundtrip,                Service,                "", "Service",                "services");
roundtrip_property_test!(configmap_roundtrip,              ConfigMap,              "", "ConfigMap",              "configmaps");
roundtrip_property_test!(secret_roundtrip,                 Secret,                 "", "Secret",                 "secrets");
roundtrip_property_test!(namespace_roundtrip,              Namespace,              "", "Namespace",              "namespaces");
roundtrip_property_test!(serviceaccount_roundtrip,         ServiceAccount,         "", "ServiceAccount",         "serviceaccounts");
roundtrip_property_test!(node_roundtrip,                   Node,                   "", "Node",                   "nodes");
roundtrip_property_test!(persistentvolume_roundtrip,       PersistentVolume,       "", "PersistentVolume",       "persistentvolumes");
roundtrip_property_test!(persistentvolumeclaim_roundtrip,  PersistentVolumeClaim,  "", "PersistentVolumeClaim",  "persistentvolumeclaims");
roundtrip_property_test!(endpoints_roundtrip,              Endpoints,              "", "Endpoints",              "endpoints");

// apps/v1
roundtrip_property_test!(deployment_roundtrip,  Deployment,  "apps", "Deployment",  "deployments");
roundtrip_property_test!(replicaset_roundtrip,  ReplicaSet,  "apps", "ReplicaSet",  "replicasets");
roundtrip_property_test!(statefulset_roundtrip, StatefulSet, "apps", "StatefulSet", "statefulsets");
roundtrip_property_test!(daemonset_roundtrip,   DaemonSet,   "apps", "DaemonSet",   "daemonsets");

// rbac/v1
roundtrip_property_test!(role_roundtrip,                Role,                "rbac.authorization.k8s.io", "Role",                "roles");
roundtrip_property_test!(clusterrole_roundtrip,         ClusterRole,         "rbac.authorization.k8s.io", "ClusterRole",         "clusterroles");
roundtrip_property_test!(rolebinding_roundtrip,         RoleBinding,         "rbac.authorization.k8s.io", "RoleBinding",         "rolebindings");
roundtrip_property_test!(clusterrolebinding_roundtrip,  ClusterRoleBinding,  "rbac.authorization.k8s.io", "ClusterRoleBinding",  "clusterrolebindings");

// Scope sanity check (cluster-scoped kinds have the right Scope const).
#[test]
fn cluster_scoped_kinds_have_cluster_scope() {
    use engenho_types::kind::Scope;
    assert_eq!(Namespace::SCOPE,          Scope::Cluster);
    assert_eq!(Node::SCOPE,               Scope::Cluster);
    assert_eq!(PersistentVolume::SCOPE,   Scope::Cluster);
    assert_eq!(ClusterRole::SCOPE,        Scope::Cluster);
    assert_eq!(ClusterRoleBinding::SCOPE, Scope::Cluster);
}

#[test]
fn namespaced_kinds_have_namespaced_scope() {
    use engenho_types::kind::Scope;
    assert_eq!(Pod::SCOPE,                   Scope::Namespaced);
    assert_eq!(Service::SCOPE,               Scope::Namespaced);
    assert_eq!(ConfigMap::SCOPE,             Scope::Namespaced);
    assert_eq!(Secret::SCOPE,                Scope::Namespaced);
    assert_eq!(ServiceAccount::SCOPE,        Scope::Namespaced);
    assert_eq!(PersistentVolumeClaim::SCOPE, Scope::Namespaced);
    assert_eq!(Endpoints::SCOPE,             Scope::Namespaced);
    assert_eq!(Deployment::SCOPE,            Scope::Namespaced);
    assert_eq!(ReplicaSet::SCOPE,            Scope::Namespaced);
    assert_eq!(StatefulSet::SCOPE,           Scope::Namespaced);
    assert_eq!(DaemonSet::SCOPE,             Scope::Namespaced);
    assert_eq!(Role::SCOPE,                  Scope::Namespaced);
    assert_eq!(RoleBinding::SCOPE,           Scope::Namespaced);
}
