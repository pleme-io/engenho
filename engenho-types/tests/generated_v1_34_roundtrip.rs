//! Property tests on the codegen'd kinds: JSON round-trip identity +
//! KubeResource GVK invariants.
//!
//! Asserts that every kind emitted by engenho-kube-codegen:
//!   1. Round-trips through serde_json identically.
//!   2. Exposes a non-empty GVR resource string.
//!   3. Has `KubeResource::name()` matching `metadata.name`.

use engenho_types::generated_v1_34::apps_v1::*;
use engenho_types::generated_v1_34::autoscaling_v2::*;
use engenho_types::generated_v1_34::batch_v1::*;
use engenho_types::generated_v1_34::coordination_v1::*;
use engenho_types::generated_v1_34::core_v1::*;
use engenho_types::generated_v1_34::networking_v1::*;
use engenho_types::generated_v1_34::node_v1::*;
use engenho_types::generated_v1_34::policy_v1::*;
use engenho_types::generated_v1_34::rbac_v1::*;
use engenho_types::generated_v1_34::scheduling_v1::*;
use engenho_types::generated_v1_34::storage_v1::*;
use engenho_types::kind::KubeResource;
use engenho_types::meta::ObjectMeta;

use proptest::prelude::*;

fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,32}".prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_meta() -> impl Strategy<Value = ObjectMeta> {
    (arb_name(), proptest::option::of("[a-z][a-z0-9-]{0,30}")).prop_map(|(name, ns)| ObjectMeta {
        name,
        namespace: ns,
        ..Default::default()
    })
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
roundtrip_property_test!(pod_roundtrip, Pod, "", "Pod", "pods");
roundtrip_property_test!(service_roundtrip, Service, "", "Service", "services");
roundtrip_property_test!(
    configmap_roundtrip,
    ConfigMap,
    "",
    "ConfigMap",
    "configmaps"
);
roundtrip_property_test!(secret_roundtrip, Secret, "", "Secret", "secrets");
roundtrip_property_test!(
    namespace_roundtrip,
    Namespace,
    "",
    "Namespace",
    "namespaces"
);
roundtrip_property_test!(
    serviceaccount_roundtrip,
    ServiceAccount,
    "",
    "ServiceAccount",
    "serviceaccounts"
);
roundtrip_property_test!(node_roundtrip, Node, "", "Node", "nodes");
roundtrip_property_test!(
    persistentvolume_roundtrip,
    PersistentVolume,
    "",
    "PersistentVolume",
    "persistentvolumes"
);
roundtrip_property_test!(
    persistentvolumeclaim_roundtrip,
    PersistentVolumeClaim,
    "",
    "PersistentVolumeClaim",
    "persistentvolumeclaims"
);
roundtrip_property_test!(endpoints_roundtrip, Endpoints, "", "Endpoints", "endpoints");

// apps/v1
roundtrip_property_test!(
    deployment_roundtrip,
    Deployment,
    "apps",
    "Deployment",
    "deployments"
);
roundtrip_property_test!(
    replicaset_roundtrip,
    ReplicaSet,
    "apps",
    "ReplicaSet",
    "replicasets"
);
roundtrip_property_test!(
    statefulset_roundtrip,
    StatefulSet,
    "apps",
    "StatefulSet",
    "statefulsets"
);
roundtrip_property_test!(
    daemonset_roundtrip,
    DaemonSet,
    "apps",
    "DaemonSet",
    "daemonsets"
);

// rbac/v1
roundtrip_property_test!(
    role_roundtrip,
    Role,
    "rbac.authorization.k8s.io",
    "Role",
    "roles"
);
roundtrip_property_test!(
    clusterrole_roundtrip,
    ClusterRole,
    "rbac.authorization.k8s.io",
    "ClusterRole",
    "clusterroles"
);
roundtrip_property_test!(
    rolebinding_roundtrip,
    RoleBinding,
    "rbac.authorization.k8s.io",
    "RoleBinding",
    "rolebindings"
);
roundtrip_property_test!(
    clusterrolebinding_roundtrip,
    ClusterRoleBinding,
    "rbac.authorization.k8s.io",
    "ClusterRoleBinding",
    "clusterrolebindings"
);

// ── M0.0.4 typed-promotion kinds (previously served opaque) ───────────────
//
// Each was a `RESOURCE_CATALOG` opaque row before this commit; flipping its
// `module`/`openapi_key` in the codegen catalog emitted a typed struct (the
// codegen is mechanical — these macros prove the emitted structs round-trip +
// carry the correct GVK/GVR, the same contract the original ~22 kinds satisfy).

// core/v1
roundtrip_property_test!(
    replicationcontroller_roundtrip,
    ReplicationController,
    "",
    "ReplicationController",
    "replicationcontrollers"
);
roundtrip_property_test!(
    podtemplate_roundtrip,
    PodTemplate,
    "",
    "PodTemplate",
    "podtemplates"
);
roundtrip_property_test!(
    limitrange_roundtrip,
    LimitRange,
    "",
    "LimitRange",
    "limitranges"
);
roundtrip_property_test!(
    resourcequota_roundtrip,
    ResourceQuota,
    "",
    "ResourceQuota",
    "resourcequotas"
);
roundtrip_property_test!(event_roundtrip, Event, "", "Event", "events");

// apps/v1
roundtrip_property_test!(
    controllerrevision_roundtrip,
    ControllerRevision,
    "apps",
    "ControllerRevision",
    "controllerrevisions"
);

// batch/v1
roundtrip_property_test!(job_roundtrip, Job, "batch", "Job", "jobs");
roundtrip_property_test!(cronjob_roundtrip, CronJob, "batch", "CronJob", "cronjobs");

// networking.k8s.io/v1
roundtrip_property_test!(
    ingress_roundtrip,
    Ingress,
    "networking.k8s.io",
    "Ingress",
    "ingresses"
);
roundtrip_property_test!(
    ingressclass_roundtrip,
    IngressClass,
    "networking.k8s.io",
    "IngressClass",
    "ingressclasses"
);
roundtrip_property_test!(
    networkpolicy_roundtrip,
    NetworkPolicy,
    "networking.k8s.io",
    "NetworkPolicy",
    "networkpolicies"
);

// policy/v1
roundtrip_property_test!(
    poddisruptionbudget_roundtrip,
    PodDisruptionBudget,
    "policy",
    "PodDisruptionBudget",
    "poddisruptionbudgets"
);

// storage.k8s.io/v1
roundtrip_property_test!(
    storageclass_roundtrip,
    StorageClass,
    "storage.k8s.io",
    "StorageClass",
    "storageclasses"
);
roundtrip_property_test!(
    csinode_roundtrip,
    CSINode,
    "storage.k8s.io",
    "CSINode",
    "csinodes"
);
roundtrip_property_test!(
    csidriver_roundtrip,
    CSIDriver,
    "storage.k8s.io",
    "CSIDriver",
    "csidrivers"
);
roundtrip_property_test!(
    volumeattachment_roundtrip,
    VolumeAttachment,
    "storage.k8s.io",
    "VolumeAttachment",
    "volumeattachments"
);
roundtrip_property_test!(
    csistoragecapacity_roundtrip,
    CSIStorageCapacity,
    "storage.k8s.io",
    "CSIStorageCapacity",
    "csistoragecapacities"
);

// scheduling.k8s.io/v1
roundtrip_property_test!(
    priorityclass_roundtrip,
    PriorityClass,
    "scheduling.k8s.io",
    "PriorityClass",
    "priorityclasses"
);

// coordination.k8s.io/v1
roundtrip_property_test!(
    lease_roundtrip,
    Lease,
    "coordination.k8s.io",
    "Lease",
    "leases"
);

// node.k8s.io/v1
roundtrip_property_test!(
    runtimeclass_roundtrip,
    RuntimeClass,
    "node.k8s.io",
    "RuntimeClass",
    "runtimeclasses"
);

// autoscaling/v2
roundtrip_property_test!(
    horizontalpodautoscaler_roundtrip,
    HorizontalPodAutoscaler,
    "autoscaling",
    "HorizontalPodAutoscaler",
    "horizontalpodautoscalers"
);

/// A real kubectl-shaped manifest deserializes into the TYPED struct (the
/// promotion's payoff: the spec is no longer opaque `serde_json::Value` — its
/// fields are typed and reachable) and re-serializes to an equivalent value.
/// This is the conformance behavior `kubectl apply` (without `--validate=false`)
/// depends on: the apiserver can decode the object into a schema-backed struct.
#[test]
fn real_cronjob_manifest_deserializes_into_typed_struct() {
    let manifest = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": { "name": "nightly", "namespace": "default" },
        "spec": {
            "schedule": "0 0 * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "restartPolicy": "OnFailure",
                            "containers": [
                                { "name": "worker", "image": "busybox:1.36" }
                            ]
                        }
                    }
                }
            }
        }
    });
    let cj: CronJob = serde_json::from_value(manifest.clone()).expect("typed CronJob deserialize");
    assert_eq!(cj.metadata.name, "nightly");
    // The spec is TYPED (Option<CronJobSpec>), not opaque JSON — the schedule
    // field is a typed String reachable without re-parsing a Value blob.
    let spec = cj.spec.as_ref().expect("CronJob has a spec");
    // `schedule` is a required typed `String` field on the typed spec — no
    // opaque Value indexing.
    assert_eq!(spec.schedule, "0 0 * * *");
    // Round-trips back to an equivalent value.
    let back = serde_json::to_value(&cj).expect("re-serialize");
    assert_eq!(
        back.get("spec")
            .and_then(|s| s.get("schedule"))
            .and_then(|v| v.as_str()),
        Some("0 0 * * *"),
    );
}

/// A real Ingress manifest deserializes into the typed struct with its rules
/// reachable as typed fields (the networking.k8s.io promotion payoff).
#[test]
fn real_ingress_manifest_deserializes_into_typed_struct() {
    let manifest = serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": { "name": "web", "namespace": "default" },
        "spec": {
            "rules": [
                { "host": "example.com" }
            ]
        }
    });
    let ing: Ingress = serde_json::from_value(manifest).expect("typed Ingress deserialize");
    assert_eq!(ing.metadata.name, "web");
    let spec = ing.spec.as_ref().expect("Ingress has a spec");
    assert_eq!(
        spec.rules.first().and_then(|r| r.host.as_deref()),
        Some("example.com")
    );
}

/// Every newly-typed group now serves a `/openapi/v3` document carrying the
/// promoted kind's schema — this is what makes `kubectl explain <kind>` work
/// and lets `kubectl apply` validate without `--validate=false`. The schema
/// key is the kind's OpenAPI definition key (`io.k8s.api.<g>.<v>.<Kind>`).
#[test]
fn promoted_kinds_have_openapi_v3_schema_documents() {
    use engenho_types::openapi_v3::document_for;
    // (group, version, schema-key) for one representative promoted kind per
    // newly-vendored group.
    let cases = [
        ("batch", "v1", "io.k8s.api.batch.v1.CronJob"),
        (
            "networking.k8s.io",
            "v1",
            "io.k8s.api.networking.v1.Ingress",
        ),
        ("policy", "v1", "io.k8s.api.policy.v1.PodDisruptionBudget"),
        ("storage.k8s.io", "v1", "io.k8s.api.storage.v1.StorageClass"),
        (
            "scheduling.k8s.io",
            "v1",
            "io.k8s.api.scheduling.v1.PriorityClass",
        ),
        (
            "coordination.k8s.io",
            "v1",
            "io.k8s.api.coordination.v1.Lease",
        ),
        ("node.k8s.io", "v1", "io.k8s.api.node.v1.RuntimeClass"),
        (
            "autoscaling",
            "v2",
            "io.k8s.api.autoscaling.v2.HorizontalPodAutoscaler",
        ),
    ];
    for (group, version, key) in cases {
        let doc = document_for(group, version)
            .unwrap_or_else(|| panic!("{group}/{version} must serve a /openapi/v3 document"));
        let parsed: serde_json::Value =
            serde_json::from_str(doc).expect("served doc parses as JSON");
        assert_eq!(
            parsed.get("openapi").and_then(|v| v.as_str()),
            Some("3.0.0"),
            "{group}/{version} doc is openapi 3.0.0"
        );
        assert!(
            parsed
                .get("components")
                .and_then(|c| c.get("schemas"))
                .and_then(|s| s.get(key))
                .is_some(),
            "{group}/{version} /openapi/v3 doc must carry the {key} schema"
        );
    }
}

// Scope sanity check (cluster-scoped kinds have the right Scope const).
#[test]
fn cluster_scoped_kinds_have_cluster_scope() {
    use engenho_types::kind::Scope;
    assert_eq!(Namespace::SCOPE, Scope::Cluster);
    assert_eq!(Node::SCOPE, Scope::Cluster);
    assert_eq!(PersistentVolume::SCOPE, Scope::Cluster);
    assert_eq!(ClusterRole::SCOPE, Scope::Cluster);
    assert_eq!(ClusterRoleBinding::SCOPE, Scope::Cluster);
}

#[test]
fn namespaced_kinds_have_namespaced_scope() {
    use engenho_types::kind::Scope;
    assert_eq!(Pod::SCOPE, Scope::Namespaced);
    assert_eq!(Service::SCOPE, Scope::Namespaced);
    assert_eq!(ConfigMap::SCOPE, Scope::Namespaced);
    assert_eq!(Secret::SCOPE, Scope::Namespaced);
    assert_eq!(ServiceAccount::SCOPE, Scope::Namespaced);
    assert_eq!(PersistentVolumeClaim::SCOPE, Scope::Namespaced);
    assert_eq!(Endpoints::SCOPE, Scope::Namespaced);
    assert_eq!(Deployment::SCOPE, Scope::Namespaced);
    assert_eq!(ReplicaSet::SCOPE, Scope::Namespaced);
    assert_eq!(StatefulSet::SCOPE, Scope::Namespaced);
    assert_eq!(DaemonSet::SCOPE, Scope::Namespaced);
    assert_eq!(Role::SCOPE, Scope::Namespaced);
    assert_eq!(RoleBinding::SCOPE, Scope::Namespaced);
}
