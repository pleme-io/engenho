use engenho_substrate::closed_enum;

closed_enum! {
    #[named]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub enum StateClass {
        Declared,
        ConsensusMetadata,
        Observed,
        Derived,
        Content,
        WorkloadData,
        IdentityAndSecrets,
        Ephemeral,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoredKind {
    pub group: &'static str,
    pub kind: &'static str,
    pub class: StateClass,
}

macro_rules! stored_kinds {
    ($( $group:literal $kind:literal => $class:ident, )+) => {
        &[ $( StoredKind { group: $group, kind: $kind, class: StateClass::$class }, )+ ]
    };
}

pub const STORED_KINDS: &[StoredKind] = stored_kinds! {
    "" "Pod" => Declared,
    "" "Service" => Declared,
    "" "ConfigMap" => Declared,
    "" "Secret" => IdentityAndSecrets,
    "" "Namespace" => Declared,
    "" "ServiceAccount" => Declared,
    "" "Node" => Declared,
    "" "PersistentVolume" => Declared,
    "" "PersistentVolumeClaim" => Declared,
    "" "Endpoints" => Derived,
    "discovery.k8s.io" "EndpointSlice" => Derived,
    "apps" "Deployment" => Declared,
    "apps" "ReplicaSet" => Declared,
    "apps" "StatefulSet" => Declared,
    "apps" "DaemonSet" => Declared,
    "rbac.authorization.k8s.io" "Role" => Declared,
    "rbac.authorization.k8s.io" "ClusterRole" => Declared,
    "rbac.authorization.k8s.io" "RoleBinding" => Declared,
    "rbac.authorization.k8s.io" "ClusterRoleBinding" => Declared,
    "apiextensions.k8s.io" "CustomResourceDefinition" => Declared,
    "admissionregistration.k8s.io" "MutatingWebhookConfiguration" => Declared,
    "admissionregistration.k8s.io" "ValidatingWebhookConfiguration" => Declared,
    "" "ReplicationController" => Declared,
    "" "PodTemplate" => Declared,
    "" "LimitRange" => Declared,
    "" "ResourceQuota" => Declared,
    "" "Event" => Observed,
    "batch" "Job" => Declared,
    "batch" "CronJob" => Declared,
    "apps" "ControllerRevision" => Derived,
    "autoscaling" "HorizontalPodAutoscaler" => Declared,
    "policy" "PodDisruptionBudget" => Declared,
    "networking.k8s.io" "Ingress" => Declared,
    "networking.k8s.io" "IngressClass" => Declared,
    "networking.k8s.io" "NetworkPolicy" => Declared,
    "storage.k8s.io" "StorageClass" => Declared,
    "storage.k8s.io" "CSINode" => Observed,
    "storage.k8s.io" "CSIDriver" => Declared,
    "storage.k8s.io" "VolumeAttachment" => Derived,
    "storage.k8s.io" "CSIStorageCapacity" => Observed,
    "node.k8s.io" "RuntimeClass" => Declared,
    "scheduling.k8s.io" "PriorityClass" => Declared,
    "coordination.k8s.io" "Lease" => Declared,
    "authentication.k8s.io" "TokenReview" => Ephemeral,
    "authorization.k8s.io" "SubjectAccessReview" => Ephemeral,
    "authorization.k8s.io" "SelfSubjectAccessReview" => Ephemeral,
    "authorization.k8s.io" "SelfSubjectRulesReview" => Ephemeral,
    "authorization.k8s.io" "LocalSubjectAccessReview" => Ephemeral,
    "certificates.k8s.io" "CertificateSigningRequest" => Declared,
    "apiregistration.k8s.io" "APIService" => Declared,
    "flowcontrol.apiserver.k8s.io" "FlowSchema" => Declared,
    "flowcontrol.apiserver.k8s.io" "PriorityLevelConfiguration" => Declared,
    "engenho.io" "Derivation" => Content,
    "engenho.io" "MaterializationReceipt" => Observed,
    "engenho.io" "Plantio" => Declared,
    "snapshot.storage.k8s.io" "VolumeSnapshot" => Declared,
    "snapshot.storage.k8s.io" "VolumeSnapshotContent" => Declared,
};

pub const CUSTOM_RESOURCE_CLASS: StateClass = StateClass::Declared;

impl StoredKind {
    #[must_use]
    pub fn lookup(group: &str, kind: &str) -> Option<&'static Self> {
        STORED_KINDS
            .iter()
            .find(|row| row.group == group && row.kind == kind)
    }
}

impl StateClass {
    #[must_use]
    pub fn of_stored_kind(group: &str, kind: &str) -> Option<Self> {
        StoredKind::lookup(group, kind).map(|row| row.class)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{CUSTOM_RESOURCE_CLASS, STORED_KINDS, StateClass, StoredKind};
    use crate::generated_v1_34::RESOURCE_CATALOG;

    #[test]
    fn the_eight_classes_are_the_fleet_designs_eight() {
        assert_eq!(
            StateClass::ALL.iter().map(|c| c.name()).collect::<Vec<_>>(),
            [
                "Declared",
                "ConsensusMetadata",
                "Observed",
                "Derived",
                "Content",
                "WorkloadData",
                "IdentityAndSecrets",
                "Ephemeral",
            ]
        );
    }

    #[test]
    fn no_stored_kind_is_registered_twice() {
        let mut seen = BTreeSet::new();
        for row in STORED_KINDS {
            assert!(
                seen.insert((row.group, row.kind)),
                "{}/{} has two rows",
                row.group,
                row.kind
            );
        }
    }

    #[test]
    fn every_catalog_kind_has_a_class() {
        assert!(RESOURCE_CATALOG.len() >= 50, "{}", RESOURCE_CATALOG.len());
        for d in RESOURCE_CATALOG {
            assert!(
                StateClass::of_stored_kind(d.group, d.kind).is_some(),
                "catalog kind {}/{} has no StateClass row",
                d.group,
                d.kind
            );
        }
    }

    #[test]
    fn lookup_is_exact_on_group_and_kind() {
        assert_eq!(
            StateClass::of_stored_kind("", "Secret"),
            Some(StateClass::IdentityAndSecrets)
        );
        assert_eq!(StateClass::of_stored_kind("apps", "Secret"), None);
        assert_eq!(StateClass::of_stored_kind("", "secret"), None);
        assert_eq!(
            StoredKind::lookup("engenho.io", "Derivation").map(|r| r.class),
            Some(StateClass::Content)
        );
        assert_eq!(CUSTOM_RESOURCE_CLASS, StateClass::Declared);
    }
}
