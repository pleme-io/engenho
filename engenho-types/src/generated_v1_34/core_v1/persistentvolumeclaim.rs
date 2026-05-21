//! `PersistentVolumeClaim` — M0.0.2 typed expansion #11.
//!
//! Storage requests carry Quantity strings (`1Gi`, `100Mi`).
//! Will be typed when `Quantity` lands at M0.0.4 codegen.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::pvc_spec::{PersistentVolumeClaimSpec, PersistentVolumeClaimStatus};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaim {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: PersistentVolumeClaimSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PersistentVolumeClaimStatus>,
}

impl KubeResource for PersistentVolumeClaim {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "PersistentVolumeClaim",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "persistentvolumeclaims",
    };
    const SCOPE: Scope = Scope::Namespaced;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.metadata.name.as_str())
    }
    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.metadata.namespace.as_deref().map(Cow::Borrowed)
    }
    fn resource_version(&self) -> Option<Cow<'_, str>> {
        if self.metadata.resource_version.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(self.metadata.resource_version.as_str()))
        }
    }
}

fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
fn is_empty_spec(s: &PersistentVolumeClaimSpec) -> bool {
    s == &PersistentVolumeClaimSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::pvc_spec::{PvcPhase, ResourceRequirements};

    #[test]
    fn pvc_round_trips_with_typed_spec_and_status() {
        let mut pvc = PersistentVolumeClaim::default();
        pvc.metadata.name = "podinfo-storage".into();
        pvc.metadata.namespace = Some("default".into());
        pvc.spec.access_modes = vec!["ReadWriteOnce".into()];
        pvc.spec.storage_class_name = Some("local-path".into());
        let mut req = ResourceRequirements::default();
        req.requests.insert("storage".into(), "1Gi".into());
        pvc.spec.resources = Some(req);
        pvc.status = Some(PersistentVolumeClaimStatus {
            phase: Some(PvcPhase::Bound),
            ..Default::default()
        });
        let json = serde_json::to_string(&pvc).unwrap();
        assert!(json.contains("\"podinfo-storage\""));
        assert!(json.contains("\"1Gi\""));
        assert!(json.contains("\"Bound\""));
        let back: PersistentVolumeClaim = serde_json::from_str(&json).unwrap();
        assert_eq!(back, pvc);
    }

    #[test]
    fn pvc_gvk() {
        assert_eq!(PersistentVolumeClaim::GVK.kind, "PersistentVolumeClaim");
        assert_eq!(PersistentVolumeClaim::GVR.resource, "persistentvolumeclaims");
        assert_eq!(PersistentVolumeClaim::SCOPE, Scope::Namespaced);
    }
}
