//! Typed `PersistentVolumeClaimSpec` + `PVCStatus` — M0.0.2 #11.
//!
//! Storage requests carry a Quantity-shaped string (e.g. `1Gi`,
//! `100Mi`). Until a typed `Quantity` lands at M0.0.4 (codegen
//! lifts the string parser into engenho-types), the wire shape
//! is `String`. Operators reading PVC status see the value as
//! it appears in K8s.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::generated_v1_34::apps_v1::LabelSelector;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimSpec {
    /// `AccessModes` contains the desired access modes.
    /// Valid values: `ReadWriteOnce` | `ReadOnlyMany` |
    /// `ReadWriteMany` | `ReadWriteOncePod`.
    #[serde(default, rename = "accessModes", skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,

    /// `Selector` is a label query over volumes to consider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,

    /// `Resources` represents the storage resources required.
    /// Keys: typically `storage`, `ephemeral-storage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// `VolumeName` is the binding reference to the PV backing this claim.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,

    /// `StorageClassName` references the StorageClass to use.
    #[serde(default, rename = "storageClassName", skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// `VolumeMode` — `Filesystem` (default) | `Block`.
    #[serde(default, rename = "volumeMode", skip_serializing_if = "Option::is_none")]
    pub volume_mode: Option<String>,
}

/// `ResourceRequirements` — typed resource requests + limits.
/// Reusable across PVC, Pod containers, and future kinds. The
/// inner Quantity strings will become a typed `Quantity` when
/// the codegen + parser land at M0.0.4.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimStatus {
    /// `Phase` — `Pending` | `Bound` | `Lost`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PvcPhase>,

    /// Access modes the volume currently provides.
    #[serde(default, rename = "accessModes", skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,

    /// Actual capacity once bound.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capacity: BTreeMap<String, String>,

    /// Conditions detail the PVC state changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PersistentVolumeClaimCondition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PvcPhase {
    Pending,
    Bound,
    Lost,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pvc_spec_round_trips_with_storage_request() {
        let mut spec = PersistentVolumeClaimSpec {
            access_modes: vec!["ReadWriteOnce".into()],
            storage_class_name: Some("local-path".into()),
            ..Default::default()
        };
        let mut resources = ResourceRequirements::default();
        resources.requests.insert("storage".into(), "1Gi".into());
        spec.resources = Some(resources);
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"storage\":\"1Gi\""), "got: {json}");
        assert!(json.contains("\"accessModes\":[\"ReadWriteOnce\"]"));
        let back: PersistentVolumeClaimSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn pvc_status_bound_round_trips() {
        let mut status = PersistentVolumeClaimStatus {
            phase: Some(PvcPhase::Bound),
            access_modes: vec!["ReadWriteOnce".into()],
            ..Default::default()
        };
        status.capacity.insert("storage".into(), "1Gi".into());
        let s = serde_json::to_string(&status).unwrap();
        assert!(s.contains("\"Bound\""));
        let back: PersistentVolumeClaimStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn pvc_phase_round_trips() {
        for p in [PvcPhase::Pending, PvcPhase::Bound, PvcPhase::Lost] {
            let s = serde_json::to_string(&p).unwrap();
            let back: PvcPhase = serde_json::from_str(&s).unwrap();
            assert_eq!(back, p);
        }
    }
}
