//! Typed `ReplicaSetSpec` + `ReplicaSetStatus` — M0.0.2 #9.
//!
//! Reuses `LabelSelector` + `PodTemplateSpec` from
//! `deployment_spec` (PodTemplateSpec wraps PodSpec from
//! core_v1::pod_spec). The Pod-spec compounds across apps_v1
//! kinds — no duplication.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};

use super::deployment_spec::{LabelSelector, PodTemplateSpec};

/// `ReplicaSetSpec` is the specification of a ReplicaSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetSpec {
    /// `Replicas` is the number of desired replicas. Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Label selector for pods. Must match the pod template's labels.
    pub selector: LabelSelector,

    /// Template describes the pods that will be created.
    pub template: PodTemplateSpec,

    /// `MinReadySeconds` — newly created pods must be ready
    /// without crashing for this many seconds before counted.
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
}

/// `ReplicaSetStatus` is the most recently observed status.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetStatus {
    /// `ObservedGeneration` — the generation observed by the
    /// replicaset controller.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// `Replicas` — most recently observed number of replicas.
    pub replicas: i32,

    /// `FullyLabeledReplicas` — number of pods matching the
    /// pod template's labels.
    #[serde(default, rename = "fullyLabeledReplicas", skip_serializing_if = "Option::is_none")]
    pub fully_labeled_replicas: Option<i32>,

    /// `ReadyReplicas` — number of ready pods.
    #[serde(default, rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    /// `AvailableReplicas` — number of available pods.
    #[serde(default, rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,

    /// Conditions describe the current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ReplicaSetCondition>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetCondition {
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
    use crate::generated_v1_34::core_v1::Container;

    #[test]
    fn replicaset_spec_round_trips_with_template() {
        let mut spec = ReplicaSetSpec {
            replicas: Some(2),
            ..Default::default()
        };
        spec.selector
            .match_labels
            .insert("app".into(), "podinfo".into());
        spec.template.spec.containers.push(Container {
            name: "podinfod".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ..Default::default()
        });
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"replicas\":2"));
        assert!(json.contains("\"matchLabels\""));
        let back: ReplicaSetSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn replicaset_status_round_trips() {
        let status = ReplicaSetStatus {
            observed_generation: Some(1),
            replicas: 2,
            ready_replicas: Some(2),
            available_replicas: Some(2),
            ..Default::default()
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"replicas\":2"));
        let back: ReplicaSetStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }
}
