//! Typed `DeploymentSpec` + `DeploymentStatus` — M0.0.2 #4.
//!
//! Deployment's spec embeds PodTemplateSpec which embeds PodSpec,
//! so we reuse `core_v1::pod_spec::PodSpec` rather than duplicating.
//! Scope-disciplined per M0.0.2: ship the fields engenho-local's
//! podinfo + flux deployments actually populate; defer the long
//! tail (rolling-update tunables, deployment history limits, …)
//! to M0.0.3 codegen.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::generated_v1_34::core_v1::PodSpec;
use crate::meta::ObjectMeta;

/// `DeploymentSpec` is the specification of the desired behavior
/// of the Deployment.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentSpec {
    /// Number of desired pods. Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Label selector for pods. Must match the pod template's labels.
    pub selector: LabelSelector,

    /// Template describes the pods that will be created.
    pub template: PodTemplateSpec,

    /// `MinReadySeconds` — minimum seconds a newly created pod
    /// should be ready without any container crashing before it
    /// is considered available.
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,

    /// `Paused` indicates that the deployment is paused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,

    /// `RevisionHistoryLimit` — number of old ReplicaSets to retain.
    /// Default 10.
    #[serde(default, rename = "revisionHistoryLimit", skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,

    /// `ProgressDeadlineSeconds` — max time in seconds for a
    /// deployment to make progress before it's considered failed.
    #[serde(default, rename = "progressDeadlineSeconds", skip_serializing_if = "Option::is_none")]
    pub progress_deadline_seconds: Option<i32>,
}

/// `LabelSelector` — `matchLabels` + `matchExpressions`. M0.0.2
/// ships `matchLabels` only; `matchExpressions` is opaque
/// `serde_json::Value` pending M0.0.3.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelSelector {
    #[serde(default, rename = "matchLabels", skip_serializing_if = "BTreeMap::is_empty")]
    pub match_labels: BTreeMap<String, String>,
}

/// `PodTemplateSpec` describes the data a pod should have when
/// created from a template.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodTemplateSpec {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "is_empty_pod_spec")]
    pub spec: PodSpec,
}

fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
fn is_empty_pod_spec(s: &PodSpec) -> bool {
    s == &PodSpec::default()
}

/// `DeploymentStatus` is the most recently observed status of the Deployment.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentStatus {
    /// `ObservedGeneration` — the generation observed by the deployment controller.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Total number of non-terminated pods targeted by this deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Total number of non-terminated pods with the desired template spec.
    #[serde(default, rename = "updatedReplicas", skip_serializing_if = "Option::is_none")]
    pub updated_replicas: Option<i32>,

    /// Total number of ready pods.
    #[serde(default, rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,

    /// Total number of available pods (ready for at least minReadySeconds).
    #[serde(default, rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,

    /// Total number of unavailable pods.
    #[serde(default, rename = "unavailableReplicas", skip_serializing_if = "Option::is_none")]
    pub unavailable_replicas: Option<i32>,

    /// Current conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DeploymentCondition>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentCondition {
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
    fn deployment_spec_with_replicas_and_template_round_trips() {
        let mut spec = DeploymentSpec {
            replicas: Some(2),
            ..Default::default()
        };
        spec.selector
            .match_labels
            .insert("app".into(), "podinfo".into());
        spec.template.metadata.name = "podinfo".into();
        spec.template.spec.containers.push(Container {
            name: "podinfod".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ..Default::default()
        });
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"replicas\":2"), "got: {json}");
        assert!(json.contains("\"matchLabels\""), "got: {json}");
        let back: DeploymentSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn deployment_status_with_conditions_round_trips() {
        let status = DeploymentStatus {
            observed_generation: Some(1),
            replicas: Some(2),
            ready_replicas: Some(2),
            available_replicas: Some(2),
            conditions: vec![DeploymentCondition {
                r#type: "Available".into(),
                status: "True".into(),
                reason: Some("MinimumReplicasAvailable".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let s = serde_json::to_string(&status).unwrap();
        assert!(s.contains("\"observedGeneration\":1"), "got: {s}");
        assert!(s.contains("\"readyReplicas\":2"), "got: {s}");
        let back: DeploymentStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(back, status);
    }
}
