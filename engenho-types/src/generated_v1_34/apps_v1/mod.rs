//! GENERATED — `apps_v1` typed kinds. Source: engenho-kube-codegen.
//!
//! Note: M0.0.2 promoted Deployment's spec + status from opaque
//! `serde_json::Value` to typed `DeploymentSpec` / `DeploymentStatus`.
//! The typed shapes live in `deployment_spec` and reuse PodSpec
//! from `core_v1::pod_spec`.

mod deployment;
mod deployment_spec;
mod replicaset;
mod replicaset_spec;
mod statefulset;
mod daemonset;

pub use deployment::Deployment;
pub use deployment_spec::{
    DeploymentCondition, DeploymentSpec, DeploymentStatus, LabelSelector, PodTemplateSpec,
};
pub use replicaset::ReplicaSet;
pub use replicaset_spec::{ReplicaSetCondition, ReplicaSetSpec, ReplicaSetStatus};
pub use statefulset::StatefulSet;
pub use daemonset::DaemonSet;
