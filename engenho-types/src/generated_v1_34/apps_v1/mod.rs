//! GENERATED — `apps_v1` typed kinds. Source: engenho-kube-codegen.

mod deployment;
mod replicaset;
mod statefulset;
mod daemonset;
mod controllerrevision;

pub use deployment::Deployment;
pub use replicaset::ReplicaSet;
pub use statefulset::StatefulSet;
pub use daemonset::DaemonSet;
pub use controllerrevision::ControllerRevision;

pub use crate::generated_v1_34::types::*;
