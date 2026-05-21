//! GENERATED — `core_v1` typed kinds. Source: engenho-kube-codegen.
//!
//! Note: M0.0.2 promoted Pod's spec + status from opaque
//! `serde_json::Value` to typed `PodSpec` / `PodStatus`. The typed
//! shapes live in the sibling `pod_spec` module (hand-authored
//! ahead of codegen catching up at M0.0.3).

mod pod;
mod pod_spec;
mod service;
mod service_spec;
mod configmap;
mod secret;
mod namespace;
mod serviceaccount;
mod node;
mod persistentvolume;
mod persistentvolumeclaim;
mod endpoints;

pub use pod::Pod;
pub use pod_spec::{
    Container, ContainerPort, ContainerStatus, EnvVar, PodCondition, PodPhase,
    PodSpec, PodStatus,
};
pub use service::Service;
pub use service_spec::{
    LoadBalancerIngress, LoadBalancerStatus, ServiceCondition, ServicePort,
    ServiceSpec, ServiceStatus, ServiceType,
};
pub use configmap::ConfigMap;
pub use secret::Secret;
pub use namespace::Namespace;
pub use serviceaccount::ServiceAccount;
pub use node::Node;
pub use persistentvolume::PersistentVolume;
pub use persistentvolumeclaim::PersistentVolumeClaim;
pub use endpoints::Endpoints;
