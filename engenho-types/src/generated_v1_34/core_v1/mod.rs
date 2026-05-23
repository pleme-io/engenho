//! GENERATED — `core_v1` typed kinds. Source: engenho-kube-codegen.
//!
//! Note: M0.0.2 promoted Pod's spec + status from opaque
//! `serde_json::Value` to typed `PodSpec` / `PodStatus`. The typed
//! shapes live in the sibling `pod_spec` module (hand-authored
//! ahead of codegen catching up at M0.0.3).

mod configmap;
mod endpoints;
mod namespace;
mod namespace_spec;
mod node;
mod node_spec;
mod persistentvolume;
mod persistentvolumeclaim;
mod pod;
mod pod_spec;
mod pvc_spec;
mod secret;
mod service;
mod service_spec;
mod serviceaccount;

pub use configmap::ConfigMap;
pub use endpoints::{EndpointAddress, EndpointPort, EndpointSubset, Endpoints, ObjectReference};
pub use namespace::Namespace;
pub use namespace_spec::{NamespaceCondition, NamespacePhase, NamespaceSpec, NamespaceStatus};
pub use node::Node;
pub use node_spec::{NodeAddress, NodeCondition, NodeSpec, NodeStatus, NodeSystemInfo, Taint};
pub use persistentvolume::PersistentVolume;
pub use persistentvolumeclaim::PersistentVolumeClaim;
pub use pod::Pod;
pub use pod_spec::{
    Container, ContainerPort, ContainerStatus, EnvVar, PodCondition, PodPhase, PodSpec, PodStatus,
};
pub use pvc_spec::{
    PersistentVolumeClaimCondition, PersistentVolumeClaimSpec, PersistentVolumeClaimStatus,
    PvcPhase, ResourceRequirements,
};
pub use secret::{KnownSecretType, Secret, SecretType};
pub use service::Service;
pub use service_spec::{
    LoadBalancerIngress, LoadBalancerStatus, ServiceCondition, ServicePort, ServiceSpec,
    ServiceStatus, ServiceType,
};
pub use serviceaccount::{LocalObjectReference, ServiceAccount};
