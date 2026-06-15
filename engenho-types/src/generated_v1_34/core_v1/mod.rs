//! GENERATED — `core_v1` typed kinds. Source: engenho-kube-codegen.

mod pod;
mod service;
mod configmap;
mod secret;
mod namespace;
mod serviceaccount;
mod node;
mod persistentvolume;
mod persistentvolumeclaim;
mod endpoints;
mod replicationcontroller;
mod podtemplate;
mod limitrange;
mod resourcequota;
mod event;

pub use pod::Pod;
pub use service::Service;
pub use configmap::ConfigMap;
pub use secret::Secret;
pub use namespace::Namespace;
pub use serviceaccount::ServiceAccount;
pub use node::Node;
pub use persistentvolume::PersistentVolume;
pub use persistentvolumeclaim::PersistentVolumeClaim;
pub use endpoints::Endpoints;
pub use replicationcontroller::ReplicationController;
pub use podtemplate::PodTemplate;
pub use limitrange::LimitRange;
pub use resourcequota::ResourceQuota;
pub use event::Event;

pub use crate::generated_v1_34::types::*;
