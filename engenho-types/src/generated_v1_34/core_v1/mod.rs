//! GENERATED — `core_v1` typed kinds. Source: engenho-kube-codegen.

mod configmap;
mod endpoints;
mod event;
mod limitrange;
mod namespace;
mod node;
mod persistentvolume;
mod persistentvolumeclaim;
mod pod;
mod podtemplate;
mod replicationcontroller;
mod resourcequota;
mod secret;
mod service;
mod serviceaccount;

pub use configmap::ConfigMap;
pub use endpoints::Endpoints;
pub use event::Event;
pub use limitrange::LimitRange;
pub use namespace::Namespace;
pub use node::Node;
pub use persistentvolume::PersistentVolume;
pub use persistentvolumeclaim::PersistentVolumeClaim;
pub use pod::Pod;
pub use podtemplate::PodTemplate;
pub use replicationcontroller::ReplicationController;
pub use resourcequota::ResourceQuota;
pub use secret::Secret;
pub use service::Service;
pub use serviceaccount::ServiceAccount;

pub use crate::generated_v1_34::types::*;
