//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;
use crate::generated_v1_34::types::*;

/// StorageClass describes the parameters for a class of storage for which PersistentVolumes can be dynamically provisioned.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageClass {
    /// allowVolumeExpansion shows whether the storage class allow volume expand.
    #[serde(default, rename = "allowVolumeExpansion", skip_serializing_if = "Option::is_none")]
    pub allow_volume_expansion: Option<bool>,
    /// allowedTopologies restrict the node topologies where volumes can be dynamically provisioned. Each volume plugin defines its own supported topology specifications. An empty TopologySelectorTerm list means there is no topology restriction. This field is only honored by servers that enable the VolumeScheduling feature.
    #[serde(default, rename = "allowedTopologies", skip_serializing_if = "Vec::is_empty")]
    pub allowed_topologies: Vec<TopologySelectorTerm>,
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// mountOptions controls the mountOptions for dynamically provisioned PersistentVolumes of this storage class. e.g. ["ro", "soft"]. Not validated - mount of the PVs will simply fail if one is invalid.
    #[serde(default, rename = "mountOptions", skip_serializing_if = "Vec::is_empty")]
    pub mount_options: Vec<String>,
    /// parameters holds the parameters for the provisioner that should create volumes of this storage class.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub parameters: std::collections::BTreeMap<String, String>,
    /// provisioner indicates the type of the provisioner.
    #[serde(default)]
    pub provisioner: String,
    /// reclaimPolicy controls the reclaimPolicy for dynamically provisioned PersistentVolumes of this storage class. Defaults to Delete.
    #[serde(default, rename = "reclaimPolicy", skip_serializing_if = "Option::is_none")]
    pub reclaim_policy: Option<String>,
    /// volumeBindingMode indicates how PersistentVolumeClaims should be provisioned and bound.  When unset, VolumeBindingImmediate is used. This field is only honored by servers that enable the VolumeScheduling feature.
    #[serde(default, rename = "volumeBindingMode", skip_serializing_if = "Option::is_none")]
    pub volume_binding_mode: Option<String>,
}

impl KubeResource for StorageClass {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "storage.k8s.io",
version: "v1",
kind:    "StorageClass",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "storage.k8s.io",
version:  "v1",
resource: "storageclasses",
};
const SCOPE: Scope = Scope::Cluster;

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

fn is_empty_meta(m: &ObjectMeta) -> bool { m == &ObjectMeta::default() }
