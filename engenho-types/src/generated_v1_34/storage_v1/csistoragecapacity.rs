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

/// CSIStorageCapacity stores the result of one CSI GetCapacity call. For a given StorageClass, this describes the available capacity in a particular topology segment.  This can be used when considering where to instantiate new PersistentVolumes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CSIStorageCapacity {
    /// capacity is the value reported by the CSI driver in its GetCapacityResponse for a GetCapacityRequest with topology and parameters that match the previous fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Quantity>,
    /// maximumVolumeSize is the value reported by the CSI driver in its GetCapacityResponse for a GetCapacityRequest with topology and parameters that match the previous fields.
    #[serde(default, rename = "maximumVolumeSize", skip_serializing_if = "Option::is_none")]
    pub maximum_volume_size: Option<Quantity>,
    /// Standard object's metadata. The name has no particular meaning. It must be a DNS subdomain (dots allowed, 253 characters). To ensure that there are no conflicts with other CSI drivers on the cluster, the recommendation is to use csisc-<uuid>, a generated name, or a reverse-domain name which ends with the unique CSI driver name.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// nodeTopology defines which nodes have access to the storage for which capacity was reported. If not set, the storage is not accessible from any node in the cluster. If empty, the storage is accessible from all nodes. This field is immutable.
    #[serde(default, rename = "nodeTopology", skip_serializing_if = "Option::is_none")]
    pub node_topology: Option<LabelSelector>,
    /// storageClassName represents the name of the StorageClass that the reported capacity applies to. It must meet the same requirements as the name of a StorageClass object (non-empty, DNS subdomain). If that object no longer exists, the CSIStorageCapacity object is obsolete and should be removed by its creator. This field is immutable.
    #[serde(default, rename = "storageClassName")]
    pub storage_class_name: String,
}

impl KubeResource for CSIStorageCapacity {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "storage.k8s.io",
version: "v1",
kind:    "CSIStorageCapacity",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "storage.k8s.io",
version:  "v1",
resource: "csistoragecapacities",
};
const SCOPE: Scope = Scope::Namespaced;

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
