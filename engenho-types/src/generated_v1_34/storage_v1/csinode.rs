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

/// CSINode holds information about all CSI drivers installed on a node. CSI drivers do not need to create the CSINode object directly. As long as they use the node-driver-registrar sidecar container, the kubelet will automatically populate the CSINode object for the CSI driver as part of kubelet plugin registration. CSINode has the same name as a node. If the object is missing, it means either there are no CSI Drivers available on the node, or the Kubelet version is low enough that it doesn't create this object. CSINode has an OwnerReference that points to the corresponding node object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CSINode {
    /// Standard object's metadata. metadata.name must be the Kubernetes node name.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// spec is the specification of CSINode
    #[serde(default)]
    pub spec: CSINodeSpec,
}

impl KubeResource for CSINode {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "storage.k8s.io",
version: "v1",
kind:    "CSINode",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "storage.k8s.io",
version:  "v1",
resource: "csinodes",
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
