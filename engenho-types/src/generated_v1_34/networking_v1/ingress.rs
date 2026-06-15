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

/// Ingress is a collection of rules that allow inbound connections to reach the endpoints defined by a backend. An Ingress can be configured to give services externally-reachable urls, load balance traffic, terminate SSL, offer name based virtual hosting etc.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Ingress {
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// spec is the desired state of the Ingress. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<IngressSpec>,
    /// status is the current state of the Ingress. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<IngressStatus>,
}

impl KubeResource for Ingress {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "networking.k8s.io",
version: "v1",
kind:    "Ingress",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "networking.k8s.io",
version:  "v1",
resource: "ingresses",
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
