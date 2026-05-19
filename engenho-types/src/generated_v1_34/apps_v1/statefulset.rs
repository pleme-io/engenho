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

/// StatefulSet represents a set of pods with consistent identities. Identities are defined as:
/// - Network: A single stable DNS and hostname.
/// - Storage: As many VolumeClaims as requested.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSet {
/// Standard object metadata.
#[serde(default, skip_serializing_if = "is_empty_meta")]
pub metadata: ObjectMeta,
/// Spec (typed expansion is M0.0.4; today opaque JSON).
#[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
pub spec: serde_json::Value,
/// Status (typed expansion is M0.0.4; today opaque JSON).
#[serde(default, skip_serializing_if = "Option::is_none")]
pub status: Option<serde_json::Value>,
}

impl KubeResource for StatefulSet {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "apps",
version: "v1",
kind:    "StatefulSet",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "apps",
version:  "v1",
resource: "statefulsets",
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
