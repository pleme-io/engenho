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

/// Endpoints is a collection of endpoints that implement the actual service. Example:
/// 
/// 	 Name: "mysvc",
/// 	 Subsets: [
/// 	   {
/// 	     Addresses: [{"ip": "10.10.1.1"}, {"ip": "10.10.2.2"}],
/// 	     Ports: [{"name": "a", "port": 8675}, {"name": "b", "port": 309}]
/// 	   },
/// 	   {
/// 	     Addresses: [{"ip": "10.10.3.3"}],
/// 	     Ports: [{"name": "a", "port": 93}, {"name": "b", "port": 76}]
/// 	   },
/// 	]
/// 
/// Endpoints is a legacy API and does not contain information about all Service features. Use discoveryv1.EndpointSlice for complete information about Service endpoints.
/// 
/// Deprecated: This API is deprecated in v1.33+. Use discoveryv1.EndpointSlice.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Endpoints {
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

impl KubeResource for Endpoints {
const GVK: GroupVersionKind = GroupVersionKind {
group:   "",
version: "v1",
kind:    "Endpoints",
};
const GVR: GroupVersionResource = GroupVersionResource {
group:    "",
version:  "v1",
resource: "endpoints",
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
