//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.
#![allow(clippy::module_name_repetitions)]
use crate::generated_v1_34::types::*;
use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
/// StatefulSet represents a set of pods with consistent identities. Identities are defined as:
/// - Network: A single stable DNS and hostname.
/// - Storage: As many VolumeClaims as requested.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSet {
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// Spec defines the desired identities of pods in this set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<StatefulSetSpec>,
    /// Status is the current status of Pods in this StatefulSet. This data may be out of date by some window of time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<StatefulSetStatus>,
}
impl KubeResource for StatefulSet {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "apps",
        version: "v1",
        kind: "StatefulSet",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "apps",
        version: "v1",
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
fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
