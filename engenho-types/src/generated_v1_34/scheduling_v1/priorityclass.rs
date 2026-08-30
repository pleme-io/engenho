//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.
#![allow(clippy::module_name_repetitions)]
use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
/// PriorityClass defines mapping from a priority class name to the priority integer value. The value can be any valid integer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PriorityClass {
    /// description is an arbitrary string that usually provides guidelines on when this priority class should be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// globalDefault specifies whether this PriorityClass should be considered as the default priority for pods that do not have any priority class. Only one PriorityClass can be marked as `globalDefault`. However, if more than one PriorityClasses exists with their `globalDefault` field set to true, the smallest value of such global default PriorityClasses will be used as the default priority.
    #[serde(
        default,
        rename = "globalDefault",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_default: Option<bool>,
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// preemptionPolicy is the Policy for preempting pods with lower priority. One of Never, PreemptLowerPriority. Defaults to PreemptLowerPriority if unset.
    #[serde(
        default,
        rename = "preemptionPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub preemption_policy: Option<String>,
    /// value represents the integer value of this priority class. This is the actual priority that pods receive when they have the name of this class in their pod spec.
    #[serde(default)]
    pub value: i32,
}
impl KubeResource for PriorityClass {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "scheduling.k8s.io",
        version: "v1",
        kind: "PriorityClass",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "scheduling.k8s.io",
        version: "v1",
        resource: "priorityclasses",
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
fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
