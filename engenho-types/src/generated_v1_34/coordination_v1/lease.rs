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
/// Lease defines a lease concept.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    /// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// spec contains the specification of the Lease. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<LeaseSpec>,
}
impl KubeResource for Lease {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "coordination.k8s.io",
        version: "v1",
        kind: "Lease",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "coordination.k8s.io",
        version: "v1",
        resource: "leases",
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
