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
/// PersistentVolumeClaim is a user's request for and claim to a persistent volume
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaim {
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// spec defines the desired characteristics of a volume requested by a pod author. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<PersistentVolumeClaimSpec>,
    /// status represents the current information/status of a persistent volume claim. Read-only. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PersistentVolumeClaimStatus>,
}
impl KubeResource for PersistentVolumeClaim {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "PersistentVolumeClaim",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "persistentvolumeclaims",
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
