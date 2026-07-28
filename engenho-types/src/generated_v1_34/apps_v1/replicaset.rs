//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::generated_v1_34::types::*;
use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// ReplicaSet ensures that a specified number of pod replicas are running at any given time.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSet {
    /// If the Labels of a ReplicaSet are empty, they are defaulted to be the same as the Pod(s) that the ReplicaSet manages. Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// Spec defines the specification of the desired behavior of the ReplicaSet. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ReplicaSetSpec>,
    /// Status is the most recently observed status of the ReplicaSet. This data may be out of date by some window of time. Populated by the system. Read-only. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ReplicaSetStatus>,
}

impl KubeResource for ReplicaSet {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "apps",
        version: "v1",
        kind: "ReplicaSet",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "apps",
        version: "v1",
        resource: "replicasets",
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
