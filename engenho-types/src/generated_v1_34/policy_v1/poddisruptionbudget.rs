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

/// PodDisruptionBudget is an object to define the max disruption that can be caused to a collection of pods
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodDisruptionBudget {
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// Specification of the desired behavior of the PodDisruptionBudget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<PodDisruptionBudgetSpec>,
    /// Most recently observed status of the PodDisruptionBudget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PodDisruptionBudgetStatus>,
}

impl KubeResource for PodDisruptionBudget {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "policy",
        version: "v1",
        kind: "PodDisruptionBudget",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "policy",
        version: "v1",
        resource: "poddisruptionbudgets",
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
