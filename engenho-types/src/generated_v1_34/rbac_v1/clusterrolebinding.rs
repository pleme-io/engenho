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

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// ClusterRoleBinding references a ClusterRole, but not contain it.  It can reference a ClusterRole in the global namespace, and adds who information via Subject.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterRoleBinding {
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

impl KubeResource for ClusterRoleBinding {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "ClusterRoleBinding",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "clusterrolebindings",
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
