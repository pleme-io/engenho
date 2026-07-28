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
// The CANONICAL RBAC primitives (carry the `kind` discriminator the codegen
// `types::{RoleRef,Subject}` drop).
use super::policy::{RoleRef, Subject};

/// RoleBinding references a role, but does not contain it.  It can reference a Role in the same namespace or a ClusterRole in the global namespace. It adds who information via Subjects and namespace information by which namespace it exists in.  RoleBindings in a given namespace only have effect in that namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoleBinding {
    /// Standard object's metadata.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// RoleRef can reference a Role in the current namespace or a ClusterRole in the global namespace. If the RoleRef cannot be resolved, the Authorizer must return an error. This field is immutable.
    #[serde(default, rename = "roleRef")]
    pub role_ref: RoleRef,
    /// Subjects holds references to the objects the role applies to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<Subject>,
}

impl KubeResource for RoleBinding {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "RoleBinding",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "rbac.authorization.k8s.io",
        version: "v1",
        resource: "rolebindings",
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
