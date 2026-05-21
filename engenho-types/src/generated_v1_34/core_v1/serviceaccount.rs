//! `ServiceAccount` — M0.0.2 typed expansion #8.
//!
//! Wire-different from Pod/Service kinds: ServiceAccount has NO
//! spec/status. The interesting fields (secrets, imagePullSecrets,
//! automountServiceAccountToken) sit at the top level.
//!
//! Secret refs here are LocalObjectReference (just `name`); the
//! actual Secret contents are managed by the Secret kind + routed
//! through `engenho-mcp::redaction::SecretView` at the boundary.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// `ServiceAccount` binds together (a) a name understood by users
/// and peripheral systems for an identity, (b) a principal that
/// can be authenticated and authorized, and (c) a set of secret
/// references the apiserver can mount into pods.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceAccount {
    /// Standard object metadata.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    /// References to Secret objects (in the same namespace) used
    /// by pods running with this ServiceAccount.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<LocalObjectReference>,

    /// References to Secrets used for pulling images from private
    /// registries.
    #[serde(default, rename = "imagePullSecrets", skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,

    /// `AutomountServiceAccountToken` — opt out of API token auto-
    /// mount on pods. `None` defaults to true at the apiserver.
    #[serde(
        default,
        rename = "automountServiceAccountToken",
        skip_serializing_if = "Option::is_none"
    )]
    pub automount_service_account_token: Option<bool>,
}

/// Reference to an object by name within the same namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LocalObjectReference {
    pub name: String,
}

impl KubeResource for ServiceAccount {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "ServiceAccount",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "serviceaccounts",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serviceaccount_round_trips_with_secrets() {
        let mut sa = ServiceAccount::default();
        sa.metadata.name = "default".into();
        sa.metadata.namespace = Some("kube-system".into());
        sa.secrets.push(LocalObjectReference {
            name: "default-token-xyz".into(),
        });
        sa.image_pull_secrets.push(LocalObjectReference {
            name: "regcred".into(),
        });
        sa.automount_service_account_token = Some(true);
        let json = serde_json::to_string(&sa).unwrap();
        // Wire-clean: no spec/status leaked.
        assert!(!json.contains("\"spec\""), "ServiceAccount must not emit spec: {json}");
        assert!(!json.contains("\"status\""), "ServiceAccount must not emit status: {json}");
        assert!(json.contains("\"imagePullSecrets\""));
        let back: ServiceAccount = serde_json::from_str(&json).unwrap();
        assert_eq!(back, sa);
    }

    #[test]
    fn serviceaccount_gvk_is_core_v1() {
        assert_eq!(ServiceAccount::GVK.kind, "ServiceAccount");
        assert_eq!(ServiceAccount::GVR.resource, "serviceaccounts");
        assert_eq!(ServiceAccount::SCOPE, Scope::Namespaced);
    }

    #[test]
    fn empty_serviceaccount_serializes_minimally() {
        let s = serde_json::to_string(&ServiceAccount::default()).unwrap();
        assert_eq!(s, "{}");
    }
}
