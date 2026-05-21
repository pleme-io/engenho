//! `Deployment` — M0.0.2 typed expansion #4 (apps/v1).
//!
//! Promotes Deployment.spec / Deployment.status from opaque
//! `serde_json::Value` to typed `DeploymentSpec` / `DeploymentStatus`
//! sourced from sibling `deployment_spec` module.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::deployment_spec::{DeploymentSpec, DeploymentStatus};

/// `Deployment` enables declarative updates for Pods and ReplicaSets.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Deployment {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: DeploymentSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}

impl KubeResource for Deployment {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "apps",
        version: "v1",
        kind: "Deployment",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "apps",
        version: "v1",
        resource: "deployments",
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
fn is_empty_spec(s: &DeploymentSpec) -> bool {
    s == &DeploymentSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated_v1_34::core_v1::Container;

    #[test]
    fn deployment_round_trips_with_typed_spec_and_status() {
        let mut d = Deployment::default();
        d.metadata.name = "podinfo".into();
        d.metadata.namespace = Some("default".into());
        d.spec.replicas = Some(2);
        d.spec
            .selector
            .match_labels
            .insert("app".into(), "podinfo".into());
        d.spec.template.spec.containers.push(Container {
            name: "podinfod".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ..Default::default()
        });
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("\"replicas\":2"), "got: {json}");
        assert!(json.contains("\"podinfod\""), "got: {json}");
        let back: Deployment = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn deployment_gvk_is_apps_v1() {
        assert_eq!(Deployment::GVK.group, "apps");
        assert_eq!(Deployment::GVK.kind, "Deployment");
        assert_eq!(Deployment::GVR.resource, "deployments");
        assert_eq!(Deployment::SCOPE, Scope::Namespaced);
    }
}
