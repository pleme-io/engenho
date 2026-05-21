//! `ReplicaSet` — M0.0.2 typed expansion #9 (apps/v1).
//!
//! Promotes ReplicaSet.spec / .status to typed
//! `ReplicaSetSpec` / `ReplicaSetStatus`. Reuses
//! `LabelSelector` + `PodTemplateSpec` from `deployment_spec`.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::replicaset_spec::{ReplicaSetSpec, ReplicaSetStatus};

/// `ReplicaSet` ensures that a specified number of pod replicas
/// are running at any given time.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSet {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: ReplicaSetSpec,

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
fn is_empty_spec(s: &ReplicaSetSpec) -> bool {
    s == &ReplicaSetSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated_v1_34::core_v1::Container;

    #[test]
    fn replicaset_round_trips_with_typed_spec() {
        let mut rs = ReplicaSet::default();
        rs.metadata.name = "podinfo-8df8b84cd".into();
        rs.metadata.namespace = Some("default".into());
        rs.spec.replicas = Some(2);
        rs.spec
            .selector
            .match_labels
            .insert("app".into(), "podinfo".into());
        rs.spec.template.spec.containers.push(Container {
            name: "podinfod".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ..Default::default()
        });
        let json = serde_json::to_string(&rs).unwrap();
        assert!(json.contains("\"podinfod\""));
        let back: ReplicaSet = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rs);
    }

    #[test]
    fn replicaset_gvk_is_apps_v1() {
        assert_eq!(ReplicaSet::GVK.group, "apps");
        assert_eq!(ReplicaSet::GVK.kind, "ReplicaSet");
        assert_eq!(ReplicaSet::GVR.resource, "replicasets");
        assert_eq!(ReplicaSet::SCOPE, Scope::Namespaced);
    }
}
