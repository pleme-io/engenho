//! `Endpoints` — M0.0.2 typed expansion #10.
//!
//! Wire-different from Pod/Service: no spec/status; just a
//! `subsets[]` of EndpointSubset (addresses + ports). The legacy
//! `core/v1` shape; EndpointSlice (discovery.k8s.io/v1) is the
//! modern replacement. Kept here for kubectl-default compat + the
//! upstream conformance pass at M0.4.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Endpoints {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    /// `Subsets` is the set of endpoint groups, each carrying
    /// addresses + ports + readiness state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subsets: Vec<EndpointSubset>,
}

/// `EndpointSubset` — addresses with a common set of ports.
/// Addresses split into Ready / NotReady.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointSubset {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<EndpointAddress>,
    #[serde(default, rename = "notReadyAddresses", skip_serializing_if = "Vec::is_empty")]
    pub not_ready_addresses: Vec<EndpointAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<EndpointPort>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointAddress {
    pub ip: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default, rename = "targetRef", skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<ObjectReference>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointPort {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(default, rename = "appProtocol", skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
}

/// Shared cross-kind object reference. Lives here because
/// EndpointAddress is its first consumer; future kinds (Events,
/// OwnerReferences) reuse the same shape.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ObjectReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(default, rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(default, rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(default, rename = "fieldPath", skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
}

impl KubeResource for Endpoints {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Endpoints",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "endpoints",
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
    fn endpoints_round_trips_with_addresses_and_ports() {
        let mut e = Endpoints::default();
        e.metadata.name = "podinfo".into();
        e.metadata.namespace = Some("default".into());
        e.subsets.push(EndpointSubset {
            addresses: vec![EndpointAddress {
                ip: "10.42.0.11".into(),
                target_ref: Some(ObjectReference {
                    kind: Some("Pod".into()),
                    namespace: Some("default".into()),
                    name: Some("podinfo-8df8b84cd-6lb4k".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ports: vec![EndpointPort {
                name: Some("http".into()),
                port: 9898,
                protocol: Some("TCP".into()),
                ..Default::default()
            }],
            ..Default::default()
        });
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"10.42.0.11\""));
        assert!(!json.contains("\"spec\""));
        assert!(!json.contains("\"status\""));
        let back: Endpoints = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn endpoints_gvk_is_core_v1() {
        assert_eq!(Endpoints::GVK.kind, "Endpoints");
        assert_eq!(Endpoints::GVR.resource, "endpoints");
        assert_eq!(Endpoints::SCOPE, Scope::Namespaced);
    }
}
