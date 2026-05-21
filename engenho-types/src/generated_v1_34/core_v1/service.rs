//! `Service` — M0.0.2 typed expansion #2.
//!
//! Promotes Service.spec / Service.status from opaque
//! `serde_json::Value` to typed `ServiceSpec` / `ServiceStatus`.
//! Same pattern as Pod's M0.0.2 bullseye.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::service_spec::{ServiceSpec, ServiceStatus};

/// `Service` is a named abstraction of software service consisting
/// of local port(s) and the selector determining which pods will
/// answer requests sent through the proxy.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Service {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: ServiceSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ServiceStatus>,
}

impl KubeResource for Service {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Service",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "services",
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
fn is_empty_spec(s: &ServiceSpec) -> bool {
    s == &ServiceSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::service_spec::{ServicePort, ServiceType};

    #[test]
    fn service_round_trips_with_typed_spec() {
        let mut svc = Service::default();
        svc.metadata.name = "podinfo".into();
        svc.metadata.namespace = Some("default".into());
        svc.spec.r#type = Some(ServiceType::ClusterIP);
        svc.spec.cluster_ip = Some("10.43.0.42".into());
        svc.spec.selector.insert("app".into(), "podinfo".into());
        svc.spec.ports.push(ServicePort {
            name: Some("http".into()),
            port: 9898,
            target_port: Some(serde_json::json!("http")),
            protocol: Some("TCP".into()),
            ..Default::default()
        });
        let json = serde_json::to_string(&svc).unwrap();
        assert!(json.contains("\"ClusterIP\""), "got: {json}");
        assert!(json.contains("\"clusterIP\":\"10.43.0.42\""));
        let back: Service = serde_json::from_str(&json).unwrap();
        assert_eq!(back, svc);
    }

    #[test]
    fn service_gvk_is_core_v1_service() {
        assert_eq!(Service::GVK.kind, "Service");
        assert_eq!(Service::GVR.resource, "services");
        assert_eq!(Service::SCOPE, Scope::Namespaced);
    }
}
