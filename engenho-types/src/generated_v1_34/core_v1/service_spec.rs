//! Typed `ServiceSpec` + `ServiceStatus` — M0.0.2 typed expansion #2.
//!
//! Same pattern as `pod_spec`: hand-author the bullseye now, the
//! M0.0.3 codegen reproduces byte-for-byte later. Scope-disciplined —
//! every field corresponds to one in `vendor/openapi/v1.34.0/
//! api__v1_openapi.json` under `io.k8s.api.core.v1.{ServiceSpec,
//! ServiceStatus}`.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `ServiceSpec` describes the attributes that a user creates on a service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceSpec {
    /// `Type` determines how the Service is exposed. Defaults to
    /// `ClusterIP`. Valid options: `ExternalName` | `ClusterIP` |
    /// `NodePort` | `LoadBalancer`.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<ServiceType>,

    /// `Selector` is a key→value map for selecting the pods backing
    /// the service. Required for ClusterIP/NodePort/LoadBalancer
    /// services; ignored for ExternalName.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub selector: BTreeMap<String, String>,

    /// List of ports exposed by this service.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ServicePort>,

    /// `ClusterIP` is the IP address of the service, usually
    /// assigned randomly. If set must not be in use; if empty
    /// allocated dynamically.
    #[serde(default, rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,

    /// List of IP addresses assigned to this service.
    #[serde(default, rename = "clusterIPs", skip_serializing_if = "Vec::is_empty")]
    pub cluster_ips: Vec<String>,

    /// `SessionAffinity` — `ClientIP` | `None`. Default `None`.
    #[serde(default, rename = "sessionAffinity", skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<String>,

    /// `ExternalName` — DNS CNAME target. Required when
    /// `type=ExternalName`.
    #[serde(default, rename = "externalName", skip_serializing_if = "Option::is_none")]
    pub external_name: Option<String>,

    /// `IPFamilies` — list of address families. Valid values:
    /// `IPv4`, `IPv6`.
    #[serde(default, rename = "ipFamilies", skip_serializing_if = "Vec::is_empty")]
    pub ip_families: Vec<String>,
}

/// `ServiceType` — closed enum of the four valid Service types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceType {
    ClusterIP,
    NodePort,
    LoadBalancer,
    ExternalName,
}

/// `ServicePort` represents a port that's exposed by this service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServicePort {
    /// The name of this port within the service. Required if more
    /// than one port is defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The port that will be exposed by this service.
    pub port: i32,

    /// `TargetPort` — number or name of the port to access on the
    /// pods targeted by the service. Encoded as either integer or
    /// string in JSON (IntOrString); we accept both.
    #[serde(default, rename = "targetPort", skip_serializing_if = "Option::is_none")]
    pub target_port: Option<serde_json::Value>,

    /// The `IP` protocol for this port. Default `TCP`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,

    /// The port on each node on which this service is exposed when
    /// `type=NodePort` or `LoadBalancer`. Default 0.
    #[serde(default, rename = "nodePort", skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
}

/// `ServiceStatus` represents the current status of a service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatus {
    /// `LoadBalancer` contains the current status of the
    /// load-balancer if one is present.
    #[serde(default, rename = "loadBalancer", skip_serializing_if = "Option::is_none")]
    pub load_balancer: Option<LoadBalancerStatus>,

    /// Current service state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ServiceCondition>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadBalancerStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ingress: Vec<LoadBalancerIngress>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadBalancerIngress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_type_round_trips() {
        for t in [
            ServiceType::ClusterIP,
            ServiceType::NodePort,
            ServiceType::LoadBalancer,
            ServiceType::ExternalName,
        ] {
            let s = serde_json::to_string(&t).unwrap();
            let back: ServiceType = serde_json::from_str(&s).unwrap();
            assert_eq!(back, t);
        }
        assert_eq!(serde_json::to_string(&ServiceType::ClusterIP).unwrap(), "\"ClusterIP\"");
    }

    #[test]
    fn service_spec_with_ports_round_trips() {
        let mut spec = ServiceSpec {
            r#type: Some(ServiceType::ClusterIP),
            cluster_ip: Some("10.43.0.1".into()),
            ports: vec![ServicePort {
                name: Some("http".into()),
                port: 80,
                target_port: Some(serde_json::json!(9898)),
                protocol: Some("TCP".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        spec.selector.insert("app".into(), "podinfo".into());
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"type\":\"ClusterIP\""), "got: {s}");
        assert!(s.contains("\"clusterIP\":\"10.43.0.1\""), "got: {s}");
        assert!(s.contains("\"targetPort\":9898"), "got: {s}");
        let back: ServiceSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn parses_kubernetes_default_service() {
        // The cluster's default `kubernetes` service shape, captured
        // from `kubectl get service kubernetes -o json` on a stock
        // k3s cluster.
        let real = r#"{
            "type": "ClusterIP",
            "clusterIP": "10.43.0.1",
            "clusterIPs": ["10.43.0.1"],
            "ipFamilies": ["IPv4"],
            "ports": [
                {"name": "https", "port": 443, "protocol": "TCP", "targetPort": 6443}
            ],
            "sessionAffinity": "None"
        }"#;
        let spec: ServiceSpec = serde_json::from_str(real).unwrap();
        assert_eq!(spec.r#type, Some(ServiceType::ClusterIP));
        assert_eq!(spec.cluster_ip.as_deref(), Some("10.43.0.1"));
        assert_eq!(spec.ports.len(), 1);
        assert_eq!(spec.ports[0].port, 443);
    }

    #[test]
    fn empty_service_spec_serializes_to_empty_object() {
        let s = serde_json::to_string(&ServiceSpec::default()).unwrap();
        assert_eq!(s, "{}");
    }

    use proptest::prelude::*;

    /// Property-based round-trip: any randomly generated ServiceSpec
    /// with port + selector survives JSON identity.
    proptest! {
        #[test]
        fn arb_service_spec_round_trips(
            port in 1i32..65535,
            port_name in "[a-z][a-z0-9-]{0,14}",
            selector_key in "[a-z][a-z0-9-]{0,14}",
            selector_value in "[a-z0-9][a-z0-9-]{0,30}",
            kind_idx in 0usize..4,
            cluster_ip in "10\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}",
        ) {
            let kinds = [
                ServiceType::ClusterIP,
                ServiceType::NodePort,
                ServiceType::LoadBalancer,
                ServiceType::ExternalName,
            ];
            let mut spec = ServiceSpec {
                r#type: Some(kinds[kind_idx]),
                cluster_ip: Some(cluster_ip.clone()),
                ports: vec![ServicePort {
                    name: Some(port_name.clone()),
                    port,
                    protocol: Some("TCP".into()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            spec.selector.insert(selector_key.clone(), selector_value.clone());
            let json = serde_json::to_string(&spec).unwrap();
            let back: ServiceSpec = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(back, spec);
        }
    }
}
