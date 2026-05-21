//! Typed `NodeSpec` + `NodeStatus` — M0.0.2 #7.
//!
//! Cluster-scoped. Second cluster-scoped expansion in the catalog
//! (after Namespace), validating the `is_cluster_scoped` dispatch
//! generalization beyond a single one-off variant.
//!
//! Scope discipline: ships the fields engenho-local's k3s node
//! actually populates (podCIDR, taints, conditions, capacity,
//! allocatable, addresses, nodeInfo). Deferred to M0.0.3 codegen:
//! configSource, volumesAttached, runtime-handler list, and the
//! long tail of provider-specific fields.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::primitives::Quantity;

/// `NodeSpec` describes the attributes that a node is created with.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    /// `PodCIDR` represents the pod IP range assigned to the node.
    #[serde(default, rename = "podCIDR", skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,

    /// `PodCIDRs` represents the IP ranges (v4 and/or v6) assigned.
    #[serde(default, rename = "podCIDRs", skip_serializing_if = "Vec::is_empty")]
    pub pod_cidrs: Vec<String>,

    /// `ProviderID` — typed identifier assigned by the cloud
    /// provider (`<ProviderName>://<ProviderSpecificNodeID>`).
    #[serde(default, rename = "providerID", skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,

    /// `Unschedulable` — if true, the node should NOT be considered
    /// for scheduling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unschedulable: Option<bool>,

    /// Taints — typed list rather than opaque.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub taints: Vec<Taint>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Taint {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Effect: `NoSchedule` | `PreferNoSchedule` | `NoExecute`.
    pub effect: String,
    #[serde(default, rename = "timeAdded", skip_serializing_if = "Option::is_none")]
    pub time_added: Option<String>,
}

/// `NodeStatus` is information about the current status of a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// `Capacity` — total resources of the node.
    /// Keys: "cpu", "memory", "pods", "ephemeral-storage", plus
    /// extended resources. Values are typed `Quantity` (wire shape
    /// still string, e.g. "4", "8Gi"; parser at the engenho-types
    /// boundary so consumers see typed numerics).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capacity: BTreeMap<String, Quantity>,

    /// `Allocatable` — resources allocatable to pods (capacity
    /// minus system reservations).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub allocatable: BTreeMap<String, Quantity>,

    /// Conditions describe the current state of the node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<NodeCondition>,

    /// Addresses where the node is reachable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<NodeAddress>,

    /// Identifying info about the node — OS, kernel, container
    /// runtime versions, kubelet version, machine ID, etc.
    #[serde(default, rename = "nodeInfo", skip_serializing_if = "Option::is_none")]
    pub node_info: Option<NodeSystemInfo>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    #[serde(default, rename = "lastHeartbeatTime", skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_time: Option<String>,
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeAddress {
    /// Address type. One of: `Hostname` | `ExternalIP` |
    /// `InternalIP` | `ExternalDNS` | `InternalDNS`.
    #[serde(rename = "type")]
    pub r#type: String,
    pub address: String,
}

/// Identifying info reported by the kubelet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSystemInfo {
    #[serde(default, rename = "machineID", skip_serializing_if = "String::is_empty")]
    pub machine_id: String,
    #[serde(default, rename = "systemUUID", skip_serializing_if = "String::is_empty")]
    pub system_uuid: String,
    #[serde(default, rename = "bootID", skip_serializing_if = "String::is_empty")]
    pub boot_id: String,
    #[serde(default, rename = "kernelVersion", skip_serializing_if = "String::is_empty")]
    pub kernel_version: String,
    #[serde(default, rename = "osImage", skip_serializing_if = "String::is_empty")]
    pub os_image: String,
    #[serde(default, rename = "containerRuntimeVersion", skip_serializing_if = "String::is_empty")]
    pub container_runtime_version: String,
    #[serde(default, rename = "kubeletVersion", skip_serializing_if = "String::is_empty")]
    pub kubelet_version: String,
    #[serde(default, rename = "kubeProxyVersion", skip_serializing_if = "String::is_empty")]
    pub kube_proxy_version: String,
    #[serde(default, rename = "operatingSystem", skip_serializing_if = "String::is_empty")]
    pub operating_system: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub architecture: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Random NodeSpec with taints + pod_cidrs round-trips
        /// through JSON identity.
        #[test]
        fn arb_node_spec_round_trips(
            cidr_octet in 0u8..32,
            taint_key in "[a-z][a-z0-9-]{0,30}",
            taint_value in "[a-z0-9-]{0,30}",
            effect_idx in 0usize..3,
            unsched in any::<bool>(),
        ) {
            let effects = ["NoSchedule", "PreferNoSchedule", "NoExecute"];
            let mut spec = NodeSpec {
                pod_cidr: Some(format!("10.42.{cidr_octet}.0/24")),
                pod_cidrs: vec![format!("10.42.{cidr_octet}.0/24")],
                provider_id: Some("k3s://engenho-local".into()),
                unschedulable: Some(unsched),
                ..Default::default()
            };
            spec.taints.push(Taint {
                key: taint_key,
                value: Some(taint_value),
                effect: effects[effect_idx].into(),
                ..Default::default()
            });
            let s = serde_json::to_string(&spec).unwrap();
            let back: NodeSpec = serde_json::from_str(&s).unwrap();
            prop_assert_eq!(back, spec);
        }
    }


    #[test]
    fn node_spec_round_trips_with_pod_cidrs_and_taints() {
        let mut spec = NodeSpec {
            pod_cidr: Some("10.42.0.0/24".into()),
            pod_cidrs: vec!["10.42.0.0/24".into()],
            provider_id: Some("k3s://engenho-local".into()),
            ..Default::default()
        };
        spec.taints.push(Taint {
            key: "node-role.kubernetes.io/master".into(),
            value: Some("".into()),
            effect: "NoSchedule".into(),
            ..Default::default()
        });
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"podCIDR\":\"10.42.0.0/24\""), "got: {s}");
        assert!(s.contains("\"providerID\""), "got: {s}");
        let back: NodeSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn node_status_round_trips_with_capacity_and_conditions() {
        use std::str::FromStr;
        let mut status = NodeStatus::default();
        status
            .capacity
            .insert("cpu".into(), Quantity::from_str("4").unwrap());
        status
            .capacity
            .insert("memory".into(), Quantity::from_str("8Gi").unwrap());
        status
            .allocatable
            .insert("cpu".into(), Quantity::from_str("4").unwrap());
        status.conditions.push(NodeCondition {
            r#type: "Ready".into(),
            status: "True".into(),
            ..Default::default()
        });
        status.addresses.push(NodeAddress {
            r#type: "InternalIP".into(),
            address: "192.168.64.10".into(),
        });
        status.node_info = Some(NodeSystemInfo {
            os_image: "NixOS 25.11".into(),
            kubelet_version: "v1.34.5+k3s1".into(),
            architecture: "arm64".into(),
            ..Default::default()
        });
        let s = serde_json::to_string(&status).unwrap();
        assert!(s.contains("\"nodeInfo\""), "got: {s}");
        assert!(s.contains("\"kubeletVersion\":\"v1.34.5+k3s1\""), "got: {s}");
        let back: NodeStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(back, status);
    }
}
