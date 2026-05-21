//! `Node` — M0.0.2 typed expansion #7. Cluster-scoped.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::node_spec::{NodeSpec, NodeStatus};

/// `Node` is a worker node in Kubernetes. Each node has a unique
/// identifier in the cluster's etcd store.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: NodeSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<NodeStatus>,
}

impl KubeResource for Node {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Node",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "nodes",
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
fn is_empty_spec(s: &NodeSpec) -> bool {
    s == &NodeSpec::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::node_spec::{NodeAddress, NodeCondition, NodeSystemInfo};
    use crate::primitives::Quantity;
    use std::str::FromStr;

    #[test]
    fn node_round_trips_with_typed_status() {
        let mut n = Node::default();
        n.metadata.name = "engenho-local".into();
        n.spec.pod_cidr = Some("10.42.0.0/24".into());
        let mut status = NodeStatus::default();
        status
            .capacity
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
            kubelet_version: "v1.34.5+k3s1".into(),
            architecture: "arm64".into(),
            ..Default::default()
        });
        n.status = Some(status);
        let json = serde_json::to_string(&n).unwrap();
        assert!(json.contains("\"engenho-local\""));
        assert!(json.contains("\"podCIDR\""));
        assert!(json.contains("\"InternalIP\""));
        let back: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(back, n);
    }

    #[test]
    fn node_is_cluster_scoped() {
        assert_eq!(Node::SCOPE, Scope::Cluster);
        assert_eq!(Node::GVK.kind, "Node");
        assert_eq!(Node::GVR.resource, "nodes");
    }
}
