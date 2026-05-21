//! `ResourceKind` — closed enum of resources engenho-mcp can list
//! through the typed engenho-types + engenho-kube-client stack.
//!
//! The substrate-compounding contract: adding a kind here is the
//! ONLY change required to make a new resource MCP-listable. The
//! dispatcher in `reader::kikai::list_resource` matches on the
//! enum + calls the typed `client.list::<R>()`. No new tools, no
//! new trait methods, no new views beyond the typed engenho-types
//! catalog itself.
//!
//! Adding a new kind requires:
//!   1. New variant here (one line + serde rename if needed).
//!   2. Match arm in `kikai::list_resource_dispatch` (3 lines).
//!   3. (engenho-types) typed expansion of the resource if it
//!      currently has opaque `serde_json::Value` spec/status.
//!
//! That's the entire surface.

use rmcp::schemars;
use serde::{Deserialize, Serialize};

/// Closed catalog of resource kinds engenho-mcp exposes for listing.
/// Each variant maps 1:1 to a `KubeResource` impl in engenho-types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// core/v1 Pod
    Pod,
    /// core/v1 Service
    Service,
    /// core/v1 ConfigMap
    ConfigMap,
    /// apps/v1 Deployment
    Deployment,
}

impl ResourceKind {
    /// Stable string identifier used in MCP error payloads and
    /// telemetry. Pairs with serde_rename for the wire shape.
    pub fn label(self) -> &'static str {
        match self {
            Self::Pod => "pod",
            Self::Service => "service",
            Self::ConfigMap => "config_map",
            Self::Deployment => "deployment",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_kind_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&ResourceKind::Pod).unwrap(), "\"pod\"");
        assert_eq!(
            serde_json::to_string(&ResourceKind::ConfigMap).unwrap(),
            "\"config_map\""
        );
        assert_eq!(
            serde_json::to_string(&ResourceKind::Deployment).unwrap(),
            "\"deployment\""
        );
    }

    #[test]
    fn resource_kind_round_trips() {
        for k in [
            ResourceKind::Pod,
            ResourceKind::Service,
            ResourceKind::ConfigMap,
            ResourceKind::Deployment,
        ] {
            let s = serde_json::to_string(&k).unwrap();
            let back: ResourceKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
    }
}
