//! StoreBackedNodeResolver — `NodeResolver` impl that reads
//! cluster nodes from engenho-store.
//!
//! Resolves abstract `JobTarget`s (AnyOne/AnyK/AllNodes) into
//! concrete `NodeId`s by listing `Node` resources from a configured
//! namespace. Each Node resource's name carries a hex NodeId.
//!
//! ## Wire shape
//!
//! Each cluster node registers as:
//!
//!   group: ""
//!   version: v1
//!   kind: Node
//!   name: {64-char hex NodeId}
//!   namespace: configurable (default: engenho-system) — or
//!     None for cluster-scoped lookups
//!
//! ## Composition
//!
//! Drop-in alternative to `StaticNodeResolver`. Operators wire
//! this into `PlantioController` when the live cluster directory
//! is the source of truth.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::StoreMesh;
use engenho_substrate::{JobTarget, NodeId};

use crate::error::ControllerError;
use crate::plantio::NodeResolver;

/// Resolver backed by Node resources in engenho-store.
pub struct StoreBackedNodeResolver {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl StoreBackedNodeResolver {
    /// New resolver reading Node resources cluster-wide.
    #[must_use]
    pub fn cluster_wide(store: Arc<StoreMesh>) -> Self {
        Self {
            store,
            namespace: None,
        }
    }

    /// New resolver scoped to a namespace.
    #[must_use]
    pub fn with_namespace(store: Arc<StoreMesh>, namespace: String) -> Self {
        Self {
            store,
            namespace: Some(namespace),
        }
    }

    /// Parse a NodeId from a Node resource's name (64-char lowercase hex).
    /// Returns None for any other format — those resources are skipped.
    #[must_use]
    pub fn parse_node_name(name: &str) -> Option<NodeId> {
        if name.len() != 64 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&name[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(NodeId::new(bytes))
    }

    /// Load every available NodeId from the store. Pure helper —
    /// exposed for tests that want to inspect the resolver's view.
    pub async fn load_all(&self) -> Result<Vec<NodeId>, ControllerError> {
        let resources = self
            .store
            .list("", "v1", "Node", self.namespace.as_deref())
            .await;
        let mut nodes: Vec<NodeId> = resources
            .into_iter()
            .filter_map(|(k, _)| Self::parse_node_name(&k.name))
            .collect();
        nodes.sort();
        Ok(nodes)
    }
}

#[async_trait]
impl NodeResolver for StoreBackedNodeResolver {
    fn name(&self) -> &'static str {
        "store-backed"
    }

    async fn resolve(&self, target: &JobTarget) -> Result<Vec<NodeId>, ControllerError> {
        let all = self.load_all().await?;
        let resolved = match target {
            JobTarget::Node(n) => {
                if all.contains(n) {
                    vec![*n]
                } else {
                    // Pin still honored even if the node isn't in
                    // the directory — the operator may know
                    // better than the cache.
                    vec![*n]
                }
            }
            JobTarget::AnyOne => all.iter().take(1).copied().collect(),
            JobTarget::AnyK { k } => all.iter().take(*k).copied().collect(),
            JobTarget::Quorum { k } => all.iter().take(*k).copied().collect(),
            JobTarget::AllNodes => all,
        };
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_name_accepts_64_lowercase_hex() {
        let n = StoreBackedNodeResolver::parse_node_name(&"a".repeat(64)).unwrap();
        assert_eq!(n.0[0], 0xaa);
        assert_eq!(n.0[31], 0xaa);
    }

    #[test]
    fn parse_node_name_rejects_wrong_length() {
        assert!(StoreBackedNodeResolver::parse_node_name("abc").is_none());
        assert!(StoreBackedNodeResolver::parse_node_name(&"a".repeat(63)).is_none());
        assert!(StoreBackedNodeResolver::parse_node_name(&"a".repeat(65)).is_none());
    }

    #[test]
    fn parse_node_name_rejects_non_hex() {
        let mut bad = String::from("g");
        bad.push_str(&"a".repeat(63));
        assert!(StoreBackedNodeResolver::parse_node_name(&bad).is_none());
    }

    #[test]
    fn parse_node_name_round_trips_via_to_hex() {
        let n = NodeId::from_bytes(b"engenho");
        let back = StoreBackedNodeResolver::parse_node_name(&n.to_hex()).unwrap();
        assert_eq!(back, n);
    }

    #[test]
    fn parse_node_name_accepts_mixed_hex_digits() {
        let n = StoreBackedNodeResolver::parse_node_name(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        assert_eq!(n.0[0], 0x01);
        assert_eq!(n.0[1], 0x23);
        assert_eq!(n.0[31], 0xef);
    }
}
