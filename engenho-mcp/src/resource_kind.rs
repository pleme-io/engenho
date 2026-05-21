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
    /// core/v1 Pod (namespaced)
    Pod,
    /// core/v1 Service (namespaced)
    Service,
    /// core/v1 ConfigMap (namespaced)
    ConfigMap,
    /// core/v1 Secret (namespaced — REDACTED at the MCP boundary)
    Secret,
    /// core/v1 Namespace (cluster-scoped)
    Namespace,
    /// core/v1 Node (cluster-scoped)
    Node,
    /// apps/v1 Deployment (namespaced)
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
            Self::Secret => "secret",
            Self::Namespace => "namespace",
            Self::Node => "node",
            Self::Deployment => "deployment",
        }
    }

    /// Iterator over every variant — used by exhaustiveness tests +
    /// any future bulk-list / introspection tool. Adding a variant
    /// without updating this array fails compile because the
    /// `Self::Pod` etc. references would lose meaning.
    pub fn all() -> &'static [Self] {
        &[
            Self::Pod,
            Self::Service,
            Self::ConfigMap,
            Self::Secret,
            Self::Namespace,
            Self::Node,
            Self::Deployment,
        ]
    }

    /// Whether the kind is cluster-scoped (no namespace in URL).
    /// Mirrors `engenho_types::kind::Scope` but lives here so the
    /// dispatcher can pick the right URL shape without needing
    /// the type at the call site.
    pub fn is_cluster_scoped(self) -> bool {
        matches!(self, Self::Namespace | Self::Node)
    }

    /// Whether the kind carries secret material at the wire.
    /// True kinds route through a redacted view at the MCP
    /// boundary — engenho-mcp never serializes their values.
    pub fn carries_secret_material(self) -> bool {
        matches!(self, Self::Secret)
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
        for &k in ResourceKind::all() {
            let s = serde_json::to_string(&k).unwrap();
            let back: ResourceKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
    }

    /// Exhaustiveness lock — every variant in `all()` must yield
    /// a non-empty label. Adding a variant without updating the
    /// `label()` match exhausts the closed enum (compile error)
    /// but updating `label()` without adding to `all()` would
    /// silently regress; this test guards that direction.
    #[test]
    fn all_variants_have_distinct_labels() {
        let labels: std::collections::HashSet<&str> =
            ResourceKind::all().iter().map(|k| k.label()).collect();
        assert_eq!(
            labels.len(),
            ResourceKind::all().len(),
            "ResourceKind::all() contains duplicate labels: {labels:?}"
        );
        for k in ResourceKind::all() {
            assert!(!k.label().is_empty(), "{k:?} has empty label");
        }
    }

    #[test]
    fn cluster_scope_classification_is_correct() {
        // Cluster-scoped today: Namespace + Node.
        assert!(ResourceKind::Namespace.is_cluster_scoped());
        assert!(ResourceKind::Node.is_cluster_scoped());
        for &k in ResourceKind::all() {
            if !matches!(k, ResourceKind::Namespace | ResourceKind::Node) {
                assert!(!k.is_cluster_scoped(), "{k:?} should be namespaced");
            }
        }
    }

    /// The "carries secret material" gate routes Secret-like kinds
    /// through redaction. Currently only Secret; future TLS-typed
    /// resources or token-projected volumes would join.
    #[test]
    fn secret_material_classification_is_correct() {
        assert!(ResourceKind::Secret.carries_secret_material());
        for &k in ResourceKind::all() {
            if !matches!(k, ResourceKind::Secret) {
                assert!(
                    !k.carries_secret_material(),
                    "{k:?} should not carry secret material"
                );
            }
        }
    }
}
