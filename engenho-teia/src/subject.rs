//! Typed NATS subject builder.
//!
//! Every engenho subject is constructed from typed primitives —
//! never raw `format!()`-string concatenation. The grammar:
//!
//! ```text
//! engenho.{cluster}.raft.{group}.append.{target_node}
//! engenho.{cluster}.raft.{group}.vote.{target_node}
//! engenho.{cluster}.raft.{group}.snapshot.{target_node}
//! engenho.{cluster}.watch.{group_version_kind}.{namespace}.{name}
//! engenho.{cluster}.content.{bucket}
//! engenho.{cluster}.attestation.{node_id}.{index}
//! engenho.{cluster}.health.{service}
//! ```
//!
//! `ClusterScope` carries the `{cluster}` segment; methods on it
//! build full subjects. Match against [`Subject`] for typed dispatch
//! on the consumer side.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Cluster-scoped subject builder. Construct once at startup +
/// reuse for all publishes/subscribes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterScope {
    cluster: String,
}

impl ClusterScope {
    /// Construct from a validated cluster name. Use
    /// [`crate::TeiaConfig::validate`] to validate.
    #[must_use]
    pub fn new(cluster: impl Into<String>) -> Self {
        Self {
            cluster: cluster.into(),
        }
    }

    /// The `engenho.{cluster}` prefix for all subjects under this scope.
    #[must_use]
    pub fn prefix(&self) -> String {
        format!("engenho.{}", self.cluster)
    }

    /// Subject for an AppendEntries RPC to `target_node`.
    #[must_use]
    pub fn raft_append(&self, group: RaftGroup, target_node: NodeId) -> String {
        format!(
            "engenho.{}.raft.{}.append.{}",
            self.cluster,
            group.as_str(),
            target_node.0
        )
    }

    /// Subject for a RequestVote RPC to `target_node`.
    #[must_use]
    pub fn raft_vote(&self, group: RaftGroup, target_node: NodeId) -> String {
        format!(
            "engenho.{}.raft.{}.vote.{}",
            self.cluster,
            group.as_str(),
            target_node.0
        )
    }

    /// Subject for an InstallSnapshot RPC to `target_node`.
    #[must_use]
    pub fn raft_snapshot(&self, group: RaftGroup, target_node: NodeId) -> String {
        format!(
            "engenho.{}.raft.{}.snapshot.{}",
            self.cluster,
            group.as_str(),
            target_node.0
        )
    }

    /// Wildcard subscription to ALL Raft RPCs for `target_node`
    /// (any RPC kind, any group). Useful for the per-node RPC
    /// handler task.
    #[must_use]
    pub fn raft_target_wildcard(&self, target_node: NodeId) -> String {
        format!("engenho.{}.raft.*.*.{}", self.cluster, target_node.0)
    }

    /// Subject for a watch event on a specific resource.
    /// `gvk` is encoded as `group_version_kind` (dots replaced by
    /// underscores to keep the subject hierarchy clean).
    #[must_use]
    pub fn watch_resource(&self, gvk: &Gvk, namespace: Option<&str>, name: &str) -> String {
        let ns = namespace.unwrap_or("_cluster_scoped");
        format!(
            "engenho.{}.watch.{}.{}.{}",
            self.cluster,
            gvk.subject_segment(),
            ns,
            name
        )
    }

    /// Wildcard subscription to all watch events for a GVK.
    #[must_use]
    pub fn watch_gvk_wildcard(&self, gvk: &Gvk) -> String {
        format!("engenho.{}.watch.{}.>", self.cluster, gvk.subject_segment())
    }

    /// Wildcard subscription to watch events across all GVKs.
    /// Used by the observability + audit layers.
    #[must_use]
    pub fn watch_all_wildcard(&self) -> String {
        format!("engenho.{}.watch.>", self.cluster)
    }

    /// Subject prefix for a content-bucket. The actual object key
    /// inside the bucket is BLAKE3-encoded.
    #[must_use]
    pub fn content_bucket(&self, bucket: &str) -> String {
        format!("engenho.{}.content.{}", self.cluster, bucket)
    }

    /// Subject for an attestation block from `node_id` at Raft
    /// log index `index`.
    #[must_use]
    pub fn attestation(&self, node_id: NodeId, index: u64) -> String {
        format!(
            "engenho.{}.attestation.{}.{}",
            self.cluster, node_id.0, index
        )
    }

    /// Wildcard subscription to ALL attestation blocks from any
    /// node. Used by the auditor's multi-witness aggregator.
    #[must_use]
    pub fn attestation_wildcard(&self) -> String {
        format!("engenho.{}.attestation.>", self.cluster)
    }

    /// Subject for a service health heartbeat.
    #[must_use]
    pub fn health(&self, service: &str) -> String {
        format!("engenho.{}.health.{}", self.cluster, service)
    }

    /// Wildcard subscription to all health subjects (Vector consumes this).
    #[must_use]
    pub fn health_wildcard(&self) -> String {
        format!("engenho.{}.health.>", self.cluster)
    }
}

/// Typed identifier for which Raft group a subject belongs to.
/// Multiple Raft groups coexist in an engenho cluster (revoada
/// for role assignments; store for K8s resources); subjects must
/// disambiguate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftGroup {
    /// engenho-revoada role-assignment Raft group.
    Revoada,
    /// engenho-store K8s resource Raft group.
    Store,
}

impl RaftGroup {
    fn as_str(self) -> &'static str {
        match self {
            Self::Revoada => "revoada",
            Self::Store => "store",
        }
    }
}

/// Typed NodeId — the 32-byte ed25519 public key, wire-encoded
/// as hex on the subject (so subject tokens stay alphanumeric).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    /// Construct from raw ed25519 public key bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        let mut hex = String::with_capacity(64);
        for b in bytes {
            hex.push_str(&format!("{b:02x}"));
        }
        Self(hex)
    }

    /// Construct from a hex string (already encoded by the caller).
    #[must_use]
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Typed Group/Version/Kind. Subject segment is
/// `group_version_kind` with empty group rendered as `core`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

impl Gvk {
    #[must_use]
    pub fn new(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        }
    }

    /// Core/v1 helper. Convention: empty group ⇒ "core".
    #[must_use]
    pub fn core(version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self::new("", version, kind)
    }

    /// Subject segment representation: `{group}_{version}_{kind}`.
    /// Empty group is encoded as `core` so subjects can't have
    /// double-underscore at the start.
    #[must_use]
    pub fn subject_segment(&self) -> String {
        let group = if self.group.is_empty() {
            "core"
        } else {
            self.group.as_str()
        };
        // Replace dots in group (e.g. "rbac.authorization.k8s.io") with
        // dashes so subject token boundaries stay clean.
        let sanitized_group = group.replace('.', "-");
        format!("{}_{}_{}", sanitized_group, self.version, self.kind)
    }
}

/// Closed-enum decoding of an arbitrary engenho subject string.
/// Used by message handlers to dispatch on subject shape rather
/// than re-parsing every time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subject {
    /// `engenho.{cluster}.raft.{group}.{rpc_kind}.{target_node}`
    Raft {
        cluster: String,
        group: RaftGroup,
        rpc_kind: RaftRpcKind,
        target_node: NodeId,
    },
    /// `engenho.{cluster}.watch.{gvk}.{namespace}.{name}`
    Watch {
        cluster: String,
        gvk_segment: String,
        namespace: String,
        name: String,
    },
    /// `engenho.{cluster}.content.{bucket}`
    Content { cluster: String, bucket: String },
    /// `engenho.{cluster}.attestation.{node}.{index}`
    Attestation {
        cluster: String,
        node_id: NodeId,
        index: u64,
    },
    /// `engenho.{cluster}.health.{service}`
    Health { cluster: String, service: String },
    /// Anything else — fallthrough for forward-compat.
    Other(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftRpcKind {
    Append,
    Vote,
    Snapshot,
}

impl Subject {
    /// Parse a subject string into the typed enum. Returns
    /// `Subject::Other(s)` for anything that doesn't match the
    /// engenho grammar — forward-compat for future channels.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() < 3 || parts[0] != "engenho" {
            return Self::Other(s.to_string());
        }
        let cluster = parts[1].to_string();
        match parts[2] {
            "raft" if parts.len() >= 6 => {
                let group = match parts[3] {
                    "revoada" => RaftGroup::Revoada,
                    "store" => RaftGroup::Store,
                    _ => return Self::Other(s.to_string()),
                };
                let rpc_kind = match parts[4] {
                    "append" => RaftRpcKind::Append,
                    "vote" => RaftRpcKind::Vote,
                    "snapshot" => RaftRpcKind::Snapshot,
                    _ => return Self::Other(s.to_string()),
                };
                Self::Raft {
                    cluster,
                    group,
                    rpc_kind,
                    target_node: NodeId::from_hex(parts[5]),
                }
            }
            "watch" if parts.len() >= 6 => Self::Watch {
                cluster,
                gvk_segment: parts[3].to_string(),
                namespace: parts[4].to_string(),
                name: parts[5..].join("."),
            },
            "content" if parts.len() == 4 => Self::Content {
                cluster,
                bucket: parts[3].to_string(),
            },
            "attestation" if parts.len() == 5 => {
                let Ok(index) = parts[4].parse::<u64>() else {
                    return Self::Other(s.to_string());
                };
                Self::Attestation {
                    cluster,
                    node_id: NodeId::from_hex(parts[3]),
                    index,
                }
            }
            "health" if parts.len() == 4 => Self::Health {
                cluster,
                service: parts[3].to_string(),
            },
            _ => Self::Other(s.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_append_subject_format() {
        let scope = ClusterScope::new("engenho-local");
        let node = NodeId::from_bytes([0xab; 32]);
        let s = scope.raft_append(RaftGroup::Revoada, node);
        assert_eq!(
            s,
            "engenho.engenho-local.raft.revoada.append.\
             abababababababababababababababababababababababababababababababab"
        );
    }

    #[test]
    fn raft_target_wildcard_format() {
        let scope = ClusterScope::new("engenho-local");
        let node = NodeId::from_hex("ff");
        assert_eq!(
            scope.raft_target_wildcard(node),
            "engenho.engenho-local.raft.*.*.ff"
        );
    }

    #[test]
    fn watch_subject_format_namespaced() {
        let scope = ClusterScope::new("c1");
        let gvk = Gvk::core("v1", "Pod");
        let s = scope.watch_resource(&gvk, Some("default"), "podinfo");
        assert_eq!(s, "engenho.c1.watch.core_v1_Pod.default.podinfo");
    }

    #[test]
    fn watch_subject_format_cluster_scoped() {
        let scope = ClusterScope::new("c1");
        let gvk = Gvk::core("v1", "Namespace");
        let s = scope.watch_resource(&gvk, None, "default");
        assert_eq!(
            s,
            "engenho.c1.watch.core_v1_Namespace._cluster_scoped.default"
        );
    }

    #[test]
    fn gvk_segment_sanitizes_dots_in_group() {
        let gvk = Gvk::new("rbac.authorization.k8s.io", "v1", "Role");
        assert_eq!(gvk.subject_segment(), "rbac-authorization-k8s-io_v1_Role");
    }

    #[test]
    fn watch_all_wildcard_format() {
        let scope = ClusterScope::new("c1");
        assert_eq!(scope.watch_all_wildcard(), "engenho.c1.watch.>");
    }

    #[test]
    fn content_bucket_format() {
        let scope = ClusterScope::new("c1");
        assert_eq!(scope.content_bucket("images"), "engenho.c1.content.images");
    }

    #[test]
    fn attestation_subject_format() {
        let scope = ClusterScope::new("c1");
        let node = NodeId::from_hex("aabb");
        assert_eq!(
            scope.attestation(node, 42),
            "engenho.c1.attestation.aabb.42"
        );
    }

    #[test]
    fn attestation_wildcard_format() {
        let scope = ClusterScope::new("c1");
        assert_eq!(scope.attestation_wildcard(), "engenho.c1.attestation.>");
    }

    #[test]
    fn health_subject_format() {
        let scope = ClusterScope::new("c1");
        assert_eq!(scope.health("store"), "engenho.c1.health.store");
        assert_eq!(scope.health_wildcard(), "engenho.c1.health.>");
    }

    // === Parsing round-trips ===

    #[test]
    fn parses_raft_append() {
        let scope = ClusterScope::new("c1");
        let node = NodeId::from_hex("cafe");
        let built = scope.raft_append(RaftGroup::Store, node.clone());
        let parsed = Subject::parse(&built);
        assert_eq!(
            parsed,
            Subject::Raft {
                cluster: "c1".into(),
                group: RaftGroup::Store,
                rpc_kind: RaftRpcKind::Append,
                target_node: node,
            }
        );
    }

    #[test]
    fn parses_attestation() {
        let parsed = Subject::parse("engenho.c1.attestation.aa.7");
        assert_eq!(
            parsed,
            Subject::Attestation {
                cluster: "c1".into(),
                node_id: NodeId::from_hex("aa"),
                index: 7,
            }
        );
    }

    #[test]
    fn parses_watch_with_namespaced_resource() {
        let parsed = Subject::parse("engenho.c1.watch.core_v1_Pod.default.podinfo");
        assert_eq!(
            parsed,
            Subject::Watch {
                cluster: "c1".into(),
                gvk_segment: "core_v1_Pod".into(),
                namespace: "default".into(),
                name: "podinfo".into(),
            }
        );
    }

    #[test]
    fn unknown_subject_falls_through_to_other() {
        let s = "engenho.c1.weird.shape";
        let parsed = Subject::parse(s);
        assert_eq!(parsed, Subject::Other(s.to_string()));
    }

    #[test]
    fn malformed_attestation_index_falls_through() {
        let s = "engenho.c1.attestation.aa.not_a_number";
        let parsed = Subject::parse(s);
        assert_eq!(parsed, Subject::Other(s.to_string()));
    }

    #[test]
    fn non_engenho_prefix_falls_through() {
        let s = "foo.bar.baz";
        let parsed = Subject::parse(s);
        assert_eq!(parsed, Subject::Other(s.to_string()));
    }

    #[test]
    fn node_id_from_bytes_is_hex_lowercase() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x0a;
        bytes[1] = 0xbc;
        bytes[2] = 0xff;
        let id = NodeId::from_bytes(bytes);
        assert_eq!(id.0.len(), 64);
        assert!(id.0.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(id.0.starts_with("0abcff"));
    }
}
