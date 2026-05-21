//! Typed mutation surface — symmetric partner to [`ClusterReader`].
//!
//! Every mutating operation against an engenho-managed cluster
//! routes through this trait. The catalog dispatch shape mirrors
//! the reader exactly: closed `ResourceKind` enum → match arm →
//! typed `KubeClient` call. Adding a new mutating capability is
//! the same bounded cost as adding a new reader capability.
//!
//! # Authority gating
//!
//! Every write takes an [`Authority`] reference. At M0.0.2 only
//! [`Authority::Placeholder`] exists — and it ALWAYS rejects.
//! That's a deliberate substrate gate: the trait + impl + tests
//! are ready, but no path from MCP wire reaches a successful write
//! until the saguão passport variant lands at P2.
//!
//! When P2 ships:
//!   1. New variant `Authority::SaguaoPassport { ref: SecretRef }`
//!   2. `Authority::can_write()` returns true for it
//!   3. The `engenho-mcp` server registers `cluster_resource_apply`
//!      + `cluster_resource_delete` MCP tools that accept a
//!      passport reference and route through this trait.
//!
//! Until then, the trait + impl exist for internal callers
//! (engenho-apiserver's M0.1 control plane, future engenho-controllers)
//! that already operate inside the cluster's trust boundary.

use async_trait::async_trait;

use crate::resource_kind::ResourceKind;

pub mod kikai;

#[cfg(test)]
pub mod mock;

/// Typed authority capsule. Every [`ClusterWriter`] method takes
/// one. The only variant that grants write permission is
/// `Authority::SaguaoPassport` (which lands at P2); until then,
/// the writer always rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// **Deny-all placeholder.** Lives in the substrate until
    /// saguão passport materialization lands at P2. Every
    /// `ClusterWriter` method given this rejects with
    /// [`WriterError::AuthorityRequired`].
    Placeholder,
    // Future:
    // SaguaoPassport(saguao::Passport),
}

impl Authority {
    /// Whether this authority is currently sufficient for write
    /// operations. False for every variant defined today — flip
    /// to true on the saguão variant when it lands.
    #[must_use]
    pub fn can_write(&self) -> bool {
        matches!(self, _placeholder if false)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("unknown cluster: {0}")]
    UnknownCluster(String),

    #[error("authority required for write — current authority cannot mutate (waiting on saguão passport materialization at P2)")]
    AuthorityRequired,

    #[error("io error during write of {what}: {source}")]
    Io {
        what: String,
        #[source]
        source: std::io::Error,
    },

    #[error("cluster API call failed for {what}: {detail}")]
    Api { what: String, detail: String },

    #[error("invalid write request: {0}")]
    Invalid(String),
}

impl WriterError {
    /// Stable identifier for telemetry + MCP error payloads.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "unknown_cluster",
            Self::AuthorityRequired => "authority_required",
            Self::Io { .. } => "io",
            Self::Api { .. } => "api",
            Self::Invalid(_) => "invalid",
        }
    }
}

#[async_trait]
pub trait ClusterWriter: Send + Sync {
    /// Server-Side Apply a typed resource body. The body is a
    /// `serde_json::Value` so the trait stays object-safe; inside
    /// the impl, the caller's responsibility is to give a body
    /// that deserializes into `R` where R matches `kind`.
    ///
    /// `field_manager` is the SSA owner — engenho-mcp uses
    /// `engenho-mcp` as the canonical value but operator-side
    /// callers can pass their own.
    async fn apply_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        field_manager: &str,
        force: bool,
        authority: &Authority,
    ) -> Result<serde_json::Value, WriterError>;

    /// Delete a resource by name.
    async fn delete_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        authority: &Authority,
    ) -> Result<(), WriterError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_error_kind_is_stable() {
        let cases = [
            (WriterError::UnknownCluster("x".into()), "unknown_cluster"),
            (WriterError::AuthorityRequired, "authority_required"),
            (
                WriterError::Io {
                    what: "x".into(),
                    source: std::io::Error::other("boom"),
                },
                "io",
            ),
            (
                WriterError::Api {
                    what: "x".into(),
                    detail: "y".into(),
                },
                "api",
            ),
            (WriterError::Invalid("x".into()), "invalid"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.kind(), expected);
        }
    }

    #[test]
    fn placeholder_authority_always_denies() {
        assert!(!Authority::Placeholder.can_write());
    }

    #[test]
    fn writer_trait_is_object_safe() {
        // Compile-time proof: Arc<dyn ClusterWriter> must work.
        fn assert_object_safe(_: &dyn ClusterWriter) {}
        struct Stub;
        #[async_trait]
        impl ClusterWriter for Stub {
            async fn apply_resource(
                &self,
                _: &str,
                _: ResourceKind,
                _: &str,
                _: &str,
                _: serde_json::Value,
                _: &str,
                _: bool,
                _: &Authority,
            ) -> Result<serde_json::Value, WriterError> {
                unimplemented!()
            }
            async fn delete_resource(
                &self,
                _: &str,
                _: ResourceKind,
                _: &str,
                _: &str,
                _: &Authority,
            ) -> Result<(), WriterError> {
                unimplemented!()
            }
        }
        assert_object_safe(&Stub);
    }
}
