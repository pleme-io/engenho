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
//! The server holds one [`Authority`], decided at launch. It answers two
//! different questions, and deliberately not the same way:
//!
//! * **The control plane** ([`crate::control`]): [`Authority::control_ceiling`]
//!   caps the control operations offered as tools — observe always, mutate
//!   with `--allow-mutate` — and the daemon enforces the same cap on its
//!   own and audits every call.
//! * **Kubernetes writes** (this trait): [`Authority::can_write`] is false for
//!   every variant defined today. Nothing on this path has a tier or an
//!   audit record, so the launch flag that grants control-plane mutation
//!   does not reach it. The variant that will is the saguão passport
//!   (dormant): `Authority::SaguaoPassport`, `can_write` true for it, and
//!   `cluster_resource_apply` / `cluster_resource_delete` tools routed
//!   through this trait.
//!
//! Until then, the trait + impl exist for internal callers
//! (engenho-apiserver's M0.1 control plane, future engenho-controllers)
//! that already operate inside the cluster's trust boundary.

use async_trait::async_trait;

use crate::resource_kind::ResourceKind;

pub mod kikai;

#[cfg(test)]
pub mod mock;

/// What the server may do, decided at launch. See the module docs for the
/// two questions it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// Read only: the default. Observe control tools only; every
    /// `ClusterWriter` method rejects with [`WriterError::AuthorityRequired`].
    Observe,
    /// Control-plane mutation on this machine's engenho, granted by the
    /// operator who launched the server.
    LocalMutate {
        /// How it was granted.
        granted_by: Grant,
    },
    // Dormant (saguão): SaguaoPassport(saguao::Passport) — the variant that
    // opens `can_write`.
}

/// How a [`Authority::LocalMutate`] was granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    /// `engenho-mcp --allow-mutate`: whoever can launch the server can
    /// already run `engenho ctl` as the same user.
    LaunchFlag,
}

impl Authority {
    /// Whether Kubernetes writes through a [`ClusterWriter`] are allowed.
    /// False for every variant defined today, `LocalMutate` included.
    #[must_use]
    pub const fn can_write(&self) -> bool {
        match self {
            Self::Observe | Self::LocalMutate { .. } => false,
        }
    }

    /// The highest control-plane tier the server acts at: the tools it
    /// offers, and the `Engenho-Ceiling` every call carries. Never
    /// destructive.
    #[must_use]
    pub const fn control_ceiling(&self) -> engenho_control_types::AuthorityTier {
        match self {
            Self::Observe => engenho_control_types::AuthorityTier::Observe,
            Self::LocalMutate { .. } => engenho_control_types::AuthorityTier::Mutate,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("unknown cluster: {0}")]
    UnknownCluster(String),

    #[error(
        "authority required for write — current authority cannot mutate (waiting on saguão passport materialization at P2)"
    )]
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

    /// The launch flag grants control-plane mutation, never Kubernetes
    /// writes, and never destructive control.
    #[test]
    fn no_authority_today_writes_kubernetes() {
        let mutate = Authority::LocalMutate {
            granted_by: Grant::LaunchFlag,
        };
        assert!(!Authority::Observe.can_write());
        assert!(!mutate.can_write());
        assert_eq!(
            Authority::Observe.control_ceiling(),
            engenho_control_types::AuthorityTier::Observe
        );
        assert_eq!(
            mutate.control_ceiling(),
            engenho_control_types::AuthorityTier::Mutate
        );
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
