//! Read-only operator surface — observes cluster state without
//! ever mutating it. The MCP server's tool catalog consumes
//! `&dyn ClusterReader`; production wires the `KikaiClusterReader`,
//! tests wire the `MockClusterReader`. The trait IS the boundary —
//! any caller that can construct a `Box<dyn ClusterReader>` can
//! drive the server end to end.

use async_trait::async_trait;

use crate::views::{
    ClusterConfigView, ClusterStatus, KubeAuthDescriptor, PodListView, SnapshotMetaView,
};

pub mod kikai;
#[cfg(test)]
pub mod mock;

/// Strict typed error surface — every variant carries enough
/// context for the MCP client to render a useful failure.
/// Never wraps `anyhow::Error`; concrete I/O / parse causes go
/// in `Io` / `Parse` variants so consumers can distinguish.
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("unknown cluster: {0}")]
    UnknownCluster(String),

    #[error("io error reading {what}: {source}")]
    Io {
        what: String,
        #[source]
        source: std::io::Error,
    },

    #[error("parse error for {what}: {source}")]
    Parse {
        what: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("invalid cluster state: {0}")]
    InvalidState(String),
}

impl ReaderError {
    /// Stable identifier for downstream classification (telemetry,
    /// MCP error payload). Order-independent + lowercase snake.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "unknown_cluster",
            Self::Io { .. } => "io",
            Self::Parse { .. } => "parse",
            Self::InvalidState(_) => "invalid_state",
        }
    }
}

#[async_trait]
pub trait ClusterReader: Send + Sync {
    /// Aggregate cluster health — six rows mirroring kikai status.
    async fn cluster_status(&self, cluster: &str) -> Result<ClusterStatus, ReaderError>;

    /// Non-secret slice of the deployed cluster config.
    async fn cluster_config(&self, cluster: &str) -> Result<ClusterConfigView, ReaderError>;

    /// Path + auth method, never the secret material itself.
    async fn kubeconfig_descriptor(
        &self,
        cluster: &str,
    ) -> Result<KubeAuthDescriptor, ReaderError>;

    /// Snapshot meta if a fast-resume artifact is present; `None`
    /// when the cluster has no saved snapshot. Snapshot files may
    /// reference store paths that don't exist anymore — the view
    /// surfaces that via `all_paths_exist`.
    async fn snapshot_meta(
        &self,
        cluster: &str,
    ) -> Result<Option<SnapshotMetaView>, ReaderError>;

    /// List pods in the given namespace. First reader method that
    /// goes BEYOND on-disk state — talks to the live cluster's API
    /// via `engenho-kube-client` and parses responses through
    /// `engenho-types::Pod` (typed PodSpec / PodStatus).
    ///
    /// Default impl returns Unsupported so non-live readers (mock)
    /// don't have to implement.
    async fn list_pods(
        &self,
        cluster: &str,
        _namespace: &str,
    ) -> Result<PodListView, ReaderError> {
        Err(ReaderError::InvalidState(format!(
            "list_pods not supported for cluster '{cluster}' (reader does not implement live API access)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_error_kind_is_stable() {
        let cases = [
            (ReaderError::UnknownCluster("x".into()), "unknown_cluster"),
            (
                ReaderError::Io {
                    what: "x".into(),
                    source: std::io::Error::other("boom"),
                },
                "io",
            ),
            (ReaderError::InvalidState("x".into()), "invalid_state"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.kind(), expected);
        }
    }

    #[test]
    fn reader_trait_is_object_safe() {
        // Compile-time proof that we can dyn-dispatch the reader.
        // The MCP server stores `Arc<dyn ClusterReader>`; if this
        // doesn't build, the substrate has lost its boundary.
        fn assert_object_safe(_: &dyn ClusterReader) {}
        struct Stub;
        #[async_trait]
        impl ClusterReader for Stub {
            async fn cluster_status(&self, _: &str) -> Result<ClusterStatus, ReaderError> {
                unimplemented!()
            }
            async fn cluster_config(&self, _: &str) -> Result<ClusterConfigView, ReaderError> {
                unimplemented!()
            }
            async fn kubeconfig_descriptor(
                &self,
                _: &str,
            ) -> Result<KubeAuthDescriptor, ReaderError> {
                unimplemented!()
            }
            async fn snapshot_meta(
                &self,
                _: &str,
            ) -> Result<Option<SnapshotMetaView>, ReaderError> {
                unimplemented!()
            }
        }
        assert_object_safe(&Stub);
    }
}
