//! Read-only operator surface — observes cluster state without
//! ever mutating it. The MCP server's tool catalog consumes
//! `&dyn ClusterReader`; production wires the `KikaiClusterReader`,
//! tests wire the `MockClusterReader`. The trait IS the boundary —
//! any caller that can construct a `Box<dyn ClusterReader>` can
//! drive the server end to end.

use async_trait::async_trait;

use crate::views::{
    ClusterConfig, ClusterStatus, KubeAuthDescriptor, PodListView, SnapshotMetaView,
};

pub mod kikai;
#[cfg(test)]
pub mod mock;
pub mod node;

/// Operator-facing filter spec for `list_resource`. Every field is
/// optional — empty `ListSpec::default()` is equivalent to "no
/// filtering, no pagination, all items". Mirrors the typed shape
/// of `engenho_types::client::ListOptions` but kept here so the
/// MCP wire schema stays stable across engenho-types' churn.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListSpec {
    /// Kubernetes label selector (e.g. `app=podinfo,tier=backend`).
    pub label_selector: String,
    /// Kubernetes field selector (e.g. `status.phase=Running`).
    pub field_selector: String,
    /// Maximum items to return. `None` = no limit.
    pub limit: Option<u32>,
}

/// Strict typed error surface — every variant carries enough
/// context for the MCP client to render a useful failure.
/// Never wraps `anyhow::Error`; concrete I/O / parse causes go
/// in `Io` / `Parse` variants so consumers can distinguish.
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// The named cluster is not one this reader knows.
    ///
    /// Carries the set that IS known, because that is what the caller needs to
    /// retry and both raise sites already hold it. Without it a typo ends the
    /// conversation; with it the retry costs one call. This is the payload
    /// kotae's `Refused` arm renders as its `legal` set.
    #[error("unknown cluster: {requested} (known: {})", known.join(", "))]
    UnknownCluster {
        requested: String,
        known: Vec<String>,
    },

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
            Self::UnknownCluster { .. } => "unknown_cluster",
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
    async fn cluster_config(&self, cluster: &str) -> Result<ClusterConfig, ReaderError>;

    /// Path + auth method, never the secret material itself.
    async fn kubeconfig_descriptor(&self, cluster: &str)
    -> Result<KubeAuthDescriptor, ReaderError>;

    /// Snapshot meta if a fast-resume artifact is present; `None`
    /// when the cluster has no saved snapshot. Snapshot files may
    /// reference store paths that don't exist anymore — the view
    /// surfaces that via `all_paths_exist`.
    async fn snapshot_meta(&self, cluster: &str) -> Result<Option<SnapshotMetaView>, ReaderError>;

    /// List pods in the given namespace. First reader method that
    /// goes BEYOND on-disk state — talks to the live cluster's API
    /// via `engenho-kube-client` and parses responses through
    /// `engenho-types::Pod` (typed PodSpec / PodStatus).
    ///
    /// Default impl returns Unsupported so non-live readers (mock)
    /// don't have to implement.
    async fn list_pods(&self, cluster: &str, _namespace: &str) -> Result<PodListView, ReaderError> {
        Err(ReaderError::InvalidState(format!(
            "list_pods not supported for cluster '{cluster}' (reader does not implement live API access)"
        )))
    }

    /// Generic typed resource listing. Dispatches on `kind` to the
    /// engenho-types catalog + engenho-kube-client. Returns the
    /// resource list as a JSON `serde_json::Value` so the trait
    /// surface remains object-safe (a generic method would NOT be).
    /// Inside the impl, dispatch is static — each kind calls the
    /// typed `client.list::<R>()`.
    ///
    /// **Compounding contract**: adding a new resource kind means
    /// (a) one variant in `ResourceKind`, (b) one match arm here,
    /// (c) typed expansion in engenho-types if needed. No new
    /// MCP tool, no new view type, no trait widening.
    async fn list_resource(
        &self,
        cluster: &str,
        kind: crate::resource_kind::ResourceKind,
        _namespace: &str,
        _spec: &ListSpec,
    ) -> Result<serde_json::Value, ReaderError> {
        Err(ReaderError::InvalidState(format!(
            "list_resource({}) not supported for cluster '{cluster}' (reader does not implement live API access)",
            kind.label()
        )))
    }

    /// Generic typed resource fetch. Symmetric to [`list_resource`]
    /// but returns a single resource by name. For cluster-scoped
    /// kinds (e.g. Namespace) the `namespace` argument is ignored.
    ///
    /// Same compounding contract — adding a kind means one variant
    /// + one match arm (list) + one match arm (get).
    async fn get_resource(
        &self,
        cluster: &str,
        kind: crate::resource_kind::ResourceKind,
        _namespace: &str,
        _name: &str,
    ) -> Result<serde_json::Value, ReaderError> {
        Err(ReaderError::InvalidState(format!(
            "get_resource({}) not supported for cluster '{cluster}' (reader does not implement live API access)",
            kind.label()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_error_kind_is_stable() {
        let cases = [
            (
                ReaderError::UnknownCluster {
                    requested: "x".into(),
                    known: vec!["a".into()],
                },
                "unknown_cluster",
            ),
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
            async fn cluster_config(&self, _: &str) -> Result<ClusterConfig, ReaderError> {
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
