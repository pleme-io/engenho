//! In-memory mock reader for tests. Constructs every view inline
//! from a typed `MockSnapshot` builder so each test scenario stays
//! readable. Production code never touches this module.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::reader::{ClusterReader, ReaderError};
use crate::views::{ClusterConfigView, ClusterStatus, KubeAuthDescriptor, SnapshotMetaView};

#[derive(Default)]
pub struct MockClusterReader {
    state: Mutex<HashMap<String, MockClusterState>>,
}

#[derive(Clone)]
pub struct MockClusterState {
    pub status: ClusterStatus,
    pub config: ClusterConfigView,
    pub auth: KubeAuthDescriptor,
    pub snapshot: Option<SnapshotMetaView>,
}

impl MockClusterReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cluster(self, name: &str, state: MockClusterState) -> Self {
        self.state.lock().unwrap().insert(name.to_string(), state);
        self
    }
}

#[async_trait]
impl ClusterReader for MockClusterReader {
    async fn cluster_status(&self, cluster: &str) -> Result<ClusterStatus, ReaderError> {
        let state = self.state.lock().unwrap();
        state
            .get(cluster)
            .map(|s| s.status.clone())
            .ok_or_else(|| ReaderError::UnknownCluster {
                requested: cluster.to_string(),
                known: state.keys().cloned().collect(),
            })
    }

    async fn cluster_config(&self, cluster: &str) -> Result<ClusterConfigView, ReaderError> {
        let state = self.state.lock().unwrap();
        state
            .get(cluster)
            .map(|s| s.config.clone())
            .ok_or_else(|| ReaderError::UnknownCluster {
                requested: cluster.to_string(),
                known: state.keys().cloned().collect(),
            })
    }

    async fn kubeconfig_descriptor(
        &self,
        cluster: &str,
    ) -> Result<KubeAuthDescriptor, ReaderError> {
        let state = self.state.lock().unwrap();
        state
            .get(cluster)
            .map(|s| s.auth.clone())
            .ok_or_else(|| ReaderError::UnknownCluster {
                requested: cluster.to_string(),
                known: state.keys().cloned().collect(),
            })
    }

    async fn snapshot_meta(&self, cluster: &str) -> Result<Option<SnapshotMetaView>, ReaderError> {
        let state = self.state.lock().unwrap();
        state
            .get(cluster)
            .map(|s| s.snapshot.clone())
            .ok_or_else(|| ReaderError::UnknownCluster {
                requested: cluster.to_string(),
                known: state.keys().cloned().collect(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::views::*;

    fn sample_state() -> MockClusterState {
        MockClusterState {
            status: ClusterStatus {
                cluster: "demo".into(),
                rows: vec![StatusRow {
                    name: "Agent".into(),
                    outcome: StatusOutcome::Ok,
                    detail: "loaded".into(),
                }],
            },
            config: ClusterConfigView {
                cluster: "demo".into(),
                vm_mode: "k3s".into(),
                cpus: 4,
                memory_mib: 8192,
                disk_size: "50G".into(),
                api_port: 6443,
                ssh_port: 2222,
                mac_address: "5a:94:ef:00:00:01".into(),
                vm_ip: "192.168.64.10".into(),
                boot_timeout_secs: 300,
                gitops: GitopsBackendView::None,
            },
            auth: KubeAuthDescriptor {
                cluster: "demo".into(),
                kubeconfig_path: "/tmp/kubeconfig".into(),
                kubeconfig_exists: true,
                auth_method: AuthMethod::BearerToken,
            },
            snapshot: None,
        }
    }

    #[tokio::test]
    async fn known_cluster_returns_state() {
        let r = MockClusterReader::new().with_cluster("demo", sample_state());
        let s = r.cluster_status("demo").await.unwrap();
        assert_eq!(s.cluster, "demo");
        assert_eq!(s.rows[0].outcome, StatusOutcome::Ok);
    }

    #[tokio::test]
    async fn unknown_cluster_returns_typed_error() {
        let r = MockClusterReader::new();
        let err = r.cluster_status("missing").await.unwrap_err();
        assert_eq!(err.kind(), "unknown_cluster");
    }

    #[tokio::test]
    async fn snapshot_meta_can_be_absent() {
        let r = MockClusterReader::new().with_cluster("demo", sample_state());
        let snap = r.snapshot_meta("demo").await.unwrap();
        assert!(snap.is_none());
    }
}
