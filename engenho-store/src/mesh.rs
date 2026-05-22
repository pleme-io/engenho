//! `StoreMesh` — public wrapper for engenho-store's Raft group.
//!
//! Mirrors `engenho-revoada::consensus::RaftMesh` but for the
//! resource catalog. K8s API operations (apiserver) decompose into
//! `propose` of typed [`crate::ResourceCommand`]s through this
//! handle; reads go through `get` / `list` on the local catalog
//! (every node has the full data via Raft replication).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, Config, Raft};
use tokio::sync::mpsc;

use crate::command::ResourceCommand;
use crate::network::{InProcessRouter, RpcRequest};
use crate::resource::{ResourceKey, ResourceValue};
use crate::state::ResourceCatalog;
use crate::store::InMemoryStore;
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("openraft config invalid: {0}")]
    ConfigInvalid(String),
    #[error("raft initialize failed: {0}")]
    InitializeFailed(String),
    #[error("client_write failed: {0}")]
    ClientWriteFailed(String),
    #[error("raft fatal: {0}")]
    Fatal(String),
}

/// Raft-replicated K8s resource store.
pub struct StoreMesh {
    raft: Raft<TypeConfig>,
    store: InMemoryStore,
    node_id: RaftNodeId,
    listen_addr: String,
    router: InProcessRouter,
    rpc_task: tokio::task::JoinHandle<()>,
}

impl StoreMesh {
    pub async fn start(
        node_id: RaftNodeId,
        listen_addr: String,
        router: InProcessRouter,
        config: Arc<Config>,
    ) -> Result<Self, StoreError> {
        let store = InMemoryStore::new();
        let log_store = store.clone();
        let state_machine = store.clone();

        let (tx_rpc, mut rx_rpc) = mpsc::channel::<RpcRequest>(256);

        let raft = Raft::<TypeConfig>::new(
            node_id,
            config,
            router.clone(),
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| StoreError::Fatal(e.to_string()))?;

        router.register(node_id, tx_rpc).await;

        let raft_for_rpc = raft.clone();
        let rpc_task = tokio::spawn(async move {
            while let Some(req) = rx_rpc.recv().await {
                match req {
                    RpcRequest::AppendEntries(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.append_entries(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::Vote(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.vote(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                    RpcRequest::InstallSnapshot(rpc, reply) => {
                        if let Ok(resp) = raft_for_rpc.install_snapshot(rpc).await {
                            let _ = reply.send(resp);
                        }
                    }
                }
            }
        });

        Ok(Self {
            raft,
            store,
            node_id,
            listen_addr,
            router,
            rpc_task,
        })
    }

    pub async fn initialize_singleton(&self) -> Result<(), StoreError> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.listen_addr.clone(),
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| StoreError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    pub async fn initialize_with_voters(
        &self,
        voters: Vec<(RaftNodeId, String)>,
    ) -> Result<(), StoreError> {
        let mut members = BTreeMap::new();
        for (id, addr) in voters {
            members.insert(id, BasicNode { addr });
        }
        self.raft
            .initialize(members)
            .await
            .map_err(|e| StoreError::InitializeFailed(e.to_string()))?;
        Ok(())
    }

    pub async fn propose(&self, cmd: ResourceCommand) -> Result<ApplyResult, StoreError> {
        let resp: ClientWriteResponse<TypeConfig> = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| StoreError::ClientWriteFailed(e.to_string()))?;
        Ok(resp.data)
    }

    /// Read a single resource by typed key — synchronous catalog
    /// read (no Raft round-trip needed; the local replica has the
    /// data once apply has caught up).
    pub async fn get(&self, key: &ResourceKey) -> Option<ResourceValue> {
        self.store.get_resource(key).await
    }

    /// List resources matching (group, version, kind), optionally
    /// namespace-scoped.
    pub async fn list(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<(ResourceKey, ResourceValue)> {
        let cat = self.store.current_catalog().await;
        cat.list(group, version, kind, namespace)
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Read-only snapshot of the whole catalog.
    pub async fn current_catalog(&self) -> ResourceCatalog {
        self.store.current_catalog().await
    }

    /// Subscribe to the watch stream — every committed mutation
    /// emits a typed [`engenho_store::WatchEvent`] (Added /
    /// Modified / Deleted with the resource value + Raft log index).
    ///
    /// Late subscribers do NOT see history. R7.5b's JetStream
    /// tier provides durable replay from a cursor; this is the
    /// fast in-process path consumed by local controllers.
    #[must_use]
    pub fn watch(&self) -> tokio::sync::broadcast::Receiver<crate::watch::WatchEvent> {
        self.store.watch_subscribe()
    }

    /// Active watch subscriber count (telemetry + test helper).
    #[must_use]
    pub fn watch_subscriber_count(&self) -> usize {
        self.store.watch_subscriber_count()
    }

    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    pub async fn wait_for_leadership(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = self.raft.metrics().clone();
        loop {
            if rx.borrow().current_leader == Some(self.node_id) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return false;
            }
        }
    }

    /// Wait for the state-machine apply index to reach `target`
    /// or `timeout` elapses.
    pub async fn wait_for_applied(&self, target: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let catalog = self.current_catalog().await;
            if catalog.last_applied_index >= target {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50.min(remaining.as_millis() as u64)))
                .await;
        }
    }

    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    pub async fn terminate(self) -> Result<(), StoreError> {
        self.router.deregister(self.node_id).await;
        self.rpc_task.abort();
        let _ = self.raft.shutdown().await;
        Ok(())
    }
}

pub fn default_config(cluster_name: &str) -> Result<Arc<Config>, StoreError> {
    let cfg = Config {
        cluster_name: cluster_name.to_string(),
        heartbeat_interval: 250,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    };
    let validated = cfg
        .validate()
        .map_err(|e| StoreError::ConfigInvalid(e.to_string()))?;
    Ok(Arc::new(validated))
}
