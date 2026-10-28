use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::mpsc;

use crate::mesh::{StoreError, StoreMesh};
use crate::network::{InProcessNetwork, InProcessRouter, MeshNetwork, RpcRequest};
use crate::type_config::{RaftNodeId, TypeConfig};

pub type Link = (RaftNodeId, RaftNodeId);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admit {
    Deliver(Option<Duration>),
    Cut,
    Dropped,
}

#[derive(Default)]
struct Faults {
    cut: BTreeSet<Link>,
    drops: HashMap<Link, usize>,
    delays: HashMap<Link, Duration>,
    lost: HashMap<Link, u64>,
    delivered: HashMap<Link, u64>,
}

impl Faults {
    fn admit(&mut self, link: Link) -> Admit {
        if self.cut.contains(&link) {
            *self.lost.entry(link).or_default() += 1;
            return Admit::Cut;
        }
        if let Some(left) = self.drops.get_mut(&link).filter(|n| **n > 0) {
            *left -= 1;
            *self.lost.entry(link).or_default() += 1;
            return Admit::Dropped;
        }
        *self.delivered.entry(link).or_default() += 1;
        Admit::Deliver(self.delays.get(&link).copied())
    }

    fn reply_lost(&mut self, link: Link) -> bool {
        let back = (link.1, link.0);
        let lost = self.cut.contains(&back);
        if lost {
            *self.lost.entry(back).or_default() += 1;
        }
        lost
    }
}

#[derive(Clone, Default)]
pub struct FaultNet {
    inner: InProcessRouter,
    faults: Arc<Mutex<Faults>>,
}

impl FaultNet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(&self, local: RaftNodeId) -> FaultRouter {
        FaultRouter {
            net: self.clone(),
            local,
        }
    }

    fn faults(&self) -> MutexGuard<'_, Faults> {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn cut(&self, from: RaftNodeId, to: RaftNodeId) {
        self.faults().cut.insert((from, to));
    }

    pub fn partition(&self, side: &[RaftNodeId], other: &[RaftNodeId]) {
        let mut faults = self.faults();
        for &a in side {
            for &b in other {
                faults.cut.insert((a, b));
                faults.cut.insert((b, a));
            }
        }
    }

    pub fn drop_next(&self, from: RaftNodeId, to: RaftNodeId, count: usize) {
        self.faults().drops.insert((from, to), count);
    }

    pub fn delay(&self, from: RaftNodeId, to: RaftNodeId, by: Duration) {
        self.faults().delays.insert((from, to), by);
    }

    pub fn heal(&self) {
        let mut faults = self.faults();
        faults.cut.clear();
        faults.drops.clear();
        faults.delays.clear();
    }

    pub fn lost(&self, from: RaftNodeId, to: RaftNodeId) -> u64 {
        self.faults().lost.get(&(from, to)).copied().unwrap_or(0)
    }

    pub fn delivered(&self, from: RaftNodeId, to: RaftNodeId) -> u64 {
        self.faults()
            .delivered
            .get(&(from, to))
            .copied()
            .unwrap_or(0)
    }

    pub fn admit(&self, link: Link) -> Admit {
        self.faults().admit(link)
    }

    fn reply_lost(&self, link: Link) -> bool {
        self.faults().reply_lost(link)
    }
}

#[derive(Clone)]
pub struct FaultRouter {
    net: FaultNet,
    local: RaftNodeId,
}

impl FaultRouter {
    pub fn local(&self) -> RaftNodeId {
        self.local
    }
}

impl MeshNetwork for FaultRouter {
    async fn register(&self, node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        assert_eq!(
            node_id, self.local,
            "a FaultRouter handle carries the faults of node {}, not of node {node_id}",
            self.local
        );
        self.net.inner.register(node_id, sender).await;
    }

    async fn deregister(&self, node_id: RaftNodeId) {
        self.net.inner.deregister(node_id).await;
    }
}

impl RaftNetworkFactory<TypeConfig> for FaultRouter {
    type Network = FaultNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        let mut inner = self.net.inner.clone();
        FaultNetwork {
            link: (self.local, target),
            net: self.net.clone(),
            inner: inner.new_client(target, node).await,
        }
    }
}

pub struct FaultNetwork {
    link: Link,
    net: FaultNet,
    inner: InProcessNetwork,
}

fn lost<E: std::error::Error>(link: Link, what: &str) -> RPCError<RaftNodeId, BasicNode, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "fault router: {what} on link {} -> {}",
        link.0, link.1
    ))))
}

async fn carry<T, E>(
    net: &FaultNet,
    link: Link,
    call: impl std::future::Future<Output = Result<T, RPCError<RaftNodeId, BasicNode, E>>>,
) -> Result<T, RPCError<RaftNodeId, BasicNode, E>>
where
    E: std::error::Error,
{
    match net.admit(link) {
        Admit::Cut => return Err(lost(link, "request cut")),
        Admit::Dropped => return Err(lost(link, "request dropped")),
        Admit::Deliver(Some(by)) => tokio::time::sleep(by).await,
        Admit::Deliver(None) => {}
    }
    let answer = call.await;
    if net.reply_lost(link) {
        return Err(lost((link.1, link.0), "reply cut"));
    }
    answer
}

impl RaftNetwork<TypeConfig> for FaultNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        carry(&self.net, self.link, self.inner.append_entries(rpc, option)).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        carry(
            &self.net,
            self.link,
            self.inner.install_snapshot(rpc, option),
        )
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        carry(&self.net, self.link, self.inner.vote(rpc, option)).await
    }
}

pub fn fast_config(cluster_name: &str) -> Result<Arc<Config>, StoreError> {
    let cfg = Config {
        cluster_name: cluster_name.to_string(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    };
    cfg.validate()
        .map(Arc::new)
        .map_err(|e| StoreError::ConfigInvalid(e.to_string()))
}

pub struct Cluster {
    net: FaultNet,
    nodes: Vec<StoreMesh<FaultRouter>>,
}

impl Cluster {
    pub async fn start(cluster_name: &str, size: u64) -> Result<Self, StoreError> {
        let net = FaultNet::new();
        let cfg = fast_config(cluster_name)?;
        let mut nodes = Vec::new();
        for id in 1..=size {
            nodes.push(StoreMesh::start(id, address(id), net.node(id), cfg.clone()).await?);
        }
        nodes[0]
            .initialize_with_voters((1..=size).map(|id| (id, address(id))).collect())
            .await?;
        Ok(Self { net, nodes })
    }

    pub fn net(&self) -> &FaultNet {
        &self.net
    }

    pub fn ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(StoreMesh::node_id).collect()
    }

    pub fn others(&self, id: RaftNodeId) -> Vec<RaftNodeId> {
        self.ids().into_iter().filter(|n| *n != id).collect()
    }

    pub fn node(&self, id: RaftNodeId) -> &StoreMesh<FaultRouter> {
        self.nodes
            .iter()
            .find(|n| n.node_id() == id)
            .unwrap_or_else(|| panic!("no node {id} in this cluster"))
    }

    pub async fn leader_among(&self, among: &[RaftNodeId], within: Duration) -> Option<RaftNodeId> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            for &id in among {
                if self.node(id).is_leader().await {
                    return Some(id);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn leader(&self, within: Duration) -> Option<RaftNodeId> {
        self.leader_among(&self.ids(), within).await
    }

    pub fn isolate(&self, id: RaftNodeId) {
        self.net.partition(&[id], &self.others(id));
    }

    pub fn heal(&self) {
        self.net.heal();
    }

    pub async fn all_applied(&self, among: &[RaftNodeId], index: u64, within: Duration) -> bool {
        for &id in among {
            if !self.node(id).wait_for_applied(index, within).await {
                return false;
            }
        }
        true
    }

    pub async fn terminate(self) -> Result<(), StoreError> {
        for node in self.nodes {
            node.terminate().await?;
        }
        Ok(())
    }
}

fn address(id: RaftNodeId) -> String {
    format!("fault-router://{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_one_way_cut_stops_requests_one_way_and_loses_the_replies_coming_back() {
        let net = FaultNet::new();
        net.cut(1, 2);
        assert_eq!(net.admit((1, 2)), Admit::Cut);
        assert_eq!(net.admit((2, 1)), Admit::Deliver(None));
        assert!(net.reply_lost((2, 1)));
        assert!(!net.reply_lost((1, 2)));
        assert_eq!(net.lost(1, 2), 2);
        assert_eq!(net.delivered(2, 1), 1);
    }

    #[test]
    fn a_partition_cuts_every_pair_across_it_both_ways_and_nothing_inside_a_side() {
        let net = FaultNet::new();
        net.partition(&[1], &[2, 3]);
        for link in [(1, 2), (2, 1), (1, 3), (3, 1)] {
            assert_eq!(net.admit(link), Admit::Cut, "{link:?}");
        }
        assert_eq!(net.admit((2, 3)), Admit::Deliver(None));
        assert_eq!(net.admit((3, 2)), Admit::Deliver(None));
    }

    #[test]
    fn drop_next_loses_exactly_that_many_then_delivers() {
        let net = FaultNet::new();
        net.drop_next(1, 2, 2);
        assert_eq!(net.admit((1, 2)), Admit::Dropped);
        assert_eq!(net.admit((1, 2)), Admit::Dropped);
        assert_eq!(net.admit((1, 2)), Admit::Deliver(None));
        assert_eq!(net.admit((2, 1)), Admit::Deliver(None));
    }

    #[test]
    fn a_delay_rides_the_delivery_of_its_link_only() {
        let net = FaultNet::new();
        net.delay(1, 2, Duration::from_millis(7));
        assert_eq!(
            net.admit((1, 2)),
            Admit::Deliver(Some(Duration::from_millis(7)))
        );
        assert_eq!(net.admit((2, 1)), Admit::Deliver(None));
    }

    #[test]
    fn heal_clears_cuts_drops_and_delays_and_keeps_the_counts() {
        let net = FaultNet::new();
        net.partition(&[1], &[2]);
        net.drop_next(2, 3, 5);
        net.delay(3, 1, Duration::from_millis(1));
        let _ = net.admit((1, 2));
        net.heal();
        for link in [(1, 2), (2, 1), (2, 3), (3, 1)] {
            assert_eq!(net.admit(link), Admit::Deliver(None), "{link:?}");
        }
        assert_eq!(net.lost(1, 2), 1);
    }
}
