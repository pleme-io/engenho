use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use engenho_control_types::pin::{self, KeyError, KeyMaterial, PinSet, Presented, Spki};
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::command::LoggedCommand;
use crate::mesh_join::{JoinAnswer, JoinGate, JoinRequest, Sealed};
use crate::network::{
    ForwardRefused, Forwarded, LearnerAsk, LearnerVerdict, MeshNetwork, RpcEnvelope, RpcRequest,
    remote_answer,
};
use crate::owned_task::OwnedTask;
use crate::peer_table::{Peer, PeerTable, PeerTableError};
use crate::type_config::{ApplyResult, RaftNodeId, TypeConfig};

pub const ALPN: &[u8] = b"engenho-mesh/2";
const ASK_TTL: Duration = Duration::from_secs(5);
const JOIN_HOPS: usize = 4;
const FORWARD_TTL: Duration = Duration::from_secs(30);

#[derive(Serialize)]
enum OutFrame<'a> {
    Raft(&'a RpcEnvelope),
    Join(&'a Sealed),
}

#[derive(Deserialize)]
enum InFrame {
    Raft(RpcEnvelope),
    Join(Sealed),
}

#[derive(Debug)]
struct Anyone;

impl PinSet for Anyone {
    fn admits(&self, _: &Spki) -> bool {
        true
    }
}
pub const MAX_FRAME: u32 = 64 << 20;

#[derive(Debug, thiserror::Error)]
pub enum QuicMeshError {
    #[error("mesh identity: {0}")]
    Key(#[from] KeyError),
    #[error("mesh tls: {0}")]
    Tls(String),
    #[error(transparent)]
    PeerTable(#[from] PeerTableError),
    #[error("join: {0}")]
    Join(String),
    #[error("bind the mesh endpoint at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeshAddr {
    pub socket: SocketAddr,
    pub pin: Option<Spki>,
}

impl MeshAddr {
    pub const fn pinned(socket: SocketAddr, pin: Spki) -> Self {
        Self {
            socket,
            pin: Some(pin),
        }
    }
}

impl std::fmt::Display for MeshAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.pin {
            Some(pin) => write!(f, "{}#{pin}", self.socket),
            None => write!(f, "{}", self.socket),
        }
    }
}

impl std::str::FromStr for MeshAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (socket, pin) = match s.split_once('#') {
            Some((socket, pin)) => (socket, Some(Spki::parse(pin).map_err(|e| e.to_string())?)),
            None => (s, None),
        };
        let socket = socket
            .parse()
            .map_err(|e| format!("mesh address {s:?}: {e}"))?;
        Ok(Self { socket, pin })
    }
}

impl From<Peer> for MeshAddr {
    fn from(peer: Peer) -> Self {
        Self::pinned(peer.address, peer.spki)
    }
}

#[derive(Debug, Default)]
struct Peers(RwLock<HashMap<RaftNodeId, Spki>>);

impl Peers {
    fn pin_of(&self, node: RaftNodeId) -> Option<Spki> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&node)
            .copied()
    }
}

impl PinSet for Peers {
    fn admits(&self, spki: &Spki) -> bool {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .any(|pin| pin == spki)
    }
}

struct Inner {
    endpoint: quinn::Endpoint,
    identity: KeyMaterial,
    peers: Arc<Peers>,
    table: Option<Arc<PeerTable>>,
    gate: Option<JoinGate>,
    connections: Mutex<HashMap<RaftNodeId, quinn::Connection>>,
    accept: Mutex<Option<OwnedTask>>,
    follow: Mutex<Option<OwnedTask>>,
}

#[derive(Clone)]
pub struct QuicRouter {
    inner: Arc<Inner>,
}

impl QuicRouter {
    pub fn bind(bind: SocketAddr, identity: KeyMaterial) -> Result<Self, QuicMeshError> {
        Self::bind_over(bind, identity, None, None)
    }

    pub fn bind_with(
        bind: SocketAddr,
        identity: KeyMaterial,
        table: Arc<PeerTable>,
    ) -> Result<Self, QuicMeshError> {
        Self::bind_over(bind, identity, Some(table), None)
    }

    pub fn bind_joinable(
        bind: SocketAddr,
        identity: KeyMaterial,
        table: Arc<PeerTable>,
        gate: JoinGate,
    ) -> Result<Self, QuicMeshError> {
        Self::bind_over(bind, identity, Some(table), Some(gate))
    }

    fn bind_over(
        bind: SocketAddr,
        identity: KeyMaterial,
        table: Option<Arc<PeerTable>>,
        gate: Option<JoinGate>,
    ) -> Result<Self, QuicMeshError> {
        let peers = Arc::new(Peers::default());
        for peer in table.iter().flat_map(|t| t.peers()) {
            peers
                .0
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(peer.node_id, peer.spki);
        }
        let presented = Arc::new(Presented::new(&identity)?);
        let clients: Arc<dyn PinSet> = if gate.is_some() {
            Arc::new(Anyone)
        } else {
            Arc::clone(&peers) as Arc<dyn PinSet>
        };
        let mut tls = pin::server_config(presented, clients)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto =
            QuicServerConfig::try_from(tls).map_err(|e| QuicMeshError::Tls(e.to_string()))?;
        let endpoint =
            quinn::Endpoint::server(quinn::ServerConfig::with_crypto(Arc::new(crypto)), bind)
                .map_err(|source| QuicMeshError::Bind { addr: bind, source })?;
        Ok(Self {
            inner: Arc::new(Inner {
                endpoint,
                identity,
                peers,
                table,
                gate,
                connections: Mutex::new(HashMap::new()),
                accept: Mutex::new(None),
                follow: Mutex::new(None),
            }),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.endpoint.local_addr()
    }

    pub fn spki(&self) -> Spki {
        self.inner.identity.spki()
    }

    pub fn admit(&self, node: RaftNodeId, pin: Spki) {
        self.inner
            .peers
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node, pin);
    }

    pub fn learn(&self, peer: Peer) -> Result<bool, PeerTableError> {
        self.admit(peer.node_id, peer.spki);
        self.inner
            .table
            .as_ref()
            .map_or(Ok(false), |table| table.record(peer))
    }

    pub fn table(&self) -> Option<&Arc<PeerTable>> {
        self.inner.table.as_ref()
    }

    pub async fn revoke(&self, node: RaftNodeId) {
        self.inner
            .peers
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&node);
        if let Some(conn) = self.inner.connections.lock().await.remove(&node) {
            conn.close(0u32.into(), b"revoked");
        }
    }

    async fn connection(
        &self,
        target: RaftNodeId,
        addr: &str,
    ) -> std::io::Result<quinn::Connection> {
        if let Some(conn) = self.inner.connections.lock().await.get(&target)
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }
        let addr: MeshAddr = addr.parse().map_err(std::io::Error::other)?;
        let pin = addr
            .pin
            .or_else(|| self.inner.peers.pin_of(target))
            .ok_or_else(|| std::io::Error::other(format!("node {target} has no admitted pin")))?;
        let addr = addr.socket;
        let mut tls =
            pin::client_config(&self.inner.identity, vec![pin]).map_err(std::io::Error::other)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto = QuicClientConfig::try_from(tls).map_err(std::io::Error::other)?;
        let conn = self
            .inner
            .endpoint
            .connect_with(
                quinn::ClientConfig::new(Arc::new(crypto)),
                addr,
                pin::CERT_NAME,
            )
            .map_err(std::io::Error::other)?
            .await
            .map_err(std::io::Error::other)?;
        self.inner
            .connections
            .lock()
            .await
            .insert(target, conn.clone());
        Ok(conn)
    }

    async fn forget(&self, target: RaftNodeId) {
        self.inner.connections.lock().await.remove(&target);
    }

    pub async fn follow(
        &self,
        mut metrics: watch::Receiver<openraft::RaftMetrics<RaftNodeId, BasicNode>>,
    ) {
        let inner = Arc::downgrade(&self.inner);
        let task = OwnedTask::spawn(async move {
            loop {
                let members: Vec<(RaftNodeId, String)> = metrics
                    .borrow_and_update()
                    .membership_config
                    .membership()
                    .nodes()
                    .map(|(id, node)| (*id, node.addr.clone()))
                    .collect();
                let Some(router) = Weak::upgrade(&inner).map(|inner| QuicRouter { inner }) else {
                    return;
                };
                for peer in members.iter().filter_map(|(id, addr)| peer_of(*id, addr)) {
                    if let Err(e) = router.learn(peer) {
                        tracing::warn!(%e, node = peer.node_id, "a member's pin was not recorded");
                    }
                }
                drop(router);
                if metrics.changed().await.is_err() {
                    return;
                }
            }
        });
        if let Some(previous) = self.inner.follow.lock().await.replace(task) {
            let _ = previous.stop().await;
        }
    }

    pub async fn join(
        &self,
        seeds: &[SocketAddr],
        me: SocketAddr,
        within: Duration,
    ) -> Result<Vec<Peer>, QuicMeshError> {
        let gate = self.inner.gate.as_ref().ok_or_else(|| {
            QuicMeshError::Join("this router was bound without a join gate".into())
        })?;
        let me = MeshAddr::pinned(me, self.spki());
        let deadline = tokio::time::Instant::now() + within;
        let mut last = String::from("no seed was given");
        loop {
            let mut targets: VecDeque<SocketAddr> = seeds.iter().copied().collect();
            let mut hops = 0;
            while let Some(target) = targets.pop_front() {
                match tokio::time::timeout(ASK_TTL, self.ask(gate, target, me)).await {
                    Ok(Ok(LearnerVerdict::Admitted { members })) => {
                        let peers: Vec<Peer> = members
                            .iter()
                            .filter_map(|(id, addr)| peer_of(*id, addr))
                            .collect();
                        for peer in &peers {
                            self.learn(*peer)?;
                        }
                        return Ok(peers);
                    }
                    Ok(Ok(LearnerVerdict::NotLeader {
                        leader: Some((id, addr)),
                    })) if hops < JOIN_HOPS => {
                        hops += 1;
                        last = format!("{target} is not the leader; node {id} is");
                        if let Some(leader) = peer_of(id, &addr) {
                            targets.push_front(leader.address);
                        }
                    }
                    Ok(Ok(LearnerVerdict::NotLeader { .. })) => {
                        last = format!("{target} knows no leader");
                    }
                    Ok(Ok(LearnerVerdict::Refused { reason })) => {
                        last = format!("{target} refused: {reason}");
                    }
                    Ok(Err(e)) => last = format!("{target}: {e}"),
                    Err(_) => last = format!("{target}: no answer in {ASK_TTL:?}"),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(QuicMeshError::Join(last));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn ask(
        &self,
        gate: &JoinGate,
        target: SocketAddr,
        me: MeshAddr,
    ) -> std::io::Result<LearnerVerdict> {
        let mut tls = pin::client_config_admitting(&self.inner.identity, Arc::new(Anyone))
            .map_err(std::io::Error::other)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto = QuicClientConfig::try_from(tls).map_err(std::io::Error::other)?;
        let conn = self
            .inner
            .endpoint
            .connect_with(
                quinn::ClientConfig::new(Arc::new(crypto)),
                target,
                pin::CERT_NAME,
            )
            .map_err(std::io::Error::other)?
            .await
            .map_err(std::io::Error::other)?;
        let asked = async {
            let answering = presented(&conn)
                .ok_or_else(|| std::io::Error::other("the seed presented no key"))?;
            let nonce = nonce()?;
            let request = JoinRequest {
                cluster: gate.cluster.clone(),
                node_id: self.spki().mesh_node_id(),
                addr: me.to_string(),
                nonce: nonce.clone(),
            };
            let sealed = gate.seal_request(&request).map_err(std::io::Error::other)?;
            let frame =
                serde_json::to_vec(&OutFrame::Join(&sealed)).map_err(std::io::Error::other)?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(std::io::Error::other)?;
            write_frame(&mut send, &frame).await?;
            send.finish().map_err(std::io::Error::other)?;
            let reply = read_frame(&mut recv).await?;
            let sealed: Sealed = serde_json::from_slice(&reply).map_err(std::io::Error::other)?;
            let answer = gate
                .open_answer(&sealed)
                .ok_or_else(|| std::io::Error::other("the answer is not sealed by this cluster"))?;
            if answer.nonce != nonce || answer.seed != answering {
                return Err(std::io::Error::other("the answer is not to this request"));
            }
            Ok(answer.verdict)
        };
        let verdict = asked.await;
        conn.close(0u32.into(), b"asked");
        verdict
    }
}

fn peer_of(node_id: RaftNodeId, addr: &str) -> Option<Peer> {
    let addr: MeshAddr = addr.parse().ok()?;
    Some(Peer {
        node_id,
        address: addr.socket,
        spki: addr.pin?,
    })
}

fn presented(conn: &quinn::Connection) -> Option<Spki> {
    let certs = conn
        .peer_identity()?
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .ok()?;
    Spki::of_certificate(certs.first()?).ok()
}

fn nonce() -> std::io::Result<String> {
    let mut bytes = [0u8; 32];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut bytes)
        .map_err(|_| std::io::Error::other("no randomness for a join nonce"))?;
    Ok(bytes.iter().fold(String::with_capacity(64), |mut hex, b| {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
        hex
    }))
}

struct Serving {
    inbox: mpsc::Sender<RpcRequest>,
    peers: Arc<Peers>,
    gate: Option<JoinGate>,
    spki: Spki,
}

impl MeshNetwork for QuicRouter {
    async fn register(&self, _node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        let endpoint = self.inner.endpoint.clone();
        let serving = Arc::new(Serving {
            inbox: sender,
            peers: Arc::clone(&self.inner.peers),
            gate: self.inner.gate.clone(),
            spki: self.spki(),
        });
        let task = OwnedTask::spawn(accept(endpoint, serving));
        if let Some(previous) = self.inner.accept.lock().await.replace(task) {
            let _ = previous.stop().await;
        }
    }

    async fn deregister(&self, _node_id: RaftNodeId) {
        if let Some(task) = self.inner.accept.lock().await.take() {
            let _ = task.stop().await;
        }
        if let Some(task) = self.inner.follow.lock().await.take() {
            let _ = task.stop().await;
        }
        self.inner.connections.lock().await.clear();
        self.inner.endpoint.close(0u32.into(), b"deregistered");
    }

    async fn forward(&self, leader: RaftNodeId, addr: String, command: LoggedCommand) -> Forwarded {
        let network = QuicNetwork {
            target: leader,
            addr,
            router: self.clone(),
        };
        match network
            .call::<ApplyResult, ForwardRefused>(&RpcEnvelope::Forward(command), FORWARD_TTL)
            .await
        {
            Ok(result) => Ok(result),
            Err(RPCError::RemoteError(remote)) => Err(remote.source),
            Err(e) => Err(ForwardRefused::Unreachable(e.to_string())),
        }
    }
}

async fn accept(endpoint: quinn::Endpoint, serving: Arc<Serving>) {
    let mut connections = JoinSet::new();
    while let Some(incoming) = endpoint.accept().await {
        while connections.try_join_next().is_some() {}
        connections.spawn(serve_connection(incoming, Arc::clone(&serving)));
    }
}

async fn serve_connection(incoming: quinn::Incoming, serving: Arc<Serving>) {
    let conn = match incoming.await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::debug!(%e, "mesh handshake refused");
            return;
        }
    };
    let Some(caller) = presented(&conn) else {
        conn.close(1u32.into(), b"no key");
        return;
    };
    let mut streams = JoinSet::new();
    while let Ok((send, recv)) = conn.accept_bi().await {
        while streams.try_join_next().is_some() {}
        streams.spawn(serve_stream(send, recv, Arc::clone(&serving), caller));
    }
}

async fn serve_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    serving: Arc<Serving>,
    caller: Spki,
) {
    let answered = async {
        let request = read_frame(&mut recv).await?;
        let frame: InFrame = serde_json::from_slice(&request).map_err(std::io::Error::other)?;
        let reply = match frame {
            InFrame::Raft(envelope) => {
                if !serving.peers.admits(&caller) {
                    return Err(std::io::Error::other(format!("{caller} is not admitted")));
                }
                envelope.deliver(&serving.inbox).await?
            }
            InFrame::Join(sealed) => answer_join(&serving, caller, &sealed).await?,
        };
        write_frame(&mut send, &reply).await?;
        send.finish().map_err(std::io::Error::other)
    };
    if let Err(e) = answered.await {
        tracing::debug!(%e, "mesh stream ended without a reply");
        let _ = send.reset(1u32.into());
    }
}

async fn answer_join(serving: &Serving, caller: Spki, sealed: &Sealed) -> std::io::Result<Vec<u8>> {
    let gate = serving
        .gate
        .as_ref()
        .ok_or_else(|| std::io::Error::other("this node takes no joins"))?;
    let request = gate
        .open_request(sealed)
        .ok_or_else(|| std::io::Error::other("a join not sealed by this cluster"))?;
    let verdict = match judge(gate, &request, caller) {
        Ok(ask) => {
            let (tx, rx) = oneshot::channel();
            serving
                .inbox
                .send(RpcRequest::AddLearner(ask, tx))
                .await
                .map_err(|_| std::io::Error::other("the node's RPC inbox is closed"))?;
            rx.await.map_err(std::io::Error::other)?
        }
        Err(reason) => LearnerVerdict::Refused { reason },
    };
    let answer = JoinAnswer {
        nonce: request.nonce,
        seed: serving.spki,
        verdict,
    };
    let sealed = gate.seal_answer(&answer).map_err(std::io::Error::other)?;
    serde_json::to_vec(&sealed).map_err(std::io::Error::other)
}

fn judge(gate: &JoinGate, request: &JoinRequest, caller: Spki) -> Result<LearnerAsk, String> {
    if request.cluster != gate.cluster {
        return Err(format!(
            "this is cluster {}, not {}",
            gate.cluster, request.cluster
        ));
    }
    let addr: MeshAddr = request.addr.parse()?;
    if addr.pin != Some(caller) {
        return Err("the address names another key than the one that asked".into());
    }
    if request.node_id != caller.mesh_node_id() {
        return Err(format!(
            "node id {} is not the id of the key that asked",
            request.node_id
        ));
    }
    Ok(LearnerAsk {
        node_id: request.node_id,
        addr: request.addr.clone(),
    })
}

async fn write_frame(send: &mut quinn::SendStream, bytes: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(bytes.len())
        .ok()
        .filter(|len| *len <= MAX_FRAME)
        .ok_or_else(|| std::io::Error::other(format!("a {} byte frame", bytes.len())))?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(std::io::Error::other)?;
    send.write_all(bytes).await.map_err(std::io::Error::other)
}

async fn read_frame(recv: &mut quinn::RecvStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len)
        .await
        .map_err(std::io::Error::other)?;
    let len = u32::from_be_bytes(len);
    if len > MAX_FRAME {
        return Err(std::io::Error::other(format!("a {len} byte frame")));
    }
    let mut bytes = vec![0u8; len as usize];
    recv.read_exact(&mut bytes)
        .await
        .map_err(std::io::Error::other)?;
    Ok(bytes)
}

impl RaftNetworkFactory<TypeConfig> for QuicRouter {
    type Network = QuicNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        QuicNetwork {
            target,
            addr: node.addr.clone(),
            router: self.clone(),
        }
    }
}

pub struct QuicNetwork {
    target: RaftNodeId,
    addr: String,
    router: QuicRouter,
}

impl QuicNetwork {
    async fn call<T, E>(
        &self,
        envelope: &RpcEnvelope,
        ttl: Duration,
    ) -> Result<T, RPCError<RaftNodeId, BasicNode, E>>
    where
        T: DeserializeOwned,
        E: DeserializeOwned + std::error::Error,
    {
        let request = serde_json::to_vec(&OutFrame::Raft(envelope))
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let conn = self
            .router
            .connection(self.target, &self.addr)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let exchange = async {
            let (mut send, mut recv) = conn.open_bi().await.map_err(std::io::Error::other)?;
            write_frame(&mut send, &request).await?;
            send.finish().map_err(std::io::Error::other)?;
            let reply = read_frame(&mut recv).await?;
            serde_json::from_slice::<Result<T, E>>(&reply).map_err(std::io::Error::other)
        };
        let failed = match tokio::time::timeout(ttl, exchange).await {
            Ok(Ok(answer)) => return remote_answer(self.target, answer),
            Ok(Err(e)) => e,
            Err(_) => {
                std::io::Error::other(format!("no reply from node {} in {ttl:?}", self.target))
            }
        };
        self.router.forget(self.target).await;
        Err(RPCError::Network(NetworkError::new(&failed)))
    }
}

impl RaftNetwork<TypeConfig> for QuicNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        self.call(&RpcEnvelope::AppendEntries(rpc), option.hard_ttl())
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        self.call(&RpcEnvelope::InstallSnapshot(rpc), option.hard_ttl())
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        self.call(&RpcEnvelope::Vote(rpc), option.hard_ttl()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound() -> QuicRouter {
        QuicRouter::bind(([127, 0, 0, 1], 0).into(), KeyMaterial::generate().unwrap()).unwrap()
    }

    async fn serving(router: &QuicRouter, id: RaftNodeId) -> mpsc::Receiver<RpcRequest> {
        let (tx, rx) = mpsc::channel(8);
        router.register(id, tx).await;
        rx
    }

    fn answer_votes(mut inbox: mpsc::Receiver<RpcRequest>) -> tokio::task::JoinHandle<u32> {
        tokio::spawn(async move {
            let mut answered = 0;
            while let Some(request) = inbox.recv().await {
                if let RpcRequest::Vote(rpc, reply) = request {
                    answered += 1;
                    let _ = reply.send(Ok(VoteResponse::new(rpc.vote, None, true)));
                }
            }
            answered
        })
    }

    async fn vote(
        from: &QuicRouter,
        to: RaftNodeId,
        at: &QuicRouter,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let node = BasicNode::new(at.local_addr().unwrap().to_string());
        let mut client = from.clone().new_client(to, &node).await;
        client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(2)),
            )
            .await
    }

    #[tokio::test]
    async fn a_pinned_peer_is_answered_and_a_stranger_never_reaches_the_inbox() {
        let (a, b, stranger) = (bound(), bound(), bound());
        a.admit(2, b.spki());
        b.admit(1, a.spki());
        stranger.admit(2, b.spki());
        let answered = answer_votes(serving(&b, 2).await);

        let granted = vote(&a, 2, &b).await.unwrap();
        assert!(granted.vote_granted);
        assert!(
            vote(&stranger, 2, &b).await.is_err(),
            "a key node 2 never admitted got an answer"
        );

        b.revoke(1).await;
        a.revoke(2).await;
        a.admit(2, b.spki());
        assert!(
            vote(&a, 2, &b).await.is_err(),
            "node 1's pin was revoked at node 2, yet a new connection got through"
        );

        b.deregister(2).await;
        assert_eq!(
            answered.await.unwrap(),
            1,
            "only the pinned peer's vote arrived"
        );
    }

    #[tokio::test]
    async fn a_server_whose_key_is_not_the_pinned_one_is_refused_by_the_client() {
        let (a, b, impostor) = (bound(), bound(), bound());
        a.admit(2, b.spki());
        impostor.admit(1, a.spki());
        let _answered = answer_votes(serving(&impostor, 2).await);
        let refused = vote(&a, 2, &impostor).await;
        assert!(
            matches!(refused, Err(RPCError::Unreachable(_))),
            "node 1 spoke to a server presenting another key than node 2's pin: {refused:?}"
        );
    }

    fn peer_of(router: &QuicRouter, id: RaftNodeId) -> Peer {
        Peer {
            node_id: id,
            address: router.local_addr().unwrap(),
            spki: router.spki(),
        }
    }

    #[tokio::test]
    async fn a_router_bound_over_a_peer_table_answers_the_peers_it_names_and_no_other() {
        let dir = tempfile::tempdir().unwrap();
        let (a, stranger) = (bound(), bound());
        let path = dir.path().join(PeerTable::FILE);
        PeerTable::open(&path)
            .unwrap()
            .record(peer_of(&a, 1))
            .unwrap();
        let b = QuicRouter::bind_with(
            ([127, 0, 0, 1], 0).into(),
            KeyMaterial::generate().unwrap(),
            Arc::new(PeerTable::open(&path).unwrap()),
        )
        .unwrap();
        a.admit(2, b.spki());
        stranger.admit(2, b.spki());
        let _answered = answer_votes(serving(&b, 2).await);
        assert!(vote(&a, 2, &b).await.unwrap().vote_granted);
        assert!(vote(&stranger, 2, &b).await.is_err());
    }

    #[tokio::test]
    async fn what_a_router_learns_is_in_its_table_and_admitted_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mesh").join(PeerTable::FILE);
        let a = bound();
        let key = KeyMaterial::generate().unwrap();
        let pem = key.to_pem();
        let first = QuicRouter::bind_with(
            ([127, 0, 0, 1], 0).into(),
            key,
            Arc::new(PeerTable::open(&path).unwrap()),
        )
        .unwrap();
        assert!(first.learn(peer_of(&a, 1)).unwrap());
        assert!(
            !first.learn(peer_of(&a, 1)).unwrap(),
            "a known peer is not rewritten"
        );
        first.deregister(2).await;
        drop(first);

        assert_eq!(
            PeerTable::open(&path).unwrap().peers(),
            vec![peer_of(&a, 1)]
        );
        let again = QuicRouter::bind_with(
            ([127, 0, 0, 1], 0).into(),
            KeyMaterial::from_pem(&pem).unwrap(),
            Arc::new(PeerTable::open(&path).unwrap()),
        )
        .unwrap();
        a.admit(2, again.spki());
        let _answered = answer_votes(serving(&again, 2).await);
        assert!(vote(&a, 2, &again).await.unwrap().vote_granted);
    }

    #[tokio::test]
    async fn a_joinable_router_never_delivers_a_raft_rpc_from_a_key_it_has_not_admitted() {
        let dir = tempfile::tempdir().unwrap();
        let b = QuicRouter::bind_joinable(
            ([127, 0, 0, 1], 0).into(),
            KeyMaterial::generate().unwrap(),
            Arc::new(PeerTable::open(dir.path().join(PeerTable::FILE)).unwrap()),
            crate::mesh_join::JoinGate {
                cluster: "c".into(),
                key: crate::mesh_join::JoinKey::derive(b"k"),
            },
        )
        .unwrap();
        let (a, stranger) = (bound(), bound());
        b.admit(1, a.spki());
        a.admit(2, b.spki());
        stranger.admit(2, b.spki());
        let answered = answer_votes(serving(&b, 2).await);
        assert!(vote(&a, 2, &b).await.unwrap().vote_granted);
        assert!(
            vote(&stranger, 2, &b).await.is_err(),
            "a key the joinable router never admitted had its vote delivered"
        );
        b.deregister(2).await;
        assert_eq!(answered.await.unwrap(), 1);
    }

    #[test]
    fn a_mesh_address_carries_its_pin_or_none() {
        let spki = KeyMaterial::generate().unwrap().spki();
        let socket: SocketAddr = "127.0.0.1:7946".parse().unwrap();
        let pinned = MeshAddr::pinned(socket, spki);
        assert_eq!(pinned.to_string().parse::<MeshAddr>(), Ok(pinned));
        assert_eq!(
            "127.0.0.1:7946".parse::<MeshAddr>(),
            Ok(MeshAddr { socket, pin: None })
        );
        for bad in ["in-process://1", "127.0.0.1:7946#sha1:00", "#sha256:00"] {
            assert!(bad.parse::<MeshAddr>().is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_member_address_that_carries_a_pin_is_reached_with_no_admit_call() {
        let (a, b) = (bound(), bound());
        b.admit(1, a.spki());
        let _answered = answer_votes(serving(&b, 2).await);
        let node = BasicNode::new(MeshAddr::pinned(b.local_addr().unwrap(), b.spki()).to_string());
        let mut client = a.clone().new_client(2, &node).await;
        let granted = client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(2)),
            )
            .await
            .unwrap();
        assert!(granted.vote_granted);
    }

    #[tokio::test]
    async fn a_peer_with_no_admitted_pin_is_unreachable_without_a_handshake() {
        let (a, b) = (bound(), bound());
        let _answered = answer_votes(serving(&b, 2).await);
        assert!(matches!(
            vote(&a, 2, &b).await,
            Err(RPCError::Unreachable(_))
        ));
    }
}
