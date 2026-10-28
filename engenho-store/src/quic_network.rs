use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, PoisonError, RwLock};
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
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;

use crate::network::{MeshNetwork, RpcEnvelope, RpcRequest, remote_answer};
use crate::owned_task::OwnedTask;
use crate::type_config::{RaftNodeId, TypeConfig};

pub const ALPN: &[u8] = b"engenho-mesh/1";
pub const MAX_FRAME: u32 = 64 << 20;

#[derive(Debug, thiserror::Error)]
pub enum QuicMeshError {
    #[error("mesh identity: {0}")]
    Key(#[from] KeyError),
    #[error("mesh tls: {0}")]
    Tls(String),
    #[error("bind the mesh endpoint at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
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
    connections: Mutex<HashMap<RaftNodeId, quinn::Connection>>,
    accept: Mutex<Option<OwnedTask>>,
}

#[derive(Clone)]
pub struct QuicRouter {
    inner: Arc<Inner>,
}

impl QuicRouter {
    pub fn bind(bind: SocketAddr, identity: KeyMaterial) -> Result<Self, QuicMeshError> {
        let peers = Arc::new(Peers::default());
        let presented = Arc::new(Presented::new(&identity)?);
        let mut tls = pin::server_config(presented, Arc::clone(&peers) as Arc<dyn PinSet>)?;
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
                connections: Mutex::new(HashMap::new()),
                accept: Mutex::new(None),
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
        let pin =
            self.inner.peers.pin_of(target).ok_or_else(|| {
                std::io::Error::other(format!("node {target} has no admitted pin"))
            })?;
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e| std::io::Error::other(format!("node {target}'s address {addr:?}: {e}")))?;
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
}

impl MeshNetwork for QuicRouter {
    async fn register(&self, _node_id: RaftNodeId, sender: mpsc::Sender<RpcRequest>) {
        let endpoint = self.inner.endpoint.clone();
        let task = OwnedTask::spawn(accept(endpoint, sender));
        if let Some(previous) = self.inner.accept.lock().await.replace(task) {
            let _ = previous.stop().await;
        }
    }

    async fn deregister(&self, _node_id: RaftNodeId) {
        if let Some(task) = self.inner.accept.lock().await.take() {
            let _ = task.stop().await;
        }
        self.inner.connections.lock().await.clear();
        self.inner.endpoint.close(0u32.into(), b"deregistered");
    }
}

async fn accept(endpoint: quinn::Endpoint, inbox: mpsc::Sender<RpcRequest>) {
    let mut connections = JoinSet::new();
    while let Some(incoming) = endpoint.accept().await {
        while connections.try_join_next().is_some() {}
        connections.spawn(serve_connection(incoming, inbox.clone()));
    }
}

async fn serve_connection(incoming: quinn::Incoming, inbox: mpsc::Sender<RpcRequest>) {
    let conn = match incoming.await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::debug!(%e, "mesh handshake refused");
            return;
        }
    };
    let mut streams = JoinSet::new();
    while let Ok((send, recv)) = conn.accept_bi().await {
        while streams.try_join_next().is_some() {}
        streams.spawn(serve_stream(send, recv, inbox.clone()));
    }
}

async fn serve_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    inbox: mpsc::Sender<RpcRequest>,
) {
    let answered = async {
        let request = read_frame(&mut recv).await?;
        let envelope: RpcEnvelope =
            serde_json::from_slice(&request).map_err(std::io::Error::other)?;
        let reply = envelope.deliver(&inbox).await?;
        write_frame(&mut send, &reply).await?;
        send.finish().map_err(std::io::Error::other)
    };
    if let Err(e) = answered.await {
        tracing::debug!(%e, "mesh stream ended without a reply");
        let _ = send.reset(1u32.into());
    }
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
        let request =
            serde_json::to_vec(envelope).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
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
