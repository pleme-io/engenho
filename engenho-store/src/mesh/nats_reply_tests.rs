use std::time::Duration;

use openraft::error::{Fatal, InstallSnapshotError, RPCError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, SnapshotMeta, StoredMembership, Vote};
use serde::de::DeserializeOwned;

use super::{StoreMesh, default_config};
use crate::nats_listener::dispatch;
use crate::nats_network::{NatsRpcEnvelope, NatsRpcReply};
use crate::network::{InProcessRouter, remote_answer};

async fn running_peer(name: &str) -> StoreMesh {
    let mesh = StoreMesh::start(
        1,
        "nats://1".into(),
        InProcessRouter::new(),
        default_config(name).unwrap(),
    )
    .await
    .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    mesh
}

async fn over_the_wire<T, E>(
    mesh: &StoreMesh,
    envelope: NatsRpcEnvelope,
) -> Result<T, RPCError<u64, BasicNode, E>>
where
    T: DeserializeOwned,
    E: DeserializeOwned + std::error::Error,
{
    let request = serde_json::to_vec(&envelope).unwrap();
    let received: NatsRpcEnvelope = serde_json::from_slice(&request).unwrap();
    let reply = dispatch(&mesh.raft, received).await.unwrap();
    let decoded: NatsRpcReply<T, E> = serde_json::from_slice(&reply).unwrap();
    remote_answer(1, decoded)
}

fn assert_peer_stopped<E: std::error::Error>(err: RPCError<u64, BasicNode, RaftError<u64, E>>) {
    match err {
        RPCError::RemoteError(remote) => {
            assert_eq!(remote.target, 1);
            assert!(matches!(remote.source, RaftError::Fatal(Fatal::Stopped)));
        }
        other => panic!("the peer's raft error was lost on the NATS wire: {other}"),
    }
}

fn append() -> NatsRpcEnvelope {
    NatsRpcEnvelope::AppendEntries(AppendEntriesRequest {
        vote: Vote::new(9, 2),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    })
}

fn snapshot() -> NatsRpcEnvelope {
    NatsRpcEnvelope::InstallSnapshot(InstallSnapshotRequest {
        vote: Vote::new(9, 2),
        meta: SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "nats-reply".into(),
        },
        offset: 0,
        data: vec![],
        done: true,
    })
}

#[tokio::test]
async fn a_served_vote_comes_back_over_the_nats_wire_as_ok() {
    let mesh = running_peer("nats-reply-ok").await;
    let answer: Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> =
        over_the_wire(
            &mesh,
            NatsRpcEnvelope::Vote(VoteRequest::new(Vote::new(0, 2), None)),
        )
        .await;
    assert!(answer.is_ok(), "{answer:?}");
}

#[tokio::test]
async fn every_failed_rpc_comes_back_over_the_nats_wire_as_the_peers_raft_error() {
    let mesh = running_peer("nats-reply-err").await;
    mesh.raft.shutdown().await.unwrap();

    let err = over_the_wire::<AppendEntriesResponse<u64>, RaftError<u64>>(&mesh, append())
        .await
        .unwrap_err();
    assert_peer_stopped(err);

    let err = over_the_wire::<VoteResponse<u64>, RaftError<u64>>(
        &mesh,
        NatsRpcEnvelope::Vote(VoteRequest::new(Vote::new(9, 2), None)),
    )
    .await
    .unwrap_err();
    assert_peer_stopped(err);

    let err = over_the_wire::<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>(
        &mesh,
        snapshot(),
    )
    .await
    .unwrap_err();
    assert_peer_stopped(err);
}
