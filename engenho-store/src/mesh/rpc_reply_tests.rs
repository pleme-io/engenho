use std::time::Duration;

use openraft::error::{Fatal, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, SnapshotMeta, StoredMembership, Vote};

use super::{StoreMesh, default_config};
use crate::network::{InProcessNetwork, InProcessRouter};

async fn stopped_peer(name: &str) -> (StoreMesh, InProcessNetwork) {
    let mut router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let mesh = StoreMesh::start(1, "in-process://1".into(), router.clone(), cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    mesh.raft.shutdown().await.unwrap();
    let client = router.new_client(1, &BasicNode::default()).await;
    (mesh, client)
}

fn option() -> RPCOption {
    RPCOption::new(Duration::from_secs(2))
}

fn assert_peer_stopped<E: std::error::Error>(err: RPCError<u64, BasicNode, RaftError<u64, E>>) {
    match err {
        RPCError::RemoteError(remote) => {
            assert_eq!(remote.target, 1);
            assert!(matches!(remote.source, RaftError::Fatal(Fatal::Stopped)));
        }
        other => panic!("the peer's raft error was lost: {other}"),
    }
}

fn peer_vote() -> Vote<u64> {
    Vote::new(9, 2)
}

#[tokio::test]
async fn a_failed_append_entries_reaches_the_caller_as_the_peers_raft_error() {
    let (_mesh, mut client) = stopped_peer("rpc-reply-append").await;
    let rpc = AppendEntriesRequest {
        vote: peer_vote(),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    let err = client.append_entries(rpc, option()).await.unwrap_err();
    assert_peer_stopped(err);
}

#[tokio::test]
async fn a_failed_vote_reaches_the_caller_as_the_peers_raft_error() {
    let (_mesh, mut client) = stopped_peer("rpc-reply-vote").await;
    let err = client
        .vote(VoteRequest::new(peer_vote(), None), option())
        .await
        .unwrap_err();
    assert_peer_stopped(err);
}

#[tokio::test]
async fn a_failed_install_snapshot_reaches_the_caller_as_the_peers_raft_error() {
    let (_mesh, mut client) = stopped_peer("rpc-reply-snapshot").await;
    let rpc = InstallSnapshotRequest {
        vote: peer_vote(),
        meta: SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "rpc-reply".into(),
        },
        offset: 0,
        data: vec![],
        done: true,
    };
    let err = client.install_snapshot(rpc, option()).await.unwrap_err();
    assert_peer_stopped(err);
}
