//! T2.1-store — `StoreMesh::quiesce` stops, and AWAITS, every background
//! task the mesh owns: the raft RPC pump and the store's bookmark ticker.
//!
//! Pinned here, against both backends:
//!
//! | behaviour | why it is not an implementation detail |
//! |---|---|
//! | the report names how each task ended: `Cancelled`, then `AlreadyStopped` | a shutdown reads it; a panicked task must never read as a clean stop |
//! | after quiesce a peer RPC is refused at the send (`Unreachable`) | a request accepted into a queue nobody drains is lost silently; a refused one is retried elsewhere |
//! | after quiesce the mesh terminates, and a durable dir reopens at once | quiesce is the first step of a stop, not a dead end |
//!
//! The strong-count half — a task parked mid-tick with an upgraded `Arc` is
//! awaited, not leaked, so the count is back to 1 when quiesce returns —
//! needs the stores' private state, so it lives in the unit tests of
//! `owned_task`, `watch_backend`, `store` and `fjall_store`.

use std::time::Duration;

use engenho_store::{InProcessRouter, Quiesced, StoreMesh, TaskStop, default_config};
use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork as _, RaftNetworkFactory as _};
use openraft::raft::VoteRequest;
use openraft::{BasicNode, Vote};

const NODE: u64 = 1;
const ADDR: &str = "in-process://1";
const LEADERSHIP: Duration = Duration::from_secs(10);

/// What a peer sees when it sends this node one RPC.
#[derive(Debug, PartialEq)]
enum PeerSees {
    /// The node answered (whatever the answer was).
    Answer,
    /// The send was refused: the peer knows at once the node is gone.
    Unreachable,
    /// Anything else — e.g. the request was accepted, then dropped unanswered.
    Other(String),
}

/// One vote RPC to `NODE` from a stranger at term 0, through the router.
/// A live node answers it (and refuses the vote); a quiesced node must not
/// be reachable at all.
async fn peer_vote(router: &InProcessRouter) -> PeerSees {
    let mut factory = router.clone();
    let mut client = factory
        .new_client(
            NODE,
            &BasicNode {
                addr: ADDR.to_owned(),
            },
        )
        .await;
    let answer = client
        .vote(
            VoteRequest::new(Vote::new(0, 2), None),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await;
    match answer {
        Ok(_) => PeerSees::Answer,
        Err(RPCError::Unreachable(_)) => PeerSees::Unreachable,
        Err(other) => PeerSees::Other(other.to_string()),
    }
}

async fn quiesce_case(mesh: &StoreMesh, router: &InProcessRouter) {
    assert_eq!(
        peer_vote(router).await,
        PeerSees::Answer,
        "precondition: a live node answers a peer"
    );

    assert_eq!(
        mesh.quiesce().await,
        Quiesced {
            rpc_pump: TaskStop::Cancelled,
            bookmark_ticker: TaskStop::Cancelled,
        },
        "the first quiesce stops both owned tasks"
    );

    assert_eq!(
        peer_vote(router).await,
        PeerSees::Unreachable,
        "after quiesce a peer must be refused at the send, not queued and dropped"
    );

    assert_eq!(
        mesh.quiesce().await,
        Quiesced {
            rpc_pump: TaskStop::AlreadyStopped,
            bookmark_ticker: TaskStop::AlreadyStopped,
        },
        "quiesce is idempotent and says so"
    );
}

#[tokio::test]
async fn quiesce_stops_and_awaits_every_owned_task_memory() {
    let router = InProcessRouter::new();
    let cfg = default_config("t21-quiesce-mem").unwrap();
    let mesh = StoreMesh::start(NODE, ADDR.into(), router.clone(), cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(LEADERSHIP).await);

    quiesce_case(&mesh, &router).await;
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn quiesce_stops_and_awaits_every_owned_task_fjall() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let router = InProcessRouter::new();
    let cfg = default_config("t21-quiesce-fjall").unwrap();
    let (mesh, _fresh) =
        StoreMesh::start_or_resume(NODE, ADDR.into(), router.clone(), cfg.clone(), &path)
            .await
            .unwrap();
    assert!(mesh.wait_for_leadership(LEADERSHIP).await);

    quiesce_case(&mesh, &router).await;
    mesh.terminate().await.unwrap();

    // The directory is free the moment terminate returns.
    let (again, fresh) = StoreMesh::start_or_resume(NODE, ADDR.into(), router, cfg, &path)
        .await
        .unwrap();
    assert!(!fresh, "the reopen resumes the state quiesce left behind");
    again.terminate().await.unwrap();
}
