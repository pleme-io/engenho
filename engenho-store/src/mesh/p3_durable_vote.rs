use std::time::Duration;

use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{VoteRequest, VoteResponse};
use openraft::{BasicNode, Vote};

use crate::mesh::StoreMesh;
use crate::testing::{FaultNet, FaultRouter, fast_config};
use crate::type_config::RaftNodeId;

const VOTER: RaftNodeId = 3;
const TERM: u64 = 5;
const REOPEN: Duration = Duration::from_secs(10);

async fn ask(net: &FaultNet, candidate: RaftNodeId, term: u64) -> VoteResponse<RaftNodeId> {
    let node = BasicNode {
        addr: format!("fault-router://{VOTER}"),
    };
    let mut client = net.node(candidate).new_client(VOTER, &node).await;
    client
        .vote(
            VoteRequest::new(Vote::new(term, candidate), None),
            RPCOption::new(Duration::from_secs(2)),
        )
        .await
        .unwrap_or_else(|e| panic!("vote from {candidate} at term {term}: {e}"))
}

async fn durable(net: &FaultNet, dir: &std::path::Path) -> StoreMesh<FaultRouter> {
    let deadline = tokio::time::Instant::now() + REOPEN;
    loop {
        match StoreMesh::start_durable(
            VOTER,
            format!("fault-router://{VOTER}"),
            net.node(VOTER),
            fast_config("p3-durable-vote").unwrap(),
            dir.join("store"),
        )
        .await
        {
            Ok(mesh) => return mesh,
            Err(e) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(%e, "store still held by the stopped node");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("reopen node {VOTER}'s store: {e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_voted_and_restarted_grants_no_lower_vote_in_that_term() {
    let dir = tempfile::tempdir().unwrap();
    let net = FaultNet::new();

    let voter = durable(&net, dir.path()).await;
    let first = ask(&net, 2, TERM).await;
    assert!(
        first.vote_granted,
        "the first candidate was refused: {first:?}"
    );
    assert_eq!(first.vote, Vote::new(TERM, 2));
    voter.terminate().await.unwrap();

    let voter = durable(&net, dir.path()).await;
    let second = ask(&net, 1, TERM).await;
    assert!(
        !second.vote_granted,
        "node {VOTER} voted for 2 at term {TERM}, restarted, and voted for 1 in the same term: \
         {second:?}"
    );
    assert_eq!(
        second.vote,
        Vote::new(TERM, 2),
        "the vote it holds after the restart"
    );

    let later = ask(&net, 1, TERM + 1).await;
    assert!(
        later.vote_granted,
        "the restarted node grants no vote at all, so the refusal above proves nothing: {later:?}"
    );
    voter.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ephemeral_node_forgets_its_vote_across_a_restart_which_is_what_the_durable_test_rules_out()
 {
    let net = FaultNet::new();
    let start = || {
        StoreMesh::start(
            VOTER,
            format!("fault-router://{VOTER}"),
            net.node(VOTER),
            fast_config("p3-ephemeral-vote").unwrap(),
        )
    };
    let voter = start().await.unwrap();
    assert!(ask(&net, 2, TERM).await.vote_granted);
    assert!(
        !ask(&net, 1, TERM).await.vote_granted,
        "without a restart the lower vote is refused"
    );
    voter.terminate().await.unwrap();

    let voter = start().await.unwrap();
    assert!(
        ask(&net, 1, TERM).await.vote_granted,
        "an in-memory store kept a vote across a restart, so the durable test cannot tell \
         durability from something else"
    );
    voter.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_higher_candidate_id_in_the_same_term_is_a_newer_vote_under_openraft_adv_leader_ids() {
    let dir = tempfile::tempdir().unwrap();
    let net = FaultNet::new();
    let voter = durable(&net, dir.path()).await;
    assert!(ask(&net, 1, TERM).await.vote_granted);
    voter.terminate().await.unwrap();
    let voter = durable(&net, dir.path()).await;
    let higher = ask(&net, 2, TERM).await;
    assert!(higher.vote_granted, "{higher:?}");
    voter.terminate().await.unwrap();
}
