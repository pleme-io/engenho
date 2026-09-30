use std::path::Path;

use engenho_revoada::attestation::NodeIdentity;
use engenho_revoada::consensus::{InProcessRouter, RaftStore, TypeConfig, default_config};
use openraft::Raft;
use openraft::Vote;
use openraft::raft::VoteRequest;

const NODE: u64 = 1;
const TERM: u64 = 7;

fn store_at(dir: &Path) -> RaftStore {
    RaftStore::durable(NodeIdentity::from_seed([0x11; 32]), dir).expect("durable store opens")
}

async fn boot(dir: &Path) -> Raft<TypeConfig> {
    let cfg = default_config("durable-vote").expect("config");
    let store = store_at(dir);
    Raft::<TypeConfig>::new(NODE, cfg, InProcessRouter::new(), store.clone(), store)
        .await
        .expect("raft boots")
}

async fn ask(raft: &Raft<TypeConfig>, candidate: u64) -> bool {
    raft.vote(VoteRequest::new(Vote::new(TERM, candidate), None))
        .await
        .expect("vote rpc")
        .vote_granted
}

async fn restart(raft: Raft<TypeConfig>, dir: &Path) -> Raft<TypeConfig> {
    raft.shutdown().await.expect("shutdown");
    drop(raft);
    boot(dir).await
}

#[tokio::test]
async fn restarted_node_refuses_a_second_vote_in_the_same_term() {
    let dir = tempfile::tempdir().expect("tempdir");

    let life = boot(dir.path()).await;
    assert!(
        ask(&life, 3).await,
        "a fresh node grants its first vote in a term"
    );
    let life = restart(life, dir.path()).await;

    assert!(
        !ask(&life, 2).await,
        "a restarted node granted a second vote in term {TERM}: its first vote was not durable"
    );
    life.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restarted_node_still_grants_the_candidate_it_voted_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let life = boot(dir.path()).await;
    assert!(ask(&life, 3).await);
    let life = restart(life, dir.path()).await;

    assert!(
        ask(&life, 3).await,
        "re-granting the candidate already voted for is safe"
    );
    life.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_corrupt_hard_state_is_refused_not_read_as_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("raft-hard-state.json"), b"{not json").expect("write");
    assert!(
        RaftStore::durable(NodeIdentity::from_seed([0x11; 32]), dir.path()).is_err(),
        "an unreadable hard state must not boot as a node that never voted"
    );
}

#[tokio::test]
async fn a_committed_promotion_survives_a_restart() {
    use engenho_revoada::NodeId;
    use engenho_revoada::consensus::{RaftMesh, Reason, RoleAssignment};
    use engenho_revoada::membership::NodeRole;
    use std::time::Duration;

    let dir = tempfile::tempdir().expect("tempdir");
    let identity = NodeIdentity::from_seed([0x22; 32]);
    let start = || async {
        RaftMesh::start_durable(
            NODE,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("durable-log").expect("config"),
            identity.clone(),
            dir.path(),
        )
        .await
        .expect("mesh starts")
    };

    let mesh = start().await;
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    let promoted = NodeId::new([0xa1; 32]);
    let applied = mesh
        .propose(RoleAssignment::Promote {
            node_id: promoted,
            roles: [NodeRole::ApiServer].into_iter().collect(),
            reason: Reason::Operator,
        })
        .await
        .expect("propose");
    mesh.terminate().await.expect("terminate");

    let mesh = start().await;
    assert!(
        mesh.wait_for_leadership(Duration::from_secs(3)).await,
        "restarted singleton re-elects itself from its durable membership"
    );
    assert!(
        mesh.wait_for_applied(applied.applied_index, Duration::from_secs(3))
            .await
    );
    assert!(
        mesh.current_shape()
            .await
            .holders(NodeRole::ApiServer)
            .contains(&promoted),
        "the committed promotion was lost across a restart"
    );
    mesh.terminate().await.expect("terminate");
}
