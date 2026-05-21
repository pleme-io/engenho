//! R4 integration tests — every Raft commit emits a signed
//! attestation block; the chain verifies end-to-end.
//!
//! Exercises the real cryptographic path: ed25519-dalek signs,
//! BLAKE3 hashes, openraft commits, attestation chain verifies.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use engenho_revoada::attestation::{verify_chain, AttestationError, NodeIdentity};
use engenho_revoada::consensus::{
    default_config, InProcessRouter, RaftMesh, Reason, RoleAssignment,
};
use engenho_revoada::membership::NodeRole;
use engenho_revoada::NodeId;

fn promote_cmd(seed: u8) -> RoleAssignment {
    let mut roles = BTreeSet::new();
    roles.insert(NodeRole::ApiServer);
    RoleAssignment::Promote {
        node_id: NodeId::new([seed; 32]),
        roles,
        reason: Reason::Operator,
    }
}

#[tokio::test]
async fn raft_propose_appends_signed_attestation_block() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r4-single").unwrap();
    let identity = NodeIdentity::from_seed([0xa1; 32]);
    let mesh = RaftMesh::start_with_identity(
        1,
        "in-process://node-1".into(),
        router,
        cfg,
        identity.clone(),
    )
    .await
    .expect("raft start with identity");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    // Chain starts empty.
    assert!(mesh.attestation_chain().is_empty());

    // Propose 3 distinct assignments.
    for i in 1..=3 {
        mesh.propose(promote_cmd(i)).await.expect("propose");
    }
    // Wait for all to apply.
    assert!(mesh.wait_for_applied(3, Duration::from_secs(3)).await);

    // Chain has 3 blocks.
    let chain = mesh.attestation_chain();
    assert_eq!(chain.len(), 3);

    // Each block signed by our identity (leader).
    let blocks = chain.snapshot();
    for b in &blocks {
        assert_eq!(b.leader, identity.node_id());
        assert_eq!(b.leader_signature.len(), 64);
    }

    // Linkage: block[0].prev_hash = [0; 32]; block[i].prev_hash = blake3(block[i-1]).
    assert_eq!(blocks[0].prev_hash, [0; 32]);
    assert_eq!(blocks[1].prev_hash, blocks[0].blake3_hash());
    assert_eq!(blocks[2].prev_hash, blocks[1].blake3_hash());

    // Full chain verification using the standalone walker — proves
    // the chain is auditor-verifiable with just the bytes + the
    // public key (no other engenho infrastructure needed).
    verify_chain(&blocks).expect("chain verifies end-to-end");

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn tampering_with_committed_chain_breaks_verification() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r4-tamper").unwrap();
    let identity = NodeIdentity::from_seed([0xb2; 32]);
    let mesh = RaftMesh::start_with_identity(
        2,
        "in-process://node-2".into(),
        router,
        cfg,
        identity,
    )
    .await
    .unwrap();
    mesh.initialize_singleton().await.unwrap();
    mesh.wait_for_leadership(Duration::from_secs(3)).await;

    mesh.propose(promote_cmd(7)).await.unwrap();
    mesh.propose(promote_cmd(8)).await.unwrap();

    let mut blocks = mesh.attestation_chain().snapshot();
    // Tamper with block 1's prev_hash to break linkage.
    blocks[1].prev_hash = [0xff; 32];
    let err = verify_chain(&blocks).unwrap_err();
    assert_eq!(err, AttestationError::BrokenLink { index: 1 });

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn three_node_raft_each_leader_signs_blocks_with_its_identity() {
    // Each Raft node has its own NodeIdentity; whichever node is
    // leader at the time of propose() signs the block.
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r4-three").unwrap();
    let id_1 = NodeIdentity::from_seed([0x01; 32]);
    let id_2 = NodeIdentity::from_seed([0x02; 32]);
    let id_3 = NodeIdentity::from_seed([0x03; 32]);

    let mesh_1 = Arc::new(
        RaftMesh::start_with_identity(
            1,
            "in-process://1".into(),
            router.clone(),
            cfg.clone(),
            id_1.clone(),
        )
        .await
        .unwrap(),
    );
    let mesh_2 = Arc::new(
        RaftMesh::start_with_identity(
            2,
            "in-process://2".into(),
            router.clone(),
            cfg.clone(),
            id_2.clone(),
        )
        .await
        .unwrap(),
    );
    let mesh_3 = Arc::new(
        RaftMesh::start_with_identity(
            3,
            "in-process://3".into(),
            router.clone(),
            cfg.clone(),
            id_3.clone(),
        )
        .await
        .unwrap(),
    );

    mesh_1
        .initialize_with_voters(vec![
            (1, "in-process://1".into()),
            (2, "in-process://2".into()),
            (3, "in-process://3".into()),
        ])
        .await
        .expect("3-node init");

    // Wait for a leader.
    for _ in 0..40 {
        if mesh_1.is_leader().await || mesh_2.is_leader().await || mesh_3.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (leader_mesh, leader_identity) = if mesh_1.is_leader().await {
        (&mesh_1, &id_1)
    } else if mesh_2.is_leader().await {
        (&mesh_2, &id_2)
    } else {
        (&mesh_3, &id_3)
    };

    leader_mesh.propose(promote_cmd(0xfe)).await.expect("propose");
    leader_mesh
        .wait_for_applied(1, Duration::from_secs(3))
        .await;

    // The leader's chain has the block signed by its identity.
    let chain = leader_mesh.attestation_chain();
    let blocks = chain.snapshot();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].leader, leader_identity.node_id());
    verify_chain(&blocks).expect("chain verifies");

    // Followers DON'T auto-receive the block at R4 (that's R4.5+);
    // their chains are still empty. We don't assert anything about
    // them — the R4 contract is "the proposing leader writes its
    // own chain", not "all nodes converge on the chain".
    let _ = (leader_mesh, leader_identity);

    Arc::try_unwrap(mesh_1).ok().unwrap().terminate().await.unwrap();
    Arc::try_unwrap(mesh_2).ok().unwrap().terminate().await.unwrap();
    Arc::try_unwrap(mesh_3).ok().unwrap().terminate().await.unwrap();
}

/// Audit scenario: an external auditor receives the chain bytes
/// + the leader's public key (via gossip's NodeId field) and
/// verifies independently. Proves the chain is portable + the
/// verification doesn't need engenho-revoada's internals.
#[tokio::test]
async fn external_auditor_verifies_serialized_chain() {
    let router = InProcessRouter::new();
    let cfg = default_config("revoada-test-r4-audit").unwrap();
    let identity = NodeIdentity::from_seed([0xc3; 32]);
    let mesh = RaftMesh::start_with_identity(
        7,
        "in-process://7".into(),
        router,
        cfg,
        identity,
    )
    .await
    .unwrap();
    mesh.initialize_singleton().await.unwrap();
    mesh.wait_for_leadership(Duration::from_secs(3)).await;

    for i in 1..=5 {
        mesh.propose(promote_cmd(i)).await.unwrap();
    }
    mesh.wait_for_applied(5, Duration::from_secs(3)).await;

    // Serialize the entire chain to JSON (the "wire" form an
    // auditor would see).
    let blocks = mesh.attestation_chain().snapshot();
    let wire = serde_json::to_string(&blocks).unwrap();

    // Auditor deserializes + verifies WITHOUT any reference to
    // engenho-revoada internals.
    let recovered: Vec<engenho_revoada::attestation::RoleAttestationBlock> =
        serde_json::from_str(&wire).unwrap();
    verify_chain(&recovered).expect("auditor verifies independently");
    assert_eq!(recovered.len(), 5);

    mesh.terminate().await.unwrap();
}
