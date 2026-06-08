//! R6.5 item-2 integration tests — the fjall-backed durable store
//! makes single-node restart NON-DESTRUCTIVE.
//!
//! Every test is single-node, in-process (`InProcessRouter`), tokio
//! async, over a temp keyspace dir that is dropped + reopened in a
//! fresh `StoreMesh` instance. No real cluster, no network, no NATS.
//! Durability is proven by terminating the mesh (dropping Raft +
//! store, flushing) and re-opening the SAME dir.

use std::time::Duration;

use engenho_store::{
    InProcessRouter, Reason, ResourceCommand, ResourceKey, Revision, StoreMesh, VersionMeta,
    default_config,
};

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "engenho-fjall-it-{}-{suffix}",
        std::process::id()
    ))
}

/// Test 1 — resources + revision state survive a restart.
#[tokio::test]
async fn restart_preserves_resources_and_revision() {
    let dir = temp_dir("resources-revision");
    let _ = std::fs::remove_dir_all(&dir);

    // ── First lifetime: write a, write b, patch a, delete b. ───────
    let pre_meta_a;
    let final_revision;
    {
        let router = InProcessRouter::new();
        let cfg = default_config("durable-resources-1").unwrap();
        let (mesh, fresh) =
            StoreMesh::start_or_resume(1, "in-process://1".into(), router, cfg, &dir)
                .await
                .unwrap();
        assert!(fresh, "first boot on a fresh dir initializes");
        assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

        // Put a (rev 1)
        mesh.propose(ResourceCommand::Put {
            key: pod_key("a"),
            value: serde_json::json!({"spec": {"image": "a:v1"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
        // Put b (rev 2)
        mesh.propose(ResourceCommand::Put {
            key: pod_key("b"),
            value: serde_json::json!({"spec": {"image": "b:v1"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
        // Patch a (rev 3)
        mesh.propose(ResourceCommand::patch(
            pod_key("a"),
            serde_json::json!({"spec": {"image": "a:v2"}}),
            Reason::Controller,
        ))
        .await
        .unwrap();
        // Delete b (rev 4)
        let deleted = mesh
            .propose(ResourceCommand::Delete {
                key: pod_key("b"),
                expected: None,
                reason: Reason::Operator,
                deletion_timestamp: None,
            })
            .await
            .unwrap();
        final_revision = deleted.revision;
        assert_eq!(final_revision, 4);

        // Capture pre-restart per-key meta for a.
        let cat = mesh.current_catalog().await;
        pre_meta_a = cat.get_with_meta(&pod_key("a")).map(|(_, m)| m).unwrap();
        assert_eq!(cat.current_revision, Revision(4));

        mesh.terminate().await.unwrap();
    }

    // ── Second lifetime: reopen the SAME dir WITHOUT re-init. ──────
    let router = InProcessRouter::new();
    let cfg = default_config("durable-resources-2").unwrap();
    let (mesh2, fresh) =
        StoreMesh::start_or_resume(1, "in-process://1".into(), router, cfg, &dir)
            .await
            .unwrap();
    assert!(!fresh, "reopen of an initialized dir does NOT re-initialize");
    assert!(mesh2.wait_for_leadership(Duration::from_secs(3)).await);

    // a survived + has the patched value.
    let a = mesh2.get(&pod_key("a")).await.expect("a survived restart");
    assert_eq!(a.get("spec").unwrap().get("image").unwrap(), "a:v2");
    // b's delete survived.
    assert!(mesh2.get(&pod_key("b")).await.is_none(), "b stayed deleted");

    // Revision counter survived.
    let cat = mesh2.current_catalog().await;
    assert_eq!(cat.current_revision, Revision(final_revision));

    // Per-key VersionMeta for a is byte-identical across the restart.
    let post_meta_a = cat.get_with_meta(&pod_key("a")).map(|(_, m)| m).unwrap();
    assert_eq!(post_meta_a, pre_meta_a);

    // metadata.resourceVersion on a == the persisted revision string
    // (the decoupled-from-Raft-index invariant holds across restart).
    let rv = a.get("metadata").unwrap().get("resourceVersion").unwrap();
    assert_eq!(rv, &serde_json::json!(post_meta_a.mod_revision.to_string()));

    mesh2.terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Test 2 — the Raft log + vote survive; boot path does not clobber.
#[tokio::test]
async fn restart_preserves_raft_log_and_does_not_reinitialize() {
    let dir = temp_dir("raft-log-vote");
    let _ = std::fs::remove_dir_all(&dir);

    {
        let router = InProcessRouter::new();
        let cfg = default_config("durable-log-1").unwrap();
        let (mesh, fresh) =
            StoreMesh::start_or_resume(2, "in-process://2".into(), router, cfg, &dir)
                .await
                .unwrap();
        assert!(fresh);
        assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
        for name in ["x", "y", "z"] {
            mesh.propose(ResourceCommand::Put {
                key: pod_key(name),
                value: serde_json::json!({"spec": {}}),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        }
        mesh.terminate().await.unwrap();
    }

    // Reopen: the store reports initialized BEFORE raft starts driving.
    let router = InProcessRouter::new();
    let cfg = default_config("durable-log-2").unwrap();
    let (mesh2, fresh) =
        StoreMesh::start_or_resume(2, "in-process://2".into(), router, cfg, &dir)
            .await
            .unwrap();
    // initialize_if_fresh saw persisted state and SKIPPED initialize.
    assert!(!fresh, "resume does not re-initialize the cluster");
    assert!(mesh2.is_initialized().await, "store reports initialized");
    assert!(mesh2.wait_for_leadership(Duration::from_secs(3)).await);

    // Calling initialize_if_fresh again is a no-op (already-initialized).
    assert!(
        !mesh2.initialize_if_fresh().await.unwrap(),
        "second initialize_if_fresh is a no-op"
    );

    // Resources from the prior lifetime are present (proves log +
    // applied state were persisted + replayed).
    for name in ["x", "y", "z"] {
        assert!(
            mesh2.get(&pod_key(name)).await.is_some(),
            "pod {name} survived restart"
        );
    }

    mesh2.terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Test 3 — current_revision + per-key meta survive enough puts that
/// the revision is well above 1 (compacted_revision round-trip is
/// proven at the unit layer in state.rs / fjall_store.rs).
#[tokio::test]
async fn restart_preserves_high_revision_and_per_key_meta() {
    let dir = temp_dir("high-revision");
    let _ = std::fs::remove_dir_all(&dir);

    let pre_meta;
    {
        let router = InProcessRouter::new();
        let cfg = default_config("durable-high-1").unwrap();
        let (mesh, fresh) =
            StoreMesh::start_or_resume(3, "in-process://3".into(), router, cfg, &dir)
                .await
                .unwrap();
        assert!(fresh);
        assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

        // 10 distinct puts (rev 1..10), then 3 bumps on "hot" so its
        // VersionMeta version climbs.
        for i in 1..=10u64 {
            mesh.propose(ResourceCommand::Put {
                key: pod_key(&format!("k{i}")),
                value: serde_json::json!({"i": i}),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        }
        for v in 0..3 {
            mesh.propose(ResourceCommand::Put {
                key: pod_key("hot"),
                value: serde_json::json!({"v": v}),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        }
        let cat = mesh.current_catalog().await;
        assert!(cat.current_revision.get() >= 11);
        pre_meta = cat.get_with_meta(&pod_key("hot")).map(|(_, m)| m).unwrap();
        assert!(pre_meta.version >= 3);
        mesh.terminate().await.unwrap();
    }

    let router = InProcessRouter::new();
    let cfg = default_config("durable-high-2").unwrap();
    let (mesh2, fresh) =
        StoreMesh::start_or_resume(3, "in-process://3".into(), router, cfg, &dir)
            .await
            .unwrap();
    assert!(!fresh);
    assert!(mesh2.wait_for_leadership(Duration::from_secs(3)).await);

    let cat = mesh2.current_catalog().await;
    assert!(cat.current_revision.get() >= 11, "high revision survived");
    let post_meta = cat.get_with_meta(&pod_key("hot")).map(|(_, m)| m).unwrap();
    assert_eq!(post_meta, pre_meta, "per-key VersionMeta survived restart");

    mesh2.terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Test 4 — a fresh dir first-boots + initializes; first Put is
/// revision 1 (matches the r6 single_node semantics).
#[tokio::test]
async fn empty_store_first_boot_initializes() {
    let dir = temp_dir("first-boot");
    let _ = std::fs::remove_dir_all(&dir);

    let router = InProcessRouter::new();
    let cfg = default_config("durable-fresh").unwrap();
    let (mesh, fresh) =
        StoreMesh::start_or_resume(4, "in-process://4".into(), router, cfg, &dir)
            .await
            .unwrap();
    assert!(fresh, "fresh dir DOES initialize");
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let result = mesh
        .propose(ResourceCommand::Put {
            key: pod_key("first"),
            value: serde_json::json!({"spec": {"image": "v1"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    // Blank entry on initialize consumes no revision → first Put is 1.
    assert_eq!(result.revision, 1);
    assert_eq!(result.op, engenho_store::ResourceOp::Created);

    let stored = mesh.get(&pod_key("first")).await.unwrap();
    let (_, meta) = mesh
        .current_catalog()
        .await
        .get_with_meta(&pod_key("first"))
        .map(|(v, m)| (v.clone(), m))
        .unwrap();
    assert_eq!(meta, VersionMeta::created_at(Revision(1)));
    assert_eq!(
        stored.get("metadata").unwrap().get("resourceVersion").unwrap(),
        &serde_json::json!("1")
    );

    mesh.terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
