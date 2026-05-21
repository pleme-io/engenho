//! R6 integration tests — the K8s resource store works end-to-end
//! via real openraft replication across 3 nodes.

use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    default_config, InProcessRouter, Reason, ResourceCommand, ResourceKey, StoreMesh,
};

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

#[tokio::test]
async fn single_node_store_put_and_get() {
    let router = InProcessRouter::new();
    let cfg = default_config("engenho-store-r6-single").unwrap();
    let mesh = StoreMesh::start(1, "in-process://1".into(), router, cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    let cmd = ResourceCommand::Put {
        key: pod_key("podinfo"),
        value: serde_json::json!({"spec": {"image": "ghcr.io/stefanprodan/podinfo:6.12.0"}}),
        reason: Reason::Operator,
    };
    let result = mesh.propose(cmd).await.unwrap();
    // openraft writes a blank entry at index 1 during initialize;
    // our client_write lands at index 2.
    assert!(result.applied_index >= 1);
    let applied_index = result.applied_index;
    assert_eq!(result.op, engenho_store::command::ResourceOp::Created);

    let stored = mesh.get(&pod_key("podinfo")).await.unwrap();
    assert_eq!(
        stored.get("spec").unwrap().get("image").unwrap(),
        "ghcr.io/stefanprodan/podinfo:6.12.0"
    );
    // resource_version reflects the Raft log index it was committed at.
    let rv = stored
        .get("metadata")
        .unwrap()
        .get("resourceVersion")
        .unwrap();
    assert_eq!(rv, &serde_json::json!(applied_index.to_string()));

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn put_then_patch_then_delete_full_lifecycle() {
    let router = InProcessRouter::new();
    let cfg = default_config("engenho-store-r6-lifecycle").unwrap();
    let mesh = StoreMesh::start(2, "in-process://2".into(), router, cfg)
        .await
        .unwrap();
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);

    // PUT
    mesh.propose(ResourceCommand::Put {
        key: pod_key("p"),
        value: serde_json::json!({"spec": {"image": "v1", "replicas": 3}}),
        reason: Reason::Operator,
    })
    .await
    .unwrap();

    // PATCH the image only
    let patched = mesh
        .propose(ResourceCommand::Patch {
            key: pod_key("p"),
            patch: serde_json::json!({"spec": {"image": "v2"}}),
            reason: Reason::Controller,
        })
        .await
        .unwrap();
    assert_eq!(patched.op, engenho_store::command::ResourceOp::Patched);
    let stored = mesh.get(&pod_key("p")).await.unwrap();
    assert_eq!(stored.get("spec").unwrap().get("image").unwrap(), "v2");
    // replicas survived the merge
    assert_eq!(stored.get("spec").unwrap().get("replicas").unwrap(), 3);

    // DELETE
    let deleted = mesh
        .propose(ResourceCommand::Delete {
            key: pod_key("p"),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    assert_eq!(deleted.op, engenho_store::command::ResourceOp::Deleted);
    assert!(mesh.get(&pod_key("p")).await.is_none());

    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn three_node_store_replicates_resource_writes() {
    let router = InProcessRouter::new();
    let cfg = default_config("engenho-store-r6-three").unwrap();
    let mesh_1 = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router.clone(), cfg.clone())
            .await
            .unwrap(),
    );
    let mesh_2 = Arc::new(
        StoreMesh::start(2, "in-process://2".into(), router.clone(), cfg.clone())
            .await
            .unwrap(),
    );
    let mesh_3 = Arc::new(
        StoreMesh::start(3, "in-process://3".into(), router.clone(), cfg.clone())
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
        .unwrap();

    // Wait for a leader.
    let mut leader_found = false;
    for _ in 0..40 {
        if mesh_1.is_leader().await || mesh_2.is_leader().await || mesh_3.is_leader().await {
            leader_found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(leader_found);

    let leader = if mesh_1.is_leader().await {
        &mesh_1
    } else if mesh_2.is_leader().await {
        &mesh_2
    } else {
        &mesh_3
    };

    // Propose 3 distinct resources through the leader.
    for name in ["a", "b", "c"] {
        leader
            .propose(ResourceCommand::Put {
                key: pod_key(name),
                value: serde_json::json!({"spec": {"image": format!("img-{name}")}}),
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    // All 3 nodes must reach apply index 3.
    assert!(mesh_1.wait_for_applied(3, Duration::from_secs(5)).await);
    assert!(mesh_2.wait_for_applied(3, Duration::from_secs(5)).await);
    assert!(mesh_3.wait_for_applied(3, Duration::from_secs(5)).await);

    // Every node must serve every resource. Read on a non-leader
    // proves replication actually delivered the data.
    for name in ["a", "b", "c"] {
        for mesh in [&mesh_1, &mesh_2, &mesh_3] {
            let v = mesh.get(&pod_key(name)).await;
            assert!(v.is_some(), "node{} missing pod/{}", mesh.node_id(), name);
            assert_eq!(
                v.unwrap().get("spec").unwrap().get("image").unwrap(),
                &serde_json::json!(format!("img-{name}"))
            );
        }
    }

    // LIST on every node must show the same 3 pods.
    for mesh in [&mesh_1, &mesh_2, &mesh_3] {
        let list = mesh.list("", "v1", "Pod", Some("default")).await;
        assert_eq!(list.len(), 3, "node{} list disagrees", mesh.node_id());
    }

    Arc::try_unwrap(mesh_1).ok().unwrap().terminate().await.unwrap();
    Arc::try_unwrap(mesh_2).ok().unwrap().terminate().await.unwrap();
    Arc::try_unwrap(mesh_3).ok().unwrap().terminate().await.unwrap();
}
