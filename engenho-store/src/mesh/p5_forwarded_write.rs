use std::time::Duration;

use crate::mesh::{StoreError, StoreMesh};
use crate::network::{InProcessRouter, MeshNetwork};
use crate::testing::{Cluster, fast_config};
use crate::{Reason, ResourceCommand, ResourceKey, ResourceOp, Revision};

const ELECT: Duration = Duration::from_secs(10);
const APPLY: Duration = Duration::from_secs(10);

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

fn put(name: &str, expected: Option<Revision>) -> ResourceCommand {
    ResourceCommand::Put {
        key: config_map(name),
        value: serde_json::json!({"data": {"written": name}}),
        expected,
        reason: Reason::Operator,
    }
}

async fn a_followers_write_is_the_leaders_and_is_read_back_on_the_follower<N: MeshNetwork>(
    cluster: Cluster<N>,
) {
    let leader = cluster.leader(ELECT).await.expect("no leader");
    let follower = cluster.others(leader)[0];
    let on = cluster.node(follower);

    let written = on.propose(put("forwarded", None)).await.unwrap();
    assert_eq!(written.op, ResourceOp::Created);
    assert!(
        on.get(&config_map("forwarded")).await.is_some(),
        "the follower does not read its own forwarded write on return"
    );
    assert!(
        cluster
            .all_applied(&cluster.ids(), written.applied_index, APPLY)
            .await
    );
    assert_eq!(
        cluster
            .node(leader)
            .get_with_meta(&config_map("forwarded"))
            .await
            .map(|(_, meta)| meta.mod_revision),
        Some(Revision(written.revision)),
        "the follower's answer is not the leader's commit"
    );

    let stale = on
        .propose(put("forwarded", Some(Revision(written.revision - 1))))
        .await
        .unwrap();
    assert_eq!(
        stale.op,
        ResourceOp::Conflict,
        "a forwarded compare-and-swap is not judged by the leader"
    );
    cluster.terminate().await.unwrap();
}

macro_rules! over_every_mesh {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn in_process() {
                let cluster = crate::testing::Cluster::start(stringify!($name), 3)
                    .await
                    .unwrap();
                super::$name(cluster).await;
            }

            #[cfg(feature = "mesh-quic")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn quic() {
                let cluster = crate::testing::quic_loopback(stringify!($name), 3)
                    .await
                    .unwrap();
                super::$name(cluster).await;
            }
        }
    )*};
}

over_every_mesh!(a_followers_write_is_the_leaders_and_is_read_back_on_the_follower);

#[tokio::test]
async fn a_write_on_a_node_that_knows_no_leader_is_refused_as_no_leader() {
    let mesh = StoreMesh::start(
        7,
        "in-process://7".into(),
        InProcessRouter::new(),
        fast_config("p5-no-leader").unwrap(),
    )
    .await
    .unwrap();
    let refused = tokio::time::timeout(ELECT, mesh.propose(put("nowhere", None)))
        .await
        .expect("a write with no leader known hung");
    assert!(matches!(refused, Err(StoreError::NoLeader)), "{refused:?}");
    mesh.terminate().await.unwrap();
}
