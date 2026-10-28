use std::time::Duration;

use crate::testing::Cluster;
use crate::{ReadConsistency, ReadRefused, Reason, ResourceCommand, ResourceKey};

const ELECT: Duration = Duration::from_secs(10);
const APPLY: Duration = Duration::from_secs(10);
const READ_WAIT: Duration = Duration::from_millis(500);

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn put(name: &str, image: &str) -> ResourceCommand {
    ResourceCommand::Put {
        key: pod(name),
        value: serde_json::json!({"spec": {"image": image}}),
        expected: None,
        reason: Reason::Operator,
    }
}

fn image(value: &serde_json::Value) -> Option<&str> {
    value
        .pointer("/spec/image")
        .and_then(serde_json::Value::as_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_2_of_3_the_minority_neither_elects_nor_commits_and_heals_to_the_majority() {
    let cluster = Cluster::start("p3-minority", 3).await.unwrap();
    let old = cluster.leader(ELECT).await.expect("no first leader");
    let before = cluster
        .node(old)
        .propose(put("before", "v1"))
        .await
        .unwrap();
    assert!(
        cluster
            .all_applied(&cluster.ids(), before.applied_index, APPLY)
            .await
    );

    cluster.isolate(old);
    let majority = cluster.others(old);

    let minority_write = tokio::time::timeout(
        Duration::from_millis(800),
        cluster.node(old).propose(put("minority", "lost")),
    )
    .await;
    assert!(
        !matches!(minority_write, Ok(Ok(_))),
        "the isolated node {old} committed a write with no quorum: {minority_write:?}"
    );

    let new = cluster
        .leader_among(&majority, ELECT)
        .await
        .expect("the majority elected no leader");
    assert_ne!(new, old);
    let after = cluster.node(new).propose(put("after", "v2")).await.unwrap();
    assert!(
        cluster
            .all_applied(&majority, after.applied_index, APPLY)
            .await
    );
    for &id in &majority {
        assert!(
            cluster.node(id).get(&pod("minority")).await.is_none(),
            "node {id}"
        );
    }
    assert!(
        cluster.net().lost(old, new) > 0,
        "the partition cut nothing"
    );

    cluster.heal();
    assert!(
        cluster
            .node(old)
            .wait_for_applied(after.applied_index, APPLY)
            .await,
        "the healed node {old} never caught up to the majority"
    );
    for id in cluster.ids() {
        let node = cluster.node(id);
        assert!(
            node.get(&pod("minority")).await.is_none(),
            "node {id} kept the minority write"
        );
        assert!(node.get(&pod("before")).await.is_some(), "node {id}");
        assert!(node.get(&pod("after")).await.is_some(), "node {id}");
    }
    assert!(!cluster.node(old).is_leader().await);
    cluster.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_leader_serves_no_stale_latest_read() {
    let cluster = Cluster::start("p3-stale-read", 3).await.unwrap();
    let old = cluster.leader(ELECT).await.expect("no first leader");
    let first = cluster.node(old).propose(put("x", "v1")).await.unwrap();
    assert!(
        cluster
            .all_applied(&cluster.ids(), first.applied_index, APPLY)
            .await
    );

    cluster.isolate(old);
    let majority = cluster.others(old);
    let new = cluster
        .leader_among(&majority, ELECT)
        .await
        .expect("the majority elected no leader");
    let second = cluster.node(new).propose(put("x", "v2")).await.unwrap();
    assert!(
        cluster
            .all_applied(&majority, second.applied_index, APPLY)
            .await
    );

    let at_new = cluster
        .node(new)
        .get_consistent(&pod("x"), ReadConsistency::Latest, READ_WAIT)
        .await
        .expect("the quorum leader refused a Latest read");
    assert_eq!(at_new.as_ref().and_then(image), Some("v2"));

    let at_old = cluster
        .node(old)
        .get_consistent(&pod("x"), ReadConsistency::Latest, READ_WAIT)
        .await;
    let stale = matches!(&at_old, Ok(Some(v)) if image(v) == Some("v1"));
    assert!(
        !stale,
        "node {old}, deposed by a partition it has not noticed (is_leader={}), served the \
         overwritten v1 as Latest after node {new} committed v2 at index {}: {at_old:?}",
        cluster.node(old).is_leader().await,
        second.applied_index,
    );
    assert!(
        matches!(at_old, Err(ReadRefused::NotLinearizable { .. })),
        "{at_old:?}"
    );

    let follower = majority.iter().copied().find(|id| *id != new).unwrap();
    let at_follower = cluster
        .node(follower)
        .get_consistent(&pod("x"), ReadConsistency::Latest, READ_WAIT)
        .await;
    assert_eq!(
        at_follower,
        Err(ReadRefused::NotLinearizable { leader: Some(new) })
    );

    cluster.heal();
    assert!(
        cluster
            .node(old)
            .wait_for_applied(second.applied_index, APPLY)
            .await
    );
    assert_eq!(
        cluster
            .node(old)
            .get(&pod("x"))
            .await
            .as_ref()
            .and_then(image),
        Some("v2")
    );
    cluster.terminate().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_and_delayed_appends_still_replicate_once_the_link_delivers() {
    let cluster = Cluster::start("p3-drop-delay", 3).await.unwrap();
    let leader = cluster.leader(ELECT).await.expect("no leader");
    let [a, b] = cluster.others(leader)[..] else {
        panic!("three nodes");
    };
    cluster.net().drop_next(leader, a, 3);
    cluster.net().delay(leader, b, Duration::from_millis(20));

    let written = cluster
        .node(leader)
        .propose(put("lossy", "v1"))
        .await
        .unwrap();
    assert!(
        cluster
            .all_applied(&cluster.ids(), written.applied_index, APPLY)
            .await
    );
    assert!(cluster.net().lost(leader, a) >= 3);
    assert!(cluster.net().delivered(leader, b) > 0);
    for id in cluster.ids() {
        assert!(
            cluster.node(id).get(&pod("lossy")).await.is_some(),
            "node {id}"
        );
    }
    cluster.terminate().await.unwrap();
}
