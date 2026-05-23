//! R7.5b / C2 integration — every committed mutation emits a
//! typed WatchEvent on the broadcast channel. Validates that
//! controllers + apiserver subscribers can react to changes
//! in real time without polling.

use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh, WatchEventKind,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

async fn boot() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("store-watch-c2").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

#[tokio::test]
async fn put_emits_added_watch_event() {
    let store = boot().await;
    let mut watch = store.watch();
    assert_eq!(store.watch_subscriber_count(), 1);

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p1"),
            value: json!({"spec": {"image": "v1"}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .expect("event arrived")
        .expect("not lagged");
    assert_eq!(ev.kind, WatchEventKind::Added);
    assert_eq!(ev.key.name, "p1");
    assert!(ev.resource_version >= 1);
    assert_eq!(ev.object.get("spec").unwrap().get("image").unwrap(), "v1");

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn put_then_put_emits_added_then_modified() {
    let store = boot().await;
    let mut watch = store.watch();

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {"v": 1}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {"v": 2}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev1 = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    let ev2 = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev1.kind, WatchEventKind::Added);
    assert_eq!(ev2.kind, WatchEventKind::Modified);
    // ev2's resource_version is strictly larger than ev1's.
    assert!(ev2.resource_version > ev1.resource_version);

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn patch_emits_modified_event() {
    let store = boot().await;
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {"v": 1, "label": "a"}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let mut watch = store.watch(); // subscribe AFTER the put

    store
        .propose(ResourceCommand::Patch {
            key: pod_key("p"),
            patch: json!({"spec": {"v": 2}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev.kind, WatchEventKind::Modified);
    assert_eq!(ev.object.get("spec").unwrap().get("v").unwrap(), 2);
    // Patch preserves other fields.
    assert_eq!(ev.object.get("spec").unwrap().get("label").unwrap(), "a");

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn delete_emits_deleted_event() {
    let store = boot().await;
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let mut watch = store.watch();

    store
        .propose(ResourceCommand::Delete {
            key: pod_key("p"),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev.kind, WatchEventKind::Deleted);
    assert_eq!(ev.key.name, "p");

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn multiple_subscribers_each_get_every_event() {
    let store = boot().await;
    let mut sub_a = store.watch();
    let mut sub_b = store.watch();
    let mut sub_c = store.watch();
    assert_eq!(store.watch_subscriber_count(), 3);

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p1"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p2"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Each subscriber sees both Add events independently.
    for sub in [&mut sub_a, &mut sub_b, &mut sub_c] {
        for expected_name in ["p1", "p2"] {
            let ev = tokio::time::timeout(Duration::from_secs(2), sub.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ev.kind, WatchEventKind::Added);
            assert_eq!(ev.key.name, expected_name);
        }
    }

    drop((sub_a, sub_b, sub_c));
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn late_subscriber_does_not_see_history() {
    let store = boot().await;
    // Apply BEFORE subscribing.
    store
        .propose(ResourceCommand::Put {
            key: pod_key("ancient"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let mut watch = store.watch();
    // Tiny pause to let any in-flight delivery settle.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // No event ready.
    assert!(
        watch.try_recv().is_err(),
        "late subscriber should NOT see past events; that's JetStream's job"
    );

    // But future events do flow.
    store
        .propose(ResourceCommand::Put {
            key: pod_key("fresh"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev.key.name, "fresh");

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn watch_event_resource_version_matches_apply_index() {
    let store = boot().await;
    let mut watch = store.watch();

    let result = store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {}}),
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    // The WatchEvent.resource_version equals the apply_index returned to the proposer.
    assert_eq!(ev.resource_version, result.applied_index);

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
