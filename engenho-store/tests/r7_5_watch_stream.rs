//! R7.5b / C2 integration — every committed mutation emits a typed
//! watch signal on the per-watcher stream. Validates that controllers
//! + apiserver subscribers can react to changes in real time without
//! polling.
//!
//! Migrated from the legacy `tokio::sync::broadcast` fan-out to the
//! resumable watch backend (M0.1 item 3): `StoreMesh::watch()` is now
//! the LIVE-TAIL shim over `watch_from` — same semantics (attach from
//! the current revision forward, no replay), but it yields a typed
//! `WatchStream` whose `next()` returns `WatchSignal` and surfaces
//! overflow as a typed `WatchGone` instead of a silent `Lagged`.

use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh, WatchEventKind, WatchSignal,
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

/// Drain one Event from the stream (skipping any bookmarks).
async fn next_event(
    stream: &mut engenho_store::WatchStream,
) -> engenho_store::WatchEvent {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("signal arrived")
        {
            Some(Ok(WatchSignal::Event(ev))) => return ev,
            Some(Ok(WatchSignal::Bookmark(_))) => continue,
            Some(Err(g)) => panic!("unexpected WatchGone: {g:?}"),
            None => panic!("stream closed unexpectedly"),
        }
    }
}

#[tokio::test]
async fn put_emits_added_watch_event() {
    let store = boot().await;
    let mut watch = store.watch().await.unwrap();
    assert_eq!(store.watch_subscriber_count().await, 1);

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p1"),
            value: json!({"spec": {"image": "v1"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev = next_event(&mut watch).await;
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
    let mut watch = store.watch().await.unwrap();

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {"v": 1}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {"v": 2}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    let ev1 = next_event(&mut watch).await;
    let ev2 = next_event(&mut watch).await;
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
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let mut watch = store.watch().await.unwrap(); // subscribe AFTER the put

    store
        .propose(ResourceCommand::patch(
            pod_key("p"),
            json!({"spec": {"v": 2}}),
            Reason::Operator,
        ))
        .await
        .unwrap();

    let ev = next_event(&mut watch).await;
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
            value: json!({"spec": {"image": "podinfo"}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let mut watch = store.watch().await.unwrap();

    store
        .propose(ResourceCommand::Delete {
            key: pod_key("p"),
            expected: None,
            reason: Reason::Operator,
            deletion_timestamp: None,
        })
        .await
        .unwrap();

    let ev = next_event(&mut watch).await;
    assert_eq!(ev.kind, WatchEventKind::Deleted);
    assert_eq!(ev.key.name, "p");
    // The Deleted event MUST carry the last-known object (the
    // tombstone), never Null. Regression for the "Deleted-event
    // prior-object" bug: catalog.apply captures the pre-image before
    // removal + returns it in the Change, so the event's object is the
    // real prior spec.
    assert!(
        !ev.object.is_null(),
        "Deleted event object must be the prior object, not Null"
    );
    assert_eq!(
        ev.object.get("spec").unwrap().get("image").unwrap(),
        "podinfo",
        "Deleted event carries the original spec"
    );

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn multiple_subscribers_each_get_every_event() {
    let store = boot().await;
    let mut sub_a = store.watch().await.unwrap();
    let mut sub_b = store.watch().await.unwrap();
    let mut sub_c = store.watch().await.unwrap();
    assert_eq!(store.watch_subscriber_count().await, 3);

    store
        .propose(ResourceCommand::Put {
            key: pod_key("p1"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    store
        .propose(ResourceCommand::Put {
            key: pod_key("p2"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // Each subscriber sees both Add events independently (per-watcher
    // channel — no shared broadcast ring).
    for sub in [&mut sub_a, &mut sub_b, &mut sub_c] {
        for expected_name in ["p1", "p2"] {
            let ev = next_event(sub).await;
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
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();

    // The LIVE-TAIL shim (`watch()`) attaches from the current revision
    // forward with no replay — exactly the legacy semantics.
    let mut watch = store.watch().await.unwrap();
    // Tiny pause to let any in-flight delivery settle.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // No event ready (live-tail does not replay history; use
    // `watch_from` for that).
    let immediate = tokio::time::timeout(Duration::from_millis(50), watch.next()).await;
    assert!(
        immediate.is_err(),
        "live-tail subscriber should NOT see past events; that's watch_from's job"
    );

    // But future events do flow.
    store
        .propose(ResourceCommand::Put {
            key: pod_key("fresh"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let ev = next_event(&mut watch).await;
    assert_eq!(ev.key.name, "fresh");

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

#[tokio::test]
async fn watch_event_resource_version_matches_revision() {
    let store = boot().await;
    let mut watch = store.watch().await.unwrap();

    let result = store
        .propose(ResourceCommand::Put {
            key: pod_key("p"),
            value: json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
    let ev = next_event(&mut watch).await;
    // The WatchEvent.resource_version equals the global MVCC revision
    // stamped on the mutation (decoupled from the Raft log index).
    assert_eq!(ev.resource_version, result.revision);
    // First real mutation → revision 1 (the blank init entry at Raft
    // index 1 consumed no revision).
    assert_eq!(result.revision, 1);

    drop(watch);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}
