//! T3.7, store level — a watch with a selector sees an object LEAVE its view.
//!
//! | behaviour | pinned by |
//! |---|---|
//! | upstream's `watch-configmaps-label-changed` conformance test, event for event | `watch_configmaps_label_changed_*` |
//! | every change a transaction commits reaches a live watcher, not only its first | `every_change_of_a_transaction_reaches_a_live_watcher_*` |
//! | a replay shares the ring's changes rather than copying them | `a_replay_shares_the_ring_*` |
//!
//! The selector here is the test's own predicate over labels. The apiserver's
//! `Selectors` lives above this crate; its router adopts
//! [`engenho_store::project`] over [`engenho_store::WatchStream::next_change`]
//! in place of filtering each event on its post-image.

mod support;

use std::time::Duration;

use engenho_store::command::{Reason, ResourceCommand, TxnOp};
use engenho_store::revision::Revision;
use engenho_store::{
    ChangeSignal, ResourceKey, ResourceValue, StoreMesh, WatchEvent, WatchEventKind, WatchOpts,
    WatchStream, project,
};
use serde_json::json;
use support::{
    bytes_allocated_by, durable_mesh, memory_mesh, replay_as_events, seed_bulk_then_one_configmap,
};

/// test/e2e/apimachinery/watch.go@v1.34: `watchConfigMapLabelKey`.
const LABEL_KEY: &str = "watch-this-configmap";
/// test/e2e/apimachinery/watch.go@v1.34: `toBeChangedLabelValue`.
const LABEL_VALUE: &str = "label-changed-and-restored";

/// `watchConfigMaps(ctx, f, "", toBeChangedLabelValue)`: the selector
/// `watch-this-configmap in (label-changed-and-restored)`.
fn selected(obj: &ResourceValue) -> bool {
    obj["metadata"]["labels"][LABEL_KEY] == LABEL_VALUE
}

fn configmap() -> ResourceKey {
    ResourceKey::namespaced(
        "",
        "v1",
        "ConfigMap",
        "default",
        "e2e-watch-test-label-changed",
    )
}

/// Write `value` to the ConfigMap and return the object the store now holds
/// (what upstream's `Create` / `updateConfigMap` return) and its revision.
async fn write(mesh: &StoreMesh, value: ResourceValue) -> (ResourceValue, Revision) {
    let applied = mesh
        .propose(ResourceCommand::Put {
            key: configmap(),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .expect("propose");
    let stored = mesh.get(&configmap()).await.expect("the ConfigMap exists");
    (stored, Revision(applied.revision))
}

/// The ConfigMap with its label set to `label` and `data.mutation` to
/// `mutation`, when there is one (`setConfigMapData(cm, "mutation", ..)`).
fn cm(label: &str, mutation: Option<&str>) -> ResourceValue {
    let mut value = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "e2e-watch-test-label-changed",
            "namespace": "default",
            "labels": { LABEL_KEY: label },
        },
    });
    if let Some(m) = mutation {
        value["data"] = json!({ "mutation": m });
    }
    value
}

/// Read `stream` through the selector until the change at `through` has
/// gone by, collecting what the watch sends. Bounded: a stream that stalls
/// fails the test rather than hanging it.
async fn projected_through(stream: &mut WatchStream, through: Revision) -> Vec<WatchEvent> {
    let mut sent = Vec::new();
    loop {
        let signal = tokio::time::timeout(Duration::from_secs(5), stream.next_change())
            .await
            .expect("the stream reaches the last write")
            .expect("the stream is open")
            .expect("the stream is not gone");
        let ChangeSignal::Change(change) = signal else {
            continue;
        };
        if let Some(event) = project(&change, selected) {
            sent.push(event);
        }
        if change.revision >= through {
            return sent;
        }
    }
}

/// `object` as a DELETED event carries it: the prior image, at the revision
/// of the change that took it out of the view.
fn at_revision(mut object: ResourceValue, revision: Revision) -> ResourceValue {
    object["metadata"]["resourceVersion"] = json!(revision.get().to_string());
    object
}

/// test/e2e/apimachinery/watch.go@v1.34, "should observe an object deletion
/// if it stops meeting the requirements of the selector"
/// (Testname: watch-configmaps-label-changed), step for step.
///
/// Upstream's `expectEvent` skips events it is not looking for; this asserts
/// the exact sequence, which is what upstream's cacher sends for these
/// writes (`cacheWatcher.convertToWatchEvent`): the relabel out is DELETED
/// carrying the object as the watch last saw it, the write made while the
/// object is outside the view sends nothing, and the relabel back in is
/// ADDED.
async fn assert_watch_configmaps_label_changed(mesh: &StoreMesh) {
    let start = mesh.current_revision().await;
    let mut watch = mesh
        .watch_from(WatchOpts::live_tail(start, 64))
        .await
        .expect("creating a watch on configmaps with a certain label");

    let (created, _) = write(mesh, cm(LABEL_VALUE, None)).await;
    let (first_update, _) = write(mesh, cm(LABEL_VALUE, Some("1"))).await;
    let (_, relabelled_out) = write(mesh, cm("wrong-value", Some("1"))).await;
    let (_, second_update) = write(mesh, cm("wrong-value", Some("2"))).await;
    let (restored, _) = write(mesh, cm(LABEL_VALUE, Some("2"))).await;
    let (third_update, _) = write(mesh, cm(LABEL_VALUE, Some("3"))).await;
    let deleted = Revision(
        mesh.propose(ResourceCommand::delete(configmap(), Reason::Operator))
            .await
            .expect("deleting the configmap")
            .revision,
    );
    assert!(
        relabelled_out < second_update && second_update < deleted,
        "every write committed its own revision"
    );

    let sent = projected_through(&mut watch, deleted).await;
    let kinds: Vec<WatchEventKind> = sent.iter().map(|ev| ev.kind).collect();
    assert_eq!(
        kinds,
        [
            WatchEventKind::Added,
            WatchEventKind::Modified,
            WatchEventKind::Deleted,
            WatchEventKind::Added,
            WatchEventKind::Modified,
            WatchEventKind::Deleted,
        ],
        "the event sequence upstream's cacher sends for these writes"
    );
    let got: Vec<(WatchEventKind, &ResourceValue)> =
        sent.iter().map(|ev| (ev.kind, &ev.object)).collect();
    let deleted_first = at_revision(first_update.clone(), relabelled_out);
    let deleted_third = at_revision(third_update.clone(), deleted);
    assert_eq!(
        got,
        vec![
            (WatchEventKind::Added, &created),
            (WatchEventKind::Modified, &first_update),
            // The relabel out: DELETED, from the object the watch last saw.
            (WatchEventKind::Deleted, &deleted_first),
            // The second update is outside the view: nothing.
            // The relabel back in: ADDED, the watch has not seen it since.
            (WatchEventKind::Added, &restored),
            (WatchEventKind::Modified, &third_update),
            (WatchEventKind::Deleted, &deleted_third),
        ]
    );
    assert!(
        sent.iter()
            .all(|ev| ev.resource_version != second_update.get()),
        "expectNoEvent: the write made outside the view sends nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_configmaps_label_changed_memory() {
    let mesh = memory_mesh("t3-7-relabel-memory").await;
    assert_watch_configmaps_label_changed(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_configmaps_label_changed_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-7-relabel-durable").await;
    assert_watch_configmaps_label_changed(&mesh).await;
}

/// A transaction commits every key it touches at one revision, and the ring
/// replays all of them. A live watcher must see the same: fed only the
/// first change, it missed the rest for good, while a watcher that joined a
/// moment later replayed them.
async fn assert_every_change_of_a_transaction_reaches_a_live_watcher(mesh: &StoreMesh) {
    let start = mesh.current_revision().await;
    let mut live = mesh
        .watch_from(WatchOpts::live_tail(start, 64))
        .await
        .expect("live watch");
    let keys: Vec<ResourceKey> = ["a", "b", "c"]
        .into_iter()
        .map(|name| ResourceKey::namespaced("", "v1", "ConfigMap", "default", name))
        .collect();
    let committed = Revision(
        mesh.propose(ResourceCommand::Txn {
            compares: Vec::new(),
            success: keys
                .iter()
                .map(|key| TxnOp::Put {
                    key: key.clone(),
                    value: json!({ "data": { "k": "v" } }),
                })
                .collect(),
            failure: Vec::new(),
            reason: Reason::Operator,
        })
        .await
        .expect("propose")
        .revision,
    );
    assert_eq!(committed, start.next(), "one transaction, one revision");

    let mut replay = mesh
        .watch_from(WatchOpts::live_tail(start, 64))
        .await
        .expect("replaying watch");
    for (who, stream) in [("live", &mut live), ("replay", &mut replay)] {
        let mut seen = Vec::new();
        while let Some(Ok(signal)) =
            tokio::time::timeout(Duration::from_secs(2), stream.next_change())
                .await
                .ok()
                .flatten()
        {
            if let ChangeSignal::Change(change) = signal {
                assert_eq!(change.revision, committed, "{who}");
                seen.push(change.key.clone());
            }
            if seen.len() == keys.len() {
                break;
            }
        }
        assert_eq!(seen, keys, "{who}: every key of the transaction");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_change_of_a_transaction_reaches_a_live_watcher_memory() {
    let mesh = memory_mesh("t3-7-txn-memory").await;
    assert_every_change_of_a_transaction_reaches_a_live_watcher(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_change_of_a_transaction_reaches_a_live_watcher_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-7-txn-durable").await;
    assert_every_change_of_a_transaction_reaches_a_live_watcher(&mesh).await;
}

/// The catalog builds each change once and the ring, the fan-out and every
/// replay share it. Opening a replay of megabytes of history costs the
/// channel, not a copy of each change's two images; the copy is paid only
/// when a change is turned into an event, by the reader that wants one.
/// The positive control is that read, over the same ring.
async fn assert_a_replay_shares_the_ring(mesh: &StoreMesh) {
    seed_bulk_then_one_configmap(mesh).await;
    let (events, ring) = replay_as_events(mesh).await;
    assert_eq!(events, 201, "200 Secret rewrites and one ConfigMap");
    assert!(
        ring > 2 * 1024 * 1024,
        "positive control: the ring as events is only {ring} bytes, so the bound below \
         would prove nothing"
    );

    let (replay, opened) =
        bytes_allocated_by(mesh.watch_from(WatchOpts::from_revision(Revision::ZERO))).await;
    let mut replay = replay.expect("nothing is compacted yet");
    assert!(
        opened < 64 * 1024,
        "opening a replay of the ring allocated {opened} bytes against {ring} to read it as \
         events: the replay is copying the ring instead of sharing it"
    );
    let mut changes = 0usize;
    while let Some(Ok(signal)) = replay.try_next_change() {
        changes += usize::from(matches!(signal, ChangeSignal::Change(_)));
    }
    assert_eq!(changes, 201, "the shared replay is the whole ring");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_shares_the_ring_memory() {
    let mesh = memory_mesh("t3-7-share-memory").await;
    assert_a_replay_shares_the_ring(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_shares_the_ring_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-7-share-durable").await;
    assert_a_replay_shares_the_ring(&mesh).await;
}
