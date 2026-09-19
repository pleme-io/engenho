//! T3.9a-lock: a watch whose resume point is ahead of the store is refused
//! by the store, under the catalog lock its registration holds.
//!
//! | behaviour | why it is not an implementation detail |
//! |---|---|
//! | `watch_from` past the current revision answers `AheadOfStore{requested, current}` and registers no watcher, on both backends and through the mesh | a client whose resourceVersion the store has not reached holds a revision from a history this store does not have; serving it the store's history would leave its cache silently stale |
//! | a rewind (a snapshot install) between a caller's read of the revision and its `watch_from` is refused, not served | the apiserver reads the revision before it registers; a check that trusts that read lets the rewind through |
//! | a watch from exactly the current revision, and the live tail, are still served after a rewind | the refusal covers only revisions the store has not reached |
//!
//! The live tail's own read-then-register is pinned per backend in
//! `store::tests` and `fjall_store::tests` (`a_rewind_queued_behind_a_live_tail_cannot_split_it`):
//! forcing that interleaving needs the backend's lock, which is private.

mod support;

use engenho_store::command::{LoggedCommand, Reason, ResourceCommand};
use engenho_store::{
    FjallStore, InMemoryStore, ResourceKey, Revision, StoreMesh, TypeConfig, WatchGone, WatchOpts,
    WatchStream,
};
use openraft::storage::{RaftSnapshotBuilder as _, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use serde_json::json;

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

/// The watch surface both backends expose as inherent methods, so one body
/// runs against each.
trait Backend: RaftStateMachine<TypeConfig> + Send {
    async fn head(&self) -> Revision;
    async fn watch(&self, opts: WatchOpts) -> Result<WatchStream, WatchGone>;
    async fn live_tail(&self) -> Result<WatchStream, WatchGone>;
    async fn watchers(&self) -> usize;
}

macro_rules! backend {
    ($store:ty) => {
        impl Backend for $store {
            async fn head(&self) -> Revision {
                self.current_revision().await
            }
            async fn watch(&self, opts: WatchOpts) -> Result<WatchStream, WatchGone> {
                self.watch_from(opts).await
            }
            async fn live_tail(&self) -> Result<WatchStream, WatchGone> {
                self.watch_subscribe().await
            }
            async fn watchers(&self) -> usize {
                self.watch_subscriber_count().await
            }
        }
    };
}
backend!(InMemoryStore);
backend!(FjallStore);

/// Apply `n` Puts at log indexes `1..=n`, one revision each, prefixed so two
/// stores' histories differ.
async fn apply_puts<S: Backend>(store: &mut S, prefix: &str, n: u64) {
    for index in 1..=n {
        let name = format!("{prefix}-{index}");
        let entry = Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(1, 0),
                index,
            },
            payload: EntryPayload::Normal(LoggedCommand::proposed(ResourceCommand::put(
                pod(&name),
                json!({"metadata": {"name": name, "namespace": "default"}}),
                Reason::Operator,
            ))),
        };
        store.apply(vec![entry]).await.expect("apply");
    }
}

/// `dst` moves to revision 5, the caller reads that revision (what the
/// apiserver does before it registers), then a snapshot of `src` at
/// revision 2 is installed over `dst`. Every resume point the rewind took
/// back is refused with the revision the store is now at; the rewound
/// history is served from its own revision.
async fn a_rewind_after_the_read_is_refused_not_served<S: Backend>(mut src: S, mut dst: S) {
    apply_puts(&mut src, "src", 2).await;
    let snap = src
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("build a snapshot at revision 2");

    apply_puts(&mut dst, "dst", 5).await;
    let read_before_registering = dst.head().await;
    assert_eq!(read_before_registering, Revision(5));

    dst.install_snapshot(&snap.meta, snap.snapshot)
        .await
        .expect("install the older snapshot");
    assert_eq!(
        dst.head().await,
        Revision(2),
        "precondition: the install rewound the store"
    );

    let watchers = dst.watchers().await;
    for requested in 3..=read_before_registering.get() {
        assert_eq!(
            dst.watch(WatchOpts::from_revision(Revision(requested)))
                .await
                .err(),
            Some(WatchGone::AheadOfStore {
                requested: Revision(requested),
                current: Revision(2),
            }),
            "a watch from {requested} after a rewind to 2 must be refused, not served from 2"
        );
    }
    assert_eq!(
        dst.watchers().await,
        watchers,
        "a refused watch registers no watcher"
    );

    assert!(
        dst.watch(WatchOpts::from_revision(Revision(2)))
            .await
            .is_ok(),
        "the rewound store serves a watch from its own revision"
    );
    assert!(
        dst.live_tail().await.is_ok(),
        "the live tail attaches to the rewound store"
    );
}

#[tokio::test]
async fn memory_backend_refuses_a_resume_point_a_rewind_took_back() {
    a_rewind_after_the_read_is_refused_not_served(InMemoryStore::new(), InMemoryStore::new()).await;
}

#[tokio::test]
async fn fjall_backend_refuses_a_resume_point_a_rewind_took_back() {
    let tmp = tempfile::tempdir().expect("tempdir");
    a_rewind_after_the_read_is_refused_not_served(
        FjallStore::open(tmp.path().join("src")).expect("open src"),
        FjallStore::open(tmp.path().join("dst")).expect("open dst"),
    )
    .await;
}

/// Through the mesh, the entry point the apiserver calls: a watch from past
/// the current revision is refused with that revision and registers nothing;
/// a watch from exactly it is served.
async fn the_mesh_refuses_a_watch_ahead_of_it(mesh: &StoreMesh) {
    for i in 0..3 {
        support::put(mesh, pod(&format!("p{i}")), json!({"i": i})).await;
    }
    let head = mesh.current_revision().await;
    let watchers = mesh.watch_subscriber_count().await;
    for requested in [head.get() + 1, head.get() + 100] {
        assert_eq!(
            mesh.watch_from(WatchOpts::from_revision(Revision(requested)))
                .await
                .err(),
            Some(WatchGone::AheadOfStore {
                requested: Revision(requested),
                current: head,
            }),
            "a watch from {requested} over a store at {head} must be refused"
        );
    }
    assert_eq!(
        mesh.watch_subscriber_count().await,
        watchers,
        "a refused watch registers no watcher"
    );
    assert!(
        mesh.watch_from(WatchOpts::from_revision(head))
            .await
            .is_ok(),
        "a watch from exactly the current revision is served"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_mesh_refuses_a_watch_ahead_of_it() {
    let mesh = support::memory_mesh("t3-9a-memory").await;
    the_mesh_refuses_a_watch_ahead_of_it(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_mesh_refuses_a_watch_ahead_of_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mesh = support::durable_mesh(tmp.path(), "t3-9a-durable").await;
    the_mesh_refuses_a_watch_ahead_of_it(&mesh).await;
}
