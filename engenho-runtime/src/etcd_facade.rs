//! `MeshEtcdStore` — the producer `engenho-etcd` never had.
//!
//! ★ THE GAP THIS CLOSES. `engenho-etcd` shipped the whole façade —
//! vendored upstream protos, the `/registry` keyspace measured against a
//! 699-key oracle from rio's live k3s, a `Kv` service, a `Watch` service
//! with history replay and compaction cancels, `Maintenance` — and 48
//! passing tests. Measured 2026-08-30:
//!
//! ```text
//! $ grep -rln 'engenho-etcd' --include=Cargo.toml . | grep -v '^./engenho-etcd'
//! Cargo.toml          (the workspace member list, and nothing else)
//! $ grep -rn 'engenho_etcd' --include=*.rs . | grep -v '^./engenho-etcd'
//! (nothing)
//! ```
//!
//! No crate depended on it, `runtime.rs` never mentioned it, and nothing
//! listened on :2379. Instance #9 of "type + backend + no producer", and
//! the most consequential one yet: the whole POINT of the façade was to be
//! an **oracle** — point upstream's real kube-apiserver at engenho's
//! :2379 and every Kubernetes conformance suite in existence becomes a test
//! of engenho-store against the genuine article. A façade nobody can dial
//! buys none of that.
//!
//! ★ WHY THE TRAITS ARE ASYNC NOW. They were synchronous, which is very
//! probably why this never got wired: `StoreMesh` is async through and
//! through, so a sync trait over it forces one of two bad answers. Blocking
//! inside the runtime risks deadlocking the reactor the store itself needs;
//! a cached snapshot means `etcdctl get` returns the cluster as it was at
//! the last refresh — a stale answer that looks exactly like a correct one,
//! which is the specific failure `engenho-etcd`'s own header forbids.
//! Making the trait match the store was three signatures and four test
//! attributes.
//!
//! ★ READ-ONLY, AND THAT IS A DECISION WITH A DATE ON IT. `Put`,
//! `DeleteRange` and `Txn` are NOT served. engenho's apiserver holds the
//! store directly and never speaks etcd, so nothing internal needs them;
//! what an external client would need them for is running upstream's
//! kube-apiserver against us, which is the Tier-B payoff and a separate
//! piece of work. Serving reads today is what makes `etcdctl get`,
//! `snapshot save` and every backup tool work — and a write path that
//! silently dropped writes would be far worse than one that is absent.

use std::sync::Arc;
use std::time::Duration;

use engenho_etcd::pb::mvccpb::{Event, KeyValue};
use engenho_etcd::server::{
    EtcdReadStore, EtcdRevision, EtcdStatusStore, EtcdWatchStore, OpenedWatch, RangeAt, StoreGone,
    WatchEnd, WatchFeed, WatchStart, WatchStep,
};
use engenho_store::resource::ResourceKey;
use engenho_store::watch_backend::{WATCH_CHANNEL_CAPACITY, WatchGone, WatchOpts, WatchSignal};
use engenho_store::{DEFAULT_HISTORY_CAPACITY, Revision, StoreMesh, WatchStream};

/// The `/registry` path for one stored object, or `None` when the kind is
/// not in the catalog.
///
/// ★ THE PLURAL COMES FROM THE CATALOG, NEVER FROM `kind + "s"`. `Endpoints`
/// pluralizes to `endpoints`, `NetworkPolicy` to `networkpolicies`. A
/// derived plural does not error — it produces a key nobody writes to, and
/// a `Range` over it returns empty, which reads exactly like an empty
/// cluster. That is the single most dangerous failure this whole crate has,
/// and it is why the catalog is consulted rather than a rule applied.
#[must_use]
pub fn registry_path(key: &ResourceKey) -> Option<String> {
    let d = engenho_types::generated_v1_34::catalog::RESOURCE_CATALOG
        .iter()
        .find(|d| d.group == key.group && d.kind == key.kind)?;
    Some(
        engenho_etcd::keyspace::object_key(
            d.group,
            d.plural,
            d.namespaced,
            key.namespace.as_deref(),
            &key.name,
        )
        .key,
    )
}

/// [`registry_path`], kept only when it lies under `prefix` — the one
/// selection rule every read below applies (Range, history replay, and the
/// live watch), written once so the three cannot disagree about what a
/// prefix contains.
fn registry_path_under(key: &ResourceKey, prefix: &str) -> Option<String> {
    registry_path(key).filter(|path| path.starts_with(prefix))
}

/// One stored object rendered onto the etcd wire.
///
/// `create_revision` / `mod_revision` / `version` come from the store's own
/// `VersionMeta`, which is an exact mirror of etcd's triple — that
/// correspondence is what makes this a façade rather than a simulation. A
/// client doing a compare-and-swap on `mod_revision` is comparing against
/// the same counter engenho's own preconditions use.
fn to_kv(
    path: String,
    value: &serde_json::Value,
    meta: &engenho_store::revision::VersionMeta,
) -> KeyValue {
    KeyValue {
        key: path.into_bytes(),
        create_revision: i64::try_from(meta.create_revision.0).unwrap_or(i64::MAX),
        mod_revision: i64::try_from(meta.mod_revision.0).unwrap_or(i64::MAX),
        version: i64::try_from(meta.version).unwrap_or(i64::MAX),
        value: serde_json::to_vec(value).unwrap_or_default(),
        lease: 0,
    }
}

/// The etcd façade over a live `StoreMesh`.
///
/// ★ HOLDS A `Weak`, NOT AN `Arc`, AND THAT IS NOT A DETAIL. The façade
/// lives in a detached listener task and is cloned into three services
/// (Kv, Watch, Maintenance). Strong references there keep the entire store
/// — and its Raft log, and its fjall handles — alive for the life of the
/// process, so a graceful shutdown can never reclaim it. Measured on the
/// first run: `StoreStillShared { strong_count: 4 }` across eight tests,
/// which is not a test artifact but a real leak of the whole store behind
/// a port nobody is using any more.
///
/// This is the SECOND time this exact shape appeared in this file's
/// neighbourhood — `WeakKubeletApi` exists for the :10250 listener for the
/// identical reason. Any future detached listener that holds cluster state
/// should start from a `Weak`.
#[derive(Clone)]
pub struct MeshEtcdStore {
    store: std::sync::Weak<StoreMesh>,
}

impl std::fmt::Debug for MeshEtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshEtcdStore").finish_non_exhaustive()
    }
}

impl MeshEtcdStore {
    /// New façade over `store`.
    #[must_use]
    pub fn new(store: &Arc<StoreMesh>) -> Self {
        Self {
            store: Arc::downgrade(store),
        }
    }

    /// The live store, or [`StoreGone`] once the node has shut down.
    ///
    /// ★ GONE IS AN ANSWER, NOT AN EMPTY ONE (T3.8). A dropped store means
    /// the process is terminating. Every accessor below used to degrade to
    /// an empty/zero answer — a `Range` of nothing at revision 0, which is
    /// exactly what an empty cluster looks like, so a backup tool dialled
    /// during a shutdown would have saved an empty keyspace and reported
    /// success. Now every one returns `StoreGone`, which the services send
    /// as gRPC `Unavailable`. Still no panic: the listener task is being
    /// torn down in the same breath, and taking the shutdown path down with
    /// a panic would turn a clean stop into a crash.
    fn live(&self) -> Result<Arc<StoreMesh>, StoreGone> {
        self.store.upgrade().ok_or(StoreGone)
    }

    /// Every object whose `/registry` path starts with `prefix`.
    ///
    /// Visits every stored object and filters. That is O(n) per Range and
    /// deliberately so at this stage: the alternative is a second index
    /// keyed by etcd path, which would be a copy of the store's own keying
    /// that could drift from it. A prefix index belongs here once a
    /// measurement says the scan hurts, not before.
    ///
    /// ★ ONE GUARD, AND ONLY THE MATCHES ARE CLONED (T3.2b). This used to
    /// clone the whole catalog — every resource plus the 8192-entry
    /// watch-replay ring — before filtering, so a `/registry/pods/` Range
    /// paid for every Secret's history. The visitor keeps the matching
    /// objects; rendering them onto the wire happens after the guard drops,
    /// so the store's lock is held for a filter and a clone, not for
    /// serialization.
    ///
    /// ★ THE REVISION COMES FROM THE SAME GUARD. `for_each_resource` returns
    /// the revision the visited objects are at, so the Range header cannot
    /// name a revision newer than its keys.
    async fn range_under(&self, prefix: &str) -> Result<RangeAt, StoreGone> {
        let store = self.live()?;
        let mut hits = Vec::new();
        let read_at = store
            .for_each_resource(|key, value, meta| {
                if let Some(path) = registry_path_under(key, prefix) {
                    hits.push((path, value.clone(), meta));
                }
            })
            .await;
        let mut kvs: Vec<KeyValue> = hits
            .into_iter()
            .map(|(path, value, meta)| to_kv(path, &value, &meta))
            .collect();
        // etcd returns a Range in byte order and clients paginate on it.
        // BTreeMap order is by ResourceKey, which is NOT the same ordering.
        kvs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(RangeAt {
            kvs,
            revision: wire_revision(read_at),
        })
    }
}

/// A store revision as the etcd wire carries it.
fn wire_revision(revision: Revision) -> i64 {
    i64::try_from(revision.0).unwrap_or(i64::MAX)
}

#[tonic::async_trait]
impl EtcdRevision for MeshEtcdStore {
    /// ★ Scalar read. The whole-catalog read this used to avoid
    /// (`current_catalog()`, which deep-cloned every resource plus the
    /// 8192-entry watch-replay ring to hand back one integer) no longer
    /// exists: the catalog is sealed inside engenho-store (T3.2b).
    async fn revision(&self) -> Result<i64, StoreGone> {
        Ok(wire_revision(self.live()?.current_revision().await))
    }
}

#[tonic::async_trait]
impl EtcdReadStore for MeshEtcdStore {
    async fn range_at(&self, prefix: &str) -> Result<RangeAt, StoreGone> {
        self.range_under(prefix).await
    }
}

#[tonic::async_trait]
impl EtcdStatusStore for MeshEtcdStore {
    async fn applied_index(&self) -> Result<u64, StoreGone> {
        Ok(self.live()?.last_applied_index().await)
    }

    async fn db_size(&self) -> Result<Option<i64>, StoreGone> {
        // ★ MEASURED, NOT GUESSED — AND NOT ZERO, WHICH IS WHY THIS IS NOT
        // `None`. The trait's doc says `None` becomes 0 "which etcd clients
        // read as unknown". That is FALSE, and it was proved by pointing
        // real etcdctl at this façade on 2026-08-30:
        //
        //   panic: runtime error: integer divide by zero
        //     printer.go:234 makeEndpointStatusTable
        //
        // `etcdctl endpoint status` — the first command any operator runs
        // and the one backup tools gate on — computes a "DB SIZE IN USE"
        // percentage and divides by this field. Zero crashes the client
        // outright. An honest-looking zero was therefore worse than no
        // façade at all for that command.
        //
        // So it is measured: the serialized size of every object the façade
        // would serve. That is a real number about real bytes, not an
        // estimate — it just is not the on-disk size, because engenho's
        // store has no single file to stat. `db_size_in_use` reports the
        // same value, which is truthful: there is no free space to
        // distinguish.
        //
        // ★ ONE GUARD, NO RING, AND NOTHING SERIALIZED UNDER IT (T3.2b).
        // This used to clone the whole catalog — its replay ring included —
        // then serialize every object into a throwaway buffer. Now the guard
        // is held only to clone the served objects (the store's contract for
        // a visitor: filter and clone, render after), and each is then
        // serialized into a byte COUNTER: the same number, with no buffer
        // per object and no copy of the ring at all.
        let store = self.live()?;
        let mut served = Vec::new();
        store
            .for_each_resource(|key, value, _| {
                if let Some(path) = registry_path(key) {
                    served.push((path.len(), value.clone()));
                }
            })
            .await;
        let bytes = served.iter().fold(0usize, |total, (path_len, value)| {
            total
                .saturating_add(*path_len)
                .saturating_add(serialized_len(value))
        });
        Ok(Some(i64::try_from(bytes).unwrap_or(i64::MAX)))
    }
}

/// How many signals one façade watch may buffer: the whole replay ring,
/// plus the store's usual live headroom.
///
/// A replay can never exceed the ring, so a far-behind start is served
/// whole rather than overflowing at registration. The bound is not an
/// allocation — a tokio channel grows by blocks as it fills.
const FEED_BUFFER: usize = DEFAULT_HISTORY_CAPACITY + WATCH_CHANNEL_CAPACITY;

#[tonic::async_trait]
impl EtcdWatchStore for MeshEtcdStore {
    type Feed = MeshWatchFeed;

    /// ★ ONE CALL TO THE STORE'S ATOMIC `watch_from` (T3.8). This used to be
    /// two: a live `subscribe`, then a `changes_since` history read. Two
    /// looks at the store are only gap-free if every caller orders them
    /// correctly, and even then a change committed between them was in both
    /// and reached the client twice. The store's `watch_from` captures the
    /// replay and attaches the live tail under ONE lock, so replay → live
    /// has no gap, no duplicate and no reorder by construction.
    ///
    /// That lock is raced in engenho-store's `tests/r7_6_resumable_watch.rs`
    /// (`gap_freedom_*`, `nonempty_replay_boundary_ordering_*`). The one
    /// read this adds before it, `current`, is both the revision a `Now`
    /// watch is acknowledged at and the revision its replay starts after,
    /// so a change landing between the two calls is replayed: the façade
    /// opens no window of its own.
    async fn watch_from(
        &self,
        prefix: &str,
        start: WatchStart,
    ) -> Result<OpenedWatch<MeshWatchFeed>, WatchEnd> {
        let store = self.live()?;
        let current = store.current_revision().await;
        // The store replays every change strictly AFTER `after`.
        let after = match start {
            // A change landing between this read and the subscription is
            // replayed, not lost: it is after `current`.
            WatchStart::Now => current,
            WatchStart::At(first) => Revision(first.get() - 1),
            // Below every watermark, zero included: refused WITH the
            // watermark, so the client learns where it may resume.
            WatchStart::BeforeHistory => {
                return Err(WatchEnd::Compacted {
                    compact_revision: watermark(store.compacted_revision().await),
                });
            }
        };
        // etcd's `start_revision` ahead of the store WAITS for it. The store
        // refuses a resume point past its revision (T3.9a-lock: right for a
        // Kubernetes watch, whose client relists), so an ahead start registers
        // at the store's own revision and the feed's `after` filter drops the
        // changes before the requested start.
        let from = after.min(current);
        let stream = store
            .watch_from(WatchOpts {
                from,
                buffer: FEED_BUFFER,
                // etcd sends no unsolicited progress markers; a bookmark
                // would only be skipped on the way out.
                bookmark_every: Duration::ZERO,
            })
            .await
            .map_err(|gone| watch_end(&gone))?;
        Ok(OpenedWatch {
            revision: wire_revision(current),
            feed: MeshWatchFeed {
                stream,
                prefix: prefix.to_owned(),
                after,
            },
        })
    }
}

/// One open etcd watch over the store's atomic watch stream.
///
/// ★ HOLDS THE STREAM, NOT THE STORE — the façade's `Weak` discipline,
/// kept. An open watch must not keep a stopped node's store alive. When the
/// store is dropped its watcher registry goes with it and the stream
/// closes; that bare close is the one place a watch learns `StoreGone`, and
/// [`WatchFeed::next`] cannot return without naming it.
#[derive(Debug)]
pub struct MeshWatchFeed {
    stream: WatchStream,
    prefix: String,
    /// The requested start, exclusive. A start AHEAD of the store attaches
    /// at the store's revision, so the live tail would also carry the
    /// changes between the two, which the client never asked for.
    after: Revision,
}

#[tonic::async_trait]
impl WatchFeed for MeshWatchFeed {
    async fn next(&mut self) -> WatchStep {
        loop {
            match self.stream.next().await {
                Some(Ok(WatchSignal::Event(event))) => {
                    if event.resource_version <= self.after.0 {
                        continue;
                    }
                    if let Some(rendered) = watch_event_to_event(&event, &self.prefix) {
                        return WatchStep::Event(rendered);
                    }
                }
                // Bookmarks are off for this stream. A stray one is skipped
                // rather than rendered as an empty event, which a client
                // would count as a change.
                Some(Ok(WatchSignal::Bookmark(_))) => {}
                Some(Err(gone)) => return WatchStep::End(watch_end(&gone)),
                // The stream ends without a reason only when every sender is
                // gone: the store, and its watcher registry, were dropped.
                None => return WatchStep::End(WatchEnd::StoreGone(StoreGone)),
            }
        }
    }
}

/// The etcd end for the store's typed watch end.
fn watch_end(gone: &WatchGone) -> WatchEnd {
    match *gone {
        WatchGone::CompactedTooOld { compacted, .. } => WatchEnd::Compacted {
            compact_revision: watermark(compacted),
        },
        WatchGone::Overflow { last_seen, .. } => WatchEnd::Overflow {
            last_seen: wire_revision(last_seen),
        },
        // Reachable only when the store is rewound (a restore, a snapshot
        // install) between the facade's read of `current` and the
        // registration, since the facade never registers past that read. The
        // client re-watches from the store's revision instead of waiting on
        // a revision the store has gone back below.
        WatchGone::AheadOfStore { current, .. } => WatchEnd::Overflow {
            last_seen: wire_revision(current),
        },
    }
}

/// A compaction watermark as the etcd wire carries it.
fn watermark(compacted: engenho_store::Revision) -> i64 {
    i64::try_from(compacted.0).unwrap_or(0)
}

/// The length of `value` serialized as JSON, counted without building the
/// bytes — what `serde_json::to_vec(value).len()` returns, minus the buffer.
fn serialized_len(value: &serde_json::Value) -> usize {
    /// A writer that keeps only how much was written to it.
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    // Serializing a `Value` cannot fail (every key is a string) and `Count`
    // never errors, so the count is complete; the old `unwrap_or(0)` on
    // `to_vec` is kept as the same fallback.
    match serde_json::to_writer(&mut count, value) {
        Ok(()) => count.0,
        Err(_) => 0,
    }
}

/// One `WatchEvent` — replayed or live — rendered as an etcd `Event`.
///
/// ★ ONE RENDERER FOR BOTH HALVES OF A WATCH (T3.8). The store's
/// `WatchEvent` carries the post-image and the revision, but no `prior` and
/// no `VersionMeta`. History used to be read as `Change`s and could fill
/// `prev_kv` (and invented its `mod_revision` as `rev - 1`); the live half
/// could not. Now both come through the one atomic stream, so both render
/// here, and `prev_kv`, `create_revision` and `version` are absent/zero —
/// etcd's "unknown" — rather than guessed. A fabricated `prev_kv` is worse
/// than an absent one: a client diffing against it would compute an empty
/// change set and conclude nothing happened.
fn watch_event_to_event(event: &engenho_store::watch::WatchEvent, prefix: &str) -> Option<Event> {
    use engenho_store::watch::WatchEventKind;

    let path = registry_path_under(&event.key, prefix)?;
    let rev = i64::try_from(event.resource_version).unwrap_or(i64::MAX);
    let deleted = matches!(event.kind, WatchEventKind::Deleted);

    Some(Event {
        r#type: i32::from(deleted),
        kv: Some(KeyValue {
            key: path.into_bytes(),
            // The live event does not carry create/version metadata. Zero
            // is etcd's "unknown", which is honest; inventing `rev` here
            // would tell a client the key was created by this very change.
            create_revision: 0,
            mod_revision: rev,
            version: 0,
            value: if deleted {
                Vec::new()
            } else {
                serde_json::to_vec(&event.object).unwrap_or_default()
            },
            lease: 0,
        }),
        prev_kv: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plural_comes_from_the_catalog_not_from_appending_an_s() {
        // A derived plural does not error — it produces a key nobody writes
        // to, and a Range over it returns empty, which reads exactly like an
        // empty cluster.
        let ep = registry_path(&ResourceKey::namespaced(
            "",
            "v1",
            "Endpoints",
            "default",
            "kubernetes",
        ))
        .expect("Endpoints is in the catalog");
        assert_eq!(ep, "/registry/services/endpoints/default/kubernetes");
        assert!(!ep.contains("endpointss"), "{ep}");

        let np = registry_path(&ResourceKey::namespaced(
            "networking.k8s.io",
            "v1",
            "NetworkPolicy",
            "ns",
            "deny",
        ))
        .expect("NetworkPolicy is in the catalog");
        assert!(np.ends_with("networkpolicies/ns/deny"), "{np}");
    }

    #[test]
    fn a_node_lands_under_minions_the_pre_1_0_name() {
        // The correction Phase 0 made against a 699-key oracle from rio's
        // live k3s. Getting this wrong is invisible: `/registry/nodes/` is
        // simply empty.
        let p = registry_path(&ResourceKey::cluster_scoped("", "v1", "Node", "cid"))
            .expect("Node is in the catalog");
        assert_eq!(p, "/registry/minions/cid");
    }

    #[test]
    fn a_kind_outside_the_catalog_has_no_path_rather_than_a_guessed_one() {
        // A custom resource has no built-in registry segment; inventing one
        // would put objects at a path upstream's apiserver never reads.
        assert!(
            registry_path(&ResourceKey::cluster_scoped(
                "example.com",
                "v1",
                "Widget",
                "w1"
            ))
            .is_none()
        );
    }

    #[test]
    fn a_grouped_kind_carries_its_group_and_a_groupless_one_does_not() {
        // The polarity Phase 0 inverted: grouped is the EXCEPTION.
        let crd = registry_path(&ResourceKey::cluster_scoped(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            "widgets.example.com",
        ))
        .unwrap();
        assert!(crd.contains("apiextensions.k8s.io"), "{crd}");

        let role = registry_path(&ResourceKey::namespaced(
            "rbac.authorization.k8s.io",
            "v1",
            "Role",
            "ns",
            "r",
        ))
        .unwrap();
        assert!(
            !role.contains("rbac.authorization.k8s.io"),
            "rbac is groupless in the keyspace: {role}"
        );
    }

    // ── T3.2b: the reads moved onto the store's single-guard surface ──
    //
    // Each of these used to clone the whole catalog, replay ring included;
    // that read no longer exists (the catalog is sealed in engenho-store).
    // These pin that the answers did not change with it.

    use engenho_store::command::{Reason, ResourceCommand};
    use engenho_store::{InProcessRouter, default_config};
    use serde_json::json;

    async fn boot(cluster: &str) -> Arc<StoreMesh> {
        let store = StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config(cluster).expect("config"),
        )
        .await
        .expect("start");
        store.initialize_singleton().await.expect("initialize");
        assert!(
            store
                .wait_for_leadership(std::time::Duration::from_secs(5))
                .await
        );
        Arc::new(store)
    }

    async fn put(store: &StoreMesh, key: ResourceKey, value: serde_json::Value) {
        store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .expect("propose");
    }

    fn pod(ns: &str, name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", ns, name)
    }

    /// Two pods in two namespaces, a Secret, and a kind with no registry
    /// path (a custom resource), which every read must leave out.
    async fn seed(store: &StoreMesh) {
        put(store, pod("default", "web"), json!({ "spec": { "n": 1 } })).await;
        put(
            store,
            pod("kube-system", "dns"),
            json!({ "spec": { "n": "é\"" } }),
        )
        .await;
        put(
            store,
            ResourceKey::namespaced("", "v1", "Secret", "default", "s"),
            json!({ "data": { "k": "dg==" } }),
        )
        .await;
        put(
            store,
            ResourceKey::cluster_scoped("example.com", "v1", "Widget", "w"),
            json!({}),
        )
        .await;
    }

    #[test]
    fn serialized_len_counts_exactly_what_to_vec_writes() {
        for value in [
            json!(null),
            json!(-1.5e300),
            json!("é\"\\\n\u{1F600}"),
            json!([1, [2, { "k": "v" }]]),
            json!({ "metadata": { "name": "web", "labels": { "a": "b" } },
                    "spec": { "containers": [{ "image": "alpine" }] } }),
        ] {
            let bytes = serde_json::to_vec(&value).expect("a Value serializes");
            assert_eq!(serialized_len(&value), bytes.len(), "{value}");
        }
    }

    #[tokio::test]
    async fn db_size_is_the_serialized_size_of_every_object_served() {
        let store = boot("facade-db-size").await;
        seed(&store).await;

        let mut expected = 0usize;
        for kind in ["Pod", "Secret"] {
            for (key, value) in store.list("", "v1", kind, None).await {
                let path = registry_path(&key).expect("a built-in kind");
                expected += path.len() + serde_json::to_vec(&value).expect("json").len();
            }
        }
        let facade = MeshEtcdStore::new(&store);
        assert_eq!(
            EtcdStatusStore::db_size(&facade).await,
            Ok(Some(i64::try_from(expected).expect("small"))),
            "the paths and bodies of the three served objects, and not the Widget"
        );
        assert_eq!(
            EtcdStatusStore::applied_index(&facade).await,
            Ok(store.last_applied_index().await)
        );
    }

    #[tokio::test]
    async fn a_range_is_exactly_the_prefix_in_byte_order() {
        let store = boot("facade-range").await;
        seed(&store).await;
        let facade = MeshEtcdStore::new(&store);

        let keys = |kvs: Vec<KeyValue>| -> Vec<String> {
            kvs.into_iter()
                .map(|kv| String::from_utf8(kv.key).expect("utf8"))
                .collect()
        };
        assert_eq!(
            keys(
                EtcdReadStore::range(&facade, "/registry/pods/")
                    .await
                    .expect("live")
            ),
            vec![
                "/registry/pods/default/web".to_owned(),
                "/registry/pods/kube-system/dns".to_owned(),
            ]
        );
        assert_eq!(
            keys(
                EtcdReadStore::range(&facade, "/registry/")
                    .await
                    .expect("live")
            ),
            vec![
                "/registry/pods/default/web".to_owned(),
                "/registry/pods/kube-system/dns".to_owned(),
                "/registry/secrets/default/s".to_owned(),
            ],
            "every served object, byte-ordered; the Widget has no path"
        );
        let web = EtcdReadStore::range(&facade, "/registry/pods/default/")
            .await
            .expect("live");
        let (value, meta) = store
            .get_with_meta(&pod("default", "web"))
            .await
            .expect("stored");
        assert_eq!(
            web,
            vec![to_kv(
                "/registry/pods/default/web".to_owned(),
                &value,
                &meta
            )]
        );

        // The seed wrote revisions 1 to 4 (the Widget is 4); the Range is
        // read at 4 even though its newest served key is the Secret at 3.
        let at = EtcdReadStore::range_at(&facade, "/registry/")
            .await
            .expect("live");
        assert_eq!(at.revision, 4, "the revision the keys were read at");
        assert_eq!(
            at.revision,
            wire_revision(store.current_revision().await),
            "nothing has been written since"
        );
    }

    // ── T3.8: one atomic watch_from; a gone store is an answer ─────────

    use engenho_etcd::pb::etcdserverpb::kv_server::Kv as _;
    use engenho_etcd::pb::etcdserverpb::maintenance_server::Maintenance as _;
    use engenho_etcd::pb::etcdserverpb::{RangeRequest, StatusRequest};
    use engenho_etcd::server::{MaintenanceSvc, ReadOnlyKv, ServerIdentity};
    use std::num::NonZeroU64;

    fn at(revision: u64) -> WatchStart {
        WatchStart::At(NonZeroU64::new(revision).expect("a start revision is at least 1"))
    }

    /// Every step the feed has ready. The replay is enqueued whole when the
    /// watch opens, so it is all ready at once; the first quiet interval
    /// ends the read.
    async fn ready(feed: &mut MeshWatchFeed) -> Vec<i64> {
        let mut revisions = Vec::new();
        while let Ok(step) =
            tokio::time::timeout(std::time::Duration::from_millis(200), feed.next()).await
        {
            match step {
                WatchStep::Event(event) => {
                    revisions.push(event.kv.expect("an event carries its kv").mod_revision);
                }
                WatchStep::End(end) => panic!("the watch ended early: {end}"),
            }
        }
        revisions
    }

    async fn open(facade: &MeshEtcdStore, prefix: &str, start: WatchStart) -> MeshWatchFeed {
        EtcdWatchStore::watch_from(facade, prefix, start)
            .await
            .expect("the watch opens")
            .feed
    }

    /// Terminate `store`, which the façade must not be keeping alive.
    async fn stop(store: Arc<StoreMesh>) {
        Arc::try_unwrap(store)
            .map_err(|_| "the facade is holding a strong reference to the store")
            .expect("reclaimable")
            .terminate()
            .await
            .expect("terminate");
    }

    #[tokio::test]
    async fn a_watch_replays_its_prefix_then_tails_live_with_nothing_lost_or_doubled() {
        let store = boot("facade-watch").await;
        seed(&store).await;
        let facade = MeshEtcdStore::new(&store);

        // Pods are revisions 1 and 2; the Secret 3; the Widget 4, which has
        // no registry path and so is never served.
        let mut everything = open(&facade, "/registry/", at(1)).await;
        assert_eq!(ready(&mut everything).await, vec![1, 2, 3]);
        let mut pods = open(&facade, "/registry/pods/", at(2)).await;
        assert_eq!(
            ready(&mut pods).await,
            vec![2],
            "from the start, and only under the prefix"
        );
        let now = EtcdWatchStore::watch_from(&facade, "/registry/", WatchStart::Now)
            .await
            .expect("opens");
        assert_eq!(now.revision, 4, "acknowledged at the store's revision");
        let mut now = now.feed;
        assert_eq!(
            ready(&mut now).await,
            Vec::<i64>::new(),
            "no history for Now"
        );

        put(&store, pod("default", "late"), json!({ "spec": {} })).await;
        for feed in [&mut everything, &mut pods, &mut now] {
            assert_eq!(ready(feed).await, vec![5], "the live change, exactly once");
        }

        // Before every watermark, zero included: refused WITH it.
        assert_eq!(
            EtcdWatchStore::watch_from(&facade, "/registry/", WatchStart::BeforeHistory)
                .await
                .err(),
            Some(WatchEnd::Compacted {
                compact_revision: 0
            }),
        );
    }

    /// etcd's `start_revision` ahead of the store waits for it. The store
    /// attaches such a watch at its own revision, so without the start
    /// filter the changes between the two would reach a client that never
    /// asked for them.
    #[tokio::test]
    async fn a_start_ahead_of_the_store_delivers_nothing_before_it() {
        let store = boot("facade-ahead").await;
        seed(&store).await;
        let facade = MeshEtcdStore::new(&store);

        let mut ahead = open(&facade, "/registry/", at(7)).await;
        assert_eq!(ready(&mut ahead).await, Vec::<i64>::new());
        for name in ["a", "b", "c", "d"] {
            put(&store, pod("default", name), json!({})).await;
        }
        assert_eq!(
            ready(&mut ahead).await,
            vec![7, 8],
            "revisions 5 and 6 precede the requested start"
        );
    }

    /// A resume point below the watermark is refused WITH the watermark:
    /// negative, and below a real one after a durable reopen (T3.3 floors a
    /// reloaded store at the revision it loaded).
    #[tokio::test]
    async fn a_resume_point_below_the_watermark_is_refused_with_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = StoreMesh::start_durable(
                1,
                "in-process://1".into(),
                InProcessRouter::new(),
                default_config("facade-gone").expect("config"),
                dir.path(),
            )
            .await
            .expect("start");
            store.initialize_singleton().await.expect("initialize");
            assert!(
                store
                    .wait_for_leadership(std::time::Duration::from_secs(5))
                    .await
            );
            seed(&store).await;
            store.terminate().await.expect("terminate");
        }
        let (store, _) = StoreMesh::start_or_resume(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("facade-gone").expect("config"),
            dir.path(),
        )
        .await
        .expect("resume");
        assert!(
            store
                .wait_for_leadership(std::time::Duration::from_secs(5))
                .await
        );
        assert_eq!(store.compacted_revision().await, Revision(4));
        let store = Arc::new(store);
        let facade = MeshEtcdStore::new(&store);

        let facade = &facade;
        let refused = |start| async move {
            EtcdWatchStore::watch_from(facade, "/registry/", start)
                .await
                .err()
        };
        let compacted = Some(WatchEnd::Compacted {
            compact_revision: 4,
        });
        assert_eq!(
            refused(at(4)).await,
            compacted,
            "below the reloaded watermark"
        );
        assert_eq!(
            refused(WatchStart::BeforeHistory).await,
            compacted,
            "a negative start is below every watermark"
        );
        let mut from_watermark = open(facade, "/registry/", at(5)).await;
        assert_eq!(
            ready(&mut from_watermark).await,
            Vec::<i64>::new(),
            "the watermark itself is servable"
        );
    }

    /// ★ T3.8. A dropped store used to answer a Range with `Ok`, no keys, at
    /// revision 0 — exactly an empty cluster, which a backup tool would
    /// save as a valid empty snapshot. Every service now answers
    /// `Unavailable`, and every store read says `StoreGone`.
    #[tokio::test]
    async fn a_dropped_store_is_unavailable_never_an_empty_answer() {
        let store = boot("facade-dropped").await;
        seed(&store).await;
        let facade = MeshEtcdStore::new(&store);
        let identity = ServerIdentity::default();
        let kv = ReadOnlyKv {
            store: facade.clone(),
            identity,
        };
        let maintenance = MaintenanceSvc {
            store: facade.clone(),
            identity,
        };
        stop(store).await;

        let range = kv
            .range(tonic::Request::new(RangeRequest {
                key: b"/registry/".to_vec(),
                range_end: engenho_etcd::keyspace::prefix_range_end(b"/registry/"),
                ..RangeRequest::default()
            }))
            .await
            .map(tonic::Response::into_inner);
        let range = range.expect_err("a Range from a dropped store must not succeed");
        assert_eq!(range.code(), tonic::Code::Unavailable);
        assert_eq!(range.message(), StoreGone.to_string());

        let status = maintenance
            .status(tonic::Request::new(StatusRequest::default()))
            .await
            .map(tonic::Response::into_inner);
        assert_eq!(
            status.expect_err("a dropped store has no status").code(),
            tonic::Code::Unavailable
        );

        assert_eq!(EtcdRevision::revision(&facade).await, Err(StoreGone));
        assert_eq!(
            EtcdReadStore::range(&facade, "/registry/").await,
            Err(StoreGone)
        );
        assert_eq!(
            EtcdStatusStore::applied_index(&facade).await,
            Err(StoreGone)
        );
        assert_eq!(EtcdStatusStore::db_size(&facade).await, Err(StoreGone));
        assert_eq!(
            EtcdWatchStore::watch_from(&facade, "/registry/", WatchStart::Now)
                .await
                .err(),
            Some(WatchEnd::StoreGone(StoreGone))
        );
    }

    /// ★ T3.8. When the store goes away under an open watch its stream just
    /// closes — no reason attached. That close is `StoreGone`, and the
    /// client is told so with a cancel; the watch used to fall silent.
    #[tokio::test]
    async fn a_watch_open_when_the_store_is_dropped_is_cancelled_with_the_reason() {
        use engenho_etcd::pb::etcdserverpb::watch_request::RequestUnion;
        use engenho_etcd::pb::etcdserverpb::{WatchCreateRequest, WatchRequest, WatchResponse};

        /// The next reply, which must come within 5s: silence is the defect.
        async fn reply(
            rx: &mut tokio::sync::mpsc::Receiver<Result<WatchResponse, tonic::Status>>,
        ) -> WatchResponse {
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("a reply within 5s — the watch fell silent")
                .expect("the stream is open")
                .expect("a response, not a status")
        }

        let store = boot("facade-watch-dropped").await;
        seed(&store).await;
        let facade = Arc::new(MeshEtcdStore::new(&store));

        let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);
        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                    key: b"/registry/".to_vec(),
                    range_end: engenho_etcd::keyspace::prefix_range_end(b"/registry/"),
                    ..WatchCreateRequest::default()
                })),
            }))
            .await
            .expect("queue");
        // The client half-closes, as one does once its watches exist.
        drop(req_tx);
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(engenho_etcd::server::run_watch_loop(
            facade,
            ServerIdentity::default(),
            tokio_stream::wrappers::ReceiverStream::new(req_rx),
            tx,
        ));
        let ack = reply(&mut rx).await;
        assert!(ack.created);

        stop(store).await;
        let end = reply(&mut rx).await;
        assert!(end.canceled, "the end of a watch is said: {end:?}");
        assert_eq!(end.watch_id, ack.watch_id);
        assert_eq!(end.cancel_reason, StoreGone.to_string());
    }
}
