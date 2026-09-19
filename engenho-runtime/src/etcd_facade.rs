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

use engenho_etcd::pb::mvccpb::{Event, KeyValue};
use engenho_etcd::server::{EtcdReadStore, EtcdStatusStore, EtcdWatchStore};
use engenho_store::StoreMesh;
use engenho_store::resource::ResourceKey;

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

    /// The live store, or `None` once the node has shut down.
    ///
    /// A dropped store means the process is terminating. Every accessor
    /// below degrades to an empty/zero answer rather than panicking: the
    /// listener task is being torn down in the same breath, and taking the
    /// shutdown path down with a panic would turn a clean stop into a
    /// crash.
    fn live(&self) -> Option<Arc<StoreMesh>> {
        self.store.upgrade()
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
    async fn kvs_under(&self, prefix: &str) -> Vec<KeyValue> {
        let Some(store) = self.live() else {
            return Vec::new();
        };
        let mut hits = Vec::new();
        store
            .for_each_resource(|key, value, meta| {
                if let Some(path) = registry_path_under(key, prefix) {
                    hits.push((path, value.clone(), meta));
                }
            })
            .await;
        let mut out: Vec<KeyValue> = hits
            .into_iter()
            .map(|(path, value, meta)| to_kv(path, &value, &meta))
            .collect();
        // etcd returns a Range in byte order and clients paginate on it.
        // BTreeMap order is by ResourceKey, which is NOT the same ordering.
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }
}

impl MeshEtcdStore {
    /// The store's current global revision, or 0 once it is gone.
    async fn current_revision(&self) -> i64 {
        let Some(store) = self.live() else {
            return 0;
        };
        // ★ Scalar read. The whole-catalog read this used to avoid
        // (`current_catalog()`, which deep-cloned every resource plus the
        // 8192-entry watch-replay ring to hand back one integer) no longer
        // exists: the catalog is sealed inside engenho-store (T3.2b).
        i64::try_from(store.current_revision().await.0).unwrap_or(i64::MAX)
    }
}

#[tonic::async_trait]
impl EtcdReadStore for MeshEtcdStore {
    async fn revision(&self) -> i64 {
        self.current_revision().await
    }

    async fn range(&self, prefix: &str) -> Vec<KeyValue> {
        self.kvs_under(prefix).await
    }
}

#[tonic::async_trait]
impl EtcdStatusStore for MeshEtcdStore {
    async fn revision(&self) -> i64 {
        self.current_revision().await
    }

    async fn applied_index(&self) -> u64 {
        match self.live() {
            Some(store) => store.last_applied_index().await,
            None => 0,
        }
    }

    async fn db_size(&self) -> Option<i64> {
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
        Some(i64::try_from(bytes).unwrap_or(i64::MAX))
    }
}

#[tonic::async_trait]
impl EtcdWatchStore for MeshEtcdStore {
    async fn revision(&self) -> i64 {
        self.current_revision().await
    }

    async fn changes_since(&self, prefix: &str, since: i64) -> Result<Vec<Event>, i64> {
        let Some(store) = self.live() else {
            return Ok(Vec::new());
        };
        // The gap case is a VALUE the caller must handle, not an empty
        // vector it could forward by accident: a client resuming below the
        // watermark has to be told where it may safely restart, or it
        // believes it is tracking a cluster it has already lost sync with.
        //
        // A negative `since` is below every watermark, including zero.
        let Ok(from) = u64::try_from(since).map(engenho_store::Revision) else {
            return Err(watermark(store.compacted_revision().await));
        };
        // ★ ONE GUARD, AND ONLY THE MATCHING CHANGES ARE CLONED (T3.2b).
        // This used to clone the whole catalog — every resource and the
        // entire replay ring — to read one window of the ring. The store
        // applies the same window and the same refusal the watch replay
        // does; rendering happens after its guard drops.
        let mut hits = Vec::new();
        store
            .for_each_change_since(from, |change| {
                if let Some(path) = registry_path_under(&change.key, prefix) {
                    hits.push((path, change.clone()));
                }
            })
            .await
            .map_err(|gone| watermark(gone.compacted))?;
        Ok(hits
            .into_iter()
            .map(|(path, change)| change_to_event(path, &change))
            .collect())
    }

    async fn subscribe(&self, prefix: &str) -> Option<tokio::sync::mpsc::Receiver<Event>> {
        use engenho_store::watch_backend::WatchSignal;

        // A store that cannot subscribe returns `None` and the caller
        // REFUSES the watch, rather than serving history and falling
        // silent — a client that believes it is tracking the cluster and
        // is not is the one failure mode a watch must never have.
        let mut stream = self.live()?.watch().await.ok()?;
        let prefix = prefix.to_string();
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(Ok(signal)) = stream.next().await {
                // A Bookmark is a progress marker with no etcd equivalent
                // in this direction; it is dropped rather than rendered as
                // an empty event, which a client would count as a change.
                let WatchSignal::Event(event) = signal else {
                    continue;
                };
                let Some(rendered) = watch_event_to_event(&event, &prefix) else {
                    continue;
                };
                // A full channel means the client is not draining. Ending
                // the watch is correct; dropping the SEND would give it a
                // stream with a silent hole in it.
                if tx.send(rendered).await.is_err() {
                    break;
                }
            }
        });
        Some(rx)
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

/// One store `Change` rendered as an etcd `Event`; the caller has already
/// selected it by its `/registry` `path`.
///
/// ★ `prev_kv` IS POPULATED HERE AND CANNOT BE ON THE LIVE PATH. `Change`
/// carries `prior`; the live `WatchEvent` the store broadcasts does not —
/// it keeps only the post-image. So a history replay can answer
/// `prev_kv` truthfully and a live event cannot, and the live path returns
/// `None` rather than echoing the current value as if it were the previous
/// one. A fabricated `prev_kv` is worse than an absent one: a client
/// diffing against it would compute an empty change set and conclude
/// nothing happened.
fn change_to_event(path: String, change: &engenho_store::revision::Change) -> Event {
    use engenho_store::revision::ChangeKind;

    let rev = i64::try_from(change.revision.0).unwrap_or(i64::MAX);
    let meta = &change.version_meta;
    let deleted = matches!(change.kind, ChangeKind::Delete);

    Event {
        // 0 = PUT, 1 = DELETE in mvccpb.
        r#type: i32::from(deleted),
        kv: Some(KeyValue {
            key: path.clone().into_bytes(),
            create_revision: i64::try_from(meta.create_revision.0).unwrap_or(0),
            mod_revision: rev,
            version: i64::try_from(meta.version).unwrap_or(0),
            // etcd sends an EMPTY value on a delete. `Change.value` holds
            // the tombstone (the object as it was), which belongs in
            // `prev_kv`, not in `kv` — a client that read it there would
            // treat a deleted object as still present.
            value: if deleted {
                Vec::new()
            } else {
                serde_json::to_vec(&change.value).unwrap_or_default()
            },
            lease: 0,
        }),
        prev_kv: change.prior.as_ref().map(|p| KeyValue {
            key: path.into_bytes(),
            create_revision: i64::try_from(meta.create_revision.0).unwrap_or(0),
            mod_revision: rev.saturating_sub(1),
            version: i64::try_from(meta.version.saturating_sub(1)).unwrap_or(0),
            value: serde_json::to_vec(p).unwrap_or_default(),
            lease: 0,
        }),
    }
}

/// One LIVE `WatchEvent` rendered as an etcd `Event`.
///
/// Separate from [`change_to_event`] because the live broadcast carries
/// strictly less information — no `prior`, no `VersionMeta` — and pretending
/// otherwise is how a façade starts lying. See that function's header.
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
    use engenho_store::{InProcessRouter, Revision, default_config};
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
            Some(i64::try_from(expected).expect("small")),
            "the paths and bodies of the three served objects, and not the Widget"
        );
        assert_eq!(
            EtcdStatusStore::applied_index(&facade).await,
            store.last_applied_index().await
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
            keys(EtcdReadStore::range(&facade, "/registry/pods/").await),
            vec![
                "/registry/pods/default/web".to_owned(),
                "/registry/pods/kube-system/dns".to_owned(),
            ]
        );
        assert_eq!(
            keys(EtcdReadStore::range(&facade, "/registry/").await),
            vec![
                "/registry/pods/default/web".to_owned(),
                "/registry/pods/kube-system/dns".to_owned(),
                "/registry/secrets/default/s".to_owned(),
            ],
            "every served object, byte-ordered; the Widget has no path"
        );
        let web = EtcdReadStore::range(&facade, "/registry/pods/default/").await;
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
    }

    #[tokio::test]
    async fn history_is_the_window_after_since_under_the_prefix() {
        let store = boot("facade-history").await;
        seed(&store).await;
        let facade = MeshEtcdStore::new(&store);

        let revs = |events: Vec<Event>| -> Vec<i64> {
            events
                .into_iter()
                .map(|e| e.kv.expect("a kv").mod_revision)
                .collect()
        };
        // Pods are revisions 1 and 2; the Secret 3; the Widget 4.
        assert_eq!(
            revs(
                EtcdWatchStore::changes_since(&facade, "/registry/", 0)
                    .await
                    .expect("ok")
            ),
            vec![1, 2, 3]
        );
        assert_eq!(
            revs(
                EtcdWatchStore::changes_since(&facade, "/registry/pods/", 1)
                    .await
                    .expect("ok")
            ),
            vec![2],
            "strictly after `since`, and only under the prefix"
        );
        assert_eq!(
            revs(
                EtcdWatchStore::changes_since(&facade, "/registry/", 4)
                    .await
                    .expect("ok")
            ),
            Vec::<i64>::new()
        );
        // Nothing is compacted, so the watermark is 0 — and a negative
        // resume point is still below it. Read as "from 0" it would replay
        // the whole window to a client that asked for something impossible.
        assert_eq!(
            EtcdWatchStore::changes_since(&facade, "/registry/", -1).await,
            Err(0),
            "a negative resume point is below even a zero watermark"
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

        assert_eq!(
            EtcdWatchStore::changes_since(&facade, "/registry/", 3).await,
            Err(4),
            "below the reloaded watermark"
        );
        assert_eq!(
            EtcdWatchStore::changes_since(&facade, "/registry/", -1).await,
            Err(4),
            "a negative resume point is below every watermark"
        );
        assert_eq!(
            EtcdWatchStore::changes_since(&facade, "/registry/", 4).await,
            Ok(Vec::new()),
            "the watermark itself is servable"
        );
    }
}
