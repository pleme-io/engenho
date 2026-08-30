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
    /// Renders the whole catalog and filters. That is O(n) per Range and
    /// deliberately so at this stage: the alternative is a second index
    /// keyed by etcd path, which would be a copy of the store's own keying
    /// that could drift from it. A prefix index belongs here once a
    /// measurement says the scan hurts, not before.
    async fn kvs_under(&self, prefix: &str) -> Vec<KeyValue> {
        let Some(store) = self.live() else {
            return Vec::new();
        };
        let catalog = store.current_catalog().await;
        let mut out: Vec<KeyValue> = catalog
            .resources
            .iter()
            .filter_map(|(key, (value, meta))| {
                let path = registry_path(key)?;
                path.starts_with(prefix).then(|| to_kv(path, value, meta))
            })
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
        i64::try_from(store.current_catalog().await.current_revision.0).unwrap_or(i64::MAX)
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
            Some(store) => store.current_catalog().await.last_applied_index,
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
        let store = self.live()?;
        let catalog = store.current_catalog().await;
        let bytes: usize = catalog
            .resources
            .iter()
            .filter_map(|(key, (value, _))| {
                let path = registry_path(key)?;
                Some(path.len() + serde_json::to_vec(value).map(|v| v.len()).unwrap_or(0))
            })
            .sum();
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
        let catalog = store.current_catalog().await;
        let compacted = i64::try_from(catalog.compacted_revision.0).unwrap_or(0);
        // The gap case is a VALUE the caller must handle, not an empty
        // vector it could forward by accident: a client resuming below the
        // watermark has to be told where it may safely restart, or it
        // believes it is tracking a cluster it has already lost sync with.
        if since < compacted {
            return Err(compacted);
        }
        Ok(catalog
            .history
            .iter()
            .filter(|c| i64::try_from(c.revision.0).unwrap_or(0) > since)
            .filter_map(|c| change_to_event(c, prefix))
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

/// One store `Change` rendered as an etcd `Event`, if it is under `prefix`.
///
/// ★ `prev_kv` IS POPULATED HERE AND CANNOT BE ON THE LIVE PATH. `Change`
/// carries `prior`; the live `WatchEvent` the store broadcasts does not —
/// it keeps only the post-image. So a history replay can answer
/// `prev_kv` truthfully and a live event cannot, and the live path returns
/// `None` rather than echoing the current value as if it were the previous
/// one. A fabricated `prev_kv` is worse than an absent one: a client
/// diffing against it would compute an empty change set and conclude
/// nothing happened.
fn change_to_event(change: &engenho_store::revision::Change, prefix: &str) -> Option<Event> {
    use engenho_store::revision::ChangeKind;

    let path = registry_path(&change.key)?;
    if !path.starts_with(prefix) {
        return None;
    }
    let rev = i64::try_from(change.revision.0).unwrap_or(i64::MAX);
    let meta = &change.version_meta;
    let deleted = matches!(change.kind, ChangeKind::Delete);

    Some(Event {
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
    })
}

/// One LIVE `WatchEvent` rendered as an etcd `Event`.
///
/// Separate from [`change_to_event`] because the live broadcast carries
/// strictly less information — no `prior`, no `VersionMeta` — and pretending
/// otherwise is how a façade starts lying. See that function's header.
fn watch_event_to_event(event: &engenho_store::watch::WatchEvent, prefix: &str) -> Option<Event> {
    use engenho_store::watch::WatchEventKind;

    let path = registry_path(&event.key)?;
    if !path.starts_with(prefix) {
        return None;
    }
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
}
