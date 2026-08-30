//! M4.0 — the etcd v3 façade over a live store, on a real socket.
//!
//! `engenho-etcd` had 48 passing tests and no consumer: nothing implemented
//! its store traits over `StoreMesh`, no crate depended on it, and nothing
//! listened on :2379. These tests exercise the producer, so the crate stops
//! being a well-tested vocabulary nobody emits.
//!
//! * **F1** an object written through the store is readable by its
//!   `/registry` path
//! * **F2** the revision triple mirrors the store's own `VersionMeta`
//! * **F3** a prefix Range returns byte-ordered results
//! * **F4** `keys_only` strips values; `count` is the TOTAL, not the page
//! * **F5** Maintenance reports the live revision and applied index
//! * **F6** a compacted watch start is REFUSED with the watermark
//! * **F7** an unknown kind has no path rather than a guessed one

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use engenho_etcd::server::{
    EtcdReadStore, EtcdStatusStore, EtcdWatchStore, ReadOnlyKv, ServerIdentity,
};
use engenho_runtime::MeshEtcdStore;
use engenho_runtime::etcd_facade::registry_path;
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};

async fn boot(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
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
        .unwrap();
}

fn pod(ns: &str, name: &str) -> (ResourceKey, serde_json::Value) {
    (
        ResourceKey::namespaced("", "v1", "Pod", ns, name),
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": name, "namespace": ns },
            "spec": { "containers": [{ "name": "c", "image": "alpine" }] }
        }),
    )
}

/// F1 + F2 — an object is readable at its registry path, with the triple.
#[tokio::test]
async fn an_object_written_to_the_store_is_readable_by_its_registry_path() {
    let store = boot("etcd-read").await;
    let (key, value) = pod("default", "web");
    put(&store, key.clone(), value.clone()).await;

    let facade = MeshEtcdStore::new(&store);
    let kvs = EtcdReadStore::range(&facade, "/registry/pods/").await;
    assert_eq!(kvs.len(), 1, "{kvs:?}");

    // F1 — upstream's path exactly. A wrong key does not error; it returns
    // an empty range that reads like an empty cluster.
    assert_eq!(
        String::from_utf8(kvs[0].key.clone()).unwrap(),
        "/registry/pods/default/web"
    );
    // The façade returns the STORED object — including the fields the
    // apiserver stamped on write (uid, resourceVersion, generation). That
    // is exactly right: what upstream's apiserver reads back out of etcd is
    // the persisted object, not the submitted one, and a façade that
    // returned the client's original would hand a restore a Pod with no
    // uid.
    let decoded: serde_json::Value = serde_json::from_slice(&kvs[0].value).unwrap();
    assert_eq!(decoded["spec"], value["spec"], "the spec round-trips");
    assert_eq!(decoded["metadata"]["name"], "web");
    assert!(
        decoded["metadata"]["uid"].is_string(),
        "the persisted object carries its server-set fields: {decoded}"
    );

    // F2 — the triple is the store's own VersionMeta, which is what makes
    // this a façade rather than a simulation: a client doing a CAS on
    // mod_revision compares against the same counter engenho's own
    // preconditions use.
    assert!(kvs[0].create_revision > 0);
    assert_eq!(kvs[0].mod_revision, kvs[0].create_revision, "first write");
    assert_eq!(kvs[0].version, 1);

    // A second write advances mod_revision and version, not create.
    put(
        &store,
        key,
        json!({ "apiVersion": "v1", "kind": "Pod", "x": 1 }),
    )
    .await;
    let kvs2 = EtcdReadStore::range(&facade, "/registry/pods/").await;
    assert_eq!(kvs2[0].create_revision, kvs[0].create_revision);
    assert!(kvs2[0].mod_revision > kvs[0].mod_revision);
    assert_eq!(kvs2[0].version, 2);

    drop(facade);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// F3 — a prefix Range comes back in BYTE order.
///
/// The store is a `BTreeMap` keyed by `ResourceKey`, which is not the same
/// ordering as the rendered path. etcd clients paginate on byte order, so
/// serving the map's order would hand a client pages that skip and repeat.
#[tokio::test]
async fn a_prefix_range_is_returned_in_byte_order() {
    let store = boot("etcd-order").await;
    for (ns, name) in [("zeta", "a"), ("alpha", "b"), ("mid", "c")] {
        let (k, v) = pod(ns, name);
        put(&store, k, v).await;
    }

    let facade = MeshEtcdStore::new(&store);
    let kvs = EtcdReadStore::range(&facade, "/registry/pods/").await;
    let paths: Vec<String> = kvs
        .iter()
        .map(|kv| String::from_utf8(kv.key.clone()).unwrap())
        .collect();
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted, "byte order, not BTreeMap order");

    drop(facade);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// F4 — the Kv service over a real request: `keys_only` and `count`.
#[tokio::test]
async fn keys_only_strips_values_and_count_is_the_total_not_the_page() {
    use engenho_etcd::pb::etcdserverpb::RangeRequest;
    use engenho_etcd::pb::etcdserverpb::kv_server::Kv;

    let store = boot("etcd-kv").await;
    for i in 0..5 {
        let (k, v) = pod("default", &format!("p{i}"));
        put(&store, k, v).await;
    }

    let kv = ReadOnlyKv {
        store: MeshEtcdStore::new(&store),
        identity: ServerIdentity::default(),
    };

    // A prefix range with a limit: `count` must report the TOTAL matching,
    // because a client paginating reads it to size the remaining work.
    // Reporting the page length makes every page look like the last.
    let resp = kv
        .range(tonic::Request::new(RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: engenho_etcd::keyspace::prefix_range_end(b"/registry/pods/"),
            limit: 2,
            keys_only: true,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.kvs.len(), 2, "the page");
    assert_eq!(resp.count, 5, "the total");
    assert!(resp.more, "there is another page");
    assert!(
        resp.kvs.iter().all(|kv| kv.value.is_empty()),
        "keys_only stripped the values"
    );
    // The header carries the live revision, which is what a client uses to
    // start a watch exactly where the read left off.
    assert!(resp.header.expect("header").revision > 0);

    drop(kv);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// F5 — Maintenance answers about the live store.
///
/// `etcdctl endpoint status` is the first thing an operator runs, and a
/// backup tool gates on it before taking a snapshot.
#[tokio::test]
async fn maintenance_reports_the_live_revision_and_applied_index() {
    let store = boot("etcd-maint").await;
    let (k, v) = pod("default", "web");
    put(&store, k, v).await;

    let facade = MeshEtcdStore::new(&store);
    assert!(EtcdStatusStore::revision(&facade).await > 0);
    assert!(EtcdStatusStore::applied_index(&facade).await > 0);
    // ★ NOT ZERO, AND NOT `None`. Real etcdctl divides by this field to
    // render "DB SIZE IN USE" and panics with an integer-divide-by-zero
    // when it is 0 — measured 2026-08-30 against this very façade. The
    // trait's doc claiming clients read 0 as "unknown" is false, so the
    // value is MEASURED: the serialized size of everything served.
    let size = EtcdStatusStore::db_size(&facade)
        .await
        .expect("a measured size, because zero crashes etcdctl");
    assert!(size > 0, "got {size}");

    drop(facade);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// F6 — a watch resuming below the compaction watermark is REFUSED, and
/// told where it may safely restart.
///
/// The gap case has to be a value the caller handles, not an empty vector
/// it could forward: a client that believes it is tracking a cluster it has
/// already lost sync with is the one failure a watch must never have.
#[tokio::test]
async fn a_watch_below_the_compaction_watermark_is_refused_with_the_watermark() {
    let store = boot("etcd-compact").await;
    let facade = MeshEtcdStore::new(&store);

    // Nothing compacted yet: revision 0 is servable.
    assert!(
        EtcdWatchStore::changes_since(&facade, "/registry/", 0)
            .await
            .is_ok()
    );

    // The negative direction, which is the one that matters. A revision
    // below the watermark comes back AS the watermark, not as silence:
    // a client resuming below it has to be told where it may safely
    // restart, or it believes it is tracking a cluster it has already lost
    // sync with. With no compaction the watermark is 0, so -1 is the probe.
    let watermark = EtcdWatchStore::changes_since(&facade, "/registry/", -1)
        .await
        .expect_err("a revision below the watermark must be refused");
    assert_eq!(
        watermark, 0,
        "the resume point the client must restart from"
    );

    // And live history is readable.
    let (k, v) = pod("default", "web");
    put(&store, k, v).await;
    let events = EtcdWatchStore::changes_since(&facade, "/registry/pods/", 0)
        .await
        .expect("history is servable");
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].r#type, 0, "PUT");

    drop(facade);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

/// F7 — a kind outside the catalog has no path.
///
/// Inventing one would place objects where upstream's apiserver never
/// looks, and the symptom is an empty range indistinguishable from an
/// empty cluster.
#[test]
fn a_kind_outside_the_catalog_has_no_registry_path() {
    assert!(
        registry_path(&ResourceKey::cluster_scoped(
            "example.com",
            "v1",
            "Widget",
            "w"
        ))
        .is_none()
    );
    // And the pre-1.0 name correction survives: Node lives under minions.
    assert_eq!(
        registry_path(&ResourceKey::cluster_scoped("", "v1", "Node", "cid")).unwrap(),
        "/registry/minions/cid"
    );
}

/// F8 — the façade must not keep the store alive, and must degrade when it
/// is gone.
///
/// ★ THIS IS A REGRESSION TEST FOR A LEAK I SHIPPED. The first version held
/// `Arc<StoreMesh>`, cloned into three services inside a detached listener
/// task. That kept the whole store — Raft log and fjall handles included —
/// alive for the life of the process, and turned eight graceful-shutdown
/// tests red with `StoreStillShared { strong_count: 4 }`. It is the SECOND
/// occurrence of this exact shape in the runtime (`WeakKubeletApi` is the
/// first, for the :10250 listener), which is why it now has a test rather
/// than a comment.
#[tokio::test]
async fn the_facade_does_not_keep_the_store_alive_and_degrades_when_it_is_gone() {
    let store = boot("etcd-weak").await;
    let (k, v) = pod("default", "web");
    put(&store, k, v).await;

    let facade = MeshEtcdStore::new(&store);
    assert_eq!(
        EtcdReadStore::range(&facade, "/registry/pods/").await.len(),
        1,
        "live while the store is live"
    );

    // The façade is still held — and the store must STILL be reclaimable.
    // This is the assertion that fails if anyone reintroduces the Arc.
    Arc::try_unwrap(store)
        .map_err(|_| "the facade is holding a strong reference to the store")
        .expect("reclaimable")
        .terminate()
        .await
        .unwrap();

    // And now it answers empty rather than panicking: the listener task is
    // being torn down in the same breath, and taking the shutdown path down
    // with a panic would turn a clean stop into a crash.
    assert!(
        EtcdReadStore::range(&facade, "/registry/pods/")
            .await
            .is_empty()
    );
    assert_eq!(EtcdReadStore::revision(&facade).await, 0);
    assert_eq!(EtcdStatusStore::applied_index(&facade).await, 0);
    assert!(
        EtcdWatchStore::changes_since(&facade, "/registry/", 0)
            .await
            .is_ok()
    );
}
