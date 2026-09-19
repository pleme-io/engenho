//! T3.2a — a paged LIST reads one scope, under one guard, and clones only
//! its page.
//!
//! | behaviour | pinned by |
//! |---|---|
//! | a scope's range is exactly the scope: nothing of another kind or namespace is visited | `a_scope_range_yields_exactly_its_keys` |
//! | pages concatenate to the unpaged list, for every limit | `pages_concatenate_to_the_unpaged_list` |
//! | a cursor from anywhere (a continue token's key comes from the client) resumes strictly after it and never panics | `a_cursor_from_anywhere_resumes_strictly_after_it` |
//! | a mesh page allocates its own items, not a clone of the catalog and its replay ring | `a_page_allocates_only_its_own_items_*` |
//! | the mesh's pages concatenate to the mesh's list, on both backends | `mesh_pages_concatenate_to_the_mesh_list_*` |
//!
//! The adversarial alphabet below puts keys right next to a scope's edges:
//! kinds `Pod\0` and `Pod ` sort immediately after `Pod`, namespace
//! `default\0` immediately after `default`, and `None` (cluster-scoped)
//! before every namespace.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{
    InProcessRouter, ListScope, ResourceCatalog, ResourceKey, StoreMesh, default_config,
};
use proptest::prelude::*;
use proptest::sample::select;
use serde_json::json;

// =================================================================
// A counting allocator: bytes allocated on THIS thread while armed
// =================================================================

/// Counts the bytes the current thread allocates while a measurement is
/// armed. Thread-local, so work on other threads (raft tasks on the
/// multi-thread runtime's workers, fjall's own threads) is not counted.
struct CountingAlloc;

thread_local! {
    /// `None` = not measuring; `Some(n)` = `n` bytes allocated so far.
    static ARMED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

fn note(bytes: usize) {
    // `try_with`: the allocator also runs during thread teardown.
    let _ = ARMED_BYTES.try_with(|c| {
        if let Some(n) = c.get() {
            c.set(Some(n.saturating_add(bytes)));
        }
    });
}

// SAFETY: every method forwards to `System` unchanged; `note` touches only a
// const-initialized thread-local `Cell`, which never allocates.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size.saturating_sub(layout.size()));
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Await `fut` on this thread, returning its output and the bytes this
/// thread allocated meanwhile.
async fn bytes_allocated_by<F: Future>(fut: F) -> (F::Output, usize) {
    ARMED_BYTES.with(|c| c.set(Some(0)));
    let out = fut.await;
    let bytes = ARMED_BYTES.with(|c| c.replace(None)).unwrap_or(0);
    (out, bytes)
}

// =================================================================
// The adversarial key alphabet
// =================================================================

const GROUPS: &[&str] = &["", "apps"];
const VERSIONS: &[&str] = &["v1", "v1beta1"];
const KINDS: &[&str] = &["Po", "Pod", "Pod\0", "Pod\0\0", "Pod ", "PodX", "Pods"];
const NAMESPACES: &[Option<&str>] = &[
    None,
    Some(""),
    Some("\0"),
    Some("de"),
    Some("default"),
    Some("default\0"),
    Some("default-a"),
    Some("defaulu"),
];

fn name() -> impl Strategy<Value = String> {
    prop::collection::vec(select(vec!['\0', 'a', 'b']), 0..3).prop_map(String::from_iter)
}

fn key() -> impl Strategy<Value = ResourceKey> {
    (
        select(GROUPS),
        select(VERSIONS),
        select(KINDS),
        select(NAMESPACES),
        name(),
    )
        .prop_map(|(group, version, kind, namespace, name)| ResourceKey {
            group: group.to_owned(),
            version: version.to_owned(),
            kind: kind.to_owned(),
            namespace: namespace.map(str::to_owned),
            name,
        })
}

/// A LIST's scope as plain strings, so the oracle below never goes
/// through the type under test.
#[derive(Clone, Copy, Debug)]
struct Want {
    group: &'static str,
    version: &'static str,
    kind: &'static str,
    namespace: Option<&'static str>,
}

impl Want {
    fn scope(self) -> ListScope<'static> {
        ListScope::new(self.group, self.version, self.kind, self.namespace)
    }

    /// The oracle: the brute-force filter over every key that the unpaged
    /// LIST has always meant. `namespace: None` spans every namespace and
    /// the cluster-scoped keys.
    fn selects(self, k: &ResourceKey) -> bool {
        k.group == self.group
            && k.version == self.version
            && k.kind == self.kind
            && match self.namespace {
                None => true,
                Some(ns) => k.namespace.as_deref() == Some(ns),
            }
    }
}

fn want() -> impl Strategy<Value = Want> {
    (
        select(GROUPS),
        select(VERSIONS),
        select(KINDS),
        select(NAMESPACES),
    )
        .prop_map(|(group, version, kind, namespace)| Want {
            group,
            version,
            kind,
            namespace,
        })
}

fn catalog_of(keys: &[ResourceKey]) -> ResourceCatalog {
    let mut cat = ResourceCatalog::default();
    for (i, k) in keys.iter().enumerate() {
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: json!({ "i": i }),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            i as u64 + 1,
        );
    }
    cat
}

/// Every key of `cat` the oracle selects, in key order, strictly after
/// `after` when given.
fn oracle(cat: &ResourceCatalog, w: Want, after: Option<&ResourceKey>) -> Vec<ResourceKey> {
    cat.resources
        .keys()
        .filter(|k| w.selects(k))
        .filter(|k| after.is_none_or(|a| *k > a))
        .cloned()
        .collect()
}

// =================================================================
// Catalog-level properties
// =================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The range IS the scope: it yields every key the oracle selects and
    /// nothing else, so serving a LIST never walks another kind. An upper
    /// bound left `Unbounded` yields the keys that sort after the scope and
    /// fails here, whatever filter a caller puts after it.
    #[test]
    fn a_scope_range_yields_exactly_its_keys(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        cursor in prop::option::of(key()),
    ) {
        let map: BTreeMap<ResourceKey, ()> = keys.iter().cloned().map(|k| (k, ())).collect();
        let yielded: Vec<ResourceKey> =
            w.scope().range(&map, cursor.as_ref()).map(|(k, ())| k.clone()).collect();
        let expected: Vec<ResourceKey> = map
            .keys()
            .filter(|k| w.selects(k))
            .filter(|k| cursor.as_ref().is_none_or(|c| *k > c))
            .cloned()
            .collect();
        prop_assert_eq!(yielded, expected);
    }

    /// Threading `next` through `list_page` visits the unpaged list exactly
    /// once, in order, for every limit; each page reports how many items
    /// follow it, and continues iff any do. The owned page a backend hands
    /// out matches the borrowed one and carries the catalog's revision.
    #[test]
    fn pages_concatenate_to_the_unpaged_list(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        limit in 1usize..7,
    ) {
        let cat = catalog_of(&keys);
        let unpaged: Vec<ResourceKey> = cat
            .list(w.group, w.version, w.kind, w.namespace)
            .into_iter()
            .map(|(k, _)| k.clone())
            .collect();
        prop_assert_eq!(&unpaged, &oracle(&cat, w, None), "the unpaged list is the oracle's");

        let mut paged: Vec<ResourceKey> = Vec::new();
        let mut after: Option<ResourceKey> = None;
        for _ in 0..=unpaged.len() {
            let page = cat.list_page(w.group, w.version, w.kind, w.namespace, after.as_ref(), limit);
            let owned = cat.list_page_at_revision(w.scope(), after.as_ref(), limit);
            prop_assert_eq!(owned.revision, cat.revision());
            prop_assert_eq!(
                owned.items.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>(),
                page.items.clone()
            );
            prop_assert_eq!(&owned.next, &page.next);
            prop_assert_eq!(owned.remaining, page.remaining);

            prop_assert!(page.items.len() <= limit, "a page never exceeds its limit");
            let emitted: Vec<ResourceKey> = page.items.iter().map(|(k, _)| (*k).clone()).collect();
            paged.extend(emitted.iter().cloned());
            let follow = oracle(&cat, w, emitted.last().or(after.as_ref())).len() as u64;
            prop_assert_eq!(page.remaining, follow, "remaining counts what follows the page");
            prop_assert_eq!(page.next.is_some(), follow > 0, "a page continues iff items follow");
            match page.next {
                Some(k) => {
                    prop_assert_eq!(Some(&k), emitted.last(), "the cursor is the last emitted key");
                    after = Some(k);
                }
                None => break,
            }
        }
        prop_assert_eq!(paged, unpaged);
    }

    /// A continue token's key comes from the client: it may name another
    /// kind, another namespace, or a key past the scope. Any of them reads
    /// the scope's keys strictly after it, never panicking.
    #[test]
    fn a_cursor_from_anywhere_resumes_strictly_after_it(
        keys in prop::collection::vec(key(), 0..40),
        w in want(),
        cursor in key(),
    ) {
        let cat = catalog_of(&keys);
        let page = cat.list_page(w.group, w.version, w.kind, w.namespace, Some(&cursor), 0);
        let got: Vec<ResourceKey> = page.items.iter().map(|(k, _)| (*k).clone()).collect();
        prop_assert_eq!(got, oracle(&cat, w, Some(&cursor)));
        prop_assert!(page.next.is_none());
        prop_assert_eq!(page.remaining, 0);
    }
}

// =================================================================
// Mesh-level: one guard, no catalog clone, both backends
// =================================================================

const LEADERSHIP: Duration = Duration::from_secs(10);

async fn memory_mesh() -> Arc<StoreMesh> {
    let mesh = StoreMesh::start(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("t3-2a-memory").expect("config"),
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");
    Arc::new(mesh)
}

async fn durable_mesh(dir: &std::path::Path) -> Arc<StoreMesh> {
    let mesh = StoreMesh::start_durable(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config("t3-2a-durable").expect("config"),
        dir,
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");
    Arc::new(mesh)
}

async fn put(mesh: &StoreMesh, key: ResourceKey, value: serde_json::Value) {
    mesh.propose(ResourceCommand::Put {
        key,
        value,
        expected: None,
        reason: Reason::Operator,
    })
    .await
    .expect("propose");
}

/// Fill the catalog with bulk a page of ConfigMaps must not pay for:
/// one Secret rewritten until the replay ring holds megabytes of its
/// images, as Helm's release Secrets filled it on rio. Then one ConfigMap.
///
/// Each rewrite changes the Secret (as each Helm release does): since T3.5
/// an identical rewrite commits nothing, so it would add nothing to the ring.
async fn seed_bulk_then_one_configmap(mesh: &StoreMesh) {
    let body = "A".repeat(16 * 1024);
    let release = ResourceKey::namespaced("", "v1", "Secret", "default", "release");
    for version in 0..200u32 {
        put(
            mesh,
            release.clone(),
            json!({ "data": { "payload": body, "version": version } }),
        )
        .await;
    }
    put(
        mesh,
        ResourceKey::namespaced("", "v1", "ConfigMap", "default", "one"),
        json!({ "data": { "k": "v" } }),
    )
    .await;
}

/// The page must cost what it returns. The positive control measures a
/// full catalog clone with the same instrument, so a broken counter or a
/// seed too small to tell the two apart fails loudly instead of passing.
async fn assert_a_page_allocates_only_its_items(mesh: &StoreMesh) {
    seed_bulk_then_one_configmap(mesh).await;

    let (catalog, clone_bytes) = bytes_allocated_by(mesh.current_catalog()).await;
    drop(catalog);
    assert!(
        clone_bytes > 2 * 1024 * 1024,
        "positive control: cloning the catalog allocated only {clone_bytes} bytes — the \
         counting allocator or the seed is broken, so the bound below would prove nothing"
    );

    let ((items, _rev, next, remaining), page_bytes) = bytes_allocated_by(
        mesh.list_page_at_revision("", "v1", "ConfigMap", Some("default"), None, 10),
    )
    .await;
    assert_eq!(items.len(), 1, "the one ConfigMap");
    assert_eq!((next, remaining), (None, 0));
    assert!(
        page_bytes < 64 * 1024,
        "a one-item page allocated {page_bytes} bytes against {clone_bytes} for a catalog \
         clone: the page path is cloning the catalog and its replay ring again, once per \
         page of every relist"
    );

    let ((items, _rev), list_bytes) =
        bytes_allocated_by(mesh.list_at_revision("", "v1", "ConfigMap", Some("default"))).await;
    assert_eq!(items.len(), 1, "the one ConfigMap");
    assert!(
        list_bytes < 64 * 1024,
        "a one-item list allocated {list_bytes} bytes against {clone_bytes} for a catalog clone"
    );
}

// Multi-thread flavor: the test body is polled on this thread while raft's
// tasks run on the workers, so the thread-local counter sees only the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_allocates_only_its_own_items_memory() {
    let mesh = memory_mesh().await;
    assert_a_page_allocates_only_its_items(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_allocates_only_its_own_items_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path()).await;
    assert_a_page_allocates_only_its_items(&mesh).await;
}

/// Keys of several kinds and namespaces, cluster-scoped among them, each
/// scope's neighbours included; every scope paged at every small limit
/// through the mesh must reproduce the mesh's own list.
async fn assert_mesh_pages_concatenate(mesh: &StoreMesh) {
    let seed = [
        ResourceKey::namespaced("", "v1", "Pod", "a", "p1"),
        ResourceKey::namespaced("", "v1", "Pod", "a", "p2"),
        ResourceKey::namespaced("", "v1", "Pod", "a\0", "p3"),
        ResourceKey::namespaced("", "v1", "Pod", "b", "p1"),
        ResourceKey::namespaced("", "v1", "Pod\0", "a", "p1"),
        ResourceKey::namespaced("", "v1", "Po", "a", "p1"),
        ResourceKey::cluster_scoped("", "v1", "Node", "n1"),
        ResourceKey::cluster_scoped("", "v1", "Node", "n2"),
        ResourceKey::namespaced("", "v1", "ConfigMap", "a", "c1"),
        ResourceKey::namespaced("apps", "v1", "Pod", "a", "p1"),
    ];
    for k in &seed {
        put(mesh, k.clone(), json!({})).await;
    }

    let scopes: [(&str, &str, &str, Option<&str>); 6] = [
        ("", "v1", "Pod", None),
        ("", "v1", "Pod", Some("a")),
        ("", "v1", "Pod", Some("b")),
        ("", "v1", "Node", None),
        ("", "v1", "ConfigMap", Some("a")),
        ("", "v1", "Secret", None),
    ];
    for (g, v, k, ns) in scopes {
        let unpaged: Vec<ResourceKey> = mesh
            .list(g, v, k, ns)
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let expected: Vec<ResourceKey> = {
            let mut e: Vec<ResourceKey> = seed
                .iter()
                .filter(|key| {
                    key.group == g
                        && key.version == v
                        && key.kind == k
                        && ns.is_none_or(|n| key.namespace.as_deref() == Some(n))
                })
                .cloned()
                .collect();
            e.sort();
            e
        };
        assert_eq!(unpaged, expected, "the mesh list of {k} {ns:?}");

        for limit in 1..=4 {
            let mut paged: Vec<ResourceKey> = Vec::new();
            let mut after: Option<ResourceKey> = None;
            for _ in 0..=seed.len() {
                let (items, rev, next, _remaining) = mesh
                    .list_page_at_revision(g, v, k, ns, after.as_ref(), limit)
                    .await;
                assert_eq!(
                    rev,
                    mesh.current_revision().await,
                    "quiescent: the live revision"
                );
                assert!(items.len() <= limit);
                paged.extend(items.into_iter().map(|(k, _)| k));
                match next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
            assert_eq!(paged, unpaged, "pages of {k} {ns:?} at limit {limit}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mesh_pages_concatenate_to_the_mesh_list_memory() {
    let mesh = memory_mesh().await;
    assert_mesh_pages_concatenate(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mesh_pages_concatenate_to_the_mesh_list_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path()).await;
    assert_mesh_pages_concatenate(&mesh).await;
}
