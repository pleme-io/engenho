//! T3.2a, mesh level — a paged LIST reads one scope, under one guard, and
//! clones only its page.
//!
//! | behaviour | pinned by |
//! |---|---|
//! | a mesh page allocates its own items, not a clone of the catalog and its replay ring | `a_page_allocates_only_its_own_items_*` |
//! | the mesh's pages concatenate to the mesh's list, on both backends | `mesh_pages_concatenate_to_the_mesh_list_*` |
//!
//! The catalog-level half (scope ranges, page concatenation under an
//! adversarial key alphabet) lives in the crate at
//! `src/catalog_tests/t3_2a_paged_list.rs`, because it drives the catalog
//! directly and T3.2b sealed it.

mod support;

use engenho_store::revision::Revision;
use engenho_store::{ResourceKey, StoreMesh, WatchOpts};
use serde_json::json;
use support::{bytes_allocated_by, durable_mesh, memory_mesh, put, seed_bulk_then_one_configmap};
/// The page must cost what it returns. The positive control measures a
/// replay of the whole ring with the same instrument, so a broken counter or
/// a seed too small to tell the two apart fails loudly instead of passing.
///
/// Until T3.2b the control cloned the whole catalog. That read no longer
/// exists outside the crate; a replay from revision zero is the one public
/// read whose cost IS the ring, by design — it clones every retained change,
/// post-image and pre-image.
async fn assert_a_page_allocates_only_its_items(mesh: &StoreMesh) {
    seed_bulk_then_one_configmap(mesh).await;

    let (replay, ring_bytes) =
        bytes_allocated_by(mesh.watch_from(WatchOpts::from_revision(Revision::ZERO))).await;
    drop(replay.expect("nothing is compacted yet"));
    assert!(
        ring_bytes > 2 * 1024 * 1024,
        "positive control: replaying the ring allocated only {ring_bytes} bytes — the \
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
        "a one-item page allocated {page_bytes} bytes against {ring_bytes} for the ring: \
         the page path is cloning the catalog and its replay ring again, once per page of \
         every relist"
    );

    let ((items, _rev), list_bytes) =
        bytes_allocated_by(mesh.list_at_revision("", "v1", "ConfigMap", Some("default"))).await;
    assert_eq!(items.len(), 1, "the one ConfigMap");
    assert!(
        list_bytes < 64 * 1024,
        "a one-item list allocated {list_bytes} bytes against {ring_bytes} for the ring"
    );
}

// Multi-thread flavor: the test body is polled on this thread while raft's
// tasks run on the workers, so the thread-local counter sees only the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_allocates_only_its_own_items_memory() {
    let mesh = memory_mesh("t3-2a-memory").await;
    assert_a_page_allocates_only_its_items(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_allocates_only_its_own_items_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-2a-durable").await;
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
    let mesh = memory_mesh("t3-2a-memory").await;
    assert_mesh_pages_concatenate(&mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mesh_pages_concatenate_to_the_mesh_list_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mesh = durable_mesh(dir.path(), "t3-2a-durable").await;
    assert_mesh_pages_concatenate(&mesh).await;
}
