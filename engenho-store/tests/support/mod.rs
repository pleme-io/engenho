//! Shared integration-test support: a thread-local counting allocator, and
//! the mesh boot + write helpers the allocation tests measure against.
//!
//! One copy, used by `t3_2a_paged_list` (a page clones only its items) and
//! `t3_2b_sealed_catalog` (the catalog's read surface clones nothing it does
//! not return). Each test binary that declares `mod support;` gets its own
//! `#[global_allocator]`; binaries that do not are unaffected.

// Each binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::revision::Revision;
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, WatchOpts, default_config};
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
pub async fn bytes_allocated_by<F: Future>(fut: F) -> (F::Output, usize) {
    ARMED_BYTES.with(|c| c.set(Some(0)));
    let out = fut.await;
    let bytes = ARMED_BYTES.with(|c| c.replace(None)).unwrap_or(0);
    (out, bytes)
}

/// Replay the whole ring and drain it as events, on this thread: returns how
/// many events it held and the bytes that allocated.
///
/// Each event carries its own copy of a retained post-image, so this is the
/// public read whose cost IS the ring, by design: the positive control every
/// allocation bound in these suites is measured against. Until T3.7 opening
/// the replay was enough, because registering cloned every retained change;
/// the replay now shares the ring's changes and copies nothing until a
/// change is turned into an event (`t3_7_relabel_out_of_selector` pins
/// that).
pub async fn replay_as_events(mesh: &StoreMesh) -> (usize, usize) {
    bytes_allocated_by(async {
        let mut replay = mesh
            .watch_from(WatchOpts::from_revision(Revision::ZERO))
            .await
            .expect("nothing is compacted yet");
        let mut events = 0usize;
        while let Some(Ok(signal)) = replay.try_next() {
            events += usize::from(signal.is_event());
        }
        events
    })
    .await
}

// =================================================================
// Mesh boot + writes
// =================================================================

pub const LEADERSHIP: Duration = Duration::from_secs(10);

/// A single-voter in-memory mesh, initialized and leading.
pub async fn memory_mesh(cluster: &str) -> Arc<StoreMesh> {
    let mesh = StoreMesh::start(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config(cluster).expect("config"),
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");
    Arc::new(mesh)
}

/// A single-voter durable (fjall) mesh over `dir`, initialized and leading.
pub async fn durable_mesh(dir: &std::path::Path, cluster: &str) -> Arc<StoreMesh> {
    let mesh = StoreMesh::start_durable(
        1,
        "in-process://1".into(),
        InProcessRouter::new(),
        default_config(cluster).expect("config"),
        dir,
    )
    .await
    .expect("start");
    mesh.initialize_singleton().await.expect("initialize");
    assert!(mesh.wait_for_leadership(LEADERSHIP).await, "leader");
    Arc::new(mesh)
}

pub async fn put(mesh: &StoreMesh, key: ResourceKey, value: serde_json::Value) {
    mesh.propose(ResourceCommand::Put {
        key,
        value,
        expected: None,
        reason: Reason::Operator,
    })
    .await
    .expect("propose");
}

/// The Helm release Secret [`seed_bulk_then_one_configmap`] rewrites.
pub fn release_secret() -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Secret", "default", "release")
}

/// Fill the catalog with bulk a small read must not pay for: one Secret
/// rewritten until the replay ring holds megabytes of its images, as Helm's
/// release Secrets filled it on rio. Then one ConfigMap.
///
/// Each rewrite changes the Secret (as each Helm release does): since T3.5
/// an identical rewrite commits nothing, so it would add nothing to the ring.
pub async fn seed_bulk_then_one_configmap(mesh: &StoreMesh) {
    let body = "A".repeat(16 * 1024);
    for version in 0..200u32 {
        put(
            mesh,
            release_secret(),
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
