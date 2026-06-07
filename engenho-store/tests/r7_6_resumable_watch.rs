//! R7.6 / M0.1 item 3 — resumable, gap-free, never-silently-lossy
//! watch.
//!
//! DESTINATION: a watch the apiserver hands straight to kubectl
//! `--watch` + controller informers, with the same atomic
//! list-then-watch contract etcd gives K8s. Proven here against BOTH
//! backends (Memory + Fjall) so the single `watch_backend`
//! implementation is exercised on both — no second copy.
//!
//! Matrix (per the test strategy):
//!   1. Resumability — exact replay-then-live.
//!   2. Gap-freedom at the boundary under concurrent writes (headline).
//!   3. Gone on compaction (registry-level, no Raft).
//!   4. Bookmark advance.
//!   5. Overflow → typed Gone, never silent loss, isolated per-watcher.
//!   6. Lock-held-send bound (regression guard — apply never blocks).

use std::sync::Arc;
use std::time::Duration;

use engenho_store::{
    InProcessRouter, ResourceCatalog, ResourceKey, Revision, StoreMesh, WatchEventKind, WatchGone,
    WatchOpts, WatchSignal,
    command::{Reason, ResourceCommand},
    default_config,
    watch_backend::WatcherRegistry,
};
use serde_json::json;

// ── boot helpers (parameterize Memory + Fjall) ────────────────────

async fn boot_memory() -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("r76-mem").unwrap();
    let mesh = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    mesh
}

async fn boot_fjall(dir: &std::path::Path) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config("r76-fjall").unwrap();
    let (mesh, _fresh) =
        StoreMesh::start_or_resume(1, "in-process://1".into(), router, cfg, dir.to_path_buf())
            .await
            .unwrap();
    let mesh = Arc::new(mesh);
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    mesh
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "engenho-r76-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

async fn put(mesh: &StoreMesh, name: &str, v: u64) -> u64 {
    mesh.propose(ResourceCommand::Put {
        key: pod_key(name),
        value: json!({"spec": {"v": v}}),
        expected: None,
        reason: Reason::Operator,
    })
    .await
    .unwrap()
    .revision
}

/// Next Event revision (skips bookmarks), with a timeout.
async fn next_event_rev(stream: &mut engenho_store::WatchStream) -> u64 {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("signal arrived before timeout")
        {
            Some(Ok(WatchSignal::Event(ev))) => return ev.resource_version,
            Some(Ok(WatchSignal::Bookmark(_))) => continue,
            Some(Err(g)) => panic!("unexpected WatchGone: {g:?}"),
            None => panic!("stream closed unexpectedly"),
        }
    }
}

// =================================================================
// 1. Resumability — exact replay-then-live
// =================================================================

async fn resumability_case(mesh: Arc<StoreMesh>) {
    // Put p1..p5 (revs 1..5).
    for i in 1..=5u64 {
        put(&mesh, &format!("p{i}"), i).await;
    }
    // Resume from rev 2 → replay yields 3,4,5.
    let mut w = mesh
        .watch_from(WatchOpts::from_revision(Revision(2)))
        .await
        .unwrap();
    assert_eq!(next_event_rev(&mut w).await, 3);
    assert_eq!(next_event_rev(&mut w).await, 4);
    assert_eq!(next_event_rev(&mut w).await, 5);

    // Concurrently Put p6,p7 (revs 6,7) — the live continuation.
    put(&mesh, "p6", 6).await;
    put(&mesh, "p7", 7).await;
    assert_eq!(next_event_rev(&mut w).await, 6, "no gap at 5→6");
    assert_eq!(next_event_rev(&mut w).await, 7, "contiguous live tail");

    drop(w);
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

#[tokio::test]
async fn resumability_memory() {
    resumability_case(boot_memory().await).await;
}

#[tokio::test]
async fn resumability_fjall() {
    let dir = temp_dir("resume");
    let _ = std::fs::remove_dir_all(&dir);
    resumability_case(boot_fjall(&dir).await).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// =================================================================
// 2. Gap-freedom at the boundary under concurrent writes (headline)
// =================================================================

async fn gap_freedom_iteration(mesh: &Arc<StoreMesh>, n: u64) {
    // rv0 = the revision JUST before we subscribe.
    let rv0 = mesh.current_catalog().await.revision();

    // Start the watch (mid-storm in the spirit of the spec: subscribe,
    // then immediately hammer writes).
    let mut w = mesh
        .watch_from(WatchOpts::from_revision(rv0))
        .await
        .unwrap();

    // Writer task: N puts on distinct keys as fast as propose allows.
    let writer = {
        let mesh = mesh.clone();
        let base = rv0.get();
        tokio::spawn(async move {
            let mut last = base;
            for i in 1..=n {
                last = put(&mesh, &format!("k{base}_{i}"), i).await;
            }
            last
        })
    };
    let final_rev = writer.await.unwrap();

    // Drain until we reach the writer's final revision. Collect the
    // multiset of delivered Event revisions.
    let mut delivered: Vec<u64> = Vec::new();
    while delivered.last().copied() != Some(final_rev) {
        let rev = next_event_rev(&mut w).await;
        delivered.push(rev);
    }

    // Expected: the exact contiguous integer range (rv0, final_rev].
    let expected: Vec<u64> = (rv0.get() + 1..=final_rev).collect();
    assert_eq!(
        delivered, expected,
        "gap-free + dup-free: delivered must equal the contiguous range (rv0, final]"
    );

    drop(w);
}

#[tokio::test]
async fn gap_freedom_memory() {
    let mesh = boot_memory().await;
    // ~50 iterations to shake the boundary race; N kept modest so the
    // in-process Raft stays fast.
    for _ in 0..50 {
        gap_freedom_iteration(&mesh, 20).await;
    }
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

#[tokio::test]
async fn gap_freedom_fjall() {
    let dir = temp_dir("gapfree");
    let _ = std::fs::remove_dir_all(&dir);
    let mesh = boot_fjall(&dir).await;
    // Fjall fsyncs per apply → fewer iterations to keep the test brisk;
    // the boundary logic is identical (single implementation).
    for _ in 0..15 {
        gap_freedom_iteration(&mesh, 10).await;
    }
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// =================================================================
// 2b. NON-EMPTY replay → live ordering at the boundary (regression)
// =================================================================
//
// The original gap-freedom cases (2 above) subscribe at rv0 == the
// CURRENT tip, so `changes_since(tip)` is EMPTY — the replay→live
// boundary with NON-EMPTY replay content was never exercised. This is
// EXACTLY the case the adversarial review reproduced as delivery order
// [6, 3, 4, 5]: a live event at boundary+1 overtook not-yet-fed replay
// revisions 3,4,5 because the racy `tokio::spawn(replay_sender.feed)`
// path pushed the replay from a SEPARATE task AFTER the lock dropped,
// while the apply path's `fan_change` ran under the lock.
//
// Here we subscribe from an OLD revision so the replay is non-empty,
// WHILE concurrently applying writes that interleave at the boundary,
// and assert the delivered stream equals the EXACT contiguous ordered
// range (from, final] — no reorder, no gap, no dup. FAILS before the
// structural fix (replay fed under the lock), PASSES after.

async fn nonempty_replay_boundary_iteration(
    mesh: &Arc<StoreMesh>,
    backlog: u64,
    live: u64,
) {
    // (1) Build a NON-EMPTY backlog: put `backlog` revisions, then pick
    // a resume point `from` partway through so the replay is non-empty.
    let base = mesh.current_catalog().await.revision().get();
    for i in 1..=backlog {
        put(&mesh, &format!("bk{base}_{i}"), i).await;
    }
    // Resume from the MIDPOINT of the backlog → replay covers the upper
    // half (non-empty), and `from` < current tip.
    let from = Revision(base + backlog / 2);
    let final_before_live = base + backlog;

    // (2) Subscribe from the OLD revision (NON-EMPTY replay sits in the
    // channel), then drive `live` writes that land at the boundary. With
    // the old spawned-feeder path, an apply at boundary+1 could `fan` its
    // event into the channel BEFORE the feeder task drained the replay —
    // delivering it ahead of the replay revisions (the [6,3,4,5] defect).
    let mut w = mesh.watch_from(WatchOpts::from_revision(from)).await.unwrap();

    let writer = {
        let mesh = mesh.clone();
        let start = final_before_live;
        tokio::spawn(async move {
            let mut last = start;
            for i in 1..=live {
                last = put(&mesh, &format!("lv{start}_{i}"), i).await;
            }
            last
        })
    };

    // (3) Drain concurrently with the writer (don't `await` it first —
    // that would hand the runtime time to drain the feeder before any
    // live event lands, masking the race). Collect delivered Event
    // revisions IN ARRIVAL ORDER until we reach the writer's final.
    let final_rev = final_before_live + live;
    let mut delivered: Vec<u64> = Vec::new();
    while delivered.last().copied() != Some(final_rev) {
        delivered.push(next_event_rev(&mut w).await);
    }
    let reported_final = writer.await.unwrap();
    assert_eq!(reported_final, final_rev, "writer reached the expected tip");

    // The exact contiguous integer range (from, final] — replay tail
    // FIRST (from+1 .. final_before_live), then the live tail
    // (final_before_live+1 .. final), in ARRIVAL ORDER, with no
    // reorder/gap/dup.
    let expected: Vec<u64> = (from.get() + 1..=final_rev).collect();
    assert_eq!(
        delivered, expected,
        "NON-EMPTY replay→live: delivered must equal the contiguous ordered range (from, final] \
         — a live event must NEVER overtake not-yet-delivered replay revisions"
    );

    drop(w);
}

#[tokio::test]
async fn nonempty_replay_boundary_ordering_memory() {
    let mesh = boot_memory().await;
    // Many iterations to shake the boundary race; backlog ensures a
    // non-empty replay, live writes interleave at the boundary.
    for _ in 0..40 {
        nonempty_replay_boundary_iteration(&mesh, 20, 10).await;
    }
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

#[tokio::test]
async fn nonempty_replay_boundary_ordering_fjall() {
    let dir = temp_dir("nonempty-replay");
    let _ = std::fs::remove_dir_all(&dir);
    let mesh = boot_fjall(&dir).await;
    // Fjall fsyncs per apply → fewer iterations; identical boundary logic.
    for _ in 0..12 {
        nonempty_replay_boundary_iteration(&mesh, 12, 6).await;
    }
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Registry-level mirror driving the primitive directly (no Raft, no
/// store): register from rev2 → replay {3,4,5}; IMMEDIATELY fan_change
/// rev6 (before the first poll); assert delivery is exactly 3,4,5,6.
/// This is the tightest reproduction of the [6,3,4,5] defect — it
/// FAILS with the old spawned-feeder path and PASSES with the replay
/// fed under the lock inside `register_captured`.
#[tokio::test]
async fn registry_nonempty_replay_then_immediate_live_is_ordered() {
    let mut cat = ResourceCatalog::default();
    for i in 1..=5u64 {
        cat.apply(
            &ResourceCommand::Put {
                key: pod_key(&format!("p{i}")),
                value: json!({"i": i}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            i,
        );
    }
    let mut reg = WatcherRegistry::new();
    // Register from rev2 → replay {3,4,5} enqueued under this call.
    let mut stream = reg
        .register(&cat, &WatchOpts::from_revision(Revision(2)))
        .unwrap();
    // Live rev6 fanned BEFORE any poll (boundary == 5, so 6 > boundary).
    cat.apply(
        &ResourceCommand::Put {
            key: pod_key("p6"),
            value: json!({"i": 6}),
            expected: None,
            reason: Reason::Operator,
        },
        1,
        6,
    );
    let change6 = cat.changes_since(Revision(5)).unwrap().pop().unwrap();
    assert_eq!(change6.revision, Revision(6));
    reg.fan_change(&change6);

    let mut got = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("signal before timeout")
        {
            Some(Ok(WatchSignal::Event(ev))) => got.push(ev.resource_version),
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(
        got,
        vec![3, 4, 5, 6],
        "replay {{3,4,5}} must precede the immediate live event 6 — no reorder"
    );
}

/// Headline variant: race a fresh watch_from(rv0) against the single
/// propose whose revision == rv0+1, asserting that event is delivered
/// exactly once regardless of interleaving.
#[tokio::test]
async fn boundary_single_event_delivered_exactly_once_memory() {
    let mesh = boot_memory().await;
    for _ in 0..50 {
        let rv0 = mesh.current_catalog().await.revision();
        let mut w = mesh
            .watch_from(WatchOpts::from_revision(rv0))
            .await
            .unwrap();
        let next = put(&mesh, &format!("solo{}", rv0.get()), 1).await;
        assert_eq!(next, rv0.get() + 1);
        // Exactly one event at rv0+1, then no duplicate.
        assert_eq!(next_event_rev(&mut w).await, next);
        // No second copy of the same revision is queued.
        // (Drain a brief window — must be empty.)
        let dup = tokio::time::timeout(Duration::from_millis(30), w.next()).await;
        assert!(dup.is_err(), "the boundary event must be delivered exactly once");
        drop(w);
    }
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

// =================================================================
// 3. Gone on compaction (registry-level, no Raft)
// =================================================================

#[tokio::test]
async fn gone_on_compaction_registry() {
    // Tiny history capacity forces compaction. Build the catalog
    // directly (per the test strategy: a direct ResourceCatalog +
    // registry unit test where no Raft is needed).
    let mut cat = ResourceCatalog::with_history_capacity(2);
    for i in 1..=5u64 {
        cat.apply(
            &ResourceCommand::Put {
                key: pod_key(&format!("p{i}")),
                value: json!({"i": i}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            i,
        );
    }
    // Only revs 4,5 retained → compacted at rev 3.
    assert_eq!(cat.compacted_revision(), Revision(3));

    let mut reg = WatcherRegistry::new();
    // watch_from below the watermark → immediate typed Gone, NO channel.
    let err = reg
        .register(&cat, &WatchOpts::from_revision(Revision(1)))
        .unwrap_err();
    assert_eq!(
        err,
        WatchGone::CompactedTooOld {
            requested: Revision(1),
            compacted: Revision(3),
        }
    );
    assert_eq!(err.kind(), "compacted_too_old");
    assert_eq!(reg.len(), 0, "rejected watch allocates no channel");

    // From exactly the watermark succeeds (watermark honored); the
    // replay (revs 4,5) is enqueued into the stream — drain it to prove
    // exactly 2 events landed.
    let mut stream = reg
        .register(&cat, &WatchOpts::from_revision(Revision(3)))
        .unwrap();
    assert_eq!(reg.len(), 1);
    let mut replayed = Vec::new();
    while let Some(Ok(WatchSignal::Event(ev))) = stream.try_next() {
        replayed.push(ev.resource_version);
    }
    assert_eq!(replayed, vec![4, 5]);
}

// =================================================================
// 4. Bookmark advance
// =================================================================

async fn bookmark_case(mesh: Arc<StoreMesh>) {
    // Quiescent store, fast bookmark cadence.
    let mut w = mesh
        .watch_from(WatchOpts {
            from: mesh.current_catalog().await.revision(),
            buffer: 64,
            bookmark_every: Duration::from_millis(50),
        })
        .await
        .unwrap();

    // At least one Bookmark arrives at the current revision.
    let rev_before = mesh.current_catalog().await.revision();
    let first_bm = wait_for_bookmark(&mut w).await;
    assert_eq!(first_bm, rev_before, "bookmark marks the current revision");

    // Put once → a later bookmark's revision is >= the new revision.
    let new_rev = put(&mesh, "after-bm", 1).await;
    // Drain the resulting Event first (so the bookmark logic can advance
    // past last_seen).
    let ev = next_event_rev(&mut w).await;
    assert_eq!(ev, new_rev);
    let later_bm = wait_for_bookmark(&mut w).await;
    assert!(
        later_bm.get() >= new_rev,
        "bookmark advanced to >= the new revision ({} >= {new_rev})",
        later_bm.get()
    );
    // Consecutive bookmarks are non-decreasing.
    assert!(later_bm >= first_bm);

    // Cross-check: watch_from(bookmarked_rev) replays zero events.
    let mut w2 = mesh
        .watch_from(WatchOpts::from_revision(later_bm))
        .await
        .unwrap();
    let replayed = tokio::time::timeout(Duration::from_millis(80), next_event_rev(&mut w2)).await;
    assert!(
        replayed.is_err(),
        "a bookmark is a valid resume point — replays nothing"
    );

    drop((w, w2));
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

/// Wait for the next Bookmark (ignoring events).
async fn wait_for_bookmark(stream: &mut engenho_store::WatchStream) -> Revision {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("bookmark arrived before timeout")
        {
            Some(Ok(WatchSignal::Bookmark(rev))) => return rev,
            Some(Ok(WatchSignal::Event(_))) => continue,
            Some(Err(g)) => panic!("unexpected WatchGone: {g:?}"),
            None => panic!("stream closed unexpectedly"),
        }
    }
}

#[tokio::test]
async fn bookmark_memory() {
    bookmark_case(boot_memory().await).await;
}

#[tokio::test]
async fn bookmark_fjall() {
    let dir = temp_dir("bookmark");
    let _ = std::fs::remove_dir_all(&dir);
    bookmark_case(boot_fjall(&dir).await).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// =================================================================
// 5. Overflow yields typed Gone, never silent loss, isolated per-watcher
// =================================================================

async fn overflow_case(mesh: Arc<StoreMesh>) {
    // A SECOND watcher opened before the storm with an adequate buffer,
    // actively drained → must receive ALL events.
    let fast = mesh
        .watch_from(WatchOpts::live_tail(
            mesh.current_catalog().await.revision(),
            1024,
        ))
        .await
        .unwrap();
    let fast = Arc::new(tokio::sync::Mutex::new(fast));
    // Drainer task: continuously pull from `fast` so it never overflows.
    let drained = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));
    let fast_task = {
        let fast = fast.clone();
        let drained = drained.clone();
        tokio::spawn(async move {
            loop {
                let item = {
                    let mut g = fast.lock().await;
                    g.next().await
                };
                match item {
                    Some(Ok(WatchSignal::Event(ev))) => {
                        drained.lock().await.push(ev.resource_version);
                    }
                    Some(Ok(WatchSignal::Bookmark(_))) => {}
                    Some(Err(g)) => panic!("fast watcher should not Gone: {g:?}"),
                    None => break,
                }
            }
        })
    };

    // Slow watcher: buffer 4, NOT polled during the storm.
    let mut slow = mesh
        .watch_from(WatchOpts::live_tail(
            mesh.current_catalog().await.revision(),
            4,
        ))
        .await
        .unwrap();

    // Propose 50 puts WITHOUT polling `slow` (force the bounded channel
    // full). `propose` must still return for each (apply never blocks on
    // the stalled slow watcher).
    let mut last = 0;
    for i in 1..=50u64 {
        last = put(&mesh, &format!("storm{i}"), i).await;
    }

    // Resume polling slow: a prefix of in-order events, then terminal
    // Overflow with last_seen == last delivered event revision.
    let mut slow_revs: Vec<u64> = Vec::new();
    let overflow_last_seen;
    loop {
        match tokio::time::timeout(Duration::from_secs(3), slow.next())
            .await
            .expect("slow signal before timeout")
        {
            Some(Ok(WatchSignal::Event(ev))) => slow_revs.push(ev.resource_version),
            Some(Ok(WatchSignal::Bookmark(_))) => {}
            Some(Err(WatchGone::Overflow { capacity, last_seen })) => {
                assert_eq!(capacity, 4, "overflow reports the watcher's buffer");
                overflow_last_seen = last_seen;
                break;
            }
            Some(Err(other)) => panic!("expected Overflow, got {other:?}"),
            None => panic!("slow closed without Overflow"),
        }
    }
    assert!(!slow_revs.is_empty(), "slow delivered a prefix before Gone");
    // in-order prefix
    let mut sorted = slow_revs.clone();
    sorted.sort_unstable();
    assert_eq!(slow_revs, sorted, "prefix is in revision order");
    assert_eq!(
        overflow_last_seen.get(),
        *slow_revs.last().unwrap(),
        "last_seen == revision of the last delivered event"
    );
    // Stream ends after the single terminal.
    assert!(slow.next().await.is_none(), "stream ends after Overflow");

    // The fast watcher received ALL 50 — overflow was isolated.
    // Give the drainer a moment to catch up.
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let g = drained.lock().await;
        let expected: Vec<u64> = ((last - 49)..=last).collect();
        assert_eq!(*g, expected, "fast watcher got ALL 50 events in order");
    }

    // Resumability-after-Gone: watch_from(last_seen) delivers exactly
    // the events the overflowed watcher missed, contiguously.
    let mut resumed = mesh
        .watch_from(WatchOpts::from_revision(overflow_last_seen))
        .await
        .unwrap();
    let mut resumed_revs = Vec::new();
    while resumed_revs.last().copied() != Some(last) {
        resumed_revs.push(next_event_rev(&mut resumed).await);
    }
    let expected_missed: Vec<u64> = (overflow_last_seen.get() + 1..=last).collect();
    assert_eq!(
        resumed_revs, expected_missed,
        "resume-from-last_seen delivers exactly the missed contiguous tail"
    );

    fast_task.abort();
    drop((slow, resumed, fast));
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

#[tokio::test]
async fn overflow_memory() {
    overflow_case(boot_memory().await).await;
}

#[tokio::test]
async fn overflow_fjall() {
    let dir = temp_dir("overflow");
    let _ = std::fs::remove_dir_all(&dir);
    overflow_case(boot_fjall(&dir).await).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// =================================================================
// 6. Lock-held-send bound — apply never blocks on a stalled watcher
// =================================================================

async fn lock_held_send_bound_case(mesh: Arc<StoreMesh>) {
    // A stalled buffer-1 watcher (never polled). Each propose must
    // still return promptly — try_send is non-blocking, so the in-lock
    // fan-out never awaits the slow consumer.
    let _stalled = mesh
        .watch_from(WatchOpts::live_tail(
            mesh.current_catalog().await.revision(),
            1,
        ))
        .await
        .unwrap();

    for i in 1..=20u64 {
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            put(&mesh, &format!("bound{i}"), i),
        )
        .await;
        assert!(
            res.is_ok(),
            "propose #{i} must return within the timeout even with a stalled watcher"
        );
    }

    drop(_stalled);
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}

#[tokio::test]
async fn lock_held_send_bound_memory() {
    lock_held_send_bound_case(boot_memory().await).await;
}

#[tokio::test]
async fn lock_held_send_bound_fjall() {
    let dir = temp_dir("bound");
    let _ = std::fs::remove_dir_all(&dir);
    lock_held_send_bound_case(boot_fjall(&dir).await).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// =================================================================
// Sanity: Added/Modified/Deleted kinds flow through replay + live
// =================================================================

#[tokio::test]
async fn event_kinds_through_watch_from_memory() {
    let mesh = boot_memory().await;
    // Create + replace + delete on one key (revs 1,2,3).
    put(&mesh, "k", 1).await;
    put(&mesh, "k", 2).await;
    mesh.propose(ResourceCommand::Delete {
        key: pod_key("k"),
        expected: None,
        reason: Reason::Operator,
    })
    .await
    .unwrap();

    // Resume from the beginning → replay all three with correct kinds.
    let mut w = mesh
        .watch_from(WatchOpts::from_revision(Revision::ZERO))
        .await
        .unwrap();
    let mut kinds = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_secs(3), w.next())
            .await
            .unwrap()
        {
            Some(Ok(WatchSignal::Event(ev))) => kinds.push(ev.kind),
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!(
        kinds,
        vec![
            WatchEventKind::Added,
            WatchEventKind::Modified,
            WatchEventKind::Deleted
        ]
    );

    drop(w);
    Arc::try_unwrap(mesh).ok().unwrap().terminate().await.unwrap();
}
