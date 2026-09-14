//! Write-latency probe — a DIAGNOSTIC, not a gate.
//!
//! Measured on rio 2026-09-14: a single small Secret write through engenho's
//! apiserver took 11–15s on a freshly-restarted daemon and 60–68s on one that
//! had been up 2.5h, against a catalog of ~50 objects. Helm's client timeout is
//! 30s, so Flux could not converge anything and wedged for five days.
//!
//! Everything cheap was ruled out first, by measurement rather than reasoning:
//! the disk (7ms per fsync, ext4, IO pressure <1%), the box (load 12 on 32
//! cores, nothing else hot), the catalog size (~50 objects), absent raft peers
//! (single node, leadership reached), admission webhooks (none registered), and
//! audit (does not write to the store and does not audit reads).
//!
//! This probe isolates the remaining question: is the store's own propose path
//! slow on an EMPTY durable store? A fast result here moves the cause to
//! accumulated state or to a layer above the store; a slow result puts it in
//! the store path itself and gives a local reproduction to profile against.
//!
//! Run explicitly: `cargo test -p engenho-store --test write_latency_probe -- --nocapture --ignored`

use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

/// A Secret-shaped payload, so the probe measures what Helm actually writes.
fn secret(name: &str, payload: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": "default" },
        "type": "Opaque",
        "data": { "payload": payload }
    })
}

#[tokio::test]
#[ignore = "diagnostic probe; run explicitly with --ignored --nocapture"]
async fn propose_latency_on_an_empty_durable_store() {
    let dir = std::env::temp_dir().join(format!("engenho-latency-probe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let router = InProcessRouter::new();
    let cfg = default_config("latency-probe").unwrap();
    let store = Arc::new(
        StoreMesh::start_durable(1, "in-process://1".into(), router, cfg, &dir)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(
        store.wait_for_leadership(Duration::from_secs(10)).await,
        "the probe needs a leader before it can time a write"
    );

    // Small payload — the rio measurement showed the SMALL write was slower
    // than a 200KB one, so payload size is not the variable under test.
    let mut timings = Vec::new();
    for i in 0..20 {
        let key = ResourceKey::namespaced("", "v1", "Secret", "default", &format!("probe-{i}"));
        let value = secret(&format!("probe-{i}"), "YQ==");
        let t = Instant::now();
        store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
        timings.push(t.elapsed());
    }

    timings.sort_unstable();
    let total: Duration = timings.iter().sum();
    let median = timings[timings.len() / 2];
    let max = *timings.last().unwrap();

    println!("--- propose latency, empty durable store, n={} ---", timings.len());
    println!("median : {median:?}");
    println!("max    : {max:?}");
    println!("mean   : {:?}", total / u32::try_from(timings.len()).unwrap());
    println!("total  : {total:?}");

    // No assertion on an absolute number: this is a diagnostic, and a
    // threshold pinned to one machine's speed is a flake generator. The
    // COMPARISON is the finding — rio served this same shape in 11-15s.
    let _ = std::fs::remove_dir_all(&dir);
}

/// THE REPRODUCTION: latency as a function of how full the watch-replay
/// history ring is.
///
/// `ResourceCatalog` carries `history: VecDeque<Change>` (capacity 8192, each
/// entry holding a full resource body) and `current_catalog()` returns
/// `state.catalog.clone()` — a DEEP clone. Every read and every write pays for
/// copying the whole ring, so cost grows with cluster AGE rather than with
/// cluster SIZE. That is why rio, with ~50 live objects, served a write in 12s
/// while this same code serves one in 12ms against an empty store, and why a
/// daemon restart "fixes" it for a few hours.
/// THE GATE: a LIST must not get slower as the watch-replay ring fills.
///
/// Fixed 2026-09-14. `MeshStore::list_at_revision` used to call
/// `current_catalog()`, which deep-clones `ResourceCatalog` — including its
/// 8192-entry `history: VecDeque<Change>` ring — so every read copied the ring
/// and read cost scaled with the cluster's AGE rather than its object count.
///
/// Measured before the fix, rewriting ONE object: 4.4ms at depth 1000 →
/// 31.8ms at depth 8000, plateauing exactly at the 8192 cap. After: 9.8µs →
/// 17.4µs, flat. On rio, whose ring held ~100KB Helm release Secrets, the
/// clone reached hundreds of MB per request — a single Secret write took 12s
/// on a fresh daemon and 60s+ after hours, against ~50 live objects. Helm
/// times out at 30s, so FluxCD could not converge and wedged for five days,
/// and a daemon restart "fixed" it only by emptying the ring.
///
/// The threshold is deliberately loose. Post-fix is ~10µs and pre-fix was
/// ~13ms at this depth, so 2ms sits ~200x above the healthy value and ~6x
/// below the broken one: it cannot flake on a slow machine and cannot pass if
/// the clone returns.
#[tokio::test]
async fn a_list_does_not_slow_down_as_the_history_ring_fills() {
    let dir = std::env::temp_dir().join(format!("engenho-history-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let router = InProcessRouter::new();
    let cfg = default_config("history-gate").unwrap();
    let store = Arc::new(
        StoreMesh::start_durable(1, "in-process://1".into(), router, cfg, &dir)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(10)).await);

    let body = "A".repeat(4096);
    let key = ResourceKey::namespaced("", "v1", "Secret", "default", "churn");

    // Depth 3000 is past where the regression was unmistakable (13.6ms) while
    // keeping the gate fast enough to live in the default suite.
    for _ in 0..3000 {
        store
            .propose(ResourceCommand::Put {
                key: key.clone(),
                value: secret("churn", &body),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    // Best of 5 — one scheduling hiccup must not turn the gate red.
    let mut best = Duration::from_secs(60);
    for _ in 0..5 {
        let t = Instant::now();
        let (items, _rev) = store
            .list_at_revision("", "v1", "Secret", Some("default"))
            .await;
        best = best.min(t.elapsed());
        assert_eq!(items.len(), 1, "one object, rewritten 3000 times");
    }

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        best < Duration::from_millis(2),
        "a LIST of ONE object took {best:?} with a full history ring — the read \
         path is cloning the watch-replay history again, which makes every read \
         and write scale with cluster AGE instead of object count (this wedged \
         FluxCD on rio for five days)"
    );
}

/// THE SECOND GATE: establishing a watch must not get slower as the ring fills.
///
/// The first gate covers LIST. This covers the path that actually took rio
/// down a second time, AFTER the LIST fix shipped: the apiserver resolved a
/// "watch from now" by calling `current_catalog().revision()` — reading one
/// `u64` by deep-cloning every resource plus the 8192-entry replay ring, whose
/// entries hold a full post-image AND pre-image each. Watches register under
/// the same lock `apply` needs, so each one stalled every concurrent WRITE.
///
/// Measured on rio with FluxCD (dozens of watches) reconciling: writes went
/// from ~40ms back to 27-51s, daemon at ~2 cores. `MostRecent` is the common
/// case — every "watch from now" took that branch.
#[tokio::test]
async fn reading_the_current_revision_does_not_scale_with_history_depth() {
    let dir = std::env::temp_dir().join(format!("engenho-rev-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let router = InProcessRouter::new();
    let cfg = default_config("rev-gate").unwrap();
    let store = Arc::new(
        StoreMesh::start_durable(1, "in-process://1".into(), router, cfg, &dir)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(10)).await);

    let body = "A".repeat(4096);
    let key = ResourceKey::namespaced("", "v1", "Secret", "default", "churn");
    for _ in 0..3000 {
        store
            .propose(ResourceCommand::Put {
                key: key.clone(),
                value: secret("churn", &body),
                expected: None,
                reason: Reason::Operator,
            })
            .await
            .unwrap();
    }

    let mut best = Duration::from_secs(60);
    for _ in 0..5 {
        let t = Instant::now();
        let _rev = store.current_revision().await;
        best = best.min(t.elapsed());
    }

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        best < Duration::from_millis(2),
        "reading the current revision took {best:?} with a full history ring — \
         it is cloning the catalog to read one integer, which stalls every \
         concurrent write because watches register under the apply lock"
    );
}

#[tokio::test]
#[ignore = "diagnostic probe; run explicitly with --ignored --nocapture"]
async fn latency_grows_with_history_depth_not_object_count() {
    let dir = std::env::temp_dir().join(format!("engenho-history-probe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let router = InProcessRouter::new();
    let cfg = default_config("history-probe").unwrap();
    let store = Arc::new(
        StoreMesh::start_durable(1, "in-process://1".into(), router, cfg, &dir)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(10)).await);

    // ~4KB per entry, the rough size of a real Secret / Pod status.
    let body = "A".repeat(4096);

    // Rewrite ONE key over and over: object count stays at 1, history depth
    // climbs. If cost tracked object count this would stay flat.
    let key = ResourceKey::namespaced("", "v1", "Secret", "default", "churn");
    let mut samples: Vec<(usize, Duration)> = Vec::new();

    for round in 0..10 {
        // 1000 writes per round, so depth passes the 8192 cap mid-run.
        for _ in 0..1000 {
            store
                .propose(ResourceCommand::Put {
                    key: key.clone(),
                    value: secret("churn", &body),
                    expected: None,
                    reason: Reason::Operator,
                })
                .await
                .unwrap();
        }
        let depth = (round + 1) * 1000;

        // Time a LIST — the read path that clones the catalog.
        let t = Instant::now();
        let _ = store.list("", "v1", "Secret", Some("default")).await;
        let read = t.elapsed();
        samples.push((depth, read));
        println!("after {depth:>5} writes (1 object): list = {read:?}");
    }

    println!("\n--- verdict ---");
    let (_, first) = samples[0];
    let (_, last) = *samples.last().unwrap();
    println!("list at depth 1000  : {first:?}");
    println!("list at depth 10000 : {last:?}");
    if last > first * 3 {
        println!(
            "CONFIRMED: read cost scales with HISTORY DEPTH, not object count \
             (1 object throughout)."
        );
    } else {
        println!("NOT confirmed by this probe — look elsewhere.");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
