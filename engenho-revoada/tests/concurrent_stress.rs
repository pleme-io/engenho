//! Concurrent + stress tests. N threads hammer the substrate to
//! catch race conditions, deadlocks, watch-stream desync, snapshot
//! consistency under concurrent writes.

use std::sync::Arc;
use std::thread;

use engenho_revoada::face::{Face, ResourceFormat, ResourceRef};
use engenho_revoada::topology::Quorum3M;
use engenho_revoada::{
    Cluster, FabricFace, FabricStrategy, FaceKind, FederatedFabric, RoutingPolicy,
};

fn yaml(name: &str, ns: &str) -> Vec<u8> {
    format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {ns}\nspec: {{}}\n")
        .into_bytes()
}

fn raft_face_shared(name: &str) -> Arc<engenho_revoada::PureRaftFace> {
    Arc::new(
        engenho_revoada::PureRaftFace::from_declaration(&FabricFace {
            name: name.into(),
            kind: FaceKind::PureRaft,
        })
        .unwrap(),
    )
}

fn cluster_shared(name: &str) -> Arc<Cluster> {
    Arc::new(
        Cluster::builder()
            .strategy(FabricStrategy::prescribed_homelab())
            .face_pure_raft(name)
            .topology(Quorum3M)
            .start()
            .unwrap(),
    )
}

// ── Concurrent apply ─────────────────────────────────────────────

#[test]
fn concurrent_apply_n_threads_n_distinct_refs_all_land() {
    let face = raft_face_shared("concurrent-apply");
    let threads = 8;
    let per_thread = 32;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let f = Arc::clone(&face);
            thread::spawn(move || {
                for i in 0..per_thread {
                    let name = format!("pod-{t}-{i}");
                    f.apply_resource(ResourceFormat::Yaml, &yaml(&name, "default"))
                        .unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(face.resource_count(), threads * per_thread);
}

#[test]
fn concurrent_apply_same_ref_last_writer_wins_no_data_corruption() {
    // N threads hammering the SAME ref: store still ends up
    // holding exactly one entry; the body matches some writer's
    // last value.
    let face = raft_face_shared("concurrent-same-ref");
    let threads = 16;
    let writes_per_thread = 100;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let f = Arc::clone(&face);
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let body = format!(
                        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: hot\n  namespace: default\nspec:\n  containers:\n    - image: t{t}-i{i}\n"
                    )
                    .into_bytes();
                    f.apply_resource(ResourceFormat::Yaml, &body).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    // Exactly one resource at the hot ref.
    assert_eq!(face.resource_count(), 1);
    let r = ResourceRef::namespaced("Pod", "hot", "default");
    // Body is valid YAML (parses correctly).
    let body = face.get_resource(&r, ResourceFormat::Yaml).unwrap();
    let parsed: serde_yaml::Value = serde_yaml::from_slice(&body).unwrap();
    assert!(parsed.get("kind").is_some(), "body should be valid YAML");
}

// ── Concurrent watch + writers ───────────────────────────────────

#[test]
fn concurrent_watch_receives_all_writes_no_loss() {
    let face = raft_face_shared("concurrent-watch");
    let mut watch = face
        .watch_resources("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();

    let writes = 50;
    let f = Arc::clone(&face);
    let writer = thread::spawn(move || {
        for i in 0..writes {
            let name = format!("pod-{i}");
            f.apply_resource(ResourceFormat::Yaml, &yaml(&name, "default"))
                .unwrap();
        }
    });

    let mut received = 0;
    while received < writes {
        let _ = watch.next_event().unwrap().expect("event");
        received += 1;
    }
    writer.join().unwrap();
    assert_eq!(received, writes);
}

#[test]
fn concurrent_multi_watch_each_subscriber_sees_every_event() {
    let face = raft_face_shared("multi-watch");
    let n_subscribers = 4;
    let writes = 20;

    // Open N subscribers BEFORE any writes.
    let mut subscribers = Vec::new();
    for _ in 0..n_subscribers {
        let w = face
            .watch_resources("Pod", Some("default"), ResourceFormat::Yaml)
            .unwrap();
        subscribers.push(w);
    }

    // Writer thread fires off all writes.
    let f = Arc::clone(&face);
    let writer = thread::spawn(move || {
        for i in 0..writes {
            let name = format!("pod-{i}");
            f.apply_resource(ResourceFormat::Yaml, &yaml(&name, "default"))
                .unwrap();
        }
    });

    // Each subscriber pulls exactly `writes` events.
    for (idx, mut w) in subscribers.into_iter().enumerate() {
        for ev_idx in 0..writes {
            let _ = w
                .next_event()
                .unwrap()
                .unwrap_or_else(|| panic!("subscriber {idx} event {ev_idx}: stream closed"));
        }
    }
    writer.join().unwrap();
}

// ── Snapshot during writes ───────────────────────────────────────

#[test]
fn snapshot_during_concurrent_writes_is_internally_consistent() {
    let face = raft_face_shared("snap-during");
    // Seed with some state.
    for i in 0..20 {
        let name = format!("seed-{i}");
        face.apply_resource(ResourceFormat::Yaml, &yaml(&name, "default"))
            .unwrap();
    }
    let seed_count = face.resource_count();

    // Background writer keeps applying.
    let f = Arc::clone(&face);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_w = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut i = 0;
        while !stop_w.load(std::sync::atomic::Ordering::Relaxed) {
            let name = format!("live-{i}");
            f.apply_resource(ResourceFormat::Yaml, &yaml(&name, "default"))
                .ok();
            i += 1;
        }
    });

    // Take 10 snapshots during heavy concurrent writes; each must
    // be a valid CBOR-decodable byte buffer with at least the
    // seeded entries.
    for _ in 0..10 {
        let snap = face.snapshot().unwrap();
        // Restore into a fresh face — should not panic.
        let fresh = raft_face_shared("fresh");
        fresh.restore(&snap).unwrap();
        // The fresh count should be >= seed_count (writer keeps
        // adding; snapshot's view is at or after the seeded state).
        assert!(fresh.resource_count() >= seed_count);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
}

// ── Federation under concurrent writes ──────────────────────────

#[test]
fn federation_concurrent_apply_across_members() {
    let mut map = std::collections::HashMap::new();
    map.insert("alpha".to_string(), 0);
    map.insert("beta".to_string(), 1);
    let policy = RoutingPolicy::NamespacePrefix {
        map,
        default_member: None,
    };
    let federation = Arc::new(
        FederatedFabric::new(
            vec![cluster_shared("alpha"), cluster_shared("beta")],
            policy,
        )
        .unwrap(),
    );

    let threads = 8;
    let per_thread = 25;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let f = Arc::clone(&federation);
            thread::spawn(move || {
                for i in 0..per_thread {
                    let ns = if i % 2 == 0 { "alpha" } else { "beta" };
                    let name = format!("t{t}-i{i}");
                    let r = ResourceRef::namespaced("Pod", &name, ns);
                    f.apply(&r, ResourceFormat::Yaml, &yaml(&name, ns)).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let total = federation.members()[0].health().resource_count
        + federation.members()[1].health().resource_count;
    assert_eq!(total, threads * per_thread);
}

// ── Drop under concurrent access ────────────────────────────────

#[test]
fn cluster_drop_during_active_watch_does_not_panic() {
    let cluster = cluster_shared("drop-during-watch");
    let mut watch = cluster
        .watch("Pod", Some("default"), ResourceFormat::Yaml)
        .unwrap();
    // Spawn a reader that loops on next_event — it will block
    // forever once the channel sender drops.
    let reader = thread::spawn(move || {
        // Drain until stream closes (face shutdown drops the
        // subscriber's Sender side).
        while let Ok(Some(_)) = watch.next_event() {
            // continue
        }
    });
    // Drop the cluster (auto-shutdown → face.shutdown drops
    // subscribers).
    drop(cluster);
    // Give the reader a moment to observe the channel close.
    reader.join().unwrap();
}
