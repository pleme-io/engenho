//! I2 / T2.9 — the restart oracle's case 4 at the `Runtime::shutdown`
//! level: **a clean stop leaves nothing to replay.**
//!
//! `engenho-store/tests/restart_is_a_kill.rs` pins case 4 for a bare
//! `StoreMesh` (`terminate` flushes last). This file pins it for the stop the
//! daemon actually runs on SIGTERM and SIGINT: children awaited, apiserver
//! stopped, store quiesced, store FLUSHED, then `try_unwrap` + `terminate`.
//!
//! | case | how the stop ends | what must hold on the next boot |
//! |---|---|---|
//! | clean | `Runtime::shutdown` returns `Ok` | nothing to replay; every ack at its revision; the revision continues |
//! | leaked | a store clone outlives the stop, so `terminate` never runs | the same: the stop flushed before it tried to take the store |
//!
//! A node here is durable (fjall under `data_dir/store`), with the fake
//! kubelet backend and every listener on an ephemeral loopback port.
//!
//! Each node lifetime runs on its own tokio runtime ([`lifetime`]), and the
//! end of the lifetime drops that runtime: every task it spawned — raft, the
//! state-machine worker, anything a leaked clone kept alive — is dropped with
//! it, and nothing else gets to flush. So the durable image read afterwards
//! is exactly what the stop left on disk.
//!
//! Before its writes each case calls `flush` once, so the image is current
//! and then falls behind by exactly those writes (`apply` batches the image
//! write by count and time). Without a flush on the stop path those writes
//! would be left in the log for the next boot to replay.
//!
//! Residual, stated plainly: an entry a driver had in flight when it was
//! aborted can still be applied after the stop's flush. An idle node with
//! the fake backend proposes nothing after boot (measured: 25 s, zero
//! applies), and the config maps written here trigger no controller, so
//! neither case has such an entry. Closing it for a busy node needs the store
//! to drain applies before flushing.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::{Runtime, RuntimeError};
use engenho_store::{
    FjallStore, InProcessRouter, MeshFlushed, Reason, ResourceCommand, ResourceKey, StoreMesh,
    default_config,
};
use openraft::LogId;
use openraft::storage::{RaftLogStorage as _, RaftStateMachine as _};
use serde_json::{Value, json};
use shikumi::TieredConfig;

const CLUSTER: &str = "engenho-clean-stop";
const LEADERSHIP: Duration = Duration::from_secs(10);

/// A durable single node: fjall store, fake backend, plaintext, ephemeral
/// ports for all three listeners so tests run in parallel.
fn config(data_dir: &Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.name = CLUSTER.into();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg
}

/// One node lifetime on its own runtime. Dropping the runtime at the end
/// drops every task it spawned, so for anything the stop did not finish, the
/// end of a lifetime is a kill.
fn lifetime<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build the runtime for one node lifetime");
    let out = rt.block_on(f);
    rt.shutdown_timeout(Duration::from_secs(5));
    out
}

/// Where the runtime keeps its durable store under `data_dir`.
fn store_dir(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("store")
}

fn config_map(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
}

/// A write the store acknowledged, with the revision it was committed at.
#[derive(Clone, Debug)]
struct Ack {
    name: String,
    revision: u64,
}

/// Bring the image current, then write `n` config maps through the store so
/// it is behind by exactly those writes.
async fn write_behind_the_image(store: &StoreMesh, prefix: &str, n: usize) -> Vec<Ack> {
    assert!(
        matches!(
            store.flush().await.expect("the harness flush"),
            MeshFlushed::Durable(_)
        ),
        "HARNESS PRECONDITION: the node must be durable, or there is no image to check"
    );
    let mut acks = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("{prefix}-{i}");
        let body = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": "default"},
            "data": {"i": i.to_string()},
        });
        let res = store
            .propose(ResourceCommand::put(
                config_map(&name),
                body,
                Reason::Operator,
            ))
            .await
            .expect("write a config map");
        acks.push(Ack {
            name,
            revision: res.revision,
        });
    }
    acks
}

/// What the next boot starts from, read by opening the directory raw —
/// before any raft replay runs over it.
#[derive(Debug)]
struct DurableImage {
    /// The revision the persisted catalog hydrates to.
    revision: u64,
    /// The persisted applied position the next boot replays from.
    last_applied: Option<LogId<u64>>,
    /// The last entry in the durable log.
    last_log: Option<LogId<u64>>,
}

fn durable_image(dir: &Path) -> DurableImage {
    lifetime(async {
        let mut store = FjallStore::open(dir).expect("reopen the store directory raw");
        let revision = store.current_revision().await.get();
        let (last_applied, _) = store.applied_state().await.expect("applied state");
        let last_log = store.get_log_state().await.expect("log state").last_log_id;
        DurableImage {
            revision,
            last_applied,
            last_log,
        }
    })
}

/// The restart half of case 4, shared by both stops: nothing left in the log
/// to replay, every acknowledged write in the durable catalog at the revision
/// it was acknowledged at, and the revision continuing where it stopped.
fn assert_the_next_boot_replays_nothing(data_dir: &Path, acks: &[Ack]) {
    let head = acks.last().expect("at least one ack").revision;
    let dir = store_dir(data_dir);

    let image = durable_image(&dir);
    assert_eq!(
        image.last_applied, image.last_log,
        "the stop left log entries for the next boot to replay: {image:?}"
    );
    assert!(
        image.revision >= head,
        "the durable catalog (revision {}) is behind the last acknowledged write ({head})",
        image.revision
    );

    lifetime(async {
        let mesh = StoreMesh::start_durable(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config(CLUSTER).expect("raft config"),
            &dir,
        )
        .await
        .expect("reopen the store after the stop");
        assert!(mesh.wait_for_leadership(LEADERSHIP).await);
        for ack in acks {
            let rv = mesh
                .get(&config_map(&ack.name))
                .await
                .and_then(|obj| {
                    obj.pointer("/metadata/resourceVersion")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| panic!("acknowledged write {} is missing", ack.name));
            assert_eq!(
                rv,
                ack.revision.to_string(),
                "acknowledged write {} came back renumbered",
                ack.name
            );
        }
        assert_eq!(
            mesh.current_revision().await.get(),
            image.revision,
            "booting replayed writes onto the durable catalog"
        );
        let next = mesh
            .propose(ResourceCommand::put(
                config_map("after-restart"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "after-restart", "namespace": "default"},
                }),
                Reason::Operator,
            ))
            .await
            .expect("write after the restart");
        assert_eq!(
            next.revision,
            image.revision + 1,
            "the revision is not continuous across the restart"
        );
        mesh.terminate()
            .await
            .expect("terminate the reopened store");
    });
}

/// **A clean `Runtime::shutdown` leaves nothing to replay** — T3.1 case 4,
/// through the stop the daemon runs on SIGTERM and SIGINT.
#[test]
fn a_clean_stop_leaves_nothing_to_replay() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let acks = lifetime(async {
        let rt = Runtime::start(config(tmp.path()))
            .await
            .expect("runtime boots");
        let store = rt.store();
        let acks = write_behind_the_image(&store, "clean", 3).await;
        drop(store);
        rt.shutdown().await.expect("the clean stop");
        acks
    });
    assert_the_next_boot_replays_nothing(tmp.path(), &acks);
}

/// **A stop that cannot take the store has already flushed it.** A clone
/// held across the stop makes the unwrap fail, so `terminate` — which also
/// flushes — never runs; the process then ends with the store still shared,
/// as it does in production when a client holds a watch open across the
/// stop. The stop flushed before it tried, so the next boot still replays
/// nothing.
#[test]
fn a_stop_that_cannot_take_the_store_has_already_flushed_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let acks = lifetime(async {
        let rt = Runtime::start(config(tmp.path()))
            .await
            .expect("runtime boots");
        let held: Arc<StoreMesh> = rt.store();
        let acks = write_behind_the_image(&held, "leaked", 3).await;
        let stopped = rt.shutdown().await;
        assert!(
            matches!(stopped, Err(RuntimeError::StoreStillShared { .. })),
            "HARNESS PRECONDITION: the held clone must make the unwrap fail, so that \
             terminate never runs: {stopped:?}"
        );
        // Dropped with the lifetime: never terminated, never flushed again.
        drop(held);
        acks
    });
    assert_the_next_boot_replays_nothing(tmp.path(), &acks);
}
