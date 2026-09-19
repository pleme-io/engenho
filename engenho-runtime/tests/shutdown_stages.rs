//! I1 — `Runtime::shutdown` runs in stages and says after which one the
//! store was still shared.
//!
//! The stages, in order: every child awaited, the apiserver stopped, the
//! store's own background tasks quiesced, the store flushed, then
//! `Arc::try_unwrap` + `terminate`. (What the flush leaves on disk is pinned
//! by `tests/clean_stop_restart.rs`, on a durable node.) These tests pin,
//! through the public surface only:
//!
//!   * a store clone that outlives the apiserver's stop is reported as
//!     `StoreStillShared { strong_count: 2, after: ApiserverStopped }`: the
//!     stage that owed sole ownership, not the unwrap that noticed;
//!   * the store has been quiesced by the time ownership is attempted, so a
//!     failed stop still leaves the raft pump and the bookmark ticker
//!     stopped rather than running on behind the leaked clone;
//!   * (ignored, pending the apiserver) a client holding a WATCH open across
//!     the stop does not keep the store alive.

use std::sync::Arc;
use std::time::Duration;

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::{Runtime, RuntimeError, ShutdownStage};
use engenho_store::{Quiesced, TaskStop};
use shikumi::TieredConfig;

/// An ephemeral single node: in-memory store, fake backend, plaintext, and
/// ephemeral ports for all three listeners so tests run in parallel.
fn config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// Boot, keep a store clone past the stop, and return the stop's result
/// with the clone.
async fn stop_while_holding_the_store() -> (Result<(), RuntimeError>, Arc<engenho_store::StoreMesh>)
{
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Runtime::start(config(tmp.path()))
        .await
        .expect("runtime boots");
    let held = rt.store();
    (rt.shutdown().await, held)
}

/// Terminate the store the test kept, so the test leaks nothing.
async fn release(held: Arc<engenho_store::StoreMesh>) {
    Arc::try_unwrap(held)
        .map_err(|_| "the runtime kept a store reference after its stop returned")
        .expect("the test's clone is the last one")
        .terminate()
        .await
        .expect("terminate");
}

/// A clone that outlives the apiserver's stop is charged to that stage: it
/// is the first that owed sole ownership. Two holders: the runtime's and
/// the test's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_clone_held_across_the_stop_is_charged_to_the_apiserver_stop() {
    let (stopped, held) = stop_while_holding_the_store().await;
    match stopped {
        Err(RuntimeError::StoreStillShared {
            strong_count,
            after,
        }) => {
            assert_eq!(strong_count, 2, "the runtime's reference and the test's");
            assert_eq!(after, ShutdownStage::ApiserverStopped);
        }
        other => panic!("expected StoreStillShared, got {other:?}"),
    }
    release(held).await;
}

/// The stop quiesces the store before it tries to take ownership: when that
/// fails, the store's own tasks are already stopped, and a second quiesce
/// finds nothing left to stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_stop_has_already_quiesced_the_store() {
    let (stopped, held) = stop_while_holding_the_store().await;
    assert!(
        matches!(stopped, Err(RuntimeError::StoreStillShared { .. })),
        "the held clone makes the unwrap fail: {stopped:?}"
    );
    assert_eq!(
        held.quiesce().await,
        Quiesced {
            rpc_pump: TaskStop::AlreadyStopped,
            bookmark_ticker: TaskStop::AlreadyStopped,
        },
        "the stop left a store task running"
    );
    release(held).await;
}

/// ★ KNOWN LEAK, MEASURED 2026-09-19, and the fix is not in this crate.
///
/// A client holding a WATCH open across the stop keeps the apiserver's
/// router alive: axum 0.7's `serve` and axum-server 0.7 each run a
/// connection in its own detached `tokio::spawn`, `ApiServer::shutdown`
/// aborts only the serve task, and a watch body never ends by itself. The
/// connection task therefore holds every `StoreBackedHandler`'s
/// `Arc<StoreMesh>` for the life of the process. Measured on both the
/// plaintext and the TLS path: `StoreStillShared { strong_count: 57, after:
/// ApiserverStopped }` after the 2 s grace. In production that is every
/// stop with kubectl, k9s or an in-cluster informer watching.
///
/// Ignored until `ApiServer::shutdown` ends open watches and awaits its
/// connection tasks (engenho-apiserver/src/server.rs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "engenho-apiserver: ApiServer::shutdown leaves connection tasks (and open watches) running"]
async fn a_watch_held_open_across_the_stop_does_not_keep_the_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Runtime::start(config(tmp.path()))
        .await
        .expect("runtime boots");
    let addr = rt.local_addr();
    let token = std::fs::read_to_string(tmp.path().join("pki/admin.token"))
        .expect("runtime minted the admin bearer token");
    let mut watch = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/v1/namespaces/default/configmaps?watch=1"
        ))
        .bearer_auth(token.trim())
        .send()
        .await
        .expect("watch opens");
    assert_eq!(watch.status(), reqwest::StatusCode::OK);
    // Let the stream start; there may be nothing to read yet.
    let _ = tokio::time::timeout(Duration::from_millis(300), watch.chunk()).await;

    let stopped = rt.shutdown().await;
    drop(watch);
    assert!(
        stopped.is_ok(),
        "an open watch kept the store alive: {stopped:?}"
    );
}
