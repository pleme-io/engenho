//! A runtime can be stopped and booted again inside one process, on the same
//! durable store — the property the control plane's supervisor is built on
//! (restart, retry after a failed boot, re-init).
//!
//! Pinned here, through the public surface only:
//!
//!   * stop → boot again on the same durable `data_dir`: the second boot
//!     resumes the store (what the first wrote is there, the revision only
//!     moves forward), and each stop returns [`StoreReleased`];
//!   * a boot that fails AFTER opening the store (the apiserver cannot bind)
//!     unwinds it — `BootUnwind::Released` — so the retry in the same process
//!     opens the store instead of being refused by its lock;
//!   * twenty stop/boot cycles leave neither tasks nor file descriptors
//!     behind.

use std::time::Duration;

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::{BootUnwind, Runtime, StoreReleased};
use serde_json::json;
use shikumi::TieredConfig;

/// A durable single node: on-disk store, fake backend, plaintext, ephemeral
/// ports for all three listeners so tests run in parallel.
fn durable(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 10;
    cfg.runtime.tls.enabled = false;
    cfg.runtime.kubeconfig_publish_path = String::new();
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

fn admin_token(data_dir: &std::path::Path) -> String {
    std::fs::read_to_string(data_dir.join("pki/admin.token"))
        .expect("runtime minted the admin bearer token")
        .trim()
        .to_string()
}

/// Create a ConfigMap through the apiserver, as any client would.
async fn create_configmap(rt: &Runtime, data_dir: &std::path::Path, name: &str) {
    let resp = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/namespaces/default/configmaps",
            rt.local_addr()
        ))
        .bearer_auth(admin_token(data_dir))
        .json(&json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name },
            "data": { "boot": "first" },
        }))
        .send()
        .await
        .expect("create request");
    assert!(
        resp.status().is_success(),
        "create {name}: {}",
        resp.status()
    );
}

/// Read a ConfigMap's `data.boot` through the apiserver.
async fn read_configmap(rt: &Runtime, data_dir: &std::path::Path, name: &str) -> Option<String> {
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{}/api/v1/namespaces/default/configmaps/{name}",
            rt.local_addr()
        ))
        .bearer_auth(admin_token(data_dir))
        .send()
        .await
        .expect("get request");
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }
    let body: serde_json::Value = resp.json().await.expect("json body");
    body["data"]["boot"].as_str().map(str::to_string)
}

async fn stop(rt: Runtime) -> StoreReleased {
    rt.shutdown().await.expect("clean stop releases the store")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_runtime_boots_again_in_process_on_the_same_durable_store() {
    let tmp = tempfile::tempdir().expect("tempdir");

    let first = Runtime::boot(durable(tmp.path()))
        .await
        .expect("first boot");
    create_configmap(&first, tmp.path(), "survives-restart").await;
    let before = first.store().current_revision().await;
    let _released: StoreReleased = stop(first).await;

    let second = Runtime::boot(durable(tmp.path()))
        .await
        .expect("second boot opens the released store");
    assert_eq!(
        read_configmap(&second, tmp.path(), "survives-restart").await,
        Some("first".to_string()),
        "the second boot resumed the first boot's store"
    );
    let after = second.store().current_revision().await;
    assert!(
        after >= before,
        "the revision moved backwards across the restart: {before:?} -> {after:?}"
    );
    stop(second).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_that_fails_after_opening_the_store_releases_it_for_the_retry() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Occupy a port, then point the apiserver at it: the boot opens the
    // durable store, runs every step up to the bind, and fails there.
    let squatter = std::net::TcpListener::bind("127.0.0.1:0").expect("squat a port");
    let taken = squatter.local_addr().expect("squatted addr");
    let mut cfg = durable(tmp.path());
    cfg.runtime.listen_addr = taken.to_string();

    let failed = match Runtime::boot(cfg).await {
        Ok(rt) => {
            let _ = rt.shutdown().await;
            panic!("the boot bound a port that was already taken");
        }
        Err(failed) => failed,
    };
    assert_eq!(
        engenho_substrate::ErrorKind::kind(&failed.error),
        "server",
        "the boot failed at the apiserver bind: {}",
        failed.error
    );
    assert!(
        matches!(failed.unwind, BootUnwind::Released(_)),
        "a boot that failed after opening the store must release it: {:?}",
        failed.unwind
    );

    // The retry, in the same process, on the same store.
    drop(squatter);
    let retried = Runtime::boot(durable(tmp.path()))
        .await
        .expect("the retry opens the store the failed boot released");
    stop(retried).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_that_fails_before_opening_the_store_says_it_opened_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = durable(tmp.path());
    // Operator-supplied PKI is read and refused (`Unhonoured`) before
    // anything is opened (tests/i21_config_read.rs).
    cfg.runtime.tls.enabled = true;
    cfg.runtime.tls.ca_cert_path = Some("/etc/pki/ca.crt".into());
    let failed = match Runtime::boot(cfg).await {
        Ok(rt) => {
            let _ = rt.shutdown().await;
            panic!("an unhonoured config booted");
        }
        Err(failed) => failed,
    };
    assert!(
        matches!(failed.unwind, BootUnwind::NeverOpened),
        "{:?}",
        failed.unwind
    );
    StoreReleased::probe(tmp.path(), true).expect("nothing holds the store");
}

/// How many descriptors this process has open.
fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("/dev/fd is readable")
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_stop_boot_cycles_leave_no_tasks_or_descriptors_behind() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let metrics = tokio::runtime::Handle::current().metrics();

    // Two cycles to reach steady state (one-time initialisations: the panic
    // hook, the rustls provider, lazily-built statics).
    for _ in 0..2 {
        let rt = Runtime::boot(durable(tmp.path())).await.expect("boot");
        stop(rt).await;
    }
    // Detached work from the last stop (a hyper connection finishing, an
    // openraft worker observing its shutdown) settles within moments.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (fds0, tasks0) = (open_fds(), metrics.num_alive_tasks());

    for _ in 0..20 {
        let rt = Runtime::boot(durable(tmp.path())).await.expect("boot");
        stop(rt).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (fds, tasks) = (open_fds(), metrics.num_alive_tasks());

    // A leak of one per cycle would show as +20; allow a little jitter.
    assert!(
        fds <= fds0 + 4,
        "descriptors grew across 20 cycles: {fds0} -> {fds}"
    );
    assert!(
        tasks <= tasks0 + 4,
        "tasks grew across 20 cycles: {tasks0} -> {tasks}"
    );
}
