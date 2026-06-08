//! M0.2 — engenho runs a REAL container via the PodmanBackend.
//!
//! The peer of `m0_1_single_node_convergence.rs`: same autonomous
//! WatchDriver wake-chain (POST a Deployment over real HTTP → RS → Pod →
//! scheduler binds → kubelet starts the container), but the kubelet is
//! driven by the **real `PodmanBackend`** instead of the FakeBackend, so
//! this proves engenho materializes an ACTUAL `podman` container on the
//! host — not just an in-memory record.
//!
//! ## Why ignore-gated
//!
//! This test shells to `podman`, needs the `busybox` image pre-cached,
//! and (because the host's container credential helper is broken) needs
//! `REGISTRY_AUTH_FILE` to point at a valid authfile. None of those hold
//! in CI, so every test here is `#[ignore]`. Run with a real container
//! runtime present via:
//!
//! ```text
//! cargo test -p engenho-runtime --test m0_2_real_container -- --ignored
//! ```
//!
//! ## Reliability knobs exercised
//!
//!   * **Pull policy `Never`** — `PodmanBackend::new()` (selected by
//!     `kubelet_backend = Podman`) defaults to `--pull never`; busybox is
//!     pre-cached so no registry contact happens.
//!   * **`REGISTRY_AUTH_FILE`** — the test writes an empty `{"auths":{}}`
//!     authfile to a tempdir, exports `REGISTRY_AUTH_FILE` pointing at it
//!     in the test process env, and the spawned podman command inherits it
//!     (tokio `Command` inherits the parent env). This bypasses the broken
//!     machine credential helper deterministically.
//!   * **Config-selection mechanism** — the test boots via `Runtime::start`
//!     with `kubelet_backend = Podman`, exercising
//!     `build_backend → make_container_runtime → PodmanBackend` (the real
//!     config-selection path, not the bypass `start_with_backend`).
//!
//! ## Teardown safety
//!
//! A [`PodmanCleanup`] Drop guard runs a best-effort `podman rm -f` for
//! every expected container name on ANY exit (success, assert panic,
//! timeout) so a failed run leaves no orphan container. The Deployment
//! name is unique-per-test so concurrent / leftover runs don't collide on
//! the deterministic `<namespace>_<pod>` container name.

use std::net::SocketAddr;
use std::process::Command;
use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::Runtime;
use shikumi::TieredConfig;

// =====================================================================
// Teardown guard — best-effort `podman rm -f` on Drop
// =====================================================================

/// Drop guard that force-removes a set of podman containers by name on
/// ANY scope exit (success, panic, timeout). Holds the deterministic
/// `<namespace>_<pod>` container names the kubelet would create. Removal
/// is best-effort: a not-found container is fine (already cleaned up by
/// the kubelet's delete-cleanup).
struct PodmanCleanup {
    names: Vec<String>,
}

impl PodmanCleanup {
    fn new() -> Self {
        Self { names: Vec::new() }
    }

    /// Register a container name to force-remove on drop. Idempotent.
    fn track(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.names.contains(&name) {
            self.names.push(name);
        }
    }
}

impl Drop for PodmanCleanup {
    fn drop(&mut self) {
        for name in &self.names {
            // Best-effort: ignore the result. A not-found container exits
            // non-zero but that's expected after a clean run.
            let _ = Command::new("podman").args(["rm", "-f", name]).output();
        }
    }
}

// =====================================================================
// podman query helpers (read-only; shell out via std::process in test)
// =====================================================================

/// `true` iff a container with exactly `name` exists (running or not).
fn podman_container_exists(name: &str) -> bool {
    let filter = format!("name=^{name}$");
    Command::new("podman")
        .args(["ps", "-a", "--filter", &filter, "--format", "{{.Names}}"])
        .output()
        .ok()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .any(|l| l.trim() == name)
        })
        .unwrap_or(false)
}

/// `true` iff a container named exactly `name` reports `State.Running`.
fn podman_container_running(name: &str) -> bool {
    Command::new("podman")
        .args(["inspect", "--format", "{{.State.Running}}", name])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}

// =====================================================================
// HTTP helpers (mirror the convergence test)
// =====================================================================

async fn http_list(
    client: &reqwest::Client,
    addr: SocketAddr,
    path: &str,
) -> Vec<serde_json::Value> {
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .expect("LIST request");
    if resp.status() != reqwest::StatusCode::OK {
        return Vec::new();
    }
    let list: serde_json::Value = resp.json().await.expect("LIST json");
    list.get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default()
}

async fn http_get(
    client: &reqwest::Client,
    addr: SocketAddr,
    path: &str,
) -> Option<serde_json::Value> {
    let resp = client.get(format!("http://{addr}{path}")).send().await.ok()?;
    if resp.status() != reqwest::StatusCode::OK {
        return None;
    }
    resp.json().await.ok()
}

/// True if `child`'s controlling owner-ref UID equals `owner_uid`.
fn is_owned_by(child: &serde_json::Value, owner_uid: &str) -> bool {
    child
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|o| o.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|r| {
                r.get("uid").and_then(|u| u.as_str()) == Some(owner_uid)
                    && r.get("controller").and_then(serde_json::Value::as_bool) == Some(true)
            })
        })
}

/// Bounded poll: call `predicate` every `interval` until it returns
/// `Some(T)` or `timeout` elapses.
async fn poll_until<F, Fut, T>(timeout: Duration, interval: Duration, mut predicate: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = predicate().await {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Synchronous bounded poll for the podman-query helpers (no async).
fn poll_until_blocking<F: FnMut() -> bool>(
    timeout: Duration,
    interval: Duration,
    mut predicate: F,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(interval);
    }
}

// =====================================================================
// Config + manifest
// =====================================================================

/// Durable config in `data_dir`, apiserver on an ephemeral loopback port,
/// kubelet driven by the **real PodmanBackend**, fast fallback + small
/// debounce. Mirrors `durable_config` from the convergence test but
/// selects `kubelet_backend = Podman` so the config-selection path
/// (`build_backend → make_container_runtime → PodmanBackend`) is exercised.
fn durable_podman_config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Podman;
    cfg.runtime.leadership_timeout_seconds = 5;
    // Plaintext: this suite asserts container convergence over http://;
    // TLS has its own integration test (m0_4_tls_kubectl.rs).
    cfg.runtime.tls.enabled = false;
    // Every reconciler on.
    cfg.controllers.enable.deployment = true;
    cfg.controllers.enable.replicaset = true;
    cfg.controllers.enable.endpoints = true;
    cfg.controllers.enable.gc = true;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// Single-replica busybox Deployment running `sleep 300`. Image is
/// fully-qualified (`docker.io/library/busybox`) to skip registry search;
/// `replicas: 1` bounds real-container resource use + teardown to a single
/// `rm`. `dep_name` is unique-per-test so leftover runs don't collide.
fn busybox_deployment_body(dep_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": dep_name },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": dep_name } },
            "template": {
                "metadata": { "labels": { "app": dep_name } },
                "spec": { "containers": [{
                    "name": "main",
                    "image": "docker.io/library/busybox",
                    "command": ["sleep", "300"]
                }] }
            }
        }
    })
}

/// Write an empty `{"auths":{}}` authfile into `dir` + export
/// `REGISTRY_AUTH_FILE` pointing at it for the test process. The spawned
/// podman command inherits this env var, bypassing the broken machine
/// credential helper. Returns the authfile path (kept alive by the
/// caller's tempdir).
fn set_empty_registry_auth(dir: &std::path::Path) -> std::path::PathBuf {
    let authfile = dir.join("auth.json");
    std::fs::write(&authfile, r#"{"auths":{}}"#).expect("write empty authfile");
    // SAFETY: set in the test process before any podman spawn; single
    // sequential test, no concurrent env mutation.
    unsafe {
        std::env::set_var("REGISTRY_AUTH_FILE", &authfile);
    }
    authfile
}

// =====================================================================
// The test
// =====================================================================

// Multi-threaded runtime is REQUIRED: the test polls podman via blocking
// `std::process::Command` + `std::thread::sleep` (the podman query helpers
// are inherently synchronous). On the default single-threaded
// `current_thread` runtime those blocking calls would starve the spawned
// driver tasks (kubelet/GC/scheduler) so reconciliation would stall. A
// multi-thread runtime lets the drivers keep ticking while the test blocks
// on podman.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-container: needs podman + cached busybox + REGISTRY_AUTH_FILE"]
async fn deployment_runs_a_real_podman_container_then_cleans_up() {
    let tmp = tempfile::tempdir().unwrap();
    // Empty authfile + env export so the real PodmanBackend inherits it.
    let _authfile = set_empty_registry_auth(tmp.path());

    // Unique-per-run Deployment name → unique deterministic container name
    // so a prior failed run / concurrent run can't collide.
    let dep_name = format!(
        "m0-2-bb-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Teardown guard — populated with the expected container name once we
    // resolve the RS-generated pod name; rm -f on ANY exit.
    let mut cleanup = PodmanCleanup::new();

    let rt = Runtime::start(durable_podman_config(tmp.path()))
        .await
        .expect("Runtime boots with Podman backend");
    let addr = rt.local_addr();
    let client = reqwest::Client::new();

    // ── ACT: POST the busybox Deployment over real HTTP ─────────────────
    let resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&busybox_deployment_body(&dep_name))
        .send()
        .await
        .expect("POST deployment");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "deployment POST should 201"
    );
    let created: serde_json::Value = resp.json().await.unwrap();
    let dep_uid = created
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("deployment got a uid")
        .to_string();

    const TIMEOUT: Duration = Duration::from_secs(30);
    const INTERVAL: Duration = Duration::from_millis(250);

    // ── Resolve the RS-generated pod name via HTTP (mirror convergence) ─
    // 1. ReplicaSet owned by the Deployment.
    let rs = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(&client, addr, "/apis/apps/v1/namespaces/default/replicasets").await;
        items.into_iter().find(|r| is_owned_by(r, &dep_uid))
    })
    .await
    .expect("ReplicaSet created for Deployment");
    let rs_uid = rs
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("RS uid")
        .to_string();

    // 2. The single Pod owned by the RS.
    let pod = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(&client, addr, "/api/v1/namespaces/default/pods").await;
        items.into_iter().find(|p| is_owned_by(p, &rs_uid))
    })
    .await
    .expect("Pod created by ReplicaSet");
    let pod_name = pod
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .expect("pod name")
        .to_string();

    // Expected real container name = `<ns>_<pod>_<containerName>` (the
    // multi-container deterministic join; busybox's single container is `main`).
    let container_name = format!("default_{pod_name}_main");
    cleanup.track(&container_name);

    // ── ASSERT: a REAL podman container is running ──────────────────────
    let running = poll_until_blocking(TIMEOUT, INTERVAL, || {
        podman_container_exists(&container_name) && podman_container_running(&container_name)
    });
    assert!(
        running,
        "expected a real podman container named {container_name} reporting Running; \
         podman never showed it (is busybox cached + REGISTRY_AUTH_FILE set?)"
    );

    // ── ASSERT: the Pod's HTTP status reconciled to Running + Ready=True ─
    // Cross-checks the real container against the status path so the proof
    // covers BOTH the container AND the kubelet status reconcile.
    let status_ok = poll_until(TIMEOUT, INTERVAL, || async {
        let pod = http_get(
            &client,
            addr,
            &format!("/api/v1/namespaces/default/pods/{pod_name}"),
        )
        .await?;
        let status = pod.get("status")?;
        let phase = status.get("phase").and_then(|p| p.as_str());
        let ready = status
            .get("conditions")
            .and_then(|c| c.as_array())
            .is_some_and(|conds| {
                conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            });
        if phase == Some("Running") && ready {
            Some(())
        } else {
            None
        }
    })
    .await;
    assert!(
        status_ok.is_some(),
        "pod {pod_name} should report status.phase=Running + Ready=True"
    );

    // ── DELETE the workload over HTTP → kubelet stop-then-remove ────────
    //
    // The intent-removal: DELETE the Deployment. Then DELETE the Pod
    // directly so it leaves the kubelet's bound set deterministically.
    //
    // Deleting only the Deployment relies on the GC controller cascading
    // Deployment→RS→Pod orphan-deletion across multiple fallback-gated
    // ticks before the kubelet sees the Pod vanish — a slow, multi-hop
    // path whose timing is governed by the (pre-M0.2) GC + watch-driver
    // cadence, NOT by the backend under test. Removing the Pod directly
    // pulls it out of the bound set in one hop: the kubelet's Pod-watch
    // (filter `Kinds(["Pod"])`) wakes on the delete event and its
    // orphan-cleanup runs `backend.stop` THEN `backend.remove` on the
    // real PodmanBackend — exactly the path M0.2 must prove. The
    // Deployment delete first stops the RS controller from recreating the
    // Pod under us, so the cleanup converges to zero containers.
    let del_dep = client
        .delete(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments/{dep_name}"
        ))
        .send()
        .await
        .expect("DELETE deployment");
    assert!(
        del_dep.status().is_success(),
        "deployment DELETE should succeed, got {}",
        del_dep.status()
    );
    let del_pod = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/pods/{pod_name}"
        ))
        .send()
        .await
        .expect("DELETE pod");
    assert!(
        del_pod.status().is_success(),
        "pod DELETE should succeed, got {}",
        del_pod.status()
    );

    // ── ASSERT: the real container is gone (kubelet stop-then-remove ran) ─
    // The kubelet wakes on the Pod-delete watch event + cleans up; allow a
    // generous timeout to absorb the watch-driver wake + two podman calls.
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
    let gone = poll_until_blocking(CLEANUP_TIMEOUT, INTERVAL, || {
        !podman_container_exists(&container_name)
    });
    assert!(
        gone,
        "expected podman to show NO container named {container_name} after workload delete \
         (kubelet delete-cleanup should stop-then-remove on the real PodmanBackend)"
    );

    // ── CLEANUP: graceful shutdown must not hang ────────────────────────
    rt.shutdown().await.expect("graceful shutdown");
    // `cleanup` Drop runs here (best-effort rm -f; container already gone).
    drop(cleanup);
}
