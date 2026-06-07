//! M0.3 — engenho gives pods REAL IPs on a shared podman network,
//! records them to `status.podIP`, and populates Service Endpoints.
//!
//! The networking peer of `m0_2_real_container.rs`. Where M0.2 proved
//! engenho materializes an ACTUAL `podman` container, M0.3 proves the
//! full pod-IP → Endpoints pipeline end to end against real netavark:
//!
//!   1. The `PodmanBackend` now attaches every container to a shared
//!      `engenho-net` user-defined network (`--network engenho-net`,
//!      created idempotently via `ensure_network`).
//!   2. `podman inspect` reads the **per-network** IP
//!      (`NetworkSettings.Networks.<net>.IPAddress`), not the empty
//!      legacy top-level field — so the kubelet writes a real
//!      `status.podIP`.
//!   3. The `EndpointsController` (already correct) populates a Service
//!      Endpoints object's `subsets[].addresses` from the ready, matching,
//!      ip-bearing pods.
//!   4. The two real containers share an L3 network → they reach each
//!      other by IP.
//!
//! ## Why ignore-gated
//!
//! Identical reasons to M0.2: shells to `podman`, needs the `busybox`
//! image pre-cached, needs `REGISTRY_AUTH_FILE` (broken machine
//! credential helper), and additionally needs the shared `engenho-net`
//! network (the backend creates it). None hold in CI, so every test here
//! is `#[ignore]`. Run with a real container runtime present via:
//!
//! ```text
//! cargo test -p engenho-runtime --test m0_3_pod_networking -- --ignored
//! ```
//!
//! ## Teardown safety
//!
//! A [`PodmanCleanup`] Drop guard force-removes every expected container
//! name on ANY exit (success, assert panic, timeout). The Deployment name
//! is unique-per-run so concurrent / leftover runs don't collide on the
//! deterministic `<namespace>_<pod>` container name. The shared
//! `engenho-net` network is intentionally LEFT in place — `ensure_network`
//! is idempotent, so reuse across runs is correct + cheaper than a
//! create-per-run.

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

/// Start a detached, self-restarting TCP listener on `port` inside
/// `container` via `podman exec -d`. busybox `nc -l -p <port>` accepts one
/// connection then exits, so we loop it to serve repeated probes. Used as
/// the reachability target — a `sleep 300` busybox has NO listener of its
/// own, and ICMP `ping` needs `CAP_NET_RAW` (denied under rootless podman),
/// so a real TCP listener + `nc -z` connect is the robust pod-to-pod L3
/// probe. Best-effort: the listener is torn down with the container.
fn podman_start_listener(container: &str, port: u16) {
    let loop_cmd = format!("while true; do nc -l -p {port} < /dev/null; done");
    let _ = Command::new("podman")
        .args(["exec", "-d", container, "sh", "-c", &loop_cmd])
        .output();
}

/// `podman exec <container> nc -z -w3 <ip> <port>` exit-0 test. Returns
/// `true` iff the exec succeeds AND `nc -z` (connect-then-close, no data)
/// reports the target's TCP port open — i.e. the two containers share a
/// reachable L3 network. Requires a listener running on `<ip>:<port>`
/// (see [`podman_start_listener`]).
fn podman_tcp_reachable(from_container: &str, to_ip: &str, port: u16) -> bool {
    let port_s = port.to_string();
    Command::new("podman")
        .args([
            "exec",
            from_container,
            "nc",
            "-z",
            "-w3",
            to_ip,
            &port_s,
        ])
        .output()
        .ok()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// =====================================================================
// HTTP helpers (mirror the M0.2 real-container test)
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
// Config + manifests
// =====================================================================

/// Durable config in `data_dir`, apiserver on an ephemeral loopback port,
/// kubelet driven by the **real PodmanBackend**, endpoints reconciler
/// ENABLED (so the podIP→Endpoints path runs), fast fallback + small
/// debounce. Mirrors `durable_podman_config` from the M0.2 test.
fn durable_podman_config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Podman;
    cfg.runtime.leadership_timeout_seconds = 5;
    // Every reconciler on — endpoints is the load-bearing one for M0.3.
    cfg.controllers.enable.deployment = true;
    cfg.controllers.enable.replicaset = true;
    cfg.controllers.enable.endpoints = true;
    cfg.controllers.enable.gc = true;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// A ClusterIP-less Service selecting `app=<dep>` on port 80→80. The
/// EndpointsController only needs the selector + ports to materialize an
/// Endpoints object from matching ready pods (no clusterIP allocation
/// exists yet — that's a later networking brick).
fn service_body(dep_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": dep_name },
        "spec": {
            "selector": { "app": dep_name },
            "ports": [{ "port": 80, "targetPort": 80 }]
        }
    })
}

/// Two-replica busybox Deployment running `sleep 300`. Image is
/// fully-qualified (`docker.io/library/busybox`) to skip registry search.
/// `replicas: 2` is required to prove the multi-address Endpoints subset +
/// pod-to-pod reachability. `dep_name` is unique-per-run.
fn busybox_deployment_body(dep_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": dep_name },
        "spec": {
            "replicas": 2,
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
/// `REGISTRY_AUTH_FILE`. The spawned podman command inherits this env var,
/// bypassing the broken machine credential helper.
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

/// Extract `status.podIP` (a non-empty dotted-quad) from a Pod value.
fn pod_ip(pod: &serde_json::Value) -> Option<String> {
    let ip = pod.get("status")?.get("podIP")?.as_str()?;
    // A dotted-quad has 3 dots + non-empty parts; cheap sanity filter so a
    // "" / null never passes the assert.
    if ip.split('.').count() == 4 && !ip.is_empty() {
        Some(ip.to_string())
    } else {
        None
    }
}

/// `true` iff the Pod's status reports phase=Running AND Ready=True.
fn pod_running_ready(pod: &serde_json::Value) -> bool {
    let Some(status) = pod.get("status") else {
        return false;
    };
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
    phase == Some("Running") && ready
}

/// Sorted IPs from an Endpoints object's `subsets[].addresses`.
fn endpoints_ips(ep: &serde_json::Value) -> Vec<String> {
    let mut out: Vec<String> = ep
        .get("subsets")
        .and_then(|s| s.as_array())
        .into_iter()
        .flatten()
        .flat_map(|s| {
            s.get("addresses")
                .and_then(|a| a.as_array())
                .into_iter()
                .flatten()
        })
        .filter_map(|a| a.get("ip").and_then(|i| i.as_str()).map(String::from))
        .collect();
    out.sort();
    out
}

// =====================================================================
// The test
// =====================================================================

// Multi-threaded runtime is REQUIRED: the test polls podman via blocking
// `std::process::Command` + `std::thread::sleep` (inherently synchronous).
// On the default single-threaded runtime those blocking calls would starve
// the spawned driver tasks (kubelet/endpoints/scheduler) so reconciliation
// would stall. A multi-thread runtime lets the drivers keep ticking while
// the test blocks on podman.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-container: needs podman + cached busybox + REGISTRY_AUTH_FILE + shared network"]
async fn two_replica_deployment_gets_real_ips_endpoints_and_pod_to_pod_reachability() {
    let tmp = tempfile::tempdir().unwrap();
    // Empty authfile + env export so the real PodmanBackend inherits it.
    let _authfile = set_empty_registry_auth(tmp.path());

    // Unique-per-run Deployment name → unique deterministic container names
    // so a prior failed / concurrent run can't collide.
    let dep_name = format!(
        "m0-3-bb-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Teardown guard — populated with the expected container names once we
    // resolve the RS-generated pod names; rm -f on ANY exit.
    let mut cleanup = PodmanCleanup::new();

    let rt = Runtime::start(durable_podman_config(tmp.path()))
        .await
        .expect("Runtime boots with Podman backend");
    let addr = rt.local_addr();
    let client = reqwest::Client::new();

    const TIMEOUT: Duration = Duration::from_secs(45);
    const INTERVAL: Duration = Duration::from_millis(250);

    // ── ARRANGE: POST the Service then the 2-replica Deployment ──────────
    let svc_resp = client
        .post(format!("http://{addr}/api/v1/namespaces/default/services"))
        .json(&service_body(&dep_name))
        .send()
        .await
        .expect("POST service");
    assert_eq!(
        svc_resp.status(),
        reqwest::StatusCode::CREATED,
        "service POST should 201"
    );

    let dep_resp = client
        .post(format!(
            "http://{addr}/apis/apps/v1/namespaces/default/deployments"
        ))
        .json(&busybox_deployment_body(&dep_name))
        .send()
        .await
        .expect("POST deployment");
    assert_eq!(
        dep_resp.status(),
        reqwest::StatusCode::CREATED,
        "deployment POST should 201"
    );
    let created: serde_json::Value = dep_resp.json().await.unwrap();
    let dep_uid = created
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .expect("deployment got a uid")
        .to_string();

    // ── Resolve the RS-generated pod names via HTTP ─────────────────────
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

    // 2. The TWO Pods owned by the RS.
    let pod_names = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(&client, addr, "/api/v1/namespaces/default/pods").await;
        let mut names: Vec<String> = items
            .iter()
            .filter(|p| is_owned_by(p, &rs_uid))
            .filter_map(|p| {
                p.get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .map(String::from)
            })
            .collect();
        names.sort();
        if names.len() == 2 { Some(names) } else { None }
    })
    .await
    .expect("two Pods created by the ReplicaSet");

    // Expected real container names = namespace-prefixed deterministic join.
    let container_names: Vec<String> = pod_names
        .iter()
        .map(|p| format!("default_{p}"))
        .collect();
    for cn in &container_names {
        cleanup.track(cn);
    }

    // ── ASSERT 1: each Pod's status.podIP is a real per-network IP ───────
    // Proves PodmanBackend extracts a real per-network IP (GAP A fix) and
    // the kubelet wrote it via item-5 CAS. Also require Running + Ready so
    // the EndpointsController will admit them.
    let pod_ips = poll_until(TIMEOUT, INTERVAL, || {
        let client = &client;
        let pod_names = &pod_names;
        async move {
            let mut ips: Vec<String> = Vec::new();
            for pn in pod_names {
                let pod = http_get(
                    client,
                    addr,
                    &format!("/api/v1/namespaces/default/pods/{pn}"),
                )
                .await?;
                if !pod_running_ready(&pod) {
                    return None;
                }
                ips.push(pod_ip(&pod)?);
            }
            ips.sort();
            // Two distinct real IPs.
            if ips.len() == 2 && ips[0] != ips[1] {
                Some(ips)
            } else {
                None
            }
        }
    })
    .await
    .expect(
        "both pods should report a non-empty dotted-quad status.podIP + Running + Ready \
         (PodmanBackend per-network IP extraction + shared engenho-net)",
    );

    // ── ASSERT 2: Endpoints carry exactly those two real IPs ─────────────
    // Proves the EndpointsController populated the Service Endpoints from
    // the real podman IPs (selector + readiness + podIP all real).
    let endpoints_ok = poll_until(TIMEOUT, INTERVAL, || {
        let client = &client;
        let dep_name = &dep_name;
        let pod_ips = &pod_ips;
        async move {
            let ep = http_get(
                client,
                addr,
                &format!("/api/v1/namespaces/default/endpoints/{dep_name}"),
            )
            .await?;
            let ips = endpoints_ips(&ep);
            if ips == *pod_ips { Some(()) } else { None }
        }
    })
    .await;
    assert!(
        endpoints_ok.is_some(),
        "Endpoints/{dep_name} should carry exactly the two pods' real podIPs {pod_ips:?}"
    );

    // ── ASSERT 3: the two real containers reach each other by IP ─────────
    // Prove pod-to-pod L3 connectivity on engenho-net: start a TCP listener
    // in each container, then `nc -z` connect-probe BOTH directions. A
    // successful connect (exit 0) means the L3 packet reached the peer +
    // the peer accepted — i.e. they share a reachable network. ICMP `ping`
    // is NOT used: rootless podman busybox lacks CAP_NET_RAW, so ping fails
    // with "permission denied" even when the network is fully reachable
    // (verified during M0.3 bring-up). A TCP listener + `nc -z` needs no
    // capability and is symmetric.
    const PROBE_PORT: u16 = 9999;
    let (ca, cb) = (&container_names[0], &container_names[1]);
    let (ip_a, ip_b) = (&pod_ips[0], &pod_ips[1]);
    podman_start_listener(ca, PROBE_PORT);
    podman_start_listener(cb, PROBE_PORT);
    // pod_ips is sorted independently of pod_names ordering; probing both
    // directions covers reachability regardless of which container owns
    // which IP.
    let a_to_b = poll_until_blocking(TIMEOUT, INTERVAL, || {
        podman_tcp_reachable(ca, ip_b, PROBE_PORT)
    });
    let b_to_a = poll_until_blocking(TIMEOUT, INTERVAL, || {
        podman_tcp_reachable(cb, ip_a, PROBE_PORT)
    });
    assert!(
        a_to_b,
        "container {ca} should reach {ip_b}:{PROBE_PORT} (pod-to-pod L3 on engenho-net)"
    );
    assert!(
        b_to_a,
        "container {cb} should reach {ip_a}:{PROBE_PORT} (pod-to-pod L3 on engenho-net)"
    );

    // ── TEARDOWN: DELETE the Deployment + both Pods over HTTP ────────────
    // DELETE the Deployment first (stops the RS recreating pods), then each
    // Pod directly so each leaves the kubelet's bound set in one hop (its
    // Pod-watch wakes + runs stop-then-remove on the real PodmanBackend).
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
    for pn in &pod_names {
        let del_pod = client
            .delete(format!("http://{addr}/api/v1/namespaces/default/pods/{pn}"))
            .send()
            .await
            .expect("DELETE pod");
        assert!(
            del_pod.status().is_success(),
            "pod {pn} DELETE should succeed, got {}",
            del_pod.status()
        );
    }

    // ── ASSERT: both real containers are gone (kubelet stop-then-remove) ─
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(45);
    let gone = poll_until_blocking(CLEANUP_TIMEOUT, INTERVAL, || {
        container_names.iter().all(|cn| !podman_container_exists(cn))
    });
    assert!(
        gone,
        "expected podman to show NO containers {container_names:?} after workload delete \
         (kubelet delete-cleanup should stop-then-remove on the real PodmanBackend)"
    );

    // ── CLEANUP: graceful shutdown must not hang ─────────────────────────
    rt.shutdown().await.expect("graceful shutdown");
    // The shared engenho-net network is intentionally left in place
    // (ensure_network is idempotent; reuse is correct). `cleanup` Drop
    // runs here (best-effort rm -f; containers already gone).
    drop(cleanup);
}
