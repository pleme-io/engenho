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
        .args(["exec", from_container, "nc", "-z", "-w3", to_ip, &port_s])
        .output()
        .ok()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Resolve `name` from a SEPARATE short-lived client container on `network`
/// via busybox `nslookup <name>`, returning the set of resolved A-record IPs.
///
/// busybox does NOT ship `getent`, so we use its built-in `nslookup`. Two
/// quirks handled: (1) busybox appends the host search domains (e.g.
/// `*.ts.net`) which NXDOMAIN and can set a NON-ZERO exit even though the bare
/// name resolves — so we parse stdout regardless of exit status; (2) the
/// resolver line is `Address: <ip>:53` (carries a port) and is excluded; real
/// answers are `Address: <ip>` (no port). The client is a throwaway `podman
/// run --rm --network <net> busybox nslookup <name>` — NOT one of the workload
/// pods, so resolution is proven network-wide (aardvark-dns on a user
/// network), not just self-resolution. Empty vec on exec failure / no answer.
fn resolve_via_client(network: &str, name: &str) -> Vec<String> {
    let out = Command::new("podman")
        .args([
            "run",
            "--rm",
            "--network",
            network,
            "docker.io/library/busybox",
            "nslookup",
            name,
        ])
        .output();
    let Ok(out) = out else {
        return Vec::new();
    };
    // Parse stdout even on non-zero exit (search-domain NXDOMAIN noise).
    let text = String::from_utf8_lossy(&out.stdout);
    let is_ipv4 =
        |s: &str| s.split('.').count() == 4 && s.split('.').all(|o| o.parse::<u8>().is_ok());
    let mut ips: Vec<String> = text
        .lines()
        .filter(|line| line.contains("Address"))
        // last whitespace token of an `Address[: N]: <ip>` line
        .filter_map(|line| line.split_whitespace().last().map(str::to_string))
        .filter(|tok| !tok.contains(':')) // drop the resolver `<ip>:53`
        .filter(|tok| is_ipv4(tok))
        .collect();
    ips.sort();
    ips.dedup();
    ips
}

/// Connect-by-name probe from a throwaway client container: `podman run
/// --rm --network <net> busybox nc -z -w3 <name> <port>` exit-0 test.
/// Proves name→IP→reachable backend end-to-end (the client resolves the
/// Service name via aardvark-dns THEN connects). Requires a listener on a
/// backend pod bound to `<name>` (see [`podman_start_listener`]).
fn client_connect_by_name(network: &str, name: &str, port: u16) -> bool {
    let port_s = port.to_string();
    Command::new("podman")
        .args([
            "run",
            "--rm",
            "--network",
            network,
            "docker.io/library/busybox",
            "nc",
            "-z",
            "-w3",
            name,
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
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .ok()?;
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
    // Plaintext: this suite asserts networking convergence over http://;
    // TLS has its own integration test (m0_4_tls_kubectl.rs).
    cfg.runtime.tls.enabled = false;
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
        let items = http_list(
            &client,
            addr,
            "/apis/apps/v1/namespaces/default/replicasets",
        )
        .await;
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
        .map(|p| format!("default_{p}_main"))
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
        container_names
            .iter()
            .all(|cn| !podman_container_exists(cn))
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

// =====================================================================
// M0.3 cluster-DNS — Service-name resolution via aardvark-dns
// =====================================================================

/// The shared engenho-net network the PodmanBackend attaches pods to (its
/// default). aardvark-dns runs for this user-defined network, resolving
/// `--network-alias` names the kubelet feeds at pod-start.
const ENGENHO_NET: &str = "engenho-net";

/// A ClusterIP-less Service named `svc_name` selecting `app=<app_label>` on
/// port 80→80. The kubelet derives `--network-alias` forms from the Service
/// NAME (`<svc_name>`, `<svc_name>.default`, `<svc_name>.default.svc.cluster.local`)
/// for every pod whose labels satisfy the selector.
fn named_service_body(svc_name: &str, app_label: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": svc_name },
        "spec": {
            "selector": { "app": app_label },
            "ports": [{ "port": 80, "targetPort": 80 }]
        }
    })
}

/// A 2-replica busybox Deployment whose pod template carries `app=<app_label>`
/// so the pods match the Service selector + earn its DNS aliases. `dep_name`
/// is the (unique-per-run) Deployment object name; `app_label` is the
/// selector value the Service keys on.
fn labeled_deployment_body(dep_name: &str, app_label: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": dep_name },
        "spec": {
            "replicas": 2,
            "selector": { "matchLabels": { "app": app_label } },
            "template": {
                "metadata": { "labels": { "app": app_label } },
                "spec": { "containers": [{
                    "name": "main",
                    "image": "docker.io/library/busybox",
                    "command": ["sleep", "300"]
                }] }
            }
        }
    })
}

/// M0.3 cluster-DNS: a Service name resolves to its backend pod IPs via
/// aardvark-dns, and a SEPARATE client container connects to a backend BY
/// NAME — proving the `--network-alias` feed the kubelet computes at
/// pod-start is load-bearing headless-Service DNS (no ClusterIP / kube-proxy).
///
/// Multi-thread runtime is REQUIRED for the same reason as the sibling test:
/// the probe path shells out to `podman` via blocking `std::process` +
/// `std::thread::sleep`, which would starve the single-threaded driver tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-container: needs podman + aardvark-dns on engenho-net + cached busybox + REGISTRY_AUTH_FILE"]
async fn service_name_resolves_via_aardvark_dns_and_client_connects_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    let _authfile = set_empty_registry_auth(tmp.path());

    // Unique-per-run names so concurrent / leftover runs don't collide on
    // the deterministic <ns>_<pod> container names OR on the Service-name
    // alias aardvark-dns answers.
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let svc_name = format!("web-{run_id}");
    let dep_name = format!("m0-3-dns-{run_id}");
    // The pod label the Service selects on; doubles as the alias source via
    // the Service name (NOT this label) — kept distinct to prove the alias
    // comes from the Service NAME, not the selector value.
    let app_label = format!("dnsapp-{run_id}");

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
        .json(&named_service_body(&svc_name, &app_label))
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
        .json(&labeled_deployment_body(&dep_name, &app_label))
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
    let rs = poll_until(TIMEOUT, INTERVAL, || async {
        let items = http_list(
            &client,
            addr,
            "/apis/apps/v1/namespaces/default/replicasets",
        )
        .await;
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

    let container_names: Vec<String> = pod_names
        .iter()
        .map(|p| format!("default_{p}_main"))
        .collect();
    for cn in &container_names {
        cleanup.track(cn);
    }

    // ── WAIT: both pods Running+Ready with real IPs + Endpoints carry them ─
    // Proves the M0.3-foundation pipeline still holds (the backends came up
    // with real per-network IPs the EndpointsController materialized).
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
            if ips.len() == 2 && ips[0] != ips[1] {
                Some(ips)
            } else {
                None
            }
        }
    })
    .await
    .expect("both pods Running+Ready with two distinct real podIPs");

    let endpoints_ok = poll_until(TIMEOUT, INTERVAL, || {
        let client = &client;
        let svc_name = &svc_name;
        let pod_ips = &pod_ips;
        async move {
            let ep = http_get(
                client,
                addr,
                &format!("/api/v1/namespaces/default/endpoints/{svc_name}"),
            )
            .await?;
            let ips = endpoints_ips(&ep);
            if ips == *pod_ips { Some(()) } else { None }
        }
    })
    .await;
    assert!(
        endpoints_ok.is_some(),
        "Endpoints/{svc_name} should carry exactly the two pods' real podIPs {pod_ips:?}"
    );

    // ── ACT + ASSERT: resolve the Service name via a SEPARATE client ─────
    // A throwaway `podman run --rm --network engenho-net busybox getent
    // hosts <svc>` resolves the Service NAME through aardvark-dns (which
    // answers the kubelet-fed --network-alias). The resolved IP(s) must be a
    // subset of the two real pod IPs from the Endpoints set — proving DNS
    // returns the real backend pod IPs (headless multi-A), not a ClusterIP.
    let resolved = poll_until(TIMEOUT, INTERVAL, || {
        let svc_name = svc_name.clone();
        let pod_ips = pod_ips.clone();
        async move {
            let ips = resolve_via_client(ENGENHO_NET, &svc_name);
            // Require at least one resolved IP, and every resolved IP must
            // be one of the real backend pod IPs.
            if !ips.is_empty() && ips.iter().all(|ip| pod_ips.contains(ip)) {
                Some(ips)
            } else {
                None
            }
        }
    })
    .await
    .expect(
        "client container should resolve the Service name to real backend pod IPs via aardvark-dns",
    );
    assert!(
        resolved.iter().all(|ip| pod_ips.contains(ip)),
        "every resolved IP {resolved:?} must be a real backend podIP {pod_ips:?}"
    );

    // FQDN form must resolve to the SAME set (proves all three alias forms
    // — bare / .<ns> / .<ns>.svc.<domain> — landed in aardvark-dns).
    let fqdn = format!("{svc_name}.default.svc.cluster.local");
    let resolved_fqdn = poll_until(TIMEOUT, INTERVAL, || {
        let fqdn = fqdn.clone();
        let pod_ips = pod_ips.clone();
        async move {
            let ips = resolve_via_client(ENGENHO_NET, &fqdn);
            if !ips.is_empty() && ips.iter().all(|ip| pod_ips.contains(ip)) {
                Some(ips)
            } else {
                None
            }
        }
    })
    .await
    .expect("FQDN form should resolve to real backend pod IPs (third alias form landed)");
    assert!(
        resolved_fqdn.iter().all(|ip| pod_ips.contains(ip)),
        "FQDN-resolved IPs {resolved_fqdn:?} must be real backend podIPs {pod_ips:?}"
    );

    // ── ASSERT: connect-by-name works (name → IP → reachable backend) ────
    // Start a TCP listener in each backend pod, then a throwaway client
    // `nc -z web <port>` — exit 0 proves the client resolved the Service
    // name AND reached a real backend. (ICMP ping needs CAP_NET_RAW, denied
    // under rootless podman; a TCP listener + nc -z needs no capability.)
    const PROBE_PORT: u16 = 9998;
    for cn in &container_names {
        podman_start_listener(cn, PROBE_PORT);
    }
    let connected = poll_until_blocking(TIMEOUT, INTERVAL, || {
        client_connect_by_name(ENGENHO_NET, &svc_name, PROBE_PORT)
    });
    assert!(
        connected,
        "a client container should connect to {svc_name}:{PROBE_PORT} BY NAME \
         (Service-name → aardvark-dns → real backend pod, headless)"
    );

    // ── ROUND-ROBIN (best-effort): both IPs appear across N resolutions ──
    // aardvark-dns returns a multi-A answer for a shared alias; over several
    // getent calls BOTH pod IPs should appear. Ordering is implementation-
    // defined, so this is best-effort/loose — the load-bearing asserts above
    // are "resolves to a real backend IP and connects by name". A non-multi-A
    // resolver (single-answer) would still pass those; this just documents
    // the headless multi-backend behavior when present.
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..8 {
        for ip in resolve_via_client(ENGENHO_NET, &svc_name) {
            seen.insert(ip);
        }
        if seen.len() >= 2 {
            break;
        }
    }
    // Loose: only assert the resolver saw at least one real backend IP (the
    // hard multi-A guarantee is aardvark-version-dependent).
    assert!(
        seen.iter().all(|ip| pod_ips.contains(ip)) && !seen.is_empty(),
        "round-robin resolutions {seen:?} must all be real backend podIPs {pod_ips:?}"
    );

    // ── TEARDOWN: DELETE the Deployment + both Pods over HTTP ────────────
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
    // Best-effort DELETE the Service too (no orphan Endpoints/Service).
    let _ = client
        .delete(format!(
            "http://{addr}/api/v1/namespaces/default/services/{svc_name}"
        ))
        .send()
        .await;

    // ── ASSERT: both real containers gone (kubelet stop-then-remove) ─────
    const CLEANUP_TIMEOUT: Duration = Duration::from_secs(45);
    let gone = poll_until_blocking(CLEANUP_TIMEOUT, INTERVAL, || {
        container_names
            .iter()
            .all(|cn| !podman_container_exists(cn))
    });
    assert!(
        gone,
        "expected podman to show NO containers {container_names:?} after workload delete"
    );

    // ── CLEANUP: graceful shutdown; engenho-net left in place (idempotent) ─
    rt.shutdown().await.expect("graceful shutdown");
    drop(cleanup);
}
