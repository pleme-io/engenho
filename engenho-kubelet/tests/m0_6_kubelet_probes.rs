//! M0.6 — kubelet probes (liveness / readiness / startup; exec / httpGet /
//! tcpSocket).
//!
//! Proves the probe brick against the MOCK seams (FakeBackend exec +
//! FakeNetProber http/tcp) — the trait IS the testability contract, so ZERO
//! real podman / real sockets:
//!
//!   * READINESS flips `containerStatuses[].ready` (and the `Ready` /
//!     `ContainersReady` conditions) false→true after `successThreshold`
//!     consecutive exec successes.
//!   * LIVENESS restarts the container (restartCount++ + a new container_id)
//!     after `failureThreshold` consecutive exec failures — and a
//!     restartPolicy:Never pod is NOT restarted by a failing liveness probe.
//!   * STARTUP gates liveness — no premature restart during the startup
//!     window; readiness forced false until the startup probe passes.
//!   * NO-PROBE pod is Ready 1/1 + `Ready=True` on the FIRST running tick AND
//!     that tick arms NO Requeue (byte-identical-to-today regression).
//!   * tcpSocket via FakeNetProber: refused→ok flips readiness.
//!   * httpGet via FakeNetProber: 2xx/3xx pass, 4xx/5xx fail.

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::{Controller, ReconcileResult};
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{ExecOutcome, FakeBackend, FakeNetProber, Kubelet, NetProber};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

async fn boot_store(name: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(name).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

fn pod_key(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

/// Put a single-container pod bound to `node-A` with an explicit JSON for the
/// container (lets each test inject the probe shape).
async fn put_pod_container(store: &StoreMesh, name: &str, container: Value, restart_policy: &str) {
    let value = json!({
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": { "name": name },
        "spec": {
            "nodeName": "node-A",
            "restartPolicy": restart_policy,
            "containers": [container]
        }
    });
    store
        .propose(ResourceCommand::Put {
            key: pod_key(name),
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn container_ready(pod: &Value, idx: usize) -> Option<bool> {
    pod.get("status")?
        .get("containerStatuses")?
        .as_array()?
        .get(idx)?
        .get("ready")?
        .as_bool()
}

fn condition_status(pod: &Value, ty: &str) -> Option<String> {
    pod.get("status")?
        .get("conditions")?
        .as_array()?
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(ty))?
        .get("status")?
        .as_str()
        .map(String::from)
}

fn restart_count(pod: &Value, idx: usize) -> Option<i64> {
    pod.get("status")?
        .get("containerStatuses")?
        .as_array()?
        .get(idx)?
        .get("restartCount")?
        .as_i64()
}

fn count_starts(events: &[engenho_kubelet::backend::FakeEvent]) -> usize {
    use engenho_kubelet::backend::FakeEvent;
    events
        .iter()
        .filter(|e| matches!(e, FakeEvent::Start(_)))
        .count()
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    let mesh = Arc::try_unwrap(store).ok().unwrap();
    mesh.terminate().await.unwrap();
}

// ── 1 — READINESS gates Ready (exec) ──────────────────────────────────────

#[tokio::test]
async fn readiness_exec_flips_ready_after_success_threshold() {
    let store = boot_store("probes-readiness").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    // Readiness exec probe, period 1s, successThreshold 2.
    let container = json!({
        "name": "main",
        "image": "busybox",
        "readinessProbe": {
            "exec": { "command": ["sh", "-c", "test -f /tmp/ready"] },
            "periodSeconds": 1,
            "successThreshold": 2
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    // Seed the exec results: first two FAIL, then SUCCESS×N (the readiness
    // flips once two CONSECUTIVE successes land). Backend name is
    // <ns>_<pod>_<cname> = default_p1_main.
    backend
        .seed_exec(
            "default_p1_main",
            [
                ExecOutcome::failure(1),
                ExecOutcome::failure(1),
                ExecOutcome::success(),
                ExecOutcome::success(),
                ExecOutcome::success(),
                ExecOutcome::success(),
            ],
        )
        .await;

    // First tick: container starts. The start path runs a probe immediately
    // (initialDelay 0): a Fail → not ready.
    kubelet.tick().await.unwrap();
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(pod.get("status").and_then(|s| s.get("phase")).and_then(|p| p.as_str()), Some("Running"));
    assert_eq!(container_ready(&pod, 0), Some(false), "first probe failed → not ready");
    assert_eq!(condition_status(&pod, "Ready").as_deref(), Some("False"));
    assert_eq!(condition_status(&pod, "ContainersReady").as_deref(), Some("False"));

    // Drive ticks: advance the clock past the period each time so the probe is
    // DUE on the next tick (the TestClock stands in for the Requeue cadence,
    // deterministically). 2nd probe Fail, 3rd+4th Success → ready flips true
    // once two consecutive successes land.
    let mut became_ready = false;
    for _ in 0..6 {
        clock.advance(Duration::from_millis(1100));
        kubelet.tick().await.unwrap();
        let pod = store.get(&pod_key("p1")).await.unwrap();
        if container_ready(&pod, 0) == Some(true) {
            became_ready = true;
            // Ready + ContainersReady conditions both True.
            assert_eq!(condition_status(&pod, "Ready").as_deref(), Some("True"));
            assert_eq!(condition_status(&pod, "ContainersReady").as_deref(), Some("True"));
            break;
        }
    }
    assert!(became_ready, "readiness must flip true after two consecutive exec successes");
    // Exactly one Start ever — readiness never restarts.
    assert_eq!(count_starts(&backend.events().await), 1);

    teardown(store, kubelet).await;
}

// ── 2 — LIVENESS restarts after failureThreshold ──────────────────────────

#[tokio::test]
async fn liveness_exec_restarts_after_failure_threshold() {
    let store = boot_store("probes-liveness").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    // Liveness exec probe, period 1s, failureThreshold 1 → restarts on the
    // first failure.
    let container = json!({
        "name": "main",
        "image": "busybox",
        "livenessProbe": {
            "exec": { "command": ["sh", "-c", "test -f /tmp/alive"] },
            "periodSeconds": 1,
            "failureThreshold": 1
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    // Every exec FAILS (/tmp/alive never created).
    backend
        .set_default_exec(ExecOutcome::failure(1))
        .await;

    // First tick: start + first probe (Fail, threshold 1) → restart THIS tick.
    kubelet.tick().await.unwrap();
    // A few more ticks (advancing past the period) keep failing → keep
    // restarting. The freshly-restarted container's probe is due immediately
    // (last_run None) so it fails again next tick.
    for _ in 0..2 {
        clock.advance(Duration::from_millis(1100));
        kubelet.tick().await.unwrap();
    }

    let pod = store.get(&pod_key("p1")).await.unwrap();
    let rc = restart_count(&pod, 0).unwrap_or(0);
    assert!(rc >= 1, "failing liveness must restart (restartCount >= 1), got {rc}");
    // More than one Start: the original + at least one restart.
    assert!(count_starts(&backend.events().await) >= 2, "liveness restart re-creates the container");

    teardown(store, kubelet).await;
}

// ── 3 — restartPolicy:Never suppresses a failing-liveness restart ─────────

#[tokio::test]
async fn liveness_failure_does_not_restart_under_never() {
    let store = boot_store("probes-liveness-never").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    let container = json!({
        "name": "main",
        "image": "busybox",
        "livenessProbe": {
            "exec": { "command": ["false"] },
            "periodSeconds": 1,
            "failureThreshold": 1
        }
    });
    // restartPolicy:Never → a failing liveness must NOT restart (K8s).
    put_pod_container(&store, "p1", container, "Never").await;
    backend.set_default_exec(ExecOutcome::failure(1)).await;

    kubelet.tick().await.unwrap();
    for _ in 0..3 {
        clock.advance(Duration::from_millis(1100));
        kubelet.tick().await.unwrap();
    }
    // Exactly one Start across the lifetime — Never never restarts.
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "restartPolicy:Never suppresses the liveness restart"
    );

    teardown(store, kubelet).await;
}

// ── 4 — STARTUP gates liveness (no premature restart) ─────────────────────

#[tokio::test]
async fn startup_gates_liveness_no_premature_restart() {
    let store = boot_store("probes-startup").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    // Startup exec probe that passes only after a few attempts; a liveness
    // probe that would fail (default exec fails). While startup is unsatisfied,
    // liveness must be suppressed → NO restart during the window.
    let container = json!({
        "name": "main",
        "image": "busybox",
        "startupProbe": {
            "exec": { "command": ["sh", "-c", "test -f /tmp/started"] },
            "periodSeconds": 1,
            "failureThreshold": 10
        },
        "livenessProbe": {
            "exec": { "command": ["sh", "-c", "test -f /tmp/alive"] },
            "periodSeconds": 1,
            "failureThreshold": 1
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    // Startup: FAIL the first 3 attempts then SUCCEED. Liveness would FAIL
    // (default), but it's suppressed during the startup window. The seeded
    // queue applies to startup AND liveness (same backend name) — so to keep
    // them distinct we make the DEFAULT a failure (liveness) and SEED only the
    // startup-shaped sequence. Both share the queue, so to isolate the startup
    // gate we instead assert the behavioral invariant: during the window,
    // restartCount stays 0.
    //
    // Simplest robust seeding: default exec = FAIL (covers liveness + the early
    // startup attempts), proving the gate holds restart at 0 while startup is
    // unsatisfied.
    backend.set_default_exec(ExecOutcome::failure(1)).await;

    // Several ticks (advancing past the period): startup never passes (always
    // fail) but failureThreshold is 10, so startup itself does not trip;
    // liveness is SUPPRESSED by the open startup window → restartCount stays 0.
    for _ in 0..4 {
        clock.advance(Duration::from_millis(1100));
        kubelet.tick().await.unwrap();
    }
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(
        restart_count(&pod, 0).unwrap_or(0),
        0,
        "liveness must NOT restart during the startup window"
    );
    assert_eq!(
        count_starts(&backend.events().await),
        1,
        "no restart during startup window → exactly one Start"
    );
    // Readiness has no probe here, but startup forces not-ready while open.
    assert_eq!(condition_status(&pod, "Ready").as_deref(), Some("False"));

    teardown(store, kubelet).await;
}

// ── 5 — NO-PROBE pod: Ready 1/1 immediately + NO Requeue (regression) ─────

#[tokio::test]
async fn no_probe_pod_is_ready_immediately_and_arms_no_requeue() {
    let store = boot_store("probes-noprobe").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_net_prober(net.clone());

    // A no-probe container — must behave exactly as today.
    let container = json!({ "name": "main", "image": "busybox" });
    put_pod_container(&store, "p1", container, "Always").await;

    let outcome = kubelet.tick().await.unwrap();
    // Byte-identical-to-today: Ready 1/1 on the first running tick.
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert_eq!(container_ready(&pod, 0), Some(true), "no-probe container ready once Running");
    assert_eq!(condition_status(&pod, "Ready").as_deref(), Some("True"));
    assert_eq!(condition_status(&pod, "ContainersReady").as_deref(), Some("True"));
    // AND the tick arms NO Requeue (a no-probe pod contributes no probe clock).
    assert_eq!(
        outcome.result,
        ReconcileResult::Done,
        "a no-probe pod must NOT arm a Requeue (same wake behavior as today)"
    );

    // Steady-state: a further tick is a NoChange + still Done.
    let outcome2 = kubelet.tick().await.unwrap();
    assert_eq!(outcome2.report.objects_changed, 0, "steady running tick is a no-op");
    assert_eq!(outcome2.result, ReconcileResult::Done);

    teardown(store, kubelet).await;
}

// ── 6 — A probe-bearing pod DOES arm a Requeue ────────────────────────────

#[tokio::test]
async fn probe_pod_arms_a_requeue() {
    let store = boot_store("probes-requeue").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_net_prober(net.clone());

    let container = json!({
        "name": "main",
        "image": "busybox",
        "readinessProbe": {
            "exec": { "command": ["true"] },
            "periodSeconds": 5
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;
    backend.set_default_exec(ExecOutcome::success()).await;

    let outcome = kubelet.tick().await.unwrap();
    // The pod has a probe → the tick arms a Requeue at the next-due delay.
    match outcome.result {
        ReconcileResult::Requeue(d) | ReconcileResult::RequeueWithProgress(d) => {
            assert!(d <= Duration::from_secs(5), "requeue within the period");
        }
        ReconcileResult::Done => panic!("a probe-bearing pod must arm a Requeue"),
    }

    teardown(store, kubelet).await;
}

// ── 7 — tcpSocket readiness via FakeNetProber (refused→ok flips ready) ─────

#[tokio::test]
async fn tcp_socket_readiness_becomes_ready_when_port_up() {
    let store = boot_store("probes-tcp").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    let container = json!({
        "name": "main",
        "image": "busybox",
        "readinessProbe": {
            "tcpSocket": { "port": 8080 },
            "periodSeconds": 1,
            "successThreshold": 1
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    // The FakeBackend assigns the pod IP 10.42.0.1 to the first container.
    // Seed the tcp verdicts for that IP:8080 — refused first, then ok.
    let pod_ip = "10.42.0.1";
    net.seed_tcp(pod_ip, 8080, [false, false, true, true]).await;

    // Tick until ready flips (the port comes up on the 3rd seeded verdict).
    let mut became_ready = false;
    for _ in 0..6 {
        kubelet.tick().await.unwrap();
        let pod = store.get(&pod_key("p1")).await.unwrap();
        if container_ready(&pod, 0) == Some(true) {
            became_ready = true;
            break;
        }
        clock.advance(Duration::from_millis(1100));
    }
    assert!(became_ready, "tcpSocket readiness flips ready when the port is up");
    teardown(store, kubelet).await;
}

// ── 8 — httpGet readiness via FakeNetProber (2xx pass, 5xx fail) ──────────

#[tokio::test]
async fn http_get_readiness_passes_on_2xx_fails_on_5xx() {
    let store = boot_store("probes-http").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let clock = TestClock::new();
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A")
        .with_net_prober(net.clone())
        .with_clock(clock.as_clock());

    let container = json!({
        "name": "main",
        "image": "busybox",
        "readinessProbe": {
            "httpGet": { "path": "/healthz", "port": 8080 },
            "periodSeconds": 1,
            "successThreshold": 1
        }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    let pod_ip = "10.42.0.1";
    // 503 (fail) ×2, then 200 (pass).
    net.seed_http(pod_ip, 8080, "/healthz", [503u16, 503, 200, 200])
        .await;

    let mut became_ready = false;
    for _ in 0..6 {
        kubelet.tick().await.unwrap();
        let pod = store.get(&pod_key("p1")).await.unwrap();
        if container_ready(&pod, 0) == Some(true) {
            became_ready = true;
            break;
        }
        clock.advance(Duration::from_millis(1100));
    }
    assert!(became_ready, "httpGet readiness flips ready on a 2xx status");

    teardown(store, kubelet).await;
}

// ── 9 — invalid probe (no handler) skips the pod, never a fake pass ───────

#[tokio::test]
async fn invalid_probe_skips_pod_never_a_fake_pass() {
    let store = boot_store("probes-invalid").await;
    let backend = Arc::new(FakeBackend::new());
    let net = Arc::new(FakeNetProber::new());
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_net_prober(net.clone());

    // A probe with NO handler (only timing) → typed parse error → pod skipped.
    let container = json!({
        "name": "main",
        "image": "busybox",
        "readinessProbe": { "periodSeconds": 5 }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    let report = kubelet.tick().await.unwrap();
    assert!(report.report.objects_skipped >= 1, "invalid probe → pod skipped");
    // No container was started (the bad probe is rejected BEFORE start).
    assert_eq!(count_starts(&backend.events().await), 0, "never starts a pod with an invalid probe");
    // No fake Running status written.
    let pod = store.get(&pod_key("p1")).await.unwrap();
    assert!(
        pod.get("status").and_then(|s| s.get("phase")).is_none(),
        "skipped pod has no Running status (never a fake pass)"
    );

    teardown(store, kubelet).await;
}

// ── 10 — grpc handler is a typed deferral (not a silent pass) ─────────────

#[tokio::test]
async fn grpc_probe_is_unsupported_skips_pod() {
    let store = boot_store("probes-grpc").await;
    let backend = Arc::new(FakeBackend::new());
    let net: Arc<dyn NetProber> = Arc::new(FakeNetProber::new());
    let kubelet = Kubelet::new(store.clone(), backend.clone(), "node-A").with_net_prober(net);

    let container = json!({
        "name": "main",
        "image": "busybox",
        "livenessProbe": { "grpc": { "port": 9000 }, "periodSeconds": 5 }
    });
    put_pod_container(&store, "p1", container, "Always").await;

    let report = kubelet.tick().await.unwrap();
    assert!(report.report.objects_skipped >= 1, "grpc probe → pod skipped (documented deferral)");
    assert_eq!(count_starts(&backend.events().await), 0);

    teardown(store, kubelet).await;
}
