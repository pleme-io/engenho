//! START CURVE — every start attempt, first or retry, goes through the
//! container's backoff curve, and a pod the kubelet has a record of still gets
//! its missing containers started.
//!
//! ★ WHY THIS FILE EXISTS. The kubelet starts a container from three places:
//! the first start of a pod's containers, a restart after an exit or a probe
//! failure, and the init sequence. Only the first consulted the start curve.
//! And once any container of a pod had started, the pod was only ever
//! OBSERVED, so a sibling whose first start failed was rendered Waiting
//! forever and never tried again. The two defects pull in opposite
//! directions: fixing the second by retrying the missing container is how the
//! first comes back (a failed start retried at the sync loop's speed,
//! measured as `pitr-lab/mysql-0` retried twice a second). So every test here
//! pins both halves: the start IS retried, and never faster than the curve.
//!
//! The curve (backoff.rs): the first attempt immediate, then 10s doubling to
//! a 5-minute cap. Replayed against a tick every second for an hour that is
//! at most 16 attempts: 0, 10, 30, 70, 150, 310, 610, then every 300s.
//!
//! Invariants:
//!   S1 a container whose start always fails is attempted at most 16 times in
//!      a virtual hour, and is still being retried at the end of it
//!   S2 a partially-started pod gets its missing container started once the
//!      curve allows, and not before; the sibling is never restarted
//!   S3 the missing container of a partially-started pod is retried on the
//!      curve: at most 16 attempts an hour, never abandoned
//!   S4 a replacement whose start always fails is retried on the curve, and
//!      the pod says CrashLoopBackOff while it waits
//!   S5 an init container whose start always fails is retried on the curve,
//!      and no app container starts meanwhile

use std::sync::Arc;
use std::time::Duration;

use engenho_controllers::Controller;
use engenho_kubelet::kubelet::TestClock;
use engenho_kubelet::{FakeBackend, Kubelet};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::{Value, json};

/// The T2.4 bound: attempts in one virtual hour of a start that always fails.
const MAX_ATTEMPTS_PER_HOUR: usize = 16;

/// One virtual hour, in one-second ticks.
const HOUR_S: u64 = 3_600;

/// The curve's cap: once reached, an attempt falls in every window this long.
const CAP_S: u64 = 300;

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

/// Put a pod bound to `node-A` with the given app container names (image =
/// name) and, optionally, init containers.
async fn put_pod(store: &StoreMesh, name: &str, apps: &[&str], inits: &[&str]) {
    let containers: Vec<Value> = apps
        .iter()
        .map(|c| json!({ "name": c, "image": c }))
        .collect();
    let mut spec = json!({
        "nodeName": "node-A",
        "restartPolicy": "Always",
        "containers": containers,
    });
    if !inits.is_empty() {
        spec["initContainers"] = inits
            .iter()
            .map(|c| json!({ "name": c, "image": c }))
            .collect();
    }
    store
        .propose(ResourceCommand::Put {
            key: pod_key(name),
            value: json!({
                "kind": "Pod",
                "apiVersion": "v1",
                "metadata": { "name": name },
                "spec": spec,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn pod(store: &StoreMesh, name: &str) -> Value {
    store.get(&pod_key(name)).await.expect("pod present")
}

fn phase(p: &Value) -> Option<String> {
    p.pointer("/status/phase")?.as_str().map(String::from)
}

/// The `containerStatuses[]` entry for `cname`.
fn status_of<'a>(p: &'a Value, cname: &str) -> Option<&'a Value> {
    p.pointer("/status/containerStatuses")?
        .as_array()?
        .iter()
        .find(|c| c.get("name").and_then(Value::as_str) == Some(cname))
}

fn waiting_reason(p: &Value, cname: &str) -> Option<String> {
    status_of(p, cname)?
        .pointer("/state/waiting/reason")?
        .as_str()
        .map(String::from)
}

fn is_running(p: &Value, cname: &str) -> bool {
    status_of(p, cname).is_some_and(|c| c.pointer("/state/running").is_some())
}

/// Tick once a second for `seconds` virtual seconds.
async fn run_for(kubelet: &Kubelet, clock: &TestClock, seconds: u64) {
    for _ in 0..seconds {
        kubelet.tick().await.unwrap();
        clock.advance(Duration::from_secs(1));
    }
}

/// Run one virtual hour and return `(attempts in the hour, attempts in its
/// last CAP_S + 1 seconds)` of `backend_name`'s start.
async fn attempts_over_an_hour(
    kubelet: &Kubelet,
    clock: &TestClock,
    backend: &FakeBackend,
    backend_name: &str,
) -> (usize, usize) {
    let before = backend.start_attempts(backend_name).await;
    run_for(kubelet, clock, HOUR_S - CAP_S - 1).await;
    let before_tail = backend.start_attempts(backend_name).await;
    run_for(kubelet, clock, CAP_S + 1).await;
    let after = backend.start_attempts(backend_name).await;
    (after - before, after - before_tail)
}

async fn teardown(store: Arc<StoreMesh>, kubelet: Kubelet) {
    drop(kubelet);
    Arc::try_unwrap(store)
        .ok()
        .unwrap()
        .terminate()
        .await
        .unwrap();
}

#[tokio::test]
async fn s1_a_start_that_always_fails_is_attempted_at_most_sixteen_times_an_hour() {
    let store = boot_store("start-curve-s1").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure("default_db_mysql", "cannot run an OCI image")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store, "db", &["mysql"], &[]).await;

    let (attempts, tail) =
        attempts_over_an_hour(&kubelet, &clock, &backend, "default_db_mysql").await;

    assert!(
        attempts <= MAX_ATTEMPTS_PER_HOUR,
        "a start that always fails is attempted at most {MAX_ATTEMPTS_PER_HOUR} times an hour, got {attempts}"
    );
    assert!(
        tail >= 1,
        "the cap is a ceiling on the wait, never a stop: no attempt in the last {CAP_S}s"
    );
    let p = pod(&store, "db").await;
    assert_eq!(phase(&p).as_deref(), Some("Pending"), "{p}");

    teardown(store, kubelet).await;
}

#[tokio::test]
async fn s2_a_partially_started_pod_gets_its_missing_container_started_on_the_curve() {
    let store = boot_store("start-curve-s2").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure("default_mp_side", "image not known")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store, "mp", &["web", "side"], &[]).await;

    // web starts, side's first start fails: the pod now has a record.
    kubelet.tick().await.unwrap();
    assert_eq!(backend.start_attempts("default_mp_web").await, 1, "premise");
    assert_eq!(
        backend.start_attempts("default_mp_side").await,
        1,
        "premise"
    );
    let p = pod(&store, "mp").await;
    assert_eq!(phase(&p).as_deref(), Some("Pending"), "{p}");

    // The cause is fixed, but the curve owes 10s: ticking inside the wait
    // attempts nothing. A retry here is the hot loop coming back.
    backend.clear_start_failure("default_mp_side").await;
    for _ in 0..5 {
        kubelet.tick().await.unwrap();
    }
    clock.advance(Duration::from_secs(9));
    kubelet.tick().await.unwrap();
    assert_eq!(
        backend.start_attempts("default_mp_side").await,
        1,
        "no retry inside the 10s the first failure earned"
    );

    // Once the wait is served the missing container is started.
    clock.advance(Duration::from_secs(2));
    kubelet.tick().await.unwrap();
    assert_eq!(
        backend.start_attempts("default_mp_side").await,
        2,
        "the container whose first start failed is started once a sibling had started"
    );
    let p = pod(&store, "mp").await;
    assert!(is_running(&p, "side"), "side is up: {p}");
    assert!(is_running(&p, "web"), "web is up: {p}");
    assert_eq!(phase(&p).as_deref(), Some("Running"), "{p}");
    assert_eq!(
        backend.start_attempts("default_mp_web").await,
        1,
        "starting the missing container never restarts the one already up"
    );

    teardown(store, kubelet).await;
}

#[tokio::test]
async fn s3_the_missing_container_of_a_started_pod_is_retried_on_the_curve() {
    let store = boot_store("start-curve-s3").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure("default_mp_side", "image not known")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store, "mp", &["web", "side"], &[]).await;

    let (attempts, tail) =
        attempts_over_an_hour(&kubelet, &clock, &backend, "default_mp_side").await;

    assert!(
        attempts <= MAX_ATTEMPTS_PER_HOUR,
        "retrying the missing container must not bring back the hot loop: {attempts} attempts in an hour"
    );
    assert!(
        tail >= 1,
        "the missing container is never abandoned: no attempt in the last {CAP_S}s"
    );
    assert_eq!(backend.start_attempts("default_mp_web").await, 1);
    let p = pod(&store, "mp").await;
    assert_eq!(phase(&p).as_deref(), Some("Pending"), "{p}");

    teardown(store, kubelet).await;
}

#[tokio::test]
async fn s4_a_replacement_that_cannot_start_is_retried_on_the_curve() {
    let store = boot_store("start-curve-s4").await;
    let backend = Arc::new(FakeBackend::new());
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store, "crasher", &["app"], &[]).await;

    kubelet.tick().await.unwrap();
    assert_eq!(backend.start_attempts("default_crasher_app").await, 1);

    // It exits, and from now on no replacement can start.
    backend
        .seed_start_failure("default_crasher_app", "image not known")
        .await;
    let id = backend
        .containers()
        .await
        .into_iter()
        .find(|(_, s)| s.is_running())
        .map(|(id, _)| id)
        .expect("a running container");
    backend.set_exit(&id, 1).await;

    let (attempts, tail) =
        attempts_over_an_hour(&kubelet, &clock, &backend, "default_crasher_app").await;

    assert!(
        attempts <= MAX_ATTEMPTS_PER_HOUR,
        "a replacement that cannot start is attempted at most {MAX_ATTEMPTS_PER_HOUR} times an hour, got {attempts}"
    );
    assert!(
        tail >= 1,
        "never abandoned: no attempt in the last {CAP_S}s"
    );

    // Between attempts the pod says why nothing is happening.
    kubelet.tick().await.unwrap();
    let p = pod(&store, "crasher").await;
    assert_eq!(
        waiting_reason(&p, "app").as_deref(),
        Some("CrashLoopBackOff"),
        "{p}"
    );
    assert_eq!(phase(&p).as_deref(), Some("Running"), "{p}");

    teardown(store, kubelet).await;
}

#[tokio::test]
async fn s5_an_init_container_that_cannot_start_is_retried_on_the_curve() {
    let store = boot_store("start-curve-s5").await;
    let backend = Arc::new(FakeBackend::new());
    backend
        .seed_start_failure("default_ip_init-setup", "image not known")
        .await;
    let clock = TestClock::new();
    let kubelet =
        Kubelet::new(store.clone(), backend.clone(), "node-A").with_clock(clock.as_clock());
    put_pod(&store, "ip", &["app"], &["setup"]).await;

    let (attempts, tail) =
        attempts_over_an_hour(&kubelet, &clock, &backend, "default_ip_init-setup").await;

    assert!(
        attempts <= MAX_ATTEMPTS_PER_HOUR,
        "an init container that cannot start is attempted at most {MAX_ATTEMPTS_PER_HOUR} times an hour, got {attempts}"
    );
    assert!(
        tail >= 1,
        "never abandoned: no attempt in the last {CAP_S}s"
    );
    assert_eq!(
        backend.start_attempts("default_ip_app").await,
        0,
        "no app container starts ahead of its init container"
    );
    let p = pod(&store, "ip").await;
    assert_eq!(phase(&p).as_deref(), Some("Pending"), "{p}");

    teardown(store, kubelet).await;
}
