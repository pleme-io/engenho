//! End to end: a WASI preview 2 component runs as a wasm pod through the
//! runtime-class router and the wasm backend. It starts, writes to its log,
//! answers the kubelet's own httpGet and exec probes, is restarted in place by
//! the blue policy after a crash, and is stopped. The component is
//! `tatara-wasm`'s probe fixture serving HTTP on a declared port.

#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::time::Duration;

use engenho_kubelet::backend::{
    ContainerRuntime, ContainerSpec, FakeBackend, FakeEvent, LogOptions, PodIdentity,
    RuntimeClassRequest, TokioNetProber,
};
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::error::KubeletError;
use engenho_kubelet::probe::{run_handler, ProbeKind, ProbeObservation, ProbeSpec};
use engenho_kubelet::runtime_class::RuntimeClassRouter;
use engenho_kubelet::wasm_backend::{WasmBackend, WASM_RUNTIME_CLASS};
use serde_json::{json, Value};

struct Pod {
    _dir: tempfile::TempDir,
    router: RuntimeClassRouter,
    default: FakeBackend,
    spec: ContainerSpec,
    container: Value,
    port: u16,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("a free port")
}

fn pod(restart_policy: &str, image_of: impl FnOnce(&std::path::Path) -> String) -> Pod {
    let dir = tempfile::tempdir().expect("tempdir");
    let component = dir.path().join("probe.wasm");
    std::fs::write(&component, tatara_wasm::fixtures::PROBE).expect("write the fixture");
    let port = free_port();
    let container = json!({
        "name": "web",
        "image": image_of(&component),
        "args": ["serve", port.to_string()],
        "ports": [{"containerPort": port}],
        "readinessProbe": {"httpGet": {"path": "/healthz", "port": port}, "timeoutSeconds": 5},
        "livenessProbe": {"exec": {"command": ["true"]}}
    });
    let pod = json!({
        "metadata": {"namespace": "ns", "name": "hello", "uid": "u1"},
        "spec": {"runtimeClassName": WASM_RUNTIME_CLASS, "restartPolicy": restart_policy, "containers": [container.clone()]}
    });
    let spec = ContainerSpec {
        name: "ns_hello_web".into(),
        image: container["image"].as_str().expect("image").into(),
        command: vec!["serve".into(), port.to_string()],
        pod: PodIdentity {
            namespace: "ns".into(),
            name: "hello".into(),
            uid: "u1".into(),
            container_name: "web".into(),
            init: false,
        },
        runtime_class: RuntimeClassRequest::of_pod(&pod, &container),
        ..ContainerSpec::default()
    };
    let default = FakeBackend::new();
    let wasm = WasmBackend::new(dir.path().join("logs"), true);
    let router = RuntimeClassRouter::new(Arc::new(default.clone()))
        .with_class(WASM_RUNTIME_CLASS, Arc::new(wasm));
    Pod {
        _dir: dir,
        router,
        default,
        spec,
        container,
        port,
    }
}

fn file_image(p: &std::path::Path) -> String {
    format!("file:{}", p.display())
}

async fn probe(p: &Pod, kind: ProbeKind, id: &str) -> ProbeObservation {
    let spec = ProbeSpec::from_container(kind, &p.container)
        .expect("the probe parses")
        .expect("the probe is declared");
    run_handler(&spec, &p.router, &TokioNetProber::new(), id, Some("127.0.0.1")).await
}

async fn until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting until {what}");
}

async fn log(p: &Pod, id: &str) -> String {
    p.router.logs(id, &LogOptions::all()).await.expect("logs")
}

async fn state(p: &Pod, id: &str) -> RunState {
    p.router.status(id).await.expect("status").expect("tracked").state
}

fn get(port: u16, path: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(s, "GET {path} HTTP/1.1\r\nhost: x\r\n\r\n")?;
    let mut out = String::new();
    s.read_to_string(&mut out)?;
    Ok(out)
}

/// RED RUN (2026-10-27): `supervise` surfacing every exit (the restart arm
/// removed) → `timed out waiting until the crashed component answers again`.
#[tokio::test(flavor = "multi_thread")]
async fn a_wasm_pod_starts_logs_answers_probes_restarts_after_a_crash_and_stops() {
    let p = pod("Always", file_image);
    let started = p.router.start(&p.spec).await.expect("the wasm pod starts");
    let id = started.container_id.clone();
    assert_eq!(started.state, RunState::Running);
    assert_eq!(started.pod_ip.as_deref(), Some("127.0.0.1"));
    assert!(p.default.events().await.is_empty(), "the wasm pod reached the default runtime");

    until("the readiness probe passes", || async {
        probe(&p, ProbeKind::Readiness, &id).await == ProbeObservation::Success
    })
    .await;
    assert_eq!(probe(&p, ProbeKind::Liveness, &id).await, ProbeObservation::Success);
    let logged = log(&p, &id).await;
    assert!(logged.contains("probe started"), "{logged}");
    assert!(logged.contains(&format!("listening on {}", p.port)), "{logged}");
    assert!(logged.contains("request /healthz"), "{logged}");

    let port = p.port;
    tokio::task::spawn_blocking(move || get(port, "/crash"))
        .await
        .expect("join")
        .expect("the crash request is answered");
    until("the crash is logged with its restart", || async {
        log(&p, &id).await.contains("restart 1 of 3")
    })
    .await;
    assert_eq!(state(&p, &id).await, RunState::Running, "an in-place restart surfaced the crash");
    until("the crashed component answers again", || async {
        probe(&p, ProbeKind::Readiness, &id).await == ProbeObservation::Success
            && log(&p, &id).await.matches("probe started").count() == 2
    })
    .await;

    p.router.stop(&id).await.expect("stop");
    until("the stop is surfaced", || async {
        state(&p, &id).await == RunState::Exited(ExitDisposition::Signal(15))
    })
    .await;
    assert_eq!(probe(&p, ProbeKind::Liveness, &id).await, ProbeObservation::Failure);
    assert_eq!(probe(&p, ProbeKind::Readiness, &id).await, ProbeObservation::Failure);
    until("the record is removed", || async {
        !matches!(p.router.remove(&id).await, Err(KubeletError::NotReaped { .. }))
    })
    .await;
    assert!(p.router.status(&id).await.expect("status").is_none());
}

/// RED RUN (2026-10-27): kidou's `policy == "Never"` arm removed → the crash
/// is restarted in place and the status stays `Running`.
#[tokio::test(flavor = "multi_thread")]
async fn under_restart_policy_never_a_crash_is_surfaced_to_the_kubelet() {
    let p = pod("Never", file_image);
    let id = p.router.start(&p.spec).await.expect("start").container_id;
    until("the component listens", || async {
        probe(&p, ProbeKind::Readiness, &id).await == ProbeObservation::Success
    })
    .await;
    let port = p.port;
    let _ = tokio::task::spawn_blocking(move || get(port, "/crash")).await;
    until("the crash is surfaced", || async {
        state(&p, &id).await == RunState::Exited(ExitDisposition::Code(1))
    })
    .await;
    assert!(log(&p, &id).await.contains("restartPolicy Never"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pod_kidou_refuses_never_starts() {
    let oci = pod("Always", |_| "ghcr.io/pleme-io/web:1".to_string());
    match oci.router.start(&oci.spec).await {
        Err(KubeletError::InvalidPod { reason, .. }) => assert!(reason.contains("OCI reference"), "{reason}"),
        other => panic!("an OCI image started as a wasm pod: {other:?}"),
    }
    let mut unknown = pod("Always", file_image);
    if let Some(class) = unknown.spec.runtime_class.as_mut() {
        class.name = "gvisor".into();
    }
    assert!(matches!(
        unknown.router.start(&unknown.spec).await,
        Err(KubeletError::RuntimeClassUnknown { .. })
    ));
    assert!(unknown.default.events().await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pod_with_no_runtime_class_runs_on_the_default_runtime() {
    let mut p = pod("Always", file_image);
    p.spec.runtime_class = None;
    p.router.start(&p.spec).await.expect("start");
    assert_eq!(p.default.events().await, vec![FakeEvent::Start("ns_hello_web".into())]);
}
