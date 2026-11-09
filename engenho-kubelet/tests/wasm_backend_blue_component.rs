//! End to end: blue's own engine component, `blue-component.wasm`, runs a blue
//! service program as a wasm pod through the runtime-class router and the wasm
//! backend, admitted by `kidou` under the service's access declaration, and
//! answers an HTTP request with what the program computes.
//!
//! The closure is the one blue's `service-greeter-component-serves` check runs
//! under wasmtime: `blue.lib.project` over blue's `examples/http`, which holds
//! `component.wasm` and the program bundle under `app/`. It is realised at run
//! time by `tests/blue_greeter_component.nix` with a real `nix build`, as
//! `native_runs_a_real_closure.rs` realises its closures; if it cannot be
//! realised the test fails rather than skipping.

#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use engenho_kubelet::backend::{
    ContainerRuntime, ContainerSpec, FakeBackend, LogOptions, PodIdentity, RuntimeClassRequest,
};
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::runtime_class::RuntimeClassRouter;
use engenho_kubelet::wasm_backend::{WASM_RUNTIME_CLASS, WasmBackend};
use serde_json::json;

const SERVICE: &str = "greeter";

fn closure() -> &'static str {
    static CLOSURE: OnceLock<String> = OnceLock::new();
    CLOSURE.get_or_init(|| {
        let file = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/blue_greeter_component.nix"
        );
        let out = std::process::Command::new("nix")
            .args([
                "build",
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--file",
                file,
            ])
            .output()
            .unwrap_or_else(|e| panic!("could not run nix to realise {file}: {e}"));
        assert!(
            out.status.success(),
            "realising {file} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let path = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string();
        assert!(
            path.starts_with("/nix/store/") && path.ends_with("-service-greeter-component"),
            "{file} realised {path:?}, not blue's greeter component closure"
        );
        path
    })
}

struct Pod {
    _dir: tempfile::TempDir,
    router: RuntimeClassRouter,
    default: FakeBackend,
    spec: ContainerSpec,
    port: u16,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("a free port")
}

fn pod(with_bundle: bool) -> Pod {
    let dir = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let access = json!({
        "effects": ["network", "clock", "entropy", "environment"],
        "egress": [],
        "reads": [],
        "writes": [],
        "listen": [port]
    });
    let mut annotations = serde_json::Map::new();
    annotations.insert("blue.pleme.io/access".into(), access.to_string().into());
    if with_bundle {
        annotations.insert("wasm.pleme.io/app".into(), "app".into());
    }
    let image = format!("nix:{}", closure());
    let container = json!({
        "name": "web",
        "image": image,
        "args": ["/app/greeter.b"],
        "ports": [{"containerPort": port}]
    });
    let pod = json!({
        "metadata": {"namespace": "ns", "name": SERVICE, "uid": "u1", "annotations": annotations},
        "spec": {"runtimeClassName": WASM_RUNTIME_CLASS, "restartPolicy": "Always", "containers": [container.clone()]}
    });
    let env: BTreeMap<String, String> = [
        ("BLUE_SERVICE", SERVICE.to_string()),
        (
            "GREETER_DEFAULTS",
            "/app/share/greeter/defaults.yaml".to_string(),
        ),
        ("GREETER_LISTEN__HOST", "127.0.0.1".to_string()),
        ("GREETER_LISTEN__PORT", port.to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let spec = ContainerSpec {
        name: "ns_greeter_web".into(),
        image,
        command: vec!["/app/greeter.b".into()],
        env,
        pod: PodIdentity {
            namespace: "ns".into(),
            name: SERVICE.into(),
            uid: "u1".into(),
            container_name: "web".into(),
            init: false,
        },
        runtime_class: RuntimeClassRequest::of_pod(&pod, &container),
        ..ContainerSpec::default()
    };
    let default = FakeBackend::new();
    let wasm = WasmBackend::new(dir.path().join("logs"), false);
    let router = RuntimeClassRouter::new(Arc::new(default.clone()))
        .with_class(WASM_RUNTIME_CLASS, Arc::new(wasm));
    Pod {
        _dir: dir,
        router,
        default,
        spec,
        port,
    }
}

fn get(port: u16, path: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n"
    )?;
    let mut out = String::new();
    s.read_to_string(&mut out)?;
    Ok(out)
}

async fn answer(port: u16, path: &'static str) -> Option<String> {
    tokio::task::spawn_blocking(move || get(port, path))
        .await
        .ok()
        .and_then(Result::ok)
        .filter(|r| r.starts_with("HTTP/1.1 200"))
}

async fn log(p: &Pod, id: &str) -> String {
    p.router.logs(id, &LogOptions::all()).await.expect("logs")
}

async fn state(p: &Pod, id: &str) -> RunState {
    p.router
        .status(id)
        .await
        .expect("status")
        .expect("tracked")
        .state
}

/// RED RUN (2026-11-09): `spawn_of` passing no preopens → `the component
/// ended before it served; log: blue-component: /app: No such file or
/// directory (os error 44)`.
#[tokio::test(flavor = "multi_thread")]
async fn blues_engine_component_serves_a_blue_program_as_a_wasm_pod() {
    let p = pod(true);
    let started = p.router.start(&p.spec).await.expect("the wasm pod starts");
    let id = started.container_id.clone();
    assert_eq!(started.state, RunState::Running);
    assert!(
        p.default.events().await.is_empty(),
        "the wasm pod reached the default runtime"
    );

    let mut greeting = None;
    for _ in 0..600 {
        greeting = answer(p.port, "/").await;
        if greeting.is_some() {
            break;
        }
        assert_eq!(
            state(&p, &id).await,
            RunState::Running,
            "the component ended before it served; log:\n{}",
            log(&p, &id).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let Some(greeting) = greeting else {
        panic!(
            "timed out waiting for the greeting; log:\n{}",
            log(&p, &id).await
        );
    };
    assert!(greeting.ends_with("hello, world\n"), "{greeting}");
    let asked = answer(p.port, "/?who=engenho")
        .await
        .expect("the component answers a second request");
    assert!(asked.ends_with("hello, engenho\n"), "{asked}");

    p.router.stop(&id).await.expect("stop");
    for _ in 0..300 {
        if state(&p, &id).await == RunState::Exited(ExitDisposition::Signal(15)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the stop was not surfaced");
}

/// The component's own `ExitCode::from(66)` reaches the kubelet as code 1:
/// WASI 0.2 `exit` carries only ok or err.
#[tokio::test(flavor = "multi_thread")]
async fn without_its_program_bundle_the_component_fails_fast_by_name() {
    let p = pod(false);
    let id = p
        .router
        .start(&p.spec)
        .await
        .expect("admission does not need the bundle")
        .container_id;
    let began = std::time::Instant::now();
    while began.elapsed() < Duration::from_secs(90) {
        if let RunState::Exited(exit) = state(&p, &id).await {
            let logged = log(&p, &id).await;
            assert_eq!(exit, ExitDisposition::Code(1), "{logged}");
            assert_eq!(
                logged
                    .matches("blue-component: /app: No such file or directory")
                    .count(),
                4,
                "{logged}"
            );
            assert!(logged.contains("in-place restarts spent"), "{logged}");
            assert!(answer(p.port, "/").await.is_none());
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "a component with no program bundle did not end within {:?}; log:\n{}",
        began.elapsed(),
        log(&p, &id).await
    );
}
