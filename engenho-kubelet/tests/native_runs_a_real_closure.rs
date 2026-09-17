//! End-to-end: the native backend really runs a real Nix closure.
//!
//! The unit tests around `NativeBackend` exercise refusals and resolution, all
//! of which pass without a single process ever being spawned. This one starts
//! an actual binary out of an actual store path and reads back what it printed,
//! because "the backend compiles and refuses the right things" is not the same
//! claim as "the backend runs a workload".
//!
//! It resolves its closure at runtime via `nix build --print-out-paths` rather
//! than hardcoding a hash, so it does not rot when nixpkgs moves. If the
//! closure cannot be realised the test FAILS rather than skipping: a silently
//! skipped end-to-end test is how a runtime comes to have no end-to-end
//! coverage at all.

use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, LogOptions, PodIdentity};
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use std::collections::BTreeMap;

/// Realise a closure and return its store path.
fn closure(attr: &str) -> String {
    let out = std::process::Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", attr])
        .output()
        .unwrap_or_else(|e| panic!("could not run nix to realise {attr}: {e}"));
    assert!(
        out.status.success(),
        "realising {attr} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let path = String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    assert!(!path.is_empty(), "{attr} produced no output path");
    path
}

fn spec(image: &str, command: &[&str], env: &[(&str, &str)]) -> ContainerSpec {
    ContainerSpec {
        name: "probe".to_string(),
        image: image.to_string(),
        command: command.iter().map(|s| (*s).to_string()).collect(),
        env: env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<BTreeMap<_, _>>(),
        pod: PodIdentity {
            namespace: "default".to_string(),
            name: "native-probe".to_string(),
            container_name: "probe".to_string(),
            ..PodIdentity::default()
        },
        ..ContainerSpec::default()
    }
}

/// ★ The claim: a container is a native host process out of a Nix closure —
/// no VM, no OCI layer, no podman.
#[tokio::test]
async fn a_nix_closure_runs_as_a_native_process_and_its_output_is_readable() {
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir);

    let mut image = String::from("nix:");
    image.push_str(&coreutils);

    let started = backend
        .start(&spec(&image, &["echo", "hello from a closure"], &[]))
        .await
        .expect("a realised closure must start");
    assert_eq!(started.container_id, "default_native-probe_probe");
    assert!(
        started.running,
        "a freshly spawned process must report running"
    );
    assert!(
        started.pod_ip.is_none(),
        "a native process shares the host network; inventing a pod IP would be \
         worse than reporting none"
    );

    // Poll for exit rather than sleeping a guessed interval.
    let mut status = None;
    for _ in 0..100 {
        let s = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("the container is tracked");
        if !s.running {
            status = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let status = status.expect("echo must exit well within two seconds");
    assert_eq!(
        status.exit_code,
        Some(0),
        "the real exit code must be readable — a dropped Child would leave a \
         zombie and report None forever, which a kubelet reads as running"
    );

    let logs = backend
        .logs(&started.container_id, &LogOptions::default())
        .await
        .expect("logs must be readable");
    assert_eq!(
        logs.trim(),
        "hello from a closure",
        "the workload's actual stdout must come back"
    );

    backend.remove(&started.container_id).await.expect("remove");
    assert!(
        backend
            .status(&started.container_id)
            .await
            .expect("status")
            .is_none(),
        "a removed container must stop being tracked"
    );
}

/// The environment a container gets is the one its spec DECLARES — nothing
/// inherited. An implicitly-inherited env is how a workload comes to depend on
/// something no manifest records, and it only shows up on a different host.
#[tokio::test]
async fn a_container_sees_only_its_declared_environment() {
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e-env");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir);

    // Set a variable in the PARENT that the container must not see.
    unsafe { std::env::set_var("ENGENHO_MUST_NOT_LEAK", "leaked") };

    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let started = backend
        .start(&spec(&image, &["env"], &[("DECLARED_ONLY", "yes")]))
        .await
        .expect("start");

    for _ in 0..100 {
        let s = backend
            .status(&started.container_id)
            .await
            .unwrap()
            .unwrap();
        if !s.running {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let logs = backend
        .logs(&started.container_id, &LogOptions::default())
        .await
        .expect("logs");
    assert!(
        logs.contains("DECLARED_ONLY=yes"),
        "the declared variable must reach the container: {logs}"
    );
    assert!(
        !logs.contains("ENGENHO_MUST_NOT_LEAK"),
        "the daemon's own environment must NOT leak into a container: {logs}"
    );
}

/// The refusal, end to end through the real backend: the image ryn runs today
/// cannot be started here, and the error says why and what to do.
#[tokio::test]
async fn the_image_ryn_runs_today_is_refused_by_the_real_backend() {
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-refuse"),
    );
    let err = backend
        .start(&spec(
            "docker.io/library/postgres:16-alpine",
            &["postgres"],
            &[],
        ))
        .await
        .expect_err("an OCI image has no native runtime under it");
    let msg = err.to_string();
    assert!(msg.contains("no Linux runtime"), "{msg}");
    assert!(msg.contains("nix:/nix/store/"), "{msg}");
}
