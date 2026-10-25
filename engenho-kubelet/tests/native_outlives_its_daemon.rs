//! A native workload outlives the backend that started it, and the next
//! backend adopts the same process: same pid, same start time, its output
//! still reaching the log. Only a changed spec or a pod nobody binds stops
//! it. Linux only, against a real closure realised with `nix build`.

#![cfg(target_os = "linux")]
#![allow(
    clippy::disallowed_methods,
    reason = "drives the native runtime directly; no kubelet in the loop"
)]

use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, LogOptions, PodIdentity};
use engenho_kubelet::cgroup::Cgroups;
use engenho_kubelet::cri::RunState;
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use engenho_kubelet::native_survival::WorkloadLifetime;
use engenho_kubelet::{KubeletError, Readoption};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn spec(bash: &str, uid: &str, env: &[(&str, &str)]) -> ContainerSpec {
    let path = [closure("nixpkgs#coreutils"), "/bin".to_string()].concat();
    ContainerSpec {
        name: "ticker".to_string(),
        image: ["nix:", bash].concat(),
        command: vec![
            "bash".to_string(),
            "-c".to_string(),
            "echo started; while sleep 0.1; do echo tick; done".to_string(),
        ],
        env: env
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .chain([("PATH".to_string(), path)])
            .collect(),
        pod: PodIdentity {
            namespace: "house".to_string(),
            name: "ticker-0".to_string(),
            uid: uid.to_string(),
            container_name: "ticker".to_string(),
            ..PodIdentity::default()
        },
        ..ContainerSpec::default()
    }
}

struct Node {
    logs: PathBuf,
    state: PathBuf,
    _dir: tempfile::TempDir,
}

impl Node {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        Self {
            logs: dir.path().join("logs"),
            state: dir.path().join("pods/native"),
            _dir: dir,
        }
    }

    fn daemon(&self, lifetime: &str) -> NativeBackend {
        let lifetime = match lifetime {
            "outlive" => WorkloadLifetime::Outlive {
                state: self.state.clone(),
            },
            _ => WorkloadLifetime::Bound,
        };
        NativeBackend::new(Isolation::HostProcess, &self.logs, Cgroups::Off)
            .with_lifetime(lifetime)
            .expect("the state directory is writable")
    }

    fn recorded_pid(&self, id: &str) -> Option<u32> {
        let bytes = std::fs::read(self.state.join([id, ".json"].concat())).ok()?;
        let record: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        record["pid"].as_u64().and_then(|p| u32::try_from(p).ok())
    }
}

fn start_time(pid: u32) -> Option<String> {
    let stat =
        std::fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("stat")).ok()?;
    let (_, rest) = stat.rsplit_once(')')?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    (fields.first() != Some(&"Z")).then(|| fields[19].to_string())
}

async fn until<F: Fn() -> bool>(what: &str, f: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn log_of(backend: &NativeBackend, id: &str) -> String {
    backend
        .logs(id, &LogOptions::default())
        .await
        .expect("the log is readable")
}

#[tokio::test]
async fn a_restarted_daemon_adopts_the_running_workload_with_its_pid_unchanged() {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let first = node.daemon("outlive");
    let started = first
        .start(&spec(&bash, "uid-1", &[]))
        .await
        .expect("the workload starts");
    let id = started.container_id.clone();
    let pid = node.recorded_pid(&id).expect("the workload is recorded");
    let born = start_time(pid).expect("the workload runs");
    until("the workload prints", || {
        std::fs::read_to_string(node.logs.join(&id)).is_ok_and(|l| l.contains("tick"))
    })
    .await;

    drop(first);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        start_time(pid).as_deref(),
        Some(born.as_str()),
        "the workload must outlive the backend that started it"
    );

    let second = node.daemon("outlive");
    assert_eq!(second.readoption(), Readoption::AdoptsRunning);
    let adopted = second
        .start(&spec(&bash, "uid-1", &[]))
        .await
        .expect("the same spec is adopted, not refused");
    assert_eq!(adopted.container_id, id);
    assert_eq!(adopted.state, RunState::Running);
    assert_eq!(
        node.recorded_pid(&id),
        Some(pid),
        "no second process was started"
    );
    assert_eq!(start_time(pid).as_deref(), Some(born.as_str()));

    let before = log_of(&second, &id).await.matches("tick").count();
    until("the adopted workload's output reaches the log", || {
        std::fs::read_to_string(node.logs.join(&id))
            .is_ok_and(|l| l.matches("tick").count() > before + 3)
    })
    .await;
    assert_eq!(
        second.status(&id).await.unwrap().map(|s| s.state),
        Some(RunState::Running)
    );

    second.stop(&id).await.expect("stop");
    until("the adopted workload is stopped", || {
        start_time(pid).is_none()
    })
    .await;
    loop {
        match second.remove(&id).await {
            Ok(()) => break,
            Err(KubeletError::NotReaped { .. }) => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(e) => panic!("remove: {e}"),
        }
    }
    assert_eq!(
        node.recorded_pid(&id),
        None,
        "a removed workload leaves no record"
    );
}

#[tokio::test]
async fn a_changed_spec_replaces_the_adopted_workload_and_an_unbound_one_is_stopped() {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let first = node.daemon("outlive");
    let id = first
        .start(&spec(&bash, "uid-1", &[]))
        .await
        .expect("start")
        .container_id;
    let old = node.recorded_pid(&id).expect("recorded");
    drop(first);

    let second = node.daemon("outlive");
    let changed = spec(&bash, "uid-1", &[("MODE", "new")]);
    match second.start(&changed).await {
        Err(KubeletError::NotReaped { .. }) => {}
        other => panic!("a changed spec must first stop the old process: {other:?}"),
    }
    until("the old process is stopped", || start_time(old).is_none()).await;
    let replaced = loop {
        match second.start(&changed).await {
            Ok(status) => break status,
            Err(KubeletError::NotReaped { .. }) => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(e) => panic!("start: {e}"),
        }
    };
    assert_eq!(replaced.state, RunState::Running);
    let new = node.recorded_pid(&id).expect("the replacement is recorded");
    assert_ne!(new, old);
    drop(second);

    let third = node.daemon("outlive");
    third
        .release_unclaimed(&BTreeSet::new())
        .await
        .expect("release");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        start_time(new).is_some(),
        "an empty bound set is no evidence that a pod is gone"
    );
    third
        .release_unclaimed(&BTreeSet::from(["uid-other".to_string()]))
        .await
        .expect("release");
    until("a workload no bound pod claims is stopped", || {
        start_time(new).is_none()
    })
    .await;
}
