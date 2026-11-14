//! A native workload outlives the backend that started it, and the next
//! backend adopts the same process: same pid, same start time, its output
//! still reaching the log. Only a changed spec or a pod nobody binds stops
//! it. Linux only, against a real closure realised with `nix build`.

#![cfg(target_os = "linux")]
#![allow(
    clippy::disallowed_methods,
    reason = "drives the native runtime directly; no kubelet in the loop"
)]

use engenho_kubelet::backend::Resources;
use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, LogOptions, PodIdentity};
use engenho_kubelet::cgroup::Cgroups;
use engenho_kubelet::cri::RunState;
use engenho_kubelet::kakoi::NativeSecurity;
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use engenho_kubelet::native_survival::WorkloadLifetime;
use engenho_kubelet::{KubeletError, Readoption};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "support/delegated.rs"]
mod delegated;

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
        self.daemon_under(lifetime, NativeSecurity::Enforce)
    }

    fn daemon_under(&self, lifetime: &str, security: NativeSecurity) -> NativeBackend {
        let lifetime = match lifetime {
            "outlive" => WorkloadLifetime::Outlive {
                state: self.state.clone(),
            },
            _ => WorkloadLifetime::Bound,
        };
        NativeBackend::new(Isolation::HostProcess, &self.logs, Cgroups::Off)
            .with_security(security)
            .with_lifetime(lifetime)
            .expect("the state directory is writable")
    }

    fn record(&self, id: &str) -> Option<serde_json::Value> {
        let bytes = std::fs::read(self.state.join([id, ".json"].concat())).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn on_cgroups(&self, root: &Path) -> NativeBackend {
        NativeBackend::new(
            Isolation::HostProcess,
            &self.logs,
            Cgroups::adopt(Box::new(delegated::DirectoryCgroupfs::at(root))),
        )
        .with_security(NativeSecurity::Enforce)
        .with_lifetime(WorkloadLifetime::Outlive {
            state: self.state.clone(),
        })
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

fn is_root() -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").expect("procfs").uid() == 0
}

fn status_field(pid: u32, field: &str) -> String {
    let status = std::fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("status"))
        .expect("the workload's status");
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
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
        .release_unclaimed(&BTreeSet::from(["uid-1".to_string()]))
        .await
        .expect("release");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        start_time(new).is_some(),
        "a workload whose pod may still claim it keeps running"
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

#[tokio::test]
async fn a_node_that_now_encloses_a_pod_differently_replaces_what_it_adopted_and_one_that_cannot_claims_it()
 {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let first = node.daemon_under("outlive", NativeSecurity::Off);
    let ticker = spec(&bash, "uid-1", &[]);
    let id = first.start(&ticker).await.expect("start").container_id;
    let old = node.recorded_pid(&id).expect("recorded");
    assert_eq!(status_field(old, "NoNewPrivs:"), "0", "off applies nothing");
    drop(first);

    let second = node.daemon_under("outlive", NativeSecurity::Enforce);
    if !engenho_kubelet::kakoi::can_enforce() {
        let claimed = second
            .start(&ticker)
            .await
            .expect("a node that cannot enforce encloses as off does, so the workload is claimed");
        assert_eq!(claimed.state, RunState::Running);
        assert_eq!(node.recorded_pid(&id), Some(old), "claimed, not replaced");
        assert_eq!(status_field(old, "NoNewPrivs:"), "0");
        second.stop(&id).await.expect("stop");
        return;
    }
    match second.start(&ticker).await {
        Err(KubeletError::NotReaped { .. }) => {}
        other => panic!("the same spec under a new verdict must not be claimed: {other:?}"),
    }
    until("the old process is stopped", || start_time(old).is_none()).await;
    let replaced = loop {
        match second.start(&ticker).await {
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
    assert_eq!(status_field(new, "NoNewPrivs:"), "1", "enforce applies it");
    second.stop(&id).await.expect("stop");
    until("the replacement stops", || start_time(new).is_none()).await;
}

#[tokio::test]
async fn an_adopted_workload_kakoi_now_refuses_is_stopped_not_left_running() {
    if !is_root() {
        return;
    }
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let mut hosted = spec(&bash, "uid-1", &[]);
    hosted.confinement.host_pid = true;
    let first = node.daemon_under("outlive", NativeSecurity::Off);
    let id = first.start(&hosted).await.expect("start").container_id;
    let pid = node.recorded_pid(&id).expect("recorded");
    drop(first);

    let second = node.daemon_under("outlive", NativeSecurity::Enforce);
    let why = second
        .start(&hosted)
        .await
        .expect_err("enforce refuses hostPID with no grant")
        .to_string();
    assert!(why.contains("hostPID"), "{why}");
    until("the refused workload is stopped", || {
        start_time(pid).is_none()
    })
    .await;
    second.remove(&id).await.ok();
}

#[tokio::test]
async fn a_command_run_into_an_adopted_workload_joins_its_enclosure() {
    if !is_root() {
        return;
    }
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let ticker = spec(&bash, "uid-1", &[("MARK", "from-the-pod-spec")]);
    let first = node.daemon("outlive");
    let id = first.start(&ticker).await.expect("start").container_id;
    let pid = node.recorded_pid(&id).expect("recorded");
    let namespace = |pid: &str| {
        std::fs::read_link(format!("/proc/{pid}/ns/mnt"))
            .expect("the mount namespace")
            .to_string_lossy()
            .into_owned()
    };
    let enclosure = namespace(&pid.to_string());
    assert_ne!(enclosure, namespace("self"), "the workload is enclosed");
    drop(first);

    let second = node.daemon("outlive");
    second.start(&ticker).await.expect("claimed");
    assert_eq!(
        status_field(pid, "Uid:").split_whitespace().next(),
        Some("65532")
    );
    assert_eq!(status_field(pid, "NoNewPrivs:"), "1");
    assert_eq!(status_field(pid, "Seccomp:"), "2");

    let out = second
        .exec(
            &id,
            &[
                "bash".to_string(),
                "-c".to_string(),
                "echo \"$MARK\"; readlink /proc/self/ns/mnt; cat /proc/self/status".to_string(),
            ],
        )
        .await
        .expect("an exec reaches the adopted workload");
    assert_eq!(out.exit_code, 0, "{out:?}");
    let mut lines = out.stdout.lines();
    assert_eq!(lines.next(), Some("from-the-pod-spec"));
    assert_eq!(lines.next(), Some(enclosure.as_str()));
    let status = |field: &str| {
        out.stdout
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .map(|v| v.trim().to_string())
            .unwrap_or_default()
    };
    assert_eq!(status("Uid:").split_whitespace().next(), Some("65532"));
    assert_eq!(status("NoNewPrivs:"), "1");
    assert_eq!(status("Seccomp:"), "2");

    second.stop(&id).await.expect("stop");
    until("the workload stops", || start_time(pid).is_none()).await;
}

fn running_with(marker: &str) -> bool {
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| start_time(*pid).is_some())
        .any(|pid| {
            std::fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
                .is_ok_and(|cmdline| String::from_utf8_lossy(&cmdline).contains(marker))
        })
}

#[tokio::test]
async fn an_occupied_leaf_no_record_names_is_killed_and_an_adopted_or_empty_one_is_not() {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let root = tempfile::tempdir().expect("tempdir");
    let unit = delegated::delegated_unit(root.path());
    let first = node.on_cgroups(root.path());
    let mut ticker = spec(&bash, "uid-1", &[]);
    ticker.resources = Resources::from_container_json(&serde_json::json!({
        "resources": { "limits": { "memory": "64Mi" } }
    }));
    let id = first.start(&ticker).await.expect("start").container_id;
    let workloads = unit.join("workloads");
    let adopted = workloads.join(&id);
    until("the workload joins its leaf", || {
        std::fs::read_to_string(adopted.join("cgroup.procs")).is_ok_and(|p| !p.trim().is_empty())
    })
    .await;
    let pid = node.recorded_pid(&id).expect("recorded");
    std::fs::write(adopted.join("cgroup.procs"), pid.to_string()).expect("the kernel lists it");
    drop(first);
    let stray = workloads.join("stray");
    std::fs::create_dir_all(&stray).expect("leaf");
    std::fs::write(stray.join("cgroup.procs"), std::process::id().to_string()).expect("procs");
    let vacant = workloads.join("vacant");
    std::fs::create_dir_all(vacant.join("nested")).expect("a child cgroup keeps it");
    std::fs::write(vacant.join("cgroup.procs"), "").expect("procs");

    let second = node.on_cgroups(root.path());
    let killed = |leaf: &Path| std::fs::read_to_string(leaf.join("cgroup.kill")).ok();
    assert!(
        adopted.is_dir() && stray.is_dir() && vacant.is_dir(),
        "an occupied leaf survives the sweep"
    );
    assert_eq!(killed(&stray).as_deref(), Some("1"));
    assert_eq!(killed(&vacant), None);
    assert_eq!(
        killed(&adopted),
        None,
        "an adopted workload's leaf is its record's"
    );
    second.stop(&id).await.expect("stop");
}

#[tokio::test]
async fn a_workload_with_stdin_or_a_terminal_is_recorded_unadoptable_and_ends_with_its_daemon() {
    let bash = closure("nixpkgs#bash^out");
    for (stdin, tty) in [(true, false), (false, true), (false, false)] {
        let node = Node::new();
        let daemon = node.daemon("outlive");
        let mut s = spec(&bash, "uid-1", &[]);
        s.stdio.stdin = stdin;
        s.stdio.tty = tty;
        let id = daemon.start(&s).await.expect("start").container_id;
        let record = node.record(&id).expect("recorded");
        let outlives = !stdin && !tty;
        assert_eq!(record["adoptable"], outlives, "stdin={stdin} tty={tty}");
        let pid = node.recorded_pid(&id).expect("pid");
        drop(daemon);
        if outlives {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(start_time(pid).is_some(), "it outlives its daemon");
            node.daemon("outlive").stop(&id).await.expect("stop");
        } else {
            until(
                "a workload that cannot outlive ends with its daemon",
                || start_time(pid).is_none(),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn a_dropped_daemon_stops_reading_what_its_workloads_print() {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let daemon = node.daemon("outlive");
    let id = daemon
        .start(&spec(&bash, "uid-1", &[]))
        .await
        .expect("start")
        .container_id;
    let log = node.logs.join(&id);
    let size = || std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
    until("the workload prints", || size() > 0).await;
    drop(daemon);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = size();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        size(),
        before,
        "no reader is left behind by the dropped daemon"
    );
    node.daemon("outlive").stop(&id).await.expect("stop");
}

#[tokio::test]
async fn a_workload_whose_record_cannot_be_kept_is_killed() {
    let bash = closure("nixpkgs#bash^out");
    let node = Node::new();
    let daemon = node.daemon("outlive");
    let marker = format!("unrecorded-{}", std::process::id());
    let mut s = spec(&bash, "uid-1", &[]);
    s.command = vec![
        "bash".to_string(),
        "-c".to_string(),
        format!("echo started; while sleep 0.1; do echo {marker}; done"),
    ];
    std::fs::create_dir_all(
        node.state
            .join("house_ticker-0_ticker.json")
            .join("occupied"),
    )
    .expect("block the record path");
    daemon
        .start(&s)
        .await
        .expect_err("a workload whose record cannot be kept does not start");
    until("the unrecorded workload is killed", || {
        !running_with(&marker)
    })
    .await;
}
