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

#![allow(
    clippy::disallowed_methods,
    reason = "drives the native runtime directly; no kubelet in the loop"
)]

use engenho_kubelet::backend::Resources;
use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, LogOptions, PodIdentity};
use engenho_kubelet::cgroup::{CgroupFs, Cgroups, HostFs};
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use engenho_substrate::HostRoot;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir, Cgroups::Off);

    let mut image = String::from("nix:");
    image.push_str(&coreutils);

    let started = backend
        .start(&spec(&image, &["echo", "hello from a closure"], &[]))
        .await
        .expect("a realised closure must start");
    assert_eq!(started.container_id, "default_native-probe_probe");
    assert!(
        started.is_running(),
        "a freshly spawned process must report running"
    );
    // ★ CORRECTED 2026-09-18. This asserted `pod_ip.is_none()`, reasoning that
    // "inventing a pod IP would be worse than reporting none". The incident
    // said otherwise: `probe.rs` mapped an http/tcp probe with no pod IP to
    // `ProbeObservation::Failure` unconditionally, so `None` did not mean
    // "no opinion" — it meant EVERY network probe failed forever. (Since T1.1
    // it is `Blind(NoTargetAddress)`, which no longer restarts the pod, but a
    // probe with nothing to dial still never passes, so this still matters.)
    //
    // Measured on ryn: pangea-operator's startupProbe (30 x 5s = 150s) could
    // never pass, so the kubelet killed a healthy operator every 2.5 minutes
    // (`restartCount: 14`, `ready: false`) while `curl 127.0.0.1:8080/healthz`
    // answered HTTP 200 in 0.4ms from the same host.
    //
    // The loopback is not invented, it is MEASURED: it is where the process is
    // bound and where a probe reaches it. Upstream agrees — a `hostNetwork`
    // pod takes `status.podIP = status.hostIP` and the prober dials that.
    assert_eq!(
        started.pod_ip.as_deref(),
        Some("127.0.0.1"),
        "a host process is reachable at the host's loopback; reporting no \
         address makes a healthy pod permanently unprobeable"
    );

    // Poll for exit rather than sleeping a guessed interval.
    let mut status = None;
    for _ in 0..100 {
        let s = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("the container is tracked");
        if !s.is_running() {
            status = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let status = status.expect("echo must exit well within two seconds");
    assert_eq!(
        status.state,
        RunState::Exited(ExitDisposition::Code(0)),
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

/// ★ T1.2, end to end through the real backend: a container ended by a
/// signal reports the SIGNAL. The backend used to report `code()`, which is
/// `None` for a killed process, and the kubelet read that absence as exit 0 —
/// a killed `restartPolicy: Never` pod published as `Succeeded`.
#[tokio::test]
async fn a_signalled_container_reports_its_signal_not_a_clean_exit() {
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-signal"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let started = backend
        .start(&spec(&image, &["sleep", "30"], &[]))
        .await
        .expect("start");
    assert!(started.is_running());

    // The backend's own stop: SIGTERM, which `sleep` does not handle.
    backend.stop(&started.container_id).await.expect("stop");
    let mut ended = None;
    for _ in 0..100 {
        let s = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("tracked");
        if !s.is_running() {
            ended = Some(s.state);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let ended = ended.expect("a SIGTERMed sleep must exit well within two seconds");
    assert_eq!(ended, RunState::Exited(ExitDisposition::Signal(15)));
    assert!(
        !ended.exit().is_some_and(ExitDisposition::is_success),
        "a signal death must never read as a success"
    );
    backend.remove(&started.container_id).await.expect("remove");
}

/// The environment a container gets is the one its spec DECLARES — nothing
/// inherited. An implicitly-inherited env is how a workload comes to depend on
/// something no manifest records, and it only shows up on a different host.
#[tokio::test]
async fn a_container_sees_only_its_declared_environment() {
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e-env");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir, Cgroups::Off);

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
        if !s.is_running() {
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
        Cgroups::Off,
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

/// Is `pid` still in the process table — running OR a zombie? `ps` lists
/// zombies too, so `false` means the process was reaped. Explicit columns:
/// macOS `ps` refuses its default format (the TIME column) under a sandbox.
fn in_process_table(pid: &str) -> bool {
    let out = std::process::Command::new("ps")
        .args(["-o", "pid=", "-p", pid])
        .output()
        .expect("run ps");
    out.status.success() && !String::from_utf8_lossy(&out.stdout).trim().is_empty()
}

/// The process group `pid` is in, as `ps` reports it.
fn pgid_of(pid: &str) -> String {
    let out = std::process::Command::new("ps")
        .args(["-o", "pgid=", "-p", pid])
        .output()
        .expect("run ps");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// ★ T2.10, end to end through the real backend: a workload that ignores
/// SIGTERM is SIGKILLed once the POD's grace period has passed, and reaped.
///
/// Before T2.10 `stop` sent SIGTERM and returned, and `remove` dropped the
/// record whatever the process was doing. This workload would have run on
/// with no record left to find it by — and a restart under the same id would
/// have started a second copy beside it.
///
/// Pinned along the way, each a behaviour the kubelet relies on:
///   * `stop` returns at once — the grace period is waited out in the
///     container's termination task, never in the kubelet's tick;
///   * while the process is inside its grace period it still reads as
///     running, `remove` refuses to drop it, and a second start under its id
///     is refused — one process per workload;
///   * the workload stays in the daemon's process group (no
///     `process_group(0)`): launchd killing the job's group is what keeps a
///     daemon restart from leaving a second copy running (plan edge 12).
#[tokio::test]
async fn a_workload_ignoring_sigterm_is_sigkilled_after_the_pods_grace_and_reaped() {
    use engenho_kubelet::KubeletError;
    use engenho_kubelet::backend::TerminationGrace;
    use std::time::{Duration, Instant};

    // `^out`: bash has several outputs and the first one printed is its man
    // pages, which hold no `bin/bash`.
    let bash = closure("nixpkgs#bash^out");
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-grace"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&bash);
    // `exec` keeps the pid and the ignored SIGTERM: the process the backend
    // signals IS the one that ignores it, with no child of its own.
    let script = format!("trap '' TERM; echo $$; echo ready; exec {coreutils}/bin/sleep 30");
    let mut s = spec(&image, &["bash", "-c", &script], &[]);
    s.termination_grace = TerminationGrace::from_seconds(2);
    let grace = s.termination_grace.duration();

    let started = backend.start(&s).await.expect("start");
    let id = started.container_id.clone();

    // Signal only once the trap is set.
    let mut log = String::new();
    for _ in 0..250 {
        log = backend
            .logs(&id, &LogOptions::default())
            .await
            .expect("logs");
        if log.contains("ready") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        log.contains("ready"),
        "the workload never got ready: {log:?}"
    );
    let pid = log.lines().next().expect("the pid line").trim().to_string();
    assert!(in_process_table(&pid), "control: the workload is running");
    assert_eq!(
        pgid_of(&pid),
        pgid_of(&std::process::id().to_string()),
        "a native workload must stay in the daemon's process group"
    );

    let stopped_at = Instant::now();
    backend.stop(&id).await.expect("stop");
    assert!(
        stopped_at.elapsed() < Duration::from_millis(500),
        "stop must hand the grace period to a task, not wait it out inline: {:?}",
        stopped_at.elapsed()
    );

    // Inside the grace period: SIGTERM was ignored, so it is still up.
    assert!(
        backend
            .status(&id)
            .await
            .expect("status")
            .expect("tracked")
            .is_running(),
        "a process inside its grace period is still running"
    );
    match backend.remove(&id).await {
        Err(KubeletError::NotReaped { container_id }) => assert_eq!(container_id, id),
        other => panic!("remove must refuse an unreaped process, got {other:?}"),
    }
    match backend.start(&s).await {
        Err(KubeletError::NotReaped { .. }) => {}
        other => panic!("a second copy must not start beside an unreaped one, got {other:?}"),
    }

    let mut ended = None;
    while stopped_at.elapsed() < grace + Duration::from_secs(5) {
        let st = backend.status(&id).await.expect("status").expect("tracked");
        if !st.is_running() {
            ended = Some((st.state, stopped_at.elapsed()));
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (state, took) = ended.expect("the workload must end within its grace period + 5s");
    assert_eq!(
        state,
        RunState::Exited(ExitDisposition::Signal(9)),
        "SIGTERM was ignored, so SIGKILL ended it"
    );
    assert!(
        took >= grace,
        "SIGKILL came before the pod's grace period ran out: {took:?} < {grace:?}"
    );
    assert!(
        !in_process_table(&pid),
        "the process must be reaped, not left a zombie"
    );

    backend
        .remove(&id)
        .await
        .expect("a reaped process's record may go");
    assert!(backend.status(&id).await.expect("status").is_none());
}

#[derive(Debug)]
struct DirectoryCgroupfs {
    host: HostFs,
    root: HostRoot,
}

impl DirectoryCgroupfs {
    fn at(root: &Path) -> Self {
        Self {
            host: HostFs::at(HostRoot::at(root)),
            root: HostRoot::at(root),
        }
    }
}

impl CgroupFs for DirectoryCgroupfs {
    fn read(&self, path: &Path) -> std::io::Result<String> {
        self.host.read(path)
    }
    fn write(&self, path: &Path, value: &str) -> std::io::Result<()> {
        self.host.write(path, value)
    }
    fn create_dir(&self, path: &Path) -> std::io::Result<()> {
        self.host.create_dir(path)
    }
    fn remove_dir(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_dir_all(self.root.resolve(path))
    }
    fn subdirs(&self, path: &Path) -> std::io::Result<Vec<String>> {
        self.host.subdirs(path)
    }
    fn writable(&self, path: &Path) -> bool {
        self.host.writable(path)
    }
    fn delegation_marked(&self, path: &Path) -> std::io::Result<bool> {
        self.host.delegation_marked(path)
    }
    fn open_procs(&self, path: &Path) -> std::io::Result<std::fs::File> {
        self.host.open_procs(path)
    }
}

fn delegated_unit(root: &Path) -> PathBuf {
    let unit = root.join("sys/fs/cgroup/test.slice/engenho.service");
    std::fs::create_dir_all(&unit).unwrap();
    std::fs::create_dir_all(root.join("proc/self")).unwrap();
    std::fs::write(
        root.join("proc/self/cgroup"),
        "0::/test.slice/engenho.service\n",
    )
    .unwrap();
    std::fs::write(unit.join("cgroup.controllers"), "cpu io memory pids").unwrap();
    std::fs::write(unit.join("cgroup.subtree_control"), "").unwrap();
    std::fs::write(unit.join("cgroup.procs"), std::process::id().to_string()).unwrap();
    rustix::fs::setxattr(
        unit.as_path(),
        "user.delegate",
        b"1",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    unit
}

#[tokio::test]
async fn a_limited_container_joins_its_leaf_before_it_runs_and_the_leaf_goes_when_it_is_reaped() {
    let coreutils = closure("nixpkgs#coreutils");
    let root = tempfile::tempdir().unwrap();
    let unit = delegated_unit(root.path());
    let cgroups = Cgroups::adopt(Box::new(DirectoryCgroupfs::at(root.path())));
    assert!(matches!(cgroups, Cgroups::Delegated(_)), "{cgroups:?}");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-cgroup"),
        cgroups,
    );
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let mut s = spec(&image, &["sleep", "2"], &[]);
    s.resources = Resources::from_container_json(&serde_json::json!({
        "resources": { "limits": { "memory": "64Mi", "cpu": "250m" } }
    }));

    let started = backend.start(&s).await.expect("a delegated node starts it");
    let leaf = unit.join("workloads").join(&started.container_id);
    let at = |knob: &str| std::fs::read_to_string(leaf.join(knob)).unwrap();
    assert_eq!(at("memory.max"), "67108864");
    assert_eq!(at("memory.swap.max"), "0");
    assert_eq!(at("memory.oom.group"), "1");
    assert_eq!(at("cpu.max"), "25000 100000");
    let mut joined = String::new();
    for _ in 0..100 {
        joined = at("cgroup.procs");
        if !joined.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(joined, "0", "the child joins its leaf itself, before exec");

    for _ in 0..250 {
        let st = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("tracked");
        if !st.is_running() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!leaf.exists(), "a reaped container's leaf is released");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_pods_run_as_user_is_the_workloads_uid_on_linux() {
    use std::os::unix::fs::MetadataExt;
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-runas"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let mut s = spec(&image, &["id", "-u"], &[]);
    s.confinement.run_as_user = Some(65_534);
    let root = std::fs::metadata("/proc/self").expect("procfs").uid() == 0;
    match backend.start(&s).await {
        Ok(started) if root => {
            for _ in 0..250 {
                let st = backend
                    .status(&started.container_id)
                    .await
                    .expect("status")
                    .expect("tracked");
                if !st.is_running() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            let out = backend
                .logs(&started.container_id, &LogOptions::default())
                .await
                .expect("logs");
            assert_eq!(out.trim(), "65534");
        }
        Err(e) if !root => {
            assert!(
                e.to_string().contains("cannot spawn"),
                "an unprivileged daemon cannot become another uid, so the pod is refused: {e}"
            );
        }
        other => panic!("root={root}: {other:?}"),
    }
}
