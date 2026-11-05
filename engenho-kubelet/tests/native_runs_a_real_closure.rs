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
    let mut s = spec(&image, &["sh", "-c", &script], &[]);
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
async fn an_enclosed_pod_that_declares_nothing_gets_its_default_bounds_in_its_leaf() {
    if !is_root() {
        return;
    }
    let coreutils = closure("nixpkgs#coreutils");
    let root = tempfile::tempdir().unwrap();
    let unit = delegated_unit(root.path());
    let cgroups = Cgroups::adopt(Box::new(DirectoryCgroupfs::at(root.path())));
    assert!(matches!(cgroups, Cgroups::Delegated(_)), "{cgroups:?}");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-default-bounds"),
        cgroups,
    );
    let mut s = spec(&format!("nix:{coreutils}"), &["sleep", "2"], &[]);
    s.pod.namespace = "kakoi-default-bounds".to_string();
    let started = backend.start(&s).await.expect("start");
    let leaf = unit.join("workloads").join(&started.container_id);
    let at = |knob: &str| std::fs::read_to_string(leaf.join(knob)).unwrap();
    assert_eq!(at("pids.max"), "4096");
    let memory: u64 = at("memory.max").parse().unwrap();
    assert!(memory >= 1 << 30, "{memory}");
    assert_eq!(at("memory.swap.max"), "0");
    backend.stop(&started.container_id).await.expect("stop");
}

#[cfg(target_os = "linux")]
async fn output_of(backend: &NativeBackend, s: &engenho_kubelet::backend::ContainerSpec) -> String {
    let started = backend.start(s).await.expect("start");
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
    backend
        .logs(&started.container_id, &LogOptions::default())
        .await
        .expect("logs")
}

#[cfg(target_os = "linux")]
fn status_field<'a>(text: &'a str, key: &str) -> &'a str {
    text.lines()
        .find_map(|l| l.split_once(key))
        .map_or("", |(_, v)| v.trim())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_pod_that_says_nothing_runs_unprivileged_as_the_kernel_reports_it() {
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata("/proc/self").expect("procfs").uid() != 0 {
        return;
    }
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-kakoi"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let s = spec(&image, &["cat", "/proc/self/status"], &[]);
    let out = output_of(&backend, &s).await;
    let uid: u32 = status_field(&out, "Uid:")
        .split_whitespace()
        .next()
        .and_then(|u| u.parse().ok())
        .expect("a uid");
    assert!(uid >= 100_000, "the namespace's own account: {out}");
    assert!(
        status_field(&out, "Gid:").starts_with(&uid.to_string()),
        "{out}"
    );
    assert_eq!(status_field(&out, "NoNewPrivs:"), "1", "{out}");
    assert_eq!(status_field(&out, "CapEff:"), "0000000000000000", "{out}");
    assert_eq!(status_field(&out, "CapPrm:"), "0000000000000000", "{out}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_enclosed_pod_cannot_write_the_host_or_see_its_secrets_and_keeps_its_volume() {
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata("/proc/self").expect("procfs").uid() != 0 {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let dir = tempfile::tempdir_in("/var/tmp").expect("volume dir");
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o777),
    )
    .expect("open the volume");
    let volume = dir.path().to_string_lossy().to_string();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-enclosure"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&tools);
    let script = format!(
        "echo x > {volume}/own && echo own-ok; \
         echo x > /tmp/scratch && echo tmp-ok; \
         (echo x > /etc/kakoi-escape) 2>/dev/null || echo etc-refused; \
         (echo x > /nix/kakoi-escape) 2>/dev/null || echo nix-refused; \
         {tools}/bin/ls / > /dev/null && echo ls-works; \
         {tools}/bin/ls /root; echo ls-root=$?; \
         test -e /proc/1/status && echo pid1-visible || echo pid1-hidden; \
         test -e /proc/$$/status && echo self-visible; \
         {tools}/bin/unshare -m {tools}/bin/true; echo unshare=$?; \
         {tools}/bin/mount -t tmpfs none /tmp; echo mount=$?; \
         test -x {tools}/bin/unshare && test -x {tools}/bin/mount && echo tools-present"
    );
    let mut s = spec(&image, &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-test".to_string();
    s.mounts = vec![engenho_kubelet::pod_volume::ResolvedMount {
        source: engenho_kubelet::pod_volume::MountSource::UserHostPath(dir.path().to_path_buf()),
        mount_path: volume.clone(),
        read_only: false,
        sub_path: None,
    }];
    let out = output_of(&backend, &s).await;
    assert!(!out.contains("pid1-visible"), "{out}");
    assert!(!out.contains("unshare=0"), "{out}");
    assert!(!out.contains("mount=0"), "{out}");
    assert!(!out.contains("ls-root=0"), "{out}");
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    for expected in [
        "own-ok",
        "tmp-ok",
        "etc-refused",
        "nix-refused",
        "ls-works",
        "pid1-hidden",
        "self-visible",
        "tools-present",
        "Operation not permitted",
    ] {
        assert!(out.contains(expected), "{expected} missing from: {out}");
    }
    assert!(std::fs::metadata(format!("{volume}/own")).is_ok(), "{out}");
    assert!(
        std::fs::metadata("/tmp/scratch").is_err(),
        "the pod's /tmp reached the host"
    );
    assert!(std::fs::metadata("/etc/kakoi-escape").is_err(), "{out}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_pods_access_reaches_the_kernel() {
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata("/proc/self").expect("procfs").uid() != 0 {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let open = tempfile::tempdir_in("/var/tmp").expect("volume");
    let shut = tempfile::tempdir_in("/var/tmp").expect("volume");
    for dir in [&open, &shut] {
        std::fs::set_permissions(
            dir.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o777),
        )
        .expect("open the volume");
    }
    let open_path = open.path().to_string_lossy().to_string();
    let shut_path = shut.path().to_string_lossy().to_string();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-access"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&tools);
    let script = format!(
        "echo x > {open_path}/f && echo writes-ok; \
         (echo x > {shut_path}/f) 2>/dev/null || echo narrowed; \
         n=0; while read -r line; do n=$((n+1)); done < /proc/net/dev; echo interface-lines=$n; \
         test -d /nix/store && echo nix-store-visible; \
         test -e /nix/var/nix/daemon-socket/socket && echo nix-socket || echo no-nix-socket; \
         (echo x > /dev/shm/kakoi-shm) && echo shm-ok"
    );
    let mut s = spec(&image, &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-access-test".to_string();
    s.confinement.access = Some(format!(
        r#"{{"effects":["filesystem"],"writes":["{open_path}"]}}"#
    ));
    s.mounts = [&open, &shut]
        .iter()
        .map(|dir| engenho_kubelet::pod_volume::ResolvedMount {
            source: engenho_kubelet::pod_volume::MountSource::UserHostPath(
                dir.path().to_path_buf(),
            ),
            mount_path: dir.path().to_string_lossy().to_string(),
            read_only: false,
            sub_path: None,
        })
        .collect();
    let out = output_of(&backend, &s).await;
    for expected in [
        "writes-ok",
        "narrowed",
        "interface-lines=3",
        "nix-store-visible",
        "no-nix-socket",
        "shm-ok",
    ] {
        assert!(out.contains(expected), "{expected} missing from: {out}");
    }
    assert!(
        std::fs::metadata(format!("{shut_path}/f")).is_err(),
        "{out}"
    );
    assert!(std::fs::metadata("/dev/shm/kakoi-shm").is_err(), "{out}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_command_run_in_an_enclosed_pod_enters_its_enclosure() {
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata("/proc/self").expect("procfs").uid() != 0 {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-enclosed-exec"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&tools);
    let mut s = spec(&image, &["sleep", "30"], &[("POD_ONLY", "declared")]);
    s.pod.namespace = "kakoi-exec-test".to_string();
    let started = backend.start(&s).await.expect("start");
    let run = |script: String| vec!["sh".to_string(), "-c".to_string(), script];
    unsafe { std::env::set_var("KAKOI_DAEMON_ONLY", "leaked") };
    let out = backend
        .exec(
            &started.container_id,
            &run(format!(
                "echo uid=$({tools}/bin/id -u); \
                 echo env=$POD_ONLY daemon=$KAKOI_DAEMON_ONLY; \
                 (echo x > /etc/kakoi-exec-escape) 2>/dev/null || echo etc-refused; \
                 {tools}/bin/ls / > /dev/null && echo ls-works; \
                 {tools}/bin/ls /root; echo ls-root=$?; \
                 test -e /proc/1/status && echo pid1-visible || echo pid1-hidden; \
                 {tools}/bin/unshare -m {tools}/bin/true; echo unshare=$?"
            )),
        )
        .await
        .expect("exec");
    let text = format!("{}{}", out.stdout, out.stderr);
    assert!(text.contains("env=declared daemon=\n"), "{text}");
    assert!(!text.contains("ls-root=0"), "{text}");
    assert!(!text.contains("unshare=0"), "{text}");
    assert!(!text.contains("not found"), "a tool never ran: {text}");
    assert!(text.contains("ls-works"), "{text}");
    let uid: u32 = text
        .lines()
        .find_map(|l| l.strip_prefix("uid="))
        .and_then(|u| u.parse().ok())
        .unwrap_or_else(|| panic!("no uid in: {text}"));
    assert!(
        uid >= 100_000,
        "the namespace's own account, not {uid}: {text}"
    );
    for expected in ["etc-refused", "pid1-hidden", "Operation not permitted"] {
        assert!(text.contains(expected), "{expected} missing from: {text}");
    }
    assert!(
        std::fs::metadata("/etc/kakoi-exec-escape").is_err(),
        "{text}"
    );
    backend.stop(&started.container_id).await.expect("stop");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn root_starts_only_with_a_grant_and_the_kernel_agrees() {
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata("/proc/self").expect("procfs").uid() != 0 {
        return;
    }
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-grant"),
        Cgroups::Off,
    );
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let mut s = spec(&image, &["cat", "/proc/self/status"], &[]);
    s.confinement.run_as_user = Some(0);
    let refused = backend.start(&s).await.expect_err("root with no grant");
    assert!(
        refused.to_string().contains("kakoi.pleme.io/root"),
        "{refused}"
    );
    s.confinement.root_grant = Some("this test".to_string());
    let out = output_of(&backend, &s).await;
    assert!(status_field(&out, "Uid:").starts_with('0'), "{out}");
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

#[cfg(target_os = "linux")]
fn is_root() -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").expect("procfs").uid() == 0
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_pods_host_fields_are_refused_by_name_unless_the_node_can_honour_them() {
    if !is_root() {
        return;
    }
    let coreutils = closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-host-fields"),
        Cgroups::Off,
    );
    let image = format!("nix:{coreutils}");
    let refused = |what: &'static str, set: &dyn Fn(&mut ContainerSpec)| {
        let mut s = spec(&image, &["cat", "/proc/self/status"], &[]);
        s.pod.namespace = "kakoi-host-fields".to_string();
        s.pod.name = what.replace(' ', "-").to_lowercase();
        set(&mut s);
        (what, s)
    };
    let cases = [
        refused("hostPID", &|s| s.confinement.host_pid = true),
        refused("hostIPC", &|s| s.confinement.host_ipc = true),
        refused("Unconfined", &|s| {
            s.confinement.seccomp_profile = Some("Unconfined".to_string());
        }),
        refused("Localhost", &|s| {
            s.confinement.seccomp_profile = Some("Localhost".to_string());
        }),
        refused("kernel.shm_rmid_forced", &|s| {
            s.confinement.sysctls = vec!["kernel.shm_rmid_forced".to_string()];
        }),
        refused("/dev/ttyUSB0", &|s| {
            s.confinement.devices = vec!["/dev/ttyUSB0".to_string()];
        }),
    ];
    for (name, s) in &cases {
        let why = backend.start(s).await.expect_err(name).to_string();
        assert!(why.contains(name), "{name}: {why}");
    }

    let mut granted = spec(&image, &["cat", "/proc/self/status"], &[]);
    granted.pod.name = "host-pid-granted".to_string();
    granted.confinement.host_pid = true;
    granted.confinement.root_grant = Some("this test".to_string());
    let out = output_of(&backend, &granted).await;
    assert!(status_field(&out, "NoNewPrivs:").starts_with('0'), "{out}");

    let mut default = spec(&image, &["cat", "/proc/self/status"], &[]);
    default.pod.namespace = "kakoi-host-fields".to_string();
    default.pod.name = "runtime-default".to_string();
    default.confinement.seccomp_profile = Some("RuntimeDefault".to_string());
    default.confinement.host_network = true;
    let out = output_of(&backend, &default).await;
    assert_eq!(status_field(&out, "NoNewPrivs:"), "1", "{out}");
    assert_eq!(status_field(&out, "Seccomp:"), "2", "{out}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_kept_capability_binds_a_low_port_and_stays_enclosed_and_filtered() {
    if !is_root() {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-keep"),
        Cgroups::Off,
    );
    let script = format!(
        "{tools}/bin/cat /proc/self/status; \
         {tools}/bin/timeout 1 {tools}/bin/nc -l -p 80 > /dev/null; echo bind=$?; \
         {tools}/bin/unshare -m {tools}/bin/true; echo unshare=$?; \
         (echo x > /etc/kakoi-keep-escape) 2>/dev/null || echo etc-refused"
    );
    let image = format!("nix:{tools}");
    let run = |pod: &str, cap_add: Vec<String>| {
        let mut s = spec(&image, &["sh", "-c", &script], &[]);
        s.pod.namespace = "kakoi-keep".to_string();
        s.pod.name = pod.to_string();
        s.confinement.access = Some(r#"{"effects":["clock"]}"#.to_string());
        s.confinement.cap_add = cap_add;
        s.confinement.cap_drop = vec!["ALL".to_string()];
        s
    };
    let kept = output_of(&backend, &run("kept", vec!["NET_BIND_SERVICE".to_string()])).await;
    assert!(!kept.contains("not found"), "a tool never ran: {kept}");
    let bit = "0000000000000400";
    assert_eq!(status_field(&kept, "CapEff:"), bit, "{kept}");
    assert_eq!(status_field(&kept, "CapPrm:"), bit, "{kept}");
    assert_eq!(status_field(&kept, "CapAmb:"), bit, "{kept}");
    assert_eq!(status_field(&kept, "NoNewPrivs:"), "1", "{kept}");
    assert_eq!(status_field(&kept, "Seccomp:"), "2", "{kept}");
    assert!(
        status_field(&kept, "Uid:")
            .split_whitespace()
            .next()
            .and_then(|u| u.parse::<u32>().ok())
            .is_some_and(|uid| uid >= 100_000),
        "{kept}"
    );
    assert!(!kept.contains("Permission denied"), "it bound 80: {kept}");
    assert!(kept.contains("Operation not permitted"), "{kept}");
    assert!(kept.contains("etc-refused"), "{kept}");

    let bare = output_of(&backend, &run("bare", Vec::new())).await;
    assert!(!bare.contains("not found"), "a tool never ran: {bare}");
    assert_eq!(status_field(&bare, "CapEff:"), "0000000000000000", "{bare}");
    assert!(bare.contains("Permission denied"), "{bare}");

    let mut raw = run("raw", vec!["NET_RAW".to_string()]);
    raw.pod.name = "raw".to_string();
    let why = backend
        .start(&raw)
        .await
        .expect_err("NET_RAW needs a grant");
    assert!(why.to_string().contains("kakoi.pleme.io/root"), "{why}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_host_services_socket_directory_is_hidden_from_an_enclosed_pod() {
    if !is_root() {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-run"),
        Cgroups::Off,
    );
    let script = format!(
        "{tools}/bin/ls /run/systemd/private; echo systemd=$?; \
         {tools}/bin/ls /run/current-system > /dev/null; echo current=$?"
    );
    let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-run".to_string();
    let out = output_of(&backend, &s).await;
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    assert!(out.contains("Permission denied"), "{out}");
    assert!(!out.contains("systemd=0"), "{out}");
    assert!(out.contains("current=0"), "{out}");
}

#[cfg(target_os = "linux")]
fn world_readable_dir(root: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir_in(root).expect("a directory");
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .expect("open it");
    dir
}

#[cfg(target_os = "linux")]
fn mount_of(path: &Path, read_only: bool) -> engenho_kubelet::pod_volume::ResolvedMount {
    engenho_kubelet::pod_volume::ResolvedMount {
        source: engenho_kubelet::pod_volume::MountSource::UserHostPath(path.to_path_buf()),
        mount_path: path.to_string_lossy().to_string(),
        read_only,
        sub_path: None,
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn landlock_holds_a_pods_declared_reads_where_the_account_could_otherwise_read() {
    if !is_root() {
        return;
    }
    assert!(
        engenho_kubelet::kakoi::landlock_abi() >= 1,
        "this kernel test needs Landlock"
    );
    let tools = closure("nixpkgs#busybox");
    let volume = world_readable_dir("/var/tmp");
    let path = volume.path().to_string_lossy().to_string();
    for sub in ["in", "out"] {
        std::fs::create_dir(volume.path().join(sub)).unwrap();
        std::fs::set_permissions(
            volume.path().join(sub),
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();
        std::fs::write(volume.path().join(sub).join("f"), "kakoi-readable\n").unwrap();
    }
    let hidden = world_readable_dir("/var/lib");
    let hidden_path = hidden.path().to_string_lossy().to_string();
    std::fs::write(hidden.path().join("f"), "kakoi-declared\n").unwrap();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-landlock-reads"),
        Cgroups::Off,
    );
    let script = format!(
        "{tools}/bin/cat {path}/in/f; echo in=$?; \
         {tools}/bin/cat {path}/out/f; echo out=$?; \
         {tools}/bin/cat {hidden_path}/f; echo declared=$?; \
         {tools}/bin/ls /var/lib; echo varlib=$?; \
         {tools}/bin/cat /etc/machine-id; echo machineid=$?; \
         {tools}/bin/cat /etc/passwd > /dev/null; echo passwd=$?; \
         {tools}/bin/ls /; echo root=$?"
    );
    let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-landlock-reads".to_string();
    s.confinement.access = Some(format!(
        r#"{{"effects":["filesystem"],"reads":["{path}/in","{hidden_path}"]}}"#
    ));
    s.mounts = vec![mount_of(volume.path(), true)];
    let out = output_of(&backend, &s).await;
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    for expected in ["in=0", "declared=0", "passwd=0", "kakoi-declared"] {
        assert!(out.contains(expected), "{expected} missing from: {out}");
    }
    for refused in ["out=0", "varlib=0", "machineid=0", "root=0"] {
        assert!(!out.contains(refused), "{refused} in: {out}");
    }
    assert!(
        out.contains(&format!("{path}/out/f': Permission denied")),
        "the kernel's own refusal: {out}"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_access_without_the_process_effect_runs_nothing_it_wrote_and_a_plain_pod_still_can() {
    if !is_root() {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-landlock-exec"),
        Cgroups::Off,
    );
    let run = |pod: &str, volume: &Path, access: Option<String>| {
        let path = volume.to_string_lossy();
        let script = format!(
            "{tools}/bin/cp {tools}/bin/busybox {path}/busybox; echo copied=$?; \
             {path}/busybox true; echo ran=$?; \
             {tools}/bin/true; echo store=$?"
        );
        let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
        s.pod.namespace = "kakoi-landlock-exec".to_string();
        s.pod.name = pod.to_string();
        s.confinement.access = access;
        s.mounts = vec![mount_of(volume, false)];
        s
    };
    let open = |dir: &tempfile::TempDir| {
        std::fs::set_permissions(
            dir.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o777),
        )
        .unwrap();
    };
    let plain_dir = world_readable_dir("/var/tmp");
    open(&plain_dir);
    let plain = output_of(&backend, &run("plain", plain_dir.path(), None)).await;
    assert!(!plain.contains("not found"), "a tool never ran: {plain}");
    for expected in ["copied=0", "ran=0", "store=0"] {
        assert!(plain.contains(expected), "{expected} missing: {plain}");
    }
    let shut_dir = world_readable_dir("/var/tmp");
    open(&shut_dir);
    let path = shut_dir.path().to_string_lossy().to_string();
    let shut = output_of(
        &backend,
        &run(
            "shut",
            shut_dir.path(),
            Some(format!(
                r#"{{"effects":["filesystem"],"writes":["{path}"]}}"#
            )),
        ),
    )
    .await;
    assert!(!shut.contains("not found"), "a tool never ran: {shut}");
    for expected in ["copied=0", "store=0", "Permission denied"] {
        assert!(shut.contains(expected), "{expected} missing: {shut}");
    }
    assert!(!shut.contains("ran=0"), "{shut}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn landlock_cuts_a_pod_off_from_the_hosts_abstract_sockets_and_keeps_its_own() {
    use std::os::linux::net::SocketAddrExt;
    if !is_root() {
        return;
    }
    let abi = engenho_kubelet::kakoi::landlock_abi();
    assert!(
        abi >= 6,
        "this kernel test needs Landlock ABI 6, found {abi}"
    );
    let tools = closure("nixpkgs#busybox");
    let socat = closure("nixpkgs#socat");
    let name = format!("kakoi-host-{}", std::process::id());
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
    let listener = std::os::unix::net::UnixListener::bind_addr(&addr).unwrap();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-landlock-abstract"),
        Cgroups::Off,
    );
    let script = format!(
        "{socat}/bin/socat OPEN:/dev/null ABSTRACT-CONNECT:{name}; echo host=$?; \
         {socat}/bin/socat ABSTRACT-LISTEN:{name}-own OPEN:/dev/null & \
         {tools}/bin/sleep 0.5; \
         {socat}/bin/socat OPEN:/dev/null ABSTRACT-CONNECT:{name}-own; echo own=$?"
    );
    let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-landlock-abstract".to_string();
    let out = output_of(&backend, &s).await;
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    assert!(out.contains("Operation not permitted"), "{out}");
    assert!(!out.contains("host=0"), "{out}");
    assert!(out.contains("own=0"), "{out}");

    let mut granted = spec(
        &format!("nix:{socat}"),
        &[
            &format!("{socat}/bin/socat"),
            "OPEN:/dev/null",
            &format!("ABSTRACT-CONNECT:{name}"),
        ],
        &[],
    );
    granted.pod.name = "granted".to_string();
    granted.confinement.root_grant = Some("this test".to_string());
    let out = output_of(&backend, &granted).await;
    assert!(!out.contains("Operation not permitted"), "{out}");
    drop(listener);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_pathname_socket_is_held_only_where_the_kernel_can_and_a_weaker_kernel_says_so() {
    if !is_root() {
        return;
    }
    let abi = engenho_kubelet::kakoi::landlock_abi();
    let tools = closure("nixpkgs#busybox");
    let socat = closure("nixpkgs#socat");
    let dir = world_readable_dir("/run");
    let socket = dir.path().join("host.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o777)).unwrap();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-landlock-pathname"),
        Cgroups::Off,
    );
    let path = socket.to_string_lossy();
    let script = format!("{socat}/bin/socat OPEN:/dev/null UNIX-CONNECT:{path}; echo connect=$?");
    let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-landlock-pathname".to_string();
    let warnings = backend.confinement_warnings(&s).await;
    let out = output_of(&backend, &s).await;
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    if abi >= 9 {
        assert!(out.contains("Permission denied"), "{out}");
        assert!(!out.contains("connect=0"), "{out}");
        assert!(
            !warnings.iter().any(|w| w.contains("ABI 9")),
            "{warnings:?}"
        );
    } else {
        assert!(out.contains("connect=0"), "the weaker state is true: {out}");
        assert!(
            warnings.iter().any(|w| w.contains("ABI 9")),
            "and it is said: {warnings:?}"
        );
    }
    drop(listener);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn two_pods_with_one_declared_uid_share_it_and_cannot_signal_each_other() {
    if !is_root() {
        return;
    }
    let abi = engenho_kubelet::kakoi::landlock_abi();
    assert!(
        abi >= 6,
        "this kernel test needs Landlock ABI 6, found {abi}"
    );
    let tools = closure("nixpkgs#busybox");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-landlock-signal"),
        Cgroups::Off,
    );
    let image = format!("nix:{tools}");
    let pod = |name: &str, script: &str| {
        let mut s = spec(&image, &["sh", "-c", script], &[]);
        s.pod.namespace = "kakoi-landlock-signal".to_string();
        s.pod.name = name.to_string();
        s.confinement.run_as_user = Some(5555);
        s
    };
    let first = backend
        .start(&pod(
            "first",
            &format!("echo $$; exec {tools}/bin/sleep 30"),
        ))
        .await
        .expect("start");
    let mut pid = String::new();
    for _ in 0..250 {
        pid = backend
            .logs(&first.container_id, &LogOptions::default())
            .await
            .expect("logs")
            .trim()
            .to_string();
        if !pid.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!pid.is_empty(), "the first pod never said its pid");
    let out = output_of(
        &backend,
        &pod(
            "second",
            &format!(
                "echo uid=$({tools}/bin/id -u); \
                 kill -0 {pid}; echo other=$?; \
                 {tools}/bin/sleep 5 & kill -0 $!; echo own=$?; kill $!"
            ),
        ),
    )
    .await;
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    assert!(out.contains("uid=5555"), "{out}");
    assert!(out.contains("Operation not permitted"), "{out}");
    assert!(!out.contains("other=0"), "{out}");
    assert!(out.contains("own=0"), "{out}");
    let inside = backend
        .exec(
            &first.container_id,
            &[
                "sh".to_string(),
                "-c".to_string(),
                format!("kill -0 {pid}; echo main=$?"),
            ],
        )
        .await
        .expect("exec");
    let text = format!("{}{}", inside.stdout, inside.stderr);
    assert!(
        text.contains("main=0"),
        "an exec reaches its own pod: {text}"
    );
    backend.stop(&first.container_id).await.expect("stop");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn postgres_initialises_serves_and_answers_inside_a_full_enclosure() {
    if !is_root() {
        return;
    }
    let tools = closure("nixpkgs#busybox");
    let pg = closure("nixpkgs#postgresql_16^out");
    let volume = world_readable_dir("/var/tmp");
    std::fs::set_permissions(
        volume.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o777),
    )
    .unwrap();
    let path = volume.path().to_string_lossy().to_string();
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-native-e2e-postgres-enclosed"),
        Cgroups::Off,
    );
    let script = format!(
        "{pg}/bin/initdb -D {path}/d -U kakoi --locale=C -A trust > /dev/null; echo initdb=$?; \
         {pg}/bin/postgres -D {path}/d -k /tmp -c listen_addresses='' > /dev/null 2>&1 & \
         n=0; until {pg}/bin/pg_isready -h /tmp -q; do \
           n=$((n+1)); test $n -gt 100 && break; {tools}/bin/sleep 0.1; \
         done; \
         {pg}/bin/psql -h /tmp -U kakoi -d postgres -tAc 'select 40 + 2'; echo psql=$?; \
         kill $!; wait $!; echo stopped=$?"
    );
    let mut s = spec(&format!("nix:{tools}"), &["sh", "-c", &script], &[]);
    s.pod.namespace = "kakoi-postgres".to_string();
    s.confinement.run_as_user = Some(65_534);
    s.mounts = vec![mount_of(volume.path(), false)];
    let started = backend.start(&s).await.expect("start");
    let mut out = String::new();
    for _ in 0..1500 {
        let st = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("tracked");
        if !st.is_running() {
            out = backend
                .logs(&started.container_id, &LogOptions::default())
                .await
                .expect("logs");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!out.contains("not found"), "a tool never ran: {out}");
    for expected in ["initdb=0", "42", "psql=0"] {
        assert!(out.contains(expected), "{expected} missing from: {out}");
    }
}

fn streaming_spec(image: &str, pod: &str, command: &[&str]) -> ContainerSpec {
    let mut s = spec(image, command, &[]);
    s.pod.name = pod.to_string();
    s
}

async fn drain(mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>) -> String {
    let mut out = Vec::new();
    while let Some(c) = rx.recv().await {
        out.extend(c);
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// ★ `kubectl exec -i` against a native container: the command resolves in
/// the container's closure, stdin reaches it while it runs, and closing stdin
/// ends it with its real exit.
#[tokio::test]
async fn a_streaming_exec_runs_the_closures_cat_on_live_stdin() {
    use engenho_kubelet::stream::{StreamExit, StreamRequest};
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e-stream");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir, Cgroups::Off);
    let image = format!("nix:{coreutils}");
    let started = backend
        .start(&streaming_spec(
            &image,
            "native-exec-stream",
            &["sleep", "30"],
        ))
        .await
        .expect("start");

    let session = backend
        .exec_stream(
            &started.container_id,
            &StreamRequest {
                argv: vec!["cat".into()],
                stdin: true,
                stdout: true,
                stderr: true,
                tty: false,
            },
        )
        .await
        .expect("a native container serves a streaming exec");
    let stdin = session.stdin.expect("stdin wired");
    stdin.send(b"live input\n".to_vec()).await.expect("send");
    drop(stdin);
    assert_eq!(drain(session.stdout.expect("stdout")).await, "live input\n");
    assert_eq!(session.exit.await.expect("exit"), StreamExit::Code(0));

    backend.stop(&started.container_id).await.expect("stop");
}

/// ★ `kubectl attach` to a native container declared `stdin: true`: the
/// attacher's input reaches the main process, its output reaches the
/// attacher AND the log, and `stdinOnce` ends it on detach.
#[tokio::test]
async fn attach_reaches_a_native_containers_main_process() {
    use engenho_kubelet::backend::ContainerStdio;
    use engenho_kubelet::stream::{StreamExit, StreamRequest};
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e-attach");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir, Cgroups::Off);
    let image = format!("nix:{coreutils}");
    let mut s = streaming_spec(&image, "native-attach", &["cat"]);
    s.stdio = ContainerStdio {
        stdin: true,
        stdin_once: true,
        tty: false,
    };
    let started = backend.start(&s).await.expect("start");

    let session = backend
        .attach(
            &started.container_id,
            &StreamRequest {
                stdin: true,
                stdout: true,
                stderr: true,
                ..StreamRequest::default()
            },
        )
        .await
        .expect("attach");
    let stdin = session.stdin.expect("stdin wired");
    stdin
        .send(b"to the main process\n".to_vec())
        .await
        .expect("send");
    drop(stdin);
    assert_eq!(
        drain(session.stdout.expect("stdout")).await,
        "to the main process\n"
    );
    assert_eq!(session.exit.await.expect("exit"), StreamExit::Ended);

    let logs = backend
        .logs(&started.container_id, &LogOptions::default())
        .await
        .expect("logs");
    assert_eq!(logs, "to the main process\n", "the log saw it too");
}

/// ★ The log carries the time each line was written: it is CRI records on
/// disk, `timestamps=true` prefixes each line with its RFC3339Nano stamp
/// (k9s strips exactly that token), and `since` filters on it.
#[tokio::test]
async fn a_native_log_is_stamped_per_line_and_filtered_by_since() {
    let coreutils = closure("nixpkgs#coreutils");
    let logs_dir = std::env::temp_dir().join("engenho-native-e2e-stamps");
    let backend = NativeBackend::new(Isolation::HostProcess, &logs_dir, Cgroups::Off);
    let mut image = String::from("nix:");
    image.push_str(&coreutils);
    let mut s = spec(&image, &["printf", "first line\\nsecond line\\n"], &[]);
    s.pod.name = "native-stamps".to_string();
    let before = std::time::SystemTime::now();
    let started = backend
        .start(&s)
        .await
        .expect("a realised closure must start");
    for _ in 0..100 {
        let st = backend
            .status(&started.container_id)
            .await
            .unwrap()
            .unwrap();
        if !st.is_running() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let stamped = backend
        .logs(
            &started.container_id,
            &LogOptions {
                timestamps: true,
                ..LogOptions::default()
            },
        )
        .await
        .expect("logs must be readable");
    let lines: Vec<&str> = stamped.lines().collect();
    assert_eq!(lines.len(), 2, "{stamped:?}");
    for (line, text) in lines.iter().zip(["first line", "second line"]) {
        let (stamp, rest) = line.split_once(' ').expect("a stamp, a space, the text");
        let at = engenho_types::time::parse_rfc3339_system_time(stamp)
            .unwrap_or_else(|| panic!("{stamp:?} is RFC3339"));
        assert!(at >= before - std::time::Duration::from_secs(1));
        assert_eq!(rest, text, "the stamp is the first token and nothing else");
    }

    let plain = backend
        .logs(&started.container_id, &LogOptions::default())
        .await
        .unwrap();
    assert_eq!(plain, "first line\nsecond line\n");

    let later = backend
        .logs(
            &started.container_id,
            &LogOptions {
                since: Some(std::time::SystemTime::now() + std::time::Duration::from_secs(60)),
                ..LogOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(later, "", "every line was written before `since`");

    let on_disk = std::fs::read_to_string(logs_dir.join(&started.container_id)).unwrap();
    assert!(
        on_disk.lines().all(|l| l.contains(" stdout F ")),
        "the file is CRI records: {on_disk:?}"
    );
}
