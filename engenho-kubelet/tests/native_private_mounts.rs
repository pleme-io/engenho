//! End-to-end: two native pods read ONE absolute path and each sees its own
//! file, because each runs in a mount namespace of its own
//! (`runtime.native_mounts`, `engenho_kubelet::private_mounts`).
//!
//! Without the namespace a native process sees the host's paths, so two
//! workloads that read `<fixed>/app.conf` read the same file, or one is
//! refused. The scenario starts real processes out of a real closure and reads
//! what they printed.
//!
//! Where it runs:
//!
//! * a process holding `CAP_SYS_ADMIN` on Linux runs it directly;
//! * an unprivileged one re-runs this test inside a user namespace of its own,
//!   mapped to root, where it may mount;
//! * anywhere else it prints `SKIPPED:` and the reason, and passes.
//!
//! The enclosure variant needs a real root daemon, since the enclosure changes
//! to an account a user namespace does not map; elsewhere it is skipped the
//! same way.

#![allow(
    clippy::disallowed_methods,
    reason = "drives the native runtime directly; no kubelet in the loop"
)]

mod support;

use engenho_kubelet::backend::{Confinement, ContainerRuntime, ContainerSpec, PodIdentity};
use engenho_kubelet::cgroup::Cgroups;
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::kakoi::NativeSecurity;
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use engenho_kubelet::pod_volume::{MountSource, ResolvedMount};
use engenho_kubelet::private_mounts::{MOUNTS_ANNOTATION, NativeMounts, PRIVATE};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const INNER: &str = "ENGENHO_PRIVATE_MOUNTS_INNER";
const TOOLS: &str = "ENGENHO_PRIVATE_MOUNTS_TOOLS";
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const SHARED_TEST: &str = "two_native_pods_read_one_absolute_path_and_see_their_own_files";

fn skip(why: &str) {
    eprintln!("SKIPPED: {why}");
}

fn open_dir(path: &Path) {
    std::fs::create_dir_all(path).expect("create a test directory");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .expect("open a test directory");
}

fn volume(base: &Path, name: &str) -> PathBuf {
    let dir = base.join("volumes").join(name);
    open_dir(&dir);
    let file = dir.join("app.conf");
    std::fs::write(&file, name).expect("write a volume's file");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644))
        .expect("open a volume's file");
    dir
}

fn bind(source: &Path, at: &str, read_only: bool) -> ResolvedMount {
    ResolvedMount {
        source: MountSource::UserHostPath(source.to_path_buf()),
        mount_path: at.to_string(),
        read_only,
        sub_path: None,
    }
}

fn spec(tools: &Path, pod: &str, script: &str, mounts: Vec<ResolvedMount>) -> ContainerSpec {
    ContainerSpec {
        name: "app".to_string(),
        image: format!("nix:{}", tools.display()),
        command: ["sh", "-c", script].map(str::to_string).to_vec(),
        pod: PodIdentity {
            namespace: "mounts-test".to_string(),
            name: pod.to_string(),
            container_name: "app".to_string(),
            ..PodIdentity::default()
        },
        mounts,
        confinement: Confinement {
            mounts: Some(PRIVATE.to_string()),
            ..Confinement::default()
        },
        ..ContainerSpec::default()
    }
}

/// Start `spec`, wait for it to exit, and return how it ended and what it
/// printed.
async fn run(backend: &NativeBackend, spec: &ContainerSpec) -> Result<(RunState, String), String> {
    let started = backend
        .start(spec)
        .await
        .map_err(|e| format!("pod {} did not start: {e}", spec.pod.name))?;
    for _ in 0..500 {
        let status = backend
            .status(&started.container_id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("the container is not tracked")?;
        if !status.is_running() {
            let logs = backend
                .logs(&started.container_id, &Default::default())
                .await
                .map_err(|e| e.to_string())?;
            backend
                .remove(&started.container_id)
                .await
                .map_err(|e| e.to_string())?;
            return Ok((status.state, logs.trim().to_string()));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(format!("pod {} did not exit within 10s", spec.pod.name))
}

fn expect(what: &str, got: &(RunState, String), state: RunState, out: &str) -> Result<(), String> {
    if got.0 == state && got.1 == out {
        return Ok(());
    }
    Err(format!(
        "{what}: expected {state:?} printing {out:?}, got {:?} printing {:?}",
        got.0, got.1
    ))
}

/// The claim, as one function: `Err` names the first part that did not hold.
async fn scenario(
    tools: &Path,
    security: NativeSecurity,
    mounts: NativeMounts,
) -> Result<(), String> {
    let base = tempfile::tempdir_in("/var/tmp").map_err(|e| e.to_string())?;
    open_dir(base.path());
    let fixed = base.path().join("etc/app/conf");
    let fixed_s = fixed.display().to_string();
    let alpha = volume(base.path(), "alpha");
    let beta = volume(base.path(), "beta");
    let delta = volume(base.path(), "delta");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        base.path().join("logs"),
        Cgroups::Off,
    )
    .with_security(security)
    .with_mounts(mounts);
    let bin = tools.join("bin");
    let bin = bin.display();
    let read = format!("{bin}/cat {fixed_s}/app.conf");
    let ok = RunState::Exited(ExitDisposition::Code(0));

    let a = run(
        &backend,
        &spec(tools, "alpha", &read, vec![bind(&alpha, &fixed_s, false)]),
    )
    .await?;
    let b = run(
        &backend,
        &spec(tools, "beta", &read, vec![bind(&beta, &fixed_s, false)]),
    )
    .await?;
    expect("pod alpha reads its own file", &a, ok, "alpha")?;
    expect(
        "pod beta reads its own file at the same path",
        &b,
        ok,
        "beta",
    )?;

    let write = format!("(echo x > {fixed_s}/written) 2>/dev/null && echo wrote || echo refused");
    let ro = run(
        &backend,
        &spec(
            tools,
            "readonly",
            &write,
            vec![bind(&alpha, &fixed_s, true)],
        ),
    )
    .await?;
    expect("a readOnly volume refuses a write", &ro, ok, "refused")?;
    if alpha.join("written").exists() {
        return Err("a readOnly volume's source was written".to_string());
    }

    if fixed.join("app.conf").exists() {
        return Err(format!("the host sees a pod's file at {fixed_s}/app.conf"));
    }

    let held = spec(
        tools,
        "delta",
        &format!("exec {bin}/sleep 30"),
        vec![bind(&delta, &fixed_s, false)],
    );
    let started = backend
        .start(&held)
        .await
        .map_err(|e| format!("pod delta did not start: {e}"))?;
    let exec = backend
        .exec(
            &started.container_id,
            &["cat".to_string(), format!("{fixed_s}/app.conf")],
        )
        .await
        .map_err(|e| format!("exec in pod delta: {e}"));
    backend
        .stop(&started.container_id)
        .await
        .map_err(|e| e.to_string())?;
    let exec = exec?;
    if exec.exit_code != 0 || exec.stdout.trim() != "delta" {
        return Err(format!(
            "an exec in pod delta reads {:?} (exit {}, stderr {:?}), not its own file",
            exec.stdout.trim(),
            exec.exit_code,
            exec.stderr.trim()
        ));
    }
    Ok(())
}

/// Run this test binary with `args` in a user namespace of its own, mapped to
/// root. With `probe`, the child first makes a mount namespace and binds
/// `probe` onto itself, the operations the scenario needs, so a kernel or a
/// policy that lets a user namespace exist but not mount answers here.
#[cfg(target_os = "linux")]
fn in_user_namespace(
    args: &[&str],
    tools: &Path,
    probe: Option<&Path>,
) -> Result<std::process::Output, String> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::process::CommandExt;
    let uid = rustix::process::getuid().as_raw();
    let gid = rustix::process::getgid().as_raw();
    let uid_map = format!("0 {uid} 1");
    let gid_map = format!("0 {gid} 1");
    let probe = probe
        .map(|p| std::ffi::CString::new(p.as_os_str().as_bytes()))
        .transpose()
        .map_err(|e| e.to_string())?;
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let mut cmd = std::process::Command::new(exe);
    cmd.args(args).env(INNER, "1").env(TOOLS, tools);
    let write = |path: &std::ffi::CStr, bytes: &[u8]| -> std::io::Result<()> {
        let fd = rustix::fs::open(path, rustix::fs::OFlags::WRONLY, rustix::fs::Mode::empty())?;
        rustix::io::write(&fd, bytes)?;
        Ok(())
    };
    #[allow(unsafe_code)]
    unsafe {
        cmd.pre_exec(move || {
            rustix::thread::unshare_unsafe(rustix::thread::UnshareFlags::NEWUSER)?;
            write(c"/proc/self/setgroups", b"deny")?;
            write(c"/proc/self/uid_map", uid_map.as_bytes())?;
            write(c"/proc/self/gid_map", gid_map.as_bytes())?;
            if let Some(probe) = &probe {
                rustix::thread::unshare_unsafe(rustix::thread::UnshareFlags::NEWNS)?;
                rustix::mount::mount_change(
                    c"/",
                    rustix::mount::MountPropagationFlags::PRIVATE
                        | rustix::mount::MountPropagationFlags::REC,
                )?;
                rustix::mount::mount_bind(probe.as_c_str(), probe.as_c_str())?;
            }
            Ok(())
        });
    }
    cmd.output().map_err(|e| e.to_string())
}

#[tokio::test]
async fn two_native_pods_read_one_absolute_path_and_see_their_own_files() {
    if !cfg!(target_os = "linux") {
        skip("a mount namespace is a Linux facility");
        return;
    }
    if let Some(tools) = std::env::var_os(TOOLS).filter(|_| std::env::var_os(INNER).is_some()) {
        scenario(Path::new(&tools), NativeSecurity::Off, NativeMounts::PerPod)
            .await
            .unwrap();
        return;
    }
    let tools = support::closure("nixpkgs#busybox");
    if engenho_kubelet::private_mounts::can_mount().is_ok() {
        scenario(&tools, NativeSecurity::Off, NativeMounts::PerPod)
            .await
            .unwrap();
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let probe = tempfile::tempdir().expect("a probe directory");
        if let Err(why) = in_user_namespace(&["--list"], &tools, Some(probe.path())) {
            skip(&format!("a user namespace here cannot mount: {why}"));
            return;
        }
        let out = in_user_namespace(&[SHARED_TEST, "--exact", "--nocapture"], &tools, None)
            .expect("a user namespace that mounted once is entered again");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.status.success(),
            "the run in a user namespace failed:\n{text}"
        );
        assert!(
            text.contains(&format!("test {SHARED_TEST} ... ok")),
            "the run in a user namespace did not run the test:\n{text}"
        );
        eprintln!("ran in a user namespace mapped to root");
    }
}

#[tokio::test]
async fn two_native_pods_see_their_own_files_under_the_default_enclosure() {
    if !cfg!(target_os = "linux") {
        skip("a mount namespace is a Linux facility");
        return;
    }
    let real_root = std::fs::read_to_string("/proc/self/uid_map")
        .is_ok_and(|m| m.split_whitespace().collect::<Vec<_>>() == ["0", "0", "4294967295"])
        && rustix::process::geteuid().is_root();
    if !real_root {
        skip("the enclosure changes to an account only a real root daemon can give");
        return;
    }
    let tools = support::closure("nixpkgs#busybox");
    scenario(&tools, NativeSecurity::Enforce, NativeMounts::EveryPod)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_pod_that_asks_on_a_node_with_it_off_is_refused_by_name() {
    let tools = support::closure("nixpkgs#coreutils");
    let backend = NativeBackend::new(
        Isolation::HostProcess,
        std::env::temp_dir().join("engenho-private-mounts-off"),
        Cgroups::Off,
    )
    .with_security(NativeSecurity::Off);
    let refused = backend
        .start(&spec(
            &tools,
            "asks",
            "",
            vec![bind(Path::new("/var/empty"), "/etc/app/conf", false)],
        ))
        .await
        .expect_err("a node with native_mounts off refuses the ask")
        .to_string();
    assert!(refused.contains("runtime.native_mounts: off"), "{refused}");
    assert!(refused.contains(MOUNTS_ANNOTATION), "{refused}");

    let own = tempfile::tempdir().expect("a volume");
    let at = own.path().display().to_string();
    let mut sibling = spec(&tools, "sibling", "", vec![bind(own.path(), &at, false)]);
    sibling.command = vec!["true".to_string()];
    sibling.confinement.mounts = None;
    let ran = run(&backend, &sibling).await.expect("the sibling runs");
    assert_eq!(ran.0, RunState::Exited(ExitDisposition::Code(0)), "{ran:?}");
}
