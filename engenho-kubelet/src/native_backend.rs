//! `NativeBackend` — run a workload as a native host process, from a Nix closure.
//!
//! ## What this is for
//!
//! Every other backend in this crate ends at a Linux runtime. On macOS that
//! means a VM: measured on ryn 2026-09-17, `podman info` reports
//! `linux/arm64 kernel=6.17.7-300.fc43.aarch64` — so a "baremetal" control
//! plane was running every one of its pods inside a Fedora guest.
//!
//! This backend has no runtime under it. A container is a **process**, its
//! image is a **realised Nix closure**, and nothing is unpacked, layered or
//! virtualised. Proven before it was written: `postgresql_16` from nixpkgs is
//! `Mach-O 64-bit executable arm64`, `initdb` succeeds, and the server answers
//! `select version()` as an ordinary host process.
//!
//! ## What it deliberately does NOT do yet
//!
//! [`Isolation`] has one variant, [`Isolation::HostProcess`], and the caller
//! must name it. That is not ceremony — it is the honest tier. A workload
//! started here runs with the **daemon's own privileges**: no Seatbelt profile,
//! no container principal, no uid-scoped reaper. Confinement lives in
//! `pleme-io/nawabari` (`shimenawa` renders the profile, `kakoi::enter` drops
//! the principal and applies it) and arrives here as a second `Isolation`
//! variant once that crate is a dependency.
//!
//! Naming it as a variant rather than a TODO is the point: a future
//! `Isolation::Confined` is an additive change, and until it exists no call
//! site can accidentally believe it got confinement. A backend that silently
//! ran workloads unconfined while reporting success is the exact failure shape
//! this crate keeps finding.

use crate::backend::{ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions};
use crate::error::KubeletError;
use crate::image_source::ImageSource;
use crate::pod_volume::MountSource;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// What the native backend refused, and why.
///
/// ★ TYPED FIELDS, rendered ONCE in `Display` through `write!`.
///
/// The first version of this module built every message with a chain of
/// `push_str` calls. That dodged the `format!()` ban while reproducing exactly
/// what the ban exists to prevent: prose assembled at the call site, where a
/// path or a reason can be dropped, reordered or silently mangled — and it was
/// mangled, reaching the daemon log with 22-space gaps mid-sentence.
///
/// `KubeletError::Backend(String)` is what pushes callers into assembling
/// strings. This type absorbs that: every call site constructs a VALUE with
/// fields, and exactly one `Display` impl decides how it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeError {
    /// An OCI reference, which this backend structurally cannot run.
    OciImage { reference: String },
    /// The image reference itself was malformed.
    Image(crate::image_source::ImageSourceError),
    /// No command, and a closure has no ENTRYPOINT to fall back to.
    NoCommand,
    /// The resolved program is outside the Nix store, so the closure would not
    /// be what actually ran.
    CommandEscapesStore { program: PathBuf },
    /// A runtime-managed named volume, which has no host path.
    NamedVolume { volume: String },
    /// A volume whose host path and mountPath differ; a native process has no
    /// mount namespace to reconcile them with.
    UnmappableMount { host: PathBuf, mount_path: String },
    /// A volume path that could not be created.
    VolumePath { path: PathBuf, detail: String },
    /// No such container is tracked.
    NoSuchContainer { id: String },
    /// The backend's own state lock was poisoned.
    StatePoisoned,
    /// The workload could not be spawned.
    Spawn { program: PathBuf, detail: String },
    /// A container's log could not be read.
    Log { path: PathBuf, detail: String },
    /// `exec` was called with no command.
    ExecNoCommand,
    /// A tracked program has no closure root to resolve an exec against.
    NoClosureRoot { program: PathBuf },
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OciImage { reference } => write!(
                f,
                "cannot run OCI image {reference} — this backend has no Linux \
                 runtime under it. Declare the image as a realised closure \
                 (nix:/nix/store/...) or schedule the pod onto a node running \
                 the podman or CRI backend."
            ),
            Self::Image(e) => write!(f, "{e}"),
            Self::NoCommand => write!(
                f,
                "the container declares no command, and a closure has no \
                 ENTRYPOINT to fall back to"
            ),
            Self::CommandEscapesStore { program } => write!(
                f,
                "the resolved command {} escapes the Nix store, so the closure \
                 would not be what actually ran",
                program.display()
            ),
            Self::NamedVolume { volume } => write!(
                f,
                "volume {volume:?} is a runtime-managed named volume, which \
                 has no host path a native process could read. Declare a \
                 hostPath volume instead."
            ),
            Self::UnmappableMount { host, mount_path } => write!(
                f,
                "cannot mount {} at {mount_path} — a native process has no \
                 mount namespace, so a volume is only honourable when its host \
                 path and its mountPath are the SAME path. Declare the volume \
                 at the path the workload already expects.",
                host.display()
            ),
            Self::VolumePath { path, detail } => {
                write!(f, "cannot create volume path {}: {detail}", path.display())
            }
            Self::NoSuchContainer { id } => write!(f, "no such container {id}"),
            Self::StatePoisoned => write!(f, "state lock poisoned"),
            Self::Spawn { program, detail } => {
                write!(f, "cannot spawn {}: {detail}", program.display())
            }
            Self::Log { path, detail } => {
                write!(f, "cannot read log {}: {detail}", path.display())
            }
            Self::ExecNoCommand => write!(f, "exec requires a command"),
            Self::NoClosureRoot { program } => write!(
                f,
                "container program {} has no closure root to resolve an exec \
                 against",
                program.display()
            ),
        }
    }
}

impl std::error::Error for NativeError {}

impl From<NativeError> for KubeletError {
    /// The ONE place a native-backend failure becomes a string, and it goes
    /// through `Display`.
    fn from(e: NativeError) -> Self {
        let mut rendered = String::from("native backend: ");
        std::fmt::Write::write_fmt(&mut rendered, format_args!("{e}"))
            .expect("writing to a String cannot fail");
        Self::Backend(rendered)
    }
}

/// How much isolation a native container actually gets.
///
/// One variant today, and the caller must choose it explicitly. See the module
/// docs for why this is a type rather than a default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// No confinement: the workload runs with the daemon's own privileges.
    ///
    /// Honest about what it is. Appropriate for a single-operator node where
    /// the alternative is a whole Linux VM; NOT a substitute for a sandbox.
    HostProcess,
}

/// A container's identity on this node: `<namespace>_<pod>_<container>`.
///
/// A type with a `Display` rather than three `push_str` calls, for the same
/// reason [`NativeError`] is: the separator and the field ORDER are part of
/// the format, and assembling them at the call site is how a second call site
/// comes to disagree with the first.
struct ContainerId<'a> {
    namespace: &'a str,
    pod: &'a str,
    container: &'a str,
}

impl std::fmt::Display for ContainerId<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}_{}", self.namespace, self.pod, self.container)
    }
}

/// A container this backend started.
#[derive(Debug)]
struct NativeContainer {
    pid: Option<u32>,
    log_path: PathBuf,
    /// The live handle. Kept rather than dropped so the process is REAPED and
    /// its real exit code is readable: a dropped `Child` leaves a zombie and
    /// makes every exit look like `None`, which a kubelet reads as "still
    /// running" forever.
    child: tokio::process::Child,
    /// Kept so `status` can report the closure a container came from without
    /// re-parsing the spec.
    program: PathBuf,
}

/// Runs containers as native host processes out of Nix closures.
pub struct NativeBackend {
    isolation: Isolation,
    log_dir: PathBuf,
    state: Arc<Mutex<HashMap<String, NativeContainer>>>,
}

impl NativeBackend {
    /// Build a backend that writes container logs under `log_dir`.
    #[must_use]
    pub fn new(isolation: Isolation, log_dir: impl Into<PathBuf>) -> Self {
        Self {
            isolation,
            log_dir: log_dir.into(),
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The isolation this backend provides, so a caller can report it rather
    /// than assume it.
    #[must_use]
    pub const fn isolation(&self) -> Isolation {
        self.isolation
    }

    /// The deterministic container id, matching the scheme the podman backend
    /// uses so a node's containers are identifiable across backends.
    fn container_id(spec: &ContainerSpec) -> String {
        ContainerId {
            namespace: &spec.pod.namespace,
            pod: &spec.pod.name,
            container: &spec.name,
        }
        .to_string()
    }

    /// Resolve the program to execute inside `closure`.
    ///
    /// An absolute `command[0]` is honoured as-is (a pod may name
    /// `/nix/store/.../bin/postgres` directly); a bare name is resolved against
    /// the closure's `bin/`. A command that resolves outside the closure is
    /// refused — the closure is the thing being promised, and executing
    /// something else silently would make that promise false.
    fn resolve_program(closure: &Path, command: &[String]) -> Result<PathBuf, NativeError> {
        let Some(first) = command.first() else {
            return Err(NativeError::NoCommand);
        };
        let candidate = if first.starts_with('/') {
            PathBuf::from(first)
        } else {
            closure.join("bin").join(first)
        };
        if !candidate.starts_with("/nix/store/") {
            return Err(NativeError::CommandEscapesStore { program: candidate });
        }
        Ok(candidate)
    }

    /// Check every mount can actually be honoured, and say precisely which
    /// cannot.
    ///
    /// ★ A native process has NO MOUNT NAMESPACE. There is no remapping
    /// available: whatever path the host has is the path the workload sees. So
    /// a mount is satisfiable only when the source host path IS the declared
    /// `mountPath`, and anything else is refused here rather than discovered
    /// later as an empty directory.
    ///
    /// That distinction matters most for exactly the workload this backend
    /// exists to run. A Postgres started with its data directory silently
    /// absent does not fail loudly — it can initdb into the wrong place, or
    /// come up empty, and report success either way.
    fn verify_mounts(spec: &ContainerSpec) -> Result<(), NativeError> {
        for m in &spec.mounts {
            let host = match &m.source {
                MountSource::HostDir(p) | MountSource::EmptyDirHostDir(p) => p.clone(),
                MountSource::PvcHostDir { path, .. } => path.clone(),
                MountSource::NamedVolume(name) => {
                    return Err(NativeError::NamedVolume {
                        volume: name.clone(),
                    });
                }
            };
            if host != Path::new(&m.mount_path) {
                return Err(NativeError::UnmappableMount {
                    host,
                    mount_path: m.mount_path.clone(),
                });
            }
            // The path must exist before the workload looks for it. Creating it
            // here rather than assuming the kubelet did keeps the failure at
            // start, where it is attributable.
            std::fs::create_dir_all(&host).map_err(|e| NativeError::VolumePath {
                path: host.clone(),
                detail: e.to_string(),
            })?;
        }
        Ok(())
    }

    /// Reject an image this backend structurally cannot run.
    ///
    /// The error NAMES the reason and the remedy. A backend that returned a
    /// generic failure here would look identical to a crashed workload, and the
    /// operator would debug the wrong thing.
    fn closure_of(spec: &ContainerSpec) -> Result<PathBuf, NativeError> {
        match ImageSource::parse(&spec.image).map_err(NativeError::Image)? {
            ImageSource::NixClosure(p) => Ok(p),
            ImageSource::Oci(reference) => Err(NativeError::OciImage { reference }),
        }
    }
}

#[async_trait::async_trait]
impl ContainerRuntime for NativeBackend {
    fn name(&self) -> &'static str {
        "native"
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let closure = Self::closure_of(spec)?;
        let program = Self::resolve_program(&closure, &spec.command)?;
        Self::verify_mounts(spec)?;

        let id = Self::container_id(spec);
        std::fs::create_dir_all(&self.log_dir).map_err(|e| NativeError::VolumePath {
            path: self.log_dir.clone(),
            detail: e.to_string(),
        })?;
        let log_path = self.log_dir.join(&id);
        let log = std::fs::File::create(&log_path).map_err(|e| NativeError::Log {
            path: log_path.clone(),
            detail: e.to_string(),
        })?;
        let log_err = log.try_clone().map_err(|e| NativeError::Log {
            path: log_path.clone(),
            detail: e.to_string(),
        })?;

        let mut cmd = tokio::process::Command::new(&program);
        cmd.args(spec.command.iter().skip(1));
        // env_clear so a container inherits the DAEMON's environment only by
        // declaration. Inheriting it implicitly is how a workload ends up
        // depending on something no manifest records.
        cmd.env_clear();
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        cmd.stdout(std::process::Stdio::from(log));
        cmd.stderr(std::process::Stdio::from(log_err));
        cmd.stdin(std::process::Stdio::null());

        let child = cmd.spawn().map_err(|e| NativeError::Spawn {
            program: program.clone(),
            detail: e.to_string(),
        })?;
        let pid = child.id();

        self.state
            .lock()
            .map_err(|_| NativeError::StatePoisoned)?
            .insert(
                id.clone(),
                NativeContainer {
                    pid,
                    log_path,
                    child,
                    program,
                },
            );

        Ok(ContainerStatus {
            container_id: id,
            running: pid.is_some(),
            // A native process shares the host's network namespace: there is no
            // per-pod IP to hand back, and inventing one would be worse than
            // reporting none.
            pod_ip: None,
            exit_code: None,
        })
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        let mut guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        let Some(c) = guard.get_mut(container_id) else {
            return Ok(None);
        };
        // try_wait REAPS an exited child and yields its real status. Asking the
        // kernel beats trusting a cached flag: a container that died a second
        // ago must not still read as running.
        let exited = c.child.try_wait().map_err(|e| NativeError::Spawn {
            program: c.program.clone(),
            detail: e.to_string(),
        })?;
        let exit_code = exited.and_then(|s| s.code());
        Ok(Some(ContainerStatus {
            container_id: container_id.to_string(),
            running: exited.is_none(),
            pod_ip: None,
            exit_code,
        }))
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        let Some(c) = guard.get_mut(container_id) else {
            // Not tracked. A typed no-op beats an error: stopping something
            // already gone is the normal end of a pod, not a failure.
            return Ok(());
        };
        // SIGTERM first, so a workload that handles it gets to shut down
        // cleanly. Postgres in particular treats SIGTERM as "smart shutdown"
        // and SIGKILL as a crash it must recover from on next start.
        if let Some(pid) = c.pid {
            signal_process(pid, SIGTERM);
        }
        Ok(())
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        guard.remove(container_id);
        Ok(())
    }

    async fn logs(&self, container_id: &str, opts: &LogOptions) -> Result<String, KubeletError> {
        let path = {
            let guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
            guard.get(container_id).map(|c| c.log_path.clone())
        };
        let Some(path) = path else {
            // A typed error, never a silently-empty success — the same rule the
            // trait states for podman.
            return Err(NativeError::NoSuchContainer {
                id: container_id.to_string(),
            }
            .into());
        };
        let body = std::fs::read_to_string(&path).map_err(|e| NativeError::Log {
            path: path.clone(),
            detail: e.to_string(),
        })?;
        Ok(match opts.tail {
            Some(n) => {
                let lines: Vec<&str> = body.lines().collect();
                let start = lines.len().saturating_sub(n as usize);
                let mut out = lines[start..].join("\n");
                if !out.is_empty() {
                    out.push('\n');
                }
                out
            }
            None => body,
        })
    }

    async fn exec(&self, container_id: &str, argv: &[String]) -> Result<ExecOutcome, KubeletError> {
        let program = {
            let guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
            guard.get(container_id).map(|c| c.program.clone())
        };
        let Some(program) = program else {
            return Err(NativeError::NoSuchContainer {
                id: container_id.to_string(),
            }
            .into());
        };
        let Some(first) = argv.first() else {
            return Err(NativeError::ExecNoCommand.into());
        };
        // Resolve against the same closure the container runs from, so an
        // exec-probe cannot accidentally test a binary from the host's PATH
        // instead of the one in the image.
        let closure =
            program
                .parent()
                .and_then(Path::parent)
                .ok_or_else(|| NativeError::NoClosureRoot {
                    program: program.clone(),
                })?;
        let target = Self::resolve_program(closure, std::slice::from_ref(first))?;
        let out = tokio::process::Command::new(&target)
            .args(argv.iter().skip(1))
            .output()
            .await
            .map_err(|e| NativeError::Spawn {
                program: target.clone(),
                detail: e.to_string(),
            })?;
        Ok(ExecOutcome {
            exit_code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

const SIGTERM: i32 = 15;

/// Is `pid` still alive? `kill(pid, 0)` performs the permission/existence
/// check without delivering anything.
fn process_is_alive(pid: u32) -> bool {
    signal_process(pid, 0)
}

/// Send `sig` to exactly `pid`. Never a negative pid: a process-group or
/// broadcast signal from here would reach far more than this container.
fn signal_process(pid: u32, sig: i32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        // Defensive and load-bearing: `kill(0, …)` signals the caller's whole
        // process group and `kill(-1, …)` everything it may reach. Neither is
        // ever what stopping one container means.
        return false;
    }
    // SAFETY: `kill` with a positive pid is a plain syscall with no memory
    // effects; the pid is validated positive above.
    #[allow(unsafe_code)]
    let rc = unsafe { libc_kill(pid, sig) };
    rc == 0
}

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PodIdentity;

    fn spec(image: &str, command: &[&str]) -> ContainerSpec {
        ContainerSpec {
            name: "postgres".to_string(),
            image: image.to_string(),
            command: command.iter().map(|s| (*s).to_string()).collect(),
            pod: PodIdentity {
                namespace: "pangea-system".to_string(),
                name: "pangea-postgres-0".to_string(),
                container_name: "postgres".to_string(),
                ..PodIdentity::default()
            },
            ..ContainerSpec::default()
        }
    }

    /// ★ The defining refusal. ryn runs this exact image today; a native
    /// backend must refuse it with a reason an operator can act on, not accept
    /// the pod and fail somewhere later.
    #[tokio::test]
    async fn an_oci_image_is_refused_with_the_reason_and_the_remedy() {
        let b = NativeBackend::new(Isolation::HostProcess, "/tmp/nb-test-logs");
        let err = b
            .start(&spec("docker.io/library/postgres:16-alpine", &["postgres"]))
            .await
            .expect_err("an OCI image must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("docker.io/library/postgres:16-alpine"),
            "{msg}"
        );
        assert!(
            msg.contains("no Linux runtime"),
            "the refusal must say WHY, or it reads like a crashed workload: {msg}"
        );
        assert!(
            msg.contains("nix:/nix/store/"),
            "the refusal must name the remedy: {msg}"
        );
    }

    /// A command resolving outside the closure would mean the thing that ran
    /// is not the thing that was promised.
    #[test]
    fn a_command_escaping_the_closure_is_refused() {
        let closure = Path::new("/nix/store/aaa-pkg");
        let err = NativeBackend::resolve_program(closure, &["/bin/sh".to_string()])
            .expect_err("must refuse");
        assert!(err.to_string().contains("escapes the Nix store"));
    }

    #[test]
    fn a_bare_command_resolves_into_the_closure_bin() {
        let closure = Path::new("/nix/store/aaa-pkg");
        let p = NativeBackend::resolve_program(closure, &["postgres".to_string()])
            .expect("must resolve");
        assert_eq!(p, Path::new("/nix/store/aaa-pkg/bin/postgres"));
    }

    /// A closure has no ENTRYPOINT to fall back on, so an empty command is a
    /// refusal rather than a guess.
    #[test]
    fn an_empty_command_is_refused_rather_than_guessed() {
        let closure = Path::new("/nix/store/aaa-pkg");
        assert!(NativeBackend::resolve_program(closure, &[]).is_err());
    }

    /// ★ The guard that keeps `stop` from becoming a broadcast. `kill(0, sig)`
    /// hits the caller's whole process group and `kill(-1, sig)` everything it
    /// can reach; either would take out the daemon itself.
    #[test]
    fn a_non_positive_pid_is_never_signalled() {
        assert!(!signal_process(0, SIGTERM), "pid 0 is the process GROUP");
        assert!(
            !process_is_alive(0),
            "pid 0 must never be probed as if it were a container"
        );
    }

    /// Liveness is measured, not assumed: this process is alive, and a pid
    /// that cannot exist is not. Without the second half the check would pass
    /// while always returning true.
    #[test]
    fn liveness_distinguishes_a_live_process_from_a_dead_one() {
        let me = std::process::id();
        assert!(process_is_alive(me), "this test's own process is alive");
        assert!(
            !process_is_alive(u32::MAX - 1),
            "an impossible pid must not report as alive, or the check is vacuous"
        );
    }

    #[test]
    fn the_container_id_is_deterministic_and_namespaced() {
        assert_eq!(
            NativeBackend::container_id(&spec("nix:/nix/store/a-b", &["x"])),
            "pangea-system_pangea-postgres-0_postgres"
        );
    }

    /// ★ A volume that cannot be honoured is refused AT START, naming both
    /// paths. The alternative is the shape this backend exists to avoid: a
    /// Postgres whose data directory is silently absent comes up, reports
    /// success, and is wrong.
    #[test]
    fn a_volume_that_cannot_be_remapped_is_refused_naming_both_paths() {
        let mut s = spec("nix:/nix/store/a-pkg", &["postgres"]);
        s.mounts = vec![crate::pod_volume::ResolvedMount {
            source: MountSource::HostDir("/Users/luis.d/pgdata".into()),
            mount_path: "/var/lib/postgresql/data".to_string(),
            read_only: false,
            sub_path: None,
        }];
        let err = NativeBackend::verify_mounts(&s).expect_err("must refuse");
        let msg = err.to_string();
        assert!(msg.contains("/Users/luis.d/pgdata"), "{msg}");
        assert!(msg.contains("/var/lib/postgresql/data"), "{msg}");
        assert!(msg.contains("no mount namespace"), "{msg}");
        // ★ A rendered run of spaces means a `\`-continued literal lost its
        // continuations and fmt baked the source indentation into the string.
        // It happened: this message reached the daemon log with 22-space gaps
        // mid-sentence. The guard is cheap and the defect is invisible in
        // review.
        assert!(
            !msg.contains("  "),
            "the message must not carry source indentation: {msg}"
        );
    }

    /// The positive control: identical paths ARE honourable, so the check is
    /// not simply refusing every volume.
    #[test]
    fn a_volume_whose_host_path_equals_its_mount_path_is_accepted() {
        let dir = std::env::temp_dir().join("engenho-native-mount-ok");
        let mut s = spec("nix:/nix/store/a-pkg", &["postgres"]);
        s.mounts = vec![crate::pod_volume::ResolvedMount {
            source: MountSource::HostDir(dir.clone()),
            mount_path: dir.to_string_lossy().into_owned(),
            read_only: false,
            sub_path: None,
        }];
        NativeBackend::verify_mounts(&s).expect("identical paths must be honourable");
        assert!(dir.exists(), "the volume path must be created before start");
    }

    /// A runtime-managed named volume has no host path at all, so it is a
    /// different refusal with a different remedy.
    #[test]
    fn a_named_volume_is_refused_because_it_has_no_host_path() {
        let mut s = spec("nix:/nix/store/a-pkg", &["postgres"]);
        s.mounts = vec![crate::pod_volume::ResolvedMount {
            source: MountSource::NamedVolume("pgdata".to_string()),
            mount_path: "/var/lib/postgresql/data".to_string(),
            read_only: false,
            sub_path: None,
        }];
        let msg = NativeBackend::verify_mounts(&s)
            .expect_err("must refuse")
            .to_string();
        assert!(msg.contains("named volume"), "{msg}");
        assert!(msg.contains("hostPath"), "the remedy must be named: {msg}");
    }

    /// Logs for an unknown container are a typed error, never an empty string
    /// that reads as "the workload printed nothing".
    #[tokio::test]
    async fn logs_for_an_unknown_container_are_an_error_not_an_empty_success() {
        let b = NativeBackend::new(Isolation::HostProcess, "/tmp/nb-test-logs");
        let err = b
            .logs("nope", &LogOptions::default())
            .await
            .expect_err("must be a typed error");
        assert!(err.to_string().contains("no such container"));
    }
}
