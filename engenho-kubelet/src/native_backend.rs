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
//!
//! ## ★ Why a restart cannot run a workload twice ([`Readoption::Cannot`])
//!
//! A workload is a child of THIS process, tracked in an in-memory table. A
//! new backend — a new daemon process, or a new runtime booted inside the same
//! one — starts with an empty table and no way to find what the old one
//! spawned, so it would start every pod a second time beside the first. For a
//! stateful workload (two Postgres on one data directory) that is corruption,
//! not waste. Three things, one per way a backend can end, keep the old copy
//! from surviving:
//!
//! * **The daemon process stops, on Linux.** The engenho daemon runs as the
//!   systemd unit `engenho-daemon`, rendered by substrate's `mkNixOSService`
//!   with `KillMode=control-group`: stopping (or restarting) the unit kills
//!   every process in its cgroup, and every native workload is in it, because
//!   it is spawned as the daemon's child. That is what makes a daemon restart
//!   safe, and it is load-bearing — so the NixOS arm in `flake.nix` ASSERTS
//!   it, and an evaluation that sets `KillMode` to anything else (`process`,
//!   `mixed`, `none`) fails instead of silently doubling every pod. A daemon
//!   that CRASHES is the same case: systemd stops the unit's remaining cgroup
//!   before `Restart=on-failure` starts it again.
//! * **The daemon process stops, on macOS.** The workload stays in the
//!   daemon's process group (see [`Termination::spawn`]), and launchd kills
//!   the job's group when it stops the job.
//! * **The backend is dropped inside a running process.** The supervisor can
//!   shut a runtime down and boot a new one without the process exiting, and
//!   the new boot builds a new backend. Every workload is spawned with
//!   `kill_on_drop`, so dropping the backend's table SIGKILLs every live
//!   process it still owns. `SIGKILL` and not a graceful stop because a
//!   `Drop` cannot wait out a grace period; a graceful drain belongs to the
//!   kubelet stopping its pods before shutdown, and this is the floor under
//!   it. A process already handed to its termination task is reaped by that
//!   task, which owns it, and is unaffected.
//!
//! Tier-honest: the first is eval-rejected (a Nix assertion), the third is
//! pinned by a test here, the second is launchd's documented behaviour and is
//! not tested in this crate. A workload's OWN children are covered by the
//! cgroup kill on Linux and by none of the others.

use crate::backend::{
    ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions, Readoption,
};
use crate::cri::{ExitDisposition, RunState};
use crate::error::KubeletError;
use crate::image_source::ImageSource;
use crate::pod_volume::BindSource;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

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
    /// No command, and the closure's `bin/` holds no executable to infer one
    /// from.
    NoEntrypoint { closure: PathBuf },
    /// No command, and the closure's `bin/` holds SEVERAL executables, so
    /// there is nothing to infer. Names them: the remedy is to pick one.
    AmbiguousEntrypoint {
        closure: PathBuf,
        candidates: Vec<String>,
    },
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
    /// Asking the kernel whether a container's process exited failed.
    Wait {
        /// The container asked about.
        id: String,
        /// What the kernel said.
        detail: String,
    },
    /// The container's process has not been reaped, so its record cannot be
    /// dropped and its id cannot be reused.
    NotReaped {
        /// The container still owed a wait.
        id: String,
    },
    /// The task terminating a container ended without saying how the process
    /// did. Its calls cannot panic, so this is a runtime shutting down under
    /// it; the process may still be alive, and nothing escalated.
    TerminationLost {
        /// The container whose termination was lost.
        id: String,
    },
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
            Self::NoEntrypoint { closure } => write!(
                f,
                "the container declares no command and {}/bin holds no \
                 executable, so there is no entrypoint to infer",
                closure.display()
            ),
            Self::AmbiguousEntrypoint {
                closure,
                candidates,
            } => write!(
                f,
                "the container declares no command and {}/bin holds {} \
                 executables ({}), so the entrypoint is ambiguous. Declare \
                 `command` naming the one to run.",
                closure.display(),
                candidates.len(),
                candidates.join(", ")
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
            Self::Wait { id, detail } => {
                write!(f, "cannot read the exit of container {id}: {detail}")
            }
            Self::NotReaped { id } => write!(
                f,
                "container {id} has not been reaped; its record stays until \
                 its process is waited on"
            ),
            Self::TerminationLost { id } => write!(
                f,
                "the task terminating container {id} ended without reaping \
                 it; the process may still be running"
            ),
        }
    }
}

impl std::error::Error for NativeError {}

impl From<NativeError> for KubeletError {
    /// The ONE place a native-backend failure becomes a string, and it goes
    /// through `Display`. A process still owed a wait keeps its type: the
    /// kubelet retries it quietly rather than reporting a broken runtime.
    fn from(e: NativeError) -> Self {
        if let NativeError::NotReaped { id } = e {
            return Self::NotReaped { container_id: id };
        }
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
    log_path: PathBuf,
    /// Kept so `status` can report the closure a container came from without
    /// re-parsing the spec.
    program: PathBuf,
    /// The pod's `SIGTERM` → `SIGKILL` window, from the spec it started with.
    grace: Duration,
    /// Where the process is in its life, and who holds it.
    process: Process,
}

/// ★ WHO HOLDS THE PROCESS IS A VALUE, NOT A HOPE.
///
/// `stop` used to send `SIGTERM` to a pid and return, and `remove` dropped
/// the record whatever the process was doing. A workload that ignored
/// `SIGTERM` kept running with no record left to find it by, and the next
/// start under the same id ran a second copy beside it. Every stage is an arm
/// now, and only [`Process::Reaped`] lets a record go.
#[derive(Debug)]
enum Process {
    /// Running, or exited and not yet waited on. The backend holds the only
    /// handle. Kept rather than dropped so the process is REAPED and its real
    /// exit is readable: a dropped `Child` leaves a zombie and makes every
    /// exit look like `None`, which a kubelet reads as "still running"
    /// forever.
    Live(tokio::process::Child),
    /// Handed to its termination task, which holds the `Child` until it has
    /// reaped it. Still alive as far as anyone may assume.
    Terminating(Termination),
    /// Waited on: the pid is released and nothing of the process remains.
    Reaped(Reaped),
}

/// A process being stopped: `SIGTERM`, then `SIGKILL` once the grace period
/// runs out, then reaped — in a task of its own, so no caller waits out a
/// grace period inline.
#[derive(Debug)]
struct Termination {
    /// The task's verdict, sent exactly once, when the process is reaped.
    verdict: oneshot::Receiver<Reaped>,
    /// The task. Held so the record owns it; never aborted — dropping the
    /// handle detaches the task, which still escalates and reaps, so a
    /// backend going away mid-stop does not strand a process that ignored
    /// `SIGTERM`.
    _task: tokio::task::JoinHandle<()>,
}

/// How a reaped process ended.
#[derive(Debug, Clone, Copy)]
enum Reaped {
    /// The kernel's wait status.
    Status(std::process::ExitStatus),
    /// The wait itself failed, which leaves nothing to wait on: the pid is no
    /// longer this process's child to reap. Gone, and how is not known.
    Unobservable,
}

impl Reaped {
    fn run_state(self) -> RunState {
        match self {
            Self::Status(status) => run_state_of(Some(status)),
            Self::Unobservable => RunState::Unknown,
        }
    }
}

impl Termination {
    /// Hand `child` to a new termination task.
    ///
    /// ── ★ THE PID, NEVER ITS PROCESS GROUP ─────────────────────────────────
    /// Signals go to exactly the child's pid, and the workload is spawned
    /// WITHOUT `process_group(0)`: it stays in the daemon's process group. On
    /// ryn that group is launchd's job, and when launchd stops the job it
    /// kills the group — today's only guard against a daemon restart leaving
    /// a second copy of every native workload running, since a new process
    /// cannot re-adopt the old one's children (plan edge 12). The cost is
    /// stated rather than hidden: a workload's own children are not
    /// signalled here. A `SIGKILL`ed shell's children are orphaned and reaped
    /// by launchd, not by this backend.
    fn spawn(child: tokio::process::Child, grace: Duration) -> Self {
        let (tx, verdict) = oneshot::channel();
        let task = tokio::spawn(async move {
            let reaped = terminate(child, grace).await;
            if tx.send(reaped).is_err() {
                // The record, and the backend with it, went away first;
                // there is nobody left to tell. The process is reaped anyway.
                tracing::debug!("native container reaped after its record was dropped");
            }
        });
        Self {
            verdict,
            _task: task,
        }
    }
}

/// The command a workload is spawned from: `program` with `args`, the declared
/// `env` and nothing else, stdin closed, and killed if its handle is dropped.
///
/// * `env_clear`, so a container inherits the DAEMON's environment only by
///   declaration. Inheriting it implicitly is how a workload ends up
///   depending on something no manifest records.
/// * `kill_on_drop`, so a backend that is dropped while its workloads run —
///   a runtime shut down and booted again inside one process — takes them
///   with it rather than leaving them for the next backend to start twice.
///   See the module header, "Why a restart cannot run a workload twice".
fn workload_command<'a>(
    program: &Path,
    args: impl IntoIterator<Item = &'a String>,
    env: &std::collections::BTreeMap<String, String>,
) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args);
    cmd.env_clear();
    cmd.envs(env);
    cmd.stdin(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    cmd
}

/// Stop `child`: `SIGTERM`, `SIGKILL` once `grace` has passed, then reap.
async fn terminate(mut child: tokio::process::Child, grace: Duration) -> Reaped {
    // `id()` is `Some` exactly while the child is unreaped, so the pid cannot
    // have been recycled to a stranger.
    if let Some(pid) = child.id()
        && !signal_process(pid, SIGTERM)
    {
        tracing::debug!(
            pid,
            "SIGTERM not delivered; the grace period still bounds the wait"
        );
    }
    let waited = match tokio::time::timeout(grace, child.wait()).await {
        Ok(waited) => waited,
        Err(_grace_elapsed) => {
            // The workload ignored SIGTERM, or is still shutting down with
            // the grace spent. SIGKILL cannot be ignored.
            if let Err(e) = child.start_kill() {
                tracing::warn!(error = %e, "SIGKILL after the grace period failed");
            }
            child.wait().await
        }
    };
    match waited {
        Ok(status) => Reaped::Status(status),
        Err(e) => {
            tracing::warn!(error = %e, "waiting on a stopped native container failed");
            Reaped::Unobservable
        }
    }
}

impl NativeContainer {
    /// Advance the record to what the kernel says now: a live process that
    /// has exited is reaped, and a termination that has finished is read.
    fn settle(&mut self, id: &str) -> Result<(), NativeError> {
        let reaped = match &mut self.process {
            Process::Live(child) => child
                .try_wait()
                .map_err(|e| NativeError::Wait {
                    id: id.to_string(),
                    detail: e.to_string(),
                })?
                .map(Reaped::Status),
            Process::Terminating(t) => match t.verdict.try_recv() {
                Ok(reaped) => Some(reaped),
                Err(oneshot::error::TryRecvError::Empty) => None,
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(NativeError::TerminationLost { id: id.to_string() });
                }
            },
            Process::Reaped(_) => None,
        };
        if let Some(reaped) = reaped {
            self.process = Process::Reaped(reaped);
        }
        Ok(())
    }

    /// What the process is doing, as last settled. A process being
    /// terminated is still running: nothing has reaped it.
    fn run_state(&self) -> RunState {
        match &self.process {
            Process::Live(_) | Process::Terminating(_) => RunState::Running,
            Process::Reaped(reaped) => reaped.run_state(),
        }
    }

    const fn is_reaped(&self) -> bool {
        matches!(self.process, Process::Reaped(_))
    }

    /// Begin stopping: a live process goes to its termination task. One
    /// already terminating or reaped is left as it is — stop is idempotent.
    fn stopping(self) -> Self {
        let process = match self.process {
            Process::Live(child) => Process::Terminating(Termination::spawn(child, self.grace)),
            other => other,
        };
        Self { process, ..self }
    }
}

/// Runs containers as native host processes out of Nix closures.
pub struct NativeBackend {
    isolation: Isolation,
    log_dir: PathBuf,
    state: Arc<Mutex<HashMap<String, NativeContainer>>>,
}

/// The address a native container answers on.
///
/// A native container is a host PROCESS: it shares the host's network
/// namespace, so its listening ports ARE the host's ports. There is no
/// per-pod IP — and this used to report `None` on that basis, with a comment
/// arguing that "inventing one would be worse than reporting none."
///
/// ★ `None` is not the neutral answer. `probe.rs` read it as
/// `ProbeObservation::Failure` unconditionally (`let Some(ip) = pod_ip else
/// { return Failure }`), so EVERY httpGet and tcpSocket probe against a
/// native pod failed forever, whatever the workload was doing.
///
/// Since T1.1 it reads as `Blind(NoTargetAddress)`: no restart, a Warning and
/// a `ProbeBlind` pod condition instead. That stops the kill loop below but
/// not the cause — a probe with nothing to dial still never PASSES, so the
/// pod stays unready. Reporting the real address is still the fix.
///
/// Measured on ryn 2026-09-18: pangea-operator's `startupProbe`
/// (failureThreshold 30 x periodSeconds 5 = 150s) could never pass, so the
/// kubelet killed a perfectly healthy operator every 2.5 minutes —
/// `restartCount: 14`, `ready: false` — while `curl 127.0.0.1:8080/healthz`
/// answered **HTTP 200 in 0.4ms** from the same host. The pod was reported
/// unready precisely because nothing could ask it.
///
/// The loopback is not invented, it is MEASURED: it is the address the
/// process is bound to and the one a probe reaches it on. Upstream has the
/// same case and the same answer — a `hostNetwork: true` pod takes
/// `status.podIP = status.hostIP`, and the prober dials that. A native
/// container is a host-network container.
const HOST_NETWORK_POD_IP: &str = "127.0.0.1";

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
            // ── ★ A CLOSURE WITH EXACTLY ONE EXECUTABLE IS UNAMBIGUOUS ─────
            // An OCI image carries an ENTRYPOINT; a Nix closure does not. But
            // when `bin/` holds exactly one executable there is nothing to
            // choose, so inferring it is a DERIVATION, not a default: with
            // zero or several the backend refuses and names what it found,
            // rather than picking one.
            //
            // This is what lets a well-formed single-binary closure run from a
            // pod spec that declares no command — the common shape for a
            // pleme-io service, and the reason pangea-operator needs no chart
            // change to run natively.
            return Self::sole_executable(closure);
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

    /// The closure's single executable, or a typed refusal naming what it
    /// found instead.
    fn sole_executable(closure: &Path) -> Result<PathBuf, NativeError> {
        let bin = closure.join("bin");
        let mut names: Vec<String> = std::fs::read_dir(&bin)
            .map_err(|_| NativeError::NoEntrypoint {
                closure: closure.to_path_buf(),
            })?
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        match names.len() {
            1 => Ok(bin.join(&names[0])),
            0 => Err(NativeError::NoEntrypoint {
                closure: closure.to_path_buf(),
            }),
            _ => Err(NativeError::AmbiguousEntrypoint {
                closure: closure.to_path_buf(),
                candidates: names,
            }),
        }
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
            let host = match m.source.bind_source() {
                BindSource::Path(p) => p.to_path_buf(),
                BindSource::Volume(name) => {
                    return Err(NativeError::NamedVolume {
                        volume: name.to_string(),
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

    /// A native workload is a child process tracked in this backend's
    /// in-memory table; a new kubelet process starts with an empty table and
    /// no handle on what the old one spawned.
    fn readoption(&self) -> Readoption {
        Readoption::Cannot
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let closure = Self::closure_of(spec)?;
        let program = Self::resolve_program(&closure, &spec.command)?;
        Self::verify_mounts(spec)?;

        let id = Self::container_id(spec);
        // ── ★ ONE PROCESS PER ID ───────────────────────────────────────────
        // Held from the check to the insert, so nothing can slip a second
        // start in between. A previous run under this id that has not been
        // reaped is still — as far as anyone may assume — running, and a
        // start beside it is two copies of the workload. Checked before the
        // log is opened, because opening it truncates that run's log.
        let mut state = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        if let Some(previous) = state.get_mut(&id) {
            previous.settle(&id)?;
            if !previous.is_reaped() {
                return Err(NativeError::NotReaped { id }.into());
            }
        }
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

        let mut cmd = workload_command(&program, spec.command.iter().skip(1), &spec.env);
        cmd.stdout(std::process::Stdio::from(log));
        cmd.stderr(std::process::Stdio::from(log_err));

        let child = cmd.spawn().map_err(|e| NativeError::Spawn {
            program: program.clone(),
            detail: e.to_string(),
        })?;
        let pid = child.id();

        state.insert(
            id.clone(),
            NativeContainer {
                log_path,
                program,
                grace: spec.termination_grace.duration(),
                process: Process::Live(child),
            },
        );
        drop(state);

        Ok(ContainerStatus {
            container_id: id,
            // No pid means the child was already reaped — gone, cause not
            // seen here. The next `status` poll owns the answer.
            state: if pid.is_some() {
                RunState::Running
            } else {
                RunState::Unknown
            },
            pod_ip: Some(HOST_NETWORK_POD_IP.to_string()),
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
        c.settle(container_id)?;
        Ok(Some(ContainerStatus {
            container_id: container_id.to_string(),
            state: c.run_state(),
            pod_ip: Some(HOST_NETWORK_POD_IP.to_string()),
        }))
    }

    /// `SIGTERM` now; `SIGKILL` once the pod's grace period has passed; then
    /// reap — all in the container's termination task, so this returns at
    /// once and the kubelet's tick never waits out a grace period.
    ///
    /// SIGTERM first, so a workload that handles it gets to shut down
    /// cleanly. Postgres in particular treats SIGTERM as "smart shutdown" and
    /// SIGKILL as a crash it must recover from on next start — which is why
    /// the grace period is the pod's own, not a constant.
    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        let Some(c) = guard.remove(container_id) else {
            // Not tracked. A typed no-op beats an error: stopping something
            // already gone is the normal end of a pod, not a failure.
            return Ok(());
        };
        guard.insert(container_id.to_string(), c.stopping());
        Ok(())
    }

    /// Drop the record — only once its process has been reaped. Before that
    /// the refusal is [`KubeletError::NotReaped`], and the record (with its
    /// id) stays, so no replacement can be started beside a live process.
    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        let mut guard = self.state.lock().map_err(|_| NativeError::StatePoisoned)?;
        let Some(c) = guard.get_mut(container_id) else {
            return Ok(());
        };
        c.settle(container_id)?;
        if !c.is_reaped() {
            return Err(NativeError::NotReaped {
                id: container_id.to_string(),
            }
            .into());
        }
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

/// What a `try_wait` result says about a native container.
///
/// ★ THE KERNEL'S TWO ANSWERS, KEPT APART. A process either exits with a code
/// or is killed by a signal, and a killed process has NO code:
/// `ExitStatus::code()` is `None`. This backend used to report exactly
/// `code()`, so every SIGKILL — the OOM killer, `kill -9`, a node agent
/// reaping it — reached the kubelet as "no exit code", which read it as 0 and
/// published a `restartPolicy: Never` pod as `Succeeded`.
///
/// A status that is neither (a stopped or continued process, which `wait`
/// without `WUNTRACED` does not report) is `Unknown`, never a guess.
fn run_state_of(exited: Option<std::process::ExitStatus>) -> RunState {
    use std::os::unix::process::ExitStatusExt;
    let Some(status) = exited else {
        return RunState::Running;
    };
    match (status.code(), status.signal()) {
        (Some(code), _) => RunState::Exited(ExitDisposition::Code(code)),
        (None, Some(signal)) => RunState::Exited(ExitDisposition::Signal(signal)),
        (None, None) => RunState::Unknown,
    }
}

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
#[allow(
    clippy::disallowed_methods,
    reason = "tests of the runtime itself call its start directly"
)]
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

    /// ★ A closure whose `bin/` holds exactly ONE executable has an
    /// unambiguous entrypoint, so a pod spec need not repeat it. This is what
    /// lets a single-binary pleme-io service run from a chart that cannot
    /// express `command`.
    #[test]
    fn a_closure_with_one_executable_needs_no_command() {
        let dir = std::env::temp_dir().join("engenho-entrypoint-one/bin");
        let _ = std::fs::remove_dir_all(dir.parent().expect("parent"));
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join("only-binary"), b"x").expect("write");
        let closure = dir.parent().expect("closure");
        assert_eq!(
            NativeBackend::sole_executable(closure).expect("one binary is unambiguous"),
            dir.join("only-binary")
        );
    }

    /// Several executables is NOT a default-to-the-first: it is a refusal that
    /// names them, because picking one would run something nobody declared.
    #[test]
    fn several_executables_are_refused_and_named() {
        let dir = std::env::temp_dir().join("engenho-entrypoint-many/bin");
        let _ = std::fs::remove_dir_all(dir.parent().expect("parent"));
        std::fs::create_dir_all(&dir).expect("mkdir");
        for n in ["initdb", "postgres", "psql"] {
            std::fs::write(dir.join(n), b"x").expect("write");
        }
        let err = NativeBackend::sole_executable(dir.parent().expect("closure"))
            .expect_err("ambiguous must refuse");
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"), "{msg}");
        for n in ["initdb", "postgres", "psql"] {
            assert!(msg.contains(n), "the candidates must be named: {msg}");
        }
    }

    /// An empty `bin/` infers nothing, and says so distinctly from ambiguous.
    #[test]
    fn a_closure_with_no_executable_has_no_entrypoint() {
        let dir = std::env::temp_dir().join("engenho-entrypoint-none/bin");
        let _ = std::fs::remove_dir_all(dir.parent().expect("parent"));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let err = NativeBackend::sole_executable(dir.parent().expect("closure"))
            .expect_err("must refuse");
        assert!(err.to_string().contains("no entrypoint to infer"), "{err}");
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
            source: crate::pod_volume::MountSource::UserHostPath("/Users/luis.d/pgdata".into()),
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
            source: crate::pod_volume::MountSource::UserHostPath(dir.clone()),
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
            source: crate::pod_volume::MountSource::NamedVolume("pgdata".to_string()),
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

    /// Pins the address itself. The two construction sites are covered by
    /// `tests/native_runs_a_real_closure.rs`, which starts a real closure and
    /// reads the address back off both `start()` and `status()`. The RULE this
    /// constant serves — a missing address is read as a FAILED probe, not as
    /// "no opinion" — is pinned in `probe.rs::probe_address`.
    #[test]
    fn a_native_container_answers_on_the_loopback() {
        assert_eq!(
            HOST_NETWORK_POD_IP, "127.0.0.1",
            "a host process is reachable at the host's loopback; reporting no \
             address at all makes every network probe fail forever"
        );
    }

    /// ★ The T1.2 defect, against a REAL kernel wait status: a `SIGKILL`ed
    /// process has no exit code, and this backend reported exactly that
    /// absence — which the kubelet read as a clean exit.
    #[test]
    fn a_sigkilled_process_is_signal_9_and_never_success() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        // std's `Child::kill` delivers SIGKILL on unix.
        child.kill().expect("deliver SIGKILL");
        let status = child.wait().expect("reap");
        let state = run_state_of(Some(status));
        assert_eq!(state, RunState::Exited(ExitDisposition::Signal(9)));
        assert!(
            !state.exit().is_some_and(ExitDisposition::is_success),
            "a killed process must never read as a success"
        );
    }

    /// Spawn `/bin/sh` running `script` with stdout piped, and wait until it
    /// prints `ready` — so a test signals it only after its traps are set.
    async fn shell_ready(script: &str) -> tokio::process::Child {
        use tokio::io::AsyncBufReadExt;
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", script])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn /bin/sh");
        let stdout = child.stdout.take().expect("stdout is piped");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let first = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("the script reports ready within 5s")
            .expect("read stdout");
        assert_eq!(first.as_deref(), Some("ready"));
        child
    }

    /// ★ A workload whose handle is dropped is killed, so a backend dropped
    /// inside a running process (a runtime shut down and booted again) leaves
    /// nothing running for the next backend to start a second copy of.
    ///
    /// Observed through the workload's stdout: the pipe reaches EOF only when
    /// the process has exited, and the script would otherwise hold it open
    /// for 30s. It ignores SIGTERM, so only a SIGKILL ends it this fast.
    #[tokio::test]
    async fn a_dropped_workload_handle_kills_the_workload() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt};
        let args = [
            "-c".to_string(),
            "trap '' TERM; echo ready; exec sleep 30".to_string(),
        ];
        let mut cmd = workload_command(
            Path::new("/bin/sh"),
            args.iter(),
            &std::collections::BTreeMap::new(),
        );
        cmd.stdout(std::process::Stdio::piped());
        let mut child = cmd.spawn().expect("spawn /bin/sh");
        let mut stdout = tokio::io::BufReader::new(child.stdout.take().expect("stdout is piped"));
        let mut first = String::new();
        tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut first))
            .await
            .expect("the script reports ready within 5s")
            .expect("read stdout");
        assert_eq!(first.trim(), "ready");

        drop(child);

        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stdout.read_to_end(&mut rest))
            .await
            .expect("dropping the handle must end the workload, not leave it running")
            .expect("read to EOF");
    }

    /// ★ T2.10: a workload that ignores SIGTERM is killed with SIGKILL once its grace
    /// period has passed — not before — and REAPED: nothing of it remains,
    /// not even a zombie (which `kill(pid, 0)` would still find).
    ///
    /// The stop this replaced sent SIGTERM and returned; a workload that
    /// ignored it ran on with nothing left to find it by.
    #[tokio::test]
    async fn a_process_ignoring_sigterm_is_sigkilled_after_its_grace_and_reaped() {
        let child = shell_ready("trap '' TERM; echo ready; exec sleep 30").await;
        let pid = child.id().expect("running");
        let grace = Duration::from_millis(400);

        let begun = std::time::Instant::now();
        let reaped = terminate(child, grace).await;
        let took = begun.elapsed();

        let Reaped::Status(status) = reaped else {
            panic!("the wait must succeed: {reaped:?}");
        };
        assert_eq!(
            run_state_of(Some(status)),
            RunState::Exited(ExitDisposition::Signal(9)),
            "SIGTERM was ignored, so only SIGKILL could end it"
        );
        assert!(
            took >= grace,
            "SIGKILL came before the grace period ran out: {took:?} < {grace:?}"
        );
        assert!(
            took < grace + Duration::from_secs(3),
            "the escalation must follow the grace period promptly: {took:?}"
        );
        assert!(
            !process_is_alive(pid),
            "the process must be reaped, not left a zombie"
        );
    }

    /// The positive control for the escalation: a workload that honours
    /// SIGTERM ends on SIGTERM, well inside its grace period. A stop that
    /// sent SIGKILL first would pass the test above and fail this one.
    #[tokio::test]
    async fn a_process_honouring_sigterm_ends_on_sigterm_inside_its_grace() {
        let child = shell_ready("echo ready; exec sleep 30").await;
        let pid = child.id().expect("running");

        let begun = std::time::Instant::now();
        let reaped = terminate(child, Duration::from_secs(20)).await;

        let Reaped::Status(status) = reaped else {
            panic!("the wait must succeed: {reaped:?}");
        };
        assert_eq!(
            run_state_of(Some(status)),
            RunState::Exited(ExitDisposition::Signal(15))
        );
        assert!(
            begun.elapsed() < Duration::from_secs(5),
            "a SIGTERM-honouring process must not wait out its grace period"
        );
        assert!(!process_is_alive(pid), "reaped");
    }

    #[test]
    fn the_wait_status_decodes_into_code_signal_running_or_unknown() {
        use std::os::unix::process::ExitStatusExt;
        // Not reaped yet: still up.
        assert_eq!(run_state_of(None), RunState::Running);
        // exit(0) and exit(3): raw status carries the code in bits 8..16.
        assert_eq!(
            run_state_of(Some(std::process::ExitStatus::from_raw(0))),
            RunState::Exited(ExitDisposition::Code(0))
        );
        assert_eq!(
            run_state_of(Some(std::process::ExitStatus::from_raw(3 << 8))),
            RunState::Exited(ExitDisposition::Code(3))
        );
        // Terminated by SIGTERM: the signal lives in the low 7 bits.
        assert_eq!(
            run_state_of(Some(std::process::ExitStatus::from_raw(15))),
            RunState::Exited(ExitDisposition::Signal(15))
        );
        // A STOPPED status (0x7f low byte) is neither an exit nor a kill.
        // It must not be guessed into either.
        assert_eq!(
            run_state_of(Some(std::process::ExitStatus::from_raw((19 << 8) | 0x7f))),
            RunState::Unknown
        );
    }
}
