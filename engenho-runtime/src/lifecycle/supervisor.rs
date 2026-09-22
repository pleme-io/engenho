//! The supervisor: the daemon above the runtime.
//!
//! `engenho daemon` used to be the runtime: boot it, wait for a signal, shut
//! it down, exit — and exit on any boot error, handing the retry to a service
//! manager that knows nothing about why. The supervisor stays up instead. It
//! owns the [`DaemonLifecycle`] machine and one [`Slot`] for the runtime, and
//! runs a single loop over:
//!
//! * requests from [`SupervisorHandle`]s (start, stop, restart, retry, exit);
//! * the running boot's progress and outcome;
//! * the runtime's shutdown, when one is draining;
//! * a dead child of the running runtime;
//! * the retry timer of a failed boot;
//! * a change to the declared configuration file.
//!
//! Every input becomes a [`LifecycleEvent`]; the machine decides; the
//! supervisor performs the one [`LifecycleEffect`] it returns. Reads never
//! wait on the loop: every transition publishes a [`Snapshot`] on a watch
//! channel, so the lifecycle can be read while a boot waits for leadership or
//! a drain waits out its grace.
//!
//! The runtime's slot is typed so that a boot can only start from
//! [`Slot::Idle`] — a state entered only when this process holds no store:
//! at startup, after a boot that never opened it, or with the
//! [`StoreReleased`] a boot's unwind or a shutdown minted.

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use engenho_config::{ConfigError, EngenhoConfig, OverrideLayer, ProvenanceMap};
use engenho_kubelet::ContainerRuntime;
use engenho_serve::{StopHandle, stop_channel};
use engenho_store::data_dir_lock::{DataDirLock, LockError};
use engenho_substrate::maquina::{MachineError, MachineRunner};
use engenho_substrate::relogio::WallClock;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info, warn};

use super::control_dir::ControlDir;
use super::journal::{
    BootAttempt, BootJournal, IdentityRecord, LastSeen, PhaseRecord, PreviousRun, RunMarker,
};
use super::machine::{
    DaemonLifecycle, ExitIntent, FailureReport, Lifecycle, LifecycleEffect, LifecycleEvent,
    LifecycleState, PendingApply, Refused, RefusedBecause, RetryClass, StoreOutcome,
};
use crate::boot::{BootKind, BootPhase, BootProgress, BootRecorder, FailureClass, Timestamp};
use crate::child::{Child, ChildState, DeadChild, Death, DeathCause, RespawnError, Respawned};
use crate::control::apply::ApplyEffect;
use crate::control::overrides::OverrideStore;
use crate::control::reinit::{self, MovedAside, Reinit, ReinitOp};
use crate::control::ring::Ring;
use crate::error::RuntimeError;
use crate::layout::Area;
use crate::publish::PublishRecord;
use crate::release::{BootFailed, BootUnwind, StoreReleased};
use crate::runtime::Runtime;

/// The declared configuration folded with an override tier (or with none).
pub type Fold =
    Arc<dyn Fn(Option<&OverrideLayer>) -> Result<ResolvedConfig, ConfigError> + Send + Sync>;

/// Resolves the configuration a boot runs on. Resolved once per boot, inside
/// its [`BootPhase::ResolveConfig`], so a configuration that does not
/// resolve is a failed boot the control plane can report, not a dead
/// process. The control plane resolves it too — to show the configuration,
/// and to plan a change against a candidate override set before making it.
#[derive(Clone)]
pub struct ConfigSource {
    fold: Fold,
    overrides: Option<Arc<OverrideStore>>,
}

impl ConfigSource {
    /// A source with no override tier.
    pub fn new(
        resolve: impl Fn() -> Result<ResolvedConfig, ConfigError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            fold: Arc::new(move |_| resolve()),
            overrides: None,
        }
    }

    /// A source that folds `overrides` over the declared configuration.
    pub fn layered(
        fold: impl Fn(Option<&OverrideLayer>) -> Result<ResolvedConfig, ConfigError>
        + Send
        + Sync
        + 'static,
        overrides: Arc<OverrideStore>,
    ) -> Self {
        Self {
            fold: Arc::new(fold),
            overrides: Some(overrides),
        }
    }

    /// The configuration with the overrides in force now.
    ///
    /// # Errors
    ///
    /// The configuration does not resolve, or the override tier cannot be
    /// read.
    pub fn resolve(&self) -> Result<ResolvedConfig, ConfigError> {
        let layer = self.overrides.as_ref().map(|o| o.layer()).transpose()?;
        (self.fold)(layer.as_ref())
    }

    /// The configuration with `overrides` in place of the ones in force.
    ///
    /// # Errors
    ///
    /// The configuration does not resolve.
    pub fn resolve_with(
        &self,
        overrides: Option<&OverrideLayer>,
    ) -> Result<ResolvedConfig, ConfigError> {
        (self.fold)(overrides)
    }

    /// The override tier, when this source has one.
    #[must_use]
    pub fn overrides(&self) -> Option<&Arc<OverrideStore>> {
        self.overrides.as_ref()
    }
}

/// A resolved configuration, and which tier gave each leaf its value.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// The configuration.
    pub config: EngenhoConfig,
    /// Per-leaf provenance; `None` from a source that does not track it.
    pub provenance: Option<ProvenanceMap>,
}

impl ResolvedConfig {
    /// A configuration with no provenance (a source that built it by hand).
    #[must_use]
    pub const fn untracked(config: EngenhoConfig) -> Self {
        Self {
            config,
            provenance: None,
        }
    }
}

/// How many lifecycle transitions the supervisor keeps in memory.
const HISTORY_CAP: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(cap) => cap,
    None => NonZeroUsize::MIN,
};

/// What a [`Supervisor`] runs.
pub struct SupervisorConfig {
    /// The data directory, fixed for the life of the process
    /// ([`super::ControlBootstrap`]).
    pub data_dir: PathBuf,
    /// Where each boot's configuration comes from.
    pub source: ConfigSource,
    /// The container backend every boot drives; built from the
    /// configuration when absent.
    pub backend: Option<Arc<dyn ContainerRuntime>>,
    /// The declared configuration file, watched so a boot held on the
    /// configuration is retried when it changes.
    pub declared: Option<PathBuf>,
}

/// Whether a stop outlives the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hold {
    /// A relaunched daemon boots as usual.
    None,
    /// A relaunched daemon comes up Stopped (the `control/hold` marker).
    AcrossRelaunch,
}

/// The daemon process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonInfo {
    /// The binary's version.
    pub version: String,
    /// The process id.
    pub pid: u32,
    /// When the daemon started.
    pub started_at: Timestamp,
}

/// Everything the supervisor publishes after each transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Where the daemon is.
    pub lifecycle: LifecycleState,
    /// How many times it has released the store.
    pub epoch: u64,
    /// The boot journal, oldest first.
    pub attempts: Vec<BootAttempt>,
    /// How the previous process ended.
    pub previous_run: PreviousRun,
    /// The identity recorded at first boot, if any.
    pub identity: Option<IdentityRecord>,
    /// The process.
    pub daemon: DaemonInfo,
    /// The data directory.
    pub data_dir: PathBuf,
}

/// A request the lifecycle accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Accepted {
    /// When.
    pub accepted_at: Timestamp,
    /// The lifecycle right after.
    pub lifecycle: LifecycleState,
}

/// A stop that finished.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StopDone {
    /// The epoch the daemon rests in.
    pub epoch: u64,
    /// The lifecycle once the drain finished (`stopped`, or `wedged` when the
    /// store could not be released).
    pub lifecycle: LifecycleState,
}

/// Why a request did not get an answer from the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandError {
    /// The lifecycle refused it.
    #[error(transparent)]
    Refused(#[from] Refused),
    /// The supervisor has ended.
    #[error("the supervisor has ended")]
    Gone,
}

/// A supervisor that could not start.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// `data_dir/control` could not be created.
    #[error("cannot create the control directory {}: {source}", path.display())]
    ControlDir {
        /// The directory.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
    /// Another daemon runs over this data directory.
    #[error(transparent)]
    Locked(#[from] LockError),
}

type Reply<T> = oneshot::Sender<Result<T, Refused>>;

enum Request {
    Start(Reply<Accepted>),
    Stop(Hold, Reply<StopDone>),
    Restart(Reply<Accepted>),
    Retry(Reply<Accepted>),
    Exit(ExitIntent, Reply<Accepted>),
    Inspect(oneshot::Sender<Inspection>),
    Reconfigure {
        restart_now: bool,
        reply: oneshot::Sender<Result<Reconfigured, ReconfigureError>>,
    },
    Republish(oneshot::Sender<Result<Vec<PublishRecord>, ReconfigureError>>),
    RestartChild {
        child: Child,
        reply: oneshot::Sender<Result<Respawned, RestartChildError>>,
    },
    Reinit {
        reinit: Reinit,
        epoch: Option<u64>,
        reply: oneshot::Sender<Result<MovedAside, ReinitRefused>>,
    },
}

/// Why the supervisor did not re-initialize ([`SupervisorHandle::reinit`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReinitRefused {
    /// It needs the runtime stopped (or its boot failed), and it is not.
    #[error("the runtime must be stopped first")]
    NotStopped,
    /// The runtime ran since the challenge was prepared.
    #[error("the runtime ran since the challenge was prepared (stop epoch {bound}, now {now})")]
    EpochMoved {
        /// The epoch it was prepared in.
        bound: u64,
        /// The epoch now.
        now: u64,
    },
    /// Something still holds the store.
    #[error("the store is held: {0}")]
    StoreHeld(String),
    /// It ran and failed; the data directory is as the reason says.
    #[error("{0}")]
    Failed(String),
    /// The supervisor has ended.
    #[error("the supervisor has ended")]
    Gone,
}

/// What the supervisor did with a configuration change
/// ([`SupervisorHandle::reconfigure`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconfigured {
    /// The runtime is up: it took what it can in place.
    Running {
        /// Whether that republished the kubeconfigs.
        republished: bool,
        /// The children it spawned, stopped or rebuilt to follow the change.
        respawned: Vec<Child>,
        /// Every leaf it reflects only after a restart.
        pending: Vec<engenho_config::LeafPath>,
        /// Whether a restart was asked for, and started.
        restarted: bool,
    },
    /// The last boot failed and was held: it is retried on the new
    /// configuration.
    Retrying,
    /// No runtime is up (or one is booting or stopping): the next boot reads
    /// the configuration afresh.
    NotRunning,
}

/// Why the running runtime did not take a configuration change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReconfigureError {
    /// The configuration does not resolve (it changed since it was planned).
    #[error("the configuration does not resolve: {0}")]
    Config(String),
    /// The runtime refused or failed to apply it.
    #[error("the running runtime could not apply it: {0}")]
    Runtime(String),
    /// No runtime is up.
    #[error("no runtime is running")]
    NotRunning,
    /// The supervisor has ended.
    #[error("the supervisor has ended")]
    Gone,
}

/// Why a child was not restarted ([`SupervisorHandle::restart_child`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestartChildError {
    /// The runtime refused.
    #[error(transparent)]
    Refused(#[from] RespawnError),
    /// No runtime is up.
    #[error("no runtime is running")]
    NotRunning,
    /// The supervisor has ended.
    #[error("the supervisor has ended")]
    Gone,
}

/// Who holds the store's lock, as far as the supervisor can tell without
/// disturbing a boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreLock {
    /// This daemon: a runtime, a boot or a drain has it open.
    HeldByThisDaemon,
    /// Another process.
    HeldByOtherProcess,
    /// Nobody: a boot can open it.
    Free,
    /// The lock file could not be examined.
    Unknown,
}

/// What the running runtime is doing, read in the supervisor's loop.
#[derive(Debug, Clone)]
pub struct RuntimeFacts {
    /// The configuration it booted on.
    pub config: EngenhoConfig,
    /// Which tier gave each leaf its value, if the source tracked it.
    pub provenance: Option<ProvenanceMap>,
    /// BLAKE3 of the declared file when the boot read it.
    pub declared_digest: Option<String>,
    /// The apiserver's bound address.
    pub apiserver_addr: std::net::SocketAddr,
    /// Created, resumed, or in memory.
    pub boot_kind: BootKind,
    /// The store's current revision.
    pub revision: u64,
    /// Whether this node leads.
    pub leader: bool,
    /// Every spawned child.
    pub children: Vec<ChildFact>,
    /// Where the boot's kubeconfigs went.
    pub publish: Vec<PublishRecord>,
    /// The SANs the apiserver's certificate carries; `None` with TLS off.
    pub server_sans: Option<Vec<String>>,
}

/// One spawned child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildFact {
    /// Which.
    pub child: Child,
    /// Running, or how it ended.
    pub state: ChildState,
    /// When it was spawned.
    pub spawned_at: Timestamp,
    /// When it ended.
    pub ended_at: Option<Timestamp>,
    /// How many times it has been spawned again since the first.
    pub generation: u64,
    /// How its latest task that ended ended, across respawns.
    pub last_death: Option<Death>,
}

/// Everything [`SupervisorHandle::inspect`] reads in the loop.
#[derive(Debug, Clone)]
pub struct Inspection {
    /// The runtime, when it is up.
    pub runtime: Option<RuntimeFacts>,
    /// Who holds the store.
    pub store_lock: StoreLock,
    /// Whether the durable store's directory exists.
    pub store_present: bool,
}

/// Something the control plane's event stream reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonEvent {
    /// The lifecycle moved.
    Lifecycle(LifecycleState),
    /// A boot phase started, or ended.
    BootPhase {
        /// Which boot.
        attempt: NonZeroU32,
        /// The phase and how it is going.
        record: PhaseRecord,
    },
    /// A child of the running runtime died.
    ChildDied {
        /// Which.
        child: Child,
        /// How.
        cause: DeathCause,
    },
    /// A child of the running runtime was spawned again.
    ChildRespawned {
        /// Which.
        child: Child,
        /// Its generation now.
        generation: u64,
    },
    /// A confirm-gated re-initialization ran.
    ReinitExecuted {
        /// Which.
        operation: ReinitOp,
    },
    /// A configuration change was applied.
    ConfigApplied {
        /// The override set's generation after it.
        generation: u64,
        /// The leaves it changed.
        leaves: Vec<engenho_config::LeafPath>,
        /// What it did.
        effect: ApplyEffect,
    },
    /// A kubeconfig was published (again) while the runtime ran.
    KubeconfigPublished {
        /// Where, and how it went.
        record: PublishRecord,
    },
}

/// How many events the ring keeps.
pub const EVENTS: usize = 4096;

/// Talks to a running [`Supervisor`]. Cheap to clone.
#[derive(Clone)]
pub struct SupervisorHandle {
    requests: mpsc::Sender<Request>,
    snapshot: watch::Receiver<Snapshot>,
    events: Arc<Ring<DaemonEvent>>,
    declared_changes: watch::Receiver<u64>,
}

impl SupervisorHandle {
    /// A counter bumped by every change to the declared file the supervisor
    /// watches — for what else reads the file (the remote listener's pins)
    /// to follow it without a second watcher.
    #[must_use]
    pub fn declared_changes(&self) -> watch::Receiver<u64> {
        self.declared_changes.clone()
    }

    /// The event stream.
    #[must_use]
    pub fn events(&self) -> &Arc<Ring<DaemonEvent>> {
        &self.events
    }

    /// Read what only the loop can: the running runtime's children and
    /// store, and who holds the store's lock. Waits for the loop (which is
    /// never busy for long: boots and drains run beside it).
    ///
    /// # Errors
    ///
    /// [`CommandError::Gone`] when the supervisor has ended.
    pub async fn inspect(&self) -> Result<Inspection, CommandError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Inspect(reply))
            .await
            .map_err(|_| CommandError::Gone)?;
        answer.await.map_err(|_| CommandError::Gone)
    }

    /// The latest snapshot. Never waits on the supervisor's loop.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.borrow().clone()
    }

    /// A receiver that sees every snapshot from now on.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<Snapshot> {
        self.snapshot.clone()
    }

    /// Boot a stopped runtime (and clear the hold marker).
    ///
    /// # Errors
    ///
    /// [`CommandError::Refused`] unless the runtime is stopped.
    pub async fn start(&self) -> Result<Accepted, CommandError> {
        self.ask(Request::Start).await
    }

    /// Stop the runtime and wait for its store to be released. The daemon
    /// stays up.
    ///
    /// # Errors
    ///
    /// [`CommandError::Refused`] unless it is booting, running or failed.
    pub async fn stop(&self, hold: Hold) -> Result<StopDone, CommandError> {
        self.ask(|reply| Request::Stop(hold, reply)).await
    }

    /// Stop the running runtime and boot it again.
    ///
    /// # Errors
    ///
    /// [`CommandError::Refused`] unless it is booting or running.
    pub async fn restart(&self) -> Result<Accepted, CommandError> {
        self.ask(Request::Restart).await
    }

    /// Retry a failed boot now.
    ///
    /// # Errors
    ///
    /// [`CommandError::Refused`] unless the last boot failed.
    pub async fn retry(&self) -> Result<Accepted, CommandError> {
        self.ask(Request::Retry).await
    }

    /// End the process, draining the runtime first if it is up.
    ///
    /// # Errors
    ///
    /// [`CommandError::Gone`] when the supervisor has already ended.
    pub async fn exit(&self, intent: ExitIntent) -> Result<Accepted, CommandError> {
        self.ask(|reply| Request::Exit(intent, reply)).await
    }

    /// Bring the runtime to the configuration as it resolves now: a running
    /// runtime takes what it can in place (and restarts for the rest when
    /// `restart_now`); a held failed boot is retried; otherwise the next boot
    /// reads it.
    ///
    /// # Errors
    ///
    /// [`ReconfigureError`].
    pub async fn reconfigure(&self, restart_now: bool) -> Result<Reconfigured, ReconfigureError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Reconfigure { restart_now, reply })
            .await
            .map_err(|_| ReconfigureError::Gone)?;
        answer.await.map_err(|_| ReconfigureError::Gone)?
    }

    /// Publish the running runtime's kubeconfigs again.
    ///
    /// # Errors
    ///
    /// [`ReconfigureError::NotRunning`] when no runtime is up.
    pub async fn republish(&self) -> Result<Vec<PublishRecord>, ReconfigureError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Republish(reply))
            .await
            .map_err(|_| ReconfigureError::Gone)?;
        answer.await.map_err(|_| ReconfigureError::Gone)?
    }

    /// Build a child of the running runtime again, with its dependents
    /// ([`Runtime::respawn`]).
    ///
    /// # Errors
    ///
    /// [`RestartChildError`].
    pub async fn restart_child(&self, child: Child) -> Result<Respawned, RestartChildError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::RestartChild { child, reply })
            .await
            .map_err(|_| RestartChildError::Gone)?;
        answer.await.map_err(|_| RestartChildError::Gone)?
    }

    /// Re-initialize the data directory ([`Reinit`]): only while the runtime
    /// is stopped in `epoch` when the operation needs it stopped, with the
    /// store's lock held throughout. The caller has checked the confirmation.
    ///
    /// # Errors
    ///
    /// [`ReinitRefused`].
    pub async fn reinit(
        &self,
        reinit: Reinit,
        epoch: Option<u64>,
    ) -> Result<MovedAside, ReinitRefused> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Reinit {
                reinit,
                epoch,
                reply,
            })
            .await
            .map_err(|_| ReinitRefused::Gone)?;
        answer.await.map_err(|_| ReinitRefused::Gone)?
    }

    async fn ask<T>(&self, request: impl FnOnce(Reply<T>) -> Request) -> Result<T, CommandError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(request(reply))
            .await
            .map_err(|_| CommandError::Gone)?;
        answer
            .await
            .map_err(|_| CommandError::Gone)?
            .map_err(CommandError::Refused)
    }
}

/// Where the runtime is, from the supervisor's side.
enum Slot {
    /// This process holds no store. `Some` when a boot's unwind or a
    /// shutdown proved the store released; `None` at startup, or after a
    /// boot that never opened it.
    Idle(Option<StoreReleased>),
    /// A boot is running.
    Booting {
        task: JoinHandle<BootOutcome>,
        cancel: StopHandle,
        progress: mpsc::UnboundedReceiver<BootProgress>,
    },
    /// The runtime is up.
    Up(Box<Running>),
    /// The runtime is shutting down.
    Draining(JoinHandle<Result<StoreReleased, RuntimeError>>),
    /// Something holds the store that this process cannot release.
    Wedged,
}

/// A running runtime, and what its boot read.
struct Running {
    runtime: Runtime,
    provenance: Option<ProvenanceMap>,
    declared_digest: Option<String>,
}

/// What a supervised boot produced.
struct BootOutcome {
    result: Result<Runtime, BootFailed>,
    provenance: Option<ProvenanceMap>,
    declared_digest: Option<String>,
}

enum SlotEvent {
    Progress(BootProgress),
    /// Boxed: a `Runtime` is large, and this is one event among small ones.
    BootDone(Box<Result<BootOutcome, JoinError>>),
    Drained(Result<Result<StoreReleased, RuntimeError>, JoinError>),
    ChildDied(DeadChild),
}

/// The next thing the slot has to say. Pends forever when it has nothing
/// to wait on. Cancel-safe: every future it awaits is.
async fn slot_event(slot: &mut Slot) -> SlotEvent {
    match slot {
        Slot::Booting { task, progress, .. } => tokio::select! {
            // Every phase report precedes the outcome it led to.
            biased;
            Some(report) = progress.recv() => SlotEvent::Progress(report),
            done = task => SlotEvent::BootDone(Box::new(done)),
        },
        // Logged at ERROR by the runtime as it returns; respawned only when
        // an operator asks (`restart_child`).
        Slot::Up(running) => SlotEvent::ChildDied(running.runtime.next_dead_child().await),
        Slot::Draining(task) => SlotEvent::Drained(task.await),
        Slot::Idle(_) | Slot::Wedged => std::future::pending().await,
    }
}

async fn retry_due(timer: &mut Option<(NonZeroU32, Pin<Box<tokio::time::Sleep>>)>) {
    match timer {
        Some((_, sleep)) => sleep.as_mut().await,
        None => std::future::pending().await,
    }
}

/// The daemon above the runtime. See the module docs.
pub struct Supervisor {
    data_dir: PathBuf,
    control: ControlDir,
    /// Held for the life of the supervisor: one daemon per data directory.
    _lock: DataDirLock,
    source: ConfigSource,
    backend: Option<Arc<dyn ContainerRuntime>>,
    machine: MachineRunner<DaemonLifecycle>,
    slot: Slot,
    journal: BootJournal,
    marker: RunMarker,
    previous_run: PreviousRun,
    identity: Option<IdentityRecord>,
    daemon: DaemonInfo,
    retry: Option<(NonZeroU32, Pin<Box<tokio::time::Sleep>>)>,
    pending_stops: Vec<Reply<StopDone>>,
    requests: mpsc::Receiver<Request>,
    snapshot: watch::Sender<Snapshot>,
    events: Arc<Ring<DaemonEvent>>,
    declared: mpsc::UnboundedReceiver<()>,
    declared_path: Option<PathBuf>,
    /// Kept alive for as long as the supervisor watches the declared file.
    _watcher: Option<shikumi::ConfigWatcher>,
}

impl Supervisor {
    /// Take the data directory's daemon lock and read what the previous
    /// process left. Nothing boots until [`Self::run`].
    ///
    /// # Errors
    ///
    /// [`SupervisorError::Locked`] when another daemon runs over the data
    /// directory; [`SupervisorError::ControlDir`] when its control directory
    /// cannot be created.
    pub fn new(config: SupervisorConfig) -> Result<(Self, SupervisorHandle), SupervisorError> {
        let SupervisorConfig {
            data_dir,
            source,
            backend,
            declared,
        } = config;
        let control = ControlDir::under(&data_dir);
        control
            .create()
            .map_err(|source| SupervisorError::ControlDir {
                path: control.root().to_path_buf(),
                source,
            })?;
        let lock = control.lock()?;

        let started_at = Timestamp::now();
        let previous_run = PreviousRun::from_marker(control.read_run().as_ref());
        let marker = RunMarker::Released { at: started_at };
        if let Err(err) = control.write_run(&marker) {
            warn!(error = %err, "cannot record the run marker");
        }
        let machine = MachineRunner::<DaemonLifecycle>::from_state(
            Lifecycle::at(started_at),
            Arc::new(WallClock),
        )
        .with_history_cap(HISTORY_CAP);
        let daemon = DaemonInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            pid: std::process::id(),
            started_at,
        };
        let (declared_tx, declared_rx) = mpsc::unbounded_channel();
        let (declared_count, declared_changes) = watch::channel(0u64);
        let watcher = declared
            .as_deref()
            .and_then(|path| watch_declared(path, declared_tx, declared_count));

        let journal = control.read_journal();
        let identity = control.read_identity();
        let (requests_tx, requests) = mpsc::channel(32);
        let first = Snapshot {
            lifecycle: machine.state().state.clone(),
            epoch: 0,
            attempts: journal.attempts(),
            previous_run: previous_run.clone(),
            identity: identity.clone(),
            daemon: daemon.clone(),
            data_dir: data_dir.clone(),
        };
        let (snapshot, snapshot_rx) = watch::channel(first);
        let events = Arc::new(Ring::new(EVENTS));
        info!(
            data_dir = %data_dir.display(),
            previous_run = ?previous_run,
            "supervisor up"
        );
        let supervisor = Self {
            data_dir,
            control,
            _lock: lock,
            source,
            backend,
            machine,
            slot: Slot::Idle(None),
            journal,
            marker,
            previous_run,
            identity,
            daemon,
            retry: None,
            pending_stops: Vec::new(),
            requests,
            snapshot,
            events: Arc::clone(&events),
            declared: declared_rx,
            declared_path: declared,
            _watcher: watcher,
        };
        let handle = SupervisorHandle {
            requests: requests_tx,
            snapshot: snapshot_rx,
            events,
            declared_changes,
        };
        Ok((supervisor, handle))
    }

    /// Run until the lifecycle exits; return how the process should end.
    ///
    /// Boots at once, unless the hold marker is present, in which case the
    /// daemon rests Stopped until it is asked to start.
    pub async fn run(mut self) -> ExitIntent {
        let at = Timestamp::now();
        let first = if self.control.is_held() {
            info!("hold marker present: staying stopped until started");
            LifecycleEvent::HeldAtStartup { at }
        } else {
            LifecycleEvent::Start { at }
        };
        self.apply_logged(first);

        let mut requests_open = true;
        loop {
            if let LifecycleState::Exiting { intent } = self.machine.state().state {
                self.finish();
                return intent;
            }
            tokio::select! {
                request = self.requests.recv(), if requests_open => match request {
                    // Answered here, awaiting the runtime: they read or
                    // change its children, which only the loop holds.
                    Some(Request::Inspect(reply)) => {
                        let _ = reply.send(self.inspect().await);
                    }
                    Some(Request::Reconfigure { restart_now, reply }) => {
                        let _ = reply.send(self.reconfigure(restart_now, Timestamp::now()).await);
                    }
                    Some(Request::RestartChild { child, reply }) => {
                        let _ = reply.send(self.restart_child(child).await);
                    }
                    Some(request) => self.on_request(request),
                    None => requests_open = false,
                },
                event = slot_event(&mut self.slot) => self.on_slot_event(event),
                () = retry_due(&mut self.retry) => {
                    self.retry = None;
                    self.apply_logged(LifecycleEvent::RetryDue { at: Timestamp::now() });
                }
                Some(()) = self.declared.recv() => {
                    // Refused (and logged at debug) unless a boot is failed:
                    // a change while running is drift, not a trigger.
                    self.apply_logged(LifecycleEvent::ConfigChanged { at: Timestamp::now() });
                }
            }
        }
    }

    fn on_request(&mut self, request: Request) {
        let at = Timestamp::now();
        match request {
            Request::Start(reply) => {
                let result = self.apply(LifecycleEvent::Start { at });
                if result.is_ok() {
                    self.write_hold(Hold::None, at);
                }
                let _ = reply.send(result.map(|()| self.accepted(at)));
            }
            Request::Stop(hold, reply) => match self.apply(LifecycleEvent::Stop { at }) {
                Ok(()) => {
                    self.write_hold(hold, at);
                    self.pending_stops.push(reply);
                    self.answer_stops();
                }
                Err(refused) => {
                    let _ = reply.send(Err(refused));
                }
            },
            Request::Restart(reply) => {
                let result = self.apply(LifecycleEvent::Restart { at });
                let _ = reply.send(result.map(|()| self.accepted(at)));
            }
            Request::Retry(reply) => {
                let result = self.apply(LifecycleEvent::Retry { at });
                let _ = reply.send(result.map(|()| self.accepted(at)));
            }
            Request::Exit(intent, reply) => {
                let result = self.apply(LifecycleEvent::Exit { intent });
                let _ = reply.send(result.map(|()| self.accepted(at)));
            }
            Request::Republish(reply) => {
                let _ = reply.send(self.republish());
            }
            Request::Reinit {
                reinit,
                epoch,
                reply,
            } => {
                let _ = reply.send(self.reinit(reinit, epoch, at));
            }
            // Answered by the loop, which awaits the runtime.
            Request::Inspect(_) | Request::Reconfigure { .. } | Request::RestartChild { .. } => {}
        }
    }

    async fn restart_child(&mut self, child: Child) -> Result<Respawned, RestartChildError> {
        let Slot::Up(running) = &mut self.slot else {
            return Err(RestartChildError::NotRunning);
        };
        let respawned = running.runtime.respawn(child).await?;
        self.report_children(&respawned.rebuilt, &respawned.died);
        Ok(respawned)
    }

    /// Put `changed` children (the ones now running) and `died` ones on the
    /// event stream.
    fn report_children(&self, changed: &[Child], died: &[DeadChild]) {
        for dead in died {
            self.events.push(DaemonEvent::ChildDied {
                child: dead.child,
                cause: dead.cause,
            });
        }
        let Slot::Up(running) = &self.slot else {
            return;
        };
        let children = running.runtime.children();
        for &child in changed {
            if let Some(handle) = children
                .get(child)
                .filter(|h| h.state() == ChildState::Running)
            {
                self.events.push(DaemonEvent::ChildRespawned {
                    child,
                    generation: handle.generation(),
                });
            }
        }
    }

    async fn reconfigure(
        &mut self,
        restart_now: bool,
        at: Timestamp,
    ) -> Result<Reconfigured, ReconfigureError> {
        match &self.machine.state().state {
            LifecycleState::Running { .. } => {}
            LifecycleState::Failed {
                retry: RetryClass::Hold,
                ..
            } => {
                self.apply_logged(LifecycleEvent::ConfigChanged { at });
                return Ok(Reconfigured::Retrying);
            }
            _ => return Ok(Reconfigured::NotRunning),
        }
        let resolved = self
            .source
            .resolve()
            .map_err(|e| ReconfigureError::Config(e.to_string()))?;
        let Slot::Up(running) = &mut self.slot else {
            return Ok(Reconfigured::NotRunning);
        };
        let adopted = running
            .runtime
            .adopt(&resolved.config)
            .await
            .map_err(|e| ReconfigureError::Runtime(e.to_string()))?;
        let pending =
            crate::control::apply::restart_pending(running.runtime.config(), &resolved.config);
        running.provenance = resolved.provenance;

        let republished = adopted.published.is_some();
        for record in adopted.published.into_iter().flatten() {
            self.events
                .push(DaemonEvent::KubeconfigPublished { record });
        }
        let respawned = adopted.children.changed;
        self.report_children(&respawned, &adopted.children.died);
        self.apply_logged(LifecycleEvent::ConfigApplied {
            pending: if pending.is_empty() {
                PendingApply::InSync
            } else {
                PendingApply::RestartNeeded {
                    leaves: pending.iter().map(ToString::to_string).collect(),
                }
            },
        });
        let restarted = restart_now
            && !pending.is_empty()
            && self.apply(LifecycleEvent::Restart { at }).is_ok();
        if restarted {
            info!(leaves = ?pending, "restarting the runtime for a configuration change");
        }
        Ok(Reconfigured::Running {
            republished,
            respawned,
            pending,
            restarted,
        })
    }

    /// Run `reinit` here, in the loop, so no start, retry or boot can begin
    /// between the checks and the move.
    fn reinit(
        &mut self,
        reinit: Reinit,
        epoch: Option<u64>,
        at: Timestamp,
    ) -> Result<MovedAside, ReinitRefused> {
        // Held until the move is done: nothing opens the store meanwhile.
        let _store_lock = if reinit.needs_stopped() {
            self.resting_in(epoch)?
        } else {
            None
        };
        let moved = reinit::move_aside(&self.data_dir, reinit, at)
            .map_err(|e| ReinitRefused::Failed(e.to_string()))?;
        if reinit == Reinit::RotateAdminToken {
            crate::runtime::load_or_generate_admin_token(&self.data_dir).map_err(|e| {
                ReinitRefused::Failed(
                    [
                        "the old token is in ",
                        moved.attic.display().to_string().as_str(),
                        ", and a new one could not be written: ",
                        e.to_string().as_str(),
                    ]
                    .concat(),
                )
            })?;
        }
        info!(
            operation = reinit.op().name(),
            attic = %moved.attic.display(),
            moved = ?moved.moved,
            "re-initialized"
        );
        self.events.push(DaemonEvent::ReinitExecuted {
            operation: reinit.op(),
        });
        Ok(moved)
    }

    /// The runtime rests — stopped, or its boot failed, with nothing booting
    /// or draining — in `epoch` (any, when `None`): then the store's lock,
    /// taken, when there is a store to lock.
    fn resting_in(&self, epoch: Option<u64>) -> Result<Option<DataDirLock>, ReinitRefused> {
        let resting = matches!(
            self.machine.state().state,
            LifecycleState::Stopped { .. } | LifecycleState::Failed { .. }
        ) && matches!(self.slot, Slot::Idle(_));
        if !resting {
            return Err(ReinitRefused::NotStopped);
        }
        let now = self.machine.state().epoch();
        if let Some(bound) = epoch.filter(|bound| *bound != now) {
            return Err(ReinitRefused::EpochMoved { bound, now });
        }
        let store = Area::Store.path(&self.data_dir);
        if !store.exists() {
            return Ok(None);
        }
        DataDirLock::acquire(&store)
            .map(Some)
            .map_err(|e| ReinitRefused::StoreHeld(e.to_string()))
    }

    fn republish(&mut self) -> Result<Vec<PublishRecord>, ReconfigureError> {
        let Slot::Up(running) = &mut self.slot else {
            return Err(ReconfigureError::NotRunning);
        };
        let records = running
            .runtime
            .republish()
            .map_err(|e| ReconfigureError::Runtime(e.to_string()))?;
        for record in &records {
            self.events.push(DaemonEvent::KubeconfigPublished {
                record: record.clone(),
            });
        }
        Ok(records)
    }

    /// What only the loop can read: the running runtime, and the store's
    /// lock — probed only while no boot or drain could be opening it.
    async fn inspect(&self) -> Inspection {
        let store_dir = self.data_dir.join(crate::runtime::STORE_DIR);
        let store_present = store_dir.exists();
        let (runtime, store_lock) = match &self.slot {
            Slot::Up(running) => {
                let rt = &running.runtime;
                let (revision, leader) = rt.store_position().await;
                let durable = rt.config().runtime.durable;
                let facts = RuntimeFacts {
                    config: rt.config().clone(),
                    provenance: running.provenance.clone(),
                    declared_digest: running.declared_digest.clone(),
                    apiserver_addr: rt.local_addr(),
                    boot_kind: rt.boot_kind(),
                    revision,
                    leader,
                    children: rt
                        .children()
                        .iter()
                        .map(|(child, handle)| ChildFact {
                            child,
                            state: handle.state(),
                            spawned_at: handle.spawned_at(),
                            ended_at: handle.ended_at(),
                            generation: handle.generation(),
                            last_death: handle.last_death(),
                        })
                        .collect(),
                    publish: rt.publish_records().to_vec(),
                    server_sans: rt.server_sans().map(<[String]>::to_vec),
                };
                let lock = if durable {
                    StoreLock::HeldByThisDaemon
                } else {
                    StoreLock::Free
                };
                (Some(facts), lock)
            }
            Slot::Booting { .. } | Slot::Draining(_) | Slot::Wedged => {
                (None, StoreLock::HeldByThisDaemon)
            }
            // Nothing of ours can be opening it, and a boot starts only
            // from this loop: the probe cannot race one.
            Slot::Idle(_) => (
                None,
                match DataDirLock::acquire(&store_dir) {
                    Ok(_) => StoreLock::Free,
                    Err(LockError::Held { .. }) => StoreLock::HeldByOtherProcess,
                    Err(LockError::Unusable { .. }) => StoreLock::Unknown,
                },
            ),
        };
        Inspection {
            runtime,
            store_lock,
            store_present,
        }
    }

    fn on_slot_event(&mut self, event: SlotEvent) {
        match event {
            SlotEvent::Progress(BootProgress::Entered { phase, at }) => {
                self.journal.entered(phase, at);
                self.persist_journal();
                self.publish_phases(2);
                self.apply_logged(LifecycleEvent::Phase { phase, at });
            }
            SlotEvent::Progress(BootProgress::Kind(kind)) => {
                self.journal.observed(kind);
                self.persist_journal();
            }
            SlotEvent::BootDone(done) => self.on_boot_done(*done),
            SlotEvent::Drained(done) => self.on_drained(done),
            SlotEvent::ChildDied(dead) => {
                self.events.push(DaemonEvent::ChildDied {
                    child: dead.child,
                    cause: dead.cause,
                });
            }
        }
    }

    /// Put the latest attempt's last `n` phase records on the event stream:
    /// the phase just entered, and the one it ended.
    fn publish_phases(&self, n: usize) {
        let Some(latest) = self.journal.latest() else {
            return;
        };
        let start = latest.phases.len().saturating_sub(n);
        for record in &latest.phases[start..] {
            self.events.push(DaemonEvent::BootPhase {
                attempt: latest.attempt,
                record: record.clone(),
            });
        }
    }

    fn on_boot_done(&mut self, done: Result<BootOutcome, JoinError>) {
        let at = Timestamp::now();
        let event = match done {
            Ok(BootOutcome {
                result: Ok(runtime),
                provenance,
                declared_digest,
            }) => {
                let apiserver_addr = runtime.local_addr().to_string();
                if runtime.boot_kind() == BootKind::FirstBoot {
                    self.record_identity(runtime.config(), at);
                }
                self.journal.succeeded();
                self.publish_phases(1);
                info!(addr = %apiserver_addr, "engenho up — apiserver bound");
                self.slot = Slot::Up(Box::new(Running {
                    runtime,
                    provenance,
                    declared_digest,
                }));
                LifecycleEvent::Booted { at, apiserver_addr }
            }
            Ok(BootOutcome {
                result:
                    Err(BootFailed {
                        error,
                        phase,
                        unwind,
                    }),
                ..
            }) => {
                let class = FailureClass::of(&error);
                let rendered = error.to_string();
                let store = match unwind {
                    BootUnwind::NeverOpened => {
                        self.slot = Slot::Idle(None);
                        StoreOutcome::Released
                    }
                    BootUnwind::Released(released) => {
                        self.slot = Slot::Idle(Some(released));
                        StoreOutcome::Released
                    }
                    BootUnwind::StillShared { strong_count } => {
                        self.slot = Slot::Wedged;
                        StoreOutcome::Held {
                            cause: format!(
                                "{strong_count} holders of the store remained after the failed boot unwound"
                            ),
                        }
                    }
                    BootUnwind::TerminateFailed(err) => {
                        self.slot = Slot::Wedged;
                        StoreOutcome::Held {
                            cause: format!("terminating the failed boot's store failed: {err}"),
                        }
                    }
                };
                if matches!(error, RuntimeError::BootCancelled { .. }) {
                    self.journal.cancelled();
                    info!(%phase, "boot cancelled");
                } else {
                    self.journal.failed(&rendered);
                    warn!(%phase, ?class, error = %rendered, "boot failed");
                }
                self.publish_phases(1);
                LifecycleEvent::BootFailed {
                    report: FailureReport {
                        phase,
                        error: rendered,
                        at,
                    },
                    class,
                    store,
                }
            }
            Err(join) => {
                let phase = match self.machine.state().state {
                    LifecycleState::Booting { phase, .. } => phase,
                    _ => BootPhase::FIRST,
                };
                let rendered = format!("the boot task ended abnormally: {join}");
                error!(%phase, error = %rendered, "boot failed");
                self.journal.failed(&rendered);
                self.slot = Slot::Wedged;
                LifecycleEvent::BootFailed {
                    report: FailureReport {
                        phase,
                        error: rendered,
                        at,
                    },
                    class: FailureClass::Hold,
                    // Whatever it had opened was dropped, not terminated:
                    // nothing proves the store released.
                    store: StoreOutcome::Held {
                        cause: "the boot task panicked".into(),
                    },
                }
            }
        };
        self.persist_journal();
        self.apply_logged(event);
    }

    fn on_drained(&mut self, done: Result<Result<StoreReleased, RuntimeError>, JoinError>) {
        let store = match done {
            Ok(Ok(released)) => {
                self.slot = Slot::Idle(Some(released));
                info!("engenho stopped cleanly");
                StoreOutcome::Released
            }
            Ok(Err(err)) => {
                self.slot = Slot::Wedged;
                error!(error = %err, "the runtime did not stop cleanly");
                StoreOutcome::Held {
                    cause: err.to_string(),
                }
            }
            Err(join) => {
                self.slot = Slot::Wedged;
                error!(error = %join, "the runtime's stop ended abnormally");
                StoreOutcome::Held {
                    cause: format!("the runtime's stop ended abnormally: {join}"),
                }
            }
        };
        self.apply_logged(LifecycleEvent::Drained {
            at: Timestamp::now(),
            store,
        });
    }

    /// Step the machine and perform what it asks.
    fn apply(&mut self, event: LifecycleEvent) -> Result<(), Refused> {
        let state = self.machine.state().state.name();
        let name = event.name();
        let effect = self.machine.step(event).map_err(|err| match err {
            MachineError::Step(refused) => refused,
            MachineError::Terminal => Refused {
                state,
                event: name,
                reason: RefusedBecause::Unexpected,
            },
        })?;
        self.perform(effect);
        self.after_transition();
        Ok(())
    }

    /// [`Self::apply`] for events nobody waits on: a refusal is logged.
    fn apply_logged(&mut self, event: LifecycleEvent) {
        if let Err(refused) = self.apply(event) {
            debug!(%refused, "lifecycle event refused");
        }
    }

    fn perform(&mut self, effect: LifecycleEffect) {
        match effect {
            LifecycleEffect::None | LifecycleEffect::Exit(_) => {}
            LifecycleEffect::StartBoot { attempt } => self.start_boot(attempt),
            LifecycleEffect::CancelBoot => {
                if let Slot::Booting { cancel, .. } = &self.slot {
                    cancel.stop();
                }
            }
            LifecycleEffect::ShutdownRuntime => {
                match std::mem::replace(&mut self.slot, Slot::Idle(None)) {
                    Slot::Up(running) => {
                        info!("stopping the runtime");
                        self.slot = Slot::Draining(tokio::spawn(running.runtime.shutdown()));
                    }
                    other => {
                        self.slot = other;
                        error!("the lifecycle asked to stop a runtime that is not up");
                    }
                }
            }
        }
    }

    fn start_boot(&mut self, attempt: NonZeroU32) {
        // A boot starts only over a released store: from Idle, and only
        // from Idle.
        let Slot::Idle(_released) = std::mem::replace(&mut self.slot, Slot::Idle(None)) else {
            error!(
                attempt = attempt.get(),
                "the lifecycle started a boot while the slot was not idle"
            );
            return;
        };
        let (cancel, signal) = stop_channel();
        let (progress_tx, progress) = mpsc::unbounded_channel();
        let rec = BootRecorder::new(progress_tx, signal);
        let task = tokio::spawn(supervised_boot(
            self.source.clone(),
            self.backend.clone(),
            self.data_dir.clone(),
            self.declared_path.clone(),
            rec,
        ));
        self.slot = Slot::Booting {
            task,
            cancel,
            progress,
        };
        let since = match &self.machine.state().state {
            LifecycleState::Booting { since, .. } => *since,
            _ => Timestamp::now(),
        };
        self.journal.begin(attempt, since);
        self.persist_journal();
        info!(attempt = attempt.get(), "booting");
    }

    fn after_transition(&mut self) {
        self.sync_retry_timer();
        self.sync_run_marker();
        self.answer_stops();
        self.publish();
    }

    fn sync_retry_timer(&mut self) {
        match &self.machine.state().state {
            LifecycleState::Failed {
                attempt,
                retry: RetryClass::Backoff { delay_ms, .. },
                ..
            } => {
                if self.retry.as_ref().map(|(a, _)| *a) != Some(*attempt) {
                    let sleep = tokio::time::sleep(Duration::from_millis(*delay_ms));
                    self.retry = Some((*attempt, Box::pin(sleep)));
                }
            }
            _ => self.retry = None,
        }
    }

    /// Keep `control/run.json` saying whether this process, dying now,
    /// would leave the store released.
    fn sync_run_marker(&mut self) {
        let live = |last_seen| RunMarker::Live {
            last_seen,
            pid: std::process::id(),
        };
        let wanted = match &self.machine.state().state {
            LifecycleState::Booting { phase, .. } => live(LastSeen::Booting { phase: *phase }),
            LifecycleState::Running { .. } => live(LastSeen::Running),
            LifecycleState::Draining { .. } => live(LastSeen::Draining),
            // The store is held; keep whatever said so.
            LifecycleState::Wedged { .. } => return,
            LifecycleState::Exiting { .. } if matches!(self.slot, Slot::Wedged) => return,
            LifecycleState::Resolving { .. }
            | LifecycleState::Stopped { .. }
            | LifecycleState::Failed { .. }
            | LifecycleState::Exiting { .. } => {
                if matches!(self.marker, RunMarker::Released { .. }) {
                    return;
                }
                RunMarker::Released {
                    at: Timestamp::now(),
                }
            }
        };
        if wanted != self.marker {
            if let Err(err) = self.control.write_run(&wanted) {
                warn!(error = %err, "cannot record the run marker");
            }
            self.marker = wanted;
        }
    }

    /// Answer every stop waiting on a drain, once nothing is draining.
    fn answer_stops(&mut self) {
        let state = &self.machine.state().state;
        if matches!(state, LifecycleState::Draining { .. }) {
            return;
        }
        let done = StopDone {
            epoch: self.machine.state().epoch(),
            lifecycle: state.clone(),
        };
        for reply in self.pending_stops.drain(..) {
            let _ = reply.send(Ok(done.clone()));
        }
    }

    fn publish(&self) {
        let lifecycle = self.machine.state();
        if self.snapshot.borrow().lifecycle != lifecycle.state {
            self.events
                .push(DaemonEvent::Lifecycle(lifecycle.state.clone()));
        }
        self.snapshot.send_replace(Snapshot {
            lifecycle: lifecycle.state.clone(),
            epoch: lifecycle.epoch(),
            attempts: self.journal.attempts(),
            previous_run: self.previous_run.clone(),
            identity: self.identity.clone(),
            daemon: self.daemon.clone(),
            data_dir: self.data_dir.clone(),
        });
    }

    fn accepted(&self, at: Timestamp) -> Accepted {
        Accepted {
            accepted_at: at,
            lifecycle: self.machine.state().state.clone(),
        }
    }

    fn write_hold(&self, hold: Hold, at: Timestamp) {
        let written = match hold {
            Hold::None => self.control.clear_hold(),
            Hold::AcrossRelaunch => self.control.set_hold(at),
        };
        if let Err(err) = written {
            warn!(error = %err, "cannot update the hold marker");
        }
    }

    fn record_identity(&mut self, config: &EngenhoConfig, at: Timestamp) {
        let identity = IdentityRecord {
            cluster_name: config.cluster.name.clone(),
            node_name: config.runtime.node_name.clone(),
            recorded_at: at,
        };
        if let Err(err) = self.control.write_identity(&identity) {
            warn!(error = %err, "cannot record the first-boot identity");
        }
        self.identity = Some(identity);
    }

    fn persist_journal(&self) {
        if let Err(err) = self.control.write_journal(&self.journal) {
            warn!(error = %err, "cannot persist the boot journal");
        }
    }

    fn finish(&mut self) {
        self.sync_run_marker();
        self.persist_journal();
        self.publish();
    }
}

/// Watch the declared configuration file; every change that may have
/// altered its content is one `()` on `changed` and one bump of `count`.
fn watch_declared(
    path: &std::path::Path,
    changed: mpsc::UnboundedSender<()>,
    count: watch::Sender<u64>,
) -> Option<shikumi::ConfigWatcher> {
    let started = shikumi::ConfigWatcher::watch(path, move |event| {
        if shikumi::WatchEventClass::classify(&event.kind) == shikumi::WatchEventClass::Reload {
            let _ = changed.send(());
            count.send_modify(|n| *n = n.wrapping_add(1));
        }
    });
    match started {
        Ok(watcher) => Some(watcher),
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot watch the declared config; a held boot will wait for an operator's retry");
            None
        }
    }
}

/// One supervised boot: resolve the configuration inside its phase, check it
/// keeps the data directory, and boot.
async fn supervised_boot(
    source: ConfigSource,
    backend: Option<Arc<dyn ContainerRuntime>>,
    data_dir: PathBuf,
    declared: Option<PathBuf>,
    mut rec: BootRecorder,
) -> BootOutcome {
    let failed = |result| BootOutcome {
        result,
        provenance: None,
        declared_digest: None,
    };
    if let Err(e) = rec.enter(BootPhase::ResolveConfig) {
        return failed(Err(rec.failed(e)));
    }
    // What the resolution is about to read, so drift is measured against
    // what this boot actually saw.
    let declared_digest = declared.as_deref().and_then(file_digest);
    let resolved = match source.resolve() {
        Ok(resolved) => resolved,
        Err(e) => return failed(Err(rec.failed(e.into()))),
    };
    if resolved.config.runtime.data_dir != data_dir {
        let err = RuntimeError::DataDirMoved {
            control: data_dir,
            resolved: resolved.config.runtime.data_dir.clone(),
        };
        return failed(Err(rec.failed(err)));
    }
    let ResolvedConfig { config, provenance } = resolved;
    BootOutcome {
        result: Runtime::boot_resolved(config, backend, &mut rec).await,
        provenance,
        declared_digest,
    }
}

/// BLAKE3 of a file's bytes, lowercase hex; `None` when it cannot be read.
#[must_use]
pub fn file_digest(path: &std::path::Path) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
}
