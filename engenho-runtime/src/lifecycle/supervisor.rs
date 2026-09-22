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

use engenho_config::{ConfigError, EngenhoConfig};
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
use super::journal::{BootAttempt, BootJournal, IdentityRecord, LastSeen, PreviousRun, RunMarker};
use super::machine::{
    DaemonLifecycle, ExitIntent, FailureReport, Lifecycle, LifecycleEffect, LifecycleEvent,
    LifecycleState, Refused, RefusedBecause, RetryClass, StoreOutcome,
};
use crate::boot::{BootKind, BootPhase, BootProgress, BootRecorder, FailureClass, Timestamp};
use crate::error::RuntimeError;
use crate::release::{BootFailed, BootUnwind, StoreReleased};
use crate::runtime::Runtime;

/// Resolves the configuration a boot runs on. Called once per boot, inside
/// its [`BootPhase::ResolveConfig`], so a configuration that does not
/// resolve is a failed boot the control plane can report, not a dead
/// process.
pub type ConfigSource = Arc<dyn Fn() -> Result<EngenhoConfig, ConfigError> + Send + Sync>;

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
}

/// Talks to a running [`Supervisor`]. Cheap to clone.
#[derive(Clone)]
pub struct SupervisorHandle {
    requests: mpsc::Sender<Request>,
    snapshot: watch::Receiver<Snapshot>,
}

impl SupervisorHandle {
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
        task: JoinHandle<Result<Runtime, BootFailed>>,
        cancel: StopHandle,
        progress: mpsc::UnboundedReceiver<BootProgress>,
    },
    /// The runtime is up.
    Up(Box<Runtime>),
    /// The runtime is shutting down.
    Draining(JoinHandle<Result<StoreReleased, RuntimeError>>),
    /// Something holds the store that this process cannot release.
    Wedged,
}

enum SlotEvent {
    Progress(BootProgress),
    /// Boxed: a `Runtime` is large, and this is one event among small ones.
    BootDone(Box<Result<Result<Runtime, BootFailed>, JoinError>>),
    Drained(Result<Result<StoreReleased, RuntimeError>, JoinError>),
    ChildDied,
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
        Slot::Up(runtime) => {
            // Logged at ERROR by the runtime as it returns; not respawned.
            let _dead = runtime.next_dead_child().await;
            SlotEvent::ChildDied
        }
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
    declared: mpsc::UnboundedReceiver<()>,
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
        let watcher = declared
            .as_deref()
            .and_then(|path| watch_declared(path, declared_tx));

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
            declared: declared_rx,
            _watcher: watcher,
        };
        let handle = SupervisorHandle {
            requests: requests_tx,
            snapshot: snapshot_rx,
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
                    self.apply_logged(LifecycleEvent::DeclaredChanged { at: Timestamp::now() });
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
        }
    }

    fn on_slot_event(&mut self, event: SlotEvent) {
        match event {
            SlotEvent::Progress(BootProgress::Entered { phase, at }) => {
                self.journal.entered(phase, at);
                self.persist_journal();
                self.apply_logged(LifecycleEvent::Phase { phase, at });
            }
            SlotEvent::Progress(BootProgress::Kind(kind)) => {
                self.journal.observed(kind);
                self.persist_journal();
            }
            SlotEvent::BootDone(done) => self.on_boot_done(*done),
            SlotEvent::Drained(done) => self.on_drained(done),
            SlotEvent::ChildDied => {}
        }
    }

    fn on_boot_done(&mut self, done: Result<Result<Runtime, BootFailed>, JoinError>) {
        let at = Timestamp::now();
        let event = match done {
            Ok(Ok(runtime)) => {
                let apiserver_addr = runtime.local_addr().to_string();
                if runtime.boot_kind() == BootKind::FirstBoot {
                    self.record_identity(runtime.config(), at);
                }
                self.journal.succeeded();
                info!(addr = %apiserver_addr, "engenho up — apiserver bound");
                self.slot = Slot::Up(Box::new(runtime));
                LifecycleEvent::Booted { at, apiserver_addr }
            }
            Ok(Err(BootFailed {
                error,
                phase,
                unwind,
            })) => {
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
                    Slot::Up(runtime) => {
                        info!("stopping the runtime");
                        self.slot = Slot::Draining(tokio::spawn(runtime.shutdown()));
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
            Arc::clone(&self.source),
            self.backend.clone(),
            self.data_dir.clone(),
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
/// altered its content is one `()` on `changed`.
fn watch_declared(
    path: &std::path::Path,
    changed: mpsc::UnboundedSender<()>,
) -> Option<shikumi::ConfigWatcher> {
    let started = shikumi::ConfigWatcher::watch(path, move |event| {
        if shikumi::WatchEventClass::classify(&event.kind) == shikumi::WatchEventClass::Reload {
            let _ = changed.send(());
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
    mut rec: BootRecorder,
) -> Result<Runtime, BootFailed> {
    rec.enter(BootPhase::ResolveConfig)
        .map_err(|e| rec.failed(e))?;
    let config = source().map_err(|e| rec.failed(e.into()))?;
    if config.runtime.data_dir != data_dir {
        let resolved = config.runtime.data_dir.clone();
        return Err(rec.failed(RuntimeError::DataDirMoved {
            control: data_dir,
            resolved,
        }));
    }
    Runtime::boot_resolved(config, backend, &mut rec).await
}
