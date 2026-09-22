//! The daemon's lifecycle, as a pure state machine.
//!
//! [`DaemonLifecycle`] is a [`maquina::StateMachine`](engenho_substrate::maquina):
//! a pure step from ([`Lifecycle`], [`LifecycleEvent`]) to the next
//! [`Lifecycle`] and one [`LifecycleEffect`] for the supervisor to perform. It
//! does no I/O and reads no clock — every moment arrives on the event — so
//! every transition is testable by value and replayable from its history.
//!
//! The one invariant it exists to hold: **a boot is started only when the
//! store is released.** The states in which the store may be open are exactly
//! `Booting`, `Running`, `Draining` and `Wedged`; the machine leaves them for
//! a resting state (`Stopped`, `Failed`) only on an outcome that says the
//! store was released, and starts a boot only from a resting state. An
//! outcome that says the store is still held leads to `Wedged`, from which
//! the only way out is ending the process. `tests::the_store_is_never_held_at_rest`
//! checks this over arbitrary event sequences.
//!
//! The serde shapes are the control API's (`spec/engenho-control.openapi.yaml`:
//! `LifecycleState`, `PendingApply`, `RetryClass`, `FailureReport`,
//! `ExitIntent`, `StopReason`, `AfterDrain`).

use std::num::NonZeroU32;
use std::time::Duration;

use engenho_substrate::maquina::StateMachine;
use serde::{Deserialize, Serialize};

use crate::boot::{BootPhase, FailureClass, Timestamp, backoff};

/// How the process ends: whether the service manager should start it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitIntent {
    /// Exit 0: stay down. A service manager told to restart only on failure
    /// leaves it down.
    Halt,
    /// Exit 75 (`EX_TEMPFAIL`): a failure exit, so the service manager
    /// starts a fresh process.
    Relaunch,
}

impl ExitIntent {
    /// The process exit code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Halt => 0,
            Self::Relaunch => 75,
        }
    }
}

/// Why the runtime is stopped while the daemon stays up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// An operator stopped it.
    OperatorRequest,
    /// The hold marker was present when the daemon started.
    HeldAtStartup,
    /// A destructive re-initialization finished; the next start boots over
    /// what it left.
    ReinitComplete,
}

/// What a draining runtime turns into once its store is released.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "then", rename_all = "snake_case")]
pub enum AfterDrain {
    /// Rest, stopped, with the process up.
    Stop,
    /// Boot again.
    Restart,
    /// End the process.
    Exit {
        /// Halt or relaunch.
        intent: ExitIntent,
    },
}

/// Whether the running runtime reflects the effective configuration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingApply {
    /// It does.
    InSync,
    /// These leaves changed and take effect at the next boot.
    RestartNeeded {
        /// The changed leaves, dotted.
        leaves: Vec<String>,
    },
}

/// When a failed boot is tried again.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum RetryClass {
    /// On a timer.
    Backoff {
        /// When.
        next_retry_at: Timestamp,
        /// The delay that was chosen.
        delay_ms: u64,
    },
    /// Not until something changes: the declared file, an override, or an
    /// operator's retry.
    Hold,
}

/// What failed, where, and when.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FailureReport {
    /// The phase the boot failed in.
    pub phase: BootPhase,
    /// The error, rendered.
    pub error: String,
    /// When.
    pub at: Timestamp,
}

/// The daemon's lifecycle. The control plane is served in every state.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LifecycleState {
    /// The daemon is up; nothing has been booted yet.
    Resolving {
        /// Since when.
        since: Timestamp,
    },
    /// A boot is running.
    Booting {
        /// Which boot of this daemon.
        attempt: NonZeroU32,
        /// The phase it is in.
        phase: BootPhase,
        /// When it entered that phase.
        since: Timestamp,
    },
    /// The runtime is serving.
    Running {
        /// Which boot of this daemon produced it.
        attempt: NonZeroU32,
        /// Since when.
        since: Timestamp,
        /// The address the apiserver bound.
        apiserver_addr: String,
        /// Whether it reflects the effective configuration.
        pending: PendingApply,
    },
    /// The runtime (or the boot producing it) is being stopped.
    Draining {
        /// Which boot is being drained.
        attempt: NonZeroU32,
        /// What happens once the store is released.
        #[serde(flatten)]
        then: AfterDrain,
    },
    /// The runtime is stopped, its store released; the daemon stays up.
    Stopped {
        /// Why.
        reason: StopReason,
        /// How many times this daemon has released the store.
        epoch: u64,
        /// Since when.
        since: Timestamp,
    },
    /// The last boot failed and released what it opened.
    Failed {
        /// Which boot failed.
        attempt: NonZeroU32,
        /// Why.
        report: FailureReport,
        /// When it is tried again.
        retry: RetryClass,
    },
    /// Something still holds the store and it cannot be released in this
    /// process. Only ending the process gets out of here.
    Wedged {
        /// What held it.
        cause: String,
        /// Since when.
        since: Timestamp,
    },
    /// The process is ending.
    Exiting {
        /// Halt or relaunch.
        intent: ExitIntent,
    },
}

impl LifecycleState {
    /// The state's wire name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Resolving { .. } => "resolving",
            Self::Booting { .. } => "booting",
            Self::Running { .. } => "running",
            Self::Draining { .. } => "draining",
            Self::Stopped { .. } => "stopped",
            Self::Failed { .. } => "failed",
            Self::Wedged { .. } => "wedged",
            Self::Exiting { .. } => "exiting",
        }
    }

    /// Whether the store may be open in this state.
    #[must_use]
    pub const fn may_hold_store(&self) -> bool {
        match self {
            Self::Booting { .. }
            | Self::Running { .. }
            | Self::Draining { .. }
            | Self::Wedged { .. } => true,
            Self::Resolving { .. }
            | Self::Stopped { .. }
            | Self::Failed { .. }
            | Self::Exiting { .. } => false,
        }
    }
}

/// What a boot, or a drain, did about the store.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum StoreOutcome {
    /// It is released (or was never opened).
    Released,
    /// Something still holds it.
    Held {
        /// What, as far as is known.
        cause: String,
    },
}

/// Everything that moves the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// The daemon found the hold marker at startup.
    HeldAtStartup {
        /// When.
        at: Timestamp,
    },
    /// Start a boot: at startup, or an operator starting a stopped runtime.
    Start {
        /// When.
        at: Timestamp,
    },
    /// An operator asked to retry a failed boot now.
    Retry {
        /// When.
        at: Timestamp,
    },
    /// A failed boot's backoff elapsed.
    RetryDue {
        /// When.
        at: Timestamp,
    },
    /// The configuration a boot would read changed: the declared file, or
    /// the override tier.
    ConfigChanged {
        /// When.
        at: Timestamp,
    },
    /// A configuration change reached the running runtime: what it took in
    /// place it now reflects, and `pending` is what it reflects only after a
    /// restart.
    ConfigApplied {
        /// What the running runtime does not reflect yet.
        pending: PendingApply,
    },
    /// The boot entered a phase.
    Phase {
        /// Which.
        phase: BootPhase,
        /// When.
        at: Timestamp,
    },
    /// The boot finished and the runtime is serving.
    Booted {
        /// When.
        at: Timestamp,
        /// The address the apiserver bound.
        apiserver_addr: String,
    },
    /// The boot failed.
    BootFailed {
        /// What failed, where, when.
        report: FailureReport,
        /// Whether waiting can fix it.
        class: FailureClass,
        /// What the failed boot did about the store.
        store: StoreOutcome,
    },
    /// Stop the runtime; the process stays up.
    Stop {
        /// When.
        at: Timestamp,
    },
    /// Stop the runtime and boot it again.
    Restart {
        /// When.
        at: Timestamp,
    },
    /// End the process.
    Exit {
        /// Halt or relaunch.
        intent: ExitIntent,
    },
    /// The runtime's shutdown finished.
    Drained {
        /// When.
        at: Timestamp,
        /// What it did about the store.
        store: StoreOutcome,
    },
}

impl LifecycleEvent {
    /// The event's wire name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::HeldAtStartup { .. } => "held_at_startup",
            Self::Start { .. } => "start",
            Self::Retry { .. } => "retry",
            Self::RetryDue { .. } => "retry_due",
            Self::ConfigChanged { .. } => "config_changed",
            Self::ConfigApplied { .. } => "config_applied",
            Self::Phase { .. } => "phase",
            Self::Booted { .. } => "booted",
            Self::BootFailed { .. } => "boot_failed",
            Self::Stop { .. } => "stop",
            Self::Restart { .. } => "restart",
            Self::Exit { .. } => "exit",
            Self::Drained { .. } => "drained",
        }
    }
}

/// What the supervisor does after a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEffect {
    /// Nothing.
    None,
    /// Start boot number `attempt` over the released store.
    StartBoot {
        /// Which boot of this daemon.
        attempt: NonZeroU32,
    },
    /// Ask the running boot to stop.
    CancelBoot,
    /// Shut the running runtime down.
    ShutdownRuntime,
    /// End the process.
    Exit(ExitIntent),
}

/// Why an event was refused. The spellings are the control API's
/// `RefusalReason` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedBecause {
    /// The runtime is already running.
    RuntimeRunning,
    /// The runtime is not running (nor booting).
    RuntimeNotRunning,
    /// Only a failed boot is retried.
    RuntimeNotFailed,
    /// Only a stopped runtime is started.
    RuntimeNotStopped,
    /// A boot or a drain is in progress.
    LifecycleBusy,
    /// The store is held and cannot be released; only exiting is possible.
    Wedged,
    /// An internal event arrived where it cannot happen (a stale timer, a
    /// report from a boot that already ended).
    Unexpected,
}

impl RefusedBecause {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeRunning => "runtime_running",
            Self::RuntimeNotRunning => "runtime_not_running",
            Self::RuntimeNotFailed => "runtime_not_failed",
            Self::RuntimeNotStopped => "runtime_not_stopped",
            Self::LifecycleBusy => "lifecycle_busy",
            Self::Wedged => "wedged",
            Self::Unexpected => "unexpected",
        }
    }
}

impl std::fmt::Display for RefusedBecause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An event the lifecycle refused, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{event} refused while {state}: {reason}")]
pub struct Refused {
    /// The state it arrived in.
    pub state: &'static str,
    /// The event.
    pub event: &'static str,
    /// Why.
    pub reason: RefusedBecause,
}

impl engenho_substrate::ErrorKind for Refused {
    fn kind(&self) -> &'static str {
        self.reason.as_str()
    }
}

/// The machine's state: the lifecycle the control API shows, and the
/// counters the transitions are computed from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Lifecycle {
    /// Where the daemon is.
    pub state: LifecycleState,
    attempts: u32,
    epoch: u64,
    streak: u32,
}

impl Lifecycle {
    /// A lifecycle that began at `since`.
    #[must_use]
    pub const fn at(since: Timestamp) -> Self {
        Self {
            state: LifecycleState::Resolving { since },
            attempts: 0,
            epoch: 0,
            streak: 0,
        }
    }

    /// Boots started by this daemon.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Times this daemon has released the store. A confirmation for a
    /// destructive operation is bound to one epoch: any boot in between
    /// makes it stale.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Consecutive failed boots since the last one that reached `Running`.
    #[must_use]
    pub const fn streak(&self) -> u32 {
        self.streak
    }

    fn begin_boot(&mut self, at: Timestamp) -> LifecycleEffect {
        self.attempts = self.attempts.saturating_add(1);
        let attempt = NonZeroU32::new(self.attempts).unwrap_or(NonZeroU32::MAX);
        self.state = LifecycleState::Booting {
            attempt,
            phase: BootPhase::FIRST,
            since: at,
        };
        LifecycleEffect::StartBoot { attempt }
    }

    fn fail(&mut self, attempt: NonZeroU32, report: &FailureReport, class: FailureClass) {
        self.epoch = self.epoch.saturating_add(1);
        self.streak = self.streak.saturating_add(1);
        let retry = match class {
            FailureClass::Backoff => {
                let delay = backoff(self.streak);
                RetryClass::Backoff {
                    next_retry_at: report.at.after(delay),
                    delay_ms: duration_ms(delay),
                }
            }
            FailureClass::Hold => RetryClass::Hold,
        };
        self.state = LifecycleState::Failed {
            attempt,
            report: report.clone(),
            retry,
        };
    }

    fn rest(&mut self, reason: StopReason, since: Timestamp) {
        self.state = LifecycleState::Stopped {
            reason,
            epoch: self.epoch,
            since,
        };
    }

    fn drain(
        &mut self,
        attempt: NonZeroU32,
        then: AfterDrain,
        effect: LifecycleEffect,
    ) -> LifecycleEffect {
        self.state = LifecycleState::Draining { attempt, then };
        effect
    }

    fn exit(&mut self, intent: ExitIntent) -> LifecycleEffect {
        self.state = LifecycleState::Exiting { intent };
        LifecycleEffect::Exit(intent)
    }

    /// A drain finished: the store is released or it is not.
    fn settle(&mut self, then: AfterDrain, store: &StoreOutcome, at: Timestamp) -> LifecycleEffect {
        match (then, store) {
            // Ending the process releases whatever it held.
            (AfterDrain::Exit { intent }, _) => self.exit(intent),
            (AfterDrain::Stop, StoreOutcome::Released) => {
                self.epoch = self.epoch.saturating_add(1);
                self.rest(StopReason::OperatorRequest, at);
                LifecycleEffect::None
            }
            (AfterDrain::Restart, StoreOutcome::Released) => {
                self.epoch = self.epoch.saturating_add(1);
                self.begin_boot(at)
            }
            (AfterDrain::Stop | AfterDrain::Restart, StoreOutcome::Held { cause }) => {
                self.state = LifecycleState::Wedged {
                    cause: cause.clone(),
                    since: at,
                };
                LifecycleEffect::None
            }
        }
    }
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The daemon lifecycle machine.
#[derive(Debug, Default)]
pub struct DaemonLifecycle;

engenho_substrate::define_named!(DaemonLifecycle, "engenho-daemon-lifecycle");

impl StateMachine for DaemonLifecycle {
    type State = Lifecycle;
    type Event = LifecycleEvent;
    type Effect = LifecycleEffect;
    type Err = Refused;

    /// A lifecycle with no clock reading. The supervisor starts its runner
    /// from [`Lifecycle::at`] instead, with the moment it started.
    fn initial() -> Lifecycle {
        Lifecycle::at(Timestamp::now())
    }

    fn step(
        m: &Lifecycle,
        event: &LifecycleEvent,
    ) -> Result<(Lifecycle, LifecycleEffect), Refused> {
        Self::starting(m, event)
            .or_else(|| Self::booting(m, event))
            .or_else(|| Self::stopping(m, event))
            .ok_or_else(|| Refused {
                state: m.state.name(),
                event: event.name(),
                reason: Self::refusal(&m.state, event),
            })
    }

    fn is_terminal(m: &Lifecycle) -> bool {
        matches!(m.state, LifecycleState::Exiting { .. })
    }
}

type Stepped = Option<(Lifecycle, LifecycleEffect)>;

impl DaemonLifecycle {
    /// A boot starting from rest, or the daemon resting at startup. A boot
    /// starts only from here.
    fn starting(m: &Lifecycle, event: &LifecycleEvent) -> Stepped {
        use LifecycleEvent as E;
        use LifecycleState as S;
        let mut next = m.clone();
        let effect = match (&m.state, event) {
            (S::Resolving { .. } | S::Stopped { .. }, E::Start { at })
            | (S::Failed { .. }, E::Retry { at } | E::ConfigChanged { at })
            | (
                S::Failed {
                    retry: RetryClass::Backoff { .. },
                    ..
                },
                E::RetryDue { at },
            ) => next.begin_boot(*at),
            (S::Resolving { .. }, E::HeldAtStartup { at }) => {
                next.rest(StopReason::HeldAtStartup, *at);
                LifecycleEffect::None
            }
            _ => return None,
        };
        Some((next, effect))
    }

    /// A running boot's progress and outcome.
    fn booting(m: &Lifecycle, event: &LifecycleEvent) -> Stepped {
        use LifecycleEvent as E;
        use LifecycleState as S;
        let mut next = m.clone();
        let effect = match (&m.state, event) {
            (S::Booting { attempt, .. }, E::Phase { phase, at }) => {
                next.state = S::Booting {
                    attempt: *attempt,
                    phase: *phase,
                    since: *at,
                };
                LifecycleEffect::None
            }
            (
                S::Running {
                    attempt,
                    since,
                    apiserver_addr,
                    ..
                },
                E::ConfigApplied { pending },
            ) => {
                next.state = S::Running {
                    attempt: *attempt,
                    since: *since,
                    apiserver_addr: apiserver_addr.clone(),
                    pending: pending.clone(),
                };
                LifecycleEffect::None
            }
            (S::Booting { attempt, .. }, E::Booted { at, apiserver_addr }) => {
                next.streak = 0;
                next.state = S::Running {
                    attempt: *attempt,
                    since: *at,
                    apiserver_addr: apiserver_addr.clone(),
                    pending: PendingApply::InSync,
                };
                LifecycleEffect::None
            }
            (
                S::Booting { attempt, .. },
                E::BootFailed {
                    report,
                    class,
                    store,
                },
            ) => {
                match store {
                    StoreOutcome::Released => next.fail(*attempt, report, *class),
                    StoreOutcome::Held { cause } => {
                        next.state = S::Wedged {
                            cause: cause.clone(),
                            since: report.at,
                        };
                    }
                }
                LifecycleEffect::None
            }
            _ => return None,
        };
        Some((next, effect))
    }

    /// Stopping, restarting, draining and exiting.
    fn stopping(m: &Lifecycle, event: &LifecycleEvent) -> Stepped {
        use LifecycleEvent as E;
        use LifecycleState as S;
        let mut next = m.clone();
        let effect = match (&m.state, event) {
            (S::Booting { attempt, .. }, E::Stop { .. }) => {
                next.drain(*attempt, AfterDrain::Stop, LifecycleEffect::CancelBoot)
            }
            (S::Booting { attempt, .. }, E::Restart { .. }) => {
                next.drain(*attempt, AfterDrain::Restart, LifecycleEffect::CancelBoot)
            }
            (S::Booting { attempt, .. }, E::Exit { intent }) => next.drain(
                *attempt,
                AfterDrain::Exit { intent: *intent },
                LifecycleEffect::CancelBoot,
            ),
            (S::Running { attempt, .. }, E::Stop { .. }) => {
                next.drain(*attempt, AfterDrain::Stop, LifecycleEffect::ShutdownRuntime)
            }
            (S::Running { attempt, .. }, E::Restart { .. }) => next.drain(
                *attempt,
                AfterDrain::Restart,
                LifecycleEffect::ShutdownRuntime,
            ),
            (S::Running { attempt, .. }, E::Exit { intent }) => next.drain(
                *attempt,
                AfterDrain::Exit { intent: *intent },
                LifecycleEffect::ShutdownRuntime,
            ),
            (S::Failed { .. }, E::Stop { at }) => {
                // The failed boot already released the store: resting is
                // immediate, and the epoch is the failure's.
                next.rest(StopReason::OperatorRequest, *at);
                LifecycleEffect::None
            }

            // ── draining ────────────────────────────────────────────────
            // The boot finished before it saw the stop: shut it down.
            (S::Draining { .. }, E::Booted { .. }) => LifecycleEffect::ShutdownRuntime,
            // Late progress from the boot being cancelled.
            (S::Draining { .. }, E::Phase { .. }) => LifecycleEffect::None,
            (S::Draining { then, .. }, E::BootFailed { report, store, .. }) => {
                next.settle(*then, store, report.at)
            }
            (S::Draining { then, .. }, E::Drained { at, store }) => next.settle(*then, store, *at),
            // Exiting overrides whatever the drain was for.
            (S::Draining { attempt, .. }, E::Exit { intent }) => {
                next.state = S::Draining {
                    attempt: *attempt,
                    then: AfterDrain::Exit { intent: *intent },
                };
                LifecycleEffect::None
            }

            // Ending the process: from rest, or out of a wedge.
            (
                S::Resolving { .. } | S::Stopped { .. } | S::Failed { .. } | S::Wedged { .. },
                E::Exit { intent },
            ) => next.exit(*intent),
            _ => return None,
        };
        Some((next, effect))
    }

    /// Why `event` is refused in `state` (every pair the transitions do not
    /// accept).
    fn refusal(state: &LifecycleState, event: &LifecycleEvent) -> RefusedBecause {
        use LifecycleEvent as E;
        use LifecycleState as S;
        match (state, event) {
            (S::Wedged { .. }, _) => RefusedBecause::Wedged,
            (S::Running { .. }, E::Start { .. }) => RefusedBecause::RuntimeRunning,
            (S::Booting { .. } | S::Draining { .. }, E::Start { .. })
            | (S::Draining { .. }, E::Stop { .. } | E::Restart { .. }) => {
                RefusedBecause::LifecycleBusy
            }
            (S::Failed { .. }, E::Start { .. }) => RefusedBecause::RuntimeNotStopped,
            (_, E::Retry { .. }) => RefusedBecause::RuntimeNotFailed,
            (S::Resolving { .. } | S::Stopped { .. }, E::Stop { .. } | E::Restart { .. })
            | (S::Failed { .. }, E::Restart { .. })
            | (_, E::ConfigApplied { .. }) => RefusedBecause::RuntimeNotRunning,
            // An internal event where it cannot happen (a stale timer, a
            // report from a boot that already ended), or anything after the
            // exit. Accepted pairs never reach here.
            _ => RefusedBecause::Unexpected,
        }
    }
}

#[cfg(test)]
mod tests;
