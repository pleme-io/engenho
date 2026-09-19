//! Typed pod-lifecycle state machine — the TYPED-SPEC + INTERPRETER border.
//!
//! This module is the **typed border** + **pure interpreter** half of the
//! kubelet's pod-lifecycle triplet (the `FakeBackend` is the mock
//! environment; see [`crate::backend`]):
//!
//!   * **Typed border** — the closed enums [`RestartPolicy`],
//!     [`ContainerState`], and the observed-shape [`ContainerObservation`].
//!     `PodPhase` is reused from `engenho_types::curated_enums` (the M0.0
//!     curated enum). Bad combinations are unrepresentable: a container is
//!     `Waiting`/`Running`/`Terminated`, never an ad-hoc string.
//!   * **Interpreter** — [`reconcile_pod_phase`], a PURE function that folds
//!     the per-container observed states + the Pod's `restartPolicy` into a
//!     `(PodPhase, Vec<ContainerStatusOut>)`. No I/O, no podman, no store —
//!     so the WHOLE pod-phase logic is unit-testable (and proptest-able)
//!     against the mock with zero container runtime.
//!
//! The kubelet's tick ([`crate::kubelet`]) is the I/O shell that calls
//! `backend.status` per container, builds the [`ContainerObservation`] slice,
//! and hands it to this pure fold. The fold decides the pod phase; the kubelet
//! decides what to DO about a terminated container (restart per policy, or
//! latch terminal) — but the *phase computation* lives here, proven in
//! isolation.
//!
//! ## No silent wrong answers
//!
//! Every arm of the fold is a real decision: a not-yet-started container is
//! `Pending`/`Waiting{reason}` (never a fake `Running`); a terminated
//! container under `restartPolicy: Never` latches terminal; under `Always`
//! the kubelet re-starts it and the fold keeps the pod `Running` because the
//! container is observed `Running` again on the next tick. There is no
//! `todo!()` / `panic!()` / placeholder `Ok`.

use crate::backend::Readoption;
use crate::cri::{ExitDisposition, RunState};
use engenho_types::curated_enums::PodPhase;
use serde::{Deserialize, Serialize};

/// `restartPolicy` — the Pod-level container-restart policy. Closed enum
/// mirroring `core/v1` `PodSpec.restartPolicy`. Default [`RestartPolicy::Always`]
/// (the K8s default), so a Pod manifest that omits `spec.restartPolicy` gets
/// `Always`.
///
/// The fold uses this to decide whether a terminated container's exit is
/// terminal for the pod (`Never`, or `OnFailure` with exit 0) or merely a
/// restart event the pod recovers from (`Always`, or `OnFailure` with a
/// non-zero exit). The kubelet acts on the same value to decide whether to
/// re-`start` the container.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum RestartPolicy {
    /// Restart every container regardless of exit code. The K8s default.
    #[default]
    Always,
    /// Restart a container only when it exits non-zero. A zero exit latches
    /// `Succeeded`.
    OnFailure,
    /// Never restart a container. The first terminal exit latches
    /// `Succeeded` (exit 0) / `Failed` (non-zero).
    Never,
}

impl RestartPolicy {
    /// Parse the `spec.restartPolicy` string into the typed policy.
    /// Unrecognized / absent ⇒ the K8s default [`RestartPolicy::Always`] —
    /// a documented no-op default, never a panic.
    #[must_use]
    pub fn from_spec_str(s: Option<&str>) -> Self {
        match s {
            Some("Never") => RestartPolicy::Never,
            Some("OnFailure") => RestartPolicy::OnFailure,
            // "Always", any other string, or absent → Always (K8s default).
            _ => RestartPolicy::Always,
        }
    }

    /// `true` iff a container that terminated with `exit` should be restarted
    /// under this policy. The kubelet uses this to decide whether to
    /// re-`start` the one exited container.
    ///
    ///   * `Always`    → always restart.
    ///   * `OnFailure` → restart unless the exit [`Termination::is_success`] —
    ///     so a signal death and an [`Termination::Unknown`] exit restart.
    ///   * `Never`     → never restart; the fold then latches the pod, and an
    ///     Unknown exit latches it `Failed`, never `Succeeded`.
    ///
    /// Unknown follows upstream: an exit nobody observed is treated as a
    /// failure, never as the success a missing code used to default to.
    #[must_use]
    pub fn should_restart(self, exit: Termination) -> bool {
        match self {
            RestartPolicy::Always => true,
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure => !exit.is_success(),
        }
    }
}

/// How a terminated container ended, as far as this kubelet observed.
///
/// ── ★ UNKNOWN IS A STATE, NOT A DEFAULT ───────────────────────────────────
/// The runtime can report a container down without saying how: CRI
/// `CONTAINER_UNKNOWN`, a container that is `Created` but was started, a
/// podman read-back that found nothing. Before T1.2 all of those arrived as
/// `exit_code: None` and three call sites in the kubelet resolved it with
/// `unwrap_or(0)` — a clean exit. An unobserved exit is now its own arm, it is
/// never a success, and it is only turned into numbers at the wire
/// ([`Self::exit_code`], [`Self::reason`]) the way upstream renders it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Termination {
    /// The runtime reported how the process ended.
    Observed(ExitDisposition),
    /// The container is down, and how it ended was never observed.
    Unknown,
}

impl Termination {
    /// The `exitCode` upstream publishes for a container whose status could
    /// not be determined.
    pub const UNKNOWN_EXIT_CODE: i32 = 137;
    /// The `reason` upstream publishes alongside [`Self::UNKNOWN_EXIT_CODE`].
    pub const UNKNOWN_REASON: &'static str = "ContainerStatusUnknown";

    /// What a runtime [`RunState`] says about a container that is not
    /// running. `None` exactly when it is running.
    ///
    /// `Created` and `Unknown` both yield [`Termination::Unknown`]: the kubelet
    /// only polls containers it started, so either one is a container that is
    /// not up and whose end nobody saw. Upstream likewise restarts both.
    #[must_use]
    pub fn from_run_state(state: RunState) -> Option<Self> {
        match state {
            RunState::Running => None,
            RunState::Exited(disposition) => Some(Self::Observed(disposition)),
            RunState::Created | RunState::Unknown => Some(Self::Unknown),
        }
    }

    /// Did the container succeed? Only an observed `Code(0)` does.
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, Self::Observed(d) if d.is_success())
    }

    /// `state.terminated.exitCode` on the wire.
    #[must_use]
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Observed(d) => d.exit_code(),
            Self::Unknown => Self::UNKNOWN_EXIT_CODE,
        }
    }

    /// `state.terminated.reason` on the wire.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::Observed(d) if d.is_success() => "Completed",
            Self::Observed(_) => "Error",
            Self::Unknown => Self::UNKNOWN_REASON,
        }
    }

    /// Read back a termination from its wire form — the inverse of
    /// [`Self::exit_code`] / [`Self::reason`] up to the one thing the wire
    /// folds away: a signal arrives as the code `128 + n`, which is still
    /// not a success.
    ///
    /// The `ContainerStatusUnknown` reason, or no readable code at all, is
    /// [`Self::Unknown`] — an absent code is never read as a clean exit.
    #[must_use]
    pub fn from_wire(exit_code: Option<i64>, reason: Option<&str>) -> Self {
        let code = exit_code.and_then(|c| i32::try_from(c).ok());
        match (reason, code) {
            (Some(Self::UNKNOWN_REASON), _) | (_, None) => Self::Unknown,
            (_, Some(code)) => Self::Observed(ExitDisposition::Code(code)),
        }
    }
}

impl From<ExitDisposition> for Termination {
    fn from(disposition: ExitDisposition) -> Self {
        Self::Observed(disposition)
    }
}

impl std::fmt::Display for Termination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Observed(d) => d.fmt(f),
            Self::Unknown => f.write_str("unobserved"),
        }
    }
}

/// A single container's observed lifecycle state. Closed enum — the typed
/// border the fold consumes + the kubelet renders into
/// `status.containerStatuses[].state`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerState {
    /// Not yet started (the kubelet hasn't created the container yet this
    /// pod-start window). `reason` is a short K8s-style reason
    /// (`"ContainerCreating"`). Renders `state: { waiting: { reason } }`.
    Waiting {
        /// Short reason string (e.g. `"ContainerCreating"`).
        reason: String,
    },
    /// The container is up. Renders `state: { running: {} }`.
    Running,
    /// The container has terminated, and this is how. Renders
    /// `state: { terminated: { exitCode, reason } }` from
    /// [`Termination::exit_code`] and [`Termination::reason`] — the wire
    /// numbers are derived, so an Unknown exit cannot be confused with a
    /// genuine exit 137 anywhere but on the wire.
    Terminated(Termination),
}

impl ContainerState {
    /// `true` iff this container is up (the `Running` arm).
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, ContainerState::Running)
    }

    /// `true` iff this container has terminated.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        matches!(self, ContainerState::Terminated(_))
    }

    /// How the container ended if terminated, else `None`.
    #[must_use]
    pub fn termination(&self) -> Option<Termination> {
        match self {
            ContainerState::Terminated(exit) => Some(*exit),
            ContainerState::Waiting { .. } | ContainerState::Running => None,
        }
    }

    /// Build the `Terminated` state for `exit`.
    #[must_use]
    pub fn terminated(exit: impl Into<Termination>) -> Self {
        ContainerState::Terminated(exit.into())
    }

    /// Build the canonical not-yet-started `Waiting` state.
    #[must_use]
    pub fn creating() -> Self {
        ContainerState::Waiting {
            reason: "ContainerCreating".to_string(),
        }
    }
}

/// One container's observed shape — the per-element input to
/// [`reconcile_pod_phase`]. The kubelet builds this slice from its local
/// bookkeeping + the backend's `status` poll.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitKind {
    /// A classic init container: runs to completion, and the next one does not
    /// start until it exits 0.
    #[default]
    Regular,
    /// A **native sidecar** — `initContainers[i].restartPolicy: Always`,
    /// KEP-753, GA in Kubernetes 1.29.
    ///
    /// The kubelet starts it, does NOT wait for it to exit, proceeds to the
    /// next init container once it has STARTED, keeps it running for the pod's
    /// whole lifetime, and terminates it AFTER the app containers.
    Sidecar,
}

impl InitKind {
    /// Read `initContainers[i].restartPolicy`.
    ///
    /// ── ★ AN UNRECOGNISED VALUE IS AN ERROR, NEVER A DEFAULT ──────────────
    /// `Always` is the only value the API permits on an init container.
    /// Defaulting anything else to [`Self::Regular`] — the way
    /// `RestartPolicy::from_spec_str` defaults a bad pod-level policy to
    /// `Always` — would make `restartPolicy: always` (lowercase) silently
    /// revert to blocking semantics, i.e. straight back to the forever-Pending
    /// hang this whole feature exists to remove, now triggered by a typo and
    /// with no error anywhere.
    ///
    /// # Errors
    /// [`InitKindError`] naming the offending value.
    pub fn from_spec_str(raw: Option<&str>) -> Result<Self, InitKindError> {
        match raw {
            None => Ok(Self::Regular),
            Some("Always") => Ok(Self::Sidecar),
            Some(other) => Err(InitKindError(other.to_string())),
        }
    }

    /// Whether this is a sidecar.
    #[must_use]
    pub fn is_sidecar(self) -> bool {
        matches!(self, Self::Sidecar)
    }
}

/// An `initContainers[i].restartPolicy` value Kubernetes does not permit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("initContainers[].restartPolicy must be absent or \"Always\", got {0:?}")]
pub struct InitKindError(pub String);

/// One container's observed shape — the per-element input to
/// [`reconcile_pod_phase`]. The kubelet builds this slice from its local
/// bookkeeping + the backend's `status` poll.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerObservation {
    /// The container's logical name (`spec.containers[i].name`).
    pub name: String,
    /// The observed lifecycle state.
    pub state: ContainerState,
    /// The backend container handle, when the container has been started.
    /// `None` while `Waiting` (not yet created).
    pub container_id: Option<String>,
    /// How many times this container has been (re)started so far. `0` on the
    /// first start; bumped by the kubelet each restart.
    pub restart_count: u32,
    /// Whether the container reports ready. Today this mirrors
    /// `state.is_running()` (probes are deferred — see crate docs); carried
    /// explicitly so the readiness source is a typed field, not a re-derive.
    pub ready: bool,
    /// Regular init container or native sidecar. Meaningless for an app
    /// container, where it stays [`InitKind::Regular`].
    pub kind: InitKind,
    /// Whether this container has EVER been started.
    ///
    /// ── ★ NOT DERIVABLE FROM `state` ──────────────────────────────────────
    /// `ContainerState::Running` answers "up right now", not "has been up". A
    /// sidecar caught mid-restart reads as not-Running, and a sequencer that
    /// re-derives startedness from the current state would let it re-block the
    /// init sequence — a pod that was Running silently falling back to Pending
    /// on a sidecar hiccup, which is the original hang with an intermittent
    /// trigger. The kubelet LATCHES this on first successful start.
    pub ever_started: bool,
}

impl ContainerObservation {
    /// A `Running` observation for a started container.
    #[must_use]
    pub fn running(
        name: impl Into<String>,
        container_id: impl Into<String>,
        restart_count: u32,
    ) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::Running,
            container_id: Some(container_id.into()),
            restart_count,
            ready: true,
            kind: InitKind::Regular,
            // It has an id, so it has started.
            ever_started: true,
        }
    }

    /// A `Terminated` observation for a container that exited.
    #[must_use]
    pub fn terminated(
        name: impl Into<String>,
        container_id: impl Into<String>,
        exit: impl Into<Termination>,
        restart_count: u32,
    ) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::terminated(exit),
            container_id: Some(container_id.into()),
            restart_count,
            ready: false,
            kind: InitKind::Regular,
            ever_started: true,
        }
    }

    /// A `Waiting` observation for a container that HAS run before and is
    /// being held off — `CrashLoopBackOff` and its kin.
    ///
    /// ★ THE `container_id` IS WHAT MAKES THIS NOT `Pending`. Upstream keeps
    /// a crash-looping pod in phase `Running` with the container `Waiting`;
    /// only a container that has never started makes a pod `Pending`. The
    /// fold reads the id to tell those apart, so passing one here is not
    /// bookkeeping — it is the phase decision.
    #[must_use]
    pub fn backing_off(
        name: impl Into<String>,
        container_id: impl Into<String>,
        reason: &'static str,
        restart_count: u32,
    ) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::Waiting {
                reason: reason.to_string(),
            },
            container_id: Some(container_id.into()),
            restart_count,
            ready: false,
            kind: InitKind::Regular,
            ever_started: true,
        }
    }

    /// A `Waiting{ContainerCreating}` observation for a not-yet-started
    /// container.
    #[must_use]
    pub fn waiting(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::creating(),
            container_id: None,
            restart_count: 0,
            ready: false,
            kind: InitKind::Regular,
            // Never started — this is the state the sidecar fold reads to
            // decide "start it", and the reason `ever_started` is latched by
            // the kubelet rather than re-derived from `state` each tick.
            ever_started: false,
        }
    }

    /// Mark this observation as a native sidecar.
    ///
    /// A builder rather than a parameter on all four constructors: every
    /// existing call site means a regular container and stays byte-identical,
    /// and only the init path — which is the only place that can know — opts in.
    #[must_use]
    pub fn as_sidecar(mut self) -> Self {
        self.kind = InitKind::Sidecar;
        self
    }

    /// Override the latched started flag (the kubelet knows; the state does not).
    #[must_use]
    pub fn with_ever_started(mut self, ever_started: bool) -> Self {
        self.ever_started = ever_started;
        self
    }
}

/// The desired per-container `status.containerStatuses[]` entry the fold
/// emits — a typed pre-render shape the kubelet serializes to `json!`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerStatusOut {
    /// `containerStatuses[].name`.
    pub name: String,
    /// `containerStatuses[].ready`.
    pub ready: bool,
    /// `containerStatuses[].state` (waiting / running / terminated).
    pub state: ContainerState,
    /// `containerStatuses[].containerID`, when the container has been
    /// started. Absent while `Waiting`.
    pub container_id: Option<String>,
    /// `containerStatuses[].restartCount`.
    pub restart_count: u32,
}

/// PURE pod-phase interpreter — the load-bearing state-machine core.
///
/// Given the Pod's `restart_policy` + every container's observed state, fold
/// to `(PodPhase, Vec<ContainerStatusOut>)`. This is the unit-testable
/// (proptest-able) heart of the lifecycle: NO I/O, NO podman.
///
/// ## Phase fold
///
///   * **Empty** (no containers) → `Pending` (degenerate; a pod with no
///     containers is rejected upstream, but the fold is total).
///   * **Any container `Waiting` AND never started** (`!ever_started`) →
///     `Pending`. The pod is not `Running` until every container is up at
///     least once. A container that IS waiting but HAS started before is a
///     `CrashLoopBackOff` hold, and upstream keeps that pod `Running`, so it
///     does not force `Pending` here. Reporting `Pending`
///     for a crash-looping pod would make it indistinguishable from one
///     still pulling its image, which is the opposite diagnosis.
///   * **All containers `Running`** → `Running`.
///   * **Mixed running + terminated** under a restarting policy
///     (`Always`, or `OnFailure` with a restartable exit) → `Running`: the
///     terminated container is about to be restarted by the kubelet, so the
///     pod is still live (this is the steady state during a restart cycle —
///     the kubelet re-starts the container and the next tick observes it
///     `Running` again).
///   * **All containers terminal** (no restartable container remains) → the
///     terminal phase:
///       * `restartPolicy: Never`  → `Succeeded` iff EVERY container exited
///         0, else `Failed` (worst-case).
///       * `restartPolicy: OnFailure` → `Succeeded` once every container has
///         exited 0 (a non-zero exit would have been restarted, so reaching
///         "all terminated" under `OnFailure` means all-zero); defensively,
///         any non-zero remaining → `Failed`.
///       * `restartPolicy: Always`  → there is no terminal "all terminated"
///         state (every exit restarts), so an all-terminated snapshot under
///         `Always` is the instantaneous window before the kubelet restarts;
///         the fold keeps it `Running` (the pod recovers).
///
/// ## Monotonicity invariants (proptest targets)
///
///   * Under `Never`, once `Succeeded`/`Failed`, the phase is terminal — the
///     kubelet never restarts, so the next observation keeps it terminal.
///   * Under `Always`, the pod stays `Running` across a container exit (the
///     kubelet restarts it) — never latches terminal.
#[must_use]
pub fn reconcile_pod_phase(
    restart_policy: RestartPolicy,
    observations: &[ContainerObservation],
) -> (PodPhase, Vec<ContainerStatusOut>) {
    let statuses: Vec<ContainerStatusOut> = observations
        .iter()
        .map(|o| ContainerStatusOut {
            name: o.name.clone(),
            ready: o.ready,
            state: o.state.clone(),
            container_id: o.container_id.clone(),
            restart_count: o.restart_count,
        })
        .collect();

    // Degenerate: no containers → Pending (total fold; never panics).
    if observations.is_empty() {
        return (PodPhase::Pending, statuses);
    }

    // ── ★ "NEVER STARTED" IS THE LATCH, NOT A PROXY FOR IT ────────────────
    // This read `container_id.is_none()`, which is true of a container that
    // has not started AND of anything a caller built without an id — and the
    // kubelet used to build exactly that for a container it merely failed to
    // POLL, so an inspect error rendered a running pod Pending. The kubelet
    // latches `ever_started` from its own record; the fold reads the latch.
    let any_never_started = observations
        .iter()
        .any(|o| matches!(o.state, ContainerState::Waiting { .. }) && !o.ever_started);
    if any_never_started {
        // Not every container is up yet → Pending. Never a fake Running.
        return (PodPhase::Pending, statuses);
    }

    // A container held in backoff has run and is not Running, so it falls
    // through the all_running check below to the restartable logic — where
    // it has no exit and so is not "restartable this instant". Handled
    // explicitly: a pod whose only non-Running container is backing off is
    // in a restart cycle, which is Running.
    let any_backing_off = observations
        .iter()
        .any(|o| matches!(o.state, ContainerState::Waiting { .. }) && o.ever_started);
    if any_backing_off && restart_policy != RestartPolicy::Never {
        return (PodPhase::Running, statuses);
    }

    let all_running = observations.iter().all(|o| o.state.is_running());
    if all_running {
        return (PodPhase::Running, statuses);
    }

    // No Waiting, not all Running ⇒ at least one Terminated (+ possibly some
    // Running). Decide whether ANY terminated container is restartable under
    // the policy. If so, the pod is in a restart cycle → still Running (the
    // kubelet re-starts the container; the next tick sees it Running). This
    // also covers the all-terminated-under-Always window.
    let any_restartable = observations.iter().any(|o| {
        o.state
            .termination()
            .is_some_and(|exit| restart_policy.should_restart(exit))
    });
    if any_restartable {
        return (PodPhase::Running, statuses);
    }

    // Some container is Running but a sibling terminated non-restartably
    // (e.g. OnFailure + a zero-exit container while another runs). The pod is
    // still live as long as a container runs.
    let any_running = observations.iter().any(|o| o.state.is_running());
    if any_running {
        return (PodPhase::Running, statuses);
    }

    // All containers terminated, none restartable → terminal phase. Worst-case
    // fold: Succeeded iff every container exited 0, else Failed. A signal death
    // or an Unknown exit is never a success, so either one makes it Failed.
    let all_succeeded = observations
        .iter()
        .all(|o| o.state.termination().is_some_and(Termination::is_success));
    let phase = if all_succeeded {
        PodPhase::Succeeded
    } else {
        PodPhase::Failed
    };
    (phase, statuses)
}

/// The kubelet's next action for a pod's **init-container** sequence — the pure
/// result of folding the ordered init-container observations against the pod
/// `restart_policy`. Init containers are the gating data-plane feature for
/// service meshes (the SPIRE-agent / proxy-bootstrap pattern) and a large slice
/// of workload conformance.
///
/// K8s init semantics this enum encodes: init containers run **one at a time,
/// in order**; each must exit 0 before the next starts; app containers do not
/// start until **every** init container has Succeeded. A failed init container
/// is restarted under `Always`/`OnFailure` and is terminal under `Never`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitAction {
    /// Init container at `index` is the first not-yet-succeeded init container.
    /// The kubelet must ensure it is started (if `Waiting`) or await its exit
    /// (if `Running` / restartable-`Terminated`). App containers must NOT start
    /// yet. This is the only arm that keeps the pod in `Pending`/initializing.
    AwaitInit {
        /// Every index the kubelet must ensure is running this tick: the
        /// sidecars cleared so far, plus the one container the sequence is
        /// gated on. Before sidecars this was always exactly one.
        start: Vec<usize>,
        /// The index the sequence is blocked on, or `None` when nothing blocks
        /// and only sidecars are still coming up.
        blocked_on: Option<usize>,
    },
    /// Init container `index` terminated non-restartably with `exit` (an
    /// unsuccessful exit under `restartPolicy: Never`) → the whole pod has
    /// **Failed** during init; app containers never start.
    InitFailed {
        /// 0-based index into `spec.initContainers`.
        index: usize,
        /// The unsuccessful exit that latched the failure.
        exit: Termination,
    },
    /// Every init container has Succeeded (exit 0), or there are no init
    /// containers. App containers may start; the pod is `Initialized`.
    Complete,
}

impl InitAction {
    /// `true` iff initialization is finished (app containers may start).
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self, InitAction::Complete)
    }

    /// `true` iff init has terminally failed (the pod is `Failed`).
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self, InitAction::InitFailed { .. })
    }
}

/// PURE init-sequence interpreter — the load-bearing core of init-container
/// support. Given the pod `restart_policy` and the **ordered** init-container
/// observations (`spec.initContainers` order), decide the next [`InitAction`].
/// NO I/O, NO podman — unit-testable in isolation exactly like
/// [`reconcile_pod_phase`].
///
/// Fold (first decisive init container wins, in order):
///   * `Terminated` exit 0 → that init container Succeeded; continue to the next.
///   * `Terminated` non-zero → restartable under the policy ⇒ `AwaitInit`
///     (the kubelet re-starts it); else ⇒ `InitFailed` (terminal).
///   * `Running` → `AwaitInit` (in flight; await its exit).
///   * `Waiting` → `AwaitInit` (needs a fresh start).
/// Empty slice (no init containers) → `Complete` — a pod with no init
/// containers initializes immediately (the behavior-preserving default: every
/// existing pod has zero init containers, so this returns `Complete` and the
/// kubelet's app-container path is reached identically to before this brick).
#[must_use]
pub fn next_init_action(
    restart_policy: RestartPolicy,
    init_observations: &[ContainerObservation],
) -> InitAction {
    let mut start: Vec<usize> = Vec::new();
    for (index, o) in init_observations.iter().enumerate() {
        if o.kind.is_sidecar() {
            // ── ★ SIDECARS ARE STARTED, NEVER AWAITED (KEP-753 R2/R5) ─────
            // The sequence proceeds once a sidecar has STARTED. It never waits
            // for one to exit — a sidecar by definition does not — which is
            // the whole reason a pod with one used to sit Pending forever.
            if !o.ever_started {
                start.push(index);
                // Upstream additionally gates on the sidecar's startupProbe
                // when it declares one. engenho runs no probes on init
                // containers at all today, so that gate degrades to
                // "proceed once started". Named rather than silently skipped:
                // `pending-sidecar-startup-probe`. The consequence is real —
                // a mesh proxy that has started but not programmed its
                // listeners will blackhole the app's first connections.
                continue;
            }
            if o.state.is_terminated() {
                // ★ RESTARTED UNCONDITIONALLY — the pod-level restartPolicy is
                // NOT consulted. Upstream restarts a sidecar regardless of the
                // pod policy, including `Never` and including a non-zero exit,
                // and a sidecar exiting never fails the pod. Consulting
                // `should_restart` here is the single most likely wrong port,
                // because that call sits in the arm right below.
                start.push(index);
            }
            continue;
        }
        match &o.state {
            // Succeeded → move on to the next init container.
            ContainerState::Terminated(exit) if exit.is_success() => continue,
            // Failed init container (a non-zero code, a signal, or an exit
            // nobody observed): restart per policy, else terminal failure.
            ContainerState::Terminated(exit) => {
                return if restart_policy.should_restart(*exit) {
                    start.push(index);
                    InitAction::AwaitInit {
                        start,
                        blocked_on: Some(index),
                    }
                } else {
                    InitAction::InitFailed { index, exit: *exit }
                };
            }
            // In flight or not yet started → this is the active init container.
            ContainerState::Running | ContainerState::Waiting { .. } => {
                if matches!(o.state, ContainerState::Waiting { .. }) {
                    start.push(index);
                }
                return InitAction::AwaitInit {
                    start,
                    blocked_on: Some(index),
                };
            }
        }
    }
    // Every REGULAR init container Succeeded (or there were none). If sidecars
    // still need starting or restarting, say so — but do not block: they are
    // started alongside the app containers, not before them.
    if start.is_empty() {
        InitAction::Complete
    } else {
        InitAction::AwaitInit {
            start,
            blocked_on: None,
        }
    }
}

/// PURE composition of init + app phase into the pod's reported
/// `(PodPhase, init_statuses, app_statuses, initialized)`. While init is
/// in-progress the pod is `Pending` and `initialized=false`; on terminal init
/// failure the pod is `Failed`; once init is `Complete` the app-container fold
/// ([`reconcile_pod_phase`]) decides the phase and `initialized=true`. The
/// kubelet renders `status.initContainerStatuses` from `init_statuses` and the
/// `Initialized` condition from `initialized`.
#[must_use]
pub fn reconcile_pod_phase_with_init(
    restart_policy: RestartPolicy,
    init_observations: &[ContainerObservation],
    app_observations: &[ContainerObservation],
) -> (
    PodPhase,
    Vec<ContainerStatusOut>,
    Vec<ContainerStatusOut>,
    bool,
) {
    let to_status = |o: &ContainerObservation| ContainerStatusOut {
        name: o.name.clone(),
        ready: o.ready,
        state: o.state.clone(),
        container_id: o.container_id.clone(),
        restart_count: o.restart_count,
    };
    let init_statuses: Vec<ContainerStatusOut> = init_observations.iter().map(to_status).collect();

    match next_init_action(restart_policy, init_observations) {
        InitAction::Complete => {
            let (phase, app_statuses) = reconcile_pod_phase(restart_policy, app_observations);
            (phase, init_statuses, app_statuses, true)
        }
        InitAction::InitFailed { .. } => {
            // App containers never started → Waiting/empty app statuses.
            let app_statuses: Vec<ContainerStatusOut> =
                app_observations.iter().map(to_status).collect();
            (PodPhase::Failed, init_statuses, app_statuses, false)
        }
        InitAction::AwaitInit { .. } => {
            let app_statuses: Vec<ContainerStatusOut> =
                app_observations.iter().map(to_status).collect();
            (PodPhase::Pending, init_statuses, app_statuses, false)
        }
    }
}

// ── Re-adoption: what the STORED status says a lost kubelet had started ────

/// One earlier run of a container, as the pod's stored status recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRun {
    /// `containerStatuses[].containerID`, when it was written.
    pub container_id: Option<String>,
    /// `containerStatuses[].restartCount`.
    pub restart_count: u32,
}

/// One container as the pod's stored status last described it — all a
/// kubelet that has no local record of the pod can still know about it.
///
/// ── ★ THE STORED STATUS IS THE ONLY SURVIVING WITNESS ─────────────────────
/// The kubelet's record of what it started lives in process memory. After a
/// restart, a pod bound to this node with no local record is either one that
/// never ran, or one a previous kubelet process ran — and the published
/// status is what tells the two apart. Treating the second as the first is
/// the defect this type exists to prevent: the pod was started again from
/// scratch, a `restartPolicy: Never` Job pod re-run in place, with nothing in
/// its status to say so.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredContainer {
    /// No run was ever published for it.
    NotStarted {
        /// `containerStatuses[].name`.
        name: String,
    },
    /// It was UP when last published — running, or held between restarts —
    /// so how it ended, if it has, nobody observed.
    Up {
        /// `containerStatuses[].name`.
        name: String,
        /// The run the status recorded.
        run: StoredRun,
    },
    /// It had ENDED, and the ending was observed and published.
    Ended {
        /// `containerStatuses[].name`.
        name: String,
        /// The run the status recorded.
        run: StoredRun,
        /// How it ended, read back from the wire.
        exit: Termination,
    },
}

impl StoredContainer {
    /// The container's name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::NotStarted { name } | Self::Up { name, .. } | Self::Ended { name, .. } => name,
        }
    }

    /// The published run, if it ever started.
    #[must_use]
    pub fn run(&self) -> Option<&StoredRun> {
        match self {
            Self::NotStarted { .. } => None,
            Self::Up { run, .. } | Self::Ended { run, .. } => Some(run),
        }
    }

    /// This container's status in a pod that will not be run again: a run
    /// nobody saw end is [`Termination::Unknown`]; an ending that was
    /// observed keeps what was observed; a container that never started is
    /// still `Waiting` — never a termination it did not have.
    #[must_use]
    pub fn lost_status(&self) -> ContainerStatusOut {
        let (state, run) = match self {
            Self::NotStarted { .. } => (ContainerState::creating(), None),
            Self::Up { run, .. } => (ContainerState::terminated(Termination::Unknown), Some(run)),
            Self::Ended { run, exit, .. } => (ContainerState::terminated(*exit), Some(run)),
        };
        ContainerStatusOut {
            name: self.name().to_string(),
            ready: false,
            state,
            container_id: run.and_then(|r| r.container_id.clone()),
            restart_count: run.map_or(0, |r| r.restart_count),
        }
    }
}

/// What the kubelet does with a bound pod it has no local record of, on a
/// runtime that cannot re-adopt what a previous kubelet process started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LostPod {
    /// No container was up when the status was last published: nothing was
    /// lost. Start it the ordinary way.
    NothingLost,
    /// Some container was up, and the policy restarts an exit nobody
    /// observed. Start it again — each container that had run is a counted
    /// restart ([`StartedAs::Restarted`]), not a first start.
    Restart,
    /// Some container was up and the policy is `Never`: it must not be run
    /// again, and it cannot succeed — an unobserved exit is never a success.
    /// The pod is `Failed`, with these container statuses.
    Failed(Vec<ContainerStatusOut>),
}

/// PURE re-adoption decision for a pod whose local record is gone, on a
/// runtime that cannot re-adopt. `stored` is the pod's APP containers as its
/// stored status last described them.
///
/// Upstream's shape: a container the kubelet knew was running and can no
/// longer find is terminated with `ContainerStatusUnknown` / 137; if the pod
/// is not deleted "it's been restarted — increment restart count", and under
/// `Never` no new sandbox is created, so the pod ends `Failed`.
#[must_use]
pub fn reconcile_lost_pod(restart_policy: RestartPolicy, stored: &[StoredContainer]) -> LostPod {
    if !stored
        .iter()
        .any(|c| matches!(c, StoredContainer::Up { .. }))
    {
        return LostPod::NothingLost;
    }
    // The one restart decision there is, asked about the one exit there is.
    if restart_policy.should_restart(Termination::Unknown) {
        return LostPod::Restart;
    }
    LostPod::Failed(stored.iter().map(StoredContainer::lost_status).collect())
}

/// How a container the start path just brought up relates to the run the
/// stored status recorded for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartedAs {
    /// No earlier run was published: a first start.
    Fresh,
    /// The runtime handed back the SAME container the status recorded: the
    /// run continues, and so does its restart count.
    Adopted {
        /// The count carried over unchanged.
        restart_count: u32,
    },
    /// A new container replaces an earlier run whose end this kubelet did
    /// not see: a restart, and it is counted.
    Restarted {
        /// The earlier run's count plus one.
        restart_count: u32,
    },
}

impl StartedAs {
    /// Relate the container `started_id`, started by a runtime with
    /// `readoption`, to the earlier run `prior`.
    ///
    /// ── ★ AN ID MATCH IS AN ADOPTION ONLY WHERE ADOPTION EXISTS ───────────
    /// The native backend derives a container's id from
    /// `namespace/pod/container`, so a process started fresh after a kubelet
    /// restart carries the SAME id as the run the old kubelet lost. Reading
    /// that as "the same container" would leave the restart uncounted on
    /// exactly the node the rule exists for. On a runtime that cannot
    /// re-adopt, every start over a published run is a restart.
    #[must_use]
    pub fn of(prior: Option<&StoredRun>, started_id: &str, readoption: Readoption) -> Self {
        match prior {
            None => Self::Fresh,
            Some(run)
                if readoption == Readoption::AdoptsRunning
                    && run.container_id.as_deref() == Some(started_id) =>
            {
                Self::Adopted {
                    restart_count: run.restart_count,
                }
            }
            Some(run) => Self::Restarted {
                restart_count: run.restart_count.saturating_add(1),
            },
        }
    }

    /// The restart count the new record starts from.
    #[must_use]
    pub fn restart_count(self) -> u32 {
        match self {
            Self::Fresh => 0,
            Self::Adopted { restart_count } | Self::Restarted { restart_count } => restart_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(name: &str, id: &str) -> ContainerObservation {
        ContainerObservation::running(name, id, 0)
    }
    fn terminated(name: &str, id: &str, code: i32) -> ContainerObservation {
        ContainerObservation::terminated(name, id, ExitDisposition::Code(code), 0)
    }
    fn killed(name: &str, id: &str, signal: i32) -> ContainerObservation {
        ContainerObservation::terminated(name, id, ExitDisposition::Signal(signal), 0)
    }
    fn unobserved(name: &str, id: &str) -> ContainerObservation {
        ContainerObservation::terminated(name, id, Termination::Unknown, 0)
    }
    fn waiting(name: &str) -> ContainerObservation {
        ContainerObservation::waiting(name)
    }

    // ── RestartPolicy parsing + should_restart ──────────────────────────

    #[test]
    fn restart_policy_default_is_always() {
        assert_eq!(RestartPolicy::default(), RestartPolicy::Always);
        assert_eq!(RestartPolicy::from_spec_str(None), RestartPolicy::Always);
        assert_eq!(
            RestartPolicy::from_spec_str(Some("Always")),
            RestartPolicy::Always
        );
        // Unrecognized → Always (K8s default), never a panic.
        assert_eq!(
            RestartPolicy::from_spec_str(Some("Bogus")),
            RestartPolicy::Always
        );
    }

    #[test]
    fn restart_policy_parses_never_and_onfailure() {
        assert_eq!(
            RestartPolicy::from_spec_str(Some("Never")),
            RestartPolicy::Never
        );
        assert_eq!(
            RestartPolicy::from_spec_str(Some("OnFailure")),
            RestartPolicy::OnFailure
        );
    }

    #[test]
    fn should_restart_matrix() {
        let clean: Termination = ExitDisposition::Code(0).into();
        let failed: Termination = ExitDisposition::Code(1).into();
        let sigkill: Termination = ExitDisposition::Signal(9).into();
        let unknown = Termination::Unknown;
        // Always → always, however it ended.
        for exit in [clean, failed, sigkill, unknown] {
            assert!(RestartPolicy::Always.should_restart(exit), "{exit}");
        }
        // Never → never, however it ended.
        for exit in [clean, failed, sigkill, unknown] {
            assert!(!RestartPolicy::Never.should_restart(exit), "{exit}");
        }
        // OnFailure → everything but an observed clean exit. A signal and an
        // unobserved exit are failures (upstream's Unknown rule).
        assert!(!RestartPolicy::OnFailure.should_restart(clean));
        assert!(RestartPolicy::OnFailure.should_restart(failed));
        assert!(RestartPolicy::OnFailure.should_restart(sigkill));
        assert!(RestartPolicy::OnFailure.should_restart(unknown));
    }

    // ── Termination: the T1.2 exit disposition ──────────────────────────

    #[test]
    fn a_sigkilled_never_pod_is_failed_not_succeeded() {
        // ★ The live defect on ryn: a SIGKILL has no exit code, the kubelet
        // read "no code" as 0, and a killed Never pod was published
        // Succeeded — which a Job then counted as a completion.
        let (phase, st) = reconcile_pod_phase(RestartPolicy::Never, &[killed("a", "id-a", 9)]);
        assert_eq!(phase, PodPhase::Failed);
        assert_eq!(
            st[0].state,
            ContainerState::Terminated(Termination::Observed(ExitDisposition::Signal(9)))
        );
        // The same kill beside a clean sibling still fails the pod.
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Never,
            &[terminated("a", "id-a", 0), killed("b", "id-b", 9)],
        );
        assert_eq!(phase, PodPhase::Failed);
    }

    #[test]
    fn an_unobserved_exit_restarts_under_always_and_onfailure_and_fails_under_never() {
        // Upstream's rule for a container whose end nobody saw.
        let obs = [unobserved("a", "id-a")];
        assert_eq!(
            reconcile_pod_phase(RestartPolicy::Always, &obs).0,
            PodPhase::Running,
            "Always: restarted, so the pod stays Running"
        );
        assert_eq!(
            reconcile_pod_phase(RestartPolicy::OnFailure, &obs).0,
            PodPhase::Running,
            "OnFailure: an unobserved exit is a failure, so it is restarted"
        );
        assert_eq!(
            reconcile_pod_phase(RestartPolicy::Never, &obs).0,
            PodPhase::Failed,
            "Never: latched Failed — never the Succeeded a default 0 produced"
        );
    }

    #[test]
    fn an_unobserved_exit_renders_as_upstreams_137_container_status_unknown() {
        // Internally it stays Unknown; only the wire gets the numbers.
        assert_eq!(Termination::Unknown.exit_code(), 137);
        assert_eq!(Termination::Unknown.reason(), "ContainerStatusUnknown");
        assert!(!Termination::Unknown.is_success());
        // Distinct from a genuine exit 137 everywhere except the wire code.
        let real_137: Termination = ExitDisposition::Code(137).into();
        assert_ne!(real_137, Termination::Unknown);
        assert_eq!(real_137.reason(), "Error");
        // A signal renders 128+n with upstream's generic reason.
        let sigkill: Termination = ExitDisposition::Signal(9).into();
        assert_eq!(sigkill.exit_code(), 137);
        assert_eq!(sigkill.reason(), "Error");
        // And the one success.
        let clean: Termination = ExitDisposition::Code(0).into();
        assert_eq!((clean.exit_code(), clean.reason()), (0, "Completed"));
        assert!(clean.is_success());
    }

    #[test]
    fn a_run_state_maps_to_a_termination_only_when_not_running() {
        assert_eq!(Termination::from_run_state(RunState::Running), None);
        assert_eq!(
            Termination::from_run_state(RunState::Exited(ExitDisposition::Signal(9))),
            Some(Termination::Observed(ExitDisposition::Signal(9)))
        );
        // The runtime lost it, or it is Created though we started it: down,
        // cause unseen. Never a clean exit.
        assert_eq!(
            Termination::from_run_state(RunState::Unknown),
            Some(Termination::Unknown)
        );
        assert_eq!(
            Termination::from_run_state(RunState::Created),
            Some(Termination::Unknown)
        );
    }

    // ── T1.2 c2: the latch, and re-adoption ─────────────────────────────

    /// A container that has started is never re-reported as not yet started,
    /// whatever else its observation lacks: the fold reads the latch.
    #[test]
    fn a_started_waiting_container_is_not_pending() {
        let held = ContainerObservation::waiting("a").with_ever_started(true);
        for policy in [RestartPolicy::Always, RestartPolicy::OnFailure] {
            let (phase, _) = reconcile_pod_phase(policy, std::slice::from_ref(&held));
            assert_eq!(phase, PodPhase::Running, "{policy:?}");
        }
        // Control: the same state, never started, is Pending.
        let (phase, _) = reconcile_pod_phase(RestartPolicy::Always, &[waiting("a")]);
        assert_eq!(phase, PodPhase::Pending);
    }

    fn up(name: &str, id: &str, restarts: u32) -> StoredContainer {
        StoredContainer::Up {
            name: name.into(),
            run: StoredRun {
                container_id: Some(id.into()),
                restart_count: restarts,
            },
        }
    }

    #[test]
    fn a_lost_pod_with_nothing_up_lost_nothing() {
        let stored = vec![
            StoredContainer::NotStarted { name: "a".into() },
            StoredContainer::Ended {
                name: "b".into(),
                run: StoredRun {
                    container_id: Some("id-b".into()),
                    restart_count: 2,
                },
                exit: ExitDisposition::Code(1).into(),
            },
        ];
        for policy in [
            RestartPolicy::Always,
            RestartPolicy::OnFailure,
            RestartPolicy::Never,
        ] {
            assert_eq!(
                reconcile_lost_pod(policy, &stored),
                LostPod::NothingLost,
                "{policy:?}"
            );
        }
        assert_eq!(
            reconcile_lost_pod(RestartPolicy::Never, &[]),
            LostPod::NothingLost
        );
    }

    #[test]
    fn a_lost_run_restarts_under_a_restarting_policy() {
        let stored = vec![up("a", "id-a", 0)];
        assert_eq!(
            reconcile_lost_pod(RestartPolicy::Always, &stored),
            LostPod::Restart
        );
        assert_eq!(
            reconcile_lost_pod(RestartPolicy::OnFailure, &stored),
            LostPod::Restart,
            "an unobserved exit is not a success, so OnFailure restarts it"
        );
    }

    #[test]
    fn a_lost_run_under_never_fails_the_pod_and_invents_nothing() {
        let stored = vec![
            up("a", "id-a", 3),
            StoredContainer::Ended {
                name: "b".into(),
                run: StoredRun {
                    container_id: Some("id-b".into()),
                    restart_count: 0,
                },
                exit: ExitDisposition::Code(0).into(),
            },
            StoredContainer::NotStarted { name: "c".into() },
        ];
        let LostPod::Failed(statuses) = reconcile_lost_pod(RestartPolicy::Never, &stored) else {
            panic!("Never + a lost run must fail the pod");
        };
        // The run nobody saw end: Unknown, with the id and count it had.
        assert_eq!(
            statuses[0].state,
            ContainerState::terminated(Termination::Unknown)
        );
        assert_eq!(statuses[0].container_id.as_deref(), Some("id-a"));
        assert_eq!(statuses[0].restart_count, 3);
        // An ending that WAS observed keeps what was observed.
        assert_eq!(
            statuses[1].state,
            ContainerState::terminated(ExitDisposition::Code(0))
        );
        // A container that never started is not given a termination.
        assert!(matches!(statuses[2].state, ContainerState::Waiting { .. }));
        assert_eq!(statuses[2].container_id, None);
        assert!(statuses.iter().all(|s| !s.ready));
    }

    #[test]
    fn a_container_started_over_a_published_run_is_adopted_or_counted() {
        let prior = StoredRun {
            container_id: Some("id-1".into()),
            restart_count: 4,
        };
        let adopts = Readoption::AdoptsRunning;
        assert_eq!(StartedAs::of(None, "id-9", adopts), StartedAs::Fresh);
        assert_eq!(
            StartedAs::of(None, "id-9", Readoption::Cannot).restart_count(),
            0
        );
        assert_eq!(
            StartedAs::of(Some(&prior), "id-1", adopts),
            StartedAs::Adopted { restart_count: 4 },
            "the same container continues its run"
        );
        assert_eq!(
            StartedAs::of(Some(&prior), "id-2", adopts),
            StartedAs::Restarted { restart_count: 5 },
            "a new container over a lost run is a counted restart"
        );
        let unnamed = StoredRun {
            container_id: None,
            restart_count: 0,
        };
        assert_eq!(
            StartedAs::of(Some(&unnamed), "id-2", adopts).restart_count(),
            1,
            "a run with no published id cannot be the one just started"
        );
    }

    /// ★ The native backend's ids are `namespace/pod/container`: a fresh
    /// process after a kubelet restart has the SAME id as the lost run. On a
    /// runtime that cannot re-adopt, that is still a restart, and it counts.
    #[test]
    fn an_id_match_is_not_an_adoption_on_a_runtime_that_cannot_adopt() {
        let prior = StoredRun {
            container_id: Some("default/job/main".into()),
            restart_count: 0,
        };
        assert_eq!(
            StartedAs::of(Some(&prior), "default/job/main", Readoption::Cannot),
            StartedAs::Restarted { restart_count: 1 }
        );
    }

    #[test]
    fn a_wire_termination_reads_back_without_inventing_success() {
        assert_eq!(
            Termination::from_wire(Some(0), Some("Completed")),
            Termination::Observed(ExitDisposition::Code(0))
        );
        assert_eq!(
            Termination::from_wire(Some(137), Some(Termination::UNKNOWN_REASON)),
            Termination::Unknown
        );
        assert_eq!(
            Termination::from_wire(None, Some("Completed")),
            Termination::Unknown,
            "no code is not a zero code"
        );
        assert_eq!(
            Termination::from_wire(Some(i64::MAX), Some("Error")),
            Termination::Unknown,
            "an unrepresentable code is not a code"
        );
    }

    #[test]
    fn a_killed_init_container_fails_the_pod_under_never() {
        let obs = vec![killed("init-0", "id0", 9)];
        assert_eq!(
            next_init_action(RestartPolicy::Never, &obs),
            InitAction::InitFailed {
                index: 0,
                exit: ExitDisposition::Signal(9).into()
            }
        );
        // And is restarted, not advanced past, under OnFailure.
        assert!(!next_init_action(RestartPolicy::OnFailure, &obs).is_complete());
    }

    // ── reconcile_pod_phase: single-container ───────────────────────────

    #[test]
    fn empty_observations_is_pending() {
        let (phase, statuses) = reconcile_pod_phase(RestartPolicy::Always, &[]);
        assert_eq!(phase, PodPhase::Pending);
        assert!(statuses.is_empty());
    }

    #[test]
    fn single_running_is_running() {
        let (phase, st) = reconcile_pod_phase(RestartPolicy::Never, &[running("a", "id-a")]);
        assert_eq!(phase, PodPhase::Running);
        assert_eq!(st.len(), 1);
        assert!(st[0].state.is_running());
        assert!(st[0].ready);
    }

    #[test]
    fn single_waiting_is_pending() {
        let (phase, st) = reconcile_pod_phase(RestartPolicy::Always, &[waiting("a")]);
        assert_eq!(phase, PodPhase::Pending);
        assert!(matches!(st[0].state, ContainerState::Waiting { .. }));
        assert!(!st[0].ready);
        assert!(st[0].container_id.is_none());
    }

    #[test]
    fn single_terminated_zero_never_is_succeeded() {
        let (phase, st) = reconcile_pod_phase(RestartPolicy::Never, &[terminated("a", "id-a", 0)]);
        assert_eq!(phase, PodPhase::Succeeded);
        assert_eq!(
            st[0].state.termination(),
            Some(Termination::Observed(ExitDisposition::Code(0)))
        );
    }

    #[test]
    fn single_terminated_nonzero_never_is_failed() {
        let (phase, _) = reconcile_pod_phase(RestartPolicy::Never, &[terminated("a", "id-a", 137)]);
        assert_eq!(phase, PodPhase::Failed);
    }

    #[test]
    fn single_terminated_under_always_stays_running() {
        // restartPolicy:Always — an exited container is about to be
        // restarted, so the pod is still Running (recovers). Even exit 0.
        let (phase, _) = reconcile_pod_phase(RestartPolicy::Always, &[terminated("a", "id-a", 0)]);
        assert_eq!(phase, PodPhase::Running);
        let (phase, _) = reconcile_pod_phase(RestartPolicy::Always, &[terminated("a", "id-a", 1)]);
        assert_eq!(phase, PodPhase::Running);
    }

    #[test]
    fn single_terminated_onfailure_zero_succeeds_nonzero_restarts() {
        // OnFailure + exit 0 → no restart → Succeeded.
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::OnFailure, &[terminated("a", "id-a", 0)]);
        assert_eq!(phase, PodPhase::Succeeded);
        // OnFailure + non-zero → restartable → Running (kubelet restarts).
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::OnFailure, &[terminated("a", "id-a", 1)]);
        assert_eq!(phase, PodPhase::Running);
    }

    // ── reconcile_pod_phase: multi-container ────────────────────────────

    #[test]
    fn two_running_is_running() {
        let (phase, st) = reconcile_pod_phase(
            RestartPolicy::Never,
            &[running("a", "id-a"), running("b", "id-b")],
        );
        assert_eq!(phase, PodPhase::Running);
        assert_eq!(st.len(), 2);
        assert!(st.iter().all(|s| s.state.is_running()));
    }

    #[test]
    fn one_running_one_waiting_is_pending() {
        // Until ALL containers have started, the pod is Pending.
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::Always, &[running("a", "id-a"), waiting("b")]);
        assert_eq!(phase, PodPhase::Pending);
    }

    #[test]
    fn two_terminated_zero_never_is_succeeded() {
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Never,
            &[terminated("a", "id-a", 0), terminated("b", "id-b", 0)],
        );
        assert_eq!(phase, PodPhase::Succeeded);
    }

    #[test]
    fn two_terminated_one_nonzero_never_is_failed() {
        // Worst-case fold: any non-zero exit → Failed.
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Never,
            &[terminated("a", "id-a", 0), terminated("b", "id-b", 5)],
        );
        assert_eq!(phase, PodPhase::Failed);
    }

    #[test]
    fn running_plus_terminated_always_stays_running() {
        // One container up, one exited, restartPolicy:Always → Running
        // (the exited one is about to be restarted).
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Always,
            &[running("a", "id-a"), terminated("b", "id-b", 1)],
        );
        assert_eq!(phase, PodPhase::Running);
    }

    #[test]
    fn running_plus_terminated_never_stays_running_while_one_runs() {
        // restartPolicy:Never, one container still running + one exited:
        // the pod is still Running as long as a container runs (it has not
        // reached the all-terminated terminal fold).
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Never,
            &[running("a", "id-a"), terminated("b", "id-b", 0)],
        );
        assert_eq!(phase, PodPhase::Running);
    }

    #[test]
    fn container_statuses_preserve_restart_count_and_id() {
        let obs = ContainerObservation::running("web", "cid-9", 3);
        let (_, st) = reconcile_pod_phase(RestartPolicy::Always, &[obs]);
        assert_eq!(st[0].restart_count, 3);
        assert_eq!(st[0].container_id.as_deref(), Some("cid-9"));
        assert_eq!(st[0].name, "web");
    }

    // ── next_init_action (pure init-container sequencer) ─────────────────

    #[test]
    fn no_init_containers_completes_immediately() {
        // The behavior-preserving default: every pre-init-brick pod has zero
        // init containers, so the fold returns Complete and the kubelet reaches
        // the app-container path identically to before.
        assert_eq!(
            next_init_action(RestartPolicy::Always, &[]),
            InitAction::Complete
        );
        assert!(next_init_action(RestartPolicy::Never, &[]).is_complete());
    }

    // ── Native sidecars (KEP-753) ────────────────────────────────────────

    #[test]
    fn a_sidecar_does_not_block_the_init_sequence() {
        // THE BUG. A sidecar never exits, and the old fold's only path to
        // Complete was `Terminated{0}` for EVERY init container — so one
        // sidecar pinned the pod Pending forever, with no event, no failing
        // condition and no non-zero exit. The reconcile loop looked healthy
        // and busy while making zero progress, indefinitely.
        let obs = vec![
            running("proxy", "id0").as_sidecar(),
            terminated("setup", "id1", 0),
        ];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::Complete
        );

        // NEGATIVE CONTROL: the identical slice with a REGULAR container in
        // position 0 must still block. Without this the test would pass on a
        // fold that simply stopped blocking on everything.
        let obs_regular = vec![running("proxy", "id0"), terminated("setup", "id1", 0)];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs_regular),
            InitAction::AwaitInit {
                start: vec![],
                blocked_on: Some(0)
            }
        );
    }

    #[test]
    fn a_sidecar_exit_never_fails_the_pod_even_under_restart_policy_never() {
        // R5: a sidecar is restarted regardless of the pod-level policy —
        // including Never, including a non-zero exit — and its exit never
        // fails the pod. Consulting `should_restart` here is the single most
        // likely wrong port, because that call sits in the arm right below.
        let obs = vec![
            terminated("proxy", "id0", 7).as_sidecar(),
            terminated("setup", "id1", 0),
        ];
        assert_eq!(
            next_init_action(RestartPolicy::Never, &obs),
            InitAction::AwaitInit {
                start: vec![0],
                blocked_on: None
            },
            "restart it, do not fail the pod, and do not block"
        );

        // NEGATIVE CONTROL: the same exit on a REGULAR init container under
        // Never is terminal. This is the pair that proves `should_restart` is
        // no longer consulted for sidecars — a naive port returns InitFailed
        // for both.
        let obs_regular = vec![terminated("setup", "id0", 7)];
        assert_eq!(
            next_init_action(RestartPolicy::Never, &obs_regular),
            InitAction::InitFailed {
                index: 0,
                exit: ExitDisposition::Code(7).into()
            }
        );
    }

    #[test]
    fn an_unstarted_sidecar_is_started_without_blocking_what_follows() {
        // R2: started, never awaited. The sequence proceeds past it.
        let obs = vec![waiting("proxy").as_sidecar(), waiting("setup")];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::AwaitInit {
                start: vec![0, 1],
                blocked_on: Some(1)
            },
            "start the sidecar AND the regular one; block only on the regular"
        );
    }

    #[test]
    fn ever_started_is_latched_not_derived_from_the_current_state() {
        // A sidecar caught mid-restart is not Running. If startedness were
        // re-derived from `state`, it would re-block the sequence — a pod that
        // was Running silently falling back to Pending on a hiccup, i.e. the
        // original hang with an intermittent trigger.
        let restarting = ContainerObservation::waiting("proxy")
            .as_sidecar()
            .with_ever_started(true);
        let obs = vec![restarting, terminated("setup", "id1", 0)];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::Complete,
            "a restarting sidecar must not re-block init"
        );
    }

    #[test]
    fn an_unrecognised_restart_policy_is_an_error_not_a_default() {
        // `Always` is the only value the API permits on an init container.
        // Defaulting anything else to Regular — the way RestartPolicy's own
        // from_spec_str defaults a bad POD policy to Always — would make
        // `restartPolicy: always` (lowercase) silently revert to blocking
        // semantics: the forever-Pending hang, triggered by a typo, with no
        // error anywhere.
        assert_eq!(InitKind::from_spec_str(None), Ok(InitKind::Regular));
        assert_eq!(
            InitKind::from_spec_str(Some("Always")),
            Ok(InitKind::Sidecar)
        );
        assert!(InitKind::from_spec_str(Some("always")).is_err());
        assert!(InitKind::from_spec_str(Some("OnFailure")).is_err());
        // The error names the offending value, so the log says what to fix.
        let e = InitKind::from_spec_str(Some("always")).unwrap_err();
        assert!(format!("{e}").contains("always"), "{e}");
    }

    #[test]
    fn first_waiting_init_is_the_active_one() {
        // init[0] not yet started → AwaitInit{0}; later ones are irrelevant.
        let obs = vec![waiting("init-0"), waiting("init-1")];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::AwaitInit {
                start: vec![0],
                blocked_on: Some(0)
            }
        );
    }

    #[test]
    fn running_init_is_awaited_not_skipped() {
        // init[0] in flight → await it; init[1] must NOT start concurrently.
        let obs = vec![running("init-0", "id0"), waiting("init-1")];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            // Running already: nothing to START, but still blocking.
            InitAction::AwaitInit {
                start: vec![],
                blocked_on: Some(0)
            }
        );
    }

    #[test]
    fn sequential_advance_after_success() {
        // init[0] Succeeded (exit 0) → advance to init[1].
        let obs = vec![terminated("init-0", "id0", 0), waiting("init-1")];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::AwaitInit {
                start: vec![1],
                blocked_on: Some(1)
            }
        );
    }

    #[test]
    fn all_init_succeeded_completes() {
        let obs = vec![
            terminated("init-0", "id0", 0),
            terminated("init-1", "id1", 0),
        ];
        assert_eq!(
            next_init_action(RestartPolicy::Never, &obs),
            InitAction::Complete
        );
    }

    #[test]
    fn failed_init_under_never_is_terminal() {
        // restartPolicy: Never + non-zero init exit → InitFailed (pod Failed).
        let obs = vec![terminated("init-0", "id0", 7)];
        assert_eq!(
            next_init_action(RestartPolicy::Never, &obs),
            InitAction::InitFailed {
                index: 0,
                exit: ExitDisposition::Code(7).into()
            }
        );
    }

    #[test]
    fn failed_init_under_always_restarts() {
        // restartPolicy: Always restarts a failed init container (AwaitInit).
        let obs = vec![terminated("init-0", "id0", 7)];
        assert_eq!(
            next_init_action(RestartPolicy::Always, &obs),
            InitAction::AwaitInit {
                start: vec![0],
                blocked_on: Some(0)
            }
        );
    }

    #[test]
    fn failed_init_under_onfailure_restarts_nonzero_but_advances_on_zero() {
        // OnFailure: non-zero init exit restarts; a zero exit advances.
        let nonzero = vec![terminated("init-0", "id0", 3)];
        assert_eq!(
            next_init_action(RestartPolicy::OnFailure, &nonzero),
            InitAction::AwaitInit {
                start: vec![0],
                blocked_on: Some(0)
            }
        );
        let zero = vec![terminated("init-0", "id0", 0), waiting("init-1")];
        assert_eq!(
            next_init_action(RestartPolicy::OnFailure, &zero),
            InitAction::AwaitInit {
                start: vec![1],
                blocked_on: Some(1)
            }
        );
    }

    // ── reconcile_pod_phase_with_init (init + app composition) ───────────

    #[test]
    fn pod_is_pending_while_init_runs_and_app_untouched() {
        let init = vec![running("init-0", "id0")];
        let app = vec![waiting("app")];
        let (phase, init_st, app_st, initialized) =
            reconcile_pod_phase_with_init(RestartPolicy::Always, &init, &app);
        assert_eq!(phase, PodPhase::Pending);
        assert!(!initialized);
        assert_eq!(init_st.len(), 1);
        assert_eq!(app_st.len(), 1);
    }

    #[test]
    fn pod_failed_when_init_fails_terminally() {
        let init = vec![terminated("init-0", "id0", 9)];
        let app = vec![waiting("app")];
        let (phase, _, _, initialized) =
            reconcile_pod_phase_with_init(RestartPolicy::Never, &init, &app);
        assert_eq!(phase, PodPhase::Failed);
        assert!(!initialized);
    }

    #[test]
    fn app_phase_governs_once_init_complete() {
        // All init Succeeded → initialized=true → app fold decides the phase.
        let init = vec![terminated("init-0", "id0", 0)];
        let app_running = vec![running("app", "appid")];
        let (phase, _, _, initialized) =
            reconcile_pod_phase_with_init(RestartPolicy::Always, &init, &app_running);
        assert_eq!(phase, PodPhase::Running);
        assert!(initialized);
    }

    #[test]
    fn no_init_pod_phase_matches_bare_app_fold() {
        // With no init containers, the composition is byte-equivalent to the
        // existing reconcile_pod_phase over the app containers (regression
        // guard for the behavior-preserving default).
        let app = vec![running("app", "appid")];
        let (p_with, _, app_st, initialized) =
            reconcile_pod_phase_with_init(RestartPolicy::Always, &[], &app);
        let (p_bare, bare_st) = reconcile_pod_phase(RestartPolicy::Always, &app);
        assert_eq!(p_with, p_bare);
        assert!(initialized);
        assert_eq!(app_st, bare_st);
    }
}
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Every way a container can end: a small code range (0 included), a
    /// signal, or an exit nobody observed.
    fn termination_strategy() -> impl Strategy<Value = Termination> {
        prop_oneof![
            (-5i32..5).prop_map(|c| Termination::Observed(ExitDisposition::Code(c))),
            (1i32..32).prop_map(|s| Termination::Observed(ExitDisposition::Signal(s))),
            Just(Termination::Unknown),
        ]
    }

    /// Strategy for a single container observation (Running / Terminated /
    /// Waiting).
    fn obs_strategy() -> impl Strategy<Value = ContainerObservation> {
        prop_oneof![
            (any::<u32>()).prop_map(|rc| ContainerObservation::running("c", "id", rc % 10)),
            (termination_strategy(), any::<u32>()).prop_map(|(exit, rc)| {
                ContainerObservation::terminated("c", "id", exit, rc % 10)
            }),
            Just(ContainerObservation::waiting("c")),
        ]
    }

    fn policy_strategy() -> impl Strategy<Value = RestartPolicy> {
        prop_oneof![
            Just(RestartPolicy::Always),
            Just(RestartPolicy::OnFailure),
            Just(RestartPolicy::Never),
        ]
    }

    proptest! {
        /// The fold is TOTAL: any policy + any observation slice yields a
        /// phase + one status per observation. Never panics.
        #[test]
        fn fold_is_total_and_status_count_matches(
            policy in policy_strategy(),
            obs in prop::collection::vec(obs_strategy(), 0..6),
        ) {
            let (_phase, statuses) = reconcile_pod_phase(policy, &obs);
            prop_assert_eq!(statuses.len(), obs.len());
        }

        /// Under restartPolicy:Always, a slice with NO Waiting container is
        /// NEVER terminal (Succeeded/Failed): every exit restarts, so the
        /// pod recovers. This is the Always anti-latch invariant.
        #[test]
        fn always_never_latches_terminal(
            obs in prop::collection::vec(
                prop_oneof![
                    (any::<u32>()).prop_map(|rc| ContainerObservation::running("c", "id", rc % 10)),
                    (termination_strategy(), any::<u32>())
                        .prop_map(|(exit, rc)| ContainerObservation::terminated("c", "id", exit, rc % 10)),
                ],
                1..6,
            ),
        ) {
            let (phase, _) = reconcile_pod_phase(RestartPolicy::Always, &obs);
            prop_assert!(
                matches!(phase, PodPhase::Running),
                "Always with no Waiting must be Running, got {phase:?}"
            );
        }

        /// Under restartPolicy:Never, a slice where EVERY container has
        /// terminated yields a TERMINAL phase (never Running/Pending) — the
        /// Never terminal-latch invariant.
        #[test]
        fn never_all_terminated_is_terminal(
            exits in prop::collection::vec(termination_strategy(), 1..6),
        ) {
            let obs: Vec<ContainerObservation> = exits
                .iter()
                .enumerate()
                .map(|(i, e)| ContainerObservation::terminated(format!("c{i}"), format!("id{i}"), *e, 0))
                .collect();
            let (phase, _) = reconcile_pod_phase(RestartPolicy::Never, &obs);
            prop_assert!(
                matches!(phase, PodPhase::Succeeded | PodPhase::Failed),
                "Never + all-terminated must be terminal, got {phase:?}"
            );
            // Succeeded iff every container was OBSERVED to exit with code 0 —
            // a signal or an unobserved exit anywhere makes it Failed.
            let all_zero = exits
                .iter()
                .all(|e| *e == Termination::Observed(ExitDisposition::Code(0)));
            if all_zero {
                prop_assert_eq!(phase, PodPhase::Succeeded);
            } else {
                prop_assert_eq!(phase, PodPhase::Failed);
            }
        }

        /// Reading a termination back from the wire keeps whether it was a
        /// success — a signal folds to its `128 + n` code, an Unknown stays
        /// Unknown, and nothing becomes a success on the round trip.
        #[test]
        fn a_termination_survives_the_wire_round_trip(t in termination_strategy()) {
            let back = Termination::from_wire(Some(i64::from(t.exit_code())), Some(t.reason()));
            prop_assert_eq!(back.is_success(), t.is_success());
            prop_assert_eq!(back.exit_code(), t.exit_code());
            prop_assert_eq!(back.reason(), t.reason());
            if matches!(t, Termination::Unknown) {
                prop_assert_eq!(back, Termination::Unknown);
            }
        }

        /// A slice with ANY Waiting container is ALWAYS Pending, under any
        /// policy (the pod isn't Running until every container has started).
        #[test]
        fn any_waiting_is_pending(
            policy in policy_strategy(),
            mut obs in prop::collection::vec(obs_strategy(), 0..5),
        ) {
            obs.push(ContainerObservation::waiting("forced-waiting"));
            let (phase, _) = reconcile_pod_phase(policy, &obs);
            prop_assert_eq!(phase, PodPhase::Pending);
        }
    }
}
