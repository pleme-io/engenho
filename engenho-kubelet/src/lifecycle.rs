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

    /// `true` iff a container that has terminated with `exit_code` should be
    /// restarted under this policy. The kubelet uses this to decide whether
    /// to re-`start` the one exited container.
    ///
    ///   * `Always`    → always restart.
    ///   * `OnFailure` → restart iff the exit code is non-zero.
    ///   * `Never`     → never restart.
    ///
    /// A terminated container with NO recorded exit code is treated as a
    /// non-zero/abnormal exit (defensively unhealthy) — so `OnFailure`
    /// restarts it.
    #[must_use]
    pub fn should_restart(self, exit_code: Option<i32>) -> bool {
        match self {
            RestartPolicy::Always => true,
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure => exit_code != Some(0),
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
    /// The container has terminated. Renders
    /// `state: { terminated: { exitCode, reason } }`.
    Terminated {
        /// Process exit code (`0` = clean).
        exit_code: i32,
        /// Short reason string (`"Completed"` for exit 0, else `"Error"`).
        reason: String,
    },
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
        matches!(self, ContainerState::Terminated { .. })
    }

    /// The exit code if terminated, else `None`.
    #[must_use]
    pub fn exit_code(&self) -> Option<i32> {
        match self {
            ContainerState::Terminated { exit_code, .. } => Some(*exit_code),
            _ => None,
        }
    }

    /// Build the canonical `Terminated` state from an exit code, deriving the
    /// reason (`"Completed"` for 0, `"Error"` otherwise).
    #[must_use]
    pub fn terminated(exit_code: i32) -> Self {
        ContainerState::Terminated {
            exit_code,
            reason: if exit_code == 0 { "Completed" } else { "Error" }.to_string(),
        }
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
}

impl ContainerObservation {
    /// A `Running` observation for a started container.
    #[must_use]
    pub fn running(name: impl Into<String>, container_id: impl Into<String>, restart_count: u32) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::Running,
            container_id: Some(container_id.into()),
            restart_count,
            ready: true,
        }
    }

    /// A `Terminated` observation for a container that exited.
    #[must_use]
    pub fn terminated(
        name: impl Into<String>,
        container_id: impl Into<String>,
        exit_code: i32,
        restart_count: u32,
    ) -> Self {
        Self {
            name: name.into(),
            state: ContainerState::terminated(exit_code),
            container_id: Some(container_id.into()),
            restart_count,
            ready: false,
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
        }
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
///   * **Any container `Waiting`** (not yet started) → `Pending`. The pod is
///     not `Running` until *every* container is up at least once.
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

    let any_waiting = observations.iter().any(|o| matches!(o.state, ContainerState::Waiting { .. }));
    if any_waiting {
        // Not every container is up yet → Pending. Never a fake Running.
        return (PodPhase::Pending, statuses);
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
            .exit_code()
            .map(|code| restart_policy.should_restart(Some(code)))
            .unwrap_or(false)
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
    // fold: Succeeded iff every container exited 0, else Failed.
    let all_zero = observations
        .iter()
        .all(|o| o.state.exit_code() == Some(0));
    let phase = if all_zero { PodPhase::Succeeded } else { PodPhase::Failed };
    (phase, statuses)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(name: &str, id: &str) -> ContainerObservation {
        ContainerObservation::running(name, id, 0)
    }
    fn terminated(name: &str, id: &str, code: i32) -> ContainerObservation {
        ContainerObservation::terminated(name, id, code, 0)
    }
    fn waiting(name: &str) -> ContainerObservation {
        ContainerObservation::waiting(name)
    }

    // ── RestartPolicy parsing + should_restart ──────────────────────────

    #[test]
    fn restart_policy_default_is_always() {
        assert_eq!(RestartPolicy::default(), RestartPolicy::Always);
        assert_eq!(RestartPolicy::from_spec_str(None), RestartPolicy::Always);
        assert_eq!(RestartPolicy::from_spec_str(Some("Always")), RestartPolicy::Always);
        // Unrecognized → Always (K8s default), never a panic.
        assert_eq!(RestartPolicy::from_spec_str(Some("Bogus")), RestartPolicy::Always);
    }

    #[test]
    fn restart_policy_parses_never_and_onfailure() {
        assert_eq!(RestartPolicy::from_spec_str(Some("Never")), RestartPolicy::Never);
        assert_eq!(RestartPolicy::from_spec_str(Some("OnFailure")), RestartPolicy::OnFailure);
    }

    #[test]
    fn should_restart_matrix() {
        // Always → always.
        assert!(RestartPolicy::Always.should_restart(Some(0)));
        assert!(RestartPolicy::Always.should_restart(Some(1)));
        assert!(RestartPolicy::Always.should_restart(None));
        // Never → never.
        assert!(!RestartPolicy::Never.should_restart(Some(0)));
        assert!(!RestartPolicy::Never.should_restart(Some(1)));
        // OnFailure → only non-zero (or unknown).
        assert!(!RestartPolicy::OnFailure.should_restart(Some(0)));
        assert!(RestartPolicy::OnFailure.should_restart(Some(1)));
        assert!(RestartPolicy::OnFailure.should_restart(None));
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
        let (phase, st) =
            reconcile_pod_phase(RestartPolicy::Never, &[terminated("a", "id-a", 0)]);
        assert_eq!(phase, PodPhase::Succeeded);
        assert_eq!(st[0].state.exit_code(), Some(0));
    }

    #[test]
    fn single_terminated_nonzero_never_is_failed() {
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::Never, &[terminated("a", "id-a", 137)]);
        assert_eq!(phase, PodPhase::Failed);
    }

    #[test]
    fn single_terminated_under_always_stays_running() {
        // restartPolicy:Always — an exited container is about to be
        // restarted, so the pod is still Running (recovers). Even exit 0.
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::Always, &[terminated("a", "id-a", 0)]);
        assert_eq!(phase, PodPhase::Running);
        let (phase, _) =
            reconcile_pod_phase(RestartPolicy::Always, &[terminated("a", "id-a", 1)]);
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
        let (phase, _) = reconcile_pod_phase(
            RestartPolicy::Always,
            &[running("a", "id-a"), waiting("b")],
        );
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
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for a single container observation (Running / Terminated /
    /// Waiting with a small exit-code range).
    fn obs_strategy() -> impl Strategy<Value = ContainerObservation> {
        prop_oneof![
            (any::<u32>()).prop_map(|rc| ContainerObservation::running("c", "id", rc % 10)),
            (-5i32..5, any::<u32>())
                .prop_map(|(code, rc)| ContainerObservation::terminated("c", "id", code, rc % 10)),
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
                    (-5i32..5, any::<u32>())
                        .prop_map(|(code, rc)| ContainerObservation::terminated("c", "id", code, rc % 10)),
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
            codes in prop::collection::vec(-5i32..5, 1..6),
        ) {
            let obs: Vec<ContainerObservation> = codes
                .iter()
                .enumerate()
                .map(|(i, c)| ContainerObservation::terminated(format!("c{i}"), format!("id{i}"), *c, 0))
                .collect();
            let (phase, _) = reconcile_pod_phase(RestartPolicy::Never, &obs);
            prop_assert!(
                matches!(phase, PodPhase::Succeeded | PodPhase::Failed),
                "Never + all-terminated must be terminal, got {phase:?}"
            );
            // Succeeded iff every code is 0.
            let all_zero = codes.iter().all(|c| *c == 0);
            if all_zero {
                prop_assert_eq!(phase, PodPhase::Succeeded);
            } else {
                prop_assert_eq!(phase, PodPhase::Failed);
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
