//! Container restart, unobserved exits and CrashLoopBackOff against upstream
//! (Kubernetes v1.34.0, pkg/kubelet), row by row: `Vector::ContainerRestart`.
//!
//! Every checked row is driven through engenho's own decisions — the three
//! per-site restart decisions ([`starts_fresh`], [`running_action`],
//! [`down_action`]), the phase fold ([`reconcile_pod_phase_with_init`]), the
//! crash gate ([`crash_gate`]), the status the kubelet publishes for a
//! container it polled down ([`ContainerObservation::after_down`]) and its
//! wire form ([`ContainerStatusOut::to_wire`]), and the published-phase rule
//! ([`phase_to_publish`]) — with the row's runtime states read into engenho's
//! vocabulary by engenho's own readers ([`Termination::from_run_state`],
//! [`Termination::from_wire`], [`Termination::known_after`]). What is out of
//! scope, and why, is in [`OUT_OF_SCOPE`]; the keys a checked row leaves
//! unanswered, and why, are in [`PARTIAL`]; where engenho differs on purpose
//! is in [`DEVIATIONS`].
//!
//! ## Reading upstream's vocabulary off engenho's
//!
//! Upstream's `ShouldContainerBeRestarted` is one function over the newest
//! runtime record. engenho's kubelet asks the same question at three sites —
//! a container it has never started, one it polls running, one it polls down
//! — and each site has its own decision returning only the actions possible
//! there. A row's newest record picks the site: none is [`starts_fresh`];
//! `running` is [`running_action`] (restart = `Restart`); anything else is read
//! by [`Termination::from_run_state`] and asked of [`down_action`] (restart =
//! `Restart`).
//!
//! `computePodActions` is read the same way per container: to start is a
//! `Restart` from either site or a fresh start; to kill is to stop a container
//! that is running, which in engenho only a probe trip does
//! ([`RunningAction::Restart`] and [`RunningAction::Kill`]). A probe failure is
//! a real [`ProbeTrip`], folded out of a failing liveness probe.
//!
//! `getPhase` reads API statuses; engenho's fold reads the kubelet's
//! observations. A status carries over as the observation the kubelet builds
//! for that container: running and terminated are themselves (an exit read
//! back by [`Termination::from_wire`]); a waiting status with no
//! lastTerminationState is a container that has not started; and a waiting
//! status WITH one is a container that ran and is down, which the kubelet
//! holds (`backing_off`) when [`down_action`] restarts it and reports as its
//! exit when it does not.
//!
//! `doBackOff` is [`crash_gate`] over the row's entry. An entry's `backoff`
//! is a step on engenho's [`CRASH`] curve; the adapter finds the step, and a
//! fixture entry off the curve fails loudly.
//!
//! ## Reading a status row
//!
//! `convertToAPIContainerStatuses` folds the runtime's records of a
//! container, its previous API status and the reason cache into the status
//! upstream publishes. engenho's kubelet publishes, per container, what its
//! record of the run plus this tick's poll say, AFTER the tick has acted
//! ([`ContainerObservation::after_down`]). A row is read as that tick:
//!
//! - The newest runtime record is the poll. engenho keeps one record per
//!   container, its current run; older records feed only upstream's
//!   lastTerminationState, which engenho does not render.
//! - No runtime record, with a previous status of Terminated, or of Running
//!   with no lastTerminationState, is a container this kubelet started and
//!   the runtime no longer lists: polled as an exit nobody observed
//!   ([`Termination::Unknown`], as `Polled::of` reads it), folded with the
//!   exit the kubelet had already observed, if any
//!   ([`Termination::known_after`]). Its count is the previous status's.
//!   Neither a record nor a previous status is a container not yet started.
//!   Every other combination decides an upstream rendering from state engenho
//!   does not keep, and is out of scope by row.
//! - A reason cached for the container is upstream's record that its last
//!   restart was held: engenho's crash gate holding it
//!   ([`DownOutcome::Held`]). No cached reason is a gate that let it go, and
//!   the replacement started this tick ([`DownOutcome::Replaced`]).
//! - An init container is polled on the init path, which publishes its exit
//!   as it is and leaves restarting to the init sequence.
//!
//! The harness compares a row's top-level keys, and a status row's are
//! container names. So both sides are re-keyed ([`rekeyed`]) one level down,
//! `<container>/<field>`, with a state's message split out as its own field:
//! a field engenho does not render is then an unanswered key, pinned in
//! [`PARTIAL`], instead of making the whole container disagree. The
//! re-keying is checked lossless ([`rekeying_a_status_row_loses_nothing`]).

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use engenho_kubelet::backoff::{CRASH, CRASH_LOOP_BACK_OFF, CrashBackoff, CrashGate, crash_gate};
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::lifecycle::{
    ContainerObservation, ContainerStatusOut, DownAction, DownOutcome, RestartPolicy,
    RunningAction, Termination, down_action, phase_to_publish, reconcile_pod_phase,
    reconcile_pod_phase_with_init, running_action, starts_fresh,
};
use engenho_kubelet::{
    PodLifecycle, ProbeKind, ProbeObservation, ProbeRuntime, ProbeSpec, ProbeTrip,
    fold_probe_observation,
};
use engenho_oracle::{Answer, Case, Deviation, OutOfScope, Table, Vector, run};
use engenho_types::curated_enums::PodPhase;
use serde_json::{Map, Value, json};

const CONTAINER_RESTART_RULES: &str = "container-level restartPolicy and restartPolicyRules (KEP-5307): the alpha \
     ContainerRestartRules gate is off at v1.34.0, and engenho's app containers read only \
     the pod's restartPolicy. Without the gate upstream ignores the container field too, \
     which the checked row `scbr/gate off ignores container-level restartPolicy` pins";

const NO_SANDBOX: &str = "a pod sandbox that is dead or has no IP. engenho has no pod sandbox: \
     a container is the unit its runtimes start and stop, and a pod's address is its \
     containers'. There is no sandbox to kill, recreate or count attempts of";

const GATED_BACKOFF: &str = "a CrashLoopBackOff curve under an alpha gate \
     (ReduceDefaultCrashLoopBackOffDecay, KubeletCrashLoopBackOffMax), both off at v1.34.0. \
     engenho implements the default curve only (backoff::CRASH, 10 s to 300 s), which the \
     default-gates row checks";

const CLIENT_GO_EXPIRY: &str = "client-go's generic flowcontrol.Backoff with its default \
     2 x max expiry. The kubelet replaces that expiry with its own 600 s rule, the only one \
     engenho implements (CrashBackoff::forgiven_by), which the kubelet_600s rows check";

const POD_IS_TERMINAL: &str = "podIsTerminal is the pod worker's verdict that it has finished \
     terminating the pod (deletion, eviction). engenho's phase fold has no such input: a pod \
     leaves engenho's kubelet when its object is gone, and a pod under Always is never \
     reported terminal by it (a terminal phase something else published is kept: \
     phase_to_publish, checked by `phase/apiserver phase Succeeded is sticky …`)";

const UNKNOWN_BY_PREVIOUS: &str = "which of upstream's two renderings of a CRI-UNKNOWN container \
     applies is decided by its previous API status: Running gives terminated 137, anything else \
     (here Waiting ContainerCreating, or none) a Waiting with an empty reason \
     (kubelet_pods.go:2138-2162). engenho polls only containers it has started, so one the \
     runtime reports UNKNOWN always has a run behind it; a previous status saying it never \
     started, or none, is not a situation engenho's kubelet can be in";

/// Rows engenho has no counterpart for.
const OUT_OF_SCOPE: &[OutOfScope] = &[
    OutOfScope::kind("container_should_restart", CONTAINER_RESTART_RULES),
    OutOfScope::kind(
        "find_matching_container_restart_rule",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::kind(
        "is_container_restartable",
        "IsContainerRestartable reads the container-level restartPolicy and \
         restartPolicyRules (KEP-5307, alpha ContainerRestartRules, off at v1.34.0) before \
         the pod's. engenho has no predicate for \"restartable at all\": its restart decision \
         is always asked with the exit in hand (down_action)",
    ),
    OutOfScope::case(
        "scbr/gate on honours container-level Never over pod Always",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::case(
        "scbr/gate on: unknown state restarts even with container-level Never",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::case(
        "scbr/gate on: deleted pod never restarts even with container-level Always",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::case(
        "phase/gate on: container-level Never failed under pod Always -> Failed",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::case(
        "phase/gate on: one restartable container failed under pod Never -> Running",
        CONTAINER_RESTART_RULES,
    ),
    OutOfScope::case(
        "phase/all succeeded under Always but podIsTerminal -> Succeeded",
        POD_IS_TERMINAL,
    ),
    OutOfScope::case(
        "phase/succeeded+failed under Always but podIsTerminal -> Failed",
        POD_IS_TERMINAL,
    ),
    OutOfScope::case(
        "actions/dead sandbox, Always: kill pod, new sandbox attempt 1, start all",
        NO_SANDBOX,
    ),
    OutOfScope::case(
        "actions/dead sandbox, OnFailure: skip the succeeded container",
        NO_SANDBOX,
    ),
    OutOfScope::case("actions/sandbox without IP is treated as dead", NO_SANDBOX),
    OutOfScope::case(
        "actions/dead sandbox, Never, all exited: kill but do NOT create sandbox",
        NO_SANDBOX,
    ),
    OutOfScope::case(
        "actions/dead sandbox, OnFailure, all succeeded: kill but do NOT create sandbox",
        NO_SANDBOX,
    ),
    OutOfScope::case(
        "actions/dead sandbox, Never, no containers ever created: DO create sandbox",
        NO_SANDBOX,
    ),
    OutOfScope::case(
        "actions/spec hash change under Never: kill AND restart regardless of policy",
        "upstream restarts a container whose spec hash changed. engenho keeps no container \
         hash and does not restart a running container on an in-place pod update (an image \
         change included): a gap, recorded against the kubelet, not a design",
    ),
    OutOfScope::case(
        "backoff-config/ReduceDefaultCrashLoopBackOffDecay: 1s / 60s",
        GATED_BACKOFF,
    ),
    OutOfScope::case(
        "backoff-config/node max 2s below initial 10s clamps initial",
        GATED_BACKOFF,
    ),
    OutOfScope::case("backoff-config/both gates, node max 2s", GATED_BACKOFF),
    OutOfScope::case("backoff-config/both gates, node max 10s", GATED_BACKOFF),
    OutOfScope::case("backoff-config/both gates, node max 300s", GATED_BACKOFF),
    OutOfScope::case(
        "backoff-config/node max 11s keeps initial 10s",
        GATED_BACKOFF,
    ),
    OutOfScope::case(
        "backoff-config/node max 300s keeps initial 10s",
        GATED_BACKOFF,
    ),
    OutOfScope::case(
        "backoff/kubelet reduced-decay schedule 1s..60s",
        GATED_BACKOFF,
    ),
    OutOfScope::case(
        "backoff/flowcontrol doubling capped at max (1s, 50s)",
        CLIENT_GO_EXPIRY,
    ),
    OutOfScope::case(
        "backoff/default expiry is strictly > 2*max (5s max, 11s gap)",
        CLIENT_GO_EXPIRY,
    ),
    OutOfScope::case(
        "backoff/high-water mark kept when gap < 2*max",
        CLIENT_GO_EXPIRY,
    ),
    OutOfScope::case(
        "do-backoff/uses newest EXITED record even if a newer non-exited record exists",
        "upstream picks the newest EXITED record out of the runtime's history of a \
         container's runs. engenho's kubelet keeps one record per container (its current \
         run) and stamps the exit on the first tick that sees it down, so there is no older \
         exited record to choose",
    ),
    OutOfScope::kind(
        "backoff_key",
        "upstream keys the backoff map by an FNV-64a hash of \
         name/namespace/uid/container/image/resources. engenho keeps the penalty on the \
         container's record under the pod's ResourceKey (namespace/name) and container name, \
         so a new UID, image or resources does NOT start it afresh: a gap, recorded against \
         the kubelet, not a design",
    ),
    OutOfScope::case(
        "status/vanished while Running but prior LastTerminationState exists: nothing synthesized",
        "upstream synthesizes the unobserved exit of a container the runtime no longer lists, \
         and bumps its restartCount, only when the previous status carries no \
         lastTerminationState (kubelet_pods.go:2358-2361). engenho keeps no \
         lastTerminationState, so the input that decides this row has no counterpart",
    ),
    OutOfScope::case(
        "status/vanished, previous status Waiting: plain ContainerCreating, no LTS",
        "the previous status is a Waiting ImagePullBackOff with restartCount 2, carried into a \
         container the runtime no longer lists. engenho has no image-pull waiting state, and \
         the container it has restarted twice is a run it polls, not a waiting status: no \
         engenho kubelet is in this situation",
    ),
    OutOfScope::case(
        "status/CRI UNKNOWN and previous was not Running: empty-reason Waiting",
        UNKNOWN_BY_PREVIOUS,
    ),
    OutOfScope::case(
        "status/CRI UNKNOWN with no previous status at all: empty-reason Waiting",
        UNKNOWN_BY_PREVIOUS,
    ),
    OutOfScope::case(
        "phase/vanished containers, deleted, podIsTerminal -> Failed",
        POD_IS_TERMINAL,
    ),
];

/// A key a checked row's `expected` has and engenho's answer does not.
struct Partial {
    /// The key.
    key: &'static str,
    /// The checked rows it is left unanswered on.
    rows: &'static [&'static str],
    /// Why engenho has no answer for it there.
    why: &'static str,
}

const READY_SANDBOX_ACTIONS: &[&str] = &[
    "actions/Always restarts exit 0 and exit 111",
    "actions/OnFailure restarts only exit 111",
    "actions/OnFailure starts created-but-never-started",
    "actions/Never restarts nothing",
    "actions/Never still kills-then-restarts an UNKNOWN container",
    "actions/Never starts a CREATED container without killing it",
    "actions/liveness failure under Never: kill, do not restart",
];

const PHASE_ROWS: &[&str] = &[
    "phase/vanished containers rendered unknown, not deleted, Always -> Running",
    "phase/vanished containers rendered unknown, deleted, Always -> Running, restartCount 0",
    "phase/apiserver phase Succeeded is sticky even though containers render unknown with restartCount 1",
];

/// Checked rows that are checked in part. The harness accepts an answer
/// missing a key and reports only the key, per kind; the test pins this
/// per-row list both ways, so an answer that drops a key not listed here
/// fails, and so does a listing whose key is now answered. On a status row
/// the key is the field under the container (`<container>/<field>`, see
/// [`rekeyed`]), the same for every container of the row.
const PARTIAL: &[Partial] = &[
    Partial {
        key: "kill_pod",
        rows: READY_SANDBOX_ACTIONS,
        why: "whether to kill the pod sandbox. engenho has no sandbox (see NO_SANDBOX); the \
              per-container starts and kills are checked",
    },
    Partial {
        key: "create_sandbox",
        rows: READY_SANDBOX_ACTIONS,
        why: "as kill_pod: engenho has no sandbox to create",
    },
    Partial {
        key: "waiting_message",
        rows: &[
            "do-backoff/second crash 5s after finish with 10s entry: CrashLoopBackOff",
            "do-backoff/message uses Go duration format (80s -> 1m20s)",
            "do-backoff/message at cap (300s -> 5m0s)",
        ],
        why: "the waiting message `back-off 1m20s restarting failed container=…`. engenho \
              publishes the waiting reason (CrashLoopBackOff, checked) and no message: \
              ContainerState::Waiting carries none",
    },
    Partial {
        key: "last_termination_state",
        rows: &[
            "status/vanished while Running, pod deleted: Waiting+LTS unknown, restartCount unchanged",
            "status/vanished while Running, pod NOT deleted: restartCount +1",
            "status/vanished while Running with init containers: default waiting reason is PodInitializing",
            "status/CRI UNKNOWN after Running with cached CrashLoopBackOff: moved to Waiting, Terminated becomes LTS",
            "status/exited, Always, cached CrashLoopBackOff: Waiting CrashLoopBackOff, exit moves to LTS",
            "status/two runtime records: newest is State, second-newest is LTS",
        ],
        why: "a container's lastState. engenho renders none: ContainerStatusOut has no field \
              for it, and the kubelet's record keeps only the current run. A crash-looping \
              container's exit is in its Started and BackOff events and in no status field \
              (a gap, not a design)",
    },
    Partial {
        key: "state_message",
        rows: &[
            "status/CRI reports UNKNOWN and previous was Running: Terminated 137 in State, restartCount +1",
            "status/CRI UNKNOWN after Running with cached CrashLoopBackOff: moved to Waiting, Terminated becomes LTS",
            "status/CRI UNKNOWN after Running, pod deleted, cached reason ignored",
            "status/exited, Always, cached CrashLoopBackOff: Waiting CrashLoopBackOff, exit moves to LTS",
        ],
        why: "the message of a waiting or terminated state. ContainerState carries a reason and \
              no message, so engenho publishes none: the reason is checked",
    },
    Partial {
        key: "state",
        rows: PHASE_ROWS,
        why: "the containers' state on these rows is the vanished-while-Running rendering the \
              `status/vanished while Running…` rows check and declare. These rows add the \
              phase, answered with the restartCount; answering the state again would put the \
              phase behind those rows' deviations",
    },
    Partial {
        key: "last_termination_state",
        rows: PHASE_ROWS,
        why: "as `state` on these rows, and engenho renders no lastState (see the status rows)",
    },
    Partial {
        key: "event",
        rows: &["do-backoff/second crash 5s after finish with 10s entry: CrashLoopBackOff"],
        why: "the BackOff event's text. engenho emits a BackOff event on every hold, but words \
              it with the remaining wait and prior restarts, not upstream's `Back-off \
              restarting failed container X in pod name_ns(uid)`; the event is emitted by the \
              kubelet loop, which these pure rows do not drive",
    },
];

const RENDERED_AFTER_ACTING: &str = "upstream renders a pod's status before the sync acts on it \
     (generateAPIPodStatus runs ahead of SyncPod), so a container it is about to restart is \
     published once as it was found: Terminated, or Waiting ContainerCreating (PodInitializing) \
     when the runtime no longer lists it. engenho's kubelet renders after the tick has acted: \
     with no hold owed (no cached reason) the replacement is already up, published Running \
     with a restartCount one above the run it replaced (ContainerObservation::after_down, \
     DownOutcome::Replaced). The exit it replaced is in the Started event and in no status \
     field, since engenho renders no lastState (PARTIAL last_termination_state)";

const COUNTED_WHEN_STARTED: &str = "upstream bumps restartCount the moment the CRI reports \
     UNKNOWN for a container whose previous status was Running (kubelet_pods.go:2153), before \
     anything restarts, and for a pod being deleted, where nothing will. engenho counts a \
     restart when its replacement starts (DownOutcome::Replaced), so while the restart is held, \
     or when a pod being deleted restarts nothing, the count stays where the run left it. The \
     state agrees";

/// Checked rows where engenho differs on purpose.
const DEVIATIONS: &[Deviation] = &[
    Deviation {
        case: "scbr/unknown/Never",
        why: "upstream restarts a container whose CRI state is UNKNOWN under every policy, \
              killing it first, because UNKNOWN means it may still be running. engenho reads \
              RunState::Unknown as Termination::Unknown — DOWN, and how it ended not observed \
              (a native process whose reap failed, a podman status it cannot read) — and under \
              Never does not run the pod again: re-running it is the in-place re-run of a \
              Never pod that plan T1.2 removed. The pod is Failed with 137 / \
              ContainerStatusUnknown instead",
    },
    Deviation {
        case: "actions/Never still kills-then-restarts an UNKNOWN container",
        why: "as scbr/unknown/Never: under Never engenho latches an unobserved exit and does \
              not start the container again",
    },
    Deviation {
        case: "scbr/created-never-started/Never restarts",
        why: "engenho folds a CREATED container into Termination::Unknown \
              (Termination::from_run_state), so under Never it is latched as an unobserved \
              exit rather than started. Upstream starts it: it never ran. No shipped backend \
              reports CREATED for a container the kubelet has started (podman's start returns \
              once it runs; native and CRI never do), so this is unreached today; giving \
              CREATED its own poll arm is the fix",
    },
    Deviation {
        case: "actions/Never starts a CREATED container without killing it",
        why: "as scbr/created-never-started/Never restarts",
    },
    Deviation {
        case: "status/exited, Always, NO cached reason: stays Terminated (not Waiting)",
        why: RENDERED_AFTER_ACTING,
    },
    Deviation {
        case: "status/CRI reports UNKNOWN and previous was Running: Terminated 137 in State, restartCount +1",
        why: RENDERED_AFTER_ACTING,
    },
    Deviation {
        case: "status/vanished while Running, pod NOT deleted: restartCount +1",
        why: RENDERED_AFTER_ACTING,
    },
    Deviation {
        case: "status/vanished while Running with init containers: default waiting reason is PodInitializing",
        why: RENDERED_AFTER_ACTING,
    },
    Deviation {
        case: "status/vanished while Running, pod deleted: Waiting+LTS unknown, restartCount unchanged",
        why: "upstream renders a container the runtime no longer lists, in a pod being deleted, as \
              Waiting ContainerCreating with 137 / ContainerStatusUnknown in lastState \
              (kubelet_pods.go:2314-2378). engenho reads a container the runtime no longer lists \
              as an exit nobody observed (Termination::Unknown, as it reads a CRI UNKNOWN), and a \
              pod being deleted restarts nothing, so it publishes that exit where upstream \
              publishes a CRI-UNKNOWN one: state.terminated 137 / ContainerStatusUnknown. It has \
              no lastState to put it in, and a container of a pod being deleted is not being \
              created. restartCount agrees",
    },
    Deviation {
        case: "status/CRI UNKNOWN after Running with cached CrashLoopBackOff: moved to Waiting, Terminated becomes LTS",
        why: COUNTED_WHEN_STARTED,
    },
    Deviation {
        case: "status/CRI UNKNOWN after Running, pod deleted, cached reason ignored",
        why: COUNTED_WHEN_STARTED,
    },
    Deviation {
        case: "status/CRI created (not started): empty-reason Waiting",
        why: "as scbr/created-never-started/Never restarts: engenho reads CREATED as an exit \
              nobody observed, which under Always it restarts at once and publishes after \
              acting (see status/exited, Always, NO cached reason): Running, restartCount 1. \
              Upstream renders a CREATED container Waiting with an empty reason and has \
              restarted nothing",
    },
    Deviation {
        case: "do-backoff/no exited record: no backoff check and no Next",
        why: "as scbr/created-never-started/Never restarts: a CREATED container is read as an \
              unobserved exit, so its restart goes through the crash gate and is charged a \
              step, where upstream finds no exited record and charges nothing",
    },
];

const CHECKED_ROWS: usize = 89;

#[test]
fn container_restart_agrees_with_upstream() {
    let table = rekeyed(Vector::ContainerRestart.load());
    let mut answers: HashMap<String, Answer> = table
        .cases
        .iter()
        .map(|case| (case.name.clone(), answer(&table, case)))
        .collect();
    let unanswered = unanswered_keys(&table, &answers);
    let report = run(&table, OUT_OF_SCOPE, DEVIATIONS, |case| {
        answers.remove(&case.name).unwrap_or(Answer::NotChecked)
    })
    .unwrap_or_else(|failures| panic!("{failures}"));
    assert_eq!(
        report.checked, CHECKED_ROWS,
        "rows checked against upstream: {report:?}"
    );
    assert_eq!(report.deviations, DEVIATIONS.len());
    let declared: BTreeSet<(String, String)> = PARTIAL
        .iter()
        .flat_map(|p| {
            p.rows
                .iter()
                .map(|row| ((*row).to_owned(), p.key.to_owned()))
        })
        .collect();
    let undeclared: Vec<_> = unanswered.difference(&declared).collect();
    let now_answered: Vec<_> = declared.difference(&unanswered).collect();
    assert!(
        undeclared.is_empty() && now_answered.is_empty(),
        "checked rows' keys left unanswered but not in PARTIAL: {undeclared:?}\n\
         PARTIAL entries that are answered (or name no checked row): {now_answered:?}"
    );
    assert!(
        PARTIAL.iter().all(|p| !p.why.trim().is_empty()),
        "every PARTIAL entry says why"
    );
}

/// Every `(row, key)` where the row is checked and its `expected` has the key
/// but the adapter's answer does not. A re-keyed `<container>/<field>` is
/// reported as its field.
fn unanswered_keys(table: &Table, answers: &HashMap<String, Answer>) -> BTreeSet<(String, String)> {
    let mut unanswered = BTreeSet::new();
    for case in &table.cases {
        if let Some(Answer::Checked(Value::Object(got))) = answers.get(&case.name)
            && let Value::Object(expected) = &case.expected
        {
            for key in expected.keys().filter(|k| !got.contains_key(*k)) {
                let field = key.split_once(SEP).map_or(key.as_str(), |(_, field)| field);
                unanswered.insert((case.name.clone(), field.to_owned()));
            }
        }
    }
    unanswered
}

fn answer(table: &Table, case: &Case) -> Answer {
    match table.kind_of(case).as_str() {
        "should_container_be_restarted" => should_container_be_restarted(case),
        "get_phase" => get_phase(case),
        "compute_pod_actions" => compute_pod_actions(case),
        "do_backoff" => do_backoff(case),
        "backoff_expiry" => backoff_expiry(case),
        "backoff_sequence" => backoff_sequence(case),
        "crashloop_backoff_config" => crashloop_backoff_config(case),
        CONVERT_STATUSES => convert_to_api_container_statuses(case),
        GENERATE_STATUS => generate_api_pod_status(case),
        _ => Answer::NotChecked,
    }
}

// =====================================================================
// Shared vocabulary
// =====================================================================

fn text<'v>(v: &'v Value, key: &str) -> Option<&'v str> {
    v.get(key).and_then(Value::as_str)
}

fn int(v: &Value, key: &str) -> Option<i64> {
    v.get(key).and_then(Value::as_i64)
}

fn seconds(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64)
}

/// Is `gate` switched on for this row (at the row's top level or in its
/// input)?
fn gate_on(case: &Case, gate: &str) -> bool {
    [
        case.extra.get("feature_gates"),
        case.input.get("feature_gates"),
    ]
    .into_iter()
    .flatten()
    .any(|gates| gates.get(gate).and_then(Value::as_bool) == Some(true))
}

/// Any alpha gate switched on.
fn any_gate_on(case: &Case) -> bool {
    [
        "ContainerRestartRules",
        "ReduceDefaultCrashLoopBackOffDecay",
        "KubeletCrashLoopBackOffMax",
    ]
    .iter()
    .any(|gate| gate_on(case, gate))
}

/// The pod's policy, read the way the kubelet reads `spec.restartPolicy`.
fn policy(input: &Value) -> RestartPolicy {
    RestartPolicy::from_spec_str(text(input, "pod_restart_policy"))
}

fn lifecycle(input: &Value) -> PodLifecycle {
    if input.get("pod_deleted").and_then(Value::as_bool) == Some(true) {
        PodLifecycle::Terminating
    } else {
        PodLifecycle::Live
    }
}

/// A runtime record's state as engenho's runtimes report it.
fn run_state(record: &Value) -> RunState {
    match text(record, "state") {
        Some("running") => RunState::Running,
        Some("exited") => {
            let code = int(record, "exit_code").expect("an exited record carries its code");
            RunState::Exited(ExitDisposition::Code(
                i32::try_from(code).expect("exit code fits i32"),
            ))
        }
        Some("created") => RunState::Created,
        Some("unknown") => RunState::Unknown,
        other => panic!("unknown runtime state {other:?} in {record}"),
    }
}

/// Seconds as a `Duration`, to the nanosecond (`600.000000001` is one
/// nanosecond past ten minutes, not a float's approximation of it).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "fixture seconds are small and non-negative, checked first"
)]
fn secs(s: f64) -> Duration {
    assert!(s >= 0.0, "a negative duration in a fixture: {s}");
    Duration::from_nanos((s * 1e9).round() as u64)
}

/// A real liveness trip: a failing liveness probe folded past a threshold of
/// one. [`ProbeTrip`] has no other constructor.
fn liveness_trip() -> ProbeTrip {
    let spec = ProbeSpec::from_k8s(
        ProbeKind::Liveness,
        &json!({ "exec": { "command": ["probe"] }, "failureThreshold": 1 }),
        &[],
    )
    .expect("a liveness exec probe parses");
    let now = Instant::now();
    let mut rt = ProbeRuntime::new(now);
    fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
        .trip
        .expect("one failure past a threshold of one trips")
}

// =====================================================================
// ShouldContainerBeRestarted
// =====================================================================

/// The per-site decisions over the row's newest record.
fn should_container_be_restarted(case: &Case) -> Answer {
    if gate_on(case, "ContainerRestartRules") {
        return Answer::NotChecked;
    }
    let input = &case.input;
    let (policy, pod) = (policy(input), lifecycle(input));
    let newest = input
        .get("runtime_statuses")
        .and_then(Value::as_array)
        .and_then(|records| records.first());
    let restart = match newest {
        None => starts_fresh(pod),
        Some(record) => match Termination::from_run_state(run_state(record)) {
            None => matches!(running_action(policy, pod, None), RunningAction::Restart(_)),
            Some(exit) => down_action(policy, pod, exit) == DownAction::Restart,
        },
    };
    Answer::Checked(json!({ "restart": restart }))
}

// =====================================================================
// getPhase
// =====================================================================

fn get_phase(case: &Case) -> Answer {
    let input = &case.input;
    if gate_on(case, "ContainerRestartRules")
        || input.get("pod_is_terminal").and_then(Value::as_bool) == Some(true)
    {
        return Answer::NotChecked;
    }
    let policy = policy(input);
    let statuses = input.get("statuses").cloned().unwrap_or_default();
    let observe = |key: &str| -> Vec<ContainerObservation> {
        input
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|name| {
                let name = name.as_str().expect("a container is named");
                match statuses.get(name) {
                    None => ContainerObservation::waiting(name),
                    Some(status) => observation(policy, name, status),
                }
            })
            .collect()
    };
    let (phase, ..) =
        reconcile_pod_phase_with_init(policy, &observe("init_containers"), &observe("containers"));
    Answer::Checked(json!({ "phase": phase }))
}

/// The observation the kubelet builds for a container whose API status is
/// `status` (see the module docs).
fn observation(policy: RestartPolicy, name: &str, status: &Value) -> ContainerObservation {
    let id = "c-1";
    let state = &status["state"];
    let terminated = |t: &Value| {
        Termination::from_wire(
            t.get("exit_code").and_then(Value::as_i64),
            text(t, "reason"),
        )
    };
    if state.get("running").is_some() {
        return ContainerObservation::running(name, id, 0);
    }
    if let Some(t) = state.get("terminated") {
        return ContainerObservation::terminated(name, id, terminated(t), 0);
    }
    match status.pointer("/last_termination_state/terminated") {
        // Waiting, never ran.
        None => ContainerObservation::waiting(name),
        // Waiting after a run: held when it will be restarted, else its exit.
        Some(t) => {
            let exit = terminated(t);
            match down_action(policy, PodLifecycle::Live, exit) {
                DownAction::Restart => ContainerObservation::backing_off(
                    name,
                    id,
                    engenho_kubelet::backoff::CRASH_LOOP_BACK_OFF,
                    1,
                ),
                DownAction::Latch => ContainerObservation::terminated(name, id, exit, 0),
            }
        }
    }
}

// =====================================================================
// computePodActions
// =====================================================================

fn compute_pod_actions(case: &Case) -> Answer {
    let input = &case.input;
    let sandbox = &input["sandbox"];
    let containers = input["containers"].as_array().expect("containers listed");
    if text(sandbox, "state") != Some("ready")
        || sandbox.get("has_ip").and_then(Value::as_bool) != Some(true)
        || containers
            .iter()
            .any(|c| c.get("hash_changed").and_then(Value::as_bool) == Some(true))
    {
        return Answer::NotChecked;
    }
    let (policy, pod) = (policy(input), lifecycle(input));
    let (mut start, mut kill) = (Vec::new(), Vec::new());
    for (idx, container) in containers.iter().enumerate() {
        let runtime = container.get("runtime").filter(|r| !r.is_null());
        let Some(record) = runtime else {
            if starts_fresh(pod) {
                start.push(idx);
            }
            continue;
        };
        match Termination::from_run_state(run_state(record)) {
            None => {
                let trip = (text(container, "liveness") == Some("failure")).then(liveness_trip);
                match running_action(policy, pod, trip) {
                    RunningAction::Keep => {}
                    RunningAction::Restart(_) => {
                        kill.push(idx);
                        start.push(idx);
                    }
                    RunningAction::Kill(_) => kill.push(idx),
                }
            }
            Some(exit) => {
                if down_action(policy, pod, exit) == DownAction::Restart {
                    start.push(idx);
                }
            }
        }
    }
    Answer::Checked(json!({ "containers_to_start": start, "containers_to_kill": kill }))
}

// =====================================================================
// convertToAPIContainerStatuses and generateAPIPodStatus
// =====================================================================

const CONVERT_STATUSES: &str = "convert_to_api_container_statuses";
const GENERATE_STATUS: &str = "generate_api_pod_status";

/// The id of the run a status row describes, and of the replacement the
/// kubelet starts for it. Neither is in upstream's `expected`.
const RUN_ID: &str = "c-1";
const REPLACEMENT_ID: &str = "c-2";

/// A count as the status carries it.
fn count(v: &Value) -> u32 {
    int(v, "restart_count").map_or(0, |n| u32::try_from(n).expect("a count fits u32"))
}

/// Did the last sync hold `name`'s restart (see the module docs)?
fn restart_held(input: &Value, name: &str) -> bool {
    match input
        .get("reason_cache")
        .and_then(|cache| cache.get(name))
        .map(|cached| text(cached, "err"))
    {
        None => false,
        Some(Some("CrashLoopBackOff")) => true,
        Some(other) => panic!("a cached reason this adapter does not read: {other:?}"),
    }
}

/// What engenho's kubelet publishes for container `name` of a status row, or
/// `None` where no engenho kubelet is in the row's situation (see the module
/// docs and [`OUT_OF_SCOPE`]).
fn status_observation(input: &Value, name: &str) -> Option<ContainerObservation> {
    let (policy, pod) = (policy(input), lifecycle(input));
    let previous = input.get("previous_statuses").and_then(|p| p.get(name));
    let was = |arm: &str| previous.is_some_and(|p| p.pointer(&format!("/state/{arm}")).is_some());
    let newest = input
        .get("runtime_statuses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|record| text(record, "name") == Some(name));
    let (polled, restart_count) = match newest {
        Some(record) => match Termination::from_run_state(run_state(record)) {
            None => return Some(ContainerObservation::running(name, RUN_ID, count(record))),
            Some(_) if text(record, "state") == Some("unknown") && !was("running") => return None,
            Some(exit) => (exit, count(record)),
        },
        // The runtime no longer lists a container this kubelet started.
        None if was("terminated")
            || (was("running")
                && previous
                    .and_then(|p| p.get("last_termination_state"))
                    .is_none()) =>
        {
            (Termination::Unknown, previous.map_or(0, count))
        }
        None if previous.is_none() => return Some(ContainerObservation::waiting(name)),
        None => return None,
    };
    let observed = previous
        .and_then(|p| p.pointer("/state/terminated"))
        .map(|t| Termination::from_wire(int(t, "exit_code"), text(t, "reason")));
    let exit = Termination::known_after(observed, polled);
    if input.get("is_init_container").and_then(Value::as_bool) == Some(true) {
        return Some(ContainerObservation::terminated(
            name,
            RUN_ID,
            exit,
            restart_count,
        ));
    }
    let outcome = match down_action(policy, pod, exit) {
        DownAction::Latch => DownOutcome::Latched,
        DownAction::Restart if restart_held(input, name) => DownOutcome::Held {
            reason: CRASH_LOOP_BACK_OFF,
        },
        DownAction::Restart => DownOutcome::Replaced {
            container_id: REPLACEMENT_ID,
        },
    };
    Some(ContainerObservation::after_down(
        name,
        RUN_ID,
        exit,
        restart_count,
        outcome,
    ))
}

/// The row's containers, observed; `None` if any is out of reach.
fn status_observations(input: &Value) -> Option<Vec<ContainerObservation>> {
    input["containers"]
        .as_array()
        .expect("containers listed")
        .iter()
        .map(|name| status_observation(input, name.as_str().expect("a container is named")))
        .collect()
}

/// A published status, in the row's vocabulary and re-keyed like its
/// `expected`, keeping the fields `keep` accepts.
fn published(statuses: &[ContainerStatusOut], keep: impl Fn(&str) -> bool) -> Map<String, Value> {
    statuses
        .iter()
        .flat_map(|status| {
            let entry = in_row_terms(&status.to_wire());
            per_container_keys(&status.name, &entry)
        })
        .filter(|(key, _)| key.split_once(SEP).is_some_and(|(_, field)| keep(field)))
        .collect()
}

/// The wire's `containerStatuses[]` entry in the table's terms: its keys
/// snake_cased, and only the fields the table records.
fn in_row_terms(wire: &Value) -> Map<String, Value> {
    fn renamed(v: &Value) -> Value {
        match v {
            Value::Object(map) => map
                .iter()
                .map(|(k, v)| {
                    let k = match k.as_str() {
                        "exitCode" => "exit_code",
                        "restartCount" => "restart_count",
                        "lastState" => "last_termination_state",
                        other => other,
                    };
                    (k.to_owned(), renamed(v))
                })
                .collect::<Map<_, _>>()
                .into(),
            other => other.clone(),
        }
    }
    let Value::Object(entry) = renamed(wire) else {
        panic!("a container status is an object: {wire}")
    };
    entry
        .into_iter()
        .filter(|(k, _)| ["state", "last_termination_state", "restart_count"].contains(&k.as_str()))
        .collect()
}

fn convert_to_api_container_statuses(case: &Case) -> Answer {
    let input = &case.input;
    let Some(observations) = status_observations(input) else {
        return Answer::NotChecked;
    };
    let is_init = input.get("is_init_container").and_then(Value::as_bool) == Some(true);
    let (init, app) = if is_init {
        (observations, Vec::new())
    } else {
        (Vec::new(), observations)
    };
    let (_, init_statuses, app_statuses, _) =
        reconcile_pod_phase_with_init(policy(input), &init, &app);
    let statuses = if is_init { init_statuses } else { app_statuses };
    Answer::Checked(Value::Object(published(&statuses, |_| true)))
}

fn generate_api_pod_status(case: &Case) -> Answer {
    let input = &case.input;
    if input.get("pod_is_terminal").and_then(Value::as_bool) == Some(true) {
        return Answer::NotChecked;
    }
    let Some(observations) = status_observations(input) else {
        return Answer::NotChecked;
    };
    let (phase, statuses) = reconcile_pod_phase(policy(input), &observations);
    let stored: Option<PodPhase> = input
        .get("api_phase")
        .map(|p| serde_json::from_value(p.clone()).expect("a phase the API knows"));
    let mut got = published(&statuses, |field| field == "restart_count");
    got.insert("phase".into(), json!(phase_to_publish(stored, phase)));
    Answer::Checked(Value::Object(got))
}

// =====================================================================
// Re-keying a status row
// =====================================================================

/// Between a container's name and a field of its status, in a re-keyed row.
const SEP: char = '/';

/// A state's message, split out of the state as a field of its own.
const STATE_MESSAGE: &str = "state_message";

/// One container's status, `<container>/<field>` → value, with the state's
/// message (if any) split out as [`STATE_MESSAGE`].
fn per_container_keys(container: &str, entry: &Map<String, Value>) -> Vec<(String, Value)> {
    let mut fields = Vec::new();
    for (field, value) in entry {
        let mut value = value.clone();
        if field == "state"
            && let Some(message) = value
                .as_object_mut()
                .and_then(|arms| arms.values_mut().next())
                .and_then(Value::as_object_mut)
                .and_then(|arm| arm.remove("message"))
        {
            fields.push((format!("{container}{SEP}{STATE_MESSAGE}"), message));
        }
        fields.push((format!("{container}{SEP}{field}"), value));
    }
    fields
}

/// The table with each status row's `expected` re-keyed one level down (see
/// the module docs). Every other row is untouched.
fn rekeyed(mut table: Table) -> Table {
    let kinds: Vec<String> = table.cases.iter().map(|c| table.kind_of(c)).collect();
    for (case, kind) in table.cases.iter_mut().zip(kinds) {
        let entries = match kind.as_str() {
            CONVERT_STATUSES => case.expected.as_object(),
            GENERATE_STATUS => case.expected["container_statuses"].as_object(),
            _ => continue,
        }
        .expect("per-container statuses")
        .clone();
        let mut flat: Map<String, Value> = entries
            .iter()
            .flat_map(|(container, entry)| {
                per_container_keys(container, entry.as_object().expect("a status object"))
            })
            .collect();
        if kind == GENERATE_STATUS {
            flat.insert("phase".into(), case.expected["phase"].clone());
        }
        case.expected = Value::Object(flat);
    }
    table
}

/// [`rekeyed`] undone, for [`rekeying_a_status_row_loses_nothing`].
fn unkeyed(kind: &str, flat: &Value) -> Value {
    let mut containers: Map<String, Value> = Map::new();
    let mut top: Map<String, Value> = Map::new();
    let mut messages = Vec::new();
    for (key, value) in flat.as_object().expect("a re-keyed row") {
        match key.split_once(SEP) {
            None => {
                top.insert(key.clone(), value.clone());
            }
            Some((container, STATE_MESSAGE)) => messages.push((container, value)),
            Some((container, field)) => {
                containers
                    .entry(container)
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .expect("a status object")
                    .insert(field.to_owned(), value.clone());
            }
        }
    }
    for (container, message) in messages {
        let arm = containers[container]["state"]
            .as_object_mut()
            .and_then(|arms| arms.values_mut().next())
            .and_then(Value::as_object_mut)
            .expect("a message belongs to a state");
        arm.insert("message".into(), message.clone());
    }
    if kind == GENERATE_STATUS {
        top.insert("container_statuses".into(), Value::Object(containers));
        Value::Object(top)
    } else {
        Value::Object(containers)
    }
}

#[test]
fn rekeying_a_status_row_loses_nothing() {
    let original = Vector::ContainerRestart.load();
    let flat = rekeyed(original.clone());
    let mut status_rows = 0;
    for (before, after) in original.cases.iter().zip(&flat.cases) {
        let kind = original.kind_of(before);
        if [CONVERT_STATUSES, GENERATE_STATUS].contains(&kind.as_str()) {
            status_rows += 1;
            assert!(
                after
                    .expected
                    .as_object()
                    .is_some_and(|keys| keys.keys().all(|k| k == "phase" || k.contains(SEP))),
                "{}: {}",
                before.name,
                after.expected
            );
            assert_eq!(
                unkeyed(&kind, &after.expected),
                before.expected,
                "{}",
                before.name
            );
        } else {
            assert_eq!(after.expected, before.expected, "{}", before.name);
        }
    }
    assert_eq!(status_rows, 22, "every status row is re-keyed");
}

// =====================================================================
// The crash gate: doBackOff, expiry, the schedule, the constants
// =====================================================================

/// Whether the row's curve is the default one engenho implements.
fn default_curve(input: &Value) -> bool {
    let at = |key| seconds(input, key).map(secs);
    at("initial_s").is_none_or(|d| d == CRASH.base())
        && at("max_s").is_none_or(|d| d == CRASH.cap())
}

/// The step on [`CRASH`] that owes `owed`.
fn step_owing(owed: Duration) -> u32 {
    (0..=32)
        .find(|&step| CRASH.delay(step) == owed)
        .unwrap_or_else(|| panic!("{owed:?} is not a step on engenho's crash curve"))
}

fn do_backoff(case: &Case) -> Answer {
    let input = &case.input;
    let several_records = input
        .get("runtime_statuses")
        .and_then(Value::as_array)
        .is_some_and(|r| r.len() > 1);
    if any_gate_on(case) || !default_curve(input) || several_records {
        return Answer::NotChecked;
    }
    let base = Instant::now();
    let at = |key: &str| seconds(input, key).map(|s| base + secs(s));
    let now = at("now_s").expect("a row names now");
    let entry = input
        .get("entry")
        .filter(|e| !e.is_null())
        .map(|e| CrashBackoff {
            step: step_owing(secs(seconds(e, "backoff_s").expect("an entry owes"))),
            last_update: base + secs(seconds(e, "last_update_s").expect("an entry was stamped")),
        });
    // engenho has no FinishedAt for a record that did not report one: its
    // kubelet stamps the exit on the first tick that sees the container down.
    let finished = at("latest_exited_finished_at_s").unwrap_or(now);
    let gate = crash_gate(entry, finished, now);
    let owes_after = match gate {
        CrashGate::Hold { .. } => entry.map(CrashBackoff::owed),
        CrashGate::Go(next) => Some(next.owed()),
    };
    let mut got = Map::new();
    got.insert(
        "in_backoff".into(),
        json!(matches!(gate, CrashGate::Hold { .. })),
    );
    if case.expected.get("entry_after_s").is_some() {
        got.insert(
            "entry_after_s".into(),
            json!(owes_after.map(|d| d.as_secs())),
        );
    }
    if case.expected.get("waiting_reason").is_some() {
        got.insert("waiting_reason".into(), json!(gate.waiting_reason()));
    }
    Answer::Checked(Value::Object(got))
}

fn backoff_expiry(case: &Case) -> Answer {
    let input = &case.input;
    if text(input, "has_expired") != Some("kubelet_600s") {
        return Answer::NotChecked;
    }
    let base = Instant::now();
    let entry = CrashBackoff {
        step: 0,
        last_update: base,
    };
    let gap = secs(seconds(input, "event_minus_last_update_s").expect("a gap"));
    Answer::Checked(json!({ "expired": entry.forgiven_by(base + gap) }))
}

/// `Next` eight times, never forgiven: each exit comes at once (a run of
/// zero) and each restart once the owed wait has long passed.
fn backoff_sequence(case: &Case) -> Answer {
    let input = &case.input;
    if any_gate_on(case)
        || text(input, "has_expired") != Some("kubelet_600s")
        || !default_curve(input)
    {
        return Answer::NotChecked;
    }
    let mut entry: Option<CrashBackoff> = None;
    let mut clock = Instant::now();
    let mut owed = Vec::new();
    for _ in 0..8 {
        let finished = clock;
        clock += Duration::from_secs(3_600);
        match crash_gate(entry, finished, clock) {
            CrashGate::Go(next) => {
                owed.push(next.owed().as_secs());
                entry = Some(next);
            }
            CrashGate::Hold { remaining } => panic!("an hour later still held {remaining:?}"),
        }
    }
    Answer::Checked(json!({ "get_after_each_next_s": owed }))
}

fn crashloop_backoff_config(case: &Case) -> Answer {
    let input = &case.input;
    if any_gate_on(case) || !input["node_max_container_restart_period_s"].is_null() {
        return Answer::NotChecked;
    }
    Answer::Checked(json!({
        "initial_s": CRASH.base().as_secs(),
        "max_s": CRASH.cap().as_secs(),
    }))
}
