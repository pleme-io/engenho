//! Container restart, unobserved exits and CrashLoopBackOff against upstream
//! (Kubernetes v1.34.0, pkg/kubelet), row by row: `Vector::ContainerRestart`.
//!
//! Every checked row is driven through engenho's own decisions — the three
//! per-site restart decisions ([`starts_fresh`], [`running_action`],
//! [`down_action`]), the phase fold ([`reconcile_pod_phase_with_init`]), and
//! the crash gate ([`crash_gate`]) — with the row's runtime states read into
//! engenho's vocabulary by engenho's own readers ([`Termination::from_run_state`],
//! [`Termination::from_wire`]). What is out of scope, and why, is in
//! [`OUT_OF_SCOPE`]; the keys a checked row leaves unanswered, and why, are in
//! [`PARTIAL`]; where engenho differs on purpose is in [`DEVIATIONS`].
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

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use engenho_kubelet::backoff::{CRASH, CrashBackoff, CrashGate, crash_gate};
use engenho_kubelet::cri::{ExitDisposition, RunState};
use engenho_kubelet::lifecycle::{
    ContainerObservation, DownAction, RestartPolicy, RunningAction, Termination, down_action,
    reconcile_pod_phase_with_init, running_action, starts_fresh,
};
use engenho_kubelet::{
    PodLifecycle, ProbeKind, ProbeObservation, ProbeRuntime, ProbeSpec, ProbeTrip,
    fold_probe_observation,
};
use engenho_oracle::{Answer, Case, Deviation, OutOfScope, Table, Vector, run};
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
        "podIsTerminal is the pod worker's verdict that it has finished terminating the pod \
         (deletion, eviction). engenho's phase fold has no such input: a pod leaves engenho's \
         kubelet when its object is gone, and a pod under Always is never reported terminal",
    ),
    OutOfScope::case(
        "phase/succeeded+failed under Always but podIsTerminal -> Failed",
        "as `phase/all succeeded under Always but podIsTerminal -> Succeeded`: podIsTerminal \
         has no counterpart in engenho's fold",
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
    OutOfScope::kind(
        "convert_to_api_container_statuses",
        "upstream folds the runtime's history of a container's runs, the previous API status \
         and the reason cache into state, lastTerminationState and a message. engenho keeps \
         one record per container and renders state and restartCount only: no \
         lastTerminationState and no waiting or terminated message (a gap). The unobserved \
         exit these rows render — terminated, 137, ContainerStatusUnknown — is \
         Termination::Unknown's wire form, which get_phase rows read back through \
         Termination::from_wire and lifecycle.rs pins",
    ),
    OutOfScope::kind(
        "generate_api_pod_status",
        "as convert_to_api_container_statuses: every row checks the per-container \
         lastTerminationState engenho does not render, alongside the phase",
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

/// Checked rows that are checked in part. The harness accepts an answer
/// missing a key and reports only the key, per kind; the test pins this
/// per-row list both ways, so an answer that drops a key not listed here
/// fails, and so does a listing whose key is now answered.
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
        key: "event",
        rows: &["do-backoff/second crash 5s after finish with 10s entry: CrashLoopBackOff"],
        why: "the BackOff event's text. engenho emits a BackOff event on every hold, but words \
              it with the remaining wait and prior restarts, not upstream's `Back-off \
              restarting failed container X in pod name_ns(uid)`; the event is emitted by the \
              kubelet loop, which these pure rows do not drive",
    },
];

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
        case: "do-backoff/no exited record: no backoff check and no Next",
        why: "as scbr/created-never-started/Never restarts: a CREATED container is read as an \
              unobserved exit, so its restart goes through the crash gate and is charged a \
              step, where upstream finds no exited record and charges nothing",
    },
];

const CHECKED_ROWS: usize = 72;

#[test]
fn container_restart_agrees_with_upstream() {
    let table = Vector::ContainerRestart.load();
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
/// but the adapter's answer does not.
fn unanswered_keys(table: &Table, answers: &HashMap<String, Answer>) -> BTreeSet<(String, String)> {
    let mut unanswered = BTreeSet::new();
    for case in &table.cases {
        if let Some(Answer::Checked(Value::Object(got))) = answers.get(&case.name)
            && let Value::Object(expected) = &case.expected
        {
            for key in expected.keys().filter(|k| !got.contains_key(*k)) {
                unanswered.insert((case.name.clone(), key.clone()));
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
