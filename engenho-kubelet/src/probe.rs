//! Typed container-probe state machine — the TYPED-SPEC + INTERPRETER border.
//!
//! This module is the **typed border** + **pure interpreter** half of the
//! kubelet's *probe* triplet (the runtime `exec` seam + the [`NetProber`]
//! seam in [`crate::backend`] are the mock environment):
//!
//!   * **Typed border** — the closed enums [`ProbeKind`], [`ProbeHandler`],
//!     [`HttpScheme`], the timing record [`ProbeTiming`], and the composed
//!     [`ProbeSpec`]. Bad states are unrepresentable: a probe ALWAYS carries
//!     exactly one handler (a probe with no handler is a parse-time
//!     [`ProbeParseError::NoHandler`], never a fake pass); an unsupported
//!     handler (grpc) is a typed [`ProbeParseError::UnsupportedHandler`]
//!     (documented deferral, NOT a silent skip).
//!   * **Interpreter** — [`fold_probe_observation`] + [`aggregate_container_readiness`],
//!     PURE functions that fold a [`ProbeObservation`] (`Success`/`Failure`,
//!     already reduced from the exec exit-code / http status / tcp connect by
//!     the I/O shell) + the per-probe [`ProbeRuntime`] threshold counters into
//!     a [`ProbeVerdict`] `(ready, needs_restart, startup_done)`. No I/O, no
//!     podman, no socket — so the WHOLE probe-verdict logic is unit-testable
//!     (and proptest-able) against mocks with zero container runtime.
//!
//! The kubelet's `reconcile_running` ([`crate::kubelet`]) is the I/O shell: it
//! decides which probes are *due* (period + initialDelay), runs each handler
//! through the [`run_handler`] wrapper (the SOLE place that touches a Fake),
//! folds the observation, then aggregates the per-container effective
//! readiness + restart decision.
//!
//! ## No silent wrong answers
//!
//! Every parse rejection is a typed error. Every unimplemented surface (grpc,
//! an unresolvable named port) is a typed error at parse time — never a fake
//! `Success`. There is no `todo!()` / `panic!()` / placeholder `Ok` in any
//! production path.
//!
//! ## Typed border derives
//!
//! The border enums use the SAME plain-serde derive set as the crate's
//! existing typed-spec border in [`crate::lifecycle`] (`RestartPolicy` /
//! `ContainerState`). The org PRIME DIRECTIVE asks for
//! `#[derive(TataraDomain)]`; that derive lives in the optional `tatara_lisp`
//! crate behind a feature gate (see `engenho-fonte`), which the hot-path
//! kubelet crate deliberately does NOT depend on. Following the in-crate
//! precedent (plain serde border) is the load-bearing choice here — the Lisp
//! authoring surface for probes, when it lands, mirrors the lifecycle border
//! the same way.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backend::{ContainerRuntime, HttpProbeTarget, NetProber, TcpProbeTarget};

// =====================================================================
// Typed border
// =====================================================================

/// Which verdict a probe feeds. Closed enum mirroring the three K8s probe
/// fields (`livenessProbe`, `readinessProbe`, `startupProbe`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ProbeKind {
    /// Drives container restart: failing past `failureThreshold` ⇒
    /// `needs_restart`.
    Liveness,
    /// Drives the container's `ready` bit (→ `containerStatuses[].ready` →
    /// the pod `Ready`/`ContainersReady` conditions): passing past
    /// `successThreshold` ⇒ ready; failing past `failureThreshold` ⇒ not
    /// ready.
    Readiness,
    /// Gates liveness + readiness during slow boot: until it passes past
    /// `successThreshold` (`startup_done`), readiness is forced false AND
    /// liveness restart is suppressed.
    Startup,
}

/// HTTP scheme for an `httpGet` probe. Closed enum; defaults to
/// [`HttpScheme::Http`] (the K8s default).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpScheme {
    /// Plain HTTP (the K8s default).
    #[default]
    Http,
    /// HTTPS.
    Https,
}

impl HttpScheme {
    /// Parse the K8s `scheme` string (`"HTTP"` / `"HTTPS"`, case-insensitive).
    /// Absent / unrecognized ⇒ [`HttpScheme::Http`] (the K8s default).
    #[must_use]
    pub fn from_k8s(s: Option<&str>) -> Self {
        match s.map(str::to_ascii_uppercase).as_deref() {
            Some("HTTPS") => HttpScheme::Https,
            _ => HttpScheme::Http,
        }
    }

    /// The URL scheme literal (`"http"` / `"https"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            HttpScheme::Http => "http",
            HttpScheme::Https => "https",
        }
    }
}

/// A resolved probe port. K8s `port` is an `IntOrString` (an integer or a
/// named container port); the parser resolves a name against
/// `spec.containers[i].ports[].name` at parse time, so by the time a
/// [`ProbeHandler`] exists the port is ALWAYS a concrete `u16`. An
/// unresolvable name is a parse-time [`ProbeParseError::UnresolvedPort`],
/// never a silent skip.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbePort(pub u16);

/// A probe's action. Closed enum — exactly one handler per probe. A K8s
/// `Probe` with NO action is a [`ProbeParseError::NoHandler`]; a `grpc`
/// action is a [`ProbeParseError::UnsupportedHandler`] (documented deferral).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeHandler {
    /// `exec` — run an argv inside the container; exit 0 = success.
    Exec {
        /// The argv to exec (no shell). Empty argv is a parse-time error.
        command: Vec<String>,
    },
    /// `httpGet` — issue an HTTP GET against the pod IP; a 2xx/3xx status =
    /// success.
    HttpGet {
        /// Request path (defaults to `/` when absent in the manifest).
        path: String,
        /// Target port (resolved at parse time).
        port: ProbePort,
        /// URL scheme.
        scheme: HttpScheme,
        /// Optional `Host` override (defaults to the pod IP).
        host: Option<String>,
        /// Custom request headers (`httpHeaders`).
        headers: Vec<(String, String)>,
    },
    /// `tcpSocket` — open a TCP connection to the pod IP:port; connect-ok =
    /// success.
    TcpSocket {
        /// Target port (resolved at parse time).
        port: ProbePort,
        /// Optional host override (defaults to the pod IP).
        host: Option<String>,
    },
}

/// Probe timing knobs, with K8s defaults applied + min-clamped at parse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeTiming {
    /// `initialDelaySeconds` — wait this long after container start before the
    /// FIRST probe run. Default 0.
    pub initial_delay: Duration,
    /// `periodSeconds` — how often to probe. Default 10s, min-clamped 1s.
    pub period: Duration,
    /// `timeoutSeconds` — per-run I/O timeout. Default 1s, min-clamped 1s.
    pub timeout: Duration,
    /// `successThreshold` — consecutive successes to flip the gate. Default 1;
    /// FORCED to 1 for liveness/startup (K8s rule). Min 1.
    pub success_threshold: u32,
    /// `failureThreshold` — consecutive failures to trip. Default 3. Min 1.
    pub failure_threshold: u32,
}

impl ProbeTiming {
    /// The K8s default timing for a probe of `kind` (before any manifest
    /// overrides): initialDelay 0, period 10s, timeout 1s, successThreshold 1,
    /// failureThreshold 3. `successThreshold` stays 1 for liveness/startup.
    #[must_use]
    pub fn k8s_defaults() -> Self {
        Self {
            initial_delay: Duration::ZERO,
            period: Duration::from_secs(10),
            timeout: Duration::from_secs(1),
            success_threshold: 1,
            failure_threshold: 3,
        }
    }
}

/// A fully-typed probe: kind + handler + timing. The parse output of
/// [`ProbeSpec::from_k8s`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeSpec {
    /// Which verdict the probe feeds.
    pub kind: ProbeKind,
    /// The probe action (exactly one).
    pub handler: ProbeHandler,
    /// Timing + threshold knobs.
    pub timing: ProbeTiming,
}

// =====================================================================
// Parse errors
// =====================================================================

/// Typed probe-parse failures. Every one is surfaced (the kubelet skips the
/// pod + bumps `objects_skipped`), NEVER a fake pass.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProbeParseError {
    /// The `Probe` declared no action (no exec/httpGet/tcpSocket/grpc).
    #[error("probe has no handler (exec/httpGet/tcpSocket required)")]
    NoHandler,
    /// An exec probe with an empty `command` argv.
    #[error("exec probe has an empty command")]
    EmptyExecCommand,
    /// A handler kind that engenho does not yet implement (grpc). Documented
    /// deferral — surfaced, never silently passed.
    #[error("unsupported probe handler: {kind}")]
    UnsupportedHandler {
        /// The handler kind string (e.g. `"grpc"`).
        kind: &'static str,
    },
    /// A named port that does not resolve against the container's
    /// `ports[].name`.
    #[error("probe port {name:?} does not resolve to a container port")]
    UnresolvedPort {
        /// The unresolvable port name.
        name: String,
    },
    /// The `port` value was neither an integer in `1..=65535` nor a string.
    #[error("probe port value is invalid: {reason}")]
    InvalidPort {
        /// Why the port is invalid.
        reason: String,
    },
}

impl ProbeSpec {
    /// Parse a raw-JSON `Probe` object (the kubelet reads pods as
    /// [`serde_json::Value`] end to end) of `kind` into the typed border,
    /// applying K8s defaults + min-clamps + the liveness/startup
    /// `successThreshold==1` rule, resolving the port against `container_ports`
    /// (`spec.containers[i].ports[]`), and rejecting no-handler / grpc /
    /// unresolved-port with typed errors.
    ///
    /// `container_ports` is the slice of `(name, number)` pairs from the
    /// container's `ports[]` — used only to resolve a NAMED probe port.
    ///
    /// # Errors
    ///
    /// [`ProbeParseError`] on no-handler, empty-exec, grpc, or unresolvable /
    /// invalid port. Never a silent skip.
    pub fn from_k8s(
        kind: ProbeKind,
        probe: &Value,
        container_ports: &[(String, u16)],
    ) -> Result<Self, ProbeParseError> {
        let timing = Self::parse_timing(kind, probe);

        // Exactly one handler. grpc → typed UnsupportedHandler. None → NoHandler.
        let handler = if let Some(exec) = probe.get("exec") {
            let command: Vec<String> = exec
                .get("command")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            if command.is_empty() {
                return Err(ProbeParseError::EmptyExecCommand);
            }
            ProbeHandler::Exec { command }
        } else if let Some(http) = probe.get("httpGet") {
            let port = resolve_port(http.get("port"), container_ports)?;
            let path = http
                .get("path")
                .and_then(|p| p.as_str())
                .unwrap_or("/")
                .to_string();
            let scheme = HttpScheme::from_k8s(http.get("scheme").and_then(|s| s.as_str()));
            let host = http
                .get("host")
                .and_then(|h| h.as_str())
                .map(String::from)
                .filter(|h| !h.is_empty());
            let headers = http
                .get("httpHeaders")
                .and_then(|h| h.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|h| {
                            let n = h.get("name")?.as_str()?.to_string();
                            let v = h.get("value")?.as_str()?.to_string();
                            Some((n, v))
                        })
                        .collect()
                })
                .unwrap_or_default();
            ProbeHandler::HttpGet {
                path,
                port,
                scheme,
                host,
                headers,
            }
        } else if let Some(tcp) = probe.get("tcpSocket") {
            let port = resolve_port(tcp.get("port"), container_ports)?;
            let host = tcp
                .get("host")
                .and_then(|h| h.as_str())
                .map(String::from)
                .filter(|h| !h.is_empty());
            ProbeHandler::TcpSocket { port, host }
        } else if probe.get("grpc").is_some() {
            return Err(ProbeParseError::UnsupportedHandler { kind: "grpc" });
        } else {
            return Err(ProbeParseError::NoHandler);
        };

        Ok(Self {
            kind,
            handler,
            timing,
        })
    }

    /// Fold the raw-JSON timing fields into a [`ProbeTiming`], applying K8s
    /// defaults, min-clamps (period/timeout ≥ 1s, thresholds ≥ 1), and the
    /// liveness/startup `successThreshold == 1` rule.
    fn parse_timing(kind: ProbeKind, probe: &Value) -> ProbeTiming {
        let mut t = ProbeTiming::k8s_defaults();
        // i64 reads tolerate either JSON integer width; negatives clamp to the
        // floor below.
        let secs =
            |key: &str| -> Option<i64> { probe.get(key).and_then(serde_json::Value::as_i64) };

        if let Some(d) = secs("initialDelaySeconds") {
            // initialDelay floor is 0 (a 0 / negative value = run from start).
            t.initial_delay = Duration::from_secs(u64::try_from(d.max(0)).unwrap_or(0));
        }
        if let Some(p) = secs("periodSeconds") {
            t.period = Duration::from_secs(u64::try_from(p.max(1)).unwrap_or(10));
        }
        if let Some(to) = secs("timeoutSeconds") {
            t.timeout = Duration::from_secs(u64::try_from(to.max(1)).unwrap_or(1));
        }
        if let Some(st) = secs("successThreshold") {
            t.success_threshold = u32::try_from(st.max(1)).unwrap_or(1);
        }
        if let Some(ft) = secs("failureThreshold") {
            t.failure_threshold = u32::try_from(ft.max(1)).unwrap_or(3);
        }
        // K8s rule: successThreshold MUST be 1 for liveness + startup.
        if matches!(kind, ProbeKind::Liveness | ProbeKind::Startup) {
            t.success_threshold = 1;
        }
        t
    }
}

/// Resolve a raw-JSON `port` value (integer or named string) against the
/// container's `ports[]` (`(name, number)` pairs). Returns a typed error for
/// an unresolvable name or an out-of-range / non-int-non-string value.
fn resolve_port(
    port: Option<&Value>,
    container_ports: &[(String, u16)],
) -> Result<ProbePort, ProbeParseError> {
    let Some(port) = port else {
        return Err(ProbeParseError::InvalidPort {
            reason: "missing port".to_string(),
        });
    };
    // Integer port: must be 1..=65535.
    if let Some(n) = port.as_i64() {
        if let Ok(p) = u16::try_from(n)
            .map_err(|_| ())
            .and_then(|p| if p >= 1 { Ok(p) } else { Err(()) })
        {
            return Ok(ProbePort(p));
        }
        return Err(ProbeParseError::InvalidPort {
            reason: format!("integer port {n} out of range 1..=65535"),
        });
    }
    // Named port: look up against the container's ports[].name.
    if let Some(name) = port.as_str() {
        // A numeric string ("8080") is also a valid integer port.
        if let Ok(p) = name
            .parse::<u16>()
            .map_err(|_| ())
            .and_then(|p| if p >= 1 { Ok(p) } else { Err(()) })
        {
            return Ok(ProbePort(p));
        }
        if let Some((_, number)) = container_ports.iter().find(|(n, _)| n == name) {
            return Ok(ProbePort(*number));
        }
        return Err(ProbeParseError::UnresolvedPort {
            name: name.to_string(),
        });
    }
    Err(ProbeParseError::InvalidPort {
        reason: "port is neither an integer nor a string".to_string(),
    })
}

// =====================================================================
// Runtime threshold/timing state (per-probe; lives on the kubelet's
// per-container record)
// =====================================================================

/// Per-probe runtime counters + timing state. One per active probe, hung off
/// the kubelet's per-container record so it persists across ticks (like
/// `restart_count`). Reset on a container restart (fresh startup window).
#[derive(Clone, Debug)]
pub struct ProbeRuntime {
    /// Consecutive successes since the last failure.
    pub consecutive_successes: u32,
    /// Consecutive failures since the last success.
    pub consecutive_failures: u32,
    /// When this probe last RAN (for the `period` cadence). `None` until the
    /// first run.
    pub last_run: Option<Instant>,
    /// When the container (this probe is attached to) started — the
    /// `initialDelay` reference point.
    pub started_at: Instant,
    /// The latched gate: readiness ⇒ ready, startup ⇒ done, liveness unused.
    pub gate_satisfied: bool,
}

impl ProbeRuntime {
    /// Fresh runtime for a probe attached to a container that started `now`.
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self {
            consecutive_successes: 0,
            consecutive_failures: 0,
            last_run: None,
            started_at: now,
            gate_satisfied: false,
        }
    }

    /// `true` iff the probe is past its `initialDelay` window at `now` (the
    /// FIRST run is allowed). Before this, the probe is not run.
    #[must_use]
    pub fn past_initial_delay(&self, spec: &ProbeSpec, now: Instant) -> bool {
        now.duration_since(self.started_at) >= spec.timing.initial_delay
    }

    /// `true` iff the probe is DUE at `now`: past `initialDelay` AND
    /// (`last_run` is `None` OR `now >= last_run + period`).
    #[must_use]
    pub fn is_due(&self, spec: &ProbeSpec, now: Instant) -> bool {
        if !self.past_initial_delay(spec, now) {
            return false;
        }
        match self.last_run {
            None => true,
            Some(last) => now.duration_since(last) >= spec.timing.period,
        }
    }

    /// When this probe is NEXT due at-or-after `now`, as a delay from `now`.
    /// `None` if the probe has never run (it's due immediately once past the
    /// initialDelay — the caller runs it this tick). Used to compute the
    /// kubelet's `Requeue{after}` cadence.
    #[must_use]
    pub fn next_due_in(&self, spec: &ProbeSpec, now: Instant) -> Duration {
        // Before initialDelay: next due is when the delay elapses.
        let since_start = now.duration_since(self.started_at);
        if since_start < spec.timing.initial_delay {
            return spec.timing.initial_delay.saturating_sub(since_start);
        }
        match self.last_run {
            None => Duration::ZERO,
            Some(last) => {
                let elapsed = now.duration_since(last);
                spec.timing.period.saturating_sub(elapsed)
            }
        }
    }
}

// =====================================================================
// Observation + verdict
// =====================================================================

/// The reduced result of running ONE probe handler — the runtime/net layer
/// collapsed exit-code / http-status / connect-result (and any timeout / I/O
/// error) into one of these two before the fold sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeObservation {
    /// The probe passed this run (exec exit 0 / http 2xx-3xx / tcp connect ok).
    Success,
    /// The probe failed this run (non-zero exit / non-2xx-3xx / connect refused
    /// / timeout / I/O error).
    Failure,
}

/// The per-probe verdict the fold produces. The exact `(ready, needs_restart,
/// startup_done)` tuple. Only the field matching the probe's [`ProbeKind`] is
/// meaningful; the others stay at their identity (`false`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeVerdict {
    /// Readiness verdict (meaningful for [`ProbeKind::Readiness`]).
    pub ready: bool,
    /// Liveness/startup restart verdict (meaningful for [`ProbeKind::Liveness`]
    /// + [`ProbeKind::Startup`]).
    pub needs_restart: bool,
    /// Startup-gate verdict (meaningful for [`ProbeKind::Startup`]).
    pub startup_done: bool,
}

/// PURE probe fold — update the counters + emit the verdict. NO I/O.
///
/// Given the probe `spec`, its mutable runtime counters `rt`, the reduced
/// `obs`, and `now`, advance the counters and return the per-kind verdict:
///
///   * **Success** → `consecutive_successes += 1`, `consecutive_failures = 0`;
///     once `successes >= success_threshold` the gate latches (readiness
///     ⇒ `ready=true`; startup ⇒ `startup_done=true`).
///   * **Failure** → `consecutive_failures += 1`, `consecutive_successes = 0`;
///     once `failures >= failure_threshold` the trip fires (readiness
///     ⇒ `ready=false`; liveness/startup ⇒ `needs_restart=true`).
///
/// `rt.gate_satisfied` is the latched gate (readiness=ready / startup=done);
/// the verdict mirrors it for readiness/startup so a steady-passing probe
/// keeps reporting `ready=true` / `startup_done=true` between threshold edges.
///
/// `now` stamps `rt.last_run` (the period reference) — the caller has already
/// decided the probe is due.
#[must_use]
pub fn fold_probe_observation(
    spec: &ProbeSpec,
    rt: &mut ProbeRuntime,
    obs: ProbeObservation,
    now: Instant,
) -> ProbeVerdict {
    rt.last_run = Some(now);
    let mut verdict = ProbeVerdict::default();

    match obs {
        ProbeObservation::Success => {
            rt.consecutive_successes = rt.consecutive_successes.saturating_add(1);
            rt.consecutive_failures = 0;
            if rt.consecutive_successes >= spec.timing.success_threshold {
                rt.gate_satisfied = true;
            }
        }
        ProbeObservation::Failure => {
            rt.consecutive_failures = rt.consecutive_failures.saturating_add(1);
            rt.consecutive_successes = 0;
            if rt.consecutive_failures >= spec.timing.failure_threshold {
                match spec.kind {
                    // Readiness: a failure trip clears the ready gate.
                    ProbeKind::Readiness => rt.gate_satisfied = false,
                    // Liveness/startup: a failure trip requests a restart.
                    ProbeKind::Liveness | ProbeKind::Startup => verdict.needs_restart = true,
                }
            }
        }
    }

    // The gate-derived fields mirror the latched gate per kind.
    match spec.kind {
        ProbeKind::Readiness => verdict.ready = rt.gate_satisfied,
        ProbeKind::Startup => verdict.startup_done = rt.gate_satisfied,
        ProbeKind::Liveness => {}
    }
    verdict
}

/// PURE per-container readiness/restart aggregation — fold the per-kind gates
/// into the container's effective `ready` + whether liveness restart may fire.
///
/// Inputs (all already computed by the kubelet from the per-probe runtimes):
///   * `startup_done` — whether the startup probe has passed (`true` if there
///     is no startup probe — no gate to satisfy).
///   * `readiness_ready` — the readiness gate (`rt.gate_satisfied`).
///   * `has_startup` / `has_readiness` — whether each probe exists.
///   * `is_running` — whether the container is observed Running.
///
/// Returns `(effective_ready, may_run_restart_probes)`:
///   * **Startup gates**: while a startup probe exists and is NOT done,
///     readiness is FORCED false AND liveness restart is suppressed
///     (`may_run_restart_probes = false`) — the startup window.
///   * **No readiness probe** ⇒ `effective_ready = is_running` (the
///     behavior-preserving common case — ready immediately once Running).
///   * **No startup probe** ⇒ liveness + readiness active from initialDelay
///     onward (`may_run_restart_probes = true`).
///
/// Note: the startup probe ITSELF can still request a restart (a container that
/// never boots IS restarted); that is the startup probe's own `needs_restart`
/// verdict, handled by the kubelet separately — `may_run_restart_probes` here
/// gates only the LIVENESS restart during the startup window.
//
// The four bools are the precise (startup_done, readiness_ready, has_startup,
// has_readiness) gate inputs from the spec — collapsing them into enums would
// obscure the K8s mapping, so the signature is intentional.
#[allow(clippy::fn_params_excessive_bools)]
#[must_use]
pub fn aggregate_container_readiness(
    startup_done: bool,
    readiness_ready: bool,
    has_startup: bool,
    has_readiness: bool,
    is_running: bool,
) -> (bool, bool) {
    // Startup window: a startup probe exists + isn't done → readiness false +
    // liveness suppressed.
    if has_startup && !startup_done {
        return (false, false);
    }
    // Past the startup gate (or no startup probe): readiness sources from the
    // readiness gate, else from is_running (behavior-preserving). Liveness may
    // run.
    let effective_ready = if has_readiness {
        readiness_ready
    } else {
        is_running
    };
    (effective_ready, true)
}

// =====================================================================
// I/O shell — the SOLE place that touches a Fake (runtime exec + NetProber)
// =====================================================================

/// Run ONE probe handler against the live runtime + net seams, reducing the
/// result to a [`ProbeObservation`]. The SOLE place that touches the runtime
/// `exec` or the [`NetProber`] — so the fold tests never need real exec /
/// http / tcp. Every handler is bounded by `spec.timing.timeout`; a timeout or
/// any I/O error maps to [`ProbeObservation::Failure`] (NOT an error that
/// aborts the tick — a failing probe is a normal, expected signal).
///
/// `container_id` is the exec target (container-scoped); `pod_ip` is the
/// http/tcp target (network-scoped). A handler whose target is unavailable
/// (e.g. http/tcp with no pod IP yet) maps to `Failure` — the probe simply
/// hasn't passed yet.
pub async fn run_handler(
    spec: &ProbeSpec,
    runtime: &dyn ContainerRuntime,
    net_prober: &dyn NetProber,
    container_id: &str,
    pod_ip: Option<&str>,
) -> ProbeObservation {
    let timeout = spec.timing.timeout;
    match &spec.handler {
        ProbeHandler::Exec { command } => {
            let fut = runtime.exec(container_id, command);
            match tokio::time::timeout(timeout, fut).await {
                // Exit 0 = healthy; any non-zero / I/O error / timeout = failure.
                Ok(Ok(outcome)) if outcome.exit_code == 0 => ProbeObservation::Success,
                _ => ProbeObservation::Failure,
            }
        }
        ProbeHandler::HttpGet {
            path,
            port,
            scheme,
            host,
            headers,
        } => {
            let Some(ip) = pod_ip else {
                return ProbeObservation::Failure;
            };
            let target = HttpProbeTarget {
                ip: ip.to_string(),
                port: port.0,
                path: path.clone(),
                scheme: *scheme,
                host: host.clone(),
                headers: headers.clone(),
                timeout,
            };
            match tokio::time::timeout(timeout, net_prober.http_get(&target)).await {
                // K8s: 2xx/3xx is healthy.
                Ok(Ok(status)) if (200..400).contains(&status) => ProbeObservation::Success,
                _ => ProbeObservation::Failure,
            }
        }
        ProbeHandler::TcpSocket { port, host } => {
            let Some(ip) = pod_ip else {
                return ProbeObservation::Failure;
            };
            let target = TcpProbeTarget {
                ip: ip.to_string(),
                port: port.0,
                host: host.clone(),
                timeout,
            };
            match tokio::time::timeout(timeout, net_prober.tcp_connect(&target)).await {
                Ok(Ok(())) => ProbeObservation::Success,
                _ => ProbeObservation::Failure,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t0() -> Instant {
        Instant::now()
    }

    fn exec_spec(kind: ProbeKind, success_threshold: u32, failure_threshold: u32) -> ProbeSpec {
        ProbeSpec {
            kind,
            handler: ProbeHandler::Exec {
                command: vec!["true".into()],
            },
            timing: ProbeTiming {
                initial_delay: Duration::ZERO,
                period: Duration::from_secs(1),
                timeout: Duration::from_secs(1),
                success_threshold,
                failure_threshold,
            },
        }
    }

    // ── fold_probe_observation: readiness threshold ────────────────────────

    #[test]
    fn readiness_flips_ready_only_after_success_threshold() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 3, 3);
        let mut rt = ProbeRuntime::new(now);

        // Two successes: not yet at threshold 3 → not ready.
        let v1 = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert!(!v1.ready);
        let v2 = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert!(!v2.ready);
        // Third success crosses threshold → ready.
        let v3 = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert!(v3.ready);
        // Steady passing keeps it ready.
        let v4 = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert!(v4.ready);
    }

    #[test]
    fn readiness_one_failure_resets_success_counter() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 2, 3);
        let mut rt = ProbeRuntime::new(now);

        let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        // One failure resets the success counter; not at failure threshold so
        // the gate is unchanged (still false — never reached ready).
        let vf = fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now);
        assert!(!vf.ready);
        assert_eq!(rt.consecutive_successes, 0);
        // Need 2 fresh successes again.
        let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        let vr = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert!(vr.ready);
    }

    #[test]
    fn readiness_failure_threshold_clears_ready() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 1, 2);
        let mut rt = ProbeRuntime::new(now);

        // One success → ready (threshold 1).
        assert!(fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now).ready);
        // One failure: not yet at failure threshold 2 → still ready (latched).
        assert!(fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).ready);
        // Second consecutive failure crosses threshold → not ready.
        assert!(!fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).ready);
    }

    // ── fold_probe_observation: liveness threshold ─────────────────────────

    #[test]
    fn liveness_needs_restart_only_after_failure_threshold() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Liveness, 1, 3);
        let mut rt = ProbeRuntime::new(now);

        // Two failures: below threshold 3 → no restart.
        assert!(
            !fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
        assert!(
            !fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
        // Third consecutive failure → needs_restart.
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
    }

    #[test]
    fn liveness_success_resets_failure_counter() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Liveness, 1, 2);
        let mut rt = ProbeRuntime::new(now);

        let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now);
        // A success resets the failure counter.
        let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert_eq!(rt.consecutive_failures, 0);
        // One more failure is NOT enough now (need 2 consecutive again).
        assert!(
            !fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
    }

    // ── fold_probe_observation: startup ────────────────────────────────────

    #[test]
    fn startup_sets_done_after_success_threshold() {
        let now = t0();
        // successThreshold forced to 1 for startup regardless; use the
        // constructor that already forces it.
        let spec = exec_spec(ProbeKind::Startup, 1, 3);
        let mut rt = ProbeRuntime::new(now);
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now).startup_done
        );
    }

    #[test]
    fn startup_needs_restart_after_failure_threshold() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Startup, 1, 2);
        let mut rt = ProbeRuntime::new(now);
        assert!(
            !fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).needs_restart
        );
    }

    // ── aggregate_container_readiness ──────────────────────────────────────

    #[test]
    fn aggregate_no_readiness_probe_is_is_running() {
        // The behavior-preserving lemma: no readiness probe → ready==is_running.
        let (ready, may_restart) = aggregate_container_readiness(true, false, false, false, true);
        assert!(ready, "no readiness probe → ready mirrors is_running");
        assert!(may_restart);

        let (ready_down, _) = aggregate_container_readiness(true, false, false, false, false);
        assert!(!ready_down, "not running → not ready");
    }

    #[test]
    fn aggregate_startup_unsatisfied_forces_not_ready_and_suppresses_liveness() {
        // has_startup=true, startup_done=false → ready forced false +
        // may_run_restart_probes false (liveness suppressed during the window).
        let (ready, may_restart) = aggregate_container_readiness(false, true, true, true, true);
        assert!(!ready, "startup not done → readiness forced false");
        assert!(!may_restart, "startup window suppresses liveness restart");
    }

    #[test]
    fn aggregate_startup_done_lets_readiness_and_liveness_through() {
        // startup_done=true → readiness sources from the readiness gate + may
        // restart.
        let (ready, may_restart) = aggregate_container_readiness(true, true, true, true, true);
        assert!(ready, "startup done → readiness gate applies");
        assert!(may_restart, "startup done → liveness active");

        let (not_ready, _) = aggregate_container_readiness(true, false, true, true, true);
        assert!(
            !not_ready,
            "startup done but readiness gate false → not ready"
        );
    }

    #[test]
    fn aggregate_no_startup_probe_active_from_start() {
        // No startup probe → readiness gate applies + liveness active.
        let (ready, may_restart) = aggregate_container_readiness(true, true, false, true, true);
        assert!(ready);
        assert!(may_restart);
    }

    // ── ProbeSpec::from_k8s defaults + errors ──────────────────────────────

    #[test]
    fn from_k8s_applies_defaults() {
        let probe = json!({ "exec": { "command": ["sh", "-c", "true"] } });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        assert_eq!(spec.timing.period, Duration::from_secs(10));
        assert_eq!(spec.timing.timeout, Duration::from_secs(1));
        assert_eq!(spec.timing.success_threshold, 1);
        assert_eq!(spec.timing.failure_threshold, 3);
        assert_eq!(spec.timing.initial_delay, Duration::ZERO);
        assert!(matches!(spec.handler, ProbeHandler::Exec { .. }));
    }

    #[test]
    fn from_k8s_liveness_forces_success_threshold_one() {
        // successThreshold 5 on a liveness probe is forced to 1 (K8s rule).
        let probe = json!({
            "exec": { "command": ["true"] },
            "successThreshold": 5
        });
        let spec = ProbeSpec::from_k8s(ProbeKind::Liveness, &probe, &[]).unwrap();
        assert_eq!(spec.timing.success_threshold, 1);
    }

    #[test]
    fn from_k8s_readiness_honors_success_threshold() {
        let probe = json!({
            "exec": { "command": ["true"] },
            "successThreshold": 4
        });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        assert_eq!(spec.timing.success_threshold, 4);
    }

    #[test]
    fn from_k8s_min_clamps() {
        let probe = json!({
            "exec": { "command": ["true"] },
            "periodSeconds": 0,
            "timeoutSeconds": 0,
            "failureThreshold": 0
        });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        assert_eq!(spec.timing.period, Duration::from_secs(1));
        assert_eq!(spec.timing.timeout, Duration::from_secs(1));
        assert_eq!(spec.timing.failure_threshold, 1);
    }

    #[test]
    fn from_k8s_no_handler_is_typed_error() {
        let probe = json!({ "periodSeconds": 5 });
        assert_eq!(
            ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap_err(),
            ProbeParseError::NoHandler
        );
    }

    #[test]
    fn from_k8s_grpc_is_unsupported_handler() {
        let probe = json!({ "grpc": { "port": 9000 } });
        assert_eq!(
            ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap_err(),
            ProbeParseError::UnsupportedHandler { kind: "grpc" }
        );
    }

    #[test]
    fn from_k8s_empty_exec_is_typed_error() {
        let probe = json!({ "exec": { "command": [] } });
        assert_eq!(
            ProbeSpec::from_k8s(ProbeKind::Liveness, &probe, &[]).unwrap_err(),
            ProbeParseError::EmptyExecCommand
        );
    }

    #[test]
    fn from_k8s_integer_port_resolves() {
        let probe = json!({ "httpGet": { "path": "/healthz", "port": 8080 } });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        match spec.handler {
            ProbeHandler::HttpGet {
                port, path, scheme, ..
            } => {
                assert_eq!(port, ProbePort(8080));
                assert_eq!(path, "/healthz");
                assert_eq!(scheme, HttpScheme::Http);
            }
            other => panic!("expected HttpGet, got {other:?}"),
        }
    }

    #[test]
    fn from_k8s_named_port_resolves_against_container_ports() {
        let probe = json!({ "httpGet": { "port": "http" } });
        let ports = vec![("http".to_string(), 8080u16), ("metrics".to_string(), 9090)];
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &ports).unwrap();
        match spec.handler {
            ProbeHandler::HttpGet { port, .. } => assert_eq!(port, ProbePort(8080)),
            other => panic!("expected HttpGet, got {other:?}"),
        }
    }

    #[test]
    fn from_k8s_unresolved_named_port_is_typed_error() {
        let probe = json!({ "tcpSocket": { "port": "nope" } });
        assert_eq!(
            ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap_err(),
            ProbeParseError::UnresolvedPort {
                name: "nope".to_string()
            }
        );
    }

    #[test]
    fn from_k8s_https_scheme_parsed() {
        let probe = json!({ "httpGet": { "port": 443, "scheme": "HTTPS" } });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        match spec.handler {
            ProbeHandler::HttpGet { scheme, .. } => assert_eq!(scheme, HttpScheme::Https),
            other => panic!("expected HttpGet, got {other:?}"),
        }
    }

    #[test]
    fn from_k8s_tcp_socket_parsed() {
        let probe = json!({ "tcpSocket": { "port": 6379 } });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        assert!(matches!(
            spec.handler,
            ProbeHandler::TcpSocket {
                port: ProbePort(6379),
                ..
            }
        ));
    }

    // ── ProbeRuntime cadence ───────────────────────────────────────────────

    #[test]
    fn probe_due_respects_initial_delay() {
        let now = t0();
        let mut spec = exec_spec(ProbeKind::Readiness, 1, 1);
        spec.timing.initial_delay = Duration::from_secs(5);
        let rt = ProbeRuntime::new(now);
        // At start: not past initialDelay → not due.
        assert!(!rt.is_due(&spec, now));
        // After 5s: due.
        assert!(rt.is_due(&spec, now + Duration::from_secs(5)));
    }

    #[test]
    fn probe_due_respects_period() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 1, 1); // period 1s
        let mut rt = ProbeRuntime::new(now);
        // First run: due (last_run None).
        assert!(rt.is_due(&spec, now));
        rt.last_run = Some(now);
        // Immediately after: not due (period 1s not elapsed).
        assert!(!rt.is_due(&spec, now));
        // After the period: due again.
        assert!(rt.is_due(&spec, now + Duration::from_secs(1)));
    }

    #[test]
    fn next_due_in_is_zero_before_first_run() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 1, 1);
        let rt = ProbeRuntime::new(now);
        assert_eq!(rt.next_due_in(&spec, now), Duration::ZERO);
    }

    #[test]
    fn next_due_in_counts_down_after_a_run() {
        let now = t0();
        let spec = exec_spec(ProbeKind::Readiness, 1, 1); // period 1s
        let mut rt = ProbeRuntime::new(now);
        rt.last_run = Some(now);
        // Half a period elapsed → ~500ms remaining.
        let remaining = rt.next_due_in(&spec, now + Duration::from_millis(500));
        assert!(remaining <= Duration::from_millis(500));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn t0() -> Instant {
        Instant::now()
    }

    fn readiness_spec(success_threshold: u32, failure_threshold: u32) -> ProbeSpec {
        ProbeSpec {
            kind: ProbeKind::Readiness,
            handler: ProbeHandler::Exec {
                command: vec!["true".into()],
            },
            timing: ProbeTiming {
                initial_delay: Duration::ZERO,
                period: Duration::from_secs(1),
                timeout: Duration::from_secs(1),
                success_threshold,
                failure_threshold,
            },
        }
    }

    fn liveness_spec(failure_threshold: u32) -> ProbeSpec {
        ProbeSpec {
            kind: ProbeKind::Liveness,
            handler: ProbeHandler::Exec {
                command: vec!["true".into()],
            },
            timing: ProbeTiming {
                initial_delay: Duration::ZERO,
                period: Duration::from_secs(1),
                timeout: Duration::from_secs(1),
                success_threshold: 1,
                failure_threshold,
            },
        }
    }

    proptest! {
        /// Readiness: a run of exactly `success_threshold` consecutive
        /// successes (with no intervening failure) ALWAYS flips ready true by
        /// the last one, and never before. Threshold honored exactly.
        #[test]
        fn readiness_threshold_honored_exactly(
            threshold in 1u32..6,
        ) {
            let now = t0();
            let spec = readiness_spec(threshold, 3);
            let mut rt = ProbeRuntime::new(now);
            for i in 1..=threshold {
                let v = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
                if i < threshold {
                    prop_assert!(!v.ready, "ready before threshold at i={i}");
                } else {
                    prop_assert!(v.ready, "ready at threshold i={i}");
                }
            }
        }

        /// Liveness: a run of exactly `failure_threshold` consecutive failures
        /// sets needs_restart on the last one, never before.
        #[test]
        fn liveness_failure_threshold_honored_exactly(
            threshold in 1u32..6,
        ) {
            let now = t0();
            let spec = liveness_spec(threshold);
            let mut rt = ProbeRuntime::new(now);
            for i in 1..=threshold {
                let v = fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now);
                if i < threshold {
                    prop_assert!(!v.needs_restart, "restart before threshold at i={i}");
                } else {
                    prop_assert!(v.needs_restart, "restart at threshold i={i}");
                }
            }
        }

        /// Counters are monotone within a run of identical observations: a
        /// success run never decreases consecutive_successes; the failure
        /// counter is zero throughout.
        #[test]
        fn success_run_monotone_counters(
            n in 1usize..10,
        ) {
            let now = t0();
            let spec = readiness_spec(100, 100); // never trips
            let mut rt = ProbeRuntime::new(now);
            let mut prev = 0u32;
            for _ in 0..n {
                let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
                prop_assert!(rt.consecutive_successes >= prev);
                prop_assert_eq!(rt.consecutive_failures, 0);
                prev = rt.consecutive_successes;
            }
        }

        /// A single intervening failure ALWAYS resets the success counter to 0.
        #[test]
        fn failure_resets_success_counter(
            pre in 1usize..6,
        ) {
            let now = t0();
            let spec = readiness_spec(100, 100);
            let mut rt = ProbeRuntime::new(now);
            for _ in 0..pre {
                let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
            }
            prop_assert!(rt.consecutive_successes >= 1);
            let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now);
            prop_assert_eq!(rt.consecutive_successes, 0);
        }

        /// The behavior-preserving lemma, proptest form: with no readiness
        /// probe, the aggregate effective_ready ALWAYS equals is_running,
        /// regardless of the (irrelevant) readiness_ready input.
        #[test]
        fn aggregate_no_readiness_equals_is_running(
            is_running in any::<bool>(),
            readiness_ready in any::<bool>(),
            startup_done in any::<bool>(),
        ) {
            // No startup probe, no readiness probe.
            let (ready, _) = aggregate_container_readiness(
                startup_done, readiness_ready, false, false, is_running,
            );
            prop_assert_eq!(ready, is_running);
        }

        /// During the startup window (has_startup + !startup_done), readiness
        /// is ALWAYS false and liveness restart ALWAYS suppressed, regardless
        /// of the readiness/running inputs.
        #[test]
        fn aggregate_startup_window_gates_everything(
            readiness_ready in any::<bool>(),
            has_readiness in any::<bool>(),
            is_running in any::<bool>(),
        ) {
            let (ready, may_restart) = aggregate_container_readiness(
                false, readiness_ready, true, has_readiness, is_running,
            );
            prop_assert!(!ready, "startup window must force not-ready");
            prop_assert!(!may_restart, "startup window must suppress liveness");
        }
    }
}
