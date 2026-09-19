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
//!     PURE functions that fold a [`ProbeObservation`] (`Success`/`Failure`/
//!     `Blind`, already reduced from the exec exit-code / http status / tcp
//!     connect by the I/O shell) + the per-probe [`ProbeRuntime`] threshold
//!     counters into a [`ProbeVerdict`] `(ready, trip, startup_done)`. No I/O,
//!     no podman, no socket — so the WHOLE probe-verdict logic is unit-testable
//!     (and proptest-able) against mocks with zero container runtime.
//!
//! ## A restart needs an observed failure
//!
//! A probe run has THREE outcomes, not two. `Success` and `Failure` are
//! answers from the workload. [`ProbeObservation::Blind`] is the absence of an
//! answer — no address to dial, a runtime that could not run the exec, a
//! request the prober could not form — and it says nothing about the workload.
//! Upstream's prober worker handles it the same way ("prober error, throw away
//! the result"): the run is stamped, neither counter moves, and the latched
//! verdict stands.
//!
//! The restart decision is a [`ProbeTrip`], not a `bool`. Its fields are
//! private and its only constructor lives in the private `trip` submodule,
//! inside the one function that also counts an observed failure — so nothing
//! outside that submodule (the kubelet included) can mint a restart, and
//! minting one IS counting a failure. That part is a compile error, not a
//! convention. That the Blind arm never calls it is pinned by tests, which is
//! a gate, not a type.
//!
//! The kubelet's `reconcile_running` ([`crate::kubelet`]) is the I/O shell: it
//! decides which probes are *due* (period + initialDelay), runs each handler
//! through the [`run_handler`] wrapper (the SOLE place that touches a Fake),
//! folds the observation, then aggregates the per-container effective
//! readiness + restart decision.
//!
//! ## No silent wrong answers
//!
//! Every parse rejection is a typed error, and every unimplemented surface
//! (grpc) is one — never a fake `Success`. There is no `todo!()` /
//! `panic!()` / placeholder `Ok` in any production path.
//!
//! A port that does not resolve is NOT a parse rejection. Upstream's API
//! validation checks only a port name's syntax, so a pod naming a port its
//! container does not declare is admitted and run; its prober then fails to
//! resolve the port on every run and throws the run away, freezing the probe
//! at its initial value (liveness never restarts, readiness never Ready,
//! startup never started). Here the unresolved port is kept on the handler
//! as an [`UnresolvablePort`] and every run of it is
//! [`BlindCause::UnresolvablePort`], which folds to the same frozen state.
//!
//! ## One run is up to three attempts
//!
//! [`run_handler`] retries an attempt that observed nothing, up to three
//! attempts in one run, as upstream's `runProbeWithRetries` does
//! (`maxProbeRetries = 3`). An answer — pass or fail — is never retried: a
//! failure retried until it passed would be a pass the workload never gave.
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

use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backend::{ContainerRuntime, HttpProbeTarget, NetProber, ProbeIoError, TcpProbeTarget};

pub use trip::{ProbeTrip, TripKind};

// =====================================================================
// Typed border
// =====================================================================

/// Which verdict a probe feeds. Closed enum mirroring the three K8s probe
/// fields (`livenessProbe`, `readinessProbe`, `startupProbe`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ProbeKind {
    /// Drives container restart: OBSERVED failures past `failureThreshold` ⇒
    /// a [`ProbeTrip`].
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

impl ProbeKind {
    /// Every kind, in the order the kubelet runs them: startup first, since
    /// it gates the other two.
    pub const ALL: [ProbeKind; 3] = [
        ProbeKind::Startup,
        ProbeKind::Readiness,
        ProbeKind::Liveness,
    ];

    /// The lower-case name upstream uses in its probe events (`liveness`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ProbeKind::Liveness => "liveness",
            ProbeKind::Readiness => "readiness",
            ProbeKind::Startup => "startup",
        }
    }

    /// The container field this probe is declared under (`livenessProbe`).
    #[must_use]
    pub fn field(self) -> &'static str {
        match self {
            ProbeKind::Liveness => "livenessProbe",
            ProbeKind::Readiness => "readinessProbe",
            ProbeKind::Startup => "startupProbe",
        }
    }

    /// Whether a probe of this kind runs on a RUNNING container whose
    /// started state is `started` (see [`container_started`]).
    ///
    /// Upstream's worker (worker.go:283-294): liveness and readiness are
    /// skipped until the container has started, and startup is skipped once
    /// it has. So a startup probe that already passed can never restart the
    /// container later, and a liveness probe cannot bank failures during the
    /// startup window that would trip it on its first run after it.
    #[must_use]
    pub fn may_run(self, started: bool) -> bool {
        match self {
            ProbeKind::Startup => !started,
            ProbeKind::Liveness | ProbeKind::Readiness => started,
        }
    }
}

/// Whether a container counts as STARTED (upstream `isContainerStarted`,
/// prober_manager.go:270-287): it is running, and it either has no startup
/// probe or that probe has passed. A startup probe that has not run, is below
/// its threshold, or tripped all leave the container not started.
#[must_use]
pub fn container_started(is_running: bool, startup: Option<&ProbeRuntime>) -> bool {
    is_running && startup.is_none_or(|rt| rt.gate_satisfied)
}

impl fmt::Display for ProbeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
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

/// A resolved probe port: `1..=65535`. K8s `port` is an `IntOrString` (an
/// integer or a named container port); the parser resolves a name against
/// `spec.containers[i].ports[].name` at parse time. Port 0 has no value of
/// this type (upstream: `port > 0 && port < 65536`, probe/util.go:43).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbePort(NonZeroU16);

impl ProbePort {
    /// `None` for port 0, the one `u16` that is not a port.
    #[must_use]
    pub fn new(port: u16) -> Option<Self> {
        NonZeroU16::new(port).map(Self)
    }

    /// The port number.
    #[must_use]
    pub fn get(self) -> u16 {
        self.0.get()
    }
}

/// Why a network probe's `port` names no port to dial.
///
/// Not a parse error: the pod is admitted and runs, and every run of the
/// probe is [`BlindCause::UnresolvablePort`] — upstream resolves the port on
/// each run, fails, and throws the run away (probe/util.go:27-47,
/// worker.go:297-301). A container's ports cannot change while it exists, so
/// resolving once at parse time gives the answer every run would.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum UnresolvablePort {
    /// A name the container's `ports[].name` does not declare.
    #[error("the container declares no port named {name:?}")]
    NoSuchName {
        /// The name the probe asked for.
        name: String,
    },
    /// A number, given or looked up, outside `1..=65535`. The text is
    /// upstream's (probe/util.go:46).
    #[error("invalid port number: {number}")]
    OutOfRange {
        /// The number.
        number: i64,
    },
    /// The handler has no `port` at all.
    #[error("the probe declares no port")]
    Missing,
    /// A `port` that is neither an integer nor a string.
    #[error("the probe's port is neither an integer nor a string")]
    NotIntOrString,
}

/// A probe's action. Closed enum — exactly one handler per probe. A K8s
/// `Probe` with NO action is a [`ProbeParseError::NoHandler`]; a `grpc`
/// action is a [`ProbeParseError::UnsupportedHandler`] (documented deferral).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeHandler {
    /// `exec` — run an argv inside the container; exit 0 = success.
    Exec {
        /// The argv to exec (no shell), `$(VAR)` references already expanded
        /// (see [`ProbeSpec::from_container`]). Empty argv is a parse-time
        /// error.
        command: Vec<String>,
    },
    /// `httpGet` — issue an HTTP GET against `host`, else the pod IP; a
    /// status in `200..400` = success.
    HttpGet {
        /// Request path (defaults to `/` when absent in the manifest).
        path: String,
        /// Target port, resolved at parse time. `Err` makes every run Blind.
        port: Result<ProbePort, UnresolvablePort>,
        /// URL scheme.
        scheme: HttpScheme,
        /// The host to dial instead of the pod IP (`httpGet.host`). It is the
        /// URL's host, not a header: a `Host` HEADER comes from `headers`.
        host: Option<String>,
        /// Custom request headers (`httpHeaders`), in manifest order.
        headers: Vec<(String, String)>,
    },
    /// `tcpSocket` — open a TCP connection to `host`, else the pod IP;
    /// connect-ok = success.
    TcpSocket {
        /// Target port, resolved at parse time. `Err` makes every run Blind.
        port: Result<ProbePort, UnresolvablePort>,
        /// The host to dial instead of the pod IP (`tcpSocket.host`).
        host: Option<String>,
    },
}

/// Probe timing knobs, with K8s defaults applied at parse. A zero (or
/// negative) value is UNSET and takes the default, as upstream's
/// `SetDefaults_Probe` does (pkg/apis/core/v1/defaults.go:236-249).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeTiming {
    /// `initialDelaySeconds` — wait this long after container start before the
    /// FIRST probe run. Default 0.
    pub initial_delay: Duration,
    /// `periodSeconds` — how often to probe. Default 10s.
    pub period: Duration,
    /// `timeoutSeconds` — per-run I/O timeout. Default 1s.
    pub timeout: Duration,
    /// `successThreshold` — consecutive successes to flip the gate. Default 1;
    /// FORCED to 1 for liveness/startup (K8s rule).
    pub success_threshold: u32,
    /// `failureThreshold` — consecutive failures to trip. Default 3.
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
///
/// Upstream's API validation rejects the first two before any kubelet sees
/// them; engenho's kubelet refuses the pod instead. A port that does not
/// resolve is deliberately NOT here: see [`UnresolvablePort`].
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
}

impl ProbeSpec {
    /// Parse the probe of `kind` declared on one `spec.containers[i]` JSON
    /// object. `Ok(None)` when the container declares no such probe (absent
    /// or `null`).
    ///
    /// This is the whole of what the kubelet parses, in one place: the
    /// named-port table and the environment come from the same container, so
    /// no caller can pair a probe with another container's ports.
    ///
    /// An exec probe's argv has its `$(VAR)` references expanded the way
    /// upstream's prober does (`ExpandContainerCommandOnlyStatic`,
    /// prober.go:154): against each env entry's LITERAL `value` — an entry
    /// set by `valueFrom` reads as the empty string, and a value is not
    /// itself expanded first. A reference to an undeclared variable stays as
    /// written.
    ///
    /// # Errors
    ///
    /// [`ProbeParseError`] on no-handler, empty-exec or grpc.
    pub fn from_container(
        kind: ProbeKind,
        container: &Value,
    ) -> Result<Option<Self>, ProbeParseError> {
        let probe = match container.get(kind.field()) {
            None | Some(Value::Null) => return Ok(None),
            Some(probe) => probe,
        };
        let mut spec = Self::from_k8s(kind, probe, &container_ports(container))?;
        if let ProbeHandler::Exec { command } = &mut spec.handler {
            let env = literal_env(container);
            for arg in command.iter_mut() {
                *arg = crate::env_ref::expand_env_refs(arg, &env);
            }
        }
        Ok(Some(spec))
    }

    /// Parse a raw-JSON `Probe` object (the kubelet reads pods as
    /// [`serde_json::Value`] end to end) of `kind` into the typed border,
    /// applying K8s defaults and the liveness/startup `successThreshold==1`
    /// rule, resolving the port against `container_ports`
    /// (`spec.containers[i].ports[]`), and rejecting no-handler / grpc with
    /// typed errors.
    ///
    /// `container_ports` is the slice of `(name, number)` pairs from the
    /// container's `ports[]` — used only to resolve a NAMED probe port. A
    /// port that does not resolve is kept on the handler as an
    /// [`UnresolvablePort`], never a parse error.
    ///
    /// The exec argv is taken verbatim; [`ProbeSpec::from_container`] is the
    /// path that also expands its `$(VAR)` references.
    ///
    /// # Errors
    ///
    /// [`ProbeParseError`] on no-handler, empty-exec or grpc. Never a silent
    /// skip.
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
            let port = resolve_port(http.get("port"), container_ports);
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
            let port = resolve_port(tcp.get("port"), container_ports);
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

    /// Fold the raw-JSON timing fields into a [`ProbeTiming`]: a field that is
    /// absent, zero or negative takes its K8s default, and liveness/startup
    /// force `successThreshold` to 1.
    ///
    /// Zero is UNSET, not a floor to clamp to: upstream defaults the zero
    /// value (`SetDefaults_Probe`, defaults.go:236-249), so an explicit
    /// `periodSeconds: 0` probes every 10s and `failureThreshold: 0` trips on
    /// the third failure — not every second and on the first. A negative
    /// value is rejected by upstream's validation and never reaches a
    /// kubelet; here it reads as unset too.
    fn parse_timing(kind: ProbeKind, probe: &Value) -> ProbeTiming {
        let defaults = ProbeTiming::k8s_defaults();
        // A positive integer, or None for absent / zero / negative / not an int.
        let set = |key: &str| -> Option<u64> {
            probe
                .get(key)
                .and_then(Value::as_i64)
                .and_then(|v| u64::try_from(v).ok())
                .filter(|&v| v > 0)
        };
        let secs = |key: &str, default: Duration| set(key).map_or(default, Duration::from_secs);
        let count = |key: &str, default: u32| {
            set(key)
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(default)
        };
        let mut t = ProbeTiming {
            initial_delay: secs("initialDelaySeconds", defaults.initial_delay),
            period: secs("periodSeconds", defaults.period),
            timeout: secs("timeoutSeconds", defaults.timeout),
            success_threshold: count("successThreshold", defaults.success_threshold),
            failure_threshold: count("failureThreshold", defaults.failure_threshold),
        };
        // K8s rule: successThreshold MUST be 1 for liveness + startup.
        if matches!(kind, ProbeKind::Liveness | ProbeKind::Startup) {
            t.success_threshold = 1;
        }
        t
    }
}

/// The container's named ports as `(name, number)` pairs. An entry without a
/// name cannot be referred to by one, and a number outside `u16` is not a
/// port; both are left out, so a probe naming them resolves to
/// [`UnresolvablePort`] rather than to a wrong port.
fn container_ports(container: &Value) -> Vec<(String, u16)> {
    container
        .get("ports")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|p| {
                    let name = p.get("name").and_then(Value::as_str)?.to_string();
                    let number = p.get("containerPort").and_then(Value::as_i64)?;
                    u16::try_from(number).ok().map(|n| (name, n))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Upstream's `EnvVarsToMap`: each env entry's literal `value` by name, the
/// empty string for an entry set by `valueFrom`, a later entry winning.
fn literal_env(container: &Value) -> BTreeMap<String, String> {
    container
        .get("env")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| {
                    let name = e.get("name").and_then(Value::as_str)?.to_string();
                    let value = e.get("value").and_then(Value::as_str).unwrap_or_default();
                    Some((name, value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve a raw-JSON `port` (integer or string) against the container's
/// `ports[]`, in upstream's order (probe/util.go:27-47): a string is looked
/// up as a port NAME first, then read as a number written as a string
/// (`"8080"`); the number must then be in `1..=65535`.
fn resolve_port(
    port: Option<&Value>,
    container_ports: &[(String, u16)],
) -> Result<ProbePort, UnresolvablePort> {
    let number = match port {
        None | Some(Value::Null) => return Err(UnresolvablePort::Missing),
        Some(Value::String(name)) => match container_ports.iter().find(|(n, _)| n == name) {
            Some((_, number)) => i64::from(*number),
            None => name
                .parse::<i64>()
                .map_err(|_| UnresolvablePort::NoSuchName { name: name.clone() })?,
        },
        Some(other) => other.as_i64().ok_or(UnresolvablePort::NotIntOrString)?,
    };
    u16::try_from(number)
        .ok()
        .and_then(ProbePort::new)
        .ok_or(UnresolvablePort::OutOfRange { number })
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
    /// The current run of [`ProbeObservation::Blind`] results, `None` once
    /// the probe observes the workload again (either way). Blind results move
    /// neither counter above; this is where they are counted instead.
    pub blind: Option<BlindStreak>,
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
            blind: None,
        }
    }

    /// How many runs in a row observed nothing (`0` when the last run
    /// observed the workload).
    #[must_use]
    pub fn consecutive_blind(&self) -> u32 {
        self.blind.map_or(0, |b| b.consecutive.get())
    }

    /// The cause, once this probe has been blind for as many consecutive runs
    /// as it would have taken a FAILING probe to trip (`failureThreshold`).
    ///
    /// Derived from the probe's own threshold rather than a second constant:
    /// the question an operator is asking is "would this have acted by now if
    /// it could see?", and the threshold is that probe's answer to it.
    #[must_use]
    pub fn sustained_blindness(&self, spec: &ProbeSpec) -> Option<BlindCause> {
        self.blind
            .filter(|b| b.consecutive.get() >= spec.timing.failure_threshold)
            .map(|b| b.cause)
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

/// Why a probe run observed NOTHING about the workload.
///
/// Not a failed check — the check was never put. Each arm names what was
/// missing, so the Warning an operator reads says which side is broken: the
/// pod's address, the container runtime, or the probe definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlindCause {
    /// An httpGet/tcpSocket probe had nothing to dial: the backend reported
    /// no pod IP.
    NoTargetAddress,
    /// The container runtime, or the transport to it, could not run an exec
    /// probe. The command never ran, so its exit status is unknown.
    RuntimeUnavailable,
    /// The prober could not form the request (an unparsable URL, a client it
    /// could not build, a header it could not encode). Nothing was sent.
    ProberSetup,
    /// The probe's `port` names no port to dial (see [`UnresolvablePort`]).
    /// It cannot change while the container exists, so every run is blind
    /// and the probe stays at its initial value, as upstream's does.
    UnresolvablePort,
}

impl BlindCause {
    /// The `reason` a pod condition or event carries (upstream CamelCase).
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            BlindCause::NoTargetAddress => "NoTargetAddress",
            BlindCause::RuntimeUnavailable => "RuntimeUnavailable",
            BlindCause::ProberSetup => "ProberSetup",
            BlindCause::UnresolvablePort => "UnresolvablePort",
        }
    }
}

impl fmt::Display for BlindCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BlindCause::NoTargetAddress => "the pod reports no address to dial",
            BlindCause::RuntimeUnavailable => "the container runtime could not run the probe",
            BlindCause::ProberSetup => "the prober could not form the request",
            BlindCause::UnresolvablePort => {
                "the probe's port does not resolve to a port of the container"
            }
        })
    }
}

/// A run of consecutive [`ProbeObservation::Blind`] results on one probe.
///
/// `consecutive` is a `NonZeroU32` so "a streak of zero" — which would read as
/// blind and not-blind at once — cannot be built; not blind is `None` on
/// [`ProbeRuntime::blind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlindStreak {
    /// The cause of the most recent Blind result in the run.
    pub cause: BlindCause,
    /// How many Blind results in a row.
    pub consecutive: NonZeroU32,
}

impl BlindStreak {
    /// The streak after one more Blind result with `cause`.
    #[must_use]
    pub fn extend(previous: Option<BlindStreak>, cause: BlindCause) -> Self {
        Self {
            cause,
            consecutive: previous.map_or(NonZeroU32::MIN, |p| p.consecutive.saturating_add(1)),
        }
    }
}

/// What an operator reads when a probe goes blind — the Warning event's
/// message and the `ProbeBlind` pod condition's message. One `Display`, so
/// the two cannot drift, and no count in it, so a condition that carries it
/// renders byte-identically on every tick the cause is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlindNotice<'a> {
    /// The container the probe belongs to.
    pub container: &'a str,
    /// Which probe.
    pub kind: ProbeKind,
    /// What it could not do.
    pub cause: BlindCause,
}

impl fmt::Display for BlindNotice<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} probe of container {} observed nothing: {}; the result was discarded \
             and the container will not be restarted on it",
            self.kind, self.container, self.cause
        )
    }
}

/// The reduced result of running ONE probe handler — the runtime/net layer
/// collapsed exit-code / http-status / connect-result (and any timeout / I/O
/// error) into one of these three before the fold sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeObservation {
    /// The probe passed this run (exec exit 0 / http 2xx-3xx / tcp connect ok).
    Success,
    /// The workload answered and the answer was no: a non-zero exit (127
    /// included — the command was looked for inside the container), a status
    /// outside 200..400, a refused or reset connection, a timeout.
    Failure,
    /// Nothing was observed: see [`BlindCause`]. Moves no counter and can
    /// never produce a [`ProbeTrip`].
    Blind(BlindCause),
}

/// The per-probe verdict the fold produces. Only the fields matching the
/// probe's [`ProbeKind`] are meaningful; the others stay at their identity
/// (`false` / `None`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeVerdict {
    /// Readiness verdict (meaningful for [`ProbeKind::Readiness`]).
    pub ready: bool,
    /// The restart request (liveness / startup only). `Some` only when THIS
    /// fold counted an observed failure at or past `failureThreshold`; there
    /// is no other way to build one.
    pub trip: Option<ProbeTrip>,
    /// Startup-gate verdict (meaningful for [`ProbeKind::Startup`]).
    pub startup_done: bool,
    /// `Some` only on the fold that STARTED a blind streak, so the caller
    /// emits one Warning per streak rather than one per period.
    pub entered_blind: Option<BlindCause>,
}

/// The restart witness. A private submodule because Rust privacy is per
/// module: [`ProbeTrip`]'s fields are private HERE, so the only code that can
/// build one is this module's `record_observed_failure` — which is also the
/// only code that counts a failure.
mod trip {
    use std::fmt;

    use super::{ProbeKind, ProbeRuntime, ProbeSpec};

    /// Which probe tripped. Readiness has no arm: it never restarts anything.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum TripKind {
        /// `livenessProbe`.
        Liveness,
        /// `startupProbe`.
        Startup,
    }

    impl fmt::Display for TripKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                TripKind::Liveness => "liveness",
                TripKind::Startup => "startup",
            })
        }
    }

    /// Evidence that a liveness or startup probe OBSERVED `failureThreshold`
    /// consecutive failures — the only thing that may restart a container on
    /// a probe's say-so.
    ///
    /// ```compile_fail,E0451
    /// // Private fields: no code outside `probe::trip` can mint a restart.
    /// let _ = engenho_kubelet::ProbeTrip {
    ///     kind: engenho_kubelet::TripKind::Liveness,
    ///     consecutive_failures: 3,
    /// };
    /// ```
    ///
    /// The same path compiles when only READ, so the snippet above fails on
    /// privacy and not on a typo:
    ///
    /// ```
    /// fn restarts_on(t: engenho_kubelet::ProbeTrip) -> (engenho_kubelet::TripKind, u32) {
    ///     (t.kind(), t.consecutive_failures())
    /// }
    /// ```
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct ProbeTrip {
        kind: TripKind,
        consecutive_failures: u32,
    }

    impl ProbeTrip {
        /// Which probe tripped.
        #[must_use]
        pub fn kind(self) -> TripKind {
            self.kind
        }

        /// How many consecutive observed failures it took.
        #[must_use]
        pub fn consecutive_failures(self) -> u32 {
            self.consecutive_failures
        }
    }

    impl fmt::Display for ProbeTrip {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{} probe failed {} consecutive times",
                self.kind, self.consecutive_failures
            )
        }
    }

    /// Count ONE observed failure and return the trip if it crossed the
    /// threshold. The sole constructor of [`ProbeTrip`]: calling it is
    /// counting a failure, so a trip without a counted failure cannot exist.
    ///
    /// Readiness crossing the threshold clears its gate and returns `None` —
    /// an unready container is taken out of rotation, never restarted.
    pub(super) fn record_observed_failure(
        spec: &ProbeSpec,
        rt: &mut ProbeRuntime,
    ) -> Option<ProbeTrip> {
        rt.consecutive_failures = rt.consecutive_failures.saturating_add(1);
        rt.consecutive_successes = 0;
        if rt.consecutive_failures < spec.timing.failure_threshold {
            return None;
        }
        let kind = match spec.kind {
            ProbeKind::Readiness => {
                rt.gate_satisfied = false;
                return None;
            }
            ProbeKind::Liveness => TripKind::Liveness,
            ProbeKind::Startup => TripKind::Startup,
        };
        Some(ProbeTrip {
            kind,
            consecutive_failures: rt.consecutive_failures,
        })
    }
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
///     ⇒ `ready=false`; liveness/startup ⇒ `trip = Some(..)`).
///   * **Blind** → neither counter moves, the blind streak grows, and the
///     latched verdict is returned unchanged. Upstream's prober worker does
///     the same with a probe error: "throw away the result".
///
/// Success and Failure both end a blind streak: the probe saw the workload.
///
/// `rt.gate_satisfied` is the latched gate (readiness=ready / startup=done);
/// the verdict mirrors it for readiness/startup so a steady-passing probe
/// keeps reporting `ready=true` / `startup_done=true` between threshold edges.
///
/// `now` stamps `rt.last_run` (the period reference) for every outcome,
/// Blind included, so a blind probe keeps its cadence instead of retrying
/// every tick — the caller has already decided the probe is due.
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
            rt.blind = None;
            rt.consecutive_successes = rt.consecutive_successes.saturating_add(1);
            rt.consecutive_failures = 0;
            if rt.consecutive_successes >= spec.timing.success_threshold {
                rt.gate_satisfied = true;
            }
        }
        ProbeObservation::Failure => {
            rt.blind = None;
            verdict.trip = trip::record_observed_failure(spec, rt);
        }
        ProbeObservation::Blind(cause) => {
            verdict.entered_blind = rt.blind.is_none().then_some(cause);
            rt.blind = Some(BlindStreak::extend(rt.blind, cause));
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
/// never boots IS restarted); that is the startup probe's own `trip`
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

/// Attempts one probe run may make. Upstream's `maxProbeRetries`
/// (prober.go:41, 134-147): an attempt that ERRORS is tried again inside the
/// same run, up to three attempts in all; an answer ends the run.
const PROBE_ATTEMPTS: u32 = 3;

/// Run ONE probe against the live runtime + net seams, reducing the result to
/// a [`ProbeObservation`]. The SOLE place that touches the runtime `exec` or
/// the [`NetProber`] — so the fold tests never need real exec / http / tcp.
/// Every attempt is bounded by `spec.timing.timeout`. Nothing here aborts the
/// tick: a failing or blind probe is a normal, expected signal.
///
/// One run is up to three attempts (`PROBE_ATTEMPTS`). An attempt that observed
/// nothing ([`ProbeObservation::Blind`]) is tried again at once, so a
/// transient runtime error followed by a pass is a pass in one run, not a
/// discarded run and a wait of a whole period. `Success` and `Failure` are
/// never retried: they are the workload's answer, and retrying a failure
/// until it passed would record a pass the workload never gave. Three blind
/// attempts return the last one's cause.
///
/// The split between Failure and Blind is "did the workload answer?":
///
/// | handler | Failure (the workload said no) | Blind (nothing was asked) |
/// |---|---|---|
/// | exec | non-zero exit, 127 included; timeout | the runtime or its transport returned an error |
/// | httpGet | status outside 200..400; refused, reset, TLS, malformed; timeout | no address; unresolvable port; the request could not be formed |
/// | tcpSocket | refused; timeout | no address; unresolvable port |
///
/// `container_id` is the exec target (container-scoped); `pod_ip` is the
/// http/tcp target (network-scoped) unless the probe names its own `host`.
pub async fn run_handler(
    spec: &ProbeSpec,
    runtime: &dyn ContainerRuntime,
    net_prober: &dyn NetProber,
    container_id: &str,
    pod_ip: Option<&str>,
) -> ProbeObservation {
    let mut attempt = 1;
    loop {
        match run_attempt(spec, runtime, net_prober, container_id, pod_ip).await {
            ProbeObservation::Blind(cause) if attempt < PROBE_ATTEMPTS => {
                tracing::debug!(container_id, attempt, %cause, "probe attempt observed nothing; retrying");
                attempt += 1;
            }
            observed_or_last => return observed_or_last,
        }
    }
}

/// How the status an httpGet probe was answered with is judged: `200..400`
/// passes, anything else fails (upstream probe/http/http.go:111-122; a 3xx
/// that reaches this point is upstream's "warning", which the kubelet counts
/// as a pass).
#[must_use]
pub fn http_status_observation(status: u16) -> ProbeObservation {
    if (200..400).contains(&status) {
        ProbeObservation::Success
    } else {
        ProbeObservation::Failure
    }
}

/// One attempt of [`run_handler`].
async fn run_attempt(
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
                Ok(Ok(outcome)) if outcome.exit_code == 0 => ProbeObservation::Success,
                // The command ran and exited non-zero. 127 ("not found") is a
                // failure too: the command was looked for INSIDE the container.
                Ok(Ok(_)) => ProbeObservation::Failure,
                // The runtime never ran the command, so there is no exit status
                // to judge. Upstream counts this as a probe error and discards it.
                Ok(Err(e)) => {
                    tracing::debug!(container_id, error = %e, "exec probe could not run");
                    ProbeObservation::Blind(BlindCause::RuntimeUnavailable)
                }
                // Ran past timeoutSeconds: upstream counts a timeout as a failure.
                Err(_elapsed) => ProbeObservation::Failure,
            }
        }
        ProbeHandler::HttpGet {
            path,
            port,
            scheme,
            host,
            headers,
        } => {
            let Ok(port) = port else {
                return ProbeObservation::Blind(BlindCause::UnresolvablePort);
            };
            // No address is not a failed check — nothing could be dialled.
            let Some(dial) = host.as_deref().or(pod_ip) else {
                return ProbeObservation::Blind(BlindCause::NoTargetAddress);
            };
            let target = HttpProbeTarget {
                host: dial.to_string(),
                port: port.get(),
                path: path.clone(),
                scheme: *scheme,
                headers: headers.clone(),
                timeout,
            };
            match tokio::time::timeout(timeout, net_prober.http_get(&target)).await {
                Ok(Ok(status)) => http_status_observation(status),
                Ok(Err(e)) => net_error_observation(&e),
                Err(_elapsed) => ProbeObservation::Failure,
            }
        }
        ProbeHandler::TcpSocket { port, host } => {
            let Ok(port) = port else {
                return ProbeObservation::Blind(BlindCause::UnresolvablePort);
            };
            let Some(dial) = host.as_deref().or(pod_ip) else {
                return ProbeObservation::Blind(BlindCause::NoTargetAddress);
            };
            let target = TcpProbeTarget {
                host: dial.to_string(),
                port: port.get(),
                timeout,
            };
            match tokio::time::timeout(timeout, net_prober.tcp_connect(&target)).await {
                Ok(Ok(())) => ProbeObservation::Success,
                Ok(Err(e)) => net_error_observation(&e),
                Err(_elapsed) => ProbeObservation::Failure,
            }
        }
    }
}

/// Classify a network prober error. Exhaustive with no wildcard, so a new
/// [`ProbeIoError`] variant cannot land without someone deciding whether the
/// workload answered.
fn net_error_observation(e: &ProbeIoError) -> ProbeObservation {
    match e {
        // The request went out and the workload did not answer well — upstream
        // counts every transport error on a SENT probe as a failure.
        ProbeIoError::Connect { .. } | ProbeIoError::Timeout { .. } | ProbeIoError::Io(_) => {
            ProbeObservation::Failure
        }
        // Nothing was sent.
        ProbeIoError::Setup { .. } => {
            tracing::debug!(error = %e, "network probe could not be set up");
            ProbeObservation::Blind(BlindCause::ProberSetup)
        }
    }
}

/// A non-zero port for a test fixture.
#[cfg(test)]
fn port_n(n: u16) -> ProbePort {
    ProbePort::new(n).unwrap_or_else(|| panic!("port {n} is zero"))
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
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_none()
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_none()
        );
        // Third consecutive failure → needs_restart.
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_some()
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
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_none()
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_some()
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
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_none()
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_some()
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

    /// Zero is UNSET, not a floor. This test used to pin `periodSeconds: 0`
    /// to a 1s period and `failureThreshold: 0` to a restart on the FIRST
    /// failure; upstream defaults the zero value (`SetDefaults_Probe`,
    /// defaults.go:236-249), so both take their defaults. Negative values
    /// (rejected by upstream validation) read as unset too.
    #[test]
    fn from_k8s_zero_or_negative_fields_take_the_defaults() {
        for v in [0, -5] {
            let probe = json!({
                "exec": { "command": ["true"] },
                "initialDelaySeconds": v,
                "periodSeconds": v,
                "timeoutSeconds": v,
                "successThreshold": v,
                "failureThreshold": v
            });
            let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
            assert_eq!(
                spec.timing,
                ProbeTiming::k8s_defaults(),
                "fields set to {v}"
            );
            assert_eq!(spec.timing.period, Duration::from_secs(10));
            assert_eq!(spec.timing.failure_threshold, 3);
        }
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
                assert_eq!(port, Ok(port_n(8080)));
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
            ProbeHandler::HttpGet { port, .. } => assert_eq!(port, Ok(port_n(8080))),
            other => panic!("expected HttpGet, got {other:?}"),
        }
    }

    /// A port the container does not declare is NOT a parse error: this test
    /// used to pin it as one, which made the kubelet refuse the whole pod.
    /// Upstream admits and runs it; the probe keeps the reason, and every run
    /// of it is blind (see `unresolvable_port` below).
    #[test]
    fn from_k8s_unresolved_named_port_is_kept_with_its_reason() {
        let probe = json!({ "tcpSocket": { "port": "nope" } });
        let spec = ProbeSpec::from_k8s(ProbeKind::Readiness, &probe, &[]).unwrap();
        assert_eq!(
            spec.handler,
            ProbeHandler::TcpSocket {
                port: Err(UnresolvablePort::NoSuchName {
                    name: "nope".to_string()
                }),
                host: None,
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
                port: Ok(p),
                ..
            } if p.get() == 6379
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
        /// yields a trip on the last one, never before.
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
                    prop_assert!(v.trip.is_none(), "restart before threshold at i={i}");
                } else {
                    prop_assert!(v.trip.is_some(), "restart at threshold i={i}");
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

#[cfg(test)]
mod probe_address {
    use super::*;

    // ── A missing address is not "no opinion" ──────────────────────────
    //
    // `run_handler` USED to map an http/tcp probe with no pod IP to `Failure`.
    // There is nothing to dial, but "could not ask" is not "asked and was told
    // no": it made a backend reporting no address indistinguishable from a
    // broken workload, and a failing startup probe restarts.
    //
    // Since T1.1 it is `Blind(NoTargetAddress)`: the run is discarded, nothing
    // is restarted, and the pod says why (a Warning, then `ProbeBlind`). The
    // probe still can never PASS without an address, so the native backend
    // must still report the loopback — the incident below is why.
    //
    // Measured on ryn 2026-09-18: the native backend returned `pod_ip: None`
    // on the stated reasoning that "inventing one would be worse than
    // reporting none". pangea-operator's startupProbe (30 x 5s) could then
    // never pass, so the kubelet killed a healthy operator every 150s —
    // `restartCount: 14`, `ready: false` — while `curl 127.0.0.1:8080/healthz`
    // answered HTTP 200 in 0.4ms from the same host.
    //
    // Both directions are pinned, against the SAME healthy prober, so the
    // difference is the address and nothing else.

    use crate::backend::{FakeBackend, FakeNetProber};

    fn http_spec() -> ProbeSpec {
        ProbeSpec {
            kind: ProbeKind::Startup,
            handler: ProbeHandler::HttpGet {
                path: "/healthz".into(),
                port: Ok(port_n(8080)),
                scheme: HttpScheme::Http,
                host: None,
                headers: Vec::new(),
            },
            timing: ProbeTiming {
                initial_delay: Duration::ZERO,
                period: Duration::from_secs(5),
                timeout: Duration::from_secs(1),
                success_threshold: 1,
                failure_threshold: 30,
            },
        }
    }

    #[tokio::test]
    async fn an_http_probe_with_an_address_reaches_a_healthy_workload() {
        let runtime = FakeBackend::new();
        let net = FakeNetProber::new();
        net.seed_http("127.0.0.1", 8080, "/healthz", [200]).await;

        let obs = run_handler(&http_spec(), &runtime, &net, "cid", Some("127.0.0.1")).await;

        assert_eq!(obs, ProbeObservation::Success);
    }

    #[tokio::test]
    async fn the_same_healthy_workload_is_blind_not_failing_when_it_reports_no_address() {
        let runtime = FakeBackend::new();
        let net = FakeNetProber::new();
        // Identical seeding: the workload is healthy either way.
        net.seed_http("127.0.0.1", 8080, "/healthz", [200]).await;

        let obs = run_handler(&http_spec(), &runtime, &net, "cid", None).await;

        assert_eq!(
            obs,
            ProbeObservation::Blind(BlindCause::NoTargetAddress),
            "no address means nothing was asked — Blind, never a Failure that \
             counts toward restarting a healthy pod"
        );
    }

    #[tokio::test]
    async fn a_tcp_probe_with_no_address_is_blind_too() {
        let spec = ProbeSpec {
            handler: ProbeHandler::TcpSocket {
                port: Ok(port_n(8080)),
                host: None,
            },
            ..http_spec()
        };
        let net = FakeNetProber::new();
        net.set_default_tcp(true).await;

        let obs = run_handler(&spec, &FakeBackend::new(), &net, "cid", None).await;

        assert_eq!(obs, ProbeObservation::Blind(BlindCause::NoTargetAddress));
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_methods,
    reason = "starts a FakeBackend container to probe; no kubelet in the loop"
)]
mod blind {
    //! T1.1: a restart needs an OBSERVED failure. Blind runs move no counter,
    //! trip nothing, and hand back the verdict that was already latched.
    use super::*;
    use crate::backend::{ContainerSpec, FakeBackend, FakeExecFault, FakeNetProber};
    use crate::{ContainerRuntime, ExecOutcome};

    const NO_ADDRESS: ProbeObservation = ProbeObservation::Blind(BlindCause::NoTargetAddress);

    fn spec(kind: ProbeKind, success_threshold: u32, failure_threshold: u32) -> ProbeSpec {
        ProbeSpec {
            kind,
            handler: ProbeHandler::Exec {
                command: vec!["true".into()],
            },
            timing: ProbeTiming {
                initial_delay: Duration::ZERO,
                period: Duration::from_secs(10),
                timeout: Duration::from_secs(1),
                success_threshold,
                failure_threshold,
            },
        }
    }

    // ── the fold ─────────────────────────────────────────────────────────

    #[test]
    fn a_thousand_blind_runs_trip_nothing_while_three_failures_do() {
        for (kind, expected) in [
            (ProbeKind::Liveness, TripKind::Liveness),
            (ProbeKind::Startup, TripKind::Startup),
        ] {
            let spec = spec(kind, 1, 3);
            let now = Instant::now();

            let mut rt = ProbeRuntime::new(now);
            for i in 0..1000 {
                let v = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
                assert_eq!(v.trip, None, "{kind}: blind run {i} tripped a restart");
            }
            assert_eq!(
                rt.consecutive_failures, 0,
                "{kind}: blind counted as failure"
            );
            assert_eq!(rt.consecutive_blind(), 1000);

            // Control: the same spec and three OBSERVED failures do trip, on
            // the third and not before.
            let mut rt = ProbeRuntime::new(now);
            let trips: Vec<Option<ProbeTrip>> = (0..3)
                .map(|_| {
                    fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now).trip
                })
                .collect();
            assert_eq!(trips[..2], [None, None], "{kind}: tripped early");
            let trip = trips[2].unwrap_or_else(|| panic!("{kind}: third failure must trip"));
            assert_eq!(trip.kind(), expected);
            assert_eq!(trip.consecutive_failures(), 3);
        }
    }

    #[test]
    fn blind_runs_neither_advance_nor_reset_the_failure_streak() {
        let spec = spec(ProbeKind::Liveness, 1, 3);
        let now = Instant::now();
        let mut rt = ProbeRuntime::new(now);

        for _ in 0..2 {
            let v = fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now);
            assert_eq!(v.trip, None);
        }
        for _ in 0..1000 {
            let v = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
            assert_eq!(
                v.trip, None,
                "blind runs must not complete a failure streak"
            );
        }
        assert_eq!(
            rt.consecutive_failures, 2,
            "blind runs must not reset it either"
        );
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Failure, now)
                .trip
                .is_some(),
            "the third OBSERVED failure trips, however many blind runs sat between"
        );
    }

    #[test]
    fn blind_runs_neither_advance_nor_reset_the_success_streak() {
        let spec = spec(ProbeKind::Readiness, 2, 3);
        let now = Instant::now();
        let mut rt = ProbeRuntime::new(now);

        assert!(!fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now).ready);
        for _ in 0..5 {
            assert!(
                !fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now).ready,
                "a blind run is not a success"
            );
        }
        assert_eq!(rt.consecutive_successes, 1);
        assert!(
            fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now).ready,
            "the second OBSERVED success latches ready"
        );
    }

    #[test]
    fn blind_hands_back_the_latched_verdict() {
        let now = Instant::now();

        // A ready container stays ready while its probe is blind…
        let readiness = spec(ProbeKind::Readiness, 1, 3);
        let mut rt = ProbeRuntime::new(now);
        assert!(fold_probe_observation(&readiness, &mut rt, ProbeObservation::Success, now).ready);
        for _ in 0..1000 {
            assert!(fold_probe_observation(&readiness, &mut rt, NO_ADDRESS, now).ready);
        }
        // …and an unready one stays unready.
        let mut rt = ProbeRuntime::new(now);
        for _ in 0..1000 {
            assert!(!fold_probe_observation(&readiness, &mut rt, NO_ADDRESS, now).ready);
        }

        // A completed startup stays complete.
        let startup = spec(ProbeKind::Startup, 1, 3);
        let mut rt = ProbeRuntime::new(now);
        assert!(
            fold_probe_observation(&startup, &mut rt, ProbeObservation::Success, now).startup_done
        );
        let v = fold_probe_observation(&startup, &mut rt, NO_ADDRESS, now);
        assert!(v.startup_done);
        assert_eq!(v.trip, None);
    }

    #[test]
    fn a_blind_run_keeps_the_probe_on_its_period() {
        let spec = spec(ProbeKind::Liveness, 1, 3);
        let start = Instant::now();
        let mut rt = ProbeRuntime::new(start);
        assert!(rt.is_due(&spec, start));

        let _ = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, start);

        assert_eq!(rt.last_run, Some(start), "a blind run is still a run");
        assert!(!rt.is_due(&spec, start), "not retried every tick");
        assert!(rt.is_due(&spec, start + spec.timing.period));
    }

    #[test]
    fn one_warning_per_blind_streak() {
        let spec = spec(ProbeKind::Liveness, 1, 3);
        let now = Instant::now();
        let mut rt = ProbeRuntime::new(now);
        let runtime_down = ProbeObservation::Blind(BlindCause::RuntimeUnavailable);

        let first = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
        assert_eq!(first.entered_blind, Some(BlindCause::NoTargetAddress));
        // Still blind, even for a different reason: the same streak.
        let second = fold_probe_observation(&spec, &mut rt, runtime_down, now);
        assert_eq!(second.entered_blind, None);
        assert_eq!(
            rt.blind.map(|b| (b.cause, b.consecutive.get())),
            Some((BlindCause::RuntimeUnavailable, 2))
        );

        // Seeing the workload ends the streak, either way it answers.
        for answer in [ProbeObservation::Success, ProbeObservation::Failure] {
            let _ = fold_probe_observation(&spec, &mut rt, answer, now);
            assert_eq!(rt.blind, None, "{answer:?} must end the blind streak");
            let again = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
            assert_eq!(again.entered_blind, Some(BlindCause::NoTargetAddress));
        }
    }

    #[test]
    fn blindness_is_sustained_at_the_probes_own_failure_threshold() {
        let spec = spec(ProbeKind::Liveness, 1, 3);
        let now = Instant::now();
        let mut rt = ProbeRuntime::new(now);

        for _ in 0..2 {
            let _ = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
            assert_eq!(rt.sustained_blindness(&spec), None);
        }
        let _ = fold_probe_observation(&spec, &mut rt, NO_ADDRESS, now);
        assert_eq!(
            rt.sustained_blindness(&spec),
            Some(BlindCause::NoTargetAddress)
        );
        let _ = fold_probe_observation(&spec, &mut rt, ProbeObservation::Success, now);
        assert_eq!(rt.sustained_blindness(&spec), None);
    }

    // ── the I/O shell ────────────────────────────────────────────────────

    fn http(timeout: Duration) -> ProbeSpec {
        ProbeSpec {
            handler: ProbeHandler::HttpGet {
                path: "/healthz".into(),
                port: Ok(port_n(8080)),
                scheme: HttpScheme::Http,
                host: None,
                headers: Vec::new(),
            },
            timing: ProbeTiming {
                timeout,
                ..spec(ProbeKind::Liveness, 1, 3).timing
            },
            ..spec(ProbeKind::Liveness, 1, 3)
        }
    }

    #[tokio::test]
    async fn http_2xx_and_3xx_pass_and_the_boundary_is_399_to_400() {
        let net = FakeNetProber::new();
        let cases = [
            (199, ProbeObservation::Failure),
            (200, ProbeObservation::Success),
            (399, ProbeObservation::Success),
            (400, ProbeObservation::Failure),
            (500, ProbeObservation::Failure),
        ];
        net.seed_http("10.0.0.1", 8080, "/healthz", cases.map(|(s, _)| s))
            .await;
        let spec = http(Duration::from_secs(1));
        for (status, expected) in cases {
            let got = run_handler(&spec, &FakeBackend::new(), &net, "cid", Some("10.0.0.1")).await;
            assert_eq!(got, expected, "HTTP {status}");
        }
    }

    /// A net prober that answers every request the same way, or never.
    struct Scripted(Option<Result<u16, ProbeIoError>>);

    #[async_trait::async_trait]
    impl NetProber for Scripted {
        async fn http_get(&self, _: &HttpProbeTarget) -> Result<u16, ProbeIoError> {
            match &self.0 {
                Some(answer) => answer.clone(),
                None => std::future::pending().await,
            }
        }
        async fn tcp_connect(&self, _: &TcpProbeTarget) -> Result<(), ProbeIoError> {
            match &self.0 {
                Some(answer) => answer.clone().map(|_| ()),
                None => std::future::pending().await,
            }
        }
    }

    #[tokio::test]
    async fn a_request_the_prober_could_not_form_is_blind() {
        let setup = Scripted(Some(Err(ProbeIoError::Setup {
            stage: crate::backend::ProbeSetupStage::Request,
            reason: "invalid header".into(),
        })));
        let got = run_handler(
            &http(Duration::from_secs(1)),
            &FakeBackend::new(),
            &setup,
            "cid",
            Some("10.0.0.1"),
        )
        .await;
        assert_eq!(got, ProbeObservation::Blind(BlindCause::ProberSetup));
    }

    #[tokio::test]
    async fn a_sent_request_that_is_refused_or_times_out_is_a_failure() {
        let refused = Scripted(Some(Err(ProbeIoError::Connect {
            target: "10.0.0.1:8080".into(),
            reason: "connection refused".into(),
        })));
        let hung = Scripted(None);
        for (label, net) in [("refused", &refused), ("timeout", &hung)] {
            let got = run_handler(
                &http(Duration::from_millis(20)),
                &FakeBackend::new(),
                net,
                "cid",
                Some("10.0.0.1"),
            )
            .await;
            assert_eq!(got, ProbeObservation::Failure, "{label}");
        }
    }

    #[test]
    fn every_prober_error_is_classified_by_whether_anything_was_sent() {
        let sent = [
            ProbeIoError::Connect {
                target: "t".into(),
                reason: "r".into(),
            },
            ProbeIoError::Timeout { target: "t".into() },
            ProbeIoError::Io("tls".into()),
        ];
        for e in sent {
            assert_eq!(net_error_observation(&e), ProbeObservation::Failure, "{e}");
        }
        let unsent = ProbeIoError::Setup {
            stage: crate::backend::ProbeSetupStage::Url,
            reason: "r".into(),
        };
        assert_eq!(
            net_error_observation(&unsent),
            ProbeObservation::Blind(BlindCause::ProberSetup)
        );
    }

    async fn started(backend: &FakeBackend) -> String {
        let spec = ContainerSpec {
            name: "default_p_main".into(),
            image: "busybox".into(),
            ..ContainerSpec::default()
        };
        match backend.start(&spec).await {
            Ok(status) => status.container_id,
            Err(e) => panic!("fake start: {e}"),
        }
    }

    #[tokio::test]
    async fn an_exec_the_runtime_could_not_run_is_blind() {
        let backend = FakeBackend::new();
        let id = started(&backend).await;
        backend
            .seed_exec_fault(
                "default_p_main",
                FakeExecFault::Unavailable("podman socket refused".into()),
            )
            .await;

        let got = run_handler(
            &spec(ProbeKind::Liveness, 1, 3),
            &backend,
            &FakeNetProber::new(),
            &id,
            None,
        )
        .await;

        assert_eq!(got, ProbeObservation::Blind(BlindCause::RuntimeUnavailable));
    }

    #[tokio::test]
    async fn an_exec_that_ran_and_said_no_is_a_failure_127_included() {
        let backend = FakeBackend::new();
        let id = started(&backend).await;
        backend
            .seed_exec(
                "default_p_main",
                [
                    ExecOutcome::failure(1),
                    ExecOutcome::failure(127),
                    ExecOutcome::success(),
                ],
            )
            .await;
        let spec = spec(ProbeKind::Liveness, 1, 3);
        let net = FakeNetProber::new();

        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(run_handler(&spec, &backend, &net, &id, None).await);
        }

        assert_eq!(
            got,
            [
                ProbeObservation::Failure,
                ProbeObservation::Failure,
                ProbeObservation::Success
            ]
        );
    }

    #[tokio::test]
    async fn an_exec_that_outlives_its_timeout_is_a_failure() {
        let backend = FakeBackend::new();
        let id = started(&backend).await;
        backend
            .seed_exec_fault("default_p_main", FakeExecFault::Hang)
            .await;
        let mut spec = spec(ProbeKind::Liveness, 1, 3);
        spec.timing.timeout = Duration::from_millis(20);

        let got = run_handler(&spec, &backend, &FakeNetProber::new(), &id, None).await;

        assert_eq!(got, ProbeObservation::Failure);
    }
}

#[cfg(test)]
mod upstream_prober {
    //! The prober's run semantics against upstream v1.34 (pkg/kubelet/prober,
    //! pkg/probe): a blind attempt is retried inside its run, a port that
    //! never resolves freezes the probe, port resolution's order and range,
    //! the started gate, zero-means-unset, exec `$(VAR)` expansion, and a
    //! probe that names its own host. The table-driven check against the
    //! upstream rows is `tests/oracle_prober.rs`; these pin each rule where it
    //! lives.
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde_json::json;

    use super::*;
    use crate::backend::{FakeBackend, FakeNetProber, ProbeSetupStage};

    /// A net prober that answers from a script, front first, then with its
    /// default — and counts every call, so a test can see each attempt.
    struct Script {
        answers: Mutex<VecDeque<Result<u16, ProbeIoError>>>,
        default: Result<u16, ProbeIoError>,
        calls: AtomicU32,
    }

    impl Script {
        fn new(answers: impl IntoIterator<Item = Result<u16, ProbeIoError>>) -> Self {
            Self {
                answers: Mutex::new(answers.into_iter().collect()),
                default: Ok(200),
                calls: AtomicU32::new(0),
            }
        }

        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }

        fn next(&self) -> Result<u16, ProbeIoError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let front = self.answers.lock().map(|mut q| q.pop_front());
            front.ok().flatten().unwrap_or_else(|| self.default.clone())
        }
    }

    #[async_trait::async_trait]
    impl NetProber for Script {
        async fn http_get(&self, _: &HttpProbeTarget) -> Result<u16, ProbeIoError> {
            self.next()
        }
        async fn tcp_connect(&self, _: &TcpProbeTarget) -> Result<(), ProbeIoError> {
            self.next().map(|_| ())
        }
    }

    /// An attempt that never reached the workload.
    fn unsent() -> Result<u16, ProbeIoError> {
        Err(ProbeIoError::Setup {
            stage: ProbeSetupStage::Request,
            reason: "could not form the request".into(),
        })
    }

    fn http_spec(kind: ProbeKind) -> ProbeSpec {
        ProbeSpec {
            kind,
            handler: ProbeHandler::HttpGet {
                path: "/healthz".into(),
                port: Ok(port_n(8080)),
                scheme: HttpScheme::Http,
                host: None,
                headers: Vec::new(),
            },
            timing: ProbeTiming::k8s_defaults(),
        }
    }

    async fn run(spec: &ProbeSpec, net: &dyn NetProber) -> ProbeObservation {
        run_handler(spec, &FakeBackend::new(), net, "cid", Some("10.0.0.1")).await
    }

    // ── one run is up to three attempts ─────────────────────────────────

    #[tokio::test]
    async fn a_blind_attempt_is_retried_in_the_same_run_and_the_answer_after_it_counts() {
        let net = Script::new([unsent(), unsent(), Ok(200)]);
        assert_eq!(
            run(&http_spec(ProbeKind::Liveness), &net).await,
            ProbeObservation::Success
        );
        assert_eq!(net.calls(), 3, "error, error, success is ONE run");
    }

    #[tokio::test]
    async fn three_blind_attempts_are_one_blind_run_and_no_fourth_is_made() {
        let net = Script::new([unsent(), unsent(), unsent(), Ok(200)]);
        assert_eq!(
            run(&http_spec(ProbeKind::Liveness), &net).await,
            ProbeObservation::Blind(BlindCause::ProberSetup)
        );
        assert_eq!(net.calls(), 3);
    }

    #[tokio::test]
    async fn an_answer_is_never_retried() {
        for (first, then, expected) in [
            (Ok(500), Ok(200), ProbeObservation::Failure),
            (Ok(200), Ok(500), ProbeObservation::Success),
        ] {
            let net = Script::new([first, then]);
            assert_eq!(run(&http_spec(ProbeKind::Liveness), &net).await, expected);
            assert_eq!(net.calls(), 1, "{expected:?} is the workload's answer");
        }
    }

    #[tokio::test]
    async fn a_failure_after_a_blind_attempt_is_a_counted_failure() {
        let net = Script::new([unsent(), Ok(500), Ok(200)]);
        assert_eq!(
            run(&http_spec(ProbeKind::Liveness), &net).await,
            ProbeObservation::Failure
        );
        assert_eq!(net.calls(), 2);
    }

    // ── a port that never resolves ──────────────────────────────────────

    #[tokio::test]
    async fn a_port_that_does_not_resolve_freezes_every_kind_at_its_initial_value() {
        let now = Instant::now();
        for handler in [
            json!({ "httpGet": { "port": "http" } }),
            json!({ "tcpSocket": { "port": "http" } }),
        ] {
            for kind in ProbeKind::ALL {
                let mut container = serde_json::Map::new();
                container.insert("name".into(), json!("main"));
                container.insert(kind.field().into(), handler.clone());
                let spec = ProbeSpec::from_container(kind, &Value::Object(container))
                    .unwrap()
                    .unwrap();
                // The workload behind the port would answer 200 if asked.
                let net = Script::new([]);
                let mut rt = ProbeRuntime::new(now);
                for _ in 0..100 {
                    let obs = run(&spec, &net).await;
                    assert_eq!(obs, ProbeObservation::Blind(BlindCause::UnresolvablePort));
                    let v = fold_probe_observation(&spec, &mut rt, obs, now);
                    assert_eq!(v.trip, None, "{kind} {handler}: restarted on nothing");
                }
                assert_eq!(net.calls(), 0, "{kind} {handler}: nothing is dialled");
                assert!(
                    !rt.gate_satisfied,
                    "{kind} {handler}: never Ready / started"
                );
                assert_eq!(rt.consecutive_failures, 0);
                assert_eq!(rt.consecutive_successes, 0);
            }
        }
    }

    #[test]
    fn port_resolution_takes_a_name_first_then_a_number_and_only_1_to_65535() {
        let ports = [("found".to_string(), 93u16), ("zero".to_string(), 0)];
        let resolved = |port: Value| resolve_port(Some(&port), &ports).map(ProbePort::get);
        let out_of_range = |number| Err(UnresolvablePort::OutOfRange { number });
        let no_such = |name: &str| {
            Err(UnresolvablePort::NoSuchName {
                name: name.to_string(),
            })
        };
        assert_eq!(resolved(json!("found")), Ok(93));
        assert_eq!(resolved(json!(76)), Ok(76));
        assert_eq!(resolved(json!("118")), Ok(118));
        assert_eq!(resolved(json!(65535)), Ok(65535));
        assert_eq!(resolved(json!("65535")), Ok(65535));
        assert_eq!(resolved(json!(1)), Ok(1));
        assert_eq!(resolved(json!(0)), out_of_range(0));
        assert_eq!(resolved(json!(-1)), out_of_range(-1));
        assert_eq!(resolved(json!("-1")), out_of_range(-1));
        assert_eq!(resolved(json!(65536)), out_of_range(65536));
        assert_eq!(resolved(json!("65536")), out_of_range(65536));
        // A NAMED port whose number is 0 is no port either.
        assert_eq!(resolved(json!("zero")), out_of_range(0));
        assert_eq!(resolved(json!("not-found")), no_such("not-found"));
        assert_eq!(resolved(json!("")), no_such(""));
        assert_eq!(resolved(json!(true)), Err(UnresolvablePort::NotIntOrString));
        assert_eq!(resolved(Value::Null), Err(UnresolvablePort::Missing));
        assert_eq!(
            resolve_port(None, &ports).map(ProbePort::get),
            Err(UnresolvablePort::Missing)
        );
    }

    #[test]
    fn port_zero_has_no_probe_port() {
        assert_eq!(ProbePort::new(0), None);
        assert_eq!(ProbePort::new(1).map(ProbePort::get), Some(1));
        assert_eq!(ProbePort::new(u16::MAX).map(ProbePort::get), Some(65535));
    }

    #[test]
    fn the_out_of_range_reason_reads_as_upstreams() {
        assert_eq!(
            UnresolvablePort::OutOfRange { number: 0 }.to_string(),
            "invalid port number: 0"
        );
    }

    // ── the started gate ────────────────────────────────────────────────

    #[test]
    fn liveness_and_readiness_run_only_once_started_and_startup_only_before() {
        for (kind, before, after) in [
            (ProbeKind::Liveness, false, true),
            (ProbeKind::Readiness, false, true),
            (ProbeKind::Startup, true, false),
        ] {
            assert_eq!(kind.may_run(false), before, "{kind} before started");
            assert_eq!(kind.may_run(true), after, "{kind} once started");
        }
    }

    #[test]
    fn a_container_is_started_once_running_with_its_startup_probe_passed() {
        let now = Instant::now();
        let mut spec = http_spec(ProbeKind::Startup);
        spec.timing.failure_threshold = 1;
        let fresh = ProbeRuntime::new(now);
        let mut tripped = ProbeRuntime::new(now);
        let _ = fold_probe_observation(&spec, &mut tripped, ProbeObservation::Failure, now);
        let mut passed = ProbeRuntime::new(now);
        let _ = fold_probe_observation(&spec, &mut passed, ProbeObservation::Success, now);

        assert!(container_started(true, None), "no startup probe");
        assert!(!container_started(true, Some(&fresh)), "not yet run");
        assert!(!container_started(true, Some(&tripped)), "tripped");
        assert!(container_started(true, Some(&passed)), "passed");
        assert!(!container_started(false, Some(&passed)), "not running");
        assert!(!container_started(false, None), "not running");
    }

    // ── exec `$(VAR)` expansion and parsing from the container ──────────

    #[test]
    fn an_exec_probe_expands_references_to_literal_env_values_only() {
        let container = json!({
            "name": "main",
            "env": [
                { "name": "A", "value": "script" },
                { "name": "B", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                { "name": "C", "value": "$(A)" }
            ],
            "livenessProbe": {
                "exec": { "command": ["/bin/bash", "-c", "some $(A) [$(B)] $(C) $(D) $$(A)"] }
            }
        });
        let spec = ProbeSpec::from_container(ProbeKind::Liveness, &container)
            .unwrap()
            .unwrap();
        assert_eq!(
            spec.handler,
            ProbeHandler::Exec {
                command: vec![
                    "/bin/bash".into(),
                    "-c".into(),
                    // B is valueFrom: the empty string. C's value is not
                    // itself expanded. D is undeclared and stays. $$ escapes.
                    "some script [] $(A) $(D) $(A)".into(),
                ]
            }
        );
    }

    #[test]
    fn from_container_reads_its_own_kind_and_ports_and_null_is_absent() {
        let container = json!({
            "name": "main",
            "ports": [{ "name": "db", "containerPort": 5432 }],
            "livenessProbe": { "tcpSocket": { "port": "db" } },
            "readinessProbe": null
        });
        let liveness = ProbeSpec::from_container(ProbeKind::Liveness, &container).unwrap();
        assert!(matches!(
            liveness.map(|s| s.handler),
            Some(ProbeHandler::TcpSocket { port: Ok(p), host: None }) if p.get() == 5432
        ));
        assert_eq!(
            ProbeSpec::from_container(ProbeKind::Readiness, &container),
            Ok(None)
        );
        assert_eq!(
            ProbeSpec::from_container(ProbeKind::Startup, &container),
            Ok(None)
        );
    }

    // ── a probe that names its own host ─────────────────────────────────

    #[tokio::test]
    async fn a_probe_naming_a_host_dials_it_even_when_the_pod_has_no_address() {
        let net = FakeNetProber::new();
        net.seed_http("example.internal", 8080, "/healthz", [200])
            .await;
        net.seed_tcp("db.internal", 8080, [true]).await;
        let http = ProbeSpec {
            handler: ProbeHandler::HttpGet {
                path: "/healthz".into(),
                port: Ok(port_n(8080)),
                scheme: HttpScheme::Http,
                host: Some("example.internal".into()),
                headers: Vec::new(),
            },
            ..http_spec(ProbeKind::Readiness)
        };
        let tcp = ProbeSpec {
            handler: ProbeHandler::TcpSocket {
                port: Ok(port_n(8080)),
                host: Some("db.internal".into()),
            },
            ..http_spec(ProbeKind::Readiness)
        };
        for spec in [http, tcp] {
            let got = run_handler(&spec, &FakeBackend::new(), &net, "cid", None).await;
            assert_eq!(got, ProbeObservation::Success, "{:?}", spec.handler);
        }
    }
}
