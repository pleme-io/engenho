//! Kubelet prober result handling against upstream (Kubernetes v1.34.0,
//! pkg/kubelet/prober and pkg/probe), row by row: `Vector::ProberResults`.
//!
//! Every row is driven through engenho's own prober code — `ProbeSpec`'s
//! parse, `run_handler` (the one function that touches a runtime or a
//! socket), `fold_probe_observation`, and the started gate — against fakes
//! where the row is about a decision, and against a real HTTP server on
//! 127.0.0.1 where it is about the wire (redirects, headers, timeouts). Each
//! kind's adapter says what it drives; what is out of scope, and why, is in
//! [`OUT_OF_SCOPE`], and where engenho differs on purpose is in
//! [`DEVIATIONS`].
//!
//! ## Reading upstream's vocabulary off engenho's state
//!
//! Upstream caches one result per probe: `Success`, `Failure` or `Unknown`.
//! engenho keeps no such cache; the kubelet reads each probe's state instead,
//! and so does this adapter ([`cached_result`]): readiness is its latched
//! gate; startup is its gate, or its trip, or neither (`Unknown`); liveness is
//! `Failure` once it has tripped.
//!
//! A handler's result upstream is `success`, `warning`, `failure`, or
//! `unknown` with an error. engenho's is `Success`, `Failure` or `Blind`, and
//! `Blind` is upstream's `unknown` with an error. engenho has no `warning`: a
//! 3xx that it passes is labelled `warning` here ([`label`]), so for an HTTP
//! row what is compared is where engenho puts the pass/fail line.
//!
//! Upstream holds a liveness worker after it fails and a startup worker after
//! any verdict, until a new container appears. In engenho a tripped probe
//! restarts the container (a new one, with fresh probe state), and a startup
//! probe does not run once the container has started. The adapter reads that
//! as upstream's hold, with a run length of zero: the next run starts from
//! nothing either way.

#![allow(
    clippy::disallowed_methods,
    reason = "starts one FakeBackend container for exec probes to run in; no kubelet in the loop"
)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use engenho_kubelet::backend::{ContainerSpec, HostPort};
use engenho_kubelet::{
    ContainerRuntime, ExecOutcome, FakeBackend, FakeExecFault, FakeNetProber, HttpProbeTarget,
    NetProber, ProbeHandler, ProbeIoError, ProbeKind, ProbeObservation, ProbeRuntime,
    ProbeSetupStage, ProbeSpec, ProbeUrl, TcpProbeTarget, TokioNetProber,
    aggregate_container_readiness, container_started, fold_probe_observation,
    http_status_observation, run_handler,
};
use engenho_oracle::{Answer, Case, Deviation, OutOfScope, Table, Vector, run};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Rows engenho has no counterpart for.
const OUT_OF_SCOPE: &[OutOfScope] = &[
    OutOfScope::kind(
        "results_enum",
        "Go's integer encoding of results.Result. engenho keeps no cached result per probe; \
         each kind's result is read off the probe's state (see the module docs)",
    ),
    OutOfScope::kind(
        "worker_tick",
        "each row is a gate of upstream's per-probe worker goroutine (pod phase, deletion \
         timestamp, container status present, restartPolicy Never ending the worker) and \
         reports keep_going, that goroutine's lifetime. engenho has no per-probe worker: the \
         kubelet's pod reconcile decides these for the whole pod. The started gate these rows \
         also carry is checked by the worker_sequence rows",
    ),
    OutOfScope::kind(
        "feature_gate_default",
        "engenho has no feature gates. It implements ExecProbeTimeout's GA default, a timed-out \
         exec is a counted failure, checked by exec_timeout_with_gate_on_is_counted_failure",
    ),
    OutOfScope::kind(
        "cri_exec_sync",
        "the CRI gRPC deadline and its error text. engenho bounds an exec probe with its own \
         timeout around ContainerRuntime::exec, checked by the exec_probe timeout row",
    ),
    OutOfScope::kind(
        "cri_exec_output",
        "probe output is not recorded: engenho keeps a probe's verdict, not its stdout/stderr",
    ),
    OutOfScope::kind(
        "grpc_probe",
        "grpc probes are a typed parse deferral (ProbeParseError::UnsupportedHandler)",
    ),
    OutOfScope::kind(
        "update_pod_status",
        "the kubelet's status writer decides ready and started for running and terminated \
         containers together. The probe-decided half is checked by worker_sequence and \
         is_container_started",
    ),
    OutOfScope::kind(
        "results_manager",
        "upstream's results.Manager update channel; engenho has no result cache or channel",
    ),
    OutOfScope::kind(
        "probe_validation",
        "API validation of a Probe is the apiserver's. engenho's kubelet forces a liveness or \
         startup successThreshold to 1 rather than rejecting it",
    ),
    OutOfScope::kind(
        "worker_loop",
        "goroutine timing (start jitter, the readiness manual trigger, stop). engenho's kubelet \
         runs due probes on its tick and requeues at the soonest next_due_in",
    ),
    OutOfScope::case(
        "prober_handler_warning_is_success",
        "no engenho exec result is a warning. The only warning upstream produces is an HTTP \
         3xx, checked by http_status_classification and the 3xx http_probe rows",
    ),
    OutOfScope::case(
        "prober_handler_unknown_without_error_is_counted_failure",
        "no engenho handler yields an unknown result without an error: every attempt is \
         Success, Failure or Blind",
    ),
    OutOfScope::case(
        "prober_unsupported_probe_type",
        "ProbeKind is a closed enum: a probe type other than liveness, readiness or startup has \
         no value",
    ),
    OutOfScope::case(
        "exec_timeout_with_gate_off_is_discarded",
        "engenho has no ExecProbeTimeout gate to turn off; it is always the GA default",
    ),
    OutOfScope::case(
        "http_redirects_follow_nonlocal_true",
        "the kubelet's prober fixes followNonLocalRedirects = false (prober.go:60) and engenho \
         implements only that policy, checked by http_redirects_kubelet_policy_no_nonlocal_follow",
    ),
];

/// Checked rows where engenho differs from upstream on purpose.
const DEVIATIONS: &[Deviation] = &[
    Deviation {
        case: "seq_readiness_success_run_carries_across_container_restart",
        why: "engenho resets a container's probe counters when the container restarts \
              (ContainerProbeState::reset), so a restarted container earns readiness with its \
              own successThreshold successes. Upstream resets only onHold on a new container \
              ID and lets the previous container's success run count toward the new one's; no \
              upstream test covers it (worker.go:235-243, 314-327). The reset is the \
              conservative reading: a new container is not Ready on its predecessor's record.",
    },
    Deviation {
        case: "prober_no_handler_is_error",
        why: "upstream's API validation rejects a probe with no handler before any kubelet \
              sees it; a kubelet that did get one would discard every run. engenho's kubelet \
              refuses the pod (ProbeParseError::NoHandler), which is what upstream's apiserver \
              gives that pod. Probe validation at admission in engenho-apiserver is the \
              destination.",
    },
    Deviation {
        case: "http_body_read_error_is_discarded_not_counted",
        why: "engenho's prober judges the status line and does not read the body, so a 2xx \
              whose body then stalls passes; upstream reads up to 10 KiB and discards the run \
              when that read errors. The status is the workload's answer either way; the body \
              is only upstream's probe output, which engenho does not record.",
    },
    Deviation {
        case: "http_request_header_overrides",
        why: "a probe that sets Accept EMPTY: upstream sends no Accept header; engenho's HTTP \
              client (reqwest 0.12) adds `Accept: */*` to every request that lacks one \
              (ClientBuilder::new, async_impl/client.rs:286, filled in at :2592) and a request \
              cannot opt out. A missing Accept means any media type is acceptable (RFC 9110 \
              12.5.1), which is what */* says, so the workload is asked the same question. The \
              adapter asserts that this is the only sub-row that differs.",
    },
    Deviation {
        case: "port_resolution_unknown_name_error_is_atoi_error",
        why: "the error TEXT. Upstream's findPortByName error is overwritten by its \
              strconv.Atoi fallback's (probe/util.go:34-38), an accident of the Go code; \
              engenho names the port it could not find (UnresolvablePort::NoSuchName). The \
              outcome, ok: false, agrees.",
    },
];

/// Every row that is not out of scope is checked. A row falling back to
/// `NotChecked` fails the harness as unclaimed; this also stops the count
/// shrinking by a row moving into [`OUT_OF_SCOPE`] unnoticed.
const CHECKED_ROWS: usize = 66;

#[tokio::test]
async fn prober_result_handling_agrees_with_upstream() {
    let table = Vector::ProberResults.load();
    let mut answers = HashMap::new();
    // A declared deviation that is narrower than its row records here when
    // anything outside it differs; reported with the harness's findings, so a
    // failing run lists everything at once.
    let mut broken_pins = Vec::new();
    for case in &table.cases {
        answers.insert(
            case.name.clone(),
            answer(&table, case, &mut broken_pins).await,
        );
    }
    let outcome = run(&table, OUT_OF_SCOPE, DEVIATIONS, |case| {
        answers.remove(&case.name).unwrap_or(Answer::NotChecked)
    });
    let pins = broken_pins.join("\n");
    let report = match outcome {
        Ok(report) if broken_pins.is_empty() => report,
        Ok(_) => panic!("a declared deviation covers more than it says:\n{pins}"),
        Err(failures) => panic!("{failures}{pins}"),
    };
    assert_eq!(
        report.checked, CHECKED_ROWS,
        "rows checked against upstream: {report:?}"
    );
    assert_eq!(report.deviations, DEVIATIONS.len());
}

async fn answer(table: &Table, case: &Case, broken_pins: &mut Vec<String>) -> Answer {
    match table.kind_of(case).as_str() {
        "worker_initial_value" => worker_initial_value(case),
        "worker_sequence" => worker_sequence(case).await,
        "worker_tick_predicate" => worker_tick_predicate(case),
        "prober_probe" => prober_probe(case).await,
        "prober_exec_command" => prober_exec_command(case),
        "prober_retries" => prober_retries(case).await,
        "exec_probe" => exec_probe(case).await,
        "http_status_classify" => http_status_classify(case),
        "http_probe" => http_probe(case).await,
        "http_redirect_policy" => http_redirect_policy(case).await,
        "http_request_headers" => http_request_headers(case, broken_pins).await,
        "http_request_url" => http_request_url(case).await,
        "resolve_container_port" => resolve_container_port(case),
        "end_to_end" => end_to_end(case).await,
        "tcp_probe" => tcp_probe(case).await,
        "tcp_target" => tcp_target(case).await,
        "is_container_started" => is_container_started(case),
        "probe_defaults" => probe_defaults(case),
        _ => Answer::NotChecked,
    }
}

// =====================================================================
// Shared vocabulary
// =====================================================================

fn text(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn kind_named(name: &str) -> ProbeKind {
    match name {
        "liveness" => ProbeKind::Liveness,
        "readiness" => ProbeKind::Readiness,
        "startup" => ProbeKind::Startup,
        other => panic!("the table names an unknown probe type {other:?}"),
    }
}

/// engenho's answer, cut down to the keys upstream's row has. The harness
/// compares every key the adapter returns and refuses one upstream lacks;
/// keys upstream has that engenho does not answer are listed in its report.
fn project(expected: &Value, got: Value) -> Answer {
    match (expected, got) {
        (Value::Object(exp), Value::Object(mut got)) => {
            got.retain(|k, _| exp.contains_key(k));
            Answer::Checked(Value::Object(got))
        }
        (_, got) => Answer::Checked(got),
    }
}

/// Upstream's cached result for one probe, read off engenho's state for it.
fn cached_result(kind: ProbeKind, rt: &ProbeRuntime, tripped: bool) -> &'static str {
    match kind {
        ProbeKind::Liveness if tripped => "Failure",
        ProbeKind::Liveness => "Success",
        ProbeKind::Readiness | ProbeKind::Startup if rt.gate_satisfied => "Success",
        ProbeKind::Readiness => "Failure",
        ProbeKind::Startup if tripped => "Failure",
        ProbeKind::Startup => "Unknown",
    }
}

/// Upstream's handler result for one engenho observation; `status` is the
/// HTTP status read, if any, which is all that separates `warning` from
/// `success`.
fn label(obs: ProbeObservation, status: Option<u16>) -> &'static str {
    match obs {
        ProbeObservation::Success if status.is_some_and(|s| (300..400).contains(&s)) => "warning",
        ProbeObservation::Success => "success",
        ProbeObservation::Failure => "failure",
        ProbeObservation::Blind(_) => "unknown",
    }
}

/// The kubelet-level result: a pass or a (counted or discarded) failure.
fn kubelet_result(obs: ProbeObservation) -> &'static str {
    match obs {
        ProbeObservation::Success => "Success",
        ProbeObservation::Failure | ProbeObservation::Blind(_) => "Failure",
    }
}

fn is_error(obs: ProbeObservation) -> bool {
    matches!(obs, ProbeObservation::Blind(_))
}

/// Several runs that upstream expects to agree: their shared label, or every
/// label when they do not (which then disagrees with upstream's one).
fn one_label(labels: &[&str]) -> String {
    match labels {
        [first, rest @ ..] if rest.iter().all(|l| l == first) => (*first).to_owned(),
        _ => labels.join(","),
    }
}

fn probe(kind: ProbeKind, json: &Value) -> ProbeSpec {
    ProbeSpec::from_k8s(kind, json, &[]).unwrap_or_else(|e| panic!("{json} parses: {e}"))
}

/// An exec probe with the row's thresholds and delay.
fn exec_probe_spec(kind: ProbeKind, spec: &Value) -> ProbeSpec {
    probe(
        kind,
        &json!({
            "exec": { "command": ["probe"] },
            "successThreshold": spec.get("success_threshold"),
            "failureThreshold": spec.get("failure_threshold"),
            "initialDelaySeconds": spec.get("initial_delay_seconds"),
            "timeoutSeconds": 1,
        }),
    )
}

/// One FakeBackend container for exec probes to run in.
struct ExecRig {
    backend: FakeBackend,
    id: String,
}

const RIG: &str = "default_oracle_main";

/// What the next exec answers.
enum Exec {
    Exit(i32),
    RuntimeError,
    Hang,
}

impl ExecRig {
    async fn new() -> Self {
        let backend = FakeBackend::new();
        let spec = ContainerSpec {
            name: RIG.into(),
            image: "busybox".into(),
            ..ContainerSpec::default()
        };
        let id = match backend.start(&spec).await {
            Ok(status) => status.container_id,
            Err(e) => panic!("fake start: {e}"),
        };
        Self { backend, id }
    }

    async fn next(&self, exec: Exec) {
        self.backend.clear_exec_fault(RIG).await;
        match exec {
            Exec::Exit(0) => self.backend.seed_exec(RIG, [ExecOutcome::success()]).await,
            Exec::Exit(code) => {
                self.backend
                    .seed_exec(RIG, [ExecOutcome::failure(code)])
                    .await;
            }
            Exec::RuntimeError => {
                self.backend
                    .seed_exec_fault(RIG, FakeExecFault::Unavailable("runtime error".into()))
                    .await;
            }
            Exec::Hang => self.backend.seed_exec_fault(RIG, FakeExecFault::Hang).await,
        }
    }

    /// The worker_sequence letters: `S`, `F`, and `E` for a prober error.
    async fn next_letter(&self, letter: &str) {
        self.next(match letter {
            "S" => Exec::Exit(0),
            "F" => Exec::Exit(1),
            _ => Exec::RuntimeError,
        })
        .await;
    }

    async fn run(&self, spec: &ProbeSpec) -> ProbeObservation {
        run_handler(spec, &self.backend, &FakeNetProber::new(), &self.id, None).await
    }
}

/// A net prober that answers from a script, front first, then 200, counting
/// every call — each one is an attempt.
struct Script {
    answers: Mutex<VecDeque<Result<u16, ProbeIoError>>>,
    calls: AtomicU32,
}

impl Script {
    fn new(answers: impl IntoIterator<Item = Result<u16, ProbeIoError>>) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            calls: AtomicU32::new(0),
        }
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn next(&self) -> Result<u16, ProbeIoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let front = self.answers.lock().map(|mut q| q.pop_front());
        front.ok().flatten().unwrap_or(Ok(200))
    }
}

#[async_trait]
impl NetProber for Script {
    async fn http_get(&self, _: &HttpProbeTarget) -> Result<u16, ProbeIoError> {
        self.next()
    }
    async fn tcp_connect(&self, _: &TcpProbeTarget) -> Result<(), ProbeIoError> {
        self.next().map(|_| ())
    }
}

/// A net prober that passes every request to `inner` and remembers what it
/// was asked and the last status it read.
struct Recording<P> {
    inner: P,
    http: Mutex<Vec<HttpProbeTarget>>,
    tcp: Mutex<Vec<TcpProbeTarget>>,
    last_status: Mutex<Option<u16>>,
}

impl<P> Recording<P> {
    fn new(inner: P) -> Self {
        Self {
            inner,
            http: Mutex::new(Vec::new()),
            tcp: Mutex::new(Vec::new()),
            last_status: Mutex::new(None),
        }
    }

    fn last_status(&self) -> Option<u16> {
        self.last_status.lock().ok().and_then(|s| *s)
    }

    fn last_http(&self) -> Option<HttpProbeTarget> {
        self.http.lock().ok().and_then(|t| t.last().cloned())
    }

    fn last_tcp(&self) -> Option<TcpProbeTarget> {
        self.tcp.lock().ok().and_then(|t| t.last().cloned())
    }
}

#[async_trait]
impl<P: NetProber> NetProber for Recording<P> {
    async fn http_get(&self, target: &HttpProbeTarget) -> Result<u16, ProbeIoError> {
        if let Ok(mut seen) = self.http.lock() {
            seen.push(target.clone());
        }
        let answer = self.inner.http_get(target).await;
        if let Ok(mut last) = self.last_status.lock() {
            *last = answer.as_ref().ok().copied();
        }
        answer
    }
    async fn tcp_connect(&self, target: &TcpProbeTarget) -> Result<(), ProbeIoError> {
        if let Ok(mut seen) = self.tcp.lock() {
            seen.push(target.clone());
        }
        self.inner.tcp_connect(target).await
    }
}

// =====================================================================
// A real HTTP/1.1 server on 127.0.0.1
// =====================================================================

/// One request as the server read it. Header names are canonicalised the way
/// Go's `http.Header` does, which is how upstream's test servers see them.
#[derive(Clone, Debug, Default)]
struct Head {
    target: String,
    headers: Vec<(String, String)>,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Every header but Host as `Name: value` lines — what upstream's header
    /// test server writes back (Go keeps Host out of `r.Header`).
    fn echo(&self) -> String {
        self.headers
            .iter()
            .filter(|(n, _)| !n.eq_ignore_ascii_case("host"))
            .map(|(n, v)| format!("{n}: {v}\n"))
            .collect()
    }
}

fn canonical(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
            })
        })
        .collect::<Vec<_>>()
        .join("-")
}

enum Reply {
    Status {
        code: u16,
        location: Option<String>,
        body: String,
    },
    /// Wait, then answer.
    After(Duration, Box<Reply>),
    /// Read the request and close without a response.
    Close,
    /// Send a 200 head promising a body, then send nothing.
    HeadThenStall,
}

impl Reply {
    fn status(code: u16) -> Self {
        Self::Status {
            code,
            location: None,
            body: String::new(),
        }
    }

    fn redirect(code: u16, to: impl Into<String>) -> Self {
        Self::Status {
            code,
            location: Some(to.into()),
            body: String::new(),
        }
    }
}

struct Server {
    port: u16,
    heads: Arc<Mutex<Vec<Head>>>,
}

impl Server {
    fn last_head(&self) -> Head {
        self.heads
            .lock()
            .ok()
            .and_then(|h| h.last().cloned())
            .unwrap_or_default()
    }
}

async fn serve(route: impl Fn(&Head) -> Reply + Send + Sync + 'static) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let heads = Arc::new(Mutex::new(Vec::new()));
    let route = Arc::new(route);
    let seen = heads.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let route = route.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let Some(head) = read_head(&mut sock).await else {
                    return;
                };
                if let Ok(mut s) = seen.lock() {
                    s.push(head.clone());
                }
                write_reply(&mut sock, route(&head)).await;
            });
        }
    });
    Server { port, heads }
}

async fn read_head(sock: &mut TcpStream) -> Option<Head> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let mut lines = raw.split("\r\n");
    let target = lines.next()?.split(' ').nth(1)?.to_owned();
    let headers = lines
        .take_while(|l| !l.is_empty())
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (canonical(n.trim()), v.trim().to_owned()))
        .collect();
    Some(Head { target, headers })
}

async fn write_reply(sock: &mut TcpStream, mut reply: Reply) {
    loop {
        match reply {
            Reply::After(wait, next) => {
                tokio::time::sleep(wait).await;
                reply = *next;
            }
            Reply::Close => return,
            Reply::HeadThenStall => {
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
                    .await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                return;
            }
            Reply::Status {
                code,
                location,
                body,
            } => {
                let location = location
                    .map(|l| format!("Location: {l}\r\n"))
                    .unwrap_or_default();
                let out = format!(
                    "HTTP/1.1 {code} Oracle\r\nContent-Length: {}\r\nConnection: close\r\n{location}\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.shutdown().await;
                return;
            }
        }
    }
}

/// A port on 127.0.0.1 with nothing listening.
async fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    listener.local_addr().expect("local addr").port()
}

/// Run an httpGet probe (`http_get`, a manifest `httpGet` object) against
/// 127.0.0.1 through engenho's parse, `run_handler` and the real prober.
/// Returns the observation and the status read, if any.
async fn probe_http(http_get: Value, timeout_seconds: u64) -> (ProbeObservation, Option<u16>) {
    let spec = probe(
        ProbeKind::Readiness,
        &json!({ "httpGet": http_get, "timeoutSeconds": timeout_seconds }),
    );
    let net = Recording::new(TokioNetProber::new());
    let obs = run_handler(&spec, &FakeBackend::new(), &net, "cid", Some("127.0.0.1")).await;
    (obs, net.last_status())
}

/// A manifest `httpHeaders` list from upstream's `{"Name": ["value", …]}`.
fn http_headers(headers: &Value) -> Value {
    let list: Vec<Value> = headers
        .as_object()
        .into_iter()
        .flatten()
        .flat_map(|(name, values)| {
            values
                .as_array()
                .into_iter()
                .flatten()
                .map(move |v| json!({ "name": name, "value": v }))
        })
        .collect();
    Value::Array(list)
}

fn u16_of(v: &Value) -> Option<u16> {
    v.as_u64().and_then(|n| u16::try_from(n).ok())
}

// =====================================================================
// Adapters, one per kind
// =====================================================================

/// `worker_initial_value`: a probe that has not run, read per kind.
fn worker_initial_value(case: &Case) -> Answer {
    let rt = ProbeRuntime::new(Instant::now());
    project(
        &case.expected,
        json!({
            "liveness": cached_result(ProbeKind::Liveness, &rt, false),
            "readiness": cached_result(ProbeKind::Readiness, &rt, false),
            "startup": cached_result(ProbeKind::Startup, &rt, false),
        }),
    )
}

/// `worker_sequence`: one probe over a sequence of ticks. Each step is one
/// period later; a new container ID is a restart, which starts the probe
/// afresh (engenho's `ContainerProbeState::reset`). Whether the probe runs is
/// engenho's decision: `ProbeKind::may_run` on the container's started state,
/// `ProbeRuntime::is_due` (initial delay and period), and not after a trip
/// (a tripped probe's container is restarted). The run is an exec through
/// `run_handler` — `E` is an exec the runtime could not run — folded by
/// `fold_probe_observation`.
async fn worker_sequence(case: &Case) -> Answer {
    let input = &case.input;
    let kind = kind_named(&text(input, "probe_type"));
    let spec = exec_probe_spec(kind, &input["spec"]);
    let rig = ExecRig::new().await;
    let mut tick = Instant::now();
    let mut container: Option<String> = None;
    let mut rt = ProbeRuntime::new(tick);
    let mut tripped = false;
    let (mut result, mut result_run, mut on_hold, mut invoked) = (vec![], vec![], vec![], vec![]);

    for step in input["steps"].as_array().into_iter().flatten() {
        tick += spec.timing.period;
        let id = text(step, "container_id");
        if container.as_deref() != Some(id.as_str()) {
            rt = ProbeRuntime::new(tick);
            tripped = false;
            container = Some(id);
        }
        let now = step
            .get("seconds_since_container_start")
            .and_then(Value::as_f64)
            .map_or(tick, |secs| rt.started_at + Duration::from_secs_f64(secs));
        let running = step["running"].as_bool().unwrap_or(true);
        // Liveness and readiness read the container's started state, which
        // the row gives: engenho derives it from the container's startup
        // probe, which is not this row's. A startup probe's own state IS.
        let started = match kind {
            ProbeKind::Startup => container_started(true, Some(&rt)),
            ProbeKind::Liveness | ProbeKind::Readiness => {
                step["started"].as_bool().unwrap_or(false)
            }
        };
        let runs = running && !tripped && kind.may_run(started) && rt.is_due(&spec, now);
        if runs {
            rig.next_letter(&text(step, "probe")).await;
            let obs = rig.run(&spec).await;
            tripped |= fold_probe_observation(&spec, &mut rt, obs, now)
                .trip
                .is_some();
        }
        let held = tripped
            || (kind == ProbeKind::Startup
                && !ProbeKind::Startup.may_run(container_started(true, Some(&rt))));
        result.push(match (running, kind) {
            (true, _) => cached_result(kind, &rt, tripped),
            // Not running: engenho does not report the container ready.
            (false, ProbeKind::Readiness) => "Failure",
            (false, _) => "not running",
        });
        result_run.push(if held {
            0
        } else {
            rt.consecutive_successes.max(rt.consecutive_failures)
        });
        on_hold.push(held);
        invoked.push(runs);
    }
    project(
        &case.expected,
        json!({
            "result": result,
            "result_run": result_run,
            "on_hold": on_hold,
            "prober_invoked": invoked,
            "container_started": container_started(true, Some(&rt)),
        }),
    )
}

/// `worker_tick_predicate`: the initial-delay test, `ProbeRuntime::past_initial_delay`.
fn worker_tick_predicate(case: &Case) -> Answer {
    let t0 = Instant::now();
    let allowed: Vec<bool> = case.input["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            let spec = probe(
                ProbeKind::Liveness,
                &json!({
                    "exec": { "command": ["probe"] },
                    "initialDelaySeconds": row["initial_delay_seconds"],
                }),
            );
            let elapsed = row["seconds_since_container_start"].as_f64().unwrap_or(0.0);
            ProbeRuntime::new(t0).past_initial_delay(&spec, t0 + Duration::from_secs_f64(elapsed))
        })
        .collect();
    project(&case.expected, json!({ "probe_allowed": allowed }))
}

/// `prober_probe`: the probe as a whole. No probe declared; a probe with no
/// handler; an exec handler's answer folded once, to see whether it counted.
async fn prober_probe(case: &Case) -> Answer {
    let input = &case.input;
    if input.get("probe_type").is_some() {
        return Answer::NotChecked;
    }
    match input.get("probe_spec") {
        Some(Value::Null) => {
            // Nothing declared: nothing parses, and readiness is the
            // container's running state.
            let declared =
                ProbeSpec::from_container(ProbeKind::Readiness, &json!({ "name": "main" }))
                    .ok()
                    .flatten()
                    .is_some();
            let (ready, _) = aggregate_container_readiness(true, false, false, declared, true);
            project(
                &case.expected,
                json!({ "result": if ready { "Success" } else { "Failure" }, "error": false }),
            )
        }
        Some(declared) => {
            let effect = match ProbeSpec::from_k8s(ProbeKind::Readiness, declared, &[]) {
                Err(e) => format!("the kubelet refuses the pod: {e}"),
                Ok(_) => "parsed".to_owned(),
            };
            project(
                &case.expected,
                json!({ "error": true, "worker_effect": effect }),
            )
        }
        None => {
            let returns = &input["handler_returns"];
            let exec = match (text(returns, "result").as_str(), returns["error"].as_bool()) {
                ("success", Some(false)) => Exec::Exit(0),
                ("failure", Some(false)) => Exec::Exit(1),
                ("unknown", Some(true)) => Exec::RuntimeError,
                _ => return Answer::NotChecked,
            };
            let rig = ExecRig::new().await;
            rig.next(exec).await;
            let spec = exec_probe_spec(ProbeKind::Readiness, &Value::Null);
            let obs = rig.run(&spec).await;
            let mut rt = ProbeRuntime::new(Instant::now());
            let _ = fold_probe_observation(&spec, &mut rt, obs, Instant::now());
            let counted = rt.consecutive_failures + rt.consecutive_successes > 0;
            project(
                &case.expected,
                json!({
                    "result": kubelet_result(obs),
                    "error": is_error(obs),
                    "worker_effect": if counted { "counted" } else { "discarded" },
                }),
            )
        }
    }
}

/// `prober_exec_command`: the argv `run_handler` hands the runtime, as
/// `ProbeSpec::from_container` parses it from a container with that env.
fn prober_exec_command(case: &Case) -> Answer {
    let container = json!({
        "name": "main",
        "env": case.input["container_env"],
        "livenessProbe": { "exec": { "command": case.input["command"] } },
    });
    let command = match ProbeSpec::from_container(ProbeKind::Liveness, &container) {
        Ok(Some(ProbeSpec {
            handler: ProbeHandler::Exec { command },
            ..
        })) => json!(command),
        other => json!(format!("{other:?}")),
    };
    project(
        &case.expected,
        json!({ "command_sent_to_runtime": command }),
    )
}

fn unsent() -> Result<u16, ProbeIoError> {
    Err(ProbeIoError::Setup {
        stage: ProbeSetupStage::Request,
        reason: "the request could not be formed".into(),
    })
}

/// `prober_retries`: one run of an httpGet probe over a scripted prober,
/// counting attempts. An upstream error is an attempt that sent nothing
/// (Blind); success and failure are statuses.
async fn prober_retries(case: &Case) -> Answer {
    let script = Script::new(case.input["attempts"].as_array().into_iter().flatten().map(
        |a| match (text(a, "result").as_str(), a["error"].as_bool()) {
            ("success", Some(false)) => Ok(200),
            ("failure", Some(false)) => Ok(500),
            _ => unsent(),
        },
    ));
    let spec = probe(
        ProbeKind::Liveness,
        &json!({ "httpGet": { "path": "/", "port": 8080 } }),
    );
    let obs = run_handler(
        &spec,
        &FakeBackend::new(),
        &script,
        "cid",
        Some("127.0.0.1"),
    )
    .await;
    project(
        &case.expected,
        json!({
            "result": label(obs, None),
            "error": is_error(obs),
            "attempts_made": script.calls(),
        }),
    )
}

/// `exec_probe`: an exec handler through `run_handler`, one run per exit
/// status the row names.
async fn exec_probe(case: &Case) -> Answer {
    let input = &case.input;
    if !input["feature_gates"]["ExecProbeTimeout"]
        .as_bool()
        .unwrap_or(true)
    {
        return Answer::NotChecked;
    }
    let run_error = &input["run_error"];
    let execs: Vec<Exec> = match text(run_error, "type").as_str() {
        "" => vec![Exec::Exit(0)],
        "ExitError" => match input.get("exit_statuses").and_then(Value::as_array) {
            Some(codes) => codes
                .iter()
                .filter_map(Value::as_i64)
                .filter_map(|c| i32::try_from(c).ok())
                .map(Exec::Exit)
                .collect(),
            None => vec![Exec::Exit(
                run_error["exit_status"]
                    .as_i64()
                    .and_then(|c| i32::try_from(c).ok())
                    .unwrap_or(1),
            )],
        },
        "ErrCommandTimedOut" => vec![Exec::Hang],
        _ => vec![Exec::RuntimeError],
    };
    let rig = ExecRig::new().await;
    let spec = exec_probe_spec(ProbeKind::Liveness, &Value::Null);
    let mut labels = Vec::new();
    let mut errors = Vec::new();
    for exec in execs {
        rig.next(exec).await;
        let obs = rig.run(&spec).await;
        labels.push(label(obs, None));
        errors.push(is_error(obs));
    }
    project(
        &case.expected,
        json!({
            "result": one_label(&labels),
            "error": errors.iter().any(|e| *e),
        }),
    )
}

/// `http_status_classify`: `http_status_observation` for each status.
fn http_status_classify(case: &Case) -> Answer {
    let results: Vec<&str> = case.input["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(u16_of)
        .map(|status| label(http_status_observation(status), Some(status)))
        .collect();
    project(&case.expected, json!({ "results": results }))
}

/// The reply the row's `server` gives a request. `redirect_code` stands in
/// for a redirect that names none (the row then tries several).
fn reply_for(server: &Value, redirect_code: u16, head: &Head) -> Reply {
    if let Some(secs) = server.get("sleep_seconds").and_then(Value::as_u64) {
        return Reply::After(Duration::from_secs(secs), Box::new(Reply::status(200)));
    }
    if server
        .get("headers_sent_then_body_stalls_past_timeout")
        .is_some()
    {
        return Reply::HeadThenStall;
    }
    if let Some(redirect) = server.get("redirect") {
        if head.target == text(redirect, "from") {
            let code = redirect
                .get("code")
                .and_then(u16_of)
                .unwrap_or(redirect_code);
            return Reply::redirect(code, text(redirect, "to"));
        }
        if head.target == text(redirect, "to") {
            if let Some(code) = server.get("target_status").and_then(u16_of) {
                return Reply::status(code);
            }
            if let Some(target) = server.get("target") {
                return Reply::Status {
                    code: u16_of(&target["status"]).unwrap_or(200),
                    location: target["location"].as_str().map(str::to_owned),
                    body: text(target, "body"),
                };
            }
            if let Some(host) = server.get("success_requires_host").and_then(Value::as_str) {
                return if head.header("host") == Some(host) {
                    Reply::status(200)
                } else {
                    Reply::status(u16_of(&server["else_status"]).unwrap_or(400))
                };
            }
        }
        return Reply::status(404);
    }
    if server["status"].as_i64().is_some_and(|s| s < 0) {
        return Reply::Close;
    }
    if let Some(host) = server.get("host_header_required").and_then(Value::as_str)
        && head.header("host") != Some(host)
    {
        return Reply::status(400);
    }
    let body = match server.get("body_repeat") {
        Some(r) => text(r, "unit").repeat(
            r["count"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(0),
        ),
        None => text(server, "body"),
    };
    Reply::Status {
        code: u16_of(&server["status"]).unwrap_or(200),
        location: None,
        body,
    }
}

/// `http_probe`: the real prober against a server behaving as the row says.
async fn http_probe(case: &Case) -> Answer {
    let input = &case.input;
    let server = input["server"].clone();
    let timeout = input["timeout_seconds"].as_u64().unwrap_or(1);
    let path = server["redirect"]["from"]
        .as_str()
        .unwrap_or("/")
        .to_owned();

    // No server at all: connection refused.
    if server.is_null() {
        let port = closed_port().await;
        let (obs, status) = probe_http(json!({ "path": "/", "port": port }), timeout).await;
        return project(
            &case.expected,
            json!({ "result": label(obs, status), "error": is_error(obs) }),
        );
    }

    // One Host header per row: the row's outcome for each.
    if let Some(rows) = input["rows"].as_array() {
        let srv = serve(move |head| reply_for(&server, 302, head)).await;
        let mut results = Vec::new();
        for row in rows {
            let headers = http_headers(&json!({ "Host": [text(row, "Host")] }));
            let (obs, status) = probe_http(
                json!({ "path": path, "port": srv.port, "httpHeaders": headers }),
                timeout,
            )
            .await;
            results.push(label(obs, status));
        }
        return project(&case.expected, json!({ "results": results }));
    }

    // Each redirect code the row lists must give the same answer.
    let codes: Vec<u16> = match input["redirect_codes"].as_array() {
        Some(codes) => codes.iter().filter_map(u16_of).collect(),
        None => vec![302],
    };
    let headers = http_headers(&input["request_headers"]);
    let mut labels = Vec::new();
    let mut errors = Vec::new();
    let mut results = Vec::new();
    for code in codes {
        let server = server.clone();
        let srv = serve(move |head| reply_for(&server, code, head)).await;
        let (obs, status) = probe_http(
            json!({ "path": path, "port": srv.port, "httpHeaders": headers }),
            timeout,
        )
        .await;
        labels.push(label(obs, status));
        errors.push(is_error(obs));
        results.push(kubelet_result(obs));
    }
    project(
        &case.expected,
        json!({
            "result": one_label(&labels),
            "error": errors.iter().any(|e| *e),
            "kubelet_result": one_label(&results),
        }),
    )
}

/// `http_redirect_policy`: where a redirect is followed. A first server
/// redirects `/redirect?loc=X` to X; a second, on another port of the same
/// host, answers `/success` and `/fail`. The row's `http://0.0.0.0/fail`
/// stands for "a different hostname"; `localhost` on the second server's port
/// is one that answers, so following it would be seen.
async fn http_redirect_policy(case: &Case) -> Answer {
    let input = &case.input;
    if input["follow_non_local_redirects"].as_bool() != Some(false) {
        return Answer::NotChecked;
    }
    let other = serve(|head| match head.target.as_str() {
        "/success" => Reply::status(200),
        "/fail" => Reply::status(500),
        _ => Reply::status(404),
    })
    .await;
    let first = serve(|head| {
        let target = head.target.as_str();
        match target {
            "/success" => Reply::status(200),
            "/fail" => Reply::status(500),
            "/loop" => Reply::redirect(302, "/loop"),
            _ => match target.strip_prefix("/redirect?loc=") {
                Some(loc) => Reply::redirect(302, loc),
                None => Reply::status(404),
            },
        }
    })
    .await;
    let mut results = Vec::new();
    for row in input["rows"].as_array().into_iter().flatten() {
        let to = text(row, "redirect_to");
        let loc = match to.as_str() {
            "<same-hostname:other-port>/success" => {
                format!("http://127.0.0.1:{}/success", other.port)
            }
            "<same-hostname:other-port>/fail" => format!("http://127.0.0.1:{}/fail", other.port),
            "http://0.0.0.0/fail" => format!("http://localhost:{}/fail", other.port),
            local => local.to_owned(),
        };
        let (obs, status) = probe_http(
            json!({ "path": format!("/redirect?loc={loc}"), "port": first.port }),
            1,
        )
        .await;
        results.push(label(obs, status));
    }
    project(&case.expected, json!({ "results": results }))
}

/// `http_request_headers`: what reaches the server, per the probe's
/// `httpHeaders`. For the override rows the needle is upstream's own line:
/// engenho's answer is that line when the request did (or did not) carry it,
/// and everything it did carry when not.
async fn http_request_headers(case: &Case, broken_pins: &mut Vec<String>) -> Answer {
    let input = &case.input;
    let srv = serve(|_| Reply::status(200)).await;
    let request = |headers: &Value| json!({ "path": "/", "port": srv.port, "httpHeaders": http_headers(headers) });

    let Some(rows) = input["rows"].as_array() else {
        let _ = probe_http(request(&input["user_headers"]), 1).await;
        let head = srv.last_head();
        let mut names: Vec<String> = head
            .headers
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| n != "Host")
            .collect();
        names.sort();
        names.dedup();
        let agent = head.header("user-agent").unwrap_or_default();
        let prefix = if agent.starts_with("kube-probe/") {
            "kube-probe/"
        } else {
            agent
        };
        return project(
            &case.expected,
            json!({
                "header_names_received": names,
                "Accept": head.header("accept"),
                "User-Agent_prefix": prefix,
                "no_Accept-Encoding": head.header("accept-encoding").is_none(),
            }),
        );
    };

    let expected_rows = case.expected["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for (row, want) in rows.iter().zip(&expected_rows) {
        let _ = probe_http(request(&row["user_headers"]), 1).await;
        let head = srv.last_head();
        let echo = head.echo();
        let mut got = Map::new();
        if let Some(needle) = want["received_contains"].as_str() {
            let seen = if echo.contains(needle) { needle } else { &echo };
            got.insert("received_contains".into(), json!(seen));
        }
        if let Some(needle) = want["received_not_contains"].as_str() {
            let seen = if echo.contains(needle) { &echo } else { needle };
            got.insert("received_not_contains".into(), json!(seen));
        }
        if want.get("request_host").is_some() {
            got.insert("request_host".into(), json!(head.header("host")));
        }
        let got = Value::Object(got);
        // The row's declared deviation is ONE sub-case, pinned here so that it
        // cannot cover any other: a probe setting Accept EMPTY reaches the
        // workload as reqwest's `Accept: */*`, and nothing else differs.
        let emptied_accept =
            want["received_not_contains"] == "Accept:" && head.header("accept") == Some("*/*");
        if &got != want && !emptied_accept {
            broken_pins.push(format!(
                "  - {}: only an emptied Accept may differ from upstream; row {row} \
                 expected {want}, got {got}",
                case.name
            ));
        }
        out.push(got);
    }
    project(&case.expected, json!({ "rows": out }))
}

/// `http_request_url`: the URL `TokioNetProber` requests — `ProbeUrl` of the
/// target `run_handler` built from the parsed probe and the pod IP.
async fn http_request_url(case: &Case) -> Answer {
    let mut urls = Vec::new();
    for row in case.input["rows"].as_array().into_iter().flatten() {
        let spec = probe(
            ProbeKind::Readiness,
            &json!({ "httpGet": {
                "scheme": row["scheme"],
                "host": row["host"],
                "port": row["port"],
                "path": row["path"],
            }}),
        );
        let net = Recording::new(FakeNetProber::new());
        let pod_ip = text(row, "pod_ip");
        let _ = run_handler(&spec, &FakeBackend::new(), &net, "cid", Some(&pod_ip)).await;
        urls.push(
            net.last_http()
                .map(|t| ProbeUrl(&t).to_string())
                .unwrap_or_default(),
        );
    }
    project(&case.expected, json!({ "urls": urls }))
}

/// `resolve_container_port`: the port `ProbeSpec::from_k8s` resolves for a
/// tcpSocket probe against the row's container ports. Each answer row is cut
/// to the keys of upstream's row, as the whole-table comparison is.
fn resolve_container_port(case: &Case) -> Answer {
    let ports: Vec<(String, u16)> = case.input["container_ports"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| Some((p["name"].as_str()?.to_owned(), u16_of(&p["containerPort"])?)))
        .collect();
    let expected_rows = case.expected["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let rows: Vec<Value> = case.input["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .zip(&expected_rows)
        .map(|(row, want)| {
            let port = row.get("int").or_else(|| row.get("str")).cloned();
            let spec = ProbeSpec::from_k8s(
                ProbeKind::Readiness,
                &json!({ "tcpSocket": { "port": port } }),
                &ports,
            );
            let got = match spec.map(|s| s.handler) {
                Ok(ProbeHandler::TcpSocket { port: Ok(p), .. }) => {
                    json!({ "ok": true, "port": p.get() })
                }
                Ok(ProbeHandler::TcpSocket { port: Err(e), .. }) => {
                    json!({ "ok": false, "error": e.to_string() })
                }
                other => json!({ "unexpected": format!("{other:?}") }),
            };
            match project(want, got) {
                Answer::Checked(v) => v,
                Answer::NotChecked => Value::Null,
            }
        })
        .collect();
    project(&case.expected, json!({ "rows": rows }))
}

/// `end_to_end`: a probe naming a port its container does not declare, for
/// each kind — parsed, then run and folded `ticks` times against a workload
/// that would pass if it were asked.
async fn end_to_end(case: &Case) -> Answer {
    let ticks = case.input["ticks"].as_u64().unwrap_or(100);
    let port = case.input["port"]["str"].clone();
    let mut cached = Map::new();
    for kind in ProbeKind::ALL {
        let mut container = Map::new();
        container.insert("name".into(), json!("main"));
        container.insert("ports".into(), case.input["container_ports"].clone());
        container.insert(kind.field().into(), json!({ "httpGet": { "port": port } }));
        let spec = match ProbeSpec::from_container(kind, &Value::Object(container)) {
            Ok(Some(spec)) => spec,
            // Refused at parse: the pod never runs. A disagreement, answered.
            other => {
                cached.insert(kind.as_str().into(), json!(format!("refused: {other:?}")));
                continue;
            }
        };
        let net = Script::new([]);
        let now = Instant::now();
        let mut rt = ProbeRuntime::new(now);
        let mut tripped = false;
        for _ in 0..ticks {
            let obs = run_handler(&spec, &FakeBackend::new(), &net, "cid", Some("127.0.0.1")).await;
            tripped |= fold_probe_observation(&spec, &mut rt, obs, now)
                .trip
                .is_some();
        }
        cached.insert(
            kind.as_str().into(),
            json!(cached_result(kind, &rt, tripped)),
        );
    }
    project(
        &case.expected,
        json!({ "cached_result_forever": Value::Object(cached) }),
    )
}

/// `tcp_probe`: the real prober against a listening or a closed port.
async fn tcp_probe(case: &Case) -> Answer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let _listening = case.input["listener"]
        .as_bool()
        .unwrap_or(false)
        .then_some(listener);
    let spec = probe(
        ProbeKind::Readiness,
        &json!({ "tcpSocket": { "port": port } }),
    );
    let obs = run_handler(
        &spec,
        &FakeBackend::new(),
        &TokioNetProber::new(),
        "cid",
        Some("127.0.0.1"),
    )
    .await;
    project(
        &case.expected,
        json!({ "result": label(obs, None), "error": is_error(obs) }),
    )
}

/// `tcp_target`: the address `run_handler` hands the prober to dial.
async fn tcp_target(case: &Case) -> Answer {
    let input = &case.input;
    let spec = probe(
        ProbeKind::Readiness,
        &json!({ "tcpSocket": { "port": input["port"], "host": input["tcp_host"] } }),
    );
    let net = Recording::new(FakeNetProber::new());
    let pod_ip = text(input, "pod_ip");
    let _ = run_handler(&spec, &FakeBackend::new(), &net, "cid", Some(&pod_ip)).await;
    let dial = net.last_tcp().map(|t| {
        HostPort {
            host: &t.host,
            port: t.port,
        }
        .to_string()
    });
    project(&case.expected, json!({ "dial": dial }))
}

/// `is_container_started`: `container_started` over a startup probe left in
/// the row's cached state by folding that verdict.
fn is_container_started(case: &Case) -> Answer {
    let now = Instant::now();
    let spec = probe(
        ProbeKind::Startup,
        &json!({ "exec": { "command": ["probe"] }, "failureThreshold": 1 }),
    );
    let started: Vec<bool> = case.input["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            let mut rt = ProbeRuntime::new(now);
            let verdict = match row["startup_cached"].as_str() {
                Some("Success") => Some(ProbeObservation::Success),
                Some("Failure") => Some(ProbeObservation::Failure),
                _ => None,
            };
            if let Some(obs) = verdict {
                let _ = fold_probe_observation(&spec, &mut rt, obs, now);
            }
            let startup = row["startup_worker"].as_bool().unwrap_or(false);
            container_started(
                row["running"].as_bool().unwrap_or(false),
                startup.then_some(&rt),
            )
        })
        .collect();
    project(&case.expected, json!({ "started": started }))
}

/// `probe_defaults`: the timing `ProbeSpec::from_k8s` applies to the row's
/// fields.
fn probe_defaults(case: &Case) -> Answer {
    let mut fields = case.input["probe"].as_object().cloned().unwrap_or_default();
    fields.insert("exec".into(), json!({ "command": ["probe"] }));
    let t = probe(ProbeKind::Readiness, &Value::Object(fields)).timing;
    project(
        &case.expected,
        json!({
            "timeoutSeconds": t.timeout.as_secs(),
            "periodSeconds": t.period.as_secs(),
            "successThreshold": t.success_threshold,
            "failureThreshold": t.failure_threshold,
            "initialDelaySeconds": t.initial_delay.as_secs(),
        }),
    )
}
