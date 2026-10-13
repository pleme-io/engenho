//! THE KUBELET HTTP API (:10250) — the node's own surface.
//!
//! ★ WHY ITS ABSENCE WAS LOAD-BEARING. Container logs work today only
//! because the kubelet happens to live in the SAME PROCESS as the
//! apiserver: `KubeletLogReader` calls straight into it. That is an
//! accident of the single-binary layout, not a design — the moment there
//! is a second node, `kubectl logs` against a pod on it has no path at
//! all. Every operator's first three commands are `get`, `describe` and
//! `logs`, and the third one silently only works on one node.
//!
//! Measured on cid 2026-08-29: an MCP log read returned zero lines for a
//! pod that was producing output, which is this gap seen from the outside.
//!
//! ★ THE URL SHAPE IS A CONTRACT, NOT A CHOICE. `kubectl logs` builds
//! `/containerLogs/{namespace}/{pod}/{container}` and passes `tailLines`,
//! `timestamps`, `follow`, `previous` and `sinceSeconds` as query
//! parameters. A server that answers a *tidier* path answers nothing that
//! exists: kubectl does not discover the route, it hardcodes it.
//!
//! ★ WHAT IS AND IS NOT SERVED, stated plainly rather than discovered.
//! `/containerLogs`, `/pods`, `/runningpods` and `/healthz` are served.
//! `/exec` and `/attach` speak `v5.channel.k8s.io` (and `v4`, kubectl's
//! fallback) over WebSocket: stdin, a real pty for `-t` with resizes, the
//! v5 close-stdin signal `kubectl cp` relies on, and the exit status as a
//! `Status` on the error channel. `/portForward` speaks upstream's WebSocket
//! port-forward protocol (`?ports=`, a data and an error channel per port).
//! A runtime that cannot stream answers with a `Status` naming why, on the
//! error channel, never with a session that hangs.

use std::net::IpAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::backend::{ExecOutcome, LogOptions};
use crate::exec_channel::{
    Channel, FrameError, SUBPROTOCOL_V4, SUBPROTOCOL_V5, decode_from_client, encode, parse_resize,
};
use crate::exec_session::{
    ExecPlan, ExecQuery, ExecRefusal, backend_failure_frame, plan, plan_attach, status_frame,
};
use crate::port_forward::{self, Inbound, PortForwardRefusal};
use crate::stream::{StreamRequest, StreamSession, StreamVerb};

/// What the HTTP surface needs from the running kubelet.
///
/// A trait rather than the concrete `Kubelet` so the server is testable
/// against `FakeBackend` without a container runtime — the same seam the
/// rest of this crate already uses.
#[async_trait::async_trait]
pub trait KubeletApi: Send + Sync + 'static {
    /// Logs for one container of one pod.
    async fn container_logs(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        opts: &LogOptions,
    ) -> Result<String, String>;

    /// Every pod this kubelet is managing, as a `v1.PodList`.
    async fn pods(&self) -> serde_json::Value;

    /// Only the pods with at least one running container.
    async fn running_pods(&self) -> serde_json::Value;

    /// Run `argv` inside one container of one pod.
    ///
    /// Mirrors [`crate::backend::ContainerRuntime::exec`]'s split
    /// deliberately: `Ok` with a non-zero `exit_code` is a command that RAN
    /// and failed, `Err` is a command that could not be run at all. The
    /// exec stream renders those as two different objects, and collapsing
    /// them here would make "no such container" indistinguishable from
    /// "your program returned 1".
    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        argv: &[String],
    ) -> Result<ExecOutcome, String>;

    /// Start a streaming exec: the process's stdio wired to a live session.
    async fn exec_stream(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        req: &StreamRequest,
    ) -> Result<StreamSession, String>;

    /// Attach to a running container's main process.
    async fn attach(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        req: &StreamRequest,
    ) -> Result<StreamSession, String>;

    /// The address a port-forward to this pod dials.
    async fn pod_address(&self, namespace: &str, pod: &str) -> Result<IpAddr, String>;
}

/// Query parameters `kubectl logs` sends.
///
/// Field names are upstream's camelCase wire spelling. Renaming them to
/// snake_case would silently ignore every parameter kubectl sends — the
/// server would answer, and answer wrongly, which is worse than a 400.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogQuery {
    pub tail_lines: Option<u32>,
    #[serde(default)]
    pub timestamps: bool,
    #[serde(default)]
    pub follow: bool,
    #[serde(default)]
    pub previous: bool,
    pub since_seconds: Option<u64>,
}

impl LogQuery {
    /// The backend options this query maps to.
    ///
    /// `follow`, `previous` and `sinceSeconds` are accepted and IGNORED
    /// rather than rejected, deliberately: `kubectl logs -f` should return
    /// the log it can rather than fail outright, and every one of these is
    /// additive — ignoring them yields a SUBSET of the right answer, never
    /// a wrong one. That asymmetry is what makes ignoring safe here and
    /// unsafe for a field selector.
    #[must_use]
    pub fn to_options(&self) -> LogOptions {
        LogOptions {
            tail: self.tail_lines,
            timestamps: self.timestamps,
            from_byte: None,
        }
    }
}

/// Router state.
#[derive(Clone)]
pub struct KubeletServer {
    api: Arc<dyn KubeletApi>,
}

impl KubeletServer {
    #[must_use]
    pub fn new(api: Arc<dyn KubeletApi>) -> Self {
        Self { api }
    }

    /// The kubelet's HTTP routes.
    ///
    /// Paths are upstream's exactly, because kubectl hardcodes them.
    #[must_use]
    pub fn routes(self) -> Router {
        Router::new()
            .route(
                "/containerLogs/:namespace/:pod/:container",
                get(container_logs),
            )
            .route("/pods", get(pods))
            .route("/runningpods/", get(running_pods))
            .route("/healthz", get(healthz))
            // Upstream's own segment shapes: exec/attach take
            // {namespace}/{pod}/{container}, portForward takes
            // {namespace}/{pod}.
            .route("/exec/:namespace/:pod/:container", get(exec))
            .route("/attach/:namespace/:pod/:container", get(attach))
            .route("/portForward/:namespace/:pod", get(port_forward))
            .with_state(self)
    }
}

async fn container_logs(
    State(s): State<KubeletServer>,
    Path((namespace, pod, container)): Path<(String, String, String)>,
    Query(q): Query<LogQuery>,
) -> impl IntoResponse {
    match s
        .api
        .container_logs(&namespace, &pod, &container, &q.to_options())
        .await
    {
        Ok(body) => (StatusCode::OK, body).into_response(),
        // The reason travels in the body: kubectl prints it verbatim, and
        // it is the only thing an operator sees.
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn pods(State(s): State<KubeletServer>) -> impl IntoResponse {
    axum::Json(s.api.pods().await)
}

async fn running_pods(State(s): State<KubeletServer>) -> impl IntoResponse {
    axum::Json(s.api.running_pods().await)
}

/// The subprotocol offer a request carries.
fn offered(headers: &axum::http::HeaderMap) -> &str {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// The answer to a stream request that is not a WebSocket upgrade.
fn needs_upgrade(what: &str, protocols: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        format!("{what} requires a WebSocket upgrade offering {protocols}"),
    )
        .into_response()
}

/// `kubectl exec` — upgrade to channel framing and run the command.
async fn exec(
    State(s): State<KubeletServer>,
    Path((namespace, pod, container)): Path<(String, String, String)>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    // ★ OPTIONAL ON PURPOSE. As a bare extractor, `WebSocketUpgrade`
    // rejects any non-upgrade request BEFORE this handler runs — which
    // makes every refusal below unreachable and answers a plain `curl` to
    // this path with a generic 400 that names nothing. Optional keeps the
    // typed reason as the thing a client actually receives.
    ws: Option<WebSocketUpgrade>,
) -> axum::response::Response {
    let query = ExecQuery::parse(raw.as_deref().unwrap_or(""));
    // ★ REFUSE BEFORE UPGRADING. A refusal after the upgrade is a socket
    // that opens and immediately closes, which kubectl reports as a
    // connection error with no reason attached.
    let plan = match plan(&query, offered(&headers)) {
        Ok(p) => p,
        Err(e) => return refusal(&e),
    };
    stream_route(s, StreamVerb::Exec, namespace, pod, container, plan, ws)
}

/// `kubectl attach` — the same channel framing, on the container's main
/// process instead of a new one.
async fn attach(
    State(s): State<KubeletServer>,
    Path((namespace, pod, container)): Path<(String, String, String)>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    ws: Option<WebSocketUpgrade>,
) -> axum::response::Response {
    let query = ExecQuery::parse(raw.as_deref().unwrap_or(""));
    let plan = match plan_attach(&query, offered(&headers)) {
        Ok(p) => p,
        Err(e) => return refusal(&e),
    };
    stream_route(s, StreamVerb::Attach, namespace, pod, container, plan, ws)
}

fn refusal(e: &ExecRefusal) -> axum::response::Response {
    let code = match e {
        ExecRefusal::NoCommand => StatusCode::BAD_REQUEST,
        ExecRefusal::SubprotocolUnsupported => StatusCode::NOT_IMPLEMENTED,
    };
    (code, e.to_string()).into_response()
}

/// Upgrade and hand the socket to the session pump.
///
/// ★ THE PATH'S CONTAINER WINS over any `?container=`. On the kubelet's own
/// surface the path IS the contract — the apiserver resolves which container
/// and puts it there.
fn stream_route(
    s: KubeletServer,
    verb: StreamVerb,
    namespace: String,
    pod: String,
    container: String,
    plan: ExecPlan,
    ws: Option<WebSocketUpgrade>,
) -> axum::response::Response {
    let Some(ws) = ws else {
        return needs_upgrade(
            &verb.to_string(),
            &format!("{SUBPROTOCOL_V5} or {SUBPROTOCOL_V4}"),
        );
    };
    ws.protocols([plan.protocol.as_str()])
        .on_upgrade(move |socket| async move {
            let req = plan.request();
            let session = match verb {
                StreamVerb::Exec => s.api.exec_stream(&namespace, &pod, &container, &req).await,
                StreamVerb::Attach => s.api.attach(&namespace, &pod, &container, &req).await,
            };
            pump_session(socket, &plan, session).await;
        })
}

/// Receive from an output that may already be closed; a closed one never
/// resolves, so `select!` keeps serving the others.
async fn next_chunk(rx: &mut Option<mpsc::Receiver<Vec<u8>>>) -> Option<Vec<u8>> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

/// Carry one session over the socket until the process ends.
///
/// Output first, status last: the terminating `Status` is sent only after
/// every output stream has reached EOF AND the process has exited, so a
/// client never sees the exit before the last line the process printed.
async fn pump_session(
    mut socket: WebSocket,
    plan: &ExecPlan,
    session: Result<StreamSession, String>,
) {
    let session = match session {
        Ok(s) => s,
        Err(reason) => {
            let _ = socket
                .send(Message::Binary(backend_failure_frame(&reason)))
                .await;
            let _ = socket.close().await;
            return;
        }
    };
    let StreamSession {
        mut stdin,
        mut stdout,
        mut stderr,
        resize,
        mut exit,
    } = session;
    let mut finished = None;
    let mut client_open = true;
    while stdout.is_some() || stderr.is_some() || finished.is_none() {
        tokio::select! {
            chunk = next_chunk(&mut stdout), if stdout.is_some() => match chunk {
                Some(b) => {
                    if socket.send(Message::Binary(encode(Channel::Stdout, &b))).await.is_err() {
                        return;
                    }
                }
                None => stdout = None,
            },
            chunk = next_chunk(&mut stderr), if stderr.is_some() => match chunk {
                Some(b) => {
                    if socket.send(Message::Binary(encode(Channel::Stderr, &b))).await.is_err() {
                        return;
                    }
                }
                None => stderr = None,
            },
            ended = &mut exit, if finished.is_none() => finished = Some(ended),
            msg = socket.recv(), if client_open => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    match decode_from_client(&bytes, plan.protocol) {
                        Ok(f) => match f.channel {
                            Channel::Stdin => {
                                if let Some(tx) = &stdin
                                    && tx.send(f.payload).await.is_err()
                                {
                                    stdin = None;
                                }
                            }
                            Channel::Resize => {
                                if let (Some(tx), Some(size)) = (&resize, parse_resize(&f.payload)) {
                                    let _ = tx.send(size).await;
                                }
                            }
                            Channel::Close => {
                                if f.payload.first() == Some(&Channel::Stdin.as_byte()) {
                                    stdin = None;
                                }
                            }
                            Channel::Stdout | Channel::Stderr | Channel::Error => {}
                        },
                        Err(FrameError::Empty) => {}
                        Err(e) => tracing::debug!(error = %e, "dropped a client frame"),
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => {
                    client_open = false;
                    stdin = None;
                }
                Some(Ok(_)) => {}
            },
        }
    }
    let frame = match finished {
        Some(Ok(exit)) => status_frame(exit),
        Some(Err(e)) => backend_failure_frame(&e.to_string()),
        None => return,
    };
    let _ = socket.send(Message::Binary(frame)).await;
    let _ = socket.close().await;
}

/// `kubectl port-forward` — one WebSocket, a data and an error channel per
/// requested port, each dialled on the pod's address.
async fn port_forward(
    State(s): State<KubeletServer>,
    Path((namespace, pod)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    ws: Option<WebSocketUpgrade>,
) -> axum::response::Response {
    let ports = match port_forward::parse_ports(raw.as_deref().unwrap_or("")) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let Some(protocol) = port_forward::negotiate(offered(&headers)) else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            PortForwardRefusal::SubprotocolUnsupported.to_string(),
        )
            .into_response();
    };
    let Some(ws) = ws else {
        return needs_upgrade("portForward", &port_forward::SUBPROTOCOLS.join(" or "));
    };
    ws.protocols([protocol]).on_upgrade(move |socket| async move {
        let addr = s.api.pod_address(&namespace, &pod).await;
        pump_port_forward(socket, &namespace, &pod, addr, &ports).await;
    })
}

/// Carry every port's TCP stream over the socket until each has closed.
async fn pump_port_forward(
    mut socket: WebSocket,
    namespace: &str,
    pod: &str,
    addr: Result<IpAddr, String>,
    ports: &[u16],
) {
    let (out_tx, mut out_rx) = mpsc::channel::<Option<(u8, Vec<u8>)>>(64);
    let mut writers: Vec<Option<mpsc::Sender<Vec<u8>>>> = Vec::with_capacity(ports.len());
    let mut live = 0usize;
    for (i, &port) in ports.iter().enumerate() {
        let data = port_forward::data_channel(i);
        let error = port_forward::error_channel(i);
        for ch in [data, error] {
            if socket
                .send(Message::Binary(port_forward::port_frame(ch, port)))
                .await
                .is_err()
            {
                return;
            }
        }
        let dialled = match &addr {
            Ok(ip) => tokio::net::TcpStream::connect((*ip, port))
                .await
                .map_err(|e| e.to_string()),
            Err(reason) => Err(reason.clone()),
        };
        match dialled {
            Ok(stream) => {
                let (read, write) = stream.into_split();
                writers.push(Some(crate::stream::pump_writer(write)));
                let mut rx = crate::stream::pump_reader(read);
                let tx = out_tx.clone();
                live += 1;
                tokio::spawn(async move {
                    while let Some(chunk) = rx.recv().await {
                        if tx.send(Some((data, chunk))).await.is_err() {
                            return;
                        }
                    }
                    let _ = tx.send(None).await;
                });
            }
            Err(reason) => {
                writers.push(None);
                let msg = format!(
                    "error forwarding port {port} to pod {namespace}/{pod}: {reason}"
                );
                if socket
                    .send(Message::Binary(port_forward::frame(error, msg.as_bytes())))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
    drop(out_tx);
    while live > 0 {
        tokio::select! {
            out = out_rx.recv() => match out {
                Some(Some((ch, chunk))) => {
                    if socket.send(Message::Binary(port_forward::frame(ch, &chunk))).await.is_err() {
                        return;
                    }
                }
                Some(None) => live -= 1,
                None => break,
            },
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    let Some((&ch, payload)) = bytes.split_first() else { continue };
                    if let Inbound::Data(i) = port_forward::route(ch, ports.len()) {
                        if let Some(Some(tx)) = writers.get(i) {
                            if tx.send(payload.to_vec()).await.is_err() {
                                writers[i] = None;
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
        }
    }
    let _ = socket.close().await;
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    struct FakeApi;

    #[async_trait::async_trait]
    impl KubeletApi for FakeApi {
        async fn container_logs(
            &self,
            namespace: &str,
            pod: &str,
            container: &str,
            opts: &LogOptions,
        ) -> Result<String, String> {
            if pod == "missing" {
                return Err("pod not found".to_string());
            }
            Ok(format!(
                "{namespace}/{pod}/{container} tail={:?} ts={}",
                opts.tail, opts.timestamps
            ))
        }
        async fn pods(&self) -> serde_json::Value {
            serde_json::json!({ "kind": "PodList", "items": [{ "metadata": { "name": "a" } }] })
        }
        async fn exec(
            &self,
            _ns: &str,
            _pod: &str,
            _container: &str,
            argv: &[String],
        ) -> Result<crate::backend::ExecOutcome, String> {
            if argv.first().map(String::as_str) == Some("missing") {
                return Err("no such container".into());
            }
            Ok(crate::backend::ExecOutcome {
                exit_code: 0,
                stdout: argv.join(" "),
                stderr: String::new(),
            })
        }

        async fn running_pods(&self) -> serde_json::Value {
            serde_json::json!({ "kind": "PodList", "items": [] })
        }

        async fn exec_stream(
            &self,
            _ns: &str,
            _pod: &str,
            _container: &str,
            _req: &StreamRequest,
        ) -> Result<StreamSession, String> {
            Err("not in this fake".into())
        }

        async fn attach(
            &self,
            _ns: &str,
            _pod: &str,
            _container: &str,
            _req: &StreamRequest,
        ) -> Result<StreamSession, String> {
            Err("not in this fake".into())
        }

        async fn pod_address(&self, _ns: &str, _pod: &str) -> Result<IpAddr, String> {
            Ok(IpAddr::from([127, 0, 0, 1]))
        }
    }

    fn app() -> Router {
        KubeletServer::new(Arc::new(FakeApi)).routes()
    }

    /// A GET carrying the v5 subprotocol offer but NO upgrade — enough to
    /// reach every refusal, which is the point of them being reachable.
    async fn get_v5(uri: &str) -> (StatusCode, String) {
        let res = app()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(
                        "sec-websocket-protocol",
                        crate::exec_channel::SUBPROTOCOL_V5,
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("routes");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn get_body(uri: &str) -> (StatusCode, String) {
        let res = app()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .expect("routes");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn the_log_path_is_the_one_kubectl_hardcodes() {
        // kubectl does not DISCOVER this route, it builds it. A tidier
        // path would answer nothing that exists.
        let (status, body) = get_body("/containerLogs/default/nginx/app").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.starts_with("default/nginx/app"), "got: {body}");
    }

    #[tokio::test]
    async fn query_parameters_use_upstreams_camel_case_spelling() {
        // Renaming these to snake_case would silently ignore everything
        // kubectl sends — the server would answer, and answer WRONGLY.
        let (_, body) =
            get_body("/containerLogs/default/nginx/app?tailLines=5&timestamps=true").await;
        assert!(body.contains("tail=Some(5)"), "got: {body}");
        assert!(body.contains("ts=true"), "got: {body}");
    }

    #[tokio::test]
    async fn unsupported_log_options_are_ignored_not_rejected() {
        // follow/previous/sinceSeconds are ADDITIVE: ignoring them yields a
        // SUBSET of the right answer, never a wrong one. `kubectl logs -f`
        // should return what it can rather than fail outright.
        let (status, _) =
            get_body("/containerLogs/default/nginx/app?follow=true&previous=true&sinceSeconds=60")
                .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn a_failure_reason_reaches_the_operator_verbatim() {
        // kubectl prints the body, and it is the only thing they see.
        let (status, body) = get_body("/containerLogs/default/missing/app").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "pod not found");
    }

    #[tokio::test]
    async fn pods_and_runningpods_are_distinct_endpoints() {
        let (s1, b1) = get_body("/pods").await;
        assert_eq!(s1, StatusCode::OK);
        assert!(b1.contains("PodList"));
        assert!(b1.contains("\"name\":\"a\""), "got: {b1}");

        // Upstream's path has a TRAILING SLASH. Without it kubectl's
        // request 404s against a server that clearly implements it.
        let (s2, b2) = get_body("/runningpods/").await;
        assert_eq!(s2, StatusCode::OK);
        assert!(b2.contains("PodList"));
    }

    #[tokio::test]
    async fn healthz_answers_so_a_probe_can_tell_the_kubelet_is_up() {
        let (status, body) = get_body("/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn interactive_exec_is_planned_and_only_the_missing_upgrade_is_reported() {
        let (status, body) = get_v5("/exec/default/p/c?command=sh&stdin=true&tty=true").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("WebSocket upgrade"), "{body}");
    }

    #[tokio::test]
    async fn exec_without_a_command_is_a_400_not_a_501() {
        // A missing parameter is the client's error; a missing capability
        // is ours. Reporting both the same way sends an operator to file a
        // bug about their own typo.
        let (status, _) = get_v5("/exec/default/p/c?command=").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_plain_get_to_exec_says_it_needs_an_upgrade() {
        // Without `Option<WebSocketUpgrade>` this is axum's own generic
        // rejection, naming nothing, and every refusal above is
        // unreachable. This test is what pins that choice.
        let (status, body) = get_v5("/exec/default/p/c?command=ls").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("WebSocket upgrade"), "got: {body}");
    }

    #[tokio::test]
    async fn a_client_without_v5_is_refused_by_capability_not_by_axum() {
        let (status, body) = get_body("/exec/default/p/c?command=ls").await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("v5.channel.k8s.io"), "got: {body}");
    }

    #[tokio::test]
    async fn attach_and_port_forward_answer_with_a_reason_not_a_404() {
        let (s, body) = get_body("/attach/default/nginx/app").await;
        assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("v5.channel.k8s.io"), "{body}");

        let (s, body) = get_v5("/attach/default/nginx/app?stdout=true").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(body.contains("WebSocket upgrade"), "{body}");

        let (s, body) = get_body("/portForward/default/nginx").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(body.contains("ports"), "{body}");

        let (s, body) = get_body("/portForward/default/nginx?ports=80").await;
        assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("subprotocol"), "{body}");
    }
}
