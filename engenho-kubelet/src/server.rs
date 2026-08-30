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
//! `/exec` is served over the `v5.channel.k8s.io` WebSocket subprotocol
//! for NON-interactive commands — `kubectl exec <pod> -- <cmd>` — which is
//! the overwhelming majority of real exec traffic. `-i` and `-t` are
//! REFUSED with a reason rather than accepted and served with batch
//! semantics; see [`crate::exec_session`] for why a refusal beats a hang.
//! `/attach` and `/portForward` remain a typed 501: both are inherently
//! interactive and there is nothing honest to serve them with yet.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Deserialize;

use crate::backend::{ExecOutcome, LogOptions};
use crate::exec_channel::SUBPROTOCOL_V5;
use crate::exec_session::{ExecQuery, ExecRefusal, backend_failure_frame, plan, session_frames};

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
            // Stream-multiplexed endpoints. A typed 501 rather than a
            // missing route: a 404 would read as "wrong kubelet", sending
            // an operator to debug their URL instead of learning the
            // capability is absent.
            // Upstream's own segment shapes: exec/attach take
            // {namespace}/{pod}/{container}, portForward takes
            // {namespace}/{pod}.
            .route("/exec/:namespace/:pod/:container", get(exec))
            .route(
                "/attach/:namespace/:pod/:container",
                get(stream_unsupported),
            )
            .route("/portForward/:namespace/:pod", get(stream_unsupported))
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

/// `kubectl exec` — upgrade to v5 channel framing and run the command.
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
    let offered = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let query = ExecQuery::parse(raw.as_deref().unwrap_or(""));

    // ★ REFUSE BEFORE UPGRADING. A refusal after the upgrade is a socket
    // that opens and immediately closes, which kubectl reports as a
    // connection error with no reason attached. Answering with a plain
    // HTTP status keeps the explanation attached to the request.
    let plan = match plan(&query, offered) {
        Ok(p) => p,
        Err(e) => {
            let code = match e {
                ExecRefusal::NoCommand => StatusCode::BAD_REQUEST,
                ExecRefusal::StdinUnsupported
                | ExecRefusal::TtyUnsupported
                | ExecRefusal::SubprotocolUnsupported => StatusCode::NOT_IMPLEMENTED,
            };
            return (code, e.to_string()).into_response();
        }
    };

    let Some(ws) = ws else {
        return (
            StatusCode::BAD_REQUEST,
            format!("exec requires a WebSocket upgrade offering {SUBPROTOCOL_V5}"),
        )
            .into_response();
    };

    // ★ THE PATH'S CONTAINER WINS over any `?container=`. On the kubelet's
    // own surface the path IS the contract — the apiserver resolves which
    // container and puts it there. The query field exists because the
    // apiserver-side subresource reuses this parser, where it is the only
    // place the name appears.
    let _ = &plan.container;
    ws.protocols([SUBPROTOCOL_V5])
        .on_upgrade(move |socket| async move {
            run_exec(socket, s, namespace, pod, container, plan).await;
        })
}

async fn run_exec(
    mut socket: WebSocket,
    s: KubeletServer,
    namespace: String,
    pod: String,
    container: String,
    plan: crate::exec_session::ExecPlan,
) {
    let frames = match s.api.exec(&namespace, &pod, &container, &plan.argv).await {
        Ok(outcome) => session_frames(&plan, &outcome),
        Err(reason) => vec![backend_failure_frame(&reason)],
    };
    for frame in frames {
        if socket.send(Message::Binary(frame)).await.is_err() {
            // The client hung up mid-stream. Nothing to report to and
            // nothing to clean up: the command already ran to completion.
            return;
        }
    }
    let _ = socket.close().await;
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

async fn stream_unsupported() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "this kubelet does not serve exec/attach/portForward: they require SPDY or WebSocket \
         stream multiplexing, and a half-implemented stream protocol hangs a client rather than \
         failing it. Use the container runtime directly on the node.",
    )
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
    async fn exec_refuses_interactive_before_upgrading() {
        // The load-bearing shape: a plain HTTP status carrying the reason,
        // NOT an upgrade that opens and immediately closes.
        let (status, body) = get_v5("/exec/default/p/c?command=sh&stdin=true&tty=true").await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("stdin"), "{body}");
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
    async fn attach_and_port_forward_are_still_501_with_a_reason() {
        // A 404 reads as "wrong kubelet" and sends an operator to debug
        // their URL instead of learning the capability is absent.
        for p in ["/attach/default/nginx/app", "/portForward/default/nginx"] {
            let (s, body) = get_body(p).await;
            assert_eq!(s, StatusCode::NOT_IMPLEMENTED, "{p}");
            assert!(body.contains("multiplexing"), "{p}: {body}");
        }
    }
}
