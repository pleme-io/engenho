//! k9s conformance: every k9s action, as the requests k9s puts on the wire,
//! answered the way an upstream apiserver answers them.
//!
//! The matrix is data ([`matrix::ROWS`]); this file is its interpreter plus
//! the properties no single row can state: the all-variants guard, and many
//! watches plus log follows multiplexed on ONE HTTP/2 connection, cancelled,
//! with the connection still answering promptly afterwards (the shape of a
//! k9s session that opens and leaves a Logs view).

mod matrix;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use engenho_apiserver::{
    ApiError, ApiServer, LogQuery, PodLogReader, PodStreamKind, PodStreamProxy, PodStreamTarget,
    PodStreamUpstream, ResourceHandler, RouterState, StoreBackedHandler, dial_kubelet,
    handlers_from_catalog,
};
use engenho_kubelet::port_forward_spdy::client::Client;
use engenho_kubelet::spdy::{Frame, RstStatus};
use engenho_apiserver::resource_metrics::{
    ContainerSample, MetricsHandler, MetricsKind, NodeSample, PodSample, ResourceMetricsReader,
    Window,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use futures::{SinkExt, StreamExt};
use matrix::{Degrade, Expected, K9sAction, NS, ROWS, Req, Row};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};

/// The subprotocols the engenho kubelet accepts (`engenho-kubelet/src/server.rs`).
const KUBELET_PROTOCOLS: &[&str] = &[
    "v5.channel.k8s.io",
    "v4.channel.k8s.io",
    "portforward.k8s.io",
];

/// Per-container log lines, each with the time it was written, served
/// with the kubelet's `/log` semantics: `since` drops what was written
/// before it, `timestamps` prefixes each line with its RFC3339Nano stamp,
/// `tailLines` keeps the last lines, and the cursor counts rendered bytes.
#[derive(Default)]
struct Logs {
    lines: Mutex<BTreeMap<String, Vec<(SystemTime, String)>>>,
    reads: AtomicUsize,
}

#[async_trait]
impl PodLogReader for Logs {
    async fn read_pod_logs(
        &self,
        _ns: &str,
        name: &str,
        query: &LogQuery,
    ) -> Result<String, ApiError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let container = query.container.as_deref().unwrap_or("main");
        let lines = self
            .lines
            .lock()
            .map_err(|_| ApiError::Internal("poisoned".into()))?
            .get(&format!("{name}/{container}"))
            .cloned()
            .ok_or_else(|| {
                ApiError::NotFound(format!("container {container:?} not found in pod"))
            })?;
        let mut all = String::new();
        for (at, text) in lines {
            if query.since.is_some_and(|since| at < since) {
                continue;
            }
            if query.timestamps {
                all.push_str(&engenho_types::time::system_time_to_rfc3339_nano(at).unwrap());
                all.push(' ');
            }
            all.push_str(&text);
            all.push('\n');
        }
        let from = usize::try_from(query.from_byte.unwrap_or(0))
            .unwrap_or(usize::MAX)
            .min(all.len());
        let rest = &all[from..];
        Ok(match query.tail_lines {
            Some(n) => engenho_apiserver::pod_logs::last_lines(rest, n),
            None => rest.to_owned(),
        })
    }
}

/// Usage for every fixture pod but `blind`, which the runtime cannot
/// measure; one node.
struct Usage;

#[async_trait]
impl ResourceMetricsReader for Usage {
    async fn pods(&self) -> Vec<PodSample> {
        let window = Window {
            timestamp: SystemTime::now(),
            length: Duration::from_secs(1),
        };
        let measured = |name: &str| PodSample {
            namespace: NS.into(),
            name: name.into(),
            usage: Ok((
                window,
                vec![ContainerSample {
                    name: "main".into(),
                    cpu_nanocores: 1_000_000,
                    memory_bytes: 4 << 20,
                }],
            )),
        };
        vec![
            measured("web"),
            measured("multi"),
            PodSample {
                namespace: NS.into(),
                name: "blind".into(),
                usage: Err("this node has no cgroup v2 (macOS)".into()),
            },
        ]
    }
    async fn nodes(&self) -> Vec<NodeSample> {
        vec![NodeSample {
            name: "n1".into(),
            usage: Ok((
                Window {
                    timestamp: SystemTime::now(),
                    length: Duration::from_secs(1),
                },
                2_000_000,
                64 << 20,
            )),
        }]
    }
}

/// Every pod is on `n1`. exec and attach reach the echoing fake kubelet;
/// port-forward reaches a real engenho kubelet, because a SPDY session is
/// only proven against the server that speaks it.
struct OneNode {
    kubelet: String,
    real_kubelet: String,
}

#[async_trait]
impl PodStreamProxy for OneNode {
    async fn open(&self, target: &PodStreamTarget) -> Result<PodStreamUpstream, ApiError> {
        match target.kind {
            PodStreamKind::PortForward => dial_kubelet(&self.real_kubelet, target).await,
            PodStreamKind::Exec | PodStreamKind::Attach => {
                dial_kubelet(&self.kubelet, target).await
            }
        }
    }
}

/// The engenho kubelet's API with every pod on loopback: port-forward is the
/// only verb routed to it.
struct LoopbackPods;

#[async_trait]
impl engenho_kubelet::server::KubeletApi for LoopbackPods {
    async fn container_logs(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &engenho_kubelet::backend::LogOptions,
    ) -> Result<String, String> {
        Err("not routed here".into())
    }
    async fn pods(&self) -> Value {
        json!({})
    }
    async fn running_pods(&self) -> Value {
        json!({})
    }
    async fn exec(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &[String],
    ) -> Result<engenho_kubelet::backend::ExecOutcome, String> {
        Err("not routed here".into())
    }
    async fn exec_stream(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &engenho_kubelet::stream::StreamRequest,
    ) -> Result<engenho_kubelet::stream::StreamSession, String> {
        Err("not routed here".into())
    }
    async fn attach(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &engenho_kubelet::stream::StreamRequest,
    ) -> Result<engenho_kubelet::stream::StreamSession, String> {
        Err("not routed here".into())
    }
    async fn pod_address(&self, _: &str, _: &str) -> Result<std::net::IpAddr, String> {
        Ok(std::net::IpAddr::from([127, 0, 0, 1]))
    }
}

/// A real engenho kubelet on loopback; its `host:port`.
async fn real_kubelet() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = engenho_kubelet::server::KubeletServer::new(Arc::new(LoopbackPods)).routes();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// A TCP echo listener on loopback: the forwarded pod port. Reports each
/// connection's end on `ended`.
async fn echo_listener() -> (u16, Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let ended = Arc::new(AtomicUsize::new(0));
    let seen = ended.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = echo.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                seen.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    (port, ended)
}

/// A kubelet that accepts exactly the engenho kubelet's subprotocols and
/// echoes frames.
async fn fake_kubelet() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            tokio::spawn(async move {
                #[allow(clippy::result_large_err)]
                let callback = |req: &Request, mut resp: Response| {
                    let chosen = req
                        .headers()
                        .get_all("sec-websocket-protocol")
                        .iter()
                        .filter_map(|v| v.to_str().ok())
                        .flat_map(|s| s.split(','))
                        .map(str::trim)
                        .find(|p| KUBELET_PROTOCOLS.contains(p))
                        .map(str::to_owned);
                    match chosen {
                        Some(p) => {
                            resp.headers_mut().insert(
                                "sec-websocket-protocol",
                                HeaderValue::from_str(&p).unwrap(),
                            );
                            Ok(resp)
                        }
                        None => {
                            let mut refusal = ErrorResponse::new(Some("no subprotocol".into()));
                            *refusal.status_mut() = tungstenite::http::StatusCode::BAD_REQUEST;
                            Err(refusal)
                        }
                    }
                };
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if matches!(msg, Message::Binary(_) | Message::Text(_))
                        && ws.send(msg).await.is_err()
                    {
                        break;
                    }
                }
            });
        }
    });
    addr
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::Put {
            key,
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn ns_key(group: &str, version: &str, kind: &str, name: &str) -> ResourceKey {
    ResourceKey::namespaced(group, version, kind, NS, name)
}

/// A running, Ready pod on `n1`. `web` names `main` its default container
/// with a `sidecar` listed first; `multi` names none and lists `zeta` first.
fn pod(name: &str) -> Value {
    let (annotations, containers) = match name {
        "web" => (
            json!({"kubectl.kubernetes.io/default-container": "main"}),
            json!([{"name":"sidecar","image":"busybox"},{"name":"main","image":"busybox"}]),
        ),
        "multi" => (
            json!({}),
            json!([{"name":"zeta","image":"busybox"},{"name":"alpha","image":"busybox"}]),
        ),
        _ => (json!({}), json!([{"name":"main","image":"busybox"}])),
    };
    json!({"apiVersion":"v1","kind":"Pod",
        "metadata":{"name":name,"namespace":NS,"uid":format!("uid-{name}"),
            "labels":{"app":name},"annotations":annotations},
        "spec":{"nodeName":"n1",
            "initContainers":[{"name":"init","image":"busybox"}],
            "containers":containers},
        "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}})
}

async fn seed(store: &StoreMesh) {
    put(
        store,
        ResourceKey::cluster_scoped("", "v1", "Namespace", NS),
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":NS}}),
    )
    .await;
    put(
        store,
        ResourceKey::cluster_scoped("", "v1", "Node", "n1"),
        json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"},"spec":{}}),
    )
    .await;
    for name in ["web", "victim", "doomed", "multi", "guarded", "blind"] {
        put(store, ns_key("", "v1", "Pod", name), pod(name)).await;
    }
    put(
        store,
        ns_key("policy", "v1", "PodDisruptionBudget", "guarded"),
        json!({"apiVersion":"policy/v1","kind":"PodDisruptionBudget",
            "metadata":{"name":"guarded","namespace":NS,"generation":1},
            "spec":{"minAvailable":1,"selector":{"matchLabels":{"app":"guarded"}}},
            "status":{"observedGeneration":1,"disruptionsAllowed":0,
                "currentHealthy":1,"desiredHealthy":1,"expectedPods":1}}),
    )
    .await;
    for name in ["cfg", "doomed"] {
        put(
            store,
            ns_key("", "v1", "ConfigMap", name),
            json!({"apiVersion":"v1","kind":"ConfigMap",
                "metadata":{"name":name,"namespace":NS},"data":{"k":"v"}}),
        )
        .await;
    }
    put(
        store,
        ns_key("apps", "v1", "Deployment", "web"),
        json!({"apiVersion":"apps/v1","kind":"Deployment",
            "metadata":{"name":"web","namespace":NS},
            "spec":{"replicas":1,"selector":{"matchLabels":{"app":"web"}},
                "template":{"metadata":{"labels":{"app":"web"}},
                    "spec":{"containers":[{"name":"main","image":"busybox"}]}}}}),
    )
    .await;
    put(
        store,
        ns_key("", "v1", "Event", "web.1"),
        json!({"apiVersion":"v1","kind":"Event",
            "metadata":{"name":"web.1","namespace":NS},
            "involvedObject":{"kind":"Pod","name":"web","namespace":NS},
            "reason":"Started","message":"started","type":"Normal"}),
    )
    .await;
    put(
        store,
        ns_key("", "v1", "Secret", "sh.helm.release.v1.web.v1"),
        json!({"apiVersion":"v1","kind":"Secret",
            "metadata":{"name":"sh.helm.release.v1.web.v1","namespace":NS,
                "labels":{"owner":"helm","name":"web"}},
            "type":"helm.sh/release.v1","data":{}}),
    )
    .await;
}

struct Cluster {
    server: ApiServer,
    logs: Arc<Logs>,
    /// The forwarded pod port's echo listener, and how many of its
    /// connections have ended.
    echo: (u16, Arc<AtomicUsize>),
}

impl Cluster {
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.server.local_addr())
    }
}

async fn cluster(test: &str) -> Cluster {
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config(test).unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    seed(&store).await;

    let logs = Arc::new(Logs::default());
    {
        let now = SystemTime::now();
        let mut lines = logs.lines.lock().unwrap();
        let mut main = vec![(now - Duration::from_secs(7200), "old news".to_owned())];
        main.extend((0..50).map(|_| (now, "main says hi".to_owned())));
        lines.insert("web/main".into(), main);
        lines.insert("web/sidecar".into(), vec![(now, "sidecar chatter".into())]);
        lines.insert("web/init".into(), vec![(now, "init done".into())]);
        lines.insert("multi/zeta".into(), vec![(now, "zeta speaks".into())]);
        lines.insert("multi/alpha".into(), vec![(now, "alpha speaks".into())]);
    }
    let descriptor = engenho_types::generated_v1_34::RESOURCE_CATALOG
        .iter()
        .find(|d| d.group.is_empty() && d.kind == "Pod")
        .expect("Pod is cataloged");
    let pods: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::from_descriptor(store.clone(), descriptor)
            .with_log_reader(logs.clone())
            .with_stream_proxy(Arc::new(OneNode {
                kubelet: fake_kubelet().await,
                real_kubelet: real_kubelet().await,
            })),
    );
    let mut handlers = handlers_from_catalog(store.clone());
    handlers.push(pods);
    for kind in [MetricsKind::Pod, MetricsKind::Node] {
        handlers.push(Arc::new(MetricsHandler::new(kind, Arc::new(Usage), store.clone())));
    }
    let server = ApiServer::start_with_state(
        "127.0.0.1:0".parse().unwrap(),
        RouterState::new(handlers),
        None,
    )
    .await
    .unwrap();
    Cluster {
        server,
        logs,
        echo: echo_listener().await,
    }
}

const PER_REQUEST: Duration = Duration::from_secs(5);

/// One row against the cluster; `Err` is the divergence, worded for the
/// failure listing.
async fn run(c: &Cluster, http: &reqwest::Client, row: &Row) -> Result<(), String> {
    match (row.req, row.expect) {
        (Req::Upgrade { path, protocols }, expect) => upgrade(c, path, protocols, expect).await,
        (Req::SpdyUpgrade { path }, Expected::Forwarded { protocol }) => {
            let (mut client, head) = spdy_client(c, path).await?;
            let lower = head.to_ascii_lowercase();
            if !lower.contains(&format!("x-stream-protocol-version: {protocol}"))
                || !lower.contains("upgrade: spdy/3.1")
            {
                return Err(format!("101 without the SPDY handshake headers: {head}"));
            }
            round_trip(&mut client, c.echo.0, b"k9s says hi over plain spdy").await
        }
        (Req::Watch { path }, Expected::WatchEvent { event }) => {
            let mut resp = send(http.get(c.url(path))).await?;
            let first = first_line(&mut resp).await?;
            let frame: Value = serde_json::from_str(&first).map_err(|e| format!("frame: {e}"))?;
            let ty = frame["type"].as_str().unwrap_or_default();
            let kind = frame["object"]["kind"].as_str().unwrap_or_default();
            if ty == event && !kind.is_empty() {
                Ok(())
            } else {
                Err(format!("first watch frame {first}"))
            }
        }
        (Req::Follow { path }, Expected::FollowChunk { contains }) => {
            let mut resp = send(http.get(c.url(path))).await?;
            status_is(&resp, 200)?;
            let chunk = tokio::time::timeout(PER_REQUEST, resp.chunk())
                .await
                .map_err(|_| "no first chunk".to_owned())?
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            let text = String::from_utf8_lossy(&chunk);
            if text.contains(contains) {
                Ok(())
            } else {
                Err(format!("first chunk lacks {contains:?}: {text:.120}"))
            }
        }
        (req, expect) => {
            let resp = request(c, http, req).await?;
            judge(c, http, resp, expect).await
        }
    }
}

async fn send(rb: reqwest::RequestBuilder) -> Result<reqwest::Response, String> {
    tokio::time::timeout(PER_REQUEST, rb.send())
        .await
        .map_err(|_| "no response in time".to_owned())?
        .map_err(|e| e.to_string())
}

async fn first_line(resp: &mut reqwest::Response) -> Result<String, String> {
    let mut buf = Vec::new();
    loop {
        let chunk = tokio::time::timeout(PER_REQUEST, resp.chunk())
            .await
            .map_err(|_| "no frame in time".to_owned())?
            .map_err(|e| e.to_string())?
            .ok_or("stream ended before a frame")?;
        buf.extend_from_slice(&chunk);
        if let Some(i) = buf.iter().position(|b| *b == b'\n') {
            return Ok(String::from_utf8_lossy(&buf[..i]).into_owned());
        }
    }
}

async fn request(
    c: &Cluster,
    http: &reqwest::Client,
    req: Req,
) -> Result<reqwest::Response, String> {
    let rb = match req {
        Req::Get { path, accept } => {
            let rb = http.get(c.url(path));
            match accept {
                Some(a) => rb.header("accept", a),
                None => rb,
            }
        }
        Req::Post { path, body } => http.post(c.url(path)).json(&body()),
        Req::Delete { path, body } => http.delete(c.url(path)).json(&body()),
        Req::Patch {
            path,
            content_type,
            body,
        } => http
            .patch(c.url(path))
            .header("content-type", content_type)
            .body(body().to_string()),
        Req::GetThenPut { path, edit } => {
            let got = send(http.get(c.url(path))).await?;
            status_is(&got, 200)?;
            let mut v: Value = got.json().await.map_err(|e| e.to_string())?;
            edit(&mut v);
            http.put(c.url(path)).json(&v)
        }
        Req::Watch { .. } | Req::Follow { .. } | Req::Upgrade { .. } | Req::SpdyUpgrade { .. } => {
            return Err("streaming request routed to the plain runner".into());
        }
    };
    send(rb).await
}

fn status_is(resp: &reqwest::Response, want: u16) -> Result<(), String> {
    let got = resp.status().as_u16();
    if got == want {
        Ok(())
    } else {
        Err(format!("status {got}, want {want}"))
    }
}

async fn judge(
    c: &Cluster,
    http: &reqwest::Client,
    resp: reqwest::Response,
    expect: Expected,
) -> Result<(), String> {
    match expect {
        Expected::Object { status, kind } => {
            status_is(&resp, status)?;
            let v: Value = resp.json().await.map_err(|e| format!("not JSON: {e}"))?;
            if kind.is_empty() || v["kind"] == kind {
                Ok(())
            } else {
                Err(format!("kind {}, want {kind}", v["kind"]))
            }
        }
        Expected::TextAtMost { bytes } => {
            status_is(&resp, 200)?;
            let t = resp.text().await.map_err(|e| e.to_string())?;
            if !t.is_empty() && t.len() <= bytes {
                Ok(())
            } else {
                Err(format!("{} bytes, want 1..={bytes}", t.len()))
            }
        }
        Expected::Refused { status } => {
            status_is(&resp, status)?;
            let v: Value = resp
                .json()
                .await
                .map_err(|e| format!("not a Status: {e}"))?;
            if v["kind"] == "Status" {
                Ok(())
            } else {
                Err(format!("refusal kind {}", v["kind"]))
            }
        }
        Expected::Lines {
            contains,
            absent,
            stamped,
        } => {
            status_is(&resp, 200)?;
            let t = resp.text().await.map_err(|e| e.to_string())?;
            if !t.contains(contains) {
                return Err(format!("text lacks {contains:?}: {t:.200}"));
            }
            if let Some(a) = absent
                && t.contains(a)
            {
                return Err(format!("text carries {a:?}: {t:.200}"));
            }
            if stamped {
                for line in t.lines() {
                    let (stamp, rest) = line
                        .split_once(' ')
                        .ok_or_else(|| format!("unstamped line {line:?}"))?;
                    if engenho_types::time::parse_rfc3339_system_time(stamp).is_none()
                        || rest.starts_with(|c: char| c.is_ascii_digit())
                    {
                        return Err(format!("not one RFC3339 token then text: {line:?}"));
                    }
                }
            }
            Ok(())
        }
        Expected::Absent { degrade, .. } => match degrade {
            Degrade::Ignored { status } => status_is(&resp, status),
            Degrade::NotDiscovered { group } => {
                status_is(&resp, 404)?;
                let groups: Value = http
                    .get(c.url("/apis"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;
                let listed = groups["groups"]
                    .as_array()
                    .is_some_and(|gs| gs.iter().any(|g| g["name"] == group));
                if listed {
                    Err(format!("{group} is advertised but not served"))
                } else {
                    Ok(())
                }
            }
            Degrade::NoSubresource { subresource } => {
                status_is(&resp, 404)?;
                let core: Value = http
                    .get(c.url("/api/v1"))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;
                let name = format!("pods/{subresource}");
                let listed = core["resources"]
                    .as_array()
                    .is_some_and(|rs| rs.iter().any(|r| r["name"] == name.as_str()));
                if listed {
                    Err(format!("{name} is advertised but not served"))
                } else {
                    Ok(())
                }
            }
            Degrade::NoUpgrade { .. } => Err("NoUpgrade on a plain request".into()),
        },
        Expected::WatchEvent { .. }
        | Expected::FollowChunk { .. }
        | Expected::Switched { .. }
        | Expected::Forwarded { .. } => Err("streaming expectation on a plain request".into()),
    }
}

async fn upgrade(c: &Cluster, path: &str, protocols: &str, expect: Expected) -> Result<(), String> {
    let mut req = format!("ws://{}{path}", c.server.local_addr())
        .into_client_request()
        .map_err(|e| e.to_string())?;
    req.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(protocols).map_err(|e| e.to_string())?,
    );
    let dialed = tokio::time::timeout(PER_REQUEST, tokio_tungstenite::connect_async(req))
        .await
        .map_err(|_| "no upgrade answer in time".to_owned())?;
    match (dialed, expect) {
        (Ok((mut ws, resp)), Expected::Switched { protocol }) => {
            let chosen = resp
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            if chosen != protocol {
                return Err(format!("negotiated {chosen:?}, want {protocol}"));
            }
            ws.send(Message::Binary(b"\x00ping".to_vec()))
                .await
                .map_err(|e| e.to_string())?;
            let echoed = tokio::time::timeout(PER_REQUEST, ws.next())
                .await
                .map_err(|_| "no frame relayed back".to_owned())?;
            match echoed {
                Some(Ok(Message::Binary(b))) if b == b"\x00ping" => Ok(()),
                other => Err(format!("relay returned {other:?}")),
            }
        }
        (Ok((ws, resp)), Expected::Forwarded { protocol }) => {
            let chosen = resp
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if chosen != protocol {
                return Err(format!("negotiated {chosen:?}, want {protocol}"));
            }
            round_trip(
                &mut tunnel_client(ws),
                c.echo.0,
                b"k9s says hi through the tunnel",
            )
            .await
        }
        (
            Err(tungstenite::Error::Http(resp)),
            Expected::Absent {
                degrade: Degrade::NoUpgrade { .. },
                ..
            },
        ) => {
            if resp.status().is_client_error() || resp.status().is_server_error() {
                Ok(())
            } else {
                Err(format!("refused with {}", resp.status()))
            }
        }
        (
            Ok((_, resp)),
            Expected::Absent {
                degrade: Degrade::NoUpgrade { protocol },
                ..
            },
        ) => {
            let chosen = resp
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if chosen == protocol {
                Err(format!(
                    "{protocol} negotiated: the row is no longer Absent"
                ))
            } else {
                Ok(())
            }
        }
        (
            Err(e),
            Expected::Absent {
                degrade: Degrade::NoUpgrade { .. },
                ..
            },
        ) => Err(format!("not a clean HTTP refusal: {e}")),
        (Err(e), _) => Err(e.to_string()),
        (Ok(_), other) => Err(format!("101 where {other:?} was expected")),
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A SPDY client over a `SPDY/3.1+portforward.k8s.io` WebSocket.
fn tunnel_client(ws: Ws) -> Client {
    let (sink, stream) = ws.split();
    let read = stream.filter_map(|m| async move {
        match m {
            Ok(Message::Binary(b)) => Some(b),
            _ => None,
        }
    });
    let write =
        sink.with(|b: Vec<u8>| async move { Ok::<_, tungstenite::Error>(Message::Binary(b)) });
    Client::over_messages(Box::pin(read), Box::pin(write))
}

/// A tunnel client on `path` through the apiserver.
async fn tunnel(c: &Cluster, path: &str) -> Client {
    let mut req = format!("ws://{}{path}", c.server.local_addr())
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static("SPDY/3.1+portforward.k8s.io"),
    );
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    tunnel_client(ws)
}

/// A plain SPDY client on `path` through the apiserver, and the 101's head.
async fn spdy_client(c: &Cluster, path: &str) -> Result<(Client, String), String> {
    let tcp = tokio::net::TcpStream::connect(c.server.local_addr())
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::timeout(
        PER_REQUEST,
        Client::upgrade(tcp, path, "portforward.k8s.io"),
    )
    .await
    .map_err(|_| "no upgrade answer in time".to_owned())?
    .map_err(|e| e.to_string())
}

/// Every frame until `done` holds for one, within [`PER_REQUEST`].
async fn frames_until(
    client: &mut Client,
    mut done: impl FnMut(&Frame) -> bool,
) -> Result<Vec<Frame>, String> {
    let mut seen = Vec::new();
    let read = async {
        while let Some(f) = client.recv().await.map_err(|e| e.to_string())? {
            let stop = done(&f);
            seen.push(f);
            if stop {
                return Ok(());
            }
        }
        Err("the session closed early".to_owned())
    };
    tokio::time::timeout(PER_REQUEST, read)
        .await
        .map_err(|_| "no such frame in time".to_owned())??;
    Ok(seen)
}

/// Read until `stream` has carried `want`.
async fn echoed(client: &mut Client, stream: u32, want: &[u8]) -> Result<(), String> {
    let mut got = Vec::new();
    frames_until(client, |f| {
        if let Frame::Data {
            stream: s, data, ..
        } = f
            && *s == stream
        {
            got.extend_from_slice(data);
        }
        got.windows(want.len()).any(|w| w == want)
    })
    .await
    .map(drop)
}

async fn send_data(client: &mut Client, stream: u32, data: &[u8]) -> Result<(), String> {
    client
        .send(&Frame::Data {
            stream,
            fin: false,
            data: data.to_vec(),
        })
        .await
        .map_err(|e| e.to_string())
}

/// Forward one connection to `port` and see `payload` come back.
async fn round_trip(client: &mut Client, port: u16, payload: &[u8]) -> Result<(), String> {
    let f = tokio::time::timeout(PER_REQUEST, client.forward(port, "0"))
        .await
        .map_err(|_| "no stream replies in time".to_owned())?
        .map_err(|e| e.to_string())?;
    send_data(client, f.data, payload).await?;
    echoed(client, f.data, payload).await
}

fn is_fin(f: &Frame, stream: u32) -> bool {
    matches!(f, Frame::Data { stream: s, fin: true, .. } if *s == stream)
}

const PORT_FORWARD: &str = "/api/v1/namespaces/k9s/pods/web/portforward";

/// Several ports on one session through the relay, then a refused port,
/// whose error stream names it while the others keep forwarding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn port_forward_carries_several_ports_and_reports_a_refused_one() {
    let c = cluster("k9s-pf-ports").await;
    let (other, _) = echo_listener().await;
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    for mut client in [
        tunnel(&c, PORT_FORWARD).await,
        spdy_client(&c, PORT_FORWARD).await.unwrap().0,
    ] {
        let a = client.forward(c.echo.0, "0").await.unwrap();
        let b = client.forward(other, "1").await.unwrap();
        send_data(&mut client, a.data, b"to the first")
            .await
            .unwrap();
        send_data(&mut client, b.data, b"to the second")
            .await
            .unwrap();
        let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
        frames_until(&mut client, |f| {
            if let Frame::Data { stream, data, .. } = f {
                if *stream == a.data {
                    got_a.extend_from_slice(data);
                } else if *stream == b.data {
                    got_b.extend_from_slice(data);
                }
            }
            got_a == b"to the first" && got_b == b"to the second"
        })
        .await
        .unwrap();

        let refused = client.forward(closed, "2").await.unwrap();
        let (mut reset, mut error_fin) = (false, false);
        let frames = frames_until(&mut client, |f| {
            reset |= matches!(f, Frame::RstStream { stream, .. } if *stream == refused.data);
            error_fin |= is_fin(f, refused.error);
            reset && error_fin
        })
        .await
        .unwrap();
        let message: Vec<u8> = frames
            .iter()
            .filter_map(|f| match f {
                Frame::Data { stream, data, .. } if *stream == refused.error => Some(data.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let message = String::from_utf8_lossy(&message);
        assert!(
            message.contains("error forwarding port") && message.contains(&closed.to_string()),
            "{message}"
        );
        send_data(&mut client, a.data, b"still forwarding")
            .await
            .unwrap();
        echoed(&mut client, a.data, b"still forwarding")
            .await
            .unwrap();
    }
}

/// A client RST_STREAM on the data stream closes the pod connection and the
/// error stream still ends, so client-go's wait on it returns; hanging up the
/// tunnel closes every pod connection it carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn port_forward_reset_and_hangup_close_the_pod_connections() {
    let c = cluster("k9s-pf-close").await;
    let ended = &c.echo.1;
    let mut client = tunnel(&c, PORT_FORWARD).await;
    let f = client.forward(c.echo.0, "0").await.unwrap();
    send_data(&mut client, f.data, b"x").await.unwrap();
    echoed(&mut client, f.data, b"x").await.unwrap();
    client
        .send(&Frame::RstStream {
            stream: f.data,
            status: RstStatus::Cancel,
        })
        .await
        .unwrap();
    frames_until(&mut client, |fr| is_fin(fr, f.error))
        .await
        .unwrap();
    wait_for(
        || ended.load(Ordering::SeqCst) == 1,
        "the reset pod connection to close",
    )
    .await;

    let g = client.forward(c.echo.0, "1").await.unwrap();
    send_data(&mut client, g.data, b"y").await.unwrap();
    echoed(&mut client, g.data, b"y").await.unwrap();
    drop(client);
    wait_for(
        || ended.load(Ordering::SeqCst) == 2,
        "hanging up to close the pod connection",
    )
    .await;
}

async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + PER_REQUEST;
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "waited for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn every_k9s_action_is_listed_and_has_a_row() {
    for (i, a) in K9sAction::ALL.iter().enumerate() {
        assert_eq!(a.ordinal(), i, "{a:?} is out of place in K9sAction::ALL");
        assert!(
            ROWS.iter().any(|r| r.action == *a),
            "{a:?} has no conformance row"
        );
    }
    assert_eq!(
        K9sAction::ALL.len(),
        K9sAction::CrdBrowse.ordinal() + 1,
        "K9sAction::ALL is missing a variant"
    );
}

#[test]
fn every_absent_row_says_why() {
    for r in ROWS {
        if let Expected::Absent { reason, .. } = r.expect {
            assert!(
                reason.len() > 10,
                "{:?} {:?}: reason too thin",
                r.action,
                r.what
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_answers_as_upstream_does() {
    let c = cluster("k9s-matrix").await;
    let http = reqwest::Client::new();
    let mut failures = Vec::new();
    for row in ROWS {
        if let Err(why) = run(&c, &http, row).await {
            failures.push(format!("{:?} / {}: {why}", row.action, row.what));
        }
    }
    let absent = ROWS
        .iter()
        .filter(|r| matches!(r.expect, Expected::Absent { .. }))
        .count();
    eprintln!(
        "k9s matrix: {} actions, {} rows, {} absent",
        K9sAction::ALL.len(),
        ROWS.len(),
        absent
    );
    assert!(failures.is_empty(), "divergences:\n{}", failures.join("\n"));
}

/// k9s keeps its informers' watches open on one HTTP/2 connection and adds a
/// log follow per container when a Logs view opens; leaving the view cancels
/// the follows. Every stream must open, cancelling must stop the server-side
/// polling, and the connection must still answer promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watches_and_follows_share_one_h2_connection_and_cancel_cleanly() {
    const WATCHES: usize = 40;
    const FOLLOWS: usize = 40;
    let c = cluster("k9s-h2").await;
    let http = reqwest::Client::builder()
        .http2_prior_knowledge()
        .pool_max_idle_per_host(1)
        .build()
        .unwrap();

    let mut open = Vec::new();
    for _ in 0..WATCHES {
        let resp = send(http.get(c.url(
            "/api/v1/namespaces/k9s/pods?allowWatchBookmarks=true&resourceVersion=0&timeoutSeconds=300&watch=true",
        )))
        .await
        .unwrap();
        assert_eq!(resp.version(), reqwest::Version::HTTP_2);
        open.push(resp);
    }
    for i in 0..FOLLOWS {
        let container = if i % 2 == 0 { "main" } else { "init" };
        let resp = send(http.get(c.url(&format!(
            "/api/v1/namespaces/k9s/pods/web/log?container={container}&follow=true&tailLines=100&timestamps=true"
        ))))
        .await
        .unwrap();
        assert_eq!(resp.status(), 200);
        open.push(resp);
    }
    for resp in &mut open {
        let chunk = tokio::time::timeout(PER_REQUEST, resp.chunk())
            .await
            .expect("every multiplexed stream delivers")
            .unwrap();
        assert!(chunk.is_some_and(|b| !b.is_empty()));
    }

    let started = std::time::Instant::now();
    for _ in 0..20 {
        let r = send(http.get(c.url(matrix_pod()))).await.unwrap();
        assert_eq!(r.status(), 200);
    }
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "requests beside {} open streams took {:?}",
        WATCHES + FOLLOWS,
        started.elapsed()
    );

    drop(open);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let settled = c.logs.reads.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        c.logs.reads.load(Ordering::SeqCst),
        settled,
        "a cancelled follow kept polling the log"
    );

    let started = std::time::Instant::now();
    let r = send(http.get(c.url("/api/v1/namespaces/k9s/pods?limit=500")))
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(started.elapsed() < Duration::from_secs(1));
}

/// A followed log with `limitBytes` ends on the chunk that spends the budget.
#[tokio::test]
async fn a_followed_log_with_limit_bytes_ends_at_the_limit() {
    let c = cluster("k9s-limit").await;
    let mut resp = reqwest::get(
        c.url("/api/v1/namespaces/k9s/pods/web/log?container=main&follow=true&limitBytes=40"),
    )
    .await
    .unwrap();
    let mut got = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PER_REQUEST, resp.chunk())
        .await
        .expect("the stream ends instead of waiting for more log")
        .unwrap()
    {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(got.len(), 40);
}

const fn matrix_pod() -> &'static str {
    "/api/v1/namespaces/k9s/pods/web"
}
