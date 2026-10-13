use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engenho_apiserver::{
    ApiError, ApiServer, Attributes, Authorizer, Decision, PodStreamProxy, PodStreamTarget,
    PodStreamUpstream, ResourceHandler, RouterState, StoreBackedHandler, dial_kubelet,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};

const KUBELET_PROTOCOLS: &[&str] = &[
    "v5.channel.k8s.io",
    "v4.channel.k8s.io",
    "portforward.k8s.io",
];
const CLOSE_TRIGGER: &[u8] = b"\xffclose-from-kubelet";

#[derive(Default)]
struct KubeletLog {
    paths: Mutex<Vec<String>>,
    closes: Mutex<Vec<Option<u16>>>,
}

async fn fake_kubelet(log: Arc<KubeletLog>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let seen = log.clone();
                #[allow(clippy::result_large_err)]
                let callback = move |req: &Request, mut resp: Response| {
                    seen.paths.lock().unwrap().push(req.uri().to_string());
                    let chosen = req
                        .headers()
                        .get_all("sec-websocket-protocol")
                        .iter()
                        .filter_map(|v| v.to_str().ok())
                        .flat_map(|s| s.split(','))
                        .map(str::trim)
                        .find(|p| KUBELET_PROTOCOLS.contains(p))
                        .map(str::to_owned);
                    if let Some(p) = chosen {
                        resp.headers_mut()
                            .insert("sec-websocket-protocol", HeaderValue::from_str(&p).unwrap());
                    }
                    Ok::<_, ErrorResponse>(resp)
                };
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    match msg {
                        Message::Binary(b) if b.as_slice() == CLOSE_TRIGGER => {
                            let _ = ws
                                .close(Some(CloseFrame {
                                    code: CloseCode::Library(4001),
                                    reason: "kubelet done".into(),
                                }))
                                .await;
                        }
                        Message::Close(frame) => {
                            log.closes
                                .lock()
                                .unwrap()
                                .push(frame.map(|f| u16::from(f.code)));
                        }
                        m @ (Message::Binary(_) | Message::Text(_)) => {
                            if ws.send(m).await.is_err() {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            });
        }
    });
    addr
}

struct NodeTable {
    live: String,
}

#[async_trait]
impl PodStreamProxy for NodeTable {
    async fn open(&self, target: &PodStreamTarget) -> Result<PodStreamUpstream, ApiError> {
        let authority = match target.node_name.as_deref() {
            Some("n1") => self.live.clone(),
            Some("dead") => "127.0.0.1:1".to_owned(),
            Some(other) => return Err(ApiError::NotFound(format!("node {other} not found"))),
            None => {
                return Err(ApiError::BadRequest(format!(
                    "pod {} does not have a host assigned",
                    target.pod
                )));
            }
        };
        dial_kubelet(&authority, target).await
    }
}

struct VerbOnly(&'static str);

#[async_trait]
impl Authorizer for VerbOnly {
    async fn authorize(&self, attrs: &Attributes) -> Decision {
        if attrs.verb == self.0 {
            Decision::Allow
        } else {
            Decision::Deny
        }
    }
}

async fn put_pod(store: &StoreMesh, name: &str, node: Option<&str>) {
    let mut spec = serde_json::json!({"containers": [{"name": "c", "image": "busybox"}]});
    if let Some(n) = node {
        spec["nodeName"] = n.into();
    }
    store
        .propose(ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
            value: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "default"},
                "spec": spec,
            }),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

async fn serve(authorizer: Option<Arc<dyn Authorizer>>) -> (ApiServer, Arc<KubeletLog>) {
    let log = Arc::new(KubeletLog::default());
    let live = fake_kubelet(log.clone()).await;
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("pod-stream-relay").unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    put_pod(&store, "p", Some("n1")).await;
    put_pod(&store, "on-dead-node", Some("dead")).await;
    put_pod(&store, "on-unknown-node", Some("gone")).await;
    put_pod(&store, "unscheduled", None).await;
    let descriptor = engenho_types::generated_v1_34::RESOURCE_CATALOG
        .iter()
        .find(|d| d.group.is_empty() && d.kind == "Pod")
        .unwrap();
    let pods: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::from_descriptor(store, descriptor)
            .with_stream_proxy(Arc::new(NodeTable { live })),
    );
    let mut state = RouterState::new(vec![pods]);
    if let Some(a) = authorizer {
        state = state.with_authorizer(a);
    }
    let server = ApiServer::start_with_state("127.0.0.1:0".parse().unwrap(), state, None)
        .await
        .unwrap();
    (server, log)
}

fn ws_request(
    server: &ApiServer,
    path: &str,
    protocols: &str,
) -> tungstenite::handshake::client::Request {
    let mut req = format!("ws://{}{path}", server.local_addr())
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(protocols).unwrap(),
    );
    req
}

async fn next_data(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Message {
    tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("a frame in time")
        .expect("stream open")
        .expect("frame ok")
}

fn http_status(err: tungstenite::Error) -> u16 {
    match err {
        tungstenite::Error::Http(resp) => resp.status().as_u16(),
        other => panic!("expected an HTTP refusal, got {other}"),
    }
}

const EXEC_PATH: &str = "/api/v1/namespaces/default/pods/p/exec?command=sh&command=-c&stdin=true&stdout=true&container=c";

#[tokio::test]
async fn exec_relays_frames_byte_identical_with_the_kubelets_subprotocol() {
    let (server, log) = serve(None).await;
    let (mut ws, resp) = tokio_tungstenite::connect_async(ws_request(
        &server,
        EXEC_PATH,
        "v5.channel.k8s.io, v4.channel.k8s.io",
    ))
    .await
    .unwrap();
    assert_eq!(
        resp.headers().get("sec-websocket-protocol").unwrap(),
        "v5.channel.k8s.io"
    );
    assert_eq!(
        log.paths.lock().unwrap().as_slice(),
        ["/exec/default/p/c?command=sh&command=-c&stdin=true&stdout=true&container=c"]
    );

    let payload: Vec<u8> = (0..=255u8).chain([0, 0, 255]).collect();
    ws.send(Message::Binary(payload.clone())).await.unwrap();
    assert_eq!(next_data(&mut ws).await, Message::Binary(payload));
    ws.send(Message::Text("\u{0}ls\n".into())).await.unwrap();
    assert_eq!(next_data(&mut ws).await, Message::Text("\u{0}ls\n".into()));

    ws.send(Message::Binary(CLOSE_TRIGGER.to_vec()))
        .await
        .unwrap();
    match next_data(&mut ws).await {
        Message::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), 4001);
            assert_eq!(frame.reason, "kubelet done");
        }
        other => panic!("expected the kubelet's close, got {other:?}"),
    }
}

#[tokio::test]
async fn a_client_close_reaches_the_kubelet_with_its_code() {
    let (server, log) = serve(None).await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(ws_request(&server, EXEC_PATH, "v4.channel.k8s.io"))
            .await
            .unwrap();
    ws.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    }))
    .await
    .unwrap();
    for _ in 0..50 {
        if !log.closes.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(log.closes.lock().unwrap().as_slice(), [Some(1000)]);
}

#[tokio::test]
async fn attach_and_portforward_reach_their_kubelet_routes() {
    let (server, log) = serve(None).await;
    let (_a, _) = tokio_tungstenite::connect_async(ws_request(
        &server,
        "/api/v1/namespaces/default/pods/p/attach?stdin=true&stdout=true",
        "v5.channel.k8s.io",
    ))
    .await
    .unwrap();
    let (_pf, resp) = tokio_tungstenite::connect_async(ws_request(
        &server,
        "/api/v1/namespaces/default/pods/p/portforward?ports=8080",
        "portforward.k8s.io",
    ))
    .await
    .unwrap();
    assert_eq!(
        resp.headers().get("sec-websocket-protocol").unwrap(),
        "portforward.k8s.io"
    );
    assert_eq!(
        log.paths.lock().unwrap().as_slice(),
        [
            "/attach/default/p/c?stdin=true&stdout=true",
            "/portForward/default/p?ports=8080"
        ]
    );
}

#[tokio::test]
async fn a_post_upgrade_is_relayed_like_a_get() {
    let (server, log) = serve(None).await;
    let mut tcp = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    let request = [
        "POST ",
        EXEC_PATH,
        " HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n",
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
        "Sec-WebSocket-Protocol: v5.channel.k8s.io\r\nContent-Length: 0\r\n\r\n",
    ]
    .concat();
    tcp.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut b = [0u8; 1];
        tcp.read_exact(&mut b).await.unwrap();
        head.push(b[0]);
    }
    let head = String::from_utf8(head).unwrap().to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 101"), "{head}");
    assert!(
        head.contains("sec-websocket-protocol: v5.channel.k8s.io"),
        "{head}"
    );
    assert!(
        head.contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="),
        "{head}"
    );
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        tcp,
        tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    ws.send(Message::Binary(vec![1, 2, 3])).await.unwrap();
    let echoed = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(echoed, Message::Binary(vec![1, 2, 3]));
    assert_eq!(log.paths.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn missing_pods_nodes_and_kubelets_are_typed() {
    let (server, _) = serve(None).await;
    for (pod, code) in [
        ("nope", 404),
        ("on-unknown-node", 404),
        ("on-dead-node", 503),
        ("unscheduled", 400),
    ] {
        let path = format!("/api/v1/namespaces/default/pods/{pod}/exec?command=sh");
        let err = tokio_tungstenite::connect_async(ws_request(&server, &path, "v5.channel.k8s.io"))
            .await
            .expect_err(pod);
        assert_eq!(http_status(err), code, "{pod}");
    }
}

#[tokio::test]
async fn spdy_and_writes_are_refused_with_a_reason() {
    let (server, _) = serve(None).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}{EXEC_PATH}", server.local_addr());
    let resp = client
        .post(&url)
        .header("connection", "Upgrade")
        .header("upgrade", "SPDY/3.1")
        .header("x-stream-protocol-version", "v4.channel.k8s.io")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "upstream's answer to an unnegotiated SPDY upgrade"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("WebSocket only"), "{message}");
    assert!(message.contains("portforward.k8s.io"), "{message}");
    assert!(message.contains("1.31"), "{message}");

    for method in [
        reqwest::Method::PUT,
        reqwest::Method::PATCH,
        reqwest::Method::DELETE,
    ] {
        let resp = client
            .request(method.clone(), &url)
            .header("content-type", "application/merge-patch+json")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{method}");
    }
}

#[tokio::test]
async fn the_upgrade_is_authorized_as_create() {
    let (create_only, _) = serve(Some(Arc::new(VerbOnly("create")))).await;
    tokio_tungstenite::connect_async(ws_request(&create_only, EXEC_PATH, "v5.channel.k8s.io"))
        .await
        .expect("create is the verb a WebSocket exec needs");

    let (get_only, log) = serve(Some(Arc::new(VerbOnly("get")))).await;
    let err =
        tokio_tungstenite::connect_async(ws_request(&get_only, EXEC_PATH, "v5.channel.k8s.io"))
            .await
            .expect_err("get alone must not open an exec stream");
    assert_eq!(http_status(err), 403);
    assert!(log.paths.lock().unwrap().is_empty());
}
