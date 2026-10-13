//! Exec, attach and port-forward over a real WebSocket against a real
//! listener, with real processes behind them.
//!
//! ★ WHY THIS EXISTS WHEN THE PUMP IS ALREADY UNIT-TESTED.
//! `exec_session` and `port_forward` prove the FRAMES are right and
//! `server`'s oneshot tests prove the REFUSALS are right, and all of them
//! would stay green against a route that never upgrades at all. The failure
//! modes only a socket can see — no upgrade, the subprotocol not echoed
//! back, frames sent as text, stdin that never reaches the process, a status
//! sent before the last output — all present to a user as a session that
//! hangs.
//!
//! Invariants:
//!   S1 the handshake completes and negotiates `v5.channel.k8s.io`
//!   S2 stdout arrives on channel 1, as a BINARY frame
//!   S3 the terminating status frame arrives on channel 3 and is last
//!   S4 a runtime failure arrives as a Status, not as an exit code
//!   S5 stdin reaches the process, and v5's close signal ends `cat`
//!   S6 a v4 client is served and a non-zero exit reaches it as its cause
//!   S7 `-t` runs on a terminal that the resize channel sizes
//!   S8 attach reaches a running container's stdin and stdout
//!   S9 port-forward relays bytes to a TCP listener, and a refused dial is
//!      an error-channel message, not a dropped session
//!   S10 SPDY tunnelled in a `SPDY/3.1+portforward.k8s.io` WebSocket (what
//!      kubectl ≥ 1.31 and k9s send) forwards several ports at once, and a
//!      refused port is an error-stream message plus a reset data stream
//!   S11 a plain `Upgrade: SPDY/3.1` (their fallback) answers 101 with the
//!      negotiated stream protocol and forwards; PING is echoed
//!   S12 the pod's EOF is a FIN on the data stream then on the error
//!      stream; a client RST_STREAM drops the pod connection and still ends
//!      the error stream
//!   S13 a SPDY upgrade without `portforward.k8s.io` is a 403 naming it, and
//!      a stream without a port is refused, not paired

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use engenho_kubelet::backend::ExecOutcome;
use engenho_kubelet::server::{KubeletApi, KubeletServer};
use engenho_kubelet::stream::{self, AttachPoint, StreamRequest, StreamSession};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

struct Api {
    container: AttachPoint,
}

#[async_trait::async_trait]
impl KubeletApi for Api {
    async fn container_logs(
        &self,
        _n: &str,
        _p: &str,
        _c: &str,
        _o: &engenho_kubelet::backend::LogOptions,
    ) -> Result<String, String> {
        Ok(String::new())
    }
    async fn pods(&self) -> serde_json::Value {
        serde_json::json!({})
    }
    async fn running_pods(&self) -> serde_json::Value {
        serde_json::json!({})
    }
    async fn exec(
        &self,
        _n: &str,
        _p: &str,
        _c: &str,
        _argv: &[String],
    ) -> Result<ExecOutcome, String> {
        Err("the batch path is not used by the server".into())
    }
    async fn exec_stream(
        &self,
        _n: &str,
        _p: &str,
        _c: &str,
        req: &StreamRequest,
    ) -> Result<StreamSession, String> {
        if req.argv.first().map(String::as_str) == Some("missing") {
            return Err("no such container".into());
        }
        let mut cmd = tokio::process::Command::new(&req.argv[0]);
        cmd.args(&req.argv[1..]);
        stream::spawn_local(cmd, req).map_err(|e| e.to_string())
    }
    async fn attach(
        &self,
        _n: &str,
        _p: &str,
        _c: &str,
        req: &StreamRequest,
    ) -> Result<StreamSession, String> {
        Ok(self.container.attach(req))
    }
    async fn pod_address(&self, _n: &str, _p: &str) -> Result<IpAddr, String> {
        Ok(IpAddr::from([127, 0, 0, 1]))
    }
}

/// A running `cat` "container" whose stdio is reachable the way a native
/// container's is: output teed to a log and fanned out, stdin kept, closed
/// after the first detach.
fn cat_container(log_dir: &std::path::Path) -> AttachPoint {
    let mut child = tokio::process::Command::new("cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn cat");
    let log = std::fs::File::create(log_dir.join("cat.log")).expect("log");
    let log_err = log.try_clone().expect("log clone");
    let (out_feed, out) = stream::fanout();
    let (err_feed, err) = stream::fanout();
    stream::tee(
        child.stdout.take().expect("stdout"),
        tokio::fs::File::from_std(log),
        out_feed,
    );
    stream::tee(
        child.stderr.take().expect("stderr"),
        tokio::fs::File::from_std(log_err),
        err_feed,
    );
    let stdin = child.stdin.take().map(stream::pump_writer);
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    AttachPoint {
        stdout: out,
        stderr: Some(err),
        stdin: Arc::new(tokio::sync::Mutex::new(stdin)),
        stdin_once: true,
        resize: None,
    }
}

/// Bind an ephemeral port, serve the kubelet routes on it, return the port.
async fn serve(log_dir: &std::path::Path) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let app = KubeletServer::new(Arc::new(Api {
        container: cat_container(log_dir),
    }))
    .routes();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    port
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(uri: &str, protocol: &str) -> (String, Ws) {
    let mut req = uri.into_client_request().expect("request");
    req.headers_mut()
        .insert("sec-websocket-protocol", protocol.parse().expect("header"));
    let (ws, response) = tokio_tungstenite::connect_async(req)
        .await
        .expect("handshake");
    let negotiated = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    (negotiated, ws)
}

/// Read frames until the server closes, with a deadline so a hang is a
/// failure rather than a stuck test.
async fn drain(ws: &mut Ws) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let read = async {
        while let Some(Ok(msg)) = ws.next().await {
            match msg {
                Message::Binary(b) if !b.is_empty() => out.push((b[0], b[1..].to_vec())),
                Message::Close(_) => break,
                // A TEXT frame is a real defect: kubectl reads binary.
                Message::Text(t) => out.push((u8::MAX, t.into_bytes())),
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("the session ended within the deadline");
    out
}

/// Read frames until `want` appears on `channel`, returning everything read.
async fn read_until(ws: &mut Ws, channel: u8, want: &[u8]) -> Vec<u8> {
    let mut seen = Vec::new();
    let read = async {
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Binary(b) = msg {
                if b.first() == Some(&channel) {
                    seen.extend_from_slice(&b[1..]);
                    if seen.windows(want.len()).any(|w| w == want) {
                        return;
                    }
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("the expected output arrived within the deadline");
    seen
}

fn status_of(frames: &[(u8, Vec<u8>)]) -> serde_json::Value {
    let (ch, payload) = frames.last().expect("at least one frame");
    assert_eq!(*ch, 3, "the last frame is the error channel: {frames:?}");
    serde_json::from_slice(payload).expect("status is JSON")
}

fn joined(frames: &[(u8, Vec<u8>)], channel: u8) -> String {
    let bytes: Vec<u8> = frames
        .iter()
        .filter(|(c, _)| *c == channel)
        .flat_map(|(_, p)| p.clone())
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn s1_s2_s3_a_command_runs_and_its_output_arrives_framed() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (negotiated, mut ws) = connect(
        &format!("ws://127.0.0.1:{port}/exec/default/p/c?command=echo&command=hello&stdout=true"),
        "v5.channel.k8s.io",
    )
    .await;
    assert_eq!(negotiated, "v5.channel.k8s.io", "subprotocol echoed back");
    let got = drain(&mut ws).await;
    assert_eq!(joined(&got, 1), "hello\n");
    assert!(
        got.iter().all(|(ch, _)| *ch != u8::MAX),
        "every frame is binary, not text"
    );
    assert_eq!(status_of(&got)["status"], "Success");
}

#[tokio::test]
async fn s4_a_runtime_failure_is_a_status_not_an_exit_code() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (_, mut ws) = connect(
        &format!("ws://127.0.0.1:{port}/exec/default/p/c?command=missing"),
        "v5.channel.k8s.io",
    )
    .await;
    let got = drain(&mut ws).await;
    assert_eq!(got.len(), 1, "only the error frame: {got:?}");
    let status = status_of(&got);
    assert_eq!(status["reason"], "InternalError");
    assert!(status.get("details").is_none(), "no exit code: {status}");
}

#[tokio::test]
async fn s5_stdin_reaches_cat_and_the_v5_close_signal_ends_it() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (_, mut ws) = connect(
        &format!(
            "ws://127.0.0.1:{port}/exec/default/p/c?command=cat&stdin=true&stdout=true&stderr=true"
        ),
        "v5.channel.k8s.io",
    )
    .await;
    ws.send(Message::Binary([&[0u8][..], b"hello cat\n"].concat()))
        .await
        .expect("stdin");
    read_until(&mut ws, 1, b"hello cat\n").await;
    ws.send(Message::Binary(vec![255, 0]))
        .await
        .expect("close stdin");
    let rest = drain(&mut ws).await;
    assert_eq!(
        status_of(&rest)["status"],
        "Success",
        "cat saw EOF and exited 0"
    );
}

#[tokio::test]
async fn s6_a_v4_client_gets_the_exit_code_as_its_cause() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (negotiated, mut ws) = connect(
        &format!(
            "ws://127.0.0.1:{port}/exec/default/p/c?command=sh&command=-c&command=echo+oops+%3E%262%3B+exit+3&stdout=true&stderr=true"
        ),
        "v4.channel.k8s.io",
    )
    .await;
    assert_eq!(negotiated, "v4.channel.k8s.io");
    let got = drain(&mut ws).await;
    assert_eq!(joined(&got, 2), "oops\n", "stderr on its own channel");
    let status = status_of(&got);
    assert_eq!(status["status"], "Failure");
    assert_eq!(status["details"]["causes"][0]["message"], "3");
}

#[tokio::test]
async fn s7_a_tty_session_is_sized_by_the_resize_channel() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (_, mut ws) = connect(
        &format!(
            "ws://127.0.0.1:{port}/exec/default/p/c?command=sh&command=-c&command=read+x%3B+stty+size&stdin=true&stdout=true&tty=true"
        ),
        "v5.channel.k8s.io",
    )
    .await;
    ws.send(Message::Binary(
        [&[4u8][..], br#"{"Width":101,"Height":33}"#].concat(),
    ))
    .await
    .expect("resize");
    tokio::time::sleep(Duration::from_millis(300)).await;
    ws.send(Message::Binary(vec![0, b'\n']))
        .await
        .expect("enter");
    let got = drain(&mut ws).await;
    assert!(
        joined(&got, 1).contains("33 101"),
        "stty reports the resized terminal: {:?}",
        joined(&got, 1)
    );
    assert!(
        got.iter().all(|(ch, _)| *ch != 2),
        "a tty merges stderr into stdout"
    );
    assert_eq!(status_of(&got)["status"], "Success");
}

#[tokio::test]
async fn s8_attach_reaches_a_running_containers_stdin_and_stdout() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (negotiated, mut ws) = connect(
        &format!("ws://127.0.0.1:{port}/attach/default/p/c?stdin=true&stdout=true&stderr=true"),
        "v5.channel.k8s.io",
    )
    .await;
    assert_eq!(negotiated, "v5.channel.k8s.io");
    ws.send(Message::Binary([&[0u8][..], b"ping\n"].concat()))
        .await
        .expect("stdin");
    read_until(&mut ws, 1, b"ping\n").await;
    // stdinOnce: closing this attacher's stdin closes the container's, cat
    // exits, its output ends, and the attach ends with Success.
    ws.send(Message::Binary(vec![255, 0]))
        .await
        .expect("close stdin");
    let rest = drain(&mut ws).await;
    assert_eq!(status_of(&rest)["status"], "Success");
    let log = std::fs::read_to_string(dir.path().join("cat.log")).expect("log");
    assert!(
        log.contains("ping"),
        "the log saw what the attacher saw: {log:?}"
    );
}

#[tokio::test]
async fn s9_port_forward_relays_bytes_and_reports_a_refused_dial() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let echo_port = echo_listener().await;
    let closed = closed_port();

    let (negotiated, mut ws) = connect(
        &format!("ws://127.0.0.1:{port}/portForward/default/p?ports={echo_port},{closed}"),
        "v4.channel.k8s.io",
    )
    .await;
    assert_eq!(negotiated, "v4.channel.k8s.io");

    let mut opening = Vec::new();
    while opening.len() < 4 {
        match ws.next().await {
            Some(Ok(Message::Binary(b))) => opening.push(b),
            other => panic!("unexpected {other:?}"),
        }
    }
    let [lo, hi] = echo_port.to_le_bytes();
    assert_eq!(
        opening[0],
        vec![0, lo, hi],
        "data channel opens with its port"
    );
    assert_eq!(
        opening[1],
        vec![1, lo, hi],
        "error channel opens with its port"
    );
    let [clo, chi] = closed.to_le_bytes();
    assert_eq!(opening[2], vec![2, clo, chi]);
    assert_eq!(opening[3], vec![3, clo, chi]);

    let refused = read_until(&mut ws, 3, b"error forwarding port").await;
    assert!(
        String::from_utf8_lossy(&refused).contains(&closed.to_string()),
        "the refusal names the port"
    );

    ws.send(Message::Binary(
        [&[0u8][..], b"through the tunnel"].concat(),
    ))
    .await
    .expect("data");
    read_until(&mut ws, 0, b"through the tunnel").await;
}

/// An echo listener on loopback; its port.
async fn echo_listener() -> u16 {
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("echo bind");
    let echo_port = echo.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = echo.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    echo_port
}

/// A loopback port nothing listens on.
fn closed_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    l.local_addr().expect("addr").port()
}

use engenho_kubelet::port_forward_spdy::TUNNEL_SUBPROTOCOL;
use engenho_kubelet::port_forward_spdy::client::Client;
use engenho_kubelet::spdy::{Frame, RstStatus};

/// A SPDY client over a tunnel WebSocket.
fn tunnel_client(ws: Ws) -> Client {
    let (sink, stream) = ws.split();
    let read = stream.filter_map(|m| async move {
        match m {
            Ok(Message::Binary(b)) => Some(b),
            _ => None,
        }
    });
    let write = sink.with(|b: Vec<u8>| async move {
        Ok::<_, tokio_tungstenite::tungstenite::Error>(Message::Binary(b))
    });
    Client::over_messages(Box::pin(read), Box::pin(write))
}

/// Every frame until `done` holds for one, with a deadline.
async fn frames_until(c: &mut Client, mut done: impl FnMut(&Frame) -> bool) -> Vec<Frame> {
    let mut seen = Vec::new();
    let read = async {
        while let Some(f) = c.recv().await.expect("frames decode") {
            let stop = done(&f);
            seen.push(f);
            if stop {
                return;
            }
        }
        panic!("the session closed early after {seen:?}");
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("the expected frame arrived within the deadline");
    seen
}

/// The bytes `stream` carried in `frames`.
fn payload(frames: &[Frame], stream: u32) -> Vec<u8> {
    frames
        .iter()
        .filter_map(|f| match f {
            Frame::Data {
                stream: s, data, ..
            } if *s == stream => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn is_fin(f: &Frame, stream: u32) -> bool {
    matches!(f, Frame::Data { stream: s, fin: true, .. } if *s == stream)
}

/// Read until `stream` has carried `want`.
async fn echoed(c: &mut Client, stream: u32, want: &[u8]) {
    let mut got = Vec::new();
    frames_until(c, |f| {
        if let Frame::Data {
            stream: s, data, ..
        } = f
            && *s == stream
        {
            got.extend_from_slice(data);
        }
        got.windows(want.len()).any(|w| w == want)
    })
    .await;
}

async fn send_data(c: &mut Client, stream: u32, data: &[u8]) {
    c.send(&Frame::Data {
        stream,
        fin: false,
        data: data.to_vec(),
    })
    .await
    .expect("send");
}

#[tokio::test]
async fn s10_spdy_tunnelled_in_a_websocket_forwards_several_ports() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let (a, b, closed) = (echo_listener().await, echo_listener().await, closed_port());
    let (negotiated, ws) = connect(
        &format!("ws://127.0.0.1:{port}/portForward/default/p"),
        TUNNEL_SUBPROTOCOL,
    )
    .await;
    assert_eq!(
        negotiated, TUNNEL_SUBPROTOCOL,
        "no ?ports= needed: SPDY names them per stream"
    );
    let mut c = tunnel_client(ws);

    let fa = c.forward(a, "0").await.expect("forward a");
    let fb = c.forward(b, "1").await.expect("forward b");
    send_data(&mut c, fa.data, b"to a").await;
    send_data(&mut c, fb.data, b"to b").await;
    echoed(&mut c, fa.data, b"to a").await;
    echoed(&mut c, fb.data, b"to b").await;

    let fc = c
        .forward(closed, "2")
        .await
        .expect("streams open before the dial");
    let (mut reset, mut error_fin) = (false, false);
    let frames = frames_until(&mut c, |f| {
        reset |= matches!(f, Frame::RstStream { stream, .. } if *stream == fc.data);
        error_fin |= is_fin(f, fc.error);
        reset && error_fin
    })
    .await;
    let message = String::from_utf8_lossy(&payload(&frames, fc.error)).into_owned();
    assert!(
        message.contains("error forwarding port") && message.contains(&closed.to_string()),
        "{message:?}"
    );

    send_data(&mut c, fa.data, b"still up").await;
    echoed(&mut c, fa.data, b"still up").await;
}

#[tokio::test]
async fn s11_a_plain_spdy_upgrade_forwards_and_echoes_ping() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let echo = echo_listener().await;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let (mut c, head) = Client::upgrade(tcp, "/portForward/default/p", "portforward.k8s.io")
        .await
        .expect("101");
    let lower = head.to_ascii_lowercase();
    assert!(lower.contains("upgrade: spdy/3.1"), "{head}");
    assert!(
        lower.contains("x-stream-protocol-version: portforward.k8s.io"),
        "{head}"
    );

    c.send(&Frame::Ping { id: 1 }).await.expect("ping");
    frames_until(&mut c, |f| *f == Frame::Ping { id: 1 }).await;
    let f = c.forward(echo, "0").await.expect("forward");
    send_data(&mut c, f.data, b"over plain spdy").await;
    echoed(&mut c, f.data, b"over plain spdy").await;
}

#[tokio::test]
async fn s12_pod_eof_fins_both_streams_and_a_reset_drops_the_pod_connection() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let says_bye = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let bye_port = says_bye.local_addr().expect("addr").port();
    let held = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let held_port = held.local_addr().expect("addr").port();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut s, _) = says_bye.accept().await.expect("accept");
        s.write_all(b"bye").await.expect("write");
    });
    tokio::spawn(async move {
        let (mut s, _) = held.accept().await.expect("accept");
        let mut buf = [0u8; 64];
        while s.read(&mut buf).await.is_ok_and(|n| n > 0) {}
        let _ = dropped_tx.send(());
    });
    let (_, ws) = connect(
        &format!("ws://127.0.0.1:{port}/portForward/default/p"),
        TUNNEL_SUBPROTOCOL,
    )
    .await;
    let mut c = tunnel_client(ws);

    let f = c.forward(bye_port, "0").await.expect("forward");
    let mut data_fin = false;
    let frames = frames_until(&mut c, |fr| {
        data_fin |= is_fin(fr, f.data);
        is_fin(fr, f.error)
    })
    .await;
    assert!(
        data_fin,
        "the data stream FINs before the error stream: {frames:?}"
    );
    assert_eq!(payload(&frames, f.data), b"bye");
    assert!(
        payload(&frames, f.error).is_empty(),
        "success leaves the error stream empty"
    );

    let g = c.forward(held_port, "1").await.expect("forward");
    c.send(&Frame::RstStream {
        stream: g.data,
        status: RstStatus::Cancel,
    })
    .await
    .expect("reset");
    tokio::time::timeout(Duration::from_secs(10), dropped_rx)
        .await
        .expect("the pod connection is dropped on reset")
        .expect("signal");
    let frames = frames_until(&mut c, |fr| is_fin(fr, g.error)).await;
    assert!(
        !frames.iter().any(|fr| is_fin(fr, g.data)),
        "nothing more is sent on a reset stream: {frames:?}"
    );
}

#[tokio::test]
async fn s13_an_unnegotiated_upgrade_and_a_portless_stream_are_refused() {
    let dir = tempfile::tempdir().expect("tmp");
    let port = serve(dir.path()).await;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let head = Client::upgrade(tcp, "/portForward/default/p", "channel.k8s.io")
        .await
        .expect_err("not negotiated")
        .to_string();
    assert!(head.contains("HTTP/1.1 403"), "{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("x-accepted-stream-protocol-versions: portforward.k8s.io"),
        "{head}"
    );

    let (_, ws) = connect(
        &format!("ws://127.0.0.1:{port}/portForward/default/p"),
        TUNNEL_SUBPROTOCOL,
    )
    .await;
    let mut c = tunnel_client(ws);
    let refused = c.open("data", "", "0").await.expect_err("no port");
    assert!(refused.to_string().contains("RefusedStream"), "{refused}");
    let echo = echo_listener().await;
    let f = c
        .forward(echo, "1")
        .await
        .expect("the session survives a refusal");
    send_data(&mut c, f.data, b"ok").await;
    echoed(&mut c, f.data, b"ok").await;
}
