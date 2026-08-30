//! ONE test that opens a real WebSocket against a real listener.
//!
//! ★ WHY THIS EXISTS WHEN THE PUMP IS ALREADY UNIT-TESTED.
//! `exec_session` proves the FRAMES are right and `server`'s oneshot tests
//! prove the REFUSALS are right, and both would stay green against a route
//! that never upgrades at all. The failure modes only a socket can see —
//! no upgrade, the subprotocol not echoed back, frames sent as text where
//! kubectl expects binary — all present to a user as a session that hangs,
//! which is exactly the outcome the refusal design exists to avoid.
//!
//! Invariants:
//!   S1 the handshake completes and negotiates `v5.channel.k8s.io`
//!   S2 stdout arrives on channel 1, as a BINARY frame
//!   S3 the terminating status frame arrives on channel 3 and is last
//!   S4 a runtime failure arrives as a Status, not as an exit code

use std::sync::Arc;

use engenho_kubelet::backend::ExecOutcome;
use engenho_kubelet::server::{KubeletApi, KubeletServer};
use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

struct Api;

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
        argv: &[String],
    ) -> Result<ExecOutcome, String> {
        if argv.first().map(String::as_str) == Some("missing") {
            return Err("no such container".into());
        }
        Ok(ExecOutcome {
            exit_code: 0,
            stdout: "hello from the container\n".into(),
            stderr: String::new(),
        })
    }
}

/// Bind an ephemeral port, serve the kubelet routes on it, return the port.
async fn serve() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let app = KubeletServer::new(Arc::new(Api)).routes();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    port
}

async fn frames(uri: &str) -> (String, Vec<(u8, Vec<u8>)>) {
    let mut req = uri.into_client_request().expect("request");
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "v5.channel.k8s.io".parse().expect("header"),
    );
    let (mut ws, response) = tokio_tungstenite::connect_async(req)
        .await
        .expect("handshake");
    let negotiated = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();

    let mut out = Vec::new();
    while let Some(Ok(msg)) = ws.next().await {
        match msg {
            Message::Binary(b) if !b.is_empty() => out.push((b[0], b[1..].to_vec())),
            Message::Close(_) => break,
            // A TEXT frame here is a real defect, not a variation: kubectl
            // reads binary. Recorded rather than skipped so it fails loudly.
            Message::Text(t) => out.push((u8::MAX, t.into_bytes())),
            _ => {}
        }
    }
    (negotiated, out)
}

#[tokio::test]
async fn s1_s2_s3_a_command_runs_and_its_output_arrives_framed() {
    let port = serve().await;
    let (negotiated, got) = frames(&format!(
        "ws://127.0.0.1:{port}/exec/default/p/c?command=echo&command=hi"
    ))
    .await;

    // S1
    assert_eq!(negotiated, "v5.channel.k8s.io", "subprotocol echoed back");

    // S2 — channel 1, and never u8::MAX (which would mean a text frame).
    let stdout = got
        .iter()
        .find(|(ch, _)| *ch == 1)
        .expect("a stdout frame arrived");
    assert_eq!(
        String::from_utf8_lossy(&stdout.1),
        "hello from the container\n"
    );
    assert!(
        got.iter().all(|(ch, _)| *ch != u8::MAX),
        "every frame is binary, not text"
    );

    // S3 — the status frame is last, and says Success for exit 0.
    let (ch, payload) = got.last().expect("at least one frame");
    assert_eq!(*ch, 3, "the last frame is the error channel");
    let status: serde_json::Value = serde_json::from_slice(payload).expect("status is JSON");
    assert_eq!(status["status"], "Success");
}

#[tokio::test]
async fn s4_a_runtime_failure_is_a_status_not_an_exit_code() {
    let port = serve().await;
    let (_, got) = frames(&format!(
        "ws://127.0.0.1:{port}/exec/default/p/c?command=missing"
    ))
    .await;

    assert_eq!(got.len(), 1, "only the error frame: {got:?}");
    let (ch, payload) = &got[0];
    assert_eq!(*ch, 3);
    let status: serde_json::Value = serde_json::from_slice(payload).expect("JSON");
    assert_eq!(status["reason"], "InternalError");
    assert!(
        status.get("details").is_none(),
        "a backend failure carries no exit code: {status}"
    );
}
