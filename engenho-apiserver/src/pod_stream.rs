//! The Pod streaming subresources (`exec`, `attach`, `portforward`): the
//! seam the apiserver relays a client's WebSocket through to the kubelet of
//! the pod's node, byte for byte.

use std::time::Duration;

use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use engenho_types::generated_v1_34::Subresource;
use futures::{Sink, SinkExt, Stream, StreamExt};
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::error::ApiError;

pub const KUBELET_DEFAULT_PORT: u16 = 10250;

const CLOSE_DRAIN: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PodStreamKind {
    Exec,
    Attach,
    PortForward,
}

impl PodStreamKind {
    #[must_use]
    pub const fn of(sub: Subresource) -> Option<Self> {
        match sub {
            Subresource::Exec => Some(Self::Exec),
            Subresource::Attach => Some(Self::Attach),
            Subresource::PortForward => Some(Self::PortForward),
            Subresource::Status | Subresource::Scale | Subresource::Log | Subresource::Token => {
                None
            }
        }
    }

    #[must_use]
    pub const fn subresource(self) -> &'static str {
        match self {
            Self::Exec => "exec",
            Self::Attach => "attach",
            Self::PortForward => "portforward",
        }
    }

    #[must_use]
    pub const fn options_kind(self) -> &'static str {
        match self {
            Self::Exec => "PodExecOptions",
            Self::Attach => "PodAttachOptions",
            Self::PortForward => "PodPortForwardOptions",
        }
    }

    #[must_use]
    pub const fn kubelet_segment(self) -> &'static str {
        match self {
            Self::Exec => "exec",
            Self::Attach => "attach",
            Self::PortForward => "portForward",
        }
    }

    #[must_use]
    pub const fn targets_container(self) -> bool {
        !matches!(self, Self::PortForward)
    }

    #[must_use]
    pub fn is_streaming_subresource(segment: &str) -> bool {
        matches!(segment, "exec" | "attach" | "portforward")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodStreamTarget {
    pub kind: PodStreamKind,
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub node_name: Option<String>,
    pub query: String,
    pub protocols: Vec<String>,
}

impl PodStreamTarget {
    #[must_use]
    pub fn kubelet_path(&self) -> String {
        let mut path = format!(
            "/{}/{}/{}",
            self.kind.kubelet_segment(),
            self.namespace,
            self.pod
        );
        if self.kind.targets_container()
            && let Some(c) = &self.container
        {
            path.push('/');
            path.push_str(c);
        }
        if !self.query.is_empty() {
            path.push('?');
            path.push_str(&self.query);
        }
        path
    }
}

pub trait FrameDuplex:
    Stream<Item = Result<Message, tungstenite::Error>>
    + Sink<Message, Error = tungstenite::Error>
    + Send
    + Unpin
{
}

impl<T> FrameDuplex for T where
    T: Stream<Item = Result<Message, tungstenite::Error>>
        + Sink<Message, Error = tungstenite::Error>
        + Send
        + Unpin
{
}

pub struct PodStreamUpstream {
    pub protocol: Option<String>,
    pub frames: Box<dyn FrameDuplex>,
}

impl std::fmt::Debug for PodStreamUpstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodStreamUpstream")
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait PodStreamProxy: Send + Sync {
    /// Open the kubelet side of a streaming request.
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when the pod's node is unknown,
    /// [`ApiError::ServiceUnavailable`] when its kubelet cannot be reached.
    async fn open(&self, target: &PodStreamTarget) -> Result<PodStreamUpstream, ApiError>;
}

/// Dial the kubelet at `authority` (`host:port`) over plain WebSocket,
/// offering the client's subprotocols unchanged.
///
/// # Errors
///
/// [`ApiError::NotFound`] / [`ApiError::BadRequest`] when the kubelet answers
/// 404 / 400, [`ApiError::ServiceUnavailable`] for anything else.
pub async fn dial_kubelet(
    authority: &str,
    target: &PodStreamTarget,
) -> Result<PodStreamUpstream, ApiError> {
    let url = format!("ws://{authority}{}", target.kubelet_path());
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| ApiError::Internal(format!("kubelet stream URL {url}: {e}")))?;
    if !target.protocols.is_empty() {
        let offered = HeaderValue::from_str(&target.protocols.join(", "))
            .map_err(|e| ApiError::BadRequest(format!("unusable Sec-WebSocket-Protocol: {e}")))?;
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, offered);
    }
    let (ws, response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| upstream_error(authority, target, e))?;
    let protocol = response
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    Ok(PodStreamUpstream {
        protocol,
        frames: Box::new(ws),
    })
}

fn upstream_error(authority: &str, target: &PodStreamTarget, e: tungstenite::Error) -> ApiError {
    let what = format!(
        "pods/{} {}/{} via kubelet {authority}",
        target.kind.subresource(),
        target.namespace,
        target.pod
    );
    match e {
        tungstenite::Error::Http(resp) => {
            let body = resp
                .body()
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default()
                .into_owned();
            match resp.status() {
                StatusCode::NOT_FOUND => ApiError::NotFound(format!("{what}: {body}")),
                StatusCode::BAD_REQUEST => ApiError::BadRequest(format!("{what}: {body}")),
                s => ApiError::ServiceUnavailable(format!("{what}: kubelet answered {s}: {body}")),
            }
        }
        other => ApiError::ServiceUnavailable(format!("{what}: kubelet unreachable: {other}")),
    }
}

/// The kubelet `host:port` a Node publishes: the first `InternalIP`, else
/// `ExternalIP`, else `Hostname` from `status.addresses`, at
/// `status.daemonEndpoints.kubeletEndpoint.Port` (default 10250).
#[must_use]
pub fn kubelet_authority_of_node(node: &serde_json::Value) -> Option<String> {
    let status = node.get("status")?;
    let addresses = status.get("addresses")?.as_array()?;
    let pick = |ty: &str| {
        addresses
            .iter()
            .filter(|a| a.get("type").and_then(|t| t.as_str()) == Some(ty))
            .find_map(|a| a.get("address").and_then(|x| x.as_str()))
            .filter(|s| !s.is_empty())
    };
    let host = pick("InternalIP")
        .or_else(|| pick("ExternalIP"))
        .or_else(|| pick("Hostname"))?;
    let port = status
        .pointer("/daemonEndpoints/kubeletEndpoint/Port")
        .and_then(serde_json::Value::as_u64)
        .and_then(|p| u16::try_from(p).ok())
        .filter(|p| *p != 0)
        .unwrap_or(KUBELET_DEFAULT_PORT);
    Some(if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    })
}

/// The container a request names, else the pod's
/// `kubectl.kubernetes.io/default-container`, else its only container.
///
/// # Errors
///
/// [`ApiError::BadRequest`] when the pod has several containers and nothing
/// picks one, or the named one does not exist.
pub fn choose_container(
    pod: &serde_json::Value,
    requested: Option<&str>,
) -> Result<String, ApiError> {
    let names: Vec<&str> = pod
        .pointer("/spec/containers")
        .and_then(serde_json::Value::as_array)
        .map(|cs| {
            cs.iter()
                .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
                .collect()
        })
        .unwrap_or_default();
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or_default();
    if let Some(c) = requested.filter(|c| !c.is_empty()) {
        let known = names.contains(&c)
            || ["initContainers", "ephemeralContainers"].iter().any(|k| {
                pod.pointer(&format!("/spec/{k}"))
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|cs| {
                        cs.iter()
                            .any(|x| x.get("name").and_then(|n| n.as_str()) == Some(c))
                    })
            });
        return if known {
            Ok(c.to_owned())
        } else {
            Err(ApiError::BadRequest(format!(
                "container {c} is not valid for pod {pod_name}"
            )))
        };
    }
    if let Some(d) = pod
        .pointer("/metadata/annotations/kubectl.kubernetes.io~1default-container")
        .and_then(|v| v.as_str())
        .filter(|d| names.contains(d))
    {
        return Ok(d.to_owned());
    }
    match names.as_slice() {
        [only] => Ok((*only).to_owned()),
        [] => Err(ApiError::BadRequest(format!(
            "pod {pod_name} has no containers"
        ))),
        many => Err(ApiError::BadRequest(format!(
            "a container name must be specified for pod {pod_name}, choose one of: [{}]",
            many.join(" ")
        ))),
    }
}

/// A client WebSocket handshake, taken off the request before the handler
/// runs. Absent when the request is not `Upgrade: websocket`.
pub struct StreamUpgrade {
    on_upgrade: hyper::upgrade::OnUpgrade,
    key: HeaderValue,
    protocols: Vec<String>,
}

impl std::fmt::Debug for StreamUpgrade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamUpgrade")
            .field("protocols", &self.protocols)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeRefusal {
    NotWebSocket,
    BadHandshake,
}

impl IntoResponse for UpgradeRefusal {
    fn into_response(self) -> Response {
        websocket_only(PodStreamKind::Exec, self).into_response()
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for StreamUpgrade {
    type Rejection = UpgradeRefusal;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if !is_websocket_upgrade(&parts.headers) {
            return Err(UpgradeRefusal::NotWebSocket);
        }
        let key = parts
            .headers
            .get(header::SEC_WEBSOCKET_KEY)
            .cloned()
            .ok_or(UpgradeRefusal::BadHandshake)?;
        if parts
            .headers
            .get(header::SEC_WEBSOCKET_VERSION)
            .is_none_or(|v| v.as_bytes() != b"13")
        {
            return Err(UpgradeRefusal::BadHandshake);
        }
        let on_upgrade = parts
            .extensions
            .remove::<hyper::upgrade::OnUpgrade>()
            .ok_or(UpgradeRefusal::BadHandshake)?;
        Ok(Self {
            on_upgrade,
            key,
            protocols: offered_protocols(&parts.headers),
        })
    }
}

impl StreamUpgrade {
    #[must_use]
    pub fn protocols(&self) -> &[String] {
        &self.protocols
    }

    /// Answer 101 with the subprotocol the kubelet chose, then relay frames
    /// between the client and `upstream` until either side closes.
    #[must_use]
    pub fn relay(self, upstream: PodStreamUpstream) -> Response {
        let mut response = StatusCode::SWITCHING_PROTOCOLS.into_response();
        let headers = response.headers_mut();
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        if let Ok(accept) = HeaderValue::from_str(&derive_accept_key(self.key.as_bytes())) {
            headers.insert(header::SEC_WEBSOCKET_ACCEPT, accept);
        }
        if let Some(p) = upstream
            .protocol
            .as_deref()
            .and_then(|p| HeaderValue::from_str(p).ok())
        {
            headers.insert(header::SEC_WEBSOCKET_PROTOCOL, p);
        }
        let on_upgrade = self.on_upgrade;
        tokio::spawn(async move {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let client = WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        Role::Server,
                        None,
                    )
                    .await;
                    pump(client, upstream.frames).await;
                }
                Err(e) => tracing::warn!(error = %e, "pod stream upgrade failed"),
            }
        });
        response
    }
}

#[must_use]
pub fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let has_token = |name: header::HeaderName, token: &str| {
        headers.get_all(name).iter().any(|v| {
            v.to_str()
                .is_ok_and(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
        })
    };
    has_token(header::UPGRADE, "websocket") && has_token(header::CONNECTION, "upgrade")
}

fn offered_protocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The refusal for a streaming request that is not a WebSocket upgrade
/// (SPDY, or a plain request).
#[must_use]
pub fn websocket_only(kind: PodStreamKind, refusal: UpgradeRefusal) -> ApiError {
    let sub = kind.subresource();
    match refusal {
        UpgradeRefusal::NotWebSocket => ApiError::BadRequest(format!(
            "pods/{sub} is served over WebSocket only (Upgrade: websocket, v5.channel.k8s.io / \
             v4.channel.k8s.io / portforward.k8s.io); SPDY is not supported. kubectl >= 1.31 \
             uses WebSocket by default"
        )),
        UpgradeRefusal::BadHandshake => ApiError::BadRequest(format!(
            "pods/{sub}: malformed WebSocket handshake (Sec-WebSocket-Key and \
             Sec-WebSocket-Version: 13 are required)"
        )),
    }
}

async fn pump<C, U>(client: C, upstream: U)
where
    C: FrameDuplex,
    U: FrameDuplex,
{
    let (mut client_tx, mut client_rx) = client.split();
    let (mut up_tx, mut up_rx) = upstream.split();
    loop {
        tokio::select! {
            m = client_rx.next() => match forward(m, &mut up_tx).await {
                Step::Continue => {}
                Step::Closed => {
                    drain(&mut up_rx).await;
                    let _ = client_tx.close().await;
                    return;
                }
            },
            m = up_rx.next() => match forward(m, &mut client_tx).await {
                Step::Continue => {}
                Step::Closed => {
                    drain(&mut client_rx).await;
                    let _ = up_tx.close().await;
                    return;
                }
            },
        }
    }
}

enum Step {
    Continue,
    Closed,
}

async fn forward<T>(m: Option<Result<Message, tungstenite::Error>>, to: &mut T) -> Step
where
    T: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    match m {
        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => Step::Continue,
        Some(Ok(close @ Message::Close(_))) => {
            let _ = to.send(close).await;
            Step::Closed
        }
        Some(Ok(msg)) => {
            if to.send(msg).await.is_err() {
                Step::Closed
            } else {
                Step::Continue
            }
        }
        Some(Err(_)) | None => {
            let _ = to.close().await;
            Step::Closed
        }
    }
}

async fn drain<S>(rx: &mut S)
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    let _ = tokio::time::timeout(CLOSE_DRAIN, async {
        while let Some(Ok(m)) = rx.next().await {
            if m.is_close() {
                break;
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target(kind: PodStreamKind, container: Option<&str>, query: &str) -> PodStreamTarget {
        PodStreamTarget {
            kind,
            namespace: "ns".into(),
            pod: "p".into(),
            container: container.map(str::to_owned),
            node_name: None,
            query: query.into(),
            protocols: vec![],
        }
    }

    #[test]
    fn kind_maps_from_catalog_subresource_only_for_streams() {
        assert_eq!(
            PodStreamKind::of(Subresource::Exec),
            Some(PodStreamKind::Exec)
        );
        assert_eq!(
            PodStreamKind::of(Subresource::Attach),
            Some(PodStreamKind::Attach)
        );
        assert_eq!(
            PodStreamKind::of(Subresource::PortForward),
            Some(PodStreamKind::PortForward)
        );
        assert_eq!(PodStreamKind::of(Subresource::Log), None);
        for k in [
            PodStreamKind::Exec,
            PodStreamKind::Attach,
            PodStreamKind::PortForward,
        ] {
            assert!(PodStreamKind::is_streaming_subresource(k.subresource()));
        }
        assert!(!PodStreamKind::is_streaming_subresource("log"));
    }

    #[test]
    fn kubelet_paths_follow_the_kubelet_routes() {
        assert_eq!(
            target(PodStreamKind::Exec, Some("c"), "command=sh&stdin=true").kubelet_path(),
            "/exec/ns/p/c?command=sh&stdin=true"
        );
        assert_eq!(
            target(PodStreamKind::Attach, Some("c"), "").kubelet_path(),
            "/attach/ns/p/c"
        );
        assert_eq!(
            target(PodStreamKind::PortForward, Some("ignored"), "ports=80").kubelet_path(),
            "/portForward/ns/p?ports=80"
        );
    }

    #[test]
    fn node_authority_prefers_internal_ip_and_daemon_port() {
        let node = json!({"status": {
            "addresses": [
                {"type": "Hostname", "address": "rio"},
                {"type": "InternalIP", "address": "10.0.0.7"}
            ],
            "daemonEndpoints": {"kubeletEndpoint": {"Port": 10251}}
        }});
        assert_eq!(
            kubelet_authority_of_node(&node).as_deref(),
            Some("10.0.0.7:10251")
        );
        let v6 = json!({"status": {"addresses": [{"type": "InternalIP", "address": "fd00::1"}]}});
        assert_eq!(
            kubelet_authority_of_node(&v6).as_deref(),
            Some("[fd00::1]:10250")
        );
        assert_eq!(kubelet_authority_of_node(&json!({"status": {}})), None);
    }

    #[test]
    fn container_choice_matches_upstream_defaulting() {
        let one = json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "a"}]}});
        assert_eq!(choose_container(&one, None).unwrap(), "a");
        assert!(choose_container(&one, Some("zz")).is_err());
        let two = json!({"metadata": {"name": "p", "annotations": {
            "kubectl.kubernetes.io/default-container": "b"}},
            "spec": {"containers": [{"name": "a"}, {"name": "b"}]}});
        assert_eq!(choose_container(&two, None).unwrap(), "b");
        assert_eq!(choose_container(&two, Some("a")).unwrap(), "a");
        let ambiguous = json!({"metadata": {"name": "p"},
            "spec": {"containers": [{"name": "a"}, {"name": "b"}]}});
        assert!(matches!(
            choose_container(&ambiguous, None),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn upgrade_detection_needs_both_headers() {
        let mut h = HeaderMap::new();
        h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        assert!(!is_websocket_upgrade(&h));
        h.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, Upgrade"),
        );
        assert!(is_websocket_upgrade(&h));
        h.insert(header::UPGRADE, HeaderValue::from_static("SPDY/3.1"));
        assert!(!is_websocket_upgrade(&h));
    }

    #[test]
    fn offered_protocols_split_and_keep_order() {
        let mut h = HeaderMap::new();
        h.append(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("v5.channel.k8s.io, v4.channel.k8s.io"),
        );
        h.append(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("channel.k8s.io"),
        );
        assert_eq!(
            offered_protocols(&h),
            ["v5.channel.k8s.io", "v4.channel.k8s.io", "channel.k8s.io"]
        );
    }
}
