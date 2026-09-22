//! The HTTP layer: one [`Router`] behind engenho-serve's owned-connection
//! loop, over the local socket ([`serve_uds`]) and the remote listener
//! ([`serve_tls`]). Either way a connection's peer is decided once, when it
//! connects, from what the transport proved — never from the request.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use engenho_control_types::pin::Spki;
use engenho_serve::{Plain, ServeReport, StopSignal, Tls};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming as HyperBody;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Request, Response, StatusCode};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio_rustls::server::TlsStream;

use crate::grant::{Peer, PeerCred};
use crate::pins::Pins;
use crate::router::{Answer, Incoming, MAX_BODY, Router};

/// How long a stop waits for in-flight control requests before severing
/// them. Long enough for a stop request to answer after its drain.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long a remote peer has to finish its TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve `router` on `listener` until `stop`.
pub async fn serve_uds(
    listener: UnixListener,
    router: Arc<Router>,
    stop: StopSignal,
    grace: Duration,
) -> ServeReport {
    engenho_serve::serve(
        listener,
        Plain,
        move |stream: &UnixStream, _: &tokio::net::unix::SocketAddr| Connection {
            router: Arc::clone(&router),
            peer: PeerSource::Local(peer_of(stream)),
        },
        stop,
        grace,
    )
    .await
}

/// Serve `router` over TLS on `listener` until `stop`. The handshake admits
/// only a client whose key `pins` holds; each REQUEST's peer is that client
/// as `pins` has it then, so a pin revoked (or a tier lowered) while a
/// connection is open takes effect on its next request.
pub async fn serve_tls(
    listener: TcpListener,
    tls: Arc<rustls::ServerConfig>,
    pins: Pins,
    router: Arc<Router>,
    stop: StopSignal,
    grace: Duration,
) -> ServeReport {
    engenho_serve::serve(
        listener,
        Tls::new(tls, HANDSHAKE_TIMEOUT),
        move |stream: &TlsStream<TcpStream>, _: &SocketAddr| Connection {
            router: Arc::clone(&router),
            peer: PeerSource::Remote {
                spki: proven_key(stream),
                pins: pins.clone(),
            },
        },
        stop,
        grace,
    )
    .await
}

/// What the kernel says about the socket's other end.
fn peer_of(stream: &UnixStream) -> Option<PeerCred> {
    stream.peer_cred().ok().map(|c| PeerCred {
        uid: c.uid(),
        gid: c.gid(),
        pid: c.pid(),
    })
}

/// The pin of the key the handshake proved the client holds.
fn proven_key(stream: &TlsStream<TcpStream>) -> Option<Spki> {
    let cert = stream.get_ref().1.peer_certificates()?.first()?;
    Spki::of_certificate(cert).ok()
}

/// Where a connection's peer comes from: fixed when it connects (a local
/// peer's credentials), or its proven key looked up per request.
#[derive(Clone)]
enum PeerSource {
    Local(Option<PeerCred>),
    Remote { spki: Option<Spki>, pins: Pins },
}

impl PeerSource {
    fn peer(&self) -> Option<Peer> {
        match self {
            Self::Local(cred) => cred.map(Peer::Local),
            Self::Remote { spki, pins } => {
                spki.and_then(|spki| pins.client(&spki)).map(Peer::Remote)
            }
        }
    }
}

/// One connection's service.
#[derive(Clone)]
struct Connection {
    router: Arc<Router>,
    peer: PeerSource,
}

type Reply = Pin<Box<dyn Future<Output = Result<Response<Full<Bytes>>, Infallible>> + Send>>;

impl tower_service::Service<Request<HyperBody>> for Connection {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Reply;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<HyperBody>) -> Reply {
        let router = Arc::clone(&self.router);
        let peer = self.peer.peer();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let answer = match Limited::new(body, MAX_BODY).collect().await {
                Ok(collected) => {
                    let incoming = Incoming {
                        method: parts.method.as_str().to_owned(),
                        path: parts.uri.path().to_owned(),
                        query: parts.uri.query().map(str::to_owned),
                        headers: parts
                            .headers
                            .iter()
                            .filter_map(|(k, v)| {
                                Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned()))
                            })
                            .collect(),
                        body: collected.to_bytes(),
                    };
                    router.handle(peer, incoming).await
                }
                Err(err) => Answer::error(&engenho_control_types::ControlError::refused(
                    engenho_control_types::types::RefusalReason::InvalidValue,
                    format!("the body could not be read (at most {MAX_BODY} bytes): {err}"),
                )),
            };
            Ok(respond(answer))
        })
    }
}

fn respond(answer: Answer) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(answer.body));
    *response.status_mut() =
        StatusCode::from_u16(answer.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(answer.content_type));
    response
}
