//! The HTTP layer: one [`Router`] behind engenho-serve's owned-connection
//! loop, over the local socket.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use engenho_serve::{Plain, ServeReport, StopSignal};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming as HyperBody;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Request, Response, StatusCode};
use tokio::net::{UnixListener, UnixStream};

use crate::grant::PeerCred;
use crate::router::{Answer, Incoming, MAX_BODY, Router};

/// How long a stop waits for in-flight control requests before severing
/// them. Long enough for a stop request to answer after its drain.
pub const GRACE: Duration = Duration::from_secs(5);

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
            peer: peer_of(stream),
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

/// One connection's service: its peer is fixed when it connects.
#[derive(Clone)]
struct Connection {
    router: Arc<Router>,
    peer: Option<PeerCred>,
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
        let peer = self.peer;
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
