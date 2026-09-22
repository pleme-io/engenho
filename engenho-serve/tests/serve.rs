//! The serve loop's stop contract, through its public surface only.
//!
//!   * a response that never ends is severed at the grace, and when `serve`
//!     returns nothing its service captured is alive;
//!   * an in-flight request that ends within the grace finishes normally;
//!   * dropping the `StopHandle` stops the server like `stop()` does;
//!   * after a stop the listener is gone: a new connection is refused;
//!   * a Unix listener works, and `make` sees the accepted stream, which is
//!     where a caller reads the peer's credentials.

use std::convert::Infallible;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, State};
use axum::routing::get;
use bytes::Bytes;
use engenho_serve::{Plain, ServeReport, StopHandle, serve, stop_channel};
use futures::StreamExt;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

const GRACE: Duration = Duration::from_millis(300);

/// Something a handler captures; its strong count says whether any
/// connection still holds the router.
#[derive(Clone)]
struct Held(Arc<()>);

/// One chunk, then nothing, forever — the shape of a WATCH.
async fn forever(State(held): State<Held>) -> Body {
    let stream = futures::stream::iter([Ok::<Bytes, Infallible>(Bytes::from_static(b"first\n"))])
        .chain(futures::stream::pending())
        // The stream owns `held`, so a live stream keeps it alive.
        .inspect(move |_| {
            std::hint::black_box(&held.0);
        });
    Body::from_stream(stream)
}

/// A request that takes a moment, then ends.
async fn slow() -> &'static str {
    tokio::time::sleep(Duration::from_millis(150)).await;
    "done"
}

fn router(held: Held) -> Router {
    Router::new()
        .route("/forever", get(forever))
        .route("/slow", get(slow))
        .with_state(held)
}

/// Serve `router` on an ephemeral loopback port.
async fn serve_tcp(router: Router) -> (std::net::SocketAddr, StopHandle, JoinHandle<ServeReport>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (handle, stop) = stop_channel();
    let task = tokio::spawn(serve(
        listener,
        Plain,
        move |_: &tokio::net::TcpStream, _: &std::net::SocketAddr| router.clone(),
        stop,
        GRACE,
    ));
    (addr, handle, task)
}

/// One HTTP/1 GET over `io`; the client connection runs in its own task.
async fn get_over<Io>(io: Io, path: &str) -> hyper::Response<Incoming>
where
    Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .unwrap();
    tokio::spawn(conn);
    sender
        .send_request(
            hyper::Request::get(path)
                .header("host", "engenho.test")
                .body(Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn a_response_that_never_ends_is_severed_and_releases_what_it_captured() {
    let marker = Arc::new(());
    let (addr, handle, task) = serve_tcp(router(Held(marker.clone()))).await;

    let resp = get_over(
        tokio::net::TcpStream::connect(addr).await.unwrap(),
        "/forever",
    )
    .await;
    assert_eq!(resp.status(), 200);
    let mut body = resp.into_body();
    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(&first[..], b"first\n");

    handle.stop();
    let report = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("serve returns within the grace plus the abort")
        .unwrap();
    assert_eq!(
        report,
        ServeReport {
            open_at_stop: 1,
            drained: 0,
            severed: 1,
        }
    );
    drop(body);
    assert_eq!(
        Arc::strong_count(&marker),
        1,
        "a severed connection still holds the router's state"
    );
}

#[tokio::test]
async fn an_in_flight_request_that_ends_within_the_grace_finishes() {
    let marker = Arc::new(());
    let (addr, handle, task) = serve_tcp(router(Held(marker.clone()))).await;

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = tokio::spawn(async move {
        let resp = get_over(stream, "/slow").await;
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, body)
    });
    // Let the request reach the handler, then stop while it is in flight.
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.stop();

    let (status, body) = request.await.unwrap();
    assert_eq!(status, 200);
    assert_eq!(&body[..], b"done");

    let report = task.await.unwrap();
    assert_eq!(report.open_at_stop, 1);
    assert_eq!(report.severed, 0, "a request that ends is never severed");
    assert_eq!(Arc::strong_count(&marker), 1);
}

#[tokio::test]
async fn dropping_the_handle_stops_the_server() {
    let marker = Arc::new(());
    let (_addr, handle, task) = serve_tcp(router(Held(marker.clone()))).await;
    drop(handle);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("a server whose owner is gone stops")
        .unwrap();
    assert_eq!(Arc::strong_count(&marker), 1);
}

#[tokio::test]
async fn after_a_stop_new_connections_are_refused() {
    let (addr, handle, task) = serve_tcp(router(Held(Arc::new(())))).await;
    handle.stop();
    task.await.unwrap();
    let refused = tokio::net::TcpStream::connect(addr).await;
    assert!(
        refused.is_err(),
        "the listener outlived the stop: {refused:?}"
    );
}

/// The peer's uid, as the per-connection `make` read it off the socket.
#[derive(Clone, Copy)]
struct PeerUid(u32);

#[tokio::test]
async fn a_unix_listener_serves_and_make_sees_the_peer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let (handle, stop) = stop_channel();

    let app = Router::new().route(
        "/whoami",
        get(|Extension(PeerUid(uid)): Extension<PeerUid>| async move { uid.to_string() }),
    );
    let task = tokio::spawn(serve(
        listener,
        Plain,
        move |stream: &tokio::net::UnixStream, _: &tokio::net::unix::SocketAddr| {
            let uid = stream.peer_cred().map_or(u32::MAX, |c| c.uid());
            app.clone().layer(Extension(PeerUid(uid)))
        },
        stop,
        GRACE,
    ));

    let resp = get_over(
        tokio::net::UnixStream::connect(&path).await.unwrap(),
        "/whoami",
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    // Our own uid, read without libc: the owner of a file we just created.
    let ours = std::fs::metadata(dir.path()).unwrap().uid();
    assert_eq!(String::from_utf8_lossy(&body), ours.to_string());

    handle.stop();
    task.await.unwrap();
}
