//! HTTP serving whose connection tasks the server **owns**.
//!
//! ## Why this exists
//!
//! axum 0.7's `serve` and axum-server 0.7 both run every accepted connection
//! in a detached `tokio::spawn`. Stopping either one ends the accept loop and
//! nothing else: a connection that never finishes on its own — a Kubernetes
//! WATCH, which by design streams until the client leaves — keeps running
//! after the server has "stopped", and keeps every `Arc` its service holds.
//! For engenho's apiserver that was the whole store: measured 2026-09-19,
//! `StoreStillShared { strong_count: 57, after: ApiserverStopped }` on every
//! stop with kubectl, k9s or an informer watching
//! (`engenho-runtime/tests/shutdown_stages.rs`).
//!
//! [`serve`] runs each connection in a [`tokio::task::JoinSet`] that the
//! serve loop itself holds. A stop:
//!
//! 1. ends the accept loop and drops the listener;
//! 2. asks every open connection to shut down gracefully (in-flight requests
//!    finish; HTTP/1 keep-alive and HTTP/2 new streams are refused);
//! 3. waits a bounded grace for them to close on their own;
//! 4. **aborts every connection still open and awaits it**, so when
//!    [`serve`] returns, no connection task — and nothing it captured — is
//!    alive.
//!
//! A long-poll that never ends is therefore *severed* at the grace, which is
//! what a Kubernetes apiserver does on shutdown: the client sees the
//! connection close and re-watches.
//!
//! ## Absence of the owner is a stop
//!
//! The stop is a [`StopHandle`] / [`StopSignal`] pair. Dropping the handle
//! stops the server exactly as [`StopHandle::stop`] does, so a server whose
//! owner went away (a boot that failed after binding, a dropped struct) does
//! not keep serving behind everyone's back.
//!
//! ## One loop, three listeners
//!
//! [`Listener`] abstracts the accept (TCP and Unix domain sockets ship here)
//! and [`Handshake`] the per-connection setup ([`Plain`] or [`Tls`]). The
//! per-connection service is built by a closure that sees the finished
//! handshake and the peer address, which is where a caller reads a verified
//! client certificate or a Unix peer credential and injects it into every
//! request of that connection.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

/// How long [`serve`] backs off after an accept error that is not about one
/// connection (e.g. the process is out of file descriptors), so it does not
/// spin on a listener that cannot accept.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

// ── stop ────────────────────────────────────────────────────────────────

/// The owner's side of a server's stop. [`Self::stop`] or dropping it stops
/// every [`serve`] loop listening on the paired [`StopSignal`].
#[derive(Debug)]
pub struct StopHandle(watch::Sender<bool>);

/// The server's side of a stop: cheap to clone, one per connection.
#[derive(Debug, Clone)]
pub struct StopSignal(watch::Receiver<bool>);

/// A new, not-yet-stopped stop pair.
#[must_use]
pub fn stop_channel() -> (StopHandle, StopSignal) {
    let (tx, rx) = watch::channel(false);
    (StopHandle(tx), StopSignal(rx))
}

impl StopHandle {
    /// Stop every server listening on the paired signal. Idempotent.
    pub fn stop(&self) {
        // `send_replace` stores the value even with no receiver left, so a
        // later `subscribe`-style clone would still see it.
        self.0.send_replace(true);
    }
}

impl StopSignal {
    /// Completes once the stop is requested or its [`StopHandle`] is gone.
    ///
    /// Cancel-safe: it only reads the channel, so a `select!` may drop it and
    /// call it again.
    pub async fn stopped(&mut self) {
        // `wait_for` returns `Err` only when the sender is gone, which is a
        // stop too (the owner went away).
        let _ = self.0.wait_for(|stopped| *stopped).await;
    }

    /// Whether the stop has already been requested (or its handle dropped).
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self.0.borrow() || self.0.has_changed().is_err()
    }
}

// ── listener ────────────────────────────────────────────────────────────

/// Something that accepts connections.
pub trait Listener: Send + 'static {
    /// One accepted connection's byte stream.
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// The peer's address, as this listener knows it.
    type Addr: fmt::Debug + Send + 'static;

    /// Accept the next connection.
    fn accept(&mut self) -> impl Future<Output = io::Result<(Self::Io, Self::Addr)>> + Send;
}

impl Listener for tokio::net::TcpListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        let (stream, addr) = tokio::net::TcpListener::accept(self).await?;
        // A watch or a small request/response should not wait on Nagle.
        let _ = stream.set_nodelay(true);
        Ok((stream, addr))
    }
}

impl Listener for tokio::net::UnixListener {
    type Io = tokio::net::UnixStream;
    type Addr = tokio::net::unix::SocketAddr;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        tokio::net::UnixListener::accept(self).await
    }
}

// ── handshake ───────────────────────────────────────────────────────────

/// What turns an accepted byte stream into the stream HTTP is spoken over.
pub trait Handshake<Io>: Clone + Send + Sync + 'static {
    /// The stream after the handshake.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Run the handshake.
    fn handshake(&self, io: Io) -> impl Future<Output = io::Result<Self::Stream>> + Send;
}

/// No handshake: HTTP directly over the accepted stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct Plain;

impl<Io> Handshake<Io> for Plain
where
    Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = Io;

    fn handshake(&self, io: Io) -> impl Future<Output = io::Result<Io>> + Send {
        std::future::ready(Ok(io))
    }
}

/// A rustls server handshake, bounded by a timeout so a peer that opens a
/// socket and says nothing cannot hold a connection slot open until the stop.
#[derive(Clone)]
pub struct Tls {
    acceptor: tokio_rustls::TlsAcceptor,
    timeout: Duration,
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tls")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Tls {
    /// A TLS handshake over `config`, abandoned after `timeout`.
    #[must_use]
    pub fn new(config: Arc<rustls::ServerConfig>, timeout: Duration) -> Self {
        Self {
            acceptor: tokio_rustls::TlsAcceptor::from(config),
            timeout,
        }
    }
}

impl<Io> Handshake<Io> for Tls
where
    Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = tokio_rustls::server::TlsStream<Io>;

    async fn handshake(&self, io: Io) -> io::Result<Self::Stream> {
        match tokio::time::timeout(self.timeout, self.acceptor.accept(io)).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            )),
        }
    }
}

// ── serve ───────────────────────────────────────────────────────────────

/// What a [`serve`] loop's stop did with the connections it found open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServeReport {
    /// Connections open when the stop began.
    pub open_at_stop: usize,
    /// Of those, the ones that closed on their own within the grace.
    pub drained: usize,
    /// The ones still open when the grace ran out: aborted, then awaited.
    pub severed: usize,
}

/// Serve HTTP (1 and 2) on `listener` until `stop`, then drain for `grace`
/// and sever whatever is left. See the crate docs for the full contract.
///
/// `make` builds the service for one connection from the finished handshake
/// and the peer address; it is where per-connection facts (a verified client
/// certificate, a Unix peer credential) are read and attached to requests.
///
/// Returns only when every connection task has ended.
pub async fn serve<L, H, F, S, B>(
    mut listener: L,
    handshake: H,
    make: F,
    mut stop: StopSignal,
    grace: Duration,
) -> ServeReport
where
    L: Listener,
    H: Handshake<L::Io>,
    F: Fn(&H::Stream, &L::Addr) -> S + Clone + Send + Sync + 'static,
    S: tower_service::Service<
            hyper::Request<Incoming>,
            Response = hyper::Response<B>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let mut conns: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            // The stop wins a tie with a pending accept.
            biased;
            () = stop.stopped() => break,
            Some(done) = conns.join_next(), if !conns.is_empty() => {
                reap(done);
            }
            accepted = listener.accept() => match accepted {
                Ok((io, addr)) => {
                    conns.spawn(connection(
                        io,
                        addr,
                        handshake.clone(),
                        make.clone(),
                        stop.clone(),
                    ));
                }
                Err(err) if is_per_connection(&err) => {
                    debug!(error = %err, "accept: connection failed before it was accepted");
                }
                Err(err) => {
                    warn!(error = %err, "accept failed; backing off");
                    tokio::select! {
                        () = stop.stopped() => break,
                        () = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => {}
                    }
                }
            },
        }
    }

    // No new connections from here on.
    drop(listener);

    // Every connection task holds a clone of the same signal, so each one has
    // already begun its graceful shutdown. Give them the grace to finish.
    let open_at_stop = conns.len();
    let mut drained = 0usize;
    let _ = tokio::time::timeout(grace, async {
        while let Some(done) = conns.join_next().await {
            reap(done);
            drained += 1;
        }
    })
    .await;

    // Whatever is still open is a long-poll that will not end by itself.
    // Abort it AND await it: when this returns, nothing the connection's
    // service captured is still alive.
    let severed = conns.len();
    conns.shutdown().await;

    ServeReport {
        open_at_stop,
        drained,
        severed,
    }
}

/// One connection, from handshake to close.
async fn connection<Io, A, H, F, S, B>(io: Io, addr: A, handshake: H, make: F, mut stop: StopSignal)
where
    A: fmt::Debug,
    H: Handshake<Io>,
    F: Fn(&H::Stream, &A) -> S,
    S: tower_service::Service<
            hyper::Request<Incoming>,
            Response = hyper::Response<B>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    // A stop during the handshake abandons the connection outright: nothing
    // has been served on it yet.
    let stream = tokio::select! {
        shook = handshake.handshake(io) => match shook {
            Ok(stream) => stream,
            Err(err) => {
                debug!(peer = ?addr, error = %err, "handshake failed");
                return;
            }
        },
        () = stop.stopped() => return,
    };

    let service = TowerToHyperService::new(make(&stream, &addr));
    let builder = auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
    tokio::pin!(conn);

    let ended = tokio::select! {
        ended = conn.as_mut() => ended,
        () = stop.stopped() => {
            // Finish what is in flight, accept nothing new. A response that
            // never ends (a watch) keeps this pending until the serve loop's
            // grace runs out and it aborts this task.
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    };
    if let Err(err) = ended {
        debug!(peer = ?addr, error = %err, "connection ended with an error");
    }
}

/// Log a finished connection task that did not end normally.
fn reap(done: Result<(), tokio::task::JoinError>) {
    if let Err(err) = done
        && err.is_panic()
    {
        error!(error = %err, "a connection task panicked");
    }
}

/// An accept error that concerns only the connection being accepted, not the
/// listener: nothing to back off from.
fn is_per_connection(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::Interrupted
    )
}
