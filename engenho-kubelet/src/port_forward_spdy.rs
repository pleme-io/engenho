//! THE SPDY PORT-FORWARD SESSION — `portforward.k8s.io` over SPDY/3.1, the
//! protocol kubectl and k9s actually send.
//!
//! A client forwards one local connection by opening TWO streams that share a
//! `requestID` header: an `error` stream it only reads, then a `data` stream
//! carrying the TCP bytes both ways. Each names the pod port in a `port`
//! header. The kubelet replies to every SYN_STREAM at once (the client blocks
//! on the reply before opening the next), and when both halves of a pair have
//! arrived it dials the port and splices it to the data stream.
//!
//! ★ THE CLOSING HANDSHAKE IS THE CONTRACT. The client reads the error stream
//! to EOF after its copy ends, so every pair ends with a FIN on its error
//! stream — empty on success, the reason first on failure. A port that cannot
//! be dialled writes `error forwarding port …` there and resets the data
//! stream; the session stays up for every other pair, exactly as upstream's
//! `httpStreamHandler` behaves. The pod's EOF is a FIN on the data stream; a
//! client FIN half-closes the pod connection; a client RST_STREAM on the data
//! stream drops it.
//!
//! The session is transport-neutral: bytes in, bytes out. The kubelet feeds it
//! from a plain `Upgrade: SPDY/3.1` connection or from the binary messages of
//! a `SPDY/3.1+portforward.k8s.io` WebSocket ([`crate::server`]), and the
//! session cannot tell which.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::spdy::{Frame, FrameReader, FrameWriter, Headers, RstStatus, SpdyError, StreamId};

/// The stream protocol a plain SPDY upgrade negotiates in
/// `X-Stream-Protocol-Version`.
pub const STREAM_PROTOCOL: &str = engenho_types::stream_protocol::PORT_FORWARD;

/// The WebSocket subprotocol that tunnels a SPDY port-forward session.
pub const TUNNEL_SUBPROTOCOL: &str = engenho_types::stream_protocol::SPDY_PORT_FORWARD_TUNNEL;

/// How long half a pair waits for the other half: upstream's default
/// `streamCreationTimeout`.
pub const STREAM_CREATION_TIMEOUT: Duration = Duration::from_secs(30);

const CHUNK: usize = 32 * 1024;
const DEPTH: usize = 64;

/// Which pod a session forwards into.
#[derive(Debug, Clone)]
pub struct ForwardTarget {
    /// The pod's namespace, for messages.
    pub namespace: String,
    /// The pod's name, for messages.
    pub pod: String,
    /// The pod's address, or why it has none.
    pub addr: Result<IpAddr, String>,
}

/// Which half of a pair a stream is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Half {
    Error,
    Data,
}

/// Why a SYN_STREAM is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamRefusal {
    /// No `port` header.
    #[error("\"port\" header is required")]
    NoPort,
    /// A `port` that is not 1-65535.
    #[error("unable to parse {0:?} as a port")]
    BadPort(String),
    /// No `streamType` header.
    #[error("\"streamType\" header is required")]
    NoStreamType,
    /// A `streamType` other than `error` or `data`.
    #[error("invalid stream type {0:?}")]
    BadStreamType(String),
}

/// What a valid SYN_STREAM asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamAsk {
    half: Half,
    port: u16,
    request: String,
}

/// Validate a SYN_STREAM's headers and name the pair it belongs to.
///
/// A client too old to send `requestID` gets upstream's best-effort pairing:
/// the error stream's own id, and the data stream's id minus two.
fn classify(stream: StreamId, headers: &Headers) -> Result<StreamAsk, StreamRefusal> {
    let raw_port = headers.get("port").ok_or(StreamRefusal::NoPort)?;
    let port = raw_port
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| StreamRefusal::BadPort(raw_port.to_owned()))?;
    let half = match headers.get("streamType") {
        None | Some("") => return Err(StreamRefusal::NoStreamType),
        Some("error") => Half::Error,
        Some("data") => Half::Data,
        Some(other) => return Err(StreamRefusal::BadStreamType(other.to_owned())),
    };
    let request = match headers.get("requestID").filter(|r| !r.is_empty()) {
        Some(r) => r.to_owned(),
        None => match half {
            Half::Error => stream.to_string(),
            Half::Data => stream.saturating_sub(2).to_string(),
        },
    };
    Ok(StreamAsk {
        half,
        port,
        request,
    })
}

/// A pair waiting for its other half, or running.
#[derive(Debug, Default)]
struct Pair {
    port: u16,
    error: Option<StreamId>,
    data: Option<StreamId>,
    running: bool,
    /// Fired when the client resets the data stream.
    reset: Option<oneshot::Sender<()>>,
}

/// One open client stream.
#[derive(Debug)]
struct Open {
    request: String,
    half: Half,
    /// The pod-bound half of a running data stream; dropped on the client's
    /// FIN, which half-closes the pod connection.
    inbound: Option<mpsc::Sender<Vec<u8>>>,
}

enum Tick {
    Bytes(Option<Vec<u8>>),
    PairEnded(String),
    TimedOut(String),
}

/// Run one port-forward session until the client closes it.
///
/// `inbound` carries the client's bytes in whatever pieces the transport
/// delivers them; every frame this side sends is written to `outbound`, in
/// order. Dropping `outbound` (when this returns) closes the transport.
pub async fn serve(
    mut inbound: mpsc::Receiver<Vec<u8>>,
    outbound: mpsc::Sender<Vec<u8>>,
    target: ForwardTarget,
) {
    let (frames, mut to_write) = mpsc::channel::<Frame>(DEPTH);
    let writer = tokio::spawn(async move {
        let mut w = FrameWriter::default();
        while let Some(f) = to_write.recv().await {
            if outbound.send(w.encode(&f)).await.is_err() {
                return;
            }
        }
    });
    let mut session = Session {
        target,
        frames: frames.clone(),
        reader: FrameReader::default(),
        pairs: HashMap::new(),
        open: HashMap::new(),
        tasks: JoinSet::new(),
        timeouts: JoinSet::new(),
        last_seen: 0,
        going_away: false,
    };
    drop(frames);
    loop {
        let tick = tokio::select! {
            b = inbound.recv() => Tick::Bytes(b),
            Some(Ok(r)) = session.tasks.join_next() => Tick::PairEnded(r),
            Some(Ok(r)) = session.timeouts.join_next() => Tick::TimedOut(r),
        };
        let carry_on = match tick {
            Tick::Bytes(None) => false,
            Tick::Bytes(Some(bytes)) => session.on_bytes(&bytes).await,
            Tick::PairEnded(request) => {
                session.forget(&request);
                !(session.going_away && session.pairs.is_empty())
            }
            Tick::TimedOut(request) => {
                session.time_out(&request).await;
                true
            }
        };
        if !carry_on {
            break;
        }
    }
    session.tasks.abort_all();
    session.timeouts.abort_all();
    drop(session);
    let _ = writer.await;
}

struct Session {
    target: ForwardTarget,
    frames: mpsc::Sender<Frame>,
    reader: FrameReader,
    pairs: HashMap<String, Pair>,
    open: HashMap<StreamId, Open>,
    tasks: JoinSet<String>,
    timeouts: JoinSet<String>,
    last_seen: StreamId,
    going_away: bool,
}

impl Session {
    async fn send(&self, f: Frame) {
        let _ = self.frames.send(f).await;
    }

    /// Decode and act on everything `bytes` completes. `false` ends the
    /// session.
    async fn on_bytes(&mut self, bytes: &[u8]) -> bool {
        self.reader.push(bytes);
        loop {
            match self.reader.next_frame() {
                Ok(None) => return true,
                Ok(Some(frame)) => {
                    if !self.on_frame(frame).await {
                        return false;
                    }
                }
                Err(e) => {
                    self.protocol_failure(&e).await;
                    return false;
                }
            }
        }
    }

    async fn protocol_failure(&self, e: &SpdyError) {
        tracing::debug!(error = %e, "port-forward SPDY session ended on a bad frame");
        self.send(Frame::GoAway {
            last_good: self.last_seen,
            status: 1,
        })
        .await;
    }

    async fn on_frame(&mut self, frame: Frame) -> bool {
        match frame {
            Frame::SynStream {
                stream, headers, ..
            } => self.on_syn_stream(stream, &headers).await,
            Frame::Data { stream, fin, data } => self.on_data(stream, fin, data).await,
            Frame::RstStream { stream, .. } => self.on_reset(stream),
            Frame::Ping { id } => self.send(Frame::Ping { id }).await,
            Frame::GoAway { .. } => {
                self.going_away = true;
                return !self.pairs.is_empty();
            }
            Frame::SynReply { .. }
            | Frame::Headers { .. }
            | Frame::Settings { .. }
            | Frame::WindowUpdate { .. }
            | Frame::Unknown { .. } => {}
        }
        true
    }

    async fn on_syn_stream(&mut self, stream: StreamId, headers: &Headers) {
        self.last_seen = self.last_seen.max(stream);
        let ask = match classify(stream, headers) {
            Ok(ask) if !self.going_away => ask,
            Ok(_) => {
                self.refuse(stream).await;
                return;
            }
            Err(why) => {
                tracing::debug!(stream, error = %why, "port-forward stream refused");
                self.refuse(stream).await;
                return;
            }
        };
        self.send(Frame::SynReply {
            stream,
            fin: false,
            headers: Headers::new(),
        })
        .await;
        let fresh = !self.pairs.contains_key(&ask.request);
        let pair = self.pairs.entry(ask.request.clone()).or_default();
        if fresh {
            pair.port = ask.port;
            let request = ask.request.clone();
            self.timeouts.spawn(async move {
                tokio::time::sleep(STREAM_CREATION_TIMEOUT).await;
                request
            });
        }
        let (taken, error) = match ask.half {
            Half::Error => (pair.error.is_some(), pair.error),
            Half::Data => (pair.data.is_some(), pair.error),
        };
        if taken {
            let half = if ask.half == Half::Error {
                "error"
            } else {
                "data"
            };
            let msg = format!(
                "error processing stream for request {}: {half} stream already assigned",
                ask.request
            );
            if let Some(error) = error {
                self.send(Frame::Data {
                    stream: error,
                    fin: false,
                    data: msg.into_bytes(),
                })
                .await;
            }
            self.send(Frame::RstStream {
                stream,
                status: RstStatus::ProtocolError,
            })
            .await;
            return;
        }
        match ask.half {
            Half::Error => pair.error = Some(stream),
            Half::Data => pair.data = Some(stream),
        }
        self.open.insert(
            stream,
            Open {
                request: ask.request.clone(),
                half: ask.half,
                inbound: None,
            },
        );
        self.start_if_complete(&ask.request);
    }

    async fn refuse(&self, stream: StreamId) {
        self.send(Frame::RstStream {
            stream,
            status: RstStatus::RefusedStream,
        })
        .await;
    }

    fn start_if_complete(&mut self, request: &str) {
        let Some(pair) = self.pairs.get_mut(request) else {
            return;
        };
        let (Some(error), Some(data), false) = (pair.error, pair.data, pair.running) else {
            return;
        };
        pair.running = true;
        let (to_pod, from_client) = mpsc::channel(DEPTH);
        let (reset_tx, reset_rx) = oneshot::channel();
        pair.reset = Some(reset_tx);
        if let Some(open) = self.open.get_mut(&data) {
            open.inbound = Some(to_pod);
        }
        let job = PairJob {
            request: request.to_owned(),
            port: pair.port,
            error,
            data,
            target: self.target.clone(),
            frames: self.frames.clone(),
        };
        self.tasks.spawn(job.run(from_client, reset_rx));
    }

    async fn on_data(&mut self, stream: StreamId, fin: bool, data: Vec<u8>) {
        let Some(open) = self.open.get_mut(&stream) else {
            return;
        };
        if open.half != Half::Data {
            return;
        }
        if !data.is_empty()
            && let Some(tx) = &open.inbound
            && tx.send(data).await.is_err()
        {
            open.inbound = None;
        }
        if fin {
            open.inbound = None;
        }
    }

    fn on_reset(&mut self, stream: StreamId) {
        let Some(open) = self.open.remove(&stream) else {
            return;
        };
        if open.half == Half::Data
            && let Some(pair) = self.pairs.get_mut(&open.request)
        {
            if let Some(reset) = pair.reset.take() {
                let _ = reset.send(());
            }
            if !pair.running {
                pair.data = None;
            }
        }
    }

    async fn time_out(&mut self, request: &str) {
        let Some(pair) = self.pairs.get(request) else {
            return;
        };
        if pair.running {
            return;
        }
        let (error, data) = (pair.error, pair.data);
        if let Some(error) = error {
            self.send(Frame::Data {
                stream: error,
                fin: true,
                data: format!("timed out waiting for streams of request {request}").into_bytes(),
            })
            .await;
        }
        if let Some(data) = data {
            self.send(Frame::RstStream {
                stream: data,
                status: RstStatus::Cancel,
            })
            .await;
        }
        self.forget(request);
    }

    fn forget(&mut self, request: &str) {
        if let Some(pair) = self.pairs.remove(request) {
            for id in [pair.error, pair.data].into_iter().flatten() {
                self.open.remove(&id);
            }
        }
    }
}

/// One running pair: the pod connection spliced to the data stream.
struct PairJob {
    request: String,
    port: u16,
    error: StreamId,
    data: StreamId,
    target: ForwardTarget,
    frames: mpsc::Sender<Frame>,
}

impl PairJob {
    async fn run(
        self,
        mut from_client: mpsc::Receiver<Vec<u8>>,
        reset: oneshot::Receiver<()>,
    ) -> String {
        let dialled = match &self.target.addr {
            Ok(ip) => tokio::net::TcpStream::connect((*ip, self.port))
                .await
                .map_err(|e| e.to_string()),
            Err(reason) => Err(reason.clone()),
        };
        let tcp = match dialled {
            Ok(tcp) => tcp,
            Err(reason) => {
                let msg = format!(
                    "error forwarding port {} to pod {}/{}: {reason}",
                    self.port, self.target.namespace, self.target.pod
                );
                let _ = self
                    .frames
                    .send(Frame::Data {
                        stream: self.error,
                        fin: true,
                        data: msg.into_bytes(),
                    })
                    .await;
                let _ = self
                    .frames
                    .send(Frame::RstStream {
                        stream: self.data,
                        status: RstStatus::Cancel,
                    })
                    .await;
                return self.request;
            }
        };
        let (mut pod_read, mut pod_write) = tcp.into_split();
        let to_pod = async move {
            while let Some(chunk) = from_client.recv().await {
                if pod_write.write_all(&chunk).await.is_err() {
                    return;
                }
            }
            let _ = pod_write.shutdown().await;
            std::future::pending::<()>().await;
        };
        let frames = self.frames.clone();
        let data = self.data;
        let from_pod = async move {
            let mut buf = vec![0u8; CHUNK];
            loop {
                match pod_read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let frame = Frame::Data {
                            stream: data,
                            fin: false,
                            data: buf[..n].to_vec(),
                        };
                        if frames.send(frame).await.is_err() {
                            return false;
                        }
                    }
                }
            }
            true
        };
        let finish_data = tokio::select! {
            ended = from_pod => ended,
            () = to_pod => false,
            _ = reset => false,
        };
        if finish_data {
            let _ = self
                .frames
                .send(Frame::Data {
                    stream: self.data,
                    fin: true,
                    data: Vec::new(),
                })
                .await;
        }
        let _ = self
            .frames
            .send(Frame::Data {
                stream: self.error,
                fin: true,
                data: Vec::new(),
            })
            .await;
        self.request
    }
}

/// Serve a session over a raw byte transport (a hijacked `Upgrade: SPDY/3.1`
/// connection).
pub async fn serve_io<T>(io: T, target: ForwardTarget)
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
{
    let (read, write) = tokio::io::split(io);
    let inbound = crate::stream::pump_reader(read);
    let outbound = crate::stream::pump_writer(write);
    serve(inbound, outbound, target).await;
}

pub mod client {
    //! The client half, as client-go's `PortForwarder` drives it: an error
    //! stream it closes at once and only reads, then a data stream, both
    //! carrying the request's `requestID`. Used by every test that proves the
    //! server against a real socket, and by nothing that ships a session.

    use std::collections::VecDeque;

    use futures_util::{Sink, SinkExt as _, Stream, StreamExt as _};
    use tokio::sync::mpsc;

    use crate::spdy::{Frame, FrameReader, FrameWriter, Headers, SpdyError, StreamId};

    /// The two streams of one forwarded connection.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Forward {
        /// The stream the server reports failure on.
        pub error: StreamId,
        /// The TCP bytes.
        pub data: StreamId,
    }

    /// A client session over a byte transport.
    #[derive(Debug)]
    pub struct Client {
        rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<Vec<u8>>,
        reader: FrameReader,
        writer: FrameWriter,
        next: StreamId,
        stashed: VecDeque<Frame>,
    }

    fn closed() -> SpdyError {
        SpdyError::Protocol("the transport closed".into())
    }

    impl Client {
        /// A session reading `rx` and writing `tx`.
        #[must_use]
        pub fn new(rx: mpsc::Receiver<Vec<u8>>, tx: mpsc::Sender<Vec<u8>>) -> Self {
            Self {
                rx,
                tx,
                reader: FrameReader::default(),
                writer: FrameWriter::default(),
                next: 1,
                stashed: VecDeque::new(),
            }
        }

        /// A session over a raw byte stream (a hijacked SPDY upgrade).
        pub fn over_io<T>(io: T) -> Self
        where
            T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
        {
            let (read, write) = tokio::io::split(io);
            Self::new(
                crate::stream::pump_reader(read),
                crate::stream::pump_writer(write),
            )
        }

        /// Upgrade `io` with `POST {path}` offering `stream_protocol` in
        /// `X-Stream-Protocol-Version`, as client-go's SPDY round tripper
        /// does, and start a session on the 101.
        ///
        /// # Errors
        ///
        /// [`SpdyError::Protocol`] carrying the response head when the answer
        /// is not a 101, or the transport fails first.
        pub async fn upgrade<T>(
            mut io: T,
            path: &str,
            stream_protocol: &str,
        ) -> Result<(Self, String), SpdyError>
        where
            T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
        {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let io_err = |e: std::io::Error| SpdyError::Protocol(format!("upgrade: {e}"));
            let request = format!(
                "POST {path} HTTP/1.1\r\nHost: engenho\r\nConnection: Upgrade\r\n\
                 Upgrade: {}\r\n{}: {stream_protocol}\r\nContent-Length: 0\r\n\r\n",
                crate::spdy::UPGRADE_TOKEN,
                engenho_types::stream_protocol::STREAM_PROTOCOL_HEADER,
            );
            io.write_all(request.as_bytes()).await.map_err(io_err)?;
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if io.read(&mut byte).await.map_err(io_err)? == 0 {
                    return Err(SpdyError::Protocol(format!(
                        "closed mid-head: {}",
                        String::from_utf8_lossy(&head)
                    )));
                }
                head.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&head).into_owned();
            if head.starts_with("HTTP/1.1 101") {
                Ok((Self::over_io(io), head))
            } else {
                Err(SpdyError::Protocol(head))
            }
        }

        /// A session over message transport halves (a WebSocket's binary
        /// messages, mapped to bytes by the caller). The sink is closed when
        /// the session is dropped.
        pub fn over_messages<R, W, E>(mut read: R, mut write: W) -> Self
        where
            R: Stream<Item = Vec<u8>> + Send + Unpin + 'static,
            W: Sink<Vec<u8>, Error = E> + Send + Unpin + 'static,
        {
            let (in_tx, in_rx) = mpsc::channel(64);
            let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(64);
            tokio::spawn(async move {
                while let Some(b) = read.next().await {
                    if in_tx.send(b).await.is_err() {
                        return;
                    }
                }
            });
            tokio::spawn(async move {
                while let Some(b) = out_rx.recv().await {
                    if write.send(b).await.is_err() {
                        return;
                    }
                }
                let _ = write.close().await;
            });
            Self::new(in_rx, out_tx)
        }

        /// Send one frame.
        ///
        /// # Errors
        ///
        /// [`SpdyError::Protocol`] once the transport is closed.
        pub async fn send(&mut self, f: &Frame) -> Result<(), SpdyError> {
            self.tx
                .send(self.writer.encode(f))
                .await
                .map_err(|_| closed())
        }

        /// The next frame, or `None` when the server closed the transport.
        ///
        /// # Errors
        ///
        /// [`SpdyError`] for a frame that does not decode.
        pub async fn recv(&mut self) -> Result<Option<Frame>, SpdyError> {
            if let Some(f) = self.stashed.pop_front() {
                return Ok(Some(f));
            }
            loop {
                if let Some(f) = self.reader.next_frame()? {
                    return Ok(Some(f));
                }
                match self.rx.recv().await {
                    Some(bytes) => self.reader.push(&bytes),
                    None => return Ok(None),
                }
            }
        }

        /// Open a stream of `kind` (`error` or `data`) to `port` and wait for
        /// the server's reply, keeping every other frame for [`Self::recv`].
        ///
        /// # Errors
        ///
        /// [`SpdyError::Protocol`] when the server resets the stream or the
        /// transport closes first.
        pub async fn open(
            &mut self,
            kind: &str,
            port: &str,
            request: &str,
        ) -> Result<StreamId, SpdyError> {
            let stream = self.next;
            self.next += 2;
            let headers = Headers::new()
                .with("streamType", kind)
                .with("port", port)
                .with("requestID", request);
            self.send(&Frame::SynStream {
                stream,
                associated: 0,
                priority: 0,
                fin: false,
                unidirectional: false,
                headers,
            })
            .await?;
            let mut seen = VecDeque::new();
            let outcome = loop {
                let f = if let Some(f) = self.reader.next_frame()? {
                    f
                } else {
                    match self.rx.recv().await {
                        Some(bytes) => {
                            self.reader.push(&bytes);
                            continue;
                        }
                        None => break Err(closed()),
                    }
                };
                match f {
                    Frame::SynReply { stream: s, .. } if s == stream => break Ok(stream),
                    Frame::RstStream { stream: s, status } if s == stream => {
                        break Err(SpdyError::Protocol(format!(
                            "stream {stream} reset: {status:?}"
                        )));
                    }
                    other => seen.push_back(other),
                }
            };
            self.stashed.extend(seen);
            outcome
        }

        /// Open the error and data streams of one forwarded connection, as
        /// client-go does: the error stream first, closed for writing at once.
        ///
        /// # Errors
        ///
        /// As [`Self::open`].
        pub async fn forward(&mut self, port: u16, request: &str) -> Result<Forward, SpdyError> {
            let port = port.to_string();
            let error = self.open("error", &port, request).await?;
            self.send(&Frame::Data {
                stream: error,
                fin: true,
                data: Vec::new(),
            })
            .await?;
            let data = self.open("data", &port, request).await?;
            Ok(Forward { error, data })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(ty: &str, port: &str, req: Option<&str>) -> Headers {
        let h = Headers::new().with("streamType", ty).with("port", port);
        match req {
            Some(r) => h.with("requestID", r),
            None => h,
        }
    }

    #[test]
    fn a_stream_names_its_half_port_and_pair() {
        assert_eq!(
            classify(5, &h("data", "8080", Some("7"))),
            Ok(StreamAsk {
                half: Half::Data,
                port: 8080,
                request: "7".into()
            })
        );
    }

    #[test]
    fn an_old_client_without_request_ids_pairs_by_stream_id() {
        assert_eq!(
            classify(1, &h("error", "80", None)).map(|a| a.request),
            Ok("1".into())
        );
        assert_eq!(
            classify(3, &h("data", "80", None)).map(|a| a.request),
            Ok("1".into())
        );
    }

    #[test]
    fn malformed_streams_are_refused_with_upstreams_reasons() {
        assert_eq!(
            classify(1, &Headers::new().with("streamType", "data")),
            Err(StreamRefusal::NoPort)
        );
        assert_eq!(
            classify(1, &h("data", "0", None)),
            Err(StreamRefusal::BadPort("0".into()))
        );
        assert_eq!(
            classify(1, &h("data", "http", None)),
            Err(StreamRefusal::BadPort("http".into()))
        );
        assert_eq!(
            classify(1, &Headers::new().with("port", "80")),
            Err(StreamRefusal::NoStreamType)
        );
        assert_eq!(
            classify(1, &h("stdin", "80", None)),
            Err(StreamRefusal::BadStreamType("stdin".into()))
        );
    }
}
