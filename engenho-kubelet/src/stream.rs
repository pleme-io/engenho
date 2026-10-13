//! THE STREAMING RUNTIME SEAM — a live process's stdio, as values.
//!
//! [`crate::backend::ContainerRuntime::exec`] runs a command to completion and
//! returns its captured output, which is the right shape for a probe and the
//! wrong one for `kubectl exec -it`, `attach` and `cp`: those need the
//! process's stdin wired to the client WHILE it runs. A [`StreamSession`] is
//! that wiring, made runtime-neutral: every backend reduces its own transport
//! (pipes, a pty, libpod's hijacked HTTP stream) to the same five handles, and
//! the kubelet's WebSocket pump in [`crate::server`] speaks only to those.
//!
//! ★ CHANNELS, NOT `AsyncRead`/`AsyncWrite` TRAIT OBJECTS. Each output is an
//! `mpsc::Receiver<Vec<u8>>` that closes at EOF and stdin is an
//! `mpsc::Sender<Vec<u8>>` whose drop IS the close-stdin signal. That is the
//! whole contract the pump needs, it is identical for a pipe, a pty and a
//! socket, and an attach fan-out (many readers of one process) is a channel
//! per reader rather than a shared reader nobody can split.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::{broadcast, mpsc, watch};

use crate::error::KubeletError;
use crate::exec_channel::TerminalSize;

/// Which stream verb a refusal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamVerb {
    /// `kubectl exec`.
    Exec,
    /// `kubectl attach`.
    Attach,
}

impl std::fmt::Display for StreamVerb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Exec => "exec",
            Self::Attach => "attach",
        })
    }
}

/// What the client asked a session to carry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamRequest {
    /// The command, for exec. Empty for attach.
    pub argv: Vec<String>,
    /// Wire the client's input to the process.
    pub stdin: bool,
    /// Carry stdout.
    pub stdout: bool,
    /// Carry stderr. Ignored with `tty`: a terminal has one output.
    pub stderr: bool,
    /// Allocate (exec) or use (attach) a terminal.
    pub tty: bool,
}

/// How a session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamExit {
    /// The process exited with this code (`128 + n` for signal `n`, the
    /// shell's convention and the one CRI runtimes report).
    Code(i32),
    /// The streams ended and this session observes no exit code: an attach
    /// detached, or the container's output closed. Upstream reports Success.
    Ended,
}

/// The future a session's exit resolves through.
pub type ExitWait = Pin<Box<dyn Future<Output = Result<StreamExit, KubeletError>> + Send>>;

/// A live process's stdio. Every field the request did not ask for is `None`.
pub struct StreamSession {
    /// Input. Dropping it closes the process's stdin.
    pub stdin: Option<mpsc::Sender<Vec<u8>>>,
    /// Standard output (the terminal's output, with a tty). Closes at EOF.
    pub stdout: Option<mpsc::Receiver<Vec<u8>>>,
    /// Standard error. Always `None` with a tty.
    pub stderr: Option<mpsc::Receiver<Vec<u8>>>,
    /// Terminal resizes. `Some` only with a tty.
    pub resize: Option<mpsc::Sender<TerminalSize>>,
    /// Resolves when the process ends.
    pub exit: ExitWait,
}

impl std::fmt::Debug for StreamSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSession")
            .field("stdin", &self.stdin.is_some())
            .field("stdout", &self.stdout.is_some())
            .field("stderr", &self.stderr.is_some())
            .field("resize", &self.resize.is_some())
            .finish_non_exhaustive()
    }
}

impl StreamSession {
    /// A session over a command that already ran to completion.
    ///
    /// Exact, not an approximation, for a request with no stdin and no tty:
    /// such a client sends nothing and only reads, so the captured output
    /// followed by the exit is everything it would have seen live.
    #[must_use]
    pub fn completed(outcome: &crate::backend::ExecOutcome, req: &StreamRequest) -> Self {
        let buffered = |on: bool, text: &str| {
            on.then(|| {
                let (tx, rx) = mpsc::channel(1);
                if !text.is_empty() {
                    let _ = tx.try_send(text.as_bytes().to_vec());
                }
                rx
            })
        };
        let code = outcome.exit_code;
        Self {
            stdin: None,
            stdout: buffered(req.stdout, &outcome.stdout),
            stderr: buffered(req.stderr, &outcome.stderr),
            resize: None,
            exit: Box::pin(async move { Ok(StreamExit::Code(code)) }),
        }
    }
}

const CHUNK: usize = 32 * 1024;
const DEPTH: usize = 64;

/// Read `reader` to EOF in a task, one channel message per read.
///
/// A read error ends the stream like EOF does: a pty master returns `EIO`
/// once the terminal's last slave closes, which IS its end of file.
#[must_use]
pub fn pump_reader<R>(mut reader: R) -> mpsc::Receiver<Vec<u8>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let (tx, rx) = mpsc::channel(DEPTH);
    tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}

/// Write every message into `writer` in a task; shut it down when the sender
/// side is dropped, which is how a client's close-stdin reaches the process.
#[must_use]
pub fn pump_writer<W>(mut writer: W) -> mpsc::Sender<Vec<u8>>
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(DEPTH);
    tokio::spawn(async move {
        while let Some(chunk) = rx.recv().await {
            if writer.write_all(&chunk).await.is_err() || writer.flush().await.is_err() {
                return;
            }
        }
        let _ = writer.shutdown().await;
    });
    tx
}

/// Apply every resize the client sends to the terminal, in a task.
#[must_use]
pub fn pump_resizes(resizer: crate::pty::PtyResizer) -> mpsc::Sender<TerminalSize> {
    let (tx, mut rx) = mpsc::channel::<TerminalSize>(8);
    tokio::spawn(async move {
        while let Some(size) = rx.recv().await {
            if let Err(e) = resizer.resize(size) {
                tracing::debug!(error = %e, "terminal resize after the session ended");
                return;
            }
        }
    });
    tx
}

/// The exit a reaped process reports, in the form a session carries.
#[must_use]
pub fn exit_of(status: std::process::ExitStatus) -> StreamExit {
    use std::os::unix::process::ExitStatusExt as _;
    match (status.code(), status.signal()) {
        (Some(code), _) => StreamExit::Code(code),
        (None, Some(sig)) => StreamExit::Code(128 + sig),
        (None, None) => StreamExit::Code(-1),
    }
}

/// Spawn `cmd` on this host as a streaming session: pipes, or a pty when the
/// request asks for one.
///
/// The child is killed if the session's exit future is dropped — a client
/// that hangs up must not leave its shell running on the node.
///
/// # Errors
///
/// [`KubeletError::Backend`] when the pty cannot be opened or the process
/// cannot be spawned.
pub fn spawn_local(
    mut cmd: tokio::process::Command,
    req: &StreamRequest,
) -> Result<StreamSession, KubeletError> {
    let program = format!("{:?}", cmd.as_std().get_program());
    let spawn_err = |e: std::io::Error| KubeletError::Backend(format!("spawn {program}: {e}"));
    cmd.kill_on_drop(true);
    if req.tty {
        let pty = crate::pty::Pty::open()
            .map_err(|e| KubeletError::Backend(format!("open a pty for {program}: {e}")))?;
        pty.wire(&mut cmd).map_err(spawn_err)?;
        let mut child = cmd.spawn().map_err(spawn_err)?;
        drop(cmd);
        let master = pty.into_master().map_err(spawn_err)?;
        let stdout = pump_reader(tokio::fs::File::from_std(master.reader));
        let stdin = req
            .stdin
            .then(|| pump_writer(tokio::fs::File::from_std(master.writer)));
        let resize = Some(pump_resizes(master.resizer));
        let exit: ExitWait = Box::pin(async move {
            child
                .wait()
                .await
                .map(exit_of)
                .map_err(|e| KubeletError::Backend(format!("wait for exec: {e}")))
        });
        return Ok(StreamSession {
            stdin,
            stdout: req.stdout.then_some(stdout),
            stderr: None,
            resize,
            exit,
        });
    }
    let piped = |on: bool| {
        if on {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        }
    };
    cmd.stdin(piped(req.stdin));
    cmd.stdout(piped(req.stdout));
    cmd.stderr(piped(req.stderr));
    let mut child = cmd.spawn().map_err(spawn_err)?;
    let stdin = child.stdin.take().map(pump_writer);
    let stdout = child.stdout.take().map(pump_reader);
    let stderr = child.stderr.take().map(pump_reader);
    let exit: ExitWait = Box::pin(async move {
        child
            .wait()
            .await
            .map(exit_of)
            .map_err(|e| KubeletError::Backend(format!("wait for exec: {e}")))
    });
    Ok(StreamSession {
        stdin,
        stdout,
        stderr,
        resize: None,
        exit,
    })
}

/// One output of a running container, readable by any number of attachers.
///
/// The pump that owns the process's pipe writes every chunk to the
/// container's log and then to here; an attacher subscribes and sees what
/// arrives after it attached, exactly as upstream's attach does.
#[derive(Debug, Clone)]
pub struct Fanout {
    tx: broadcast::Sender<Vec<u8>>,
    done: watch::Receiver<bool>,
}

/// The pump side of a [`Fanout`].
#[derive(Debug)]
pub struct FanoutFeed {
    tx: broadcast::Sender<Vec<u8>>,
    done: watch::Sender<bool>,
}

impl FanoutFeed {
    /// Deliver one chunk to every current attacher. Nobody attached is not
    /// an error: the log still has it.
    pub fn send(&self, chunk: &[u8]) {
        let _ = self.tx.send(chunk.to_vec());
    }
}

impl Drop for FanoutFeed {
    fn drop(&mut self) {
        let _ = self.done.send(true);
    }
}

/// A new fan-out and the feed its pump writes through. Dropping the feed is
/// the stream's EOF.
#[must_use]
pub fn fanout() -> (FanoutFeed, Fanout) {
    let (tx, _) = broadcast::channel(256);
    let (done_tx, done) = watch::channel(false);
    (
        FanoutFeed {
            tx: tx.clone(),
            done: done_tx,
        },
        Fanout { tx, done },
    )
}

impl Fanout {
    /// A reader of everything that arrives from now on, closing at EOF.
    ///
    /// A reader too slow to keep up skips what it missed rather than stalling
    /// the container: an attacher is an observer, and a process blocked on a
    /// full pipe because someone's terminal is slow is an outage.
    #[must_use]
    pub fn subscribe(&self) -> mpsc::Receiver<Vec<u8>> {
        let mut rx = self.tx.subscribe();
        let mut done = self.done.clone();
        let (out, out_rx) = mpsc::channel(DEPTH);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    m = rx.recv() => match m {
                        Ok(chunk) => {
                            if out.send(chunk).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                    () = async { let _ = done.wait_for(|d| *d).await; } => {
                        while let Ok(chunk) = rx.try_recv() {
                            if out.send(chunk).await.is_err() {
                                return;
                            }
                        }
                        return;
                    }
                }
            }
        });
        out_rx
    }

    /// Resolves once the stream has reached EOF.
    pub async fn ended(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|d| *d).await;
    }

    /// Whether the stream has reached EOF.
    #[must_use]
    pub fn is_ended(&self) -> bool {
        *self.done.borrow()
    }
}

/// Copy `reader` into `log` and `feed` until EOF.
///
/// The log is written FIRST and flushed per chunk, so `kubectl logs` never
/// shows less than an attacher saw.
pub fn tee<R>(mut reader: R, mut log: tokio::fs::File, feed: FanoutFeed)
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Err(e) = log.write_all(&buf[..n]).await {
                        tracing::warn!(error = %e, "container log write failed");
                    }
                    let _ = log.flush().await;
                    feed.send(&buf[..n]);
                }
            }
        }
        drop(feed);
    });
}

/// What an attacher reaches on a running container.
#[derive(Debug, Clone)]
pub struct AttachPoint {
    /// The container's stdout (its terminal, with `tty`).
    pub stdout: Fanout,
    /// The container's stderr; `None` with `tty`.
    pub stderr: Option<Fanout>,
    /// The container's stdin, when its spec says `stdin: true`. Shared by
    /// every attacher; taken (closed) after the first detach when
    /// `stdin_once`.
    pub stdin: Arc<tokio::sync::Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
    /// `stdinOnce`: the first attacher's close-stdin closes the container's.
    pub stdin_once: bool,
    /// Resize handle, with `tty`.
    pub resize: Option<mpsc::Sender<TerminalSize>>,
}

impl AttachPoint {
    /// A session on this container's stdio.
    #[must_use]
    pub fn attach(&self, req: &StreamRequest) -> StreamSession {
        let stdin = req.stdin.then(|| {
            let shared = Arc::clone(&self.stdin);
            let once = self.stdin_once;
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(DEPTH);
            tokio::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    let target = shared.lock().await.clone();
                    let Some(target) = target else { continue };
                    if target.send(chunk).await.is_err() {
                        break;
                    }
                }
                if once {
                    shared.lock().await.take();
                }
            });
            tx
        });
        let stdout = req.stdout.then(|| self.stdout.subscribe());
        let stderr = if req.tty {
            None
        } else {
            self.stderr
                .as_ref()
                .filter(|_| req.stderr)
                .map(Fanout::subscribe)
        };
        let out = self.stdout.clone();
        let err = self.stderr.clone();
        let exit: ExitWait = Box::pin(async move {
            out.ended().await;
            if let Some(err) = err {
                err.ended().await;
            }
            Ok(StreamExit::Ended)
        });
        StreamSession {
            stdin,
            stdout,
            stderr,
            resize: if req.tty { self.resize.clone() } else { None },
            exit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn drain(mut rx: mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(c) = rx.recv().await {
            out.extend(c);
        }
        out
    }

    #[tokio::test]
    async fn a_piped_session_echoes_stdin_and_ends_when_stdin_closes() {
        let mut cmd = tokio::process::Command::new("cat");
        cmd.kill_on_drop(true);
        let req = StreamRequest {
            argv: vec!["cat".into()],
            stdin: true,
            stdout: true,
            stderr: true,
            tty: false,
        };
        let s = spawn_local(cmd, &req).expect("spawn");
        let stdin = s.stdin.expect("stdin wired");
        stdin.send(b"hello\n".to_vec()).await.expect("send");
        drop(stdin);
        assert_eq!(drain(s.stdout.expect("stdout")).await, b"hello\n");
        assert_eq!(s.exit.await.expect("exit"), StreamExit::Code(0));
    }

    #[tokio::test]
    async fn an_exit_code_and_a_signal_are_both_reported() {
        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.args(["-c", "exit 7"]);
        let s = spawn_local(cmd, &StreamRequest::default()).expect("spawn");
        assert_eq!(s.exit.await.expect("exit"), StreamExit::Code(7));

        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.args(["-c", "kill -9 $$"]);
        let s = spawn_local(cmd, &StreamRequest::default()).expect("spawn");
        assert_eq!(s.exit.await.expect("exit"), StreamExit::Code(137));
    }

    #[tokio::test]
    async fn a_fanout_reaches_every_subscriber_and_closes_at_eof() {
        let (feed, out) = fanout();
        let a = out.subscribe();
        let b = out.subscribe();
        feed.send(b"one");
        feed.send(b"two");
        drop(feed);
        assert_eq!(drain(a).await, b"onetwo");
        assert_eq!(drain(b).await, b"onetwo");
        assert!(out.is_ended());
    }

    #[tokio::test]
    async fn stdin_once_closes_the_containers_stdin_after_the_first_detach() {
        let (container_stdin, mut seen) = mpsc::channel::<Vec<u8>>(4);
        let (feed, out) = fanout();
        let point = AttachPoint {
            stdout: out,
            stderr: None,
            stdin: Arc::new(tokio::sync::Mutex::new(Some(container_stdin))),
            stdin_once: true,
            resize: None,
        };
        let s = point.attach(&StreamRequest {
            stdin: true,
            stdout: true,
            ..StreamRequest::default()
        });
        let tx = s.stdin.expect("stdin");
        tx.send(b"x".to_vec()).await.expect("send");
        drop(tx);
        assert_eq!(seen.recv().await, Some(b"x".to_vec()));
        assert_eq!(seen.recv().await, None, "the container's stdin closed");
        drop(feed);
        assert_eq!(s.exit.await.expect("exit"), StreamExit::Ended);
    }
}
