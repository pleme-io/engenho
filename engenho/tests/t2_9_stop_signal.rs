//! T2.9 — the daemon stops cleanly on SIGTERM, not only on ctrl-c.
//!
//! SIGTERM is what `systemctl stop` and `launchctl kickstart -k` send. Until
//! T2.9 the daemon subscribed to SIGINT alone (`tokio::signal::ctrl_c`), so a
//! SIGTERM hit the default disposition: the process died on the signal, with
//! no "shutdown signal received" line, no `Runtime::shutdown`, and the store
//! never terminated. Every service-manager stop was, in effect, a crash.
//!
//! These tests boot the BUILT binary (`CARGO_BIN_EXE_engenho`) exactly as a
//! unit file does — `engenho daemon`, config from `$ENGENHO_CONFIG` — on a
//! throwaway `data_dir` with every listener on an ephemeral loopback port and
//! the fake kubelet backend, then deliver the real signal and read the exit
//! status and the log the operator would read.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

/// The line `run_daemon` logs once the apiserver is bound — by then the
/// daemon is fully up and its stop signals are subscribed.
const BOOTED: &str = "engenho up";
/// The line `run_daemon` logs after `Runtime::shutdown` returned `Ok`.
const STOPPED: &str = "engenho stopped cleanly";
/// The line `run_daemon` logs when a stop signal arrives.
const SIGNALLED: &str = "shutdown signal received";

const BOOT_TIMEOUT: Duration = Duration::from_secs(90);
/// Generous next to the apiserver's 2 s drain; a stop that takes longer than
/// this would already overrun launchd's default `ExitTimeOut`.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Write the operator config for one throwaway node and return its path.
///
/// Serialized from a typed value (JSON is a subset of YAML 1.2), never
/// assembled as text. Every address is `127.0.0.1:0` so concurrent tests
/// never contend for :6443, :10250 or :2379, and the kubeconfig publish path
/// is pinned inside the tempdir so nothing is written under `$HOME`.
fn write_config(root: &Path) -> PathBuf {
    let config = serde_json::json!({
        "runtime": {
            "listen_addr": "127.0.0.1:0",
            "kubelet_listen_addr": "127.0.0.1:0",
            "etcd_listen_addr": "127.0.0.1:0",
            "data_dir": root.join("data"),
            "durable": true,
            "node_name": "t2-9-stop-node",
            "kubelet_backend": "fake",
            "kubeconfig_publish_path": root.join("published-kubeconfig"),
            "tls": { "enabled": false },
        },
    });
    let path = root.join("engenho.yaml");
    let body = serde_json::to_vec_pretty(&config).expect("serialize test config");
    std::fs::write(&path, body).expect("write test config");
    path
}

/// A running `engenho daemon` child whose stdout and stderr are drained, line
/// by line, into one channel. Dropping it kills the child, so a failed
/// assertion never leaks a daemon.
struct Daemon {
    child: Child,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
    readers: Vec<thread::JoinHandle<()>>,
}

impl Daemon {
    fn spawn(root: &Path) -> Self {
        let config = write_config(root);
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("create throwaway HOME");
        let mut child = Command::new(env!("CARGO_BIN_EXE_engenho"))
            .arg("daemon")
            .env("ENGENHO_CONFIG", &config)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join(".config"))
            // The production filter, not whatever the test runner exported,
            // and no ANSI escapes splitting the fields we read back.
            .env_remove("RUST_LOG")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the engenho binary");

        let (tx, lines) = mpsc::channel();
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let readers = vec![pump(stdout, tx.clone()), pump(stderr, tx)];
        Self {
            child,
            lines,
            seen: Vec::new(),
            readers,
        }
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(i32::try_from(self.child.id()).expect("pid fits in pid_t"))
    }

    /// Wait until a line containing `needle` has been printed. `false` on
    /// timeout, or when the child's output closed without it.
    fn wait_for_line(&mut self, needle: &str, timeout: Duration) -> bool {
        if self.seen.iter().any(|l| l.contains(needle)) {
            return true;
        }
        let deadline = Instant::now() + timeout;
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    let hit = line.contains(needle);
                    self.seen.push(line);
                    if hit {
                        return true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                    return false;
                }
            }
        }
        false
    }

    /// Wait for the child to exit, then collect every line it printed.
    /// `None` when it is still running at the deadline.
    fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                break status;
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(50));
        };
        // The pipes close with the process, so the readers finish and every
        // line they read is already in the channel.
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        self.seen.extend(self.lines.try_iter());
        Some(status)
    }

    fn output(&self) -> String {
        self.seen.join("\n")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pump(stream: impl Read + Send + 'static, tx: mpsc::Sender<String>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    })
}

/// Boot a daemon, deliver `signal`, and assert the stop was CLEAN: exit
/// status 0, the stop cause named in the log, and the clean-stop line —
/// which is printed only after `Runtime::shutdown` returned `Ok`, i.e. every
/// child was stopped, the apiserver drained, the store flushed and then
/// terminated. What the flush leaves on disk is pinned by
/// `engenho-runtime/tests/clean_stop_restart.rs`.
fn assert_stops_cleanly_on(signal: Signal, cause: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut daemon = Daemon::spawn(tmp.path());

    assert!(
        daemon.wait_for_line(BOOTED, BOOT_TIMEOUT),
        "the daemon never reported {BOOTED:?}; output so far:\n{}",
        daemon.output()
    );

    kill(daemon.pid(), signal).expect("deliver the signal");

    let status = daemon.wait_for_exit(STOP_TIMEOUT);
    let output = daemon.output();
    let Some(status) = status else {
        panic!("{cause} did not stop the daemon within {STOP_TIMEOUT:?}; output:\n{output}");
    };
    assert!(
        status.success(),
        "{cause} must stop the daemon cleanly with exit status 0, but it ended \
         with {status}; output:\n{output}"
    );
    assert!(
        output
            .lines()
            .any(|l| l.contains(SIGNALLED) && l.contains(cause)),
        "the stop log must name the cause ({cause}); output:\n{output}"
    );
    assert!(
        output.contains(STOPPED),
        "a clean stop must log {STOPPED:?} after the runtime shut down; output:\n{output}"
    );
}

/// **SIGTERM — the signal every service manager stops with — is a clean
/// stop.** Red before T2.9: the daemon died on the signal (`signal: 15
/// (SIGTERM)`), printing neither the cause nor the clean-stop line.
#[test]
fn sigterm_stops_the_daemon_cleanly() {
    assert_stops_cleanly_on(Signal::SIGTERM, "SIGTERM");
}

/// **SIGINT (ctrl-c at a terminal) stays a clean stop, and is reported as
/// SIGINT** — adding SIGTERM must not have cost the interactive path, nor
/// may the log blur which of the two signals arrived.
#[test]
fn sigint_stops_the_daemon_cleanly() {
    assert_stops_cleanly_on(Signal::SIGINT, "SIGINT");
}
