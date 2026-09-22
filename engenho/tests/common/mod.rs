//! A running `engenho daemon` for tests that drive the BUILT binary
//! (`CARGO_BIN_EXE_engenho`) exactly as a unit file does: `engenho daemon`,
//! config from `$ENGENHO_CONFIG`, a throwaway `$HOME`.

#![allow(dead_code, reason = "each test file uses its own subset")]

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::unistd::Pid;

/// A running `engenho daemon` child whose stdout and stderr are drained, line
/// by line, into one channel. Dropping it kills the child, so a failed
/// assertion never leaks a daemon.
pub struct Daemon {
    child: Child,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
    readers: Vec<thread::JoinHandle<()>>,
}

impl Daemon {
    /// Start `engenho daemon` on the config at `config`, with `root/home` as
    /// its `$HOME`.
    pub fn spawn(root: &Path, config: &Path) -> Self {
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("create throwaway HOME");
        let mut child = Command::new(env!("CARGO_BIN_EXE_engenho"))
            .arg("daemon")
            .env("ENGENHO_CONFIG", config)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env_remove("XDG_STATE_HOME")
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

    pub fn pid(&self) -> Pid {
        Pid::from_raw(i32::try_from(self.child.id()).expect("pid fits in pid_t"))
    }

    /// Wait until a line containing `needle` has been printed. `false` on
    /// timeout, or when the child's output closed without it.
    pub fn wait_for_line(&mut self, needle: &str, timeout: Duration) -> bool {
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
    pub fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
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

    /// Every line seen so far.
    pub fn output(&mut self) -> String {
        self.seen.extend(self.lines.try_iter());
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

/// Run `engenho ctl --socket <socket> <args…>` with the built binary.
pub fn ctl(socket: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_engenho"))
        .arg("ctl")
        .arg("--socket")
        .arg(socket)
        .args(args)
        .env_remove("RUST_LOG")
        .output()
        .expect("run engenho ctl")
}

/// `ctl … --json`'s body, parsed; panics with both streams when it failed.
pub fn ctl_json(socket: &Path, args: &[&str]) -> serde_json::Value {
    let out = ctl(socket, &[&["--json"], args].concat());
    assert!(
        out.status.success(),
        "engenho ctl {args:?} failed ({}):\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "engenho ctl {args:?} printed no JSON ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

/// Poll `ctl … --json` until `pred` holds for its body.
pub fn ctl_until(
    socket: &Path,
    args: &[&str],
    timeout: Duration,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    loop {
        let out = ctl(socket, &[&["--json"], args].concat());
        if out.status.success() {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if pred(&value) {
                    return value;
                }
                assert!(
                    Instant::now() < deadline,
                    "engenho ctl {args:?} never matched; last: {value}"
                );
            }
        } else {
            assert!(
                Instant::now() < deadline,
                "engenho ctl {args:?} never matched; last failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        thread::sleep(Duration::from_millis(200));
    }
}
