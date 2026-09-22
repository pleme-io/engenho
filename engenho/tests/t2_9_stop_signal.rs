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

use std::path::{Path, PathBuf};
use std::time::Duration;

use nix::sys::signal::{Signal, kill};

mod common;
use common::Daemon;

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

/// Boot a daemon, deliver `signal`, and assert the stop was CLEAN: exit
/// status 0, the stop cause named in the log, and the clean-stop line —
/// which is printed only after `Runtime::shutdown` returned `Ok`, i.e. every
/// child was stopped, the apiserver drained, the store flushed and then
/// terminated. What the flush leaves on disk is pinned by
/// `engenho-runtime/tests/clean_stop_restart.rs`.
fn assert_stops_cleanly_on(signal: Signal, cause: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut daemon = Daemon::spawn(tmp.path(), &write_config(tmp.path()));

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
