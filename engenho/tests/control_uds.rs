//! The control socket, end to end, against the BUILT binary.
//!
//! A daemon whose config does not parse still comes up, binds its control
//! socket, and says what is wrong over it; fixing the declared file brings
//! the runtime up without anyone touching the daemon; and from there the
//! socket reads its children, store and init state, stops and starts it,
//! and ends the process — all with `engenho ctl`, all without the
//! Kubernetes API.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod common;
use common::{Daemon, ctl, ctl_json, ctl_until};

const PATIENCE: Duration = Duration::from_secs(90);

/// The operator config for one throwaway node; `broken` adds a field the
/// runtime does not know, so the configuration does not resolve.
fn write_config(root: &Path, broken: bool) -> PathBuf {
    let mut runtime = serde_json::json!({
        "listen_addr": "127.0.0.1:0",
        "kubelet_listen_addr": "127.0.0.1:0",
        "etcd_listen_addr": "127.0.0.1:0",
        "data_dir": root.join("data"),
        "durable": true,
        "node_name": "control-uds-node",
        "kubelet_backend": "fake",
        "kubeconfig_publish_path": root.join("published-kubeconfig"),
        "tls": { "enabled": false },
    });
    if broken {
        runtime["not_a_field"] = serde_json::json!(true);
    }
    let config = serde_json::json!({
        "runtime": runtime,
        "control": { "socket": { "path": socket(root) } },
    });
    let path = root.join("engenho.yaml");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&config).expect("serialize"),
    )
    .expect("write");
    path
}

fn socket(root: &Path) -> PathBuf {
    root.join("run").join("ctl.sock")
}

fn wait_for(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn state(v: &serde_json::Value) -> &str {
    v["lifecycle"]["state"].as_str().unwrap_or_default()
}

#[test]
fn a_daemon_that_cannot_boot_is_reachable_repaired_and_operated_over_its_socket() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let config = write_config(root, true);
    let mut daemon = Daemon::spawn(root, &config);
    let sock = socket(root);

    socket_is_private(&sock);
    broken_and_saying_why(&sock);
    repaired_through_the_declared_file(root, &sock);
    stopped_started_and_audited(&sock);
    mistakes_are_usage_errors(&sock);
    exits_on_request(&mut daemon, &sock);
}

fn socket_is_private(sock: &Path) {
    wait_for("the control socket", PATIENCE, || sock.exists());
    let mode = std::fs::metadata(sock).expect("stat").permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "the socket is the daemon user's alone");
    let dir_mode = std::fs::metadata(sock.parent().expect("dir"))
        .expect("stat")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(dir_mode, 0o700);
}

/// Broken: the daemon is up, the runtime is held, and it says why.
fn broken_and_saying_why(sock: &Path) {
    let failed = ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "failed"
    });
    assert_eq!(failed["lifecycle"]["report"]["phase"], "resolve_config");
    assert_eq!(failed["lifecycle"]["retry"]["class"], "hold");
    assert!(
        failed["lifecycle"]["report"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not_a_field"),
        "{failed}"
    );

    let hello = ctl_json(sock, &["hello", "show"]);
    assert_eq!(hello["api_version"], "engenho.control/v1");
    assert_eq!(hello["transport"], "uds");
    assert_eq!(
        hello["principal"]["attested"]["uid"],
        nix::unistd::getuid().as_raw(),
        "the kernel's word for who called"
    );
    assert_eq!(hello["grant"]["effective"], "destructive");
    assert!(
        hello["capabilities"]
            .as_array()
            .expect("capabilities")
            .iter()
            .any(|c| c == "getInitState")
    );

    let init = ctl_json(sock, &["init", "show"]);
    assert_eq!(
        init["data_dir"]["source"]["source"], "lenient_fallback",
        "{init}"
    );
    assert_eq!(init["store"]["state"]["store"], "absent");

    // A config the daemon cannot resolve is refused, typed, not a crash.
    let refused = ctl(sock, &["config", "show"]);
    assert_eq!(refused.status.code(), Some(3), "refused exits 3");
}

/// Repaired through the declared file: the daemon notices by itself.
fn repaired_through_the_declared_file(root: &Path, sock: &Path) {
    write_config(root, false);
    let running = ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "running"
    });
    assert_eq!(running["lifecycle"]["attempt"], 2);

    let children = ctl_json(sock, &["children", "list"]);
    assert_eq!(children["runtime"], "up");
    let kubelet = children["children"]
        .as_array()
        .expect("children")
        .iter()
        .find(|c| c["child"] == "kubelet")
        .expect("the kubelet is a child");
    assert_eq!(kubelet["state"]["state"], "running");
    assert_eq!(kubelet["respawn"], "rebuild_with");

    let store = ctl_json(sock, &["store", "show"]);
    assert_eq!(store["facts"]["state"]["store"], "live");
    assert_eq!(store["facts"]["lock"], "held_by_this_daemon");

    let kubeconfigs = ctl_json(sock, &["kubeconfigs", "list"]);
    assert!(
        kubeconfigs["records"]
            .as_array()
            .expect("records")
            .iter()
            .all(|r| r["outcome"]["reason"] == "tls_disabled")
    );
}

/// Operated: stopped, started, and the audit chain records it.
fn stopped_started_and_audited(sock: &Path) {
    let stopped = ctl_json(sock, &["runtime", "stop"]);
    assert_eq!(stopped["lifecycle"]["state"], "stopped");
    assert_eq!(
        stopped["epoch"], 2,
        "the failed boot released it once, the stop again"
    );
    let store = ctl_json(sock, &["store", "show"]);
    assert_eq!(store["facts"]["state"]["store"], "present_offline");
    assert_eq!(store["facts"]["lock"], "free");

    ctl_json(sock, &["runtime", "start"]);
    ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "running"
    });

    let audit = ctl_json(sock, &["audit", "list"]);
    let operations: Vec<(String, String)> = audit["records"]
        .as_array()
        .expect("records")
        .iter()
        .map(|r| {
            (
                r["operation"].as_str().unwrap_or_default().to_owned(),
                r["phase"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    for expected in [
        ("stopRuntime", "intent"),
        ("stopRuntime", "result"),
        ("startRuntime", "intent"),
        ("startRuntime", "result"),
    ] {
        assert!(
            operations.contains(&(expected.0.to_owned(), expected.1.to_owned())),
            "{expected:?} missing from {operations:?}"
        );
    }

    let events = ctl_json(sock, &["events", "list", "--limit", "1024"]);
    assert!(
        events["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|e| e["kind"]["event"] == "boot_phase")
    );
    let logs = ctl_json(
        sock,
        &["logs", "list", "--level", "info", "--limit", "1024"],
    );
    assert!(logs["lines"].as_array().expect("lines").iter().any(|l| {
        l["message"]
            .as_str()
            .unwrap_or_default()
            .contains("engenho up")
    }));
}

/// Mistakes are usage errors, caught before anything is sent.
fn mistakes_are_usage_errors(sock: &Path) {
    assert_eq!(ctl(sock, &["nonsense", "show"]).status.code(), Some(2));
    assert_eq!(
        ctl(sock, &["logs", "list", "--after", "x"]).status.code(),
        Some(2)
    );
}

/// And the process ends on request, cleanly, removing its socket.
fn exits_on_request(daemon: &mut Daemon, sock: &Path) {
    ctl_json(sock, &["runtime", "exit"]);
    let status = daemon
        .wait_for_exit(PATIENCE)
        .unwrap_or_else(|| panic!("the daemon did not exit:\n{}", daemon.output()));
    assert!(status.success(), "{status}:\n{}", daemon.output());
    assert!(!sock.exists(), "the socket is removed on exit");
}

/// Nothing listens: `engenho ctl` says where it looked, and exits 4.
#[test]
fn no_daemon_is_unreachable_and_names_where_it_looked() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sock = tmp.path().join("nothing.sock");
    let out = ctl(&sock, &["runtime", "show"]);
    assert_eq!(out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("nothing.sock"), "{stderr}");
}
