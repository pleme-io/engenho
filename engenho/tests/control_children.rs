//! Child control, end to end, against the BUILT binary.
//!
//! A running daemon's children are restarted one at a time — with their
//! dependents where they have them — drivers are switched off and on through
//! the override tier and follow at once, and a listener moved to a new
//! address is rebuilt there. What only a runtime restart can do is refused or
//! deferred, and says so.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod common;
use common::{Daemon, ctl, ctl_json, ctl_until};

const PATIENCE: Duration = Duration::from_secs(90);

fn write_config(root: &Path) -> PathBuf {
    let config = serde_json::json!({
        "runtime": {
            "listen_addr": "127.0.0.1:0",
            "kubelet_listen_addr": "127.0.0.1:0",
            "etcd_listen_addr": "127.0.0.1:0",
            "data_dir": root.join("data"),
            "durable": true,
            "node_name": "control-children-node",
            "kubelet_backend": "fake",
            "kubeconfig_publish_path": root.join("published-kubeconfig"),
            "tls": { "enabled": false },
        },
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

fn child(sock: &Path, name: &str) -> serde_json::Value {
    ctl_json(sock, &["children", "get", name])["child"].clone()
}

fn effect(report: &serde_json::Value) -> &serde_json::Value {
    &report["apply"]["effect"]
}

/// `args` is refused for `reason`, with exit code 3.
fn refused(sock: &Path, args: &[&str], reason: &str) -> String {
    let out = ctl(sock, args);
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(out.status.code(), Some(3), "{args:?}: {stderr}");
    assert!(
        stderr.contains(&["refused (", reason, ")"].concat()),
        "{args:?}: {stderr}"
    );
    stderr
}

#[test]
fn children_are_restarted_switched_and_moved_while_the_runtime_runs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let config = write_config(root);
    let sock = socket(root);

    let mut daemon = Daemon::spawn(root, &config);
    ctl_until(&sock, &["runtime", "show"], PATIENCE, |v| {
        v["lifecycle"]["state"] == "running"
    });
    restarted_one_at_a_time(&sock);
    switched_through_the_override_tier(&sock);
    moved_to_a_new_address(&sock);

    let events = ctl_json(&sock, &["events", "list", "--limit", "1024"]);
    let respawns = events["events"]
        .as_array()
        .expect("events")
        .iter()
        .filter(|e| e["kind"]["event"] == "child_respawned")
        .count();
    assert!(respawns >= 6, "{events}");

    ctl_json(&sock, &["runtime", "exit"]);
    let status = daemon
        .wait_for_exit(PATIENCE)
        .unwrap_or_else(|| panic!("the daemon did not exit:\n{}", daemon.output()));
    assert!(status.success(), "{status}:\n{}", daemon.output());
}

/// A stateless driver comes back alone; the kubelet brings the lease and its
/// HTTP listener with it; a child whose state the router holds does not
/// come back without the runtime.
fn restarted_one_at_a_time(sock: &Path) {
    let gc = child(sock, "gc");
    assert_eq!(gc["generation"], 0, "{gc}");
    assert_eq!(gc["last_death"]["death"], "never", "{gc}");

    let report = ctl_json(sock, &["children", "restart", "gc"]);
    assert_eq!(
        report,
        serde_json::json!({"child": "gc", "generation": 1, "rebuilt": ["gc"]})
    );
    let gc = child(sock, "gc");
    assert_eq!(gc["state"]["state"], "running", "{gc}");
    assert_eq!(gc["generation"], 1);
    assert_eq!(
        gc["last_death"]["cause"], "cancelled",
        "the stop is its last death"
    );

    let report = ctl_json(sock, &["children", "restart", "kubelet"]);
    assert_eq!(
        report["rebuilt"],
        serde_json::json!(["kubelet", "node_lease", "kubelet_http"]),
        "{report}"
    );
    for name in ["kubelet", "node_lease", "kubelet_http"] {
        assert_eq!(child(sock, name)["generation"], 1, "{name}");
    }

    let stderr = refused(sock, &["children", "restart", "crd"], "respawn_refused");
    assert!(stderr.contains("runtime restart"), "{stderr}");
    assert_eq!(child(sock, "crd")["generation"], 0);
}

/// A switch that can be flipped in place is, at once, and persisted as an
/// override; one that cannot waits for a restart; a driver with no switch
/// has nothing to flip.
fn switched_through_the_override_tier(sock: &Path) {
    let off = ctl_json(sock, &["children", "disable", "gc"]);
    assert_eq!(off["enabled"], false);
    assert_eq!(
        *effect(&off),
        serde_json::json!({"effect": "respawned", "children": ["gc"]}),
        "{off}"
    );
    assert_eq!(child(sock, "gc")["state"]["state"], "disabled");
    let leaf = ctl_json(sock, &["config", "get", "controllers.enable.gc"]);
    assert_eq!(leaf["value"], false);
    assert_eq!(leaf["provenance"]["tier"], "override", "{leaf}");

    let stderr = refused(sock, &["children", "restart", "gc"], "respawn_refused");
    assert!(stderr.contains("children enable gc"), "{stderr}");

    let on = ctl_json(sock, &["children", "enable", "gc"]);
    assert_eq!(effect(&on)["effect"], "respawned", "{on}");
    let gc = child(sock, "gc");
    assert_eq!(gc["state"]["state"], "running", "{gc}");
    assert_eq!(gc["generation"], 0, "a re-enabled driver starts afresh");

    // One switch, three drivers.
    let binder = ctl_json(sock, &["children", "disable", "pv_binder"]);
    assert_eq!(
        effect(&binder)["children"],
        serde_json::json!(["pv_binder", "volume_snapshot", "pvc_protection"]),
        "{binder}"
    );
    ctl_json(sock, &["children", "enable", "pv_binder"]);

    let crd = ctl_json(sock, &["children", "disable", "crd"]);
    assert_eq!(
        *effect(&crd),
        serde_json::json!({"effect": "restart_deferred", "leaves": ["controllers.enable.crd"]}),
        "{crd}"
    );
    assert_eq!(
        child(sock, "crd")["state"]["state"],
        "running",
        "deferred, not applied"
    );
    let back = ctl_json(sock, &["children", "enable", "crd"]);
    assert_eq!(effect(&back)["effect"], "applied_live", "{back}");
    let running = ctl_json(sock, &["runtime", "show"]);
    assert_eq!(
        running["lifecycle"]["pending"]["kind"], "in_sync",
        "{running}"
    );

    refused(sock, &["children", "enable", "kubelet"], "invalid_value");
}

/// A listener's address set while it runs: it is rebuilt there, and serves.
fn moved_to_a_new_address(sock: &Path) {
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free port")
        .port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port)).to_string();
    let moved = ctl_json(
        sock,
        &[
            "config",
            "set",
            "runtime.kubelet_listen_addr",
            "--value",
            &addr,
        ],
    );
    assert_eq!(
        moved["effect"],
        serde_json::json!({"effect": "respawned", "children": ["kubelet_http"]}),
        "{moved}"
    );
    assert_eq!(child(sock, "kubelet_http")["generation"], 2);

    let deadline = Instant::now() + PATIENCE;
    while TcpStream::connect(&addr).is_err() {
        assert!(
            Instant::now() < deadline,
            "nothing listens at the new address {addr}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
