//! Destructive re-initialization, end to end, against the BUILT binary.
//!
//! Every operation passes the confirmation handshake: `engenho ctl` prepares
//! the challenge, shows it, and executes under the phrase; a missing or wrong
//! phrase aborts with nothing done. The daemon refuses what the handshake
//! guards against — a store operation while running, a challenge used twice,
//! one prepared for another operation, one prepared before the runtime ran
//! again. What an operation replaces goes to the attic, never away; after a
//! wipe the next boot is a first boot, after a re-seed the CA is new.

use std::path::{Path, PathBuf};
use std::time::Duration;

mod common;
use common::{Daemon, ctl, ctl_json, ctl_until};

const PATIENCE: Duration = Duration::from_secs(90);

fn write_config(root: &Path) -> PathBuf {
    let config = serde_json::json!({
        "cluster": { "name": "reinit-test" },
        "runtime": {
            "listen_addr": "127.0.0.1:0",
            "kubelet_listen_addr": "127.0.0.1:0",
            "etcd_listen_addr": "127.0.0.1:0",
            "data_dir": root.join("data"),
            "durable": true,
            "node_name": "reinit-node",
            "kubelet_backend": "fake",
            "kubeconfig_publish_path": root.join("published-kubeconfig"),
            "tls": { "enabled": true },
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

fn state(sock: &Path) -> String {
    ctl_json(sock, &["runtime", "show"])["lifecycle"]["state"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

fn until_state(sock: &Path, want: &str) {
    ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        v["lifecycle"]["state"] == want
    });
}

fn token(root: &Path) -> String {
    std::fs::read_to_string(root.join("data/pki/admin.token")).expect("admin.token")
}

/// Run `args` with no terminal on stdin; its exit code and stderr.
fn run(sock: &Path, args: &[&str]) -> (Option<i32>, String) {
    let out = ctl(sock, args);
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn refused(sock: &Path, args: &[&str], reason: &str) {
    let (code, stderr) = run(sock, args);
    assert_eq!(code, Some(3), "{args:?}: {stderr}");
    assert!(
        stderr.contains(&["refused (", reason, ")"].concat()),
        "{args:?}: {stderr}"
    );
}

/// A challenge prepared for `request`, by hand: its id.
fn prepare(sock: &Path, request: &serde_json::Value) -> String {
    let challenge = ctl_json(
        sock,
        &["reinit", "prepare", "--request", &request.to_string()],
    );
    assert_eq!(challenge["phrase_hint"], "reinit-test", "{challenge}");
    challenge["id"].as_str().expect("an id").to_owned()
}

#[test]
fn destructive_operations_are_confirmed_moved_aside_and_take_effect() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let config = write_config(root);
    let sock = socket(root);

    let mut daemon = Daemon::spawn(root, &config);
    until_state(&sock, "running");
    confirmed_while_running(root, &sock);
    ctl_json(&sock, &["runtime", "stop"]);
    until_state(&sock, "stopped");
    the_handshake_refuses_what_it_guards_against(&sock);
    let ca_before = ctl_json(&sock, &["pki", "show"])["ca"].clone();
    reseeded_and_wiped(root, &sock);

    ctl_json(&sock, &["runtime", "start"]);
    until_state(&sock, "running");
    let store = ctl_json(&sock, &["store", "show"]);
    assert_eq!(store["facts"]["state"]["kind"], "first_boot", "{store}");
    let ca_after = ctl_json(&sock, &["pki", "show"])["ca"].clone();
    assert_eq!(ca_after["ca"], "present", "{ca_after}");
    assert_ne!(ca_after, ca_before, "the re-seed left the CA as it was");

    let events = ctl_json(&sock, &["events", "list", "--limit", "1024"]);
    let executed = events["events"]
        .as_array()
        .expect("events")
        .iter()
        .filter(|e| e["kind"]["event"] == "reinit_executed")
        .count();
    assert_eq!(executed, 4, "{events}");

    ctl_json(&sock, &["runtime", "exit"]);
    let status = daemon
        .wait_for_exit(PATIENCE)
        .unwrap_or_else(|| panic!("the daemon did not exit:\n{}", daemon.output()));
    assert!(status.success(), "{status}:\n{}", daemon.output());
}

/// The admin token rotates under a running runtime; a store operation does
/// not; a missing or wrong phrase does nothing.
fn confirmed_while_running(root: &Path, sock: &Path) {
    let before = token(root);
    let (code, stderr) = run(sock, &["reinit", "rotate-admin-token"]);
    assert_eq!(code, Some(5), "no phrase and no terminal: {stderr}");
    assert!(stderr.contains("--confirm-phrase reinit-test"), "{stderr}");
    let (code, stderr) = run(
        sock,
        &["reinit", "rotate-admin-token", "--confirm-phrase", "nope"],
    );
    assert_eq!(code, Some(5), "{stderr}");
    assert!(stderr.contains("aborted"), "{stderr}");
    assert_eq!(token(root), before, "an aborted rotation rotated");

    let report = ctl_json(
        sock,
        &[
            "reinit",
            "rotate-admin-token",
            "--confirm-phrase",
            "reinit-test",
        ],
    );
    assert_eq!(report["operation"], "rotate_admin_token", "{report}");
    let attic = PathBuf::from(report["attic_path"].as_str().expect("an attic"));
    assert!(attic.starts_with(root.join("data/control/attic")));
    assert_eq!(
        std::fs::read_to_string(attic.join("pki/admin.token")).expect("kept"),
        before
    );
    assert_ne!(token(root), before);
    assert_eq!(
        state(sock),
        "running",
        "the token rotated under the runtime"
    );

    refused(
        sock,
        &[
            "reinit",
            "wipe-store",
            "--scope",
            "store_only",
            "--confirm-phrase",
            "reinit-test",
        ],
        "runtime_running",
    );
}

/// With the runtime stopped: a challenge is used once, for its own
/// operation, in the epoch it was prepared in.
fn the_handshake_refuses_what_it_guards_against(sock: &Path) {
    let rotate = serde_json::json!({"operation": "rotate_admin_token"});
    let execute = |id: &str| {
        vec![
            "reinit".to_owned(),
            "rotate-admin-token".to_owned(),
            "--engenho-confirmation".to_owned(),
            id.to_owned(),
            "--confirm-phrase".to_owned(),
            "reinit-test".to_owned(),
        ]
    };

    // Used once.
    let id = prepare(sock, &rotate);
    let args = execute(&id);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    ctl_json(sock, &args);
    refused(sock, &args, "confirmation_required");

    // For its own operation.
    let id = prepare(sock, &rotate);
    refused(
        sock,
        &[
            "reinit",
            "reseed-pki",
            "--engenho-confirmation",
            &id,
            "--sa-key",
            "keep",
            "--confirm-phrase",
            "reinit-test",
        ],
        "confirmation_mismatch",
    );
    // With the phrase.
    let id = prepare(sock, &rotate);
    refused(
        sock,
        &[
            "reinit",
            "rotate-admin-token",
            "--engenho-confirmation",
            &id,
            "--confirm-phrase",
            "not-this-cluster",
        ],
        "confirmation_mismatch",
    );

    // In the epoch it was prepared in: the runtime ran since.
    let id = prepare(
        sock,
        &serde_json::json!({"operation": "wipe_store", "scope": "store_only"}),
    );
    ctl_json(sock, &["runtime", "start"]);
    until_state(sock, "running");
    ctl_json(sock, &["runtime", "stop"]);
    until_state(sock, "stopped");
    refused(
        sock,
        &[
            "reinit",
            "wipe-store",
            "--engenho-confirmation",
            &id,
            "--scope",
            "store_only",
            "--confirm-phrase",
            "reinit-test",
        ],
        "confirmation_mismatch",
    );
    assert_eq!(
        ctl_json(sock, &["store", "show"])["path"]
            .as_str()
            .map(|p| Path::new(p).exists()),
        Some(true),
        "a refused wipe wiped"
    );

    // Withdrawn.
    let id = prepare(sock, &rotate);
    ctl_json(sock, &["reinit", "cancel", &id]);
    let args = execute(&id);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    refused(sock, &args, "confirmation_required");
}

/// Re-seed and wipe, stopped: each is moved aside, and says what it made
/// stale.
fn reseeded_and_wiped(root: &Path, sock: &Path) {
    let reseed = ctl_json(
        sock,
        &[
            "reinit",
            "reseed-pki",
            "--sa-key",
            "keep",
            "--confirm-phrase",
            "reinit-test",
        ],
    );
    let attic = PathBuf::from(reseed["attic_path"].as_str().expect("an attic"));
    for kept in [
        "pki/ca.crt",
        "pki/ca.key",
        "pki/cluster-seed",
        "pki/admin.crt",
    ] {
        assert!(attic.join(kept).exists(), "{kept} was not kept");
    }
    assert!(
        root.join("data/pki/sa.key").exists(),
        "sa.key was kept in place"
    );
    let stale: Vec<&str> = reseed["stale_kubeconfigs"]
        .as_array()
        .expect("stale")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    assert!(
        stale.contains(&root.join("published-kubeconfig").to_string_lossy().as_ref()),
        "{reseed}"
    );

    let wipe = ctl_json(
        sock,
        &[
            "reinit",
            "wipe-store",
            "--scope",
            "store_and_node_local",
            "--confirm-phrase",
            "reinit-test",
        ],
    );
    let attic = PathBuf::from(wipe["attic_path"].as_str().expect("an attic"));
    assert!(attic.join("store").is_dir(), "{wipe}");
    assert!(!root.join("data/store").exists());
    assert!(
        root.join("data/control/boot-journal.json").exists(),
        "a wipe took the journal"
    );
}
