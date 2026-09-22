//! Remote control, end to end, against the BUILT binary: a client key made
//! by `engenho remote keygen`, pinned in the daemon's declared file, reaches
//! the daemon over mTLS with `engenho ctl --remote` — while the cluster's own
//! PKI is broken, because the control plane does not use it. The client gets
//! the tier the daemon declared for its pin, and loses access when the pin is
//! removed from the declared file, without a restart.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

mod common;
use common::{Daemon, ctl_json, ctl_until};

const PATIENCE: Duration = Duration::from_secs(90);

fn socket(root: &Path) -> PathBuf {
    root.join("run").join("ctl.sock")
}

fn home(root: &Path) -> PathBuf {
    root.join("home")
}

/// `engenho <args…>` as the client user: the same throwaway `$HOME` the
/// daemon's harness gives it, and its `~/.config`.
fn engenho(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_engenho"))
        .args(args)
        .env("HOME", home(root))
        .env("XDG_CONFIG_HOME", home(root).join(".config"))
        .env_remove("RUST_LOG")
        .output()
        .expect("run engenho")
}

fn remote_json(root: &Path, args: &[&str]) -> serde_json::Value {
    let out = engenho(
        root,
        &[&["ctl", "--remote", "node", "--json"], args].concat(),
    );
    assert!(
        out.status.success(),
        "engenho ctl --remote {args:?} failed ({}):\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("JSON")
}

/// The declared file: TLS on, remote control on, `clients` pinned.
fn write_config(root: &Path, clients: &serde_json::Value) -> PathBuf {
    let config = serde_json::json!({
        "runtime": {
            "listen_addr": "127.0.0.1:0",
            "kubelet_listen_addr": "127.0.0.1:0",
            "etcd_listen_addr": "127.0.0.1:0",
            "data_dir": root.join("data"),
            "durable": true,
            "node_name": "control-remote-node",
            "kubelet_backend": "fake",
            "tls": { "enabled": true },
        },
        "control": {
            "socket": { "path": socket(root) },
            "remote": {
                "enable": true,
                "listen_addr": "127.0.0.1:0",
                "authorized_clients": clients,
            },
        },
    });
    let path = root.join("engenho.yaml");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&config).expect("serialize"),
    )
    .expect("write");
    path
}

/// `remotes.yaml`: the daemon `node` at `address`, trusted by `server_pin`.
fn write_remotes(root: &Path, address: &str, server_pin: &str) {
    let path = home(root)
        .join(".config")
        .join("engenho")
        .join("remotes.yaml");
    let yaml = [
        "remotes:\n  node:\n    address: ",
        address,
        "\n    server_spki: [",
        server_pin,
        "]\n",
    ]
    .concat();
    std::fs::write(path, yaml).expect("remotes.yaml");
}

/// What the daemon says about its own side: where it listens, its pin.
struct Server {
    address: String,
    pin: String,
}

#[test]
fn a_pinned_client_controls_the_daemon_remotely_even_with_its_cluster_pki_broken() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    std::fs::create_dir_all(home(root)).expect("home");

    let pin = a_client_key(root);
    break_the_cluster_pki(root);
    let clients = serde_json::json!([{ "name": "devbox", "spki_sha256": pin, "tier": "mutate" }]);
    let config = write_config(root, &clients);
    let sock = socket(root);
    let mut daemon = Daemon::spawn(root, &config);

    let server = the_daemons_side(&sock);
    write_remotes(root, &server.address, &server.pin);
    over_mtls_at_the_pins_tier(root, &pin);
    a_server_the_client_does_not_pin_is_refused(root, &server, &pin);
    revoked_in_the_declared_file(root, &sock);

    ctl_json(&sock, &["runtime", "exit"]);
    let status = daemon
        .wait_for_exit(PATIENCE)
        .unwrap_or_else(|| panic!("the daemon did not exit:\n{}", daemon.output()));
    assert!(status.success(), "{status}:\n{}", daemon.output());
}

/// This machine's key for `node`, made once, and its pin.
fn a_client_key(root: &Path) -> String {
    let made = engenho(root, &["remote", "keygen", "node"]);
    assert!(
        made.status.success(),
        "{}",
        String::from_utf8_lossy(&made.stderr)
    );
    let pin = String::from_utf8_lossy(&made.stdout).trim().to_owned();
    let again = engenho(root, &["remote", "fingerprint", "node"]);
    assert_eq!(String::from_utf8_lossy(&again.stdout).trim(), pin);
    assert_eq!(
        engenho(root, &["remote", "keygen", "node"]).status.code(),
        Some(1),
        "a key is never replaced"
    );
    pin
}

/// The cluster's PKI, broken before the first boot.
fn break_the_cluster_pki(root: &Path) {
    let pki = root.join("data").join("pki");
    std::fs::create_dir_all(&pki).expect("pki");
    std::fs::write(pki.join("ca.crt"), "not a certificate").expect("ca.crt");
    std::fs::write(pki.join("ca.key"), "not a key").expect("ca.key");
}

/// The daemon's own side, over its socket: where it listens, its pin.
fn the_daemons_side(sock: &Path) -> Server {
    let control = ctl_until(sock, &["control", "show"], PATIENCE, |v| {
        v["remote"]["listener"] == "serving"
    });
    assert_eq!(control["authorized_clients"][0]["name"], "devbox");
    Server {
        address: control["remote"]["addr"].as_str().expect("addr").to_owned(),
        pin: control["identity"]["spki"]
            .as_str()
            .expect("spki")
            .to_owned(),
    }
}

/// Over mTLS: who the daemon says we are, what we may do, and that it
/// answers while the boot it supervises failed on the cluster's PKI.
fn over_mtls_at_the_pins_tier(root: &Path, pin: &str) {
    let hello = remote_json(root, &["hello", "show"]);
    assert_eq!(hello["transport"], "mtls", "{hello}");
    assert_eq!(hello["principal"]["attested"]["via"], "remote_pin");
    assert_eq!(hello["principal"]["attested"]["client"], "devbox");
    assert_eq!(hello["principal"]["attested"]["spki"], pin);
    assert_eq!(hello["grant"]["effective"], "mutate");

    // The boot failed on the cluster's PKI; the control plane did not.
    let deadline = Instant::now() + PATIENCE;
    let failed = loop {
        let runtime = remote_json(root, &["runtime", "show"]);
        if runtime["lifecycle"]["state"] == "failed" {
            break runtime;
        }
        assert!(Instant::now() < deadline, "never failed: {runtime}");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(
        failed["lifecycle"]["report"]["phase"], "issue_pki",
        "{failed}"
    );

    // Mutate, not destructive: the tier is the daemon's word for this pin.
    let denied = engenho(
        root,
        &[
            "ctl",
            "--remote",
            "node",
            "reinit",
            "cancel",
            "0123456789abcdef0123456789abcdef",
        ],
    );
    assert_eq!(denied.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("insufficient_authority"),
        "{}",
        String::from_utf8_lossy(&denied.stderr)
    );
}

/// A client that does not hold the daemon's pin refuses to talk to it.
fn a_server_the_client_does_not_pin_is_refused(root: &Path, server: &Server, client_pin: &str) {
    write_remotes(root, &server.address, client_pin);
    let hello = engenho(root, &["ctl", "--remote", "node", "hello", "show"]);
    assert_eq!(hello.status.code(), Some(4));
    write_remotes(root, &server.address, &server.pin);
}

/// Revoked in the declared file: refused from then on, no restart.
fn revoked_in_the_declared_file(root: &Path, sock: &Path) {
    write_config(root, &serde_json::json!([]));
    ctl_until(sock, &["control", "show"], PATIENCE, |v| {
        v["authorized_clients"] == serde_json::json!([])
    });
    let refused = engenho(root, &["ctl", "--remote", "node", "hello", "show"]);
    assert_ne!(
        refused.status.code(),
        Some(0),
        "a revoked client got through"
    );
}
