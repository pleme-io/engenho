//! The override tier, end to end, against the BUILT binary.
//!
//! A daemon whose declared file does not validate is repaired by an
//! override — nobody touches the file — and then changed while it runs: a
//! live leaf applied at once, a restart-class leaf deferred and then applied
//! by a restart, mistakes refused before anything is written. The overrides
//! it keeps outlive the process; the in-memory one does not.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

mod common;
use common::{Daemon, ctl, ctl_json, ctl_until};

const PATIENCE: Duration = Duration::from_secs(90);

/// The declared file: valid but for a zero scheduler tick, which would
/// hot-loop, so a boot on it is refused and held.
fn write_config(root: &Path) -> PathBuf {
    let config = serde_json::json!({
        "runtime": {
            "listen_addr": "127.0.0.1:0",
            "kubelet_listen_addr": "127.0.0.1:0",
            "etcd_listen_addr": "127.0.0.1:0",
            "data_dir": root.join("data"),
            "durable": true,
            "node_name": "control-config-node",
            "kubelet_backend": "fake",
            "kubeconfig_publish_path": root.join("published-kubeconfig"),
            "tls": { "enabled": false },
        },
        "scheduler": { "tick_interval_seconds": 0 },
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

fn state(v: &serde_json::Value) -> &str {
    v["lifecycle"]["state"].as_str().unwrap_or_default()
}

fn effect(report: &serde_json::Value) -> &str {
    report["effect"]["effect"].as_str().unwrap_or_default()
}

#[test]
fn the_override_tier_repairs_changes_and_outlives_the_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let config = write_config(root);
    let sock = socket(root);

    let mut daemon = Daemon::spawn(root, &config);
    repaired_by_an_override(root, &sock);
    refused_before_anything_is_written(&sock);
    changed_while_running(&sock);
    drift_names_what_shadows_the_declared_file(&sock);
    exits(&mut daemon, &sock);
    drop(daemon);

    let mut relaunched = Daemon::spawn(root, &config);
    kept_across_a_relaunch(&sock);
    shown_by_config_show(root, &config);
    exits(&mut relaunched, &sock);
}

/// Held on the declared file, repaired by an override: the boot is retried
/// by itself.
fn repaired_by_an_override(root: &Path, sock: &Path) {
    let failed = ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "failed"
    });
    assert_eq!(failed["lifecycle"]["retry"]["class"], "hold", "{failed}");

    let report = ctl_json(
        sock,
        &[
            "config",
            "set",
            "scheduler.tick_interval_seconds",
            "--value",
            "5",
        ],
    );
    assert_eq!(effect(&report), "restart_scheduled", "{report}");
    assert_eq!(report["generation"], 1);
    ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "running"
    });

    let leaf = ctl_json(sock, &["config", "get", "scheduler.tick_interval_seconds"]);
    assert_eq!(leaf["value"], 5);
    assert_eq!(leaf["mutability"]["class"], "restart_runtime");
    assert_eq!(leaf["provenance"]["tier"], "override", "{leaf}");
    assert_eq!(
        leaf["provenance"]["set_by"]["attested"]["uid"],
        nix::unistd::getuid().as_raw(),
        "who set it is what the kernel said"
    );

    let file = root.join("data").join("control").join("overrides.yaml");
    let mode = std::fs::metadata(&file)
        .expect("the override file")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

/// Every mistake is refused with its reason, and the set does not move.
fn refused_before_anything_is_written(sock: &Path) {
    let generation = || ctl_json(sock, &["config", "overrides"])["generation"].clone();
    let before = generation();
    for (args, reason) in [
        (
            &["config", "set", "runtime.data_dir", "--value", "/elsewhere"][..],
            "not_overridable_leaf",
        ),
        (
            &["config", "set", "scheduler.not_a_leaf", "--value", "1"][..],
            "unknown_leaf",
        ),
        (
            &[
                "config",
                "set",
                "scheduler.tick_interval_seconds",
                "--value",
                "soon",
            ][..],
            "invalid_value",
        ),
        // A value a boot would refuse is refused here, by the same validator.
        (
            &[
                "config",
                "set",
                "scheduler.tick_interval_seconds",
                "--value",
                "0",
            ][..],
            "invalid_value",
        ),
        // Clearing the override that repairs the declared file would break it.
        (&["config", "clear"][..], "config_rejected"),
        (
            &[
                "config",
                "unset",
                "scheduler.tick_interval_seconds",
                "--precondition-generation",
                "99",
            ][..],
            "precondition_failed",
        ),
    ] {
        let out = ctl(sock, args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(3), "{args:?}: {stderr}");
        assert!(
            stderr.contains(&["refused (", reason, ")"].concat()),
            "{args:?}: {stderr}"
        );
    }
    assert_eq!(generation(), before, "nothing refused was written");
}

/// Live, next-boot, deferred, then restarted.
fn changed_while_running(sock: &Path) {
    let live = ctl_json(
        sock,
        &[
            "config",
            "set",
            "runtime.kubeconfig_publish_visibility",
            "--value",
            "group",
        ],
    );
    assert_eq!(effect(&live), "applied_live", "{live}");
    assert_eq!(
        live["changed"][0]["path"],
        "runtime.kubeconfig_publish_visibility"
    );

    let boot_only = ctl_json(
        sock,
        &[
            "config",
            "set",
            "runtime.leadership_timeout_seconds",
            "--value",
            "31",
        ],
    );
    assert_eq!(effect(&boot_only), "next_boot", "{boot_only}");

    let planned = ctl_json(
        sock,
        &[
            "config",
            "set",
            "controllers.namespace",
            "--value",
            "tenants",
            "--dry-run",
            "true",
        ],
    );
    assert_eq!(effect(&planned), "planned_only");
    assert!(
        ctl_json(sock, &["config", "overrides"])["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .all(|e| e["path"] != "controllers.namespace")
    );

    let deferred = ctl_json(
        sock,
        &[
            "config",
            "set",
            "controllers.namespace",
            "--value",
            "tenants",
            "--persist",
            "false",
        ],
    );
    assert_eq!(effect(&deferred), "restart_deferred", "{deferred}");
    assert_eq!(
        deferred["effect"]["leaves"],
        serde_json::json!(["controllers.namespace"])
    );
    let running = ctl_json(sock, &["runtime", "show"]);
    assert_eq!(
        running["lifecycle"]["pending"],
        serde_json::json!({"kind": "restart_needed", "leaves": ["controllers.namespace"]}),
        "{running}"
    );

    let reloaded = ctl_json(sock, &["config", "reload", "--restart-policy", "now"]);
    assert_eq!(effect(&reloaded), "restart_scheduled", "{reloaded}");
    let running = ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "running" && v["lifecycle"]["pending"]["kind"] == "in_sync"
    });
    assert_eq!(
        running["lifecycle"]["attempt"], 3,
        "one restart past the repaired boot"
    );

    let events = ctl_json(sock, &["events", "list", "--limit", "1024"]);
    let kinds: Vec<&str> = events["events"]
        .as_array()
        .expect("events")
        .iter()
        .filter_map(|e| e["kind"]["event"].as_str())
        .collect();
    assert!(kinds.contains(&"config_applied"), "{kinds:?}");
    assert!(kinds.contains(&"kubeconfig_published"), "{kinds:?}");
}

/// The drift report names each override and whether it changes anything.
fn drift_names_what_shadows_the_declared_file(sock: &Path) {
    // Restating the declared (here: compiled-default) value.
    ctl_json(
        sock,
        &[
            "config",
            "set",
            "consistency.default_tier",
            "--value",
            "strong",
        ],
    );
    let drift = ctl_json(sock, &["config", "drift"]);
    let kind_of = |path: &str| {
        drift["leaves"]
            .as_array()
            .expect("leaves")
            .iter()
            .find(|l| l["path"] == path)
            .map(|l| l["kind"].as_str().unwrap_or_default().to_owned())
    };
    assert_eq!(
        kind_of("runtime.kubeconfig_publish_visibility").as_deref(),
        Some("shadowing")
    );
    assert_eq!(
        kind_of("consistency.default_tier").as_deref(),
        Some("redundant")
    );
    // The declared file alone does not resolve (the zero tick), so there is
    // no declared value to show against the override that repairs it.
    assert_eq!(
        kind_of("scheduler.tick_interval_seconds").as_deref(),
        Some("shadowing")
    );
}

fn exits(daemon: &mut Daemon, sock: &Path) {
    ctl_json(sock, &["runtime", "exit"]);
    let status = daemon
        .wait_for_exit(PATIENCE)
        .unwrap_or_else(|| panic!("the daemon did not exit:\n{}", daemon.output()));
    assert!(status.success(), "{status}:\n{}", daemon.output());
}

/// The persisted overrides are in force from the first boot of a new
/// process; the in-memory one is gone.
fn kept_across_a_relaunch(sock: &Path) {
    let running = ctl_until(sock, &["runtime", "show"], PATIENCE, |v| {
        state(v) == "running"
    });
    assert_eq!(
        running["lifecycle"]["attempt"], 1,
        "no failed boot this time: {running}"
    );
    let overrides = ctl_json(sock, &["config", "overrides"]);
    let paths: Vec<&str> = overrides["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .filter_map(|e| e["path"].as_str())
        .collect();
    assert_eq!(
        paths,
        [
            "consistency.default_tier",
            "runtime.kubeconfig_publish_visibility",
            "runtime.leadership_timeout_seconds",
            "scheduler.tick_interval_seconds",
        ],
        "the ephemeral controllers.namespace is gone"
    );
    assert!(overrides["generation"].as_u64().expect("generation") >= 5);
    let namespace = ctl_json(sock, &["config", "get", "controllers.namespace"]);
    assert_ne!(namespace["value"], "tenants");
}

/// `engenho config-show` folds the override tier too, and credits it.
fn shown_by_config_show(root: &Path, config: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_engenho"))
        .arg("config-show")
        .env("ENGENHO_CONFIG", config)
        .env("HOME", root.join("home"))
        .output()
        .expect("run engenho config-show");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("tick_interval_seconds: 5"), "{stdout}");
    let credited = stdout
        .lines()
        .find(|l| l.contains("scheduler.tick_interval_seconds  <-"))
        .unwrap_or_default();
    assert!(credited.contains("overrides.yaml"), "{credited}");
}
