//! `engenho unit-run` through the BUILT BINARY — the contract the Nix side
//! calls.
//!
//! The Nix side will render
//! `engenho unit-run --unit ${config.systemd.units."x.service".unit}/x.service`
//! into a pod's command, so what matters is that the binary accepts exactly
//! that, that `--check` answers without touching anything, and that a unit it
//! cannot honour is refused with a typed message and a distinguishable exit
//! code rather than started wrong.

use std::path::Path;
use std::process::Command;

const UNIT: &str = "\
[Unit]
Description=A test service
[Service]
User=svc
StateDirectory=svc
LoadCredential=token:/run/secrets/token
ProtectSystem=strict
Restart=always
ExecStartPre=/bin/sh -c \"true\"
ExecStart=/bin/sh -c \"sleep 1\"
[Install]
WantedBy=multi-user.target
";

const DYNAMIC: &str = "\
[Service]
DynamicUser=true
User=dyn
ExecStart=/bin/sh
";

fn root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let etc = root.path().join("etc");
    std::fs::create_dir_all(&etc).unwrap();
    std::fs::write(etc.join("passwd"), "svc:x:4242:4242::/var/empty:/bin/sh\n").unwrap();
    std::fs::write(etc.join("group"), "svc:x:4242:\n").unwrap();
    root
}

fn unit_run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_engenho"))
        .arg("unit-run")
        .args(["--root", root.to_str().unwrap()])
        .args(args)
        .output()
        .expect("the engenho binary runs")
}

#[test]
fn check_prints_the_plan_and_changes_nothing() {
    let root = root();
    let unit = root.path().join("svc.service");
    std::fs::write(&unit, UNIT).unwrap();

    let output = unit_run(root.path(), &["--unit", unit.to_str().unwrap(), "--check"]);
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{printed}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(printed.contains("unit: svc.service"), "{printed}");
    assert!(printed.contains("runs as: svc (4242)"), "{printed}");
    assert!(printed.contains("StateDirectory:"), "{printed}");
    assert!(printed.contains("credentials:"), "{printed}");
    assert!(
        printed.contains("not enforced: ProtectSystem"),
        "the plan says what is NOT in force: {printed}"
    );
    assert!(printed.contains("ExecStart: /bin/sh"), "{printed}");
    assert!(
        !root.path().join("var/lib/svc").exists(),
        "--check must touch nothing"
    );
}

#[test]
fn a_dynamic_user_unit_is_refused_with_its_remedy() {
    let root = root();
    let unit = root.path().join("dyn.service");
    std::fs::write(&unit, DYNAMIC).unwrap();

    let output = unit_run(root.path(), &["--unit", unit.to_str().unwrap(), "--check"]);
    assert_eq!(
        output.status.code(),
        Some(78),
        "EX_CONFIG: this unit can never work as written here"
    );
    let printed = String::from_utf8_lossy(&output.stderr);
    assert!(printed.contains("DynamicUser"), "{printed}");
    assert!(printed.contains("--user"), "the remedy is named: {printed}");
}

#[test]
fn the_arguments_are_checked_before_anything_is_read() {
    let root = root();
    let output = unit_run(root.path(), &["--check"]);
    assert_eq!(
        output.status.code(),
        Some(64),
        "EX_USAGE for a missing --unit"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--unit"),
        "the error names the missing flag"
    );

    let output = unit_run(root.path(), &["--unit", "/nonexistent.service"]);
    assert_eq!(
        output.status.code(),
        Some(78),
        "a unit file that is not there"
    );
}

#[test]
fn the_verb_is_advertised_by_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_engenho"))
        .arg("--help")
        .output()
        .expect("the engenho binary runs");
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(printed.contains("unit-run --unit <PATH>"), "{printed}");
    assert!(printed.contains("DynamicUser=yes"), "{printed}");
}
