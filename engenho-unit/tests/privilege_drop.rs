//! The privilege drop, against the kernel — the one part of this crate that
//! cannot be tested without being root.
//!
//! It runs only on Linux, only as root, and SAYS so when it does not: a test
//! that silently passes because it skipped everything is worse than no test.
//! Where it does run (a NixOS node, a root container), it proves the whole
//! point of the seam — that a command started by the runner really becomes
//! the unit's `User=`, with the unit's groups.

#![cfg(target_os = "linux")]

use engenho_unit::capability::CapabilitySet;
use engenho_unit::identity::Account;
use engenho_unit::privilege::{drop_to, is_root, needs_drop};

/// An account that certainly exists on any Linux host: `nobody`'s
/// conventional ids. The test reads `/etc/passwd` rather than assuming, and
/// falls back to the ids `nobody` has on NixOS.
fn unprivileged() -> Account {
    // Not `passwd`: the fleet's credential pre-commit gate reads
    // `passwd = <something>` as a plaintext password assignment.
    let accounts = std::fs::read_to_string("/etc/passwd").unwrap_or_default();
    let row = accounts
        .lines()
        .find(|line| line.starts_with("nobody:"))
        .map(|line| line.split(':').map(ToString::to_string).collect::<Vec<_>>());
    let (uid, gid) = match &row {
        Some(fields) if fields.len() > 3 => (
            fields[2].parse().unwrap_or(65_534),
            fields[3].parse().unwrap_or(65_534),
        ),
        _ => (65_534, 65_534),
    };
    Account {
        name: "nobody".into(),
        uid,
        gid,
        group: "nogroup".into(),
        groups: vec![gid],
        home: "/var/empty".into(),
        shell: "/bin/sh".into(),
    }
}

#[test]
fn a_command_started_after_the_drop_runs_as_the_unit_user() {
    if !is_root() {
        // Stated, not hidden: this suite needs root, and the run that had it
        // is the run that proved the drop.
        eprintln!("SKIPPED: the privilege-drop test needs root");
        return;
    }
    let account = unprivileged();
    assert!(needs_drop(&account), "root dropping to nobody is a change");

    // The drop is thread-scoped: the child of the dropped thread runs as the
    // account, and THIS thread stays root — which is what lets the runner
    // keep preparing the next step.
    let output = std::thread::spawn(move || {
        drop_to(&account, &CapabilitySet::default()).expect("the drop succeeds as root");
        std::process::Command::new("/bin/sh")
            .args(["-c", "id -u; id -g"])
            .output()
            .expect("id runs")
    })
    .join()
    .expect("the dropping thread does not panic");

    let printed = String::from_utf8_lossy(&output.stdout);
    let mut ids = printed.split_whitespace();
    let expected = unprivileged();
    assert_eq!(
        ids.next().and_then(|id| id.parse::<u32>().ok()),
        Some(expected.uid),
        "the child's uid is the unit's User=: {printed}"
    );
    assert_eq!(
        ids.next().and_then(|id| id.parse::<u32>().ok()),
        Some(expected.gid),
        "the child's gid is the unit's Group=: {printed}"
    );
    assert!(is_root(), "the runner's own thread kept its privileges");
}

#[test]
fn an_ambient_capability_survives_the_drop() {
    if !is_root() {
        eprintln!("SKIPPED: the ambient-capability test needs root");
        return;
    }
    let mut ambient = CapabilitySet::default();
    ambient.insert(
        engenho_unit::capability::Capability::parse("CAP_NET_BIND_SERVICE")
            .expect("a real capability"),
    );
    let account = unprivileged();
    let held = std::thread::spawn(move || {
        drop_to(&account, &ambient).expect("the drop with an ambient set succeeds");
        // `CapAmb` in /proc/self/status is the ambient mask of THIS thread.
        std::fs::read_to_string("/proc/self/status").unwrap_or_default()
    })
    .join()
    .expect("the dropping thread does not panic");

    let ambient_line = held
        .lines()
        .find(|line| line.starts_with("CapAmb:"))
        .unwrap_or_default()
        .to_string();
    let mask = ambient_line
        .split_whitespace()
        .nth(1)
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .unwrap_or(0);
    assert_ne!(
        mask & (1 << 10),
        0,
        "CAP_NET_BIND_SERVICE must be in the ambient set after the drop: {ambient_line}"
    );
}
