//! What the native-backend end-to-end tests share.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Realise `attr` and return the output that holds a `bin/` directory.
///
/// A multi-output derivation prints several paths, and the first is not
/// always the package: `postgresql_16` prints its `-man` output first. The
/// output is chosen by what it contains, never by position. A closure that
/// cannot be realised fails the test rather than skipping it.
#[allow(
    clippy::disallowed_methods,
    reason = "runs nix, a node tool, not a pod's process"
)]
pub fn closure(attr: &str) -> PathBuf {
    let out = std::process::Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", attr])
        .output()
        .unwrap_or_else(|e| panic!("could not run nix for {attr}: {e}"));
    assert!(
        out.status.success(),
        "realising {attr} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let path = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .find(|l| Path::new(l).join("bin").is_dir())
        .unwrap_or_else(|| panic!("no output of {attr} contains a bin/ directory; got:\n{stdout}"));
    PathBuf::from(path)
}
