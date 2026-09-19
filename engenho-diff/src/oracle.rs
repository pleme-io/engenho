//! Where the reference oracle lives, and what to say when it does not answer.
//!
//! ONE typed resolution of the oracle's kubeconfig ([`OracleKubeconfig`]) and
//! ONE typed diagnostic for an oracle that stayed silent
//! ([`OracleUnreachable`]).
//!
//! Both used to be inlined per test binary. `m0_core_crud_parity.rs` carried
//! its own copy of each, and its copy of the hint told the operator to check
//! a tunnel — `kubectl --tls-server-name 127.0.0.1 …` — to a hand-built k3s
//! VM that has not existed since 2026-08-08. So the one sentence an operator
//! acts on named a knob that is gone instead of
//! [`ORACLE_KUBECONFIG_ENV`], the knob that works.
//!
//! That is how a duplicated operator-facing hint fails: silently. Both
//! binaries went on passing against a live oracle and failing loudly against
//! a dead one, exactly as designed — only the human reading the panic was
//! sent to the wrong place. A hint has to hold wherever the harness panics,
//! so it is represented once, at that scope, and every binary reads it from
//! here.

use std::fmt;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// The environment variable that selects the oracle's kubeconfig. The ONE
/// knob; every diagnostic in this module names it.
pub const ORACLE_KUBECONFIG_ENV: &str = "ENGENHO_ORACLE_KUBECONFIG";

/// Where the oracle's kubeconfig is looked for when [`ORACLE_KUBECONFIG_ENV`]
/// says nothing: `$HOME` joined with this.
///
/// It is the historical path of a hand-built k3s VM's tunnel and it is
/// expected to be absent. Kept as the fallback because
/// `.config/nextest.toml` documents it as such; any conformant apiserver
/// works, and a disposable `kind` cluster pinned to the target Kubernetes
/// version is the reproducible choice.
pub const ORACLE_KUBECONFIG_FALLBACK: &str = ".kube/engenho-local-tunnel.yaml";

/// Neither [`ORACLE_KUBECONFIG_ENV`] nor `HOME` named anything, so there is
/// no path to even attempt.
///
/// A typed absence rather than a guessed path: resolving to `""` or to a
/// bare relative name would turn "nothing was configured" into "the oracle
/// at <nonsense> did not answer", which reads as a dead cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "no oracle kubeconfig to try: {ORACLE_KUBECONFIG_ENV} is unset or empty and HOME is unset, \
     so the $HOME/{ORACLE_KUBECONFIG_FALLBACK} fallback cannot be formed. Set \
     {ORACLE_KUBECONFIG_ENV} to a live cluster's kubeconfig."
)]
pub struct OracleUnresolved;

/// The reference oracle's kubeconfig path, resolved once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleKubeconfig(PathBuf);

impl OracleKubeconfig {
    /// Resolve from an already-read environment value and home directory.
    ///
    /// Pure on purpose: the resolution rule is then testable without
    /// mutating the process environment (`set_var` is `unsafe` under edition
    /// 2024, and a test that mutates it races every other test in the same
    /// binary).
    ///
    /// An EMPTY environment value counts as unset — a shell that exports
    /// `ENGENHO_ORACLE_KUBECONFIG=` has configured nothing, and `Kubeconfig::load("")`
    /// would report a missing file instead.
    ///
    /// # Errors
    ///
    /// [`OracleUnresolved`] when neither input names a path.
    pub fn resolve(env_value: Option<&str>, home: Option<&Path>) -> Result<Self, OracleUnresolved> {
        if let Some(configured) = env_value.filter(|v| !v.is_empty()) {
            return Ok(Self(PathBuf::from(configured)));
        }
        home.map(|h| Self(h.join(ORACLE_KUBECONFIG_FALLBACK)))
            .ok_or(OracleUnresolved)
    }

    /// Resolve from this process's environment.
    ///
    /// # Errors
    ///
    /// [`OracleUnresolved`] when neither [`ORACLE_KUBECONFIG_ENV`] nor `HOME`
    /// is set.
    pub fn from_env() -> Result<Self, OracleUnresolved> {
        let configured = std::env::var(ORACLE_KUBECONFIG_ENV).ok();
        let home = std::env::var("HOME").ok();
        Self::resolve(configured.as_deref(), home.as_deref().map(Path::new))
    }

    /// The resolved path, for the loader that opens it.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// The fail-loud diagnostic for an oracle at this path that did not
    /// answer `GET /api/v1`.
    #[must_use]
    pub fn unreachable(&self) -> OracleUnreachable {
        OracleUnreachable {
            kubeconfig: self.clone(),
        }
    }
}

impl fmt::Display for OracleKubeconfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

/// The oracle did not answer — the ONE operator-facing hint for that state.
///
/// Carries the path it tried, so the message never says "the oracle" without
/// saying which one.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "Verdict::ReferenceUnreachable — the oracle at {kubeconfig} did not answer GET /api/v1. A \
     live differential run cannot proceed. Point {ORACLE_KUBECONFIG_ENV} at a live cluster's \
     kubeconfig (a kind cluster pinned to the target Kubernetes version works) and run \
     `cargo nextest run --profile oracle`."
)]
pub struct OracleUnreachable {
    kubeconfig: OracleKubeconfig,
}

impl OracleUnreachable {
    /// The kubeconfig that was tried.
    #[must_use]
    pub fn kubeconfig(&self) -> &OracleKubeconfig {
        &self.kubeconfig
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A second oracle loader is spelled one of two ways: opening the
    /// kubeconfig, or restating the unreachable hint.
    const SECOND_LOADER: &[&str] = &["from_kubeconfig(", "tls-server-name"];

    #[test]
    fn a_configured_kubeconfig_wins_over_the_home_fallback() {
        let resolved = OracleKubeconfig::resolve(
            Some("/tmp/kind-oracle.yaml"),
            Some(Path::new("/home/operator")),
        )
        .expect("a configured value resolves");
        assert_eq!(resolved.path(), Path::new("/tmp/kind-oracle.yaml"));
    }

    #[test]
    fn an_empty_configured_value_counts_as_unset() {
        let resolved = OracleKubeconfig::resolve(Some(""), Some(Path::new("/home/operator")))
            .expect("the home fallback resolves");
        assert_eq!(
            resolved.path(),
            Path::new("/home/operator").join(ORACLE_KUBECONFIG_FALLBACK)
        );
    }

    #[test]
    fn nothing_configured_and_no_home_is_a_typed_absence_not_a_guessed_path() {
        assert_eq!(
            OracleKubeconfig::resolve(None, None),
            Err(OracleUnresolved),
            "with no env value and no HOME there is no path to try, and inventing one \
             would report a configuration gap as a dead cluster"
        );
    }

    /// The defect meta-04 pins. The unreachable hint is the only sentence an
    /// operator acts on, so it must name the knob that EXISTS — not the
    /// tunnel of a VM that was decommissioned on 2026-08-08.
    #[test]
    fn the_unreachable_hint_names_the_env_knob_and_never_the_dead_tunnel() {
        let kubeconfig = OracleKubeconfig::resolve(Some("/tmp/kind-oracle.yaml"), None)
            .expect("a configured value resolves");
        let hint = kubeconfig.unreachable().to_string();

        assert!(
            hint.contains(ORACLE_KUBECONFIG_ENV),
            "the hint must name the knob that repoints the oracle; got: {hint}"
        );
        assert!(
            hint.contains("/tmp/kind-oracle.yaml"),
            "the hint must name the kubeconfig it actually tried; got: {hint}"
        );
        for dead in ["tls-server-name", "tunnel", "192.168.64.10"] {
            assert!(
                !hint.contains(dead),
                "the hint still sends the operator to the decommissioned k3s VM \
                 ({dead}); got: {hint}"
            );
        }
    }

    /// ONE loader. No integration binary may open the oracle's kubeconfig
    /// itself or carry its own unreachable hint: `common/mod.rs` owns both,
    /// and every binary routes through `common::load_oracle`.
    ///
    /// `m0_core_crud_parity.rs` did carry its own, which is how its hint
    /// drifted a year behind the shared one without anything going red.
    /// `tests/common/` is the carve-out and is not scanned; everything
    /// directly under `tests/` is.
    #[test]
    fn no_integration_binary_loads_the_oracle_itself() {
        let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");

        // Positive controls: a scan over a directory that moved would find
        // no offender and pass while checking nothing.
        assert!(
            tests_dir.join("common").join("mod.rs").is_file(),
            "the shared loader is not at {}/common/mod.rs — this gate is scanning \
             the wrong tree",
            tests_dir.display()
        );
        assert!(
            tests_dir.join("m0_core_crud_parity.rs").is_file(),
            "the binary this gate exists for is not at {}/m0_core_crud_parity.rs \
             — this gate is scanning the wrong tree",
            tests_dir.display()
        );

        let mut offenders: Vec<String> = Vec::new();
        let entries = std::fs::read_dir(&tests_dir).expect("engenho-diff/tests is readable");
        for entry in entries {
            let path = entry.expect("a readable directory entry").path();
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("a readable test source");
            let name = path
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or("<unnamed>")
                .to_owned();
            for needle in SECOND_LOADER {
                if source.contains(needle) {
                    offenders.push(format!("{name} contains {needle:?}"));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "a second oracle loader lives outside tests/common/mod.rs, and its copy of \
             the hint will drift out of sync silently:\n  {}\n\nUse \
             `common::load_oracle()`; if it does not return what the binary needs, add \
             that to common/mod.rs.",
            offenders.join("\n  ")
        );
    }
}
