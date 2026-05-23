//! SubprocessStage — concrete ProvisioningStage that invokes an
//! external binary (magma, pangea, terraform, …) via typed Command
//! construction.
//!
//! This is the canonical NO SHELL pattern: operators pass in a typed
//! closure that emits the argv from a typed Sistema — no string
//! templating, no shell escapes. The closure's return shape is a
//! typed `Vec<String>` so cargo's argument-handling rules apply.
//!
//! Always-on (no Cargo feature flag) — tokio's process module ships
//! with the runtime fonte already depends on.
//!
//! ## Usage
//!
//! Magma cloud stage:
//!
//! ```ignore
//! let cloud = Arc::new(SubprocessStage::new(
//!     StageKind::Cloud,
//!     "magma",
//!     |sistema| vec![
//!         "apply".into(),
//!         "--workspace".into(),
//!         sistema.name.to_string(),
//!         "--auto-approve".into(),
//!     ],
//! ));
//! ```
//!
//! Pangea networking stage:
//!
//! ```ignore
//! let net = Arc::new(SubprocessStage::new(
//!     StageKind::Networking,
//!     "pangea",
//!     |sistema| vec![
//!         "deploy".into(),
//!         format!("--cluster={}", sistema.name),
//!     ],
//! ));
//! ```
//!
//! Any binary that accepts argv works — terraform, kubectl,
//! ansible-playbook, custom Rust binaries.

use crate::{FonteResult, ProvisioningStage, Sistema, StageKind};
use async_trait::async_trait;
use std::ffi::OsString;
use std::sync::Mutex;
use tokio::process::Command;

/// Record of one subprocess invocation — captured for tests + audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubprocessInvocation {
    /// Sistema name that triggered the invocation.
    pub sistema_name: String,
    /// argv that was sent to the binary.
    pub argv: Vec<String>,
    /// Process exit code (0 = success). `None` if the command never
    /// terminated cleanly (signal, OS error).
    pub exit_code: Option<i32>,
    /// stdout captured from the process (UTF-8 lossy).
    pub stdout: String,
}

/// ProvisioningStage that runs an external binary with typed argv.
pub struct SubprocessStage {
    kind: StageKind,
    binary: OsString,
    argv_fn: Box<dyn Fn(&Sistema) -> Vec<String> + Send + Sync>,
    log: Mutex<Vec<SubprocessInvocation>>,
    /// When true, the stage records the invocation but doesn't
    /// actually run the binary — useful for dry-run + tests where
    /// the binary isn't installed.
    dry_run: bool,
}

impl SubprocessStage {
    /// New stage that LIVE-INVOKES the binary on each provision.
    pub fn new<F>(kind: StageKind, binary: impl Into<OsString>, argv_fn: F) -> Self
    where
        F: Fn(&Sistema) -> Vec<String> + Send + Sync + 'static,
    {
        Self {
            kind,
            binary: binary.into(),
            argv_fn: Box::new(argv_fn),
            log: Mutex::new(Vec::new()),
            dry_run: false,
        }
    }

    /// Switch to dry-run mode — records the invocation but doesn't
    /// run the binary. Useful for CI + machine-bootstrap tests.
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Read the log of invocations.
    pub fn invocations(&self) -> Vec<SubprocessInvocation> {
        self.log.lock().expect("subprocess stage poisoned").clone()
    }
}

#[async_trait]
impl ProvisioningStage for SubprocessStage {
    fn kind(&self) -> StageKind {
        self.kind
    }

    async fn provision(&self, sistema: &Sistema) -> FonteResult<()> {
        let argv = (self.argv_fn)(sistema);
        let mut invocation = SubprocessInvocation {
            sistema_name: sistema.name.to_string(),
            argv: argv.clone(),
            exit_code: None,
            stdout: String::new(),
        };
        if self.dry_run {
            invocation.exit_code = Some(0);
            self.log
                .lock()
                .expect("subprocess stage poisoned")
                .push(invocation);
            return Ok(());
        }
        let output = Command::new(&self.binary)
            .args(&argv)
            .output()
            .await
            .map_err(|e| {
                crate::FonteError::Propose(format!("subprocess-stage {:?} spawn: {e}", self.kind))
            })?;
        invocation.exit_code = output.status.code();
        invocation.stdout = String::from_utf8_lossy(&output.stdout).to_string();
        self.log
            .lock()
            .expect("subprocess stage poisoned")
            .push(invocation);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(crate::FonteError::Propose(format!(
                "subprocess-stage {:?} failed: exit={:?} stderr={stderr}",
                self.kind, output.status
            )));
        }
        Ok(())
    }
}

/// Convenience constructor: a magma cloud-allocation stage.
/// Invokes `magma apply --workspace <sistema-name>`.
#[must_use]
pub fn magma_cloud_stage() -> SubprocessStage {
    SubprocessStage::new(StageKind::Cloud, "magma", |s| {
        vec![
            "apply".into(),
            "--workspace".into(),
            s.name.to_string(),
            "--auto-approve".into(),
        ]
    })
}

/// Convenience constructor: a pangea networking stage.
/// Invokes `pangea deploy --cluster <sistema-name>`.
#[must_use]
pub fn pangea_networking_stage() -> SubprocessStage {
    SubprocessStage::new(StageKind::Networking, "pangea", |s| {
        vec!["deploy".into(), format!("--cluster={}", s.name)]
    })
}
