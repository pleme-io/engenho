//! CaixaHelmInstaller — bridges caixa-helm's rendered ChartDir to
//! a live cluster via `helm install/upgrade` subprocess.
//!
//! v1.45's CaixaAppReconciler renders typed charts; v1.50 installs
//! them. Two-step typed pipeline: render-then-install. Operators
//! get audit-friendly intermediate state (the rendered ChartDir is
//! inspectable + diff-able before the install commits).
//!
//! Gated `with-caixa` (re-uses the existing caixa deps).

use crate::FonteResult;
use caixa_helm::ChartDir;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::process::Command;

/// Result of one helm install/upgrade invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallRecord {
    /// Chart name (release name in helm terms).
    pub release_name: String,
    /// Path the ChartDir was written to before installation.
    pub chart_path: PathBuf,
    /// helm process exit code (0 = success).
    pub exit_code: Option<i32>,
    /// stdout captured from helm.
    pub stdout: String,
}

/// Installs caixa-rendered ChartDirs into a Kubernetes cluster via
/// `helm install/upgrade`. Renders the ChartDir to disk in a
/// tempdir, then invokes helm against it.
pub struct CaixaHelmInstaller {
    namespace: String,
    kubeconfig: Option<PathBuf>,
    dry_run: bool,
    log: Mutex<Vec<InstallRecord>>,
}

impl Default for CaixaHelmInstaller {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            kubeconfig: None,
            dry_run: false,
            log: Mutex::new(Vec::new()),
        }
    }
}

impl CaixaHelmInstaller {
    /// New installer for the `default` namespace, using ambient
    /// kubeconfig (`KUBECONFIG` env or `~/.kube/config`).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the target namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the explicit kubeconfig path. When `None`, helm uses
    /// `$KUBECONFIG` or `~/.kube/config`.
    #[must_use]
    pub fn kubeconfig(mut self, path: impl Into<PathBuf>) -> Self {
        self.kubeconfig = Some(path.into());
        self
    }

    /// Dry-run mode: writes the ChartDir to disk + builds the
    /// helm argv but doesn't execute helm. Useful for CI + audit.
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Borrow the invocation log.
    pub fn invocations(&self) -> Vec<InstallRecord> {
        self.log.lock().expect("installer poisoned").clone()
    }

    /// Install (or upgrade) the given ChartDir. Writes it to a
    /// tempdir + invokes `helm upgrade --install`. Returns the
    /// captured InstallRecord; if helm exits non-zero the record
    /// is still captured before returning the typed error.
    ///
    /// # Errors
    ///
    /// - `FonteError::Propose` if the chart can't be written to
    ///   tempdir, helm spawn fails, or helm exits non-zero.
    pub async fn install(&self, chart: &ChartDir) -> FonteResult<InstallRecord> {
        // 1. Write ChartDir to a tempdir.
        let tmp = tempfile::TempDir::new()
            .map_err(|e| crate::FonteError::Propose(format!("install tempdir: {e}")))?;
        let chart_path = tmp.path().join(&chart.name);
        chart
            .write_to(tmp.path())
            .map_err(|e| crate::FonteError::Propose(format!("chart write: {e}")))?;

        // 2. Build argv.
        let mut argv: Vec<String> = vec![
            "upgrade".into(),
            "--install".into(),
            chart.name.clone(),
            chart_path.display().to_string(),
            "--namespace".into(),
            self.namespace.clone(),
            "--create-namespace".into(),
        ];
        if let Some(kc) = &self.kubeconfig {
            argv.push("--kubeconfig".into());
            argv.push(kc.display().to_string());
        }

        let mut record = InstallRecord {
            release_name: chart.name.clone(),
            chart_path: chart_path.clone(),
            exit_code: None,
            stdout: String::new(),
        };

        if self.dry_run {
            record.exit_code = Some(0);
            record.stdout = format!("DRY RUN: would invoke `helm {}`", argv.join(" "));
            self.log
                .lock()
                .expect("installer poisoned")
                .push(record.clone());
            return Ok(record);
        }

        // 3. Spawn helm.
        let output = Command::new("helm")
            .args(&argv)
            .output()
            .await
            .map_err(|e| crate::FonteError::Propose(format!("helm spawn: {e}")))?;
        record.exit_code = output.status.code();
        record.stdout = String::from_utf8_lossy(&output.stdout).to_string();
        self.log
            .lock()
            .expect("installer poisoned")
            .push(record.clone());

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(crate::FonteError::Propose(format!(
                "helm exit={:?} stderr={stderr}",
                output.status
            )));
        }
        Ok(record)
    }

    /// Verify a chart path is writable / readable. Returns the path
    /// for chaining.
    fn _verify_path(path: &Path) -> std::io::Result<&Path> {
        path.try_exists().map(|_| path)
    }
}
