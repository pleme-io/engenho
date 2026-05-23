//! CaixaAppReconciler — `AppReconciler` that runs caixa-helm's
//! chart renderer for each AppRef + records the resulting ChartDir.
//!
//! The fonte AppRef is minimal (name + version). Real caixa Servicos
//! carry more (computeunit spec, deps, etc.). This reconciler
//! synthesizes a minimal-shape Caixa per AppRef via the canonical
//! `(defcaixa …)` form parsed by `Caixa::from_lisp`, then invokes
//! `caixa_helm::render_chart_for_servico` against a default
//! ComputeUnit YAML. The rendered ChartDir is recorded for tests +
//! downstream Helm-apply pipelines.
//!
//! Gated `with-caixa`.

use crate::{AppReconciler, AppRef, FonteResult};
use async_trait::async_trait;
use caixa_core::manifest::Caixa;
use caixa_helm::{ChartDir, render_chart_for_servico};
use std::sync::Mutex;

/// AppReconciler backed by caixa-helm's chart renderer.
#[derive(Default)]
pub struct CaixaAppReconciler {
    rendered: Mutex<Vec<ChartDir>>,
}

impl std::fmt::Debug for CaixaAppReconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaixaAppReconciler")
            .field(
                "rendered_count",
                &self.rendered.lock().map(|v| v.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl CaixaAppReconciler {
    /// New reconciler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the log of rendered ChartDirs for tests + downstream
    /// `helm install` / `helm upgrade` pipelines.
    pub fn rendered(&self) -> Vec<ChartDir> {
        self.rendered
            .lock()
            .expect("caixa reconciler poisoned")
            .clone()
    }

    /// Synthesize a minimal-shape Caixa Servico from an AppRef via
    /// the canonical `(defcaixa …)` form. Uses Caixa::from_lisp so
    /// we don't have to maintain field-by-field construction for
    /// the substrate's evolving optional slots.
    fn synthesize_caixa(&self, app: &AppRef) -> FonteResult<Caixa> {
        let version = app
            .version
            .as_ref()
            .map(|v| v.as_ref().to_string())
            .unwrap_or_else(|| "0.1.0".to_string());
        let src = format!(
            "(defcaixa\n  :nome \"{name}\"\n  :versao \"{version}\"\n  :kind Servico\n  :servicos (\"{name}-cu.yaml\"))",
            name = app.name,
            version = version,
        );
        Caixa::from_lisp(&src)
            .map_err(|e| crate::FonteError::Propose(format!("caixa from_lisp: {e:?}")))
    }

    /// Minimal placeholder ComputeUnit YAML — real wiring sources
    /// the actual ComputeUnit from the operator's repo.
    fn default_computeunit(&self, app: &AppRef) -> serde_yaml::Value {
        serde_yaml::from_str(&format!(
            "apiVersion: wasm-operator.pleme.io/v1\nkind: ComputeUnit\nmetadata:\n  name: {}-cu\nspec:\n  image: {}:latest\n",
            app.name, app.name
        ))
        .expect("default ComputeUnit YAML is well-formed")
    }
}

#[async_trait]
impl AppReconciler for CaixaAppReconciler {
    async fn reconcile_app(&self, app: &AppRef) -> FonteResult<()> {
        let caixa = self.synthesize_caixa(app)?;
        let cu = self.default_computeunit(app);
        let chart = render_chart_for_servico(&caixa, &cu)
            .map_err(|e| crate::FonteError::Propose(format!("caixa-helm render: {e}")))?;
        self.rendered
            .lock()
            .expect("caixa reconciler poisoned")
            .push(chart);
        Ok(())
    }
}
