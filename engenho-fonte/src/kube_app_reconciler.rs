//! Real `AppReconciler` that translates each [`AppRef`] into a
//! typed engenho-types [`Deployment`] manifest.
//!
//! Two modes:
//!
//!   - **Dry-run** (default — `KubeAppReconciler::new()`): records
//!     every emitted Deployment manifest in an in-memory log;
//!     useful for testing, CI fixtures, and audit ("what would the
//!     reconciler apply?").
//!   - **Live apply** (M3.5+ behind `with-engenho-kube-client`):
//!     pipes the manifest through `engenho_kube_client::KubeClient`
//!     against a real cluster. The dry-run shape stays the same;
//!     only the terminal `apply()` side effect changes.
//!
//! The translation rule (deterministic):
//!
//! ```text
//! AppRef { name: "podinfo", version: Some("6.4.1") }
//!   →
//! Deployment {
//!   metadata.name: "podinfo"
//!   metadata.namespace: "default"
//!   metadata.labels: { "app": "podinfo", "pleme.io/sistema-managed": "true" }
//!   spec.replicas: 1
//!   spec.selector.matchLabels: { "app": "podinfo" }
//!   spec.template.metadata.labels: { "app": "podinfo" }
//!   spec.template.spec.containers: [{
//!     name: "podinfo",
//!     image: "podinfo:6.4.1"
//!   }]
//! }
//! ```
//!
//! `version = None` falls back to image tag `"latest"`.

use crate::{AppReconciler, AppRef, FonteResult};
use async_trait::async_trait;
use engenho_types::generated_v1_34::apps_v1::{
    Deployment, DeploymentSpec, LabelSelector, PodTemplateSpec,
};
use engenho_types::generated_v1_34::core_v1::{Container, PodSpec};
use engenho_types::meta::ObjectMeta;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// `AppReconciler` that produces typed K8s Deployment manifests for
/// each AppRef. Dry-run by default — records emitted manifests in
/// memory.
#[derive(Debug, Default)]
pub struct KubeAppReconciler {
    namespace: String,
    emitted: Mutex<Vec<Deployment>>,
}

impl KubeAppReconciler {
    /// New reconciler emitting into the `default` namespace.
    #[must_use]
    pub fn new() -> Self {
        Self::default_with_namespace("default")
    }

    /// New reconciler with a custom namespace.
    #[must_use]
    pub fn default_with_namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            emitted: Mutex::new(Vec::new()),
        }
    }

    /// Translate an AppRef to a typed Deployment manifest. Pure
    /// function — call directly for tests, the reconcile_app path
    /// also goes through this.
    #[must_use]
    pub fn translate(&self, app: &AppRef) -> Deployment {
        let name = app.name.as_ref().to_string();
        let image_tag = app
            .version
            .as_ref()
            .map(|v| v.as_ref().to_string())
            .unwrap_or_else(|| "latest".to_string());
        let image = format!("{name}:{image_tag}");

        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), name.clone());
        labels.insert("pleme.io/sistema-managed".to_string(), "true".to_string());

        let mut selector_labels = BTreeMap::new();
        selector_labels.insert("app".to_string(), name.clone());

        Deployment {
            metadata: ObjectMeta {
                name: name.clone(),
                namespace: Some(self.namespace.clone()),
                labels: labels.clone(),
                ..Default::default()
            },
            spec: DeploymentSpec {
                replicas: Some(1),
                selector: LabelSelector {
                    match_labels: selector_labels.clone(),
                },
                template: PodTemplateSpec {
                    metadata: ObjectMeta {
                        labels: selector_labels,
                        ..Default::default()
                    },
                    spec: PodSpec {
                        containers: vec![Container {
                            name: name.clone(),
                            image,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            status: None,
        }
    }

    /// Read the log of emitted Deployment manifests (for tests +
    /// audit).
    pub fn emitted(&self) -> Vec<Deployment> {
        self.emitted
            .lock()
            .expect("kube reconciler poisoned")
            .clone()
    }
}

#[async_trait]
impl AppReconciler for KubeAppReconciler {
    async fn reconcile_app(&self, app: &AppRef) -> FonteResult<()> {
        let dep = self.translate(app);
        self.emitted
            .lock()
            .expect("kube reconciler poisoned")
            .push(dep);
        // M3.5+ wiring: pipe `dep` to engenho_kube_client::KubeClient::apply
        // behind `with-engenho-kube-client` feature flag.
        Ok(())
    }
}
