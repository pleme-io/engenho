//! Route each pod to the runtime its `runtimeClassName` names.
//!
//! A pod that names no class runs on the node's default runtime, through the
//! same calls as before this router existed. A pod naming a class this node
//! serves runs on that class's runtime; a pod naming any other class is
//! refused at start with [`KubeletError::RuntimeClassUnknown`], since running
//! it on the default runtime would silently ignore what it asked for.
//!
//! Calls after `start` are routed by container id: the router records which
//! class runtime started each container, and every id it did not record
//! belongs to the default runtime.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::backend::{
    ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions, LogRead, Readoption,
};
use crate::error::KubeletError;

/// The default runtime plus the class runtimes this node serves.
pub struct RuntimeClassRouter {
    default: Arc<dyn ContainerRuntime>,
    classes: BTreeMap<String, Arc<dyn ContainerRuntime>>,
    owners: Mutex<HashMap<String, String>>,
}

impl RuntimeClassRouter {
    /// A router with only the default runtime: every class is refused.
    #[must_use]
    pub fn new(default: Arc<dyn ContainerRuntime>) -> Self {
        Self {
            default,
            classes: BTreeMap::new(),
            owners: Mutex::new(HashMap::new()),
        }
    }

    /// Serve `class` with `runtime`.
    #[must_use]
    pub fn with_class(
        mut self,
        class: impl Into<String>,
        runtime: Arc<dyn ContainerRuntime>,
    ) -> Self {
        self.classes.insert(class.into(), runtime);
        self
    }

    /// The classes this router serves, sorted.
    #[must_use]
    pub fn classes(&self) -> Vec<String> {
        self.classes.keys().cloned().collect()
    }

    fn route(&self, container_id: &str) -> Arc<dyn ContainerRuntime> {
        self.owners
            .lock()
            .ok()
            .and_then(|o| o.get(container_id).cloned())
            .and_then(|class| self.classes.get(&class).cloned())
            .unwrap_or_else(|| Arc::clone(&self.default))
    }
}

#[async_trait]
impl ContainerRuntime for RuntimeClassRouter {
    fn name(&self) -> &'static str {
        self.default.name()
    }

    fn version(&self) -> Option<String> {
        self.default.version()
    }

    fn readoption(&self) -> Readoption {
        self.default.readoption()
    }

    fn readoption_of(&self, runtime_class: Option<&str>) -> Readoption {
        runtime_class
            .and_then(|class| self.classes.get(class))
            .map_or_else(|| self.default.readoption(), |runtime| runtime.readoption())
    }

    fn resource_enforcement(&self) -> crate::cgroup::ResourceEnforcement {
        self.default.resource_enforcement()
    }

    async fn exec(&self, container_id: &str, argv: &[String]) -> Result<ExecOutcome, KubeletError> {
        self.route(container_id).exec(container_id, argv).await
    }

    async fn exec_stream(
        &self,
        container_id: &str,
        req: &crate::stream::StreamRequest,
    ) -> Result<crate::stream::StreamSession, KubeletError> {
        self.route(container_id)
            .exec_stream(container_id, req)
            .await
    }

    async fn attach(
        &self,
        container_id: &str,
        req: &crate::stream::StreamRequest,
    ) -> Result<crate::stream::StreamSession, KubeletError> {
        self.route(container_id).attach(container_id, req).await
    }

    #[allow(clippy::disallowed_methods)]
    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        let Some(request) = spec.runtime_class.as_ref() else {
            return self.default.start(spec).await;
        };
        let Some(runtime) = self.classes.get(&request.name) else {
            return Err(KubeletError::RuntimeClassUnknown {
                pod: format!("{}/{}", spec.pod.namespace, spec.pod.name),
                class: request.name.clone(),
                served: self.classes(),
            });
        };
        let started = runtime.start(spec).await?;
        if let Ok(mut owners) = self.owners.lock() {
            owners.insert(started.container_id.clone(), request.name.clone());
        }
        Ok(started)
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        self.route(container_id).status(container_id).await
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        self.route(container_id).stop(container_id).await
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        self.route(container_id).remove(container_id).await?;
        if let Ok(mut owners) = self.owners.lock() {
            owners.remove(container_id);
        }
        Ok(())
    }

    async fn logs(&self, container_id: &str, opts: &LogOptions) -> Result<String, KubeletError> {
        self.route(container_id).logs(container_id, opts).await
    }

    async fn usage(&self, container_id: &str) -> Result<crate::usage::Reading, KubeletError> {
        self.route(container_id).usage(container_id).await
    }

    async fn read_log(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<LogRead, KubeletError> {
        self.route(container_id).read_log(container_id, opts).await
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::backend::{FakeBackend, FakeEvent, RuntimeClassRequest};

    fn spec(name: &str, class: Option<&str>) -> ContainerSpec {
        ContainerSpec {
            name: name.to_string(),
            image: "img".to_string(),
            runtime_class: class.map(|c| RuntimeClassRequest {
                name: c.to_string(),
                ..RuntimeClassRequest::default()
            }),
            pod: crate::backend::PodIdentity {
                namespace: "ns".into(),
                name: "pod".into(),
                container_name: name.into(),
                ..Default::default()
            },
            ..ContainerSpec::default()
        }
    }

    fn router() -> (RuntimeClassRouter, FakeBackend, FakeBackend) {
        let default = FakeBackend::new().with_ids_from_names();
        let wasm = FakeBackend::new().with_ids_from_names();
        let r = RuntimeClassRouter::new(Arc::new(default.clone()))
            .with_class("wasm", Arc::new(wasm.clone()));
        (r, default, wasm)
    }

    /// RED RUN (2026-10-27): `start` sending every spec to the default
    /// runtime → `the wasm class did not reach the wasm runtime`.
    #[tokio::test]
    async fn a_pod_naming_a_served_class_runs_on_that_class_and_stays_there() {
        let (r, default, wasm) = router();
        let started = r.start(&spec("c", Some("wasm"))).await.expect("start");
        assert_eq!(
            wasm.events().await,
            vec![FakeEvent::Start("c".into())],
            "the wasm class did not reach the wasm runtime"
        );
        assert!(default.events().await.is_empty());
        r.stop(&started.container_id).await.expect("stop");
        r.remove(&started.container_id).await.expect("remove");
        assert_eq!(
            wasm.events().await.len(),
            3,
            "stop and remove left the class runtime"
        );
        assert!(default.events().await.is_empty());
    }

    #[tokio::test]
    async fn a_pod_naming_no_class_runs_on_the_default_runtime_unchanged() {
        let (r, default, wasm) = router();
        let started = r.start(&spec("c", None)).await.expect("start");
        assert!(
            r.status(&started.container_id)
                .await
                .expect("status")
                .is_some()
        );
        assert_eq!(default.events().await, vec![FakeEvent::Start("c".into())]);
        assert!(wasm.events().await.is_empty());
    }

    /// RED RUN (2026-10-27): the unknown arm falling back to the default
    /// runtime → `an unknown class was started`.
    #[tokio::test]
    async fn a_pod_naming_an_unknown_class_is_refused_and_nothing_starts() {
        let (r, default, wasm) = router();
        let err = r
            .start(&spec("c", Some("gvisor")))
            .await
            .expect_err("an unknown class was started");
        match err {
            KubeletError::RuntimeClassUnknown { class, served, .. } => {
                assert_eq!(class, "gvisor");
                assert_eq!(served, vec!["wasm".to_string()]);
            }
            other => panic!("wrong error: {other:?}"),
        }
        assert!(default.events().await.is_empty());
        assert!(wasm.events().await.is_empty());
    }
}
