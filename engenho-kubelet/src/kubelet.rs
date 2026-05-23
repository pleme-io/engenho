//! `Kubelet` — the per-node reconcile loop.
//!
//! Watches Pods bound to this node (`spec.nodeName == self.node_name`),
//! materializes containers via the [`ContainerRuntime`] backend,
//! patches `status.podIP` + `status.conditions[Ready]=True`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_controllers::{Controller, ControllerError, ReconcileReport};
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::backend::{ContainerRuntime, ContainerSpec};
use crate::error::KubeletError;

/// Per-node kubelet. Implements [`Controller`] so it slots into
/// the standard [`engenho_controllers::ControllerRuntime`] + benefits
/// from `WatchDriver` event-driven wakeup.
pub struct Kubelet {
    store: Arc<StoreMesh>,
    backend: Arc<dyn ContainerRuntime>,
    node_name: String,
    /// Local map of bound-pod name → container_id assigned by the
    /// backend. Persists for the kubelet's process lifetime; on
    /// restart we re-derive from the backend's inspect.
    local: Mutex<BTreeMap<String, String>>,
}

impl Kubelet {
    /// Construct a kubelet for `node_name`.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        backend: Arc<dyn ContainerRuntime>,
        node_name: impl Into<String>,
    ) -> Self {
        Self {
            store,
            backend,
            node_name: node_name.into(),
            local: Mutex::new(BTreeMap::new()),
        }
    }

    /// This kubelet's node name (telemetry helper).
    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Backend name (telemetry helper).
    #[must_use]
    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    fn pod_key(namespace: &str, name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", namespace, name)
    }

    /// Extract the first container's spec from a Pod manifest.
    fn pod_to_container_spec(
        namespace: &str,
        name: &str,
        pod: &Value,
    ) -> Result<ContainerSpec, KubeletError> {
        let containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .ok_or_else(|| KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers missing".into(),
            })?;
        let first = containers.first().ok_or_else(|| KubeletError::InvalidPod {
            pod: format!("{namespace}/{name}"),
            reason: "spec.containers is empty".into(),
        })?;
        let image = first
            .get("image")
            .and_then(|i| i.as_str())
            .ok_or_else(|| KubeletError::InvalidPod {
                pod: format!("{namespace}/{name}"),
                reason: "spec.containers[0].image missing".into(),
            })?
            .to_string();
        let env = first
            .get("env")
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        let n = e.get("name")?.as_str()?.to_string();
                        let v = e.get("value")?.as_str()?.to_string();
                        Some((n, v))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let command = first
            .get("command")
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| c.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(ContainerSpec {
            // Container name = namespace_name to avoid collisions.
            name: format!("{namespace}_{name}"),
            image,
            env,
            command,
        })
    }

    fn pod_already_running(pod: &Value) -> bool {
        pod.get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            .map(|p| p == "Running")
            .unwrap_or(false)
    }

    fn pod_is_bound_to(pod_value: &Value, node_name: &str) -> bool {
        pod_value
            .get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
            .map(|n| n == node_name)
            .unwrap_or(false)
    }
}

#[async_trait]
impl Controller for Kubelet {
    fn name(&self) -> &'static str {
        "kubelet"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let pods = self.store.list("", "v1", "Pod", None).await;
        let mut report = ReconcileReport::default();

        let bound: Vec<(ResourceKey, Value)> = pods
            .into_iter()
            .filter(|(_, p)| Self::pod_is_bound_to(p, &self.node_name))
            .collect();
        report.objects_examined = bound.len();

        for (key, value) in bound {
            if Self::pod_already_running(&value) {
                continue;
            }
            let namespace = key.namespace.as_deref().unwrap_or("default");
            let spec = match Self::pod_to_container_spec(namespace, &key.name, &value) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "skipping pod with invalid manifest"
                    );
                    report.objects_skipped += 1;
                    continue;
                }
            };

            // Skip if we already started this pod locally.
            {
                let local = self.local.lock().await;
                if local.contains_key(&spec.name) {
                    continue;
                }
            }

            debug!(
                pod = %key.label(),
                image = %spec.image,
                backend = self.backend.name(),
                "kubelet starting container"
            );

            match self.backend.start(&spec).await {
                Ok(status) => {
                    self.local
                        .lock()
                        .await
                        .insert(spec.name.clone(), status.container_id.clone());

                    // Patch the Pod's status with phase=Running +
                    // podIP + Ready=True. Eventual consistency: the
                    // EndpointsController will see this and add
                    // the pod to the Endpoints object.
                    let mut status_patch = json!({
                        "phase": "Running",
                        "conditions": [{ "type": "Ready", "status": "True" }],
                    });
                    if let Some(ip) = status.pod_ip {
                        status_patch["podIP"] = Value::String(ip);
                    }
                    let patch_value = json!({ "status": status_patch });
                    self.store
                        .propose(ResourceCommand::Patch {
                            key: key.clone(),
                            patch: patch_value,
                            reason: Reason::Controller,
                        })
                        .await
                        .map_err(|e| ControllerError::Store(e.to_string()))?;
                    report.objects_changed += 1;
                }
                Err(e) => {
                    warn!(
                        pod = %key.label(),
                        error = %e,
                        "container start failed; pod remains pending"
                    );
                    report.objects_skipped += 1;
                }
            }
        }

        if report.objects_changed > 0 {
            info!(
                node = %self.node_name,
                changed = report.objects_changed,
                examined = report.objects_examined,
                "kubelet tick"
            );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_to_container_spec_extracts_image_and_env() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "nginx:1.27",
                    "env": [
                        {"name": "FOO", "value": "bar"}
                    ]
                }]
            }
        });
        let spec = Kubelet::pod_to_container_spec("default", "p1", &pod).unwrap();
        assert_eq!(spec.name, "default_p1");
        assert_eq!(spec.image, "nginx:1.27");
        assert_eq!(spec.env.get("FOO").map(String::as_str), Some("bar"));
        assert!(spec.command.is_empty());
    }

    #[test]
    fn pod_to_container_spec_extracts_command_when_present() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "image": "alpine",
                    "command": ["sleep", "3600"]
                }]
            }
        });
        let spec = Kubelet::pod_to_container_spec("ns", "x", &pod).unwrap();
        assert_eq!(spec.command, vec!["sleep", "3600"]);
    }

    #[test]
    fn pod_to_container_spec_rejects_missing_image() {
        let pod = json!({"spec": {"containers": [{}]}});
        let err = Kubelet::pod_to_container_spec("ns", "p", &pod).unwrap_err();
        assert_eq!(err.kind(), "invalid_pod");
    }

    #[test]
    fn pod_to_container_spec_rejects_empty_containers() {
        let pod = json!({"spec": {"containers": []}});
        assert!(Kubelet::pod_to_container_spec("n", "p", &pod).is_err());
    }

    #[test]
    fn pod_to_container_spec_rejects_no_spec() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(Kubelet::pod_to_container_spec("n", "p", &pod).is_err());
    }

    #[test]
    fn pod_is_bound_to_matches_node_name() {
        let pod = json!({"spec": {"nodeName": "node-1"}});
        assert!(Kubelet::pod_is_bound_to(&pod, "node-1"));
        assert!(!Kubelet::pod_is_bound_to(&pod, "other-node"));
    }

    #[test]
    fn pod_is_bound_to_false_when_unbound() {
        let pod = json!({"spec": {}});
        assert!(!Kubelet::pod_is_bound_to(&pod, "node-1"));
    }

    #[test]
    fn pod_already_running_detects_phase() {
        let pod = json!({"status": {"phase": "Running"}});
        assert!(Kubelet::pod_already_running(&pod));
        let pending = json!({"status": {"phase": "Pending"}});
        assert!(!Kubelet::pod_already_running(&pending));
        let no_status = json!({"spec": {}});
        assert!(!Kubelet::pod_already_running(&no_status));
    }
}
