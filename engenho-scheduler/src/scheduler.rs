//! The reconcile loop.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_controllers::{Controller, ControllerError, ReconcileReport};
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::error::SchedulerError;
use crate::strategy::SchedulingStrategy;

/// The scheduler.
pub struct Scheduler {
    store: Arc<StoreMesh>,
    strategy: Box<dyn SchedulingStrategy>,
    /// Namespace filter — `None` means all namespaces.
    namespace: Option<String>,
}

impl Scheduler {
    #[must_use]
    pub fn new<S: SchedulingStrategy + 'static>(
        store: Arc<StoreMesh>,
        strategy: S,
        namespace: Option<String>,
    ) -> Self {
        Self {
            store,
            strategy: Box::new(strategy),
            namespace,
        }
    }

    /// One reconcile tick.
    ///
    /// 1. List all Pods (matching namespace filter).
    /// 2. Filter to those with empty/missing `spec.nodeName`.
    /// 3. For each pending pod, ask the strategy to pick a node.
    /// 4. Patch the pod with the binding.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Store`] if the store list/patch fails.
    pub async fn tick(&self) -> Result<TickReport, SchedulerError> {
        let pods = self
            .store
            .list("", "v1", "Pod", self.namespace.as_deref())
            .await;
        let nodes = self.store.list("", "v1", "Node", None).await;

        let mut report = TickReport::default();
        report.pods_examined = pods.len();
        report.nodes_available = nodes.len();

        let node_values: Vec<Value> = nodes.iter().map(|(_, v)| v.clone()).collect();

        for (pod_key, pod_value) in &pods {
            if !is_pending(pod_value) {
                continue;
            }
            report.pending_pods += 1;
            let Some(node_name) = self.strategy.pick(pod_value, &node_values).await else {
                report.skipped_no_node += 1;
                warn!(
                    pod = %pod_key.label(),
                    "no schedulable node available; pod stays pending"
                );
                continue;
            };
            debug!(
                pod = %pod_key.label(),
                node = %node_name,
                strategy = self.strategy.name(),
                "binding pod"
            );
            let patch = serde_json::json!({ "spec": { "nodeName": node_name } });
            self.store
                .propose(ResourceCommand::Patch {
                    key: pod_key.clone(),
                    patch,
                    expected: None,
                    reason: Reason::Scheduler,
                })
                .await
                .map_err(|e| SchedulerError::Store(e.to_string()))?;
            report.bound.push(Binding {
                pod_key: pod_key.clone(),
                node_name,
            });
        }
        if !report.bound.is_empty() || report.pending_pods > 0 {
            info!(
                bound = report.bound.len(),
                pending = report.pending_pods,
                skipped = report.skipped_no_node,
                "scheduler tick done"
            );
        }
        Ok(report)
    }

    /// Strategy in use (for telemetry / introspection).
    #[must_use]
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }
}

/// Third-site extraction: Scheduler is the FIRST `Controller`
/// site (R8); ReplicaSet/Deployment/Endpoints/GC are sites 2-5
/// in engenho-controllers (R9 onward). This impl unifies them
/// under one trait so a [`engenho_controllers::ControllerRuntime`]
/// or [`engenho_controllers::WatchDriver`] can host the Scheduler
/// alongside the other controllers — same trait, same runtime,
/// same event-driven wake.
#[async_trait]
impl Controller for Scheduler {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let report = Scheduler::tick(self).await.map_err(|e| match e {
            SchedulerError::Store(s) => ControllerError::Store(s),
            SchedulerError::NoSchedulableNodes => {
                ControllerError::Internal("no schedulable nodes".into())
            }
            SchedulerError::InvalidPodMetadata => {
                ControllerError::InvalidResource("invalid pod metadata".into())
            }
            SchedulerError::Internal(s) => ControllerError::Internal(s),
        })?;
        Ok(ReconcileReport {
            objects_examined: report.pods_examined,
            objects_changed: report.bound.len(),
            objects_skipped: report.skipped_no_node,
            note: if report.pending_pods > 0 {
                Some(format!(
                    "{} pending → {} bound, {} skipped",
                    report.pending_pods,
                    report.bound.len(),
                    report.skipped_no_node
                ))
            } else {
                None
            },
        })
    }
}

/// Result of one [`Scheduler::tick`].
#[derive(Default, Debug)]
pub struct TickReport {
    pub pods_examined: usize,
    pub nodes_available: usize,
    pub pending_pods: usize,
    pub skipped_no_node: usize,
    pub bound: Vec<Binding>,
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub pod_key: ResourceKey,
    pub node_name: String,
}

/// A pod is pending if its `spec.nodeName` is empty/missing.
pub fn is_pending(pod: &Value) -> bool {
    pod.get("spec")
        .and_then(|s| s.get("nodeName"))
        .and_then(|n| n.as_str())
        .map(str::is_empty)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn is_pending_no_spec_is_pending() {
        let p = json!({"metadata": {"name": "x"}});
        assert!(is_pending(&p));
    }

    #[test]
    fn is_pending_no_nodename_is_pending() {
        let p = json!({"spec": {"image": "x"}});
        assert!(is_pending(&p));
    }

    #[test]
    fn is_pending_empty_nodename_is_pending() {
        let p = json!({"spec": {"nodeName": ""}});
        assert!(is_pending(&p));
    }

    #[test]
    fn is_pending_with_nodename_is_not_pending() {
        let p = json!({"spec": {"nodeName": "node-1"}});
        assert!(!is_pending(&p));
    }
}
