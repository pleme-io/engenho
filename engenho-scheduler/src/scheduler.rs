//! The reconcile loop.

use std::collections::HashMap;
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
use crate::fit::{NodeResources, fits, node_allocatable, pod_requests};
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
    /// 1. List all Pods (matching namespace filter) + all Nodes.
    /// 2. Seed each Node's running **free** capacity =
    ///    `allocatable − Σ requests of pods already bound there`
    ///    (the resource-fit **Filter** stage's accumulator).
    /// 3. For each pending Pod (empty/missing `spec.nodeName`): compute
    ///    its summed resource requests; filter Nodes to those that
    ///    currently FIT the request; ask the strategy to pick from the
    ///    fitting subset only; on a pick, patch `spec.nodeName` AND
    ///    decrement that Node's running free so a later Pod in the SAME
    ///    tick can't overcommit it; on NO fitting node, leave the Pod
    ///    unbound + write a typed `PodScheduled=False /
    ///    reason=Unschedulable` status.
    ///
    /// The fit predicate runs in FRONT of the strategy — exactly the
    /// upstream kube-scheduler `Filter (Predicates) → Score (Strategy)`
    /// split. The strategy stays pure over its candidate set and is
    /// never handed a node the Pod can't fit.
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

        // Seed per-node running free capacity = allocatable − Σ requests
        // of pods already bound to it. We scan EVERY pod (cluster-wide,
        // not just the namespace-scoped pending set) so a pod bound in
        // another namespace still counts against the node's capacity.
        let all_pods = self.store.list("", "v1", "Pod", None).await;
        let mut free: HashMap<String, NodeResources> = node_values
            .iter()
            .filter_map(|n| node_name_of(n).map(|name| (name, node_allocatable(n))))
            .collect();
        for (_, pod) in &all_pods {
            let Some(node) = bound_node(pod) else {
                continue;
            };
            if let Some(f) = free.get_mut(&node) {
                let req = pod_requests(pod);
                f.cpu_milli = (f.cpu_milli - req.cpu_milli).max(0);
                f.mem_milli = (f.mem_milli - req.mem_milli).max(0);
            }
        }

        for (pod_key, pod_value) in &pods {
            if !is_pending(pod_value) {
                continue;
            }
            report.pending_pods += 1;

            // Resource-fit Filter: restrict candidates to nodes that fit
            // THIS pod's request given current running free capacity.
            let req = pod_requests(pod_value);
            let fitting: Vec<Value> = node_values
                .iter()
                .filter(|n| {
                    node_name_of(n)
                        .and_then(|name| free.get(&name).copied())
                        .is_some_and(|f| fits(&f, &req))
                })
                .cloned()
                .collect();

            if fitting.is_empty() {
                report.unschedulable_no_fit += 1;
                warn!(
                    pod = %pod_key.label(),
                    nodes = report.nodes_available,
                    "no node fits pod's resource requests; staying Pending"
                );
                self.mark_unschedulable(pod_key, report.nodes_available)
                    .await?;
                continue;
            }

            let Some(node_name) = self.strategy.pick(pod_value, &fitting).await else {
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

            // Decrement the chosen node's running free so a later pending
            // pod in THIS SAME tick can't also "fit" capacity that is now
            // spoken for (within-tick overcommit defense).
            if let Some(f) = free.get_mut(&node_name) {
                f.cpu_milli = (f.cpu_milli - req.cpu_milli).max(0);
                f.mem_milli = (f.mem_milli - req.mem_milli).max(0);
            }

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
                unschedulable = report.unschedulable_no_fit,
                "scheduler tick done"
            );
        }
        Ok(report)
    }

    /// Write a typed `PodScheduled=False / reason=Unschedulable` status
    /// condition onto a pod that fits no node — mirroring upstream
    /// kube-scheduler. The Pod is NOT bound; its `spec.nodeName` stays
    /// absent (so "Pending" is still the absence of a binding), but the
    /// reason is now machine-readable instead of an invisible omission.
    async fn mark_unschedulable(
        &self,
        pod_key: &ResourceKey,
        node_count: usize,
    ) -> Result<(), SchedulerError> {
        let patch = serde_json::json!({
            "status": {
                "phase": "Pending",
                "conditions": [{
                    "type": "PodScheduled",
                    "status": "False",
                    "reason": "Unschedulable",
                    "message": format!(
                        "0/{node_count} nodes are available: insufficient cpu/memory"
                    ),
                }]
            }
        });
        self.store
            .propose(ResourceCommand::Patch {
                key: pod_key.clone(),
                patch,
                expected: None,
                reason: Reason::Scheduler,
            })
            .await
            .map_err(|e| SchedulerError::Store(e.to_string()))?;
        Ok(())
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
            SchedulerError::UnsupportedStrategy { requested } => {
                ControllerError::Internal(format!("unsupported scheduling strategy: {requested:?}"))
            }
            SchedulerError::Internal(s) => ControllerError::Internal(s),
        })?;
        Ok(ReconcileReport {
            objects_examined: report.pods_examined,
            // Both binds AND Unschedulable-status writes mutate objects.
            objects_changed: report.bound.len() + report.unschedulable_no_fit,
            objects_skipped: report.skipped_no_node,
            note: if report.pending_pods > 0 {
                Some(format!(
                    "{} pending → {} bound, {} unschedulable, {} skipped",
                    report.pending_pods,
                    report.bound.len(),
                    report.unschedulable_no_fit,
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
    /// Pending pods left unbound because NO schedulable node existed at
    /// all (every node cordoned / not-Ready). Distinct from
    /// `unschedulable_no_fit`.
    pub skipped_no_node: usize,
    /// Pending pods left unbound because no node had enough free
    /// cpu/memory to fit the pod's requests. Each such pod gets a typed
    /// `PodScheduled=False / reason=Unschedulable` status.
    pub unschedulable_no_fit: usize,
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

/// A node's `metadata.name`, if present.
fn node_name_of(node: &Value) -> Option<String> {
    node.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// The node a pod is already bound to (non-empty `spec.nodeName`), or
/// `None` if the pod is still pending.
fn bound_node(pod: &Value) -> Option<String> {
    pod.get("spec")
        .and_then(|s| s.get("nodeName"))
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
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
