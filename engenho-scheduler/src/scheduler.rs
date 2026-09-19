//! The reconcile loop.

use std::sync::Arc;

use async_trait::async_trait;
use engenho_controllers::{Controller, ControllerError, ReconcileOutcome, ReconcileReport};
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::error::SchedulerError;
use crate::fit::pod_requests;
use crate::ledger::{NodeLedger, node_name_of};
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
    /// 2. Open a [`NodeLedger`]: each Node's allocatable minus the
    ///    effective requests of every pod bound there that still holds
    ///    capacity (the resource-fit **Filter** stage's accumulator).
    /// 3. For each pending Pod (empty/missing `spec.nodeName`): compute
    ///    its effective requests ([`pod_requests`]); filter Nodes to those that
    ///    currently FIT the request; ask the strategy to pick from the
    ///    fitting subset only; on a pick, patch `spec.nodeName` AND
    ///    debit the ledger so a later Pod in the SAME tick can't
    ///    overcommit that Node; on NO fitting node, leave the Pod
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

        // Open the books over EVERY pod (cluster-wide, not just the
        // namespace-scoped pending set): a pod bound in another namespace
        // still occupies its node.
        let all_pods = self.store.list("", "v1", "Pod", None).await;
        let mut ledger = NodeLedger::seed(&node_values, all_pods.iter().map(|(_, v)| v));

        for (pod_key, pod_value) in &pods {
            if !is_pending(pod_value) {
                continue;
            }
            report.pending_pods += 1;

            // Resource-fit Filter: restrict candidates to nodes that fit
            // THIS pod's request given the ledger's current balances.
            let req = pod_requests(pod_value);
            let fitting: Vec<Value> = node_values
                .iter()
                .filter(|n| node_name_of(n).is_some_and(|name| ledger.fits(name, &req)))
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
                .propose(ResourceCommand::patch(
                    pod_key.clone(),
                    patch,
                    Reason::Scheduler,
                ))
                .await?;

            // Charge the chosen node so a later pending pod in THIS SAME
            // tick can't also "fit" capacity that is now spoken for
            // (within-tick overcommit defense).
            ledger.debit(&node_name, &req);

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
            .propose(ResourceCommand::patch(
                pod_key.clone(),
                patch,
                Reason::Scheduler,
            ))
            .await?;
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

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
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
        }
        .into())
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
