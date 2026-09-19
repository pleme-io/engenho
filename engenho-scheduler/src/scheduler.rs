//! The reconcile loop.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_controllers::node_lease::lease_key;
use engenho_controllers::{Controller, ControllerError, ReconcileOutcome, ReconcileReport};
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::error::SchedulerError;
use crate::filter::{Diagnosis, Filtered, filter};
use crate::fit::pod_requests;
use crate::ledger::{NodeLedger, node_name_of};
use crate::observed::ObservedNode;
use crate::scope::NamespaceScope;
use crate::strategy::SchedulingStrategy;

/// The scheduler.
pub struct Scheduler {
    store: Arc<StoreMesh>,
    strategy: Box<dyn SchedulingStrategy>,
    /// Which pods this scheduler places.
    scope: NamespaceScope,
}

impl Scheduler {
    /// A scheduler over `store` placing pods from `namespace`, where `None`
    /// and `Some("")` both mean every namespace.
    ///
    /// Operator config goes through [`Scheduler::from_config`] instead, which
    /// reads every field of `SchedulerConfig`.
    #[must_use]
    pub fn new<S: SchedulingStrategy + 'static>(
        store: Arc<StoreMesh>,
        strategy: S,
        namespace: Option<String>,
    ) -> Self {
        Self::assemble(store, Box::new(strategy), NamespaceScope::from(namespace))
    }

    /// The one place a `Scheduler` is put together.
    pub(crate) fn assemble(
        store: Arc<StoreMesh>,
        strategy: Box<dyn SchedulingStrategy>,
        scope: NamespaceScope,
    ) -> Self {
        Self {
            store,
            strategy,
            scope,
        }
    }

    /// The store this scheduler reads and writes.
    pub(crate) fn store(&self) -> &Arc<StoreMesh> {
        &self.store
    }

    /// Which pods this scheduler places.
    #[must_use]
    pub fn scope(&self) -> &NamespaceScope {
        &self.scope
    }

    /// One reconcile tick.
    ///
    /// 1. List all Pods (matching namespace filter) + all Nodes, and
    ///    derive each Node's `Ready` from its Lease ([`Self::observe`]).
    /// 2. Open a [`NodeLedger`]: each Node's allocatable minus the
    ///    effective requests of every pod bound there that still holds
    ///    capacity.
    /// 3. For each pending Pod (empty/missing `spec.nodeName`), run the
    ///    Filter stage ([`filter`]): every [`crate::FilterPlugin`] against
    ///    every observed node. Then, by its outcome:
    ///    - **Feasible**: the strategy picks one node from the non-empty
    ///      feasible set; patch `spec.nodeName` and debit the ledger, so a
    ///      later Pod in the SAME tick cannot overcommit that node.
    ///    - **Infeasible**: leave the Pod unbound and give it a
    ///      `PodScheduled=False / reason=Unschedulable` condition whose
    ///      message says how many nodes each plugin excluded. Written only
    ///      when the Pod does not already carry that exact condition.
    ///    - **No nodes observed**: write nothing. There is no reason to
    ///      report, only an absence of nodes.
    ///
    /// The Filter stage runs in FRONT of the strategy — upstream
    /// kube-scheduler's `Filter → Score` split. The strategy is never handed a
    /// node a plugin rejected, nor an empty set.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::Store`] if the store list/patch fails.
    pub async fn tick(&self) -> Result<TickReport, SchedulerError> {
        let pods = self
            .store
            .list("", "v1", "Pod", self.scope.list_filter())
            .await;
        let nodes = self.store.list("", "v1", "Node", None).await;

        let mut report = TickReport::default();
        report.pods_examined = pods.len();
        report.nodes_available = nodes.len();

        let node_values: Vec<Value> = nodes.into_iter().map(|(_, v)| v).collect();

        // Open the books over EVERY pod (cluster-wide, not just the
        // namespace-scoped pending set): a pod bound in another namespace
        // still occupies its node.
        let all_pods = self.store.list("", "v1", "Pod", None).await;
        let mut ledger = NodeLedger::seed(&node_values, all_pods.iter().map(|(_, v)| v));

        let observed = self.observe(node_values).await;

        for (pod_key, pod_value) in &pods {
            if !is_pending(pod_value) {
                continue;
            }
            report.pending_pods += 1;

            let req = pod_requests(pod_value);
            let node_name = match filter(pod_value, &req, &observed, &ledger) {
                Filtered::Feasible(feasible) => self.strategy.pick(pod_value, &feasible).await,
                Filtered::Infeasible(diagnosis) => {
                    report.unschedulable += 1;
                    warn!(
                        pod = %pod_key.label(),
                        %diagnosis,
                        "no node passes every filter; staying Pending"
                    );
                    if self
                        .mark_unschedulable(pod_key, pod_value, &diagnosis)
                        .await?
                    {
                        report.unschedulable_written += 1;
                    }
                    continue;
                }
                Filtered::NoNodesObserved => {
                    report.no_nodes_observed += 1;
                    debug!(
                        pod = %pod_key.label(),
                        "no Node observed; pod stays Pending, nothing written"
                    );
                    continue;
                }
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
                unschedulable = report.unschedulable,
                no_nodes_observed = report.no_nodes_observed,
                "scheduler tick done"
            );
        }
        Ok(report)
    }

    /// Derive each Node's `Ready` condition from its Lease.
    ///
    /// This is the apiserver's Node read path, run by the scheduler: the
    /// Lease is read by [`lease_key`] straight from the store and handed to
    /// the one projection
    /// ([`engenho_controllers::node_lease::project_ready_condition`], through
    /// [`ObservedNode::project`]). The scheduler therefore places pods by the
    /// same `Ready` that `kubectl get node` shows, never by the condition
    /// storage happens to hold.
    ///
    /// One `now` for the whole tick, so every node is judged at one instant.
    /// A node with no name has no Lease to look up and reads `Unknown`.
    async fn observe(&self, nodes: Vec<Value>) -> Vec<ObservedNode> {
        let now = engenho_types::time::now_rfc3339_utc();
        let mut observed = Vec::with_capacity(nodes.len());
        for node in nodes {
            let lease = match node_name_of(&node) {
                Some(name) => self.store.get(&lease_key(name)).await,
                None => None,
            };
            observed.push(ObservedNode::project(node, lease.as_ref(), &now));
        }
        observed
    }

    /// Give a pod no node admits a `PodScheduled=False /
    /// reason=Unschedulable` condition carrying `diagnosis`, mirroring
    /// upstream kube-scheduler. The Pod is NOT bound; its `spec.nodeName`
    /// stays absent, so "Pending" is still the absence of a binding.
    ///
    /// Returns whether it wrote. A pod that already carries this exact
    /// condition is left alone: the scheduler watches Pods, so rewriting an
    /// unchanged condition every tick would wake the scheduler on its own
    /// write.
    async fn mark_unschedulable(
        &self,
        pod_key: &ResourceKey,
        pod: &Value,
        diagnosis: &Diagnosis,
    ) -> Result<bool, SchedulerError> {
        let condition = unschedulable_condition(diagnosis);
        if already_marked(pod, &condition) {
            return Ok(false);
        }
        let patch = serde_json::json!({
            "status": { "phase": "Pending", "conditions": [condition] }
        });
        self.store
            .propose(ResourceCommand::patch(
                pod_key.clone(),
                patch,
                Reason::Scheduler,
            ))
            .await?;
        Ok(true)
    }

    /// Strategy in use (for telemetry / introspection).
    #[must_use]
    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }
}

/// The condition an unschedulable pod carries.
fn unschedulable_condition(diagnosis: &Diagnosis) -> Value {
    serde_json::json!({
        "type": "PodScheduled",
        "status": "False",
        "reason": "Unschedulable",
        "message": diagnosis.to_string(),
    })
}

/// Does `pod` already carry `condition` (by type, status, reason and message)
/// with phase `Pending`?
fn already_marked(pod: &Value, condition: &Value) -> bool {
    const KEYS: [&str; 4] = ["type", "status", "reason", "message"];
    pod.pointer("/status/phase").and_then(Value::as_str) == Some("Pending")
        && pod
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|c| KEYS.iter().all(|k| c.get(k) == condition.get(k)))
            })
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
            e @ SchedulerError::ZeroTickInterval => ControllerError::Internal(e.to_string()),
            SchedulerError::Internal(s) => ControllerError::Internal(s),
        })?;
        Ok(report.controller_report().into())
    }
}

/// Result of one [`Scheduler::tick`].
#[derive(Default, Debug)]
pub struct TickReport {
    pub pods_examined: usize,
    pub nodes_available: usize,
    pub pending_pods: usize,
    /// Pending pods no observed node admitted: every node was rejected by
    /// some [`crate::FilterPlugin`]. Each carries a `PodScheduled=False /
    /// reason=Unschedulable` condition naming the rejections.
    pub unschedulable: usize,
    /// Of [`Self::unschedulable`], the pods whose condition was written this
    /// tick. The rest already carried the identical condition.
    pub unschedulable_written: usize,
    /// Pending pods left untouched because the scheduler observed no Node
    /// at all. Nothing is written for them.
    pub no_nodes_observed: usize,
    pub bound: Vec<Binding>,
}

impl TickReport {
    /// Pending pods this tick neither bound nor wrote a condition for.
    #[must_use]
    pub fn left_untouched(&self) -> usize {
        self.no_nodes_observed.saturating_add(
            self.unschedulable
                .saturating_sub(self.unschedulable_written),
        )
    }

    /// This tick as the controller runtime counts it.
    ///
    /// Binds and Unschedulable-condition writes both mutate a Pod, so both
    /// are changes. A pending pod the tick neither bound nor wrote for is a
    /// skip. The note is carried only when something was pending.
    ///
    /// ★ Only the counts the scheduler keeps are named. Every other field
    /// of [`ReconcileReport`] comes from its `Default`, so a field the
    /// controllers crate adds lands here at its default instead of breaking
    /// this crate's build, and the scheduler never reports a count it does
    /// not keep. Tier-honest: `..Default::default()` makes a new field
    /// COMPILE; that it stays at its default here is a test
    /// (`controller_report_names_only_the_counts_the_scheduler_keeps`), not
    /// a type.
    #[must_use]
    #[allow(
        clippy::needless_update,
        reason = "every field is named today; the update is what lets engenho-controllers add one without editing this crate"
    )]
    pub fn controller_report(&self) -> ReconcileReport {
        ReconcileReport {
            objects_examined: self.pods_examined,
            objects_changed: self.bound.len() + self.unschedulable_written,
            objects_skipped: self.left_untouched(),
            note: (self.pending_pods > 0).then(|| self.to_string()),
            ..Default::default()
        }
    }
}

impl fmt::Display for TickReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} pending → {} bound, {} unschedulable ({} written), {} with no node observed",
            self.pending_pods,
            self.bound.len(),
            self.unschedulable,
            self.unschedulable_written,
            self.no_nodes_observed
        )
    }
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

    fn condition(message: &str) -> Value {
        json!({ "type": "PodScheduled", "status": "False",
                "reason": "Unschedulable", "message": message })
    }

    #[test]
    fn a_pod_already_carrying_the_condition_is_not_rewritten() {
        let want = condition("0/1 nodes are available: 1 node(s) were unschedulable.");
        let mut carried = want.clone();
        carried["lastTransitionTime"] = json!("2026-09-19T12:00:00Z");
        let pod = json!({ "status": { "phase": "Pending", "conditions": [
            { "type": "Initialized", "status": "True" }, carried
        ] } });
        assert!(already_marked(&pod, &want));
    }

    #[test]
    fn a_changed_message_or_phase_is_rewritten() {
        let want = condition("0/2 nodes are available: 2 node(s) were unschedulable.");
        let stale_message = json!({ "status": { "phase": "Pending", "conditions": [
            condition("0/1 nodes are available: 1 node(s) were unschedulable.")
        ] } });
        assert!(!already_marked(&stale_message, &want));
        let no_phase = json!({ "status": { "conditions": [want.clone()] } });
        assert!(!already_marked(&no_phase, &want));
        assert!(!already_marked(&json!({}), &want));
    }

    // ── I31: the controller report names only the counts the scheduler keeps ──

    /// A tick with `pending` pending pods: `bound` bound, `unschedulable`
    /// admitted by no node (of which `written` got a fresh condition), and
    /// `no_nodes` left because no Node was observed at all.
    fn tick_of(
        pending: usize,
        bound: usize,
        unschedulable: usize,
        written: usize,
        no_nodes: usize,
    ) -> TickReport {
        TickReport {
            pods_examined: pending + 2,
            nodes_available: 3,
            pending_pods: pending,
            unschedulable,
            unschedulable_written: written,
            no_nodes_observed: no_nodes,
            bound: (0..bound)
                .map(|i| Binding {
                    pod_key: ResourceKey::namespaced("", "v1", "Pod", "default", &i.to_string()),
                    node_name: "node-1".into(),
                })
                .collect(),
        }
    }

    #[test]
    fn controller_report_counts_binds_and_condition_writes_as_changes() {
        // 6 pending: 2 bound, 3 unschedulable (1 written, 2 already
        // carrying it), 1 with no node observed.
        let report = tick_of(6, 2, 3, 1, 1).controller_report();
        assert_eq!(report.objects_examined, 8);
        assert_eq!(report.objects_changed, 3, "2 binds + 1 condition write");
        assert_eq!(
            report.objects_skipped, 3,
            "2 unschedulable already marked + 1 with no node observed"
        );
    }

    #[test]
    fn controller_report_carries_a_note_only_when_something_was_pending() {
        let busy = tick_of(1, 1, 0, 0, 0);
        assert_eq!(
            busy.controller_report().note.as_deref(),
            Some(busy.to_string().as_str())
        );
        assert_eq!(tick_of(0, 0, 0, 0, 0).controller_report().note, None);
    }

    /// Every field of the runtime's report other than the four the
    /// scheduler counts is at `ReconcileReport::default()`. When
    /// engenho-controllers adds a field, this crate still builds (the
    /// `..Default::default()` in `controller_report`) and this test is
    /// what says the scheduler did not invent a value for it. The whole
    /// value is compared through `Debug` because `ReconcileReport` has no
    /// `PartialEq`.
    #[test]
    #[allow(
        clippy::field_reassign_with_default,
        reason = "the point is which fields move off Default; naming them one by one says so"
    )]
    fn controller_report_names_only_the_counts_the_scheduler_keeps() {
        let tick = tick_of(4, 1, 2, 2, 1);
        let mut expected = ReconcileReport::default();
        expected.objects_examined = 6;
        // Counted the way every count is (T1.8): three landed writes.
        for _ in 0..3 {
            expected.record(engenho_controllers::Effect::answered(true));
        }
        expected.objects_skipped = 1;
        expected.note = Some(tick.to_string());
        assert_eq!(
            format!("{:?}", tick.controller_report()),
            format!("{expected:?}")
        );
    }
}
