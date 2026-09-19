//! T2.5 — how a scheduler reads a pod, and the two writes it makes to one.
//!
//! ★ THE DEFECTS this module is the fix for, all in engenho-scheduler's tick
//! as this module was written (the scheduler adopts it in its own change;
//! until then they stand there):
//!
//!   * **The bind was unconditional.** `spec.nodeName` was merged in with no
//!     precondition, on a pod read at the top of the tick. A pod deleted,
//!     rebound or re-specified in between was bound anyway, onto a node
//!     chosen for the pod as it was.
//!   * **A bind was counted by being proposed.** The report listed a
//!     binding and the ledger was debited whatever the store answered.
//!   * **"Pending" was one bit** (`spec.nodeName` empty), so a Terminating
//!     pod, or one naming another scheduler, was placed like any other.
//!   * **The Unschedulable condition was rewritten from a stale read**, and
//!     the rewrite replaced the whole `conditions` list with one entry.
//!
//! ★ THE SHAPE.
//!
//!   * [`PodSchedulingState::of`] classifies a pod into one of five named
//!     states. Only [`PodSchedulingState::Schedulable`] carries a
//!     [`Schedulable`]: the pod's key and the revision it was read at. Its
//!     fields are private and nothing else builds one.
//!   * [`bind_cas`] takes a `Schedulable` BY VALUE and writes
//!     `spec.nodeName` conditioned on its revision. A pod never classified
//!     schedulable cannot be bound (there is no token to pass), and one
//!     token binds at most once.
//!   * [`BindOutcome::of`] maps every `ResourceOp` with no wildcard, so a new
//!     op does not compile until it is placed.
//!   * [`Binding`] has private fields and is built only in the `Patched` arm
//!     of that mapping: a binding in a report is a bind the store confirmed.
//!   * [`mark_unschedulable_cas`] sets `PodScheduled=False/Unschedulable` by
//!     type ([`crate::condition::upsert_condition`]) with the one-retry CAS
//!     edit, and stands down when the re-read pod is no longer schedulable.
//!
//! Tier-honest: "binding needs a token" and "a Binding needs a `Patched`
//! answer" are type-level (no constructor). That the scheduler debits its
//! ledger only for a [`BindOutcome::Bound`] will be held at its one call
//! site. `tests/t2_5_condition_cas.rs` drives [`mark_unschedulable_cas`]
//! under a `WatchDriver`: at most 3 ticks in 2 s for one unschedulable pod.

use engenho_store::{command::ResourceOp, resource::ResourceKey, revision::Revision};
use serde_json::{Value, json};

use crate::condition::{ConditionStatus, DesiredCondition, upsert_condition};
use crate::effect::{Effect, Refusal};
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::status::{CasEnv, StatusEdit, StatusEditOutcome, edit_status_cas, resource_version_of};

/// The scheduler a pod names when it names none: the apiserver defaults
/// `spec.schedulerName` to this.
pub const DEFAULT_SCHEDULER: &str = "default-scheduler";

/// The pod condition type a scheduler owns.
pub const POD_SCHEDULED: &str = "PodScheduled";

/// The `PodScheduled=False` reason for a pod no node admits.
pub const UNSCHEDULABLE: &str = "Unschedulable";

/// How a pod stands, for a scheduler named `scheduler`.
///
/// The states are checked in the order declared: a bound pod is `Bound`
/// even while Terminating. Every state but `Bound` is an unbound, that is
/// Pending, pod.
#[derive(Debug, PartialEq, Eq)]
pub enum PodSchedulingState {
    /// `spec.nodeName` names a node. Nothing to schedule.
    Bound,
    /// Unbound, and a delete was accepted (`metadata.deletionTimestamp`).
    /// Placing it would start a pod nobody wants.
    Terminating,
    /// Unbound, and `spec.schedulerName` names another scheduler, or holds
    /// something that is not a name.
    OtherScheduler,
    /// Unbound and ours, with no parseable `metadata.resourceVersion`: a
    /// bind could not be conditioned on the pod as read.
    NoRevision,
    /// Unbound, alive, ours, and read at a known revision.
    Schedulable(Schedulable),
}

impl PodSchedulingState {
    /// Classify the pod `pod` stored at `key`.
    #[must_use]
    pub fn of(key: &ResourceKey, pod: &Value, scheduler: &str) -> Self {
        if is_bound(pod) {
            Self::Bound
        } else if pod.is_terminating() {
            Self::Terminating
        } else if !names_scheduler(pod, scheduler) {
            Self::OtherScheduler
        } else {
            match resource_version_of(pod) {
                None => Self::NoRevision,
                Some(revision) => Self::Schedulable(Schedulable {
                    key: key.clone(),
                    revision,
                }),
            }
        }
    }

    /// Whether the pod is unbound (Pending), whatever else stands in the
    /// way of placing it.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        !matches!(self, Self::Bound)
    }
}

/// `spec.nodeName` is a non-empty string.
fn is_bound(pod: &Value) -> bool {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .is_some_and(|node| !node.is_empty())
}

/// `spec.schedulerName` is `scheduler`, or unset (absent, `null` or empty),
/// which the apiserver defaults to [`DEFAULT_SCHEDULER`].
fn names_scheduler(pod: &Value, scheduler: &str) -> bool {
    match pod.pointer("/spec/schedulerName") {
        None | Some(Value::Null) => scheduler == DEFAULT_SCHEDULER,
        Some(Value::String(name)) if name.is_empty() => scheduler == DEFAULT_SCHEDULER,
        Some(Value::String(name)) => name == scheduler,
        Some(_) => false,
    }
}

/// A pod classified [`PodSchedulingState::Schedulable`]: its key and the
/// revision it was read at.
///
/// The fields are private: only [`PodSchedulingState::of`] builds one.
///
/// ```
/// use engenho_controllers::pod_scheduling::{DEFAULT_SCHEDULER, PodSchedulingState};
/// use engenho_store::{ResourceKey, Revision};
///
/// let key = ResourceKey::namespaced("", "v1", "Pod", "ns", "p");
/// let read = serde_json::json!({"metadata": {"resourceVersion": "7"}});
/// let PodSchedulingState::Schedulable(pod) =
///     PodSchedulingState::of(&key, &read, DEFAULT_SCHEDULER)
/// else {
///     unreachable!("an unbound pod of ours with a revision");
/// };
/// assert_eq!(pod.revision(), Revision(7));
/// ```
///
/// and never assembled by hand:
///
/// ```compile_fail,E0451
/// use engenho_controllers::pod_scheduling::Schedulable;
/// use engenho_store::{ResourceKey, Revision};
///
/// let pod = Schedulable {
///     key: ResourceKey::namespaced("", "v1", "Pod", "ns", "p"),
///     revision: Revision(7),
/// };
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct Schedulable {
    key: ResourceKey,
    revision: Revision,
}

impl Schedulable {
    /// The pod's key.
    #[must_use]
    pub fn key(&self) -> &ResourceKey {
        &self.key
    }

    /// The revision the pod was read at, which a bind is conditioned on.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }
}

/// A bind the store confirmed: `spec.nodeName` of `pod_key` was set to
/// `node_name`.
///
/// The fields are private: only [`BindOutcome::of`] builds one, from a
/// `Patched` answer.
///
/// ```
/// use engenho_controllers::pod_scheduling::{BindOutcome, DEFAULT_SCHEDULER, PodSchedulingState};
/// use engenho_store::{ResourceKey, command::ResourceOp};
///
/// let key = ResourceKey::namespaced("", "v1", "Pod", "ns", "p");
/// let read = serde_json::json!({"metadata": {"resourceVersion": "7"}});
/// let PodSchedulingState::Schedulable(pod) =
///     PodSchedulingState::of(&key, &read, DEFAULT_SCHEDULER)
/// else {
///     unreachable!("an unbound pod of ours with a revision");
/// };
/// let BindOutcome::Bound(bound) = BindOutcome::of(ResourceOp::Patched, pod, "n1".to_owned())
/// else {
///     unreachable!("Patched is a bind");
/// };
/// assert_eq!(bound.node_name(), "n1");
/// ```
///
/// and never assembled by hand:
///
/// ```compile_fail,E0451
/// use engenho_controllers::pod_scheduling::Binding;
/// use engenho_store::ResourceKey;
///
/// let bound = Binding {
///     pod_key: ResourceKey::namespaced("", "v1", "Pod", "ns", "p"),
///     node_name: "n1".to_owned(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pod_key: ResourceKey,
    node_name: String,
}

impl Binding {
    /// The pod that was bound.
    #[must_use]
    pub fn pod_key(&self) -> &ResourceKey {
        &self.pod_key
    }

    /// The node it was bound to.
    #[must_use]
    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

/// What the store did with a bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindOutcome {
    /// The bind landed.
    Bound(Binding),
    /// The store refused it. `Conflict`: the pod moved (or went away) since
    /// it was read; the write that moved it fires a watch event, and the
    /// next tick classifies the pod afresh.
    Refused(Refusal),
    /// The store accepted the patch and changed nothing.
    Unchanged,
    /// The store answered with an op a patch does not produce. Not a bind.
    Unexpected(ResourceOp),
}

impl BindOutcome {
    /// The outcome of binding `pod` to `node_name`, read off the store's
    /// answer `op`.
    ///
    /// Exhaustive with no wildcard: a new [`ResourceOp`] does not compile
    /// until it is placed here.
    #[must_use]
    pub fn of(op: ResourceOp, pod: Schedulable, node_name: String) -> Self {
        match op {
            ResourceOp::Patched => Self::Bound(Binding {
                pod_key: pod.key,
                node_name,
            }),
            ResourceOp::Conflict => Self::Refused(Refusal::Conflict),
            ResourceOp::PatchRejected => Self::Refused(Refusal::PatchRejected),
            ResourceOp::ApplyConflict => Self::Refused(Refusal::ApplyConflict),
            ResourceOp::Unchanged | ResourceOp::NoOp => Self::Unchanged,
            ResourceOp::Created
            | ResourceOp::Replaced
            | ResourceOp::Deleted
            | ResourceOp::DeletionPending => Self::Unexpected(op),
        }
    }

    /// What the bind did, as one [`Effect`], for
    /// [`crate::ReconcileReport::record`].
    pub const fn effect(&self) -> Effect {
        match self {
            Self::Bound(_) => Effect::of(ResourceOp::Patched),
            Self::Refused(refusal) => Effect::Rejected(*refusal),
            Self::Unchanged => Effect::Unchanged,
            Self::Unexpected(op) => Effect::of(*op),
        }
    }
}

/// Bind `pod` to `node_name`: merge `spec.nodeName`, conditioned on the
/// revision `pod` was classified at.
///
/// Not retried: a conflict means the pod changed, and the node was chosen
/// for the pod as it was.
///
/// # Errors
///
/// [`ControllerError::Store`] on a transport failure. A refused bind is a
/// [`BindOutcome::Refused`], not an error.
pub async fn bind_cas<E>(
    env: &E,
    pod: Schedulable,
    node_name: String,
) -> Result<BindOutcome, ControllerError>
where
    E: CasEnv + ?Sized,
{
    let answer = env
        .patch_at(
            &pod.key,
            json!({ "spec": { "nodeName": node_name } }),
            pod.revision,
        )
        .await?;
    Ok(BindOutcome::of(answer.op, pod, node_name))
}

/// The condition a pod no node admits carries, with `message` saying why.
#[must_use]
pub fn unschedulable_condition(message: String) -> DesiredCondition {
    DesiredCondition {
        condition_type: POD_SCHEDULED,
        status: ConditionStatus::False,
        reason: UNSCHEDULABLE,
        message,
    }
}

/// Give `pod` a `PodScheduled=False / reason=Unschedulable` condition
/// carrying `message`, and `status.phase: Pending` when it has no phase
/// (engenho's apiserver does not default one). Mirrors upstream
/// kube-scheduler's failure path; the pod stays unbound.
///
/// Through [`edit_status_cas`]: the pod is re-read, the condition set by
/// type, and the result written at the revision read, retrying once on a
/// conflict. `Unchanged` when the pod already carries the condition and a
/// phase, so a scheduler that re-derives the same reason every tick
/// proposes nothing and does not wake itself. `Superseded` when the re-read
/// pod is no longer ours to mark: bound, Terminating, or claimed by another
/// scheduler since it was classified.
///
/// Emit the `FailedScheduling` Event only when [`StatusEditOutcome::changed`].
///
/// # Errors
///
/// As [`edit_status_cas`].
pub async fn mark_unschedulable_cas<E>(
    env: &E,
    pod: &Schedulable,
    scheduler: &str,
    message: String,
    now: &str,
) -> Result<StatusEditOutcome, ControllerError>
where
    E: CasEnv + ?Sized,
{
    let desired = unschedulable_condition(message);
    edit_status_cas(env, &pod.key, |live| {
        if !matches!(
            PodSchedulingState::of(&pod.key, live, scheduler),
            PodSchedulingState::Schedulable(_) | PodSchedulingState::NoRevision
        ) {
            return Ok(StatusEdit::Superseded);
        }
        let mut fields = upsert_condition(live, &desired, now)?
            .into_status_fields()
            .unwrap_or_default();
        if live.pointer("/status/phase").is_none_or(Value::is_null) {
            fields.insert("phase".to_owned(), json!("Pending"));
        }
        Ok(if fields.is_empty() {
            StatusEdit::Current
        } else {
            StatusEdit::Write(fields)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "ns", "p")
    }

    fn state(pod: &Value) -> PodSchedulingState {
        PodSchedulingState::of(&key(), pod, DEFAULT_SCHEDULER)
    }

    fn schedulable(revision: u64) -> Schedulable {
        Schedulable {
            key: key(),
            revision: Revision(revision),
        }
    }

    #[test]
    fn a_pod_with_a_node_is_bound_even_while_terminating() {
        let pod = json!({"metadata": {"resourceVersion": "3", "deletionTimestamp": "t"},
                         "spec": {"nodeName": "n1"}});
        assert_eq!(state(&pod), PodSchedulingState::Bound);
        assert!(!state(&pod).is_pending());
    }

    #[test]
    fn an_empty_node_name_is_unbound() {
        let pod = json!({"metadata": {"resourceVersion": "3"}, "spec": {"nodeName": ""}});
        assert_eq!(state(&pod), PodSchedulingState::Schedulable(schedulable(3)));
    }

    #[test]
    fn an_unbound_pod_being_deleted_is_terminating() {
        let pod = json!({"metadata": {"resourceVersion": "3", "deletionTimestamp": "t"}});
        assert_eq!(state(&pod), PodSchedulingState::Terminating);
        assert!(state(&pod).is_pending());
    }

    #[test]
    fn another_scheduler_s_pod_is_not_ours() {
        for name in [json!("custom"), json!(7)] {
            let pod = json!({"metadata": {"resourceVersion": "3"},
                             "spec": {"schedulerName": name}});
            assert_eq!(state(&pod), PodSchedulingState::OtherScheduler, "{name}");
        }
    }

    #[test]
    fn an_unset_scheduler_name_is_the_default_scheduler() {
        for spec in [
            json!({}),
            json!({"schedulerName": null}),
            json!({"schedulerName": ""}),
            json!({"schedulerName": DEFAULT_SCHEDULER}),
        ] {
            let pod = json!({"metadata": {"resourceVersion": "3"}, "spec": spec});
            assert_eq!(
                state(&pod),
                PodSchedulingState::Schedulable(schedulable(3)),
                "{spec}"
            );
            // ...and not a pod a scheduler with another name places.
            assert_eq!(
                PodSchedulingState::of(&key(), &pod, "custom"),
                PodSchedulingState::OtherScheduler,
                "{spec}"
            );
        }
    }

    #[test]
    fn a_pod_with_no_revision_cannot_be_scheduled() {
        assert_eq!(state(&json!({"spec": {}})), PodSchedulingState::NoRevision);
        assert_eq!(
            state(&json!({"metadata": {"resourceVersion": "x"}})),
            PodSchedulingState::NoRevision
        );
    }

    #[test]
    fn a_schedulable_pod_carries_its_key_and_revision() {
        let PodSchedulingState::Schedulable(pod) =
            state(&json!({"metadata": {"resourceVersion": "42"}}))
        else {
            panic!("expected Schedulable");
        };
        assert_eq!(pod.key(), &key());
        assert_eq!(pod.revision(), Revision(42));
    }

    /// Every op, and what it means for a bind. Only `Patched` binds.
    #[test]
    fn only_a_patched_answer_is_a_binding() {
        let expected = [
            (
                ResourceOp::Created,
                BindOutcome::Unexpected(ResourceOp::Created),
            ),
            (
                ResourceOp::Replaced,
                BindOutcome::Unexpected(ResourceOp::Replaced),
            ),
            (
                ResourceOp::Patched,
                BindOutcome::Bound(Binding {
                    pod_key: key(),
                    node_name: "n1".to_owned(),
                }),
            ),
            (
                ResourceOp::Deleted,
                BindOutcome::Unexpected(ResourceOp::Deleted),
            ),
            (
                ResourceOp::DeletionPending,
                BindOutcome::Unexpected(ResourceOp::DeletionPending),
            ),
            (ResourceOp::NoOp, BindOutcome::Unchanged),
            (ResourceOp::Unchanged, BindOutcome::Unchanged),
            (
                ResourceOp::Conflict,
                BindOutcome::Refused(Refusal::Conflict),
            ),
            (
                ResourceOp::PatchRejected,
                BindOutcome::Refused(Refusal::PatchRejected),
            ),
            (
                ResourceOp::ApplyConflict,
                BindOutcome::Refused(Refusal::ApplyConflict),
            ),
        ];
        for (op, outcome) in expected {
            assert_eq!(
                BindOutcome::of(op, schedulable(1), "n1".to_owned()),
                outcome,
                "{op:?}"
            );
            assert_eq!(
                outcome.effect(),
                Effect::of(op),
                "{op:?}: a bind counts as what the store did"
            );
        }
    }

    #[test]
    fn a_binding_names_the_pod_and_the_node() {
        let BindOutcome::Bound(binding) =
            BindOutcome::of(ResourceOp::Patched, schedulable(1), "n1".to_owned())
        else {
            panic!("expected Bound");
        };
        assert_eq!(binding.pod_key(), &key());
        assert_eq!(binding.node_name(), "n1");
    }

    #[test]
    fn the_unschedulable_condition_is_pod_scheduled_false() {
        let c = unschedulable_condition("0/1 nodes".to_owned());
        assert_eq!(
            (c.condition_type, c.status, c.reason, c.message.as_str()),
            (
                "PodScheduled",
                ConditionStatus::False,
                "Unschedulable",
                "0/1 nodes"
            )
        );
    }
}
