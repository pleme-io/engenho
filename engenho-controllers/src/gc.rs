//! `GcController` — orphan-reference garbage collector.
//!
//! K8s rule: a dependent is deleted when NONE of its
//! `metadata.ownerReferences` names a solid owner (kube-controller-manager's
//! garbage collector). References to owners that are gone are removed from a
//! dependent that still has a live one.
//!
//! ## The owner is resolved from the ownerReference, never from a list
//!
//! This controller used to build a set of live UIDs by listing TWO
//! hardcoded kinds — Deployment and ReplicaSet — and delete any Pod whose
//! controller UID was not in it. A StatefulSet's Pod is therefore an
//! "orphan" on every single tick.
//!
//! Measured on ryn 2026-09-18: `pitr-lab/mysql-0` (owned by the
//! StatefulSet `pitr-lab/mysql`) was deleted ~10 times per SECOND for
//! days. The statefulset controller recreated it, the scheduler bound it,
//! gc deleted it, forever — three controllers at full tilt, `changed=1`
//! on every tick of each, and 14,306 log lines about one pod. The churn
//! was also the event storm that overflowed the kubelet's watch buffer,
//! which is what exposed the driver defect fixed in `watch_driver.rs`.
//!
//! The old doc comment called the fix "more entries in the scan list".
//! That is the wrong destination: the next owner kind would have re-armed
//! the same trap. Upstream does not keep a list — `garbagecollector`
//! resolves an ownerReference through the RESTMapper and does a live GET
//! of THAT owner, and it deletes only against a confirmed-absent owner
//! (`absentOwnerCache`). So do we, and the hardcoded enumeration is gone
//! rather than extended.
//!
//! **Fail closed.** Not knowing an owner's kind is not evidence the owner
//! is missing. A dependent is deleted only when a positive observation
//! says so: the owner is absent at its own coordinates, or an object with
//! that name exists under a DIFFERENT uid (the owner was recreated, so
//! this dependent belongs to a dead generation).
//!
//! ## Every owner reference, classified (I19 GC oracle)
//!
//! Checked row by row against upstream's garbage collector (v1.34.0,
//! `tests/oracle_gc.rs`). Three things the controlling-owner-only reading
//! got wrong, each a way to delete a dependent upstream keeps:
//!
//! - **Only the controller was consulted.** Upstream classifies EVERY
//!   ownerReference and deletes only when none is solid; with some solid
//!   and some dangling it patches the dangling ones out and keeps the
//!   object. A pod whose controller is gone but whose other owner lives is
//!   kept now, and the dead reference is removed.
//! - **An unserved apiVersion read as Absent.** `extensions/v1beta1`
//!   Deployment for an owner stored as `apps/v1` was looked up at a key
//!   nothing is written under, found missing, and the pod deleted.
//!   Resolution goes through the served catalog ([`ServedKinds`]), pinned
//!   to the reference's exact group/version; a miss is [`Unresolvable`] and
//!   the dependent is left alone.
//! - **The foreground finalizer was ignored.** An owner being deleted with
//!   `foregroundDeletion` waits for its dependents; it is not solid.
//!
//! The DEPENDENT side is still a scan of two kinds (Pods, `ReplicaSets`).
//! Widening it to every served kind is refused (`IMPROVEMENT-PLAN` §9): it
//! would put every kind's objects through this path on every tick.

mod collect;
mod served;

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    revision::Revision,
};
use serde_json::{Value, json};
use tracing::debug;

pub use collect::{
    Candidate, Classification, Classified, Collected, Decision, FOREGROUND_DELETION, GcEnv,
    OwnerState, classify, collect, owner_references, owner_state,
};
pub use served::{ServedKind, ServedKinds, Unresolvable};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::reads::{DeclaresReads, Reads};

/// The dependent kinds gc scans. Owners are resolved from each reference
/// through the served catalog, so this list does not bound which OWNERS
/// are recognised; it bounds which DEPENDENTS are collected.
const DEPENDENT_KINDS: [(&str, &str, &str); 2] = [("", "v1", "Pod"), ("apps", "v1", "ReplicaSet")];

pub struct GcController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl GcController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }

    /// Every kind served right now: the compiled-in catalog plus each
    /// stored CRD's served versions.
    async fn served(&self) -> ServedKinds {
        let crds = self
            .store
            .list(
                "apiextensions.k8s.io",
                "v1",
                "CustomResourceDefinition",
                None,
            )
            .await;
        ServedKinds::builtin().with_crds(crds.iter().map(|(_, crd)| crd))
    }
}

/// [`GcEnv`] over the store. Every write carries the revision it is pinned
/// to, and a delete carries its clock (T3.6).
struct StoreEnv<'a> {
    store: &'a StoreMesh,
}

#[async_trait]
impl GcEnv for StoreEnv<'_> {
    async fn get(&self, key: &ResourceKey) -> Option<Value> {
        self.store.get(key).await
    }

    async fn delete(&self, key: &ResourceKey, pinned: Revision) -> Result<Effect, ControllerError> {
        let applied = self
            .store
            .propose(ResourceCommand::delete_at(
                key.clone(),
                Some(pinned),
                Reason::GarbageCollector,
                Some(engenho_types::time::now_rfc3339_utc()),
            ))
            .await?;
        Ok(Effect::of(applied.op))
    }

    async fn replace_owner_references(
        &self,
        key: &ResourceKey,
        references: Vec<Value>,
        pinned: Revision,
    ) -> Result<Effect, ControllerError> {
        // An RFC 7396 merge of a list replaces the list: upstream's JSON
        // merge-patch form of this write, with its resourceVersion
        // precondition.
        let applied = self
            .store
            .propose(ResourceCommand::patch_cas(
                key.clone(),
                json!({"metadata": {"ownerReferences": references}}),
                Some(pinned),
                Reason::GarbageCollector,
            ))
            .await?;
        Ok(Effect::of(applied.op))
    }
}

/// Every kind: it looks an owner up by whatever kind the dependent's
/// owner reference names, which it cannot know ahead of time, so any event
/// may be the deletion that orphans something.
impl DeclaresReads for GcController {
    fn reads(&self) -> Reads {
        Reads::every()
    }
}

#[async_trait]
impl Controller for GcController {
    fn name(&self) -> &'static str {
        "gc"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let ns = self.namespace.as_deref();
        let served = self.served().await;
        let env = StoreEnv { store: &self.store };

        for (group, version, kind) in DEPENDENT_KINDS {
            let dependents = self.store.list(group, version, kind, ns).await;
            report.objects_examined += dependents.len();
            for (key, value) in dependents {
                let Some(candidate) = Candidate::listed(key, &value) else {
                    continue;
                };
                match collect(&env, &served, &candidate).await? {
                    Collected::Deleted(effect) => {
                        debug!(dependent = %candidate.key.label(), "deleted: no owner is solid");
                        report.record(effect);
                    }
                    Collected::ReferencesRemoved { uids, effect } => {
                        debug!(
                            dependent = %candidate.key.label(),
                            removed = ?uids,
                            "removed owner references that are not solid"
                        );
                        report.record(effect);
                    }
                    Collected::Unresolvable(why) => {
                        debug!(dependent = %candidate.key.label(), %why, "left alone");
                    }
                    Collected::BeingDeleted
                    | Collected::ItemGone
                    | Collected::NoOwners
                    | Collected::Unpinned
                    | Collected::Kept => {}
                }
            }
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::owner::{OwnerReference, controlling_owner, set_owner_reference};

    fn owner_ref(uid: &str) -> OwnerReference {
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "n".into(),
            uid: uid.into(),
            controller: true,
            block_owner_deletion: true,
        }
    }

    #[test]
    fn owner_uid_returns_controller_uid() {
        let mut pod = json!({"metadata": {"name": "p"}});
        set_owner_reference(&mut pod, owner_ref("uid-123")).unwrap();
        assert_eq!(
            controlling_owner(&pod).map(|o| o.uid),
            Some("uid-123".into())
        );
    }

    #[test]
    fn owner_uid_none_without_owner_ref() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(controlling_owner(&pod).map(|o| o.uid).is_none());
    }
}
