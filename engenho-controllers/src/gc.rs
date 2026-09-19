//! `GcController` — orphan-reference garbage collector.
//!
//! K8s rule: any resource with `metadata.ownerReferences[].controller=true`
//! pointing at a non-existent owner is an orphan and must be deleted
//! (matches kube-controller-manager's garbage collector).
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

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
};
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::owner::controlling_owner;

pub struct GcController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
}

impl GcController {
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, namespace: Option<String>) -> Self {
        Self { store, namespace }
    }
}

/// What a live lookup of a dependent's controller actually established.
///
/// Three outcomes, never two: "I could not resolve this" is its own state
/// and is NOT rounded into "absent". Collapsing them is the defect this
/// enum exists to make unrepresentable — a missing arm is a compile error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerPresence {
    /// The owner exists at its own coordinates with the recorded uid.
    Alive,
    /// Positively observed as gone, or present under a different uid.
    Absent,
    /// The ownerReference could not be turned into coordinates to look up
    /// (an unparseable `apiVersion`). Never a reason to delete.
    Unresolvable,
}

impl GcController {
    /// Resolve a dependent's controller by ITS OWN apiVersion + kind.
    ///
    /// A namespaced dependent's owner is either in the same namespace or
    /// cluster-scoped, so both are tried before anything is called absent.
    async fn owner_presence(
        &self,
        child: &ResourceKey,
        owner: &crate::owner::OwnerReference,
    ) -> OwnerPresence {
        let Some((group, version)) = split_api_version(&owner.api_version) else {
            return OwnerPresence::Unresolvable;
        };

        let mut candidates: Vec<ResourceKey> = Vec::new();
        if let Some(ns) = child.namespace.as_deref() {
            candidates.push(ResourceKey::namespaced(
                group.clone(),
                version.clone(),
                owner.kind.clone(),
                ns.to_string(),
                owner.name.clone(),
            ));
        }
        candidates.push(ResourceKey::cluster_scoped(
            group,
            version,
            owner.kind.clone(),
            owner.name.clone(),
        ));

        for key in &candidates {
            if let Some(found) = self.store.get(key).await {
                return if uid_of(&found).as_deref() == Some(owner.uid.as_str()) {
                    OwnerPresence::Alive
                } else {
                    // Same coordinates, different object: the owner was
                    // recreated and this dependent belongs to the dead one.
                    OwnerPresence::Absent
                };
            }
        }
        OwnerPresence::Absent
    }
}

/// `"apps/v1"` -> `("apps", "v1")`; `"v1"` -> `("", "v1")`.
///
/// Anything else is unresolvable rather than guessed — a wrong guess here
/// deletes a live workload.
fn split_api_version(api_version: &str) -> Option<(String, String)> {
    match api_version.split_once('/') {
        Some((g, v)) if !g.is_empty() && !v.is_empty() => Some((g.to_string(), v.to_string())),
        Some(_) => None,
        None if !api_version.is_empty() => Some((String::new(), api_version.to_string())),
        None => None,
    }
}

fn uid_of(value: &serde_json::Value) -> Option<String> {
    value
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .map(ToString::to_string)
}

#[async_trait]
impl Controller for GcController {
    fn name(&self) -> &'static str {
        "gc"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let ns = self.namespace.as_deref();

        // Build the set of existing parent UIDs (anything we'd
        // potentially own). We're conservative and gather UIDs
        // from all kinds we know controllers create:
        //   - Deployment (parents of ReplicaSet)
        //   - ReplicaSet (parents of Pod)
        // Plus any other kind a future R9.x might add — extend
        // this list in lockstep.
        for (group, version, kind) in [("", "v1", "Pod"), ("apps", "v1", "ReplicaSet")] {
            let children = self.store.list(group, version, kind, ns).await;
            report.objects_examined += children.len();
            for (key, value) in children {
                let Some(owner) = controlling_owner(&value) else {
                    continue;
                };
                match self.owner_presence(&key, &owner).await {
                    OwnerPresence::Alive | OwnerPresence::Unresolvable => continue,
                    OwnerPresence::Absent => {}
                }
                debug!(
                    child = %key.label(),
                    orphan_uid = %owner.uid,
                    owner_kind = %owner.kind,
                    "deleting orphan"
                );
                let applied = self
                    .store
                    .propose(ResourceCommand::delete(key, Reason::GarbageCollector))
                    .await?;
                // A finalizer-bearing orphan with no timestamp to stamp,
                // or one already gone, is a NoOp: no change to count.
                report.record(Effect::of(applied.op));
            }
        }
        Ok(report.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::owner::{OwnerReference, set_owner_reference};

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
