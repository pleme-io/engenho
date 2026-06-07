//! `GcController` — orphan-reference garbage collector.
//!
//! K8s rule: any resource with `metadata.ownerReferences[].controller=true`
//! pointing at a non-existent UID is an orphan + must be deleted
//! (matches kube-controller-manager's garbage-collector behavior).
//!
//! R9.7 implementation: scans Pod + ReplicaSet kinds (the ones
//! produced by our R8 + R9 controllers). Future R9.7b can extend
//! to arbitrary kinds via a per-kind registry — same shape, just
//! more entries in the scan list.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
};
use serde_json::Value;
use tracing::debug;

use crate::controller::{Controller, ReconcileReport};
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

    /// Returns the controller-owner's UID if `child` has one.
    fn owner_uid(child: &Value) -> Option<String> {
        controlling_owner(child).map(|o| o.uid)
    }
}

#[async_trait]
impl Controller for GcController {
    fn name(&self) -> &'static str {
        "gc"
    }

    async fn tick(&self) -> Result<ReconcileReport, ControllerError> {
        let mut report = ReconcileReport::default();
        let ns = self.namespace.as_deref();

        // Build the set of existing parent UIDs (anything we'd
        // potentially own). We're conservative and gather UIDs
        // from all kinds we know controllers create:
        //   - Deployment (parents of ReplicaSet)
        //   - ReplicaSet (parents of Pod)
        // Plus any other kind a future R9.x might add — extend
        // this list in lockstep.
        let mut known_uids: HashSet<String> = HashSet::new();
        for (group, version, kind) in [("apps", "v1", "Deployment"), ("apps", "v1", "ReplicaSet")] {
            for (_, obj) in self.store.list(group, version, kind, ns).await {
                if let Some(uid) = obj
                    .get("metadata")
                    .and_then(|m| m.get("uid"))
                    .and_then(|u| u.as_str())
                {
                    known_uids.insert(uid.to_string());
                }
            }
        }

        // Scan children. For each, if it has a controlling
        // ownerRef whose UID is NOT in known_uids, delete it.
        for (group, version, kind) in [("", "v1", "Pod"), ("apps", "v1", "ReplicaSet")] {
            let children = self.store.list(group, version, kind, ns).await;
            report.objects_examined += children.len();
            for (key, value) in children {
                let Some(uid) = Self::owner_uid(&value) else {
                    // No controller-owner — not our problem.
                    continue;
                };
                if known_uids.contains(&uid) {
                    continue;
                }
                debug!(
                    child = %key.label(),
                    orphan_uid = %uid,
                    "deleting orphan"
                );
                self.store
                    .propose(ResourceCommand::Delete {
                        key,
                        expected: None,
                        reason: Reason::GarbageCollector,
                    })
                    .await
                    .map_err(|e| ControllerError::Store(e.to_string()))?;
                report.objects_changed += 1;
            }
        }
        Ok(report)
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
        set_owner_reference(&mut pod, owner_ref("uid-123"));
        assert_eq!(GcController::owner_uid(&pod), Some("uid-123".into()));
    }

    #[test]
    fn owner_uid_none_without_owner_ref() {
        let pod = json!({"metadata": {"name": "p"}});
        assert!(GcController::owner_uid(&pod).is_none());
    }
}
