//! A Node as the scheduler is allowed to see it: its `Ready` condition
//! DERIVED from the node's Lease, never read from storage.
//!
//! ## Why a type
//!
//! Before T1.3d the strategy judged readiness from `status.conditions` as
//! stored, and it had two "assume schedulable" arms: a node with no
//! conditions, and a node with no `Ready` condition. The stored condition is
//! the one value the apiserver refuses to serve.
//! [`engenho_controllers::node_lease::project_ready_condition`] replaces it on
//! every Node read with the condition the node's Lease implies, because a
//! kubelet that wedges leaves `Ready=True` standing (on rio: three days, every
//! pod Pending, node Ready). So `kubectl get node` said `Unknown` while the
//! scheduler kept placing pods on the node. And a node nobody had ever heard
//! from was schedulable outright.
//!
//! [`ObservedNode`] has one constructor, [`ObservedNode::project`], and it runs
//! that same projection. The Filter stage ([`crate::filter`]) judges only
//! `ObservedNode`s, and its `NodeReady` plugin reads [`ObservedNode::is_ready`],
//! so no node reaches a strategy on the strength of a stored readiness.
//!
//! ## What the type does NOT guarantee
//!
//! It proves the projection RAN. It does not prove the projection ran against
//! the right Lease: [`crate::Scheduler`] looks the Lease up at one call site,
//! keyed by [`engenho_controllers::node_lease::lease_key`] just as the
//! apiserver's Node read path is. Tests pin that lookup. The type does not.

use engenho_controllers::node_lease::{find_ready_condition, project_ready_condition};
use serde_json::Value;

use crate::ledger::node_name_of;

/// A Node whose `Ready` condition was derived from its Lease.
///
/// The field is private and [`Self::project`] is the only constructor, so
/// every value of this type has been through the projection.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedNode {
    value: Value,
}

impl ObservedNode {
    /// Project `node`'s `Ready` condition from `lease`.
    ///
    /// `lease` is the node's Lease as read from the store, and `None` when it
    /// has none. A node that has never heartbeat reads `Unknown`, and so does
    /// one whose heartbeat is older than the grace period. Neither is ready.
    /// `now` stamps `lastHeartbeatTime`; the Lease's age is judged by
    /// [`engenho_controllers::node_lease::project_ready_condition`] itself.
    #[must_use]
    pub fn project(mut node: Value, lease: Option<&Value>, now: &str) -> Self {
        project_ready_condition(&mut node, lease, now);
        Self { value: node }
    }

    /// The projected Node, as the apiserver would serve it.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The node's `metadata.name`, if present and a string.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        node_name_of(&self.value)
    }

    /// The projected `Ready` condition's status is `"True"`.
    ///
    /// Nothing is assumed. `Unknown`, `False`, and a missing condition are all
    /// not ready. After the projection a JSON-object Node always carries a
    /// `Ready` condition, so "missing" happens only for a Node that is not an
    /// object.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        find_ready_condition(&self.value)
            .and_then(|c| c.get("status"))
            .and_then(Value::as_str)
            == Some("True")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_controllers::node_lease::lease_value;
    use serde_json::json;

    const NOW: &str = "2026-09-19T12:00:00Z";
    const LONG_AGO: &str = "2020-01-01T00:00:00Z";

    fn fresh_lease(node: &str) -> Value {
        lease_value(node, &engenho_types::time::now_rfc3339_utc(), 0)
    }

    fn stale_lease(node: &str) -> Value {
        lease_value(node, LONG_AGO, 0)
    }

    fn node_storing_ready(status: &str) -> Value {
        json!({
            "metadata": { "name": "n" },
            "status": { "conditions": [{ "type": "Ready", "status": status }] }
        })
    }

    #[test]
    fn a_stored_ready_true_does_not_survive_a_stale_lease() {
        // The rio failure: the kubelet wedged, the Node kept its last
        // published Ready=True. The scheduler must see what the apiserver
        // serves, which is Unknown.
        let n = ObservedNode::project(node_storing_ready("True"), Some(&stale_lease("n")), NOW);
        assert!(!n.is_ready(), "{:#}", n.value());
        let ready = find_ready_condition(n.value()).expect("projection writes Ready");
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], "NodeStatusUnknown");
    }

    #[test]
    fn no_lease_is_never_ready_whatever_is_stored() {
        for stored in [
            json!({ "metadata": { "name": "n" } }),
            json!({ "metadata": { "name": "n" }, "status": { "conditions": [] } }),
            json!({ "metadata": { "name": "n" },
                    "status": { "conditions": [{ "type": "MemoryPressure", "status": "False" }] } }),
            node_storing_ready("True"),
        ] {
            let n = ObservedNode::project(stored.clone(), None, NOW);
            assert!(!n.is_ready(), "stored {stored:#} read ready with no lease");
        }
    }

    #[test]
    fn a_fresh_lease_is_ready_whatever_is_stored() {
        // Readiness comes from the heartbeat. A node that registered with
        // Ready=Unknown (or with no condition at all) is ready as soon as its
        // lease is fresh, not only once the kubelet republishes the Node.
        for stored in [
            json!({ "metadata": { "name": "n" } }),
            node_storing_ready("Unknown"),
            node_storing_ready("True"),
        ] {
            let n = ObservedNode::project(stored.clone(), Some(&fresh_lease("n")), NOW);
            assert!(n.is_ready(), "stored {stored:#} with a fresh lease");
        }
    }

    #[test]
    fn a_node_that_is_not_an_object_is_not_ready() {
        // The projection writes nothing into a non-object, so there is no
        // Ready condition to find. Absence reads not-ready.
        let n = ObservedNode::project(json!("garbage"), Some(&fresh_lease("n")), NOW);
        assert!(!n.is_ready());
        assert_eq!(n.name(), None);
    }

    #[test]
    fn the_projection_keeps_the_node_name() {
        let n = ObservedNode::project(
            json!({ "metadata": { "name": "n" } }),
            Some(&fresh_lease("n")),
            NOW,
        );
        assert_eq!(n.name(), Some("n"));
    }
}
