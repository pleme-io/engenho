//! NODE HEARTBEATS — the Lease a node renews to prove it is alive.
//!
//! ★ WHY THIS EXISTS. A Node was registered once at boot with a hardcoded
//! `[{type: Ready, status: True}]` and nothing ever updated it again. The
//! `kube-node-lease` namespace was seeded and `coordination.k8s.io` was
//! served, but no Lease was ever written or renewed. The consequence is
//! the worst kind of monitoring failure: **a node that has crashed,
//! partitioned, or been powered off reports `Ready` forever.** A scheduler
//! keeps placing pods on it; `kubectl get nodes` shows a healthy cluster;
//! nothing ever transitions to `NotReady`.
//!
//! ★ WHY A LEASE AND NOT A STATUS WRITE. Upstream moved node heartbeats to
//! `coordination.k8s.io/v1 Lease` precisely because writing
//! `Node.status` every few seconds is expensive — the object is large,
//! every write wakes every Node watcher, and on a big cluster that traffic
//! dominates the apiserver. A Lease is a tiny object whose only job is to
//! carry `renewTime`. engenho gets the same property for the same reason,
//! and gets it in the shape every existing tool already reads.
//!
//! ★ LIVENESS IS DERIVED, NEVER ASSERTED. `Ready` is computed from the
//! lease's age against a grace period. Nothing writes "I am healthy" —
//! a node proves it by renewing, and stops proving it by failing to. That
//! inversion is the entire point: an asserted condition survives the death
//! of whatever asserted it, which is exactly the bug being fixed here.
//!
//! ★ CLOCK-INJECTED AND PURE, so the whole grace-period curve is tested
//! without sleeping.

use std::time::Duration;

use engenho_store::{ResourceKey, ResourceValue};
use serde_json::json;

/// How often a node renews its lease. Upstream's default.
pub const RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// How stale a lease may get before the node is judged `NotReady`.
///
/// Upstream's default is 40s — four missed renewals. Deliberately several
/// intervals, not one: a single missed renewal is a hiccup, and flapping a
/// node to `NotReady` on one slow tick would evict workloads for nothing.
pub const GRACE_PERIOD: Duration = Duration::from_secs(40);

/// The namespace node leases live in.
pub const LEASE_NAMESPACE: &str = "kube-node-lease";

/// Build the `Lease` object for a node.
///
/// `holder` is the node name — upstream sets `holderIdentity` to it, which
/// is what makes the lease attributable when several exist.
#[must_use]
pub fn lease_value(node: &str, renew_time: &str, transitions: u64) -> ResourceValue {
    json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": { "name": node, "namespace": LEASE_NAMESPACE },
        "spec": {
            "holderIdentity": node,
            // Seconds, not the Duration — upstream's field is an int32 of
            // seconds and a client comparing against it would misread a
            // millisecond value by three orders of magnitude.
            "leaseDurationSeconds": GRACE_PERIOD.as_secs(),
            "renewTime": renew_time,
            "leaseTransitions": transitions,
        }
    })
}

/// The store key for a node's lease.
#[must_use]
pub fn lease_key(node: &str) -> ResourceKey {
    ResourceKey::namespaced("coordination.k8s.io", "v1", "Lease", LEASE_NAMESPACE, node)
}

/// A node's readiness, DERIVED from how stale its heartbeat is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeReadiness {
    /// Renewed within the grace period.
    Ready,
    /// The heartbeat is older than the grace period.
    NotReady,
    /// No lease has ever been observed.
    ///
    /// Distinct from `NotReady` on purpose: a node that has never
    /// heartbeat is mid-registration, while one that HAS and then stopped
    /// has failed. Collapsing them would make a booting node look like a
    /// dying one, and every autoscaler treats those differently.
    Unknown,
}

impl NodeReadiness {
    /// The `status` string of the `Ready` condition.
    #[must_use]
    pub fn condition_status(self) -> &'static str {
        match self {
            Self::Ready => "True",
            Self::NotReady => "False",
            // Upstream's third value. Not "False": a node whose state
            // cannot be determined is not the same as one known to be bad.
            Self::Unknown => "Unknown",
        }
    }

    /// The `reason` upstream pairs with the condition.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::Ready => "KubeletReady",
            Self::NotReady => "NodeStatusUnknown",
            Self::Unknown => "NodeStatusNeverUpdated",
        }
    }
}

/// Judge a node from the age of its last heartbeat.
///
/// `since_renew` is `None` when no lease has ever been seen.
#[must_use]
pub fn readiness(since_renew: Option<Duration>) -> NodeReadiness {
    match since_renew {
        None => NodeReadiness::Unknown,
        Some(age) if age <= GRACE_PERIOD => NodeReadiness::Ready,
        Some(_) => NodeReadiness::NotReady,
    }
}

/// The `Ready` condition to publish on `Node.status`.
#[must_use]
pub fn ready_condition(state: NodeReadiness, now: &str) -> ResourceValue {
    json!({
        "type": "Ready",
        "status": state.condition_status(),
        "reason": state.reason(),
        "message": match state {
            NodeReadiness::Ready => "kubelet is posting ready status",
            NodeReadiness::NotReady => "Kubelet stopped posting node status",
            NodeReadiness::Unknown => "Kubelet never posted node status",
        },
        "lastHeartbeatTime": now,
        "lastTransitionTime": now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: fn(u64) -> Duration = Duration::from_secs;

    #[test]
    fn a_node_that_stops_renewing_becomes_not_ready() {
        // The whole bug: before this, a powered-off node reported Ready
        // forever and the scheduler kept placing pods on it.
        assert_eq!(readiness(Some(S(0))), NodeReadiness::Ready);
        assert_eq!(readiness(Some(GRACE_PERIOD)), NodeReadiness::Ready);
        assert_eq!(
            readiness(Some(GRACE_PERIOD + S(1))),
            NodeReadiness::NotReady
        );
    }

    #[test]
    fn the_grace_period_is_several_intervals_not_one() {
        // A single missed renewal is a hiccup. Flapping to NotReady on one
        // slow tick would evict workloads for nothing.
        assert!(
            GRACE_PERIOD >= RENEW_INTERVAL * 3,
            "grace must tolerate several missed renewals"
        );
        assert_eq!(readiness(Some(RENEW_INTERVAL * 2)), NodeReadiness::Ready);
    }

    #[test]
    fn never_heartbeat_is_distinct_from_stopped_heartbeating() {
        // A booting node must not look like a dying one — every autoscaler
        // treats those differently.
        assert_eq!(readiness(None), NodeReadiness::Unknown);
        assert_eq!(NodeReadiness::Unknown.condition_status(), "Unknown");
        assert_eq!(NodeReadiness::NotReady.condition_status(), "False");
        assert_ne!(
            NodeReadiness::Unknown.reason(),
            NodeReadiness::NotReady.reason()
        );
    }

    #[test]
    fn the_condition_carries_what_kubectl_describe_node_prints() {
        let c = ready_condition(NodeReadiness::Ready, "2026-08-29T21:00:00Z");
        assert_eq!(c["type"], "Ready");
        assert_eq!(c["status"], "True");
        assert_eq!(c["reason"], "KubeletReady");
        assert_eq!(c["lastHeartbeatTime"], "2026-08-29T21:00:00Z");
    }

    #[test]
    fn lease_duration_is_seconds_because_upstreams_field_is() {
        // A millisecond value here would be misread by three orders of
        // magnitude by anything comparing against it.
        let v = lease_value("cid", "2026-08-29T21:00:00Z", 0);
        assert_eq!(v["spec"]["leaseDurationSeconds"], 40);
        assert_eq!(v["spec"]["holderIdentity"], "cid");
        assert_eq!(v["metadata"]["namespace"], LEASE_NAMESPACE);
        assert_eq!(v["apiVersion"], "coordination.k8s.io/v1");
    }

    #[test]
    fn the_lease_lands_where_every_tool_looks_for_it() {
        let k = lease_key("cid");
        assert_eq!(k.group, "coordination.k8s.io");
        assert_eq!(k.kind, "Lease");
        assert_eq!(k.namespace.as_deref(), Some("kube-node-lease"));
        assert_eq!(k.name, "cid");
    }
}
