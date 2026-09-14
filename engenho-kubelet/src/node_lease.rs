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

/// How stale a lease may get before the node is judged [`NodeReadiness::Stale`].
///
/// Upstream's default is 40s — four missed renewals. Deliberately several
/// intervals, not one: a single missed renewal is a hiccup, and flapping a
/// node stale on one slow tick would evict workloads for nothing.
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
    /// The heartbeat is older than the grace period, so this node's health
    /// is no longer KNOWN.
    ///
    /// ── ★ RENAMED FROM `NotReady` AND ITS WIRE VALUE CORRECTED 2026-09-14 ──
    /// It rendered `status: "False"` while carrying `reason:
    /// "NodeStatusUnknown"` and `message: "Kubelet stopped posting node
    /// status"` — upstream's **Unknown** pair, verbatim. The struct disagreed
    /// with itself, and the wire value was the half that was wrong.
    ///
    /// Upstream distinguishes two different facts and engenho can only observe
    /// one of them:
    ///   * `Ready=False` is what a LIVE kubelet posts about ITSELF when it
    ///     knows it is unhealthy (reason `KubeletNotReady`) — the runtime is
    ///     down, the network plugin is not ready. It means "I am here and I am
    ///     broken."
    ///   * `Ready=Unknown` is what the node-lifecycle-controller posts when the
    ///     heartbeat has simply STOPPED. It means "nobody is answering."
    ///
    /// A stale lease is the second. engenho has no health probe on the
    /// container runtime at all (`ContainerRuntime` has no `health()` — its
    /// `status` asks about one container), so it cannot honestly produce the
    /// first, and claiming `False` asserts knowledge it does not have.
    ///
    /// The distinction is not cosmetic: upstream's taint manager applies
    /// `node.kubernetes.io/unreachable` for Unknown and
    /// `node.kubernetes.io/not-ready` for False, and workloads tolerate them
    /// differently.
    Stale,
    /// No lease has ever been observed.
    ///
    /// Distinct from [`Self::Stale`] on purpose: a node that has never
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
            // "Unknown", NOT "False" — see the variant's doc. A stale
            // heartbeat means nobody is answering, which is not the same
            // claim as "I am here and broken".
            Self::Stale => "Unknown",
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
            Self::Stale => "NodeStatusUnknown",
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
        Some(_) => NodeReadiness::Stale,
    }
}

/// The `Ready` condition to publish on `Node.status`.
///
/// `previous` is the condition currently on the Node, when there is one.
///
/// ── ★ THE TWO TIMESTAMPS ARE NOT THE SAME TIMESTAMP ───────────────────────
/// This function used to stamp both with `now` unconditionally, which makes
/// `lastTransitionTime` a synonym for `lastHeartbeatTime` and destroys the only
/// question either field exists to answer: **how long has the node been in this
/// state?** With both moving every tick, a node that went unreachable an hour
/// ago reports a transition time of *now*, forever.
///
/// That is load-bearing rather than cosmetic. Upstream's pod-eviction path is
/// driven by how long `Ready` has been non-`True` (`--pod-eviction-timeout`
/// against `lastTransitionTime`), so a transition time that always reads
/// "just now" is a timer that never fires. It is also what `kubectl describe
/// node` prints, so an operator reading it is told every outage is fresh.
///
/// Hence:
///   * `lastHeartbeatTime` — **always** `now`. It records that we looked.
///   * `lastTransitionTime` — `now` ONLY when the status string differs from
///     `previous`; otherwise the previous value is carried forward verbatim.
///
/// A `previous` with no readable `lastTransitionTime` (a hand-written Node, or
/// the boot-time literal this replaces) falls back to `now`, which is the
/// honest answer: we genuinely do not know when it transitioned.
#[must_use]
pub fn ready_condition(
    state: NodeReadiness,
    now: &str,
    previous: Option<&ResourceValue>,
) -> ResourceValue {
    let status = state.condition_status();
    let last_transition = previous
        .filter(|p| p.get("status").and_then(|s| s.as_str()) == Some(status))
        .and_then(|p| p.get("lastTransitionTime"))
        .and_then(|t| t.as_str())
        .unwrap_or(now);
    json!({
        "type": "Ready",
        "status": status,
        "reason": state.reason(),
        "message": match state {
            NodeReadiness::Ready => "kubelet is posting ready status",
            NodeReadiness::Stale => "Kubelet stopped posting node status",
            NodeReadiness::Unknown => "Kubelet never posted node status",
        },
        "lastHeartbeatTime": now,
        "lastTransitionTime": last_transition,
    })
}

/// Find the `Ready` condition in a Node's `status.conditions`, if present.
///
/// By `type`, never by index: the array's order is not a contract, and
/// `conditions[0]` happens to be Ready only until something else writes one.
#[must_use]
pub fn find_ready_condition(node: &ResourceValue) -> Option<&ResourceValue> {
    node.get("status")?
        .get("conditions")?
        .as_array()?
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))
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
        assert_eq!(readiness(Some(GRACE_PERIOD + S(1))), NodeReadiness::Stale);
    }

    #[test]
    fn the_grace_period_is_several_intervals_not_one() {
        // A single missed renewal is a hiccup. Flapping to Stale on one
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
        // ★ Both render "Unknown" on the wire, and that is CORRECT — upstream
        // has no third status for "we used to hear from it". They stay
        // distinct in the REASON, which is the field that carries the
        // difference an autoscaler acts on.
        assert_eq!(NodeReadiness::Stale.condition_status(), "Unknown");
        assert_ne!(
            NodeReadiness::Unknown.reason(),
            NodeReadiness::Stale.reason()
        );
    }

    #[test]
    fn the_condition_carries_what_kubectl_describe_node_prints() {
        let c = ready_condition(NodeReadiness::Ready, "2026-08-29T21:00:00Z", None);
        assert_eq!(c["type"], "Ready");
        assert_eq!(c["status"], "True");
        assert_eq!(c["reason"], "KubeletReady");
        assert_eq!(c["lastHeartbeatTime"], "2026-08-29T21:00:00Z");
        // With no previous condition there is nothing to carry, so the
        // transition is honestly "now".
        assert_eq!(c["lastTransitionTime"], "2026-08-29T21:00:00Z");
    }

    #[test]
    fn last_transition_time_only_moves_when_the_status_does() {
        // THE bug this signature exists to make impossible: both stamps set to
        // `now` every tick makes "how long has this node been down" always
        // read zero, which is the input upstream's eviction timer runs on.
        let first = ready_condition(NodeReadiness::Ready, "2026-01-01T00:00:00Z", None);
        // Same status, later heartbeat -> transition carried forward.
        let later = ready_condition(NodeReadiness::Ready, "2026-01-01T01:00:00Z", Some(&first));
        assert_eq!(later["lastHeartbeatTime"], "2026-01-01T01:00:00Z");
        assert_eq!(
            later["lastTransitionTime"], "2026-01-01T00:00:00Z",
            "a steady node has not transitioned; its transition time must not move"
        );
        // Status CHANGES -> transition is now.
        let flipped = ready_condition(NodeReadiness::Stale, "2026-01-01T02:00:00Z", Some(&later));
        assert_eq!(flipped["status"], "Unknown");
        assert_eq!(flipped["lastTransitionTime"], "2026-01-01T02:00:00Z");
        // And back again, from the flipped one.
        let back = ready_condition(NodeReadiness::Ready, "2026-01-01T03:00:00Z", Some(&flipped));
        assert_eq!(back["lastTransitionTime"], "2026-01-01T03:00:00Z");
    }

    #[test]
    fn the_ready_condition_is_found_by_type_not_by_index() {
        // conditions[] order is not a contract; Ready is conditions[0] only
        // until something else writes one.
        let node = json!({"status": {"conditions": [
            {"type": "MemoryPressure", "status": "False"},
            {"type": "Ready", "status": "True", "lastTransitionTime": "T"}
        ]}});
        let found = find_ready_condition(&node).expect("Ready is present");
        assert_eq!(found["lastTransitionTime"], "T");
        assert!(find_ready_condition(&json!({"status": {"conditions": []}})).is_none());
        assert!(find_ready_condition(&json!({})).is_none());
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
