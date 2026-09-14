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
    fn a_stale_lease_makes_a_stored_ready_true_read_as_unknown() {
        // THE POINT. This is the rio failure exactly: the kubelet wedged, the
        // Node kept its last-published `Ready=True`, and the apiserver served
        // it for three days. Derivation means the stored value cannot lie,
        // because it is not what gets served.
        let mut node = json!({"status": {"conditions": [
            {"type": "Ready", "status": "True", "reason": "KubeletReady",
             "lastTransitionTime": "2026-01-01T00:00:00Z"}
        ]}});
        let stale = json!({"spec": {"renewTime": "2020-01-01T00:00:00Z"}});
        project_ready_condition(&mut node, Some(&stale), "2026-09-14T12:00:00Z");
        let ready = find_ready_condition(&node).expect("Ready");
        assert_eq!(ready["status"], "Unknown", "a stale heartbeat cannot read Ready: {node}");
        assert_eq!(ready["reason"], "NodeStatusUnknown");
        // The status CHANGED, so the transition time moves to now.
        assert_eq!(ready["lastTransitionTime"], "2026-09-14T12:00:00Z");
    }

    #[test]
    fn no_lease_at_all_reads_as_unknown_never_as_ready() {
        // A Node object with a stored Ready=True and NO lease — the shape
        // `register_node` produces at boot before the first heartbeat.
        let mut node = json!({"status": {"conditions": [
            {"type": "Ready", "status": "True"}
        ]}});
        project_ready_condition(&mut node, None, "2026-09-14T12:00:00Z");
        let ready = find_ready_condition(&node).expect("Ready");
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], "NodeStatusNeverUpdated", "{node}");
    }

    #[test]
    fn a_fresh_lease_reads_ready_and_preserves_foreign_conditions() {
        let mut node = json!({"status": {"conditions": [
            {"type": "MemoryPressure", "status": "False"},
            {"type": "Ready", "status": "Unknown",
             "lastTransitionTime": "2026-01-01T00:00:00Z"}
        ]}});
        let fresh = json!({"spec": {"renewTime": engenho_types::time::now_rfc3339_utc()}});
        project_ready_condition(&mut node, Some(&fresh), "2026-09-14T12:00:00Z");
        let ready = find_ready_condition(&node).expect("Ready");
        assert_eq!(ready["status"], "True");
        // NEGATIVE CONTROL: the projection must REPLACE only Ready. A
        // condition it does not own surviving is what separates this from a
        // renderer that rebuilds the array.
        assert!(
            node["status"]["conditions"].as_array().unwrap()
                .iter().any(|c| c["type"] == "MemoryPressure"),
            "a foreign condition must survive the projection: {node}"
        );
        // And exactly one Ready, not one appended beside the old.
        let n = node["status"]["conditions"].as_array().unwrap()
            .iter().filter(|c| c["type"] == "Ready").count();
        assert_eq!(n, 1, "{node}");
    }

    #[test]
    fn an_unparseable_renew_time_reads_unknown_rather_than_fresh() {
        // A future or malformed timestamp must not read as a 0-second-old
        // heartbeat, which is exactly what a healthy node looks like. This is
        // the most convincing possible lie.
        let mut node = json!({});
        let future = json!({"spec": {"renewTime": "2099-01-01T00:00:00Z"}});
        project_ready_condition(&mut node, Some(&future), "2026-09-14T12:00:00Z");
        assert_eq!(find_ready_condition(&node).unwrap()["status"], "Unknown");
        let garbage = json!({"spec": {"renewTime": "not-a-time"}});
        let mut node2 = json!({});
        project_ready_condition(&mut node2, Some(&garbage), "2026-09-14T12:00:00Z");
        assert_eq!(find_ready_condition(&node2).unwrap()["status"], "Unknown");
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

// =================================================================
// Read-time projection
// =================================================================

/// Project the `Ready` condition a Node's Lease implies onto that Node.
///
/// ── ★ WHY A DERIVATION AND NOT JUST A STORED FIELD ────────────────────────
/// Upstream stores `Ready` as a fact because the writer (the
/// node-lifecycle-controller) and the reader (scheduler, kubectl) live on
/// different machines and communicate through etcd. That separation is also
/// what lets a dead kubelet be *judged*: something else is still running.
///
/// engenho is one binary. Removing the distribution removes the observer, not
/// the problem — so reproducing upstream's shape faithfully would reproduce a
/// stored condition with nobody to correct it. A kubelet task that wedges
/// (measured on rio: three days, 2,050 failed reconciles) leaves `Ready=True`
/// standing while the apiserver happily serves it.
///
/// So the condition is DERIVED when a Node is read. There is no stored copy to
/// go stale, which makes the bad state unrepresentable rather than reconciled
/// — the `invariant-by-consistency-and-controller` third case: re-derive the
/// value from live inputs instead of storing one that can drift.
///
/// The API contract is unchanged. A client GETting a Node sees a conformant
/// `Ready` condition with status, reason, message and both timestamps. Only
/// the mechanism differs, which is the naturalize posture: speak the API, own
/// the implementation.
///
/// ── ★ WHAT THIS DOES NOT FIX, SO NOBODY READS IT AS MORE ──────────────────
/// A derived value changes by the PASSAGE OF TIME, so no write happens, so no
/// WATCH event fires. `kubectl get` is correct; `kubectl get --watch` stays
/// silent until something writes. That is why the kubelet still publishes the
/// condition on transition — the two halves are not alternatives:
/// derivation makes the answer correct, the write makes it observable.
///
/// And if the WHOLE process is dead nothing serves reads either, so there is
/// no one to lie to. This covers exactly the set where a reader outlives the
/// writer — a wedged kubelet beside a live apiserver, or a peer serving a read
/// of another node's object from the replicated store.
///
/// `lease` is that node's Lease, `None` when it has none. Conditions the
/// kubelet does not own are preserved; only `Ready` is replaced.
pub fn project_ready_condition(node: &mut ResourceValue, lease: Option<&ResourceValue>, now: &str) {
    let since_renew = lease
        .and_then(|l| l.get("spec"))
        .and_then(|s| s.get("renewTime"))
        .and_then(|t| t.as_str())
        .and_then(engenho_types::time::age_since_rfc3339);
    let state = readiness(since_renew);

    // Carry `lastTransitionTime` from whatever is stored, so the derived value
    // still answers "how long has it been like this" when the status agrees.
    let previous = find_ready_condition(node).cloned();
    let condition = ready_condition(state, now, previous.as_ref());

    let mut conditions: Vec<ResourceValue> = node
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|c| c.get("type").and_then(|t| t.as_str()) != Some("Ready"))
        .collect();
    conditions.push(condition);

    if let Some(obj) = node.as_object_mut() {
        let status = obj.entry("status").or_insert_with(|| json!({}));
        if let Some(status_obj) = status.as_object_mut() {
            status_obj.insert("conditions".to_string(), json!(conditions));
        }
    }
}
