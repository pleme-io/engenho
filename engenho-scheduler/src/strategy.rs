//! The scheduling strategy trait + the canonical
//! [`RoundRobinStrategy`] impl.
//!
//! Strategies are PURE: given the pending Pod + the list of
//! candidate Nodes, return the chosen node name (or None if
//! nothing is schedulable). The scheduler does the I/O.
//!
//! Strategies are also STATEFUL (carry their own state across
//! ticks); `RoundRobinStrategy` keeps a rotating cursor so
//! consecutive pods land on different nodes.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::observed::ObservedNode;

#[async_trait]
pub trait SchedulingStrategy: Send + Sync {
    /// Stable identifier — telemetry + audit.
    fn name(&self) -> &'static str;

    /// Pick a node for `pod` from `candidates`. Return `None` if
    /// no candidate is suitable.
    ///
    /// `pod` is the full K8s JSON resource. Each candidate is an
    /// [`ObservedNode`]: its `Ready` condition was derived from its Lease
    /// by the projection the apiserver serves, so a strategy cannot judge
    /// readiness from a stored condition.
    async fn pick<'a>(&self, pod: &'a Value, candidates: &'a [ObservedNode]) -> Option<String>;
}

/// Blanket impl so the boxed trait object that
/// [`crate::make_scheduling_strategy`] returns composes directly with
/// [`crate::Scheduler::new`]`<S: SchedulingStrategy + 'static>`. Without
/// this, the config-driven factory output (`Box<dyn SchedulingStrategy>`)
/// can't be handed to the scheduler, forcing callers to match on the
/// strategy kind a second time. Delegates every method to the inner
/// strategy.
#[async_trait]
impl SchedulingStrategy for Box<dyn SchedulingStrategy> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    async fn pick<'a>(&self, pod: &'a Value, candidates: &'a [ObservedNode]) -> Option<String> {
        (**self).pick(pod, candidates).await
    }
}

/// Round-robin across schedulable nodes. Cursor advances on every
/// pick so consecutive pods spread evenly.
///
/// Skips every candidate [`is_schedulable`] rejects.
pub struct RoundRobinStrategy {
    cursor: Mutex<usize>,
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self {
            cursor: Mutex::new(0),
        }
    }
}

impl RoundRobinStrategy {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn schedulable_nodes(candidates: &[ObservedNode]) -> Vec<&ObservedNode> {
        candidates.iter().filter(|n| is_schedulable(n)).collect()
    }
}

#[async_trait]
impl SchedulingStrategy for RoundRobinStrategy {
    fn name(&self) -> &'static str {
        "round_robin"
    }

    async fn pick<'a>(&self, _pod: &'a Value, candidates: &'a [ObservedNode]) -> Option<String> {
        let schedulable = Self::schedulable_nodes(candidates);
        if schedulable.is_empty() {
            return None;
        }
        let mut cursor = self.cursor.lock().unwrap();
        let chosen = &schedulable[*cursor % schedulable.len()];
        *cursor = cursor.wrapping_add(1);
        chosen.name().map(String::from)
    }
}

/// A node may take a pod: it is not cordoned, and its Lease-derived
/// `Ready` status is `"True"`.
///
/// There is no "assume schedulable" arm. A node with no conditions, or no
/// `Ready` condition, used to be treated as newly registered and therefore
/// schedulable; readiness now comes from the Lease through
/// [`ObservedNode::project`], so a node that has never heartbeat reads
/// `Unknown` and a node whose heartbeat went stale reads `Unknown` even
/// when storage still says `Ready=True`. Neither takes a pod.
#[must_use]
pub fn is_schedulable(node: &ObservedNode) -> bool {
    !node.is_cordoned() && node.is_ready()
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_controllers::node_lease::lease_value;
    use serde_json::json;

    const NOW: &str = "2026-09-19T12:00:00Z";

    /// A lease renewed just now.
    fn fresh(name: &str) -> Value {
        lease_value(name, &engenho_types::time::now_rfc3339_utc(), 0)
    }

    /// A lease last renewed years ago: far past the grace period.
    fn stale(name: &str) -> Value {
        lease_value(name, "2020-01-01T00:00:00Z", 0)
    }

    fn node(name: &str, unschedulable: bool, stored_ready: &str) -> Value {
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "spec": { "unschedulable": unschedulable },
            "status": {
                "conditions": [{ "type": "Ready", "status": stored_ready }]
            }
        })
    }

    /// Stored Ready=True and a fresh lease.
    fn ready_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, false, "True"), Some(&fresh(name)), NOW)
    }

    /// Stored Ready=True, but the heartbeat stopped long ago.
    fn stale_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, false, "True"), Some(&stale(name)), NOW)
    }

    /// Heartbeating, but cordoned.
    fn cordoned_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, true, "True"), Some(&fresh(name)), NOW)
    }

    fn pending_pod() -> Value {
        json!({
            "metadata": { "name": "p" },
            "spec": {}
        })
    }

    #[test]
    fn is_schedulable_classifies_correctly() {
        assert!(is_schedulable(&ready_node("a")));
        assert!(!is_schedulable(&stale_node("b")));
        assert!(!is_schedulable(&cordoned_node("c")));
    }

    #[test]
    fn a_stale_lease_is_not_schedulable_even_when_storage_says_ready() {
        // The stored condition is the value a wedged kubelet leaves behind.
        // The scheduler must agree with what the apiserver serves: Unknown.
        let n = stale_node("wedged");
        assert!(!is_schedulable(&n), "{:#}", n.value());
    }

    #[test]
    fn a_node_with_no_conditions_and_no_lease_is_not_schedulable() {
        // Was: "no status yet, assume schedulable". A node nobody has heard
        // from is Unknown, not ready.
        let n = ObservedNode::project(json!({ "metadata": { "name": "x" } }), None, NOW);
        assert!(!is_schedulable(&n), "{:#}", n.value());
    }

    #[test]
    fn a_node_with_no_ready_condition_and_no_lease_is_not_schedulable() {
        // Was: "no Ready condition yet, assume schedulable".
        let n = ObservedNode::project(
            json!({
                "metadata": { "name": "x" },
                "status": { "conditions": [{ "type": "MemoryPressure", "status": "False" }] }
            }),
            None,
            NOW,
        );
        assert!(!is_schedulable(&n), "{:#}", n.value());
    }

    #[test]
    fn a_fresh_lease_makes_a_node_without_a_stored_ready_schedulable() {
        // The positive control: readiness is DERIVED, so a registering node
        // is schedulable as soon as it heartbeats.
        let n = ObservedNode::project(
            json!({ "metadata": { "name": "x" } }),
            Some(&fresh("x")),
            NOW,
        );
        assert!(is_schedulable(&n), "{:#}", n.value());
    }

    #[tokio::test]
    async fn round_robin_picks_each_node_in_sequence() {
        let strategy = RoundRobinStrategy::new();
        let nodes = vec![ready_node("a"), ready_node("b"), ready_node("c")];
        let pod = pending_pod();
        // Round-robin iterates the candidates in input order.
        let pick1 = strategy.pick(&pod, &nodes).await;
        let pick2 = strategy.pick(&pod, &nodes).await;
        let pick3 = strategy.pick(&pod, &nodes).await;
        let pick4 = strategy.pick(&pod, &nodes).await; // wraps to a
        assert_eq!(pick1, Some("a".into()));
        assert_eq!(pick2, Some("b".into()));
        assert_eq!(pick3, Some("c".into()));
        assert_eq!(pick4, Some("a".into()));
    }

    #[tokio::test]
    async fn round_robin_skips_unschedulable() {
        let strategy = RoundRobinStrategy::new();
        let nodes = vec![
            cordoned_node("a"),
            ready_node("b"),
            stale_node("c"),
            ready_node("d"),
        ];
        let pod = pending_pod();
        let p1 = strategy.pick(&pod, &nodes).await;
        let p2 = strategy.pick(&pod, &nodes).await;
        assert_eq!(p1, Some("b".into()));
        assert_eq!(p2, Some("d".into()));
    }

    #[tokio::test]
    async fn round_robin_returns_none_when_no_candidates() {
        let strategy = RoundRobinStrategy::new();
        let nodes = vec![cordoned_node("a"), stale_node("b")];
        let pod = pending_pod();
        assert!(strategy.pick(&pod, &nodes).await.is_none());
    }

    #[tokio::test]
    async fn round_robin_strategy_name() {
        assert_eq!(RoundRobinStrategy::new().name(), "round_robin");
    }
}
