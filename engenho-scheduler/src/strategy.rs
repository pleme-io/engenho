//! The scheduling strategy trait + the canonical
//! [`RoundRobinStrategy`] impl.
//!
//! A strategy is the **Score** stage. It is handed a [`Feasible`], the
//! non-empty set of nodes every [`crate::FilterPlugin`] admitted, and returns
//! the name of one of them. It does no filtering of its own and has no "found
//! nothing" answer: [`Feasible`] can only be built by [`crate::filter`], and
//! it is never empty. The scheduler does the I/O.
//!
//! Strategies are STATEFUL (carry their own state across ticks);
//! `RoundRobinStrategy` keeps a rotating cursor so consecutive pods land on
//! different nodes.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::Value;

use crate::filter::Feasible;

#[async_trait]
pub trait SchedulingStrategy: Send + Sync {
    /// Stable identifier — telemetry + audit.
    fn name(&self) -> &'static str;

    /// Pick the node `pod` is bound to, from `feasible`.
    ///
    /// `pod` is the full K8s JSON resource. Every candidate in `feasible`
    /// passed every filter plugin for this pod, including readiness derived
    /// from its Lease and the pod's resource fit against this tick's ledger.
    async fn pick<'a>(&self, pod: &'a Value, feasible: &'a Feasible<'a>) -> String;
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

    async fn pick<'a>(&self, pod: &'a Value, feasible: &'a Feasible<'a>) -> String {
        (**self).pick(pod, feasible).await
    }
}

/// Round-robin across the feasible nodes. The cursor advances on every pick
/// so consecutive pods spread evenly.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    cursor: AtomicUsize,
}

impl RoundRobinStrategy {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SchedulingStrategy for RoundRobinStrategy {
    fn name(&self) -> &'static str {
        "round_robin"
    }

    async fn pick<'a>(&self, _pod: &'a Value, feasible: &'a Feasible<'a>) -> String {
        // `fetch_add` wraps on overflow, and `nth_wrapping` is total.
        let turn = self.cursor.fetch_add(1, Ordering::Relaxed);
        feasible.nth_wrapping(turn).name().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{Filtered, filter};
    use crate::fit::pod_requests;
    use crate::ledger::NodeLedger;
    use crate::observed::ObservedNode;
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

    fn node(name: &str, unschedulable: bool) -> Value {
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "spec": { "unschedulable": unschedulable },
            "status": {
                "allocatable": { "cpu": "4", "memory": "8Gi" },
                "conditions": [{ "type": "Ready", "status": "True" }]
            }
        })
    }

    fn ready_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, false), Some(&fresh(name)), NOW)
    }

    fn stale_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, false), Some(&stale(name)), NOW)
    }

    fn cordoned_node(name: &str) -> ObservedNode {
        ObservedNode::project(node(name, true), Some(&fresh(name)), NOW)
    }

    fn pending_pod() -> Value {
        json!({ "metadata": { "name": "p" }, "spec": {} })
    }

    /// Pick `n` times for one pod over `nodes`, through the Filter stage.
    async fn picks(strategy: &RoundRobinStrategy, nodes: &[ObservedNode], n: usize) -> Vec<String> {
        let pod = pending_pod();
        let ledger = NodeLedger::seed(nodes.iter().map(ObservedNode::value), []);
        let Filtered::Feasible(feasible) = filter(&pod, &pod_requests(&pod), nodes, &ledger) else {
            panic!("the fixture has a feasible node");
        };
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(strategy.pick(&pod, &feasible).await);
        }
        out
    }

    #[tokio::test]
    async fn round_robin_picks_each_node_in_sequence() {
        let strategy = RoundRobinStrategy::new();
        let nodes = [ready_node("a"), ready_node("b"), ready_node("c")];
        // Round-robin iterates the feasible nodes in observed order, and wraps.
        assert_eq!(picks(&strategy, &nodes, 4).await, ["a", "b", "c", "a"]);
    }

    #[tokio::test]
    async fn round_robin_rotates_over_the_feasible_nodes_only() {
        // The cordoned and the stale node never reach the strategy: the
        // Filter stage removed them, so the cursor rotates over {b, d}.
        let strategy = RoundRobinStrategy::new();
        let nodes = [
            cordoned_node("a"),
            ready_node("b"),
            stale_node("c"),
            ready_node("d"),
        ];
        assert_eq!(picks(&strategy, &nodes, 3).await, ["b", "d", "b"]);
    }

    #[tokio::test]
    async fn round_robin_strategy_name() {
        assert_eq!(RoundRobinStrategy::new().name(), "round_robin");
    }
}
