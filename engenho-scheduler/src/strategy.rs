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

#[async_trait]
pub trait SchedulingStrategy: Send + Sync {
    /// Stable identifier — telemetry + audit.
    fn name(&self) -> &'static str;

    /// Pick a node for `pod` from `candidates`. Return `None` if
    /// no candidate is suitable.
    ///
    /// Both `pod` and `candidates` are full K8s JSON resources
    /// (the substrate keeps them opaque; per-kind typed handlers
    /// land at R7.5c).
    async fn pick<'a>(&self, pod: &'a Value, candidates: &'a [Value]) -> Option<String>;
}

/// Round-robin across schedulable nodes. Cursor advances on every
/// pick so consecutive pods spread evenly.
///
/// Treats a node as unschedulable when:
///   * `spec.unschedulable == true` (cordoned)
///   * `status.conditions[Ready].status != "True"`
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

    fn schedulable_nodes<'a>(candidates: &'a [Value]) -> Vec<&'a Value> {
        candidates
            .iter()
            .filter(|n| is_schedulable(n))
            .collect()
    }
}

#[async_trait]
impl SchedulingStrategy for RoundRobinStrategy {
    fn name(&self) -> &'static str {
        "round_robin"
    }

    async fn pick<'a>(&self, _pod: &'a Value, candidates: &'a [Value]) -> Option<String> {
        let schedulable = Self::schedulable_nodes(candidates);
        if schedulable.is_empty() {
            return None;
        }
        let mut cursor = self.cursor.lock().unwrap();
        let chosen = &schedulable[*cursor % schedulable.len()];
        *cursor = cursor.wrapping_add(1);
        chosen
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(String::from)
    }
}

/// Returns `true` if the node is healthy + not cordoned.
pub fn is_schedulable(node: &Value) -> bool {
    let unsched = node
        .get("spec")
        .and_then(|s| s.get("unschedulable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if unsched {
        return false;
    }
    let conditions = node
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array());
    let Some(conditions) = conditions else {
        // No status yet — assume schedulable (newly-registered node)
        return true;
    };
    let ready = conditions
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"));
    match ready {
        Some(r) => r.get("status").and_then(|s| s.as_str()) == Some("True"),
        // No Ready condition yet — assume schedulable
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ready_node(name: &str) -> Value {
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "spec": { "unschedulable": false },
            "status": {
                "conditions": [{ "type": "Ready", "status": "True" }]
            }
        })
    }

    fn unready_node(name: &str) -> Value {
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "status": {
                "conditions": [{ "type": "Ready", "status": "False" }]
            }
        })
    }

    fn cordoned_node(name: &str) -> Value {
        json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": { "name": name },
            "spec": { "unschedulable": true },
            "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
        })
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
        assert!(!is_schedulable(&unready_node("b")));
        assert!(!is_schedulable(&cordoned_node("c")));
    }

    #[test]
    fn is_schedulable_assumes_true_when_no_status() {
        let n = json!({"metadata": {"name": "x"}});
        assert!(is_schedulable(&n));
    }

    #[tokio::test]
    async fn round_robin_picks_each_node_in_sequence() {
        let strategy = RoundRobinStrategy::new();
        let nodes = vec![ready_node("a"), ready_node("b"), ready_node("c")];
        let pod = pending_pod();
        // 3 picks → a, b, c (in BTreeMap-sorted order over input, but
        // round-robin actually iterates in input order from the candidates list)
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
            unready_node("c"),
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
        let nodes = vec![cordoned_node("a"), unready_node("b")];
        let pod = pending_pod();
        assert!(strategy.pick(&pod, &nodes).await.is_none());
    }

    #[tokio::test]
    async fn round_robin_strategy_name() {
        assert_eq!(RoundRobinStrategy::new().name(), "round_robin");
    }
}
