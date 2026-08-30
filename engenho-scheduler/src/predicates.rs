//! SCHEDULING PREDICATES beyond resource fit.
//!
//! ★ WHY FIT ALONE IS NOT A SCHEDULER. `fit.rs` answers "does this pod's
//! cpu/memory fit here", which is necessary and nowhere near sufficient.
//! Without the predicates below the scheduler will cheerfully place a pod
//! on a node the operator has cordoned for maintenance, on a node whose
//! taint exists precisely to keep that pod off, or on a node that does not
//! have the GPU the pod's `nodeSelector` asked for. Each of those is a
//! placement a human explicitly forbade, honoured by nothing.
//!
//! ★ THE DEFAULT IS TO EXCLUDE, and it is the safe direction here. If a
//! rule cannot be evaluated — an unparseable selector, a malformed taint —
//! the node is not a candidate. A scheduler that guesses places a workload
//! somewhere nobody sanctioned; one that declines leaves the pod Pending,
//! which is visible and recoverable. The asymmetry is the whole argument.
//!
//! ★ PURE FUNCTIONS OVER `serde_json::Value`, matching `fit.rs`, so every
//! rule is testable without a cluster and the Filter stage stays a fold of
//! independent predicates rather than one tangled condition.

use serde_json::Value;

/// Upstream's taint effects.
///
/// `PreferNoSchedule` is deliberately NOT treated as a hard filter: it is a
/// SCORING signal, and demoting it to a filter would make a soft preference
/// silently behave as a hard exclusion — turning a hint into an outage when
/// every node carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaintEffect {
    NoSchedule,
    PreferNoSchedule,
    NoExecute,
}

impl TaintEffect {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "NoSchedule" => Some(Self::NoSchedule),
            "PreferNoSchedule" => Some(Self::PreferNoSchedule),
            "NoExecute" => Some(Self::NoExecute),
            _ => None,
        }
    }

    /// Does this effect block scheduling outright?
    #[must_use]
    pub fn blocks_scheduling(self) -> bool {
        matches!(self, Self::NoSchedule | Self::NoExecute)
    }
}

/// Is the node accepting new pods at all?
///
/// `spec.unschedulable` is what `kubectl cordon` sets, and it is the most
/// direct instruction an operator can give a scheduler. Ignoring it means
/// draining a node for maintenance does not actually stop work arriving.
#[must_use]
pub fn is_cordoned(node: &Value) -> bool {
    node.get("spec")
        .and_then(|s| s.get("unschedulable"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Does the node satisfy the pod's `spec.nodeSelector`?
///
/// Every key must be present on the node's labels with the exact value —
/// upstream's semantics are AND across keys and exact string equality, not
/// a subset or a pattern match.
#[must_use]
pub fn matches_node_selector(pod: &Value, node: &Value) -> bool {
    let Some(sel) = pod
        .get("spec")
        .and_then(|s| s.get("nodeSelector"))
        .and_then(Value::as_object)
    else {
        return true; // no selector ⇒ every node qualifies
    };
    let labels = node.get("metadata").and_then(|m| m.get("labels"));
    sel.iter().all(|(k, want)| {
        labels
            .and_then(|l| l.get(k))
            .is_some_and(|have| have == want)
    })
}

/// Does `spec.nodeName` pin this pod to a specific node?
///
/// A pinned pod bypasses scoring entirely but NOT the other predicates:
/// upstream still refuses to run it where a taint forbids it, and quietly
/// honouring the pin would let a pin defeat a taint.
#[must_use]
pub fn node_name_matches(pod: &Value, node: &Value) -> bool {
    let Some(want) = pod
        .get("spec")
        .and_then(|s| s.get("nodeName"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return true;
    };
    node.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        == Some(want)
}

/// One taint the pod must tolerate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Taint {
    pub key: String,
    pub value: Option<String>,
    pub effect: TaintEffect,
}

/// Read a node's scheduling-blocking taints.
///
/// A taint with an UNRECOGNISED effect is treated as blocking. It was put
/// there deliberately, and the safe reading of "I do not understand this
/// restriction" is to honour it rather than to ignore it.
#[must_use]
pub fn blocking_taints(node: &Value) -> Vec<Taint> {
    node.get("spec")
        .and_then(|s| s.get("taints"))
        .and_then(Value::as_array)
        .map(|ts| {
            ts.iter()
                .filter_map(|t| {
                    let key = t.get("key").and_then(Value::as_str)?.to_string();
                    let effect_str = t.get("effect").and_then(Value::as_str).unwrap_or_default();
                    let effect = TaintEffect::parse(effect_str)
                        // Unknown effect ⇒ treat as NoSchedule (honour it).
                        .unwrap_or(TaintEffect::NoSchedule);
                    if !effect.blocks_scheduling() {
                        return None;
                    }
                    Some(Taint {
                        key,
                        value: t
                            .get("value")
                            .and_then(Value::as_str)
                            .map(ToString::to_string),
                        effect,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Does the pod tolerate `taint`?
///
/// Upstream's operators: `Equal` (default) matches key AND value; `Exists`
/// matches the key whatever its value. An empty toleration `key` with
/// `Exists` tolerates EVERYTHING — that is the wildcard control-plane
/// components use, and omitting it would make them unschedulable on the
/// very nodes they must run on.
#[must_use]
pub fn tolerates(pod: &Value, taint: &Taint) -> bool {
    let Some(tols) = pod
        .get("spec")
        .and_then(|s| s.get("tolerations"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    tols.iter().any(|t| {
        let op = t.get("operator").and_then(Value::as_str).unwrap_or("Equal");
        let key = t.get("key").and_then(Value::as_str).unwrap_or_default();
        // A toleration naming an effect only tolerates THAT effect; one
        // naming none tolerates every effect for its key.
        if let Some(e) = t
            .get("effect")
            .and_then(Value::as_str)
            .filter(|e| !e.is_empty())
        {
            if TaintEffect::parse(e) != Some(taint.effect) {
                return false;
            }
        }
        match op {
            "Exists" => key.is_empty() || key == taint.key,
            // "Equal" and anything unrecognised: an unknown operator must
            // not accidentally tolerate, so it falls through to the strict
            // comparison rather than to `true`.
            _ => {
                key == taint.key
                    && t.get("value")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        == taint.value
            }
        }
    })
}

/// Why a node was rejected. Carried rather than collapsed to a bool so the
/// scheduler can report `FailedScheduling` with the actual reason — "0/3
/// nodes are available: 2 node(s) had untolerated taint" is actionable;
/// "no node fits" is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    Cordoned,
    NodeNameMismatch,
    NodeSelectorMismatch,
    UntoleratedTaint { key: String },
}

impl Rejection {
    /// The clause upstream puts in the FailedScheduling message.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Cordoned => "node(s) were unschedulable".to_string(),
            Self::NodeNameMismatch => "node(s) didn't match the requested node name".to_string(),
            Self::NodeSelectorMismatch => "node(s) didn't match Pod's node selector".to_string(),
            Self::UntoleratedTaint { key } => {
                format!("node(s) had untolerated taint {{{key}}}")
            }
        }
    }
}

/// Run every non-resource predicate. `None` ⇒ the node is a candidate.
///
/// Ordered cheapest-first, and the order is also most-specific-first so the
/// reported reason is the one an operator can act on: a cordoned node
/// should say "cordoned", not "taint", even though a cordon usually implies
/// one.
#[must_use]
pub fn reject_reason(pod: &Value, node: &Value) -> Option<Rejection> {
    if is_cordoned(node) {
        return Some(Rejection::Cordoned);
    }
    if !node_name_matches(pod, node) {
        return Some(Rejection::NodeNameMismatch);
    }
    if !matches_node_selector(pod, node) {
        return Some(Rejection::NodeSelectorMismatch);
    }
    for taint in blocking_taints(node) {
        if !tolerates(pod, &taint) {
            return Some(Rejection::UntoleratedTaint { key: taint.key });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(name: &str, extra: Value) -> Value {
        let mut n = json!({ "metadata": { "name": name }, "spec": {}, "status": {} });
        if let (Some(o), Some(e)) = (n.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                o.insert(k.clone(), v.clone());
            }
        }
        n
    }

    fn pod(spec: Value) -> Value {
        json!({ "metadata": { "name": "p" }, "spec": spec })
    }

    #[test]
    fn a_bare_pod_fits_a_bare_node() {
        // Anti-vacuity: a predicate set that rejected everything would pass
        // every negative test below.
        assert_eq!(reject_reason(&pod(json!({})), &node("n", json!({}))), None);
    }

    #[test]
    fn a_cordoned_node_is_excluded() {
        // kubectl cordon is the most direct instruction an operator can
        // give a scheduler; ignoring it means draining does not drain.
        let n = node("n", json!({ "spec": { "unschedulable": true } }));
        assert_eq!(
            reject_reason(&pod(json!({})), &n),
            Some(Rejection::Cordoned)
        );
    }

    #[test]
    fn node_selector_requires_every_key_to_match_exactly() {
        let gpu = node(
            "gpu",
            json!({ "metadata": { "name": "gpu", "labels": { "gpu": "true", "zone": "a" } } }),
        );
        let plain = node("plain", json!({}));

        let wants_gpu = pod(json!({ "nodeSelector": { "gpu": "true" } }));
        assert_eq!(reject_reason(&wants_gpu, &gpu), None);
        assert_eq!(
            reject_reason(&wants_gpu, &plain),
            Some(Rejection::NodeSelectorMismatch)
        );

        // AND across keys, and EXACT value — not a subset, not a prefix.
        let wants_two = pod(json!({ "nodeSelector": { "gpu": "true", "zone": "b" } }));
        assert_eq!(
            reject_reason(&wants_two, &gpu),
            Some(Rejection::NodeSelectorMismatch)
        );
        let wrong_value = pod(json!({ "nodeSelector": { "gpu": "yes" } }));
        assert_eq!(
            reject_reason(&wrong_value, &gpu),
            Some(Rejection::NodeSelectorMismatch)
        );
    }

    #[test]
    fn an_untolerated_taint_excludes_and_names_the_key() {
        // "2 node(s) had untolerated taint {node-role...}" is actionable;
        // "no node fits" is not.
        let tainted = node(
            "cp",
            json!({ "spec": { "taints": [
                { "key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule" }
            ] } }),
        );
        let r = reject_reason(&pod(json!({})), &tainted);
        assert_eq!(
            r,
            Some(Rejection::UntoleratedTaint {
                key: "node-role.kubernetes.io/control-plane".to_string()
            })
        );
        assert!(r.unwrap().describe().contains("untolerated taint"));
    }

    #[test]
    fn a_matching_toleration_admits_the_pod() {
        let tainted = node(
            "cp",
            json!({ "spec": { "taints": [
                { "key": "dedicated", "value": "db", "effect": "NoSchedule" }
            ] } }),
        );
        let exact = pod(json!({ "tolerations": [
            { "key": "dedicated", "operator": "Equal", "value": "db", "effect": "NoSchedule" }
        ] }));
        assert_eq!(reject_reason(&exact, &tainted), None);

        // Wrong value must NOT tolerate.
        let wrong = pod(json!({ "tolerations": [
            { "key": "dedicated", "operator": "Equal", "value": "cache" }
        ] }));
        assert!(reject_reason(&wrong, &tainted).is_some());
    }

    #[test]
    fn the_empty_key_exists_toleration_is_the_wildcard_control_planes_need() {
        // Omitting this makes control-plane components unschedulable on the
        // very nodes they must run on.
        let tainted = node(
            "cp",
            json!({ "spec": { "taints": [
                { "key": "anything", "effect": "NoSchedule" },
                { "key": "else", "effect": "NoExecute" }
            ] } }),
        );
        let wildcard = pod(json!({ "tolerations": [ { "operator": "Exists" } ] }));
        assert_eq!(reject_reason(&wildcard, &tainted), None);
    }

    #[test]
    fn prefer_no_schedule_is_a_score_signal_not_a_filter() {
        // Demoting a soft preference to a hard exclusion turns a hint into
        // an outage when every node carries it.
        let soft = node(
            "n",
            json!({ "spec": { "taints": [
                { "key": "spot", "effect": "PreferNoSchedule" }
            ] } }),
        );
        assert_eq!(reject_reason(&pod(json!({})), &soft), None);
        assert!(blocking_taints(&soft).is_empty());
    }

    #[test]
    fn an_unrecognised_taint_effect_is_honoured_not_ignored() {
        // It was put there deliberately. The safe reading of "I do not
        // understand this restriction" is to obey it.
        let weird = node(
            "n",
            json!({ "spec": { "taints": [ { "key": "k", "effect": "Mystery" } ] } }),
        );
        assert!(reject_reason(&pod(json!({})), &weird).is_some());
    }

    #[test]
    fn an_effect_scoped_toleration_does_not_tolerate_other_effects() {
        let no_execute = node(
            "n",
            json!({ "spec": { "taints": [ { "key": "k", "effect": "NoExecute" } ] } }),
        );
        // Tolerates NoSchedule only — must NOT admit a NoExecute taint.
        let scoped = pod(json!({ "tolerations": [
            { "key": "k", "operator": "Exists", "effect": "NoSchedule" }
        ] }));
        assert!(reject_reason(&scoped, &no_execute).is_some());
    }

    #[test]
    fn a_node_name_pin_does_not_defeat_a_taint() {
        // Quietly honouring the pin would let it override a restriction the
        // operator set precisely to keep pods off.
        let tainted = node(
            "target",
            json!({ "spec": { "taints": [ { "key": "k", "effect": "NoSchedule" } ] } }),
        );
        let pinned = pod(json!({ "nodeName": "target" }));
        assert!(matches!(
            reject_reason(&pinned, &tainted),
            Some(Rejection::UntoleratedTaint { .. })
        ));
        // And a pin to a DIFFERENT node excludes this one.
        let elsewhere = pod(json!({ "nodeName": "other" }));
        assert_eq!(
            reject_reason(&elsewhere, &node("target", json!({}))),
            Some(Rejection::NodeNameMismatch)
        );
    }
}
