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
//!
//! ★ WIRED THROUGH [`crate::filter::FilterPlugin`]. Each predicate here is the
//! body of one plugin (`Cordon`, `NodeName`, `NodeSelector`,
//! `TaintToleration`), and the fold, its order and the rejection vocabulary
//! live there, once. Until T5.7 these functions had tests and no caller.

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

/// The first scheduling-blocking taint on `node` that `pod` does not
/// tolerate, or `None` when the pod tolerates them all.
///
/// "First" is the node's own taint order, so the taint a rejection names is
/// stable across ticks.
#[must_use]
pub fn untolerated_taint(pod: &Value, node: &Value) -> Option<Taint> {
    blocking_taints(node)
        .into_iter()
        .find(|taint| !tolerates(pod, taint))
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

    fn untolerated_key(pod: &Value, node: &Value) -> Option<String> {
        untolerated_taint(pod, node).map(|t| t.key)
    }

    #[test]
    fn a_bare_pod_passes_every_predicate_on_a_bare_node() {
        // Anti-vacuity: predicates that rejected everything would pass every
        // negative test below.
        let (p, n) = (pod(json!({})), node("n", json!({})));
        assert!(!is_cordoned(&n));
        assert!(node_name_matches(&p, &n));
        assert!(matches_node_selector(&p, &n));
        assert_eq!(untolerated_taint(&p, &n), None);
    }

    #[test]
    fn a_cordon_is_an_explicit_true_and_nothing_else() {
        // kubectl cordon is the most direct instruction an operator can
        // give a scheduler; ignoring it means draining does not drain. An
        // absent or non-boolean field is not a cordon.
        assert!(is_cordoned(&node(
            "n",
            json!({ "spec": { "unschedulable": true } })
        )));
        for spec in [
            json!({}),
            json!({ "unschedulable": false }),
            json!({ "unschedulable": "true" }),
        ] {
            assert!(
                !is_cordoned(&node("n", json!({ "spec": spec.clone() }))),
                "{spec}"
            );
        }
    }

    #[test]
    fn node_selector_requires_every_key_to_match_exactly() {
        let gpu = node(
            "gpu",
            json!({ "metadata": { "name": "gpu", "labels": { "gpu": "true", "zone": "a" } } }),
        );
        let plain = node("plain", json!({}));

        let wants_gpu = pod(json!({ "nodeSelector": { "gpu": "true" } }));
        assert!(matches_node_selector(&wants_gpu, &gpu));
        assert!(!matches_node_selector(&wants_gpu, &plain));

        // AND across keys, and EXACT value — not a subset, not a prefix.
        let wants_two = pod(json!({ "nodeSelector": { "gpu": "true", "zone": "b" } }));
        assert!(!matches_node_selector(&wants_two, &gpu));
        let wrong_value = pod(json!({ "nodeSelector": { "gpu": "yes" } }));
        assert!(!matches_node_selector(&wrong_value, &gpu));
    }

    #[test]
    fn an_untolerated_taint_is_found_and_named() {
        let tainted = node(
            "cp",
            json!({ "spec": { "taints": [
                { "key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule" }
            ] } }),
        );
        assert_eq!(
            untolerated_key(&pod(json!({})), &tainted).as_deref(),
            Some("node-role.kubernetes.io/control-plane")
        );
    }

    #[test]
    fn the_first_untolerated_taint_in_node_order_is_the_one_named() {
        let tainted = node(
            "n",
            json!({ "spec": { "taints": [
                { "key": "tolerated", "effect": "NoSchedule" },
                { "key": "second", "effect": "NoSchedule" },
                { "key": "third", "effect": "NoExecute" }
            ] } }),
        );
        let p = pod(json!({ "tolerations": [ { "key": "tolerated", "operator": "Exists" } ] }));
        assert_eq!(untolerated_key(&p, &tainted).as_deref(), Some("second"));
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
        assert_eq!(untolerated_taint(&exact, &tainted), None);

        // Wrong value must NOT tolerate.
        let wrong = pod(json!({ "tolerations": [
            { "key": "dedicated", "operator": "Equal", "value": "cache" }
        ] }));
        assert!(untolerated_taint(&wrong, &tainted).is_some());
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
        assert_eq!(untolerated_taint(&wildcard, &tainted), None);
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
        assert_eq!(untolerated_taint(&pod(json!({})), &soft), None);
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
        assert!(untolerated_taint(&pod(json!({})), &weird).is_some());
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
        assert!(untolerated_taint(&scoped, &no_execute).is_some());
    }

    #[test]
    fn a_node_name_pin_matches_only_its_own_node() {
        let target = node("target", json!({}));
        assert!(node_name_matches(
            &pod(json!({ "nodeName": "target" })),
            &target
        ));
        assert!(!node_name_matches(
            &pod(json!({ "nodeName": "other" })),
            &target
        ));
        // An empty pin is no pin.
        assert!(node_name_matches(&pod(json!({ "nodeName": "" })), &target));
    }
}
