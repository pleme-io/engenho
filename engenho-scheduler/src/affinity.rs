//! POD AFFINITY, ANTI-AFFINITY AND TOPOLOGY SPREAD.
//!
//! ★ WHAT THESE BUY THAT NOTHING ELSE DOES. `fit.rs` answers capacity and
//! `predicates.rs` answers permission; neither can express a RELATIONSHIP
//! between pods. Every high-availability guarantee an operator writes is
//! one of these: "keep the three replicas on different nodes"
//! (anti-affinity), "put the cache beside the app it serves" (affinity),
//! "spread evenly across zones" (topology spread). Without them a
//! three-replica Deployment can land all three on one node and report
//! healthy right up until that node dies — the failure mode the replicas
//! existed to prevent.
//!
//! ★ TOPOLOGY IS A LABEL ON THE NODE, NOT A PROPERTY OF THE CLUSTER. Every
//! rule here is evaluated against a `topologyKey` — `kubernetes.io/hostname`
//! for per-node, `topology.kubernetes.io/zone` for per-zone. A node MISSING
//! that label is not in any domain, and upstream treats it as ineligible
//! rather than as its own domain. Treating it as a domain of one would make
//! an unlabelled node look maximally attractive to a spread rule and
//! collect every pod.
//!
//! ★ REQUIRED IS A FILTER, PREFERRED IS A SCORE, and conflating them is the
//! classic error. `requiredDuringSchedulingIgnoredDuringExecution` excludes
//! a node; `preferred…` only ranks it. A scheduler that filters on
//! `preferred` turns a soft wish into an unschedulable pod the moment no
//! node satisfies it — and the pod stays Pending forever with a message
//! that reads like a hard constraint failure.

use serde_json::Value;

/// A pod already placed on a node, reduced to what these rules need.
#[derive(Debug, Clone)]
pub struct PlacedPod<'a> {
    pub labels: &'a Value,
    /// The node it is bound to.
    pub node_name: String,
}

/// Read a node's value for a topology key.
///
/// `None` means the node carries no such label and is therefore in NO
/// domain — see the module note on why that is not "its own domain".
#[must_use]
pub fn topology_domain<'a>(node: &'a Value, topology_key: &str) -> Option<&'a str> {
    node.get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.get(topology_key))
        .and_then(Value::as_str)
}

/// Does `labels` satisfy a `labelSelector`'s `matchLabels`?
///
/// `matchExpressions` is NOT evaluated and a selector carrying one is
/// reported as unsupported by [`selector_is_supported`] rather than
/// silently treated as matching everything — a selector that matches more
/// than it should is how an anti-affinity rule quietly stops separating
/// anything.
#[must_use]
pub fn matches_match_labels(selector: &Value, labels: &Value) -> bool {
    let Some(ml) = selector.get("matchLabels").and_then(Value::as_object) else {
        // An EMPTY selector matches every pod — upstream's semantics, and
        // the shape `podAntiAffinity` uses to mean "any pod of this set".
        return true;
    };
    ml.iter()
        .all(|(k, want)| labels.get(k).is_some_and(|have| have == want))
}

/// Can this selector be evaluated faithfully?
///
/// Returning `false` for `matchExpressions` is what keeps an
/// unimplemented operator from silently widening a rule.
#[must_use]
pub fn selector_is_supported(selector: &Value) -> bool {
    selector
        .get("matchExpressions")
        .and_then(Value::as_array)
        .is_none_or(|e| e.is_empty())
}

/// One required affinity or anti-affinity term.
#[derive(Debug, Clone)]
pub struct Term<'a> {
    pub selector: &'a Value,
    pub topology_key: &'a str,
}

/// Read the REQUIRED terms of one affinity block.
///
/// `preferred…` is deliberately not read here: it belongs to scoring, and a
/// function that returned both would invite a caller to filter on it.
#[must_use]
pub fn required_terms<'a>(pod: &'a Value, kind: &str) -> Vec<Term<'a>> {
    pod.get("spec")
        .and_then(|s| s.get("affinity"))
        .and_then(|a| a.get(kind))
        .and_then(|k| k.get("requiredDuringSchedulingIgnoredDuringExecution"))
        .and_then(Value::as_array)
        .map(|terms| {
            terms
                .iter()
                .filter_map(|t| {
                    Some(Term {
                        selector: t.get("labelSelector")?,
                        topology_key: t.get("topologyKey").and_then(Value::as_str)?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Why an affinity rule rejected a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AffinityRejection {
    /// A required `podAffinity` term found no matching pod in the domain.
    AffinityUnsatisfied { topology_key: String },
    /// A required `podAntiAffinity` term found a matching pod in the domain.
    AntiAffinityViolated { topology_key: String },
    /// The rule uses a selector feature that cannot be evaluated. Refused
    /// rather than approximated: see the module note.
    UnsupportedSelector,
    /// The node carries no label for the term's `topologyKey`.
    NoTopologyDomain { topology_key: String },
}

/// Evaluate required pod affinity and anti-affinity for one candidate node.
///
/// `placed` is every pod already bound anywhere in the cluster; the domain
/// comparison is what narrows it.
#[must_use]
pub fn check_affinity(
    pod: &Value,
    node: &Value,
    placed: &[PlacedPod<'_>],
    nodes: &[(String, Value)],
) -> Option<AffinityRejection> {
    for (kind, violated_when_found) in [("podAffinity", false), ("podAntiAffinity", true)] {
        for term in required_terms(pod, kind) {
            if !selector_is_supported(term.selector) {
                return Some(AffinityRejection::UnsupportedSelector);
            }
            let Some(domain) = topology_domain(node, term.topology_key) else {
                return Some(AffinityRejection::NoTopologyDomain {
                    topology_key: term.topology_key.to_string(),
                });
            };
            // A pod counts only if it sits on a node in the SAME domain.
            let found = placed.iter().any(|p| {
                let same_domain = nodes
                    .iter()
                    .find(|(n, _)| *n == p.node_name)
                    .and_then(|(_, nv)| topology_domain(nv, term.topology_key))
                    .is_some_and(|d| d == domain);
                same_domain && matches_match_labels(term.selector, p.labels)
            });
            if found == violated_when_found {
                return Some(if violated_when_found {
                    AffinityRejection::AntiAffinityViolated {
                        topology_key: term.topology_key.to_string(),
                    }
                } else {
                    AffinityRejection::AffinityUnsatisfied {
                        topology_key: term.topology_key.to_string(),
                    }
                });
            }
        }
    }
    None
}

/// A `topologySpreadConstraint` reduced to what the check needs.
#[derive(Debug, Clone)]
pub struct SpreadConstraint<'a> {
    pub max_skew: i64,
    pub topology_key: &'a str,
    pub when_unsatisfiable: &'a str,
    pub selector: &'a Value,
}

/// Read a pod's spread constraints.
#[must_use]
pub fn spread_constraints(pod: &Value) -> Vec<SpreadConstraint<'_>> {
    pod.get("spec")
        .and_then(|s| s.get("topologySpreadConstraints"))
        .and_then(Value::as_array)
        .map(|cs| {
            cs.iter()
                .filter_map(|c| {
                    Some(SpreadConstraint {
                        // maxSkew is required and must be >= 1; a 0 or
                        // missing value would make every placement violate,
                        // so it is treated as absent rather than as zero.
                        max_skew: c
                            .get("maxSkew")
                            .and_then(Value::as_i64)
                            .filter(|n| *n >= 1)?,
                        topology_key: c.get("topologyKey").and_then(Value::as_str)?,
                        when_unsatisfiable: c
                            .get("whenUnsatisfiable")
                            .and_then(Value::as_str)
                            .unwrap_or("DoNotSchedule"),
                        selector: c.get("labelSelector").unwrap_or(&Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Would placing `pod` on `node` exceed a `DoNotSchedule` constraint's skew?
///
/// Skew is `count(this domain, after placement) − min(count over all
/// eligible domains)`. Only `DoNotSchedule` filters; `ScheduleAnyway` is a
/// scoring hint and must not exclude — the required/preferred distinction
/// again, under a different name.
#[must_use]
pub fn check_spread(
    pod: &Value,
    node: &Value,
    placed: &[PlacedPod<'_>],
    nodes: &[(String, Value)],
) -> Option<AffinityRejection> {
    for c in spread_constraints(pod) {
        if c.when_unsatisfiable != "DoNotSchedule" {
            continue;
        }
        if !selector_is_supported(c.selector) {
            return Some(AffinityRejection::UnsupportedSelector);
        }
        let Some(target) = topology_domain(node, c.topology_key) else {
            return Some(AffinityRejection::NoTopologyDomain {
                topology_key: c.topology_key.to_string(),
            });
        };

        // Count matching pods per domain, over the domains that EXIST.
        let mut counts: std::collections::BTreeMap<&str, i64> = std::collections::BTreeMap::new();
        for (_, nv) in nodes {
            if let Some(d) = topology_domain(nv, c.topology_key) {
                counts.entry(d).or_insert(0);
            }
        }
        for p in placed {
            if !matches_match_labels(c.selector, p.labels) {
                continue;
            }
            if let Some(d) = nodes
                .iter()
                .find(|(n, _)| *n == p.node_name)
                .and_then(|(_, nv)| topology_domain(nv, c.topology_key))
            {
                *counts.entry(d).or_insert(0) += 1;
            }
        }
        let after = counts.get(target).copied().unwrap_or(0) + 1;
        let min = counts.values().copied().min().unwrap_or(0);
        if after - min > c.max_skew {
            return Some(AffinityRejection::AntiAffinityViolated {
                topology_key: c.topology_key.to_string(),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(name: &str, zone: Option<&str>) -> (String, Value) {
        let labels = match zone {
            Some(z) => json!({ "kubernetes.io/hostname": name, "topology.kubernetes.io/zone": z }),
            None => json!({ "kubernetes.io/hostname": name }),
        };
        (
            name.to_string(),
            json!({ "metadata": { "name": name, "labels": labels } }),
        )
    }

    fn unlabelled(name: &str) -> (String, Value) {
        (name.to_string(), json!({ "metadata": { "name": name } }))
    }

    const APP: &str = "app";

    fn placed<'a>(labels: &'a Value, node: &str) -> PlacedPod<'a> {
        PlacedPod {
            labels,
            node_name: node.to_string(),
        }
    }

    fn anti_affinity(topology_key: &str) -> Value {
        json!({ "spec": { "affinity": { "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                { "labelSelector": { "matchLabels": { APP: "web" } },
                  "topologyKey": topology_key }
            ]
        } } } })
    }

    #[test]
    fn a_bare_pod_is_unaffected() {
        // Anti-vacuity: a checker that rejected everything would pass every
        // negative test below.
        let (_, n) = node("n1", Some("a"));
        assert_eq!(check_affinity(&json!({}), &n, &[], &[]), None);
        assert_eq!(check_spread(&json!({}), &n, &[], &[]), None);
    }

    #[test]
    fn anti_affinity_keeps_replicas_off_the_same_node() {
        // The guarantee: three replicas on one node report healthy right up
        // until that node dies.
        let nodes = vec![node("n1", Some("a")), node("n2", Some("a"))];
        let web = json!({ APP: "web" });
        let existing = vec![placed(&web, "n1")];
        let pod = anti_affinity("kubernetes.io/hostname");

        assert!(matches!(
            check_affinity(&pod, &nodes[0].1, &existing, &nodes),
            Some(AffinityRejection::AntiAffinityViolated { .. })
        ));
        // n2 is free — same zone, different host.
        assert_eq!(check_affinity(&pod, &nodes[1].1, &existing, &nodes), None);
    }

    #[test]
    fn the_topology_key_decides_the_domain() {
        // Identical rule, different key: per-zone anti-affinity excludes n2
        // that per-host anti-affinity allowed.
        let nodes = vec![node("n1", Some("a")), node("n2", Some("a"))];
        let web = json!({ APP: "web" });
        let existing = vec![placed(&web, "n1")];
        let by_zone = anti_affinity("topology.kubernetes.io/zone");
        assert!(matches!(
            check_affinity(&by_zone, &nodes[1].1, &existing, &nodes),
            Some(AffinityRejection::AntiAffinityViolated { .. })
        ));
    }

    #[test]
    fn affinity_requires_a_match_in_the_domain() {
        let nodes = vec![node("n1", Some("a")), node("n2", Some("b"))];
        let cache = json!({ APP: "cache" });
        let existing = vec![placed(&cache, "n1")];
        let pod = json!({ "spec": { "affinity": { "podAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                { "labelSelector": { "matchLabels": { APP: "cache" } },
                  "topologyKey": "kubernetes.io/hostname" }
            ]
        } } } });
        // Beside the cache: allowed. Elsewhere: refused.
        assert_eq!(check_affinity(&pod, &nodes[0].1, &existing, &nodes), None);
        assert!(matches!(
            check_affinity(&pod, &nodes[1].1, &existing, &nodes),
            Some(AffinityRejection::AffinityUnsatisfied { .. })
        ));
    }

    #[test]
    fn a_node_missing_the_topology_label_is_in_no_domain() {
        // Treating it as its own domain would make it maximally attractive
        // to a spread rule and collect every pod.
        let nodes = vec![node("n1", Some("a")), unlabelled("n2")];
        let pod = anti_affinity("topology.kubernetes.io/zone");
        assert_eq!(
            check_affinity(&pod, &nodes[1].1, &[], &nodes),
            Some(AffinityRejection::NoTopologyDomain {
                topology_key: "topology.kubernetes.io/zone".to_string()
            })
        );
    }

    #[test]
    fn preferred_terms_are_not_read_as_filters() {
        // Filtering on `preferred` turns a soft wish into a pod that stays
        // Pending forever with a message that reads like a hard failure.
        let pod = json!({ "spec": { "affinity": { "podAntiAffinity": {
            "preferredDuringSchedulingIgnoredDuringExecution": [
                { "weight": 100, "podAffinityTerm": {
                    "labelSelector": { "matchLabels": { APP: "web" } },
                    "topologyKey": "kubernetes.io/hostname" } }
            ]
        } } } });
        assert!(required_terms(&pod, "podAntiAffinity").is_empty());
        let nodes = vec![node("n1", Some("a"))];
        let web = json!({ APP: "web" });
        assert_eq!(
            check_affinity(&pod, &nodes[0].1, &[placed(&web, "n1")], &nodes),
            None,
            "a preferred term must never exclude"
        );
    }

    #[test]
    fn an_unevaluable_selector_is_refused_not_widened() {
        // A selector that matches MORE than it should is how an
        // anti-affinity rule quietly stops separating anything.
        let pod = json!({ "spec": { "affinity": { "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                { "labelSelector": { "matchExpressions": [
                    { "key": APP, "operator": "In", "values": ["web"] } ] },
                  "topologyKey": "kubernetes.io/hostname" }
            ]
        } } } });
        let nodes = vec![node("n1", Some("a"))];
        assert_eq!(
            check_affinity(&pod, &nodes[0].1, &[], &nodes),
            Some(AffinityRejection::UnsupportedSelector)
        );
    }

    #[test]
    fn an_empty_selector_matches_every_pod() {
        // Upstream's semantics, and the shape podAntiAffinity uses to mean
        // "any pod of this set".
        assert!(matches_match_labels(
            &json!({}),
            &json!({ APP: "anything" })
        ));
    }

    // ── topology spread ───────────────────────────────────────────────

    fn spread(max_skew: i64, when: &str) -> Value {
        json!({ "spec": { "topologySpreadConstraints": [
            { "maxSkew": max_skew,
              "topologyKey": "topology.kubernetes.io/zone",
              "whenUnsatisfiable": when,
              "labelSelector": { "matchLabels": { APP: "web" } } }
        ] } })
    }

    #[test]
    fn spread_refuses_a_placement_that_would_exceed_max_skew() {
        let nodes = vec![node("n1", Some("a")), node("n2", Some("b"))];
        let web = json!({ APP: "web" });
        // Zone a already has one, zone b has none. maxSkew 1 ⇒ placing a
        // second in a gives skew 2, which exceeds it.
        let existing = vec![placed(&web, "n1")];
        assert!(matches!(
            check_spread(&spread(1, "DoNotSchedule"), &nodes[0].1, &existing, &nodes),
            Some(AffinityRejection::AntiAffinityViolated { .. })
        ));
        // Zone b balances it.
        assert_eq!(
            check_spread(&spread(1, "DoNotSchedule"), &nodes[1].1, &existing, &nodes),
            None
        );
    }

    #[test]
    fn schedule_anyway_never_filters() {
        // The required/preferred distinction again, under another name.
        let nodes = vec![node("n1", Some("a")), node("n2", Some("b"))];
        let web = json!({ APP: "web" });
        let existing = vec![placed(&web, "n1"), placed(&web, "n1")];
        assert_eq!(
            check_spread(&spread(1, "ScheduleAnyway"), &nodes[0].1, &existing, &nodes),
            None
        );
    }

    #[test]
    fn a_zero_or_missing_max_skew_is_treated_as_absent() {
        // maxSkew must be >= 1; a 0 would make EVERY placement violate.
        assert!(spread_constraints(&spread(0, "DoNotSchedule")).is_empty());
        let no_skew = json!({ "spec": { "topologySpreadConstraints": [
            { "topologyKey": "topology.kubernetes.io/zone" } ] } });
        assert!(spread_constraints(&no_skew).is_empty());
    }

    #[test]
    fn pods_that_do_not_match_the_selector_are_not_counted() {
        // Counting unrelated pods would spread against the wrong set and
        // refuse placements for no reason.
        let nodes = vec![node("n1", Some("a")), node("n2", Some("b"))];
        let other = json!({ APP: "batch" });
        let existing = vec![placed(&other, "n1"), placed(&other, "n1")];
        assert_eq!(
            check_spread(&spread(1, "DoNotSchedule"), &nodes[0].1, &existing, &nodes),
            None
        );
    }
}
