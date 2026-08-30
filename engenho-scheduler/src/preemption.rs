//! PREEMPTION — making room for a pod that matters more.
//!
//! ★ WHAT IT BUYS. Without preemption a cluster at capacity schedules
//! strictly first-come-first-served: a batch job that arrived first holds a
//! node against a control-plane pod that arrives second, and the important
//! workload stays Pending until something unrelated finishes. Priority
//! classes exist precisely to break that tie, and a `priorityClassName` the
//! scheduler never reads is a promise the cluster does not keep.
//!
//! ★ PREEMPTION IS A LAST RESORT, and the ordering here enforces it. A
//! candidate node is only considered for preemption once ordinary
//! scheduling has failed everywhere — evicting a running pod to place one
//! that would have fit elsewhere is strictly worse than waiting a moment,
//! because the eviction is immediate and irreversible while the wait is
//! neither.
//!
//! ★ IT ONLY EVER EVICTS STRICTLY LOWER PRIORITY. Equal priority does NOT
//! preempt: two pods of the same class have no ordering between them, and
//! allowing it would let a cluster thrash — each new pod evicting its
//! predecessor forever, making progress for nobody. This is the single most
//! important rule in the module and it is the one a naive `>=` gets wrong.
//!
//! ★ THE VICTIM SET IS MINIMAL AND ITS ORDER IS DEFINED. Victims are chosen
//! lowest-priority-first, and only as many as are needed to fit. Evicting
//! more than necessary is destroyed work that bought nothing; choosing them
//! in an undefined order makes the same cluster state produce different
//! outcomes on different ticks, which is untestable and unexplainable to an
//! operator reading the events afterwards.

use serde_json::Value;

use crate::fit::{NodeResources, PodRequests, pod_requests};

/// A pod already running on the candidate node.
#[derive(Debug, Clone)]
pub struct Victim<'a> {
    pub pod: &'a Value,
    pub priority: i64,
    pub requests: PodRequests,
}

/// The priority a pod is scheduled at.
///
/// Upstream resolves `spec.priorityClassName` to a number at admission and
/// writes it into `spec.priority`; a pod without one is priority 0. Reading
/// the resolved NUMBER rather than the class name is deliberate — the name
/// is a reference that may not resolve, and a scheduler that re-resolved it
/// could disagree with the value admission already stamped.
#[must_use]
pub fn priority_of(pod: &Value) -> i64 {
    pod.get("spec")
        .and_then(|s| s.get("priority"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// Is this pod exempt from being preempted?
///
/// A pod whose own `preemptionPolicy` is `Never` still gets preempted —
/// that field governs whether it PREEMPTS OTHERS, not whether it can be a
/// victim. Conflating the two would make a low-priority batch pod
/// un-evictable simply for declining to evict anyone itself, which is the
/// opposite of what the field means.
#[must_use]
pub fn is_preemption_exempt(pod: &Value) -> bool {
    // A pod already terminating is not a useful victim: evicting it frees
    // nothing that was not already being freed.
    pod.get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .is_some()
}

/// May `pod` preempt at all?
///
/// `preemptionPolicy: Never` means "schedule me by priority, but do not
/// evict anyone for me" — it is how a high-priority pod says it would
/// rather wait than destroy work.
#[must_use]
pub fn may_preempt(pod: &Value) -> bool {
    pod.get("spec")
        .and_then(|s| s.get("preemptionPolicy"))
        .and_then(Value::as_str)
        != Some("Never")
}

/// The outcome of a preemption attempt on one node.
#[derive(Debug, Clone, PartialEq)]
pub enum PreemptionPlan {
    /// Evicting these pods, in this order, makes room.
    Evict(Vec<String>),
    /// Nothing on this node can be evicted to make room.
    Infeasible,
}

/// Plan the minimal eviction that would let `incoming` fit on a node.
///
/// `free` is the node's currently-free capacity; `running` is every pod
/// bound there.
#[must_use]
pub fn plan(incoming: &Value, free: &NodeResources, running: &[Victim<'_>]) -> PreemptionPlan {
    if !may_preempt(incoming) {
        return PreemptionPlan::Infeasible;
    }
    let need = pod_requests(incoming);
    if need.unparseable {
        // An unparseable request can never be shown to fit, so evicting for
        // it would destroy work for a placement that might still fail.
        return PreemptionPlan::Infeasible;
    }
    let my_priority = priority_of(incoming);

    // Only strictly-lower-priority, non-exempt pods are candidates.
    let mut candidates: Vec<&Victim<'_>> = running
        .iter()
        .filter(|v| v.priority < my_priority && !is_preemption_exempt(v.pod))
        .collect();

    // Lowest priority first; ties broken by name so the same cluster state
    // always yields the same plan.
    candidates.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| pod_name(a.pod).cmp(&pod_name(b.pod)))
    });

    let mut cpu = free.cpu_milli;
    let mut mem = free.mem_milli;
    let mut evicted = Vec::new();
    for v in candidates {
        if cpu >= need.cpu_milli && mem >= need.mem_milli {
            break; // already enough — evict no more than necessary
        }
        cpu += v.requests.cpu_milli;
        mem += v.requests.mem_milli;
        evicted.push(pod_name(v.pod));
    }

    if cpu >= need.cpu_milli && mem >= need.mem_milli {
        PreemptionPlan::Evict(evicted)
    } else {
        // Even evicting everything eligible does not make room. Reporting
        // Infeasible rather than a partial eviction matters: a partial one
        // destroys work and STILL leaves the pod Pending.
        PreemptionPlan::Infeasible
    }
}

fn pod_name(pod: &Value) -> String {
    pod.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(name: &str, priority: Option<i64>, cpu: &str, mem: &str) -> Value {
        let mut p = json!({
            "metadata": { "name": name },
            "spec": { "containers": [ { "name": "c", "image": "i",
                "resources": { "requests": { "cpu": cpu, "memory": mem } } } ] }
        });
        if let Some(pr) = priority {
            p["spec"]["priority"] = json!(pr);
        }
        p
    }

    fn victim<'a>(p: &'a Value) -> Victim<'a> {
        Victim {
            pod: p,
            priority: priority_of(p),
            requests: pod_requests(p),
        }
    }

    fn free(cpu_milli: i128, mem_milli: i128) -> NodeResources {
        NodeResources {
            cpu_milli,
            mem_milli,
        }
    }

    #[test]
    fn a_pod_that_already_fits_needs_no_eviction() {
        // Anti-vacuity, and the last-resort rule: this module must never be
        // the reason a pod that would have fit loses a neighbour.
        let incoming = pod("new", Some(100), "1", "1Gi");
        let low = pod("low", Some(1), "1", "1Gi");
        assert_eq!(
            // mem_milli is milli-BYTES, so 4Gi is 4 * 1024^3 * 1000.
            plan(
                &incoming,
                &free(2000, 4 * 1024 * 1024 * 1024 * 1000),
                &[victim(&low)]
            ),
            PreemptionPlan::Evict(vec![]),
            "room already exists ⇒ an EMPTY eviction set, not a victim"
        );
    }

    #[test]
    fn equal_priority_never_preempts() {
        // THE rule a naive `>=` gets wrong. Allowing it lets a cluster
        // thrash: each new pod evicts its predecessor forever and nobody
        // makes progress.
        let incoming = pod("new", Some(100), "2", "1Gi");
        let peer = pod("peer", Some(100), "2", "1Gi");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&peer)]),
            PreemptionPlan::Infeasible
        );
    }

    #[test]
    fn only_strictly_lower_priority_pods_are_victims() {
        let incoming = pod("new", Some(100), "1", "0");
        let higher = pod("higher", Some(1000), "4", "0");
        let lower = pod("lower", Some(1), "4", "0");
        // The higher-priority pod is not a candidate even though evicting
        // it would free plenty.
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&higher)]),
            PreemptionPlan::Infeasible
        );
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&lower)]),
            PreemptionPlan::Evict(vec!["lower".to_string()])
        );
    }

    #[test]
    fn the_victim_set_is_minimal() {
        // Evicting more than necessary is destroyed work that bought
        // nothing.
        let incoming = pod("new", Some(100), "1", "0");
        let a = pod("a", Some(1), "1", "0");
        let b = pod("b", Some(1), "1", "0");
        let c = pod("c", Some(1), "1", "0");
        match plan(
            &incoming,
            &free(0, 0),
            &[victim(&a), victim(&b), victim(&c)],
        ) {
            PreemptionPlan::Evict(v) => assert_eq!(v.len(), 1, "one is enough, got {v:?}"),
            other => panic!("expected an eviction, got {other:?}"),
        }
    }

    #[test]
    fn victims_are_chosen_lowest_priority_first_and_deterministically() {
        // An undefined order makes the same cluster state produce different
        // outcomes on different ticks — untestable, and unexplainable to an
        // operator reading the events afterwards.
        let incoming = pod("new", Some(100), "1", "0");
        let mid = pod("mid", Some(50), "1", "0");
        let low = pod("low", Some(1), "1", "0");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&mid), victim(&low)]),
            PreemptionPlan::Evict(vec!["low".to_string()]),
            "the cheapest victim must be chosen"
        );
        // Ties break by name, both orders of the input.
        let a = pod("a", Some(1), "1", "0");
        let z = pod("z", Some(1), "1", "0");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&z), victim(&a)]),
            plan(&incoming, &free(0, 0), &[victim(&a), victim(&z)])
        );
    }

    #[test]
    fn preemption_policy_never_declines_to_evict_anyone() {
        // How a high-priority pod says it would rather wait than destroy
        // work.
        let mut incoming = pod("new", Some(1000), "1", "0");
        incoming["spec"]["preemptionPolicy"] = json!("Never");
        let low = pod("low", Some(1), "4", "0");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&low)]),
            PreemptionPlan::Infeasible
        );
    }

    #[test]
    fn a_victims_own_preemption_policy_does_not_protect_it() {
        // preemptionPolicy governs whether a pod PREEMPTS OTHERS, not
        // whether it can be a victim. Conflating them would make a
        // low-priority batch pod un-evictable for declining to evict
        // anyone itself — the opposite of what the field means.
        let incoming = pod("new", Some(100), "1", "0");
        let mut low = pod("low", Some(1), "4", "0");
        low["spec"]["preemptionPolicy"] = json!("Never");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&low)]),
            PreemptionPlan::Evict(vec!["low".to_string()])
        );
    }

    #[test]
    fn a_terminating_pod_is_not_a_useful_victim() {
        // Evicting it frees nothing that was not already being freed.
        let incoming = pod("new", Some(100), "1", "0");
        let mut dying = pod("dying", Some(1), "4", "0");
        dying["metadata"]["deletionTimestamp"] = json!("2026-08-29T21:00:00Z");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&dying)]),
            PreemptionPlan::Infeasible
        );
    }

    #[test]
    fn insufficient_room_even_after_evicting_everything_is_infeasible() {
        // A PARTIAL eviction destroys work and STILL leaves the pod
        // Pending — the worst of both outcomes.
        let incoming = pod("new", Some(100), "8", "0");
        let low = pod("low", Some(1), "1", "0");
        assert_eq!(
            plan(&incoming, &free(0, 0), &[victim(&low)]),
            PreemptionPlan::Infeasible
        );
    }

    #[test]
    fn a_pod_with_no_priority_field_is_priority_zero() {
        // Upstream's default, and it means an unprioritised pod cannot
        // preempt another unprioritised one.
        let plain = pod("plain", None, "1", "0");
        assert_eq!(priority_of(&plain), 0);
        let other = pod("other", None, "4", "0");
        assert_eq!(
            plan(&plain, &free(0, 0), &[victim(&other)]),
            PreemptionPlan::Infeasible
        );
    }
}
