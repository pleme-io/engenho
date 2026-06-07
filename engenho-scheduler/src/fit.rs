//! Typed resource-fit predicate — the **Filter** stage that runs in
//! front of the [`crate::strategy::SchedulingStrategy`] **Score** stage,
//! mirroring upstream kube-scheduler's `Filter (Predicates) → Score`
//! split.
//!
//! ## What it does
//!
//! Given a pending Pod's resource **requests** (cpu/memory) and a Node's
//! **free** capacity (`allocatable − Σ requests of already-bound pods`),
//! decide whether the Pod fits. The scheduler binds only to nodes that
//! pass this predicate — never to a node that can't hold the Pod, never
//! overcommitting a node.
//!
//! ## All math goes through the typed [`Quantity`] surface
//!
//! Every cpu/memory string (`100m`, `1Gi`, …) is parsed with
//! [`Quantity::from_str`] and reduced to its canonical milli-value
//! ([`Quantity::milli_value`]) — cpu in milli-cores, memory in
//! milli-bytes (×1000). There is NO hand-rolled `m`/`Ki`/`Gi` suffix
//! parsing here; per the org-level TYPED-EMISSION rule, the typed
//! `Quantity` border is THE place that knowledge lives. The scheduler is
//! the first consumer of that border.
//!
//! ## Zero-on-absent policy (safe by construction)
//!
//! A Node whose `status.allocatable` is missing a dimension is treated
//! as **zero free** for that dimension (after falling back to
//! `status.capacity` if present). The consequence: an un-sized node
//! cannot fit a Pod that requests cpu/memory. This forbids overcommit
//! by construction — the alternative (treat-absent-as-infinite) would
//! silently bind Pods onto nodes with unknown capacity. The companion
//! runtime fix (`register_node` writes `status.allocatable`) is what
//! keeps the single-node convergence path green under this policy.
//!
//! ## Unparseable request → un-fittable (never silently zero)
//!
//! If any container's request quantity is unparseable (`Quantity::Other`,
//! `milli_value() == None`), the whole Pod is reported un-fittable via
//! [`PodRequests::unparseable`] rather than silently treating the
//! request as 0. A silent-zero would let a malformed Pod schedule
//! anywhere — the TYPED-SPEC anti-stub rule forbids the silent wrong
//! answer.

use std::str::FromStr;

use engenho_types::primitives::Quantity;
use serde_json::Value;

/// A Node's free compute capacity, in canonical milli-units.
///
/// `cpu_milli` is milli-cores (`1` core = `1000`); `mem_milli` is
/// milli-bytes (`1` byte = `1000`, matching [`Quantity::milli_value`]'s
/// `×1000` convention).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeResources {
    /// Free cpu, milli-cores.
    pub cpu_milli: i128,
    /// Free memory, milli-bytes.
    pub mem_milli: i128,
}

/// A Pod's summed resource requests, in canonical milli-units.
///
/// `unparseable` latches `true` if any container request quantity could
/// not be parsed; such a Pod fits NO node (see module docs).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PodRequests {
    /// Summed cpu request, milli-cores.
    pub cpu_milli: i128,
    /// Summed memory request, milli-bytes.
    pub mem_milli: i128,
    /// `true` if any container request quantity was unparseable.
    pub unparseable: bool,
}

impl PodRequests {
    /// Whether this Pod requested any cpu or memory at all.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.cpu_milli == 0 && self.mem_milli == 0 && !self.unparseable
    }
}

/// Parse one JSON resource value (`"100m"`, `"1Gi"`, …) into its
/// canonical milli-value via the typed [`Quantity`] surface. Returns
/// `None` for a missing key OR an unparseable quantity (`Quantity::Other`).
fn quantity_milli(map: Option<&Value>, key: &str) -> Option<i128> {
    let s = map?.get(key)?.as_str()?;
    Quantity::from_str(s).ok().and_then(|q| q.milli_value())
}

/// Read a Node's free-at-rest allocatable resources from its JSON.
///
/// Policy (see module docs): a dimension absent from `status.allocatable`
/// falls back to `status.capacity`; absent from BOTH → **zero free** for
/// that dimension. This forbids overcommit onto un-sized nodes by
/// construction.
#[must_use]
pub fn node_allocatable(node: &Value) -> NodeResources {
    let status = node.get("status");
    let allocatable = status.and_then(|s| s.get("allocatable"));
    let capacity = status.and_then(|s| s.get("capacity"));

    let read = |key: &str| -> i128 {
        quantity_milli(allocatable, key)
            .or_else(|| quantity_milli(capacity, key))
            .unwrap_or(0)
    };

    NodeResources {
        cpu_milli: read("cpu"),
        mem_milli: read("memory"),
    }
}

/// Sum a Pod's container resource requests from its JSON.
///
/// Sums `spec.containers[].resources.requests["cpu"|"memory"]` across all
/// regular containers via the typed [`Quantity`] surface. An unparseable
/// request quantity latches `unparseable = true` (the Pod then fits no
/// node).
///
/// ## init-container semantics (deferred for M0.1)
///
/// Upstream K8s computes a Pod's effective request as
/// `max(Σ init-container requests, Σ regular-container requests)` per
/// dimension. M0.1 sums regular containers only; init-container max-in is
/// a documented follow-up (`spec.initContainers` is not consulted here).
/// Single-node M0.1 workloads (podinfo) have no init containers, so this
/// is correct for the convergence path and conservative otherwise.
#[must_use]
pub fn pod_requests(pod: &Value) -> PodRequests {
    let mut req = PodRequests::default();
    let Some(containers) = pod
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(Value::as_array)
    else {
        return req;
    };

    for container in containers {
        let requests = container.get("resources").and_then(|r| r.get("requests"));
        let Some(requests) = requests else {
            continue;
        };

        // cpu
        if let Some(v) = requests.get("cpu") {
            match v.as_str().and_then(|s| Quantity::from_str(s).ok()) {
                Some(q) => match q.milli_value() {
                    Some(milli) => req.cpu_milli = req.cpu_milli.saturating_add(milli),
                    None => req.unparseable = true,
                },
                None => req.unparseable = true,
            }
        }
        // memory
        if let Some(v) = requests.get("memory") {
            match v.as_str().and_then(|s| Quantity::from_str(s).ok()) {
                Some(q) => match q.milli_value() {
                    Some(milli) => req.mem_milli = req.mem_milli.saturating_add(milli),
                    None => req.unparseable = true,
                },
                None => req.unparseable = true,
            }
        }
    }

    req
}

/// Does `free` capacity hold `req`? Per-dimension AND: cpu free ≥ cpu
/// request AND memory free ≥ memory request. An unparseable request
/// never fits.
#[must_use]
pub fn fits(free: &NodeResources, req: &PodRequests) -> bool {
    if req.unparseable {
        return false;
    }
    free.cpu_milli >= req.cpu_milli && free.mem_milli >= req.mem_milli
}

/// Free capacity on a node = `allocatable − Σ requests of pods already
/// bound there`. Saturating subtraction — free never wraps negative even
/// if a node is (already) overcommitted from a prior binding.
#[must_use]
pub fn free_on_node(node: &Value, bound_pods_on_node: &[&Value]) -> NodeResources {
    let mut free = node_allocatable(node);
    for pod in bound_pods_on_node {
        let req = pod_requests(pod);
        free.cpu_milli = free.cpu_milli.saturating_sub(req.cpu_milli);
        free.mem_milli = free.mem_milli.saturating_sub(req.mem_milli);
    }
    NodeResources {
        cpu_milli: free.cpu_milli.max(0),
        mem_milli: free.mem_milli.max(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(cpu: &str, mem: &str) -> Value {
        json!({
            "kind": "Node",
            "metadata": { "name": "n" },
            "status": { "allocatable": { "cpu": cpu, "memory": mem } }
        })
    }

    fn pod_with(reqs: &[(&str, &str)]) -> Value {
        let containers: Vec<Value> = reqs
            .iter()
            .map(|(cpu, mem)| {
                json!({
                    "name": "c",
                    "resources": { "requests": { "cpu": cpu, "memory": mem } }
                })
            })
            .collect();
        json!({ "spec": { "containers": containers } })
    }

    #[test]
    fn pod_requests_sums_containers() {
        // 100m + 250m cpu = 350 milli-cores; 64Mi + 128Mi mem = 192Mi.
        let pod = pod_with(&[("100m", "64Mi"), ("250m", "128Mi")]);
        let req = pod_requests(&pod);
        // cpu: 100m=100 milli, 250m=250 milli → 350.
        assert_eq!(req.cpu_milli, 350);
        // mem: 64Mi + 128Mi = 192Mi, in milli-bytes (×1000).
        let mi = 1024i128 * 1024;
        assert_eq!(req.mem_milli, 192 * mi * 1000);
        assert!(!req.unparseable);
    }

    #[test]
    fn node_free_subtracts_bound_requests() {
        // Node: cpu 2 cores, memory 4Gi.
        let n = node("2", "4Gi");
        // Two bound pods, each 500m cpu / 1Gi mem.
        let p1 = pod_with(&[("500m", "1Gi")]);
        let p2 = pod_with(&[("500m", "1Gi")]);
        let bound = [&p1, &p2];
        let free = free_on_node(&n, &bound);
        // cpu: 2000 − 500 − 500 = 1000 (= 1 core).
        assert_eq!(free.cpu_milli, 1000);
        // mem: 4Gi − 1Gi − 1Gi = 2Gi.
        let gi = 1i128 << 30;
        assert_eq!(free.mem_milli, 2 * gi * 1000);
    }

    #[test]
    fn fits_true_when_free_ge_request() {
        let free = NodeResources {
            cpu_milli: 1000,
            mem_milli: (1i128 << 30) * 1000,
        };
        let req = pod_requests(&pod_with(&[("500m", "512Mi")]));
        assert!(fits(&free, &req));
    }

    #[test]
    fn fits_false_when_either_dimension_short() {
        let gi = (1i128 << 30) * 1000;
        let free = NodeResources {
            cpu_milli: 1000,
            mem_milli: gi,
        };
        // Short on cpu only.
        let cpu_short = pod_requests(&pod_with(&[("2", "512Mi")]));
        assert!(!fits(&free, &cpu_short), "cpu-short pod must not fit");
        // Short on memory only.
        let mem_short = pod_requests(&pod_with(&[("500m", "4Gi")]));
        assert!(!fits(&free, &mem_short), "mem-short pod must not fit");
    }

    #[test]
    fn node_with_absent_allocatable_fits_nothing() {
        // No status.allocatable at all → zero free in both dimensions.
        let n = json!({ "kind": "Node", "metadata": { "name": "x" } });
        let free = node_allocatable(&n);
        assert_eq!(free, NodeResources::default());
        // A pod requesting even 1m cpu does NOT fit.
        let req = pod_requests(&pod_with(&[("1m", "0")]));
        assert!(!fits(&free, &req));
    }

    #[test]
    fn capacity_fallback_when_allocatable_absent() {
        let n = json!({
            "kind": "Node",
            "status": { "capacity": { "cpu": "4", "memory": "8Gi" } }
        });
        let free = node_allocatable(&n);
        assert_eq!(free.cpu_milli, 4000);
        assert_eq!(free.mem_milli, 8 * (1i128 << 30) * 1000);
    }

    #[test]
    fn unparseable_request_quantity_is_unfittable() {
        // A garbage cpu quantity → Quantity::Other → milli_value()==None.
        let pod = json!({
            "spec": { "containers": [{
                "name": "c",
                "resources": { "requests": { "cpu": "1.5e-3", "memory": "0" } }
            }] }
        });
        let req = pod_requests(&pod);
        assert!(req.unparseable, "unparseable cpu must latch unparseable");
        // Even a huge node cannot fit it.
        let free = NodeResources {
            cpu_milli: i128::MAX / 2,
            mem_milli: i128::MAX / 2,
        };
        assert!(!fits(&free, &req));
    }

    #[test]
    fn free_on_node_saturates_at_zero() {
        // Allocatable 1 core; a bound pod requesting 2 cores already
        // overcommits — free must clamp to 0, not wrap negative.
        let n = node("1", "1Gi");
        let big = pod_with(&[("2", "0")]);
        let bound = [&big];
        let free = free_on_node(&n, &bound);
        assert_eq!(free.cpu_milli, 0);
    }

    #[test]
    fn pod_with_no_containers_requests_zero() {
        let pod = json!({ "spec": {} });
        let req = pod_requests(&pod);
        assert!(req.is_zero());
    }
}
