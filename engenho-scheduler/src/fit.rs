//! Typed resource-fit predicate — the **Filter** stage that runs in
//! front of the [`crate::strategy::SchedulingStrategy`] **Score** stage,
//! mirroring upstream kube-scheduler's `Filter (Predicates) → Score`
//! split.
//!
//! ## What it does
//!
//! Given a pending Pod's effective resource **requests** (cpu/memory, see
//! [`pod_requests`]) and a Node's **free** capacity, decide whether the Pod
//! fits. Free capacity is kept by one ledger,
//! [`crate::ledger::NodeLedger`]: allocatable minus the requests of every
//! bound pod that still holds capacity. The scheduler binds only to nodes
//! that pass this predicate — never to a node that can't hold the Pod,
//! never overcommitting a node.
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
//! If any request quantity is unparseable (`Quantity::Other`,
//! `milli_value() == None`) or negative, or an init container carries a
//! `restartPolicy` other than `Always`, the whole Pod is reported
//! un-fittable via [`PodRequests::unparseable`] rather than silently
//! treating the request as 0. A silent-zero would let a malformed Pod
//! schedule anywhere — the TYPED-SPEC anti-stub rule forbids the silent
//! wrong answer.

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
/// `unparseable` latches `true` if any request could not be read (see
/// [`pod_requests`]); such a Pod fits NO node (see module docs).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PodRequests {
    /// Summed cpu request, milli-cores.
    pub cpu_milli: i128,
    /// Summed memory request, milli-bytes.
    pub mem_milli: i128,
    /// `true` if any request, or an init container's role, was unreadable.
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

/// A cpu/memory pair in milli-units, the arithmetic of [`pod_requests`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Pair {
    cpu: i128,
    mem: i128,
}

impl Pair {
    fn plus(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_add(other.cpu),
            mem: self.mem.saturating_add(other.mem),
        }
    }

    /// Per-dimension maximum — upstream's `maxResourceList`.
    fn max_each(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.max(other.cpu),
            mem: self.mem.max(other.mem),
        }
    }
}

/// How an init container runs, read from `initContainers[i].restartPolicy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InitRole {
    /// Absent: runs to completion before the next one starts.
    Plain,
    /// `Always`: a native sidecar (KEP-753) that keeps running beside the
    /// app containers.
    Sidecar,
}

/// Reads request quantities, latching `malformed` on the first value it
/// cannot read as a non-negative quantity.
#[derive(Default)]
struct RequestReader {
    malformed: bool,
}

impl RequestReader {
    /// One quantity. Strings and JSON numbers are both quantities on the
    /// wire. Anything else, an unparseable string, or a negative value is
    /// malformed and contributes zero: a request can never credit a node.
    fn quantity(&mut self, v: &Value) -> i128 {
        let parsed = match v {
            Value::String(s) => Quantity::from_str(s).ok(),
            Value::Number(n) => Quantity::from_str(&n.to_string()).ok(),
            _ => None,
        }
        .and_then(|q| q.milli_value());
        match parsed {
            Some(milli) if milli >= 0 => milli,
            _ => {
                self.malformed = true;
                0
            }
        }
    }

    /// The `cpu` and `memory` entries of a `ResourceList`; an absent entry
    /// is zero.
    fn list(&mut self, list: Option<&Value>) -> Pair {
        let mut read = |key: &str| {
            list.and_then(|l| l.get(key))
                .map_or(0, |v| self.quantity(v))
        };
        Pair {
            cpu: read("cpu"),
            mem: read("memory"),
        }
    }

    /// A container's `resources.requests`.
    fn container(&mut self, c: &Value) -> Pair {
        self.list(c.pointer("/resources/requests"))
    }

    /// An init container's role. A `restartPolicy` other than `Always` is
    /// rejected upstream; here it is malformed, and it is charged as a
    /// sidecar, which is never less than the plain reading.
    fn init_role(&mut self, c: &Value) -> InitRole {
        match c.get("restartPolicy") {
            None | Some(Value::Null) => InitRole::Plain,
            Some(Value::String(s)) if s == "Always" => InitRole::Sidecar,
            Some(_) => {
                self.malformed = true;
                InitRole::Sidecar
            }
        }
    }
}

/// A Pod's effective resource requests — what it occupies on its node.
///
/// Follows upstream's `PodRequests` (k8s.io/component-helpers/resource),
/// per dimension:
///
/// 1. Sum the app containers' requests.
/// 2. Walk `spec.initContainers` in order, keeping a running sum of the
///    sidecars (`restartPolicy: Always`) seen so far:
///    - a **sidecar** keeps running beside the app containers, so it is
///      added to the app total and to the sidecar sum; while it starts,
///      the pod uses the sidecar sum including it;
///    - a **plain** init container runs alone with the sidecars started
///      before it, so while it runs the pod uses its own request plus the
///      sidecar sum.
/// 3. The pod's request is the maximum of the app total and the largest
///    use seen during initialization. Plain init containers therefore
///    contribute their maximum, not their sum.
/// 4. Pod-level `spec.resources.requests` (KEP-2837), where it names cpu or
///    memory, replaces the container aggregate for that dimension.
/// 5. `spec.overhead` is added last.
///
/// A quantity that cannot be read as a non-negative number, or an init
/// `restartPolicy` other than `Always`, latches `unparseable = true`: a
/// pending Pod then fits no node. The value still carries the best reading
/// of what could be read, which is what a bound Pod is charged.
#[must_use]
pub fn pod_requests(pod: &Value) -> PodRequests {
    let mut reader = RequestReader::default();
    let spec = pod.get("spec");
    let listed = |key: &str| {
        spec.and_then(|s| s.get(key))
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice)
    };

    let mut total = listed("containers")
        .iter()
        .fold(Pair::default(), |acc, c| acc.plus(reader.container(c)));

    let mut sidecars = Pair::default();
    let mut init_peak = Pair::default();
    for c in listed("initContainers") {
        let own = reader.container(c);
        let during = match reader.init_role(c) {
            InitRole::Sidecar => {
                total = total.plus(own);
                sidecars = sidecars.plus(own);
                sidecars
            }
            InitRole::Plain => own.plus(sidecars),
        };
        init_peak = init_peak.max_each(during);
    }
    let mut effective = total.max_each(init_peak);

    if let Some(pod_level) = spec.and_then(|s| s.pointer("/resources/requests")) {
        if let Some(cpu) = pod_level.get("cpu") {
            effective.cpu = reader.quantity(cpu);
        }
        if let Some(mem) = pod_level.get("memory") {
            effective.mem = reader.quantity(mem);
        }
    }

    let effective = effective.plus(reader.list(spec.and_then(|s| s.get("overhead"))));

    PodRequests {
        cpu_milli: effective.cpu,
        mem_milli: effective.mem,
        unparseable: reader.malformed,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn pod_with_no_containers_requests_zero() {
        let pod = json!({ "spec": {} });
        let req = pod_requests(&pod);
        assert!(req.is_zero());
    }

    const MI: i128 = 1024 * 1024 * 1000;

    /// A container with requests; `restart` sets `restartPolicy`.
    fn container(cpu: &str, mem: &str, restart: Option<&str>) -> Value {
        let mut c = json!({
            "name": "c",
            "resources": { "requests": { "cpu": cpu, "memory": mem } }
        });
        if let Some(policy) = restart {
            c["restartPolicy"] = json!(policy);
        }
        c
    }

    fn pod_of(containers: &[Value], init: &[Value]) -> Value {
        json!({ "spec": { "containers": containers, "initContainers": init } })
    }

    #[test]
    fn plain_init_containers_contribute_their_maximum_not_their_sum() {
        // Apps 100m/64Mi. Two plain init containers run one at a time:
        // 500m/32Mi, then 300m/256Mi. Per dimension the pod needs
        // max(app total, largest init) = cpu 500m, memory 256Mi.
        let pod = pod_of(
            &[container("100m", "64Mi", None)],
            &[
                container("500m", "32Mi", None),
                container("300m", "256Mi", None),
            ],
        );
        let req = pod_requests(&pod);
        assert_eq!((req.cpu_milli, req.mem_milli), (500, 256 * MI));
        assert!(!req.unparseable);
    }

    #[test]
    fn sidecars_add_to_the_running_total_and_to_later_init_containers() {
        // Plain init 250m BEFORE the sidecar runs without it: 250m.
        // Sidecar 200m keeps running: app total becomes 100m + 200m = 300m.
        // Plain init 250m AFTER the sidecar runs beside it: 450m.
        // Pod = max(300m, 250m, 200m, 450m) = 450m.
        let pod = pod_of(
            &[container("100m", "0", None)],
            &[
                container("250m", "0", None),
                container("200m", "0", Some("Always")),
                container("250m", "0", None),
            ],
        );
        assert_eq!(pod_requests(&pod).cpu_milli, 450);

        // Without the trailing plain init container, the sidecar's running
        // cost dominates: 100m + 200m = 300m, not max(100m, 250m, 200m).
        let pod = pod_of(
            &[container("100m", "0", None)],
            &[
                container("250m", "0", None),
                container("200m", "0", Some("Always")),
            ],
        );
        assert_eq!(pod_requests(&pod).cpu_milli, 300);
    }

    #[test]
    fn overhead_is_added_after_the_maximum() {
        // max(100m app, 500m init) + 50m overhead = 550m.
        let mut pod = pod_of(
            &[container("100m", "64Mi", None)],
            &[container("500m", "0", None)],
        );
        pod["spec"]["overhead"] = json!({ "cpu": "50m", "memory": "10Mi" });
        let req = pod_requests(&pod);
        assert_eq!((req.cpu_milli, req.mem_milli), (550, 74 * MI));
    }

    #[test]
    fn pod_level_requests_replace_the_aggregate_per_dimension() {
        // Pod-level cpu 2 replaces the containers' 100m; memory, not named
        // at pod level, stays the containers' 64Mi; overhead still adds.
        let mut pod = pod_of(&[container("100m", "64Mi", None)], &[]);
        pod["spec"]["resources"] = json!({ "requests": { "cpu": "2" } });
        pod["spec"]["overhead"] = json!({ "cpu": "10m" });
        let req = pod_requests(&pod);
        assert_eq!((req.cpu_milli, req.mem_milli), (2010, 64 * MI));
    }

    #[test]
    fn an_unknown_init_restart_policy_is_malformed_and_charged_as_a_sidecar() {
        let pod = pod_of(
            &[container("100m", "0", None)],
            &[container("200m", "0", Some("always"))],
        );
        let req = pod_requests(&pod);
        assert!(req.unparseable, "only `Always` is a sidecar");
        assert_eq!(req.cpu_milli, 300, "the larger of the two readings");
    }

    #[test]
    fn json_numbers_are_quantities_and_negatives_are_malformed() {
        let pod = json!({ "spec": { "containers": [{ "name": "c",
            "resources": { "requests": { "cpu": 2, "memory": 0.5 } } }] } });
        let req = pod_requests(&pod);
        assert_eq!((req.cpu_milli, req.mem_milli), (2000, 500));
        assert!(!req.unparseable);

        let pod = pod_with(&[("-1", "64Mi")]);
        let req = pod_requests(&pod);
        assert!(req.unparseable, "a negative request is malformed");
        assert_eq!(req.cpu_milli, 0, "and it never becomes a credit");
    }
}
