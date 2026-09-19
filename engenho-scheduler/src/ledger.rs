//! The capacity ledger — the ONE place a node's remaining capacity is
//! computed.
//!
//! ## Why one ledger
//!
//! Before this module the scheduler kept two hand-written clamp loops over
//! one `HashMap<String, NodeResources>`: one seeded it from bound pods, the
//! other decremented it after each bind in the same tick. Both clamped at
//! zero on every write, both charged every bound pod regardless of phase,
//! and a third copy (`fit::free_on_node`) repeated the arithmetic for its
//! own callers. Three copies of one rule can disagree, and the census the
//! plan asks for (per-node headroom, published before any change that
//! shrinks it) could not be answered at all: a balance clamped on write has
//! already forgotten by how much a node is overcommitted.
//!
//! [`NodeLedger`] replaces all three:
//!
//! - [`NodeLedger::seed`] opens the books from the node list and the pod
//!   list, charging each bound pod that [`holds_capacity`];
//! - [`NodeLedger::debit`] charges a pod bound during the tick;
//! - [`NodeLedger::fits`] / [`NodeLedger::free`] are the scheduler's reads,
//!   clamped at zero;
//! - [`NodeLedger::headroom`] is the census read, signed.
//!
//! The balance map is private and signed. Nothing outside this module can
//! write it, and it is clamped only when read for fitting — which is also
//! where upstream's fit check effectively clamps: a pod that requests zero
//! of a resource fits even on a node already overcommitted in it.
//!
//! ## Only an observed terminal phase releases capacity
//!
//! [`holds_capacity`] has three arms, not two. A pod whose phase was
//! observed `Succeeded` or `Failed` has no container left to run and never
//! restarts, so its requests go back to the node — upstream's scheduler
//! drops such pods from its cache for the same reason. A pod observed
//! `Pending` or `Running` holds. A pod whose phase is absent, `Unknown`, or
//! not a pod phase at all ALSO holds: nobody observed it finish, and
//! releasing capacity on the strength of an unread field would let the
//! scheduler hand a node's memory to a second pod while the first is still
//! using it.

use std::collections::BTreeMap;
use std::fmt;

use engenho_types::curated_enums::PodPhase;
use serde::Deserialize;
use serde_json::Value;

use crate::fit::{NodeResources, PodRequests, fits, node_allocatable, pod_requests};

/// Whether a bound pod still holds its requests on its node.
///
/// The three arms are the three things the scheduler can know about a
/// pod's phase; only one of them releases capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityHold {
    /// Phase observed `Pending` or `Running`: the pod is live and holds.
    Live,
    /// Phase absent, `Unknown`, or not a pod phase: the pod was not
    /// observed to finish, so it holds.
    Unobserved,
    /// Phase observed `Succeeded` or `Failed`: every container has
    /// terminated for good, so the pod's requests return to the node.
    Released,
}

impl CapacityHold {
    /// Does the pod still count against its node's capacity?
    #[must_use]
    pub const fn holds(self) -> bool {
        match self {
            Self::Live | Self::Unobserved => true,
            Self::Released => false,
        }
    }
}

/// Classify a pod by the phase last written to `status.phase`.
///
/// The phase is read through the typed [`PodPhase`] enum and matched
/// exhaustively, so a new phase cannot silently fall into the releasing
/// arm.
#[must_use]
pub fn holds_capacity(pod: &Value) -> CapacityHold {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|v| PodPhase::deserialize(v).ok());
    match phase {
        Some(PodPhase::Succeeded | PodPhase::Failed) => CapacityHold::Released,
        Some(PodPhase::Pending | PodPhase::Running) => CapacityHold::Live,
        Some(PodPhase::Unknown) | None => CapacityHold::Unobserved,
    }
}

/// A node's signed balance: allocatable minus every held request, in the
/// same milli-units as [`NodeResources`]. Negative means overcommitted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Balance {
    cpu_milli: i128,
    mem_milli: i128,
}

impl Balance {
    fn opening(allocatable: NodeResources) -> Self {
        Self {
            cpu_milli: allocatable.cpu_milli,
            mem_milli: allocatable.mem_milli,
        }
    }

    /// Charge a request. A negative request dimension charges nothing: a
    /// request can shrink free capacity, never grow it.
    fn charge(&mut self, req: &PodRequests) {
        self.cpu_milli = self.cpu_milli.saturating_sub(req.cpu_milli.max(0));
        self.mem_milli = self.mem_milli.saturating_sub(req.mem_milli.max(0));
    }

    /// The balance as free capacity, clamped at zero per dimension.
    fn free(self) -> NodeResources {
        NodeResources {
            cpu_milli: self.cpu_milli.max(0),
            mem_milli: self.mem_milli.max(0),
        }
    }
}

/// Per-node capacity books for one scheduling pass.
#[derive(Clone, Debug, Default)]
pub struct NodeLedger {
    /// Signed balance per node name. Private, so the only writers are
    /// [`Self::seed`] and [`Self::debit`], and the only clamp is on read.
    balances: BTreeMap<String, Balance>,
}

impl NodeLedger {
    /// Open the books.
    ///
    /// Every node with a `metadata.name` gets an opening balance of its
    /// allocatable capacity (see [`node_allocatable`]). Every pod bound to
    /// one of those nodes that still [`holds_capacity`] is charged its
    /// effective requests (see [`pod_requests`]). A pod bound to a node
    /// not in `nodes` charges nothing, because there is no balance for it
    /// to charge and no pod can be fitted there this pass.
    ///
    /// `pods` must be the cluster-wide pod list, not a namespace-filtered
    /// one: a pod in another namespace still occupies its node.
    #[must_use]
    pub fn seed<'a, N, P>(nodes: N, pods: P) -> Self
    where
        N: IntoIterator<Item = &'a Value>,
        P: IntoIterator<Item = &'a Value>,
    {
        let mut ledger = Self {
            balances: nodes
                .into_iter()
                .filter_map(|n| {
                    node_name_of(n)
                        .map(|name| (name.to_owned(), Balance::opening(node_allocatable(n))))
                })
                .collect(),
        };
        for pod in pods {
            let Some(node) = bound_node_of(pod) else {
                continue;
            };
            if holds_capacity(pod).holds() {
                ledger.debit(node, &pod_requests(pod));
            }
        }
        ledger
    }

    /// Charge `req` to `node`. Used for a pod bound during this pass, so a
    /// later pod in the same pass cannot be fitted into capacity already
    /// spoken for. A node with no balance is left alone.
    pub fn debit(&mut self, node: &str, req: &PodRequests) {
        if let Some(balance) = self.balances.get_mut(node) {
            balance.charge(req);
        }
    }

    /// Free capacity on `node`, clamped at zero per dimension, or `None`
    /// if the ledger has no balance for it.
    #[must_use]
    pub fn free(&self, node: &str) -> Option<NodeResources> {
        self.balances.get(node).map(|b| b.free())
    }

    /// Does `req` fit on `node` right now? A node with no balance fits
    /// nothing.
    #[must_use]
    pub fn fits(&self, node: &str, req: &PodRequests) -> bool {
        self.free(node).is_some_and(|free| fits(&free, req))
    }

    /// The signed balance of every node, for the headroom census. Unlike
    /// [`Self::free`], this is NOT clamped: a negative value is the amount
    /// by which the node is already overcommitted.
    pub fn headroom(&self) -> impl Iterator<Item = Headroom<'_>> {
        self.balances.iter().map(|(node, b)| Headroom {
            node,
            cpu_milli: b.cpu_milli,
            mem_milli: b.mem_milli,
        })
    }
}

/// One node's signed headroom, as read by [`NodeLedger::headroom`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Headroom<'a> {
    /// The node's `metadata.name`.
    pub node: &'a str,
    /// Remaining cpu, milli-cores. Negative when overcommitted.
    pub cpu_milli: i128,
    /// Remaining memory, milli-bytes. Negative when overcommitted.
    pub mem_milli: i128,
}

impl Headroom<'_> {
    /// Is the node already charged past its allocatable in any dimension?
    #[must_use]
    pub const fn is_overcommitted(&self) -> bool {
        self.cpu_milli < 0 || self.mem_milli < 0
    }
}

impl fmt::Display for Headroom<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verdict = if self.is_overcommitted() {
            "OVERCOMMITTED"
        } else {
            "within"
        };
        write!(
            f,
            "{} cpu={}m memory={}B {}",
            self.node,
            self.cpu_milli,
            self.mem_milli / 1000,
            verdict
        )
    }
}

/// A node's `metadata.name`, if present and a string.
pub(crate) fn node_name_of(node: &Value) -> Option<&str> {
    node.pointer("/metadata/name").and_then(Value::as_str)
}

/// The node a pod is bound to: a non-empty `spec.nodeName`.
fn bound_node_of(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GI_MILLI: i128 = (1i128 << 30) * 1000;

    fn node(name: &str, cpu: &str, mem: &str) -> Value {
        json!({
            "metadata": { "name": name },
            "status": { "allocatable": { "cpu": cpu, "memory": mem } }
        })
    }

    fn bound(node: &str, cpu: &str, mem: &str, phase: Option<&str>) -> Value {
        let mut p = json!({
            "metadata": { "name": "p" },
            "spec": {
                "nodeName": node,
                "containers": [{
                    "name": "c",
                    "resources": { "requests": { "cpu": cpu, "memory": mem } }
                }]
            }
        });
        if let Some(phase) = phase {
            p["status"] = json!({ "phase": phase });
        }
        p
    }

    fn one_core() -> PodRequests {
        PodRequests {
            cpu_milli: 1000,
            mem_milli: 0,
            unparseable: false,
        }
    }

    #[test]
    fn seed_charges_every_bound_pod_that_holds() {
        // 2 cores / 4Gi, two Running pods of 500m / 1Gi: 1 core / 2Gi left.
        let n = node("n1", "2", "4Gi");
        let pods = [
            bound("n1", "500m", "1Gi", Some("Running")),
            bound("n1", "500m", "1Gi", Some("Running")),
        ];
        let ledger = NodeLedger::seed([&n], &pods);
        assert_eq!(
            ledger.free("n1"),
            Some(NodeResources {
                cpu_milli: 1000,
                mem_milli: 2 * GI_MILLI
            })
        );
    }

    #[test]
    fn terminal_pods_release_their_capacity() {
        let n = node("n1", "1", "1Gi");
        let pods = [
            bound("n1", "1", "1Gi", Some("Succeeded")),
            bound("n1", "1", "1Gi", Some("Failed")),
        ];
        let ledger = NodeLedger::seed([&n], &pods);
        assert_eq!(
            ledger.free("n1"),
            Some(NodeResources {
                cpu_milli: 1000,
                mem_milli: GI_MILLI
            }),
            "a Succeeded and a Failed pod hold nothing"
        );
        assert!(ledger.fits("n1", &one_core()));
    }

    #[test]
    fn a_phase_nobody_observed_to_finish_holds_capacity() {
        // Absent, the deprecated `Unknown`, a value that is not a phase at
        // all, a lowercase near-miss and a non-string: each one HOLDS. Only
        // an observed Succeeded/Failed may release.
        let n = node("n1", "1", "1Gi");
        for phase in [
            None,
            Some(json!("Unknown")),
            Some(json!("Evicted")),
            Some(json!("succeeded")),
            Some(json!(3)),
        ] {
            let mut pod = bound("n1", "1", "0", None);
            if let Some(phase) = phase.clone() {
                pod["status"] = json!({ "phase": phase });
            }
            assert_eq!(holds_capacity(&pod), CapacityHold::Unobserved, "{phase:?}");
            let ledger = NodeLedger::seed([&n], [&pod]);
            assert!(
                !ledger.fits("n1", &one_core()),
                "phase {phase:?} must keep holding the core"
            );
        }
    }

    #[test]
    fn live_phases_hold_and_terminal_phases_release() {
        let arm = |phase: &str| holds_capacity(&json!({ "status": { "phase": phase } }));
        assert_eq!(arm("Pending"), CapacityHold::Live);
        assert_eq!(arm("Running"), CapacityHold::Live);
        assert_eq!(arm("Succeeded"), CapacityHold::Released);
        assert_eq!(arm("Failed"), CapacityHold::Released);
        assert!(CapacityHold::Live.holds());
        assert!(CapacityHold::Unobserved.holds());
        assert!(!CapacityHold::Released.holds());
    }

    #[test]
    fn debit_charges_a_pod_bound_during_the_pass() {
        let n = node("n1", "1", "1Gi");
        let mut ledger = NodeLedger::seed([&n], []);
        assert!(ledger.fits("n1", &one_core()));
        ledger.debit("n1", &one_core());
        assert!(!ledger.fits("n1", &one_core()), "the core is spoken for");
    }

    #[test]
    fn the_balance_is_signed_and_clamped_only_when_read() {
        // 1 core allocatable, 3 cores held: 2 cores overcommitted.
        let n = node("n1", "1", "1Gi");
        let pod = bound("n1", "3", "0", Some("Running"));
        let ledger = NodeLedger::seed([&n], [&pod]);

        let rows: Vec<_> = ledger.headroom().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cpu_milli, -2000, "the census sees the debt");
        assert!(rows[0].is_overcommitted());

        assert_eq!(ledger.free("n1").map(|f| f.cpu_milli), Some(0));
        // A pod requesting no cpu still fits, as upstream's fit check
        // skips a dimension the pod does not request.
        let no_cpu = PodRequests {
            cpu_milli: 0,
            mem_milli: 1000,
            unparseable: false,
        };
        assert!(ledger.fits("n1", &no_cpu));
        assert!(!ledger.fits("n1", &one_core()));
    }

    #[test]
    fn a_negative_request_never_credits_a_node() {
        let n = node("n1", "1", "1Gi");
        let mut ledger = NodeLedger::seed([&n], []);
        ledger.debit(
            "n1",
            &PodRequests {
                cpu_milli: -5000,
                mem_milli: -1,
                unparseable: false,
            },
        );
        assert_eq!(
            ledger.free("n1"),
            Some(NodeResources {
                cpu_milli: 1000,
                mem_milli: GI_MILLI
            })
        );
    }

    #[test]
    fn pods_on_unknown_nodes_and_unbound_pods_charge_nothing() {
        let n = node("n1", "1", "1Gi");
        let elsewhere = bound("gone", "1", "0", Some("Running"));
        let pending = json!({
            "spec": { "containers": [{ "name": "c",
                "resources": { "requests": { "cpu": "1" } } }] }
        });
        let ledger = NodeLedger::seed([&n], [&elsewhere, &pending]);
        assert!(ledger.fits("n1", &one_core()));
        assert!(!ledger.fits("gone", &PodRequests::default()));
        assert_eq!(ledger.free("gone"), None);
    }

    #[test]
    fn headroom_renders_bytes_and_a_verdict() {
        let n = node("n1", "2", "1Gi");
        let pod = bound("n1", "500m", "512Mi", Some("Running"));
        let ledger = NodeLedger::seed([&n], [&pod]);
        let row = ledger.headroom().next().map(|h| h.to_string());
        assert_eq!(
            row.as_deref(),
            Some("n1 cpu=1500m memory=536870912B within")
        );
    }
}
