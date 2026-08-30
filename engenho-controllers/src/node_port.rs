//! NODEPORT ALLOCATION.
//!
//! ★ WHY IT WAS MISSING AND WHY THAT SHOWS. `Service.spec.type: NodePort`
//! was accepted, stored, and then did nothing: no port was assigned, so
//! `kubectl get svc` showed an empty `nodePort` column and nothing outside
//! the cluster could reach the service. The Service reported no error —
//! the field simply stayed absent — which is the worst shape a gap can
//! take, because the operator's YAML was correct and the cluster agreed
//! with it right up until they tried to connect.
//!
//! ★ THE RANGE IS A CONTRACT WITH THE NODE, NOT A PREFERENCE. 30000–32767
//! is upstream's default `--service-node-port-range`, and it is chosen to
//! sit above the ephemeral-port floor so an allocated NodePort cannot
//! collide with an outbound connection's source port. Allocating outside
//! it produces a service that works until the kernel happens to pick the
//! same port for something else — an intermittent failure with no
//! discoverable cause.
//!
//! ★ AN EXPLICIT REQUEST IS HONOURED OR REFUSED, NEVER SILENTLY MOVED. A
//! user who wrote `nodePort: 30080` has almost certainly written it into a
//! firewall rule or a load-balancer config too. Quietly assigning 30081
//! instead would produce a service that is up, reachable on a port nobody
//! configured, and unreachable on the one everybody did.
//!
//! ★ THE ALLOCATOR IS RESEEDED FROM LIVE SERVICES, never persisted
//! separately. The Services ARE the ledger — the same decision
//! `cluster_ip` makes, and for the same reason: a second copy of the
//! allocation state is a second thing that can be wrong, and on restart
//! the wrong one wins silently.

use std::collections::BTreeSet;

use thiserror::Error;

/// Upstream's default `--service-node-port-range`.
pub const DEFAULT_RANGE: (u16, u16) = (30000, 32767);

/// Allocation failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodePortError {
    /// Every port in the range is taken.
    #[error("no NodePort available in {low}-{high}: the range is exhausted")]
    Exhausted { low: u16, high: u16 },
    /// The requested port lies outside the configured range.
    #[error("requested nodePort {port} is outside the range {low}-{high}")]
    OutOfRange { port: u16, low: u16, high: u16 },
    /// The requested port is already held by another Service.
    #[error("requested nodePort {port} is already allocated")]
    AlreadyAllocated { port: u16 },
}

/// Collision-free NodePort allocation over a fixed range.
#[derive(Debug, Clone)]
pub struct NodePortAllocator {
    low: u16,
    high: u16,
    in_use: BTreeSet<u16>,
}

impl Default for NodePortAllocator {
    fn default() -> Self {
        Self::new(DEFAULT_RANGE.0, DEFAULT_RANGE.1)
    }
}

impl NodePortAllocator {
    /// A new allocator over `[low, high]`, inclusive at both ends.
    #[must_use]
    pub fn new(low: u16, high: u16) -> Self {
        Self {
            low,
            high,
            in_use: BTreeSet::new(),
        }
    }

    /// Mark a port as held.
    ///
    /// Used to reseed from live Services at startup. Returns whether the
    /// port was newly claimed — a caller reseeding can detect a DUPLICATE
    /// in the live set, which means two Services already believe they own
    /// the same port and no allocator decision can fix it.
    pub fn claim(&mut self, port: u16) -> bool {
        self.in_use.insert(port)
    }

    /// Release a port back to the range.
    pub fn release(&mut self, port: u16) {
        self.in_use.remove(&port);
    }

    /// Is this port currently held?
    #[must_use]
    pub fn is_allocated(&self, port: u16) -> bool {
        self.in_use.contains(&port)
    }

    /// Honour an explicitly requested port, or say precisely why not.
    pub fn allocate_specific(&mut self, port: u16) -> Result<u16, NodePortError> {
        if port < self.low || port > self.high {
            return Err(NodePortError::OutOfRange {
                port,
                low: self.low,
                high: self.high,
            });
        }
        if !self.in_use.insert(port) {
            return Err(NodePortError::AlreadyAllocated { port });
        }
        Ok(port)
    }

    /// Allocate the lowest free port.
    ///
    /// Lowest-first rather than random: it makes the same sequence of
    /// creates produce the same assignments, which is what lets a test
    /// assert an allocation at all and what makes an operator's firewall
    /// rules stable across a cluster rebuild.
    pub fn allocate(&mut self) -> Result<u16, NodePortError> {
        for port in self.low..=self.high {
            if self.in_use.insert(port) {
                return Ok(port);
            }
        }
        Err(NodePortError::Exhausted {
            low: self.low,
            high: self.high,
        })
    }
}

/// Does this Service type get NodePorts?
///
/// `LoadBalancer` does too — upstream allocates a NodePort for it as the
/// backing path, and omitting that would make a LoadBalancer Service
/// unreachable on every cloud whose controller programs the balancer to
/// forward at the node port.
#[must_use]
pub fn wants_node_ports(service_type: &str) -> bool {
    matches!(service_type, "NodePort" | "LoadBalancer")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_range_is_upstreams() {
        // Chosen to sit above the ephemeral-port floor so an allocated
        // NodePort cannot collide with an outbound connection's source
        // port. Allocating outside it yields intermittent, causeless
        // failures.
        assert_eq!(DEFAULT_RANGE, (30000, 32767));
        let a = NodePortAllocator::default();
        assert_eq!((a.low, a.high), (30000, 32767));
    }

    #[test]
    fn allocation_is_collision_free_and_deterministic() {
        // Anti-vacuity plus the stability property: the same sequence of
        // creates must produce the same assignments, or an operator's
        // firewall rules break on every cluster rebuild.
        let mut a = NodePortAllocator::new(30000, 30002);
        assert_eq!(a.allocate(), Ok(30000));
        assert_eq!(a.allocate(), Ok(30001));
        assert_eq!(a.allocate(), Ok(30002));
        assert_eq!(
            a.allocate(),
            Err(NodePortError::Exhausted {
                low: 30000,
                high: 30002
            })
        );
    }

    #[test]
    fn an_explicit_request_is_honoured_exactly_or_refused() {
        // Quietly assigning a NEIGHBOURING port would produce a service
        // that is up, reachable on a port nobody configured, and
        // unreachable on the one written into every firewall rule.
        let mut a = NodePortAllocator::default();
        assert_eq!(a.allocate_specific(30080), Ok(30080));
        assert_eq!(
            a.allocate_specific(30080),
            Err(NodePortError::AlreadyAllocated { port: 30080 })
        );
        // Never silently moved to 30081.
        assert!(!a.is_allocated(30081));
    }

    #[test]
    fn a_request_outside_the_range_is_refused_with_the_bounds() {
        let mut a = NodePortAllocator::default();
        assert_eq!(
            a.allocate_specific(8080),
            Err(NodePortError::OutOfRange {
                port: 8080,
                low: 30000,
                high: 32767
            })
        );
        assert_eq!(
            a.allocate_specific(40000),
            Err(NodePortError::OutOfRange {
                port: 40000,
                low: 30000,
                high: 32767
            })
        );
    }

    #[test]
    fn an_explicitly_taken_port_is_skipped_by_automatic_allocation() {
        let mut a = NodePortAllocator::new(30000, 30002);
        assert_eq!(a.allocate_specific(30000), Ok(30000));
        assert_eq!(a.allocate(), Ok(30001), "must not hand out a held port");
    }

    #[test]
    fn a_released_port_is_reusable() {
        let mut a = NodePortAllocator::new(30000, 30000);
        assert_eq!(a.allocate(), Ok(30000));
        assert!(a.allocate().is_err());
        a.release(30000);
        assert_eq!(a.allocate(), Ok(30000));
    }

    #[test]
    fn claim_reports_a_duplicate_in_the_live_set() {
        // Reseeding from live Services is how the allocator survives a
        // restart. A duplicate there means two Services ALREADY believe
        // they own the same port — no allocator decision fixes that, and
        // the caller must be able to see it rather than have it swallowed.
        let mut a = NodePortAllocator::default();
        assert!(a.claim(30080), "first claim is new");
        assert!(!a.claim(30080), "second claim reports the collision");
    }

    #[test]
    fn load_balancer_services_get_node_ports_too() {
        // Upstream allocates one as the backing path; omitting it makes a
        // LoadBalancer unreachable on every cloud whose controller
        // programs the balancer to forward at the node port.
        assert!(wants_node_ports("NodePort"));
        assert!(wants_node_ports("LoadBalancer"));
        assert!(!wants_node_ports("ClusterIP"));
        assert!(!wants_node_ports("ExternalName"));
    }

    #[test]
    fn a_single_port_range_is_usable_not_degenerate() {
        // An off-by-one in the inclusive bound would make this range empty,
        // and the failure would only appear on a cluster configured with a
        // tiny range.
        let mut a = NodePortAllocator::new(31000, 31000);
        assert_eq!(a.allocate(), Ok(31000));
    }
}
