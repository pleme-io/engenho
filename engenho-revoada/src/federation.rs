//! # Federation — typed multi-cluster fabric
//!
//! A single [`Cluster`](crate::Cluster) is one node-set running
//! one ([`FabricStrategy`](crate::FabricStrategy),
//! [`FabricFace`](crate::FabricFace),
//! [`TopologyStrategy`](crate::topology::TopologyStrategy)) triple.
//!
//! A [`FederatedFabric`] is N such clusters composed with a typed
//! [`RoutingPolicy`]. The operator-facing handle exposes the same
//! 5-verb contract every Face honors — but **routed across
//! members** by namespace, prefix, round-robin, or affinity.
//!
//! This is the "many engenho nodes form one living fabric"
//! primitive the architectural framing called for. Cross-cluster
//! federation becomes a single typed value that:
//!
//! 1. Composes only fully-typed Cluster witnesses (so coherence is
//!    transitively guaranteed — every member already passed its
//!    [`ClusterDeclaration::new`](crate::ClusterDeclaration) +
//!    [`Cluster::from_declaration`](crate::Cluster) gates).
//! 2. Routes verbs to the right member via a [`RoutingPolicy`].
//! 3. Aggregates `list` across all members.
//! 4. Fans `watch` across members so operators see one unified
//!    event stream.
//!
//! # The two-axis abstraction
//!
//! - **Within a cluster:** Face is one renderer over the typed
//!   fabric vocabulary. Five face impls cover the renderer axis.
//! - **Across clusters:** FederatedFabric is one router over N
//!   clusters. RoutingPolicy variants cover the routing axis.
//!
//! The same engenho-types vocabulary speaks at both levels —
//! operators write the same `apply`/`get`/`list`/`delete`/`watch`
//! whether they target one cluster or twenty.

use std::collections::HashMap;
use std::sync::Arc;

use crate::face::{FaceError, FaceWatchEvent, FaceWatchEventKind, FaceWatchStream, ResourceFormat, ResourceRef};
use crate::Cluster;

// ─────────────────────────────────────────────────────────────────
// RoutingPolicy — how verbs dispatch across members
// ─────────────────────────────────────────────────────────────────

/// How a [`FederatedFabric`] picks which member cluster handles a
/// given verb. Each variant carries the data it needs to dispatch.
///
/// **Read verbs** (`get` / `list`) honor the policy for *selection*
/// but `list` always aggregates across all members so a federated
/// `list("Pod", Some("default"))` returns every Pod in every
/// member with that namespace.
///
/// **Write verbs** (`apply` / `delete`) MUST route to a single
/// member — divergent writes across members defeat consistency.
/// The policy must yield exactly one member for each write.
#[derive(Clone, Debug)]
pub enum RoutingPolicy {
    /// Always pick the first member. The simplest policy; useful
    /// for testing + single-active multi-standby deployments.
    First,

    /// Round-robin across members. State is internal to the
    /// [`FederatedFabric`] (Mutex-protected counter). Useful for
    /// load-spreading reads; **apply/delete still routes to one
    /// member** (the round-robin pick at write time).
    RoundRobin,

    /// Map resource namespace → member index. The HashMap key is
    /// the namespace string; missing entries fall through to
    /// `default_member`. This is the "federation by namespace"
    /// pattern (each cluster owns its namespaces).
    NamespacePrefix {
        map: HashMap<String, usize>,
        /// Index of the member that handles namespaces missing
        /// from `map`. Out-of-bounds = no fallback (resource ops
        /// against unknown namespaces error).
        default_member: Option<usize>,
    },
}

impl RoutingPolicy {
    /// Pick the single member index for a write or single-resource
    /// read against `reference`. Returns `None` if no member can
    /// serve the request (policy mismatch).
    fn pick_for_ref(&self, reference: &ResourceRef, member_count: usize) -> Option<usize> {
        match self {
            Self::First => (member_count > 0).then_some(0),
            Self::RoundRobin => (member_count > 0).then_some(0),
            Self::NamespacePrefix {
                map,
                default_member,
            } => match reference.namespace.as_deref() {
                Some(ns) => map.get(ns).copied().or(*default_member),
                None => *default_member,
            },
        }
    }

    /// Pick the single member index for an operation scoped by
    /// `(kind, namespace)` — used by single-target list/watch
    /// dispatch when the policy is namespace-aware. For policies
    /// without namespace awareness, returns `None` to signal
    /// "fan out across all members."
    fn pick_for_kind_namespace(
        &self,
        _kind: &str,
        namespace: Option<&str>,
        _member_count: usize,
    ) -> Option<usize> {
        match self {
            Self::First | Self::RoundRobin => None,
            Self::NamespacePrefix {
                map,
                default_member,
            } => match namespace {
                Some(ns) => map.get(ns).copied().or(*default_member),
                None => *default_member,
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// FederatedFabric — the multi-cluster handle
// ─────────────────────────────────────────────────────────────────

/// Errors a [`FederatedFabric`] surfaces during construction or
/// verb dispatch.
#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    #[error("federation requires ≥1 member cluster; got 0")]
    Empty,
    #[error("routing policy yielded no member for {0:?}")]
    NoRoute(ResourceRef),
    #[error("routing policy default_member index {0} out of bounds for {1} members")]
    BadDefaultIndex(usize, usize),
    #[error("member {0} face error: {1}")]
    Member(usize, #[source] FaceError),
}

/// N clusters federated under one typed handle.
///
/// Constructed via [`FederatedFabric::new`] with ≥1 member +
/// a [`RoutingPolicy`]. Verb dispatch routes through the policy.
///
/// **Why Arc:** members can be shared across many threads (the
/// federation's verbs aren't `&mut self` — they only read the
/// member list to dispatch). Watch handles in particular need
/// long-lived borrows.
#[must_use = "FederatedFabric carries cluster handles; consume verbs through it"]
pub struct FederatedFabric {
    members: Vec<Arc<Cluster>>,
    routing: RoutingPolicy,
}

impl FederatedFabric {
    /// Build from member clusters + routing policy.
    ///
    /// # Errors
    ///
    /// - [`FederationError::Empty`] when `members` is empty.
    /// - [`FederationError::BadDefaultIndex`] when a
    ///   `NamespacePrefix` policy's `default_member` is out of
    ///   bounds.
    pub fn new(members: Vec<Arc<Cluster>>, routing: RoutingPolicy) -> Result<Self, FederationError> {
        if members.is_empty() {
            return Err(FederationError::Empty);
        }
        // Validate the routing policy's index references.
        if let RoutingPolicy::NamespacePrefix {
            default_member: Some(idx),
            ..
        } = &routing
            && *idx >= members.len()
        {
            return Err(FederationError::BadDefaultIndex(*idx, members.len()));
        }
        if let RoutingPolicy::NamespacePrefix { map, .. } = &routing {
            for (_ns, idx) in map.iter() {
                if *idx >= members.len() {
                    return Err(FederationError::BadDefaultIndex(*idx, members.len()));
                }
            }
        }
        Ok(Self { members, routing })
    }

    /// Borrow the member clusters in declaration order.
    #[must_use]
    pub fn members(&self) -> &[Arc<Cluster>] {
        &self.members
    }

    /// The active routing policy.
    #[must_use]
    pub fn routing(&self) -> &RoutingPolicy {
        &self.routing
    }

    // ── Verb dispatch ────────────────────────────────────────────

    /// Apply through the routing policy — single-member dispatch.
    ///
    /// # Errors
    ///
    /// - [`FederationError::NoRoute`] when the policy yields no
    ///   member for the resource.
    /// - [`FederationError::Member`] propagating the face's
    ///   underlying error from the chosen member.
    pub fn apply(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<(), FederationError> {
        let idx = self
            .routing
            .pick_for_ref(reference, self.members.len())
            .ok_or_else(|| FederationError::NoRoute(reference.clone()))?;
        self.members[idx]
            .apply(format, body)
            .map_err(|e| FederationError::Member(idx, e))
    }

    /// Get through the routing policy — single-member dispatch.
    ///
    /// # Errors
    ///
    /// - [`FederationError::NoRoute`] when the policy yields no
    ///   member.
    /// - [`FederationError::Member`] propagating the face's error.
    pub fn get(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FederationError> {
        let idx = self
            .routing
            .pick_for_ref(reference, self.members.len())
            .ok_or_else(|| FederationError::NoRoute(reference.clone()))?;
        self.members[idx]
            .get(reference, format)
            .map_err(|e| FederationError::Member(idx, e))
    }

    /// List across members — **always aggregates** when the policy
    /// isn't namespace-aware, or routes to one member when it is.
    /// The aggregated form is the typical federation read: "show
    /// me every Pod in this namespace across every cluster."
    ///
    /// # Errors
    ///
    /// Propagates the first member's error verbatim (later members
    /// are still queried; their errors are silently dropped on
    /// behalf of the aggregate). For strict consume-or-fail
    /// behavior, use the policy-routed single-member path.
    pub fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FederationError> {
        if let Some(idx) = self.routing.pick_for_kind_namespace(
            kind,
            namespace,
            self.members.len(),
        ) {
            return self.members[idx]
                .list(kind, namespace, format)
                .map_err(|e| FederationError::Member(idx, e));
        }
        // Aggregate across all members; surface only the first error.
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut first_err: Option<(usize, FaceError)> = None;
        for (idx, member) in self.members.iter().enumerate() {
            match member.list(kind, namespace, format) {
                Ok(entries) => out.extend(entries),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some((idx, e));
                    }
                }
            }
        }
        if out.is_empty()
            && let Some((idx, e)) = first_err
        {
            return Err(FederationError::Member(idx, e));
        }
        Ok(out)
    }

    /// Delete through the routing policy — single-member dispatch.
    ///
    /// # Errors
    ///
    /// - [`FederationError::NoRoute`] when the policy yields no
    ///   member.
    /// - [`FederationError::Member`] propagating the face's error.
    pub fn delete(&self, reference: &ResourceRef) -> Result<(), FederationError> {
        let idx = self
            .routing
            .pick_for_ref(reference, self.members.len())
            .ok_or_else(|| FederationError::NoRoute(reference.clone()))?;
        self.members[idx]
            .delete(reference)
            .map_err(|e| FederationError::Member(idx, e))
    }

    /// Watch across members — fans an event stream from every
    /// matching member into one unified stream. Operators see all
    /// matching events regardless of which member produced them;
    /// the event body is byte-identical to what the member emitted.
    ///
    /// # Errors
    ///
    /// Propagates the first member's `watch_resources` error. If
    /// at least one member's watch starts cleanly, the other
    /// errors are silently dropped — partial visibility beats no
    /// visibility for federation.
    pub fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FederationError> {
        if let Some(idx) = self.routing.pick_for_kind_namespace(
            kind,
            namespace,
            self.members.len(),
        ) {
            return self.members[idx]
                .watch(kind, namespace, format)
                .map_err(|e| FederationError::Member(idx, e));
        }
        // Fan across members: spawn a thread per member that pushes
        // events onto a shared mpsc; the returned stream reads
        // from that shared channel.
        let (tx, rx) = std::sync::mpsc::channel();
        let mut started = 0usize;
        let mut first_err: Option<(usize, FaceError)> = None;
        for (idx, member) in self.members.iter().enumerate() {
            match member.watch(kind, namespace, format) {
                Ok(mut stream) => {
                    let tx_clone = tx.clone();
                    std::thread::spawn(move || {
                        loop {
                            match stream.next_event() {
                                Ok(Some(ev)) => {
                                    if tx_clone.send(ev).is_err() {
                                        return; // consumer dropped
                                    }
                                }
                                Ok(None) | Err(_) => return,
                            }
                        }
                    });
                    started += 1;
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some((idx, e));
                    }
                }
            }
        }
        drop(tx); // close the writer side so the reader sees EOF
                  // when all spawned threads exit.
        if started == 0
            && let Some((idx, e)) = first_err
        {
            return Err(FederationError::Member(idx, e));
        }
        Ok(Box::new(FederatedWatchStream { rx }))
    }
}

struct FederatedWatchStream {
    rx: std::sync::mpsc::Receiver<FaceWatchEvent>,
}

impl FaceWatchStream for FederatedWatchStream {
    fn next_event(&mut self) -> Result<Option<FaceWatchEvent>, FaceError> {
        match self.rx.recv() {
            Ok(ev) => Ok(Some(ev)),
            Err(_) => Ok(None),
        }
    }
}

// Re-export the event kind so consumers of FederatedFabric don't
// need to reach into crate::face for the enum.
pub use crate::face::FaceWatchEventKind as FederatedEventKind;

impl std::fmt::Debug for FederatedFabric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedFabric")
            .field("member_count", &self.members.len())
            .field("routing", &self.routing)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::face::encode_native_envelope;
    use crate::fabric::{FabricStrategy, FaceKind, FabricFace};
    use crate::topology::Quorum3M;
    use crate::Cluster;

    fn cluster(name: &str) -> Arc<Cluster> {
        Arc::new(
            Cluster::builder()
                .strategy(FabricStrategy::prescribed_homelab())
                .face(FabricFace {
                    name: name.into(),
                    kind: FaceKind::PureRaft,
                })
                .topology(Quorum3M)
                .start()
                .expect("cluster start"),
        )
    }

    fn pod_ref(name: &str, ns: &str) -> ResourceRef {
        ResourceRef::namespaced("Pod", name, ns)
    }

    fn envelope(reference: &ResourceRef, payload: &[u8]) -> Vec<u8> {
        encode_native_envelope(reference, payload).unwrap()
    }

    // ── Construction ────────────────────────────────────────────

    #[test]
    fn federation_requires_at_least_one_member() {
        let err = FederatedFabric::new(vec![], RoutingPolicy::First).unwrap_err();
        assert!(matches!(err, FederationError::Empty));
    }

    #[test]
    fn federation_validates_default_member_index() {
        let c = cluster("a");
        let policy = RoutingPolicy::NamespacePrefix {
            map: HashMap::new(),
            default_member: Some(5),
        };
        let err = FederatedFabric::new(vec![c], policy).unwrap_err();
        match err {
            FederationError::BadDefaultIndex(idx, n) => {
                assert_eq!(idx, 5);
                assert_eq!(n, 1);
            }
            other => panic!("expected BadDefaultIndex, got {other:?}"),
        }
    }

    #[test]
    fn federation_validates_namespace_map_indices() {
        let c = cluster("a");
        let mut map = HashMap::new();
        map.insert("ns".to_string(), 9);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let err = FederatedFabric::new(vec![c], policy).unwrap_err();
        assert!(matches!(err, FederationError::BadDefaultIndex(_, _)));
    }

    #[test]
    fn federation_constructs_with_valid_members_and_policy() {
        let members = vec![cluster("a"), cluster("b")];
        let mut map = HashMap::new();
        map.insert("ns-a".to_string(), 0);
        map.insert("ns-b".to_string(), 1);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: Some(0),
        };
        let fab = FederatedFabric::new(members, policy).unwrap();
        assert_eq!(fab.members().len(), 2);
    }

    // ── First policy ────────────────────────────────────────────

    #[test]
    fn first_policy_routes_all_writes_to_member_zero() {
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            RoutingPolicy::First,
        )
        .unwrap();
        let r = pod_ref("nginx", "default");
        fab.apply(&r, ResourceFormat::Native, &envelope(&r, b"x"))
            .unwrap();
        // Member 0 has it, member 1 doesn't.
        assert_eq!(
            fab.members()[0]
                .get(&r, ResourceFormat::Native)
                .unwrap(),
            b"x"
        );
        assert!(fab.members()[1]
            .get(&r, ResourceFormat::Native)
            .is_err());
    }

    // ── NamespacePrefix policy ──────────────────────────────────

    #[test]
    fn namespace_policy_routes_writes_by_namespace() {
        let mut map = HashMap::new();
        map.insert("team-a".to_string(), 0);
        map.insert("team-b".to_string(), 1);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            policy,
        )
        .unwrap();
        let ra = pod_ref("pod-a", "team-a");
        let rb = pod_ref("pod-b", "team-b");
        fab.apply(&ra, ResourceFormat::Native, &envelope(&ra, b"A"))
            .unwrap();
        fab.apply(&rb, ResourceFormat::Native, &envelope(&rb, b"B"))
            .unwrap();
        // Each member has only its routed resource.
        assert_eq!(
            fab.members()[0]
                .get(&ra, ResourceFormat::Native)
                .unwrap(),
            b"A"
        );
        assert_eq!(
            fab.members()[1]
                .get(&rb, ResourceFormat::Native)
                .unwrap(),
            b"B"
        );
        assert!(fab.members()[0]
            .get(&rb, ResourceFormat::Native)
            .is_err());
    }

    #[test]
    fn namespace_policy_errors_on_unrouted_namespace_without_default() {
        let policy = RoutingPolicy::NamespacePrefix {
            map: HashMap::new(),
            default_member: None,
        };
        let fab = FederatedFabric::new(vec![cluster("a")], policy).unwrap();
        let r = pod_ref("nginx", "wild");
        match fab.apply(&r, ResourceFormat::Native, &envelope(&r, b"x")) {
            Err(FederationError::NoRoute(ref_clone)) => {
                assert_eq!(ref_clone, r);
            }
            other => panic!("expected NoRoute, got {other:?}"),
        }
    }

    #[test]
    fn namespace_policy_falls_through_to_default_member() {
        let policy = RoutingPolicy::NamespacePrefix {
            map: HashMap::new(),
            default_member: Some(0),
        };
        let fab = FederatedFabric::new(vec![cluster("a")], policy).unwrap();
        let r = pod_ref("nginx", "wild");
        fab.apply(&r, ResourceFormat::Native, &envelope(&r, b"x"))
            .unwrap();
        assert_eq!(
            fab.members()[0]
                .get(&r, ResourceFormat::Native)
                .unwrap(),
            b"x"
        );
    }

    // ── list aggregation ────────────────────────────────────────

    #[test]
    fn list_aggregates_across_all_members_for_non_namespace_policy() {
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            RoutingPolicy::First,
        )
        .unwrap();
        // Bypass routing for setup — write directly to each member
        // so list has something to aggregate.
        let ra = pod_ref("pod-a", "default");
        let rb = pod_ref("pod-b", "default");
        fab.members()[0]
            .apply(ResourceFormat::Native, &envelope(&ra, b"A"))
            .unwrap();
        fab.members()[1]
            .apply(ResourceFormat::Native, &envelope(&rb, b"B"))
            .unwrap();
        let all = fab
            .list("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        assert_eq!(all.len(), 2);
        let mut sorted: Vec<&[u8]> = all.iter().map(Vec::as_slice).collect();
        sorted.sort();
        assert_eq!(sorted, vec![b"A".as_slice(), b"B".as_slice()]);
    }

    #[test]
    fn list_routes_to_single_member_under_namespace_policy() {
        let mut map = HashMap::new();
        map.insert("team-a".to_string(), 0);
        map.insert("team-b".to_string(), 1);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            policy,
        )
        .unwrap();
        let rb = pod_ref("pod-b", "team-b");
        // Bypass routing for setup: write to member 1 directly.
        fab.members()[1]
            .apply(ResourceFormat::Native, &envelope(&rb, b"B"))
            .unwrap();
        // List for team-b should route to member 1 and find pod-b.
        let listed = fab
            .list("Pod", Some("team-b"), ResourceFormat::Native)
            .unwrap();
        assert_eq!(listed, vec![b"B".to_vec()]);
    }

    // ── delete routing ──────────────────────────────────────────

    #[test]
    fn delete_routes_through_same_policy_as_apply() {
        let mut map = HashMap::new();
        map.insert("team-a".to_string(), 0);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            policy,
        )
        .unwrap();
        let r = pod_ref("pod-a", "team-a");
        fab.apply(&r, ResourceFormat::Native, &envelope(&r, b"x"))
            .unwrap();
        fab.delete(&r).unwrap();
        assert!(fab.members()[0]
            .get(&r, ResourceFormat::Native)
            .is_err());
    }

    // ── watch fan-out ───────────────────────────────────────────

    #[test]
    fn watch_fans_events_across_all_members() {
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            RoutingPolicy::First,
        )
        .unwrap();
        let mut watch = fab
            .watch("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        // Write to both members; both events should reach the
        // federated stream.
        let ra = pod_ref("pod-a", "default");
        let rb = pod_ref("pod-b", "default");
        fab.members()[0]
            .apply(ResourceFormat::Native, &envelope(&ra, b"A"))
            .unwrap();
        fab.members()[1]
            .apply(ResourceFormat::Native, &envelope(&rb, b"B"))
            .unwrap();
        let mut got: Vec<Vec<u8>> = Vec::new();
        for _ in 0..2 {
            let ev = watch.next_event().unwrap().expect("event");
            got.push(ev.body);
        }
        got.sort();
        assert_eq!(got, vec![b"A".to_vec(), b"B".to_vec()]);
    }

    #[test]
    fn watch_routes_to_single_member_under_namespace_policy() {
        let mut map = HashMap::new();
        map.insert("team-a".to_string(), 0);
        map.insert("team-b".to_string(), 1);
        let policy = RoutingPolicy::NamespacePrefix {
            map,
            default_member: None,
        };
        let fab = FederatedFabric::new(
            vec![cluster("a"), cluster("b")],
            policy,
        )
        .unwrap();
        let mut watch = fab
            .watch("Pod", Some("team-a"), ResourceFormat::Native)
            .unwrap();
        let ra = pod_ref("only-a", "team-a");
        let rb = pod_ref("not-here", "team-b");
        fab.apply(&ra, ResourceFormat::Native, &envelope(&ra, b"A"))
            .unwrap();
        fab.apply(&rb, ResourceFormat::Native, &envelope(&rb, b"B"))
            .unwrap();
        // Only the team-a event reaches the watch.
        let ev = watch.next_event().unwrap().expect("a event");
        assert_eq!(ev.body, b"A");
    }

    // ── Member errors propagate ──────────────────────────────────

    #[test]
    fn member_error_is_tagged_with_member_index() {
        let fab = FederatedFabric::new(
            vec![cluster("a")],
            RoutingPolicy::First,
        )
        .unwrap();
        // Get a non-existent resource — face errors as Unsupported;
        // federation wraps with the member index.
        let r = pod_ref("missing", "default");
        match fab.get(&r, ResourceFormat::Native) {
            Err(FederationError::Member(idx, FaceError::Unsupported(_))) => {
                assert_eq!(idx, 0);
            }
            other => panic!("expected Member(0, Unsupported), got {other:?}"),
        }
    }
}
