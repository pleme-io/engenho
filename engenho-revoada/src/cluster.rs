//! # Cluster declaration — the typed triple
//!
//! A cluster is the composition of three orthogonal typed surfaces:
//!
//! - [`FabricStrategy`] — HOW the fabric converges (consensus +
//!   placement + cadence).
//! - [`FabricFace`] — WHICH external API the fabric speaks (K8s /
//!   Nomad / Systemd / PureRaft / BareMetalSupervisor).
//! - [`Box<dyn TopologyStrategy>`] — WHAT SHAPE the cluster takes
//!   (Solo / Pair / Quorum3M / Cluster3MNW / MeshAllPeers / Phalanx).
//!
//! Each surface alone is well-typed. The interesting failure mode is
//! the *combination*: declaring a Solo (1-node) topology alongside a
//! consensus strategy that needs a 3-quorum is well-formed in
//! isolation but **incoherent as a cluster** — the cluster will boot
//! but immediately fail liveness because no quorum can ever form.
//!
//! [`ClusterDeclaration`] is the typed shield. Construction runs
//! every cross-check between the three surfaces; if any fires, the
//! declaration cannot exist. The runtime takes `ClusterDeclaration`
//! and trusts the typed witness — no runtime coherence detection
//! needed, because the configuration that would allow incoherence
//! cannot be expressed.
//!
//! This is the "by construction" form of the engenho-as-fabric
//! invariant chain:
//!
//! 1. [`ReconciliationCadence::new`] rejects zero ticks.
//! 2. [`FabricStrategy::prove_liveness`] rejects incoherent strategy
//!    self-fields.
//! 3. [`ClusterDeclaration::new`] rejects incoherent
//!    strategy + face + topology combinations.
//!
//! Three layers, each catching a strictly larger class of errors at
//! a strictly earlier moment.

use crate::fabric::{ConsensusKind, FabricFace, FabricStrategy, FaceKind};
use crate::topology::TopologyStrategy;

/// Errors that surface when a [`ClusterDeclaration`] is constructed
/// from incoherent inputs. Each variant names exactly which cross-
/// surface invariant the inputs violated.
#[derive(Debug, thiserror::Error)]
pub enum ClusterCoherenceError {
    #[error("strategy self-incoherent: {0}")]
    StrategyError(#[from] crate::fabric::FabricStrategyError),

    #[error(
        "topology {topology:?} requires only {topology_min} nodes minimum but strategy consensus quorum is {quorum_size} — the cluster will boot but immediately fail liveness because no quorum can ever form"
    )]
    TopologyTooSmallForQuorum {
        topology: String,
        topology_min: usize,
        quorum_size: usize,
    },

    #[error(
        "face {face:?} ({face_kind}) requires operator signatures on every attestation block, but the strategy does not require operator signatures — this exposes the fabric to operator-impersonation attacks since the face surface promises something the strategy doesn't enforce"
    )]
    FaceSignaturePromiseUnmet {
        face: String,
        face_kind: &'static str,
    },
}

/// A fully-coherent declaration of a cluster. The only way to obtain
/// one is via [`ClusterDeclaration::new`], which runs every cross-
/// surface check before constructing.
///
/// Once constructed, the runtime can pass this value around and
/// trust that every invariant in [`ClusterCoherenceError`] holds.
/// No runtime re-verification needed; the typed witness IS the
/// proof carrier.
#[must_use = "ClusterDeclaration carries proofs; consume it via the runtime entry point"]
pub struct ClusterDeclaration {
    strategy: FabricStrategy,
    face: FabricFace,
    topology: Box<dyn TopologyStrategy>,
}

impl ClusterDeclaration {
    /// Construct from the three surfaces. Runs every cross-coherence
    /// check; returns `Err` naming the first violated invariant.
    ///
    /// # Errors
    ///
    /// - [`ClusterCoherenceError::StrategyError`] when the strategy
    ///   fails its own [`FabricStrategy::prove_liveness`] check.
    /// - [`ClusterCoherenceError::TopologyTooSmallForQuorum`] when
    ///   the topology can't satisfy the consensus quorum size.
    /// - [`ClusterCoherenceError::FaceSignaturePromiseUnmet`] when
    ///   the face promises operator signatures but the strategy
    ///   doesn't enforce them.
    pub fn new(
        strategy: FabricStrategy,
        face: FabricFace,
        topology: Box<dyn TopologyStrategy>,
    ) -> Result<Self, ClusterCoherenceError> {
        // Check #1: strategy is internally coherent.
        strategy.prove_liveness()?;

        // Check #2: topology can satisfy the consensus quorum.
        let quorum_size = match strategy.consensus.kind {
            ConsensusKind::OpenRaft { quorum_size, .. } => quorum_size as usize,
        };
        if topology.min_nodes() < quorum_size {
            return Err(ClusterCoherenceError::TopologyTooSmallForQuorum {
                topology: topology.name().to_string(),
                topology_min: topology.min_nodes(),
                quorum_size,
            });
        }

        // Check #3: face attestation promises match strategy
        // attestation enforcement. The Kubernetes face publishes its
        // audit log externally; if the face's contract implies
        // operator-attested events but the strategy doesn't enforce
        // operator-signed seals, the audit log shows attestations
        // the strategy never actually validated.
        //
        // The current Face variants don't expose a
        // "requires-operator-signature" flag — every face is content
        // with whatever the strategy provides. This check is
        // reserved for future face variants that publish their
        // operator-attestation guarantee externally (e.g. a
        // future FedRAMP-mode face) so the cross-check shape is
        // already in place when that variant lands.
        let _ = &face;

        Ok(Self {
            strategy,
            face,
            topology,
        })
    }

    /// Borrow the strategy.
    #[must_use]
    pub fn strategy(&self) -> &FabricStrategy {
        &self.strategy
    }

    /// Borrow the face.
    #[must_use]
    pub fn face(&self) -> &FabricFace {
        &self.face
    }

    /// Borrow the topology trait object.
    #[must_use]
    pub fn topology(&self) -> &dyn TopologyStrategy {
        self.topology.as_ref()
    }

    /// Stable identifier for telemetry: `"<face>/<strategy>/<topology>"`.
    #[must_use]
    pub fn id(&self) -> String {
        format!(
            "{}/{}/{}",
            self.face.name,
            self.strategy.name,
            self.topology.name()
        )
    }
}

impl std::fmt::Debug for ClusterDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterDeclaration")
            .field("strategy", &self.strategy.name)
            .field("face", &self.face.name)
            .field("face_kind", &face_kind_str(&self.face.kind))
            .field("topology", &self.topology.name())
            .finish()
    }
}

fn face_kind_str(kind: &FaceKind) -> &'static str {
    match kind {
        FaceKind::Kubernetes { .. } => "Kubernetes",
        FaceKind::Nomad { .. } => "Nomad",
        FaceKind::Systemd { .. } => "Systemd",
        FaceKind::PureRaft => "PureRaft",
        FaceKind::BareMetalSupervisor => "BareMetalSupervisor",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric::{ConsensusConfig, FabricStrategy, ReconciliationCadence};
    use crate::topology::{Cluster3MNW, Phalanx, Quorum3M, Solo};

    fn prescribed_strategy() -> FabricStrategy {
        FabricStrategy::prescribed_homelab()
    }

    fn raft_face() -> FabricFace {
        FabricFace {
            name: "pure-raft".into(),
            kind: FaceKind::PureRaft,
        }
    }

    fn k8s_face() -> FabricFace {
        FabricFace::prescribed_kubernetes_v1_34()
    }

    // ── Happy path ────────────────────────────────────────────────

    #[test]
    fn quorum3m_topology_works_with_3node_consensus() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            raft_face(),
            Box::new(Quorum3M),
        );
        assert!(cluster.is_ok(), "Quorum3M + OpenRaft{{3}} should be coherent");
    }

    #[test]
    fn cluster3mnw_topology_works_with_3node_consensus() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            k8s_face(),
            Box::new(Cluster3MNW),
        );
        assert!(cluster.is_ok(), "Cluster3MNW + OpenRaft{{3}} should be coherent");
    }

    #[test]
    fn phalanx_topology_works_with_3node_consensus() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            raft_face(),
            Box::new(Phalanx),
        );
        // Phalanx min_nodes is configured by Phalanx itself; if it
        // satisfies the 3-quorum it must succeed here.
        if Phalanx.min_nodes() >= 3 {
            assert!(cluster.is_ok(), "Phalanx min >= 3 should be coherent");
        } else {
            assert!(cluster.is_err(), "Phalanx min < 3 should fail coherence");
        }
    }

    // ── Cross-check fires correctly ───────────────────────────────

    #[test]
    fn solo_topology_fails_against_3node_consensus() {
        // Solo.min_nodes() == 1, OpenRaft quorum == 3 → coherence
        // failure. The cluster cannot boot to a state where any
        // write is committed.
        let err = ClusterDeclaration::new(
            prescribed_strategy(),
            raft_face(),
            Box::new(Solo),
        )
        .unwrap_err();
        match err {
            ClusterCoherenceError::TopologyTooSmallForQuorum {
                topology,
                topology_min,
                quorum_size,
            } => {
                assert_eq!(topology, "solo");
                assert_eq!(topology_min, 1);
                assert_eq!(quorum_size, 3);
            }
            other => panic!("expected TopologyTooSmallForQuorum, got {other:?}"),
        }
    }

    #[test]
    fn invalid_strategy_propagates_through_cluster_construction() {
        // A strategy that fails prove_liveness must surface up
        // through ClusterDeclaration::new rather than slipping past.
        let mut s = prescribed_strategy();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 4, // even quorum — should fail prove_liveness
            election_timeout_ms: 150,
            snapshot_interval_entries: 1000,
        };
        let err = ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).unwrap_err();
        assert!(matches!(err, ClusterCoherenceError::StrategyError(_)));
    }

    #[test]
    fn detector_outpaces_reconciliation_caught_via_cluster() {
        let mut s = prescribed_strategy();
        s.membership.failure_detector_timeout_ms = s.reconciliation.millis() + 1;
        let err = ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).unwrap_err();
        match err {
            ClusterCoherenceError::StrategyError(inner) => {
                assert!(matches!(
                    inner,
                    crate::fabric::FabricStrategyError::DetectorOutpacesReconciliation { .. }
                ));
            }
            other => panic!("expected nested StrategyError, got {other:?}"),
        }
    }

    // ── Accessors + observability ────────────────────────────────

    #[test]
    fn cluster_id_format_is_stable() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            k8s_face(),
            Box::new(Quorum3M),
        )
        .unwrap();
        let id = cluster.id();
        assert!(id.contains("k8s-v1.34"));
        assert!(id.contains("homelab-3node"));
        assert!(id.contains(Quorum3M.name()));
    }

    #[test]
    fn accessors_return_the_constructed_surfaces() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            k8s_face(),
            Box::new(Cluster3MNW),
        )
        .unwrap();
        assert_eq!(cluster.strategy().name, "homelab-3node");
        assert_eq!(cluster.face().name, "k8s-v1.34");
        assert_eq!(cluster.topology().name(), Cluster3MNW.name());
    }

    #[test]
    fn debug_format_names_all_three_surfaces() {
        let cluster = ClusterDeclaration::new(
            prescribed_strategy(),
            raft_face(),
            Box::new(Quorum3M),
        )
        .unwrap();
        let dbg = format!("{cluster:?}");
        assert!(dbg.contains("homelab-3node"));
        assert!(dbg.contains("pure-raft"));
        assert!(dbg.contains("PureRaft"));
        assert!(dbg.contains(Quorum3M.name()));
    }

    #[test]
    fn nonzero_reconciliation_with_huge_election_timeout_still_works() {
        // Sanity: prove_liveness only enforces the cross-field
        // invariants we declared; arbitrary other knob values are
        // allowed. This test pins the "minimal-rule, not
        // exhaustive" surface so future maintainers don't add
        // accidental rules.
        let mut s = prescribed_strategy();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 3,
            election_timeout_ms: 60_000, // 1 minute — extreme but valid
            snapshot_interval_entries: 1,
        };
        assert!(
            ClusterDeclaration::new(s, raft_face(), Box::new(Quorum3M)).is_ok()
        );
        let _ = ConsensusConfig {
            kind: ConsensusKind::OpenRaft {
                quorum_size: 3,
                election_timeout_ms: 1,
                snapshot_interval_entries: 1,
            },
        };
        // Confirm the imports compile cleanly.
        let _ = ReconciliationCadence::new(1).unwrap();
    }
}
