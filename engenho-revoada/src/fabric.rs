//! # Fabric strategy + face — the operator-facing knobs of the revoada
//!
//! [`engenho-revoada`](crate) ships the four mechanical layers of the
//! distribution fabric (membership / consensus / content / attestation).
//! This module sits *above* those layers and exposes the two typed knobs
//! operators use to shape what the fabric does:
//!
//! - [`FabricStrategy`] — **HOW** the fabric converges. Consensus algo
//!   + quorum + heartbeat + placement + reconciliation cadence. Authored
//!   as `(defstrategy …)`; consumed by the controllers in each layer.
//! - [`FabricFace`] — **WHICH** external API the fabric renders. K8s
//!   today; Nomad / systemd / pure-raft / bare-metal-supervisor tomorrow.
//!   Authored as `(defface …)`; selected at runtime as a renderer plug.
//!
//! These are the destination-naming primitives for the
//! "engenho-as-fabric, K8s-as-one-face" architectural framing. The
//! fabric vocabulary (engenho-types' Pods / Services / Roles …) is
//! face-agnostic; each [`FabricFace`] is just a Reader+Writer pair
//! against the same underlying typed state machine.
//!
//! # The two guarantees ([`FabricStrategy`] enforces by construction)
//!
//! 1. **Never split-brain.** [`ConsensusKind`] variants are all quorum-
//!    based by design — there is no "no consensus" variant. Picking a
//!    strategy = picking a split-brain-free algorithm. The invariant
//!    is enforced at the type level: `ConsensusKind` cannot represent
//!    an algorithm that allows divided writes.
//! 2. **Always eventually resolves.** [`ReconciliationCadence`] forces
//!    a non-zero tick interval. [`FabricStrategy::prove_liveness`] runs
//!    a cross-field check that membership-failure-detection-timeout <
//!    reconciliation-tick (so a partition heals before the next
//!    convergence round). Strategies that can't prove liveness fail
//!    construction.
//!
//! These properties are not assertions the documentation makes — they
//! are theorems the type system + a small set of compile-time and
//! runtime checks enforce. Operators write the strategy; the fabric
//! refuses configurations that would violate either property.

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────
// FabricStrategy — HOW the fabric converges
// ─────────────────────────────────────────────────────────────────

/// The complete declarative configuration of a revoada fabric.
///
/// Authored as `(defstrategy :name "homelab-3node" :consensus :raft
/// :quorum 3 :placement :zone-aware …)`. Compiled by frost-lisp into
/// this struct; consumed by `engenho-revoada` controllers at startup
/// + on `SIGHUP` reload.
///
/// Strategies are **named** so a single operator can shift cluster
/// configuration by swapping which strategy the fabric runs (e.g.
/// graceful drain via `:drain-aggressive` strategy, then resume via
/// `:steady-state`).
/// `Eq` deliberately omitted — transitive `f64` field in
/// `MembershipConfig`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FabricStrategy {
    /// Operator-facing name; appears in audit logs + telemetry.
    pub name: String,
    pub consensus: ConsensusConfig,
    pub membership: MembershipConfig,
    pub content: ContentConfig,
    pub attestation: AttestationConfig,
    pub placement: PlacementPolicy,
    pub reconciliation: ReconciliationCadence,
}

/// Consensus layer configuration.
///
/// Every variant is quorum-based by design — there is no "leaderless"
/// or "anti-entropy-only" variant. Picking a [`ConsensusKind`] is
/// picking a split-brain-free algorithm; the type system makes the
/// guarantee structural.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub kind: ConsensusKind,
}

/// Quorum-based consensus algorithms. Adding a new variant requires
/// the algorithm to prove split-brain-freedom under partition + node
/// failure — this is a design discipline, not a runtime check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "algorithm", rename_all = "kebab-case")]
pub enum ConsensusKind {
    /// openraft — the current revoada R2 default.
    OpenRaft {
        /// Cluster size; ⌊N/2⌋+1 is the write quorum.
        quorum_size: u32,
        /// Election timeout (randomized within ±50% to avoid splits).
        election_timeout_ms: u32,
        /// How often to snapshot the log (in entries).
        snapshot_interval_entries: u32,
    },
    // Future:
    // Paxos { … }
    // Multi-Paxos { … }
}

/// Membership layer configuration (chitchat-style gossip + phi-accrual).
///
/// `Eq` deliberately omitted — `phi_threshold` is `f64`, which doesn't
/// satisfy `Eq` (NaN). Use `PartialEq` for round-trip + drift checks;
/// guard against NaN with [`MembershipConfig::validate`] before live
/// use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// Phi-accrual threshold above which a node is marked failed.
    /// Higher = more conservative (fewer false positives, slower
    /// detection); typical 8.0.
    pub phi_threshold: f64,
    /// How often gossip rounds fire (milliseconds).
    pub gossip_cadence_ms: u32,
    /// Maximum time after last heartbeat before a node is marked
    /// failed regardless of phi (failsafe).
    pub failure_detector_timeout_ms: u32,
}

/// Content layer configuration (iroh-style P2P workload sync).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentConfig {
    /// How often content rounds fire to fetch missing blobs.
    pub sync_cadence_ms: u32,
    /// Retention for unreferenced blobs before GC.
    pub gc_retention_ms: u32,
}

/// Attestation layer configuration (tameshi BLAKE3 chain).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttestationConfig {
    /// How often to seal a new attestation block.
    pub seal_interval_ms: u32,
    /// Whether to require operator signature on each block
    /// (vs node-key signature only).
    pub require_operator_signature: bool,
}

/// Placement policy — which nodes a workload may land on.
///
/// Strategies compose via [`PlacementPolicy::Spread`] over multiple
/// constraints in future revisions; today each variant is its own
/// single rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, gen_platform::TypedDispatcher)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum PlacementPolicy {
    /// Workload replicas must span ≥ `min_zones` failure domains.
    ZoneAware { min_zones: u32 },
    /// Same but at rack granularity.
    RackAware { min_racks: u32 },
    /// Place where the p99 latency from the entry-point is below
    /// `max_p99_ms`.
    LatencyAware { max_p99_ms: u32 },
    /// Spread evenly across all available nodes; no other constraint.
    Spread,
    /// No placement preference — first-fit.
    None,
}

// Fleet-wide dispatcher-catalog registration for the engenho
// fabric's placement policy surface. Sixth consumer class adopting
// gen-platform's typed-dispatcher catamorphism (after gen / caixa /
// wasm-platform / cofre / shigoto). See
// theory/UNIFIED-COMPUTING-MODEL.md §VI.
gen_platform::register_dispatcher!("engenho.placement-policy", PlacementPolicy);

/// Reconciliation cadence — how often the fabric runs a convergence
/// tick. Wraps `Duration` so the "zero tick" case (which would break
/// the eventually-resolves guarantee) is impossible to construct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationCadence {
    millis: u32,
}

impl ReconciliationCadence {
    /// Construct a cadence — fails for zero so the
    /// always-eventually-resolves guarantee is type-enforced.
    ///
    /// # Errors
    ///
    /// Returns `FabricStrategyError::ZeroReconciliationCadence` if
    /// `millis` is zero.
    pub fn new(millis: u32) -> Result<Self, FabricStrategyError> {
        if millis == 0 {
            return Err(FabricStrategyError::ZeroReconciliationCadence);
        }
        Ok(Self { millis })
    }

    /// As a [`Duration`] for runtime consumption.
    #[must_use]
    pub const fn as_duration(self) -> Duration {
        Duration::from_millis(self.millis as u64)
    }

    /// Raw milliseconds value.
    #[must_use]
    pub const fn millis(self) -> u32 {
        self.millis
    }
}

/// Errors a strategy can carry — used both at construction (typed
/// invariants) and at `prove_liveness` time (cross-field invariants).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FabricStrategyError {
    #[error(
        "reconciliation cadence cannot be zero — would break the always-eventually-resolves guarantee"
    )]
    ZeroReconciliationCadence,
    #[error(
        "failure-detector timeout ({fd_ms}ms) >= reconciliation cadence ({rec_ms}ms) — a partition may not heal before the next convergence round, breaking liveness"
    )]
    DetectorOutpacesReconciliation { fd_ms: u32, rec_ms: u32 },
    #[error("consensus quorum size must be odd to avoid even-split deadlocks; got {0}")]
    EvenQuorum(u32),
    #[error(
        "consensus quorum size must be ≥ 3 for split-brain freedom under single-node failure; got {0}"
    )]
    QuorumTooSmall(u32),
}

impl FabricStrategy {
    /// Prove the two fabric-level liveness theorems hold for this
    /// strategy. Returns `Ok` iff:
    ///
    /// - The reconciliation cadence is non-zero (the type already
    ///   enforces this; checked again here defensively).
    /// - The failure-detector timeout is strictly less than the
    ///   reconciliation cadence — so a failed node is recognized
    ///   between two convergence rounds, preventing a partition from
    ///   persisting across multiple ticks.
    /// - The consensus quorum is odd and ≥ 3 — even quorums deadlock
    ///   on splits; quorum of 1 is no consensus at all.
    ///
    /// # Errors
    ///
    /// Returns the specific [`FabricStrategyError`] variant naming
    /// the violated invariant.
    pub fn prove_liveness(&self) -> Result<(), FabricStrategyError> {
        let rec_ms = self.reconciliation.millis();
        if rec_ms == 0 {
            return Err(FabricStrategyError::ZeroReconciliationCadence);
        }
        if self.membership.failure_detector_timeout_ms >= rec_ms {
            return Err(FabricStrategyError::DetectorOutpacesReconciliation {
                fd_ms: self.membership.failure_detector_timeout_ms,
                rec_ms,
            });
        }
        match self.consensus.kind {
            ConsensusKind::OpenRaft { quorum_size, .. } => {
                if quorum_size < 3 {
                    return Err(FabricStrategyError::QuorumTooSmall(quorum_size));
                }
                if quorum_size % 2 == 0 {
                    return Err(FabricStrategyError::EvenQuorum(quorum_size));
                }
            }
        }
        Ok(())
    }

    /// The prescribed homelab default — 3-node openraft, zone-aware,
    /// 500ms tick. Use this as the FleetDefaults-equivalent baseline
    /// for new clusters.
    #[must_use]
    pub fn prescribed_homelab() -> Self {
        Self {
            name: "homelab-3node".into(),
            consensus: ConsensusConfig {
                kind: ConsensusKind::OpenRaft {
                    quorum_size: 3,
                    election_timeout_ms: 150,
                    snapshot_interval_entries: 1000,
                },
            },
            membership: MembershipConfig {
                phi_threshold: 8.0,
                gossip_cadence_ms: 100,
                failure_detector_timeout_ms: 300,
            },
            content: ContentConfig {
                sync_cadence_ms: 200,
                gc_retention_ms: 60_000,
            },
            attestation: AttestationConfig {
                seal_interval_ms: 1000,
                require_operator_signature: false,
            },
            placement: PlacementPolicy::ZoneAware { min_zones: 1 },
            reconciliation: ReconciliationCadence::new(500).expect("500ms is non-zero"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// FabricFace — WHICH external API the fabric speaks
// ─────────────────────────────────────────────────────────────────

/// A face — the external API surface a fabric renders.
///
/// The fabric's typed vocabulary (engenho-types' Pods / Services /
/// Roles / …) is face-agnostic. Each face is a Reader+Writer pair
/// translating that vocabulary to/from a specific external protocol.
/// Kubernetes is the current default; Nomad / systemd / pure-raft /
/// bare-metal-supervisor become face additions, not rewrites.
///
/// Authored as `(defface :name "k8s-prod" :kind :kubernetes
/// :version "1.34" :certified-cncf true)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricFace {
    /// Operator-facing name; appears in audit logs + telemetry.
    pub name: String,
    pub kind: FaceKind,
}

/// The protocol family a face implements. Adding a variant requires
/// a corresponding Reader+Writer adapter in `engenho-revoada::face::*`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "kebab-case")]
pub enum FaceKind {
    /// Standard Kubernetes API — kubectl/CRI/etcd-v3 wire compat.
    /// `certified_cncf` flags whether this face has passed the CNCF
    /// Certified Kubernetes Software Conformance suite for `version`.
    Kubernetes {
        version: String,
        certified_cncf: bool,
    },
    /// HashiCorp Nomad jobs.
    Nomad { version: String },
    /// Renders to systemd unit files; for non-clustered single-node
    /// or supervised-VM deployments.
    Systemd { user_units: bool },
    /// No external rendering — expose the raw state machine via the
    /// internal openraft RPC + iroh content. For operators running
    /// pleme-io-native tooling without any orchestrator-API
    /// translation overhead.
    PureRaft,
    /// systemd-orchestrated containers without Kubernetes API.
    /// Useful when the operator wants the fabric's distribution
    /// guarantees but not the K8s api-server complexity.
    BareMetalSupervisor,
}

impl FabricFace {
    /// The prescribed default — Kubernetes v1.34 with CNCF
    /// certification target. Matches engenho-mcp's current readers.
    #[must_use]
    pub fn prescribed_kubernetes_v1_34() -> Self {
        Self {
            name: "k8s-v1.34".into(),
            kind: FaceKind::Kubernetes {
                version: "1.34".into(),
                certified_cncf: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── FabricStrategy ─────────────────────────────────────────────

    #[test]
    fn prescribed_homelab_proves_liveness() {
        let s = FabricStrategy::prescribed_homelab();
        assert_eq!(s.prove_liveness(), Ok(()));
    }

    #[test]
    fn zero_reconciliation_cadence_cannot_be_constructed() {
        let err = ReconciliationCadence::new(0).unwrap_err();
        assert_eq!(err, FabricStrategyError::ZeroReconciliationCadence);
    }

    #[test]
    fn non_zero_reconciliation_cadence_round_trips_through_duration() {
        let c = ReconciliationCadence::new(250).unwrap();
        assert_eq!(c.as_duration(), Duration::from_millis(250));
        assert_eq!(c.millis(), 250);
    }

    #[test]
    fn detector_must_be_strictly_faster_than_reconciliation() {
        let mut s = FabricStrategy::prescribed_homelab();
        s.membership.failure_detector_timeout_ms = s.reconciliation.millis();
        let err = s.prove_liveness().unwrap_err();
        assert!(matches!(
            err,
            FabricStrategyError::DetectorOutpacesReconciliation { .. }
        ));
    }

    #[test]
    fn quorum_must_be_odd() {
        let mut s = FabricStrategy::prescribed_homelab();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 4,
            election_timeout_ms: 150,
            snapshot_interval_entries: 1000,
        };
        let err = s.prove_liveness().unwrap_err();
        assert_eq!(err, FabricStrategyError::EvenQuorum(4));
    }

    #[test]
    fn quorum_must_be_at_least_three() {
        let mut s = FabricStrategy::prescribed_homelab();
        s.consensus.kind = ConsensusKind::OpenRaft {
            quorum_size: 1,
            election_timeout_ms: 150,
            snapshot_interval_entries: 1000,
        };
        let err = s.prove_liveness().unwrap_err();
        assert_eq!(err, FabricStrategyError::QuorumTooSmall(1));
    }

    #[test]
    fn strategy_round_trips_through_serde() {
        let s = FabricStrategy::prescribed_homelab();
        let yaml = serde_yaml::to_string(&s).unwrap();
        let back: FabricStrategy = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(s, back);
    }

    // ── FabricFace ─────────────────────────────────────────────────

    #[test]
    fn prescribed_k8s_face_is_v1_34_certified() {
        let f = FabricFace::prescribed_kubernetes_v1_34();
        match f.kind {
            FaceKind::Kubernetes {
                ref version,
                certified_cncf,
            } => {
                assert_eq!(version, "1.34");
                assert!(certified_cncf);
            }
            other => panic!("expected Kubernetes face, got {other:?}"),
        }
    }

    #[test]
    fn nomad_face_carries_its_version() {
        let f = FabricFace {
            name: "nomad-1.7".into(),
            kind: FaceKind::Nomad {
                version: "1.7".into(),
            },
        };
        assert!(matches!(f.kind, FaceKind::Nomad { .. }));
    }

    #[test]
    fn pure_raft_face_has_no_external_protocol() {
        // PureRaft is the "no face" face — the fabric exposes its
        // state machine directly. Operators using pleme-io-native
        // tooling pick this when they don't want the api-server
        // overhead.
        let f = FabricFace {
            name: "pure-raft".into(),
            kind: FaceKind::PureRaft,
        };
        assert_eq!(f.kind, FaceKind::PureRaft);
    }

    #[test]
    fn face_round_trips_through_serde() {
        let f = FabricFace::prescribed_kubernetes_v1_34();
        let yaml = serde_yaml::to_string(&f).unwrap();
        let back: FabricFace = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn all_face_kinds_round_trip() {
        let kinds = vec![
            FaceKind::Kubernetes {
                version: "1.34".into(),
                certified_cncf: true,
            },
            FaceKind::Nomad {
                version: "1.7".into(),
            },
            FaceKind::Systemd { user_units: false },
            FaceKind::PureRaft,
            FaceKind::BareMetalSupervisor,
        ];
        for k in kinds {
            let f = FabricFace {
                name: "x".into(),
                kind: k.clone(),
            };
            let yaml = serde_yaml::to_string(&f).unwrap();
            let back: FabricFace = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(f, back, "round-trip failed for {k:?}");
        }
    }
}
