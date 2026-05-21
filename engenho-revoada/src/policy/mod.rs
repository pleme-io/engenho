//! Layer R3 — typed policy engine.
//!
//! The policy engine is the **closing loop** that wires Layer A
//! (gossip-detected membership) to Layer B (Raft-committed
//! consensus). Where R1 sees who's alive + R2 commits role
//! assignments, R3 emits the typed proposals that turn a
//! membership view into an evolved [`MeshShape`].
//!
//! ## Architecture
//!
//! [`Policy`] is the trait abstraction. Each policy is a pure
//! function `(membership, consensus, target) → Vec<RoleAssignment>`.
//! Multiple policies compose in a [`PolicyEngine`]: each tick
//! evaluates every registered policy in order; the resulting
//! proposals all flow through the same [`crate::consensus::RaftMesh`]
//! commit path so consensus is preserved.
//!
//! ## Today's policies
//!
//!   * [`AutoReplacementPolicy`] — keep the role counts at the
//!     declared [`TargetTopology`]. If a role-holder disappears
//!     from gossip, propose Demote + propose Promote on a healthy
//!     replacement.
//!
//! ## Discipline
//!
//! Policies MUST be:
//!   * Pure — no I/O during `evaluate`. Read the inputs, emit
//!     proposals; the engine commits.
//!   * Idempotent — re-running the same evaluation against the
//!     same inputs produces the same proposal set. The engine may
//!     re-tick on every gossip change; bad behavior under retry
//!     would amplify into oscillation.
//!   * Bounded — emit at most a few proposals per tick. The Raft
//!     log shouldn't grow without bound from policy thrashing.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::consensus::{ApplyResult, RaftError, RaftMesh, Reason, RoleAssignment};
use crate::membership::{GossipMesh, MembershipView, NodeRole};
use crate::NodeId;

/// The "desired shape" of the mesh — declared by the operator.
/// Policies compare this against `MeshShape` (the actual
/// assignments) to decide what proposals to emit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetTopology {
    pub api_servers: u32,
    pub etcd_replicas: u32,
    pub schedulers: u32,
    pub controller_managers: u32,
    pub min_workers: u32,
}

impl TargetTopology {
    /// Sane default for a small homelab cluster.
    #[must_use]
    pub fn homelab() -> Self {
        Self {
            api_servers: 1,
            etcd_replicas: 1,
            schedulers: 1,
            controller_managers: 1,
            min_workers: 1,
        }
    }

    /// A reasonable HA shape.
    #[must_use]
    pub fn ha_three() -> Self {
        Self {
            api_servers: 3,
            etcd_replicas: 3,
            schedulers: 1,
            controller_managers: 1,
            min_workers: 3,
        }
    }

    pub fn target_for(&self, role: NodeRole) -> u32 {
        match role {
            NodeRole::ApiServer => self.api_servers,
            NodeRole::Etcd => self.etcd_replicas,
            NodeRole::Scheduler => self.schedulers,
            NodeRole::ControllerManager => self.controller_managers,
            NodeRole::Worker => self.min_workers,
            NodeRole::Quarantined | NodeRole::Observer => 0,
        }
    }
}

/// Pure decision function — the policy abstraction.
#[async_trait]
pub trait Policy: Send + Sync {
    /// Stable identifier for logs + telemetry.
    fn name(&self) -> &'static str;

    /// Inspect inputs + emit proposed assignments. Pure: must NOT
    /// touch I/O or commit. The engine drives commits.
    async fn evaluate(
        &self,
        membership: &MembershipView,
        consensus: &crate::consensus::MeshShape,
        target: &TargetTopology,
    ) -> Vec<RoleAssignment>;
}

/// **`AutoReplacementPolicy`** — keep role counts at the target.
///
/// For each control-plane role (ApiServer/Etcd/Scheduler/
/// ControllerManager):
///
///   1. Filter assignments to nodes still alive in gossip. Holders
///      that vanished from gossip are "dead" → emit `Demote` for
///      the missing role.
///   2. Compare alive-holder count to `target`. If short, find a
///      healthy gossip-member without the role + emit `Promote`.
///
/// Workers are NOT auto-promoted — they accumulate naturally as
/// new nodes join + the min_workers floor is informational only
/// at R3 (R4 may auto-quarantine excess workers).
pub struct AutoReplacementPolicy;

#[async_trait]
impl Policy for AutoReplacementPolicy {
    fn name(&self) -> &'static str {
        "auto_replacement"
    }

    async fn evaluate(
        &self,
        membership: &MembershipView,
        consensus: &crate::consensus::MeshShape,
        target: &TargetTopology,
    ) -> Vec<RoleAssignment> {
        let mut proposals = Vec::new();

        // Set of NodeIds currently alive according to gossip.
        let alive: BTreeSet<NodeId> = membership
            .members
            .iter()
            .map(|m| m.node_id)
            .collect();

        // Iterate the four control-plane roles.
        for &role in &[
            NodeRole::ApiServer,
            NodeRole::Etcd,
            NodeRole::Scheduler,
            NodeRole::ControllerManager,
        ] {
            // Holders of this role per consensus state.
            let holders = consensus.holders(role);
            let alive_holders: BTreeSet<NodeId> =
                holders.iter().copied().filter(|n| alive.contains(n)).collect();
            let dead_holders: BTreeSet<NodeId> =
                holders.iter().copied().filter(|n| !alive.contains(n)).collect();

            // (1) Demote each dead holder for THIS role.
            for dead in &dead_holders {
                let mut relinquished = BTreeSet::new();
                relinquished.insert(role);
                proposals.push(RoleAssignment::Demote {
                    node_id: *dead,
                    roles_relinquished: relinquished,
                    reason: Reason::ReplacingFailed,
                });
            }

            // (2) Promote replacements if alive count < target.
            let needed = target.target_for(role) as i64
                - alive_holders.len() as i64;
            if needed > 0 {
                // Find healthy gossip members not already holding the role.
                let candidates: Vec<NodeId> = membership
                    .members
                    .iter()
                    .map(|m| m.node_id)
                    .filter(|n| !alive_holders.contains(n))
                    .filter(|n| {
                        // Don't propose to quarantined nodes.
                        consensus
                            .assignments
                            .get(n)
                            .map(|roles| !roles.contains(&NodeRole::Quarantined))
                            .unwrap_or(true)
                    })
                    .collect();

                for candidate in candidates.into_iter().take(needed as usize) {
                    let mut promoted = BTreeSet::new();
                    promoted.insert(role);
                    proposals.push(RoleAssignment::Promote {
                        node_id: candidate,
                        roles: promoted,
                        reason: Reason::ReplacingFailed,
                    });
                }
            }
        }

        proposals
    }
}

/// Configuration for [`PolicyEngine::start`].
pub struct PolicyEngineConfig {
    /// Periodic audit interval (proposals get re-evaluated on this
    /// timer even if gossip didn't change).
    pub audit_interval: Duration,
    /// Target topology the policies aim for.
    pub target: TargetTopology,
}

impl Default for PolicyEngineConfig {
    fn default() -> Self {
        Self {
            audit_interval: Duration::from_secs(30),
            target: TargetTopology::homelab(),
        }
    }
}

/// Runtime engine that ticks one or more [`Policy`] impls against
/// the gossip view + Raft state.
pub struct PolicyEngine {
    gossip: Arc<GossipMesh>,
    raft: Arc<RaftMesh>,
    policies: Vec<Box<dyn Policy>>,
    target: TargetTopology,
    audit_interval: Duration,
}

impl PolicyEngine {
    pub fn new(
        gossip: Arc<GossipMesh>,
        raft: Arc<RaftMesh>,
        config: PolicyEngineConfig,
    ) -> Self {
        Self {
            gossip,
            raft,
            policies: Vec::new(),
            target: config.target,
            audit_interval: config.audit_interval,
        }
    }

    /// Register a policy. Call before [`run`] / [`tick`].
    pub fn with_policy<P: Policy + 'static>(mut self, policy: P) -> Self {
        self.policies.push(Box::new(policy));
        self
    }

    /// Run ONE evaluation pass + commit any proposals through the
    /// Raft mesh. Returns the apply results. Pure — caller decides
    /// whether to schedule again.
    pub async fn tick(&self) -> Result<TickReport, RaftError> {
        // Only the leader can submit client writes; followers see
        // the same view but don't propose (the leader will).
        if !self.raft.is_leader().await {
            return Ok(TickReport::default());
        }

        let membership = self.gossip.current_view();
        let consensus = self.raft.current_shape().await;

        let mut report = TickReport::default();
        for policy in &self.policies {
            let proposals = policy
                .evaluate(&membership, &consensus, &self.target)
                .await;
            report.proposals_seen += proposals.len();
            for cmd in proposals {
                match self.raft.propose(cmd.clone()).await {
                    Ok(result) => {
                        report.applied.push((policy.name(), cmd, result));
                    }
                    Err(e) => {
                        report.errors.push((policy.name(), e.to_string()));
                    }
                }
            }
        }
        Ok(report)
    }

    /// Spawn a long-running task that ticks on either a gossip
    /// change or the audit interval. Returns a handle the caller
    /// can abort to stop the engine.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut gossip_rx = self.gossip.subscribe();
            loop {
                // React on either:
                //   - the next gossip change
                //   - the audit interval elapsing
                let _ = tokio::time::timeout(
                    self.audit_interval,
                    gossip_rx.changed(),
                )
                .await;
                let _ = self.tick().await;
            }
        })
    }
}

/// Result of one [`PolicyEngine::tick`] — informational; the
/// integration test asserts on this.
#[derive(Default)]
pub struct TickReport {
    pub proposals_seen: usize,
    pub applied: Vec<(&'static str, RoleAssignment, ApplyResult)>,
    pub errors: Vec<(&'static str, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::MeshShape;
    use crate::membership::{MembershipEntry, MembershipView, NodeCapacity, NodeState};
    use engenho_types::primitives::Quantity;
    use std::str::FromStr;

    fn ns(id: NodeId, role: NodeRole) -> NodeState {
        let mut roles = BTreeSet::new();
        roles.insert(role);
        NodeState {
            node_id: id,
            gossip_addr: "127.0.0.1:0".into(),
            raft_addr: None,
            roles,
            capacity: NodeCapacity {
                cpu: Quantity::from_str("4").unwrap(),
                memory: Quantity::from_str("8Gi").unwrap(),
                storage: Quantity::from_str("50Gi").unwrap(),
                pods: 32,
            },
            k8s_version: "v1.34.0".into(),
            uptime_sec: 0,
            membership_generation: 0,
        }
    }

    #[tokio::test]
    async fn auto_replacement_promotes_when_target_unmet() {
        let policy = AutoReplacementPolicy;
        // Three healthy nodes in gossip; consensus has nobody as ApiServer.
        let ids: Vec<NodeId> = (1..=3).map(|i| NodeId::new([i as u8; 32])).collect();
        let membership = MembershipView {
            members: ids
                .iter()
                .map(|id| MembershipEntry {
                    node_id: *id,
                    gossip_addr: "127.0.0.1:0".into(),
                    state: ns(*id, NodeRole::Worker),
                })
                .collect(),
        };
        let consensus = MeshShape::default();
        let target = TargetTopology {
            api_servers: 1,
            etcd_replicas: 0,
            schedulers: 0,
            controller_managers: 0,
            min_workers: 1,
        };

        let proposals = policy.evaluate(&membership, &consensus, &target).await;
        // One Promote for ApiServer (since 0 < target 1) — etcd / scheduler / cm targets are 0.
        assert_eq!(proposals.len(), 1);
        match &proposals[0] {
            RoleAssignment::Promote { roles, reason, .. } => {
                assert!(roles.contains(&NodeRole::ApiServer));
                assert_eq!(*reason, Reason::ReplacingFailed);
            }
            other => panic!("expected Promote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn auto_replacement_demotes_dead_holder_and_promotes_replacement() {
        let policy = AutoReplacementPolicy;
        let id_a = NodeId::new([1; 32]);
        let id_b = NodeId::new([2; 32]);
        // Gossip only sees B (A vanished).
        let membership = MembershipView {
            members: vec![MembershipEntry {
                node_id: id_b,
                gossip_addr: "127.0.0.1:0".into(),
                state: ns(id_b, NodeRole::Worker),
            }],
        };
        // Consensus has A as ApiServer.
        let mut consensus = MeshShape::default();
        let mut a_roles = BTreeSet::new();
        a_roles.insert(NodeRole::ApiServer);
        consensus.assignments.insert(id_a, a_roles);

        let target = TargetTopology {
            api_servers: 1,
            etcd_replicas: 0,
            schedulers: 0,
            controller_managers: 0,
            min_workers: 1,
        };

        let proposals = policy.evaluate(&membership, &consensus, &target).await;
        // Expect: 1 Demote on A + 1 Promote on B.
        assert_eq!(proposals.len(), 2);
        let has_demote_a = proposals.iter().any(|p| matches!(
            p,
            RoleAssignment::Demote { node_id, .. } if *node_id == id_a
        ));
        let has_promote_b = proposals.iter().any(|p| matches!(
            p,
            RoleAssignment::Promote { node_id, .. } if *node_id == id_b
        ));
        assert!(has_demote_a, "expected Demote on dead A: {proposals:?}");
        assert!(has_promote_b, "expected Promote on B: {proposals:?}");
    }

    #[tokio::test]
    async fn auto_replacement_emits_nothing_when_at_target() {
        let policy = AutoReplacementPolicy;
        let id_a = NodeId::new([1; 32]);
        let membership = MembershipView {
            members: vec![MembershipEntry {
                node_id: id_a,
                gossip_addr: "127.0.0.1:0".into(),
                state: ns(id_a, NodeRole::ApiServer),
            }],
        };
        let mut consensus = MeshShape::default();
        let mut roles = BTreeSet::new();
        roles.insert(NodeRole::ApiServer);
        consensus.assignments.insert(id_a, roles);

        let target = TargetTopology {
            api_servers: 1,
            etcd_replicas: 0,
            schedulers: 0,
            controller_managers: 0,
            min_workers: 0,
        };

        let proposals = policy.evaluate(&membership, &consensus, &target).await;
        assert!(proposals.is_empty(), "expected no proposals at target: {proposals:?}");
    }

    #[test]
    fn target_topology_default_homelab() {
        let t = TargetTopology::homelab();
        assert_eq!(t.api_servers, 1);
        assert_eq!(t.etcd_replicas, 1);
        let t_ha = TargetTopology::ha_three();
        assert_eq!(t_ha.api_servers, 3);
    }
}
