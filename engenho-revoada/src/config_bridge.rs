//! Bridge from operator-facing [`engenho_config::RevoadaConfig`]
//! to a runtime [`Box<dyn TopologyStrategy>`].
//!
//! Factory function — operator picks `phalanx` (or `solo`/`pair`/
//! `quorum_3m`/`cluster_3m_nw`/`mesh_all_peers`) in the YAML;
//! this constructs the matching trait object.

use engenho_config::TopologyStrategyKind;

use crate::topology::{Cluster3MNW, MeshAllPeers, Pair, Phalanx, Quorum3M, Solo, TopologyStrategy};

/// Construct the topology trait object the operator's config asks for.
#[must_use]
pub fn make_topology_strategy(cfg: &engenho_config::RevoadaConfig) -> Box<dyn TopologyStrategy> {
    match cfg.topology.strategy {
        TopologyStrategyKind::Solo => Box::new(Solo),
        TopologyStrategyKind::Pair => Box::new(Pair),
        TopologyStrategyKind::Quorum3M => Box::new(Quorum3M),
        TopologyStrategyKind::Cluster3MNW => Box::new(Cluster3MNW),
        TopologyStrategyKind::MeshAllPeers => Box::new(MeshAllPeers),
        TopologyStrategyKind::Phalanx => Box::new(Phalanx),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_config::{RevoadaConfig, TopologyConfig};

    fn cfg_for(kind: TopologyStrategyKind) -> RevoadaConfig {
        RevoadaConfig {
            topology: TopologyConfig {
                strategy: kind,
                min_nodes: 1,
                grace_period_seconds: 10,
            },
        }
    }

    #[test]
    fn every_strategy_kind_constructs_a_trait_object() {
        let cases = [
            (TopologyStrategyKind::Solo, "solo"),
            (TopologyStrategyKind::Pair, "pair"),
            (TopologyStrategyKind::Quorum3M, "quorum_3m"),
            (TopologyStrategyKind::Cluster3MNW, "cluster_3m_nw"),
            (TopologyStrategyKind::MeshAllPeers, "mesh_all_peers"),
            (TopologyStrategyKind::Phalanx, "phalanx"),
        ];
        for (kind, expected_name) in cases {
            let s = make_topology_strategy(&cfg_for(kind));
            assert_eq!(s.name(), expected_name, "strategy mismatch for {kind:?}");
        }
    }

    #[test]
    fn default_yields_phalanx() {
        // Construct the prescribed-default RevoadaConfig directly
        // (no shikumi dep in revoada itself).
        let cfg = RevoadaConfig {
            topology: TopologyConfig {
                strategy: TopologyStrategyKind::Phalanx,
                min_nodes: 1,
                grace_period_seconds: 10,
            },
        };
        let s = make_topology_strategy(&cfg);
        assert_eq!(s.name(), "phalanx");
    }
}
