//! PlantioPipeline — builder that assembles the full production
//! roça stack from a typed config.
//!
//! The composition story today requires 8+ Arc::new calls to wire
//! the full pipeline. This builder collapses that to one call:
//!
//! ```ignore
//! let pipeline = PlantioPipeline::build(config).await?;
//! pipeline.controller.tick().await?;
//! ```
//!
//! ## What the builder does
//!
//! - Picks the right `Roceiro` (Fake or BuildBackend-composed)
//! - Picks the right `MaterializationLedger` (Memory or StoreBacked,
//!   optionally wrapped with Broadcast + Gossip)
//! - Picks the right `NodeResolver` (Static or StoreBacked)
//! - Assembles a `PlantioController` ready to tick
//!
//! ## The PipelineConfig
//!
//! Single typed value the operator constructs. Defaults match what
//! the bootstrap cluster uses (FakeRoceiro + MemoryLedger + Static
//! resolver with one node). Production overrides toggle each.

use std::sync::Arc;

use engenho_store::StoreMesh;
use engenho_substrate::{
    BroadcastLedger, DerivationCacheBackend, FakeVerifier, GossipBroadcaster, GossipLedger,
    MaterializationLedger, MemoryDerivationCache, MemoryLedger, NodeId, Verifier,
};

use crate::build_backend_roceiro::BuildBackendRoceiro;
use crate::drv_build::{BuildBackend, FakeBuildBackend};
use crate::plantio::{NodeResolver, PlantioController, StaticNodeResolver};
use crate::roceiro::{FakeRoceiro, Roceiro};
use crate::store_ledger::StoreBackedLedger;
use crate::store_resolver::StoreBackedNodeResolver;

/// Which materializer to wire.
pub enum RoceiroChoice {
    /// Deterministic fake — for tests + bootstrap.
    Fake,
    /// Production — composed from BuildBackend + Cache + Verifier.
    /// Operator supplies all three; the builder wires them through
    /// BuildBackendRoceiro.
    BuildBackend {
        /// Build backend (single or Tiered).
        build: Arc<dyn BuildBackend>,
        /// Cache (single or Tiered).
        cache: Arc<dyn DerivationCacheBackend>,
        /// Verifier (single or Chained).
        verifier: Arc<dyn Verifier>,
    },
    /// Operator-supplied custom Roceiro.
    Custom(Arc<dyn Roceiro>),
}

impl Default for RoceiroChoice {
    fn default() -> Self {
        Self::Fake
    }
}

/// Which ledger backend + wrappers to apply.
pub enum LedgerChoice {
    /// Memory-only — tests + single-node bootstrap.
    Memory,
    /// Store-backed — receipts commit via Raft for cross-process
    /// durability.
    StoreBacked,
    /// Operator-supplied custom Ledger.
    Custom(Arc<dyn MaterializationLedger>),
}

impl Default for LedgerChoice {
    fn default() -> Self {
        Self::Memory
    }
}

/// Wrappers applied to whichever ledger is chosen.
#[derive(Default, Clone)]
pub struct LedgerWrappers {
    /// Wrap with BroadcastLedger so subscribers receive typed events.
    pub broadcast: bool,
    /// Wrap with GossipLedger via the given GossipBroadcaster.
    /// Ignored when None.
    pub gossip: Option<Arc<dyn GossipBroadcaster>>,
}

/// Which NodeResolver to wire.
pub enum NodeResolverChoice {
    /// Static list — tests + bootstrap.
    Static(Vec<NodeId>),
    /// StoreBacked cluster-wide (no namespace).
    StoreBackedClusterWide,
    /// StoreBacked scoped to a namespace.
    StoreBackedNamespace(String),
    /// Operator-supplied custom resolver.
    Custom(Arc<dyn NodeResolver>),
}

impl Default for NodeResolverChoice {
    fn default() -> Self {
        Self::Static(Vec::new())
    }
}

/// Typed pipeline config — one value, three choices.
pub struct PipelineConfig {
    /// Store the pipeline reads PlantioCRs from.
    pub store: Arc<StoreMesh>,
    /// Which materializer.
    pub roceiro: RoceiroChoice,
    /// Which ledger.
    pub ledger: LedgerChoice,
    /// Ledger wrappers.
    pub ledger_wrappers: LedgerWrappers,
    /// Which node resolver.
    pub resolver: NodeResolverChoice,
    /// Namespace the controller watches (None = cluster-wide).
    pub namespace: Option<String>,
}

impl PipelineConfig {
    /// Minimal config: store + single-node fake stack (the
    /// bootstrap shape — useful for tests + first-boot).
    #[must_use]
    pub fn minimal(store: Arc<StoreMesh>) -> Self {
        Self {
            store,
            roceiro: RoceiroChoice::Fake,
            ledger: LedgerChoice::Memory,
            ledger_wrappers: LedgerWrappers::default(),
            resolver: NodeResolverChoice::Static(vec![NodeId::from_bytes(b"bootstrap")]),
            namespace: None,
        }
    }
}

/// The assembled pipeline. Operator owns + ticks the controller;
/// the other handles are exposed for telemetry / event subscription.
pub struct PlantioPipeline {
    /// The wired controller — call `controller.tick()` in a loop.
    pub controller: Arc<PlantioController>,
    /// The wired ledger (after wrappers applied).
    pub ledger: Arc<dyn MaterializationLedger>,
    /// The wired Roceiro.
    pub roceiro: Arc<dyn Roceiro>,
    /// The wired NodeResolver.
    pub resolver: Arc<dyn NodeResolver>,
    /// If the ledger was wrapped in BroadcastLedger, this is the
    /// underlying broadcaster (subscribe for events). None when
    /// `broadcast` wasn't set.
    pub broadcast_ledger: Option<Arc<BroadcastLedger>>,
}

impl PlantioPipeline {
    /// Build the pipeline from typed config.
    pub fn build(config: PipelineConfig) -> Self {
        // 1. Roceiro.
        let roceiro: Arc<dyn Roceiro> = match config.roceiro {
            RoceiroChoice::Fake => Arc::new(FakeRoceiro::new()),
            RoceiroChoice::BuildBackend {
                build,
                cache,
                verifier,
            } => Arc::new(BuildBackendRoceiro::default_named(build, cache, verifier)),
            RoceiroChoice::Custom(r) => r,
        };

        // 2. Base ledger.
        let base_ledger: Arc<dyn MaterializationLedger> = match config.ledger {
            LedgerChoice::Memory => Arc::new(MemoryLedger::new()),
            LedgerChoice::StoreBacked => Arc::new(StoreBackedLedger::new(config.store.clone())),
            LedgerChoice::Custom(l) => l,
        };

        // 3. Wrap with Broadcast?
        let (broadcast_handle, after_broadcast): (
            Option<Arc<BroadcastLedger>>,
            Arc<dyn MaterializationLedger>,
        ) = if config.ledger_wrappers.broadcast {
            let bcast = Arc::new(BroadcastLedger::new(base_ledger));
            (Some(bcast.clone()), bcast as Arc<dyn MaterializationLedger>)
        } else {
            (None, base_ledger)
        };

        // 4. Wrap with Gossip?
        let final_ledger: Arc<dyn MaterializationLedger> =
            if let Some(transport) = config.ledger_wrappers.gossip {
                Arc::new(GossipLedger::new(after_broadcast, transport))
            } else {
                after_broadcast
            };

        // 5. Resolver.
        let resolver: Arc<dyn NodeResolver> = match config.resolver {
            NodeResolverChoice::Static(nodes) => Arc::new(StaticNodeResolver::new(nodes)),
            NodeResolverChoice::StoreBackedClusterWide => {
                Arc::new(StoreBackedNodeResolver::cluster_wide(config.store.clone()))
            }
            NodeResolverChoice::StoreBackedNamespace(ns) => Arc::new(
                StoreBackedNodeResolver::with_namespace(config.store.clone(), ns),
            ),
            NodeResolverChoice::Custom(r) => r,
        };

        // 6. Controller.
        let controller = Arc::new(PlantioController::new(
            config.store,
            roceiro.clone(),
            final_ledger.clone(),
            resolver.clone(),
            config.namespace,
        ));

        Self {
            controller,
            ledger: final_ledger,
            roceiro,
            resolver,
            broadcast_ledger: broadcast_handle,
        }
    }
}

/// Helper: the typical bootstrap stack — FakeBuildBackend +
/// MemoryDerivationCache + FakeVerifier composed into a
/// BuildBackendRoceiro, plus Broadcast wrapper. Useful for
/// integration tests + first-boot single-node clusters.
#[must_use]
pub fn bootstrap_pipeline(store: Arc<StoreMesh>, nodes: Vec<NodeId>) -> PlantioPipeline {
    let build: Arc<dyn BuildBackend> = Arc::new(FakeBuildBackend::new());
    let cache: Arc<dyn DerivationCacheBackend> = Arc::new(MemoryDerivationCache::new());
    let verifier: Arc<dyn Verifier> = Arc::new(FakeVerifier::new());
    let config = PipelineConfig {
        store,
        roceiro: RoceiroChoice::BuildBackend {
            build,
            cache,
            verifier,
        },
        ledger: LedgerChoice::Memory,
        ledger_wrappers: LedgerWrappers {
            broadcast: true,
            gossip: None,
        },
        resolver: NodeResolverChoice::Static(nodes),
        namespace: None,
    };
    PlantioPipeline::build(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::Stage;

    fn make_store_stub() -> Arc<StoreMesh> {
        // We don't actually need a live store for these tests —
        // we test pipeline shape, not end-to-end reconcile. Real
        // integration tests use the live store fixture.
        // Here: skip via #[ignore] when StoreMesh::stub isn't
        // available. The pipeline-shape tests below use fields
        // that don't dispatch into the store.
        unimplemented!("integration tests use a live StoreMesh; see plantio_integration_test")
    }

    // Pure-shape tests that don't need a store.
    #[test]
    fn ledger_wrappers_default_off() {
        let w = LedgerWrappers::default();
        assert!(!w.broadcast);
        assert!(w.gossip.is_none());
    }

    #[test]
    fn roceiro_choice_defaults_fake() {
        match RoceiroChoice::default() {
            RoceiroChoice::Fake => {}
            _ => panic!("default should be Fake"),
        }
    }

    #[test]
    fn ledger_choice_defaults_memory() {
        match LedgerChoice::default() {
            LedgerChoice::Memory => {}
            _ => panic!("default should be Memory"),
        }
    }

    #[test]
    fn node_resolver_choice_defaults_empty_static() {
        match NodeResolverChoice::default() {
            NodeResolverChoice::Static(nodes) => assert!(nodes.is_empty()),
            _ => panic!("default should be empty Static"),
        }
    }

    // Stage marker test — keep the stage type in scope (without
    // it pulled-in, the file is technically valid but the unused
    // import doesn't show up as a useful sanity check).
    #[test]
    fn stage_imports_compile() {
        let _ = std::any::type_name::<Stage>();
    }

    // Avoid unused-import warning for the stubbed helper.
    #[test]
    #[ignore]
    fn _stub_keeps_import() {
        let _ = make_store_stub;
    }
}
