//! PlantioPipeline — builder that assembles the full production
//! roça stack from a typed config.
//!
//! The composition story today requires 8+ Arc::new calls to wire
//! the full pipeline. This builder collapses that to one call:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use engenho_controllers::{Controller, ControllerError, PipelineConfig, PlantioPipeline};
//! # async fn example(store: Arc<engenho_store::StoreMesh>) -> Result<(), ControllerError> {
//! let pipeline = PlantioPipeline::build(PipelineConfig::minimal(store));
//! pipeline.controller.tick().await?;
//! # Ok(())
//! # }
//! ```
//!
//! (The example is compiled: `build` is synchronous and cannot fail.)
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
//! Single typed value the operator constructs. The materializer is
//! always named: `RoceiroChoice` has no default, so a pipeline that
//! would confirm stages through `FakeRoceiro` says so at the call site
//! (`PipelineConfig::minimal`). There is no bootstrap helper that wires
//! a verifier nobody configured (T5.6).

use std::sync::Arc;

use engenho_store::StoreMesh;
use engenho_substrate::{
    BroadcastLedger, DerivationCacheBackend, GossipBroadcaster, GossipLedger,
    MaterializationLedger, MemoryLedger, NodeId, Verifier,
};

use crate::build_backend_roceiro::BuildBackendRoceiro;
use crate::drv_build::BuildBackend;
use crate::plantio::{NodeResolver, PlantioController, StaticNodeResolver};
use crate::roceiro::{FakeRoceiro, Roceiro};
use crate::store_ledger::StoreBackedLedger;
use crate::store_resolver::StoreBackedNodeResolver;

/// Which materializer to wire. No `Default`: a fake materializer is
/// chosen by name or not at all.
///
/// ```compile_fail
/// // T5.6: there is no default materializer to fall back on.
/// let _ = engenho_controllers::RoceiroChoice::default();
/// ```
///
/// ```compile_fail
/// // T5.6: nor a bootstrap helper wiring a verifier nobody configured.
/// use engenho_controllers::bootstrap_pipeline;
/// ```
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
        // Every field, and every wrapper flag, is bound by name with no
        // `..`: a field added to either struct does not compile here
        // (E0027) until the builder wires it.
        let PipelineConfig {
            store,
            roceiro,
            ledger,
            ledger_wrappers: LedgerWrappers { broadcast, gossip },
            resolver,
            namespace,
        } = config;

        // 1. Roceiro.
        let roceiro: Arc<dyn Roceiro> = match roceiro {
            RoceiroChoice::Fake => Arc::new(FakeRoceiro::new()),
            RoceiroChoice::BuildBackend {
                build,
                cache,
                verifier,
            } => Arc::new(BuildBackendRoceiro::default_named(build, cache, verifier)),
            RoceiroChoice::Custom(r) => r,
        };

        // 2. Base ledger.
        let base_ledger: Arc<dyn MaterializationLedger> = match ledger {
            LedgerChoice::Memory => Arc::new(MemoryLedger::new()),
            LedgerChoice::StoreBacked => Arc::new(StoreBackedLedger::new(store.clone())),
            LedgerChoice::Custom(l) => l,
        };

        // 3. Wrap with Broadcast?
        let (broadcast_handle, after_broadcast): (
            Option<Arc<BroadcastLedger>>,
            Arc<dyn MaterializationLedger>,
        ) = if broadcast {
            let bcast = Arc::new(BroadcastLedger::new(base_ledger));
            (Some(bcast.clone()), bcast as Arc<dyn MaterializationLedger>)
        } else {
            (None, base_ledger)
        };

        // 4. Wrap with Gossip?
        let final_ledger: Arc<dyn MaterializationLedger> = if let Some(transport) = gossip {
            Arc::new(GossipLedger::new(after_broadcast, transport))
        } else {
            after_broadcast
        };

        // 5. Resolver.
        let resolver: Arc<dyn NodeResolver> = match resolver {
            NodeResolverChoice::Static(nodes) => Arc::new(StaticNodeResolver::new(nodes)),
            NodeResolverChoice::StoreBackedClusterWide => {
                Arc::new(StoreBackedNodeResolver::cluster_wide(store.clone()))
            }
            NodeResolverChoice::StoreBackedNamespace(ns) => {
                Arc::new(StoreBackedNodeResolver::with_namespace(store.clone(), ns))
            }
            NodeResolverChoice::Custom(r) => r,
        };

        // 6. Controller.
        let controller = Arc::new(PlantioController::new(
            store,
            roceiro.clone(),
            final_ledger.clone(),
            resolver.clone(),
            namespace,
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

#[cfg(test)]
mod tests {
    use super::*;

    // Pure-shape tests that don't need a store.
    #[test]
    fn ledger_wrappers_default_off() {
        let w = LedgerWrappers::default();
        assert!(!w.broadcast);
        assert!(w.gossip.is_none());
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
}
