//! # engenho-substrate
//!
//! Shared substrate helpers — primitives that emerged at ≥3 sites
//! across the engenho + kasou + tend codebases. Pulling them out
//! here closes the third-site rule + lets future consumers reach
//! one canonical implementation.
//!
//! ## Modules
//!
//!   * [`atomic_write`] — fsync-anchored tmp+rename atomic write
//!     (kasou v0.2.0 machine-identifier, engenho-store v0.20.0
//!     catalog snapshot, tend daemon state — three sites)
//!   * [`magic_blob`] — versioned magic header + BLAKE3-hashed
//!     payload format for any disk artifact that needs corruption
//!     detection + forward-compatible version bumps
//!   * [`derivation`] — typed Nix-derivation primitive +
//!     `DerivationCacheBackend` pluggable trait. Sui-as-substrate:
//!     derivations become a location-independent typed value the
//!     engenho fabric can move + cache anywhere.

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod atomic_write;
pub mod broadcast_ledger;
pub mod chained_verifier;
pub mod command_runner;
pub mod compose_ir;
pub mod derivation;
pub mod drv_disk;
pub mod gossip_ledger;
pub mod ledger;
pub mod magic_blob;
pub mod oci_renderer;
pub mod promotion;
pub mod quorum;
pub mod receipt;
pub mod retrying_cache;
pub mod roca;
pub mod shape;
pub mod shape_renderers;
pub mod tiered_cache;
pub mod verifier;
pub mod verifier_impls;
pub mod watched_cache;

pub use atomic_write::{AtomicWriteError, write_atomic};
pub use derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, MemoryDerivationCache, NarBlob,
    NarHash, OutputPath, Realisation,
};
pub use drv_disk::DiskDerivationCache;
pub use magic_blob::{MagicBlob, MagicBlobError};
pub use broadcast_ledger::{BroadcastLedger, LedgerEvent};
pub use chained_verifier::ChainedVerifier;
pub use command_runner::{
    CommandError, CommandRequest, CommandResponse, CommandRunner, FakeCommandRunner,
};
pub use compose_ir::{
    ComposeError, ComposeHealthcheck, ComposeIr, ComposeService, ComposeStack,
};
pub use gossip_ledger::{
    FakeGossipTransport, GossipBroadcast, GossipBroadcaster, GossipChannel,
    GossipDelivery, GossipError, GossipLedger,
};
pub use ledger::{LedgerError, LedgerKey, MaterializationLedger, MemoryLedger};
pub use oci_renderer::{OciDestReader, OciDestRef, OciImageRenderer, OciSourceRef};
pub use promotion::{PromotionContext, PromotionGate, PromotionPolicy};
pub use quorum::{QuorumOutcome, QuorumTracker};
pub use receipt::{MaterializationReceipt, NodeId, ReceiptKind};
pub use retrying_cache::{BackoffConfig, RetryingCacheBackend};
pub use roca::{
    ConfirmacaoPolicy, JobTarget, MaterializationJob, Placement, Plantio, PlantioError,
    Stage, StageId,
};
pub use shape::{RenderedArtifact, ShapeError, ShapeRenderer, WorkloadShape};
pub use shape_renderers::{CompositeShapeRenderer, FakeShapeRenderer};
pub use tiered_cache::TieredCache;
pub use verifier::{FakeVerifier, Verificacao, VerificationReceipt, VerifierId, Verifier, VerifyError};
pub use verifier_impls::{
    BytesAccessor, HashEqualityVerifier, IndependentRebuild, IndependentVerifier,
    SignerCheck, SmokeBuilder, SmokeTestVerifier, TameshiVerifier,
};
pub use watched_cache::{CacheEvent, WatchedCache};
