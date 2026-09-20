//! # engenho-substrate
//!
//! The substrate's leaf: every module a shipped engenho crate references.
//! It re-exports [`engenho_substrate_core`] whole, so a path such as
//! `engenho_substrate::write_atomic` or `engenho_substrate::impl_error_kind!`
//! resolves exactly as it did before the carve.
//!
//! The substrate is carved in three by what is reachable:
//!
//! | Crate | Holds | Who may depend on it |
//! |---|---|---|
//! | `engenho-substrate-core` | error kinds, names, hex, hash newtypes, fingerprints, atomic write, magic blobs, redaction, clocks; no async runtime | anyone |
//! | `engenho-substrate` (this crate) | every module a shipped crate references, including the Drv, receipt and verifier types the dormant controllers use | shipped crates |
//! | `engenho-substrate-incubator` | `pesquisa`, `orcamento`, `compose_ir`, `oci_renderer`, `command_runner`, `fake_shell`, `disposable` | tests and drafts only |
//!
//! The incubator depends on this crate, so this crate can never depend on
//! the incubator: Cargo rejects the cycle. What a shipped crate compiles
//! is therefore fenced by the manifests, not by a convention.
//!
//! ## Modules
//!
//!   * [`derivation`] — typed Nix-derivation primitive +
//!     `DerivationCacheBackend` pluggable trait. Sui-as-substrate:
//!     derivations become a location-independent typed value the
//!     engenho fabric can move + cache anywhere.
//!   * [`rollout`] — one `Rollout{Shadow, Enforce}` gate type plus the
//!     would-reject ledger every tightened check reports through
//!   * [`freshness`] — the pure judges health is derived from:
//!     `Freshness{NeverObserved, Fresh, Stale}` for one observation and
//!     `Liveness{Unknown, Alive, Stalled, Dead}` for one child
//!   * [`owned_task`] — `OwnedTask`, a spawned task one value owns and stops
//!     by request-then-await, reporting a typed `TaskStop`
//!   * [`verifier`] — the `Verificacao` predicates and the `Verifier`
//!     contract; a verifier with no outcome for a predicate refuses it
//!     rather than issuing a receipt for a check that never ran
//!
//! ## The core's macros resolve through the leaf
//!
//! A glob re-export carries `#[macro_export]` macros, but it carries them in
//! the macro namespace, which no `pub use` list names — so nothing in the
//! item lists below would go red if it stopped. This doctest is the pin: a
//! consumer that reaches the core's `closed_enum!` through this crate keeps
//! compiling.
//!
//! ```
//! engenho_substrate::closed_enum! {
//!     #[named]
//!     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//!     pub enum Colour { Red, Green }
//! }
//! assert_eq!(Colour::ALL, &[Colour::Red, Colour::Green]);
//! assert_eq!(Colour::Green.name(), "Green");
//! ```
//!
//! ## What this crate does not hold
//!
//! None of the incubator's modules is reachable through this crate. Each
//! line below is its own doctest, so moving any one module back into the
//! leaf turns exactly that test red.
//!
//! ```compile_fail
//! use engenho_substrate::pesquisa;
//! ```
//! ```compile_fail
//! use engenho_substrate::orcamento;
//! ```
//! ```compile_fail
//! use engenho_substrate::compose_ir;
//! ```
//! ```compile_fail
//! use engenho_substrate::oci_renderer;
//! ```
//! ```compile_fail
//! use engenho_substrate::command_runner;
//! ```
//! ```compile_fail
//! use engenho_substrate::fake_shell;
//! ```
//! ```compile_fail
//! use engenho_substrate::disposable;
//! ```

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

// The tokio-free core, re-exported whole: its modules, root items and
// `#[macro_export]` macros all appear here under their old paths.
pub use engenho_substrate_core::*;

pub mod broadcast_ledger;
pub mod chained_verifier;
pub mod closure_types;
pub mod derivation;
pub mod drv_disk;
pub mod freshness;
pub mod gossip_ledger;
pub mod ledger;
pub mod linhagem_aberta;
pub mod maquina;
pub mod mirante;
pub mod owned_task;
pub mod promotion;
pub mod provacao;
pub mod quorum;
pub mod receipt;
pub mod replay;
pub mod retrying_cache;
pub mod roca;
pub mod rollout;
pub mod selo;
pub mod shape;
pub mod shape_renderers;
pub mod tiered_cache;
pub mod verifier;
pub mod verifier_impls;
pub mod watched_cache;

#[allow(deprecated)]
pub use broadcast_ledger::BroadcastLedgerSnapshot;
pub use broadcast_ledger::{BroadcastLedger, LedgerEvent};
pub use chained_verifier::ChainedVerifier;
pub use derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, MemoryDerivationCache, NarBlob, NarBlobError,
    NarHash, OutputPath, Realisation,
};
pub use drv_disk::DiskDerivationCache;
pub use freshness::{Freshness, Liveness, StaleAfter, TaskState, WindowTooShort};
pub use gossip_ledger::{
    FakeGossipTransport, GossipBroadcast, GossipBroadcaster, GossipChannel, GossipDelivery,
    GossipError, GossipLedger,
};
pub use ledger::{LedgerError, LedgerKey, MaterializationLedger, MemoryLedger};
pub use linhagem_aberta::{LineageError, LineageGraph, LineageNode, LineageProof};
pub use maquina::{MachineError, MachineRunner, MachineSnapshot, StateMachine, TransitionRecord};
pub use mirante::{
    AnyChannel, ChildCountSnapshot, Mirante, MiranteSnapshot, Observable, ObservationChannel,
    SubscriberSnapshot,
};
pub use owned_task::{OwnedTask, StopSignal, TaskStop};
pub use promotion::{PromotionContext, PromotionGate, PromotionPolicy};
pub use provacao::{Policy, Provacao};
pub use quorum::{QuorumState, QuorumTracker, QuorumVerdict, Tally};
pub use receipt::{MaterializationReceipt, NodeId, ReceiptKind};
pub use replay::{ReplayCursor, ReplayCursorSnapshot, replay_into, replay_until};
pub use retrying_cache::{BackoffConfig, RetryingCacheBackend};
pub use roca::{
    ConfirmacaoPolicy, JobTarget, MaterializationJob, Placement, Plantio, PlantioError, Stage,
    StageId,
};
pub use rollout::{
    Gate, Proceed, Refused, RejectReason, Rollout, WouldReject, WouldRejectCount, WouldRejectHook,
    WouldRejectLedger,
};
pub use selo::{Selo, SeloError, SeloIssuer};
pub use shape::{RenderedArtifact, ShapeError, ShapeRenderer, WorkloadShape};
pub use shape_renderers::{CompositeShapeRenderer, FakeShapeRenderer};
pub use tiered_cache::TieredCache;
pub use verifier::{
    FakeOutcome, FakeVerifier, Verificacao, VerificacaoKind, VerificationReceipt, Verifier,
    VerifierId, VerifyError,
};
pub use verifier_impls::{
    BytesAccessor, HashEqualityVerifier, IndependentRebuild, IndependentVerifier, SignerCheck,
    SmokeBuilder, SmokeTestVerifier, TameshiVerifier,
};
#[allow(deprecated)]
pub use watched_cache::WatchedCacheSnapshot;
pub use watched_cache::{CacheEvent, WatchedCache};
