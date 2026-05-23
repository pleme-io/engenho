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
pub mod closure_types;
pub mod command_runner;
pub mod compose_ir;
pub mod derivation;
pub mod disposable;
pub mod drv_disk;
pub mod error_kind;
pub mod fake_shell;
pub mod fingerprint;
pub mod gossip_ledger;
pub mod hex;
pub mod ledger;
pub mod linhagem_aberta;
pub mod magic_blob;
pub mod maquina;
pub mod mirante;
pub mod named;
pub mod oci_renderer;
pub mod orcamento;
pub mod pesquisa;
pub mod promotion;
pub mod provacao;
pub mod quorum;
pub mod receipt;
pub mod relogio;
pub mod replay;
pub mod retrying_cache;
pub mod risca;
pub mod roca;
pub mod selo;
pub mod shape;
pub mod shape_renderers;
pub mod tiered_cache;
pub mod verifier;
pub mod verifier_impls;
pub mod watched_cache;

pub use atomic_write::{AtomicWriteError, tmp_path_for, write_atomic};
pub use broadcast_ledger::{BroadcastLedger, LedgerEvent};
pub use chained_verifier::ChainedVerifier;
pub use command_runner::{
    CommandError, CommandRequest, CommandResponse, CommandRunner, FakeCommandRunner,
};
pub use compose_ir::{ComposeError, ComposeHealthcheck, ComposeIr, ComposeService, ComposeStack};
pub use derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, MemoryDerivationCache, NarBlob, NarHash,
    OutputPath, Realisation,
};
pub use disposable::{Disposable, DisposableError, Transient};
pub use drv_disk::DiskDerivationCache;
pub use error_kind::ErrorKind;
pub use fingerprint::{Fingerprint, fingerprint_blake3};
pub use gossip_ledger::{
    FakeGossipTransport, GossipBroadcast, GossipBroadcaster, GossipChannel, GossipDelivery,
    GossipError, GossipLedger,
};
pub use hex::{Hex, hex_encode};
pub use ledger::{LedgerError, LedgerKey, MaterializationLedger, MemoryLedger};
pub use linhagem_aberta::{LineageError, LineageGraph, LineageNode, LineageProof};
pub use magic_blob::{MagicBlob, MagicBlobError};
pub use maquina::{MachineError, MachineRunner, MachineSnapshot, StateMachine, TransitionRecord};
pub use mirante::{AnyChannel, Mirante, Observable, ObservationChannel};
pub use named::Named;
pub use oci_renderer::{OciDestReader, OciDestRef, OciImageRenderer, OciSourceRef};
pub use orcamento::{Budget, BudgetError, BudgetSnapshot};
pub use pesquisa::{
    Aptidao, Arquivo, Ensaio, EnsaioId, Evidence, FakeFitness, FakeSearchSpace, Fitness,
    FitnessError, Geracao, GeracaoId, Linhagem, NicheKey, PesquisaError, SearchEngine, SearchId,
    SearchRng, SearchSpace, Selector, TournamentSelector, aptidao_to_receipt, score_ensaio,
};
pub use promotion::{PromotionContext, PromotionGate, PromotionPolicy};
pub use provacao::{Policy, Provacao};
pub use quorum::{QuorumOutcome, QuorumTracker};
pub use receipt::{MaterializationReceipt, NodeId, ReceiptKind};
pub use relogio::{Clock, FrozenClock, HlcClock, Instant, LogicalClock, WallClock};
pub use replay::{ReplayCursor, replay_into, replay_until};
pub use retrying_cache::{BackoffConfig, RetryingCacheBackend};
pub use risca::{REDACTED, Redact, Risca, redact_credit_card, redact_email, redact_token};
pub use roca::{
    ConfirmacaoPolicy, JobTarget, MaterializationJob, Placement, Plantio, PlantioError, Stage,
    StageId,
};
pub use selo::{Selo, SeloError, SeloIssuer};
pub use shape::{RenderedArtifact, ShapeError, ShapeRenderer, WorkloadShape};
pub use shape_renderers::{CompositeShapeRenderer, FakeShapeRenderer};
pub use tiered_cache::TieredCache;
pub use verifier::{
    FakeVerifier, Verificacao, VerificationReceipt, Verifier, VerifierId, VerifyError,
};
pub use verifier_impls::{
    BytesAccessor, HashEqualityVerifier, IndependentRebuild, IndependentVerifier, SignerCheck,
    SmokeBuilder, SmokeTestVerifier, TameshiVerifier,
};
pub use watched_cache::{CacheEvent, WatchedCache};
