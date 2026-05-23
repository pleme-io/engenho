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
pub mod derivation;
pub mod drv_disk;
pub mod magic_blob;
pub mod promotion;
pub mod tiered_cache;
pub mod watched_cache;

pub use atomic_write::{AtomicWriteError, write_atomic};
pub use derivation::{
    CacheError, DerivationCacheBackend, Drv, DrvHash, MemoryDerivationCache, NarBlob,
    NarHash, OutputPath, Realisation,
};
pub use drv_disk::DiskDerivationCache;
pub use magic_blob::{MagicBlob, MagicBlobError};
pub use promotion::{PromotionContext, PromotionGate, PromotionPolicy};
pub use tiered_cache::TieredCache;
pub use watched_cache::{CacheEvent, WatchedCache};
