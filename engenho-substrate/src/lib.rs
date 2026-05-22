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
//!
//! Both are no-async-runtime helpers; they only need `std::fs` +
//! `serde` + `blake3`.

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod atomic_write;
pub mod magic_blob;

pub use atomic_write::{AtomicWriteError, write_atomic};
pub use magic_blob::{MagicBlob, MagicBlobError};
