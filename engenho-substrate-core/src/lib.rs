//! # engenho-substrate-core
//!
//! The tokio-free core of engenho's substrate. Everything here is plain
//! data, pure functions and `std` I/O; no async runtime is a dependency of
//! this crate, so no module can reach one (the manifest is the seal, and
//! `tests/tokio_free.rs` pins it).
//!
//! The substrate is carved in three by what is reachable:
//!
//! | Crate | Holds | Who may depend on it |
//! |---|---|---|
//! | `engenho-substrate-core` (this crate) | the ten modules below | anyone |
//! | `engenho-substrate` (the leaf) | every module a shipped crate references; re-exports this crate whole | shipped crates |
//! | `engenho-substrate-incubator` | modules no shipped crate references | tests and drafts only |
//!
//! Every path under `engenho_substrate::` that named one of these modules
//! still resolves: the leaf re-exports this crate with a glob, macros
//! included.
//!
//! ## Modules
//!
//!   * [`closed_enum`] — `closed_enum!`, a fieldless enum and its `ALL`
//!     (and optionally its `name()`) generated from one variant list
//!   * [`error_kind`] — `ErrorKind` + `impl_error_kind!`, the stable tag
//!     every typed error reports
//!   * [`named`] — `Named` + `define_named!` / `impl_named_field!`
//!   * [`hex`] / [`hash_newtype`] — hex encoding and the
//!     `define_hash_newtype!` BLAKE3 newtype surface
//!   * [`fingerprint`] — `Fingerprint` + `impl_fingerprint!`
//!   * [`atomic_write`] — fsync-anchored tmp+rename atomic write
//!   * [`magic_blob`] — versioned magic header + BLAKE3-hashed payload
//!   * [`risca`] — `Risca<T>` redaction + `assert_risca_no_leak!`
//!   * [`relogio`] — `Clock`, `Instant` and the wall/frozen/HLC clocks

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod atomic_write;
pub mod closed_enum;
pub mod error_kind;
pub mod fingerprint;
pub mod hash_newtype;
pub mod hex;
pub mod magic_blob;
pub mod named;
pub mod relogio;
pub mod risca;

pub use atomic_write::{AtomicWriteError, TempPath, write_atomic, write_atomic_mode};
pub use error_kind::ErrorKind;
pub use fingerprint::{Fingerprint, fingerprint_blake3};
pub use hash_newtype::{
    HashNewtypeError, hex_full, hex_prefix, parse_hex_32_padded, parse_hex_32_strict,
};
pub use hex::{Hex, hex_encode};
pub use magic_blob::{MagicBlob, MagicBlobError};
pub use named::Named;
pub use relogio::{Clock, FrozenClock, HlcClock, Instant, LogicalClock, WallClock};
pub use risca::{REDACTED, Redact, Risca, redact_credit_card, redact_email, redact_token};
