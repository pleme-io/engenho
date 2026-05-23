//! # engenho-sui-typescape
//!
//! The **bridge** between sui's untyped Nix value tree and engenho's
//! typed substrate primitives.
//!
//! ## Why this crate exists
//!
//! sui (pleme-io's pure-Rust Nix replacement, bytecode VM, 3× CppNix)
//! evaluates Nix expressions into a `sui_eval::Value` tree with shape
//! `{ Null | Bool | Int | Float | String | Path | List | Attrs | Lambda
//! | Builtin | Thunk }`. The whole shape is single-threaded — every
//! `Rc<...>` inside makes `sui_eval::Value` `!Send`.
//!
//! engenho's typescape — the catalog of typed Rust primitives registered
//! via `#[derive(TataraDomain)]` — is `Send + Sync` because every
//! primitive flows across:
//!
//!   - engenho-revoada's Raft log (cross-thread, cross-node)
//!   - tameshi+sekiban admission attestation (background workers)
//!   - mirante's `ObservationChannel` (broadcast to subscribers)
//!   - the Viggy controller's reconcile loop (tokio multi-thread runtime)
//!
//! The two worlds cannot meet directly. This crate is the bridge.
//!
//! ## The TypescapeValue mirror
//!
//! [`TypescapeValue`] is a Send+Sync, thread-portable mirror of the
//! sui `Value` shape:
//!
//!   - Lambda / Builtin / Thunk variants ARE INTENTIONALLY ABSENT —
//!     thunks must be forced before crossing the bridge; lambdas + builtins
//!     are eval-time machinery that do not survive serialization
//!   - `Rc<...>` becomes `Arc<...>` everywhere
//!   - The bridge is one-way at the variant level (sui::Value can degrade
//!     to TypescapeValue; TypescapeValue can lift back to sui::Value
//!     LOSSLESSLY because no Lambda/Builtin/Thunk ever appears)
//!
//! ## The Typescape trait
//!
//! Every typed primitive that participates in the live-config loop
//! implements [`Typescape`]:
//!
//! ```rust
//! use engenho_sui_typescape::{Typescape, TypescapeValue, TypescapeError};
//!
//! struct MyDomain { n: u64 }
//!
//! impl Typescape for MyDomain {
//!     fn to_typescape_value(&self) -> TypescapeValue {
//!         TypescapeValue::int(self.n as i64)
//!     }
//!     fn from_typescape_value(v: &TypescapeValue) -> Result<Self, TypescapeError> {
//!         Ok(Self { n: v.as_int()? as u64 })
//!     }
//! }
//! ```
//!
//! Round-trip property: for every well-formed `T`,
//! `T::from_typescape_value(&T::to_typescape_value(&t))? == t`.
//!
//! ## Why a hand-defined `TypescapeValue` (not sui's `Value`)
//!
//! Building the bridge against sui's evolving public surface today
//! means rebuilding the bridge on every sui release. Instead the
//! bridge speaks `TypescapeValue` (this crate's stable surface);
//! `with-sui-eval` (future, gated behind a Cargo feature) adapts
//! `sui_eval::Value → TypescapeValue` once sui's API hardens. The
//! adapter is a single ~50-line file; nothing in engenho-fonte /
//! engenho-controllers / the TataraDomains depends on sui directly.
//!
//! ## What plugs into this
//!
//!   - **engenho-fonte** consumes [`Typescape`]: a `(defsistema …)`
//!     form parsed via tlisp → TypescapeValue → `Sistema` typed value
//!     → Raft propose → reconcile.
//!   - **arch-synthesizer typescape** registers [`Typescape`] impls for
//!     every TataraDomain alongside the existing Serialize/Deserialize.
//!   - **caixa / pangea / magma / viggy promessa** all gain [`Typescape`]
//!     so a `(defsistema)` can refer to them by name without losing
//!     type-safety at the bridge.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod ext;
mod value;

pub use error::TypescapeError;
pub use ext::Typescape;
pub use value::{TypescapeAttrs, TypescapeList, TypescapeString, TypescapeValue};
