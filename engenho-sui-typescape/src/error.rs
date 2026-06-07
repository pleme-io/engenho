//! Typed errors at the bridge boundary.

use thiserror::Error;

/// Errors raised when converting between `TypescapeValue` and Rust
/// typed values via the [`Typescape`](crate::Typescape) trait.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TypescapeError {
    /// The value was the wrong variant for the requested conversion.
    /// Mirrors sui_eval::Value's "type error" surface.
    #[error("typescape variant mismatch: expected {expected}, got {got}")]
    VariantMismatch {
        /// Variant the consumer asked for (e.g. `"int"`, `"attrs"`).
        expected: &'static str,
        /// Variant the value actually was (e.g. `"string"`).
        got: &'static str,
    },

    /// An attribute lookup found nothing where the consumer expected
    /// a required field.
    #[error("typescape: missing required attribute `{0}`")]
    MissingAttr(String),

    /// A list/attrs cardinality assertion failed (e.g. expected 3
    /// elements, got 5).
    #[error("typescape: cardinality mismatch on `{location}`: expected {expected}, got {got}")]
    Cardinality {
        /// Field/path where the mismatch surfaced.
        location: String,
        /// Expected element count.
        expected: usize,
        /// Actual element count.
        got: usize,
    },

    /// The value passed the type-shape check but failed the typed
    /// domain's invariant check (e.g. a negative duration).
    #[error("typescape: invariant violated on `{location}`: {reason}")]
    Invariant {
        /// Field/path where the invariant tripped.
        location: String,
        /// Human-readable reason.
        reason: String,
    },
}

engenho_substrate::impl_error_kind! {
    TypescapeError {
        { VariantMismatch { .. } } => "variant_mismatch",
        (MissingAttr(_)) => "missing_attr",
        { Cardinality { .. } } => "cardinality",
        { Invariant { .. } } => "invariant",
    }
}
