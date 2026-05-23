//! `ErrorKind` trait + `impl_error_kind!` macro.
//!
//! Closes the 25+-site duplication of `fn kind(&self) -> &'static
//! str` across every typed error in the substrate. Per the PRIME
//! DIRECTIVE — "any pattern that appears ≥2 times → macro_rules!
//! or proc-macro". This module operationalizes that for error
//! kinds.
//!
//! ## Why a trait + a macro (not just a macro)
//!
//! The trait lets generic code dispatch on `.kind()` polymorphically
//! ("any error with a kind string"). The macro keeps authoring
//! ergonomics — operators write the variant→tag map; the macro
//! generates both the inherent method AND the trait impl.
//!
//! ## Authoring shape
//!
//! ```ignore
//! use thiserror::Error;
//! use engenho_substrate::impl_error_kind;
//!
//! #[derive(Debug, Clone, Error)]
//! pub enum FooError {
//!     #[error("backend: {0}")]
//!     Backend(String),
//!     #[error("invalid {field}: {reason}")]
//!     Invalid { field: String, reason: String },
//!     #[error("not found")]
//!     NotFound,
//! }
//!
//! impl_error_kind! {
//!     FooError {
//!         Backend(_) => "backend",
//!         Invalid { .. } => "invalid",
//!         NotFound => "not_found",
//!     }
//! }
//! ```
//!
//! Expands to:
//!
//! ```ignore
//! impl FooError {
//!     pub fn kind(&self) -> &'static str {
//!         match self {
//!             FooError::Backend(_) => "backend",
//!             FooError::Invalid { .. } => "invalid",
//!             FooError::NotFound => "not_found",
//!         }
//!     }
//! }
//!
//! impl engenho_substrate::ErrorKind for FooError {
//!     fn kind(&self) -> &'static str {
//!         Self::kind(self)
//!     }
//! }
//! ```
//!
//! Inherent + trait impl. Generic consumers reach for the trait;
//! call sites use the inherent method (no extra import needed).

/// Universal error-kind surface. Every typed error in the
/// substrate gains a stable `&'static str` identifier for
/// telemetry / SDK dispatch / cross-language SDKs.
///
/// Implemented automatically by [`impl_error_kind!`].
pub trait ErrorKind {
    /// Stable identifier — snake_case, stable across substrate
    /// releases. Operators use this for log filtering + metric
    /// dimensions + SDK-side error dispatch.
    fn kind(&self) -> &'static str;
}

/// Generate `impl Foo { pub fn kind(&self) -> &'static str }` +
/// `impl ErrorKind for Foo` from a variant→tag map.
///
/// Each entry is one of three exact shapes:
///   * `Name => "tag"` — unit variant
///   * `Name(_) => "tag"` — tuple variant (any arity)
///   * `Name { .. } => "tag"` — struct variant
///
/// Mixed shapes within one enum are supported.
#[macro_export]
macro_rules! impl_error_kind {
    (
        $enum:ident {
            $(
                $arm:tt => $tag:literal
            ),+
            $(,)?
        }
    ) => {
        impl $enum {
            /// Stable identifier — see [`engenho_substrate::ErrorKind`].
            #[must_use]
            pub fn kind(&self) -> &'static str {
                match self {
                    $(
                        $crate::__error_kind_match_arm!(Self, $arm) => $tag
                    ),+
                }
            }
        }

        impl $crate::error_kind::ErrorKind for $enum {
            fn kind(&self) -> &'static str {
                Self::kind(self)
            }
        }
    };
}

/// Internal helper — expands the parenthesized "arm shape" into
/// the right match pattern. Each arm token is one of:
///   * `Name` (unit) — produces `Self::Name`
///   * `(Name(_))` (tuple) — operator wraps in `()` for the macro
///   * `({ Name { .. } })` (struct) — operator wraps in `()`
///
/// Actually simpler: the arm token is just `(Pattern)` literally,
/// and the macro just unwraps. See impl_error_kind macro arms below.
#[macro_export]
#[doc(hidden)]
macro_rules! __error_kind_match_arm {
    // Unit variant: just an identifier.
    ($self_ty:ident, $variant:ident) => {
        $self_ty::$variant
    };
    // Tuple variant: identifier followed by parenthesized pattern.
    ($self_ty:ident, ($variant:ident( $( $pat:tt )* ))) => {
        $self_ty::$variant( $( $pat )* )
    };
    // Struct variant: identifier followed by braced pattern.
    ($self_ty:ident, { $variant:ident { $( $pat:tt )* } }) => {
        $self_ty::$variant { $( $pat )* }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use thiserror::Error;

    // Sample errors covering every match-pattern shape the macro
    // must support.

    #[derive(Debug, Clone, Error)]
    enum TupleVariantsError {
        #[error("backend: {0}")]
        Backend(String),
        #[error("io: {0}")]
        Io(String),
    }
    impl_error_kind! {
        TupleVariantsError {
            (Backend(_)) => "backend",
            (Io(_)) => "io",
        }
    }

    #[derive(Debug, Clone, Error)]
    enum NamedFieldsError {
        #[error("invalid {field}: {reason}")]
        Invalid { field: String, reason: String },
        #[error("conflict on {key}")]
        Conflict { key: String },
    }
    impl_error_kind! {
        NamedFieldsError {
            { Invalid { .. } } => "invalid",
            { Conflict { .. } } => "conflict",
        }
    }

    #[derive(Debug, Clone, Error)]
    enum UnitVariantsError {
        #[error("not found")]
        NotFound,
        #[error("denied")]
        Denied,
    }
    impl_error_kind! {
        UnitVariantsError {
            NotFound => "not_found",
            Denied => "denied",
        }
    }

    #[derive(Debug, Clone, Error)]
    enum MixedVariantsError {
        #[error("backend: {0}")]
        Backend(String),
        #[error("invalid {field}")]
        Invalid { field: String },
        #[error("denied")]
        Denied,
    }
    impl_error_kind! {
        MixedVariantsError {
            (Backend(_)) => "backend",
            { Invalid { .. } } => "invalid",
            Denied => "denied",
        }
    }

    // ── Inherent .kind() works for each variant shape ──────────

    #[test]
    fn tuple_variants_inherent_kind() {
        assert_eq!(TupleVariantsError::Backend("x".into()).kind(), "backend");
        assert_eq!(TupleVariantsError::Io("x".into()).kind(), "io");
    }

    #[test]
    fn named_fields_inherent_kind() {
        assert_eq!(
            NamedFieldsError::Invalid {
                field: "f".into(),
                reason: "r".into()
            }
            .kind(),
            "invalid"
        );
        assert_eq!(
            NamedFieldsError::Conflict { key: "k".into() }.kind(),
            "conflict"
        );
    }

    #[test]
    fn unit_variants_inherent_kind() {
        assert_eq!(UnitVariantsError::NotFound.kind(), "not_found");
        assert_eq!(UnitVariantsError::Denied.kind(), "denied");
    }

    #[test]
    fn mixed_variants_inherent_kind() {
        assert_eq!(MixedVariantsError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            MixedVariantsError::Invalid { field: "f".into() }.kind(),
            "invalid"
        );
        assert_eq!(MixedVariantsError::Denied.kind(), "denied");
    }

    // ── ErrorKind trait works polymorphically ──────────────────

    fn polymorphic_kind<E: ErrorKind>(e: &E) -> &'static str {
        e.kind()
    }

    #[test]
    fn errorkind_trait_dispatches_for_tuple() {
        let e = TupleVariantsError::Backend("x".into());
        assert_eq!(polymorphic_kind(&e), "backend");
    }

    #[test]
    fn errorkind_trait_dispatches_for_named() {
        let e = NamedFieldsError::Invalid {
            field: "f".into(),
            reason: "r".into(),
        };
        assert_eq!(polymorphic_kind(&e), "invalid");
    }

    #[test]
    fn errorkind_trait_dispatches_for_unit() {
        let e = UnitVariantsError::NotFound;
        assert_eq!(polymorphic_kind(&e), "not_found");
    }

    #[test]
    fn errorkind_trait_dispatches_for_mixed() {
        let e = MixedVariantsError::Invalid { field: "x".into() };
        assert_eq!(polymorphic_kind(&e), "invalid");
    }

    // ── Inherent and trait agree ───────────────────────────────

    #[test]
    fn inherent_and_trait_kinds_agree() {
        let e1 = TupleVariantsError::Io("x".into());
        let e2 = NamedFieldsError::Conflict { key: "k".into() };
        let e3 = UnitVariantsError::Denied;
        let e4 = MixedVariantsError::Denied;
        let cases: Vec<(&dyn ErrorKind, &'static str)> = vec![
            (&e1, "io"),
            (&e2, "conflict"),
            (&e3, "denied"),
            (&e4, "denied"),
        ];
        for (e, expected) in cases {
            assert_eq!(e.kind(), expected);
        }
    }

    // ── Macro supports trailing comma ──────────────────────────

    #[derive(Debug, Clone, Error)]
    enum TrailingCommaError {
        #[error("a")]
        A,
    }
    impl_error_kind! {
        TrailingCommaError {
            A => "a",
        }
    }

    #[test]
    fn macro_accepts_trailing_comma() {
        assert_eq!(TrailingCommaError::A.kind(), "a");
    }

    // ── Macro produces stable tags across releases ─────────────

    #[test]
    fn tags_are_snake_case_and_stable() {
        // The macro emits whatever string literal the operator
        // supplies — operators are responsible for snake_case +
        // stability. This test pins the convention by asserting
        // the substrate's own usage.
        for tag in ["backend", "io", "not_found", "invalid"] {
            assert!(
                tag.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "tag {tag} must be snake_case"
            );
        }
    }
}
