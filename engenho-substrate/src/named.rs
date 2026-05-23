//! `Named` trait + `define_named!` macro.
//!
//! Closes the 25+-site duplication of `fn name(&self) -> &'static
//! str` across every backend trait in the substrate. Sibling to
//! [`crate::error_kind::ErrorKind`] — both standardize a tiny
//! universal surface that every typed primitive in the substrate
//! exposes for telemetry / dispatch / SDK consumption.
//!
//! ## Why a trait
//!
//! Currently every backend trait declares its own `fn name(&self)`
//! method. That works locally, but generic code that wants to
//! handle "any backend with a name" has to constrain on each
//! trait individually. `Named` is the universal surface.
//!
//! ## Two adoption modes
//!
//!   1. **Stand-alone `impl Named`** — for plain types (`pub struct
//!      Foo`). Use [`define_named!`] to generate.
//!   2. **Supertrait** — new backend traits can add `Named` as a
//!      supertrait (`trait MyBackend: Named { ... }`). Existing
//!      backend traits' `fn name(&self)` methods are kept for
//!      compatibility; consumers can also call `Named::name(&x)`
//!      to dispatch generically.
//!
//! ## Authoring shape
//!
//! Stand-alone:
//!
//! ```ignore
//! use engenho_substrate::define_named;
//!
//! pub struct Foo;
//! define_named!(Foo, "foo");
//! ```
//!
//! Expands to:
//!
//! ```ignore
//! impl engenho_substrate::Named for Foo {
//!     fn name(&self) -> &'static str { "foo" }
//! }
//! ```
//!
//! For generic types, pass the type params:
//!
//! ```ignore
//! define_named!(Foo<T>, "foo");                  // single param
//! define_named!(Foo<T, U: Send>, "foo");         // bounded params
//! ```

/// Universal "this thing has a stable telemetry name" surface.
///
/// Implemented automatically by [`define_named!`].
pub trait Named {
    /// Stable identifier — snake_case (or kebab-case for things
    /// the operator may type), stable across substrate releases.
    /// Used for log filtering + metric dimensions + SDK dispatch.
    fn name(&self) -> &'static str;
}

/// Generate `impl Named for $type` returning the given literal.
///
/// See module docs for the authoring shape.
#[macro_export]
macro_rules! define_named {
    ($type:ident, $name:literal) => {
        impl $crate::named::Named for $type {
            fn name(&self) -> &'static str {
                $name
            }
        }
    };
    ($type:ident<$($param:ident $(: $bound:tt)?),+>, $name:literal) => {
        impl<$($param $(: $bound)?),+> $crate::named::Named for $type<$($param),+> {
            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}

/// Generate `impl Named for $type` that reads the type's own
/// `self.name: &'static str` field. Third-site extraction (v0.93 TSR):
/// `Budget`, `CompositeShapeRenderer`, `ChainedVerifier` all had
/// hand-rolled `impl Named { fn name(&self) -> self.name }` blocks.
///
/// For generic types with a single type parameter + bound (or sum
/// of bounds), use the [`impl_named_field_generic!`] variant
/// instead (v0.94 — completes the Named macro family).
///
/// ## Usage
///
/// ```ignore
/// use engenho_substrate::impl_named_field;
///
/// pub struct Foo {
///     name: &'static str,
///     // other fields ...
/// }
///
/// impl_named_field!(Foo);
/// ```
///
/// Expands to:
///
/// ```ignore
/// impl engenho_substrate::Named for Foo {
///     fn name(&self) -> &'static str { self.name }
/// }
/// ```
#[macro_export]
macro_rules! impl_named_field {
    ($type:ident) => {
        impl $crate::named::Named for $type {
            fn name(&self) -> &'static str {
                self.name
            }
        }
    };
}

/// Generic single-parameter variant of [`impl_named_field!`] —
/// generates `impl<P: $bounds> Named for $type<P>` reading `self.name`.
/// Third-site extraction (v0.94 TSR): `MachineRunner<M: StateMachine>`,
/// `Provacao<F: ErrorKind + Clone>`, `ReplayCursor<E: Clone>` all
/// shared this exact shape.
///
/// ## Usage
///
/// ```ignore
/// use engenho_substrate::impl_named_field_generic;
///
/// pub struct Foo<T: SomeTrait + Clone> {
///     name: &'static str,
///     // ...
/// }
///
/// impl_named_field_generic!(Foo, T: SomeTrait + Clone);
/// ```
///
/// Expands to:
///
/// ```ignore
/// impl<T: SomeTrait + Clone> engenho_substrate::Named for Foo<T> {
///     fn name(&self) -> &'static str { self.name }
/// }
/// ```
#[macro_export]
macro_rules! impl_named_field_generic {
    ($type:ident, $param:ident: $($bound:tt)+) => {
        impl<$param: $($bound)+> $crate::named::Named for $type<$param> {
            fn name(&self) -> &'static str {
                self.name
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    pub struct PlainType;
    define_named!(PlainType, "plain");

    pub struct GenericOne<T>(pub T);
    define_named!(GenericOne<T>, "generic-one");

    pub struct GenericTwo<T, U>(pub T, pub U);
    define_named!(GenericTwo<T, U>, "generic-two");

    pub struct BoundedGeneric<T: Send>(pub T);
    define_named!(BoundedGeneric<T: Send>, "bounded-generic");

    #[test]
    fn plain_type_named() {
        let p = PlainType;
        assert_eq!(p.name(), "plain");
    }

    #[test]
    fn generic_one_named() {
        let g: GenericOne<u32> = GenericOne(42);
        assert_eq!(g.name(), "generic-one");
    }

    #[test]
    fn generic_two_named() {
        let g: GenericTwo<u32, String> = GenericTwo(1, "x".into());
        assert_eq!(g.name(), "generic-two");
    }

    #[test]
    fn bounded_generic_named() {
        let g: BoundedGeneric<u64> = BoundedGeneric(5);
        assert_eq!(g.name(), "bounded-generic");
    }

    // Polymorphic dispatch test — generic over any Named.
    fn telemetry<N: Named>(n: &N) -> &'static str {
        n.name()
    }

    #[test]
    fn polymorphic_dispatch_via_trait() {
        let p = PlainType;
        let g: GenericOne<bool> = GenericOne(true);
        assert_eq!(telemetry(&p), "plain");
        assert_eq!(telemetry(&g), "generic-one");
    }

    #[test]
    fn dyn_dispatch_via_trait_object() {
        let p = PlainType;
        let g: GenericTwo<u8, u8> = GenericTwo(0, 0);
        let names: Vec<&dyn Named> = vec![&p, &g];
        let collected: Vec<&'static str> = names.iter().map(|n| n.name()).collect();
        assert_eq!(collected, vec!["plain", "generic-two"]);
    }

    // Sanity: names are kebab-case OR snake_case (operator-supplied;
    // no enforcement, but the substrate's own usage follows one
    // of these conventions).
    #[test]
    fn substrate_names_use_kebab_or_snake_case() {
        for name in ["plain", "generic-one", "generic-two", "bounded-generic"] {
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_'),
                "name {name} must be kebab- or snake-case"
            );
        }
    }
}
