//! `closed_enum!` — a fieldless enum, the list of its variants, and
//! optionally their names, all written once.
//!
//! A catalog enum is only useful if something can walk every variant: the
//! runtime spawns each child in its catalog, the scheduler runs each filter
//! plugin in its list, a test ticks each one, a decoder maps each stored code
//! back. A hand-written `ALL` array next to the enum is a second list, and the
//! second list is where a new variant goes missing — the enum compiles, every
//! exhaustive `match` gets its arm, and the variant is simply never walked.
//!
//! This macro generates the enum and `ALL` from the SAME token list, so `ALL`
//! cannot miss a variant or hold one twice. Every per-variant fact stays an
//! exhaustive `match` on the enum, which is a compile error (E0004) when a
//! variant is added without its row.
//!
//! `ALL` is in declaration order, and a fieldless enum's discriminants are
//! `0..N` in declaration order, so `ALL[v as usize] == v` for every variant.
//! A decoder that maps a stored code back to a variant relies on that.
//!
//! ## Why it lives here
//!
//! This shape was derived three separate times — `engenho-controllers`, the
//! scheduler's `filter_plugins!` (which added `name()`), and a test-local copy
//! in `engenho-apiserver` — and `engenho-config` could reach none of them, so
//! `KubeletBackendKind` has a hand-written list. Three independent arrivals at
//! one shape is the signal to extract, and the extraction belongs in the crate
//! every other crate may depend on: this one is tokio-free and has no engenho
//! edges, so no crate is barred from the macro by its dependency set.

/// Declare a fieldless enum together with `pub const ALL: &[Self]`, every
/// variant in declaration order, generated from the enum's own variant list.
///
/// ```
/// engenho_substrate_core::closed_enum! {
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum Colour { Red, Green }
/// }
/// assert_eq!(Colour::ALL, &[Colour::Red, Colour::Green]);
/// ```
///
/// Lead the invocation with `#[named]` to also generate
/// `pub const fn name(self) -> &'static str`, each variant's name taken from
/// its own spelling, so the enum, its list and its identifiers are one list:
///
/// ```
/// engenho_substrate_core::closed_enum! {
///     #[named]
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum Plugin { NodeReady, TaintToleration }
/// }
/// assert_eq!(Plugin::NodeReady.name(), "NodeReady");
/// ```
///
/// `#[named]` must come first. Written after a `derive`, it is consumed as an
/// ordinary attribute and rustc rejects it (`cannot find attribute 'named'`),
/// so a misplaced marker is a compile error rather than an enum that silently
/// has no `name`.
#[macro_export]
macro_rules! closed_enum {
    // Named: `ALL` and `name()`, both from the one variant list.
    (
        #[named]
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident ),+ $(,)?
        }
    ) => {
        $crate::closed_enum! {
            $(#[$meta])*
            $vis enum $name {
                $( $(#[$vmeta])* $variant ),+
            }
        }

        impl $name {
            /// This variant's stable identifier: its own spelling, for logs,
            /// metric dimensions and census rows.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $( Self::$variant => stringify!($variant), )+
                }
            }
        }
    };

    // Plain: `ALL` only.
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            /// Every variant, in declaration order. Generated from the enum's
            /// own variant list, so it is complete by construction.
            pub const ALL: &'static [Self] = &[ $( Self::$variant ),+ ];
        }
    };
}

#[cfg(test)]
mod tests {
    crate::closed_enum! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Probe { A, B, C }
    }

    crate::closed_enum! {
        #[named]
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Plugin {
            /// A per-variant doc comment rides through the named arm too.
            NodeReady,
            TaintToleration,
            Resources,
        }
    }

    #[test]
    fn all_is_every_variant_in_declaration_order() {
        assert_eq!(Probe::ALL, &[Probe::A, Probe::B, Probe::C]);
        for (i, v) in Probe::ALL.iter().enumerate() {
            assert_eq!(
                *v as usize, i,
                "ALL[{i}] is not the variant with discriminant {i}"
            );
        }
    }

    #[test]
    fn the_named_arm_still_generates_all_in_declaration_order() {
        assert_eq!(
            Plugin::ALL,
            &[
                Plugin::NodeReady,
                Plugin::TaintToleration,
                Plugin::Resources
            ]
        );
        for (i, v) in Plugin::ALL.iter().enumerate() {
            assert_eq!(
                *v as usize, i,
                "ALL[{i}] is not the variant with discriminant {i}"
            );
        }
    }

    #[test]
    fn name_is_the_variants_own_spelling() {
        assert_eq!(Plugin::NodeReady.name(), "NodeReady");
        assert_eq!(Plugin::TaintToleration.name(), "TaintToleration");
        assert_eq!(Plugin::Resources.name(), "Resources");
    }

    /// The names come from the same list as the variants, so every variant
    /// has one and no two share one. A hand-written `match` could return the
    /// neighbour's string for a new variant and still compile; this asserts
    /// the property, not the arm.
    #[test]
    fn every_variant_has_a_distinct_name() {
        let mut names: Vec<&'static str> = Plugin::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(names.len(), Plugin::ALL.len());
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "two variants report the same name");
    }
}
