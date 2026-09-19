//! `closed_enum!` — a fieldless enum and the list of its variants, written
//! once.
//!
//! A catalog enum is only useful if something can walk every variant: the
//! runtime spawns each child in its catalog, a test ticks each one, a decoder
//! maps each stored code back. A hand-written `ALL` array next to the enum is
//! a second list, and the second list is where a new variant goes missing —
//! the enum compiles, every exhaustive `match` gets its arm, and the variant
//! is simply never walked.
//!
//! This macro generates the enum and `ALL` from the SAME token list, so `ALL`
//! cannot miss a variant or hold one twice. Every per-variant fact stays an
//! exhaustive `match` on the enum, which is a compile error (E0004) when a
//! variant is added without its row.
//!
//! `ALL` is in declaration order, and a fieldless enum's discriminants are
//! `0..N` in declaration order, so `ALL[v as usize] == v` for every variant.
//! [`crate::heartbeat::TickClass`] relies on that to decode a stored code.

/// Declare a fieldless enum together with `pub const ALL: &[Self]`, every
/// variant in declaration order, generated from the enum's own variant list.
///
/// ```
/// engenho_controllers::closed_enum! {
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum Colour { Red, Green }
/// }
/// assert_eq!(Colour::ALL, &[Colour::Red, Colour::Green]);
/// ```
#[macro_export]
macro_rules! closed_enum {
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
}
