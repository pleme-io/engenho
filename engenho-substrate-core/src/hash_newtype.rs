//! `define_hash_newtype!` — the canonical `[u8; 32]` hash-newtype macro.
//!
//! Closes the 7-site duplication of the hand-written
//! "`pub struct Name(pub [u8; 32])` + `new` + `from_bytes` + `to_hex`
//! + `from_hex` + `impl Display`" quartet that grew independently
//! across the substrate + revoada:
//!
//!   * `DrvHash`  / `NarHash`   — `derivation.rs`
//!   * `EnsaioId` / `GeracaoId` — `pesquisa.rs`
//!   * `NodeId`                 — `receipt.rs`
//!   * `ContentHash`            — `engenho-revoada` `content/mod.rs`
//!   * `NodeId`                 — `engenho-revoada` `lib.rs`
//!
//! Sibling to [`crate::impl_fingerprint!`], [`crate::define_named!`],
//! and [`crate::impl_error_kind!`]. Per the PRIME DIRECTIVE — "any
//! pattern that appears ≥2 times → `macro_rules!`" — this collapses
//! the seven hand-written quartets into one typed emitter.
//!
//! ## Hex reuse
//!
//! The generated `to_hex` / `from_hex` / `Display` go through the
//! canonical [`crate::hex::hex_encode`] helper + [`crate::hex::Hex`]
//! trait — hex is implemented once, never re-rolled per newtype.
//!
//! ## Authoring shape
//!
//! Base form (standard derives, full-hex `Display`):
//!
//! ```ignore
//! use engenho_substrate::define_hash_newtype;
//!
//! define_hash_newtype! {
//!     /// BLAKE3 hash of a derivation's canonical encoding.
//!     DrvHash
//! }
//! ```
//!
//! Expands to `pub struct DrvHash(pub [u8; 32])` with
//! `new([u8; 32]) -> Self`, `from_bytes(&[u8]) -> Self`
//! (`Self(*blake3::hash(bytes).as_bytes())`), `to_hex(&self) -> String`,
//! `from_hex(&str) -> Result<Self, HashNewtypeError>`, and a full-hex
//! `Display` impl.
//!
//! Variants — supply optional leading attributes (extra derives,
//! `#[serde(transparent)]`, …) and a body block to extend or
//! re-shape the surface:
//!
//! ```ignore
//! // Copy newtype (id-style):
//! define_hash_newtype! {
//!     #[derive(Copy)]
//!     /// Trial identifier.
//!     EnsaioId
//! }
//!
//! // Transparent-serde + Default + Copy + short-prefix Display:
//! define_hash_newtype! {
//!     #[derive(Copy, Default)]
//!     #[serde(transparent)]
//!     /// Stable node identifier — ed25519 public key bytes.
//!     NodeId { display = prefix(6), from_hex = padded }
//! }
//! ```
//!
//! The `{ … }` body accepts two independent knobs:
//!   * `display = full` (default) — 64-char lowercase-hex `Display`.
//!   * `display = prefix($n:literal)` — first `$n` bytes (`2·$n`
//!     hex chars) `Display`, for read-friendly log lines.
//!   * `from_hex = strict` (default) — exact-length parse; rejects
//!     odd-length / non-hex / >64-char input.
//!   * `from_hex = padded` — left-zero-pads `1..=64` chars before
//!     parsing, so shorthand like `"ab"` round-trips.

use thiserror::Error;

use crate::hex::{Hex, hex_encode};

/// Error returned by the generated `from_hex` constructors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HashNewtypeError {
    /// Input length was not the 64 hex chars a `[u8; 32]` needs
    /// (or, for the `padded` variant, was empty or exceeded 64).
    #[error("invalid hex length: expected {expected}, got {got}")]
    Length {
        /// The length the parser required.
        expected: usize,
        /// The length the input actually had.
        got: usize,
    },
    /// Input contained a non-hex character.
    #[error("invalid hex digit at byte {index}")]
    Digit {
        /// Byte offset of the offending nibble pair.
        index: usize,
    },
}

crate::impl_error_kind! {
    HashNewtypeError {
        { Length { .. } } => "length",
        { Digit { .. } } => "digit",
    }
}

/// Parse exactly 64 lowercase/uppercase hex chars into `[u8; 32]`.
/// Used by the `from_hex = strict` arm.
///
/// # Errors
/// Returns [`HashNewtypeError::Length`] if `s.len() != 64`, or
/// [`HashNewtypeError::Digit`] on the first non-hex nibble pair.
pub fn parse_hex_32_strict(s: &str) -> Result<[u8; 32], HashNewtypeError> {
    if s.len() != 64 {
        return Err(HashNewtypeError::Length {
            expected: 64,
            got: s.len(),
        });
    }
    decode_pairs(s)
}

/// Parse `1..=64` hex chars into `[u8; 32]`, left-zero-padding to
/// 64 chars first. Used by the `from_hex = padded` arm so
/// test-friendly shorthand like `"ab"` round-trips.
///
/// # Errors
/// Returns [`HashNewtypeError::Length`] if `s` is empty or longer
/// than 64 chars, or [`HashNewtypeError::Digit`] on a non-hex pair.
pub fn parse_hex_32_padded(s: &str) -> Result<[u8; 32], HashNewtypeError> {
    if s.is_empty() || s.len() > 64 {
        return Err(HashNewtypeError::Length {
            expected: 64,
            got: s.len(),
        });
    }
    let mut padded = String::with_capacity(64);
    for _ in 0..(64 - s.len()) {
        padded.push('0');
    }
    padded.push_str(s);
    decode_pairs(&padded)
}

/// Decode an exactly-64-char hex string into `[u8; 32]`.
fn decode_pairs(s: &str) -> Result<[u8; 32], HashNewtypeError> {
    debug_assert_eq!(s.len(), 64);
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let pair = &s[i * 2..i * 2 + 2];
        *byte = u8::from_str_radix(pair, 16).map_err(|_| HashNewtypeError::Digit { index: i })?;
    }
    Ok(bytes)
}

/// Render the first `n` bytes of a 32-byte array as lowercase hex —
/// the read-friendly `Display` prefix. Reuses the canonical
/// [`hex_encode`] helper over a byte sub-slice.
#[must_use]
pub fn hex_prefix(bytes: &[u8; 32], n: usize) -> String {
    hex_encode(&bytes[..n.min(32)])
}

/// Full 64-char lowercase-hex of a 32-byte array, via [`Hex`].
#[must_use]
pub fn hex_full(bytes: &[u8; 32]) -> String {
    bytes.to_hex()
}

/// Emit a `[u8; 32]` hash newtype with the canonical surface.
///
/// See the module docs for the full authoring shape + knobs.
#[macro_export]
macro_rules! define_hash_newtype {
    // ── Base: standard derives, full-hex Display, strict from_hex ──
    (
        $(#[$attr:meta])*
        $name:ident
    ) => {
        $crate::define_hash_newtype! {
            $(#[$attr])*
            $name { display = full, from_hex = strict }
        }
    };

    // ── Full form: explicit body block (any subset of knobs) ──────
    (
        $(#[$attr:meta])*
        $name:ident { $($body:tt)* }
    ) => {
        #[derive(
            ::core::clone::Clone,
            ::core::fmt::Debug,
            ::core::cmp::PartialEq,
            ::core::cmp::Eq,
            ::core::hash::Hash,
            ::core::cmp::PartialOrd,
            ::core::cmp::Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        $(#[$attr])*
        pub struct $name(pub [u8; 32]);

        impl $name {
            /// New from raw 32 bytes.
            #[must_use]
            pub fn new(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Compute from arbitrary bytes via BLAKE3.
            #[must_use]
            pub fn from_bytes(bytes: &[u8]) -> Self {
                Self(*::blake3::hash(bytes).as_bytes())
            }

            /// Lowercase hex representation (64 chars).
            #[must_use]
            pub fn to_hex(&self) -> String {
                $crate::hex::hex_encode(&self.0)
            }

            $crate::__hash_newtype_from_hex!($($body)*);
        }

        $crate::__hash_newtype_display!($name, $($body)*);
    };
}

/// Internal — emit the `from_hex` constructor for the selected knob.
#[macro_export]
#[doc(hidden)]
macro_rules! __hash_newtype_from_hex {
    // padded selected (in any knob order)
    (from_hex = padded $($rest:tt)*) => {
        $crate::__hash_newtype_from_hex_padded!();
    };
    ($_other:ident = $_val:tt , from_hex = padded $($rest:tt)*) => {
        $crate::__hash_newtype_from_hex_padded!();
    };
    ($_other:ident = $_val:tt ( $($_v:tt)* ) , from_hex = padded $($rest:tt)*) => {
        $crate::__hash_newtype_from_hex_padded!();
    };
    // strict (default) — any leading knobs, no padded marker
    ($($_rest:tt)*) => {
        $crate::__hash_newtype_from_hex_strict!();
    };
}

/// Internal — strict `from_hex` body.
#[macro_export]
#[doc(hidden)]
macro_rules! __hash_newtype_from_hex_strict {
    () => {
        /// Parse from exactly 64 lowercase/uppercase hex chars.
        ///
        /// # Errors
        /// Returns [`$crate::hash_newtype::HashNewtypeError`] on a
        /// wrong-length or non-hex input.
        pub fn from_hex(
            s: &str,
        ) -> ::core::result::Result<Self, $crate::hash_newtype::HashNewtypeError> {
            $crate::hash_newtype::parse_hex_32_strict(s).map(Self)
        }
    };
}

/// Internal — left-zero-padding `from_hex` body (shorthand-friendly).
#[macro_export]
#[doc(hidden)]
macro_rules! __hash_newtype_from_hex_padded {
    () => {
        /// Parse from `1..=64` hex chars, left-zero-padding to 64
        /// first so shorthand like `"ab"` round-trips.
        ///
        /// # Errors
        /// Returns [`$crate::hash_newtype::HashNewtypeError`] on an
        /// empty, over-long, or non-hex input.
        pub fn from_hex(
            s: &str,
        ) -> ::core::result::Result<Self, $crate::hash_newtype::HashNewtypeError> {
            $crate::hash_newtype::parse_hex_32_padded(s).map(Self)
        }
    };
}

/// Internal — emit the `Display` impl for the selected style.
#[macro_export]
#[doc(hidden)]
macro_rules! __hash_newtype_display {
    // prefix(N) selected (in any knob order)
    ($name:ident, display = prefix($n:literal) $($rest:tt)*) => {
        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&$crate::hash_newtype::hex_prefix(&self.0, $n))
            }
        }
    };
    ($name:ident, $_k:ident = $_v:tt , display = prefix($n:literal) $($rest:tt)*) => {
        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&$crate::hash_newtype::hex_prefix(&self.0, $n))
            }
        }
    };
    // full (default) — any leading knobs, no prefix marker
    ($name:ident, $($_rest:tt)*) => {
        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&$crate::hash_newtype::hex_full(&self.0))
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // Base form — full-hex Display, strict from_hex, not Copy.
    define_hash_newtype! {
        /// Sample base hash newtype.
        SampleHash
    }

    // Copy id-style newtype.
    define_hash_newtype! {
        #[derive(Copy)]
        /// Sample copy id.
        SampleId { display = full, from_hex = strict }
    }

    // Transparent-serde + Default + Copy + short-prefix Display +
    // padded from_hex — the revoada NodeId shape.
    define_hash_newtype! {
        #[derive(Copy, Default)]
        #[serde(transparent)]
        /// Sample transparent node id with short Display.
        SampleNode { display = prefix(6), from_hex = padded }
    }

    #[test]
    fn new_and_from_bytes_distinct() {
        let raw = SampleHash::new([0xab; 32]);
        let hashed = SampleHash::from_bytes(b"payload");
        assert_eq!(raw.0, [0xab; 32]);
        assert_ne!(raw, hashed);
        // from_bytes is BLAKE3 of input.
        assert_eq!(hashed.0, *blake3::hash(b"payload").as_bytes());
    }

    #[test]
    fn to_hex_is_64_lowercase() {
        let h = SampleHash::new([0xab; 32]);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
        assert_eq!(hex, "ab".repeat(32));
    }

    #[test]
    fn display_full_matches_to_hex() {
        let h = SampleHash::new([0xcd; 32]);
        assert_eq!(h.to_string(), h.to_hex());
        assert_eq!(h.to_string(), "cd".repeat(32));
    }

    #[test]
    fn from_hex_strict_round_trips() {
        let h = SampleHash::new([0x12; 32]);
        let hex = h.to_hex();
        let back = SampleHash::from_hex(&hex).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn from_hex_strict_rejects_short_and_nonhex() {
        assert!(matches!(
            SampleHash::from_hex("ab"),
            Err(HashNewtypeError::Length {
                expected: 64,
                got: 2
            })
        ));
        let bad = "zz".to_string() + &"00".repeat(31);
        assert!(matches!(
            SampleHash::from_hex(&bad),
            Err(HashNewtypeError::Digit { index: 0 })
        ));
    }

    #[test]
    fn copy_id_is_copy_and_carries_full_surface() {
        let a = SampleId::new([1u8; 32]);
        let b = a; // Copy — a still usable below.
        assert_eq!(a, b);
        // Exercise the full generated surface on the Copy variant too.
        let hashed = SampleId::from_bytes(b"id");
        assert_eq!(hashed.0, *blake3::hash(b"id").as_bytes());
        let hex = a.to_hex();
        assert_eq!(hex, "01".repeat(32));
        assert_eq!(SampleId::from_hex(&hex).unwrap(), a);
    }

    #[test]
    fn transparent_node_serde_is_bare_array_and_display_is_prefix() {
        let n = SampleNode::new([0xab; 32]);
        // serde(transparent) → serialize as the inner array, not a wrapper.
        let json = serde_json::to_string(&n).unwrap();
        let back: SampleNode = serde_json::from_str(&json).unwrap();
        assert_eq!(n, back);
        // Display = first 6 bytes (12 hex chars).
        assert_eq!(n.to_string(), "abababababab");
        // Default present.
        assert_eq!(SampleNode::default(), SampleNode::new([0u8; 32]));
        // from_bytes still BLAKE3s the input even on the prefix-Display variant.
        let hashed = SampleNode::from_bytes(b"node");
        assert_eq!(hashed.0, *blake3::hash(b"node").as_bytes());
    }

    #[test]
    fn padded_from_hex_round_trips_shorthand() {
        let n = SampleNode::from_hex("ab").unwrap();
        // "ab" left-zero-pads → last byte is 0xab, rest zero.
        let mut expected = [0u8; 32];
        expected[31] = 0xab;
        assert_eq!(n, SampleNode::new(expected));
        // to_hex is still full 64 chars.
        assert_eq!(n.to_hex().len(), 64);
    }

    #[test]
    fn padded_from_hex_rejects_empty_and_overlong() {
        assert!(matches!(
            SampleNode::from_hex(""),
            Err(HashNewtypeError::Length { .. })
        ));
        let over = "a".repeat(65);
        assert!(matches!(
            SampleNode::from_hex(&over),
            Err(HashNewtypeError::Length {
                expected: 64,
                got: 65
            })
        ));
    }

    #[test]
    fn hash_newtype_error_kind_tags() {
        assert_eq!(
            (HashNewtypeError::Length {
                expected: 64,
                got: 2
            })
            .kind(),
            "length"
        );
        assert_eq!((HashNewtypeError::Digit { index: 0 }).kind(), "digit");
    }
}
