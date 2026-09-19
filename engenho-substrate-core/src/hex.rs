//! `Hex` trait + `hex_encode` helper.
//!
//! Closes the ≥3-site duplication of `fn hex_encode(bytes: &[u8])
//! -> String` AND the parallel `[u8; 32]::to_hex()` pattern on
//! every typed hash newtype (`EnsaioId`, `GeracaoId`, `DrvHash`,
//! `NarHash`, `NodeId`). One helper + one trait + an inherent
//! blanket impl makes every byte slice + every typed hash carry
//! a stable `.to_hex()` method.
//!
//! ## Authoring shape
//!
//! ```ignore
//! use engenho_substrate::{Hex, hex_encode};
//!
//! // Direct helper for byte slices:
//! let h = hex_encode(&[0xab, 0xcd]); // "abcd"
//!
//! // Trait method on anything that owns 32 bytes:
//! impl Hex for MyHash {
//!     fn as_bytes(&self) -> &[u8] {
//!         &self.0
//!     }
//! }
//! my_hash.to_hex(); // → 64-char lowercase hex
//! ```
//!
//! Lowercase output by design — matches `blake3::Hash::to_hex()`
//! convention. Every hex string the substrate emits is the same
//! shape: snake-case-safe, URL-safe, filesystem-safe.

/// Lowercase-hex encode any byte slice. Stable: same input →
/// byte-identical output forever. No-alloc-after-output beyond
/// the returned `String`.
#[must_use]
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Universal "this value renders as hex" surface. Implementers
/// supply `as_bytes(&self) -> &[u8]`; the trait provides the
/// `.to_hex()` method via the canonical [`hex_encode`] helper.
///
/// Newtype hash wrappers (`EnsaioId`, `DrvHash`, etc.) implement
/// this; their hex output is always lowercase + always
/// `2 × bytes.len()` characters.
pub trait Hex {
    /// The underlying bytes that get rendered as hex.
    fn as_bytes(&self) -> &[u8];

    /// Lowercase hex representation. Default impl delegates to
    /// [`hex_encode`] — operators normally don't override.
    fn to_hex(&self) -> String {
        hex_encode(self.as_bytes())
    }
}

/// Blanket impl for typed byte arrays + slices. Lets generic
/// code dispatch `.to_hex()` on any `&[u8]` / `&[u8; N]` without
/// per-type ceremony.
impl Hex for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl<const N: usize> Hex for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encode_known_vectors() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0]), "00");
        assert_eq!(hex_encode(&[0xab]), "ab");
        assert_eq!(hex_encode(&[0xab, 0xcd, 0xef]), "abcdef");
        assert_eq!(hex_encode(&[0xff]), "ff");
        assert_eq!(hex_encode(&[0x00, 0x01, 0x02, 0x03]), "00010203");
    }

    #[test]
    fn hex_encode_always_lowercase() {
        let bytes = (0u8..=255).collect::<Vec<_>>();
        let hex = hex_encode(&bytes);
        for c in hex.chars() {
            assert!(c.is_ascii_lowercase() || c.is_ascii_digit());
        }
    }

    #[test]
    fn hex_encode_doubles_input_length() {
        for n in [0usize, 1, 7, 16, 32, 64, 1024] {
            let input = vec![0xab; n];
            let hex = hex_encode(&input);
            assert_eq!(hex.len(), n * 2);
        }
    }

    #[test]
    fn slice_to_hex_via_trait() {
        let bytes: &[u8] = &[0xab, 0xcd];
        assert_eq!(bytes.to_hex(), "abcd");
    }

    #[test]
    fn array_to_hex_via_trait() {
        let bytes: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
        assert_eq!(bytes.to_hex(), "01020304");
    }

    #[test]
    fn blake3_output_to_hex_matches_helper() {
        let h = blake3::hash(b"hello");
        let bytes = h.as_bytes();
        // The trait method on the [u8; 32] array.
        let our_hex = bytes.to_hex();
        let blake_hex = h.to_hex().to_string();
        assert_eq!(our_hex, blake_hex);
    }

    // Custom newtype demonstrating manual impl.
    pub struct CustomHash(pub [u8; 8]);

    impl Hex for CustomHash {
        fn as_bytes(&self) -> &[u8] {
            &self.0
        }
    }

    #[test]
    fn custom_newtype_to_hex_works() {
        let h = CustomHash([0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04]);
        assert_eq!(h.to_hex(), "deadbeef01020304");
    }

    #[test]
    fn polymorphic_dispatch_via_trait_object() {
        let a = CustomHash([0xff; 8]);
        let b: [u8; 4] = [0xab, 0xcd, 0xef, 0x12];
        let hexers: Vec<&dyn Hex> = vec![&a, &b];
        let outs: Vec<String> = hexers.iter().map(|h| h.to_hex()).collect();
        assert_eq!(outs[0], "ffffffffffffffff");
        assert_eq!(outs[1], "abcdef12");
    }
}
