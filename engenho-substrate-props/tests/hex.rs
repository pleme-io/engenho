//! Property: Hex trait — lowercase round-trip + length invariant.

use engenho_substrate::{hex_encode, Hex};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    })]

    /// hex_encode is always 2× input length.
    #[test]
    fn hex_doubles_input_length(
        bytes in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        prop_assert_eq!(hex_encode(&bytes).len(), bytes.len() * 2);
    }

    /// hex_encode output is always lowercase ASCII hex.
    #[test]
    fn hex_output_is_lowercase_ascii_hex(
        bytes in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let s = hex_encode(&bytes);
        for c in s.chars() {
            prop_assert!(c.is_ascii_digit() || (c >= 'a' && c <= 'f'));
        }
    }

    /// hex_encode is deterministic for the same input.
    #[test]
    fn hex_is_deterministic(
        bytes in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let h1 = hex_encode(&bytes);
        let h2 = hex_encode(&bytes);
        prop_assert_eq!(h1, h2);
    }

    /// Different inputs → different hex (no collisions).
    #[test]
    fn hex_is_injective(
        b1 in proptest::collection::vec(any::<u8>(), 0..256),
        b2 in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        prop_assume!(b1 != b2);
        prop_assert_ne!(hex_encode(&b1), hex_encode(&b2));
    }

    /// Slice trait impl matches helper.
    #[test]
    fn slice_trait_matches_helper(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let via_helper = hex_encode(&bytes);
        let via_trait = bytes.as_slice().to_hex();
        prop_assert_eq!(via_helper, via_trait);
    }
}
