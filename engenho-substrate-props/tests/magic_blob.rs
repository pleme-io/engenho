//! Property: MagicBlob encode→decode is identity; corruption
//! rejected.

use engenho_substrate::MagicBlob;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

const TEST_MAGIC: &[u8] = b"engenho-prop-test v1\n";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Payload {
    n: u32,
    s: String,
    bytes: Vec<u8>,
}

proptest_with_env! {
    /// encode → decode produces the original value.
    #[test]
    fn encode_decode_round_trips(
        n in any::<u32>(),
        s in ".{0,256}",
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let value = Payload { n, s, bytes };
        let blob = MagicBlob { magic: TEST_MAGIC, value: value.clone() };
        let encoded = blob.encode().unwrap();
        let decoded: Payload = MagicBlob::<Payload>::decode(TEST_MAGIC, &encoded).unwrap();
        prop_assert_eq!(decoded, value);
    }

    /// Decode rejects wrong magic.
    #[test]
    fn decode_rejects_bad_magic(
        n in any::<u32>(),
        s in ".{0,128}",
    ) {
        let value = Payload { n, s, bytes: Vec::new() };
        let blob = MagicBlob { magic: TEST_MAGIC, value };
        let encoded = blob.encode().unwrap();
        let result = MagicBlob::<Payload>::decode(b"different magic", &encoded);
        prop_assert!(result.is_err());
        if let Err(e) = result {
            prop_assert_eq!(e.kind(), "bad_magic");
        }
    }

    /// Decode rejects truncation.
    #[test]
    fn decode_rejects_truncation(
        n in any::<u32>(),
        s in ".{0,128}",
        truncate_at in 1usize..32,
    ) {
        let value = Payload { n, s, bytes: Vec::new() };
        let blob = MagicBlob { magic: TEST_MAGIC, value };
        let encoded = blob.encode().unwrap();
        prop_assume!(encoded.len() > truncate_at);
        let truncated = &encoded[..encoded.len() - truncate_at];
        let result = MagicBlob::<Payload>::decode(TEST_MAGIC, truncated);
        prop_assert!(result.is_err());
    }

    /// Decode rejects corrupted payload (BLAKE3 mismatch).
    #[test]
    fn decode_rejects_payload_corruption(
        n in any::<u32>(),
        s in ".{1,128}",
        corrupt_at in 0usize..128,
    ) {
        let value = Payload { n, s, bytes: vec![1, 2, 3, 4, 5, 6, 7, 8] };
        let blob = MagicBlob { magic: TEST_MAGIC, value };
        let encoded = blob.encode().unwrap();
        // Corrupt the payload region (after magic + length + hash).
        let payload_offset = TEST_MAGIC.len() + 8 + 32;
        prop_assume!(encoded.len() > payload_offset);
        let mut corrupted = encoded.clone();
        let idx = payload_offset + (corrupt_at % (corrupted.len() - payload_offset));
        corrupted[idx] ^= 0xff;
        let result = MagicBlob::<Payload>::decode(TEST_MAGIC, &corrupted);
        // Either hash mismatch OR decode failure — both are valid
        // rejections; the substrate's contract is "any corruption
        // is detected and refused", not a specific error kind.
        prop_assert!(result.is_err());
    }
}
