//! Property: derivation primitives (Drv, NarBlob, NarHash, OutputPath, Realisation).

use engenho_substrate::{Drv, DrvHash, NarBlob, NarHash, OutputPath, Realisation};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

proptest_with_env! {
    /// DrvHash::new + to_hex round-trip preserves bytes via hex parse.
    #[test]
    fn drv_hash_to_hex_is_64_chars(bytes in any::<[u8; 32]>()) {
        let h = DrvHash::new(bytes);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// DrvHash::from_bytes is deterministic — same input bytes → same hash.
    #[test]
    fn drv_hash_from_bytes_is_deterministic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let h1 = DrvHash::from_bytes(&bytes);
        let h2 = DrvHash::from_bytes(&bytes);
        assert_eq!(h1, h2);
    }

    /// DrvHash::from_bytes diverges for distinct inputs (BLAKE3
    /// collision-resistant in practice).
    #[test]
    fn drv_hash_distinct_inputs_diverge(
        a in proptest::collection::vec(any::<u8>(), 1..128),
        b in proptest::collection::vec(any::<u8>(), 1..128),
    ) {
        prop_assume!(a != b);
        let ha = DrvHash::from_bytes(&a);
        let hb = DrvHash::from_bytes(&b);
        assert_ne!(ha, hb);
    }

    /// NarHash::from_bytes preserves length invariant + determinism.
    #[test]
    fn nar_hash_to_hex_is_64_chars(bytes in any::<[u8; 32]>()) {
        let h = NarHash::new(bytes);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
    }

    /// NarBlob::from_bytes computes correct size + hash.
    #[test]
    fn nar_blob_from_bytes_invariants(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
        let blob = NarBlob::from_bytes(bytes.clone());
        assert_eq!(blob.size, bytes.len() as u64);
        assert_eq!(blob.bytes, bytes);
        // Hash equals direct NarHash::from_bytes — round-trip identity.
        assert_eq!(blob.hash, NarHash::from_bytes(&bytes));
    }

    /// Two NarBlobs with same content are equal.
    #[test]
    fn nar_blob_eq_reflects_content(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let a = NarBlob::from_bytes(bytes.clone());
        let b = NarBlob::from_bytes(bytes);
        assert_eq!(a, b);
    }

    /// Drv::synthetic produces a minimal but valid Drv.
    #[test]
    fn drv_synthetic_has_no_inputs(hash_byte in any::<u8>(), system in "[a-z0-9_-]{2,20}") {
        let d = Drv::synthetic(DrvHash::new([hash_byte; 32]), &system);
        assert_eq!(d.system, system);
        assert!(d.input_drvs.is_empty());
        assert!(d.outputs.is_empty());
        assert!(d.env.is_empty());
    }

    /// Drv serde round-trips through JSON.
    #[test]
    fn drv_serde_round_trips(hash_byte in any::<u8>()) {
        let d = Drv::synthetic(DrvHash::new([hash_byte; 32]), "x86_64-linux");
        let json = serde_json::to_string(&d).unwrap();
        let back: Drv = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
    }

    /// NarBlob serde round-trips through JSON.
    #[test]
    fn nar_blob_serde_round_trips(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
        let blob = NarBlob::from_bytes(bytes);
        let json = serde_json::to_vec(&blob).unwrap();
        let back: NarBlob = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, blob);
    }

    /// Realisation serde round-trips through JSON.
    #[test]
    fn realisation_serde_round_trips(
        hash in any::<u8>(),
        output_byte in any::<u8>(),
        output_name in "[a-z]{1,16}",
        path in "[a-z/]{2,32}",
    ) {
        let r = Realisation {
            drv_hash: DrvHash::new([hash; 32]),
            output_name,
            output_path: OutputPath::new(path),
            nar_hash: Some(NarHash::new([output_byte; 32])),
        };
        let json = serde_json::to_vec(&r).unwrap();
        let back: Realisation = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, r);
    }

    /// Realisation with no nar_hash serde round-trips.
    #[test]
    fn realisation_without_nar_hash_serde_round_trips(
        hash in any::<u8>(),
        output_name in "[a-z]{1,16}",
        path in "[a-z/]{2,32}",
    ) {
        let r = Realisation {
            drv_hash: DrvHash::new([hash; 32]),
            output_name,
            output_path: OutputPath::new(path),
            nar_hash: None,
        };
        let json = serde_json::to_vec(&r).unwrap();
        let back: Realisation = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, r);
    }

    /// OutputPath::new + as_str round-trip preserves the string.
    #[test]
    fn output_path_as_str_round_trips(path in "[a-zA-Z0-9/_.-]{1,64}") {
        let op = OutputPath::new(&path);
        assert_eq!(op.as_str(), path);
    }
}
