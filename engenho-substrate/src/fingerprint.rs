//! `Fingerprint` trait + `impl_fingerprint!` macro + helper
//! function for the canonical BLAKE3-over-serde-bytes pattern.
//!
//! Closes the ≥3-site duplication of `pub fn fingerprint(&self)
//! -> [u8; 32] { ... }` across substrate primitives. Per the
//! PRIME DIRECTIVE — "ruthlessly standardize" — extracted as
//! sibling to `ErrorKind` and `Named`.
//!
//! ## Existing duplicate sites (extracted as proof)
//!
//!   - `ComposeIr::fingerprint()` — BLAKE3 over canonical JSON
//!   - `Linhagem::fingerprint()` — BLAKE3-chained generations
//!   - `MaterializationReceipt::id()` — BLAKE3 over canonical JSON
//!   - `EnsaioId::derive()` — composite BLAKE3
//!   - `GeracaoId::derive()` — composite BLAKE3
//!
//! All shared the same idiom: serialize via serde_json → hash
//! with BLAKE3 → return `[u8; 32]`. This module gives ONE typed
//! surface (`Fingerprint`) + ONE macro (`impl_fingerprint!`) +
//! ONE helper (`fingerprint_blake3`).
//!
//! ## Authoring
//!
//! ```ignore
//! use engenho_substrate::impl_fingerprint;
//!
//! #[derive(Serialize)]
//! pub struct MyConfig { ... }
//!
//! impl_fingerprint!(MyConfig);
//! // → impl Fingerprint for MyConfig { fn fingerprint(&self) -> [u8; 32] { ... } }
//! ```
//!
//! Manual impl for special cases (composite hashing, salted
//! domains, version-stamped wrappers):
//!
//! ```ignore
//! impl Fingerprint for MyChain {
//!     fn fingerprint(&self) -> [u8; 32] {
//!         let mut hasher = blake3::Hasher::new();
//!         hasher.update(b"engenho-chain-v1");
//!         for link in &self.links {
//!             hasher.update(&link.0);
//!         }
//!         *hasher.finalize().as_bytes()
//!     }
//! }
//! ```

use serde::Serialize;

/// Universal "this value has a deterministic 32-byte fingerprint"
/// surface. Implemented automatically by [`impl_fingerprint!`].
///
/// Fingerprint contract:
///   * Same value → same fingerprint (determinism)
///   * Different values → different fingerprint (overwhelming probability)
///   * Replayable: serialize → hash → byte-identical across runs
///
/// Used as the substrate's universal receipt subject / cache key /
/// ledger key derivation primitive.
pub trait Fingerprint {
    /// BLAKE3 hash of the canonical encoding.
    fn fingerprint(&self) -> [u8; 32];
}

/// Helper: BLAKE3-over-canonical-JSON for any `Serialize` value.
/// Used by `impl_fingerprint!` macro + callable directly.
///
/// # Panics
/// Panics only if `serde_json::to_vec` fails — should be
/// impossible for typed substrate values (no `f64::NAN` or other
/// JSON-incompatible inputs).
#[must_use]
pub fn fingerprint_blake3<T: Serialize>(value: &T) -> [u8; 32] {
    let bytes = serde_json::to_vec(value).expect("substrate value serializes");
    *blake3::hash(&bytes).as_bytes()
}

/// Generate `impl Fingerprint for $type` via the canonical
/// serde_json → BLAKE3 path.
///
/// Use the manual `impl Fingerprint` form for composite or
/// salted hashing (chain-style, magic-versioned, etc.).
#[macro_export]
macro_rules! impl_fingerprint {
    ($type:ident) => {
        impl $crate::fingerprint::Fingerprint for $type {
            fn fingerprint(&self) -> [u8; 32] {
                $crate::fingerprint::fingerprint_blake3(self)
            }
        }
    };
    ($type:ident<$($param:ident $(: $bound:tt)?),+>) => {
        impl<$($param $(: $bound)?),+> $crate::fingerprint::Fingerprint
            for $type<$($param),+>
        where
            Self: ::serde::Serialize,
        {
            fn fingerprint(&self) -> [u8; 32] {
                $crate::fingerprint::fingerprint_blake3(self)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
    struct Sample {
        n: u32,
        s: String,
    }

    impl_fingerprint!(Sample);

    #[derive(Serialize, Deserialize)]
    struct Generic<T>(T);

    impl_fingerprint!(Generic<T>);

    #[test]
    fn fingerprint_is_deterministic_for_same_value() {
        let s = Sample {
            n: 42,
            s: "hello".into(),
        };
        assert_eq!(s.fingerprint(), s.fingerprint());
    }

    #[test]
    fn fingerprint_diverges_per_distinct_value() {
        let a = Sample {
            n: 1,
            s: "a".into(),
        };
        let b = Sample {
            n: 2,
            s: "a".into(),
        };
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn fingerprint_is_32_bytes() {
        let s = Sample {
            n: 0,
            s: String::new(),
        };
        assert_eq!(s.fingerprint().len(), 32);
    }

    #[test]
    fn fingerprint_helper_matches_macro_impl() {
        let s = Sample {
            n: 5,
            s: "x".into(),
        };
        assert_eq!(s.fingerprint(), fingerprint_blake3(&s));
    }

    #[test]
    fn generic_type_fingerprint_works() {
        let g1: Generic<u32> = Generic(7);
        let g2: Generic<u32> = Generic(7);
        let g3: Generic<u32> = Generic(8);
        assert_eq!(g1.fingerprint(), g2.fingerprint());
        assert_ne!(g1.fingerprint(), g3.fingerprint());
    }

    #[test]
    fn fingerprint_is_polymorphic_via_trait_object() {
        let s1 = Sample {
            n: 1,
            s: "a".into(),
        };
        let s2 = Sample {
            n: 2,
            s: "b".into(),
        };
        let fps: Vec<&dyn Fingerprint> = vec![&s1, &s2];
        let collected: Vec<[u8; 32]> = fps.iter().map(|f| f.fingerprint()).collect();
        assert_ne!(collected[0], collected[1]);
    }

    // ── Composite-hash use case (manual impl, no macro) ────────

    struct Chain {
        links: Vec<[u8; 32]>,
    }

    impl Fingerprint for Chain {
        fn fingerprint(&self) -> [u8; 32] {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"engenho-chain-v1");
            for link in &self.links {
                hasher.update(link);
            }
            *hasher.finalize().as_bytes()
        }
    }

    #[test]
    fn manual_impl_works_for_composite_hashing() {
        let c1 = Chain {
            links: vec![[0u8; 32], [1u8; 32]],
        };
        let c2 = Chain {
            links: vec![[0u8; 32], [1u8; 32]],
        };
        let c3 = Chain {
            links: vec![[1u8; 32], [0u8; 32]],
        };
        assert_eq!(c1.fingerprint(), c2.fingerprint());
        assert_ne!(c1.fingerprint(), c3.fingerprint());
    }
}
