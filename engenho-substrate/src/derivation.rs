//! Sui-derivation substrate primitive — Nix derivations as a typed,
//! location-independent value the engenho fabric can move + cache anywhere.
//!
//! ## Design source
//!
//! This module's surface is the load-bearing primitive identified in the
//! `engenho/docs/SUI-DERIVATION-FABRIC.md` design (research output of the
//! Nix-as-substrate exploration). The recommendation was:
//!
//!   Highest leverage = `DerivationCacheBackend` trait + `MemoryBackend`
//!   impl (~150 LoC). One typed slot unblocks every downstream consumer;
//!   later iroh / NATS-Object / federation impls slot in without
//!   touching call sites.
//!
//! ## Mapping to the four transports (planned)
//!
//! | Tier     | Wire shape                                                 |
//! |----------|------------------------------------------------------------|
//! | Strong   | `MagicBlob<Drv>` committed via `engenho-store` Raft         |
//! | Eventual | Per-node `DrvCacheState` gossiped via chitchat              |
//! | Durable  | `BuildEvent` stream via JetStream                           |
//! | Content  | `NarBlob` bytes resolved P2P via iroh / NATS Object         |
//!
//! ## What's in this commit
//!
//! - [`Drv`] — typed derivation value (drv_hash, system, outputs, inputs)
//! - [`DrvHash`] / [`NarHash`] — newtype BLAKE3 hashes; not interchangeable
//! - [`OutputPath`] — typed `/nix/store/...` paths
//! - [`NarBlob`] — content-addressed blob with the NAR bytes
//! - [`Realisation`] — `(DrvHash, OutputName) → OutputPath` binding
//! - [`DerivationCacheBackend`] trait — the load-bearing pluggable slot
//! - [`MemoryDerivationCache`] — in-memory impl for tests + bootstrap
//!
//! Substrate-level only; no consumer wiring yet. The next round wires
//! `engenho-store::propose_drv()` + a Raft path for `MagicBlob<Drv>`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

crate::define_hash_newtype! {
    /// BLAKE3 hash of a derivation's canonical encoding. Newtype so the
    /// type system distinguishes "this is a drv hash" from "this is a
    /// NAR hash" — different things, both BLAKE3, easy to confuse.
    DrvHash
}

crate::define_hash_newtype! {
    /// BLAKE3 hash of a NAR's bytes. Sibling type to [`DrvHash`].
    NarHash
}

/// Typed `/nix/store/{hash}-{name}` path. The hash here is the
/// Nix-style truncated hash (matched 1:1 to upstream CppNix paths
/// when bridging sui's translation); we keep it opaque since it's
/// store-format-dependent, not a BLAKE3 of payload.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OutputPath(pub String);

impl OutputPath {
    /// New from arbitrary string. Caller responsible for `/nix/store/`
    /// shape; trait does not validate (different stores have different
    /// shapes — sui-store / CppNix / future content-addressed stores).
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow as `&str`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for OutputPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Typed derivation — substrate-side analog of sui's
/// `sui_compat::derivation::Derivation`.
///
/// Keeps the same shape (fields named to match the ATerm format) so
/// translation in/out is a field-by-field copy. Sui owns the canonical
/// parser + serializer for ATerm; the substrate owns the typed value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drv {
    /// BLAKE3 hash of the canonical-form drv bytes. Identity for the
    /// content tier + Raft tier (committed under this hash).
    pub drv_hash: DrvHash,
    /// `x86_64-linux` / `aarch64-darwin` / `wasm32-wasi`.
    pub system: String,
    /// Output spec: outputname → typed path (after realisation).
    /// `outputs["out"]` is the conventional default.
    pub outputs: BTreeMap<String, OutputPath>,
    /// `input_drvs[input_drv_hash] = [requested output names]`.
    pub input_drvs: BTreeMap<DrvHash, Vec<String>>,
    /// `input_srcs` — store paths the drv depends on directly (source
    /// files, fetchurl results, vendored tarballs).
    pub input_srcs: Vec<OutputPath>,
    /// Builder executable path.
    pub builder: String,
    /// Builder argv.
    pub args: Vec<String>,
    /// Environment variables presented to the builder.
    pub env: BTreeMap<String, String>,
}

impl Drv {
    /// Construct a minimal Drv for tests. Field order = struct order.
    #[must_use]
    pub fn synthetic(drv_hash: DrvHash, system: impl Into<String>) -> Self {
        Self {
            drv_hash,
            system: system.into(),
            outputs: BTreeMap::new(),
            input_drvs: BTreeMap::new(),
            input_srcs: Vec::new(),
            builder: String::new(),
            args: Vec::new(),
            env: BTreeMap::new(),
        }
    }
}

/// Content-addressed NAR blob. The NAR is the canonical
/// Nix-Archive serialization of a /nix/store path's directory tree.
///
/// Sealed: the address and the length are derived from the bytes, never
/// supplied. The fields are private and [`NarBlob::from_bytes`] is the one
/// constructor; deserialisation goes through it too and refuses a record
/// whose stored `hash` or `size` disagrees with its bytes
/// ([`NarBlobError`]). A blob whose address is not the BLAKE3 of its own
/// payload therefore has no code path, and a cache never has to re-check
/// one it was handed.
///
/// ```compile_fail
/// // The struct literal that used to build a lying blob no longer compiles.
/// use engenho_substrate::{NarBlob, NarHash};
/// let _ = NarBlob { hash: NarHash::from_bytes(b"other"), size: 5, bytes: b"hello".to_vec() };
/// ```
///
/// ```compile_fail
/// // Nor can a sound blob be edited into a lying one.
/// use engenho_substrate::NarBlob;
/// let mut blob = NarBlob::from_bytes(b"hello".to_vec());
/// blob.bytes = b"other".to_vec();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "NarBlobRecord")]
pub struct NarBlob {
    /// BLAKE3 of `bytes`. Address for the content tier.
    hash: NarHash,
    /// The bytes themselves.
    bytes: Vec<u8>,
}

impl NarBlob {
    /// Build a blob from raw bytes; the hash and size are derived from them.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let hash = NarHash::from_bytes(&bytes);
        Self { hash, bytes }
    }

    /// BLAKE3 of the bytes: the blob's address in the content tier.
    #[must_use]
    pub fn hash(&self) -> &NarHash {
        &self.hash
    }

    /// Byte length of the payload.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// The payload.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Take the payload, dropping the (derived) address.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Why a stored NAR record was refused at the parse boundary.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NarBlobError {
    /// The record's `hash` is not the BLAKE3 of its bytes.
    #[error("nar record says hash {recorded}, its bytes hash to {derived}")]
    HashMismatch {
        /// The hash the record carried.
        recorded: NarHash,
        /// The hash of the bytes it carried.
        derived: NarHash,
    },
    /// The record's `size` is not the length of its bytes.
    #[error("nar record says {recorded} bytes, it carries {derived}")]
    SizeMismatch {
        /// The size the record carried.
        recorded: u64,
        /// The length of the bytes it carried.
        derived: u64,
    },
}

crate::impl_error_kind! {
    NarBlobError {
        { HashMismatch { .. } } => "hash_mismatch",
        { SizeMismatch { .. } } => "size_mismatch",
    }
}

/// The on-the-wire shape of a [`NarBlob`]: `hash`, `size`, `bytes`, in that
/// order. Unchanged from before the seal, so every record already written
/// still reads; only the checks on the way in are new.
#[derive(Deserialize)]
struct NarBlobRecord {
    hash: NarHash,
    size: u64,
    bytes: Vec<u8>,
}

/// Borrowed twin of [`NarBlobRecord`] for writing, so serialising a blob
/// never copies its payload.
#[derive(Serialize)]
struct NarBlobRecordRef<'a> {
    hash: &'a NarHash,
    size: u64,
    bytes: &'a [u8],
}

impl TryFrom<NarBlobRecord> for NarBlob {
    type Error = NarBlobError;

    fn try_from(record: NarBlobRecord) -> Result<Self, Self::Error> {
        let blob = Self::from_bytes(record.bytes);
        if blob.hash != record.hash {
            return Err(NarBlobError::HashMismatch {
                recorded: record.hash,
                derived: blob.hash,
            });
        }
        if blob.size() != record.size {
            return Err(NarBlobError::SizeMismatch {
                recorded: record.size,
                derived: blob.size(),
            });
        }
        Ok(blob)
    }
}

impl Serialize for NarBlob {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        NarBlobRecordRef {
            hash: &self.hash,
            size: self.size(),
            bytes: &self.bytes,
        }
        .serialize(serializer)
    }
}

/// `(DrvHash, OutputName) → OutputPath` binding. The proof that "drv X's
/// `out` output is at /nix/store/Y". Future tameshi-signed; today opaque
/// (signing wired in next round when Raft commits these).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Realisation {
    /// Drv this realisation is for.
    pub drv_hash: DrvHash,
    /// Output name (typically "out").
    pub output_name: String,
    /// Resulting store path.
    pub output_path: OutputPath,
    /// Optional NAR hash if the cache has the blob too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nar_hash: Option<NarHash>,
}

/// Backend errors.
#[derive(Debug, Clone, Error)]
pub enum CacheError {
    /// Backend (iroh, NATS, local disk) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Hash mismatch — a backend served a blob whose address is not the
    /// one requested, or a stored file failed its outer checksum. Surface
    /// this loudly so corrupt caches don't silently propagate.
    #[error("hash mismatch: requested {requested}, got {actual}")]
    HashMismatch {
        /// What the caller asked for.
        requested: String,
        /// What the cache served.
        actual: String,
    },
}

// Absence is not an error: every lookup returns `Ok(None)` (or an empty
// `Vec`) for a key the cache does not hold, and the caller may try a
// higher tier. There is deliberately no "not found" variant here; nothing
// produced one, and a second spelling of absence is one a caller could
// match on while the real answer arrived as `None`.

crate::impl_error_kind! {
    CacheError {
        (Backend(_)) => "backend",
        { HashMismatch { .. } } => "hash_mismatch",
    }
}

/// The load-bearing pluggable slot. Peer to `ContainerRuntime` /
/// `VolumeRuntime` / `ServiceRouter` from engenho-controllers.
///
/// Tiered consumers compose multiple backends:
///   * L0 in-memory eval cache (per-node, hot)
///   * L1 local-disk NAR store (per-node)
///   * L2 cluster-wide content cache (cluster-wide CAS via iroh / NATS-Object)
///   * L3 federation cache (cross-cluster via teia leaf-nodes)
///
/// Each tier implements this trait; a `TieredCache` (future round)
/// walks them in order, promoting hits.
#[async_trait]
pub trait DerivationCacheBackend: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// Look up a derivation by hash. `Ok(None)` = not in this cache.
    ///
    /// # Errors
    /// [`CacheError::Backend`] on backend failure.
    async fn get_drv(&self, hash: &DrvHash) -> Result<Option<Drv>, CacheError>;

    /// Insert a derivation. Idempotent — re-insert of the same drv
    /// (same hash + same value) is a no-op.
    ///
    /// # Errors
    /// [`CacheError::Backend`] on backend failure.
    async fn put_drv(&self, drv: &Drv) -> Result<(), CacheError>;

    /// Fetch a NAR blob by hash. `Ok(None)` = not in this cache.
    ///
    /// A [`NarBlob`] is self-consistent by construction (its address is the
    /// BLAKE3 of its bytes), so what a backend must still check is that the
    /// blob it serves is the one asked for: a blob whose `hash()` is not
    /// `hash` MUST come back as [`CacheError::HashMismatch`], never as a
    /// value.
    ///
    /// # Errors
    /// [`CacheError::Backend`], [`CacheError::HashMismatch`].
    async fn get_nar(&self, hash: &NarHash) -> Result<Option<NarBlob>, CacheError>;

    /// Insert a NAR blob. Idempotent.
    ///
    /// # Errors
    /// [`CacheError::Backend`].
    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError>;

    /// List all realisations for a derivation (typically one per
    /// output; "out", "dev", "doc", etc.).
    ///
    /// # Errors
    /// [`CacheError::Backend`].
    async fn list_realisations(&self, drv_hash: &DrvHash) -> Result<Vec<Realisation>, CacheError>;

    /// Record a realisation. Idempotent.
    ///
    /// # Errors
    /// [`CacheError::Backend`].
    async fn put_realisation(&self, realisation: &Realisation) -> Result<(), CacheError>;
}

// =================================================================
// MemoryDerivationCache — deterministic in-memory backend
// =================================================================

/// In-memory L0 cache. Fast, deterministic, suitable for unit tests
/// + bootstrap-cluster scenarios where no on-disk store is yet
/// configured. Production tiers wrap on-disk + cluster + federation
/// backends (future rounds).
#[derive(Default, Clone)]
pub struct MemoryDerivationCache {
    inner: Arc<Mutex<MemoryState>>,
}

#[derive(Default)]
struct MemoryState {
    drvs: BTreeMap<DrvHash, Drv>,
    nars: BTreeMap<NarHash, NarBlob>,
    /// realisations[drv_hash] = realisations for that drv
    realisations: BTreeMap<DrvHash, Vec<Realisation>>,
}

impl MemoryDerivationCache {
    /// Fresh empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count of stored derivations (telemetry helper).
    pub async fn drv_count(&self) -> usize {
        self.inner.lock().await.drvs.len()
    }

    /// Count of stored NARs (telemetry helper).
    pub async fn nar_count(&self) -> usize {
        self.inner.lock().await.nars.len()
    }
}

#[async_trait]
impl DerivationCacheBackend for MemoryDerivationCache {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn get_drv(&self, hash: &DrvHash) -> Result<Option<Drv>, CacheError> {
        Ok(self.inner.lock().await.drvs.get(hash).cloned())
    }

    async fn put_drv(&self, drv: &Drv) -> Result<(), CacheError> {
        let mut s = self.inner.lock().await;
        s.drvs.insert(drv.drv_hash.clone(), drv.clone());
        Ok(())
    }

    async fn get_nar(&self, hash: &NarHash) -> Result<Option<NarBlob>, CacheError> {
        // Every entry is keyed by its own `hash()` (see put_nar), so the
        // blob under `hash` is the blob asked for.
        Ok(self.inner.lock().await.nars.get(hash).cloned())
    }

    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError> {
        // No claim to check: a NarBlob's address is derived from its bytes.
        let mut s = self.inner.lock().await;
        s.nars.insert(blob.hash().clone(), blob.clone());
        Ok(())
    }

    async fn list_realisations(&self, drv_hash: &DrvHash) -> Result<Vec<Realisation>, CacheError> {
        Ok(self
            .inner
            .lock()
            .await
            .realisations
            .get(drv_hash)
            .cloned()
            .unwrap_or_default())
    }

    async fn put_realisation(&self, realisation: &Realisation) -> Result<(), CacheError> {
        let mut s = self.inner.lock().await;
        let entry = s
            .realisations
            .entry(realisation.drv_hash.clone())
            .or_default();
        // Replace any existing realisation for the same output_name.
        entry.retain(|r| r.output_name != realisation.output_name);
        entry.push(realisation.clone());
        Ok(())
    }
}

// =================================================================
// Helpers
// =================================================================

// hex_encode helper extracted to crate::hex per PRIME DIRECTIVE;
// DrvHash / NarHash now go through `define_hash_newtype!`.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex::hex_encode;

    fn sample_drv() -> Drv {
        let hash = DrvHash::from_bytes(b"sample-drv");
        Drv::synthetic(hash, "x86_64-linux")
    }

    // ── Hash newtypes ───────────────────────────────────────────

    #[test]
    fn drv_hash_and_nar_hash_are_distinct_types() {
        let d = DrvHash::from_bytes(b"x");
        let n = NarHash::from_bytes(b"x");
        // Both BLAKE3-of-"x"; same bytes, different types.
        assert_eq!(d.0, n.0);
        // Distinct types — assignment between them would be a compile error.
    }

    #[test]
    fn drv_hash_hex_is_64_chars() {
        let h = DrvHash::from_bytes(b"engenho-substrate");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn drv_hash_display_matches_hex() {
        let h = DrvHash::from_bytes(b"x");
        assert_eq!(format!("{h}"), h.to_hex());
    }

    #[test]
    fn nar_hash_display_matches_hex() {
        let h = NarHash::from_bytes(b"x");
        assert_eq!(format!("{h}"), h.to_hex());
    }

    #[test]
    fn nar_blob_from_bytes_computes_hash_and_size() {
        let blob = NarBlob::from_bytes(b"hello".to_vec());
        assert_eq!(blob.size(), 5);
        assert_eq!(blob.hash(), &NarHash::from_bytes(b"hello"));
        assert_eq!(blob.bytes(), b"hello");
        assert_eq!(blob.into_bytes(), b"hello".to_vec());
    }

    /// The wire record of a sound blob, as `serde_json::Value`, so a test
    /// can tamper with one field and hand it back.
    fn wire(bytes: &[u8]) -> serde_json::Value {
        serde_json::to_value(NarBlob::from_bytes(bytes.to_vec())).unwrap()
    }

    /// ★ T5.6: a record whose hash is not its bytes' BLAKE3 used to
    /// deserialise into a `NarBlob` that lied about its address. It is now
    /// refused at the parse boundary, naming both hashes.
    #[test]
    fn deserialising_a_record_with_a_foreign_hash_is_refused() {
        let mut v = wire(b"hello");
        v["hash"] = wire(b"other")["hash"].clone();
        let err = serde_json::from_value::<NarBlob>(v).unwrap_err();
        let expected = NarBlobError::HashMismatch {
            recorded: NarHash::from_bytes(b"other"),
            derived: NarHash::from_bytes(b"hello"),
        };
        assert_eq!(err.to_string(), expected.to_string());
    }

    /// ★ T5.6: the size is derived from the bytes too; a record that
    /// disagrees is refused rather than trusted for "fast metadata".
    #[test]
    fn deserialising_a_record_with_a_wrong_size_is_refused() {
        let mut v = wire(b"hello");
        v["size"] = serde_json::json!(4);
        let err = serde_json::from_value::<NarBlob>(v).unwrap_err();
        let expected = NarBlobError::SizeMismatch {
            recorded: 4,
            derived: 5,
        };
        assert_eq!(err.to_string(), expected.to_string());
    }

    #[test]
    fn deserialising_a_record_without_a_size_is_refused() {
        let mut v = wire(b"hello");
        v.as_object_mut().unwrap().remove("size");
        assert!(serde_json::from_value::<NarBlob>(v).is_err());
    }

    /// The seal changed what is checked on the way in, not the bytes on
    /// the wire: records written before it still read, field for field.
    #[test]
    fn wire_shape_is_hash_size_bytes() {
        let v = wire(b"abc");
        assert_eq!(
            v["hash"],
            serde_json::to_value(NarHash::from_bytes(b"abc")).unwrap()
        );
        assert_eq!(v["size"], serde_json::json!(3));
        assert_eq!(v["bytes"], serde_json::json!([97, 98, 99]));
        assert_eq!(v.as_object().unwrap().len(), 3);
    }

    #[test]
    fn nar_blob_error_kinds_are_stable() {
        let h = NarHash::from_bytes(b"x");
        assert_eq!(
            NarBlobError::HashMismatch {
                recorded: h.clone(),
                derived: h
            }
            .kind(),
            "hash_mismatch"
        );
        assert_eq!(
            NarBlobError::SizeMismatch {
                recorded: 1,
                derived: 2
            }
            .kind(),
            "size_mismatch"
        );
    }

    #[test]
    fn output_path_round_trips_string() {
        let p = OutputPath::new("/nix/store/abc-foo");
        assert_eq!(p.as_str(), "/nix/store/abc-foo");
        assert_eq!(format!("{p}"), "/nix/store/abc-foo");
    }

    // ── MemoryDerivationCache ────────────────────────────────────

    #[tokio::test]
    async fn cache_put_get_drv_round_trip() {
        let cache = MemoryDerivationCache::new();
        let drv = sample_drv();
        assert_eq!(cache.get_drv(&drv.drv_hash).await.unwrap(), None);
        cache.put_drv(&drv).await.unwrap();
        assert_eq!(
            cache.get_drv(&drv.drv_hash).await.unwrap(),
            Some(drv.clone())
        );
        assert_eq!(cache.drv_count().await, 1);
    }

    #[tokio::test]
    async fn cache_put_drv_idempotent() {
        let cache = MemoryDerivationCache::new();
        let drv = sample_drv();
        cache.put_drv(&drv).await.unwrap();
        cache.put_drv(&drv).await.unwrap();
        assert_eq!(cache.drv_count().await, 1);
    }

    #[tokio::test]
    async fn cache_put_get_nar_round_trip() {
        let cache = MemoryDerivationCache::new();
        let blob = NarBlob::from_bytes(b"nar-content".to_vec());
        let h = blob.hash().clone();
        assert_eq!(cache.get_nar(&h).await.unwrap(), None);
        cache.put_nar(&blob).await.unwrap();
        let got = cache.get_nar(&h).await.unwrap();
        assert_eq!(got, Some(blob));
        assert_eq!(cache.nar_count().await, 1);
    }

    /// Absence is `Ok(None)` for both lookups — the only spelling of "not
    /// here" a caller can receive.
    #[tokio::test]
    async fn cache_absence_is_ok_none() {
        let cache = MemoryDerivationCache::new();
        assert_eq!(
            cache.get_drv(&DrvHash::from_bytes(b"no")).await.unwrap(),
            None
        );
        assert_eq!(
            cache.get_nar(&NarHash::from_bytes(b"no")).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn cache_realisations_grouped_by_drv() {
        let cache = MemoryDerivationCache::new();
        let drv_hash = DrvHash::from_bytes(b"d");
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: OutputPath::new("/nix/store/a-out"),
                nar_hash: None,
            })
            .await
            .unwrap();
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "dev".into(),
                output_path: OutputPath::new("/nix/store/b-dev"),
                nar_hash: None,
            })
            .await
            .unwrap();
        let list = cache.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 2);
        assert!(list.iter().any(|r| r.output_name == "out"));
        assert!(list.iter().any(|r| r.output_name == "dev"));
    }

    #[tokio::test]
    async fn cache_realisation_put_replaces_same_output_name() {
        let cache = MemoryDerivationCache::new();
        let drv_hash = DrvHash::from_bytes(b"d");
        let path_v1 = OutputPath::new("/nix/store/v1-out");
        let path_v2 = OutputPath::new("/nix/store/v2-out");
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: path_v1,
                nar_hash: None,
            })
            .await
            .unwrap();
        cache
            .put_realisation(&Realisation {
                drv_hash: drv_hash.clone(),
                output_name: "out".into(),
                output_path: path_v2.clone(),
                nar_hash: None,
            })
            .await
            .unwrap();
        let list = cache.list_realisations(&drv_hash).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].output_path, path_v2);
    }

    #[tokio::test]
    async fn cache_list_realisations_empty_when_unknown() {
        let cache = MemoryDerivationCache::new();
        let unknown = DrvHash::from_bytes(b"never-seen");
        assert!(cache.list_realisations(&unknown).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cache_name_is_stable() {
        let cache = MemoryDerivationCache::new();
        assert_eq!(cache.name(), "memory");
    }

    // ── Serde + error kinds ─────────────────────────────────────

    #[test]
    fn drv_round_trips_serde() {
        let drv = sample_drv();
        let bytes = serde_json::to_vec(&drv).unwrap();
        let back: Drv = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, drv);
    }

    #[test]
    fn nar_blob_round_trips_serde() {
        let blob = NarBlob::from_bytes(b"abc".to_vec());
        let bytes = serde_json::to_vec(&blob).unwrap();
        let back: NarBlob = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, blob);
    }

    #[test]
    fn realisation_round_trips_serde() {
        let r = Realisation {
            drv_hash: DrvHash::from_bytes(b"d"),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a-out"),
            nar_hash: Some(NarHash::from_bytes(b"n")),
        };
        let bytes = serde_json::to_vec(&r).unwrap();
        let back: Realisation = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn cache_error_kinds_are_stable() {
        assert_eq!(CacheError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            CacheError::HashMismatch {
                requested: "a".into(),
                actual: "b".into()
            }
            .kind(),
            "hash_mismatch"
        );
    }

    /// ★ T5.6: `CacheError` is exactly `{Backend, HashMismatch}`. Nothing
    /// ever produced `NotFound` (absence is `Ok(None)`); this match has no
    /// wildcard, so a third variant — that one or any other — is E0004
    /// here, and has to be argued for.
    #[test]
    fn cache_error_has_no_absence_variant() {
        fn tag(e: &CacheError) -> &'static str {
            match e {
                CacheError::Backend(_) => "backend",
                CacheError::HashMismatch { .. } => "hash_mismatch",
            }
        }
        for e in [
            CacheError::Backend("x".into()),
            CacheError::HashMismatch {
                requested: "a".into(),
                actual: "b".into(),
            },
        ] {
            assert_eq!(tag(&e), e.kind());
        }
    }

    #[test]
    fn hex_encode_known_vector() {
        // BLAKE3 hash of empty string starts with af1349b9...
        let h = blake3::hash(b"");
        let s = hex_encode(h.as_bytes());
        assert!(s.starts_with("af1349b9"));
    }
}
