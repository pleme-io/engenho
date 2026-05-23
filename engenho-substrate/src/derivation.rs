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

/// BLAKE3 hash of a derivation's canonical encoding. Newtype so the
/// type system distinguishes "this is a drv hash" from "this is a
/// NAR hash" — different things, both BLAKE3, easy to confuse.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DrvHash(pub [u8; 32]);

impl DrvHash {
    /// New from raw bytes.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Compute from arbitrary bytes via BLAKE3.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Lowercase hex representation (64 chars).
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }
}

impl std::fmt::Display for DrvHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// BLAKE3 hash of a NAR's bytes. Sibling type to [`DrvHash`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NarHash(pub [u8; 32]);

impl NarHash {
    /// New from raw bytes.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Compute from NAR bytes.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Lowercase hex representation.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }
}

impl std::fmt::Display for NarHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NarBlob {
    /// BLAKE3 of `bytes`. Address for the content tier.
    pub hash: NarHash,
    /// Byte length (kept separate for fast metadata queries).
    pub size: u64,
    /// The bytes themselves.
    pub bytes: Vec<u8>,
}

impl NarBlob {
    /// Build a blob from raw bytes; computes hash + size.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let hash = NarHash::from_bytes(&bytes);
        let size = bytes.len() as u64;
        Self { hash, size, bytes }
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
    /// Hash mismatch — a returned blob didn't match its requested hash.
    /// Surface this loudly so corrupt caches don't silently propagate.
    #[error("hash mismatch: requested {requested}, got {actual}")]
    HashMismatch {
        /// What the caller asked for.
        requested: String,
        /// What the cache served.
        actual: String,
    },
    /// Item not found in this cache. Caller may try a higher tier.
    #[error("not found: {0}")]
    NotFound(String),
}

crate::impl_error_kind! {
    CacheError {
        (Backend(_)) => "backend",
        { HashMismatch { .. } } => "hash_mismatch",
        (NotFound(_)) => "not_found",
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

    /// Look up a derivation by hash. None = not in this cache.
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

    /// Fetch a NAR blob by hash. Backends MUST verify the returned
    /// bytes BLAKE3-hash to `hash` before returning; on mismatch they
    /// MUST return [`CacheError::HashMismatch`] (not silently serve
    /// corrupt content).
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
        let blob = self.inner.lock().await.nars.get(hash).cloned();
        // Trait contract: backends MUST verify the returned bytes
        // hash to `hash`. The memory backend trivially does (we
        // stored under the right key) but we still recompute on the
        // way out — costs ~µs, catches every-other-day corruption
        // we couldn't otherwise see in tests.
        if let Some(b) = &blob {
            let actual = NarHash::from_bytes(&b.bytes);
            if &actual != hash {
                return Err(CacheError::HashMismatch {
                    requested: hash.to_hex(),
                    actual: actual.to_hex(),
                });
            }
        }
        Ok(blob)
    }

    async fn put_nar(&self, blob: &NarBlob) -> Result<(), CacheError> {
        // Verify the caller's claim matches the bytes (NarBlob is a
        // value with declared hash — we don't trust it blindly).
        let actual = NarHash::from_bytes(&blob.bytes);
        if actual != blob.hash {
            return Err(CacheError::HashMismatch {
                requested: blob.hash.to_hex(),
                actual: actual.to_hex(),
            });
        }
        let mut s = self.inner.lock().await;
        s.nars.insert(blob.hash.clone(), blob.clone());
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

// hex_encode helper extracted to crate::hex per PRIME DIRECTIVE.
use crate::hex::hex_encode;

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(blob.size, 5);
        assert_eq!(blob.hash, NarHash::from_bytes(b"hello"));
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
        let h = blob.hash.clone();
        assert_eq!(cache.get_nar(&h).await.unwrap(), None);
        cache.put_nar(&blob).await.unwrap();
        let got = cache.get_nar(&h).await.unwrap();
        assert_eq!(got, Some(blob));
        assert_eq!(cache.nar_count().await, 1);
    }

    #[tokio::test]
    async fn cache_put_nar_rejects_hash_mismatch() {
        let cache = MemoryDerivationCache::new();
        // Construct a blob claiming a hash that doesn't match its bytes.
        let bad = NarBlob {
            hash: NarHash::from_bytes(b"different"),
            size: 5,
            bytes: b"hello".to_vec(),
        };
        let err = cache.put_nar(&bad).await.unwrap_err();
        assert_eq!(err.kind(), "hash_mismatch");
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
        assert_eq!(CacheError::NotFound("x".into()).kind(), "not_found");
    }

    #[test]
    fn hex_encode_known_vector() {
        // BLAKE3 hash of empty string starts with af1349b9...
        let h = blake3::hash(b"");
        let s = hex_encode(h.as_bytes());
        assert!(s.starts_with("af1349b9"));
    }
}
