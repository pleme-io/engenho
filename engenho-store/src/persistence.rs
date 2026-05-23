//! C4 — disk persistence for the catalog (Raft state machine).
//!
//! Until C4, the entire substrate was in-memory. A node restart
//! discarded every K8s resource. This module ships the durable
//! catalog primitive: `CatalogSnapshot::save_to(path)` writes the
//! current state atomically + fsync-anchored;
//! `CatalogSnapshot::load_from(path)` reads it back; the store
//! can call both around graceful shutdown + boot to survive
//! restarts.
//!
//! ## Scope
//!
//! This commit ships the **catalog-snapshot** primitive. Full
//! openraft `RaftLogStorage` / `RaftStateMachine` over sled or
//! redb is C4b — substantial follow-up. The snapshot primitive
//! covers the most operationally-important case (resource state
//! survives node restart) without the broader rewrite.
//!
//! ## File format
//!
//! BLAKE3-prefixed serde_json bytes:
//!
//! ```text
//! "engenho-catalog-snapshot v1\n"  ← 28-byte magic
//! <8 LE bytes — length of payload>
//! <32 bytes — BLAKE3 hash of payload>
//! <payload JSON>
//! ```
//!
//! The header + hash let `load_from` reject corrupt files loudly
//! rather than silently replay a partial state. Atomic write goes
//! to `{path}.tmp` first, fsync, then rename — same pattern as
//! kasou v0.2.0's machine-identifier persistence.

use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::state::ResourceCatalogSnapshot;

/// File-format magic header — version-stamped so future formats
/// can be detected on load.
const MAGIC_V1: &[u8] = b"engenho-catalog-snapshot v1\n";

/// Wrapper around [`ResourceCatalogSnapshot`] that knows how to
/// serialize itself to a magic-headered, hash-checked file.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    /// Inner catalog state.
    pub catalog: ResourceCatalogSnapshot,
    /// Last-applied Raft log index — recorded so the apply path
    /// can resume from this offset rather than re-applying.
    pub last_applied_index: u64,
}

impl CatalogSnapshot {
    /// New empty snapshot (zero index, empty catalog).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct from a typed catalog + log index.
    #[must_use]
    pub fn from_catalog(catalog: ResourceCatalogSnapshot, last_applied_index: u64) -> Self {
        Self {
            catalog,
            last_applied_index,
        }
    }

    /// Serialize to the disk-format bytes (header + len + hash + JSON).
    /// Pure — no I/O. Test helper.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Encode`] if the catalog can't be
    /// serialized (should never happen for valid input).
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        let payload = serde_json::to_vec(self)
            .map_err(|e| SnapshotError::Encode(e.to_string()))?;
        let mut out = Vec::with_capacity(
            MAGIC_V1.len() + 8 + blake3::OUT_LEN + payload.len(),
        );
        out.extend_from_slice(MAGIC_V1);
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(blake3::hash(&payload).as_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Decode disk-format bytes back into a typed snapshot.
    ///
    /// # Errors
    ///
    /// - [`SnapshotError::BadMagic`] if the magic header doesn't
    ///   match (file is wrong format / version mismatch).
    /// - [`SnapshotError::Truncated`] if the file is shorter than
    ///   the declared payload length.
    /// - [`SnapshotError::HashMismatch`] if BLAKE3 of payload
    ///   doesn't match the recorded hash (corruption detected).
    /// - [`SnapshotError::Decode`] if JSON parse fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        if bytes.len() < MAGIC_V1.len() + 8 + blake3::OUT_LEN {
            return Err(SnapshotError::Truncated);
        }
        if &bytes[..MAGIC_V1.len()] != MAGIC_V1 {
            return Err(SnapshotError::BadMagic);
        }
        let len_offset = MAGIC_V1.len();
        let hash_offset = len_offset + 8;
        let payload_offset = hash_offset + blake3::OUT_LEN;
        let payload_len = u64::from_le_bytes(
            bytes[len_offset..hash_offset]
                .try_into()
                .map_err(|_| SnapshotError::Truncated)?,
        ) as usize;
        if bytes.len() < payload_offset + payload_len {
            return Err(SnapshotError::Truncated);
        }
        let stored_hash = &bytes[hash_offset..payload_offset];
        let payload = &bytes[payload_offset..payload_offset + payload_len];
        let actual_hash = blake3::hash(payload);
        if actual_hash.as_bytes() != stored_hash {
            return Err(SnapshotError::HashMismatch);
        }
        serde_json::from_slice(payload).map_err(|e| SnapshotError::Decode(e.to_string()))
    }

    /// Atomic write to `path`. Goes to `{path}.tmp`, fsync, rename.
    /// Mirrors kasou's machine-identifier persistence pattern.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] on filesystem error or
    /// [`SnapshotError::Encode`] on serialization failure.
    pub fn save_to(&self, path: &Path) -> Result<(), SnapshotError> {
        use std::io::Write;
        let bytes = self.encode()?;

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    SnapshotError::Io(format!("mkdir {}: {e}", parent.display()))
                })?;
            }
        }
        let tmp = path.with_extension("snap.tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| {
                SnapshotError::Io(format!("create {}: {e}", tmp.display()))
            })?;
            f.write_all(&bytes).map_err(|e| {
                SnapshotError::Io(format!("write {}: {e}", tmp.display()))
            })?;
            f.sync_all().map_err(|e| {
                SnapshotError::Io(format!("fsync {}: {e}", tmp.display()))
            })?;
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            SnapshotError::Io(format!(
                "rename {} → {}: {e}",
                tmp.display(),
                path.display()
            ))
        })?;
        Ok(())
    }

    /// Load + decode from `path`.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] if the path doesn't exist
    /// or the file can't be read. Decoding errors propagate from
    /// [`Self::decode`].
    pub fn load_from(path: &Path) -> Result<Self, SnapshotError> {
        let bytes = std::fs::read(path).map_err(|e| {
            SnapshotError::Io(format!("read {}: {e}", path.display()))
        })?;
        Self::decode(&bytes)
    }
}

/// Snapshot persistence errors. Stable `.kind()` for telemetry.
#[derive(Debug, Clone, Error)]
pub enum SnapshotError {
    /// Filesystem I/O failure.
    #[error("io: {0}")]
    Io(String),

    /// File magic header didn't match — wrong format/version.
    #[error("bad magic header (wrong format or unsupported version)")]
    BadMagic,

    /// File is shorter than declared payload length.
    #[error("file truncated")]
    Truncated,

    /// BLAKE3 hash of payload didn't match the recorded hash.
    #[error("hash mismatch — snapshot is corrupt")]
    HashMismatch,

    /// JSON encode failed.
    #[error("encode: {0}")]
    Encode(String),

    /// JSON decode failed.
    #[error("decode: {0}")]
    Decode(String),
}

engenho_substrate::impl_error_kind! {
    SnapshotError {
        (Io(_)) => "io",
        BadMagic => "bad_magic",
        Truncated => "truncated",
        HashMismatch => "hash_mismatch",
        (Encode(_)) => "encode",
        (Decode(_)) => "decode",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{Reason, ResourceCommand};
    use crate::resource::ResourceKey;
    use crate::state::ResourceCatalog;
    use serde_json::json;

    fn sample_catalog() -> ResourceCatalogSnapshot {
        let mut catalog = ResourceCatalog::default();
        catalog.apply(
            &ResourceCommand::Put {
                key: ResourceKey::namespaced("", "v1", "Pod", "default", "p1"),
                value: json!({"spec": {"containers": [{"image": "podinfo:6"}]}}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        catalog.apply(
            &ResourceCommand::Put {
                key: ResourceKey::namespaced("", "v1", "Pod", "default", "p2"),
                value: json!({"spec": {"containers": [{"image": "nginx:latest"}]}}),
                reason: Reason::Operator,
            },
            1,
            2,
        );
        ResourceCatalogSnapshot { catalog }
    }

    #[test]
    fn snapshot_round_trips_through_bytes() {
        let original = CatalogSnapshot::from_catalog(sample_catalog(), 42);
        let encoded = original.encode().unwrap();
        let back = CatalogSnapshot::decode(&encoded).unwrap();
        assert_eq!(back.last_applied_index, 42);
        // Sanity: both contain p1 and p2 with their images.
        let p1 = back
            .catalog
            .catalog
            .get(&ResourceKey::namespaced("", "v1", "Pod", "default", "p1"))
            .unwrap();
        assert_eq!(
            p1.get("spec").unwrap().get("containers").unwrap()[0]
                .get("image")
                .unwrap(),
            "podinfo:6"
        );
    }

    #[test]
    fn snapshot_magic_header_present() {
        let snap = CatalogSnapshot::from_catalog(sample_catalog(), 1);
        let bytes = snap.encode().unwrap();
        assert!(bytes.starts_with(MAGIC_V1));
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bad = MAGIC_V1.to_vec();
        bad[0] = b'X';
        bad.extend_from_slice(&[0u8; 40]); // length + hash
        let err = CatalogSnapshot::decode(&bad).unwrap_err();
        assert_eq!(err.kind(), "bad_magic");
    }

    #[test]
    fn decode_rejects_truncated() {
        let snap = CatalogSnapshot::from_catalog(sample_catalog(), 1);
        let bytes = snap.encode().unwrap();
        let truncated = &bytes[..bytes.len() / 2];
        let err = CatalogSnapshot::decode(truncated).unwrap_err();
        assert_eq!(err.kind(), "truncated");
    }

    #[test]
    fn decode_rejects_hash_mismatch() {
        let snap = CatalogSnapshot::from_catalog(sample_catalog(), 1);
        let mut bytes = snap.encode().unwrap();
        // Corrupt one byte of the payload (after magic + len + hash).
        let payload_offset = MAGIC_V1.len() + 8 + blake3::OUT_LEN;
        bytes[payload_offset + 5] ^= 0xff;
        let err = CatalogSnapshot::decode(&bytes).unwrap_err();
        assert_eq!(err.kind(), "hash_mismatch");
    }

    #[test]
    fn save_load_round_trip_on_disk() {
        let dir = std::env::temp_dir().join(format!(
            "engenho-snap-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("catalog.snap");
        let _ = std::fs::remove_file(&path);

        let snap = CatalogSnapshot::from_catalog(sample_catalog(), 7);
        snap.save_to(&path).unwrap();
        assert!(path.exists());

        let back = CatalogSnapshot::load_from(&path).unwrap();
        assert_eq!(back.last_applied_index, 7);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn save_creates_parent_dir() {
        let nested = std::env::temp_dir().join(format!(
            "engenho-snap-nested-{}/sub/dir/catalog.snap",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(nested.parent().unwrap());
        let snap = CatalogSnapshot::new();
        snap.save_to(&nested).unwrap();
        assert!(nested.exists());
        let _ = std::fs::remove_dir_all(nested.parent().unwrap().parent().unwrap().parent().unwrap());
    }

    #[test]
    fn load_from_missing_file_returns_io_error() {
        let path = std::env::temp_dir().join("definitely-does-not-exist.snap");
        let _ = std::fs::remove_file(&path);
        let err = CatalogSnapshot::load_from(&path).unwrap_err();
        assert_eq!(err.kind(), "io");
    }

    #[test]
    fn empty_catalog_snapshot_round_trips() {
        let snap = CatalogSnapshot::new();
        let bytes = snap.encode().unwrap();
        let back = CatalogSnapshot::decode(&bytes).unwrap();
        assert_eq!(back.last_applied_index, 0);
    }

    #[test]
    fn save_to_is_atomic_via_temp_rename() {
        // Verifies the tmp file is gone after a successful save —
        // proves the rename completed.
        let dir = std::env::temp_dir().join(format!(
            "engenho-snap-atomic-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("catalog.snap");
        let tmp = path.with_extension("snap.tmp");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&tmp);

        let snap = CatalogSnapshot::from_catalog(sample_catalog(), 3);
        snap.save_to(&path).unwrap();

        assert!(path.exists(), "final path exists");
        assert!(!tmp.exists(), "tmp was renamed (not lingering)");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(SnapshotError::Io("x".into()).kind(), "io");
        assert_eq!(SnapshotError::BadMagic.kind(), "bad_magic");
        assert_eq!(SnapshotError::Truncated.kind(), "truncated");
        assert_eq!(SnapshotError::HashMismatch.kind(), "hash_mismatch");
        assert_eq!(SnapshotError::Encode("x".into()).kind(), "encode");
        assert_eq!(SnapshotError::Decode("x".into()).kind(), "decode");
    }
}
