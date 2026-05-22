//! Magic-headered, BLAKE3-hashed blob format.
//!
//! Reusable disk-artifact wrapper for any serde-serializable value
//! that needs:
//!   * Versioning via magic header (e.g. `"engenho-foo v1\n"`)
//!   * Corruption detection via BLAKE3 hash
//!   * Atomic write via [`crate::atomic_write::write_atomic`]
//!
//! Extracted from `engenho_store::persistence::CatalogSnapshot`
//! (v0.20.0) so future consumers (audit chain dumps, attestation
//! receipts, federation sync packs) reach one canonical encoding.

use std::path::Path;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::atomic_write::{write_atomic, AtomicWriteError};

/// Errors at encode / decode / I/O time.
#[derive(Debug, Clone, Error)]
pub enum MagicBlobError {
    /// File magic header didn't match (wrong format / version).
    #[error("bad magic header (wrong format or unsupported version)")]
    BadMagic,

    /// File is shorter than declared payload length.
    #[error("file truncated")]
    Truncated,

    /// BLAKE3 hash of payload didn't match recorded hash.
    #[error("hash mismatch — blob is corrupt")]
    HashMismatch,

    /// JSON encode failed.
    #[error("encode: {0}")]
    Encode(String),

    /// JSON decode failed.
    #[error("decode: {0}")]
    Decode(String),

    /// Atomic-write or read I/O failure.
    #[error("io: {0}")]
    Io(String),
}

impl MagicBlobError {
    /// Stable identifier for telemetry / SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::BadMagic => "bad_magic",
            Self::Truncated => "truncated",
            Self::HashMismatch => "hash_mismatch",
            Self::Encode(_) => "encode",
            Self::Decode(_) => "decode",
            Self::Io(_) => "io",
        }
    }
}

impl From<AtomicWriteError> for MagicBlobError {
    fn from(e: AtomicWriteError) -> Self {
        Self::Io(e.to_string())
    }
}

/// Generic typed blob with a magic-versioned header + BLAKE3 hash.
///
/// `MAGIC` is a const byte string identifying both format AND
/// version (e.g. `b"engenho-catalog-snapshot v1\n"`). Bumping the
/// version means picking a new magic string; old loaders reject
/// new files via `BadMagic`.
pub struct MagicBlob<'m, T> {
    /// Magic header bytes — typically a const str literal like
    /// `b"engenho-foo v1\n"`.
    pub magic: &'m [u8],
    /// The payload value.
    pub value: T,
}

impl<'m, T: Serialize + DeserializeOwned> MagicBlob<'m, T> {
    /// Encode to bytes: `magic | u64-le-length | blake3 | json-payload`.
    ///
    /// # Errors
    ///
    /// Returns [`MagicBlobError::Encode`] if serde serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, MagicBlobError> {
        let payload = serde_json::to_vec(&self.value)
            .map_err(|e| MagicBlobError::Encode(e.to_string()))?;
        let mut out =
            Vec::with_capacity(self.magic.len() + 8 + blake3::OUT_LEN + payload.len());
        out.extend_from_slice(self.magic);
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(blake3::hash(&payload).as_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Decode bytes back to a typed value. Verifies magic + hash
    /// before deserializing the payload.
    ///
    /// # Errors
    ///
    /// - [`MagicBlobError::BadMagic`] if magic prefix mismatches.
    /// - [`MagicBlobError::Truncated`] if file shorter than length.
    /// - [`MagicBlobError::HashMismatch`] if BLAKE3 differs.
    /// - [`MagicBlobError::Decode`] if JSON parse fails.
    pub fn decode(magic: &[u8], bytes: &[u8]) -> Result<T, MagicBlobError> {
        if bytes.len() < magic.len() + 8 + blake3::OUT_LEN {
            return Err(MagicBlobError::Truncated);
        }
        if &bytes[..magic.len()] != magic {
            return Err(MagicBlobError::BadMagic);
        }
        let len_offset = magic.len();
        let hash_offset = len_offset + 8;
        let payload_offset = hash_offset + blake3::OUT_LEN;
        let payload_len = u64::from_le_bytes(
            bytes[len_offset..hash_offset]
                .try_into()
                .map_err(|_| MagicBlobError::Truncated)?,
        ) as usize;
        if bytes.len() < payload_offset + payload_len {
            return Err(MagicBlobError::Truncated);
        }
        let stored_hash = &bytes[hash_offset..payload_offset];
        let payload = &bytes[payload_offset..payload_offset + payload_len];
        if blake3::hash(payload).as_bytes() != stored_hash {
            return Err(MagicBlobError::HashMismatch);
        }
        serde_json::from_slice(payload).map_err(|e| MagicBlobError::Decode(e.to_string()))
    }

    /// Atomic-write the encoded blob to `path`.
    ///
    /// # Errors
    ///
    /// Propagates encoding or I/O failures.
    pub fn save_to(&self, path: &Path) -> Result<(), MagicBlobError> {
        let bytes = self.encode()?;
        write_atomic(path, &bytes).map_err(MagicBlobError::from)
    }

    /// Load + decode from `path`.
    ///
    /// # Errors
    ///
    /// Propagates I/O + decode failures.
    pub fn load_from(magic: &[u8], path: &Path) -> Result<T, MagicBlobError> {
        let bytes = std::fs::read(path)
            .map_err(|e| MagicBlobError::Io(format!("read {}: {e}", path.display())))?;
        Self::decode(magic, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    const TEST_MAGIC: &[u8] = b"engenho-substrate-test v1\n";

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Sample {
        n: u32,
        name: String,
    }

    fn sample() -> Sample {
        Sample {
            n: 42,
            name: "rio".into(),
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let blob = MagicBlob {
            magic: TEST_MAGIC,
            value: sample(),
        };
        let bytes = blob.encode().unwrap();
        let back: Sample = MagicBlob::<Sample>::decode(TEST_MAGIC, &bytes).unwrap();
        assert_eq!(back, sample());
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let blob = MagicBlob {
            magic: TEST_MAGIC,
            value: sample(),
        };
        let bytes = blob.encode().unwrap();
        let err = MagicBlob::<Sample>::decode(b"different magic", &bytes).unwrap_err();
        assert_eq!(err.kind(), "bad_magic");
    }

    #[test]
    fn decode_rejects_truncated() {
        let blob = MagicBlob {
            magic: TEST_MAGIC,
            value: sample(),
        };
        let bytes = blob.encode().unwrap();
        let err =
            MagicBlob::<Sample>::decode(TEST_MAGIC, &bytes[..bytes.len() - 5]).unwrap_err();
        assert_eq!(err.kind(), "truncated");
    }

    #[test]
    fn decode_rejects_corrupt_payload() {
        let blob = MagicBlob {
            magic: TEST_MAGIC,
            value: sample(),
        };
        let mut bytes = blob.encode().unwrap();
        let payload_offset = TEST_MAGIC.len() + 8 + blake3::OUT_LEN;
        bytes[payload_offset + 2] ^= 0xff;
        let err = MagicBlob::<Sample>::decode(TEST_MAGIC, &bytes).unwrap_err();
        assert_eq!(err.kind(), "hash_mismatch");
    }

    #[test]
    fn save_load_round_trip_on_disk() {
        let path = std::env::temp_dir().join(format!(
            "engenho-magic-blob-test-{}.bin",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let blob = MagicBlob {
            magic: TEST_MAGIC,
            value: sample(),
        };
        blob.save_to(&path).unwrap();
        let back: Sample = MagicBlob::<Sample>::load_from(TEST_MAGIC, &path).unwrap();
        assert_eq!(back, sample());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(MagicBlobError::BadMagic.kind(), "bad_magic");
        assert_eq!(MagicBlobError::Truncated.kind(), "truncated");
        assert_eq!(MagicBlobError::HashMismatch.kind(), "hash_mismatch");
        assert_eq!(MagicBlobError::Encode("x".into()).kind(), "encode");
        assert_eq!(MagicBlobError::Decode("x".into()).kind(), "decode");
        assert_eq!(MagicBlobError::Io("x".into()).kind(), "io");
    }
}
