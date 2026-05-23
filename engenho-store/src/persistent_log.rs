//! C4b — append-only persistent log primitive.
//!
//! Sits next to C4's [`CatalogSnapshot`] but for the Raft *log*
//! rather than the state machine. Operators can replay the log
//! on restart to rebuild any state the snapshot doesn't carry.
//!
//! ## Scope (C4b vs full openraft persistent log)
//!
//! This module ships the **typed log file primitive** + a
//! `PersistentLog` API that any caller (including a future
//! openraft `RaftLogStorage` impl) can build on. Wiring openraft
//! itself onto this is C4c — substantial follow-up.
//!
//! ## File format — one entry per record
//!
//! ```text
//! "engenho-persistent-log v1\n"   (26-byte magic, written once)
//! repeat:
//!   <u64 LE>       (entry index)
//!   <u64 LE>       (payload length)
//!   <32 bytes>     (BLAKE3 hash of payload)
//!   <payload>      (serde-encoded bytes)
//! ```
//!
//! Records append-only. On `replay`, we read the magic, then
//! repeatedly read (index, len, hash, payload) until EOF; any
//! partial trailing record is truncated (a crash mid-append).
//!
//! ## Why not just use openraft's storage trait directly
//!
//! C4c wires this. C4b ships a SIMPLER consumer-grade log that
//! doesn't need to implement the full openraft RaftLogStorage
//! trait (vote, last_purged, snapshot machinery). Use cases:
//!   * audit log replay
//!   * controller event sourcing
//!   * recovery-from-snapshot supplemental log

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

const MAGIC_V1: &[u8] = b"engenho-persistent-log v1\n";

/// Errors at append/replay/I/O time.
#[derive(Debug, Clone, Error)]
pub enum PersistentLogError {
    /// Filesystem I/O failure.
    #[error("io: {0}")]
    Io(String),
    /// File magic header didn't match (wrong format / version).
    #[error("bad magic header")]
    BadMagic,
    /// Per-entry BLAKE3 hash mismatch — entry is corrupt.
    #[error("entry hash mismatch at index {index}")]
    HashMismatch {
        /// The entry index that failed verification.
        index: u64,
    },
    /// JSON serialize failed.
    #[error("encode: {0}")]
    Encode(String),
    /// JSON deserialize failed.
    #[error("decode at index {index}: {detail}")]
    Decode {
        /// The entry index whose payload couldn't be decoded.
        index: u64,
        /// Underlying error message.
        detail: String,
    },
}

engenho_substrate::impl_error_kind! {
    PersistentLogError {
        (Io(_)) => "io",
        BadMagic => "bad_magic",
        { HashMismatch { .. } } => "hash_mismatch",
        (Encode(_)) => "encode",
        { Decode { .. } } => "decode",
    }
}

/// Append-only persistent log keyed by `u64` index.
#[derive(Debug)]
pub struct PersistentLog {
    path: PathBuf,
}

impl PersistentLog {
    /// Open or create a log at `path`. Creates the magic header
    /// if the file doesn't yet exist.
    ///
    /// # Errors
    ///
    /// Returns [`PersistentLogError::Io`] on filesystem failure.
    /// Returns [`PersistentLogError::BadMagic`] if an existing
    /// file's magic doesn't match.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, PersistentLogError> {
        let path: PathBuf = path.into();
        if path.exists() {
            // Verify magic.
            let mut f = std::fs::File::open(&path)
                .map_err(|e| PersistentLogError::Io(format!("open {}: {e}", path.display())))?;
            let mut header = vec![0u8; MAGIC_V1.len()];
            f.read_exact(&mut header).map_err(|e| {
                PersistentLogError::Io(format!("read magic from {}: {e}", path.display()))
            })?;
            if header != MAGIC_V1 {
                return Err(PersistentLogError::BadMagic);
            }
        } else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        PersistentLogError::Io(format!("mkdir {}: {e}", parent.display()))
                    })?;
                }
            }
            let mut f = std::fs::File::create(&path)
                .map_err(|e| PersistentLogError::Io(format!("create {}: {e}", path.display())))?;
            f.write_all(MAGIC_V1)
                .map_err(|e| PersistentLogError::Io(format!("write magic: {e}")))?;
            f.sync_all()
                .map_err(|e| PersistentLogError::Io(format!("fsync magic: {e}")))?;
        }
        Ok(Self { path })
    }

    /// Append one entry. Each call fsyncs to ensure crash safety.
    ///
    /// # Errors
    ///
    /// [`PersistentLogError::Encode`] if serialization fails;
    /// [`PersistentLogError::Io`] on filesystem failure.
    pub fn append<T: Serialize>(&self, index: u64, entry: &T) -> Result<(), PersistentLogError> {
        let payload =
            serde_json::to_vec(entry).map_err(|e| PersistentLogError::Encode(e.to_string()))?;
        let hash = blake3::hash(&payload);

        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                PersistentLogError::Io(format!("open append {}: {e}", self.path.display()))
            })?;
        f.write_all(&index.to_le_bytes())
            .map_err(|e| PersistentLogError::Io(format!("write index: {e}")))?;
        f.write_all(&(payload.len() as u64).to_le_bytes())
            .map_err(|e| PersistentLogError::Io(format!("write len: {e}")))?;
        f.write_all(hash.as_bytes())
            .map_err(|e| PersistentLogError::Io(format!("write hash: {e}")))?;
        f.write_all(&payload)
            .map_err(|e| PersistentLogError::Io(format!("write payload: {e}")))?;
        f.sync_all()
            .map_err(|e| PersistentLogError::Io(format!("fsync append: {e}")))?;
        Ok(())
    }

    /// Replay the log from the magic header onward, yielding
    /// every (index, decoded entry) in commit order. Truncates a
    /// partial trailing entry (a crash mid-append leaves at most
    /// one incomplete record — we ignore it).
    ///
    /// # Errors
    ///
    /// [`PersistentLogError::Io`] on filesystem failure.
    /// [`PersistentLogError::HashMismatch`] if any entry's hash
    /// doesn't match its payload.
    /// [`PersistentLogError::Decode`] if any payload can't deserialize.
    pub fn replay<T: DeserializeOwned>(&self) -> Result<Vec<(u64, T)>, PersistentLogError> {
        let mut f = std::fs::File::open(&self.path).map_err(|e| {
            PersistentLogError::Io(format!("open replay {}: {e}", self.path.display()))
        })?;

        // Read + verify magic.
        let mut header = vec![0u8; MAGIC_V1.len()];
        f.read_exact(&mut header)
            .map_err(|e| PersistentLogError::Io(format!("read magic: {e}")))?;
        if header != MAGIC_V1 {
            return Err(PersistentLogError::BadMagic);
        }

        let mut entries = Vec::new();
        loop {
            let mut idx_buf = [0u8; 8];
            match f.read_exact(&mut idx_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(PersistentLogError::Io(e.to_string())),
            }
            let index = u64::from_le_bytes(idx_buf);

            let mut len_buf = [0u8; 8];
            match f.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // partial
                Err(e) => return Err(PersistentLogError::Io(e.to_string())),
            }
            let payload_len = u64::from_le_bytes(len_buf) as usize;

            let mut hash_buf = [0u8; blake3::OUT_LEN];
            match f.read_exact(&mut hash_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(PersistentLogError::Io(e.to_string())),
            }

            let mut payload = vec![0u8; payload_len];
            match f.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(PersistentLogError::Io(e.to_string())),
            }

            if blake3::hash(&payload).as_bytes() != &hash_buf {
                return Err(PersistentLogError::HashMismatch { index });
            }

            let decoded: T =
                serde_json::from_slice(&payload).map_err(|e| PersistentLogError::Decode {
                    index,
                    detail: e.to_string(),
                })?;
            entries.push((index, decoded));
        }
        Ok(entries)
    }

    /// Path the log writes to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Entry {
        cmd: String,
        n: u32,
    }

    fn temp_path(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("engenho-plog-{}-{suffix}.log", std::process::id()))
    }

    #[test]
    fn open_creates_file_with_magic() {
        let path = temp_path("create");
        let _ = std::fs::remove_file(&path);
        let _log = PersistentLog::open(&path).unwrap();
        assert!(path.exists());
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(MAGIC_V1));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_and_replay_round_trip() {
        let path = temp_path("append");
        let _ = std::fs::remove_file(&path);
        let log = PersistentLog::open(&path).unwrap();
        log.append(
            1,
            &Entry {
                cmd: "put".into(),
                n: 1,
            },
        )
        .unwrap();
        log.append(
            2,
            &Entry {
                cmd: "patch".into(),
                n: 2,
            },
        )
        .unwrap();
        log.append(
            3,
            &Entry {
                cmd: "delete".into(),
                n: 3,
            },
        )
        .unwrap();

        let entries: Vec<(u64, Entry)> = log.replay().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[0].1.cmd, "put");
        assert_eq!(entries[2].1.n, 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_preserves_existing_entries() {
        let path = temp_path("reopen");
        let _ = std::fs::remove_file(&path);
        {
            let log = PersistentLog::open(&path).unwrap();
            log.append(
                10,
                &Entry {
                    cmd: "x".into(),
                    n: 99,
                },
            )
            .unwrap();
            log.append(
                11,
                &Entry {
                    cmd: "y".into(),
                    n: 100,
                },
            )
            .unwrap();
        }
        // Drop the first log; reopen.
        let log2 = PersistentLog::open(&path).unwrap();
        let entries: Vec<(u64, Entry)> = log2.replay().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].1.n, 100);

        // Continue appending after replay.
        log2.append(
            12,
            &Entry {
                cmd: "z".into(),
                n: 101,
            },
        )
        .unwrap();
        let entries: Vec<(u64, Entry)> = log2.replay().unwrap();
        assert_eq!(entries.len(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = temp_path("badmagic");
        // Must be >= MAGIC_V1 length so we get past read_exact +
        // into the bad_magic comparison (shorter reads return Io
        // because read_exact fails first).
        let mut bad = vec![0u8; MAGIC_V1.len()];
        bad.iter_mut().for_each(|b| *b = b'X');
        std::fs::write(&path, &bad).unwrap();
        let err = PersistentLog::open(&path).unwrap_err();
        assert_eq!(err.kind(), "bad_magic");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replay_detects_hash_corruption() {
        let path = temp_path("corrupt");
        let _ = std::fs::remove_file(&path);
        let log = PersistentLog::open(&path).unwrap();
        log.append(
            1,
            &Entry {
                cmd: "good".into(),
                n: 1,
            },
        )
        .unwrap();

        // Corrupt one byte of the payload area.
        let mut bytes = std::fs::read(&path).unwrap();
        // Magic + 8 (index) + 8 (len) + 32 (hash) = first payload byte
        let payload_offset = MAGIC_V1.len() + 8 + 8 + 32;
        bytes[payload_offset] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let result: Result<Vec<(u64, Entry)>, _> = log.replay();
        assert!(matches!(
            result.unwrap_err(),
            PersistentLogError::HashMismatch { index: 1 }
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replay_truncates_partial_trailing_entry() {
        let path = temp_path("partial");
        let _ = std::fs::remove_file(&path);
        let log = PersistentLog::open(&path).unwrap();
        log.append(
            1,
            &Entry {
                cmd: "complete".into(),
                n: 1,
            },
        )
        .unwrap();
        log.append(
            2,
            &Entry {
                cmd: "complete".into(),
                n: 2,
            },
        )
        .unwrap();

        // Truncate the last byte to simulate a crash mid-append.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.pop();
        std::fs::write(&path, &bytes).unwrap();

        let entries: Vec<(u64, Entry)> = log.replay().unwrap();
        assert_eq!(entries.len(), 1, "partial 2nd entry truncated");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_log_replays_to_empty_vec() {
        let path = temp_path("empty");
        let _ = std::fs::remove_file(&path);
        let log = PersistentLog::open(&path).unwrap();
        let entries: Vec<(u64, Entry)> = log.replay().unwrap();
        assert!(entries.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_creates_parent_dir() {
        let nested = temp_path("nested-parent").join("a/b/c.log");
        let _ = std::fs::remove_dir_all(nested.parent().unwrap());
        let _log = PersistentLog::open(&nested).unwrap();
        assert!(nested.exists());
        let _ = std::fs::remove_dir_all(temp_path("nested-parent"));
    }

    #[test]
    fn error_kinds_are_stable() {
        assert_eq!(PersistentLogError::Io("x".into()).kind(), "io");
        assert_eq!(PersistentLogError::BadMagic.kind(), "bad_magic");
        assert_eq!(
            PersistentLogError::HashMismatch { index: 1 }.kind(),
            "hash_mismatch"
        );
        assert_eq!(PersistentLogError::Encode("x".into()).kind(), "encode");
        assert_eq!(
            PersistentLogError::Decode {
                index: 1,
                detail: "x".into()
            }
            .kind(),
            "decode"
        );
    }
}
