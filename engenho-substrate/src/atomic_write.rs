//! Atomic file write — tmp + fsync + rename.
//!
//! Same pattern used at:
//!   * `kasou::config::load_or_create_machine_identifier`
//!   * `engenho_store::persistence::CatalogSnapshot::save_to`
//!   * `tend` daemon state writer (planned migration)
//!
//! Three sites = extraction trigger per the prime directive.

use std::io::Write;
use std::path::Path;

use thiserror::Error;

/// Atomic-write errors. Stable `.kind()` for telemetry —
/// generated via [`crate::impl_error_kind!`] per the PRIME DIRECTIVE.
#[derive(Debug, Clone, Error)]
pub enum AtomicWriteError {
    /// Filesystem I/O failure.
    #[error("io: {0}")]
    Io(String),
}

crate::impl_error_kind! {
    AtomicWriteError {
        (Io(_)) => "io",
    }
}

/// Write `bytes` to `path` atomically — survives crash mid-write.
///
/// Algorithm:
///   1. `mkdir -p` the parent directory if missing
///   2. Write to `{path}.tmp` (overwrites any existing tmp file)
///   3. `file.sync_all()` to flush the fsdata to disk
///   4. `rename({path}.tmp, path)` — atomic on POSIX
///
/// A crash before step 4 leaves the canonical path unchanged; a
/// crash after step 4 leaves the new bytes in place.
///
/// # Errors
///
/// Returns [`AtomicWriteError::Io`] for any filesystem failure
/// (permission denied, ENOSPC, etc).
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| AtomicWriteError::Io(format!("mkdir {}: {e}", parent.display())))?;
        }
    }
    let tmp = tmp_path_for(path);
    {
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| AtomicWriteError::Io(format!("create {}: {e}", tmp.display())))?;
        f.write_all(bytes)
            .map_err(|e| AtomicWriteError::Io(format!("write {}: {e}", tmp.display())))?;
        f.sync_all()
            .map_err(|e| AtomicWriteError::Io(format!("fsync {}: {e}", tmp.display())))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        AtomicWriteError::Io(format!(
            "rename {} → {}: {e}",
            tmp.display(),
            path.display()
        ))
    })
}

/// Build the temp file path for an atomic write. Exposed for
/// callers that need to know where the tmp ends up (e.g. cleanup
/// after a crashed previous run).
#[must_use]
pub fn tmp_path_for(path: &Path) -> std::path::PathBuf {
    let mut s: std::ffi::OsString = path.as_os_str().to_os_string();
    s.push(".tmp");
    s.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "engenho-substrate-atomic-{}-{suffix}",
            std::process::id()
        ))
    }

    #[test]
    fn writes_bytes_to_path() {
        let path = temp_path("basic");
        let _ = std::fs::remove_file(&path);
        write_atomic(&path, b"hello").unwrap();
        let read = std::fs::read(&path).unwrap();
        assert_eq!(read, b"hello");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn creates_parent_directory_if_missing() {
        let nested = temp_path("nested").join("a/b/c/file.bin");
        let _ = std::fs::remove_dir_all(nested.parent().unwrap());
        write_atomic(&nested, b"abc").unwrap();
        assert!(nested.exists());
        let _ = std::fs::remove_dir_all(temp_path("nested"));
    }

    #[test]
    fn overwrites_existing_file_atomically() {
        let path = temp_path("overwrite");
        write_atomic(&path, b"v1").unwrap();
        write_atomic(&path, b"v2").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"v2");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cleans_up_tmp_after_successful_rename() {
        let path = temp_path("tmpcleanup");
        let tmp = tmp_path_for(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&tmp);
        write_atomic(&path, b"x").unwrap();
        assert!(path.exists(), "final path exists");
        assert!(!tmp.exists(), "tmp gone after rename");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tmp_path_for_appends_dot_tmp() {
        let p = std::path::PathBuf::from("/tmp/x.bin");
        assert_eq!(tmp_path_for(&p), std::path::PathBuf::from("/tmp/x.bin.tmp"));
    }

    #[test]
    fn error_kind_is_stable() {
        assert_eq!(AtomicWriteError::Io("x".into()).kind(), "io");
    }

    #[test]
    fn write_to_directory_returns_io_error() {
        // Try to atomic_write to /tmp itself (a directory).
        let dir = std::env::temp_dir();
        let err = write_atomic(&dir, b"x").unwrap_err();
        assert_eq!(err.kind(), "io");
    }
}
