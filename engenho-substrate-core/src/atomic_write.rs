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
    write_atomic_with(path, bytes, None)
}

/// [`write_atomic`], the file carrying exactly `mode` (Unix permission bits)
/// from the moment it exists: the temp file is created at `mode`, before any
/// byte is in it, so a private file is never briefly readable at the umask's
/// default. Ignored where there are no Unix permissions.
///
/// # Errors
///
/// As [`write_atomic`].
pub fn write_atomic_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<(), AtomicWriteError> {
    write_atomic_with(path, bytes, Some(mode))
}

fn write_atomic_with(path: &Path, bytes: &[u8], mode: Option<u32>) -> Result<(), AtomicWriteError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| AtomicWriteError::Io(format!("mkdir {}: {e}", parent.display())))?;
    }
    let tmp = TempPath::mint(path);
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(mode);
        }
        let mut f = options
            .open(tmp.path())
            .map_err(|e| AtomicWriteError::Io(format!("create {}: {e}", tmp.path().display())))?;
        // The umask may have narrowed it; `mode` is what was asked for.
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt;
            f.set_permissions(std::fs::Permissions::from_mode(mode))
                .map_err(|e| {
                    AtomicWriteError::Io(format!("chmod {}: {e}", tmp.path().display()))
                })?;
        }
        #[cfg(not(unix))]
        let _ = mode;
        f.write_all(bytes)
            .map_err(|e| AtomicWriteError::Io(format!("write {}: {e}", tmp.path().display())))?;
        f.sync_all()
            .map_err(|e| AtomicWriteError::Io(format!("fsync {}: {e}", tmp.path().display())))?;
    }
    // Consumes `tmp`: a second publish is a MOVE ERROR, and an early return
    // above drops it, removing the stray file.
    tmp.publish(path)
}

/// A temp file that exactly ONE writer can name, publish at most once, and
/// never leave behind.
///
/// ## The corruption this makes unrepresentable
///
/// `tmp_path_for` used to be `pub` and purely deterministic (`<path>.tmp`), so
/// any two writers of the same target shared one temp file. Measured on ryn
/// 2026-09-17, two concurrent reconciles of one pod:
///
/// ```text
///   A: create <p>.tmp, write, fsync
///   B: create <p>.tmp          ← truncates A's file
///   A: rename <p>.tmp → <p>    ← succeeds; the temp file is now gone
///   B: rename <p>.tmp → <p>    ← ENOENT, pod stuck Pending
/// ```
///
/// ENOENT was the LUCKY interleaving. The dangerous one is B renaming a file
/// A had only partially written — an atomic-write helper publishing a torn
/// file, silently, which is the exact failure it exists to prevent.
///
/// Three invariants, each carried by the type rather than by care:
///
/// 1. **No caller can name it.** The field is private and `mint` is private to
///    this module, so there is no way to construct a path a second writer
///    might also be using. Nothing outside can rename a temp file at all.
/// 2. **It publishes at most once.** [`publish`](Self::publish) takes `self`
///    BY VALUE, so a second publish of the same temp is a move error, not a
///    second rename racing the first.
/// 3. **It cannot be left behind.** `Drop` removes an unpublished temp, so an
///    error path between mint and publish leaks nothing.
#[derive(Debug)]
pub struct TempPath {
    path: std::path::PathBuf,
    published: bool,
}

impl TempPath {
    /// Mint a temp path beside `target`, unique across processes and across
    /// concurrent calls within one.
    ///
    /// Beside, not in a temp dir: the rename must stay on ONE filesystem to be
    /// atomic, and a temp dir elsewhere would risk `EXDEV`.
    fn mint(target: &Path) -> Self {
        use std::fmt::Write as _;
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut suffix = String::new();
        // Writing to a String is infallible; the Result is discarded knowingly.
        let _ = write!(suffix, ".{}.{seq}.tmp", std::process::id());

        let mut s: std::ffi::OsString = target.as_os_str().to_os_string();
        s.push(suffix);
        Self {
            path: s.into(),
            published: false,
        }
    }

    /// The path to write bytes into before publishing.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Atomically publish this temp file as `target`, consuming it.
    ///
    /// # Errors
    /// [`AtomicWriteError::Io`] if the rename fails.
    pub fn publish(mut self, target: &Path) -> Result<(), AtomicWriteError> {
        std::fs::rename(&self.path, target).map_err(|e| {
            AtomicWriteError::Io(
                [
                    "rename ",
                    &self.path.display().to_string(),
                    " → ",
                    &target.display().to_string(),
                    ": ",
                    &e.to_string(),
                ]
                .concat(),
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if !self.published {
            // Best-effort: an error path between mint and publish must not
            // leave a stray temp file beside the target.
            let _ = std::fs::remove_file(&self.path);
        }
    }
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
        let _ = std::fs::remove_file(&path);
        write_atomic(&path, b"x").unwrap();
        assert!(path.exists(), "final path exists");

        // A temp name is no longer guessable, so assert the STRONGER thing:
        // no sibling temp of this target survives, whatever it was called.
        let dir = path.parent().expect("parent");
        let stem = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .into_owned();
        let strays: Vec<_> = std::fs::read_dir(dir)
            .expect("read_dir")
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(&stem) && n.ends_with(".tmp"))
            .collect();
        assert!(strays.is_empty(), "temp files left behind: {strays:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// ★ An UNPUBLISHED temp removes itself, so no error path leaks one.
    #[test]
    fn an_unpublished_temp_file_is_cleaned_up_on_drop() {
        let target = temp_path("dropme");
        let stray = {
            let t = TempPath::mint(&target);
            std::fs::write(t.path(), b"partial").expect("write");
            assert!(t.path().exists());
            t.path().to_path_buf()
            // `t` drops here without publish
        };
        assert!(
            !stray.exists(),
            "a temp that was never published must not survive its owner"
        );
    }

    /// ```compile_fail,E0382
    /// use engenho_substrate::TempPath;
    /// use std::path::Path;
    /// // `publish` takes self BY VALUE, so publishing twice cannot compile —
    /// // which is what makes "two renames racing for one temp" unrepresentable
    /// // rather than merely unlikely. (TempPath::mint is private; this snippet
    /// // fails to build for that reason too, which is itself the first
    /// // invariant: no caller can name a temp path at all.)
    /// let t = TempPath::mint(Path::new("/tmp/x"));
    /// t.publish(Path::new("/tmp/x")).unwrap();
    /// t.publish(Path::new("/tmp/x")).unwrap();
    /// ```
    const _PUBLISH_CONSUMES: () = ();

    /// ★ THE REGRESSION. Two calls must never collide: a shared temp name is
    /// how one writer renames another's partially-written file.
    #[test]
    fn two_calls_never_return_the_same_temp_path() {
        let p = std::path::PathBuf::from("/tmp/x.bin");
        let a = TempPath::mint(&p);
        let b = TempPath::mint(&p);
        assert_ne!(
            a.path(),
            b.path(),
            "a deterministic temp path is a lost-update race"
        );
    }

    /// Concurrent writers to the SAME path must all succeed. With the old
    /// `<path>.tmp` this failed with ENOENT on rename.
    #[test]
    fn concurrent_writers_to_one_path_all_succeed() {
        let path = temp_path("concurrent");
        let _ = std::fs::remove_file(&path);
        let mut handles = Vec::new();
        for i in 0..16u8 {
            let p = path.clone();
            handles.push(std::thread::spawn(move || write_atomic(&p, &[i; 64])));
        }
        for h in handles {
            h.join()
                .expect("thread")
                .expect("every atomic write must succeed");
        }
        // Whoever won, the file must be WHOLE — never a torn mixture.
        let got = std::fs::read(&path).expect("read back");
        assert_eq!(got.len(), 64, "a torn write would not be 64 bytes");
        assert!(
            got.iter().all(|b| *b == got[0]),
            "the published file must be exactly ONE writer's bytes, not a mix"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tmp_path_for_is_a_sibling_ending_in_dot_tmp() {
        let p = std::path::PathBuf::from("/tmp/x.bin");
        let minted = TempPath::mint(&p);
        let t = minted.path().to_path_buf();
        // Same DIRECTORY, so the rename stays on one filesystem and therefore
        // atomic; a temp dir elsewhere would risk EXDEV.
        assert_eq!(t.parent(), p.parent());
        let name = t.to_string_lossy().into_owned();
        assert!(name.starts_with("/tmp/x.bin."), "{name}");
        assert!(name.ends_with(".tmp"), "{name}");
    }

    #[cfg(unix)]
    #[test]
    fn a_mode_is_the_files_from_the_start() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_path("mode");
        let _ = std::fs::remove_file(&path);
        write_atomic_mode(&path, b"secret", 0o600).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        // Rewriting keeps it: the replacement is created at the mode too.
        write_atomic_mode(&path, b"secret2", 0o600).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"secret2");
        let _ = std::fs::remove_file(&path);
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
