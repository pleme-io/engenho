//! Exclusive ownership of a store directory — one process, or none.
//!
//! ## Why this exists (measured, 2026-09-01)
//!
//! A store directory was destroyed on the cid/ryn workstation. The sequence,
//! reconstructed afterwards:
//!
//! 1. a daemon was running and healthy, holding `<data_dir>/store`;
//! 2. a second daemon was started against the SAME `data_dir`;
//! 3. the second one opened the store, seeded ClusterRoles + namespaces, and
//!    only THEN tried to bind the apiserver port — which failed with
//!    `Address already in use`, so it exited;
//! 4. the store had taken interleaved writes from two processes. It reported
//!    `FjallError: Poisoned`, then `FjallError: Storage(Unrecoverable)`, and
//!    never opened again. Dropping the newest journal did not recover it;
//!    dropping every journal did not either — the damage was in the
//!    partitions.
//!
//! Two facts made that possible, and neither is fjall misbehaving:
//!
//! * **fjall does not lock its directory.** 2.11.2 depends on no file-locking
//!   crate, calls no `flock`, writes no lock file, and has no "already open"
//!   error to return. Opening the same directory twice is simply allowed.
//! * **the port bind is far too late to be the guard.** The apiserver bind was
//!   acting as engenho's de-facto single-instance check, but the store is
//!   opened and WRITTEN before it. By the time `Address already in use`
//!   arrives, the corruption has happened.
//!
//! So the guard belongs here — at the store, before the first byte is read —
//! and not at the daemon, which is only one of the openers.
//!
//! ## Why `flock` and not a pidfile
//!
//! A pidfile written with `create_new` fails closed, which is right, but it
//! **outlives the process that wrote it**: a crash leaves a stale file, the
//! next start refuses, and under a supervisor that means a permanent restart
//! loop over a lock nobody holds. `flock` is released by the kernel when the
//! holder dies for ANY reason — signal, panic, `kill -9`, power loss — so a
//! crash is self-healing while a live double-open is still refused.
//!
//! The lock is advisory and per open-file-description, which is exactly the
//! grain wanted: two opens conflict even inside one process, so the invariant
//! is testable without spawning anything.
//!
//! ## What it does NOT protect
//!
//! Two processes on two different hosts sharing the directory over a network
//! filesystem. `flock` semantics over NFS/SMB are the filesystem's business,
//! not ours, and a store on a network mount is outside what engenho supports.
//! Naming the limit rather than implying it is covered.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// Suffix appended to the store path to name its lock file.
///
/// The lock is `<store_path>.lock` — a SIBLING of the store directory, derived
/// from the store's own path.
///
/// ── ★ NOT `<parent>/engenho.lock` ─────────────────────────────────────────
/// The first version of this locked one fixed filename in the store's PARENT
/// directory, which silently made every store under a shared parent contend
/// for one lock. engenho-store's own tests found it immediately: they place
/// each store under `std::env::temp_dir()`, so seven concurrent tests all
/// fought over `$TMPDIR/engenho.lock` and failed. The same flaw in production
/// would make two daemons with genuinely separate data dirs refuse each other
/// whenever those dirs happened to share a parent.
///
/// Deriving from the store path makes the lock's identity exactly the store's
/// identity, which is the thing being protected. It also stays out of the
/// keyspace directory, which fjall owns and enumerates.
pub const LOCK_FILE_SUFFIX: &str = ".lock";

/// The lock file guarding `store_path`.
#[must_use]
pub fn lock_path_for(store_path: &Path) -> PathBuf {
    let mut s = store_path.as_os_str().to_owned();
    s.push(LOCK_FILE_SUFFIX);
    PathBuf::from(s)
}

/// Why a data directory could not be taken exclusively.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// Another live process holds the directory. Carries the PID it recorded,
    /// when readable — the operator's next question is always "which one?".
    #[error(
        "another engenho process holds {}{}; refusing to open the store — \
         a second opener corrupts it, because fjall does not lock its own \
         directory",
        path.display(),
        match holder_pid { Some(p) => format!(" (pid {p})"), None => String::new() }
    )]
    Held {
        /// The lock file that is held.
        path: PathBuf,
        /// PID recorded by the holder, if the file could be read.
        holder_pid: Option<i32>,
    },
    /// The lock file could not be created or opened at all.
    #[error("cannot open lock file {}: {source}", path.display())]
    Unusable {
        /// The lock path that could not be opened.
        path: PathBuf,
        /// Underlying IO failure.
        source: std::io::Error,
    },
}

/// An exclusive claim on one data directory, released on drop (and by the
/// kernel if this process dies without dropping).
///
/// Hold it for as long as the store is open. Dropping it releases the
/// directory, so storing it in a `_`-prefixed field is a mistake: see the
/// note on the field that owns it.
#[derive(Debug)]
pub struct DataDirLock {
    /// Held open purely to own the lock — the kernel releases the `flock` when
    /// this descriptor closes. Never read from.
    _file: File,
    path: PathBuf,
}

impl DataDirLock {
    /// Take `store_path` exclusively, creating `<store_path>.lock` if absent.
    ///
    /// Non-blocking on purpose: a caller that blocks here waits forever behind
    /// a healthy daemon, which looks like a hang rather than the conflict it
    /// is.
    ///
    /// # Errors
    ///
    /// [`LockError::Held`] when another process has it, [`LockError::Unusable`]
    /// when the lock file itself cannot be opened.
    pub fn acquire(store_path: impl AsRef<Path>) -> Result<Self, LockError> {
        let path = lock_path_for(store_path.as_ref());
        if let Some(parent) = path.parent() {
            // A missing data dir is the first-boot case, not an error.
            let _ = std::fs::create_dir_all(parent);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| LockError::Unusable { path: path.clone(), source })?;

        if !try_lock_exclusive(&file) {
            // Read the holder's PID for the message. Best-effort: the holder
            // may not have written it yet, and a missing PID must not turn a
            // clear "held" into a confusing IO error.
            let holder_pid = std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok());
            return Err(LockError::Held { path, holder_pid });
        }

        // Record who holds it, for the error message the NEXT process prints.
        // Truncate-then-write: the file is reused across boots, so a shorter
        // pid must not leave a longer one's tail behind.
        let _ = file.set_len(0);
        let _ = write!(file, "{}", std::process::id());
        let _ = file.flush();

        Ok(Self { _file: file, path })
    }

    /// The lock file being held.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// `flock(fd, LOCK_EX | LOCK_NB)` — true when the lock was taken.
#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> bool {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `fd` is owned by `file` and valid for this call; flock takes no
    // pointers and cannot invalidate it.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

/// Non-unix has no `flock`. Refusing to guess is the typed-gap posture: a
/// stub returning `true` would mean the platform silently loses the guarantee
/// the rest of this module exists to provide.
#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariant: a second acquisition is REFUSED while the first lives.
    ///
    /// This is the whole point — a store directory was destroyed because two
    /// processes opened it. `flock` is per open-file-description, so two
    /// acquisitions conflict even in one process and the invariant needs no
    /// subprocess to test.
    #[test]
    fn a_second_acquisition_is_refused_while_the_first_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let first = DataDirLock::acquire(tmp.path()).expect("first acquire");

        let second = DataDirLock::acquire(tmp.path());
        match second {
            Err(LockError::Held { holder_pid, .. }) => {
                assert_eq!(
                    holder_pid,
                    Some(std::process::id() as i32),
                    "the error must name the holder — the operator's first question"
                );
            }
            Err(other) => panic!("expected Held, got {other}"),
            Ok(_) => panic!(
                "a second lock was granted; this is the exact condition that \
                 destroyed a store — two openers on one fjall directory"
            ),
        }
        drop(first);
    }

    /// Releasing must actually release, or a restart cannot reopen its own
    /// store and the supervisor loops forever on a lock nobody holds.
    #[test]
    fn releasing_lets_the_next_process_in() {
        let tmp = tempfile::tempdir().unwrap();
        let first = DataDirLock::acquire(tmp.path()).expect("first");
        drop(first);
        let again = DataDirLock::acquire(tmp.path());
        assert!(again.is_ok(), "a released lock must be re-acquirable: {:?}", again.err());
    }

    /// The lock is `<store_path>.lock`: a sibling of the keyspace directory,
    /// named from the store's own path.
    #[test]
    fn the_lock_file_is_derived_from_the_store_path() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let lock = DataDirLock::acquire(&store).unwrap();
        assert_eq!(lock.path(), tmp.path().join("store.lock"));
        assert!(
            !lock.path().starts_with(&store),
            "the lock must not sit inside the fjall keyspace directory"
        );
    }

    /// ★ The regression this crate's own test suite caught: two DIFFERENT
    /// stores under one shared parent must not contend. The first version
    /// locked `<parent>/engenho.lock`, so every store under `$TMPDIR` fought
    /// over one file and seven tests failed at once.
    #[test]
    fn two_stores_under_a_shared_parent_do_not_contend() {
        let tmp = tempfile::tempdir().unwrap();
        let a = DataDirLock::acquire(tmp.path().join("store-a")).expect("first store");
        let b = DataDirLock::acquire(tmp.path().join("store-b"));
        assert!(
            b.is_ok(),
            "separate stores sharing a parent are separate stores: {:?}",
            b.err()
        );
        assert_ne!(a.path(), b.unwrap().path());
    }

    /// A stale lock file from a previous boot is re-usable: the FILE persists,
    /// the LOCK does not. This is why flock was chosen over a pidfile.
    #[test]
    fn a_leftover_lock_file_from_a_dead_process_does_not_block_startup() {
        let tmp = tempfile::tempdir().unwrap();
        // A pid that is not us, written as a previous boot would have.
        let store = tmp.path().join("store");
        std::fs::write(lock_path_for(&store), "999999").unwrap();
        let lock = DataDirLock::acquire(&store);
        assert!(
            lock.is_ok(),
            "a lock FILE with no holder must not refuse startup — that would \
             make a crash a permanent outage: {:?}",
            lock.err()
        );
    }
}
