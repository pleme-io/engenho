//! Binding the local control socket, safely.
//!
//! The socket is the recovery path — the one way in when the config is
//! broken or the cluster PKI is gone — so binding it must never go wrong
//! quietly:
//!
//! * **One daemon per socket.** The socket is claimed with an exclusive
//!   `flock` on `<socket>.lock` (the store's [`DataDirLock`], reused). A
//!   socket file whose lock is free was left by a daemon that died, and is
//!   replaced; one whose lock is held belongs to a live daemon, and binding
//!   fails.
//! * **Only a socket is ever unlinked.** A symlink, a regular file or a
//!   directory at the path is refused, never removed: whatever put it there
//!   is not ours to delete.
//! * **The directory is the gate.** Permissions on a socket file are not
//!   honoured everywhere, and there is a window between `bind` and `chmod`;
//!   the directory is not. It must be the daemon's own, not writable by
//!   anyone else, and for [`SocketAccess::Owner`] it is made `0700`.
//! * **The path fits.** A path longer than `sun_path` is refused by name,
//!   not truncated.

use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use engenho_config::{SocketAccess, check_socket_path_len};
use engenho_store::data_dir_lock::{DataDirLock, LockError};
use tokio::net::UnixListener;

/// Why the socket could not be bound.
#[derive(Debug, thiserror::Error)]
pub enum SocketError {
    /// The path is too long for a socket address, or not absolute.
    #[error("control socket path: {0}")]
    Path(String),
    /// A live daemon holds the socket.
    #[error("another engenho daemon serves {}: {source}", path.display())]
    InUse {
        /// The socket.
        path: PathBuf,
        /// The lock that is held.
        source: LockError,
    },
    /// Something other than a socket is at the path; it is left alone.
    #[error("refusing to replace {}: it is a {found}, not a socket", path.display())]
    NotASocket {
        /// The path.
        path: PathBuf,
        /// What is there.
        found: &'static str,
    },
    /// The socket's directory would let someone else in.
    #[error("the control socket directory {} {problem}", dir.display())]
    UnsafeDirectory {
        /// The directory.
        dir: PathBuf,
        /// What is wrong with it.
        problem: String,
    },
    /// A filesystem operation failed.
    #[error("{op} {}: {source}", path.display())]
    Io {
        /// What was being done.
        op: &'static str,
        /// To what.
        path: PathBuf,
        /// Why it failed.
        source: std::io::Error,
    },
}

/// A bound control socket: the listener, and the guard that holds its lock
/// and removes the socket file when dropped.
#[derive(Debug)]
pub struct BoundSocket {
    /// The listener.
    pub listener: UnixListener,
    /// Held for as long as the socket is served.
    pub guard: SocketGuard,
}

/// Holds a socket's lock; removes the socket file on drop.
#[derive(Debug)]
pub struct SocketGuard {
    path: PathBuf,
    _lock: DataDirLock,
}

impl SocketGuard {
    /// The socket's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        // Ours while the lock is held: remove it, if it is still a socket.
        if std::fs::symlink_metadata(&self.path).is_ok_and(|m| m.file_type().is_socket()) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// The socket file's mode for `access`.
#[must_use]
pub const fn socket_mode(access: SocketAccess) -> u32 {
    match access {
        SocketAccess::Owner => 0o600,
        SocketAccess::Group => 0o660,
    }
}

/// Bind the control socket at `path`, as a process running as `euid`.
///
/// # Errors
///
/// See [`SocketError`].
pub fn bind(path: &Path, access: SocketAccess, euid: u32) -> Result<BoundSocket, SocketError> {
    if !path.is_absolute() {
        return Err(SocketError::Path(format!(
            "{} is not absolute",
            path.display()
        )));
    }
    check_socket_path_len(path).map_err(SocketError::Path)?;
    let dir = path
        .parent()
        .ok_or_else(|| SocketError::Path(format!("{} has no directory", path.display())))?;
    prepare_dir(dir, access, euid)?;

    let lock = DataDirLock::acquire(path).map_err(|source| SocketError::InUse {
        path: path.to_path_buf(),
        source,
    })?;
    let io = |op: &'static str| {
        let path = path.to_path_buf();
        move |source| SocketError::Io { op, path, source }
    };
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            // The lock is ours, so nothing serves it: a dead daemon's.
            std::fs::remove_file(path).map_err(io("remove the stale socket"))?;
        }
        Ok(meta) => {
            return Err(SocketError::NotASocket {
                path: path.to_path_buf(),
                found: kind(&meta),
            });
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(io("inspect")(err)),
    }
    let listener = UnixListener::bind(path).map_err(io("bind"))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(socket_mode(access)))
        .map_err(io("set the mode of"))?;
    Ok(BoundSocket {
        listener,
        guard: SocketGuard {
            path: path.to_path_buf(),
            _lock: lock,
        },
    })
}

/// Create the socket's directory if needed, and refuse one that would let
/// someone else in.
fn prepare_dir(dir: &Path, access: SocketAccess, euid: u32) -> Result<(), SocketError> {
    let io = |op: &'static str| {
        let path = dir.to_path_buf();
        move |source| SocketError::Io { op, path, source }
    };
    let created = !dir.exists();
    if created {
        std::fs::create_dir_all(dir).map_err(io("create"))?;
    }
    let meta = std::fs::symlink_metadata(dir).map_err(io("inspect"))?;
    let unsafe_dir = |problem: String| SocketError::UnsafeDirectory {
        dir: dir.to_path_buf(),
        problem,
    };
    if !meta.is_dir() {
        return Err(unsafe_dir(format!("is a {}, not a directory", kind(&meta))));
    }
    if meta.uid() != euid {
        return Err(unsafe_dir(format!(
            "is owned by uid {}, not by the daemon's uid {euid}",
            meta.uid()
        )));
    }
    match access {
        SocketAccess::Owner => {
            // The directory is the daemon's alone.
            if meta.mode() & 0o777 != 0o700 {
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(io("set the mode of"))?;
            }
        }
        SocketAccess::Group => {
            if meta.mode() & 0o002 != 0 {
                return Err(unsafe_dir(format!(
                    "is writable by anyone (mode {:o})",
                    meta.mode() & 0o7777
                )));
            }
        }
    }
    Ok(())
}

fn kind(meta: &std::fs::Metadata) -> &'static str {
    let t = meta.file_type();
    if t.is_symlink() {
        "symlink"
    } else if t.is_dir() {
        "directory"
    } else if t.is_file() {
        "regular file"
    } else if t.is_socket() {
        "socket"
    } else {
        "special file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn euid() -> u32 {
        nix::unistd::geteuid().as_raw()
    }

    #[tokio::test]
    async fn a_socket_is_private_and_a_second_daemon_is_refused() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("run").join("control.sock");
        let bound = bind(&path, SocketAccess::Owner, euid()).expect("bind");
        let mode = std::fs::metadata(&path).expect("stat").mode() & 0o777;
        assert_eq!(mode, 0o600);
        let dir_mode = std::fs::metadata(path.parent().expect("dir"))
            .expect("stat")
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert!(matches!(
            bind(&path, SocketAccess::Owner, euid()),
            Err(SocketError::InUse { .. })
        ));
        drop(bound);
        assert!(!path.exists(), "the guard removes the socket");
    }

    #[tokio::test]
    async fn a_dead_daemons_socket_is_replaced() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("control.sock");
        // A socket nobody holds the lock of: bound, then its guard leaked.
        let stale = std::os::unix::net::UnixListener::bind(&path).expect("stale socket");
        drop(stale);
        assert!(path.exists());
        bind(&path, SocketAccess::Owner, euid()).expect("the stale socket is replaced");
    }

    #[tokio::test]
    async fn nothing_but_a_socket_is_ever_removed() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("precious");
        std::fs::write(&target, b"keep me").expect("write");
        let path = tmp.path().join("control.sock");
        std::os::unix::fs::symlink(&target, &path).expect("symlink");
        assert!(matches!(
            bind(&path, SocketAccess::Owner, euid()),
            Err(SocketError::NotASocket {
                found: "symlink",
                ..
            })
        ));
        assert!(
            std::fs::symlink_metadata(&path).is_ok(),
            "the symlink is still there"
        );
        assert_eq!(std::fs::read(&target).expect("read"), b"keep me");

        let file = tmp.path().join("file.sock");
        std::fs::write(&file, b"x").expect("write");
        assert!(matches!(
            bind(&file, SocketAccess::Owner, euid()),
            Err(SocketError::NotASocket {
                found: "regular file",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn a_path_too_long_for_a_socket_is_refused_by_name() {
        let path = PathBuf::from(format!("/tmp/{}/control.sock", "d".repeat(120)));
        assert!(matches!(
            bind(&path, SocketAccess::Owner, euid()),
            Err(SocketError::Path(_))
        ));
    }

    #[tokio::test]
    async fn a_directory_someone_else_owns_is_refused() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("control.sock");
        let other = euid().wrapping_add(1);
        assert!(matches!(
            bind(&path, SocketAccess::Owner, other),
            Err(SocketError::UnsafeDirectory { .. })
        ));
    }
}
