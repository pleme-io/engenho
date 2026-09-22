//! The remote listener's own identity: a random ed25519 key kept in
//! `data_dir/control/identity/`, created the first time it is needed.
//!
//! It depends on nothing a boot creates — not the cluster CA, not the seed —
//! so remote control works while the cluster's PKI is broken or absent, and
//! the key is never derivable from anything else (unlike the cluster PKI,
//! which is seed-derived by design). Clients pin its SPKI.
//!
//! It can be replaced while the listener serves ([`ControlIdentity::rotate`]):
//! the listener presents the key through [`Presented`], so the next
//! handshake presents the new one.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};

use chrono::{DateTime, Utc};
use engenho_control_types::pin::{KeyMaterial, Presented, Spki};
use engenho_substrate_core::write_atomic_mode;

/// The key's file name.
pub const KEY: &str = "key.pem";

/// The directory's name under `data_dir/control/`.
pub const DIR: &str = "identity";

/// The listener's identity: its key file, the key in it, and the
/// certificate the listener presents for it.
#[derive(Debug)]
pub struct ControlIdentity {
    path: PathBuf,
    current: RwLock<Current>,
    presented: Arc<Presented>,
}

/// The key in force.
#[derive(Debug, Clone, Copy)]
struct Current {
    spki: Spki,
    created_at: DateTime<Utc>,
}

/// The identity could not be loaded or created.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// Reading or writing the key failed.
    #[error("{}: {source}", path.display())]
    Io {
        /// Which file.
        path: PathBuf,
        /// Why.
        source: io::Error,
    },
    /// The file is not a usable key.
    #[error("{}: {why}", path.display())]
    Key {
        /// Which file.
        path: PathBuf,
        /// Why.
        why: String,
    },
    /// Someone other than its owner can read it.
    #[error("{}: mode {mode:04o} lets others read the control identity's private key; `chmod 600` it", path.display())]
    Exposed {
        /// Which file.
        path: PathBuf,
        /// Its mode.
        mode: u32,
    },
}

impl ControlIdentity {
    /// The identity in `dir`, created when absent: the directory `0700`, the
    /// key `0600`, written atomically. An existing key others can read is
    /// refused, not used.
    ///
    /// # Errors
    ///
    /// [`IdentityError`].
    pub fn load_or_create(dir: &Path) -> Result<Self, IdentityError> {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(KEY);
        let io = |source| IdentityError::Io {
            path: path.clone(),
            source,
        };
        let (key, created_at) = match std::fs::metadata(&path) {
            Ok(meta) => {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(IdentityError::Exposed { path, mode });
                }
                let pem = std::fs::read_to_string(&path).map_err(io)?;
                let key = KeyMaterial::from_pem(&pem).map_err(|e| key_error(&path, &e))?;
                let created_at = meta
                    .modified()
                    .map_or_else(|_| Utc::now(), DateTime::<Utc>::from);
                (key, created_at)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                std::fs::create_dir_all(dir).map_err(io)?;
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(io)?;
                let key = KeyMaterial::generate().map_err(|e| key_error(&path, &e))?;
                write_key(&path, &key)?;
                tracing::info!(spki = %key.spki(), "created the control identity");
                (key, Utc::now())
            }
            Err(err) => return Err(io(err)),
        };
        let presented = Presented::new(&key).map_err(|e| key_error(&path, &e))?;
        Ok(Self {
            current: RwLock::new(Current {
                spki: key.spki(),
                created_at,
            }),
            path,
            presented: Arc::new(presented),
        })
    }

    /// Replace the key: the old key file is moved to `keep_old_at` (never
    /// deleted), a new key is written in its place (`0600`, atomically), and
    /// the listener presents it from the next handshake on. Returns the new
    /// pin, which every client's `server_spki` must now name.
    ///
    /// # Errors
    ///
    /// [`IdentityError`]; the old key is back in place and still presented.
    pub fn rotate(&self, keep_old_at: &Path) -> Result<Spki, IdentityError> {
        let key = KeyMaterial::generate().map_err(|e| key_error(&self.path, &e))?;
        let io = |path: &Path, source| IdentityError::Io {
            path: path.to_path_buf(),
            source,
        };
        if let Some(parent) = keep_old_at.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        }
        std::fs::rename(&self.path, keep_old_at).map_err(|e| io(&self.path, e))?;
        let put_back = || std::fs::rename(keep_old_at, &self.path);
        if let Err(err) = write_key(&self.path, &key) {
            let _ = put_back();
            return Err(err);
        }
        if let Err(e) = self.presented.present(&key) {
            let _ = put_back();
            return Err(key_error(&self.path, &e));
        }
        let spki = key.spki();
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Current {
            spki,
            created_at: Utc::now(),
        };
        tracing::info!(%spki, old = %keep_old_at.display(), "rotated the control identity");
        Ok(spki)
    }

    fn current(&self) -> Current {
        *self.current.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Its pin: what a client puts in `server_spki`.
    #[must_use]
    pub fn spki(&self) -> Spki {
        self.current().spki
    }

    /// When the key in force was created (the key file's modification time,
    /// or the rotation that made it).
    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.current().created_at
    }

    /// What the listener presents.
    #[must_use]
    pub fn presented(&self) -> Arc<Presented> {
        Arc::clone(&self.presented)
    }

    /// Where the key is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn key_error(path: &Path, why: &impl std::fmt::Display) -> IdentityError {
    IdentityError::Key {
        path: path.to_path_buf(),
        why: why.to_string(),
    }
}

fn write_key(path: &Path, key: &KeyMaterial) -> Result<(), IdentityError> {
    write_atomic_mode(path, key.to_pem().as_bytes(), 0o600).map_err(|e| IdentityError::Io {
        path: path.to_path_buf(),
        source: io::Error::other(e.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn created_once_private_and_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("control").join(DIR);
        let first = ControlIdentity::load_or_create(&dir).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(first.path()), 0o600);

        let again = ControlIdentity::load_or_create(&dir).unwrap();
        assert_eq!(again.spki(), first.spki(), "the same key, not a new one");
    }

    #[test]
    fn a_key_others_can_read_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let id = ControlIdentity::load_or_create(tmp.path()).unwrap();
        std::fs::set_permissions(id.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            ControlIdentity::load_or_create(tmp.path()),
            Err(IdentityError::Exposed { mode: 0o644, .. })
        ));
    }

    /// A rotation keeps the old key, writes a private new one, and is what
    /// the next load reads.
    #[test]
    fn a_rotation_keeps_the_old_key_and_replaces_it() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(DIR);
        let id = ControlIdentity::load_or_create(&dir).unwrap();
        let old = id.spki();
        let old_pem = std::fs::read_to_string(id.path()).unwrap();
        let kept = tmp.path().join("attic").join(DIR).join(KEY);

        let new = id.rotate(&kept).unwrap();

        assert_ne!(new, old);
        assert_eq!(id.spki(), new);
        assert_eq!(
            std::fs::read_to_string(&kept).unwrap(),
            old_pem,
            "the old key was kept"
        );
        let mode = std::fs::metadata(id.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(ControlIdentity::load_or_create(&dir).unwrap().spki(), new);
    }

    /// A rotation that cannot keep the old key changes nothing.
    #[test]
    fn a_failed_rotation_leaves_the_key_in_force() {
        let tmp = tempfile::tempdir().unwrap();
        let id = ControlIdentity::load_or_create(tmp.path()).unwrap();
        let old = id.spki();
        // A file where the attic's directory should be.
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, "x").unwrap();
        assert!(id.rotate(&blocker.join(KEY)).is_err());
        assert_eq!(id.spki(), old);
        assert_eq!(
            ControlIdentity::load_or_create(tmp.path()).unwrap().spki(),
            old
        );
    }

    #[test]
    fn a_garbage_key_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(KEY);
        std::fs::write(&path, "not a key").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(matches!(
            ControlIdentity::load_or_create(tmp.path()),
            Err(IdentityError::Key { .. })
        ));
    }
}
