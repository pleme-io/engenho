//! The remote listener's own identity: a random ed25519 key kept in
//! `data_dir/control/identity/`, created the first time it is needed.
//!
//! It depends on nothing a boot creates — not the cluster CA, not the seed —
//! so remote control works while the cluster's PKI is broken or absent, and
//! the key is never derivable from anything else (unlike the cluster PKI,
//! which is seed-derived by design). Clients pin its SPKI.

use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use engenho_control_types::pin::{KeyMaterial, Spki};
use engenho_substrate_core::write_atomic_mode;

/// The key's file name.
pub const KEY: &str = "key.pem";

/// The directory's name under `data_dir/control/`.
pub const DIR: &str = "identity";

/// The listener's identity.
#[derive(Debug)]
pub struct ControlIdentity {
    key: KeyMaterial,
    path: PathBuf,
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
        match std::fs::metadata(&path) {
            Ok(meta) => {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(IdentityError::Exposed { path, mode });
                }
                let pem = std::fs::read_to_string(&path).map_err(io)?;
                let key = KeyMaterial::from_pem(&pem).map_err(|e| IdentityError::Key {
                    path: path.clone(),
                    why: e.to_string(),
                })?;
                let created_at = meta
                    .modified()
                    .map_or_else(|_| Utc::now(), DateTime::<Utc>::from);
                Ok(Self {
                    key,
                    path,
                    created_at,
                })
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                std::fs::create_dir_all(dir).map_err(io)?;
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(io)?;
                let key = KeyMaterial::generate().map_err(|e| IdentityError::Key {
                    path: path.clone(),
                    why: e.to_string(),
                })?;
                write_atomic_mode(&path, key.to_pem().as_bytes(), 0o600).map_err(|e| {
                    IdentityError::Io {
                        path: path.clone(),
                        source: io::Error::other(e.to_string()),
                    }
                })?;
                tracing::info!(spki = %key.spki(), "created the control identity");
                Ok(Self {
                    key,
                    path,
                    created_at: Utc::now(),
                })
            }
            Err(err) => Err(io(err)),
        }
    }

    /// The key.
    #[must_use]
    pub const fn key(&self) -> &KeyMaterial {
        &self.key
    }

    /// Its pin: what a client puts in `server_spki`.
    #[must_use]
    pub fn spki(&self) -> Spki {
        self.key.spki()
    }

    /// When it was created (the key file's modification time).
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Where the key is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
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
