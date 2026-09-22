//! Remote daemons: `remotes.yaml`, and this client's keys beside it.
//!
//! ```yaml
//! # ~/.config/engenho/remotes.yaml
//! remotes:
//!   plo:
//!     address: plo.pleme.internal:7443
//!     server_spki: [sha256:…]          # `engenho ctl control show` on plo
//!     # key: defaults to ~/.config/engenho/remotes/plo.key
//! ```
//!
//! A client key is made by `engenho remote keygen <name>` and never leaves
//! the machine: its pin (`engenho remote fingerprint <name>`) is what the
//! daemon's `control.remote.authorized_clients` lists.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use engenho_control_types::pin::{KeyMaterial, Spki};
use serde::{Deserialize, Serialize};

/// The file's name under [`config_dir`].
pub const REMOTES_FILE: &str = "remotes.yaml";
/// The keys' directory under [`config_dir`].
pub const KEYS_DIR: &str = "remotes";

/// `remotes.yaml`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemotesConfig {
    /// Each remote daemon, by name.
    #[serde(default)]
    pub remotes: BTreeMap<String, RemoteEndpoint>,
}

/// One remote daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteEndpoint {
    /// `host:port` of its remote listener.
    pub address: String,
    /// Its pins: `identity.spki` of `engenho ctl control show` on it. More
    /// than one while a rotation overlaps.
    pub server_spki: Vec<Spki>,
    /// This client's key for it; `remotes/<name>.key` beside the file by
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<PathBuf>,
}

/// Why a remote could not be used.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// Neither `$XDG_CONFIG_HOME` nor `$HOME` is set.
    #[error("no configuration directory: set $XDG_CONFIG_HOME or $HOME")]
    NoConfigDir,
    /// A file could not be read or written.
    #[error("{}: {source}", path.display())]
    Io {
        /// Which.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
    /// `remotes.yaml` does not parse.
    #[error("{}: {why}", path.display())]
    Invalid {
        /// Which.
        path: PathBuf,
        /// Why.
        why: String,
    },
    /// No remote by that name.
    #[error("no remote named {name:?} in {}; known: {}", path.display(), known.join(", "))]
    Unknown {
        /// The name asked for.
        name: String,
        /// The file.
        path: PathBuf,
        /// The names it has.
        known: Vec<String>,
    },
    /// A remote with no pins would trust nobody.
    #[error("remote {0:?} lists no server_spki")]
    NoPins(String),
    /// `keygen` would replace a key.
    #[error("{} exists; a key is never replaced (remove it first to rotate)", .0.display())]
    KeyExists(PathBuf),
    /// The key is not a usable key.
    #[error("{}: {why}", path.display())]
    Key {
        /// Which.
        path: PathBuf,
        /// Why.
        why: String,
    },
    /// Someone other than its owner can read the key.
    #[error("{}: mode {mode:04o} lets others read this client's key; `chmod 600` it", path.display())]
    Exposed {
        /// Which.
        path: PathBuf,
        /// Its mode.
        mode: u32,
    },
}

/// Where a user's engenho client configuration lives:
/// `$XDG_CONFIG_HOME/engenho`, else `~/.config/engenho`.
///
/// # Errors
///
/// [`RemoteError::NoConfigDir`].
pub fn config_dir() -> Result<PathBuf, RemoteError> {
    let from = |var: &str| {
        std::env::var_os(var)
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
    };
    from("XDG_CONFIG_HOME")
        .or_else(|| from("HOME").map(|home| home.join(".config")))
        .map(|dir| dir.join("engenho"))
        .ok_or(RemoteError::NoConfigDir)
}

impl RemotesConfig {
    /// Read `dir/remotes.yaml`; none there is no remotes.
    ///
    /// # Errors
    ///
    /// [`RemoteError::Io`] or [`RemoteError::Invalid`].
    pub fn load(dir: &Path) -> Result<Self, RemoteError> {
        let path = dir.join(REMOTES_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => serde_yaml::from_str(&text).map_err(|e| RemoteError::Invalid {
                path,
                why: e.to_string(),
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(source) => Err(RemoteError::Io { path, source }),
        }
    }

    /// The remote named `name`, from the file in `dir`.
    ///
    /// # Errors
    ///
    /// [`RemoteError::Unknown`], or [`RemoteError::NoPins`] for one that
    /// would trust nobody.
    pub fn get(&self, dir: &Path, name: &str) -> Result<&RemoteEndpoint, RemoteError> {
        let endpoint = self.remotes.get(name).ok_or_else(|| RemoteError::Unknown {
            name: name.to_owned(),
            path: dir.join(REMOTES_FILE),
            known: self.remotes.keys().cloned().collect(),
        })?;
        if endpoint.server_spki.is_empty() {
            return Err(RemoteError::NoPins(name.to_owned()));
        }
        Ok(endpoint)
    }
}

/// Where the key for remote `name` is: its `key`, else `dir/remotes/<name>.key`.
#[must_use]
pub fn key_path(dir: &Path, name: &str, endpoint: Option<&RemoteEndpoint>) -> PathBuf {
    endpoint
        .and_then(|e| e.key.clone())
        .unwrap_or_else(|| dir.join(KEYS_DIR).join([name, ".key"].concat()))
}

/// Read a client key, refusing one others can read.
///
/// # Errors
///
/// [`RemoteError::Io`], [`RemoteError::Exposed`] or [`RemoteError::Key`].
pub fn read_key(path: &Path) -> Result<KeyMaterial, RemoteError> {
    use std::os::unix::fs::PermissionsExt;
    let io = |source| RemoteError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mode = std::fs::metadata(path).map_err(io)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(RemoteError::Exposed {
            path: path.to_path_buf(),
            mode,
        });
    }
    let pem = std::fs::read_to_string(path).map_err(io)?;
    KeyMaterial::from_pem(&pem).map_err(|e| RemoteError::Key {
        path: path.to_path_buf(),
        why: e.to_string(),
    })
}

/// Make a new key at `path` (mode `0600`, its directory `0700`), never
/// replacing one, and return its pin.
///
/// # Errors
///
/// [`RemoteError::KeyExists`], or the key could not be made or written.
pub fn keygen(path: &Path) -> Result<Spki, RemoteError> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let io = |source| RemoteError::Io {
        path: path.to_path_buf(),
        source,
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(io)?;
    }
    let key = KeyMaterial::generate().map_err(|e| RemoteError::Key {
        path: path.to_path_buf(),
        why: e.to_string(),
    })?;
    // O_EXCL: an existing key is never replaced, even by a race.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                RemoteError::KeyExists(path.to_path_buf())
            } else {
                io(e)
            }
        })?;
    file.write_all(key.to_pem().as_bytes()).map_err(io)?;
    file.sync_all().map_err(io)?;
    Ok(key.spki())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn a_key_is_made_once_private_and_read_back_to_the_same_pin() {
        let tmp = tempfile::tempdir().unwrap();
        let path = key_path(tmp.path(), "plo", None);
        let pin = keygen(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(read_key(&path).unwrap().spki(), pin);
        assert!(matches!(keygen(&path), Err(RemoteError::KeyExists(_))));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(read_key(&path), Err(RemoteError::Exposed { .. })));
    }

    #[test]
    fn remotes_are_read_by_name_and_a_pinless_one_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let pin = KeyMaterial::generate().unwrap().spki();
        std::fs::write(
            tmp.path().join(REMOTES_FILE),
            [
                "remotes:\n  plo:\n    address: 127.0.0.1:7443\n    server_spki: [",
                &pin.to_string(),
                "]\n  nopins:\n    address: 127.0.0.1:1\n    server_spki: []\n",
            ]
            .concat(),
        )
        .unwrap();
        let remotes = RemotesConfig::load(tmp.path()).unwrap();
        assert_eq!(remotes.get(tmp.path(), "plo").unwrap().server_spki, [pin]);
        assert!(matches!(
            remotes.get(tmp.path(), "rio"),
            Err(RemoteError::Unknown { known, .. }) if known == ["nopins", "plo"]
        ));
        assert!(matches!(
            remotes.get(tmp.path(), "nopins"),
            Err(RemoteError::NoPins(_))
        ));
        assert_eq!(
            RemotesConfig::load(&tmp.path().join("none")).unwrap(),
            RemotesConfig::default()
        );
    }
}
