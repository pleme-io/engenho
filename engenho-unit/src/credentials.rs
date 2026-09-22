//! `LoadCredential=` / `SetCredential=` → `$CREDENTIALS_DIRECTORY`.
//!
//! systemd hands a service its secrets through a private directory it
//! populates before the service starts, exporting the path as
//! `$CREDENTIALS_DIRECTORY` and expanding it in unit values as `%d`. nixpkgs
//! uses it for exactly the secrets that must not be world-readable in the Nix
//! store: zwave-js's `secrets.json`, mosquitto's per-user password files.
//!
//! This runner reproduces the directory — `/run/credentials/<unit>`, mode
//! `0500`, each credential `0400`, all owned by the service's user — but not
//! its ramfs mount: the files are ordinary files on `/run`. That is a
//! WEAKER guarantee than systemd's (a privileged process could read them
//! through the filesystem rather than only through the mount namespace), and
//! it is the same guarantee every other file under `/run` on the node has.
//!
//! Not supported, by decision: `LoadCredentialEncrypted=` /
//! `ImportCredential=` (systemd's credential store, TPM-sealed) are refused in
//! [`crate::unit`] rather than silently skipped — a service whose secret is
//! missing fails in its own logs, far from the cause.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::layout::Layout;
use crate::unit::UnitError;
use crate::words;

/// Where a credential's bytes come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// `LoadCredential=ID:PATH` — a file, or a directory of files.
    Path(PathBuf),
    /// `LoadCredential=ID` — the credential store under `/etc/credstore` or
    /// `/run/credstore`.
    Store,
    /// `SetCredential=ID:VALUE` — the value, inline in the unit.
    Inline(String),
}

/// One credential the unit declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSpec {
    /// The id the service reads it by: `$CREDENTIALS_DIRECTORY/<id>`.
    pub id: String,
    /// Where its bytes come from.
    pub source: CredentialSource,
}

impl CredentialSpec {
    /// Parse a `LoadCredential=`/`SetCredential=` value.
    ///
    /// # Errors
    ///
    /// [`UnitError::BadValue`] for an empty or path-bearing id.
    pub fn parse(key: &str, value: &str, line: usize) -> Result<Self, UnitError> {
        let (id, rest) = match value.split_once(':') {
            Some((id, rest)) => (id, Some(rest)),
            None => (value, None),
        };
        let bad = |expected| UnitError::BadValue {
            key: key.to_string(),
            line,
            value: value.to_string(),
            expected,
        };
        if id.is_empty() || id.contains('/') || id == "." || id == ".." {
            return Err(bad("ID[:SOURCE] with an ID that is not a path"));
        }
        let source = match (key, rest) {
            ("SetCredential", Some(text)) => CredentialSource::Inline(unescape(text)),
            ("SetCredential", None) => return Err(bad("ID:VALUE")),
            (_, None) => CredentialSource::Store,
            (_, Some(path)) => {
                if path.starts_with('/') {
                    CredentialSource::Path(PathBuf::from(path))
                } else {
                    // A relative LoadCredential source names the credential
                    // store, like a bare id does.
                    CredentialSource::Store
                }
            }
        };
        Ok(Self {
            id: id.to_string(),
            source,
        })
    }
}

/// C-unescape a `SetCredential=` value; an invalid escape stays literal,
/// because a credential's bytes are data, not a command line.
fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        let mut lookahead = chars.clone();
        match words::unescape_one(&mut lookahead) {
            Ok(decoded) => {
                out.push(decoded);
                chars = lookahead;
            }
            Err(_) => out.push('\\'),
        }
    }
    out
}

/// Why the credentials directory could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// The directory could not be created or cleared.
    Directory {
        /// The directory.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// A credential's source could not be read.
    Source {
        /// The credential id.
        id: String,
        /// The source.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// A bare `LoadCredential=ID` whose id is in no credential store.
    NotInStore {
        /// The credential id.
        id: String,
        /// The directories searched.
        searched: Vec<PathBuf>,
    },
    /// A credential could not be written.
    Write {
        /// The credential id.
        id: String,
        /// The file.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Directory { path, detail } => write!(
                f,
                "the credentials directory {} could not be prepared: {detail}",
                path.display()
            ),
            Self::Source { id, path, detail } => write!(
                f,
                "credential {id}: {} could not be read: {detail}",
                path.display()
            ),
            Self::NotInStore { id, searched } => {
                write!(f, "credential {id} is in no credential store (looked in ")?;
                for (i, dir) in searched.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", dir.display())?;
                }
                f.write_str(")")
            }
            Self::Write { id, path, detail } => write!(
                f,
                "credential {id} could not be written to {}: {detail}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for CredentialError {}

/// The directories a bare `LoadCredential=ID` is looked up in.
const STORES: [&str; 3] = ["/etc/credstore", "/run/credstore", "/usr/lib/credstore"];

/// Build `/run/credentials/<unit>` and fill it.
///
/// Returns the directory — the same path `$CREDENTIALS_DIRECTORY` carries and
/// `%d` expanded to, because both come from the [`Layout`].
///
/// The directory is rebuilt from scratch on every start, as systemd's is: a
/// credential removed from the unit must not survive in it.
///
/// # Errors
///
/// A [`CredentialError`] naming the credential and the path.
pub fn install(
    specs: &[CredentialSpec],
    layout: &Layout,
    unit: &str,
    owner: Option<(u32, u32)>,
) -> Result<PathBuf, CredentialError> {
    let dir = layout.credentials_dir(unit);
    let fail = |detail: String| CredentialError::Directory {
        path: dir.clone(),
        detail,
    };
    if dir.exists() {
        // The directory is 0500 once installed, so make it writable again
        // before removing what is in it.
        set_mode(&dir, 0o700).map_err(&fail)?;
        std::fs::remove_dir_all(&dir).map_err(|e| fail(e.to_string()))?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| fail(e.to_string()))?;
    set_mode(&dir, 0o700).map_err(&fail)?;

    for spec in specs {
        for (id, bytes) in read_source(spec, layout)? {
            let path = dir.join(&id);
            std::fs::write(&path, bytes).map_err(|e| CredentialError::Write {
                id: id.clone(),
                path: path.clone(),
                detail: e.to_string(),
            })?;
            let write_fail = |detail: String| CredentialError::Write {
                id: id.clone(),
                path: path.clone(),
                detail,
            };
            set_mode(&path, 0o400).map_err(&write_fail)?;
            chown(&path, owner).map_err(&write_fail)?;
        }
    }
    chown(&dir, owner).map_err(&fail)?;
    set_mode(&dir, 0o500).map_err(&fail)?;
    Ok(dir)
}

/// The bytes of one credential — several, when the source is a directory.
fn read_source(
    spec: &CredentialSpec,
    layout: &Layout,
) -> Result<Vec<(String, Vec<u8>)>, CredentialError> {
    let source = match &spec.source {
        CredentialSource::Inline(value) => {
            return Ok(vec![(spec.id.clone(), value.clone().into_bytes())]);
        }
        CredentialSource::Path(path) => layout.on_disk(path),
        CredentialSource::Store => {
            let searched: Vec<PathBuf> = STORES
                .iter()
                .map(|s| layout.on_disk(Path::new(s)).join(&spec.id))
                .collect();
            searched
                .iter()
                .find(|p| p.is_file())
                .cloned()
                .ok_or_else(|| CredentialError::NotInStore {
                    id: spec.id.clone(),
                    searched,
                })?
        }
    };
    let read = |path: &Path| {
        std::fs::read(path).map_err(|e| CredentialError::Source {
            id: spec.id.clone(),
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    };
    if source.is_dir() {
        let entries = std::fs::read_dir(&source).map_err(|e| CredentialError::Source {
            id: spec.id.clone(),
            path: source.clone(),
            detail: e.to_string(),
        })?;
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| CredentialError::Source {
                id: spec.id.clone(),
                path: source.clone(),
                detail: e.to_string(),
            })?;
            if entry.path().is_file() {
                let mut id = spec.id.clone();
                id.push('_');
                id.push_str(&entry.file_name().to_string_lossy());
                out.push((id, read(&entry.path())?));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    } else {
        Ok(vec![(spec.id.clone(), read(&source)?)])
    }
}

fn set_mode(path: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|e| e.to_string())
}

fn chown(path: &Path, owner: Option<(u32, u32)>) -> Result<(), String> {
    match owner {
        Some((uid, gid)) => {
            std::os::unix::fs::chown(path, Some(uid), Some(gid)).map_err(|e| e.to_string())
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::HostRoot;
    use std::os::unix::fs::PermissionsExt;

    fn spec(key: &str, value: &str) -> CredentialSpec {
        CredentialSpec::parse(key, value, 1).unwrap()
    }

    #[test]
    fn the_nixpkgs_forms_parse() {
        assert_eq!(
            spec("LoadCredential", "secrets.json:/run/secrets/zwave-js.json"),
            CredentialSpec {
                id: "secrets.json".into(),
                source: CredentialSource::Path("/run/secrets/zwave-js.json".into()),
            }
        );
        assert_eq!(
            spec(
                "LoadCredential",
                "listener-0-user-0-passwordFile:/run/secrets/mqtt-ha"
            )
            .id,
            "listener-0-user-0-passwordFile"
        );
        assert_eq!(
            spec("LoadCredential", "bare").source,
            CredentialSource::Store
        );
        assert_eq!(
            spec("SetCredential", "token:a\\nb").source,
            CredentialSource::Inline("a\nb".into())
        );
    }

    #[test]
    fn an_id_that_is_a_path_is_refused() {
        assert!(CredentialSpec::parse("LoadCredential", "a/b:/x", 3).is_err());
        assert!(CredentialSpec::parse("LoadCredential", ":/x", 3).is_err());
        assert!(CredentialSpec::parse("SetCredential", "novalue", 3).is_err());
    }

    #[test]
    fn credentials_land_in_the_directory_with_0400_and_the_directory_is_0500() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::new(HostRoot::at(root.path()));
        let source = root.path().join("run/secrets");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("zwave-js.json"), "{\"secret\":1}").unwrap();

        let dir = install(
            &[
                spec("LoadCredential", "secrets.json:/run/secrets/zwave-js.json"),
                spec("SetCredential", "inline:hello"),
            ],
            &layout,
            "zwave-js.service",
            None,
        )
        .unwrap();
        assert_eq!(
            dir,
            root.path().join("run/credentials/zwave-js.service"),
            "the directory is rooted by the layout, and %d says the same"
        );
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o500
        );
        let file = dir.join("secrets.json");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "{\"secret\":1}");
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o400
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("inline")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn a_second_start_rebuilds_the_directory_rather_than_adding_to_it() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::new(HostRoot::at(root.path()));
        install(
            &[spec("SetCredential", "old:x")],
            &layout,
            "u.service",
            None,
        )
        .unwrap();
        install(
            &[spec("SetCredential", "new:y")],
            &layout,
            "u.service",
            None,
        )
        .unwrap();
        let dir = layout.credentials_dir("u.service");
        assert!(dir.join("new").exists());
        assert!(
            !dir.join("old").exists(),
            "a removed credential does not survive"
        );
    }

    #[test]
    fn a_directory_source_becomes_one_credential_per_file() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::new(HostRoot::at(root.path()));
        let source = root.path().join("run/secrets/users");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("ha"), "p1").unwrap();
        std::fs::write(source.join("nr"), "p2").unwrap();
        install(
            &[spec("LoadCredential", "users:/run/secrets/users")],
            &layout,
            "u.service",
            None,
        )
        .unwrap();
        let dir = layout.credentials_dir("u.service");
        assert_eq!(std::fs::read_to_string(dir.join("users_ha")).unwrap(), "p1");
        assert_eq!(std::fs::read_to_string(dir.join("users_nr")).unwrap(), "p2");
    }

    #[test]
    fn a_missing_source_names_the_credential_and_the_path() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::new(HostRoot::at(root.path()));
        let err = install(
            &[spec("LoadCredential", "x:/run/secrets/absent")],
            &layout,
            "u.service",
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, CredentialError::Source { ref id, .. } if id == "x"),
            "{err}"
        );
        let err = install(&[spec("LoadCredential", "x")], &layout, "u.service", None).unwrap_err();
        assert!(matches!(err, CredentialError::NotInStore { .. }), "{err}");
    }
}
