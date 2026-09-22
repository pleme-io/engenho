//! Destructive re-initialization of the data directory (P7): what each
//! operation replaces, and moving it aside.
//!
//! **Nothing is deleted.** Every file or directory an operation replaces is
//! renamed into `data_dir/control/attic/<operation>-<time>/`, at the same
//! path relative to the data directory, so an operator who regrets it moves
//! it back. A move that fails part-way puts back what it had moved.
//!
//! **The control plane's own state is never replaced** by these operations:
//! what each one moves is a row here, over [`Area`] and [`PkiFile`], and a
//! test holds every row out of [`Area::Control`]. (Rotating the control
//! identity replaces a file in there, through its own path, since the
//! listener has to take the new key live.)
//!
//! What gates them — the confirmation, the runtime being stopped in the
//! bound epoch, the store's lock — is the caller's ([`super::confirm`], the
//! supervisor). This module only moves.

use std::path::{Path, PathBuf};

use engenho_apiserver::PkiFile;

use crate::boot::Timestamp;
use crate::layout::Area;

/// Every confirm-gated operation, by name: the three here, and rotating the
/// control identity, which the control server runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReinitOp {
    /// [`Reinit::RotateAdminToken`].
    RotateAdminToken,
    /// [`Reinit::ReseedPki`].
    ReseedPki,
    /// [`Reinit::WipeStore`].
    WipeStore,
    /// A new control identity key.
    RotateControlIdentity,
}

impl ReinitOp {
    /// Its name, as the control API spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::RotateAdminToken => "rotate_admin_token",
            Self::ReseedPki => "reseed_pki",
            Self::WipeStore => "wipe_store",
            Self::RotateControlIdentity => "rotate_control_identity",
        }
    }
}

/// A PKI re-seed's treatment of the `ServiceAccount` signing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaKey {
    /// Keep it: tokens already minted stay valid.
    Keep,
    /// Replace it: every token minted so far stops verifying.
    Rotate,
}

/// How much of the data directory a store wipe takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WipeScope {
    /// The store alone.
    StoreOnly,
    /// The store and what workloads left on this node ([`Area::node_local`]).
    StoreAndNodeLocal,
}

/// One re-initialization of the data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reinit {
    /// Replace the bootstrap admin bearer token.
    RotateAdminToken,
    /// Re-seed the cluster PKI: the seed, and the CA and admin credential
    /// derived from it; the `ServiceAccount` key as asked.
    ReseedPki {
        /// What happens to the `ServiceAccount` key.
        sa_key: SaKey,
    },
    /// Move the store aside: the next boot is a first boot.
    WipeStore {
        /// How much with it.
        scope: WipeScope,
    },
}

impl Reinit {
    /// Which operation it is.
    #[must_use]
    pub const fn op(self) -> ReinitOp {
        match self {
            Self::RotateAdminToken => ReinitOp::RotateAdminToken,
            Self::ReseedPki { .. } => ReinitOp::ReseedPki,
            Self::WipeStore { .. } => ReinitOp::WipeStore,
        }
    }

    /// Whether it may run only while no runtime holds the store: the PKI a
    /// running apiserver serves from, and the store itself. The admin token
    /// is read once at boot, so replacing it under a running runtime is
    /// safe — the running one keeps the old until it boots again.
    #[must_use]
    pub const fn needs_stopped(self) -> bool {
        match self {
            Self::ReseedPki { .. } | Self::WipeStore { .. } => true,
            Self::RotateAdminToken => false,
        }
    }

    /// What it replaces, relative to the data directory.
    #[must_use]
    pub fn replaces(self) -> Vec<PathBuf> {
        let pki = |file: PkiFile| Path::new(PkiFile::DIR).join(file.name());
        match self {
            Self::RotateAdminToken => vec![pki(PkiFile::AdminToken)],
            Self::ReseedPki { sa_key } => PkiFile::ALL
                .iter()
                .copied()
                .filter(|f| f.seed_derived() || (*f == PkiFile::SaKey && sa_key == SaKey::Rotate))
                .map(pki)
                .collect(),
            Self::WipeStore { scope } => Area::ALL
                .iter()
                .copied()
                .filter(|area| {
                    *area == Area::Store
                        || (scope == WipeScope::StoreAndNodeLocal && area.node_local())
                })
                .map(|area| PathBuf::from(area.dir()))
                .collect(),
        }
    }
}

/// Where an operation's replaced files go.
#[must_use]
pub fn attic_dir(data_dir: &Path, operation: &str, at: Timestamp) -> PathBuf {
    let stamp = at.utc().format("%Y%m%dT%H%M%S%.3fZ").to_string();
    Area::Control
        .path(data_dir)
        .join("attic")
        .join([operation, "-", stamp.as_str()].concat())
}

/// What moving an operation's files aside did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovedAside {
    /// The attic they are in.
    pub attic: PathBuf,
    /// What moved, relative to the data directory. What the operation
    /// replaces and did not exist is not here.
    pub moved: Vec<PathBuf>,
}

/// Why an operation's files were not moved aside.
#[derive(Debug, thiserror::Error)]
pub enum MoveError {
    /// The attic could not be made.
    #[error("cannot create the attic {}: {source}", path.display())]
    Attic {
        /// Where.
        path: PathBuf,
        /// Why.
        source: std::io::Error,
    },
    /// One move failed. Every earlier move was put back if `restored`.
    #[error(
        "cannot move {} aside: {source}{}",
        from.display(),
        if *restored { "; everything moved before it was put back" } else { "; putting back what moved before it failed too" }
    )]
    Move {
        /// What.
        from: PathBuf,
        /// Why.
        source: std::io::Error,
        /// Whether the data directory is as it was.
        restored: bool,
    },
}

/// Move everything `operation` replaces into a new attic, keeping each
/// path relative to the data directory. A rename, so it is atomic per path
/// and costs nothing however large the store is; a path on another
/// filesystem (a volume mounted into the data directory) is refused, not
/// copied.
///
/// # Errors
///
/// [`MoveError`]; on a failed move, what had moved is put back.
pub fn move_aside(
    data_dir: &Path,
    operation: Reinit,
    at: Timestamp,
) -> Result<MovedAside, MoveError> {
    let attic = attic_dir(data_dir, operation.op().name(), at);
    create_private_dir(&attic).map_err(|source| MoveError::Attic {
        path: attic.clone(),
        source,
    })?;
    let mut moved = Vec::new();
    for rel in operation.replaces() {
        let from = data_dir.join(&rel);
        if std::fs::symlink_metadata(&from).is_err() {
            continue;
        }
        let to = attic.join(&rel);
        let renamed = to
            .parent()
            .map_or(Ok(()), create_private_dir)
            .and_then(|()| std::fs::rename(&from, &to));
        if let Err(source) = renamed {
            let restored = put_back(data_dir, &attic, &moved);
            return Err(MoveError::Move {
                from,
                source,
                restored,
            });
        }
        moved.push(rel);
    }
    Ok(MovedAside { attic, moved })
}

/// Rename each of `moved` back out of `attic` — every one, even after one
/// fails. Whether every one went back.
fn put_back(data_dir: &Path, attic: &Path, moved: &[PathBuf]) -> bool {
    let failed = moved
        .iter()
        .rev()
        .map(|rel| std::fs::rename(attic.join(rel), data_dir.join(rel)))
        .filter(Result::is_err)
        .count();
    failed == 0
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY: [Reinit; 5] = [
        Reinit::RotateAdminToken,
        Reinit::ReseedPki {
            sa_key: SaKey::Keep,
        },
        Reinit::ReseedPki {
            sa_key: SaKey::Rotate,
        },
        Reinit::WipeStore {
            scope: WipeScope::StoreOnly,
        },
        Reinit::WipeStore {
            scope: WipeScope::StoreAndNodeLocal,
        },
    ];

    fn paths(reinit: Reinit) -> Vec<String> {
        reinit
            .replaces()
            .iter()
            .map(|p| p.display().to_string())
            .collect()
    }

    /// No operation replaces the control plane's own state, and only a PKI
    /// operation touches the PKI.
    #[test]
    fn nothing_replaces_the_control_state() {
        for reinit in EVERY {
            for rel in reinit.replaces() {
                assert!(
                    !rel.starts_with(Area::Control.dir()),
                    "{reinit:?} replaces {}",
                    rel.display()
                );
                let is_pki = rel.starts_with(PkiFile::DIR);
                let pki_op = matches!(reinit, Reinit::RotateAdminToken | Reinit::ReseedPki { .. });
                assert_eq!(is_pki, pki_op, "{reinit:?} replaces {}", rel.display());
            }
        }
    }

    #[test]
    fn each_operation_replaces_what_it_says() {
        assert_eq!(paths(Reinit::RotateAdminToken), ["pki/admin.token"]);
        assert_eq!(
            paths(Reinit::ReseedPki {
                sa_key: SaKey::Keep
            }),
            [
                "pki/cluster-seed",
                "pki/ca.crt",
                "pki/ca.key",
                "pki/admin.crt",
                "pki/admin.key"
            ]
        );
        assert!(
            paths(Reinit::ReseedPki {
                sa_key: SaKey::Rotate
            })
            .contains(&"pki/sa.key".to_owned())
        );
        assert!(
            !paths(Reinit::ReseedPki {
                sa_key: SaKey::Keep
            })
            .contains(&"pki/sa.key".to_owned())
        );
        assert_eq!(
            paths(Reinit::WipeStore {
                scope: WipeScope::StoreOnly
            }),
            ["store"]
        );
        assert_eq!(
            paths(Reinit::WipeStore {
                scope: WipeScope::StoreAndNodeLocal
            }),
            [
                "store",
                "local-path",
                "volumes",
                "plugins",
                "pods",
                "snapshots"
            ]
        );
        assert!(
            Reinit::WipeStore {
                scope: WipeScope::StoreOnly
            }
            .needs_stopped()
        );
        assert!(
            Reinit::ReseedPki {
                sa_key: SaKey::Keep
            }
            .needs_stopped()
        );
        assert!(!Reinit::RotateAdminToken.needs_stopped());
    }

    fn touch(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
        std::fs::write(path, text).expect("write");
    }

    #[test]
    fn moving_aside_keeps_every_path_and_deletes_nothing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data = tmp.path();
        touch(&data.join("store/raft/log"), "entries");
        touch(&data.join("volumes/ns_pod/secret"), "s");
        touch(&data.join("pki/ca.crt"), "ca");
        touch(&data.join("control/overrides.yaml"), "o");

        let wipe = Reinit::WipeStore {
            scope: WipeScope::StoreAndNodeLocal,
        };
        let aside = move_aside(data, wipe, Timestamp::now()).expect("moved");

        assert_eq!(
            aside.moved,
            [PathBuf::from("store"), PathBuf::from("volumes")],
            "only what existed"
        );
        assert!(aside.attic.starts_with(data.join("control/attic")));
        assert!(
            aside
                .attic
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("wipe_store-"))
        );
        assert_eq!(
            std::fs::read_to_string(aside.attic.join("store/raft/log")).expect("kept"),
            "entries"
        );
        assert!(aside.attic.join("volumes/ns_pod/secret").exists());
        assert!(!data.join("store").exists());
        assert!(data.join("pki/ca.crt").exists(), "a wipe left the PKI");
        assert!(data.join("control/overrides.yaml").exists());
    }

    #[test]
    fn a_failed_move_puts_back_what_moved_before_it() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data = tmp.path();
        touch(&data.join("store/raft/log"), "entries");
        touch(&data.join("volumes/v"), "v");
        let at = Timestamp::now();
        // The attic's `volumes` slot is already a non-empty directory, so the
        // second rename fails after the first succeeded.
        touch(
            &attic_dir(data, "wipe_store", at).join("volumes/occupied"),
            "x",
        );

        let wipe = Reinit::WipeStore {
            scope: WipeScope::StoreAndNodeLocal,
        };
        let err = move_aside(data, wipe, at).expect_err("the second move fails");

        assert!(
            matches!(err, MoveError::Move { restored: true, .. }),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(data.join("store/raft/log")).expect("put back"),
            "entries"
        );
        assert!(data.join("volumes/v").exists());
    }
}
