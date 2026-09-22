//! `data_dir/control/` — the daemon's own state, beside the store and never
//! inside it.
//!
//! | File | What |
//! |---|---|
//! | `daemon.lock` | held for the life of the daemon: one daemon per data directory |
//! | `boot-journal.json` | the [`BootJournal`] |
//! | `run.json` | the [`RunMarker`] |
//! | `identity.json` | the [`IdentityRecord`] written at first boot |
//! | `hold` | present: a relaunched daemon comes up Stopped instead of booting |
//!
//! Every write is atomic (tmp + fsync + rename), so a crash leaves the old
//! file or the new one, never a torn one. The directory is `0700`: what is in
//! it is the daemon's, not the cluster's.

use std::path::{Path, PathBuf};

use engenho_store::data_dir_lock::{DataDirLock, LockError};
use engenho_substrate::{AtomicWriteError, write_atomic};

use super::journal::{BootJournal, IdentityRecord, RunMarker};
use crate::boot::Timestamp;

/// A data directory's `control/` subdirectory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlDir {
    root: PathBuf,
}

/// Writing one of the control files failed.
#[derive(Debug, thiserror::Error)]
pub enum ControlWriteError {
    /// Serializing it failed.
    #[error("serialize {file}: {source}")]
    Serialize {
        /// Which file.
        file: &'static str,
        /// Why.
        source: serde_json::Error,
    },
    /// Writing it failed.
    #[error("write {file}: {source}")]
    Write {
        /// Which file.
        file: &'static str,
        /// Why.
        source: AtomicWriteError,
    },
    /// Removing it failed.
    #[error("remove {file}: {source}")]
    Remove {
        /// Which file.
        file: &'static str,
        /// Why.
        source: std::io::Error,
    },
}

impl ControlDir {
    /// The directory's name under the data directory.
    pub const NAME: &'static str = crate::layout::Area::Control.dir();
    const JOURNAL: &'static str = "boot-journal.json";
    const RUN: &'static str = "run.json";
    const IDENTITY: &'static str = "identity.json";
    const HOLD: &'static str = "hold";
    const LOCK_STEM: &'static str = "daemon";

    /// `data_dir/control`.
    #[must_use]
    pub fn under(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join(Self::NAME),
        }
    }

    /// The directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Create the directory (and the data directory) if absent, mode `0700`.
    ///
    /// # Errors
    ///
    /// The directory cannot be created or its mode set.
    pub fn create(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.root, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    /// Take the daemon lock: only one daemon runs over a data directory.
    ///
    /// # Errors
    ///
    /// [`LockError::Held`] when another daemon has it.
    pub fn lock(&self) -> Result<DataDirLock, LockError> {
        DataDirLock::acquire(self.root.join(Self::LOCK_STEM))
    }

    /// The boot journal (empty when there is none).
    #[must_use]
    pub fn read_journal(&self) -> BootJournal {
        BootJournal::from_bytes(self.read(Self::JOURNAL).as_deref())
    }

    /// Persist the boot journal.
    ///
    /// # Errors
    ///
    /// See [`ControlWriteError`].
    pub fn write_journal(&self, journal: &BootJournal) -> Result<(), ControlWriteError> {
        let bytes = journal
            .to_bytes()
            .map_err(|source| ControlWriteError::Serialize {
                file: Self::JOURNAL,
                source,
            })?;
        self.write(Self::JOURNAL, &bytes)
    }

    /// The run marker the last process left, if it is there and readable.
    #[must_use]
    pub fn read_run(&self) -> Option<RunMarker> {
        self.read_json(Self::RUN)
    }

    /// Persist the run marker.
    ///
    /// # Errors
    ///
    /// See [`ControlWriteError`].
    pub fn write_run(&self, marker: &RunMarker) -> Result<(), ControlWriteError> {
        self.write_json(Self::RUN, marker)
    }

    /// The identity recorded at first boot, if any.
    #[must_use]
    pub fn read_identity(&self) -> Option<IdentityRecord> {
        self.read_json(Self::IDENTITY)
    }

    /// Record the identity (at first boot).
    ///
    /// # Errors
    ///
    /// See [`ControlWriteError`].
    pub fn write_identity(&self, identity: &IdentityRecord) -> Result<(), ControlWriteError> {
        self.write_json(Self::IDENTITY, identity)
    }

    /// Whether the hold marker is present.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.root.join(Self::HOLD).exists()
    }

    /// Place the hold marker: a relaunched daemon comes up Stopped.
    ///
    /// # Errors
    ///
    /// See [`ControlWriteError`].
    pub fn set_hold(&self, at: Timestamp) -> Result<(), ControlWriteError> {
        self.write(Self::HOLD, at.to_rfc3339().as_bytes())
    }

    /// Remove the hold marker, if present.
    ///
    /// # Errors
    ///
    /// See [`ControlWriteError`].
    pub fn clear_hold(&self) -> Result<(), ControlWriteError> {
        match std::fs::remove_file(self.root.join(Self::HOLD)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ControlWriteError::Remove {
                file: Self::HOLD,
                source,
            }),
        }
    }

    fn read(&self, file: &str) -> Option<Vec<u8>> {
        std::fs::read(self.root.join(file)).ok()
    }

    fn read_json<T: serde::de::DeserializeOwned>(&self, file: &'static str) -> Option<T> {
        let bytes = self.read(file)?;
        serde_json::from_slice(&bytes)
            .map_err(
                |err| tracing::warn!(file, error = %err, "unreadable control file; ignoring it"),
            )
            .ok()
    }

    fn write_json<T: serde::Serialize>(
        &self,
        file: &'static str,
        value: &T,
    ) -> Result<(), ControlWriteError> {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|source| ControlWriteError::Serialize { file, source })?;
        self.write(file, &bytes)
    }

    fn write(&self, file: &'static str, bytes: &[u8]) -> Result<(), ControlWriteError> {
        write_atomic(&self.root.join(file), bytes)
            .map_err(|source| ControlWriteError::Write { file, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_directory_is_private_and_its_files_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let control = ControlDir::under(tmp.path());
        control.create().expect("create");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(control.root())
                .expect("stat")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        let at = Timestamp::parse("2026-09-22T00:00:00Z").expect("literal");
        assert!(control.read_run().is_none());
        control
            .write_run(&RunMarker::Released { at })
            .expect("write run");
        assert_eq!(control.read_run(), Some(RunMarker::Released { at }));

        assert!(!control.is_held());
        control.set_hold(at).expect("hold");
        assert!(control.is_held());
        control.clear_hold().expect("clear");
        control.clear_hold().expect("clearing twice is fine");
        assert!(!control.is_held());
    }

    #[test]
    fn one_daemon_per_data_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let control = ControlDir::under(tmp.path());
        control.create().expect("create");
        let first = control.lock().expect("the first daemon locks");
        assert!(matches!(control.lock(), Err(LockError::Held { .. })));
        drop(first);
        control.lock().expect("released with the first");
    }
}
