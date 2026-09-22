//! The override tier's keeper: `data_dir/control/overrides.yaml`.
//!
//! [`OverrideStore`] holds the leaves set through the control plane — each
//! with who set it and when — and hands the configuration fold the
//! [`OverrideLayer`] it folds over the declared file. An override is
//! persisted by default (written atomically, mode `0600`) or kept in memory
//! only ([`Durability::Ephemeral`]); either way the set has one
//! **generation**, bumped by every change and persisted with it, so the
//! optimistic-concurrency token keeps rising across restarts.
//!
//! A file that cannot be read does not stop the daemon: the store reports it,
//! every resolution fails naming it (a boot is held, not crash-looped), and
//! `engenho ctl config clear` replaces it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use engenho_config::{ConfigError, LeafPath, OverrideLayer};
use engenho_control_types::types::PrincipalView;
use engenho_substrate::{AtomicWriteError, write_atomic_mode};
use serde::{Deserialize, Serialize};

/// The file's name under `data_dir/control/`.
pub const FILE: &str = "overrides.yaml";

/// The on-disk schema's one version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Schema {
    #[serde(rename = "engenho.control/overrides/v1")]
    V1,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverrideFile {
    schema: Schema,
    generation: u64,
    entries: BTreeMap<LeafPath, Stored>,
}

/// One override: the value, and who set it when.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stored {
    /// The overriding value.
    pub value: serde_json::Value,
    /// Who set it (what the kernel or the handshake proved, and what they
    /// claimed).
    pub set_by: PrincipalView,
    /// When.
    pub set_at: DateTime<Utc>,
}

/// Whether an override outlives the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// Written to `overrides.yaml`.
    Persisted,
    /// Kept in memory; gone when the process ends.
    Ephemeral,
}

/// One override, as the control API lists it.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// The leaf.
    pub path: LeafPath,
    /// Its override.
    pub stored: Stored,
    /// Whether it outlives the process.
    pub durability: Durability,
}

/// A change to the override set.
#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    /// Override one leaf.
    Set {
        /// Which.
        path: LeafPath,
        /// With what, set by whom and when.
        stored: Stored,
        /// Whether it outlives the process.
        durability: Durability,
    },
    /// Remove the override of one leaf.
    Unset {
        /// Which.
        path: LeafPath,
    },
    /// Remove every override, or every one under a dotted prefix.
    Clear {
        /// Only under this prefix.
        prefix: Option<String>,
    },
}

/// The overrides at one generation. Cheap to clone; changing it is pure
/// ([`Self::with`]), and only [`OverrideStore::commit`] makes a change real.
#[derive(Clone, Debug, PartialEq)]
pub struct OverrideSet {
    generation: u64,
    persisted: BTreeMap<LeafPath, Stored>,
    ephemeral: BTreeMap<LeafPath, Stored>,
    /// The file could not be read: why. Every resolution fails naming it
    /// until a change replaces the file.
    unreadable: Option<String>,
}

impl OverrideSet {
    fn empty() -> Self {
        Self {
            generation: 0,
            persisted: BTreeMap::new(),
            ephemeral: BTreeMap::new(),
            unreadable: None,
        }
    }

    /// The optimistic-concurrency token.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Why the file could not be read, when it could not.
    #[must_use]
    pub fn unreadable(&self) -> Option<&str> {
        self.unreadable.as_deref()
    }

    /// Every override in force, by leaf; an ephemeral one shadows a
    /// persisted one for the same leaf.
    #[must_use]
    pub fn entries(&self) -> Vec<Entry> {
        let mut by_path: BTreeMap<&LeafPath, Entry> = BTreeMap::new();
        for (durability, map) in [
            (Durability::Persisted, &self.persisted),
            (Durability::Ephemeral, &self.ephemeral),
        ] {
            for (path, stored) in map {
                by_path.insert(
                    path,
                    Entry {
                        path: path.clone(),
                        stored: stored.clone(),
                        durability,
                    },
                );
            }
        }
        by_path.into_values().collect()
    }

    /// The override of `path`, if there is one.
    #[must_use]
    pub fn get(&self, path: &LeafPath) -> Option<(&Stored, Durability)> {
        self.ephemeral
            .get(path)
            .map(|s| (s, Durability::Ephemeral))
            .or_else(|| self.persisted.get(path).map(|s| (s, Durability::Persisted)))
    }

    /// How many leaves are overridden.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries().len()
    }

    /// Whether no leaf is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.persisted.is_empty() && self.ephemeral.is_empty()
    }

    /// What the configuration fold folds over the declared file, kept at
    /// `path`.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Parse`] naming the file when it could not be read.
    pub fn layer(&self, path: &Path) -> Result<OverrideLayer, ConfigError> {
        if let Some(why) = &self.unreadable {
            return Err(ConfigError::Parse(format!(
                "the override tier {} cannot be read ({why}); `engenho ctl config clear` replaces it",
                path.display()
            )));
        }
        Ok(OverrideLayer {
            path: path.to_path_buf(),
            values: self
                .entries()
                .into_iter()
                .map(|entry| (entry.path, entry.stored.value))
                .collect(),
        })
    }

    /// The leaves `change` removes or replaces an override of.
    #[must_use]
    pub fn touched(&self, change: &Change) -> Vec<LeafPath> {
        match change {
            Change::Set { path, .. } | Change::Unset { path } => vec![path.clone()],
            Change::Clear { prefix } => self
                .entries()
                .into_iter()
                .map(|e| e.path)
                .filter(|path| prefix.as_deref().is_none_or(|p| path.is_under(p)))
                .collect(),
        }
    }

    /// This set with `change` applied, one generation on. Any change
    /// replaces an unreadable file, so a clear is how one is recovered.
    #[must_use]
    pub fn with(&self, change: &Change) -> Self {
        let mut next = self.clone();
        next.generation = self.generation.saturating_add(1);
        next.unreadable = None;
        match change {
            Change::Set {
                path,
                stored,
                durability,
            } => {
                next.persisted.remove(path);
                next.ephemeral.remove(path);
                let map = match durability {
                    Durability::Persisted => &mut next.persisted,
                    Durability::Ephemeral => &mut next.ephemeral,
                };
                map.insert(path.clone(), stored.clone());
            }
            Change::Unset { path } => {
                next.persisted.remove(path);
                next.ephemeral.remove(path);
            }
            Change::Clear { prefix } => {
                let keep = |path: &LeafPath| prefix.as_deref().is_some_and(|p| !path.is_under(p));
                next.persisted.retain(|path, _| keep(path));
                next.ephemeral.retain(|path, _| keep(path));
            }
        }
        next
    }

    fn to_file(&self) -> OverrideFile {
        OverrideFile {
            schema: Schema::V1,
            generation: self.generation,
            entries: self.persisted.clone(),
        }
    }
}

/// Writing the override file failed.
#[derive(Debug, thiserror::Error)]
pub enum OverrideWriteError {
    /// Serializing it failed.
    #[error("serialize {FILE}: {0}")]
    Serialize(#[from] serde_yaml::Error),
    /// Writing it failed.
    #[error("write {FILE}: {0}")]
    Write(#[from] AtomicWriteError),
    /// The set changed since the change was planned.
    #[error("the override set is at generation {current}, not {planned}")]
    Moved {
        /// What the change was planned against.
        planned: u64,
        /// What it is now.
        current: u64,
    },
}

/// The override set of one data directory.
#[derive(Debug)]
pub struct OverrideStore {
    path: PathBuf,
    set: Mutex<OverrideSet>,
}

impl OverrideStore {
    /// The store kept in `control_dir` (the file need not exist).
    #[must_use]
    pub fn open(control_dir: &Path) -> Self {
        let path = control_dir.join(FILE);
        let set = match std::fs::read(&path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => OverrideSet::empty(),
            Err(err) => OverrideSet {
                unreadable: Some(err.to_string()),
                ..OverrideSet::empty()
            },
            Ok(bytes) => match serde_yaml::from_slice::<OverrideFile>(&bytes) {
                Ok(file) => OverrideSet {
                    generation: file.generation,
                    persisted: file.entries,
                    ..OverrideSet::empty()
                },
                Err(err) => {
                    tracing::warn!(path = %path.display(), error = %err, "the override tier cannot be read");
                    OverrideSet {
                        unreadable: Some(err.to_string()),
                        ..OverrideSet::empty()
                    }
                }
            },
        };
        Self {
            path,
            set: Mutex::new(set),
        }
    }

    /// Where the file is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The set as it is now.
    #[must_use]
    pub fn current(&self) -> OverrideSet {
        self.lock().clone()
    }

    /// The layer the current set folds as.
    ///
    /// # Errors
    ///
    /// As [`OverrideSet::layer`].
    pub fn layer(&self) -> Result<OverrideLayer, ConfigError> {
        self.lock().layer(&self.path)
    }

    /// Make `next` the set, writing the file, provided nothing else changed
    /// the set since `next` was derived from it.
    ///
    /// # Errors
    ///
    /// [`OverrideWriteError::Moved`] when it did; a write error leaves the
    /// set as it was.
    pub fn commit(&self, next: OverrideSet) -> Result<u64, OverrideWriteError> {
        let mut set = self.lock();
        let planned = next.generation.saturating_sub(1);
        if set.generation != planned {
            return Err(OverrideWriteError::Moved {
                planned,
                current: set.generation,
            });
        }
        let bytes = serde_yaml::to_string(&next.to_file())?;
        write_atomic_mode(&self.path, bytes.as_bytes(), 0o600)?;
        let generation = next.generation;
        *set = next;
        Ok(generation)
    }

    fn lock(&self) -> MutexGuard<'_, OverrideSet> {
        // Every critical section is a clone or a swap: a panic inside one
        // cannot leave the set half-changed.
        self.set.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use engenho_control_types::types::{AttestedView, DeclaredView};

    use super::*;

    fn by() -> PrincipalView {
        PrincipalView {
            attested: AttestedView::LocalUid {
                uid: 501,
                gid: 20,
                pid: 7,
            },
            declared: DeclaredView::Human,
        }
    }

    fn set(path: &str, value: serde_json::Value, durability: Durability) -> Change {
        Change::Set {
            path: LeafPath::parse(path).unwrap(),
            stored: Stored {
                value,
                set_by: by(),
                set_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            },
            durability,
        }
    }

    fn leaf(path: &str) -> LeafPath {
        LeafPath::parse(path).unwrap()
    }

    #[test]
    fn a_persisted_override_survives_the_process_and_an_ephemeral_one_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let store = OverrideStore::open(dir.path());
        assert_eq!(store.current().generation(), 0);

        let s = store.current();
        let s = s.with(&set(
            "scheduler.tick_interval_seconds",
            9.into(),
            Durability::Persisted,
        ));
        store.commit(s).unwrap();
        let s = store.current().with(&set(
            "controllers.namespace",
            "x".into(),
            Durability::Ephemeral,
        ));
        assert_eq!(store.commit(s).unwrap(), 2);
        assert_eq!(store.current().len(), 2);

        let mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the override tier is the daemon's alone");

        let reopened = OverrideStore::open(dir.path()).current();
        assert_eq!(
            reopened.generation(),
            2,
            "the generation outlives the process"
        );
        let entries = reopened.entries();
        assert_eq!(entries.len(), 1, "only the persisted one: {entries:?}");
        assert_eq!(entries[0].path, leaf("scheduler.tick_interval_seconds"));
        assert_eq!(entries[0].stored.set_by, by());
    }

    #[test]
    fn a_stale_plan_does_not_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = OverrideStore::open(dir.path());
        let base = store.current();
        let a = base.with(&set(
            "scheduler.namespace",
            "a".into(),
            Durability::Persisted,
        ));
        let b = base.with(&set(
            "scheduler.namespace",
            "b".into(),
            Durability::Persisted,
        ));
        store.commit(a).unwrap();
        assert!(matches!(
            store.commit(b),
            Err(OverrideWriteError::Moved {
                planned: 0,
                current: 1
            })
        ));
        let layer = store.layer().unwrap();
        assert_eq!(layer.values[&leaf("scheduler.namespace")], "a");
    }

    #[test]
    fn unset_and_clear_by_prefix() {
        let s = OverrideSet::empty()
            .with(&set(
                "runtime.tls.enabled",
                true.into(),
                Durability::Persisted,
            ))
            .with(&set(
                "runtime.tls.extra_sans",
                serde_json::json!(["a"]),
                Durability::Ephemeral,
            ))
            .with(&set(
                "runtime.listen_addr",
                "0.0.0.0:6443".into(),
                Durability::Persisted,
            ));
        let clear = Change::Clear {
            prefix: Some("runtime.tls".into()),
        };
        assert_eq!(
            s.touched(&clear),
            [leaf("runtime.tls.enabled"), leaf("runtime.tls.extra_sans")]
        );
        let cleared = s.with(&clear);
        assert_eq!(cleared.len(), 1);
        assert!(cleared.get(&leaf("runtime.listen_addr")).is_some());
        let unset = cleared.with(&Change::Unset {
            path: leaf("runtime.listen_addr"),
        });
        assert!(unset.is_empty());
        assert_eq!(unset.generation(), s.generation() + 2);
    }

    #[test]
    fn an_unreadable_file_fails_resolution_until_replaced() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE), "entries: [not, a, map]\n").unwrap();
        let store = OverrideStore::open(dir.path());
        let err = store.layer().unwrap_err();
        assert!(err.to_string().contains("config clear"), "{err}");

        let cleared = store.current().with(&Change::Clear { prefix: None });
        store.commit(cleared).unwrap();
        assert!(store.layer().unwrap().values.is_empty());
        assert!(
            OverrideStore::open(dir.path())
                .current()
                .unreadable()
                .is_none()
        );
    }
}
