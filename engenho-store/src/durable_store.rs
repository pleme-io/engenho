//! C4c — durable wrapper around `InMemoryStore` that appends every
//! committed mutation to a [`PersistentLog`] + replays on construction.
//!
//! ## Scope
//!
//! This layer is the bridge between C4b (typed persistent log) and
//! a future full-openraft `RaftLogStorage` impl. It doesn't replace
//! openraft's storage traits — those still live on `InMemoryStore`.
//! Instead, it WRAPS the in-memory state machine + journals every
//! committed `ResourceCommand` to disk.
//!
//! On node restart:
//!   1. `DurableInMemoryStore::open(path)` loads the existing
//!      log file (or creates one if absent).
//!   2. The log is replayed; each entry re-applied to a fresh
//!      `ResourceCatalog`. The state machine ends up byte-identical
//!      to its pre-restart state.
//!   3. New mutations append to the log + apply to memory in lockstep.
//!
//! ## Crash semantics
//!
//! Each `append` calls `fsync_all()` so a crash mid-write leaves
//! at most one partially-written entry, which `PersistentLog::replay`
//! truncates. The catalog state never diverges from the disk log.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::command::ResourceCommand;
use crate::persistent_log::{PersistentLog, PersistentLogError};
use crate::resource::ResourceKey;
use crate::state::ResourceCatalog;

/// A typed log entry — what we journal to disk on every commit.
/// Distinct from openraft's internal log entry shape so future
/// log-format bumps don't break openraft compatibility.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JournalEntry {
    /// Raft term + log index when this command was committed.
    pub term: u64,
    /// Raft log index.
    pub index: u64,
    /// The committed resource mutation.
    pub command: ResourceCommand,
}

/// Durable wrapper around the in-memory catalog.
#[derive(Debug)]
pub struct DurableInMemoryStore {
    catalog: Arc<Mutex<ResourceCatalog>>,
    log: PersistentLog,
}

impl DurableInMemoryStore {
    /// Open or create a durable store at `log_path`. Replays the
    /// existing log into a fresh catalog if the file exists.
    ///
    /// # Errors
    ///
    /// Propagates [`PersistentLogError`] on I/O or replay failure.
    pub fn open(log_path: impl Into<std::path::PathBuf>) -> Result<Self, PersistentLogError> {
        let log = PersistentLog::open(log_path)?;
        let entries: Vec<(u64, JournalEntry)> = log.replay()?;
        let mut catalog = ResourceCatalog::default();
        for (_, entry) in entries {
            catalog.apply(&entry.command, entry.term, entry.index);
        }
        Ok(Self {
            catalog: Arc::new(Mutex::new(catalog)),
            log,
        })
    }

    /// Apply a command + journal it. Returns the resource op the
    /// catalog produced (Created/Replaced/Patched/Deleted/NoOp).
    ///
    /// # Errors
    ///
    /// [`PersistentLogError::Encode`] on serialization failure or
    /// [`PersistentLogError::Io`] on disk failure. The catalog is
    /// NOT updated when the journal write fails — we keep memory +
    /// disk in lockstep.
    pub async fn apply_and_journal(
        &self,
        command: &ResourceCommand,
        term: u64,
        index: u64,
    ) -> Result<crate::command::ResourceOp, PersistentLogError> {
        let entry = JournalEntry {
            term,
            index,
            command: command.clone(),
        };
        // Journal FIRST; catalog state must never get ahead of disk.
        self.log.append(index, &entry)?;
        let mut catalog = self.catalog.lock().await;
        Ok(catalog.apply(command, term, index))
    }

    /// Read a resource from the in-memory catalog.
    pub async fn get(&self, key: &ResourceKey) -> Option<serde_json::Value> {
        self.catalog.lock().await.get(key).cloned()
    }

    /// Snapshot of the current catalog (for tests + telemetry).
    pub async fn snapshot(&self) -> ResourceCatalog {
        self.catalog.lock().await.clone()
    }

    /// Path the log writes to.
    #[must_use]
    pub fn log_path(&self) -> &Path {
        self.log.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{Reason, ResourceOp};
    use serde_json::json;

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "engenho-durable-{}-{suffix}.log",
            std::process::id()
        ))
    }

    fn pod_key(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    #[tokio::test]
    async fn open_creates_empty_store() {
        let path = temp_path("open-empty");
        let _ = std::fs::remove_file(&path);
        let store = DurableInMemoryStore::open(&path).unwrap();
        assert!(store.get(&pod_key("nothing")).await.is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn apply_and_journal_persists_through_restart() {
        let path = temp_path("restart");
        let _ = std::fs::remove_file(&path);

        // First lifetime: write 3 pods.
        {
            let store = DurableInMemoryStore::open(&path).unwrap();
            for (i, name) in ["a", "b", "c"].iter().enumerate() {
                let op = store
                    .apply_and_journal(
                        &ResourceCommand::Put {
                            key: pod_key(name),
                            value: json!({"spec": {"image": format!("v{}", i)}}),
                            reason: Reason::Operator,
                        },
                        1,
                        (i + 1) as u64,
                    )
                    .await
                    .unwrap();
                assert_eq!(op, ResourceOp::Created);
            }
        } // first store drops here; log persists on disk

        // Second lifetime: reopen, replay should restore the catalog.
        let store2 = DurableInMemoryStore::open(&path).unwrap();
        for name in ["a", "b", "c"] {
            let v = store2.get(&pod_key(name)).await;
            assert!(v.is_some(), "pod {name} survived restart");
        }
        // The replayed image for "c" should be "v2".
        let c = store2.get(&pod_key("c")).await.unwrap();
        assert_eq!(c.get("spec").unwrap().get("image").unwrap(), "v2");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn delete_command_journaled_and_replayed() {
        let path = temp_path("delete-replay");
        let _ = std::fs::remove_file(&path);

        {
            let store = DurableInMemoryStore::open(&path).unwrap();
            store
                .apply_and_journal(
                    &ResourceCommand::Put {
                        key: pod_key("doomed"),
                        value: json!({"spec": {}}),
                        reason: Reason::Operator,
                    },
                    1,
                    1,
                )
                .await
                .unwrap();
            store
                .apply_and_journal(
                    &ResourceCommand::Delete {
                        key: pod_key("doomed"),
                        reason: Reason::Operator,
                    },
                    1,
                    2,
                )
                .await
                .unwrap();
        }

        let store2 = DurableInMemoryStore::open(&path).unwrap();
        assert!(
            store2.get(&pod_key("doomed")).await.is_none(),
            "delete survived restart"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn patch_command_journaled_and_replayed() {
        let path = temp_path("patch-replay");
        let _ = std::fs::remove_file(&path);

        {
            let store = DurableInMemoryStore::open(&path).unwrap();
            store
                .apply_and_journal(
                    &ResourceCommand::Put {
                        key: pod_key("p"),
                        value: json!({"spec": {"image": "v1", "replicas": 1}}),
                        reason: Reason::Operator,
                    },
                    1,
                    1,
                )
                .await
                .unwrap();
            store
                .apply_and_journal(
                    &ResourceCommand::Patch {
                        key: pod_key("p"),
                        patch: json!({"spec": {"replicas": 5}}),
                        reason: Reason::Operator,
                    },
                    1,
                    2,
                )
                .await
                .unwrap();
        }

        let store2 = DurableInMemoryStore::open(&path).unwrap();
        let pod = store2.get(&pod_key("p")).await.unwrap();
        assert_eq!(pod.get("spec").unwrap().get("image").unwrap(), "v1");
        assert_eq!(pod.get("spec").unwrap().get("replicas").unwrap(), 5);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn replay_after_many_writes() {
        let path = temp_path("many");
        let _ = std::fs::remove_file(&path);
        {
            let store = DurableInMemoryStore::open(&path).unwrap();
            for i in 0..100 {
                store
                    .apply_and_journal(
                        &ResourceCommand::Put {
                            key: pod_key(&format!("p{i}")),
                            value: json!({"spec": {"image": format!("v{i}")}}),
                            reason: Reason::Operator,
                        },
                        1,
                        (i + 1) as u64,
                    )
                    .await
                    .unwrap();
            }
        }
        let store2 = DurableInMemoryStore::open(&path).unwrap();
        for i in 0..100 {
            assert!(store2.get(&pod_key(&format!("p{i}"))).await.is_some());
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn journal_entry_serde_round_trip() {
        let entry = JournalEntry {
            term: 5,
            index: 42,
            command: ResourceCommand::Put {
                key: pod_key("x"),
                value: json!({"spec": {}}),
                reason: Reason::Operator,
            },
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let back: JournalEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, entry);
    }
}
