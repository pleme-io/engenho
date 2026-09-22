//! The audit chain.
//!
//! Every call at mutate authority or above, every call whose response is
//! sensitive, and every refusal is recorded — a call that changes something
//! twice: its intent before it runs and its result after, so a crash between
//! the two leaves the intent standing alone rather than nothing.
//!
//! Records are the spec's own [`AuditRecord`], one JSON line each, appended
//! to `audit.jsonl` and fsync'd before the call proceeds. Each carries the
//! BLAKE3 of the line before it (`prev`), so an edited, removed or reordered
//! line breaks the chain at that record ([`verify_chain`]). Parameters are
//! never written, only their BLAKE3 — a secret passed to a call does not
//! land in the log.

use std::collections::VecDeque;
use std::io::{BufRead, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use engenho_control_types::types::{AuditPhase, AuditRecord, Blake3Hex, PrincipalView, Replay};
use engenho_control_types::wire::HttpParts;
use engenho_control_types::{AuthorityTier, OperationId, Principal};

/// Where calls are recorded.
pub trait Audit: Send + Sync {
    /// Whether a call is recorded before and after it runs: mutate authority
    /// or above, or a sensitive response. Refusals are always recorded.
    fn wants(&self, by: &Principal, id: OperationId) -> bool {
        let _ = by;
        let row = id.spec();
        row.tier >= AuthorityTier::Mutate || row.sensitive
    }

    /// Record one entry.
    fn record(&self, entry: AuditEntry);
}

/// Records nothing (tests, and callers that audit elsewhere).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoAudit;

impl Audit for NoAudit {
    fn wants(&self, _: &Principal, _: OperationId) -> bool {
        false
    }

    fn record(&self, _: AuditEntry) {}
}

/// One entry, before the chain gives it a sequence number and a link.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditEntry {
    principal: PrincipalView,
    operation: OperationId,
    phase: AuditPhase,
    params_blake3: String,
    http_status: Option<u16>,
    confirmation: Option<String>,
}

impl AuditEntry {
    /// A call about to run.
    #[must_use]
    pub fn intent(by: &Principal, id: OperationId, parts: &HttpParts) -> Self {
        let confirmation = parts.header("engenho-confirmation").map(str::to_owned);
        Self {
            principal: by.view(),
            operation: id,
            phase: AuditPhase::Intent,
            params_blake3: params_digest(parts),
            http_status: None,
            confirmation,
        }
    }

    /// The same call, finished with `status`.
    #[must_use]
    pub fn result(self, status: u16) -> Self {
        Self {
            phase: AuditPhase::Result,
            http_status: Some(status),
            ..self
        }
    }

    /// A call refused before it ran.
    #[must_use]
    pub fn refused(by: &Principal, id: OperationId, status: u16) -> Self {
        Self {
            principal: by.view(),
            operation: id,
            phase: AuditPhase::Refused,
            params_blake3: blake3::hash(b"").to_hex().to_string(),
            http_status: Some(status),
            confirmation: None,
        }
    }
}

/// The BLAKE3 of a call's parameters: its path parameters, query and body,
/// canonically serialized. Headers are left out (they carry the caller's
/// declarations, recorded in the principal).
fn params_digest(parts: &HttpParts) -> String {
    let canonical = serde_json::json!({
        "path": parts.path_params,
        "query": parts.query,
        "body": parts.body,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

/// How many recent records are kept in memory for paging.
pub const RECENT: usize = 1024;

/// The file-backed chain.
#[derive(Debug)]
pub struct AuditLog {
    path: PathBuf,
    state: Mutex<ChainState>,
}

#[derive(Debug)]
struct ChainState {
    file: std::fs::File,
    next_seq: NonZeroU64,
    prev: String,
    recent: VecDeque<AuditRecord>,
}

/// Why the audit log could not be opened.
#[derive(Debug, thiserror::Error)]
#[error("audit log {}: {source}", path.display())]
pub struct AuditOpenError {
    path: PathBuf,
    source: std::io::Error,
}

/// The `prev` of the first record.
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

impl AuditLog {
    /// The file under `dir`.
    pub const FILE: &'static str = "audit.jsonl";

    /// Open (or start) the chain in `dir`, continuing from its last record.
    ///
    /// # Errors
    ///
    /// The directory or the file cannot be created or read.
    pub fn open(dir: &Path) -> Result<Self, AuditOpenError> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let path = dir.join(Self::FILE);
        let err = |source| AuditOpenError {
            path: path.clone(),
            source,
        };
        std::fs::create_dir_all(dir).map_err(err)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(err)?;
        let mut next_seq = NonZeroU64::MIN;
        let mut prev = GENESIS.to_string();
        let mut recent = VecDeque::new();
        if let Ok(existing) = std::fs::File::open(&path) {
            for line in std::io::BufReader::new(existing).lines() {
                let line = line.map_err(err)?;
                match serde_json::from_str::<AuditRecord>(&line) {
                    Ok(record) => {
                        next_seq = record.seq.saturating_add(1);
                        prev = blake3::hash(line.as_bytes()).to_hex().to_string();
                        if recent.len() == RECENT {
                            recent.pop_front();
                        }
                        recent.push_back(record);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "unreadable audit line; the chain continues after it");
                    }
                }
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .map_err(err)?;
        Ok(Self {
            path,
            state: Mutex::new(ChainState {
                file,
                next_seq,
                prev,
                recent,
            }),
        })
    }

    /// The file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Records after `after`, at most `limit`, with the cursor to continue
    /// from and whether older records fell out of memory.
    #[must_use]
    pub fn page(&self, after: u64, limit: usize) -> (Vec<AuditRecord>, u64, Replay) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let oldest = state
            .recent
            .front()
            .map_or(state.next_seq.get(), |r| r.seq.get());
        let replay = if after.saturating_add(1) < oldest && after < state.next_seq.get() - 1 {
            Replay::Truncated(oldest.saturating_sub(1))
        } else {
            Replay::Complete
        };
        let records: Vec<AuditRecord> = state
            .recent
            .iter()
            .filter(|r| r.seq.get() > after)
            .take(limit)
            .cloned()
            .collect();
        let next = records.last().map_or(after, |r| r.seq.get());
        (records, next, replay)
    }
}

impl Audit for AuditLog {
    fn record(&self, entry: AuditEntry) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (Ok(prev), Ok(params)) = (
            Blake3Hex::try_from(state.prev.as_str()),
            Blake3Hex::try_from(entry.params_blake3.as_str()),
        ) else {
            tracing::error!("an audit digest is not BLAKE3 hex; record dropped");
            return;
        };
        let record = AuditRecord {
            seq: state.next_seq,
            at: chrono::Utc::now(),
            prev,
            principal: entry.principal,
            operation: entry.operation.as_str().to_owned(),
            tier: entry.operation.spec().tier,
            phase: entry.phase,
            params_blake3: params,
            http_status: entry.http_status,
            confirmation: entry
                .confirmation
                .and_then(|c| engenho_control_types::types::ConfirmationId::try_from(c).ok()),
        };
        let Ok(mut line) = serde_json::to_vec(&record) else {
            return;
        };
        let digest = blake3::hash(&line).to_hex().to_string();
        line.push(b'\n');
        if let Err(err) = state
            .file
            .write_all(&line)
            .and_then(|()| state.file.sync_data())
        {
            tracing::error!(error = %err, "cannot append to the audit log");
            return;
        }
        state.prev = digest;
        state.next_seq = state.next_seq.saturating_add(1);
        if state.recent.len() == RECENT {
            state.recent.pop_front();
        }
        state.recent.push_back(record);
    }
}

/// Where a chain is broken.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChainBreak {
    /// A line is not a record.
    #[error("line {line} is not an audit record")]
    Unreadable {
        /// The 1-based line.
        line: usize,
    },
    /// A record's `prev` is not the BLAKE3 of the line before it.
    #[error("record {seq} does not link to the record before it")]
    Unlinked {
        /// The record's sequence number.
        seq: u64,
    },
    /// A record's sequence number is not one more than the one before.
    #[error("record {seq} is out of sequence")]
    OutOfSequence {
        /// The record's sequence number.
        seq: u64,
    },
}

/// Walk the chain in `path`; the number of records when it is whole.
///
/// # Errors
///
/// The first [`ChainBreak`]; an unreadable file is treated as empty.
pub fn verify_chain(path: &Path) -> Result<u64, ChainBreak> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let mut prev = GENESIS.to_string();
    let mut expected = 1u64;
    for (i, line) in text.lines().enumerate() {
        let record: AuditRecord =
            serde_json::from_str(line).map_err(|_| ChainBreak::Unreadable { line: i + 1 })?;
        if record.seq.get() != expected {
            return Err(ChainBreak::OutOfSequence {
                seq: record.seq.get(),
            });
        }
        if record.prev.as_str() != prev {
            return Err(ChainBreak::Unlinked {
                seq: record.seq.get(),
            });
        }
        prev = blake3::hash(line.as_bytes()).to_hex().to_string();
        expected += 1;
    }
    Ok(expected - 1)
}

#[cfg(test)]
mod tests {
    use engenho_control_types::types::{AttestedView, DeclaredView, GrantView};

    use super::*;

    fn by() -> Principal {
        Principal::mint(
            AttestedView::LocalUid {
                uid: 501,
                gid: 20,
                pid: 9,
            },
            DeclaredView::Human,
            GrantView {
                granted: AuthorityTier::Destructive,
                effective: AuthorityTier::Destructive,
            },
        )
    }

    fn write_three(dir: &Path) -> AuditLog {
        let log = AuditLog::open(dir).expect("open");
        let parts = HttpParts::default();
        let intent = AuditEntry::intent(&by(), OperationId::StopRuntime, &parts);
        log.record(intent.clone());
        log.record(intent.result(200));
        log.record(AuditEntry::refused(&by(), OperationId::WipeStore, 403));
        log
    }

    #[test]
    fn the_chain_links_every_record_and_survives_a_reopen() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log = write_three(tmp.path());
        assert_eq!(verify_chain(log.path()), Ok(3));
        let (records, next, replay) = log.page(0, 10);
        assert_eq!(records.len(), 3);
        assert_eq!(next, 3);
        assert_eq!(replay, Replay::Complete);
        assert_eq!(records[1].phase, AuditPhase::Result);
        assert_eq!(records[1].http_status, Some(200));
        drop(log);

        let reopened = AuditLog::open(tmp.path()).expect("reopen");
        reopened.record(AuditEntry::refused(&by(), OperationId::ReseedPki, 403));
        assert_eq!(verify_chain(reopened.path()), Ok(4));
        let mode = std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(reopened.path())
                .expect("stat")
                .permissions(),
        );
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn an_edited_record_breaks_the_chain_where_it_was_edited() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log = write_three(tmp.path());
        let path = log.path().to_path_buf();
        drop(log);
        let text = std::fs::read_to_string(&path).expect("read");
        let tampered = text.replacen("\"result\"", "\"intent\"", 1);
        assert_ne!(text, tampered);
        std::fs::write(&path, tampered).expect("write");
        // Record 2 was edited, so record 3's link no longer matches it.
        assert_eq!(verify_chain(&path), Err(ChainBreak::Unlinked { seq: 3 }));

        let lines: Vec<&str> = text.lines().collect();
        std::fs::write(&path, format!("{}\n{}\n", lines[0], lines[2])).expect("write");
        assert_eq!(
            verify_chain(&path),
            Err(ChainBreak::OutOfSequence { seq: 3 })
        );
    }

    #[test]
    fn only_mutations_and_sensitive_reads_are_audited() {
        let log = NoAudit;
        assert!(!log.wants(&by(), OperationId::GetRuntime));
        let tmp = tempfile::tempdir().expect("tempdir");
        let chain = AuditLog::open(tmp.path()).expect("open");
        assert!(!chain.wants(&by(), OperationId::GetRuntime));
        assert!(chain.wants(&by(), OperationId::StopRuntime));
        assert!(chain.wants(&by(), OperationId::WipeStore));
    }

    #[test]
    fn parameters_are_hashed_never_written() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let log = AuditLog::open(tmp.path()).expect("open");
        let parts = HttpParts {
            body: Some(serde_json::json!({"value": "s3cr3t-token"})),
            ..HttpParts::default()
        };
        log.record(AuditEntry::intent(
            &by(),
            OperationId::SetConfigLeaf,
            &parts,
        ));
        let text = std::fs::read_to_string(log.path()).expect("read");
        assert!(!text.contains("s3cr3t"), "{text}");
    }
}
