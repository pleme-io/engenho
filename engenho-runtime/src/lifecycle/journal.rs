//! What the daemon remembers about its boots and its previous run.
//!
//! * [`BootJournal`] — the last [`BootJournal::CAP`] boot attempts, each with
//!   the phases it entered, how long each took and how it ended.
//! * [`RunMarker`] — whether this process, if it died now, would leave the
//!   store released. The next daemon reads it back as [`PreviousRun`].
//! * [`IdentityRecord`] — the cluster and node names as they were when the
//!   store was created, so a later rename is visible as drift.
//!
//! The serde shapes of [`BootAttempt`], [`PreviousRun`] and their parts are
//! the control API's (`BootAttempt`, `PhaseRecord`, `PreviousRun`, …).

use std::collections::VecDeque;
use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::boot::{BootKind, BootPhase, Timestamp};

/// How one phase of a boot went.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum PhaseResult {
    /// The boot is in it.
    InProgress,
    /// The boot went on to the next phase (or finished).
    Completed {
        /// How long it took.
        elapsed_ms: u64,
    },
    /// The boot failed in it.
    Failed {
        /// How long until it failed.
        elapsed_ms: u64,
        /// The error, rendered.
        error: String,
    },
    /// The boot was stopped in it.
    Cancelled {
        /// How long until it stopped.
        elapsed_ms: u64,
    },
}

/// One phase of one boot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseRecord {
    /// Which phase.
    pub phase: BootPhase,
    /// When the boot entered it.
    pub started_at: Timestamp,
    /// How it went.
    pub result: PhaseResult,
}

/// The store's [`BootKind`], once the boot has opened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BootKindObservation {
    /// The boot has not opened the store (yet).
    NotYetKnown,
    /// What it found.
    Known {
        /// Created, resumed, or in memory.
        kind: BootKind,
    },
}

/// How a boot attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptResult {
    /// It is running.
    InProgress,
    /// The runtime came up.
    Succeeded,
    /// It failed.
    Failed,
    /// It was stopped.
    Cancelled,
}

/// One boot attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootAttempt {
    /// Which boot of the daemon that made it.
    pub attempt: NonZeroU32,
    /// When it started.
    pub started_at: Timestamp,
    /// What it found in the store.
    pub kind: BootKindObservation,
    /// How it ended.
    pub result: AttemptResult,
    /// Every phase it entered, in order.
    pub phases: Vec<PhaseRecord>,
}

/// The boot journal: the most recent attempts, oldest first.
///
/// Timings come from a monotonic clock held beside the journal, not from the
/// wall-clock stamps, so a clock step during a boot cannot produce a
/// negative (or wildly long) phase.
#[derive(Debug, Default)]
pub struct BootJournal {
    attempts: VecDeque<BootAttempt>,
    /// When the current phase began, on the monotonic clock.
    phase_began: Option<tokio::time::Instant>,
}

/// The persisted form of [`BootJournal`] (`control/boot-journal.json`).
#[derive(Debug, Serialize, Deserialize)]
struct JournalFile {
    version: u32,
    attempts: VecDeque<BootAttempt>,
}

const JOURNAL_VERSION: u32 = 1;

impl BootJournal {
    /// How many attempts are kept.
    pub const CAP: usize = 16;

    /// The journal as persisted, or empty when there is none (or it cannot
    /// be read — the journal is a record, never a reason not to boot).
    #[must_use]
    pub fn from_bytes(bytes: Option<&[u8]>) -> Self {
        let attempts = bytes
            .and_then(|b| match serde_json::from_slice::<JournalFile>(b) {
                Ok(file) if file.version == JOURNAL_VERSION => Some(file.attempts),
                Ok(file) => {
                    tracing::warn!(
                        version = file.version,
                        "boot journal of an unknown version; starting a new one"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(error = %err, "unreadable boot journal; starting a new one");
                    None
                }
            })
            .unwrap_or_default();
        Self {
            attempts,
            phase_began: None,
        }
    }

    /// The journal, serialized for persisting.
    ///
    /// # Errors
    ///
    /// Serialization failed (it does not, for these types).
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec_pretty(&JournalFile {
            version: JOURNAL_VERSION,
            attempts: self.attempts.clone(),
        })
    }

    /// Every recorded attempt, oldest first.
    #[must_use]
    pub fn attempts(&self) -> Vec<BootAttempt> {
        self.attempts.iter().cloned().collect()
    }

    /// The most recent attempt.
    #[must_use]
    pub fn latest(&self) -> Option<&BootAttempt> {
        self.attempts.back()
    }

    /// A boot started.
    pub fn begin(&mut self, attempt: NonZeroU32, at: Timestamp) {
        if self.attempts.len() == Self::CAP {
            self.attempts.pop_front();
        }
        self.attempts.push_back(BootAttempt {
            attempt,
            started_at: at,
            kind: BootKindObservation::NotYetKnown,
            result: AttemptResult::InProgress,
            phases: Vec::new(),
        });
        self.phase_began = None;
    }

    /// The boot entered `phase`: the phase before it completed.
    pub fn entered(&mut self, phase: BootPhase, at: Timestamp) {
        let elapsed = self.lap();
        let Some(current) = self.attempts.back_mut() else {
            return;
        };
        if let Some(last) = current.phases.last_mut()
            && last.result == PhaseResult::InProgress
        {
            last.result = PhaseResult::Completed {
                elapsed_ms: elapsed,
            };
        }
        current.phases.push(PhaseRecord {
            phase,
            started_at: at,
            result: PhaseResult::InProgress,
        });
    }

    /// The boot opened the store and found `kind`.
    pub fn observed(&mut self, kind: BootKind) {
        if let Some(current) = self.attempts.back_mut() {
            current.kind = BootKindObservation::Known { kind };
        }
    }

    /// The boot finished: the runtime is up.
    pub fn succeeded(&mut self) {
        self.close(AttemptResult::Succeeded, |elapsed_ms| {
            PhaseResult::Completed { elapsed_ms }
        });
    }

    /// The boot failed with `error`.
    pub fn failed(&mut self, error: &str) {
        self.close(AttemptResult::Failed, |elapsed_ms| PhaseResult::Failed {
            elapsed_ms,
            error: error.to_owned(),
        });
    }

    /// The boot was stopped.
    pub fn cancelled(&mut self) {
        self.close(AttemptResult::Cancelled, |elapsed_ms| {
            PhaseResult::Cancelled { elapsed_ms }
        });
    }

    fn close(&mut self, result: AttemptResult, last: impl FnOnce(u64) -> PhaseResult) {
        let elapsed = self.lap();
        let Some(current) = self.attempts.back_mut() else {
            return;
        };
        if current.result != AttemptResult::InProgress {
            return;
        }
        current.result = result;
        if let Some(phase) = current.phases.last_mut()
            && phase.result == PhaseResult::InProgress
        {
            phase.result = last(elapsed);
        }
        self.phase_began = None;
    }

    /// Milliseconds since the current phase began; restarts the lap.
    fn lap(&mut self) -> u64 {
        let now = tokio::time::Instant::now();
        let elapsed = self.phase_began.map_or(0, |began| {
            u64::try_from(now.duration_since(began).as_millis()).unwrap_or(u64::MAX)
        });
        self.phase_began = Some(now);
        elapsed
    }
}

/// What the daemon was doing, as far as the store goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "seen", rename_all = "snake_case")]
pub enum LastSeen {
    /// Booting, in this phase.
    Booting {
        /// Which.
        phase: BootPhase,
    },
    /// Running.
    Running,
    /// Draining.
    Draining,
}

/// Whether this process, if it died now, would leave the store released
/// (`control/run.json`). Rewritten as the lifecycle moves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunMarker {
    /// The store may be open.
    Live {
        /// What the daemon was doing.
        last_seen: LastSeen,
        /// The process.
        pid: u32,
    },
    /// The store is released.
    Released {
        /// Since when.
        at: Timestamp,
    },
}

/// How the previous daemon process ended, read from its [`RunMarker`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "previous", rename_all = "snake_case")]
pub enum PreviousRun {
    /// No daemon has run over this data directory before.
    FirstEver,
    /// It ended with the store released.
    CleanStop {
        /// When the store was last released.
        at: Timestamp,
    },
    /// It ended with the store possibly open: killed, crashed, or lost power.
    Unclean {
        /// What it was doing.
        last_seen: LastSeen,
    },
}

impl PreviousRun {
    /// Read back from the marker the previous process left.
    #[must_use]
    pub fn from_marker(marker: Option<&RunMarker>) -> Self {
        match marker {
            None => Self::FirstEver,
            Some(RunMarker::Released { at }) => Self::CleanStop { at: *at },
            Some(RunMarker::Live { last_seen, .. }) => Self::Unclean {
                last_seen: *last_seen,
            },
        }
    }
}

/// The cluster and node names when the store was created
/// (`control/identity.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRecord {
    /// `cluster.name` at first boot.
    pub cluster_name: String,
    /// `runtime.node_name` at first boot.
    pub node_name: String,
    /// When.
    pub recorded_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: u64) -> Timestamp {
        Timestamp::parse("2026-09-22T00:00:00Z")
            .expect("literal")
            .after(std::time::Duration::from_secs(secs))
    }

    #[tokio::test(start_paused = true)]
    async fn phases_are_timed_on_the_monotonic_clock_and_closed_by_the_outcome() {
        let mut journal = BootJournal::default();
        journal.begin(NonZeroU32::MIN, t(0));
        journal.entered(BootPhase::ResolveConfig, t(0));
        tokio::time::advance(std::time::Duration::from_millis(250)).await;
        journal.entered(BootPhase::ReadBootConfig, t(0));
        journal.observed(BootKind::Resume);
        tokio::time::advance(std::time::Duration::from_millis(40)).await;
        journal.failed("nope");
        let latest = journal.latest().expect("an attempt");
        assert_eq!(latest.result, AttemptResult::Failed);
        assert_eq!(
            latest.kind,
            BootKindObservation::Known {
                kind: BootKind::Resume
            }
        );
        assert_eq!(
            latest
                .phases
                .iter()
                .map(|p| p.result.clone())
                .collect::<Vec<_>>(),
            [
                PhaseResult::Completed { elapsed_ms: 250 },
                PhaseResult::Failed {
                    elapsed_ms: 40,
                    error: "nope".into()
                },
            ]
        );
    }

    #[test]
    fn the_journal_keeps_the_most_recent_attempts_and_survives_a_round_trip() {
        let mut journal = BootJournal::default();
        for n in 1..=20u32 {
            journal.begin(NonZeroU32::new(n).expect("non-zero"), t(u64::from(n)));
            journal.cancelled();
        }
        let attempts = journal.attempts();
        assert_eq!(attempts.len(), BootJournal::CAP);
        assert_eq!(attempts[0].attempt.get(), 5);
        let bytes = journal.to_bytes().expect("serialize");
        assert_eq!(BootJournal::from_bytes(Some(&bytes)).attempts(), attempts);
        assert!(
            BootJournal::from_bytes(Some(b"not json"))
                .attempts()
                .is_empty()
        );
        assert!(BootJournal::from_bytes(None).attempts().is_empty());
    }

    #[test]
    fn the_previous_run_is_read_from_the_marker_it_left() {
        assert_eq!(PreviousRun::from_marker(None), PreviousRun::FirstEver);
        assert_eq!(
            PreviousRun::from_marker(Some(&RunMarker::Released { at: t(1) })),
            PreviousRun::CleanStop { at: t(1) }
        );
        let live = RunMarker::Live {
            last_seen: LastSeen::Booting {
                phase: BootPhase::OpenStore,
            },
            pid: 7,
        };
        assert_eq!(
            PreviousRun::from_marker(Some(&live)),
            PreviousRun::Unclean {
                last_seen: LastSeen::Booting {
                    phase: BootPhase::OpenStore
                }
            }
        );
        assert_eq!(
            serde_json::to_value(PreviousRun::Unclean {
                last_seen: LastSeen::Running
            })
            .expect("serialize"),
            serde_json::json!({"previous": "unclean", "last_seen": {"seen": "running"}})
        );
    }
}
