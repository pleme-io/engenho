//! What a failed boot means for the next one.
//!
//! A daemon that exits on a boot error hands the decision to its service
//! manager, which knows nothing about the error: launchd and systemd restart
//! it on a timer, forever, whether the cause was a port another process held
//! for two seconds or a typo in the config that no number of restarts will
//! fix. The supervisor stays up instead, and decides here.
//!
//! [`FailureClass::of`] is one exhaustive match over [`RuntimeError`] — and,
//! where the cause lives one level down, over the store's and the
//! apiserver's own error — so a new error variant does not compile until
//! someone has said whether waiting fixes it.

use std::time::Duration;

use engenho_apiserver::ServerError;
use engenho_store::StoreError;
use serde::{Deserialize, Serialize};

use crate::error::RuntimeError;

/// Whether retrying the same boot, unchanged, can succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// The cause is outside this configuration and may clear by itself: a
    /// port still held, a store another process has open, a container
    /// runtime not up yet. Retried on [`backoff`].
    Backoff,
    /// The same configuration will fail the same way. Held until something
    /// changes: the declared file, an override, or an operator's retry.
    Hold,
}

impl FailureClass {
    /// Classify why a boot failed.
    #[must_use]
    pub fn of(error: &RuntimeError) -> Self {
        match error {
            // Held: the configuration (or what it names) is the cause.
            RuntimeError::Config(_)
            | RuntimeError::Unhonoured(_)
            | RuntimeError::Scheduler(_)
            | RuntimeError::BackendRefused(_)
            | RuntimeError::ListenAddr { .. }
            | RuntimeError::ExtraSan { .. }
            | RuntimeError::PublicCaOnReachableAddress { .. }
            | RuntimeError::Kubeconfig(_)
            | RuntimeError::DataDirMoved { .. }
            // A boot someone stopped is not retried on a timer.
            | RuntimeError::BootCancelled { .. }
            // Never produced by a boot (a stop's error); retrying cannot
            // release a store something else holds.
            | RuntimeError::StoreStillShared { .. } => Self::Hold,

            // Backoff: something outside the configuration, likely transient.
            RuntimeError::LeadershipTimeout { .. }
            | RuntimeError::ContainerRuntimeUnavailable { .. }
            | RuntimeError::KubeconfigIo { .. }
            | RuntimeError::NodeRegistration(_) => Self::Backoff,

            RuntimeError::Store(store) => Self::of_store(store),
            RuntimeError::Server(server) => Self::of_server(server),
        }
    }

    const fn of_store(error: &StoreError) -> Self {
        match error {
            // The raft configuration is built from ours: it fails the same way.
            StoreError::ConfigInvalid(_) => Self::Hold,
            // `Fatal` is what a store another process holds reports, and what
            // an unreadable keyspace reports. The first clears when that
            // process exits; the second costs one attempt a minute.
            StoreError::InitializeFailed(_)
            | StoreError::ClientWriteFailed(_)
            | StoreError::Fatal(_)
            | StoreError::Persist(_) => Self::Backoff,
        }
    }

    const fn of_server(error: &ServerError) -> Self {
        match error {
            // A port another process holds, or an interface not up yet.
            ServerError::Bind(_) | ServerError::Serve(_) => Self::Backoff,
            // Certificates built from our own material fail the same way.
            ServerError::Tls(_) | ServerError::Pki(_) => Self::Hold,
        }
    }
}

/// The first retry's delay.
pub const BACKOFF_FLOOR: Duration = Duration::from_secs(1);

/// No retry waits longer than this.
pub const BACKOFF_CAP: Duration = Duration::from_secs(60);

/// The delay before retrying after the `streak`-th consecutive failure:
/// [`BACKOFF_FLOOR`], doubling, capped at [`BACKOFF_CAP`]. A streak of zero
/// is treated as one.
#[must_use]
pub fn backoff(streak: u32) -> Duration {
    let doublings = streak.saturating_sub(1).min(16);
    BACKOFF_FLOOR
        .saturating_mul(1 << doublings)
        .min(BACKOFF_CAP)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_starts_at_the_floor_and_doubles_to_the_cap() {
        let seconds: Vec<u64> = (1..=9).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(seconds, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(backoff(0), BACKOFF_FLOOR);
        assert_eq!(backoff(u32::MAX), BACKOFF_CAP);
    }

    #[test]
    fn a_port_someone_else_holds_is_waited_out() {
        let bind = RuntimeError::Server(ServerError::Bind(std::io::Error::from(
            std::io::ErrorKind::AddrInUse,
        )));
        assert_eq!(FailureClass::of(&bind), FailureClass::Backoff);
    }

    #[test]
    fn a_config_error_is_held_not_retried() {
        let config = RuntimeError::Config(engenho_config::ConfigError::Parse("bad".into()));
        assert_eq!(FailureClass::of(&config), FailureClass::Hold);
        let cancelled = RuntimeError::BootCancelled {
            phase: crate::boot::BootPhase::AwaitLeadership,
        };
        assert_eq!(FailureClass::of(&cancelled), FailureClass::Hold);
    }
}
