//! Typed errors for scheduling operations.

use engenho_config::SchedulerStrategyKind;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("store error during list/patch: {0}")]
    Store(#[from] engenho_store::StoreError),

    #[error("no schedulable nodes available")]
    NoSchedulableNodes,

    #[error("pod metadata.name missing or invalid")]
    InvalidPodMetadata,

    /// The operator's config asked for a scheduling strategy that is
    /// designed but not yet implemented. Surfaced as a typed error at
    /// strategy-construction time rather than a silent downgrade to
    /// round-robin (a warn-then-wrong-answer). Per the TYPED-SPEC +
    /// INTERPRETER TRIPLET rule, an unimplemented surface returns a
    /// typed error so the gap is mechanical, not silent.
    #[error("scheduling strategy {requested:?} is not implemented (no silent fallback)")]
    UnsupportedStrategy {
        /// The strategy the operator requested.
        requested: SchedulerStrategyKind,
    },

    /// `scheduler.tick_interval_seconds` was zero. It is the scheduler's
    /// fallback tick, the one that runs when no Pod or Node event wakes it,
    /// and a zero interval would re-run the scheduler back to back.
    #[error(
        "scheduler.tick_interval_seconds is 0; the fallback tick would re-run the scheduler back to back"
    )]
    ZeroTickInterval,

    #[error("internal: {0}")]
    Internal(String),
}

impl SchedulerError {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Store(_) => "store",
            Self::NoSchedulableNodes => "no_schedulable_nodes",
            Self::InvalidPodMetadata => "invalid_pod_metadata",
            Self::UnsupportedStrategy { .. } => "unsupported_strategy",
            Self::ZeroTickInterval => "zero_tick_interval",
            Self::Internal(_) => "internal",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kind_is_stable() {
        for (e, k) in [
            (
                SchedulerError::Store(engenho_store::StoreError::ClientWriteFailed("x".into())),
                "store",
            ),
            (SchedulerError::NoSchedulableNodes, "no_schedulable_nodes"),
            (SchedulerError::InvalidPodMetadata, "invalid_pod_metadata"),
            (
                SchedulerError::UnsupportedStrategy {
                    requested: SchedulerStrategyKind::BinPack,
                },
                "unsupported_strategy",
            ),
            (SchedulerError::ZeroTickInterval, "zero_tick_interval"),
            (SchedulerError::Internal("x".into()), "internal"),
        ] {
            assert_eq!(e.kind(), k);
        }
    }
}
