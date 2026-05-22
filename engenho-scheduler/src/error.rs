//! Typed errors for scheduling operations.

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("store error during list/patch: {0}")]
    Store(String),

    #[error("no schedulable nodes available")]
    NoSchedulableNodes,

    #[error("pod metadata.name missing or invalid")]
    InvalidPodMetadata,

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
            (SchedulerError::Store("x".into()), "store"),
            (SchedulerError::NoSchedulableNodes, "no_schedulable_nodes"),
            (SchedulerError::InvalidPodMetadata, "invalid_pod_metadata"),
            (SchedulerError::Internal("x".into()), "internal"),
        ] {
            assert_eq!(e.kind(), k);
        }
    }
}
