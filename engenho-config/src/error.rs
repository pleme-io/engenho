//! Typed config errors.

use thiserror::Error;

use crate::node_local::{ListenerAddrRejection, NodeLocalListener};

/// Errors a config can return at parse/validate time.
#[derive(Debug, Clone, Error)]
pub enum ConfigError {
    /// YAML parse error or merge round-trip failure.
    #[error("config parse error: {0}")]
    Parse(String),

    /// Cross-section invariant violation (e.g. `Quorum3M` + `min_nodes` < 3).
    #[error("incoherent config: {0}")]
    Incoherent(String),

    /// Field-level validation error (e.g. zero tick interval).
    #[error("invalid field {field}: {reason}")]
    InvalidField {
        /// The field path that failed validation.
        field: String,
        /// Why it failed.
        reason: String,
    },

    /// A node-local listener was given an address it may not bind. It cannot
    /// authenticate its callers yet, so it binds loopback only (T4.9).
    #[error(
        "invalid field {}: {rejection}; {listener} has no {} yet, so it binds loopback only",
        .listener.field(),
        .listener.missing_gate()
    )]
    NodeLocalListener {
        /// Which listener.
        listener: NodeLocalListener,
        /// Why its address was refused.
        rejection: ListenerAddrRejection,
    },
}

impl ConfigError {
    /// Stable identifier for telemetry / cross-language SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Parse(_) => "parse",
            Self::Incoherent(_) => "incoherent",
            Self::InvalidField { .. } => "invalid_field",
            Self::NodeLocalListener { .. } => "node_local_listener",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_stable() {
        assert_eq!(ConfigError::Parse("x".into()).kind(), "parse");
        assert_eq!(ConfigError::Incoherent("x".into()).kind(), "incoherent");
        assert_eq!(
            ConfigError::InvalidField {
                field: "x".into(),
                reason: "y".into(),
            }
            .kind(),
            "invalid_field"
        );
        assert_eq!(
            ConfigError::NodeLocalListener {
                listener: NodeLocalListener::Kubelet,
                rejection: ListenerAddrRejection::NotALiteral("x".into()),
            }
            .kind(),
            "node_local_listener"
        );
    }
}
