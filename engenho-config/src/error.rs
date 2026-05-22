//! Typed config errors.

use thiserror::Error;

/// Errors a config can return at parse/validate time.
#[derive(Debug, Clone, Error)]
pub enum ConfigError {
    /// YAML parse error or merge round-trip failure.
    #[error("config parse error: {0}")]
    Parse(String),

    /// Cross-section invariant violation (e.g. Quorum3M + min_nodes < 3).
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
}

impl ConfigError {
    /// Stable identifier for telemetry / cross-language SDK dispatch.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Parse(_) => "parse",
            Self::Incoherent(_) => "incoherent",
            Self::InvalidField { .. } => "invalid_field",
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
    }
}
