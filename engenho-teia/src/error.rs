//! Typed errors for teia operations.

use std::sync::Arc;

#[derive(Debug, thiserror::Error, Clone)]
pub enum TeiaError {
    #[error("connect to NATS failed: {0}")]
    ConnectFailed(String),

    #[error("publish to {subject} failed: {detail}")]
    PublishFailed { subject: String, detail: String },

    #[error("subscribe to {subject} failed: {detail}")]
    SubscribeFailed { subject: String, detail: String },

    #[error("request to {subject} failed: {detail}")]
    RequestFailed { subject: String, detail: String },

    #[error("encode payload: {0}")]
    Encode(String),

    #[error("decode payload: {0}")]
    Decode(String),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("invalid subject: {0}")]
    InvalidSubject(String),
}

impl TeiaError {
    /// Stable identifier for telemetry + cross-language SDK
    /// dispatch (`teia.error.kind` field).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ConnectFailed(_) => "connect_failed",
            Self::PublishFailed { .. } => "publish_failed",
            Self::SubscribeFailed { .. } => "subscribe_failed",
            Self::RequestFailed { .. } => "request_failed",
            Self::Encode(_) => "encode",
            Self::Decode(_) => "decode",
            Self::InvalidConfig(_) => "invalid_config",
            Self::InvalidSubject(_) => "invalid_subject",
        }
    }
}

/// Convenience boxed Arc'd variant so async tasks can clone an
/// error without re-cloning the underlying `async_nats` error.
pub type SharedTeiaError = Arc<TeiaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kind_is_stable_across_variants() {
        let cases = [
            (TeiaError::ConnectFailed("x".into()), "connect_failed"),
            (
                TeiaError::PublishFailed {
                    subject: "x".into(),
                    detail: "y".into(),
                },
                "publish_failed",
            ),
            (
                TeiaError::SubscribeFailed {
                    subject: "x".into(),
                    detail: "y".into(),
                },
                "subscribe_failed",
            ),
            (
                TeiaError::RequestFailed {
                    subject: "x".into(),
                    detail: "y".into(),
                },
                "request_failed",
            ),
            (TeiaError::Encode("x".into()), "encode"),
            (TeiaError::Decode("x".into()), "decode"),
            (TeiaError::InvalidConfig("x".into()), "invalid_config"),
            (TeiaError::InvalidSubject("x".into()), "invalid_subject"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.kind(), expected);
        }
    }
}
