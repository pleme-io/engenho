//! Typed errors shared across controller impls.

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error("store error during reconcile: {0}")]
    Store(String),

    #[error("invalid resource: {0}")]
    InvalidResource(String),

    #[error("internal: {0}")]
    Internal(String),
}

impl ControllerError {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Store(_) => "store",
            Self::InvalidResource(_) => "invalid_resource",
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
            (ControllerError::Store("x".into()), "store"),
            (ControllerError::InvalidResource("x".into()), "invalid_resource"),
            (ControllerError::Internal("x".into()), "internal"),
        ] {
            assert_eq!(e.kind(), k);
        }
    }
}
