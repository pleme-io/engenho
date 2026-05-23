//! K8s-style API errors. Mapped to HTTP status codes by the
//! router; serialized to JSON per the K8s API conventions.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("conflict on resource {0}: {1}")]
    Conflict(String, String),
    #[error("invalid request: {0}")]
    BadRequest(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Stable kind tag used in error JSON for clients to dispatch on.
#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    NotFound,
    Conflict,
    BadRequest,
    Internal,
    StorageError,
}

impl ApiError {
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotFound(_) => ErrorKind::NotFound,
            Self::Conflict(_, _) => ErrorKind::Conflict,
            Self::BadRequest(_) => ErrorKind::BadRequest,
            Self::Internal(_) => ErrorKind::Internal,
            Self::StorageError(_) => ErrorKind::StorageError,
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_, _) => StatusCode::CONFLICT,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Internal(_) | Self::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Serialize)]
struct K8sStatus {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    status: &'static str,
    code: u16,
    reason: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self.status_code();
        let reason = match self {
            ApiError::NotFound(_) => "NotFound",
            ApiError::Conflict(_, _) => "AlreadyExists",
            ApiError::BadRequest(_) => "BadRequest",
            ApiError::Internal(_) => "InternalError",
            ApiError::StorageError(_) => "ServiceUnavailable",
        };
        let payload = K8sStatus {
            kind: "Status",
            api_version: "v1",
            status: "Failure",
            code: code.as_u16(),
            reason,
            message: self.to_string(),
        };
        (code, Json(payload)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_status_codes_match_k8s_conventions() {
        assert_eq!(
            ApiError::NotFound("x".into()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::Conflict("x".into(), "y".into()).status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            ApiError::BadRequest("x".into()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::Internal("x".into()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn error_kind_is_stable() {
        assert!(matches!(
            ApiError::NotFound("x".into()).kind(),
            ErrorKind::NotFound
        ));
        assert!(matches!(
            ApiError::StorageError("x".into()).kind(),
            ErrorKind::StorageError
        ));
    }
}
