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
    /// Optimistic-concurrency precondition failed — the inbound
    /// `metadata.resourceVersion` (or `?resourceVersion=` on DELETE) did
    /// not match the live object's revision. The K8s 409 "Conflict"
    /// equivalent, DISTINCT from [`ApiError::Conflict`] (which renders
    /// reason "AlreadyExists" for create-already-exists). Carries the
    /// human-readable message rendered into the `Status` body.
    #[error("{0}")]
    ResourceVersionConflict(String),
    #[error("invalid request: {0}")]
    BadRequest(String),
    /// The request carried a `Content-Type` the apiserver does not accept
    /// for this verb — the K8s 415 Unsupported Media Type equivalent.
    /// Rendered as a proper K8s `Status` JSON body (reason
    /// "UnsupportedMediaType", code 415), NOT axum's built-in plain-text
    /// `JsonRejection`. The write handlers extract the raw body + headers
    /// themselves precisely so this typed error renders instead of the
    /// framework's 415.
    #[error("{0}")]
    UnsupportedMediaType(String),
    /// An admission webhook DENIED the request — the K8s 403 Forbidden
    /// equivalent. Carries the chain's human-readable deny reason
    /// (prefixed with the rejecting webhook's name by the
    /// [`crate::handler::AdmissionChain`]).
    #[error("admission denied: {0}")]
    Forbidden(String),
    /// The requested `resourceVersion` resume point has been compacted
    /// away — the K8s 410 Gone / Expired equivalent. The client must
    /// re-LIST + re-WATCH from the fresh list revision. Carries the
    /// human-readable message rendered into the `Status` body.
    #[error("{0}")]
    Gone(String),
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
    ResourceVersionConflict,
    BadRequest,
    UnsupportedMediaType,
    Forbidden,
    Gone,
    Internal,
    StorageError,
}

impl ApiError {
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotFound(_) => ErrorKind::NotFound,
            Self::Conflict(_, _) => ErrorKind::Conflict,
            Self::ResourceVersionConflict(_) => ErrorKind::ResourceVersionConflict,
            Self::BadRequest(_) => ErrorKind::BadRequest,
            Self::UnsupportedMediaType(_) => ErrorKind::UnsupportedMediaType,
            Self::Forbidden(_) => ErrorKind::Forbidden,
            Self::Gone(_) => ErrorKind::Gone,
            Self::Internal(_) => ErrorKind::Internal,
            Self::StorageError(_) => ErrorKind::StorageError,
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_, _) | Self::ResourceVersionConflict(_) => StatusCode::CONFLICT,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Gone(_) => StatusCode::GONE,
            Self::Internal(_) | Self::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// A typed K8s `Status` object. The single render surface for every
/// failure body — both the [`IntoResponse`] path (a real HTTP error)
/// and the in-band watch-stream 410 line (`params::status_410_line`)
/// build their JSON through this struct, never `format!()` of JSON.
#[derive(Serialize)]
struct K8sStatus {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    status: &'static str,
    code: u16,
    reason: String,
    message: String,
}

/// Build a typed K8s `Status` value (`{kind:"Status",apiVersion:"v1",
/// status:"Failure",code,reason,message}`) as `serde_json::Value`.
///
/// Used by the watch streaming path to emit an in-band terminal Status
/// object (e.g. the mid-stream 410 Expired) — the response is already
/// HTTP 200, so the per-event status is carried in-band, matching
/// kube-apiserver's long-poll watch behavior.
#[must_use]
pub fn status_object(message: String, code: u16, reason: &str) -> serde_json::Value {
    let status = K8sStatus {
        kind: "Status",
        api_version: "v1",
        status: "Failure",
        code,
        reason: reason.to_string(),
        message,
    };
    // Infallible for this concrete struct.
    serde_json::to_value(status).unwrap_or(serde_json::Value::Null)
}

/// A protobuf-codec failure at the HTTP boundary becomes an
/// [`ApiError::BadRequest`] — bad magic, an uncataloged kind, or an
/// undecodable object body are all malformed-request conditions (HTTP
/// 400 with a proper K8s `Status`), never a panic. The codec is a TOTAL
/// function returning typed errors; this is the single mapping into the
/// apiserver's error surface.
impl From<engenho_kube_proto::CodecError> for ApiError {
    fn from(e: engenho_kube_proto::CodecError) -> Self {
        ApiError::BadRequest(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self.status_code();
        let reason = match self {
            ApiError::NotFound(_) => "NotFound",
            ApiError::Conflict(_, _) => "AlreadyExists",
            // Optimistic-concurrency failure uses reason "Conflict"
            // (K8s uses `.details` for these) — distinct from the
            // create-already-exists "AlreadyExists" above.
            ApiError::ResourceVersionConflict(_) => "Conflict",
            ApiError::BadRequest(_) => "BadRequest",
            ApiError::UnsupportedMediaType(_) => "UnsupportedMediaType",
            ApiError::Forbidden(_) => "Forbidden",
            ApiError::Gone(_) => "Expired",
            ApiError::Internal(_) => "InternalError",
            ApiError::StorageError(_) => "ServiceUnavailable",
        };
        let payload = K8sStatus {
            kind: "Status",
            api_version: "v1",
            status: "Failure",
            code: code.as_u16(),
            reason: reason.to_string(),
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
        assert!(matches!(
            ApiError::Gone("x".into()).kind(),
            ErrorKind::Gone
        ));
    }

    #[test]
    fn resource_version_conflict_renders_409_conflict() {
        // Optimistic-concurrency failure → HTTP 409 + reason "Conflict"
        // (distinct from create-already-exists "AlreadyExists").
        let err = ApiError::ResourceVersionConflict(
            "Operation cannot be fulfilled on pods \"p\": resourceVersion mismatch".into(),
        );
        assert_eq!(err.status_code(), StatusCode::CONFLICT);
        assert!(matches!(err.kind(), ErrorKind::ResourceVersionConflict));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn conflict_and_rv_conflict_have_distinct_reasons() {
        // Both are 409, but the create-already-exists path is
        // "AlreadyExists" and the optimistic-concurrency path is
        // "Conflict" — assert the reason strings differ via the rendered
        // Status body.
        use axum::body::to_bytes;
        async fn reason_of(err: ApiError) -> String {
            let resp = err.into_response();
            let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            v.get("reason").unwrap().as_str().unwrap().to_string()
        }
        let already = reason_of(ApiError::Conflict("p".into(), "exists".into())).await;
        let cas = reason_of(ApiError::ResourceVersionConflict("mismatch".into())).await;
        assert_eq!(already, "AlreadyExists");
        assert_eq!(cas, "Conflict");
        assert_ne!(already, cas);
    }

    #[test]
    fn forbidden_renders_403_forbidden_status() {
        // Admission Deny → ApiError::Forbidden → HTTP 403 + reason
        // "Forbidden".
        let err = ApiError::Forbidden("compliance: image not signed".into());
        assert_eq!(err.status_code(), StatusCode::FORBIDDEN);
        assert!(matches!(err.kind(), ErrorKind::Forbidden));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn gone_renders_410_expired_status() {
        // CompactedTooOld → ApiError::Gone → HTTP 410 + reason "Expired".
        let err = ApiError::Gone("too old resource version: 2 (5)".into());
        assert_eq!(err.status_code(), StatusCode::GONE);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::GONE);
    }
}
