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
    /// The request body failed VALIDATION — the K8s 422 Invalid equivalent
    /// (reason "Invalid"). DISTINCT from [`Self::BadRequest`] (400): a 400
    /// means the request could not be understood, a 422 means it was
    /// understood and is not a legal object. client-go's `apierrors::IsInvalid`
    /// keys on reason "Invalid", so a controller's retry logic reads the two
    /// differently — collapsing them would make a permanently-invalid object
    /// look like a transient protocol error and be retried forever.
    #[error("{0}")]
    Invalid(String),
    /// The request's `Accept` named no media type this server can produce —
    /// the K8s 406 Not Acceptable equivalent. Distinct from
    /// [`Self::UnsupportedMediaType`] (415), which is about the REQUEST body's
    /// Content-Type; this is about the RESPONSE the client will accept.
    #[error("{0}")]
    NotAcceptable(String),
    /// Authentication failed on a typed-bad credential — the K8s 401
    /// Unauthorized equivalent. Carries the human-readable reason rendered
    /// into the `Status` body (reason "Unauthorized", code 401). This brick
    /// the ONLY producer is a structurally-ServiceAccount bearer token that
    /// engenho cannot validate yet (SA-token authn is tied to the kubelet
    /// projection brick). Authorize-ALL is retained, so this is NOT an authz
    /// denial — a no-credential request resolves to anonymous + proceeds.
    #[error("{0}")]
    Unauthorized(String),
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
    /// The RBAC authorizer DENIED the request — the same K8s 403 Forbidden
    /// equivalent as [`Self::Forbidden`], but the message is the standard RBAC
    /// `forbidden: User "<u>" cannot <verb> ...` string built by
    /// [`forbidden_message`] (NOT the admission `"admission denied: "` prefix).
    /// Distinct variant so authz-deny + admission-deny share the 403 `Status`
    /// SHAPE while keeping their own message text. The message string is the
    /// only composed text and goes in the typed `Status.message` field (the JSON
    /// wire is built by serde, never `format!()`).
    #[error("{0}")]
    AuthzForbidden(String),
    /// A LIST `continue` token that no longer decodes or verifies — the K8s
    /// 410 Gone / Expired equivalent. The client must restart the LIST.
    /// Carries the human-readable message rendered into the `Status` body.
    ///
    /// Never a WATCH's answer: a watch the store cannot serve from its
    /// `resourceVersion` ends in-band ([`crate::watch_start::WatchRefusal`]),
    /// because kube-rs retries an HTTP error at the same revision forever.
    #[error("{0}")]
    Gone(String),
    /// A server-side apply hit field-ownership conflicts `force` did not
    /// override — the K8s 409 Conflict for apply. Renders the
    /// `Apply failed with N conflict(s)` `Status` with reason "Conflict",
    /// code 409, AND a `details.causes` array (one `FieldManagerConflict`
    /// cause per field) — distinct from the plain
    /// [`Self::ResourceVersionConflict`] (no causes). Carries the typed
    /// causes array (built by [`engenho_store::ssa::ApplyConflicts::to_causes`])
    /// so the body is serde-rendered, never `format!()`ed.
    #[error("Apply failed with conflicts")]
    ApplyConflict(serde_json::Value),
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
    Invalid,
    NotAcceptable,
    Unauthorized,
    UnsupportedMediaType,
    Forbidden,
    AuthzForbidden,
    Gone,
    ApplyConflict,
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
            Self::Invalid(_) => ErrorKind::Invalid,
            Self::NotAcceptable(_) => ErrorKind::NotAcceptable,
            Self::Unauthorized(_) => ErrorKind::Unauthorized,
            Self::UnsupportedMediaType(_) => ErrorKind::UnsupportedMediaType,
            Self::Forbidden(_) => ErrorKind::Forbidden,
            Self::AuthzForbidden(_) => ErrorKind::AuthzForbidden,
            Self::Gone(_) => ErrorKind::Gone,
            Self::ApplyConflict(_) => ErrorKind::ApplyConflict,
            Self::Internal(_) => ErrorKind::Internal,
            Self::StorageError(_) => ErrorKind::StorageError,
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_, _) | Self::ResourceVersionConflict(_) | Self::ApplyConflict(_) => {
                StatusCode::CONFLICT
            }
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Invalid(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Forbidden(_) | Self::AuthzForbidden(_) => StatusCode::FORBIDDEN,
            Self::Gone(_) => StatusCode::GONE,
            Self::Internal(_) | Self::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// A typed K8s `Status` FAILURE object. The single render surface for
/// every failure body: the [`IntoResponse`] path (a real HTTP error), the
/// in-band watch-stream ends (`params::status_410_line`, the 429 of
/// `watch_end::NoProgress`, an [`ApiError`] met while a watch resumes) and
/// the apply-conflict 409 all build their JSON
/// through this struct, never `format!()` of JSON.
///
/// `D` is the `details` block: [`NoDetails`] for the plain shape (the block
/// is then absent, and cannot be present), [`StatusCauseDetails`] for an
/// apply conflict, [`RetryAfterDetails`] for a 429.
#[derive(Serialize)]
struct K8sStatus<D> {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    status: &'static str,
    code: u16,
    reason: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<D>,
}

impl<D: Serialize> K8sStatus<D> {
    fn failure(code: u16, reason: &str, message: String, details: Option<D>) -> Self {
        Self {
            kind: "Status",
            api_version: "v1",
            status: "Failure",
            code,
            reason: reason.to_string(),
            message,
            details,
        }
    }

    /// As a JSON value. Infallible for these concrete shapes.
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// The `details` of a plain failure `Status`: none. Uninhabited, so a plain
/// Status can never carry a details block.
#[derive(Serialize)]
enum NoDetails {}

/// The `metav1.StatusDetails` of a 429: how long the client should wait
/// before it retries. client-go reads it through
/// `apierrors.SuggestsClientDelay`.
#[derive(Serialize)]
struct RetryAfterDetails {
    #[serde(rename = "retryAfterSeconds")]
    retry_after_seconds: u32,
}

/// A typed K8s `Status` SUCCESS object — the `metav1.Status{status:"Success"}`
/// shape kube-apiserver returns from a DELETE that had no object to hand
/// back (an idempotent delete of an already-absent name). DISTINCT from
/// [`K8sStatus`] (the failure shape): no `code`, no `reason`/`message`,
/// `status:"Success"`, and a `details{name,kind}` block naming the target.
/// Built with serde, never `format!()` of JSON (TYPED EMISSION).
#[derive(Serialize)]
struct K8sStatusSuccess {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    status: &'static str,
    details: StatusDetails,
}

/// The `metav1.StatusDetails` block — names the resource a Status refers
/// to. For the DELETE-success fallback only `name` + `kind` are known.
#[derive(Serialize)]
struct StatusDetails {
    name: String,
    kind: String,
}

/// The `metav1.StatusDetails` block carrying `causes` — the shape
/// server-side apply returns on a field-ownership conflict (`Apply failed
/// with N conflict(s)`, reason "Conflict", code 409, one
/// `FieldManagerConflict` cause per conflicting field). The `causes` array
/// is the typed `serde_json::Value` built store-side by
/// `engenho_store::ssa::ApplyConflicts::to_causes` — serde all the way
/// (TYPED EMISSION; never `format!()` of the wire object).
#[derive(Serialize)]
struct StatusCauseDetails {
    causes: serde_json::Value,
}

/// Build the typed `metav1.Status{status:"Success"}` value DELETE returns
/// when there was no object to hand back (idempotent delete of an absent
/// name). kubectl's delete codepath decodes this body and treats it as a
/// clean success — it is NEVER the empty body that crashes
/// `json.Unmarshal([]byte{})`.
///
/// `kind` is the deleted resource's kind (e.g. `"ConfigMap"`); the
/// envelope is always the meta/v1 `Status` kind (`apiVersion:"v1"`). This
/// value is only ever rendered as JSON — the meta/v1 `Status` protobuf
/// descriptor is not reachable through the kube-proto core/v1 package map,
/// and the no-object branch is the only place it arises (the conformance
/// DELETE always targets an existing object → object path → protobuf works).
#[must_use]
pub fn delete_status_success(name: &str, kind: &str) -> serde_json::Value {
    let status = K8sStatusSuccess {
        kind: "Status",
        api_version: "v1",
        status: "Success",
        details: StatusDetails {
            name: name.to_string(),
            kind: kind.to_string(),
        },
    };
    // Infallible for this concrete struct.
    serde_json::to_value(status).unwrap_or(serde_json::Value::Null)
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
    K8sStatus::<NoDetails>::failure(code, reason, message, None).to_value()
}

/// Build `Status{code: 429, reason: "TooManyRequests",
/// details.retryAfterSeconds}`, the shape of upstream's
/// `errors.NewTooManyRequests`. Crate-private: the one in-band watch 429 is
/// `watch_end::NoProgress::status_line`.
#[must_use]
pub(crate) fn too_many_requests_object(
    message: String,
    retry_after_seconds: u32,
) -> serde_json::Value {
    K8sStatus::failure(
        429,
        "TooManyRequests",
        message,
        Some(RetryAfterDetails {
            retry_after_seconds,
        }),
    )
    .to_value()
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

/// Build the standard Kubernetes RBAC forbidden message for a denied request —
/// the exact `forbidden: User "<u>" cannot <verb> resource "<resource>" in API
/// group "<group>" [in the namespace "<ns>"]` shape kube-apiserver renders. For
/// a non-resource request it's `forbidden: User "<u>" cannot <verb> path
/// "<path>"`. This is the ONLY composed text in the authz-deny path; it lands in
/// the typed [`K8sStatus::message`] field (the JSON wire is built by serde, per
/// TYPED EMISSION).
#[must_use]
pub fn forbidden_message(attrs: &crate::authz::Attributes) -> String {
    let user = &attrs.user.username;
    if let Some(path) = &attrs.non_resource_url {
        return format!(
            "forbidden: User \"{user}\" cannot {} path \"{path}\"",
            attrs.verb
        );
    }
    let group_clause = if attrs.group.is_empty() {
        " in API group \"\"".to_string()
    } else {
        format!(" in API group \"{}\"", attrs.group)
    };
    let resource = attrs.resource_key();
    let ns_clause = match &attrs.namespace {
        Some(ns) => format!(" in the namespace \"{ns}\""),
        None => String::new(),
    };
    format!(
        "forbidden: User \"{user}\" cannot {} resource \"{resource}\"{group_clause}{ns_clause}",
        attrs.verb
    )
}

impl ApiError {
    /// The `Status.reason` this error carries on the wire.
    fn reason(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "NotFound",
            Self::Conflict(_, _) => "AlreadyExists",
            // Optimistic-concurrency failure uses reason "Conflict" (K8s uses
            // `.details` for these), distinct from the create-already-exists
            // "AlreadyExists" above. An apply conflict shares the reason and
            // adds `details.causes` (see `status`).
            Self::ResourceVersionConflict(_) | Self::ApplyConflict(_) => "Conflict",
            Self::BadRequest(_) => "BadRequest",
            Self::Invalid(_) => "Invalid",
            Self::NotAcceptable(_) => "NotAcceptable",
            Self::Unauthorized(_) => "Unauthorized",
            Self::UnsupportedMediaType(_) => "UnsupportedMediaType",
            Self::Forbidden(_) | Self::AuthzForbidden(_) => "Forbidden",
            Self::Gone(_) => "Expired",
            Self::Internal(_) => "InternalError",
            Self::StorageError(_) => "ServiceUnavailable",
        }
    }

    /// This error as a K8s failure `Status`: the one body both an HTTP error
    /// response and an in-band watch `ERROR` line carry.
    fn status(&self) -> K8sStatus<StatusCauseDetails> {
        let code = self.status_code().as_u16();
        // The apply-conflict path renders a Status WITH `details.causes`, a
        // distinct body shape from every other error (which has no details).
        // The message names the conflict count.
        if let Self::ApplyConflict(causes) = self {
            let n = causes.as_array().map_or(0, Vec::len);
            let message = if n == 1 {
                "Apply failed with 1 conflict".to_string()
            } else {
                format!("Apply failed with {n} conflicts")
            };
            return K8sStatus::failure(
                code,
                self.reason(),
                message,
                Some(StatusCauseDetails {
                    causes: causes.clone(),
                }),
            );
        }
        K8sStatus::failure(code, self.reason(), self.to_string(), None)
    }

    /// This error as the object of an in-band watch `ERROR` line, for a
    /// failure met after the watch's HTTP 200 has gone out: the same `Status`
    /// its HTTP response would carry.
    #[must_use]
    pub(crate) fn to_status_object(&self) -> serde_json::Value {
        self.status().to_value()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status_code(), Json(self.status())).into_response()
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
        assert!(matches!(ApiError::Gone("x".into()).kind(), ErrorKind::Gone));
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
    fn unauthorized_renders_401_status() {
        // A structurally-SA bearer this brick can't validate → ApiError::
        // Unauthorized → HTTP 401 + reason "Unauthorized".
        let err = ApiError::Unauthorized(
            "service account token authentication is not yet supported".into(),
        );
        assert_eq!(err.status_code(), StatusCode::UNAUTHORIZED);
        assert!(matches!(err.kind(), ErrorKind::Unauthorized));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn gone_renders_410_expired_status() {
        // An expired continue token → ApiError::Gone → HTTP 410 + reason
        // "Expired".
        let err = ApiError::Gone("invalid or expired continue token".into());
        assert_eq!(err.status_code(), StatusCode::GONE);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::GONE);
    }
}
