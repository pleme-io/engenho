//! `KubeError` taxonomy.
//!
//! Every fallible operation across the kube traits returns
//! `Result<T, KubeError>`. The variants are chosen so an automated
//! caller (a controller, an informer, an apiserver) can mechanically
//! decide whether to retry, escalate, or give up — mirroring shigoto's
//! `FailureKind::{Transient, Declarative}` split.
//!
//! Per Operating Principle #2 (load-bearing fix), errors carry enough
//! context that the caller's reaction is a *typed decision* on the
//! variant — not a string-match on a message.

use std::time::Duration;

use thiserror::Error;

/// Error returned by every `KubeClient` / `Watcher` / `Informer` /
/// `Reconciler` operation.
#[derive(Debug, Error)]
pub enum KubeError {
    /// HTTP-layer error before the apiserver responded (TCP reset,
    /// TLS failure, DNS resolution, request build). Always [`Transient`]:
    /// the caller should retry with backoff.
    ///
    /// [`Transient`]: FailureClass::Transient
    #[error("network error: {0}")]
    Network(String),

    /// The apiserver returned a 4xx/5xx response carrying a typed
    /// `Status` body. Inspect [`ApiStatusKind`] to decide retry vs
    /// escalate — `Conflict` is retryable with backoff, `Forbidden`
    /// is declarative (don't retry).
    #[error("apiserver status {code} {kind:?}: {message}")]
    ApiStatus {
        /// HTTP status code (404, 409, 410, 422, 500, …).
        code:    u16,
        /// Typed classification of the status body's `reason` field.
        kind:    ApiStatusKind,
        /// Human-readable message from the status body.
        message: String,
    },

    /// JSON/YAML/Protobuf deserialization failed. Almost always
    /// [`Declarative`] — the wire shape doesn't match our typed
    /// expectation and retrying won't fix it.
    ///
    /// [`Declarative`]: FailureClass::Declarative
    #[error("decode error: {0}")]
    Decode(String),

    /// JSON/YAML/Protobuf serialization failed. [`Declarative`].
    #[error("encode error: {0}")]
    Encode(String),

    /// Auth setup failed (kubeconfig parse, token exec plugin,
    /// client cert load). [`Declarative`] — won't fix by retrying.
    #[error("auth error: {0}")]
    Auth(String),

    /// The watch stream ended at the server side. The caller should
    /// re-list + re-watch from the new resourceVersion.
    /// [`Transient`].
    #[error("watch closed by server")]
    WatchClosed,

    /// Watch event is too old for the cache — apiserver gave us a
    /// 410 Gone on `?resourceVersion=N`. The informer must trigger
    /// a relist. [`Transient`] (but expensive).
    #[error("resourceVersion {0} expired — relist required")]
    ResourceVersionExpired(String),

    /// Resource not found. Often expected (e.g. GET-on-deleted);
    /// the caller decides what it means. Classified [`Declarative`]
    /// for the "GET X-by-name where X never existed" case; reactive
    /// controllers treat it as a stop signal, not a retry signal.
    #[error("not found: {0}")]
    NotFound(String),

    /// Generic catch-all for anything not in the typed taxonomy.
    /// Adding a new variant is preferred over reaching for this.
    #[error("{0}")]
    Other(String),
}

/// Classification of an apiserver Status response. Mirrors the
/// upstream `metav1.StatusReason` constants but condensed to the
/// variants the substrate actually decides on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiStatusKind {
    /// 401 Unauthorized — auth missing or wrong.
    Unauthorized,
    /// 403 Forbidden — auth OK, action denied.
    Forbidden,
    /// 404 Not Found — resource doesn't exist.
    NotFound,
    /// 409 Conflict — resourceVersion mismatch (optimistic concurrency).
    Conflict,
    /// 409 AlreadyExists — create on existing resource.
    AlreadyExists,
    /// 410 Gone — resourceVersion expired or stale.
    Gone,
    /// 422 Invalid — admission validation rejected the object.
    Invalid,
    /// 429 TooManyRequests — apiserver rate-limited us.
    TooManyRequests,
    /// 500 InternalError — apiserver fault.
    InternalError,
    /// 503 ServiceUnavailable — apiserver overloaded / electing.
    ServiceUnavailable,
    /// 504 Timeout — apiserver took too long.
    Timeout,
    /// Anything else (the message string carries the original reason).
    Other,
}

impl ApiStatusKind {
    /// Map an HTTP status code to the typed kind. Note: callers should
    /// prefer reading the Status body's `reason` field when available
    /// (more specific than the code alone), but this is a useful
    /// fallback when the body is missing.
    #[must_use]
    pub fn from_code(code: u16) -> Self {
        match code {
            401 => Self::Unauthorized,
            403 => Self::Forbidden,
            404 => Self::NotFound,
            409 => Self::Conflict,
            410 => Self::Gone,
            422 => Self::Invalid,
            429 => Self::TooManyRequests,
            500 => Self::InternalError,
            503 => Self::ServiceUnavailable,
            504 => Self::Timeout,
            _   => Self::Other,
        }
    }
}

/// Retry classification — the typed decision a controller makes per
/// failure. Mirrors `shigoto_types::failure::FailureKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Try again with backoff. The substrate's automatic retry loop
    /// is appropriate.
    Transient,
    /// Don't retry — the input itself is wrong + bot retry will fix
    /// it. Surface to the operator.
    Declarative,
}

impl KubeError {
    /// Classify this error for the substrate's retry decision. Pair
    /// with [`Self::retry_after`] to know if + when to retry.
    #[must_use]
    pub fn classify(&self) -> FailureClass {
        match self {
            Self::Network(_)
            | Self::WatchClosed
            | Self::ResourceVersionExpired(_) => FailureClass::Transient,
            Self::ApiStatus { kind, .. } => match kind {
                ApiStatusKind::Conflict
                | ApiStatusKind::TooManyRequests
                | ApiStatusKind::InternalError
                | ApiStatusKind::ServiceUnavailable
                | ApiStatusKind::Timeout
                | ApiStatusKind::Gone => FailureClass::Transient,
                _ => FailureClass::Declarative,
            },
            Self::NotFound(_)
            | Self::Decode(_)
            | Self::Encode(_)
            | Self::Auth(_)
            | Self::Other(_) => FailureClass::Declarative,
        }
    }

    /// Suggested retry delay, if [`Transient`]. `None` for declarative
    /// errors. Default: 1s for network, 5s for TooManyRequests, 0
    /// (immediate) for the `Conflict` retry-loop. Callers MAY override
    /// (e.g., exponential backoff).
    ///
    /// [`Transient`]: FailureClass::Transient
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Network(_) => Some(Duration::from_secs(1)),
            Self::WatchClosed | Self::ResourceVersionExpired(_) => {
                Some(Duration::from_millis(100))
            }
            Self::ApiStatus { kind, .. } => match kind {
                ApiStatusKind::Conflict => Some(Duration::ZERO),
                ApiStatusKind::TooManyRequests => Some(Duration::from_secs(5)),
                ApiStatusKind::InternalError
                | ApiStatusKind::ServiceUnavailable
                | ApiStatusKind::Timeout
                | ApiStatusKind::Gone => Some(Duration::from_secs(1)),
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_error_is_transient() {
        let e = KubeError::Network("connection refused".into());
        assert_eq!(e.classify(), FailureClass::Transient);
        assert_eq!(e.retry_after(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn auth_error_is_declarative() {
        let e = KubeError::Auth("kubeconfig parse failed".into());
        assert_eq!(e.classify(), FailureClass::Declarative);
        assert_eq!(e.retry_after(), None);
    }

    #[test]
    fn conflict_is_immediate_retry() {
        let e = KubeError::ApiStatus {
            code:    409,
            kind:    ApiStatusKind::Conflict,
            message: "resourceVersion mismatch".into(),
        };
        assert_eq!(e.classify(), FailureClass::Transient);
        assert_eq!(e.retry_after(), Some(Duration::ZERO));
    }

    #[test]
    fn forbidden_is_declarative() {
        let e = KubeError::ApiStatus {
            code:    403,
            kind:    ApiStatusKind::Forbidden,
            message: "RBAC denied".into(),
        };
        assert_eq!(e.classify(), FailureClass::Declarative);
    }

    #[test]
    fn too_many_requests_backs_off_5s() {
        let e = KubeError::ApiStatus {
            code:    429,
            kind:    ApiStatusKind::TooManyRequests,
            message: "calm down".into(),
        };
        assert_eq!(e.retry_after(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn status_kind_from_code_covers_canonical() {
        assert_eq!(ApiStatusKind::from_code(401), ApiStatusKind::Unauthorized);
        assert_eq!(ApiStatusKind::from_code(403), ApiStatusKind::Forbidden);
        assert_eq!(ApiStatusKind::from_code(404), ApiStatusKind::NotFound);
        assert_eq!(ApiStatusKind::from_code(409), ApiStatusKind::Conflict);
        assert_eq!(ApiStatusKind::from_code(410), ApiStatusKind::Gone);
        assert_eq!(ApiStatusKind::from_code(422), ApiStatusKind::Invalid);
        assert_eq!(ApiStatusKind::from_code(429), ApiStatusKind::TooManyRequests);
        assert_eq!(ApiStatusKind::from_code(500), ApiStatusKind::InternalError);
        assert_eq!(ApiStatusKind::from_code(503), ApiStatusKind::ServiceUnavailable);
        assert_eq!(ApiStatusKind::from_code(504), ApiStatusKind::Timeout);
        assert_eq!(ApiStatusKind::from_code(418), ApiStatusKind::Other);
    }
}
