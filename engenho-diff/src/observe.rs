//! The response half of the harness — a typed [`Observation`] captured
//! from a [`DiffTarget`](crate::target::DiffTarget), plus the [`HttpMethod`]
//! verb and the [`DiffError`] failure taxonomy.

use serde_json::Value;
use thiserror::Error;

/// The HTTP verb of an [`Operation`](crate::op::Operation) — typed, so the
/// harness never string-matches a method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    /// The `reqwest::Method` this verb maps to.
    #[must_use]
    pub fn as_reqwest(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Put => reqwest::Method::PUT,
            Self::Patch => reqwest::Method::PATCH,
            Self::Delete => reqwest::Method::DELETE,
        }
    }

    /// A stable label for reports + signatures.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

/// One observed response from a target: the wire status, the content-type,
/// and the parsed body.
#[derive(Clone, Debug)]
pub struct Observation {
    /// HTTP status code.
    pub status: u16,
    /// `Content-Type` header, if present.
    pub content_type: Option<String>,
    /// The parsed response body. `Value::Null` when the body was empty;
    /// a non-JSON body is wrapped as `Value::String` (never silently
    /// dropped).
    pub json: Value,
}

impl Observation {
    /// `true` iff the status is in the 2xx range.
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Failure taxonomy for target execution. A transport failure against the
/// k3s reference is what the runner turns into
/// [`Verdict::ReferenceUnreachable`](crate::verdict::Verdict::ReferenceUnreachable)
/// — a fail-loud signal, never a silent skip.
#[derive(Debug, Error)]
pub enum DiffError {
    /// The HTTP request itself failed (connection refused, TLS, DNS).
    #[error("transport error against {side}: {source}")]
    Transport {
        side: &'static str,
        #[source]
        source: reqwest::Error,
    },
    /// A kube-client / kubeconfig error (auth, parse).
    #[error("kube error: {0}")]
    Kube(String),
    /// Booting the in-process engenho failed.
    #[error("engenho boot error: {0}")]
    Boot(String),
}
