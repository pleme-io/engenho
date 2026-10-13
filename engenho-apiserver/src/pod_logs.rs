//! The Pod `/log` subresource seam — typed query + the in-process reader trait.
//!
//! The apiserver's Pod handler serves `kubectl logs <pod> [-c <container>]` by
//! delegating to an installed [`PodLogReader`] (single-node: the in-process
//! kubelet). This module declares the seam WITHOUT depending on
//! `engenho-kubelet` (a layering inversion — the apiserver is below the
//! kubelet): the runtime supplies a thin adapter implementing [`PodLogReader`]
//! that translates to the kubelet's `LogOptions` + maps `KubeletError` →
//! [`ApiError`].

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::ApiError;

/// Typed `?container=&tailLines=&timestamps=` query for a Pod-logs request.
/// Decoded by the router from the request query string — the typed knobs
/// kubectl sends for `kubectl logs`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LogQuery {
    /// `?container=<name>` — which container's log (kubectl `-c`). `None` =
    /// the pod's default container
    /// ([`engenho_types::pod::default_container`]).
    #[serde(default)]
    pub container: Option<String>,
    /// `?tailLines=<n>` — return only the last `n` lines (kubectl `--tail`).
    /// `None` = all lines.
    #[serde(default, rename = "tailLines")]
    pub tail_lines: Option<u32>,
    /// `?timestamps=true` — prefix each line with the `RFC3339Nano` time it
    /// was written (kubectl `--timestamps`, every k9s Logs view). Default
    /// `false`.
    #[serde(default)]
    pub timestamps: bool,
    /// `?follow=true` — keep the response open and stream what is appended
    /// (kubectl `-f`, and every k9s Logs view).
    #[serde(default)]
    pub follow: bool,
    /// `?previous=true` — the previous terminated instance's log (kubectl
    /// `-p`, k9s `p`). No backend keeps a previous instance's log, so this is
    /// refused the way upstream refuses it when there is none, never answered
    /// with the current instance's log.
    #[serde(default)]
    pub previous: bool,
    /// `?limitBytes=<n>` — end the response after `n` bytes (k9s "head").
    #[serde(default, rename = "limitBytes")]
    pub limit_bytes: Option<u64>,
    /// `?sinceSeconds=<n>` — only lines written in the last `n` seconds
    /// (k9s Logs view `2`..`6`, kubectl `--since`).
    #[serde(default, rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    /// `?sinceTime=<RFC3339>` — only lines written at or after it (kubectl
    /// `--since-time`). At most one of this and `sinceSeconds`.
    #[serde(default, rename = "sinceTime")]
    pub since_time: Option<String>,
    /// `sinceSeconds` / `sinceTime` resolved to an instant once, when the
    /// request is admitted, so a follow filters against one fixed point.
    #[serde(skip)]
    pub since: Option<std::time::SystemTime>,
    /// Where a follow resumes: the cursor the previous read returned.
    #[serde(skip)]
    pub from_byte: Option<u64>,
}

impl LogQuery {
    /// Refuse what upstream refuses before any log is read.
    ///
    /// # Errors
    ///
    /// [`ApiError::BadRequest`] for `previous=true` (no previous instance is
    /// kept), a `sinceSeconds` below 1, a `sinceTime` that is not RFC3339,
    /// both of them at once, or a `limitBytes` of 0.
    pub fn admit(&self, pod: &str) -> Result<(), ApiError> {
        if self.previous {
            let container = self.container.as_deref().unwrap_or_default();
            return Err(ApiError::BadRequest(format!(
                "previous terminated container {container:?} in pod {pod:?} not found"
            )));
        }
        if self.since_seconds.is_some_and(|s| s < 1) {
            return Err(ApiError::BadRequest(
                "sinceSeconds: Invalid value: must be greater than 0".into(),
            ));
        }
        if self.since_seconds.is_some() && self.since_time.is_some() {
            return Err(ApiError::BadRequest(
                "at most one of `sinceTime` or `sinceSeconds` may be specified".into(),
            ));
        }
        if self.since_time.as_deref().is_some_and(|t| {
            engenho_types::time::parse_rfc3339_system_time(t).is_none()
        }) {
            return Err(ApiError::BadRequest(
                "sinceTime: Invalid value: must be an RFC3339 timestamp".into(),
            ));
        }
        if self.limit_bytes == Some(0) {
            return Err(ApiError::BadRequest(
                "limitBytes: Invalid value: must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

impl LogQuery {
    /// Resolve `sinceSeconds` / `sinceTime` against `now` into
    /// [`Self::since`]. Call once, after [`Self::admit`].
    pub fn resolve_since(&mut self, now: std::time::SystemTime) {
        self.since = match (self.since_seconds, self.since_time.as_deref()) {
            (Some(secs), _) => u64::try_from(secs)
                .ok()
                .and_then(|s| now.checked_sub(std::time::Duration::from_secs(s))),
            (None, Some(t)) => engenho_types::time::parse_rfc3339_system_time(t),
            (None, None) => None,
        };
    }
}

/// One read of a Pod log: the text, and where a follow's next read resumes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogChunk {
    /// The log text.
    pub text: String,
    /// Pass back as [`LogQuery::from_byte`] to read only what follows.
    pub cursor: u64,
}

/// Cut `text` to at most `limit` bytes on a char boundary.
#[must_use]
pub fn truncate_to(mut text: String, limit: Option<u64>) -> String {
    let Some(limit) = limit.and_then(|l| usize::try_from(l).ok()) else {
        return text;
    };
    if text.len() > limit {
        let mut cut = limit;
        while !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
    }
    text
}

/// The in-process Pod-log reader the apiserver's Pod handler delegates to.
///
/// Implemented by the runtime's kubelet adapter (single-node: the kubelet runs
/// in the same process). A multi-node future replaces this with a node-proxy
/// that forwards to the pod's node's kubelet — the trait is the seam that makes
/// that swap a one-line change.
#[async_trait]
pub trait PodLogReader: Send + Sync {
    /// Read a Pod container's logs.
    ///
    /// # Errors
    ///
    /// [`ApiError::NotFound`] when the pod isn't running on a reachable node /
    /// the container doesn't exist; [`ApiError::Internal`] on a backend
    /// failure. NEVER an empty-Ok for a missing container.
    async fn read_pod_logs(
        &self,
        namespace: &str,
        name: &str,
        query: &LogQuery,
    ) -> Result<String, ApiError>;

    /// [`Self::read_pod_logs`] with the cursor a follow resumes from.
    ///
    /// The default treats the text as the stored bytes and `from_byte` as an
    /// offset into them. A reader whose text is rendered (timestamps
    /// prefixed, records parsed) overrides it, or a follow resumes at the
    /// wrong place.
    ///
    /// # Errors
    ///
    /// As [`Self::read_pod_logs`].
    async fn read_pod_log_chunk(
        &self,
        namespace: &str,
        name: &str,
        query: &LogQuery,
    ) -> Result<LogChunk, ApiError> {
        let text = self.read_pod_logs(namespace, name, query).await?;
        let cursor =
            query.from_byte.unwrap_or(0) + u64::try_from(text.len()).unwrap_or(u64::MAX);
        Ok(LogChunk { text, cursor })
    }
}

#[must_use]
pub fn last_lines(text: &str, n: u32) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n as usize);
    let mut out = lines[start..].join("\n");
    if !out.is_empty() && text.ends_with('\n') {
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{last_lines, truncate_to};

    #[test]
    fn truncate_to_respects_the_limit_and_char_boundaries() {
        assert_eq!(truncate_to("abcdef".into(), Some(3)), "abc");
        assert_eq!(truncate_to("ab".into(), Some(3)), "ab");
        assert_eq!(truncate_to("aé".into(), Some(2)), "a");
        assert_eq!(truncate_to("abc".into(), None), "abc");
    }

    #[test]
    fn last_lines_keeps_the_tail_and_its_newline() {
        assert_eq!(last_lines("a\nb\nc\n", 2), "b\nc\n");
        assert_eq!(last_lines("a\nb", 5), "a\nb");
        assert_eq!(last_lines("a\nb\n", 0), "");
    }
}
