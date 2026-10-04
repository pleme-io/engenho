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
    /// the pod's default (first) container.
    #[serde(default)]
    pub container: Option<String>,
    /// `?tailLines=<n>` — return only the last `n` lines (kubectl `--tail`).
    /// `None` = all lines.
    #[serde(default, rename = "tailLines")]
    pub tail_lines: Option<u32>,
    /// `?timestamps=true` — prefix each line with an RFC3339 timestamp
    /// (kubectl `--timestamps`). Default `false`.
    #[serde(default)]
    pub timestamps: bool,
    #[serde(default)]
    pub follow: bool,
    #[serde(skip)]
    pub from_byte: Option<u64>,
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
    use super::last_lines;

    #[test]
    fn last_lines_keeps_the_tail_and_its_newline() {
        assert_eq!(last_lines("a\nb\nc\n", 2), "b\nc\n");
        assert_eq!(last_lines("a\nb", 5), "a\nb");
        assert_eq!(last_lines("a\nb\n", 0), "");
    }
}
