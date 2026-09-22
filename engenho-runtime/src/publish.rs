//! Where a boot's kubeconfigs went.
//!
//! A boot writes up to four kubeconfigs: its own copy in `data_dir`, the
//! operator's (where `$KUBECONFIG` tooling looks), a pod-facing one (the
//! address containers reach) and a remote one (the advertised address). A
//! publish that fails does not fail the boot — the daemon is serving — so
//! the outcome was a log line and nothing else. [`PublishRecord`] keeps it,
//! per target, for the control plane to report: written (where, at what
//! mode), failed (why), or skipped (why not).
//!
//! The serde shapes are the control API's (`KubeconfigTarget`,
//! `PublishOutcome`, `PublishRecord`, `SkipReason`).

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Who a kubeconfig is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KubeconfigTarget {
    /// engenho's own copy, `data_dir/kubeconfig`.
    DataDir,
    /// The operator's, where `$KUBECONFIG` tooling looks.
    Operator,
    /// The pod-facing one: the address containers reach.
    Pod,
    /// The remote one: the advertised address.
    Remote,
}

impl KubeconfigTarget {
    /// Every target, in the order a boot writes them.
    pub const ALL: [Self; 4] = [Self::DataDir, Self::Pod, Self::Remote, Self::Operator];
}

/// Why a kubeconfig was not written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// Its publish path is empty.
    NotConfigured,
    /// TLS is off: there is no CA to hand a client.
    TlsDisabled,
    /// A remote kubeconfig needs `advertise_address`.
    NoAdvertiseAddress,
    /// No address pods can reach the apiserver on is known.
    NoPodAddress,
    /// The path is under `~/` and `$HOME` is unset.
    NoHome,
    /// The runtime is not running.
    RuntimeDown,
}

/// What happened to one kubeconfig.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PublishOutcome {
    /// Written.
    Written {
        /// Where.
        path: String,
        /// Its mode, octal (`0600`).
        mode: String,
    },
    /// Writing failed; the daemon serves anyway.
    Failed {
        /// Where it was to go.
        path: String,
        /// Why it failed.
        error: String,
    },
    /// Not written, deliberately.
    Skipped {
        /// Why not.
        reason: SkipReason,
    },
}

/// One kubeconfig's fate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublishRecord {
    /// Who it is for.
    pub target: KubeconfigTarget,
    /// What happened.
    pub outcome: PublishOutcome,
}

impl PublishRecord {
    /// Written at `path` with `mode`.
    #[must_use]
    pub fn written(target: KubeconfigTarget, path: &Path, mode: u32) -> Self {
        Self {
            target,
            outcome: PublishOutcome::Written {
                path: path.display().to_string(),
                mode: format!("{mode:04o}"),
            },
        }
    }

    /// Failed at `path`.
    #[must_use]
    pub fn failed(target: KubeconfigTarget, path: &Path, error: &dyn std::fmt::Display) -> Self {
        Self {
            target,
            outcome: PublishOutcome::Failed {
                path: path.display().to_string(),
                error: error.to_string(),
            },
        }
    }

    /// Skipped, for `reason`.
    #[must_use]
    pub const fn skipped(target: KubeconfigTarget, reason: SkipReason) -> Self {
        Self {
            target,
            outcome: PublishOutcome::Skipped { reason },
        }
    }

    /// Every target skipped for the same `reason`.
    #[must_use]
    pub fn all_skipped(reason: SkipReason) -> Vec<Self> {
        KubeconfigTarget::ALL
            .into_iter()
            .map(|target| Self::skipped(target, reason))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_shapes_are_the_control_apis() {
        assert_eq!(
            serde_json::to_value(PublishRecord::written(
                KubeconfigTarget::Operator,
                Path::new("/h/.kube/configs/engenho"),
                0o640
            ))
            .expect("serialize"),
            serde_json::json!({
                "target": "operator",
                "outcome": {"status": "written", "path": "/h/.kube/configs/engenho", "mode": "0640"},
            })
        );
        assert_eq!(
            serde_json::to_value(PublishRecord::skipped(
                KubeconfigTarget::Pod,
                SkipReason::NoPodAddress
            ))
            .expect("serialize"),
            serde_json::json!({"target": "pod", "outcome": {"status": "skipped", "reason": "no_pod_address"}})
        );
        assert_eq!(PublishRecord::all_skipped(SkipReason::TlsDisabled).len(), 4);
    }
}
