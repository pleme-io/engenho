//! The typed command set replicated through openraft.
//!
//! Each variant is an atomic mutation of the resource catalog.
//! K8s API operations decompose into these primitives:
//!
//!   * `kubectl apply` → `Put` (idempotent create-or-replace)
//!   * `kubectl create` → `Put` (with metadata.resourceVersion = 0
//!     enforced to detect existing)
//!   * `kubectl patch` → `Patch` (merge / strategic merge)
//!   * `kubectl delete` → `Delete`

use serde::{Deserialize, Serialize};

use crate::resource::{ResourceKey, ResourceValue};
use crate::revision::Revision;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceCommand {
    /// Create or replace the resource at `key` with `value`.
    /// State machine sets `metadata.resourceVersion` to the
    /// committed global revision. If the resource already exists,
    /// updates atomically (last-write-wins by Raft order).
    ///
    /// `expected` is the optimistic-concurrency precondition — the
    /// caller's expected per-key `mod_revision`. `None` is
    /// unconditional (K8s: absent `resourceVersion`); `Some(N)`
    /// succeeds iff the live `VersionMeta.mod_revision == N`,
    /// otherwise the apply refuses with [`ResourceOp::Conflict`] and
    /// NO mutation / NO revision advance. The precondition is part of
    /// the replicated command so EVERY node applies the identical CAS
    /// decision deterministically (a leader-only check would diverge
    /// replicas).
    Put {
        key: ResourceKey,
        value: ResourceValue,
        expected: Option<Revision>,
        reason: Reason,
    },
    /// Apply a JSON merge patch on top of the existing value.
    /// If the resource doesn't exist, the patch is rejected.
    ///
    /// `expected` is the optimistic-concurrency precondition — see
    /// [`ResourceCommand::Put`].
    Patch {
        key: ResourceKey,
        patch: ResourceValue,
        expected: Option<Revision>,
        reason: Reason,
    },
    /// Remove the resource. Idempotent — deleting a non-existent
    /// resource succeeds silently.
    ///
    /// `expected` is the optimistic-concurrency precondition (K8s
    /// `Preconditions.resourceVersion`, surfaced as `?resourceVersion=`
    /// on DELETE) — see [`ResourceCommand::Put`].
    Delete {
        key: ResourceKey,
        expected: Option<Revision>,
        reason: Reason,
    },
}

impl ResourceCommand {
    /// Construct an UNCONDITIONAL `Put` (no CAS precondition) — the
    /// common case for controllers + reconcilers that own the object
    /// and don't race a concurrent writer. Equivalent to the struct
    /// literal with `expected: None`.
    #[must_use]
    pub fn put(key: ResourceKey, value: ResourceValue, reason: Reason) -> Self {
        Self::Put {
            key,
            value,
            expected: None,
            reason,
        }
    }

    /// Construct an UNCONDITIONAL `Patch` (no CAS precondition).
    #[must_use]
    pub fn patch(key: ResourceKey, patch: ResourceValue, reason: Reason) -> Self {
        Self::Patch {
            key,
            patch,
            expected: None,
            reason,
        }
    }

    /// Construct an UNCONDITIONAL `Delete` (no CAS precondition).
    #[must_use]
    pub fn delete(key: ResourceKey, reason: Reason) -> Self {
        Self::Delete {
            key,
            expected: None,
            reason,
        }
    }
}

/// Why this command was issued — telemetry + audit chain anchor
/// (matches the shape of `engenho-revoada::consensus::Reason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// Explicit operator action (kubectl apply / create / patch / delete).
    Operator,
    /// Controller reconciliation (Deployment → ReplicaSet, etc.).
    Controller,
    /// Garbage collection (orphan owner references).
    GarbageCollector,
    /// Admission webhook decision (validation, mutation).
    Admission,
    /// Scheduler binding a pod to a node.
    Scheduler,
}

/// What apply emits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceOp {
    /// Resource didn't exist before; now created at the listed
    /// resource_version (Raft log index).
    Created,
    /// Existing resource replaced.
    Replaced,
    /// Patch successfully applied.
    Patched,
    /// Existing resource removed.
    Deleted,
    /// Optimistic-concurrency precondition failed: the caller's
    /// `expected` per-key `mod_revision` did not match the live one
    /// (or the key was absent for a `Some(_)` precondition). The
    /// mutation was REFUSED — no revision consumed, no history entry,
    /// no watch event. Maps to a typed HTTP 409 "Conflict" in the
    /// apiserver (distinct from create-already-exists "AlreadyExists").
    Conflict,
    /// Idempotent no-op (delete-not-found, etc.).
    #[default]
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_command_serializes_tagged() {
        let cmd = ResourceCommand::Put {
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo"),
            value: serde_json::json!({"spec": {}}),
            expected: None,
            reason: Reason::Operator,
        };
        let s = serde_json::to_string(&cmd).unwrap();
        assert!(s.contains("\"kind\":\"put\""));
        assert!(s.contains("\"reason\":\"operator\""));
        let back: ResourceCommand = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cmd);
    }

    #[test]
    fn every_variant_round_trips() {
        let key = ResourceKey::namespaced("", "v1", "ConfigMap", "kube-system", "coredns");
        let cases = [
            ResourceCommand::Put {
                key: key.clone(),
                value: serde_json::json!({"data": {"k": "v"}}),
                expected: None,
                reason: Reason::Controller,
            },
            ResourceCommand::Patch {
                key: key.clone(),
                patch: serde_json::json!({"data": {"k": "v2"}}),
                expected: Some(crate::revision::Revision(3)),
                reason: Reason::Admission,
            },
            ResourceCommand::Delete {
                key: key.clone(),
                expected: None,
                reason: Reason::GarbageCollector,
            },
        ];
        for cmd in cases {
            let s = serde_json::to_string(&cmd).unwrap();
            let back: ResourceCommand = serde_json::from_str(&s).unwrap();
            assert_eq!(back, cmd);
        }
    }
}
