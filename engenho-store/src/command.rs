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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceCommand {
    /// Create or replace the resource at `key` with `value`.
    /// State machine sets `metadata.resourceVersion` to the
    /// committed Raft log index. If the resource already exists,
    /// updates atomically (last-write-wins by Raft order).
    Put {
        key: ResourceKey,
        value: ResourceValue,
        reason: Reason,
    },
    /// Apply a JSON merge patch on top of the existing value.
    /// If the resource doesn't exist, the patch is rejected.
    Patch {
        key: ResourceKey,
        patch: ResourceValue,
        reason: Reason,
    },
    /// Remove the resource. Idempotent — deleting a non-existent
    /// resource succeeds silently.
    Delete { key: ResourceKey, reason: Reason },
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
                reason: Reason::Controller,
            },
            ResourceCommand::Patch {
                key: key.clone(),
                patch: serde_json::json!({"data": {"k": "v2"}}),
                reason: Reason::Admission,
            },
            ResourceCommand::Delete {
                key: key.clone(),
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
