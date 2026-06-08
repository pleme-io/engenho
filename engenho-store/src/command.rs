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

/// Re-export of the typed patch-algorithm discriminant carried by
/// [`ResourceCommand::Patch`] — consumers (controllers, the apiserver) get it
/// from the crate that defines the command, not a second import.
pub use engenho_types::patch::PatchType;

/// The default [`PatchType`] for a replayed pre-patch-type log entry — the
/// historical behavior was an unconditional RFC 7396 merge, so an old
/// serialized `Patch` command (which has no `patch_type` field) deserializes
/// as [`PatchType::Merge`], preserving its original semantics.
fn default_patch_type() -> PatchType {
    PatchType::Merge
}

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
    /// Apply a patch on top of the existing value, dispatching on
    /// `patch_type` into the correct algorithm (RFC 7396 merge / RFC 6902
    /// json-patch / strategic list-merge-by-key) via
    /// [`crate::patch_apply::apply`]. If the resource doesn't exist, the
    /// patch is rejected.
    ///
    /// `patch_type` is the typed discriminant of the client's `Content-Type`
    /// — it is part of the REPLICATED command so every Raft node dispatches
    /// the IDENTICAL algorithm deterministically (a leader-only dispatch
    /// would diverge replicas). Carried alongside the (already-decoded)
    /// `patch` `Value`; for json-patch the value is the RFC 6902 op array.
    ///
    /// `expected` is the optimistic-concurrency precondition — see
    /// [`ResourceCommand::Put`].
    Patch {
        key: ResourceKey,
        patch: ResourceValue,
        /// Which patch algorithm to run. Defaults to
        /// [`PatchType::Merge`] when an older serialized command (pre-patch-
        /// type) is replayed, preserving the historical RFC7396 behavior for
        /// any persisted log entry.
        #[serde(default = "default_patch_type")]
        patch_type: PatchType,
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

    /// Construct an UNCONDITIONAL `Patch` (no CAS precondition) with an
    /// explicit [`PatchType`].
    #[must_use]
    pub fn patch_typed(
        key: ResourceKey,
        patch: ResourceValue,
        patch_type: PatchType,
        reason: Reason,
    ) -> Self {
        Self::Patch {
            key,
            patch,
            patch_type,
            expected: None,
            reason,
        }
    }

    /// Construct an UNCONDITIONAL RFC 7396 merge `Patch` (no CAS
    /// precondition) — the common controller/reconciler shape (these always
    /// author merge-shaped patches, never strategic/json-patch). Equivalent
    /// to [`Self::patch_typed`] with [`PatchType::Merge`].
    #[must_use]
    pub fn patch(key: ResourceKey, patch: ResourceValue, reason: Reason) -> Self {
        Self::patch_typed(key, patch, PatchType::Merge, reason)
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
    /// The patch algorithm REFUSED the patch (RFC 6902 `test` failure,
    /// a bad JSON-Pointer, a missing strategic merge-key, an unrecognized
    /// `$patch` directive, or the typed-deferred server-side-apply path).
    /// Like [`Self::Conflict`]: no mutation, no revision consumed, no
    /// history entry, no watch event — the catalog is byte-identical. The
    /// carried [`crate::patch_apply::PatchError`] is surfaced via
    /// [`crate::ApplyOutcome::patch_error`] so the apiserver renders the
    /// correct typed Status (415 for SSA, 422/400 for a bad patch).
    PatchRejected,
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
                patch_type: PatchType::Strategic,
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
