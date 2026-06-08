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

/// The default `deletion_timestamp` for a replayed pre-field `Delete` log
/// entry — historical `Delete` commands carried no timestamp, so an old
/// serialized entry deserializes as `None` (no Terminating stamp),
/// preserving its original immediate-remove semantics. Mirrors the
/// [`default_patch_type`] forward-compat shape.
fn default_deletion_timestamp() -> Option<String> {
    None
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
    ///
    /// `deletion_timestamp` is the FROZEN RFC3339-UTC instant the
    /// apiserver boundary captured for this delete (a CLOCK read, the one
    /// non-deterministic input — see `engenho_types::time`). It is part of
    /// the REPLICATED command so the finalizer delete-gate in
    /// `state::apply_delete` stamps `metadata.deletionTimestamp` from this
    /// replicated scalar (NOT a per-node clock), keeping every Raft
    /// replica byte-identical. `None` ⇒ no timestamp threaded (the
    /// boundary judged the object finalizer-free, OR a pre-field log entry
    /// is being replayed): apply_delete removes immediately as before. The
    /// gate decision (finalizers present ⇒ Terminating, else remove) is
    /// made deterministically inside apply_delete; this scalar only
    /// supplies the timestamp it would stamp.
    Delete {
        key: ResourceKey,
        expected: Option<Revision>,
        reason: Reason,
        /// Frozen boundary timestamp (see variant docs). `#[serde(default)]`
        /// so replayed pre-field log entries deserialize as `None`.
        #[serde(default = "default_deletion_timestamp")]
        deletion_timestamp: Option<String>,
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

    /// Construct an UNCONDITIONAL `Delete` (no CAS precondition, no frozen
    /// timestamp). The common controller/GC shape — these don't read a
    /// boundary clock; the finalizer gate falls back to "no timestamp to
    /// stamp" which is correct for a no-finalizer object (immediate
    /// remove) and for a finalizer-bearing one stamps nothing this pass
    /// (the next pass with a threaded timestamp, or a `Put`-carried
    /// timestamp, supplies it). Equivalent to the struct literal with
    /// `expected: None, deletion_timestamp: None`.
    #[must_use]
    pub fn delete(key: ResourceKey, reason: Reason) -> Self {
        Self::Delete {
            key,
            expected: None,
            reason,
            deletion_timestamp: None,
        }
    }

    /// Construct a `Delete` carrying a FROZEN boundary `deletion_timestamp`
    /// (and optional CAS precondition). The apiserver delete path uses
    /// this so the finalizer gate stamps `metadata.deletionTimestamp` from
    /// the replicated scalar — deterministic across replicas.
    #[must_use]
    pub fn delete_at(
        key: ResourceKey,
        expected: Option<Revision>,
        reason: Reason,
        deletion_timestamp: Option<String>,
    ) -> Self {
        Self::Delete {
            key,
            expected,
            reason,
            deletion_timestamp,
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
    /// A delete on a FINALIZER-BEARING object: the object was NOT removed.
    /// Instead `metadata.deletionTimestamp` was stamped (Terminating) and
    /// the object kept; a `ChangeKind::Put` (Modified) watch event fired
    /// carrying the now-Terminating object, and one revision was consumed.
    /// The object is removed later, when an update/patch empties
    /// `metadata.finalizers` (apply_put/apply_patch convert that mutation
    /// into a removal). Idempotent: a second delete while already
    /// Terminating is a [`Self::NoOp`] (no revision, no event). Maps to a
    /// normal DELETE response in the apiserver — kubectl sees the object
    /// still present with a `deletionTimestamp`, exactly like upstream.
    DeletionPending,
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
                deletion_timestamp: Some("2026-06-08T12:34:56Z".into()),
            },
        ];
        for cmd in cases {
            let s = serde_json::to_string(&cmd).unwrap();
            let back: ResourceCommand = serde_json::from_str(&s).unwrap();
            assert_eq!(back, cmd);
        }
    }

    #[test]
    fn delete_deserializes_pre_field_log_entry_as_no_timestamp() {
        // Forward-compat: a serialized Delete from BEFORE deletion_timestamp
        // existed (no such field) deserializes with deletion_timestamp ==
        // None, preserving its original immediate-remove semantics — the
        // same #[serde(default)] contract default_patch_type relies on.
        let legacy = serde_json::json!({
            "kind": "delete",
            "key": ResourceKey::namespaced("", "v1", "ConfigMap", "default", "old"),
            "expected": null,
            "reason": "operator"
        });
        let cmd: ResourceCommand = serde_json::from_value(legacy).unwrap();
        match cmd {
            ResourceCommand::Delete {
                deletion_timestamp, ..
            } => assert_eq!(deletion_timestamp, None),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn delete_at_threads_the_frozen_timestamp() {
        let cmd = ResourceCommand::delete_at(
            ResourceKey::namespaced("", "v1", "ConfigMap", "default", "fz"),
            None,
            Reason::Operator,
            Some("2026-06-08T00:00:00Z".into()),
        );
        match cmd {
            ResourceCommand::Delete {
                deletion_timestamp, ..
            } => assert_eq!(deletion_timestamp.as_deref(), Some("2026-06-08T00:00:00Z")),
            other => panic!("expected Delete, got {other:?}"),
        }
    }
}
