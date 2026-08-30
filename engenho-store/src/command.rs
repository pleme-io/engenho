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

/// The frozen, boundary-supplied server-side-apply metadata carried on a
/// `Patch` command whose `patch_type == PatchType::Apply`. `None` for every
/// non-apply patch (merge / strategic / json-patch) AND for a replayed
/// pre-field log entry (`#[serde(default)]`), so those paths stay
/// byte-identical — the BEHAVIOR-PRESERVATION seam.
///
/// `manager` is the `?fieldManager=` query param (REQUIRED on an apply
/// request — a missing one is a typed 422 at the boundary, never reaching
/// here). `force` is `?force=true|false` (default false). `time` is the
/// boundary-frozen RFC3339 instant stamped onto the manager's
/// managedFields entry — the one non-deterministic input, captured once at
/// the apiserver boundary so every Raft replica replays identical bytes
/// (the same discipline `deletion_timestamp` + `creationTimestamp` obey).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyMeta {
    /// The field manager identity (`?fieldManager=`).
    pub manager: String,
    /// Take ownership of conflicting fields (`?force=`).
    pub force: bool,
    /// Frozen boundary RFC3339 instant for the managedFields entry.
    pub time: String,
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
        /// Server-side-apply metadata (`fieldManager` / `force` / frozen
        /// `time`). `Some` ONLY when `patch_type == PatchType::Apply`;
        /// `None` for every other patch algorithm and for a replayed
        /// pre-field log entry (`#[serde(default)]` → forward-compat). The
        /// REPLICATED apply metadata so every Raft node runs the IDENTICAL
        /// SSA decision (a leader-only `fieldManager`/clock read would
        /// diverge replicas — same law `deletion_timestamp` obeys).
        #[serde(default)]
        apply: Option<ApplyMeta>,
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

    /// An ATOMIC multi-key transaction — etcd's `Txn`.
    ///
    /// ★ WHY THIS EXISTS AND WHY IT MUST BE ONE COMMAND. Every other
    /// variant here touches exactly one key, which is all engenho's own
    /// apiserver ever needed. etcd's contract is different: a `Txn`
    /// evaluates a compare-list and then applies a WHOLE branch, and the
    /// result must be all-or-nothing. Expressing that as N separate
    /// commands would let a replica apply half a branch and would give each
    /// key its own revision — both observable, both wrong.
    ///
    /// ★ ONE TRANSACTION IS ONE REVISION. etcd stamps every key a `Txn`
    /// mutates with the SAME `mod_revision`. That falls out of the design
    /// here because [`crate::state::ResourceCatalog::apply`] reserves one
    /// revision per command; the branch's ops all commit at it.
    ///
    /// ★ THE COMPARES ARE PART OF THE REPLICATED COMMAND, for the same
    /// reason `expected` is on `Put`: a leader-only evaluation would let
    /// replicas take different branches and diverge.
    Txn {
        /// Predicates, ALL of which must hold to take `success`. An empty
        /// list is vacuously true and takes `success` — etcd's behaviour
        /// for an unconditional transaction.
        compares: Vec<TxnCompare>,
        /// Applied when every compare holds.
        success: Vec<TxnOp>,
        /// Applied otherwise. Usually empty (etcd's `Txn` without an else).
        failure: Vec<TxnOp>,
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
            apply: None,
            expected: None,
            reason,
        }
    }

    /// Construct a server-side-apply `Patch` (`patch_type ==
    /// PatchType::Apply`) carrying the boundary-frozen [`ApplyMeta`]
    /// (`fieldManager` / `force` / `time`). The apply body is the
    /// (possibly partial) declared object. `expected` is the optional CAS
    /// precondition (an apply may carry `metadata.resourceVersion`).
    #[must_use]
    pub fn apply_ssa(
        key: ResourceKey,
        body: ResourceValue,
        apply: ApplyMeta,
        expected: Option<Revision>,
        reason: Reason,
    ) -> Self {
        Self::Patch {
            key,
            patch: body,
            patch_type: PatchType::Apply,
            apply: Some(apply),
            expected,
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

    /// Construct an RFC 7396 merge `Patch` carrying a CAS precondition
    /// (`expected`) — the status-write shape that races a concurrent
    /// writer. `apply` is `None` (non-SSA). Equivalent to the struct literal
    /// with `patch_type: Merge, apply: None`.
    #[must_use]
    pub fn patch_cas(
        key: ResourceKey,
        patch: ResourceValue,
        expected: Option<Revision>,
        reason: Reason,
    ) -> Self {
        Self::Patch {
            key,
            patch,
            patch_type: PatchType::Merge,
            apply: None,
            expected,
            reason,
        }
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
    /// a bad JSON-Pointer, a missing strategic merge-key, or an unrecognized
    /// `$patch` directive). Like [`Self::Conflict`]: no mutation, no
    /// revision consumed, no history entry, no watch event — the catalog is
    /// byte-identical. The carried [`crate::patch_apply::PatchError`] is
    /// surfaced via [`crate::ApplyOutcome::patch_error`] so the apiserver
    /// renders the correct typed Status (422/400 for a bad patch).
    PatchRejected,
    /// A server-side apply hit one or more field-ownership CONFLICTS that
    /// `force` did not override — a path the apply wants is owned by a
    /// DIFFERENT apply-manager with a differing value. Like
    /// [`Self::Conflict`] / [`Self::PatchRejected`]: NO mutation, NO
    /// revision consumed, NO history entry, NO watch event — the catalog is
    /// byte-identical. The serialized `details.causes` array
    /// ([`crate::ssa::ApplyConflicts::to_causes`]) rides on
    /// [`crate::ApplyOutcome::patch_error`] so the apiserver renders the K8s
    /// 409 `Status` (reason "Conflict" + per-field causes) — NEVER a silent
    /// overwrite.
    ApplyConflict,
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
                apply: None,
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
    fn patch_deserializes_pre_apply_field_log_entry_as_none() {
        // Forward-compat: a serialized Patch from BEFORE the `apply` field
        // existed deserializes with apply == None — the same #[serde(default)]
        // contract default_patch_type/deletion_timestamp rely on, keeping
        // non-SSA patches byte-identical.
        let legacy = serde_json::json!({
            "kind": "patch",
            "key": ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm"),
            "patch": {"data": {"k": "v"}},
            "patch_type": "strategic",
            "expected": null,
            "reason": "operator"
        });
        let cmd: ResourceCommand = serde_json::from_value(legacy).unwrap();
        match cmd {
            ResourceCommand::Patch { apply, .. } => assert_eq!(apply, None),
            other => panic!("expected Patch, got {other:?}"),
        }
    }

    #[test]
    fn apply_ssa_constructor_round_trips() {
        let cmd = ResourceCommand::apply_ssa(
            ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm"),
            serde_json::json!({"data": {"a": "1"}}),
            ApplyMeta {
                manager: "kubectl".into(),
                force: true,
                time: "2026-06-08T00:00:00Z".into(),
            },
            None,
            Reason::Operator,
        );
        let s = serde_json::to_string(&cmd).unwrap();
        let back: ResourceCommand = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cmd);
        match back {
            ResourceCommand::Patch {
                patch_type, apply, ..
            } => {
                assert_eq!(patch_type, PatchType::Apply);
                let meta = apply.expect("apply meta present");
                assert_eq!(meta.manager, "kubectl");
                assert!(meta.force);
            }
            other => panic!("expected Patch, got {other:?}"),
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

/// One predicate in a [`ResourceCommand::Txn`].
///
/// Deliberately NOT etcd's full compare grammar. etcd can compare on
/// `VERSION`, `CREATE`, `MOD` and `VALUE`; kube-apiserver only ever emits
/// `MOD` (and existence, expressed as `MOD == 0`). Modelling the two forms
/// that are actually used keeps the unused ones UNREPRESENTABLE rather than
/// stubbed — a stubbed compare that silently returns `true` would take the
/// wrong branch and corrupt state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmp", rename_all = "snake_case")]
pub enum TxnCompare {
    /// The key's live `mod_revision` equals `revision`.
    ModRevisionEq {
        key: ResourceKey,
        revision: Revision,
    },
    /// The key does not exist. etcd spells this `MOD(key) == 0`, and it is
    /// the compare kube-apiserver uses for every create.
    NotExists { key: ResourceKey },
}

/// One operation inside a transaction branch.
///
/// A branch cannot contain a nested `Txn`: etcd allows it, kube-apiserver
/// never emits it, and permitting it would make the revision-per-command
/// invariant recursive for no consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TxnOp {
    Put {
        key: ResourceKey,
        value: ResourceValue,
    },
    Delete {
        key: ResourceKey,
        /// Frozen at the boundary, exactly as [`ResourceCommand::Delete`]
        /// requires — the clock is not a replicated input.
        deletion_timestamp: Option<String>,
    },
}
