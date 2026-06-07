//! `ResourceCatalog` — the deterministic state machine over the
//! Raft log. Each committed [`ResourceCommand`] mutates this;
//! followers and leader converge by replay.
//!
//! ## MVCC revision model (R6.1)
//!
//! The catalog tracks a global, strictly-monotonic [`Revision`]
//! counter that advances by EXACTLY ONE per real mutation (Put /
//! Patch / Delete that actually changes state). No-op mutations
//! (patch-missing, delete-not-found) and the Raft-internal entries
//! (blank on init, membership changes) do NOT consume a revision —
//! they are filtered out before `apply` is ever called for them, and
//! a no-op outcome here leaves the revision untouched.
//!
//! Per key the catalog stores `(value, VersionMeta)` where
//! [`VersionMeta`] carries `(create_revision, mod_revision,
//! version)` — etcd's per-key versioning triple. The wire
//! `metadata.resourceVersion` is stamped from the global revision
//! (NOT the Raft log index).
//!
//! A bounded in-memory history ring records every committed
//! [`Change`]; it is the watch-replay source. When the ring
//! overflows `history_capacity`, the oldest entry is evicted and the
//! `compacted_revision` watermark advances. Reads / watches that ask
//! for history below the watermark get a typed [`CompactedTooOld`].

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use crate::command::{ResourceCommand, ResourceOp};
use crate::pagination::ListPage;
use crate::resource::{ResourceKey, ResourceValue};
use crate::revision::{Change, ChangeKind, CompactedTooOld, Revision, VersionMeta};

/// Optimistic-concurrency precondition check (the CAS heart). Pure
/// function over the caller's expected revision + the key's live
/// version metadata:
///
///   * `expected == None` → unconditional, always ok (K8s semantics:
///     absent `resourceVersion`).
///   * `expected == Some(N)` and the key exists with
///     `meta.mod_revision == N` → ok.
///   * `expected == Some(N)` and (key missing OR `mod_revision != N`)
///     → CONFLICT.
///
/// Called FIRST inside each `apply_*` BEFORE any mutation. On `false`
/// the apply returns [`ResourceOp::Conflict`] + `change: None`, so the
/// outer [`ResourceCatalog::apply`] leaves `current_revision`
/// untouched, pushes no history, fans nothing to watchers — the
/// "no mutation / no revision advance" guarantee is reused verbatim.
#[must_use]
pub fn check_precondition(expected: Option<Revision>, live: Option<VersionMeta>) -> bool {
    match expected {
        None => true,
        Some(n) => live.is_some_and(|m| m.mod_revision == n),
    }
}

/// Default bound on the in-memory history ring. 8192 committed
/// mutations is enough to cover the recent window + the live tail
/// every local watch consumer replays; older revisions fall below
/// `compacted_revision`.
pub const DEFAULT_HISTORY_CAPACITY: usize = 8192;

/// What [`ResourceCatalog::apply`] returns: the resource operation
/// the apiserver / client gets back, plus the typed [`Change`] the
/// mutation committed (or `None` for a no-op).
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOutcome {
    /// Created / Replaced / Patched / Deleted / NoOp.
    pub op: ResourceOp,
    /// The committed change — `Some` for every real mutation, `None`
    /// for a no-op. Carries the post-image, the prior object (so the
    /// Deleted event always has the real prior, never Null), the
    /// stamped revision, and the per-key version metadata.
    pub change: Option<Change>,
}

/// In-memory K8s resource catalog. Keyed by [`ResourceKey`] (which
/// encodes group + version + kind + namespace + name), values are
/// `(ResourceValue, VersionMeta)` — the opaque JSON object plus its
/// MVCC version metadata.
///
/// The catalog tracks `last_applied_index` (Raft log index, for
/// read-after-write + snapshot resume) AND `current_revision` (the
/// global MVCC counter consumers stamp resourceVersion from).
#[derive(Clone, Debug)]
pub struct ResourceCatalog {
    /// Keyed store: value + per-key version metadata.
    pub resources: BTreeMap<ResourceKey, (ResourceValue, VersionMeta)>,
    pub last_applied_term: u64,
    pub last_applied_index: u64,
    /// Global MVCC revision counter — advances by 1 per real
    /// mutation. `Revision(0)` means "no write has happened yet".
    pub current_revision: Revision,
    /// Bounded history ring — the watch-replay source.
    pub history: VecDeque<Change>,
    /// Lowest revision still retained in `history`. Reads / watches
    /// below this return [`CompactedTooOld`]. `Revision(0)` means
    /// nothing has been compacted yet.
    pub compacted_revision: Revision,
    /// Max entries retained in `history` before the oldest is evicted
    /// (advancing `compacted_revision`).
    pub history_capacity: usize,
}

impl Default for ResourceCatalog {
    fn default() -> Self {
        Self {
            resources: BTreeMap::new(),
            last_applied_term: 0,
            last_applied_index: 0,
            current_revision: Revision::ZERO,
            history: VecDeque::new(),
            compacted_revision: Revision::ZERO,
            history_capacity: DEFAULT_HISTORY_CAPACITY,
        }
    }
}

impl PartialEq for ResourceCatalog {
    /// Equality covers the durable, convergence-relevant state:
    /// resources (value + version metadata), applied position, and
    /// the global revision. The history ring + capacity are a local
    /// replay buffer, not part of the converged state, so they're
    /// excluded — two nodes that applied the same command sequence
    /// are equal even if one has compacted more of its ring.
    fn eq(&self, other: &Self) -> bool {
        self.resources == other.resources
            && self.last_applied_term == other.last_applied_term
            && self.last_applied_index == other.last_applied_index
            && self.current_revision == other.current_revision
    }
}

// serde_json doesn't support struct map-keys directly. Custom impl
// flattens the BTreeMap into Vec<(ResourceKey, (ResourceValue,
// VersionMeta))> at the wire — preserves order (BTreeMap iter is
// sorted) so the resulting bytes are deterministic across nodes.
//
// The history ring is intentionally NOT serialized: it is a local
// watch-replay buffer rebuilt by re-applying the log, not part of
// the converged durable state. Persisting `current_revision` +
// `compacted_revision` keeps the contract honest across snapshot /
// restart.
impl Serialize for ResourceCatalog {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = ser.serialize_struct("ResourceCatalog", 5)?;
        let entries: Vec<(&ResourceKey, &(ResourceValue, VersionMeta))> =
            self.resources.iter().collect();
        state.serialize_field("resources", &entries)?;
        state.serialize_field("last_applied_term", &self.last_applied_term)?;
        state.serialize_field("last_applied_index", &self.last_applied_index)?;
        state.serialize_field("current_revision", &self.current_revision)?;
        state.serialize_field("compacted_revision", &self.compacted_revision)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ResourceCatalog {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            resources: Vec<(ResourceKey, (ResourceValue, VersionMeta))>,
            last_applied_term: u64,
            last_applied_index: u64,
            #[serde(default)]
            current_revision: Revision,
            #[serde(default)]
            compacted_revision: Revision,
        }
        let h = Helper::deserialize(de)?;
        Ok(Self {
            resources: h.resources.into_iter().collect(),
            last_applied_term: h.last_applied_term,
            last_applied_index: h.last_applied_index,
            current_revision: h.current_revision,
            history: VecDeque::new(),
            compacted_revision: h.compacted_revision,
            history_capacity: DEFAULT_HISTORY_CAPACITY,
        })
    }
}

impl ResourceCatalog {
    /// Construct a catalog with an explicit history-ring capacity.
    /// Used by callers that want a tighter compaction window (and by
    /// tests that force compaction with a tiny capacity).
    #[must_use]
    pub fn with_history_capacity(history_capacity: usize) -> Self {
        Self {
            history_capacity: history_capacity.max(1),
            ..Self::default()
        }
    }

    /// Apply a single committed command. Pure function. Returns the
    /// [`ApplyOutcome`] — operation + committed [`Change`].
    ///
    /// Revision semantics:
    ///
    ///   * A NoOp outcome (delete-not-found, patch-missing) does NOT
    ///     advance `current_revision` and emits no `Change`.
    ///   * Any real mutation advances `current_revision` by exactly
    ///     1, stamps `metadata.resourceVersion = <revision>` (NOT the
    ///     Raft index), preserves/sets `metadata.uid`, updates the
    ///     per-key [`VersionMeta`], and pushes a [`Change`] onto the
    ///     history ring (evicting the front + advancing
    ///     `compacted_revision` when over capacity).
    pub fn apply(&mut self, cmd: &ResourceCommand, term: u64, index: u64) -> ApplyOutcome {
        // Tentatively reserve the next revision; only committed if the
        // mutation is real (not a no-op).
        let rev = self.current_revision.next();
        let outcome = match cmd {
            ResourceCommand::Put {
                key,
                value,
                expected,
                ..
            } => self.apply_put(key, value, *expected, rev),
            ResourceCommand::Patch {
                key,
                patch,
                expected,
                ..
            } => self.apply_patch(key, patch, *expected, rev),
            ResourceCommand::Delete { key, expected, .. } => {
                self.apply_delete(key, *expected, rev)
            }
        };
        self.last_applied_term = term;
        self.last_applied_index = index;
        if let Some(change) = &outcome.change {
            // Commit the revision + record history only for real
            // mutations. The stamped revision is exactly `rev`.
            debug_assert_eq!(change.revision, rev);
            self.current_revision = rev;
            self.push_history(change.clone());
        }
        outcome
    }

    /// Push a change onto the ring, evicting the oldest + advancing
    /// the compaction watermark when over capacity.
    fn push_history(&mut self, change: Change) {
        self.history.push_back(change);
        while self.history.len() > self.history_capacity {
            if let Some(evicted) = self.history.pop_front() {
                // The lowest retained revision is now the revision of
                // the NEW front. Anything <= evicted.revision is gone.
                self.compacted_revision = evicted.revision;
            }
        }
    }

    fn apply_put(
        &mut self,
        key: &ResourceKey,
        value: &ResourceValue,
        expected: Option<Revision>,
        rev: Revision,
    ) -> ApplyOutcome {
        // Capture the pre-image BEFORE mutating — this is the prior
        // object every consumer (watch, optimistic-concurrency) needs.
        let prior_entry = self.resources.get(key).cloned();

        // Optimistic-concurrency precondition (CAS) — checked BEFORE any
        // mutation. On conflict: ResourceOp::Conflict + change: None, so
        // the outer apply leaves the catalog byte-identical (no revision
        // advance, no history, no fan-out).
        if !check_precondition(expected, prior_entry.as_ref().map(|(_, m)| *m)) {
            return ApplyOutcome {
                op: ResourceOp::Conflict,
                change: None,
            };
        }

        let prior_value = prior_entry.as_ref().map(|(v, _)| v.clone());

        let version_meta = match &prior_entry {
            Some((_, meta)) => meta.bumped_at(rev),
            None => VersionMeta::created_at(rev),
        };

        let mut new_value = value.clone();
        if let Some(obj) = new_value.as_object_mut() {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                meta_obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(rev.to_string()),
                );
                // Preserve uid across updates; mint a deterministic
                // one (from key + create_revision) on first create.
                let prior_uid = prior_entry
                    .as_ref()
                    .and_then(|(v, _)| v.get("metadata"))
                    .and_then(|m| m.get("uid"))
                    .cloned();
                if let Some(prior_uid) = prior_uid {
                    meta_obj.insert("uid".to_string(), prior_uid);
                } else if !meta_obj.contains_key("uid") {
                    let uid = format!(
                        "uid-{}-{}",
                        key.label().replace('/', "-"),
                        version_meta.create_revision
                    );
                    meta_obj.insert("uid".to_string(), serde_json::Value::String(uid));
                }
            }
        }

        self.resources
            .insert(key.clone(), (new_value.clone(), version_meta));

        let op = if prior_value.is_some() {
            ResourceOp::Replaced
        } else {
            ResourceOp::Created
        };
        ApplyOutcome {
            op,
            change: Some(Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: new_value,
                prior: prior_value,
                version_meta,
            }),
        }
    }

    fn apply_patch(
        &mut self,
        key: &ResourceKey,
        patch: &ResourceValue,
        expected: Option<Revision>,
        rev: Revision,
    ) -> ApplyOutcome {
        let Some((existing_value, existing_meta)) = self.resources.get(key) else {
            // Missing key. A `Some(_)` precondition can't be satisfied
            // (cannot CAS a non-existent object) → Conflict; an
            // unconditional patch on a missing key stays a NoOp (existing
            // semantics). Both leave the catalog byte-identical.
            let op = if expected.is_some() {
                ResourceOp::Conflict
            } else {
                ResourceOp::NoOp
            };
            return ApplyOutcome { op, change: None };
        };
        // Optimistic-concurrency precondition (CAS) on the existing key —
        // checked BEFORE the merge. On conflict the catalog is untouched.
        if !check_precondition(expected, Some(*existing_meta)) {
            return ApplyOutcome {
                op: ResourceOp::Conflict,
                change: None,
            };
        }
        // Capture pre-image before the merge.
        let prior_value = existing_value.clone();
        let version_meta = existing_meta.bumped_at(rev);

        let mut merged = existing_value.clone();
        merge_json(&mut merged, patch);
        if let Some(obj) = merged.as_object_mut() {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                meta_obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(rev.to_string()),
                );
            }
        }

        self.resources
            .insert(key.clone(), (merged.clone(), version_meta));

        ApplyOutcome {
            op: ResourceOp::Patched,
            change: Some(Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: merged,
                prior: Some(prior_value),
                version_meta,
            }),
        }
    }

    fn apply_delete(
        &mut self,
        key: &ResourceKey,
        expected: Option<Revision>,
        rev: Revision,
    ) -> ApplyOutcome {
        // Precondition (CAS) BEFORE the remove. A `Some(_)` precondition
        // on a missing key, or one that mismatches the live mod_revision,
        // is a Conflict; an unconditional delete-not-found stays NoOp.
        // Both leave the catalog byte-identical (the `remove` only runs
        // after the precondition passes).
        let live_meta = self.resources.get(key).map(|(_, m)| *m);
        if !check_precondition(expected, live_meta) {
            return ApplyOutcome {
                op: ResourceOp::Conflict,
                change: None,
            };
        }
        let Some((removed_value, removed_meta)) = self.resources.remove(key) else {
            return ApplyOutcome {
                op: ResourceOp::NoOp,
                change: None,
            };
        };
        // The tombstone (post-image) IS the last-known object — so
        // watch consumers see what was deleted. Both `value` and
        // `prior` carry it; the Deleted event never carries Null.
        ApplyOutcome {
            op: ResourceOp::Deleted,
            change: Some(Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Delete,
                value: removed_value.clone(),
                prior: Some(removed_value),
                version_meta: removed_meta,
            }),
        }
    }

    /// Read a single resource by key (value only).
    #[must_use]
    pub fn get(&self, key: &ResourceKey) -> Option<&ResourceValue> {
        self.resources.get(key).map(|(v, _)| v)
    }

    /// Read a single resource by key with its MVCC version metadata.
    #[must_use]
    pub fn get_with_meta(&self, key: &ResourceKey) -> Option<(&ResourceValue, VersionMeta)> {
        self.resources.get(key).map(|(v, m)| (v, *m))
    }

    /// The current global revision — the atomic list-snapshot
    /// resourceVersion. `list()` callers capture this under the same
    /// lock to get a consistent list-then-watch resume point.
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.current_revision
    }

    /// The compaction watermark — the lowest revision still
    /// retained in the history ring.
    #[must_use]
    pub fn compacted_revision(&self) -> Revision {
        self.compacted_revision
    }

    /// All committed changes with `revision > rv`, in revision order.
    ///
    /// This is the watch-replay primitive: a client that last saw
    /// revision `rv` resumes by replaying exactly these changes.
    ///
    /// # Errors
    ///
    /// Returns [`CompactedTooOld`] when `rv < compacted_revision` —
    /// the requested resume point has been compacted away (the 410
    /// Gone equivalent). Asking from exactly `compacted_revision` (or
    /// above) is always honored.
    pub fn changes_since(&self, rv: Revision) -> Result<Vec<Change>, CompactedTooOld> {
        if rv < self.compacted_revision {
            return Err(CompactedTooOld {
                requested: rv,
                compacted: self.compacted_revision,
            });
        }
        Ok(self
            .history
            .iter()
            .filter(|c| c.revision > rv)
            .cloned()
            .collect())
    }

    /// List resources matching (group, version, kind), optionally
    /// scoped to a namespace.
    #[must_use]
    pub fn list(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<(&ResourceKey, &ResourceValue)> {
        self.resources
            .iter()
            .filter(|(k, _)| k.group == group && k.version == version && k.kind == kind)
            .filter(|(k, _)| match (namespace, k.namespace.as_deref()) {
                (None, _) => true,
                (Some(want), Some(have)) => want == have,
                (Some(_), None) => false,
            })
            .map(|(k, (v, _))| (k, v))
            .collect()
    }

    /// One page of resources matching (group, version, kind), optionally
    /// namespace-scoped, in total [`ResourceKey`] order — the
    /// range-pagination primitive (etcd consistent-list semantics).
    ///
    /// Iterates the underlying [`BTreeMap`] in key order starting
    /// STRICTLY AFTER `after` (the continue cursor's last key), filtered
    /// by GVK + optional namespace, taking up to `limit` matching items.
    ///
    ///   * `next` = the last key returned IFF more matching items remain
    ///     after it (the cursor for the following page); else `None`.
    ///   * `remaining` = the count of still-unreturned matching items
    ///     after this page.
    ///   * `limit == 0` → return ALL matching items after `after`
    ///     (K8s: limit unset/0 = no bound), `next = None`,
    ///     `remaining = 0`.
    ///
    /// Filtering by GVK/namespace stays in the store (the catalog is
    /// GVK-keyed); SELECTOR filtering remains apiserver-side, which forces
    /// the apiserver's limit/continue over-fetch loop.
    ///
    /// ## Consistency: cursor-based, NOT MVCC snapshot-isolated (M0.1)
    ///
    /// Each call ranges the LIVE `BTreeMap` from `Bound::Excluded(after)`.
    /// For a QUIESCENT key set the page series is gap-free + dup-free, and
    /// a key that sorts AT OR BEFORE the cursor cannot resurface
    /// (mechanical cursor exclusion). It is NOT true MVCC snapshot
    /// isolation: a key inserted AFTER the cursor between page calls WILL
    /// surface on a later page (real etcd excludes it by revision). The
    /// continue token's `snapshot_rev` is only the envelope
    /// `resourceVersion` LABEL — it is NOT a read-isolation mechanism.
    ///
    /// DESTINATION (deferred): revision-indexed historical reads — page
    /// each request against the catalog AS OF the token's snapshot
    /// revision, which requires retaining historical MVCC views (a
    /// per-key revision history / time-travel index), not the single live
    /// materialized map M0.1 keeps. Until then, do not claim snapshot
    /// consistency for the page series.
    #[must_use]
    pub fn list_page(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> ListPage<'_> {
        // BTreeMap range starting STRICTLY after `after` (Excluded), to
        // the end (Unbounded). When `after` is None, scan from the start.
        let lower = match after {
            Some(k) => Bound::Excluded(k.clone()),
            None => Bound::Unbounded,
        };
        let matches = |k: &ResourceKey| {
            k.group == group
                && k.version == version
                && k.kind == kind
                && match (namespace, k.namespace.as_deref()) {
                    (None, _) => true,
                    (Some(want), Some(have)) => want == have,
                    (Some(_), None) => false,
                }
        };

        let mut filtered = self
            .resources
            .range((lower, Bound::Unbounded))
            .filter(|(k, _)| matches(k))
            .map(|(k, (v, _))| (k, v));

        // limit == 0 → unbounded: take all matching items after `after`.
        if limit == 0 {
            let items: Vec<(&ResourceKey, &ResourceValue)> = filtered.collect();
            return ListPage {
                items,
                next: None,
                remaining: 0,
            };
        }

        let mut items: Vec<(&ResourceKey, &ResourceValue)> = Vec::with_capacity(limit);
        for entry in filtered.by_ref().take(limit) {
            items.push(entry);
        }

        // Peek the remaining matching tail to set `next` + `remaining`.
        // `next` is the last EMITTED key iff at least one more matching
        // item exists after the page.
        let remaining_tail: u64 = filtered.count() as u64;
        let next = if remaining_tail > 0 {
            items.last().map(|(k, _)| (*k).clone())
        } else {
            None
        };

        ListPage {
            items,
            next,
            remaining: remaining_tail,
        }
    }

    /// Total resource count (across all kinds + namespaces).
    #[must_use]
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

/// Recursive JSON merge — fields in `patch` overwrite + null in
/// patch deletes. Mirrors RFC 7396 (JSON Merge Patch).
fn merge_json(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if !patch.is_object() {
        *target = patch.clone();
        return;
    }
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let target_obj = target.as_object_mut().unwrap();
    for (k, v) in patch.as_object().unwrap() {
        if v.is_null() {
            target_obj.remove(k);
        } else {
            let entry = target_obj
                .entry(k.clone())
                .or_insert_with(|| serde_json::Value::Null);
            merge_json(entry, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Reason;

    fn pod_key(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    fn put(cat: &mut ResourceCatalog, key: &ResourceKey, value: serde_json::Value, index: u64) -> ApplyOutcome {
        cat.apply(
            &ResourceCommand::Put {
                key: key.clone(),
                value,
                expected: None,
                reason: Reason::Operator,
            },
            1,
            index,
        )
    }

    fn delete(cat: &mut ResourceCatalog, key: &ResourceKey, index: u64) -> ApplyOutcome {
        cat.apply(
            &ResourceCommand::Delete {
                key: key.clone(),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            index,
        )
    }

    #[test]
    fn put_creates_then_replaces() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        let op = put(&mut cat, &k, serde_json::json!({"spec": {"image": "v1"}}), 1);
        assert_eq!(op.op, ResourceOp::Created);
        assert_eq!(cat.len(), 1);

        let op = put(&mut cat, &k, serde_json::json!({"spec": {"image": "v2"}}), 2);
        assert_eq!(op.op, ResourceOp::Replaced);
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn put_stamps_revision_not_raft_index() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        // Raft index 42, but this is the FIRST real mutation → rev 1.
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 42);
        let stored = cat.get(&k).unwrap();
        let metadata = stored.get("metadata").unwrap();
        assert_eq!(
            metadata.get("resourceVersion").unwrap(),
            &serde_json::json!("1"),
            "resourceVersion is the global revision (1), not the Raft index (42)"
        );
        assert_eq!(cat.revision(), Revision(1));
    }

    #[test]
    fn put_then_put_advances_mod_revision_stable_create_revision() {
        // I3: create_revision stable, mod_revision advances, version 1->2.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 5);
        let (_, m1) = cat.get_with_meta(&k).unwrap();
        assert_eq!(m1.create_revision, Revision(1));
        assert_eq!(m1.mod_revision, Revision(1));
        assert_eq!(m1.version, 1);

        put(&mut cat, &k, serde_json::json!({"spec": {"replaced": true}}), 6);
        let (_, m2) = cat.get_with_meta(&k).unwrap();
        assert_eq!(m2.create_revision, Revision(1), "create_revision stable");
        assert_eq!(m2.mod_revision, Revision(2), "mod_revision advances");
        assert_eq!(m2.version, 2, "version increments by exactly 1");
    }

    #[test]
    fn uid_is_set_and_preserved_across_replaces() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 1);
        let uid_1 = cat
            .get(&k)
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert!(uid_1.starts_with("uid-"));
        put(&mut cat, &k, serde_json::json!({"spec": {"x": true}}), 2);
        let uid_2 = cat
            .get(&k)
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(uid_1, uid_2);
    }

    #[test]
    fn delete_then_recreate_yields_new_create_revision() {
        // I3: recreate-after-delete gets a NEW create_revision + version resets.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("phoenix");
        put(&mut cat, &k, serde_json::json!({"spec": {"gen": 1}}), 1); // rev 1
        let (_, m1) = cat.get_with_meta(&k).unwrap();
        assert_eq!(m1.create_revision, Revision(1));

        delete(&mut cat, &k, 2); // rev 2
        assert!(cat.get(&k).is_none());

        put(&mut cat, &k, serde_json::json!({"spec": {"gen": 2}}), 3); // rev 3
        let (_, m2) = cat.get_with_meta(&k).unwrap();
        assert_eq!(
            m2.create_revision,
            Revision(3),
            "recreate gets a fresh create_revision"
        );
        assert_eq!(m2.mod_revision, Revision(3));
        assert_eq!(m2.version, 1, "version resets to 1 on recreate");
    }

    #[test]
    fn noop_does_not_advance_revision_or_history() {
        // I2: patch-missing + delete-not-found leave revision + history untouched.
        let mut cat = ResourceCatalog::default();
        assert_eq!(cat.revision(), Revision(0));

        let patched = cat.apply(
            &ResourceCommand::Patch {
                key: pod_key("ghost"),
                patch: serde_json::json!({"x": 1}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            1,
        );
        assert_eq!(patched.op, ResourceOp::NoOp);
        assert!(patched.change.is_none());
        assert_eq!(cat.revision(), Revision(0));
        assert!(cat.history.is_empty());

        let deleted = delete(&mut cat, &pod_key("ghost"), 2);
        assert_eq!(deleted.op, ResourceOp::NoOp);
        assert!(deleted.change.is_none());
        assert_eq!(cat.revision(), Revision(0));
        assert!(cat.history.is_empty());
    }

    #[test]
    fn patch_merges_into_existing_and_bumps_version() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        put(&mut cat, &k, serde_json::json!({"spec": {"image": "v1", "replicas": 2}}), 1);
        let op = cat.apply(
            &ResourceCommand::Patch {
                key: k.clone(),
                patch: serde_json::json!({"spec": {"image": "v2"}}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            2,
        );
        assert_eq!(op.op, ResourceOp::Patched);
        let change = op.change.unwrap();
        assert_eq!(change.revision, Revision(2));
        assert_eq!(change.version_meta.create_revision, Revision(1));
        assert_eq!(change.version_meta.mod_revision, Revision(2));
        assert_eq!(change.version_meta.version, 2);
        let stored = cat.get(&k).unwrap();
        assert_eq!(stored.get("spec").unwrap().get("image").unwrap(), "v2");
        assert_eq!(stored.get("spec").unwrap().get("replicas").unwrap(), 2);
    }

    #[test]
    fn delete_change_carries_prior_object_not_null() {
        // I4: every Delete change has prior == Some(last_known) and value != Null.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"image": "podinfo:6", "replicas": 3}}),
            1,
        );
        let op = delete(&mut cat, &k, 2);
        assert_eq!(op.op, ResourceOp::Deleted);
        let change = op.change.unwrap();
        assert_eq!(change.kind, ChangeKind::Delete);
        // value is the tombstone (the prior object), NEVER Null.
        assert!(!change.value.is_null());
        assert_eq!(
            change.value.get("spec").unwrap().get("image").unwrap(),
            "podinfo:6"
        );
        // prior carries the same last-known object.
        let prior = change.prior.unwrap();
        assert_eq!(prior.get("spec").unwrap().get("replicas").unwrap(), 3);
    }

    #[test]
    fn changes_since_returns_tail_in_order() {
        // I5: 5 puts (rev 1..5); changes_since(2) yields revs 3,4,5.
        let mut cat = ResourceCatalog::default();
        for i in 1..=5 {
            put(&mut cat, &pod_key(&format!("p{i}")), serde_json::json!({"i": i}), i);
        }
        let tail = cat.changes_since(Revision(2)).unwrap();
        let revs: Vec<u64> = tail.iter().map(|c| c.revision.0).collect();
        assert_eq!(revs, vec![3, 4, 5]);
        // changes_since(0) yields all five.
        assert_eq!(cat.changes_since(Revision(0)).unwrap().len(), 5);
        // changes_since(current) yields none.
        assert!(cat.changes_since(Revision(5)).unwrap().is_empty());
    }

    #[test]
    fn changes_since_below_compaction_errors() {
        // I5: force compaction with tiny capacity; below-watermark → CompactedTooOld.
        let mut cat = ResourceCatalog::with_history_capacity(2);
        for i in 1..=5 {
            put(&mut cat, &pod_key(&format!("p{i}")), serde_json::json!({"i": i}), i);
        }
        // Only the last 2 changes (rev 4, 5) are retained; compacted at rev 3.
        assert_eq!(cat.history.len(), 2);
        assert_eq!(cat.compacted_revision(), Revision(3));
        let err = cat.changes_since(Revision(1)).unwrap_err();
        assert_eq!(err.requested, Revision(1));
        assert_eq!(err.compacted, Revision(3));
        // Asking from exactly the watermark is honored.
        assert_eq!(cat.changes_since(Revision(3)).unwrap().len(), 2);
    }

    #[test]
    fn list_filters_by_gvk_and_namespace() {
        let mut cat = ResourceCatalog::default();
        let mut idx = 0;
        for name in ["a", "b", "c"] {
            idx += 1;
            put(&mut cat, &ResourceKey::namespaced("", "v1", "Pod", "default", name), serde_json::json!({}), idx);
        }
        idx += 1;
        put(&mut cat, &ResourceKey::namespaced("", "v1", "Pod", "kube-system", "coredns"), serde_json::json!({}), idx);
        idx += 1;
        put(&mut cat, &ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm-1"), serde_json::json!({}), idx);

        assert_eq!(cat.list("", "v1", "Pod", Some("default")).len(), 3);
        assert_eq!(cat.list("", "v1", "ConfigMap", Some("default")).len(), 1);
        assert_eq!(cat.list("", "v1", "Pod", None).len(), 4);
    }

    #[test]
    fn json_merge_patch_null_deletes_field() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("p");
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"image": "v1", "annotations": {"a": "b"}}}),
            1,
        );
        cat.apply(
            &ResourceCommand::Patch {
                key: k.clone(),
                patch: serde_json::json!({"spec": {"annotations": null}}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            2,
        );
        let stored = cat.get(&k).unwrap();
        assert!(
            stored
                .get("spec")
                .unwrap()
                .as_object()
                .unwrap()
                .get("annotations")
                .is_none()
        );
        assert_eq!(stored.get("spec").unwrap().get("image").unwrap(), "v1");
    }

    #[test]
    fn catalog_serde_carries_revision_state() {
        // Locks item-1's serde contract independent of fjall: the
        // catalog's hand-written Serialize/Deserialize round-trips
        // current_revision + compacted_revision + per-key VersionMeta.
        // This is exactly the durable state the fjall `catalog`
        // partition persists — proving the contract here means the
        // backend gets revision survival for free.
        let mut cat = ResourceCatalog::with_history_capacity(2);
        let k = pod_key("rev-state");
        // 5 puts → current_revision 5; capacity 2 forces compaction so
        // compacted_revision advances to 3 (last two changes retained).
        for i in 1..=5u64 {
            put(&mut cat, &pod_key(&format!("p{i}")), serde_json::json!({"i": i}), i);
        }
        // One more put on a tracked key so we can assert its meta.
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 6); // rev 6 (create)
        put(&mut cat, &k, serde_json::json!({"spec": {"x": 1}}), 7); // rev 7 (bump)
        assert_eq!(cat.current_revision, Revision(7));
        assert_eq!(cat.compacted_revision, Revision(5));
        let (_, meta_before) = cat.get_with_meta(&k).unwrap();
        assert_eq!(meta_before.create_revision, Revision(6));
        assert_eq!(meta_before.mod_revision, Revision(7));
        assert_eq!(meta_before.version, 2);

        // Round-trip through serde — the disk form.
        let bytes = serde_json::to_vec(&cat).unwrap();
        let back: ResourceCatalog = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(
            back.current_revision,
            Revision(7),
            "current_revision survives serde"
        );
        assert_eq!(
            back.compacted_revision,
            Revision(5),
            "compacted_revision survives serde"
        );
        let (_, meta_after) = back.get_with_meta(&k).unwrap();
        assert_eq!(
            meta_after, meta_before,
            "per-key VersionMeta is byte-identical across serde"
        );
        // History is deliberately not persisted (rebuilt by replay).
        assert!(back.history.is_empty());
        assert_eq!(back.history_capacity, DEFAULT_HISTORY_CAPACITY);
    }

    #[test]
    fn current_revision_counts_real_mutations() {
        // current_revision == count of non-NoOp ops.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("p");
        put(&mut cat, &k, serde_json::json!({}), 1); // rev 1
        put(&mut cat, &k, serde_json::json!({"x": 1}), 2); // rev 2
        delete(&mut cat, &k, 3); // rev 3
        delete(&mut cat, &k, 4); // NoOp — no revision
        cat.apply(
            &ResourceCommand::Patch {
                key: pod_key("ghost"),
                patch: serde_json::json!({"y": 1}),
                expected: None,
                reason: Reason::Operator,
            },
            1,
            5,
        ); // NoOp — no revision
        assert_eq!(cat.revision(), Revision(3));
    }
}
