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
use std::sync::Arc;

use engenho_types::patch::PatchType;

use crate::command::{ApplyMeta, ResourceCommand, ResourceOp};
use crate::pagination::ListPage;
use crate::patch_apply::{self, Gvk, OpenApiPatchEnv, PatchBody, PatchError, PatchSchemaEnv};
use crate::resource::{ResourceKey, ResourceValue};
use crate::ssa;
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
    /// Created / Replaced / Patched / Deleted / Conflict / PatchRejected /
    /// NoOp.
    pub op: ResourceOp,
    /// The committed change — `Some` for every real mutation, `None`
    /// for a no-op. Carries the post-image, the prior object (so the
    /// Deleted event always has the real prior, never Null), the
    /// stamped revision, and the per-key version metadata.
    pub change: Option<Change>,
    /// The typed patch-interpreter error string when `op ==
    /// [`ResourceOp::PatchRejected`]`; `None` otherwise. The apiserver maps
    /// it to the correct typed `ApiError` (415 for server-side apply,
    /// 422/400 for a bad patch body). Carried as a `String` so
    /// [`ApplyOutcome`] keeps its derives (the `PatchError` is rendered to a
    /// stable message at the rejection site).
    pub patch_error: Option<String>,
}

impl ApplyOutcome {
    /// An outcome with no committed change (a Conflict / NoOp / etc.) and no
    /// patch error.
    #[must_use]
    pub fn no_change(op: ResourceOp) -> Self {
        Self {
            op,
            change: None,
            patch_error: None,
        }
    }

    /// An outcome carrying a committed [`Change`].
    #[must_use]
    pub fn with_change(op: ResourceOp, change: Change) -> Self {
        Self {
            op,
            change: Some(change),
            patch_error: None,
        }
    }

    /// A [`ResourceOp::PatchRejected`] outcome carrying the typed patch
    /// error's stable message — no mutation, no revision consumed.
    #[must_use]
    pub fn patch_rejected(error: String) -> Self {
        Self {
            op: ResourceOp::PatchRejected,
            change: None,
            patch_error: Some(error),
        }
    }

    /// A [`ResourceOp::ApplyConflict`] outcome carrying the serialized
    /// `details.causes` JSON (from
    /// [`crate::ssa::ApplyConflicts::to_causes`]) — a server-side apply hit
    /// field-ownership conflicts `force` did not override. No mutation, no
    /// revision consumed; the apiserver renders a 409 `Status` with reason
    /// "Conflict" + the per-field causes. The causes JSON is carried as a
    /// string (via [`Self::patch_error`]) so [`ApplyOutcome`] keeps its
    /// derives, mirroring [`Self::patch_rejected`].
    #[must_use]
    pub fn apply_conflict(causes_json: String) -> Self {
        Self {
            op: ResourceOp::ApplyConflict,
            change: None,
            patch_error: Some(causes_json),
        }
    }
}

/// In-memory K8s resource catalog. Keyed by [`ResourceKey`] (which
/// encodes group + version + kind + namespace + name), values are
/// `(ResourceValue, VersionMeta)` — the opaque JSON object plus its
/// MVCC version metadata.
///
/// The catalog tracks `last_applied_index` (Raft log index, for
/// read-after-write + snapshot resume) AND `current_revision` (the
/// global MVCC counter consumers stamp resourceVersion from).
#[derive(Clone)]
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
    /// Strategic-merge schema resolver — the [`PatchSchemaEnv`] the patch
    /// interpreter consults for per-list merge strategies. NOT part of the
    /// converged/serialized state (it's a stateless, deterministic lookup
    /// over the `&'static` BLAKE3-pinned vendored OpenAPI docs — identical on
    /// every node), so it's excluded from `Serialize`/`Deserialize`/`PartialEq`
    /// and reconstructed via [`Default`] on deserialize. `Arc` so `Clone` is
    /// a cheap refcount bump (the catalog is cloned on snapshot/read paths).
    patch_env: Arc<dyn PatchSchemaEnv>,
}

impl std::fmt::Debug for ResourceCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceCatalog")
            .field("resources", &self.resources)
            .field("last_applied_term", &self.last_applied_term)
            .field("last_applied_index", &self.last_applied_index)
            .field("current_revision", &self.current_revision)
            .field("history", &self.history)
            .field("compacted_revision", &self.compacted_revision)
            .field("history_capacity", &self.history_capacity)
            // patch_env intentionally elided (trait object, no Debug).
            .finish_non_exhaustive()
    }
}

/// The default [`PatchSchemaEnv`] for a freshly-constructed catalog — the
/// OpenAPI-backed real resolver over the vendored docs. Cheap to build (empty
/// cache; documents are `&'static`).
fn default_patch_env() -> Arc<dyn PatchSchemaEnv> {
    Arc::new(OpenApiPatchEnv::new())
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
            patch_env: default_patch_env(),
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
            patch_env: default_patch_env(),
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
                patch_type,
                apply,
                expected,
                ..
            } => self.apply_patch(key, patch, *patch_type, apply.as_ref(), *expected, rev),
            ResourceCommand::Delete {
                key,
                expected,
                deletion_timestamp,
                ..
            } => self.apply_delete(key, *expected, deletion_timestamp.as_deref(), rev),
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
            return ApplyOutcome::no_change(ResourceOp::Conflict);
        }

        let prior_value = prior_entry.as_ref().map(|(v, _)| v.clone());

        let version_meta = match &prior_entry {
            Some((_, meta)) => meta.bumped_at(rev),
            None => VersionMeta::created_at(rev),
        };

        // generation reflects SPEC-INTENT revisions (the K8s contract
        // `observedGeneration` reconciles against), NOT every mutation.
        // On first create generation == 1; on replace it bumps iff
        // `spec` changed (deep compare prior vs new spec), else the prior
        // generation is preserved. A status-only replace (rare via Put,
        // common via Patch) thus leaves generation untouched. Computed in
        // the deterministic apply path so every Raft node stamps the
        // identical value.
        let next_generation = compute_generation_on_put(
            prior_entry.as_ref().map(|(v, _)| v),
            value,
        );

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
                meta_obj.insert(
                    "generation".to_string(),
                    serde_json::Value::Number(next_generation.into()),
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
                // Preserve creationTimestamp across updates (K8s: a create-
                // time, IMMUTABLE field — like uid). A Put of a fresh body
                // that drops the field (controllers re-Put a rebuilt object)
                // must NOT lose it: thread the prior object's value back in.
                // First create leaves whatever the (boundary-stamped) body
                // carries.
                let prior_creation = prior_entry
                    .as_ref()
                    .and_then(|(v, _)| v.get("metadata"))
                    .and_then(|m| m.get("creationTimestamp"))
                    .cloned();
                if let Some(prior_creation) = prior_creation {
                    meta_obj.insert("creationTimestamp".to_string(), prior_creation);
                }
            }
        }

        // Finalizer release: a Put that empties `metadata.finalizers` on a
        // deletionTimestamp-bearing object is the trigger that ACTUALLY
        // removes it (the finalizer gate kept it Terminating until now). The
        // post-merge value IS the object that would be stored — if it is
        // Terminating with no finalizers left, convert this Put into a
        // removal (emit Deleted) instead of storing the no-finalizer
        // Terminating object.
        if let Some(outcome) =
            self.finalizer_release_removal(key, &new_value, prior_value.as_ref(), rev)
        {
            return outcome;
        }

        self.resources
            .insert(key.clone(), (new_value.clone(), version_meta));

        let op = if prior_value.is_some() {
            ResourceOp::Replaced
        } else {
            ResourceOp::Created
        };
        ApplyOutcome::with_change(
            op,
            Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: new_value,
                prior: prior_value,
                version_meta,
            },
        )
    }

    fn apply_patch(
        &mut self,
        key: &ResourceKey,
        patch: &ResourceValue,
        patch_type: PatchType,
        apply: Option<&ApplyMeta>,
        expected: Option<Revision>,
        rev: Revision,
    ) -> ApplyOutcome {
        // ── Server-side apply branch ──────────────────────────────────────
        //
        // SSA is an UPSERT (create-if-absent + merge-if-present), so it must
        // run BEFORE the missing-key early-return below (a plain patch on a
        // missing key is a NoOp; an apply on a missing key CREATES). The
        // frozen `ApplyMeta` (fieldManager/force/time) carries the
        // replicated apply identity + clock, so every Raft node runs the
        // identical SSA decision. Non-apply patches fall through to the
        // existing typed-dispatch path UNCHANGED (BEHAVIOR PRESERVATION).
        if patch_type == PatchType::Apply {
            // An apply-typed command with NO ApplyMeta is a construction bug
            // at the boundary (the boundary rejects a missing fieldManager
            // with a 422 before proposing) — surface a typed rejection, never
            // a silent wrong answer.
            let Some(meta) = apply else {
                return ApplyOutcome::patch_rejected(
                    "server-side apply requires a fieldManager".to_string(),
                );
            };
            return self.apply_ssa_command(key, patch, meta, expected, rev);
        }

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
            return ApplyOutcome::no_change(op);
        };
        // Optimistic-concurrency precondition (CAS) on the existing key —
        // checked BEFORE the merge. On conflict the catalog is untouched.
        if !check_precondition(expected, Some(*existing_meta)) {
            return ApplyOutcome::no_change(ResourceOp::Conflict);
        }
        // Capture pre-image before the merge.
        let prior_value = existing_value.clone();
        let version_meta = existing_meta.bumped_at(rev);

        // ── Typed patch dispatch (the load-bearing fix) ────────────────────
        //
        // Instead of unconditionally RFC7396-merging EVERY patch (which
        // silently corrupted json-patch / strategic-merge), dispatch on the
        // typed `patch_type` into the correct algorithm via the
        // `patch_apply` interpreter. The `PatchSchemaEnv` (OpenAPI-backed by
        // default) resolves strategic-merge list strategies. A typed
        // `PatchError` (test failure, bad pointer, missing merge-key, SSA
        // deferral) becomes a `PatchRejected` outcome — NO mutation, NO
        // revision consumed, catalog byte-identical — surfaced to the
        // apiserver as the correct typed Status.
        let gvk = Gvk::from(key);
        let body = match PatchBody::from_raw(patch_type, patch.clone()) {
            Ok(b) => b,
            Err(e) => return ApplyOutcome::patch_rejected(e.to_string()),
        };
        let merged_result =
            patch_apply::apply(patch_type, existing_value, &body, &*self.patch_env, &gvk);
        let mut merged = match merged_result {
            Ok(m) => m,
            Err(PatchError::Unsupported { what }) => {
                // Typed-deferred (server-side apply) — surface as a typed
                // rejection, NEVER a silent strategic/merge fallback.
                return ApplyOutcome::patch_rejected(
                    PatchError::Unsupported { what }.to_string(),
                );
            }
            Err(e) => return ApplyOutcome::patch_rejected(e.to_string()),
        };
        // generation bumps iff the MERGED `spec` differs from the prior
        // `spec`. A patch touching ONLY `status` (or only metadata) leaves
        // `spec` unchanged → generation preserved. This is the
        // load-bearing invariant the controllers' observedGeneration
        // convergence relies on: a status-only write never advances the
        // generation it is meant to be catching up to.
        let next_generation = compute_generation_on_put(Some(&prior_value), &merged);
        if let Some(obj) = merged.as_object_mut() {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                meta_obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(rev.to_string()),
                );
                meta_obj.insert(
                    "generation".to_string(),
                    serde_json::Value::Number(next_generation.into()),
                );
            }
        }

        // Finalizer release (same rule as apply_put): a patch that empties
        // `metadata.finalizers` on a deletionTimestamp-bearing object
        // converts into a removal. THIS is what makes
        // `kubectl patch …finalizers:null` actually delete a Terminating
        // object.
        if let Some(outcome) =
            self.finalizer_release_removal(key, &merged, Some(&prior_value), rev)
        {
            return outcome;
        }

        self.resources
            .insert(key.clone(), (merged.clone(), version_meta));

        ApplyOutcome::with_change(
            ResourceOp::Patched,
            Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: merged,
                prior: Some(prior_value),
                version_meta,
            },
        )
    }

    /// Server-side apply — the upsert path for an `application/apply-patch`
    /// request. REUSES the strategic-merge engine via
    /// [`crate::ssa::apply_ssa`] (no second merge engine) + the SAME
    /// `patch_env` strategic-merge consults, and stamps the SAME
    /// resourceVersion/generation/uid metadata the create/patch paths do.
    ///
    /// On an unforced field-ownership conflict → [`ResourceOp::ApplyConflict`]
    /// (no mutation, catalog byte-identical) carrying the serialized
    /// `details.causes`. Otherwise the merged object (with its updated
    /// `metadata.managedFields`) is stored and a [`ResourceOp::Created`] /
    /// [`ResourceOp::Patched`] change is emitted — the same store-and-emit
    /// shape as the non-SSA paths.
    fn apply_ssa_command(
        &mut self,
        key: &ResourceKey,
        body: &ResourceValue,
        meta: &ApplyMeta,
        expected: Option<Revision>,
        rev: Revision,
    ) -> ApplyOutcome {
        let prior_entry = self.resources.get(key).cloned();

        // Optimistic-concurrency precondition (CAS) — checked BEFORE the
        // merge, identical to apply_put/apply_patch. On conflict the catalog
        // is untouched (no revision advance, no history, no fan-out).
        if !check_precondition(expected, prior_entry.as_ref().map(|(_, m)| *m)) {
            return ApplyOutcome::no_change(ResourceOp::Conflict);
        }

        let prior_value = prior_entry.as_ref().map(|(v, _)| v.clone());
        let gvk = Gvk::from(key);

        // Run the PURE SSA interpreter (reuses strategic_merge + the env).
        let outcome = ssa::apply_ssa(
            prior_value.as_ref(),
            body,
            &meta.manager,
            meta.force,
            &gvk,
            &*self.patch_env,
            &meta.time,
        );
        let ssa_out = match outcome {
            Ok(o) => o,
            Err(conflicts) => {
                // Typed 409 — NEVER a silent overwrite. The serialized
                // details.causes ride on patch_error.
                let causes = conflicts.to_causes();
                return ApplyOutcome::apply_conflict(causes.to_string());
            }
        };

        let mut merged = ssa_out.merged().clone();

        // Stamp resourceVersion + generation + uid — the SAME metadata the
        // create/patch paths stamp, computed deterministically in the apply
        // path so every Raft replica is byte-identical. (managedFields was
        // already written by apply_ssa into the merged object.)
        let version_meta = match &prior_entry {
            Some((_, m)) => m.bumped_at(rev),
            None => VersionMeta::created_at(rev),
        };
        let next_generation = compute_generation_on_put(prior_value.as_ref(), &merged);
        stamp_object_metadata(
            &mut merged,
            key,
            rev,
            next_generation,
            version_meta,
            prior_entry.as_ref().map(|(v, _)| v),
        );

        // Finalizer release (same rule as apply_put/apply_patch): an apply
        // that empties finalizers on a Terminating object converts to a
        // removal. Only meaningful for an UPDATE (prior present).
        if prior_value.is_some() {
            if let Some(out) =
                self.finalizer_release_removal(key, &merged, prior_value.as_ref(), rev)
            {
                return out;
            }
        }

        self.resources
            .insert(key.clone(), (merged.clone(), version_meta));

        let op = if prior_value.is_some() {
            ResourceOp::Patched
        } else {
            ResourceOp::Created
        };
        ApplyOutcome::with_change(
            op,
            Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: merged,
                prior: prior_value,
                version_meta,
            },
        )
    }

    /// Shared finalizer-release rule consumed by both [`Self::apply_put`]
    /// and [`Self::apply_patch`]: if `post` is a Terminating object
    /// (carries `metadata.deletionTimestamp`) whose `metadata.finalizers`
    /// is now EMPTY, the mutation that produced `post` is the trigger that
    /// actually removes the object. Returns `Some(Deleted outcome)` (and
    /// removes the key) when the rule fires; `None` otherwise (the caller
    /// proceeds with its normal store-and-emit).
    ///
    /// Only fires for an UPDATE (`prior.is_some()`): a fresh create can't
    /// be Terminating, and we never want a first-create Put to vanish. The
    /// Deleted change carries `post` as the tombstone (the last-known
    /// object, finalizers already cleared) so watch consumers see the real
    /// final object, never Null.
    fn finalizer_release_removal(
        &mut self,
        key: &ResourceKey,
        post: &ResourceValue,
        prior: Option<&ResourceValue>,
        rev: Revision,
    ) -> Option<ApplyOutcome> {
        if prior.is_none() {
            return None;
        }
        if deletion_timestamp_of(post).is_none() || has_finalizers(post) {
            return None;
        }
        // Terminating + finalizers cleared ⇒ remove now. The version_meta
        // on the tombstone is the live key's metadata (it is being removed,
        // so its mod_revision is whatever it last was — we surface `rev` as
        // the change revision, matching apply_delete's removal shape).
        let removed_meta = self
            .resources
            .remove(key)
            .map(|(_, m)| m)
            .unwrap_or_else(|| VersionMeta::created_at(rev));
        Some(ApplyOutcome::with_change(
            ResourceOp::Deleted,
            Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Delete,
                value: post.clone(),
                prior: Some(post.clone()),
                version_meta: removed_meta,
            },
        ))
    }

    /// The finalizer delete-gate (the deterministic replay site — runs
    /// IDENTICALLY on every Raft replica).
    ///
    /// Behavior, in order:
    ///
    /// 1. CAS precondition (unchanged) — Conflict on mismatch; NoOp on
    ///    unconditional delete-not-found. Both leave the catalog
    ///    byte-identical.
    /// 2. **No finalizers** ⇒ remove immediately, exactly as the historical
    ///    behavior: same `remove`, same `ChangeKind::Delete`, same
    ///    [`ResourceOp::Deleted`] (BEHAVIOR-PRESERVATION — a normal delete
    ///    on a no-finalizer object is untouched).
    /// 3. **Finalizers present + `deletionTimestamp` NOT yet set** ⇒ do NOT
    ///    remove. Stamp `metadata.deletionTimestamp` from the REPLICATED
    ///    `deletion_timestamp` scalar (never a per-node clock), bump the
    ///    per-key version + global revision, and emit a `ChangeKind::Put`
    ///    (Modified) event carrying the now-Terminating object. Returns
    ///    [`ResourceOp::DeletionPending`].
    /// 4. **Finalizers present + `deletionTimestamp` ALREADY set** ⇒
    ///    idempotent [`ResourceOp::NoOp`] (no revision, no event) — repeated
    ///    `kubectl delete` on a Terminating object does not churn.
    ///
    /// The object is removed ONLY later, when an update/patch empties
    /// `metadata.finalizers` on a deletionTimestamp-bearing object (see
    /// [`Self::apply_put`] / [`Self::apply_patch`]).
    fn apply_delete(
        &mut self,
        key: &ResourceKey,
        expected: Option<Revision>,
        deletion_timestamp: Option<&str>,
        rev: Revision,
    ) -> ApplyOutcome {
        // Precondition (CAS) BEFORE any mutation. A `Some(_)` precondition
        // on a missing key, or one that mismatches the live mod_revision,
        // is a Conflict; an unconditional delete-not-found stays NoOp.
        // Both leave the catalog byte-identical.
        let live_meta = self.resources.get(key).map(|(_, m)| *m);
        if !check_precondition(expected, live_meta) {
            return ApplyOutcome::no_change(ResourceOp::Conflict);
        }

        // Read the live object to inspect finalizers + an existing
        // deletionTimestamp. Absent key (after the precondition passed —
        // only possible for an unconditional delete) ⇒ NoOp, unchanged.
        let Some((live_value, live_meta)) = self.resources.get(key).cloned() else {
            return ApplyOutcome::no_change(ResourceOp::NoOp);
        };

        // ── BEHAVIOR-PRESERVATION: no finalizers ⇒ remove immediately. ──
        if !has_finalizers(&live_value) {
            let (removed_value, removed_meta) = self
                .resources
                .remove(key)
                .expect("key present (just read above)");
            return ApplyOutcome::with_change(
                ResourceOp::Deleted,
                Change {
                    revision: rev,
                    key: key.clone(),
                    kind: ChangeKind::Delete,
                    value: removed_value.clone(),
                    prior: Some(removed_value),
                    version_meta: removed_meta,
                },
            );
        }

        // ── Finalizers present. ──
        // Idempotent: already Terminating ⇒ no churn (no revision, no event).
        if deletion_timestamp_of(&live_value).is_some() {
            return ApplyOutcome::no_change(ResourceOp::NoOp);
        }

        // First delete on a finalizer-bearing object: stamp
        // deletionTimestamp from the REPLICATED scalar (deterministic).
        // A None scalar here (an unconditional GC delete that didn't
        // freeze a boundary clock) means we have no timestamp to stamp —
        // leave the object untouched (NoOp) rather than invent a
        // non-replicated value. The apiserver delete path always threads
        // a frozen timestamp for finalizer-bearing objects, so the
        // operator-driven path always reaches the Terminating stamp; a
        // controller/GC pass that wants the stamp threads it too.
        let Some(ts) = deletion_timestamp else {
            return ApplyOutcome::no_change(ResourceOp::NoOp);
        };

        let version_meta = live_meta.bumped_at(rev);
        // Pre-image (before the deletionTimestamp stamp) — the `prior` the
        // Modified watch event carries.
        let prior = live_value.clone();
        let mut terminating = live_value;
        stamp_deletion_timestamp(&mut terminating, ts);
        // resourceVersion follows the bump (so a watcher's CAS sees the new
        // rev); generation is spec-intent and unchanged by a metadata-only
        // deletionTimestamp stamp, so it is preserved verbatim.
        if let Some(meta_obj) = terminating
            .as_object_mut()
            .and_then(|o| o.get_mut("metadata"))
            .and_then(|m| m.as_object_mut())
        {
            meta_obj.insert(
                "resourceVersion".to_string(),
                serde_json::Value::String(rev.to_string()),
            );
        }

        self.resources
            .insert(key.clone(), (terminating.clone(), version_meta));

        // A Put (Modified) event, NOT a Delete — the object is still there,
        // now Terminating. Watch consumers see a MODIFIED, kubectl sees the
        // object with a deletionTimestamp.
        ApplyOutcome::with_change(
            ResourceOp::DeletionPending,
            Change {
                revision: rev,
                key: key.clone(),
                kind: ChangeKind::Put,
                value: terminating,
                prior: Some(prior),
                version_meta,
            },
        )
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

/// Borrow `value.spec` if present.
fn spec_of(value: &serde_json::Value) -> Option<&serde_json::Value> {
    value.get("spec")
}

/// `true` iff the object carries a NON-EMPTY `metadata.finalizers` array.
/// An absent field, a non-array, or an empty array all count as "no
/// finalizers" (the K8s contract: finalizers gate deletion only while the
/// list is non-empty).
fn has_finalizers(value: &serde_json::Value) -> bool {
    value
        .get("metadata")
        .and_then(|m| m.get("finalizers"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|a| !a.is_empty())
}

/// Borrow the object's `metadata.deletionTimestamp` string when present
/// and non-empty (Terminating marker). `None` when absent/empty/non-string.
fn deletion_timestamp_of(value: &serde_json::Value) -> Option<&str> {
    value
        .get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Stamp `metadata.resourceVersion` + `metadata.generation` + `metadata.uid`
/// on `value` — the deterministic apiserver-owned-metadata pass shared by
/// the SSA path (and modeled on the identical inline block in `apply_put`):
/// resourceVersion from the global `rev`, generation from
/// `compute_generation_on_put`, uid preserved from the prior object or minted
/// deterministically (from key + create_revision) on first create. Pure JSON
/// mutation — no clock, no RNG — so every Raft replica is byte-identical.
fn stamp_object_metadata(
    value: &mut serde_json::Value,
    key: &ResourceKey,
    rev: Revision,
    next_generation: i64,
    version_meta: VersionMeta,
    prior_value: Option<&serde_json::Value>,
) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return;
    };
    meta_obj.insert(
        "resourceVersion".to_string(),
        serde_json::Value::String(rev.to_string()),
    );
    meta_obj.insert(
        "generation".to_string(),
        serde_json::Value::Number(next_generation.into()),
    );
    let prior_uid = prior_value
        .and_then(|v| v.get("metadata"))
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
    // Preserve creationTimestamp across updates (K8s create-time IMMUTABLE
    // field — mirrors the uid preservation above + the inline `apply_put`
    // block). A re-Put that dropped the field must not lose it.
    let prior_creation = prior_value
        .and_then(|v| v.get("metadata"))
        .and_then(|m| m.get("creationTimestamp"))
        .cloned();
    if let Some(prior_creation) = prior_creation {
        meta_obj.insert("creationTimestamp".to_string(), prior_creation);
    }
}

/// Set `metadata.deletionTimestamp` to the REPLICATED `ts` string,
/// creating `metadata` if absent. Pure JSON mutation of the opaque body —
/// the typed RFC3339 render already happened at the apiserver boundary
/// (`engenho_types::time`); here we only thread the frozen scalar in.
fn stamp_deletion_timestamp(value: &mut serde_json::Value, ts: &str) {
    if let Some(obj) = value.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            meta_obj.insert(
                "deletionTimestamp".to_string(),
                serde_json::Value::String(ts.to_string()),
            );
        }
    }
}

/// Read `metadata.generation` off a stored object (the value already in
/// hand). `0` (the "no generation" sentinel) when absent or non-integer —
/// the very first create's prior is `None`, not a zero-generation object.
fn generation_of(value: &serde_json::Value) -> i64 {
    value
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
}

/// Compute the `metadata.generation` to stamp on a create/replace/patch.
///
///   * No prior (first create) → `1`.
///   * Prior exists + `spec` UNCHANGED → preserve the prior generation
///     (treating a missing prior generation as `1`, since a
///     pre-generation object is logically at its first spec-intent).
///   * Prior exists + `spec` CHANGED → prior generation + 1.
///
/// `spec`-equality is a structural deep compare of the two `spec`
/// subtrees (both-absent counts as equal). Pure + deterministic so every
/// Raft node stamps the identical generation.
#[must_use]
fn compute_generation_on_put(prior: Option<&serde_json::Value>, next: &serde_json::Value) -> i64 {
    match prior {
        None => 1,
        Some(prior_value) => {
            let prior_gen = generation_of(prior_value).max(1);
            if spec_of(prior_value) == spec_of(next) {
                prior_gen
            } else {
                prior_gen + 1
            }
        }
    }
}

// The RFC 7396 JSON Merge Patch implementation that previously lived here
// (`merge_json`) is now the canonical `patch_apply::merge_rfc7396`, consumed
// by the typed `Merge` arm of `patch_apply::apply`. apply_patch no longer
// merges directly — it dispatches through the interpreter on `patch_type`.

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
                deletion_timestamp: None,
            },
            1,
            index,
        )
    }

    /// Delete carrying a frozen boundary timestamp (the apiserver-path shape).
    fn delete_at(
        cat: &mut ResourceCatalog,
        key: &ResourceKey,
        ts: &str,
        index: u64,
    ) -> ApplyOutcome {
        cat.apply(
            &ResourceCommand::Delete {
                key: key.clone(),
                expected: None,
                reason: Reason::Operator,
                deletion_timestamp: Some(ts.to_string()),
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
                patch_type: PatchType::Merge,
                apply: None,
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
                patch_type: PatchType::Merge,
                apply: None,
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
                patch_type: PatchType::Merge,
                apply: None,
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

    /// Read `metadata.generation` off the live stored object.
    fn live_generation(cat: &ResourceCatalog, key: &ResourceKey) -> i64 {
        cat.get(key)
            .and_then(|v| v.get("metadata"))
            .and_then(|m| m.get("generation"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or_else(|| panic!("metadata.generation missing"))
    }

    fn patch(
        cat: &mut ResourceCatalog,
        key: &ResourceKey,
        patch: serde_json::Value,
        index: u64,
    ) -> ApplyOutcome {
        cat.apply(
            &ResourceCommand::Patch {
                key: key.clone(),
                patch,
                patch_type: PatchType::Merge,
                apply: None,
                expected: None,
                reason: Reason::Operator,
            },
            1,
            index,
        )
    }

    #[test]
    fn put_first_create_stamps_generation_1() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-create");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 1}}), 1);
        assert_eq!(live_generation(&cat, &k), 1, "first create → generation 1");
    }

    #[test]
    fn put_replace_with_changed_spec_bumps_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-bump");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 1}}), 1);
        assert_eq!(live_generation(&cat, &k), 1);
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 2}}), 2);
        assert_eq!(
            live_generation(&cat, &k),
            2,
            "spec changed on replace → generation bumps"
        );
    }

    #[test]
    fn put_replace_with_identical_spec_preserves_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-stable");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 3}}), 1);
        assert_eq!(live_generation(&cat, &k), 1);
        // Replace with the SAME spec but a changed status — generation
        // must NOT bump (status is not spec-intent).
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 3}, "status": {"replicas": 3}}),
            2,
        );
        assert_eq!(
            live_generation(&cat, &k),
            1,
            "identical spec on replace → generation preserved"
        );
    }

    #[test]
    fn patch_status_only_does_not_bump_generation() {
        // THE LOAD-BEARING INVARIANT: a status-only patch leaves
        // metadata.generation unchanged so observedGeneration can
        // converge against it.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-status-patch");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 2}}), 1);
        assert_eq!(live_generation(&cat, &k), 1);
        let out = patch(
            &mut cat,
            &k,
            serde_json::json!({"status": {"readyReplicas": 2}}),
            2,
        );
        assert_eq!(out.op, ResourceOp::Patched);
        assert_eq!(
            live_generation(&cat, &k),
            1,
            "status-only patch must NOT bump generation"
        );
        // The status landed.
        assert_eq!(
            cat.get(&k).unwrap().get("status").unwrap().get("readyReplicas").unwrap(),
            2
        );
    }

    #[test]
    fn patch_spec_change_bumps_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-spec-patch");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 2}}), 1);
        assert_eq!(live_generation(&cat, &k), 1);
        let out = patch(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 5}}),
            2,
        );
        assert_eq!(out.op, ResourceOp::Patched);
        assert_eq!(
            live_generation(&cat, &k),
            2,
            "spec-changing patch bumps generation"
        );
    }

    #[test]
    fn patch_metadata_only_does_not_bump_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-meta-patch");
        put(&mut cat, &k, serde_json::json!({"spec": {"replicas": 1}}), 1);
        let out = patch(
            &mut cat,
            &k,
            serde_json::json!({"metadata": {"labels": {"team": "x"}}}),
            2,
        );
        assert_eq!(out.op, ResourceOp::Patched);
        assert_eq!(
            live_generation(&cat, &k),
            1,
            "metadata-only patch must NOT bump generation"
        );
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
                patch_type: PatchType::Merge,
                apply: None,
                expected: None,
                reason: Reason::Operator,
            },
            1,
            5,
        ); // NoOp — no revision
        assert_eq!(cat.revision(), Revision(3));
    }

    // ── Finalizer delete-gate ──────────────────────────────────────────

    fn put_with_finalizer(
        cat: &mut ResourceCatalog,
        key: &ResourceKey,
        index: u64,
    ) -> ApplyOutcome {
        put(
            cat,
            key,
            serde_json::json!({
                "spec": {"image": "v1"},
                "metadata": {"finalizers": ["example.com/hold"]}
            }),
            index,
        )
    }

    #[test]
    fn delete_no_finalizer_removes_immediately_behavior_preserved() {
        // (a) BEHAVIOR-PRESERVATION: a normal delete on a no-finalizer
        // object removes it immediately + emits Deleted (UNCHANGED today).
        let mut cat = ResourceCatalog::default();
        let k = pod_key("plain");
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 1);
        let out = delete(&mut cat, &k, 2);
        assert_eq!(out.op, ResourceOp::Deleted);
        assert_eq!(out.change.unwrap().kind, ChangeKind::Delete);
        assert!(cat.get(&k).is_none(), "no-finalizer object is gone");
        assert_eq!(cat.revision(), Revision(2));
    }

    #[test]
    fn delete_with_finalizer_stamps_deletion_timestamp_keeps_object() {
        // (b) finalizer-bearing delete with a replicated deletion_timestamp:
        // stamps metadata.deletionTimestamp, KEEPS the object, emits a
        // Put/Modified Change, consumes one revision.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("held");
        put_with_finalizer(&mut cat, &k, 1); // rev 1
        let out = delete_at(&mut cat, &k, "2026-06-08T12:00:00Z", 2);
        assert_eq!(out.op, ResourceOp::DeletionPending);
        let change = out.change.expect("a change committed");
        assert_eq!(change.kind, ChangeKind::Put, "Modified event, not Delete");
        assert_eq!(change.revision, Revision(2), "one revision consumed");
        assert_eq!(cat.revision(), Revision(2));
        // The object is STILL present, now Terminating.
        let live = cat.get(&k).expect("object kept (Terminating)");
        assert_eq!(
            live.get("metadata").unwrap().get("deletionTimestamp").unwrap(),
            "2026-06-08T12:00:00Z"
        );
        // Finalizer preserved (still blocking removal).
        assert!(
            live.get("metadata")
                .unwrap()
                .get("finalizers")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "example.com/hold")
        );
    }

    #[test]
    fn second_delete_on_terminating_is_idempotent_noop() {
        // (c) a second identical delete while already Terminating is an
        // idempotent NoOp (no revision, no event).
        let mut cat = ResourceCatalog::default();
        let k = pod_key("held2");
        put_with_finalizer(&mut cat, &k, 1); // rev 1
        delete_at(&mut cat, &k, "2026-06-08T12:00:00Z", 2); // rev 2 (Terminating)
        assert_eq!(cat.revision(), Revision(2));
        let out = delete_at(&mut cat, &k, "2026-06-08T13:00:00Z", 3);
        assert_eq!(out.op, ResourceOp::NoOp, "already Terminating → NoOp");
        assert!(out.change.is_none(), "no event on idempotent re-delete");
        assert_eq!(cat.revision(), Revision(2), "no revision consumed");
        // deletionTimestamp unchanged (the FIRST one, not the second).
        assert_eq!(
            cat.get(&k)
                .unwrap()
                .get("metadata")
                .unwrap()
                .get("deletionTimestamp")
                .unwrap(),
            "2026-06-08T12:00:00Z"
        );
    }

    #[test]
    fn patch_clearing_finalizers_on_terminating_removes_object() {
        // (d) a patch clearing finalizers on a deletionTimestamp-bearing
        // object converts to a removal (Deleted emitted, key gone). THIS is
        // `kubectl patch …finalizers:null`.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("release");
        put_with_finalizer(&mut cat, &k, 1); // rev 1
        delete_at(&mut cat, &k, "2026-06-08T12:00:00Z", 2); // rev 2 → Terminating
        assert!(cat.get(&k).is_some(), "still present while finalized");
        // Merge-patch finalizers to [] (empty) — the release trigger.
        let out = patch(
            &mut cat,
            &k,
            serde_json::json!({"metadata": {"finalizers": []}}),
            3,
        );
        assert_eq!(out.op, ResourceOp::Deleted, "finalizer-release → Deleted");
        assert_eq!(out.change.unwrap().kind, ChangeKind::Delete);
        assert!(cat.get(&k).is_none(), "object removed once finalizers cleared");
    }

    #[test]
    fn put_clearing_finalizers_on_terminating_removes_object() {
        // The apply_put sibling of the patch rule: a full replace that drops
        // finalizers on a Terminating object also removes it.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("release-put");
        put_with_finalizer(&mut cat, &k, 1);
        delete_at(&mut cat, &k, "2026-06-08T12:00:00Z", 2);
        // Replace the whole object WITHOUT finalizers but carrying the
        // deletionTimestamp (as a real client doing the final write would).
        let out = put(
            &mut cat,
            &k,
            serde_json::json!({
                "spec": {"image": "v1"},
                "metadata": {"deletionTimestamp": "2026-06-08T12:00:00Z"}
            }),
            3,
        );
        assert_eq!(out.op, ResourceOp::Deleted);
        assert!(cat.get(&k).is_none());
    }

    #[test]
    fn delete_with_finalizer_but_no_timestamp_is_noop() {
        // A finalizer-bearing object deleted WITHOUT a replicated timestamp
        // (an unconditional GC delete that didn't freeze a boundary clock):
        // we don't invent a non-replicated value, so it's a NoOp (object
        // kept, no churn). The apiserver path always threads a timestamp for
        // finalizer-bearing objects, so this is only the controller/GC edge.
        let mut cat = ResourceCatalog::default();
        let k = pod_key("held-no-ts");
        put_with_finalizer(&mut cat, &k, 1);
        let out = delete(&mut cat, &k, 2);
        assert_eq!(out.op, ResourceOp::NoOp);
        assert!(out.change.is_none());
        assert!(cat.get(&k).is_some(), "kept (no timestamp to stamp)");
    }

    #[test]
    fn finalizer_gate_is_deterministic_across_two_replays() {
        // DETERMINISM: two independent catalogs applying the SAME command
        // sequence (with the SAME frozen deletion_timestamp) produce
        // byte-identical objects — the property every Raft replica relies
        // on. The deletion_timestamp is a replicated scalar; apply_delete
        // never reads a clock.
        let seq: Vec<ResourceCommand> = vec![
            ResourceCommand::put(
                pod_key("d"),
                serde_json::json!({
                    "spec": {"image": "v1"},
                    "metadata": {"finalizers": ["example.com/hold"]}
                }),
                Reason::Operator,
            ),
            ResourceCommand::delete_at(
                pod_key("d"),
                None,
                Reason::Operator,
                Some("2026-06-08T12:34:56Z".to_string()),
            ),
        ];
        let replay = |cmds: &[ResourceCommand]| {
            let mut cat = ResourceCatalog::default();
            for (i, cmd) in cmds.iter().enumerate() {
                cat.apply(cmd, 1, (i + 1) as u64);
            }
            cat
        };
        let a = replay(&seq);
        let b = replay(&seq);
        // The two catalogs are equal (resources + revision), and the stored
        // Terminating object is byte-identical.
        assert_eq!(a, b, "two replays of the same sequence converge");
        let va = serde_json::to_vec(a.get(&pod_key("d")).unwrap()).unwrap();
        let vb = serde_json::to_vec(b.get(&pod_key("d")).unwrap()).unwrap();
        assert_eq!(va, vb, "Terminating object is byte-identical across replays");
    }
}
