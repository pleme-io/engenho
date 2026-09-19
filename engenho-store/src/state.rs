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
//! a no-op outcome here leaves the revision untouched. Nor does a Put or
//! Patch whose result equals the stored object ([`unchanged`], T3.5): it
//! answers [`ResourceOp::Unchanged`] under [`ApplySemantics::V1`], the rules
//! every entry this binary proposes carries.
//!
//! Per key the catalog stores `(value, VersionMeta)` where
//! [`VersionMeta`] carries `(create_revision, mod_revision,
//! version)` — etcd's per-key versioning triple. The wire
//! `metadata.resourceVersion` is stamped from the global revision
//! (NOT the Raft log index).
//!
//! A bounded in-memory history ring records every committed
//! [`Change`]; it is the watch-replay source. The ring, its compaction
//! floor, the current revision and the ring's capacity are one sealed
//! value, `WatchHistory` (T3.3): when the ring overflows its capacity the
//! oldest whole revision is evicted and the floor rises to it. Reads /
//! watches that ask for history below the floor get a typed
//! [`CompactedTooOld`]. The ring is never persisted, so a catalog loaded
//! from disk or from a snapshot starts with its floor AT its current
//! revision.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use engenho_types::patch::PatchType;

use crate::command::{
    ApplyMeta, ApplySemantics, LoggedCommand, ResourceCommand, ResourceOp, TxnCompare, TxnOp,
};
use crate::pagination::{ListPage, PageAtRevision};
use crate::patch_apply::{self, Gvk, OpenApiPatchEnv, PatchBody, PatchError, PatchSchemaEnv};
use crate::resource::{ListScope, ResourceKey, ResourceValue};
use crate::revision::{Change, ChangeKind, CompactedTooOld, Revision, VersionMeta};
use crate::ssa;
use crate::watch_history::{self, WatchHistory};

pub use crate::watch_history::DEFAULT_HISTORY_CAPACITY;

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

/// The `metadata` fields no writer sets: the store stamps both from the
/// revision a write commits at, so they differ on every write and say
/// nothing about whether the object changed.
const COMPUTED_METADATA: [&str; 2] = ["resourceVersion", "generation"];

/// The field of a managedFields entry the apiserver boundary stamps on every
/// server-side apply: when the apply happened, not what it owns.
const MANAGED_FIELDS_TIME: &str = "time";

/// ★ A REVISION MEANS A CHANGE (T3.5). `true` when storing `candidate` over
/// `prior` would leave the object as it is, so the write must consume no
/// revision and wake no watcher.
///
/// Pure and total. Equality is structural over everything except what is
/// stamped on every write whether or not anything changed:
///
///   * `metadata.resourceVersion` and `metadata.generation` — the store
///     computes both from the revision the write would commit at;
///   * the `time` of `manager`'s own `metadata.managedFields` entries — the
///     instant of a server-side apply (upstream ignores it the same way, in
///     `IgnoreManagedFieldsTimestampsTransformer`). `None`, for a Put or a
///     non-apply Patch, ignores no managedFields time.
///
/// Everything else counts, ownership included: a second manager applying
/// values already present adds a managedFields entry, and that is a change.
/// Entries compare by position, because the apply path keeps each entry in
/// place.
///
/// `candidate` is the object as it would be stored short of those stamps —
/// after the merge, and with the identity the store preserves across updates
/// (`uid`, `creationTimestamp`) already carried over.
#[must_use]
pub fn unchanged(
    prior: &serde_json::Value,
    candidate: &serde_json::Value,
    manager: Option<&str>,
) -> bool {
    objects_equal_except(prior, candidate, &[], |field, p, c| match field {
        "metadata" => objects_equal_except(p, c, &COMPUTED_METADATA, |field, p, c| match field {
            "managedFields" => managed_fields_unchanged(p, c, manager),
            _ => p == c,
        }),
        _ => p == c,
    })
}

/// Structural equality of two JSON objects that skips the `ignored` keys and
/// compares every other key with `field_eq`. Values that are not both objects
/// compare with plain `==`.
fn objects_equal_except(
    prior: &serde_json::Value,
    candidate: &serde_json::Value,
    ignored: &[&str],
    field_eq: impl Fn(&str, &serde_json::Value, &serde_json::Value) -> bool,
) -> bool {
    let (Some(before), Some(after)) = (prior.as_object(), candidate.as_object()) else {
        return prior == candidate;
    };
    let kept = |key: &String| !ignored.contains(&key.as_str());
    // Same number of kept keys, and every kept key of `before` present and
    // equal in `after`: the kept key sets are identical.
    before.keys().filter(|k| kept(k)).count() == after.keys().filter(|k| kept(k)).count()
        && before
            .iter()
            .filter(|(key, _)| kept(key))
            .all(|(key, b)| after.get(key).is_some_and(|a| field_eq(key, b, a)))
}

/// `metadata.managedFields` equality with `manager`'s entries compared
/// without their `time`.
fn managed_fields_unchanged(
    prior: &serde_json::Value,
    candidate: &serde_json::Value,
    manager: Option<&str>,
) -> bool {
    let (Some(manager), Some(before), Some(after)) =
        (manager, prior.as_array(), candidate.as_array())
    else {
        return prior == candidate;
    };
    let callers = |entry: &serde_json::Value| {
        entry.get("manager").and_then(serde_json::Value::as_str) == Some(manager)
    };
    before.len() == after.len()
        && before.iter().zip(after).all(|(b, a)| {
            if callers(b) && callers(a) {
                objects_equal_except(b, a, &[MANAGED_FIELDS_TIME], |_, b, a| b == a)
            } else {
                b == a
            }
        })
}

/// What a Put or Patch does when its result equals the stored object — the
/// rule an [`ApplySemantics`] version selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OnIdentical {
    /// Commit it anyway: a revision, a history entry, a MODIFIED event.
    /// [`ApplySemantics::V0`], and etcd's rule for a `Txn` Put.
    Commit,
    /// Write nothing and answer [`ResourceOp::Unchanged`]
    /// ([`ApplySemantics::V1`]).
    Skip,
}

impl OnIdentical {
    /// The rule `semantics` names. Exhaustive on purpose: a new version must
    /// decide here, never inherit one by a wildcard.
    const fn under(semantics: ApplySemantics) -> Self {
        match semantics {
            ApplySemantics::V0 => Self::Commit,
            ApplySemantics::V1 => Self::Skip,
        }
    }

    /// `true` when the write is to be skipped: the rule says so, it updates
    /// an object that exists, the result is [`unchanged`], and it does not
    /// release the object's finalizers. A release removes the object, and a
    /// removal is a change even when the content is the same.
    fn skips(
        self,
        prior: Option<&serde_json::Value>,
        candidate: &serde_json::Value,
        manager: Option<&str>,
    ) -> bool {
        match self {
            Self::Commit => false,
            Self::Skip => prior.is_some_and(|prior| {
                unchanged(prior, candidate, manager) && !finalizers_released(candidate)
            }),
        }
    }
}

/// How faithful a historical read's [`VersionMeta`] is.
///
/// The VALUE at a past revision is always exact — it is reconstructed by
/// undoing each retained change's `prior`. The METADATA is not always, and
/// the difference is worth a TYPE rather than a doc note: a client that
/// uses `mod_revision` for optimistic concurrency would corrupt state if
/// handed a plausible-looking guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaFidelity {
    /// The change at or before the requested revision is still retained, so
    /// every field is the exact value the store held at that revision.
    Exact,
    /// The key was not modified anywhere in the retained window at or before
    /// the requested revision, so its true `mod_revision` lies at or below
    /// `compacted_revision` and cannot be recovered. The reported value is
    /// that FLOOR, not a guess — the real one is `<=` it.
    ModRevisionFloor,
}

/// One key's state as of a past revision.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalEntry {
    pub value: ResourceValue,
    pub meta: VersionMeta,
    pub fidelity: MetaFidelity,
}

/// What [`ResourceCatalog::apply`] returns: the resource operation
/// the apiserver / client gets back, plus the typed [`Change`] the
/// mutation committed (or `None` for a no-op).
#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOutcome {
    /// Created / Replaced / Patched / Deleted / Conflict / PatchRejected /
    /// Unchanged / NoOp.
    pub op: ResourceOp,
    /// The committed change — `Some` for every real mutation, `None`
    /// for a no-op. Carries the post-image, the prior object (so the
    /// Deleted event always has the real prior, never Null), the
    /// stamped revision, and the per-key version metadata.
    ///
    /// Built once, here, at apply time: the history ring and every watcher
    /// share this one allocation (T3.7).
    pub change: Option<Arc<Change>>,
    /// The typed patch-interpreter error string when `op ==
    /// [`ResourceOp::PatchRejected`]`; `None` otherwise. The apiserver maps
    /// it to the correct typed `ApiError` (415 for server-side apply,
    /// 422/400 for a bad patch body). Carried as a `String` so
    /// [`ApplyOutcome`] keeps its derives (the `PatchError` is rendered to a
    /// stable message at the rejection site).
    pub patch_error: Option<String>,
    /// Additional committed changes beyond `change`, for a command that
    /// mutates MORE THAN ONE key — today only [`ResourceCommand::Txn`].
    ///
    /// Every entry shares `change`'s revision: etcd stamps one transaction
    /// with one `mod_revision`, and splitting them would be observable on
    /// the wire. Empty for every single-key command, so no existing caller
    /// changes behaviour.
    pub extra_changes: Vec<Arc<Change>>,
}

impl ApplyOutcome {
    /// An outcome with no committed change (a Conflict / NoOp / etc.) and no
    /// patch error.
    #[must_use]
    pub fn no_change(op: ResourceOp) -> Self {
        Self {
            op,
            extra_changes: Vec::new(),
            change: None,
            patch_error: None,
        }
    }

    /// An outcome carrying a committed [`Change`].
    #[must_use]
    pub fn with_change(op: ResourceOp, change: Change) -> Self {
        Self::committed(op, Arc::new(change), Vec::new())
    }

    /// An outcome carrying one committed revision: `first`, plus every
    /// further key the same command touched at that revision.
    fn committed(op: ResourceOp, first: Arc<Change>, rest: Vec<Arc<Change>>) -> Self {
        Self {
            op,
            change: Some(first),
            patch_error: None,
            extra_changes: rest,
        }
    }

    /// Every change this outcome committed, in the order the history
    /// records them: [`Self::change`], then [`Self::extra_changes`]. Empty
    /// for a no-op.
    ///
    /// The one list both the history ring and the live watch fan-out are
    /// fed from, so a live watcher and one replaying the ring see the same
    /// changes for one revision: a transaction's second key is not a replay
    /// -only event.
    pub fn changes(&self) -> impl Iterator<Item = &Arc<Change>> {
        self.change.iter().chain(&self.extra_changes)
    }

    /// A [`ResourceOp::PatchRejected`] outcome carrying the typed patch
    /// error's stable message — no mutation, no revision consumed.
    #[must_use]
    pub fn patch_rejected(error: String) -> Self {
        Self {
            op: ResourceOp::PatchRejected,
            change: None,
            patch_error: Some(error),
            extra_changes: Vec::new(),
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
            extra_changes: Vec::new(),
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
///
/// ── ★ SEALED (T3.2b): `pub(crate)`, and no `pub fn` may hand it out ─────
/// The catalog carries `history`, the 8192-change watch-replay ring whose
/// entries each hold a full post-image AND a full pre-image. While the type
/// was public, `current_catalog()` returned it by value, and every caller
/// that wanted one integer (`.revision()`, `.last_applied_index`) paid a deep
/// clone of every resource plus that ring, under the lock `apply` needs. On
/// rio that stalled writes for tens of seconds and wedged Flux for five days.
///
/// So the type is crate-private and the crate root denies
/// `private_interfaces`: a `pub fn` that returns or takes a catalog is a
/// compile error, not a review comment. Callers outside the crate read
/// through `StoreMesh`'s scalar and single-guard accessors, which see the
/// catalog only by reference, under one guard, and clone only what they
/// return.
#[derive(Clone)]
pub(crate) struct ResourceCatalog {
    /// Keyed store: value + per-key version metadata.
    pub resources: BTreeMap<ResourceKey, (ResourceValue, VersionMeta)>,
    pub last_applied_term: u64,
    pub last_applied_index: u64,
    /// The global MVCC revision counter, the watch-replay ring, its
    /// compaction floor and its capacity, as ONE sealed value (T3.3). The
    /// revision advances by exactly 1 per real mutation, and only by
    /// [`WatchHistory::commit`], which records that mutation's changes in
    /// the same step: a revision with no history behind it, or a floor the
    /// ring does not back, has no code path. Private, so no line in the
    /// crate can swap it out from under the resources whose revisions it
    /// counts.
    history: WatchHistory,
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
            .field("history", &self.history)
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
            history: WatchHistory::default(),
            patch_env: default_patch_env(),
        }
    }
}

impl PartialEq for ResourceCatalog {
    /// Equality covers the durable, convergence-relevant state:
    /// resources (value + version metadata), applied position, and
    /// the global revision. The history ring, its floor and its capacity
    /// are a local replay buffer, not part of the converged state, so
    /// they're excluded — two nodes that applied the same command
    /// sequence are equal even if one has compacted more of its ring.
    fn eq(&self, other: &Self) -> bool {
        self.resources == other.resources
            && self.last_applied_term == other.last_applied_term
            && self.last_applied_index == other.last_applied_index
            && self.revision() == other.revision()
    }
}

// serde_json doesn't support struct map-keys directly. Custom impl
// flattens the BTreeMap into Vec<(ResourceKey, (ResourceValue,
// VersionMeta))> at the wire — preserves order (BTreeMap iter is
// sorted) so the resulting bytes are deterministic across nodes.
//
// The history ring is intentionally NOT serialized: it is a local
// watch-replay buffer rebuilt by re-applying the log, not part of
// the converged durable state. `compacted_revision` is still WRITTEN,
// byte for byte as before, so a blob stays readable by a previous
// release and a replayed log still produces the recorded catalog
// bytes (T3.1 case 5). It is NOT trusted on the way back in: see the
// Deserialize impl below for why the floor on load is
// `current_revision`.
impl Serialize for ResourceCatalog {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = ser.serialize_struct("ResourceCatalog", 5)?;
        let entries: Vec<(&ResourceKey, &(ResourceValue, VersionMeta))> =
            self.resources.iter().collect();
        state.serialize_field("resources", &entries)?;
        state.serialize_field("last_applied_term", &self.last_applied_term)?;
        state.serialize_field("last_applied_index", &self.last_applied_index)?;
        state.serialize_field("current_revision", &self.history.head())?;
        state.serialize_field("compacted_revision", &self.history.floor())?;
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
            /// The floor the WRITER held. Still decoded — so the set of
            /// blobs this reader accepts is exactly what it was — and then
            /// deliberately dropped: it described a ring that did not
            /// survive the trip to disk.
            #[serde(default, rename = "compacted_revision")]
            _stale_floor: Revision,
        }
        let h = Helper::deserialize(de)?;
        // ★ THE FLOOR ON LOAD IS THE CURRENT REVISION (T3.3).
        //
        // The floor promises "every change above me is in the ring".
        // The ring is not serialized, so a catalog that comes back from
        // disk or from a snapshot holds NO history at all, and the only
        // floor that promise is true of is `current_revision`. Keeping
        // the writer's floor instead is how a restarted store answered
        // `changes_since(0)` with `Ok(<only the changes replayed after
        // the blob>)`: a client resumed into a strict subset of what
        // happened and believed itself caught up. With the floor here,
        // every resume point below the load revision is an honest
        // `CompactedTooOld` (a 410: relist), and every change applied
        // after the load lands in the ring above the floor.
        // `WatchHistory::loaded_at` is the only constructor that starts
        // above revision 0, and it takes no floor to believe.
        Ok(Self {
            resources: h.resources.into_iter().collect(),
            last_applied_term: h.last_applied_term,
            last_applied_index: h.last_applied_index,
            history: WatchHistory::loaded_at(h.current_revision, watch_history::DEFAULT_CAPACITY),
            patch_env: default_patch_env(),
        })
    }
}

impl ResourceCatalog {
    /// Construct a catalog with an explicit history-ring capacity.
    /// Used by callers that want a tighter compaction window (and by
    /// tests that force compaction with a tiny capacity).
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "a tighter compaction window is configured only by tests today (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn with_history_capacity(history_capacity: usize) -> Self {
        Self {
            history: WatchHistory::new(
                NonZeroUsize::new(history_capacity).unwrap_or(NonZeroUsize::MIN),
            ),
            ..Self::default()
        }
    }

    /// Apply a command this binary proposes, under
    /// [`ApplySemantics::CURRENT`]. A Raft log entry replays through
    /// [`Self::apply_logged`] instead, under the rules it was written with.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production applies Raft log entries through `apply_logged`; `apply` is the test entry (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn apply(&mut self, cmd: &ResourceCommand, term: u64, index: u64) -> ApplyOutcome {
        self.apply_under(cmd, ApplySemantics::CURRENT, term, index)
    }

    /// Apply one committed Raft log entry, under the [`ApplySemantics`] it
    /// carries — an entry written before the marker replays under V0, so
    /// replaying an old log reproduces its revisions exactly.
    pub fn apply_logged(&mut self, entry: &LoggedCommand, term: u64, index: u64) -> ApplyOutcome {
        self.apply_under(entry.command(), entry.semantics(), term, index)
    }

    /// Apply a single committed command. Pure function. Returns the
    /// [`ApplyOutcome`] — operation + committed [`Change`].
    ///
    /// Revision semantics:
    ///
    ///   * A NoOp outcome (delete-not-found, patch-missing) does NOT
    ///     advance `current_revision` and emits no `Change`.
    ///   * Under [`ApplySemantics::V1`], neither does a Put or Patch whose
    ///     result is [`unchanged`]: it answers [`ResourceOp::Unchanged`].
    ///   * Any real mutation advances `current_revision` by exactly
    ///     1, stamps `metadata.resourceVersion = <revision>` (NOT the
    ///     Raft index), preserves/sets `metadata.uid`, updates the
    ///     per-key [`VersionMeta`], and commits its [`Change`]s to the
    ///     history as one revision ([`WatchHistory::commit`]: the oldest
    ///     whole revisions leave and the floor rises when over capacity).
    fn apply_under(
        &mut self,
        cmd: &ResourceCommand,
        semantics: ApplySemantics,
        term: u64,
        index: u64,
    ) -> ApplyOutcome {
        // Tentatively reserve the next revision; only committed if the
        // mutation is real (not a no-op).
        let rev = self.history.next_revision();
        let on_identical = OnIdentical::under(semantics);
        let outcome = match cmd {
            ResourceCommand::Put {
                key,
                value,
                expected,
                ..
            } => self.apply_put(key, value, *expected, on_identical, rev),
            ResourceCommand::Patch {
                key,
                patch,
                patch_type,
                apply,
                expected,
                ..
            } => self.apply_patch(
                key,
                patch,
                *patch_type,
                apply.as_ref(),
                *expected,
                on_identical,
                rev,
            ),
            ResourceCommand::Delete {
                key,
                expected,
                deletion_timestamp,
                ..
            } => self.apply_delete(key, *expected, deletion_timestamp.as_deref(), rev),
            ResourceCommand::Txn {
                compares,
                success,
                failure,
                ..
            } => self.apply_txn(compares, success, failure, rev),
        };
        self.last_applied_term = term;
        self.last_applied_index = index;
        if let Some(change) = &outcome.change {
            // Commit the revision + record history only for real
            // mutations, in one step. The stamped revision is exactly
            // `rev`. A transaction commits EVERY key it touched at the
            // SAME revision (see `ResourceCommand::Txn`), so its extra
            // changes go in with the first, whole; they are empty for
            // every single-key command.
            self.history.commit(change, &outcome.extra_changes);
        }
        outcome
    }

    /// Apply an atomic multi-key transaction — etcd `Txn` semantics.
    ///
    /// ★ ALL-OR-NOTHING IS ENFORCED BY EVALUATING FIRST. Every compare is
    /// evaluated against the live catalog BEFORE any mutation, so a branch
    /// is chosen from a single consistent view. Within the chosen branch
    /// each op is unconditional — the compares were the condition — which
    /// is exactly etcd's model and is why an op here carries no `expected`.
    ///
    /// ★ ONE TRANSACTION, ONE REVISION. Every op commits at `rev`, so all
    /// touched keys share a `mod_revision`. The first real change becomes
    /// `ApplyOutcome.change` and the rest `extra_changes`; the caller
    /// pushes them all to history at the same revision.
    ///
    /// ★ AN EMPTY BRANCH IS A NoOp, not a silent success-with-no-revision:
    /// `change: None` means the outer `apply` leaves `current_revision`
    /// untouched and fans nothing, which is the same "no mutation ⇒ no
    /// revision" law every other path obeys.
    fn apply_txn(
        &mut self,
        compares: &[TxnCompare],
        success: &[TxnOp],
        failure: &[TxnOp],
        rev: Revision,
    ) -> ApplyOutcome {
        // An empty compare list is vacuously true — etcd's unconditional Txn.
        let taken = compares.iter().all(|c| self.eval_compare(c));
        let branch = if taken { success } else { failure };

        let mut changes: Vec<Arc<Change>> = Vec::new();
        for op in branch {
            let outcome = match op {
                // etcd: a Put is a revision even when the value is the same,
                // so a transaction keeps committing identical writes.
                TxnOp::Put { key, value } => {
                    self.apply_put(key, value, None, OnIdentical::Commit, rev)
                }
                TxnOp::Delete {
                    key,
                    deletion_timestamp,
                } => self.apply_delete(key, None, deletion_timestamp.as_deref(), rev),
            };
            if let Some(c) = outcome.change {
                changes.push(c);
            }
        }

        // The op reported is the branch's shape, not any one key's: a
        // transaction is one outcome. `Replaced` reads correctly for a
        // multi-key mutation and keeps the enum closed.
        let mut iter = changes.into_iter();
        match iter.next() {
            None => ApplyOutcome::no_change(ResourceOp::NoOp),
            Some(first) => ApplyOutcome::committed(ResourceOp::Replaced, first, iter.collect()),
        }
    }

    /// Evaluate ONE transaction predicate against the live catalog.
    fn eval_compare(&self, cmp: &TxnCompare) -> bool {
        match cmp {
            TxnCompare::ModRevisionEq { key, revision } => self
                .resources
                .get(key)
                .is_some_and(|(_, m)| m.mod_revision == *revision),
            TxnCompare::NotExists { key } => !self.resources.contains_key(key),
        }
    }

    fn apply_put(
        &mut self,
        key: &ResourceKey,
        value: &ResourceValue,
        expected: Option<Revision>,
        on_identical: OnIdentical,
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

        let (prior_value, prior_meta) = prior_entry.unzip();

        let version_meta = match prior_meta {
            Some(meta) => meta.bumped_at(rev),
            None => VersionMeta::created_at(rev),
        };

        // The object this write would store, short of the revision stamp:
        // the body plus the identity the store owns. uid and
        // creationTimestamp are create-time, IMMUTABLE fields, so a
        // controller that re-Puts a rebuilt body without them neither loses
        // them nor reads as a change for having dropped them.
        let mut new_value = value.clone();
        stamp_identity(
            &mut new_value,
            key,
            version_meta.create_revision,
            prior_value.as_ref(),
        );

        // ★ A REVISION MEANS A CHANGE (T3.5), decided before anything is
        // stamped or committed: an identical write leaves the catalog
        // byte-identical, like a Conflict.
        if on_identical.skips(prior_value.as_ref(), &new_value, None) {
            return ApplyOutcome::no_change(ResourceOp::Unchanged);
        }

        // generation reflects SPEC-INTENT revisions (the K8s contract
        // `observedGeneration` reconciles against), NOT every mutation.
        // On first create generation == 1; on replace it bumps iff
        // `spec` changed (deep compare prior vs new spec), else the prior
        // generation is preserved. A status-only replace (rare via Put,
        // common via Patch) thus leaves generation untouched. Computed in
        // the deterministic apply path so every Raft node stamps the
        // identical value.
        let next_generation = compute_generation_on_put(prior_value.as_ref(), value);
        stamp_revision(&mut new_value, rev, next_generation);

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

    #[allow(clippy::too_many_arguments)] // one per replicated command field, plus the rule and the revision
    fn apply_patch(
        &mut self,
        key: &ResourceKey,
        patch: &ResourceValue,
        patch_type: PatchType,
        apply: Option<&ApplyMeta>,
        expected: Option<Revision>,
        on_identical: OnIdentical,
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
            return self.apply_ssa_command(key, patch, meta, expected, on_identical, rev);
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
        let prior_meta = *existing_meta;

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
                return ApplyOutcome::patch_rejected(PatchError::Unsupported { what }.to_string());
            }
            Err(e) => return ApplyOutcome::patch_rejected(e.to_string()),
        };
        // ★ A REVISION MEANS A CHANGE (T3.5): a patch whose merge leaves the
        // object as it is commits nothing. Decided on the merged object,
        // before the version is bumped or any field stamped.
        if on_identical.skips(Some(&prior_value), &merged, None) {
            return ApplyOutcome::no_change(ResourceOp::Unchanged);
        }
        let version_meta = prior_meta.bumped_at(rev);

        // generation bumps iff the MERGED `spec` differs from the prior
        // `spec`. A patch touching ONLY `status` (or only metadata) leaves
        // `spec` unchanged → generation preserved. This is the
        // load-bearing invariant the controllers' observedGeneration
        // convergence relies on: a status-only write never advances the
        // generation it is meant to be catching up to.
        let next_generation = compute_generation_on_put(Some(&prior_value), &merged);
        stamp_revision(&mut merged, rev, next_generation);

        // Finalizer release (same rule as apply_put): a patch that empties
        // `metadata.finalizers` on a deletionTimestamp-bearing object
        // converts into a removal. THIS is what makes
        // `kubectl patch …finalizers:null` actually delete a Terminating
        // object.
        if let Some(outcome) = self.finalizer_release_removal(key, &merged, Some(&prior_value), rev)
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
        on_identical: OnIdentical,
        rev: Revision,
    ) -> ApplyOutcome {
        let prior_entry = self.resources.get(key).cloned();

        // Optimistic-concurrency precondition (CAS) — checked BEFORE the
        // merge, identical to apply_put/apply_patch. On conflict the catalog
        // is untouched (no revision advance, no history, no fan-out).
        if !check_precondition(expected, prior_entry.as_ref().map(|(_, m)| *m)) {
            return ApplyOutcome::no_change(ResourceOp::Conflict);
        }

        let (prior_value, prior_meta) = prior_entry.unzip();
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

        // Stamp uid + creationTimestamp, then resourceVersion + generation —
        // the SAME metadata the create/patch paths stamp, computed
        // deterministically in the apply path so every Raft replica is
        // byte-identical. (managedFields was already written by apply_ssa
        // into the merged object.)
        let version_meta = match prior_meta {
            Some(m) => m.bumped_at(rev),
            None => VersionMeta::created_at(rev),
        };
        stamp_identity(
            &mut merged,
            key,
            version_meta.create_revision,
            prior_value.as_ref(),
        );

        // ★ A REVISION MEANS A CHANGE (T3.5): re-applying what is already
        // there commits nothing. The manager's managedFields `time` is the
        // one field every apply restamps, so it is not a change by itself;
        // the stored entry keeps the time of the apply that changed it.
        if on_identical.skips(prior_value.as_ref(), &merged, Some(&meta.manager)) {
            return ApplyOutcome::no_change(ResourceOp::Unchanged);
        }

        let next_generation = compute_generation_on_put(prior_value.as_ref(), &merged);
        stamp_revision(&mut merged, rev, next_generation);

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
        if prior.is_none() || !finalizers_released(post) {
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
        // A None scalar here means we have no timestamp to stamp — leave
        // the object untouched (NoOp) rather than invent a non-replicated
        // value. Since T3.6 `ResourceCommand::delete()` always carries a
        // clock, so None arrives only from `delete_at(.., None)`, a struct
        // literal, or a log entry written before T3.6 — and that entry
        // must replay exactly as it did when it was written.
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
        stamp_resource_version(&mut terminating, rev);

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

    /// Discard history at or below `target`, advancing the compaction
    /// watermark — etcd's `Compact`.
    ///
    /// ★ WHY AN EXPLICIT OPERATION AND NOT JUST THE RING. Compaction here
    /// has always been implicit: the bounded history ring evicts its oldest
    /// revision once it overflows its capacity. That is a CAPACITY
    /// policy, and etcd's `Compact` is an OPERATOR one — "I have taken a
    /// backup, reclaim everything older". A façade cannot synthesize it
    /// from the ring, because the caller names a revision the ring knows
    /// nothing about.
    ///
    /// ★ COMPACTION RECLAIMS HISTORY, NEVER DATA. The live map keeps
    /// exactly one version per key, so nothing a client can still read is
    /// ever removed — only the ability to WATCH FROM or (once historical
    /// reads land) read AT a revision below the watermark. This is why the
    /// return is the new watermark rather than a byte count: there is no
    /// space to report.
    ///
    /// ★ THE WATERMARK ONLY EVER MOVES FORWARD. A `Compact` to a revision
    /// below the current watermark is a no-op returning the existing
    /// watermark, not an error and not a rewind — etcd's own behaviour, and
    /// rewinding would promise history that has already been dropped.
    /// A target above `current_revision` is clamped to it: you cannot
    /// compact away revisions that do not exist yet.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "no Compact command reaches the catalog yet: the etcd façade is read-only (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn compact(&mut self, target: Revision) -> Revision {
        self.history.compact(target)
    }

    /// Reconstruct the whole keyspace as of `rev` — etcd's
    /// `Range{revision: N}`.
    ///
    /// ★ NO SECOND INDEX. The obvious implementation is a per-key version
    /// history, and it was the deferred design. It is unnecessary: every
    /// retained [`Change`] already carries `prior`, the pre-image, so the
    /// past is reachable by REWINDING the live map — undo each change newer
    /// than `rev`, newest first. That reuses storage already paid for, and
    /// it makes the reachable window exactly the retained window, which is
    /// the honest boundary rather than an arbitrary one.
    ///
    /// ★ COST IS PROPORTIONAL TO RECENCY, which matches the consumer.
    /// kube-apiserver reads at a past revision to initialise a watch cache
    /// and to keep a paginated list self-consistent — both a few revisions
    /// back, not thousands. A read far in the past is `O(changes since)`,
    /// and one past the watermark is refused outright.
    ///
    /// ★ VALUES ARE EXACT; METADATA CARRIES ITS OWN FIDELITY. See
    /// [`MetaFidelity`] — a guessed `mod_revision` handed to a client doing
    /// optimistic concurrency is worse than a declared floor.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "historical reads have no producer yet: the etcd façade serves no Range at a past revision (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn state_at(
        &self,
        rev: Revision,
    ) -> Result<BTreeMap<ResourceKey, HistoricalEntry>, CompactedTooOld> {
        // The one refusal rule the watch replay also uses: below the floor,
        // the changes that would rewind the map are gone.
        let (through, after) = self.history.split(rev)?;
        let floor = self.history.floor();

        // Start from the present and walk backwards.
        let mut state: BTreeMap<ResourceKey, HistoricalEntry> = self
            .resources
            .iter()
            .map(|(k, (v, m))| {
                (
                    k.clone(),
                    HistoricalEntry {
                        value: v.clone(),
                        meta: *m,
                        fidelity: MetaFidelity::Exact,
                    },
                )
            })
            .collect();

        // Undo every change strictly newer than `rev`, newest first. After
        // undoing change C, the entry holds C's PRE-image; its metadata is
        // whatever an older retained change to the same key says, which the
        // second pass below supplies.
        for c in after.rev() {
            match &c.prior {
                Some(prior) => {
                    let e = state.entry(c.key.clone()).or_insert(HistoricalEntry {
                        value: prior.clone(),
                        meta: c.version_meta,
                        fidelity: MetaFidelity::ModRevisionFloor,
                    });
                    e.value = prior.clone();
                    // create_revision is stable across modifications, so it
                    // survives the undo; mod_revision/version do not and are
                    // corrected below.
                    e.meta = VersionMeta {
                        create_revision: c.version_meta.create_revision,
                        mod_revision: floor,
                        version: 1,
                    };
                    e.fidelity = MetaFidelity::ModRevisionFloor;
                }
                // No pre-image ⇒ the key was CREATED by this change, so it
                // did not exist at `rev`.
                None => {
                    state.remove(&c.key);
                }
            }
        }

        // Second pass: for every key still present, the newest retained
        // change at or before `rev` gives its exact metadata.
        for c in through {
            if let Some(e) = state.get_mut(&c.key) {
                e.meta = c.version_meta;
                e.fidelity = MetaFidelity::Exact;
            }
        }

        Ok(state)
    }

    /// One key's state as of `rev`. See [`Self::state_at`].
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "historical reads have no producer yet: the etcd façade serves no Range at a past revision (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn get_at(
        &self,
        key: &ResourceKey,
        rev: Revision,
    ) -> Result<Option<HistoricalEntry>, CompactedTooOld> {
        Ok(self.state_at(rev)?.remove(key))
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
        self.history.head()
    }

    /// The compaction watermark: every change above it is retained in
    /// the history ring, and none at or below it. A catalog loaded from
    /// disk or a snapshot starts with this equal to [`Self::revision`].
    #[must_use]
    pub fn compacted_revision(&self) -> Revision {
        self.history.floor()
    }

    /// The sealed history, for tests that inspect the ring itself.
    #[cfg(test)]
    pub(crate) fn history(&self) -> &WatchHistory {
        &self.history
    }

    /// All committed changes with `revision > rv`, in revision order: the
    /// watch-replay window. A client that last saw revision `rv` resumes by
    /// replaying exactly these changes.
    ///
    /// Each item is the ring's own `Arc`, so a reader that keeps a change
    /// (the watch replay) shares it rather than copying it, and one that
    /// keeps only some (the etcd façade's prefix filter) copies nothing it
    /// drops. The one definition of the window and of its refusal, so no
    /// reader can drift from the watch replay's rule.
    ///
    /// # Errors
    ///
    /// Returns [`CompactedTooOld`] when `rv < compacted_revision` —
    /// the requested resume point has been compacted away (the 410
    /// Gone equivalent). Asking from exactly `compacted_revision` (or
    /// above) is always honored.
    pub(crate) fn changes_after(
        &self,
        rv: Revision,
    ) -> Result<impl Iterator<Item = &Arc<Change>>, CompactedTooOld> {
        self.history.since(rv)
    }

    /// [`Self::changes_after`], copied out: for tests that compare whole
    /// windows by value.
    #[cfg(test)]
    pub fn changes_since(&self, rv: Revision) -> Result<Vec<Change>, CompactedTooOld> {
        Ok(self.changes_after(rv)?.map(|c| Change::clone(c)).collect())
    }

    /// List resources matching (group, version, kind), optionally
    /// scoped to a namespace, in key order.
    ///
    /// Reads only the scope's own run of the map — see [`ListScope`].
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the borrowed LIST form; the stores hand out `list_at_revision`, which clones under one guard (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn list(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<(&ResourceKey, &ResourceValue)> {
        ListScope::new(group, version, kind, namespace)
            .range(&self.resources, None)
            .map(|(k, (v, _))| (k, v))
            .collect()
    }

    /// Every item of `scope`, cloned, with this catalog's revision: what a
    /// store hands out from under its lock. Only the listed items are
    /// cloned, never the catalog or its watch-replay ring.
    #[must_use]
    pub fn list_at_revision(
        &self,
        scope: ListScope<'_>,
    ) -> (Vec<(ResourceKey, ResourceValue)>, Revision) {
        let items = scope
            .range(&self.resources, None)
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect();
        (items, self.revision())
    }

    /// One page of resources matching (group, version, kind), optionally
    /// namespace-scoped, in total [`ResourceKey`] order — the
    /// range-pagination primitive (etcd consistent-list semantics).
    ///
    /// Ranges the underlying [`BTreeMap`] over the scope's own run of keys
    /// (see [`ListScope`]), starting STRICTLY AFTER `after` (the continue
    /// cursor's last key), taking up to `limit` items. Nothing outside the
    /// scope is visited, so a page of one kind costs nothing for the size
    /// of any other kind.
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the borrowed page form; the stores hand out `list_page_at_revision`, which clones under one guard (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn list_page(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> ListPage<'_> {
        self.page(
            ListScope::new(group, version, kind, namespace),
            after,
            limit,
        )
    }

    /// [`Self::list_page`], cloned out with this catalog's revision: what a
    /// store hands out from under its lock. Only the page's own items are
    /// cloned, never the catalog or its watch-replay ring.
    #[must_use]
    pub fn list_page_at_revision(
        &self,
        scope: ListScope<'_>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> PageAtRevision {
        let page = self.page(scope, after, limit);
        PageAtRevision {
            items: page
                .items
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            revision: self.revision(),
            next: page.next,
            remaining: page.remaining,
        }
    }

    fn page(
        &self,
        scope: ListScope<'_>,
        after: Option<&ResourceKey>,
        limit: usize,
    ) -> ListPage<'_> {
        let mut run = scope
            .range(&self.resources, after)
            .map(|(k, (v, _))| (k, v));

        // limit == 0 → unbounded: take all matching items after `after`.
        if limit == 0 {
            return ListPage {
                items: run.collect(),
                next: None,
                remaining: 0,
            };
        }

        // Collected, not `Vec::with_capacity(limit)`: `limit` is the
        // client's number, and reserving it up front would let one request
        // ask for any amount of memory.
        let items: Vec<(&ResourceKey, &ResourceValue)> = run.by_ref().take(limit).collect();

        // Count the rest of the scope to set `next` + `remaining`. `next` is
        // the last EMITTED key iff at least one more item follows the page.
        let remaining = u64::try_from(run.count()).unwrap_or(u64::MAX);
        let next = if remaining > 0 {
            items.last().map(|(k, _)| (*k).clone())
        } else {
            None
        };

        ListPage {
            items,
            next,
            remaining,
        }
    }

    /// Total resource count (across all kinds + namespaces).
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the resource count is read by tests only (sealed by T3.2b, so dead code is now visible)"
        )
    )]
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the resource count is read by tests only (sealed by T3.2b, so dead code is now visible)"
        )
    )]
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

/// `true` when `post` is Terminating (`metadata.deletionTimestamp`) with no
/// finalizers left — the state in which the write that produced it removes
/// the object instead of storing it (see
/// [`ResourceCatalog::finalizer_release_removal`]).
fn finalizers_released(post: &serde_json::Value) -> bool {
    deletion_timestamp_of(post).is_some() && !has_finalizers(post)
}

/// The `metadata` object of `value`, created empty when absent. `None` when
/// `value` is not an object or its `metadata` is not one (a shape the stamps
/// below leave alone rather than overwrite).
fn metadata_mut(
    value: &mut serde_json::Value,
) -> Option<&mut serde_json::Map<String, serde_json::Value>> {
    value
        .as_object_mut()?
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
}

/// Stamp the identity the store owns on `value`, shared by the Put and
/// server-side-apply paths: `uid` preserved from the prior object, else kept
/// from the body, else minted deterministically from key + `create_revision`;
/// `creationTimestamp` preserved from the prior object when it has one (a
/// K8s create-time, IMMUTABLE field, like uid — a re-Put that dropped it must
/// not lose it; a first create keeps whatever the boundary-stamped body
/// carries). Pure JSON mutation — no clock, no RNG — so every Raft replica is
/// byte-identical.
///
/// Stamped BEFORE the T3.5 [`unchanged`] gate: identity is part of the
/// object a write would store, not of the revision it would commit at.
fn stamp_identity(
    value: &mut serde_json::Value,
    key: &ResourceKey,
    create_revision: Revision,
    prior_value: Option<&serde_json::Value>,
) {
    let Some(meta_obj) = metadata_mut(value) else {
        return;
    };
    let prior_meta = prior_value.and_then(|v| v.get("metadata"));
    if let Some(prior_uid) = prior_meta.and_then(|m| m.get("uid")) {
        meta_obj.insert("uid".to_string(), prior_uid.clone());
    } else if !meta_obj.contains_key("uid") {
        let uid = mint_uid(&key.label(), create_revision);
        meta_obj.insert("uid".to_string(), serde_json::Value::String(uid));
    }
    if let Some(prior_creation) = prior_meta.and_then(|m| m.get("creationTimestamp")) {
        meta_obj.insert("creationTimestamp".to_string(), prior_creation.clone());
    }
}

/// Stamp `metadata.resourceVersion` (the global `rev`) and
/// `metadata.generation` (from [`compute_generation_on_put`]) on `value` —
/// the per-revision pass every Put, Patch and server-side apply shares, run
/// only once the write is known to commit.
fn stamp_revision(value: &mut serde_json::Value, rev: Revision, generation: i64) {
    stamp_resource_version(value, rev);
    let Some(meta_obj) = metadata_mut(value) else {
        return;
    };
    meta_obj.insert(
        "generation".to_string(),
        serde_json::Value::Number(generation.into()),
    );
}

/// Stamp `metadata.resourceVersion = rev` on `value`, creating `metadata`
/// when absent; a `value` that is not an object is left alone. The one
/// writer of the field: the commit path, the Terminating stamp, and a watch
/// event that carries an object at a revision other than its own (a DELETED
/// built from the prior image, T3.7) all go through it.
pub(crate) fn stamp_resource_version(value: &mut serde_json::Value, rev: Revision) {
    if let Some(meta_obj) = metadata_mut(value) {
        meta_obj.insert(
            "resourceVersion".to_string(),
            serde_json::Value::String(rev.to_string()),
        );
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

    fn put(
        cat: &mut ResourceCatalog,
        key: &ResourceKey,
        value: serde_json::Value,
        index: u64,
    ) -> ApplyOutcome {
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
        let op = put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"image": "v1"}}),
            1,
        );
        assert_eq!(op.op, ResourceOp::Created);
        assert_eq!(cat.len(), 1);

        let op = put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"image": "v2"}}),
            2,
        );
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

        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replaced": true}}),
            6,
        );
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
        // The uid is an RFC-4122 UUID, NOT the old `uid-<label>-<rev>` string
        // this assertion used to pin. Upstream guarantees the UUID shape, and
        // anything that PARSES a uid (ownership graphs, CSI volume handles,
        // audit correlation) breaks on the old form.
        assert_eq!(uid_1.len(), 36, "metadata.uid is an RFC-4122 UUID: {uid_1}");
        assert!(
            uid_1.chars().all(|c| c.is_ascii_hexdigit() || c == '-'),
            "uid is lowercase hex + dashes: {uid_1}"
        );
        assert!(
            !uid_1.starts_with("uid-"),
            "the non-UUID prefix must not return"
        );
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
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"image": "v1", "replicas": 2}}),
            1,
        );
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
        let prior = change.prior.as_ref().unwrap();
        assert_eq!(prior.get("spec").unwrap().get("replicas").unwrap(), 3);
    }

    #[test]
    fn changes_since_returns_tail_in_order() {
        // I5: 5 puts (rev 1..5); changes_since(2) yields revs 3,4,5.
        let mut cat = ResourceCatalog::default();
        for i in 1..=5 {
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
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
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
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
            put(
                &mut cat,
                &ResourceKey::namespaced("", "v1", "Pod", "default", name),
                serde_json::json!({}),
                idx,
            );
        }
        idx += 1;
        put(
            &mut cat,
            &ResourceKey::namespaced("", "v1", "Pod", "kube-system", "coredns"),
            serde_json::json!({}),
            idx,
        );
        idx += 1;
        put(
            &mut cat,
            &ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm-1"),
            serde_json::json!({}),
            idx,
        );

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
        // current_revision + per-key VersionMeta, and puts the
        // compaction floor at current_revision on the way back in (the
        // ring does not survive serde, so no lower floor is true — T3.3).
        // This is exactly the durable state the fjall `catalog`
        // partition persists — proving the contract here means the
        // backend gets revision survival for free.
        let mut cat = ResourceCatalog::with_history_capacity(2);
        let k = pod_key("rev-state");
        // 5 puts → current_revision 5; capacity 2 forces compaction so
        // compacted_revision advances to 3 (last two changes retained).
        for i in 1..=5u64 {
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
        }
        // One more put on a tracked key so we can assert its meta.
        put(&mut cat, &k, serde_json::json!({"spec": {}}), 6); // rev 6 (create)
        put(&mut cat, &k, serde_json::json!({"spec": {"x": 1}}), 7); // rev 7 (bump)
        assert_eq!(cat.revision(), Revision(7));
        assert_eq!(cat.compacted_revision(), Revision(5));
        let (_, meta_before) = cat.get_with_meta(&k).unwrap();
        assert_eq!(meta_before.create_revision, Revision(6));
        assert_eq!(meta_before.mod_revision, Revision(7));
        assert_eq!(meta_before.version, 2);

        // Round-trip through serde — the disk form.
        let bytes = serde_json::to_vec(&cat).unwrap();
        let back: ResourceCatalog = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(
            back.revision(),
            Revision(7),
            "current_revision survives serde"
        );
        assert_eq!(
            back.compacted_revision(),
            Revision(7),
            "the floor on load is the current revision, not the writer's floor of 5: \
             the ring that backed revisions 6 and 7 did not survive serde"
        );
        let (_, meta_after) = back.get_with_meta(&k).unwrap();
        assert_eq!(
            meta_after, meta_before,
            "per-key VersionMeta is byte-identical across serde"
        );
        // History is deliberately not persisted (rebuilt by replay).
        assert!(back.history.is_empty());
        assert_eq!(back.history.capacity().get(), DEFAULT_HISTORY_CAPACITY);
    }

    /// Round-trip `cat` through its disk form.
    fn rehydrate(cat: &ResourceCatalog) -> ResourceCatalog {
        let bytes = serde_json::to_vec(cat).expect("serialize the catalog");
        serde_json::from_slice(&bytes).expect("deserialize the catalog")
    }

    /// T3.3: the ring does not survive the trip to disk, so a rehydrated
    /// catalog must refuse every resume point below its load revision.
    /// Answering from the empty ring instead would tell a client that
    /// nothing happened between its resume point and now.
    #[test]
    fn a_rehydrated_catalog_refuses_every_resume_point_below_its_load_revision() {
        // Default capacity: nothing is evicted, so the WRITER's floor is 0.
        let mut cat = ResourceCatalog::default();
        for i in 1..=5u64 {
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
        }
        assert_eq!(cat.compacted_revision(), Revision::ZERO);
        assert_eq!(cat.changes_since(Revision::ZERO).map(|c| c.len()), Ok(5));

        let back = rehydrate(&cat);
        assert_eq!(back.revision(), Revision(5));
        assert_eq!(back.compacted_revision(), Revision(5));
        for from in 0..5u64 {
            let gone = CompactedTooOld {
                requested: Revision(from),
                compacted: Revision(5),
            };
            assert_eq!(
                back.changes_since(Revision(from)),
                Err(gone),
                "changes_since({from}) after a load must be a 410, not a replay from an empty ring"
            );
            assert_eq!(
                back.state_at(Revision(from)).err(),
                Some(gone),
                "state_at({from}) after a load must be refused, not answered with today's state"
            );
        }
        // Resuming from exactly the load revision is honoured, with
        // nothing to replay, and a read AT it is the loaded state.
        assert_eq!(back.changes_since(Revision(5)), Ok(Vec::new()));
        let at_load = back
            .state_at(Revision(5))
            .expect("a read at the load revision");
        assert_eq!(at_load.len(), 5);
        assert!(at_load.values().all(|e| e.fidelity == MetaFidelity::Exact));
    }

    /// T3.3: the floor on load does not freeze history — every change
    /// applied after the load is replayable from the load revision on.
    #[test]
    fn changes_after_a_load_replay_from_the_load_revision() {
        let mut cat = ResourceCatalog::default();
        for i in 1..=3u64 {
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
        }
        let mut back = rehydrate(&cat);
        put(&mut back, &pod_key("p4"), serde_json::json!({"i": 4}), 4); // rev 4
        put(&mut back, &pod_key("p1"), serde_json::json!({"i": 10}), 5); // rev 5

        let revs: Vec<u64> = back
            .changes_since(Revision(3))
            .expect("resume from the load revision")
            .iter()
            .map(|c| c.revision.get())
            .collect();
        assert_eq!(revs, vec![4, 5], "every change since the load, in order");
        assert!(
            back.changes_since(Revision(2)).is_err(),
            "below the load revision stays refused after new writes"
        );
        let at_4 = back.state_at(Revision(4)).expect("a read inside the ring");
        assert_eq!(
            at_4.get(&pod_key("p1")).and_then(|e| e.value.get("i")),
            Some(&serde_json::json!(1)),
            "a read at revision 4 undoes the rev-5 write"
        );
        assert!(at_4.contains_key(&pod_key("p4")));
    }

    /// T3.3 against real bytes: a catalog blob written by release
    /// v0.53.118 carries `current_revision: 57, compacted_revision: 0`.
    /// It still decodes, and its stale floor is not believed.
    #[test]
    fn a_released_blob_with_a_stale_floor_loads_with_its_floor_at_its_revision() {
        let blob = include_bytes!("../tests/fixtures/replay/v0.53.118/catalog.json");
        let raw: serde_json::Value = serde_json::from_slice(blob).expect("the fixture is JSON");
        assert_eq!(
            (
                raw["current_revision"].as_u64(),
                raw["compacted_revision"].as_u64()
            ),
            (Some(57), Some(0)),
            "FIXTURE PRECONDITION: the released blob carries a floor below its revision"
        );

        let cat: ResourceCatalog = serde_json::from_slice(blob).expect("a released blob loads");
        assert_eq!(cat.revision(), Revision(57));
        assert_eq!(cat.compacted_revision(), Revision(57));
        assert!(cat.changes_since(Revision::ZERO).is_err());
        assert_eq!(cat.changes_since(Revision(57)), Ok(Vec::new()));
    }

    /// T3.3: whatever floor the blob names — none at all (a pre-floor
    /// blob), one below the revision, or one above it — the loaded floor
    /// is the loaded revision.
    #[test]
    fn the_floor_on_load_ignores_whatever_floor_the_blob_names() {
        let mut cat = ResourceCatalog::default();
        for i in 1..=4u64 {
            put(
                &mut cat,
                &pod_key(&format!("p{i}")),
                serde_json::json!({"i": i}),
                i,
            );
        }
        let disk = serde_json::to_value(&cat).expect("serialize the catalog");
        let named: [Option<u64>; 4] = [None, Some(0), Some(2), Some(99)];
        for floor in named {
            let mut blob = disk.clone();
            let obj = blob.as_object_mut().expect("the catalog is a JSON object");
            match floor {
                None => {
                    obj.remove("compacted_revision");
                }
                Some(f) => {
                    obj.insert("compacted_revision".into(), serde_json::json!(f));
                }
            }
            let back: ResourceCatalog = serde_json::from_value(blob).expect("the blob loads");
            assert_eq!(
                back.compacted_revision(),
                Revision(4),
                "a blob naming floor {floor:?} must load with its floor at its revision"
            );
        }
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
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 1}}),
            1,
        );
        assert_eq!(live_generation(&cat, &k), 1, "first create → generation 1");
    }

    #[test]
    fn put_replace_with_changed_spec_bumps_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-bump");
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 1}}),
            1,
        );
        assert_eq!(live_generation(&cat, &k), 1);
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 2}}),
            2,
        );
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
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 3}}),
            1,
        );
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
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 2}}),
            1,
        );
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
            cat.get(&k)
                .unwrap()
                .get("status")
                .unwrap()
                .get("readyReplicas")
                .unwrap(),
            2
        );
    }

    #[test]
    fn patch_spec_change_bumps_generation() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("gen-spec-patch");
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 2}}),
            1,
        );
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
        put(
            &mut cat,
            &k,
            serde_json::json!({"spec": {"replicas": 1}}),
            1,
        );
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
            live.get("metadata")
                .unwrap()
                .get("deletionTimestamp")
                .unwrap(),
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
        assert!(
            cat.get(&k).is_none(),
            "object removed once finalizers cleared"
        );
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
        // (the clockless shape a pre-T3.6 log entry carries): we don't
        // invent a non-replicated value, so it's a NoOp (object kept, no
        // churn). `ResourceCommand::delete()` no longer builds this shape.
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
        assert_eq!(
            va, vb,
            "Terminating object is byte-identical across replays"
        );
    }
}

// ── T3.5 — a revision means a change ─────────────────────────────────────

#[cfg(test)]
mod a_revision_means_a_change {
    use super::*;
    use crate::command::Reason;
    use serde_json::json;

    const T1: &str = "2026-09-19T00:00:00Z";
    const T2: &str = "2026-09-19T00:05:00Z";

    fn cm(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "ConfigMap", "default", name)
    }

    fn lease() -> ResourceKey {
        ResourceKey::namespaced(
            "coordination.k8s.io",
            "v1",
            "Lease",
            "kube-node-lease",
            "n1",
        )
    }

    /// Apply `cmd` as this binary proposes it, at the next index.
    fn step(cat: &mut ResourceCatalog, cmd: &ResourceCommand) -> ApplyOutcome {
        let index = cat.last_applied_index + 1;
        cat.apply(cmd, 1, index)
    }

    fn put(key: &ResourceKey, value: serde_json::Value) -> ResourceCommand {
        ResourceCommand::put(key.clone(), value, Reason::Operator)
    }

    fn merge(key: &ResourceKey, patch: serde_json::Value) -> ResourceCommand {
        ResourceCommand::patch(key.clone(), patch, Reason::Operator)
    }

    fn ssa(
        key: &ResourceKey,
        body: serde_json::Value,
        manager: &str,
        time: &str,
    ) -> ResourceCommand {
        ResourceCommand::apply_ssa(
            key.clone(),
            body,
            ApplyMeta {
                manager: manager.into(),
                force: false,
                time: time.into(),
            },
            None,
            Reason::Operator,
        )
    }

    /// The entry a pre-T3.5 binary logged for `cmd`: the bare command.
    fn unmarked(cmd: &ResourceCommand) -> LoggedCommand {
        serde_json::from_value(serde_json::to_value(cmd).unwrap()).unwrap()
    }

    fn rv(cat: &ResourceCatalog, key: &ResourceKey) -> String {
        cat.get(key).unwrap()["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Everything a skipped write must leave as it was.
    fn assert_nothing_committed(
        before: &ResourceCatalog,
        out: &ApplyOutcome,
        after: &ResourceCatalog,
    ) {
        assert_eq!(out.op, ResourceOp::Unchanged);
        assert!(out.change.is_none(), "an unchanged write emits no change");
        assert_eq!(after.revision(), before.revision(), "no revision consumed");
        assert_eq!(after.history, before.history, "no history entry");
        assert_eq!(
            after.resources, before.resources,
            "stored objects untouched"
        );
    }

    // ── the pure gate ──────────────────────────────────────────────────────

    #[test]
    fn unchanged_ignores_only_what_every_write_restamps() {
        let prior = json!({
            "metadata": {
                "name": "a", "uid": "u", "resourceVersion": "5", "generation": 2,
                "labels": {"app": "x"},
                "managedFields": [
                    {"manager": "kubectl", "operation": "Apply", "time": T1, "fieldsV1": {"f:data": {}}},
                    {"manager": "helm", "operation": "Apply", "time": T1, "fieldsV1": {"f:spec": {}}}
                ]
            },
            "spec": {"replicas": 3},
            "data": {"k": "v"}
        });
        let with = |edit: &dyn Fn(&mut serde_json::Value)| {
            let mut v = prior.clone();
            edit(&mut v);
            v
        };

        // Restamped on every write: not a change.
        let restamped = with(&|v| {
            v["metadata"]["resourceVersion"] = json!("9");
            v["metadata"]["generation"] = json!(3);
        });
        assert!(unchanged(&prior, &restamped, None));
        let unstamped = with(&|v| {
            let m = v["metadata"].as_object_mut().unwrap();
            m.remove("resourceVersion");
            m.remove("generation");
        });
        assert!(unchanged(&prior, &unstamped, None));
        let reapplied = with(&|v| v["metadata"]["managedFields"][0]["time"] = json!(T2));
        assert!(unchanged(&prior, &reapplied, Some("kubectl")));

        // Everything else is a change.
        assert!(
            !unchanged(&prior, &reapplied, None),
            "no manager: no time is ignored"
        );
        let other_time = with(&|v| v["metadata"]["managedFields"][1]["time"] = json!(T2));
        assert!(
            !unchanged(&prior, &other_time, Some("kubectl")),
            "only the caller's own entry's time is ignored"
        );
        let other_fields = with(&|v| v["metadata"]["managedFields"][0]["fieldsV1"] = json!({}));
        assert!(
            !unchanged(&prior, &other_fields, Some("kubectl")),
            "ownership is content"
        );
        let new_owner = with(&|v| {
            v["metadata"]["managedFields"]
                .as_array_mut()
                .unwrap()
                .push(json!({"manager": "flux", "operation": "Apply", "time": T2}));
        });
        assert!(!unchanged(&prior, &new_owner, Some("flux")));
        let relabelled = with(&|v| v["metadata"]["labels"]["app"] = json!("y"));
        assert!(!unchanged(&prior, &relabelled, None));
        let unlabelled = with(&|v| {
            v["metadata"].as_object_mut().unwrap().remove("labels");
        });
        assert!(!unchanged(&prior, &unlabelled, None));
        let rescaled = with(&|v| v["spec"]["replicas"] = json!(4));
        assert!(!unchanged(&prior, &rescaled, None));
        let with_status = with(&|v| v["status"] = json!({}));
        assert!(
            !unchanged(&prior, &with_status, None),
            "an added top-level key"
        );
        let dropped_data = with(&|v| {
            v.as_object_mut().unwrap().remove("data");
        });
        assert!(
            !unchanged(&prior, &dropped_data, None),
            "a removed top-level key"
        );
        let unowned_rv = with(&|v| v["spec"]["resourceVersion"] = json!("1"));
        assert!(
            !unchanged(&prior, &unowned_rv, None),
            "resourceVersion is ignored under metadata only"
        );

        // Total over non-objects.
        assert!(unchanged(&json!(1), &json!(1), None));
        assert!(!unchanged(&json!(1), &json!("1"), None));
        assert!(!unchanged(&prior, &json!(null), None));
    }

    // ── Put ────────────────────────────────────────────────────────────────

    #[test]
    fn an_identical_put_keeps_its_resource_version_and_commits_nothing() {
        let mut cat = ResourceCatalog::default();
        let body = json!({"metadata": {"name": "a"}, "data": {"k": "v"}});
        assert_eq!(
            step(&mut cat, &put(&cm("a"), body.clone())).op,
            ResourceOp::Created
        );
        let before = cat.clone();

        let out = step(&mut cat, &put(&cm("a"), body));

        assert_nothing_committed(&before, &out, &cat);
        assert_eq!(rv(&cat, &cm("a")), "1");
        assert_eq!(
            cat.get_with_meta(&cm("a")).unwrap().1,
            VersionMeta::created_at(Revision(1)),
            "mod_revision and version stay put"
        );
    }

    #[test]
    fn a_rebuilt_body_missing_the_identity_the_store_owns_is_unchanged() {
        // Controllers re-Put a rebuilt object without uid, creationTimestamp
        // or resourceVersion. The store would carry uid and creationTimestamp
        // over, so the stored object would come out identical.
        let mut cat = ResourceCatalog::default();
        let first = json!({
            "metadata": {"name": "a", "creationTimestamp": T1},
            "data": {"k": "v"}
        });
        step(&mut cat, &put(&cm("a"), first));
        let before = cat.clone();

        let rebuilt = json!({"metadata": {"name": "a"}, "data": {"k": "v"}});
        let out = step(&mut cat, &put(&cm("a"), rebuilt));
        assert_nothing_committed(&before, &out, &cat);

        // A real edit still commits, and still keeps the identity.
        let uid = cat.get(&cm("a")).unwrap()["metadata"]["uid"].clone();
        let edited = json!({"metadata": {"name": "a"}, "data": {"k": "w"}});
        assert_eq!(
            step(&mut cat, &put(&cm("a"), edited)).op,
            ResourceOp::Replaced
        );
        let stored = cat.get(&cm("a")).unwrap();
        assert_eq!(stored["metadata"]["uid"], uid);
        assert_eq!(stored["metadata"]["creationTimestamp"], T1);
        assert_eq!(rv(&cat, &cm("a")), "2");
    }

    #[test]
    fn the_precondition_is_judged_before_the_gate() {
        let mut cat = ResourceCatalog::default();
        let body = json!({"data": {"k": "v"}});
        step(&mut cat, &put(&cm("a"), body.clone()));
        let stale = ResourceCommand::Put {
            key: cm("a"),
            value: body.clone(),
            expected: Some(Revision(99)),
            reason: Reason::Operator,
        };
        assert_eq!(step(&mut cat, &stale).op, ResourceOp::Conflict);
        let current = ResourceCommand::Put {
            key: cm("a"),
            value: body,
            expected: Some(Revision(1)),
            reason: Reason::Operator,
        };
        assert_eq!(step(&mut cat, &current).op, ResourceOp::Unchanged);
    }

    #[test]
    fn a_lease_renewal_still_bumps() {
        // A heartbeat changes renewTime on every write, so it is a change.
        let mut cat = ResourceCatalog::default();
        let lease_at = |t: &str| json!({"spec": {"holderIdentity": "n1", "renewTime": t}});
        step(&mut cat, &put(&lease(), lease_at(T1)));
        let out = step(&mut cat, &put(&lease(), lease_at(T2)));
        assert_eq!(out.op, ResourceOp::Replaced);
        assert_eq!(cat.revision(), Revision(2));

        let renew = json!({"spec": {"renewTime": "2026-09-19T00:10:00Z"}});
        assert_eq!(
            step(&mut cat, &merge(&lease(), renew.clone())).op,
            ResourceOp::Patched
        );
        assert_eq!(cat.revision(), Revision(3));
        assert_eq!(rv(&cat, &lease()), "3");
        // The same renewal twice is not.
        assert_eq!(
            step(&mut cat, &merge(&lease(), renew)).op,
            ResourceOp::Unchanged
        );
        assert_eq!(cat.revision(), Revision(3));
    }

    // ── Patch ──────────────────────────────────────────────────────────────

    #[test]
    fn a_patch_that_sets_what_is_already_there_is_unchanged() {
        let mut cat = ResourceCatalog::default();
        let seeded = json!({"data": {"k": "v"}, "status": {"phase": "Ready"}});
        step(&mut cat, &put(&cm("a"), seeded));
        for noop in [
            json!({"data": {"k": "v"}}),
            json!({"status": {"phase": "Ready"}}),
            json!({}),
            // A client echoing a stale resourceVersion in the body changes
            // nothing the store keeps.
            json!({"metadata": {"resourceVersion": "0"}}),
        ] {
            let before = cat.clone();
            let out = step(&mut cat, &merge(&cm("a"), noop));
            assert_nothing_committed(&before, &out, &cat);
        }
        let gone = merge(&cm("a"), json!({"status": {"phase": "Gone"}}));
        assert_eq!(step(&mut cat, &gone).op, ResourceOp::Patched);
        assert_eq!(cat.revision(), Revision(2));
    }

    // ── server-side apply ──────────────────────────────────────────────────

    #[test]
    fn a_reapplied_config_is_unchanged_and_keeps_the_time_it_changed_at() {
        let mut cat = ResourceCatalog::default();
        let config = json!({"apiVersion": "v1", "kind": "ConfigMap", "data": {"k": "v"}});
        assert_eq!(
            step(&mut cat, &ssa(&cm("a"), config.clone(), "kubectl", T1)).op,
            ResourceOp::Created
        );
        let before = cat.clone();

        let out = step(&mut cat, &ssa(&cm("a"), config, "kubectl", T2));

        assert_nothing_committed(&before, &out, &cat);
        assert_eq!(
            cat.get(&cm("a")).unwrap()["metadata"]["managedFields"][0]["time"],
            T1,
            "the stored entry keeps the time of the apply that changed it"
        );
    }

    #[test]
    fn a_second_manager_claiming_the_same_values_is_a_change() {
        let mut cat = ResourceCatalog::default();
        let config = json!({"apiVersion": "v1", "kind": "ConfigMap", "data": {"k": "v"}});
        step(&mut cat, &ssa(&cm("a"), config.clone(), "kubectl", T1));
        let out = step(&mut cat, &ssa(&cm("a"), config, "flux", T2));
        assert_eq!(out.op, ResourceOp::Patched, "managedFields gained an owner");
        assert_eq!(cat.revision(), Revision(2));
    }

    // ── what the gate leaves alone ─────────────────────────────────────────

    #[test]
    fn an_identical_write_that_releases_the_last_finalizer_still_removes() {
        // A Terminating object with no finalizers left is removed by the next
        // write that stores it — even one that changes nothing else. Only a
        // create can store one, so seed it that way.
        let mut cat = ResourceCatalog::default();
        let terminating = json!({"metadata": {"deletionTimestamp": T1}, "data": {"k": "v"}});
        step(&mut cat, &put(&cm("a"), terminating.clone()));
        let out = step(&mut cat, &put(&cm("a"), terminating));
        assert_eq!(out.op, ResourceOp::Deleted);
        assert!(cat.get(&cm("a")).is_none());
    }

    #[test]
    fn a_transaction_put_commits_identical_content_as_etcd_does() {
        let mut cat = ResourceCatalog::default();
        let body = json!({"data": {"k": "v"}});
        step(&mut cat, &put(&cm("a"), body.clone()));
        let txn = ResourceCommand::Txn {
            compares: Vec::new(),
            success: vec![TxnOp::Put {
                key: cm("a"),
                value: body,
            }],
            failure: Vec::new(),
            reason: Reason::Operator,
        };
        assert_eq!(step(&mut cat, &txn).op, ResourceOp::Replaced);
        assert_eq!(cat.revision(), Revision(2));
    }

    // ── replay ─────────────────────────────────────────────────────────────

    #[test]
    fn an_unmarked_log_entry_replays_under_the_old_rules() {
        // Every write shape a pre-T3.5 log holds, replayed through
        // apply_logged: each identical rewrite still commits a revision,
        // exactly as it did when the entry was written.
        let config = json!({"apiVersion": "v1", "kind": "ConfigMap", "data": {"k": "v"}});
        let log = [
            put(&cm("a"), json!({"data": {"k": "v"}})),
            put(&cm("a"), json!({"data": {"k": "v"}})),
            merge(&cm("a"), json!({"data": {"k": "v"}})),
            ssa(&cm("b"), config.clone(), "kubectl", T1),
            ssa(&cm("b"), config, "kubectl", T2),
        ];
        let expected = [
            ResourceOp::Created,
            ResourceOp::Replaced,
            ResourceOp::Patched,
            ResourceOp::Created,
            ResourceOp::Patched,
        ];

        let mut old = ResourceCatalog::default();
        for (i, (cmd, op)) in log.iter().zip(expected).enumerate() {
            let entry = unmarked(cmd);
            assert_eq!(entry.semantics(), ApplySemantics::V0);
            assert_eq!(
                old.apply_logged(&entry, 1, i as u64 + 1).op,
                op,
                "entry {i}"
            );
        }
        assert_eq!(old.revision(), Revision(5));
        assert_eq!(rv(&old, &cm("a")), "3");

        // The same writes proposed today: only the real changes commit.
        let mut new = ResourceCatalog::default();
        let ops: Vec<ResourceOp> = log
            .iter()
            .enumerate()
            .map(|(i, cmd)| {
                new.apply_logged(&LoggedCommand::proposed(cmd.clone()), 1, i as u64 + 1)
                    .op
            })
            .collect();
        assert_eq!(
            ops,
            [
                ResourceOp::Created,
                ResourceOp::Unchanged,
                ResourceOp::Unchanged,
                ResourceOp::Created,
                ResourceOp::Unchanged,
            ]
        );
        assert_eq!(new.revision(), Revision(2));
    }
}

// ── metadata.uid — one deterministic minter, RFC-4122 shaped ──────────────

/// The engenho UID namespace. A fixed random constant, so the derivation is
/// domain-separated from any other BLAKE3 use in the fleet — two different
/// subsystems hashing the same label must not collide into one identifier.
const UID_NAMESPACE: &str = "engenho.pleme.io/resource-uid/v1";

/// Mint `metadata.uid` for a newly-created object.
///
/// ## Why this is derived and not random
///
/// engenho's store is attested (`theory/ENGENHO.md` §VI.1, the determinism
/// contract): replaying the same command sequence must reproduce the same
/// bytes. A `uuid::new_v4()` would break that — the uid is part of the object,
/// the object is part of the hash chain, so a random uid makes every replay
/// diverge. The uid is therefore a **pure function of (key, create_revision)**,
/// which is also why it is safe for the two call sites to mint independently:
/// they cannot disagree.
///
/// ## Why the old format was a conformance defect
///
/// It emitted `uid-v1-v1-ConfigMap-default-x-640` — not a UUID at all, and
/// with a doubled `v1-v1` because [`ResourceKey::label`] renders an empty core
/// group as the literal `"v1"` and then formats `{group}/{version}`. Upstream
/// guarantees `metadata.uid` is an RFC-4122 UUID; client-go's `types.UID` is a
/// bare string so most clients tolerate it, but anything that PARSES a uid
/// (`kubectl` ownership graphs, CSI volume handles, audit correlation) breaks
/// on a non-UUID, and it is a straightforward conformance divergence.
///
/// ## The shape
///
/// An RFC 9562 **version-8** UUID — the variant explicitly reserved for
/// custom/vendor-defined derivations, which is exactly what this is. Honest by
/// construction: a v4 would claim randomness this value does not have, and a
/// v5 would claim SHA-1. The 128 bits are the first 16 bytes of
/// `BLAKE3(namespace || key-label || create-revision)`, with the version and
/// variant nibbles overwritten per the RFC.
///
/// BLAKE3 is already this crate's dependency and the fleet's attestation hash,
/// so this adds no dependency and reuses the primitive the determinism
/// contract is already stated in terms of.
#[must_use]
pub fn mint_uid(key_label: &str, create_revision: crate::revision::Revision) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(UID_NAMESPACE.as_bytes());
    hasher.update(b"\x00");
    hasher.update(key_label.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&create_revision.get().to_be_bytes());
    let digest = hasher.finalize();
    let b = digest.as_bytes();

    let mut u = [0u8; 16];
    u.copy_from_slice(&b[..16]);
    // RFC 9562 §4.1: version in the high nibble of octet 6.
    u[6] = (u[6] & 0x0F) | 0x80; // version 8 (custom)
    // RFC 9562 §4.1: variant `10xx` in the high bits of octet 8.
    u[8] = (u[8] & 0x3F) | 0x80;

    let mut out = String::with_capacity(36);
    for (i, byte) in u.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        // Lowercase hex, two digits — upstream renders uids lowercase.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod uid_tests {
    use super::mint_uid;
    use crate::revision::Revision;

    /// Shape: 8-4-4-4-12 lowercase hex, 36 chars.
    #[test]
    fn is_rfc4122_shaped() {
        let u = mint_uid("v1/v1/ConfigMap/default/x", Revision(640));
        assert_eq!(u.len(), 36, "a UUID renders as 36 chars: {u}");
        let parts: Vec<&str> = u.split('-').collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12],
            "8-4-4-4-12 grouping: {u}"
        );
        assert!(
            u.chars().all(|c| c.is_ascii_hexdigit() || c == '-'),
            "lowercase hex + dashes only: {u}"
        );
        assert!(
            u.chars().all(|c| !c.is_ascii_uppercase()),
            "upstream renders uids lowercase: {u}"
        );
    }

    /// The version nibble is 8 and the variant bits are `10xx` (RFC 9562).
    #[test]
    fn carries_version_8_and_the_rfc_variant() {
        let u = mint_uid("v1/v1/Pod/default/p", Revision(1));
        let version = u.split('-').nth(2).unwrap().as_bytes()[0] as char;
        assert_eq!(
            version, '8',
            "version-8 = custom/derived, honest for a hash: {u}"
        );
        let variant = u.split('-').nth(3).unwrap().as_bytes()[0] as char;
        assert!(
            matches!(variant, '8' | '9' | 'a' | 'b'),
            "variant must be 10xx: {u}"
        );
    }

    /// Determinism — the whole reason this is not `new_v4()`.
    #[test]
    fn is_deterministic() {
        assert_eq!(
            mint_uid("v1/v1/ConfigMap/ns/a", Revision(7)),
            mint_uid("v1/v1/ConfigMap/ns/a", Revision(7)),
            "the same (key, revision) MUST mint the same uid — the store is attested"
        );
    }

    /// Distinct inputs mint distinct uids, on BOTH axes.
    #[test]
    fn distinguishes_key_and_revision() {
        let base = mint_uid("v1/v1/ConfigMap/ns/a", Revision(7));
        assert_ne!(
            base,
            mint_uid("v1/v1/ConfigMap/ns/b", Revision(7)),
            "name must matter"
        );
        assert_ne!(
            base,
            mint_uid("v1/v1/ConfigMap/ns2/a", Revision(7)),
            "namespace must matter"
        );
        assert_ne!(
            base,
            mint_uid("v1/v1/Secret/ns/a", Revision(7)),
            "kind must matter"
        );
        assert_ne!(
            base,
            mint_uid("v1/v1/ConfigMap/ns/a", Revision(8)),
            "revision must matter"
        );
    }

    /// A recreated object (same name, later revision) gets a DIFFERENT uid —
    /// upstream's rule, and what lets a stale ownerReference be detected.
    #[test]
    fn recreation_mints_a_new_identity() {
        assert_ne!(
            mint_uid("v1/v1/Pod/ns/p", Revision(10)),
            mint_uid("v1/v1/Pod/ns/p", Revision(99)),
            "delete+recreate is a NEW object; a reused uid would make a stale \
             ownerReference silently adopt the replacement"
        );
    }

    /// The old format's actual defect, pinned so it cannot come back.
    #[test]
    fn is_not_the_old_prefixed_format() {
        let u = mint_uid("v1/v1/ConfigMap/conformance-probe/x", Revision(640));
        assert!(
            !u.starts_with("uid-"),
            "the `uid-` prefix was never a UUID: {u}"
        );
        assert!(
            !u.contains("v1-v1"),
            "the doubled core-group render must be gone: {u}"
        );
        assert!(
            !u.contains("ConfigMap"),
            "a uid must not leak the kind: {u}"
        );
    }
}

// ── T3.6 — a delete carries its clock ─────────────────────────────────────

#[cfg(test)]
mod a_delete_carries_its_clock {
    use super::*;
    use crate::command::Reason;
    use serde_json::json;

    fn pod(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    /// A pod; `finalizers` decides whether a delete can remove it at once.
    fn pod_body(name: &str, finalizers: &[&str]) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "default", "finalizers": finalizers},
            "spec": {"containers": [{"name": "c", "image": "img:1"}]}
        })
    }

    /// A catalog holding `value` at `key`, committed at revision 1.
    fn holding(key: &ResourceKey, value: serde_json::Value) -> ResourceCatalog {
        let mut cat = ResourceCatalog::default();
        cat.apply(
            &ResourceCommand::put(key.clone(), value, Reason::Operator),
            1,
            1,
        );
        cat
    }

    fn deletion_timestamp(cat: &ResourceCatalog, key: &ResourceKey) -> Option<String> {
        cat.get(key)
            .and_then(|v| v["metadata"]["deletionTimestamp"].as_str())
            .map(str::to_owned)
    }

    fn carried(cmd: &ResourceCommand) -> Option<String> {
        match cmd {
            ResourceCommand::Delete {
                deletion_timestamp, ..
            } => deletion_timestamp.clone(),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    /// The controller and GC shape. Before T3.6 this delete came out as a
    /// `NoOp` on a finalizer-bearing object — nothing stamped, nothing
    /// removed — so the writer proposed it again on every tick.
    #[test]
    fn a_controller_delete_of_a_finalizer_bearing_object_goes_terminating() {
        let k = pod("held");
        let mut cat = holding(&k, pod_body("held", &["example.com/hold"]));

        let before = engenho_types::time::now_rfc3339_utc();
        let cmd = ResourceCommand::delete(k.clone(), Reason::GarbageCollector);
        let after = engenho_types::time::now_rfc3339_utc();
        let out = cat.apply(&cmd, 1, 2);

        assert_eq!(
            out.op,
            ResourceOp::DeletionPending,
            "a clocked delete of a finalizer-bearing object goes Terminating"
        );
        assert_eq!(cat.revision(), Revision(2), "the stamp is a real change");
        let stamped = deletion_timestamp(&cat, &k).expect("kept, now Terminating");
        // Fixed-width RFC3339 Zulu strings order chronologically.
        assert!(
            before <= stamped && stamped <= after,
            "stamped with the instant the constructor read: {before} <= {stamped} <= {after}"
        );
        assert_eq!(
            Some(stamped),
            carried(&cmd),
            "every replica stamps the command's bytes, never its own clock"
        );
        assert_eq!(
            cat.get(&k).map(|v| v["metadata"]["finalizers"].clone()),
            Some(json!(["example.com/hold"])),
            "the finalizer still holds the object"
        );
    }

    /// Unchanged by T3.6: a finalizer-free object is removed at once.
    #[test]
    fn a_controller_delete_of_a_finalizer_free_object_still_removes_it() {
        let k = pod("plain");
        let mut cat = holding(&k, pod_body("plain", &[]));
        let out = cat.apply(
            &ResourceCommand::delete(k.clone(), Reason::Controller),
            1,
            2,
        );
        assert_eq!(out.op, ResourceOp::Deleted);
        assert!(cat.get(&k).is_none(), "removed, not stamped");
        assert_eq!(cat.revision(), Revision(2));
    }

    /// The log holds clockless deletes in two spellings: the field absent
    /// (written before it existed) and the field `null` (written by the
    /// clockless `delete()` before T3.6). Both replay exactly as they did.
    #[test]
    fn a_clockless_log_entry_replays_exactly_as_before() {
        for spelling in [None, Some(serde_json::Value::Null)] {
            let k = pod("old");
            let mut wire = json!({
                "kind": "delete",
                "key": k,
                "expected": null,
                "reason": "garbage_collector"
            });
            if let Some(ts) = spelling.clone() {
                wire["deletion_timestamp"] = ts;
            }
            let entry: LoggedCommand = serde_json::from_value(wire).unwrap();
            assert_eq!(
                carried(entry.command()),
                None,
                "{spelling:?}: decodes clockless"
            );

            // Finalizer-bearing: left exactly as it was.
            let before = holding(&k, pod_body("old", &["example.com/hold"]));
            let mut after = before.clone();
            let out = after.apply_logged(&entry, 1, 2);
            assert_eq!(out.op, ResourceOp::NoOp, "{spelling:?}: no clock, no stamp");
            assert!(out.change.is_none(), "{spelling:?}: no event");
            assert_eq!(
                after.revision(),
                before.revision(),
                "{spelling:?}: no revision"
            );
            assert_eq!(after.history, before.history, "{spelling:?}: no history");
            assert_eq!(
                after.resources, before.resources,
                "{spelling:?}: stored object untouched"
            );

            // Finalizer-free: removed, as always.
            let mut cat = holding(&k, pod_body("old", &[]));
            let out = cat.apply_logged(&entry, 1, 2);
            assert_eq!(out.op, ResourceOp::Deleted, "{spelling:?}");
            assert!(cat.get(&k).is_none(), "{spelling:?}: removed");
        }
    }
}
