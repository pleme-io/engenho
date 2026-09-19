//! M0.1 item 8 — shared status-subresource write primitive.
//!
//! Every workload controller (Deployment / ReplicaSet / StatefulSet /
//! Job) computes a `.status` from the LIVE owned children it already
//! listed in its tick + writes it back. Per the Prime Directive this is
//! solved ONCE here: [`write_status_cas`] is the single shape all four
//! controllers consume.
//!
//! ## Three invariants this primitive enforces
//!
//! 1. **Idempotent skip (the primary hot-loop defense).** The desired
//!    status is compared field-by-field against the status already on the
//!    live object; if they are equal AND `status.observedGeneration`
//!    already equals `metadata.generation`, NO command is proposed
//!    ([`StatusWriteOutcome::NoChange`]). A no-op reconcile tick mutates
//!    nothing, so the watch event it would otherwise produce never
//!    happens and the loop terminates.
//!
//! 2. **Optimistic concurrency (item-5 CAS).** The status `Patch` carries
//!    `expected: Some(rv)` where `rv` is the object's
//!    `metadata.resourceVersion` AS SEEN THIS TICK. If a concurrent spec
//!    change (operator scale) committed between the controller's list and
//!    this write, `mod_revision` advanced, the precondition fails, and
//!    the store returns `ResourceOp::Conflict`. That is a benign retry
//!    (`Proposed(Rejected(Conflict))`) — the write is dropped, the next
//!    watch-wake re-reads fresh state and recomputes. A status write thus
//!    NEVER clobbers a concurrent spec change.
//!
//!    What the store did is read through [`Effect::of`], never by testing
//!    for one op: a patch the store refused for any reason
//!    (`PatchRejected`, `ApplyConflict`) is reported refused, not
//!    `Written`.
//!
//! 3. **Typed emission.** The status JSON is authored as a typed
//!    `serde_json::Value` (the store is opaque-JSON); no `format!()` of
//!    JSON.
//!
//! ## The second shape: an edit of the object as read (T2.5)
//!
//! [`write_status_cas`] writes a status computed from what the controller
//! LISTED, so after a conflict it cannot recompute: it drops the write and
//! the next watch-wake starts over. [`edit_status_cas`] is for a status
//! that is a function of the object itself, such as one condition set by
//! type ([`upsert_condition_cas`]). It reads the object, asks the edit what
//! to write, writes that at the revision it read, and on a conflict reads
//! again and retries exactly once. Its outcome names each way nothing was
//! written ([`StatusEditOutcome`]), where [`StatusWriteOutcome::NoChange`]
//! folds "already current" and "no revision" into one arm.
//!
//! The reads and writes go through [`CasEnv`], the store in production
//! ([`StoreCasEnv`]) and a fake that races a concurrent writer in tests.

use async_trait::async_trait;
use engenho_store::{
    ApplyResult, StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
    revision::Revision,
};
use serde_json::{Map, Value};
use tracing::{debug, warn};

use crate::condition::{DesiredCondition, upsert_condition};
use crate::effect::{Effect, Refusal};
use crate::error::ControllerError;
use crate::meta::{DefaultedInt, ShapeError};

/// Outcome of a [`write_status_cas`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusWriteOutcome {
    /// Nothing was proposed: the computed status already matched the live
    /// one (which carries `observedGeneration`, so the generation has been
    /// observed too), or the parent had no resourceVersion to CAS against.
    /// The idempotent-skip hot-loop defense: no Raft round trip.
    NoChange,
    /// A status patch was proposed, and this is what the store did with it:
    /// `Written` when it landed, `Rejected(Conflict)` when a concurrent spec
    /// change moved the object first (benign — the next watch-wake
    /// recomputes), `Rejected(PatchRejected | ApplyConflict)` when the store
    /// refused the patch itself.
    Proposed(Effect),
}

impl StatusWriteOutcome {
    /// What this call did, as one [`Effect`]: nothing proposed is
    /// `Unchanged`.
    pub const fn effect(self) -> Effect {
        match self {
            Self::NoChange => Effect::Unchanged,
            Self::Proposed(effect) => effect,
        }
    }

    /// True iff a status patch landed.
    #[must_use]
    pub const fn changed(self) -> bool {
        self.effect().landed()
    }
}

/// Parse a stored object's `metadata.resourceVersion` string into a
/// [`Revision`] — the CAS `expected` value for the status write.
///
/// Returns `None` when the field is absent or unparseable (a freshly
/// minted object the controller hasn't re-read yet); the caller then
/// skips the CAS write this tick rather than issuing an unconditional one
/// that could clobber a concurrent spec change.
#[must_use]
pub fn resource_version_of(value: &Value) -> Option<Revision> {
    value
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|rv| rv.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Revision)
}

/// `metadata.generation`, the spec-intent revision `observedGeneration`
/// reconciles against. The store stamps it on every write; `0` (never
/// stamped) when absent.
pub const GENERATION: DefaultedInt = DefaultedInt::new(&["metadata", "generation"], 0);

/// Read `metadata.generation` off a stored object. `0` when absent.
///
/// # Errors
///
/// [`ShapeError::NotAnInteger`] when the field holds anything but an
/// integer: a status claiming to have observed it would be a guess.
pub fn generation_of(value: &Value) -> Result<i64, ShapeError> {
    GENERATION.read(value)
}

/// Borrow the live `.status` object, or a static empty object when
/// absent — so the idempotent comparison treats "no status yet" as an
/// empty status (any non-empty desired status differs → first write).
#[must_use]
fn live_status(value: &Value) -> Value {
    value
        .get("status")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
}

/// Write a controller-computed `.status` onto `parent` via an
/// optimistic-concurrency `Patch`, skipping the write entirely when the
/// status is already current.
///
/// `parent` is the LIVE object the controller listed this tick (it
/// carries `metadata.resourceVersion` + `metadata.generation` +
/// existing `.status`). `desired_status` is the status object computed
/// from the live owned children — it MUST already include
/// `observedGeneration` set to the parent's `metadata.generation` (the
/// owned-children blanket reads it once through [`generation_of`] and
/// hands it to `compute_status`).
///
/// # Errors
///
/// [`ControllerError::Store`] only on a genuine store/transport failure.
/// A write the store refused is NOT an error — it returns
/// `Proposed(Effect::Rejected(_))`, naming the refusal.
pub async fn write_status_cas(
    store: &StoreMesh,
    key: &ResourceKey,
    parent: &Value,
    desired_status: &Value,
) -> Result<StatusWriteOutcome, ControllerError> {
    // Idempotent skip (hot-loop defense): if the live status already
    // equals the desired status (which carries observedGeneration), there
    // is nothing to write. Because desired_status embeds
    // observedGeneration == generation, equality here also implies the
    // generation has been observed — one comparison covers both.
    if &live_status(parent) == desired_status {
        return Ok(StatusWriteOutcome::NoChange);
    }

    // CAS precondition = the object's resourceVersion as seen this tick.
    // When it's missing/unparseable the object was just minted and not
    // re-read; skip rather than issue an unconditional clobbering write.
    let Some(expected) = resource_version_of(parent) else {
        debug!(
            key = %key.label(),
            "status write skipped: parent has no parseable resourceVersion this tick"
        );
        return Ok(StatusWriteOutcome::NoChange);
    };

    let result = store
        .propose(ResourceCommand::patch_cas(
            key.clone(),
            serde_json::json!({ "status": desired_status }),
            Some(expected),
            Reason::Controller,
        ))
        .await?;

    Ok(read_answer(key, expected, &result))
}

/// Read the store's answer to a proposed status patch, by [`Effect::of`]:
/// every op the store can return is placed, so a refusal of any kind is
/// reported refused and only a landed patch is `Written`.
fn read_answer(key: &ResourceKey, expected: Revision, answer: &ApplyResult) -> StatusWriteOutcome {
    let effect = read_effect(key, answer);
    if effect == Effect::Rejected(Refusal::Conflict) {
        // A concurrent spec change advanced mod_revision between the
        // list and this write. Benign — dropped; the next watch-wake
        // re-reads fresh state and recomputes.
        debug!(
            key = %key.label(),
            expected = %expected,
            "status write conflicted with a concurrent spec change; dropping (will recompute on next wake)"
        );
    }
    StatusWriteOutcome::Proposed(effect)
}

/// What the store did with a proposed status patch, logging a refusal of
/// the patch itself. A conflict is the caller's to explain: one drops the
/// write, the other retries it.
fn read_effect(key: &ResourceKey, answer: &ApplyResult) -> Effect {
    let effect = Effect::of(answer.op);
    if let Effect::Rejected(refusal @ (Refusal::PatchRejected | Refusal::ApplyConflict)) = effect {
        // The store refused the patch itself. Nothing was written, and
        // proposing the same patch again will not change that.
        warn!(
            key = %key.label(),
            %refusal,
            detail = answer.patch_error.as_deref().unwrap_or(""),
            "status write refused; nothing was written"
        );
    }
    effect
}

/// The reads and writes of a read-modify-write: read the object, and write
/// an RFC 7396 merge patch conditioned on the revision read.
///
/// [`StoreCasEnv`] in production; a fake in tests, which can move the
/// object between the read and the write.
#[async_trait]
pub trait CasEnv: Send + Sync {
    /// The live object at `key`, if any.
    async fn get(&self, key: &ResourceKey) -> Option<Value>;

    /// Merge `patch` into the object at `key` if it is still at revision
    /// `pinned`, and return the store's answer.
    ///
    /// # Errors
    ///
    /// [`ControllerError::Store`] on a transport failure. A refused write
    /// is an answer (`ResourceOp::Conflict`, …), not an error.
    async fn patch_at(
        &self,
        key: &ResourceKey,
        patch: Value,
        pinned: Revision,
    ) -> Result<ApplyResult, ControllerError>;
}

/// [`CasEnv`] over the store, writing as one [`Reason`].
#[derive(Clone, Copy)]
pub struct StoreCasEnv<'a> {
    store: &'a StoreMesh,
    reason: Reason,
}

impl<'a> StoreCasEnv<'a> {
    /// Read `store`, and write to it as `reason` (the scheduler writes as
    /// [`Reason::Scheduler`], a controller as [`Reason::Controller`]).
    #[must_use]
    pub const fn new(store: &'a StoreMesh, reason: Reason) -> Self {
        Self { store, reason }
    }
}

#[async_trait]
impl CasEnv for StoreCasEnv<'_> {
    async fn get(&self, key: &ResourceKey) -> Option<Value> {
        self.store.get(key).await
    }

    async fn patch_at(
        &self,
        key: &ResourceKey,
        patch: Value,
        pinned: Revision,
    ) -> Result<ApplyResult, ControllerError> {
        Ok(self
            .store
            .propose(ResourceCommand::patch_cas(
                key.clone(),
                patch,
                Some(pinned),
                self.reason,
            ))
            .await?)
    }
}

/// What an edit makes of the object it was handed.
#[derive(Debug, Clone, PartialEq)]
pub enum StatusEdit {
    /// The object already says what the edit would write.
    Current,
    /// The object no longer calls for this edit: it moved on since the
    /// caller decided to make it (a pod bound in the meantime).
    Superseded,
    /// Merge these fields into `.status`.
    Write(Map<String, Value>),
}

/// Outcome of an [`edit_status_cas`] call. Every arm but `Proposed` means
/// nothing was proposed, and each says why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusEditOutcome {
    /// The object already carries the edit. No Raft round trip, no event.
    Unchanged,
    /// The edit no longer applies ([`StatusEdit::Superseded`]).
    Superseded,
    /// There is no object at the key.
    Absent,
    /// The object has no parseable `metadata.resourceVersion` to write at.
    /// An unconditional write could clobber a concurrent one, so none is
    /// made.
    NoRevision,
    /// A patch was proposed at the revision read, and this is what the
    /// store did with it. When the first attempt conflicted, this is the
    /// answer to the one retry.
    Proposed(Effect),
}

impl StatusEditOutcome {
    /// What this call did, as one [`Effect`]: nothing proposed is
    /// `Unchanged`.
    pub const fn effect(self) -> Effect {
        match self {
            Self::Unchanged | Self::Superseded | Self::Absent | Self::NoRevision => {
                Effect::Unchanged
            }
            Self::Proposed(effect) => effect,
        }
    }

    /// True iff a status patch landed.
    #[must_use]
    pub const fn changed(self) -> bool {
        self.effect().landed()
    }
}

/// Read the object at `key`, ask `edit` what to merge into its `.status`,
/// and write that at the revision read. On a conflict, read again and retry
/// exactly once.
///
/// The retry re-runs `edit` on the object as re-read, so it writes on top of
/// whatever the concurrent writer did, and finds `Unchanged` or `Superseded`
/// when that writer made the edit unnecessary. A second conflict is
/// returned as `Proposed(Rejected(Conflict))`; the write that moved the
/// object fires a watch event, which re-ticks the caller.
///
/// # Errors
///
/// [`ControllerError::Store`] on a transport failure, and
/// [`ControllerError::Shape`] when `edit` finds the object malformed. A
/// write the store refused is an outcome, not an error.
pub async fn edit_status_cas<E, F>(
    env: &E,
    key: &ResourceKey,
    edit: F,
) -> Result<StatusEditOutcome, ControllerError>
where
    E: CasEnv + ?Sized,
    F: Fn(&Value) -> Result<StatusEdit, ShapeError> + Sync,
{
    match edit_once(env, key, &edit).await? {
        StatusEditOutcome::Proposed(Effect::Rejected(Refusal::Conflict)) => {
            debug!(
                key = %key.label(),
                "status edit conflicted with a concurrent write; reading again and retrying once"
            );
            edit_once(env, key, &edit).await
        }
        outcome => Ok(outcome),
    }
}

/// One read, one edit, at most one write.
async fn edit_once<E, F>(
    env: &E,
    key: &ResourceKey,
    edit: &F,
) -> Result<StatusEditOutcome, ControllerError>
where
    E: CasEnv + ?Sized,
    F: Fn(&Value) -> Result<StatusEdit, ShapeError> + Sync,
{
    let Some(object) = env.get(key).await else {
        return Ok(StatusEditOutcome::Absent);
    };
    let fields = match edit(&object)? {
        StatusEdit::Current => return Ok(StatusEditOutcome::Unchanged),
        StatusEdit::Superseded => return Ok(StatusEditOutcome::Superseded),
        StatusEdit::Write(fields) => fields,
    };
    let Some(pinned) = resource_version_of(&object) else {
        debug!(
            key = %key.label(),
            "status edit skipped: the object has no parseable resourceVersion"
        );
        return Ok(StatusEditOutcome::NoRevision);
    };
    let mut patch = Map::new();
    patch.insert("status".to_owned(), Value::Object(fields));
    let answer = env.patch_at(key, Value::Object(patch), pinned).await?;
    Ok(StatusEditOutcome::Proposed(read_effect(key, &answer)))
}

/// Set one condition on the object at `key`, by type
/// ([`upsert_condition`]), writing at the revision read and retrying once
/// on a conflict ([`edit_status_cas`]).
///
/// `Unchanged` when the object already carries the condition: nothing is
/// proposed, so a controller that asserts the same condition every tick does
/// not wake itself. `now` is the RFC 3339 instant recorded as
/// `lastTransitionTime` when the status transitions.
///
/// # Errors
///
/// As [`edit_status_cas`].
pub async fn upsert_condition_cas<E>(
    env: &E,
    key: &ResourceKey,
    desired: &DesiredCondition,
    now: &str,
) -> Result<StatusEditOutcome, ControllerError>
where
    E: CasEnv + ?Sized,
{
    edit_status_cas(env, key, |object| {
        Ok(upsert_condition(object, desired, now)?
            .into_status_fields()
            .map_or(StatusEdit::Current, StatusEdit::Write))
    })
    .await
}

/// True iff `pod` reports a `status.conditions[type=Ready,status=True]`.
/// The shared readiness predicate for the replica-counting controllers.
#[must_use]
pub fn pod_is_ready(pod: &Value) -> bool {
    pod.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .is_some_and(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resource_version_parses_string_field() {
        let v = json!({"metadata": {"resourceVersion": "42"}});
        assert_eq!(resource_version_of(&v), Some(Revision(42)));
    }

    #[test]
    fn resource_version_none_when_absent_or_bad() {
        assert_eq!(resource_version_of(&json!({"metadata": {}})), None);
        assert_eq!(
            resource_version_of(&json!({"metadata": {"resourceVersion": "x"}})),
            None
        );
    }

    #[test]
    fn generation_reads_metadata_field() {
        assert_eq!(
            generation_of(&json!({"metadata": {"generation": 3}})),
            Ok(3)
        );
        assert_eq!(generation_of(&json!({"metadata": {}})), Ok(0));
    }

    /// A generation that is not an integer is not generation 0: a status
    /// stamped `observedGeneration: 0` from it would claim to have
    /// observed a spec revision nobody can name.
    #[test]
    fn a_malformed_generation_is_an_error_not_zero() {
        assert_eq!(
            generation_of(&json!({"metadata": {"generation": "3"}}))
                .unwrap_err()
                .to_string(),
            "metadata.generation is not an integer (found a string)"
        );
    }

    #[test]
    fn live_status_defaults_to_empty_object() {
        assert_eq!(live_status(&json!({})), json!({}));
        assert_eq!(
            live_status(&json!({"status": {"replicas": 2}})),
            json!({"replicas": 2})
        );
    }

    #[test]
    fn pod_is_ready_detects_ready_condition() {
        let ready = json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}});
        let not_ready = json!({"status": {"conditions": [{"type": "Ready", "status": "False"}]}});
        let no_status = json!({"metadata": {"name": "p"}});
        assert!(pod_is_ready(&ready));
        assert!(!pod_is_ready(&not_ready));
        assert!(!pod_is_ready(&no_status));
    }

    fn answer(op: engenho_store::command::ResourceOp) -> ApplyResult {
        ApplyResult {
            op,
            ..ApplyResult::default()
        }
    }

    /// A status patch the store refused is reported refused, naming why —
    /// never `Written`. Only `Conflict` used to be read; `PatchRejected` and
    /// `ApplyConflict` fell through to `Written`, so a controller counted a
    /// status change the catalog never saw.
    #[test]
    fn a_refused_status_patch_is_reported_refused() {
        use engenho_store::command::ResourceOp;
        let key = ResourceKey::namespaced("apps", "v1", "Deployment", "ns", "web");
        for (op, refusal) in [
            (ResourceOp::Conflict, Refusal::Conflict),
            (ResourceOp::PatchRejected, Refusal::PatchRejected),
            (ResourceOp::ApplyConflict, Refusal::ApplyConflict),
        ] {
            let outcome = read_answer(&key, Revision(7), &answer(op));
            assert_eq!(
                outcome,
                StatusWriteOutcome::Proposed(Effect::Rejected(refusal)),
                "{op:?}"
            );
            assert!(!outcome.changed(), "{op:?} wrote nothing");
        }
        assert!(read_answer(&key, Revision(7), &answer(ResourceOp::Patched)).changed());
        assert_eq!(
            read_answer(&key, Revision(7), &answer(ResourceOp::NoOp)).effect(),
            Effect::Unchanged
        );
    }

    #[test]
    fn outcome_changed_only_when_the_patch_landed() {
        use engenho_store::command::ResourceOp;
        assert!(StatusWriteOutcome::Proposed(Effect::of(ResourceOp::Patched)).changed());
        assert!(!StatusWriteOutcome::NoChange.changed());
        for refused in [
            ResourceOp::Conflict,
            ResourceOp::PatchRejected,
            ResourceOp::ApplyConflict,
            ResourceOp::NoOp,
        ] {
            assert!(
                !StatusWriteOutcome::Proposed(Effect::of(refused)).changed(),
                "{refused:?} wrote nothing"
            );
        }
    }
}
