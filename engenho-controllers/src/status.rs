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
//!    the store returns [`ResourceOp::Conflict`]. We treat that as a
//!    benign retry ([`StatusWriteOutcome::Conflict`]) — the write is
//!    dropped, the next watch-wake re-reads fresh state and recomputes. A
//!    status write thus NEVER clobbers a concurrent spec change.
//!
//! 3. **Typed emission.** The status JSON is authored as a typed
//!    `serde_json::Value` (the store is opaque-JSON); no `format!()` of
//!    JSON.

use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand, ResourceOp},
    resource::ResourceKey,
    revision::Revision,
};
use serde_json::Value;
use tracing::debug;

use crate::error::ControllerError;

/// Outcome of a [`write_status_cas`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusWriteOutcome {
    /// The status differed (or `observedGeneration` was behind) and the
    /// patch committed.
    Written,
    /// The computed status already matched the live one AND
    /// `observedGeneration == generation` — nothing was proposed
    /// (the idempotent-skip hot-loop defense).
    NoChange,
    /// The CAS precondition failed: a concurrent spec change advanced the
    /// object's `mod_revision` between this tick's list and the status
    /// write. Benign — dropped; the next watch-wake recomputes.
    Conflict,
}

impl StatusWriteOutcome {
    /// True iff a command actually committed (used to bump
    /// `ReconcileReport.objects_changed`).
    #[must_use]
    pub fn changed(self) -> bool {
        matches!(self, Self::Written)
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

/// Read `metadata.generation` off a stored object (the spec-intent
/// revision `observedGeneration` reconciles against). `0` when absent.
#[must_use]
pub fn generation_of(value: &Value) -> i64 {
    value
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(Value::as_i64)
        .unwrap_or(0)
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
/// shared `observed_generation_marker` helper does this).
///
/// # Errors
///
/// [`ControllerError::Store`] only on a genuine store/transport failure.
/// A CAS precondition failure is NOT an error — it returns
/// [`StatusWriteOutcome::Conflict`].
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

    if result.op == ResourceOp::Conflict {
        // A concurrent spec change advanced mod_revision between the list
        // and this write. Benign — drop it; the next watch-wake re-reads
        // fresh state and recomputes. NEVER unwrap/error on Conflict.
        debug!(
            key = %key.label(),
            expected = %expected,
            "status write conflicted with a concurrent spec change; dropping (will recompute on next wake)"
        );
        return Ok(StatusWriteOutcome::Conflict);
    }
    Ok(StatusWriteOutcome::Written)
}

/// Build the `observedGeneration` field value from the parent's live
/// `metadata.generation` — every workload status carries this so
/// consumers (and the controller's own idempotent-skip) can tell whether
/// the status reflects the current spec-intent.
#[must_use]
pub fn observed_generation(parent: &Value) -> i64 {
    generation_of(parent)
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
        assert_eq!(generation_of(&json!({"metadata": {"generation": 3}})), 3);
        assert_eq!(generation_of(&json!({"metadata": {}})), 0);
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

    #[test]
    fn outcome_changed_only_for_written() {
        assert!(StatusWriteOutcome::Written.changed());
        assert!(!StatusWriteOutcome::NoChange.changed());
        assert!(!StatusWriteOutcome::Conflict.changed());
    }
}
