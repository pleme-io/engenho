//! `creationTimestamp` boundary stamp for controller-created objects.
//!
//! ## The compat gap this closes
//!
//! The apiserver HTTP boundary stamps `metadata.creationTimestamp` on
//! every kubectl-created object (`handler::stamp_creation_timestamp`),
//! so `kubectl get <kind>` renders a real `AGE`. But objects a
//! CONTROLLER creates — ReplicaSets (from the Deployment controller),
//! Pods (from the ReplicaSet/StatefulSet/Job controllers), Endpoints
//! (from the Endpoints controller) — never crossed that HTTP boundary;
//! they are born inside a reconcile via [`engenho_store::command::ResourceCommand::Put`].
//! Without this stamp they carried NO `creationTimestamp` → `kubectl get
//! rs/pods` showed `AGE <unknown>`.
//!
//! ## The determinism contract (★★ — mirror the apiserver boundary clock)
//!
//! `creationTimestamp` is the ONE non-deterministic input on a create.
//! It MUST be read ONCE, at the LEADER, BEFORE the Raft propose, and
//! frozen into the single replicated `Put` value — so every Raft replica
//! replays identical bytes. It MUST NOT be read inside the deterministic
//! store-apply path (which runs per-node on replay and would diverge
//! replicas — the exact law `engenho_types::time` documents, and the
//! reason the store stamps uid/resourceVersion from REPLICATED inputs but
//! never reads a clock).
//!
//! The controller-side reconcile (where [`stamp_create_timestamp`] runs)
//! IS that leader boundary: a reconcile proposes through the leader's
//! `StoreMesh::propose`, exactly once per delta, BEFORE the command is
//! replicated. So the clock read here is the controller-family analogue
//! of `handler::stamp_creation_timestamp`.
//!
//! ## The `Clock` seam (unit-test determinism)
//!
//! The wall-clock read is injected as a [`Clock`] (`fn() -> String`) so
//! unit tests pin a FIXED timestamp and assert exact bytes, while
//! production uses [`wall_clock`] (`engenho_types::time::now_rfc3339_utc`).
//! Reconcile determinism tests stay deterministic — the clock is never
//! called inline.
//!
//! ## Idempotency
//!
//! The stamp is applied ONLY when the object does not already carry a
//! `creationTimestamp` AND only on a CREATE (a `Put` of a key absent from
//! the store this tick). An update-shaped `Put` of an existing key is left
//! untouched — the timestamp is never bumped (matching k8s: a create-time
//! field, immutable thereafter).

use serde_json::Value;

/// The boundary clock read: a function yielding the RFC3339-UTC wire
/// timestamp. Injected so unit tests pin a fixed instant; production
/// passes [`wall_clock`].
///
/// Distinct from `engenho_substrate::relogio::Clock` (a monotonic /
/// causal `Instant`/`unix_secs` clock used for lease/HLC ordering): this
/// is purpose-built for the `metadata.creationTimestamp` RFC3339 WIRE
/// string, mirroring the apiserver boundary's `chrono`-rendered stamp.
pub type CreateClock = fn() -> String;

/// Production clock — the same typed RFC3339 render surface the apiserver
/// boundary uses (`engenho_types::time::now_rfc3339_utc`). NEVER `format!()`
/// of the wire string (★★ TYPED EMISSION).
#[must_use]
pub fn wall_clock() -> String {
    engenho_types::time::now_rfc3339_utc()
}

/// `true` iff the `metadata.creationTimestamp` slot is effectively UNSET —
/// absent, JSON `null`, an empty string, OR an empty object `{}`. Byte-for-
/// byte the same predicate the apiserver boundary uses
/// (`handler::creation_timestamp_is_unset`): the empty-object case matters
/// because a protobuf-decoded zero `metav1.Time` is `{}`.
#[must_use]
pub fn creation_timestamp_is_unset(slot: Option<&Value>) -> bool {
    match slot {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => s.is_empty(),
        Some(Value::Object(m)) => m.is_empty(),
        _ => false,
    }
}

/// Inject `metadata.creationTimestamp` from `clock()` into `value`, IFF the
/// slot is currently unset. Idempotent: an object that already carries a
/// non-empty creationTimestamp is left untouched (never bumped). Pure JSON
/// mutation of the (about-to-be-replicated) value — the frozen string then
/// travels inside the single replicated `Put`.
///
/// Returns `true` iff a stamp was applied (a fresh create), `false` iff the
/// slot was already set (no-op).
pub fn stamp_create_timestamp(value: &mut Value, clock: CreateClock) -> bool {
    let Some(obj) = value.as_object_mut() else {
        return false;
    };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return false;
    };
    if creation_timestamp_is_unset(meta_obj.get("creationTimestamp")) {
        meta_obj.insert(
            "creationTimestamp".to_string(),
            Value::String(clock()),
        );
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FIXED: &str = "2026-06-14T00:00:00Z";
    fn fixed_clock() -> String {
        FIXED.to_string()
    }

    #[test]
    fn stamps_when_absent() {
        let mut v = json!({"metadata": {"name": "rs1"}});
        let stamped = stamp_create_timestamp(&mut v, fixed_clock);
        assert!(stamped, "an absent timestamp must be stamped");
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED
        );
    }

    #[test]
    fn creates_metadata_when_missing() {
        let mut v = json!({"kind": "Pod"});
        assert!(stamp_create_timestamp(&mut v, fixed_clock));
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED
        );
    }

    #[test]
    fn idempotent_does_not_bump_existing() {
        let mut v = json!({"metadata": {"creationTimestamp": "2020-01-01T00:00:00Z"}});
        let stamped = stamp_create_timestamp(&mut v, fixed_clock);
        assert!(!stamped, "an existing timestamp must NOT be re-stamped");
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            "2020-01-01T00:00:00Z",
            "the existing timestamp must be preserved, never bumped"
        );
    }

    #[test]
    fn treats_empty_string_as_unset() {
        let mut v = json!({"metadata": {"creationTimestamp": ""}});
        assert!(stamp_create_timestamp(&mut v, fixed_clock));
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED
        );
    }

    #[test]
    fn treats_empty_object_as_unset() {
        // protobuf-decoded zero metav1.Time → {}
        let mut v = json!({"metadata": {"creationTimestamp": {}}});
        assert!(stamp_create_timestamp(&mut v, fixed_clock));
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED
        );
    }

    #[test]
    fn treats_null_as_unset() {
        let mut v = json!({"metadata": {"creationTimestamp": null}});
        assert!(stamp_create_timestamp(&mut v, fixed_clock));
        assert_eq!(
            v.get("metadata").unwrap().get("creationTimestamp").unwrap(),
            FIXED
        );
    }

    #[test]
    fn unset_predicate_matches_apiserver() {
        assert!(creation_timestamp_is_unset(None));
        assert!(creation_timestamp_is_unset(Some(&Value::Null)));
        assert!(creation_timestamp_is_unset(Some(&json!(""))));
        assert!(creation_timestamp_is_unset(Some(&json!({}))));
        assert!(!creation_timestamp_is_unset(Some(&json!("2026-01-01T00:00:00Z"))));
    }

    #[test]
    fn wall_clock_renders_zulu_rfc3339() {
        let s = wall_clock();
        assert!(s.ends_with('Z'), "wire timestamp must be Zulu: {s}");
        assert!(!s.contains('.'), "second precision: {s}");
    }
}
