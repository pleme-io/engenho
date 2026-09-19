//! T2.5 — one condition, set by its type, carried at most once.
//!
//! ★ THE DEFECT. A controller that reports a condition by writing it every
//! tick wakes itself: the write fires a watch event, the event re-ticks the
//! controller, and the controller writes again. The scheduler did this to an
//! unschedulable pod, about 19 times a second (38 ticks in 2 s, measured
//! 2026-09-18). Two ways into that loop:
//!
//!   * proposing a condition the object already carries, and
//!   * re-stamping `lastTransitionTime` on every write, so that each write
//!     differs from the last one even when nothing about the condition did.
//!
//! ★ THE SHAPE. [`upsert_condition`] is the whole rule, as a pure function of
//! the object read and the condition asserted:
//!
//!   * a condition of the same `type`, `status`, `reason` and `message` is
//!     [`ConditionUpsert::Unchanged`]: there is nothing to write;
//!   * otherwise the condition is set in place, at the position the type
//!     already holds, or appended; every other condition is left as it was;
//!   * `lastTransitionTime` moves only when `status` does. A write that only
//!     rewords the message carries the recorded time forward, so "how long
//!     has this been False" keeps its answer. When there is no recorded time
//!     to carry, `now` is the honest fallback (the rule
//!     [`crate::node_lease::ready_condition`] already follows for a Node);
//!   * the result holds exactly one condition of the type: duplicates are
//!     collapsed into the first.
//!
//! [`crate::status::upsert_condition_cas`] reads the object, applies this and
//! writes the result at the revision it read. This mirrors upstream's
//! `meta.SetStatusCondition` (apimachinery `pkg/api/meta/conditions.go`).

use std::fmt;

use serde_json::{Map, Value, json};

use crate::meta::{ShapeError, array_mut, object_mut};

crate::closed_enum! {
    /// A condition's `status`: upstream's closed set.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ConditionStatus {
        True,
        False,
        Unknown,
    }
}

impl ConditionStatus {
    /// The wire string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::True => "True",
            Self::False => "False",
            Self::Unknown => "Unknown",
        }
    }
}

impl fmt::Display for ConditionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One condition as the controller that owns its `type` asserts it.
///
/// There is no `lastTransitionTime` field: when the condition transitioned
/// is not the author's to say. [`upsert_condition`] decides it from the
/// condition already recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredCondition {
    /// Which aspect of the object this condition reports (`PodScheduled`,
    /// `Ready`). One controller owns a type.
    pub condition_type: &'static str,
    pub status: ConditionStatus,
    /// A CamelCase identifier for why the condition is in this status.
    pub reason: &'static str,
    /// Human-readable detail.
    pub message: String,
}

impl DesiredCondition {
    /// Whether `recorded` already says what this condition says.
    ///
    /// `type` is matched by the caller. `lastTransitionTime` and any other
    /// field (`lastProbeTime`, `observedGeneration`) are not compared: they
    /// are not part of what the author asserts. An absent `reason` or
    /// `message` reads as empty, the way upstream omits an empty one.
    fn is_recorded_by(&self, recorded: &Value) -> bool {
        text(recorded, "status") == self.status.as_str()
            && text(recorded, "reason") == self.reason
            && text(recorded, "message") == self.message
    }

    /// Whether `condition` is of this condition's type.
    fn is_type_of(&self, condition: &Value) -> bool {
        condition.get("type").and_then(Value::as_str) == Some(self.condition_type)
    }

    /// Set this condition's fields on the condition object `slot`, stamped
    /// with `transition`. Fields the author does not assert are kept.
    fn write_into(&self, slot: &mut Map<String, Value>, transition: String) {
        slot.insert("type".to_owned(), json!(self.condition_type));
        slot.insert("status".to_owned(), json!(self.status.as_str()));
        slot.insert("reason".to_owned(), json!(self.reason));
        slot.insert("message".to_owned(), json!(self.message));
        slot.insert("lastTransitionTime".to_owned(), Value::String(transition));
    }
}

/// A string field of a condition; empty when absent or not a string.
fn text<'v>(condition: &'v Value, field: &str) -> &'v str {
    condition.get(field).and_then(Value::as_str).unwrap_or("")
}

/// What [`upsert_condition`] makes of the conditions an object carries.
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionUpsert {
    /// The object already carries the condition, once. Nothing to write.
    Unchanged,
    /// The whole `status.conditions` list to write.
    Changed(Vec<Value>),
}

impl ConditionUpsert {
    /// The `.status` fields to merge for this upsert: `conditions`, or
    /// nothing when unchanged. An RFC 7396 merge replaces a list whole, so
    /// the list is the complete one [`upsert_condition`] computed.
    #[must_use]
    pub fn into_status_fields(self) -> Option<Map<String, Value>> {
        match self {
            Self::Unchanged => None,
            Self::Changed(conditions) => {
                let mut fields = Map::new();
                fields.insert("conditions".to_owned(), Value::Array(conditions));
                Some(fields)
            }
        }
    }
}

/// Set `desired` on the conditions `object` carries, by type.
///
/// `now` is the RFC 3339 instant to record when the condition transitions
/// (or when no transition time is recorded to carry forward).
///
/// # Errors
///
/// [`ShapeError::Wrong`] when `status` is not an object or
/// `status.conditions` is not a list. Nothing can be set in place in a list
/// that is not there, and replacing the value would discard what someone
/// wrote.
pub fn upsert_condition(
    object: &Value,
    desired: &DesiredCondition,
    now: &str,
) -> Result<ConditionUpsert, ShapeError> {
    let mut status = object.get("status").cloned().unwrap_or(Value::Null);
    let conditions = array_mut(&mut status, &["conditions"]).map_err(|e| e.under(&["status"]))?;

    // Collapse duplicates of the type into the first one.
    let before = conditions.len();
    let mut seen = false;
    conditions.retain(|c| {
        let duplicate = desired.is_type_of(c) && seen;
        seen |= desired.is_type_of(c);
        !duplicate
    });
    let collapsed = conditions.len() != before;

    let Some(recorded) = conditions.iter_mut().find(|c| desired.is_type_of(c)) else {
        let mut appended = Map::new();
        desired.write_into(&mut appended, now.to_owned());
        conditions.push(Value::Object(appended));
        return Ok(ConditionUpsert::Changed(std::mem::take(conditions)));
    };
    if !collapsed && desired.is_recorded_by(recorded) {
        return Ok(ConditionUpsert::Unchanged);
    }

    let transition = match recorded.get("lastTransitionTime").and_then(Value::as_str) {
        Some(at) if text(recorded, "status") == desired.status.as_str() => at.to_owned(),
        _ => now.to_owned(),
    };
    // `recorded` has a `type`, so it is an object; `object_mut` says so
    // without a panic path.
    let slot = object_mut(recorded, &[]).map_err(|e| e.under(&["status", "conditions"]))?;
    desired.write_into(slot, transition);
    Ok(ConditionUpsert::Changed(std::mem::take(conditions)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: &str = "2026-09-19T10:00:00Z";
    const NOW: &str = "2026-09-19T12:00:00Z";

    fn unschedulable(message: &str) -> DesiredCondition {
        DesiredCondition {
            condition_type: "PodScheduled",
            status: ConditionStatus::False,
            reason: "Unschedulable",
            message: message.to_owned(),
        }
    }

    fn recorded(status: &str, message: &str, at: &str) -> Value {
        json!({"type": "PodScheduled", "status": status, "reason": "Unschedulable",
               "message": message, "lastTransitionTime": at})
    }

    fn changed(upsert: ConditionUpsert) -> Vec<Value> {
        match upsert {
            ConditionUpsert::Changed(list) => list,
            ConditionUpsert::Unchanged => panic!("expected a change"),
        }
    }

    #[test]
    fn every_status_renders_its_wire_string() {
        let rendered: Vec<&str> = ConditionStatus::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(rendered, ["True", "False", "Unknown"]);
    }

    #[test]
    fn a_missing_condition_is_appended_stamped_now() {
        let pod = json!({"status": {"conditions": [{"type": "Initialized", "status": "True"}]}});
        let list = changed(upsert_condition(&pod, &unschedulable("0/1 nodes"), NOW).unwrap());
        assert_eq!(
            list,
            vec![
                json!({"type": "Initialized", "status": "True"}),
                recorded("False", "0/1 nodes", NOW),
            ]
        );
    }

    #[test]
    fn an_object_with_no_status_gets_its_first_condition() {
        let list = changed(upsert_condition(&json!({}), &unschedulable("m"), NOW).unwrap());
        assert_eq!(list, vec![recorded("False", "m", NOW)]);
    }

    /// The hot-loop defense: the same assertion is not a change, whatever
    /// the transition time or the clock says, and whatever other fields
    /// the recorded condition carries.
    #[test]
    fn the_same_condition_is_unchanged_at_any_later_now() {
        let mut carried = recorded("False", "0/1 nodes", T0);
        carried["lastProbeTime"] = json!(T0);
        let pod = json!({"status": {"conditions": [carried]}});
        assert_eq!(
            upsert_condition(&pod, &unschedulable("0/1 nodes"), NOW).unwrap(),
            ConditionUpsert::Unchanged
        );
    }

    /// Rewording is not a transition: the recorded time is carried
    /// forward, in place, and other conditions keep their positions.
    #[test]
    fn a_new_message_keeps_the_transition_time_and_the_position() {
        let pod = json!({"status": {"conditions": [
            recorded("False", "0/1 nodes", T0),
            {"type": "Initialized", "status": "True"},
        ]}});
        let list = changed(upsert_condition(&pod, &unschedulable("0/2 nodes"), NOW).unwrap());
        assert_eq!(
            list,
            vec![
                recorded("False", "0/2 nodes", T0),
                json!({"type": "Initialized", "status": "True"}),
            ]
        );
    }

    #[test]
    fn a_status_flip_stamps_now() {
        let pod = json!({"status": {"conditions": [recorded("True", "", T0)]}});
        let list = changed(upsert_condition(&pod, &unschedulable("m"), NOW).unwrap());
        assert_eq!(list, vec![recorded("False", "m", NOW)]);
    }

    /// A recorded condition with no transition time has nothing to carry;
    /// when it is rewritten for another reason, `now` is recorded.
    #[test]
    fn a_rewrite_with_no_recorded_time_stamps_now() {
        let pod = json!({"status": {"conditions": [
            {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "old"}
        ]}});
        let list = changed(upsert_condition(&pod, &unschedulable("new"), NOW).unwrap());
        assert_eq!(list, vec![recorded("False", "new", NOW)]);
    }

    /// Fields the author does not assert survive a rewrite.
    #[test]
    fn a_rewrite_keeps_fields_the_author_does_not_assert() {
        let mut carried = recorded("False", "old", T0);
        carried["lastProbeTime"] = json!(T0);
        let pod = json!({"status": {"conditions": [carried]}});
        let list = changed(upsert_condition(&pod, &unschedulable("new"), NOW).unwrap());
        assert_eq!(list[0]["lastProbeTime"], T0);
        assert_eq!(list[0]["message"], "new");
    }

    #[test]
    fn duplicates_of_the_type_collapse_into_the_first() {
        let pod = json!({"status": {"conditions": [
            recorded("False", "m", T0),
            {"type": "Initialized", "status": "True"},
            recorded("False", "stale", T0),
        ]}});
        let list = changed(upsert_condition(&pod, &unschedulable("m"), NOW).unwrap());
        assert_eq!(
            list,
            vec![
                recorded("False", "m", T0),
                json!({"type": "Initialized", "status": "True"}),
            ]
        );
    }

    #[test]
    fn a_malformed_list_is_an_error_naming_it() {
        for (pod, path) in [
            (json!({"status": "Pending"}), "status"),
            (json!({"status": {"conditions": {}}}), "status.conditions"),
        ] {
            let err = upsert_condition(&pod, &unschedulable("m"), NOW).unwrap_err();
            assert_eq!(err.path().to_string(), path, "{pod}");
        }
    }

    #[test]
    fn only_a_change_carries_status_fields() {
        assert_eq!(ConditionUpsert::Unchanged.into_status_fields(), None);
        let fields = ConditionUpsert::Changed(vec![json!(1)])
            .into_status_fields()
            .unwrap();
        assert_eq!(Value::Object(fields), json!({"conditions": [1]}));
    }
}
