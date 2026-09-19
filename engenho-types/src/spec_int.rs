//! An integer field of an opaque object has three states, not two.
//!
//! ★ THE DEFECT. A controller reading a count off a stored object wrote
//! `.and_then(Value::as_i64).unwrap_or(1)`. That one expression folds two
//! different facts into the default: the field is ABSENT (the API default
//! applies — upstream's defaulting would have written it), and the field
//! is PRESENT BUT NOT AN INTEGER. A Deployment declaring `replicas: "3"`
//! read as `1` and was scaled to one pod, with no word to anyone about
//! why. Upstream never sees that object: decoding into `*int32` rejects it
//! at the API boundary.
//!
//! ★ THE SHAPE. [`SpecInt`] names the three states, and nothing on it
//! collapses one into another — there is no `unwrap_or`, no
//! `into_option`, no `value_or_default`. A reader has to say, per arm,
//! what absent means for its field and what it does with a value it
//! cannot read. The rule the callers follow: `Absent` is the field's API
//! default, `Malformed` is "do nothing for this object, and say why".
//!
//! `null` is [`SpecInt::Absent`]. Upstream decodes a JSON `null` into a
//! nil pointer, and defaulting then fills it exactly as it fills a missing
//! key, so this is the same answer, not a leniency.
//!
//! An integer is what `serde_json` reads as an `i64`. A float (`2.5`,
//! `3.0`), a string (`"3"`), a boolean, an array, an object, and an
//! integer beyond `i64` are all [`SpecInt::Malformed`].
//!
//! Tier-honest: the three arms are a type — a reader cannot get an `i64`
//! out of a [`SpecInt`] without matching all three. That a reader builds
//! a `SpecInt` at all, rather than calling `as_i64` directly, is not: a
//! fresh `.as_i64().unwrap_or(..)` still compiles, and only review (or a
//! lint) catches it.

use serde_json::Value;

/// An integer field read from an opaque object.
///
/// Built by [`SpecInt::of`] or [`SpecInt::at`]. There is deliberately no
/// method that turns it into an `i64`: match it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecInt<'a> {
    /// The field is missing or `null`: the API default applies.
    Absent,
    /// The field holds this integer.
    Int(i64),
    /// The field holds something that is not an integer — or, from
    /// [`SpecInt::at`], a step above it holds something that is not an
    /// object. The value is the one that stood in the way.
    Malformed(&'a Value),
}

impl<'a> SpecInt<'a> {
    /// Classify a field that may be missing.
    #[must_use]
    pub fn of(field: Option<&'a Value>) -> Self {
        match field {
            None | Some(Value::Null) => Self::Absent,
            Some(value) => value.as_i64().map_or(Self::Malformed(value), Self::Int),
        }
    }

    /// The field at `path` below `object`, e.g. `["spec", "replicas"]`.
    ///
    /// Every step above the field has to be an object to be read through.
    /// A missing or `null` step makes the field [`SpecInt::Absent`] —
    /// nothing was declared under it. A step that is anything else (a
    /// `spec` that is a string) makes the field [`SpecInt::Malformed`],
    /// carrying that step's value: the field cannot be read, and reading
    /// it as absent would apply the default to an object that declared
    /// something else.
    ///
    /// The empty path classifies `object` itself.
    #[must_use]
    pub fn at(object: &'a Value, path: &[&str]) -> Self {
        let mut here = object;
        for step in path {
            here = match here {
                Value::Null => return Self::Absent,
                Value::Object(map) => match map.get(*step) {
                    Some(next) => next,
                    None => return Self::Absent,
                },
                Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
                    return Self::Malformed(here);
                }
            };
        }
        Self::of(Some(here))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const REPLICAS: &[&str] = &["spec", "replicas"];

    #[test]
    fn an_integer_is_int() {
        assert_eq!(
            SpecInt::at(&json!({"spec": {"replicas": 3}}), REPLICAS),
            SpecInt::Int(3)
        );
        assert_eq!(
            SpecInt::at(&json!({"spec": {"replicas": -2}}), REPLICAS),
            SpecInt::Int(-2)
        );
        assert_eq!(
            SpecInt::at(&json!({"spec": {"replicas": 0}}), REPLICAS),
            SpecInt::Int(0)
        );
    }

    /// The worked defect: `"3"` is not absent, and it is not 1.
    #[test]
    fn a_quoted_number_is_malformed_not_the_default() {
        let obj = json!({"spec": {"replicas": "3"}});
        assert_eq!(SpecInt::at(&obj, REPLICAS), SpecInt::Malformed(&json!("3")));
    }

    #[test]
    fn every_non_integer_value_is_malformed_and_carried() {
        for value in [
            json!("3"),
            json!("three"),
            json!(2.5),
            json!(3.0),
            json!(true),
            json!([3]),
            json!({"n": 3}),
            json!(u64::MAX),
        ] {
            let obj = json!({"spec": {"replicas": value.clone()}});
            assert_eq!(
                SpecInt::at(&obj, REPLICAS),
                SpecInt::Malformed(&value),
                "{value}"
            );
        }
    }

    #[test]
    fn missing_or_null_is_absent_at_every_step() {
        for obj in [
            json!({}),
            json!({"spec": null}),
            json!({"spec": {}}),
            json!({"spec": {"replicas": null}}),
            Value::Null,
        ] {
            assert_eq!(SpecInt::at(&obj, REPLICAS), SpecInt::Absent, "{obj}");
        }
    }

    /// A step above the field that is not an object cannot be read
    /// through. Reading it as absent would apply the default to an object
    /// that declared something else there.
    #[test]
    fn a_non_object_step_above_the_field_is_malformed() {
        let obj = json!({"spec": "oops"});
        assert_eq!(
            SpecInt::at(&obj, REPLICAS),
            SpecInt::Malformed(&json!("oops"))
        );
        let scalar = json!(7);
        assert_eq!(SpecInt::at(&scalar, REPLICAS), SpecInt::Malformed(&scalar));
    }

    #[test]
    fn of_classifies_an_optional_field() {
        assert_eq!(SpecInt::of(None), SpecInt::Absent);
        assert_eq!(SpecInt::of(Some(&Value::Null)), SpecInt::Absent);
        assert_eq!(SpecInt::of(Some(&json!(5))), SpecInt::Int(5));
        assert_eq!(
            SpecInt::of(Some(&json!("5"))),
            SpecInt::Malformed(&json!("5"))
        );
    }

    #[test]
    fn the_empty_path_classifies_the_object_itself() {
        assert_eq!(SpecInt::at(&json!(4), &[]), SpecInt::Int(4));
        assert_eq!(SpecInt::at(&Value::Null, &[]), SpecInt::Absent);
    }
}
