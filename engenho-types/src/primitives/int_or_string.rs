//! Typed `IntOrString` — K8s's polyglot type for fields that
//! accept either an integer or a string (e.g. `targetPort`,
//! `maxSurge`/`maxUnavailable` in RollingUpdate, intstr.IntOrString).
//!
//! Wire shape is a JSON int OR a JSON string; the typed enum
//! `IntOrString::Int(i32)` / `IntOrString::String(String)` carries
//! the variant unambiguously.

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IntOrString {
    Int(i32),
    String(String),
}

impl IntOrString {
    /// Construct from an integer.
    #[must_use]
    pub fn from_int(v: i32) -> Self {
        Self::Int(v)
    }

    /// Construct from a string (e.g. `"http"` for a Service named-port).
    #[must_use]
    pub fn from_string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }
}

impl Serialize for IntOrString {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Int(n) => ser.serialize_i32(*n),
            Self::String(s) => ser.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for IntOrString {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let v = JsonValue::deserialize(de)?;
        match v {
            JsonValue::Number(n) => {
                let int = n.as_i64().ok_or_else(|| {
                    DeError::custom(format!("non-integer number in IntOrString: {n}"))
                })?;
                let int32 = i32::try_from(int).map_err(|_| {
                    DeError::custom(format!("integer out of i32 range: {int}"))
                })?;
                Ok(Self::Int(int32))
            }
            JsonValue::String(s) => Ok(Self::String(s)),
            other => Err(DeError::custom(format!(
                "IntOrString accepts only int or string; got: {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_variant_serializes_as_int() {
        let v = IntOrString::Int(9898);
        assert_eq!(serde_json::to_string(&v).unwrap(), "9898");
    }

    #[test]
    fn string_variant_serializes_as_string() {
        let v = IntOrString::String("http".into());
        assert_eq!(serde_json::to_string(&v).unwrap(), "\"http\"");
    }

    #[test]
    fn int_deserializes_from_json_int() {
        let v: IntOrString = serde_json::from_str("9898").unwrap();
        assert_eq!(v, IntOrString::Int(9898));
    }

    #[test]
    fn string_deserializes_from_json_string() {
        let v: IntOrString = serde_json::from_str("\"http\"").unwrap();
        assert_eq!(v, IntOrString::String("http".into()));
    }

    #[test]
    fn float_rejected() {
        let r: Result<IntOrString, _> = serde_json::from_str("3.14");
        assert!(r.is_err());
    }

    #[test]
    fn bool_rejected() {
        let r: Result<IntOrString, _> = serde_json::from_str("true");
        assert!(r.is_err());
    }

    #[test]
    fn round_trip_preserves_variant() {
        for v in [IntOrString::Int(80), IntOrString::String("https".into())] {
            let json = serde_json::to_string(&v).unwrap();
            let back: IntOrString = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v);
        }
    }
}
