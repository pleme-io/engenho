//! Typescape registration for the substrate's
//! [`engenho_substrate::WorkloadShape`].
//!
//! The orphan rule forbids `impl Typescape for WorkloadShape` here
//! (both are foreign), so the conversion lives in two free functions
//! ([`workload_shape_to_typescape`] / [`workload_shape_from_typescape`])
//! that other local FSM states reuse, plus a local newtype [`ShapeTs`]
//! that *does* impl [`Typescape`] to demonstrate (and test) the
//! round-trip law on a real substrate type.

use engenho_substrate::WorkloadShape;
use engenho_sui_typescape::{Typescape, TypescapeError, TypescapeValue};

/// Build a single-key `{ shape = <tag> }` attrs value.
fn tagged(tag: &str) -> TypescapeValue {
    TypescapeValue::attrs([("shape", TypescapeValue::string(tag))])
}

/// Project a [`WorkloadShape`] into a [`TypescapeValue`] (an attrs
/// with a `shape` tag + any payload key).
#[must_use]
pub fn workload_shape_to_typescape(shape: &WorkloadShape) -> TypescapeValue {
    match shape {
        WorkloadShape::OciImage => tagged("oci_image"),
        WorkloadShape::NixClosure => tagged("nix_closure"),
        WorkloadShape::Qcow2 => tagged("qcow2"),
        WorkloadShape::Wasm => tagged("wasm"),
        WorkloadShape::HelmChart => tagged("helm_chart"),
        WorkloadShape::StaticBinary { triple } => TypescapeValue::attrs([
            ("shape", TypescapeValue::string("static_binary")),
            ("triple", TypescapeValue::string(triple.as_str())),
        ]),
        WorkloadShape::Custom { name } => TypescapeValue::attrs([
            ("shape", TypescapeValue::string("custom")),
            ("name", TypescapeValue::string(name.as_str())),
        ]),
    }
}

/// Reconstruct a [`WorkloadShape`] from a [`TypescapeValue`].
///
/// # Errors
/// [`TypescapeError`] when the value is not the expected attrs shape,
/// the `shape` tag is missing/unknown, or a payload key is absent.
pub fn workload_shape_from_typescape(v: &TypescapeValue) -> Result<WorkloadShape, TypescapeError> {
    let tag = v.attr("shape")?.as_str()?;
    Ok(match tag {
        "oci_image" => WorkloadShape::OciImage,
        "nix_closure" => WorkloadShape::NixClosure,
        "qcow2" => WorkloadShape::Qcow2,
        "wasm" => WorkloadShape::Wasm,
        "helm_chart" => WorkloadShape::HelmChart,
        "static_binary" => WorkloadShape::StaticBinary {
            triple: v.attr("triple")?.as_str()?.to_string(),
        },
        "custom" => WorkloadShape::Custom {
            name: v.attr("name")?.as_str()?.to_string(),
        },
        other => {
            return Err(TypescapeError::Invariant {
                location: "WorkloadShape".into(),
                reason: format!("unknown shape tag: {other}"),
            });
        }
    })
}

/// Local newtype over [`WorkloadShape`] carrying the [`Typescape`]
/// impl (orphan-rule-legal because the newtype is local). This is the
/// pattern every substrate domain type follows to join the typescape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShapeTs(pub WorkloadShape);

impl Typescape for ShapeTs {
    fn to_typescape_value(&self) -> TypescapeValue {
        workload_shape_to_typescape(&self.0)
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        Ok(Self(workload_shape_from_typescape(value)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(shape: WorkloadShape) {
        let ts = ShapeTs(shape.clone());
        let tv = ts.to_typescape_value();
        let back = ShapeTs::from_typescape_value(&tv).expect("round-trips");
        assert_eq!(back.0, shape);
    }

    #[test]
    fn every_unit_shape_round_trips() {
        for s in [
            WorkloadShape::OciImage,
            WorkloadShape::NixClosure,
            WorkloadShape::Qcow2,
            WorkloadShape::Wasm,
            WorkloadShape::HelmChart,
        ] {
            round_trip(s);
        }
    }

    #[test]
    fn static_binary_round_trips_with_triple() {
        round_trip(WorkloadShape::StaticBinary {
            triple: "x86_64-unknown-linux-musl".to_string(),
        });
    }

    #[test]
    fn custom_round_trips_with_name() {
        round_trip(WorkloadShape::Custom {
            name: "engenho-internal".to_string(),
        });
    }

    #[test]
    fn unknown_tag_is_invariant_error() {
        let tv = TypescapeValue::attrs([("shape", TypescapeValue::string("nope"))]);
        assert!(workload_shape_from_typescape(&tv).is_err());
    }

    #[test]
    fn non_attrs_is_variant_mismatch() {
        let tv = TypescapeValue::int(7);
        assert!(workload_shape_from_typescape(&tv).is_err());
    }
}
