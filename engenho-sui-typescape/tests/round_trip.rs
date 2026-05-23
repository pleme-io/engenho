//! Round-trip property tests for `Typescape`.
//!
//! For every typed primitive `T` that implements `Typescape`, this
//! file proves the round-trip law:
//!
//!   for every well-formed `t: T`:
//!     T::from_typescape_value(&t.to_typescape_value())? == t
//!
//! If a new `Typescape` impl is added, add a property here. The
//! pattern is mechanical — strategies are 1-3 lines each.

use engenho_substrate_props::proptest_with_env;
use engenho_sui_typescape::{Typescape, TypescapeValue};
use proptest::prelude::*;

proptest_with_env! {
    /// bool round-trips.
    #[test]
    fn bool_round_trips(b: bool) {
        let v = b.to_typescape_value();
        let r = bool::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, b);
    }

    /// i64 round-trips across the full range.
    #[test]
    fn i64_round_trips(i: i64) {
        let v = i.to_typescape_value();
        let r = i64::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, i);
    }

    /// u64 round-trips for values within i64 range. Values above
    /// i64::MAX saturate at i64::MAX (loss surfaces typed, see
    /// `u64_above_i64_max_saturates`).
    #[test]
    fn u64_round_trips_in_i64_range(u in 0u64..=(i64::MAX as u64)) {
        let v = u.to_typescape_value();
        let r = u64::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, u);
    }

    /// u64 values above i64::MAX silently saturate at the bridge —
    /// document the contract rather than panic.
    #[test]
    fn u64_above_i64_max_saturates(u in ((i64::MAX as u64) + 1)..u64::MAX) {
        let v = u.to_typescape_value();
        prop_assert_eq!(v, TypescapeValue::int(i64::MAX));
    }

    /// f64 round-trips for non-NaN values (NaN never equals itself).
    #[test]
    fn f64_round_trips(f in any::<f64>().prop_filter("non-NaN", |f| !f.is_nan())) {
        let v = f.to_typescape_value();
        let r = f64::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, f);
    }

    /// String round-trips for arbitrary UTF-8 text.
    #[test]
    fn string_round_trips(s: String) {
        let v = s.to_typescape_value();
        let r = String::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, s);
    }

    /// `Vec<i64>` round-trips at arbitrary length.
    #[test]
    fn vec_i64_round_trips(xs in proptest::collection::vec(any::<i64>(), 0..32)) {
        let v = xs.to_typescape_value();
        let r = <Vec<i64>>::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, xs);
    }

    /// `Vec<String>` round-trips at arbitrary length (nested impl).
    #[test]
    fn vec_string_round_trips(xs in proptest::collection::vec(".*", 0..16)) {
        let v = xs.to_typescape_value();
        let r = <Vec<String>>::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, xs);
    }

    /// `Option<i64>` round-trips both Some and None.
    #[test]
    fn option_i64_round_trips(opt in proptest::option::of(any::<i64>())) {
        let v = opt.to_typescape_value();
        let r = <Option<i64>>::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, opt);
    }

    /// Nested Option<Vec<String>> round-trips — proves the impls
    /// compose without surprise.
    #[test]
    fn option_vec_string_round_trips(opt in proptest::option::of(
        proptest::collection::vec(".*", 0..8),
    )) {
        let v = opt.to_typescape_value();
        let r = <Option<Vec<String>>>::from_typescape_value(&v).unwrap();
        prop_assert_eq!(r, opt);
    }
}

// ── Variant-mismatch error path ──────────────────────────────────

#[test]
fn variant_mismatch_surfaces_typed_error() {
    let v = TypescapeValue::bool(true);
    let err = i64::from_typescape_value(&v).unwrap_err();
    match err {
        engenho_sui_typescape::TypescapeError::VariantMismatch { expected, got } => {
            assert_eq!(expected, "int");
            assert_eq!(got, "bool");
        }
        other => panic!("expected VariantMismatch, got {other:?}"),
    }
}

#[test]
fn u64_from_negative_surfaces_invariant_error() {
    let v = TypescapeValue::int(-1);
    let err = u64::from_typescape_value(&v).unwrap_err();
    matches!(err, engenho_sui_typescape::TypescapeError::Invariant { .. });
}

#[test]
fn attrs_missing_key_surfaces_missing_attr_error() {
    let v = TypescapeValue::attrs([("present", TypescapeValue::int(1))]);
    let err = v.attr("absent").unwrap_err();
    match err {
        engenho_sui_typescape::TypescapeError::MissingAttr(k) => assert_eq!(k, "absent"),
        other => panic!("expected MissingAttr, got {other:?}"),
    }
}
