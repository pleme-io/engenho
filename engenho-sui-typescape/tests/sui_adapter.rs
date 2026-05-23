//! Integration test for the `with-sui-eval` feature — evaluate a
//! real Nix expression via sui-eval, project to TypescapeValue,
//! type-check it as a Sistema-shaped Rust value (here a simple
//! attrset → AppRef-shaped record because the real Sistema lives in
//! engenho-fonte; this crate stays substrate-agnostic).

#![cfg(feature = "with-sui-eval")]

use engenho_sui_typescape::{Typescape, TypescapeError, TypescapeValue, from_sui_value};

#[test]
fn nix_literal_evaluates_to_typescape_value() {
    let val = sui_eval::eval("42").expect("eval 42");
    let tv = from_sui_value(&val).expect("convert 42");
    assert_eq!(tv, TypescapeValue::int(42));
}

#[test]
fn nix_bool_evaluates_to_typescape_value() {
    let val = sui_eval::eval("true").unwrap();
    assert_eq!(from_sui_value(&val).unwrap(), TypescapeValue::bool(true));
}

#[test]
fn nix_string_evaluates_to_typescape_value() {
    let val = sui_eval::eval(r#""hello""#).unwrap();
    let tv = from_sui_value(&val).unwrap();
    assert_eq!(tv, TypescapeValue::string("hello"));
}

#[test]
fn nix_list_evaluates_to_typescape_value() {
    let val = sui_eval::eval("[1 2 3]").unwrap();
    let tv = from_sui_value(&val).unwrap();
    assert_eq!(
        tv,
        TypescapeValue::list(vec![
            TypescapeValue::int(1),
            TypescapeValue::int(2),
            TypescapeValue::int(3),
        ])
    );
}

#[test]
fn nix_attrset_evaluates_to_typescape_value() {
    let val = sui_eval::eval("{ a = 1; b = \"two\"; }").unwrap();
    let tv = from_sui_value(&val).unwrap();
    let attrs = tv.as_attrs().unwrap();
    assert_eq!(attrs.get("a").unwrap(), &TypescapeValue::int(1));
    assert_eq!(attrs.get("b").unwrap(), &TypescapeValue::string("two"));
}

#[test]
fn nix_lambda_surfaces_typed_error() {
    let val = sui_eval::eval("x: x + 1").unwrap();
    let err = from_sui_value(&val).unwrap_err();
    matches!(err, TypescapeError::Invariant { .. });
}

#[test]
fn nix_thunk_is_forced_transparently() {
    // `let` introduces thunks; the adapter forces them via demand().
    let val = sui_eval::eval("let x = 42; in x").unwrap();
    assert_eq!(from_sui_value(&val).unwrap(), TypescapeValue::int(42));
}

#[test]
fn nix_nested_attrset_round_trips_to_rust_typed_value() {
    // Define a small typed shape and round-trip through Nix.
    #[derive(Debug, PartialEq)]
    struct Point {
        x: i64,
        y: i64,
    }

    impl Typescape for Point {
        fn to_typescape_value(&self) -> TypescapeValue {
            TypescapeValue::attrs(vec![
                ("x", TypescapeValue::int(self.x)),
                ("y", TypescapeValue::int(self.y)),
            ])
        }
        fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
            Ok(Self {
                x: value.attr("x")?.as_int()?,
                y: value.attr("y")?.as_int()?,
            })
        }
    }

    let nix_value = sui_eval::eval("{ x = 3; y = 4; }").unwrap();
    let tv = from_sui_value(&nix_value).unwrap();
    let point = Point::from_typescape_value(&tv).unwrap();
    assert_eq!(point, Point { x: 3, y: 4 });
}

#[test]
fn nix_evaluated_sistema_shape_round_trips() {
    // The full Sistema shape lives in engenho-fonte, but this crate
    // exercises the equivalent attrset shape — proving the bridge
    // is shape-stable end-to-end before higher-level consumers
    // depend on it.
    let nix_expr = r#"{
        name = "rio";
        apps = [
            { name = "podinfo"; version = null; }
            { name = "lilitu";  version = "1.0"; }
        ];
        infra = [];
        promises = [];
        topology = { strategy = "solo"; nodes = 1; };
    }"#;
    let val = sui_eval::eval(nix_expr).unwrap();
    let tv = from_sui_value(&val).unwrap();
    let attrs = tv.as_attrs().unwrap();
    assert_eq!(attrs.get("name").unwrap(), &TypescapeValue::string("rio"));
    let apps = attrs.get("apps").unwrap().as_list().unwrap();
    assert_eq!(apps.len(), 2);
    let app0 = apps[0].as_attrs().unwrap();
    assert_eq!(
        app0.get("name").unwrap(),
        &TypescapeValue::string("podinfo")
    );
}
