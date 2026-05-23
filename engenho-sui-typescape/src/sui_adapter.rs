//! `with-sui-eval` — real adapter from [`sui_eval::Value`] to
//! [`TypescapeValue`].
//!
//! See lib.rs for why this lives behind a feature flag. The
//! adapter:
//!
//!   1. Calls `Value::demand()` to force thunks — sui-eval's typed
//!      forcing API returns a `Concrete` enum that has no Thunk
//!      variant by construction.
//!   2. Recursively converts every Attrs / List element (calling
//!      `demand()` on each as the recursion descends — Nix
//!      semantics: list elements + attrs values may still be lazy
//!      after the parent is forced).
//!   3. Returns `TypescapeError::Invariant` for Lambda / Builtin
//!      variants — these have no Send+Sync representation and a
//!      typed system declaration ought not to encode behavior here.
//!   4. Drops sui's per-string context set (the set of store paths
//!      a string transitively depends on). The bridged value is
//!      the materialized text only; context is sui-eval-only
//!      machinery.

use crate::{TypescapeError, TypescapeValue};
use std::sync::Arc;
use sui_eval::Value;

/// Convert a sui-evaluated [`Value`] into a Send+Sync
/// [`TypescapeValue`]. Forces thunks via `demand()`. Returns typed
/// errors for variants the bridge intentionally rejects.
///
/// # Errors
///
/// - [`TypescapeError::Invariant`] when the value is a `Lambda` or
///   `Builtin` (no Send+Sync representation; not a typed declaration).
/// - [`TypescapeError::Invariant`] when `demand()` itself fails
///   (cycle, missing attr in a `select`, etc.) — the underlying
///   `EvalError` is folded into the reason string.
pub fn from_sui_value(value: &Value) -> Result<TypescapeValue, TypescapeError> {
    let concrete = value.demand().map_err(|e| TypescapeError::Invariant {
        location: "from_sui_value".into(),
        reason: format!("sui demand failed: {e:?}"),
    })?;
    convert_concrete(&concrete)
}

fn convert_concrete(c: &sui_eval::value::Concrete) -> Result<TypescapeValue, TypescapeError> {
    use sui_eval::value::Concrete;
    match c {
        Concrete::Null => Ok(TypescapeValue::null()),
        Concrete::Bool(b) => Ok(TypescapeValue::bool(*b)),
        Concrete::Int(i) => Ok(TypescapeValue::int(*i)),
        Concrete::Float(f) => Ok(TypescapeValue::float(*f)),
        Concrete::String(s) => Ok(TypescapeValue::string(s.as_str().to_string())),
        Concrete::Path(p) => Ok(TypescapeValue::path(p.as_str().to_string())),
        Concrete::List(items) => {
            let mut out: Vec<TypescapeValue> = Vec::with_capacity(items.len());
            for item in items.iter() {
                out.push(from_sui_value(item)?);
            }
            Ok(TypescapeValue::List(Arc::from(out)))
        }
        Concrete::Attrs(attrs) => {
            let mut pairs: Vec<(Arc<str>, TypescapeValue)> = Vec::new();
            for (k, v) in attrs.iter() {
                pairs.push((Arc::from(k.as_str()), from_sui_value(v)?));
            }
            Ok(TypescapeValue::attrs(pairs))
        }
        Concrete::Lambda(_) => Err(TypescapeError::Invariant {
            location: "from_sui_value".into(),
            reason: "Lambda not representable in typescape (no Send+Sync)".into(),
        }),
        Concrete::Builtin(_) => Err(TypescapeError::Invariant {
            location: "from_sui_value".into(),
            reason: "Builtin not representable in typescape (no Send+Sync)".into(),
        }),
    }
}
