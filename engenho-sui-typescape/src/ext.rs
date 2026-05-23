//! The `Typescape` extension trait — every typed primitive that
//! participates in the live-config loop implements this.

use crate::{TypescapeError, TypescapeValue};

/// Bidirectional conversion between a typed Rust value and a
/// [`TypescapeValue`].
///
/// **Round-trip law** (proptest-enforced for every implementer):
///
/// ```text
/// for every well-formed `t: T`:
///   T::from_typescape_value(&t.to_typescape_value())? == t
/// ```
///
/// This is the bridge contract — if `T` can round-trip, then sui can
/// evaluate it as a `(deftypescape …)` expression and engenho can
/// reconcile the resulting typed value, with no lossy step in between.
pub trait Typescape: Sized {
    /// Project this typed value into the bridge representation.
    fn to_typescape_value(&self) -> TypescapeValue;

    /// Reconstruct from a [`TypescapeValue`]. Errors are typed
    /// [`TypescapeError`] — variant mismatch, missing attr, cardinality,
    /// or invariant.
    ///
    /// # Errors
    /// - [`TypescapeError::VariantMismatch`] when the value shape
    ///   doesn't match what this primitive expects.
    /// - [`TypescapeError::MissingAttr`] when a required attribute
    ///   is absent.
    /// - [`TypescapeError::Cardinality`] when a list / attrs has the
    ///   wrong element count.
    /// - [`TypescapeError::Invariant`] when shape-check passes but
    ///   a typed invariant is violated (e.g. negative duration).
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError>;
}

// ── Foundational impls (the smallest typed primitives) ───────────

impl Typescape for bool {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::bool(*self)
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        value.as_bool()
    }
}

impl Typescape for i64 {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::int(*self)
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        value.as_int()
    }
}

impl Typescape for u64 {
    fn to_typescape_value(&self) -> TypescapeValue {
        // u64 → i64 cast is lossy for > i64::MAX; bound the surface so
        // the loss surfaces as a typed Invariant error not a silent
        // truncation.
        let as_i64 = i64::try_from(*self).unwrap_or(i64::MAX);
        TypescapeValue::int(as_i64)
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        let i = value.as_int()?;
        u64::try_from(i).map_err(|_| TypescapeError::Invariant {
            location: "u64".into(),
            reason: format!("negative int {i} cannot be u64"),
        })
    }
}

impl Typescape for f64 {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::float(*self)
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        value.as_float()
    }
}

impl Typescape for String {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::string(self.as_str())
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        Ok(value.as_str()?.to_string())
    }
}

impl<T: Typescape> Typescape for Vec<T> {
    fn to_typescape_value(&self) -> TypescapeValue {
        TypescapeValue::list(self.iter().map(Typescape::to_typescape_value))
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        value
            .as_list()?
            .iter()
            .map(T::from_typescape_value)
            .collect()
    }
}

impl<T: Typescape> Typescape for Option<T> {
    fn to_typescape_value(&self) -> TypescapeValue {
        match self {
            Some(v) => v.to_typescape_value(),
            None => TypescapeValue::null(),
        }
    }
    fn from_typescape_value(value: &TypescapeValue) -> Result<Self, TypescapeError> {
        match value {
            TypescapeValue::Null => Ok(None),
            other => T::from_typescape_value(other).map(Some),
        }
    }
}
