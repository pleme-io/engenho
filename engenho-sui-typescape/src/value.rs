//! `TypescapeValue` — Send+Sync mirror of sui's `Value` shape.
//!
//! See lib.rs §"The TypescapeValue mirror" for the why.
//!
//! Shape parity (variants kept in lockstep with `sui_eval::Value`):
//!
//! | sui::Value | TypescapeValue | Notes |
//! |---|---|---|
//! | `Null`     | `Null`     | |
//! | `Bool(b)`  | `Bool(b)`  | |
//! | `Int(i)`   | `Int(i)`   | |
//! | `Float(f)` | `Float(f)` | |
//! | `String(s)`| `String(s)`| `Arc<str>` not `Rc<NixString>` |
//! | `Path(p)`  | `Path(p)`  | `Arc<str>` not `Box<SmolStr>` |
//! | `List(l)`  | `List(l)`  | `Arc<[Value]>` not `Rc<Vec<Value>>` |
//! | `Attrs(a)` | `Attrs(a)` | `Arc<BTreeMap<…>>` (sorted, deterministic) |
//! | `Lambda`   | **omitted**| force-before-bridge contract |
//! | `Builtin`  | **omitted**| eval-time only |
//! | `Thunk`    | **omitted**| must be demanded by sui first |

use crate::TypescapeError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Shared string slice. Uses `Arc<str>` not `Rc<str>` so values cross
/// thread boundaries cleanly.
pub type TypescapeString = Arc<str>;

/// Shared list. `Arc<[T]>` is cheap to clone (one atomic ref-count
/// bump) and never copies the payload.
pub type TypescapeList = Arc<[TypescapeValue]>;

/// Shared attribute set. `BTreeMap` keeps keys sorted for
/// deterministic serialization + content-address hashing.
pub type TypescapeAttrs = Arc<BTreeMap<TypescapeString, TypescapeValue>>;

/// The Send+Sync mirror of `sui_eval::Value`. See module docs for
/// the shape-parity table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tag", content = "value", rename_all = "snake_case")]
pub enum TypescapeValue {
    /// Nix `null` literal.
    Null,
    /// Nix boolean.
    Bool(bool),
    /// Nix integer (64-bit signed; matches sui).
    Int(i64),
    /// Nix float (64-bit IEEE-754; matches sui).
    Float(f64),
    /// Nix string (UTF-8). String context (sui's per-string set of
    /// store paths it depends on) is dropped at the bridge — the
    /// bridged value is the materialized string only.
    String(TypescapeString),
    /// Nix path literal. Stored as the source path text; resolution
    /// is the consumer's responsibility.
    Path(TypescapeString),
    /// Heterogeneous list.
    List(TypescapeList),
    /// Sorted-key attribute set. Keys are sorted (BTreeMap) so two
    /// equal logical attrs hash to the same BLAKE3.
    Attrs(TypescapeAttrs),
}

// ── Constructors (Pattern #7: concrete-first) ────────────────────

impl TypescapeValue {
    /// Construct `Null`.
    #[must_use]
    pub fn null() -> Self {
        Self::Null
    }

    /// Construct `Bool(b)`.
    #[must_use]
    pub fn bool(b: bool) -> Self {
        Self::Bool(b)
    }

    /// Construct `Int(i)`.
    #[must_use]
    pub fn int(i: i64) -> Self {
        Self::Int(i)
    }

    /// Construct `Float(f)`.
    #[must_use]
    pub fn float(f: f64) -> Self {
        Self::Float(f)
    }

    /// Construct `String(s)` from any `Into<Arc<str>>`.
    pub fn string(s: impl Into<TypescapeString>) -> Self {
        Self::String(s.into())
    }

    /// Construct `Path(s)` from any `Into<Arc<str>>`.
    pub fn path(s: impl Into<TypescapeString>) -> Self {
        Self::Path(s.into())
    }

    /// Construct `List(items)` from any iterable of values.
    pub fn list(items: impl IntoIterator<Item = TypescapeValue>) -> Self {
        let v: Vec<TypescapeValue> = items.into_iter().collect();
        Self::List(Arc::from(v))
    }

    /// Construct `Attrs(map)` from any iterable of (key, value) pairs.
    /// Keys are sorted by `BTreeMap` for deterministic identity.
    pub fn attrs(
        pairs: impl IntoIterator<Item = (impl Into<TypescapeString>, TypescapeValue)>,
    ) -> Self {
        let map: BTreeMap<TypescapeString, TypescapeValue> =
            pairs.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Self::Attrs(Arc::new(map))
    }
}

// ── Accessors (sui parity: as_int / as_string / as_attrs …) ──────

impl TypescapeValue {
    /// Variant tag for error messages. Mirrors sui's diagnostic shape.
    #[must_use]
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Path(_) => "path",
            Self::List(_) => "list",
            Self::Attrs(_) => "attrs",
        }
    }

    /// Cast to `bool` or raise `VariantMismatch`.
    pub fn as_bool(&self) -> Result<bool, TypescapeError> {
        if let Self::Bool(b) = self {
            Ok(*b)
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "bool",
                got: self.tag(),
            })
        }
    }

    /// Cast to `i64` or raise `VariantMismatch`.
    pub fn as_int(&self) -> Result<i64, TypescapeError> {
        if let Self::Int(i) = self {
            Ok(*i)
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "int",
                got: self.tag(),
            })
        }
    }

    /// Cast to `f64` or raise `VariantMismatch`.
    pub fn as_float(&self) -> Result<f64, TypescapeError> {
        if let Self::Float(f) = self {
            Ok(*f)
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "float",
                got: self.tag(),
            })
        }
    }

    /// Borrow as `&str` or raise `VariantMismatch`.
    pub fn as_str(&self) -> Result<&str, TypescapeError> {
        if let Self::String(s) = self {
            Ok(s.as_ref())
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "string",
                got: self.tag(),
            })
        }
    }

    /// Borrow as `&str` (path payload) or raise `VariantMismatch`.
    pub fn as_path(&self) -> Result<&str, TypescapeError> {
        if let Self::Path(s) = self {
            Ok(s.as_ref())
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "path",
                got: self.tag(),
            })
        }
    }

    /// Borrow as `&[TypescapeValue]` or raise `VariantMismatch`.
    pub fn as_list(&self) -> Result<&[TypescapeValue], TypescapeError> {
        if let Self::List(l) = self {
            Ok(l.as_ref())
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "list",
                got: self.tag(),
            })
        }
    }

    /// Borrow as `&BTreeMap<...>` or raise `VariantMismatch`.
    pub fn as_attrs(&self) -> Result<&BTreeMap<TypescapeString, TypescapeValue>, TypescapeError> {
        if let Self::Attrs(a) = self {
            Ok(a.as_ref())
        } else {
            Err(TypescapeError::VariantMismatch {
                expected: "attrs",
                got: self.tag(),
            })
        }
    }

    /// Look up a required key in an Attrs, raising `MissingAttr` if
    /// absent or `VariantMismatch` if self is not an Attrs.
    pub fn attr(&self, key: &str) -> Result<&TypescapeValue, TypescapeError> {
        self.as_attrs()?
            .get(key)
            .ok_or_else(|| TypescapeError::MissingAttr(key.to_string()))
    }
}

// `Send + Sync` is automatic — every payload uses `Arc<...>` and
// `BTreeMap`, both Send+Sync. Compile-time check below.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TypescapeValue>();
};
