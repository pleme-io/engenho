//! `PatchAlgorithm` — the typed-spec + interpreter triplet for the three
//! implemented Kubernetes patch algorithms (and the typed-deferred fourth).
//!
//! ## Why this module exists
//!
//! Before this module, [`crate::state::ResourceCatalog::apply_patch`]
//! unconditionally ran an RFC 7396 JSON Merge Patch (`merge_json`) over the
//! existing object for EVERY patch — regardless of the Content-Type the
//! client requested. That silently corrupted:
//!
//!   * `kubectl patch --type=json` — a `{"op":"remove",…}` object got
//!     deep-merged into the resource instead of executing the op.
//!   * `kubectl set image` — the whole `containers` list was replaced rather
//!     than the single named container being patched (strategic list-merge).
//!   * `kubectl apply` / `kubectl edit` (default = strategic) — list fields
//!     merged wrong.
//!
//! This module is the **load-bearing fix**: a typed interpreter
//! [`apply`] that dispatches on a typed [`PatchType`] discriminant carried
//! through the store command, and an [`Environment`-trait](PatchSchemaEnv)
//! that abstracts the ONE side effect strategic-merge needs — the per-list
//! schema lookup `(gvk, json_path) → ListMergeStrategy`.
//!
//! ## The triplet (★★ TYPED-SPEC + INTERPRETER TRIPLET)
//!
//! 1. **Typed border** — [`engenho_types::patch::PatchType`] (the type-only
//!    discriminant of `Patch`) + the strongly-typed [`JsonPatchOp`] list
//!    (already defined) + the typed [`ListMergeStrategy`] / [`PatchDirective`]
//!    / [`PatchError`] in this module.
//! 2. **Authored Lisp spec** — TYPED FOLLOW-UP. engenho's store/types crates
//!    do not depend on tatara-lisp; bolting a lisp engine into the hot patch
//!    path is explicitly out of scope. `specs/patch.lisp` + a
//!    `(defpatch-algorithm …)` form (modeled on engenho-fonte's
//!    feature-gated `with-tatara-lisp` precedent and sui-spec's per-domain
//!    `Spec` trait) is the third leg owed — it would author the algorithms'
//!    phases as Lisp data + drive THIS interpreter behind a feature, never in
//!    the store's runtime dep graph.
//! 3. **Working interpreter** — [`apply`], below. Pure for Merge/Json
//!    (no env touch); Strategic consults the env for each list field.
//!
//! No silent wrong answers: an unimplemented surface (server-side apply)
//! returns [`PatchError::Unsupported`], never a wrong `Ok` / panic / `todo!`.

use std::collections::HashMap;

use serde_json::Value;

use engenho_types::patch::{JsonPatchOp, PatchType};

// ─── typed border: gvk + strategy + directive + error ──────────────────────

/// Group/Version/Kind of the object being patched — the schema-lookup key the
/// strategic-merge algorithm passes to [`PatchSchemaEnv::merge_strategy`].
///
/// A local lightweight value (NOT the `engenho-kube-proto` `Gvk`) so the
/// store crate keeps a clean dependency graph — the interpreter only needs
/// the three identity strings, which are already present on
/// [`crate::resource::ResourceKey`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Gvk {
    /// API group; empty string for core/v1.
    pub group: String,
    /// API version, e.g. `"v1"`.
    pub version: String,
    /// Kind, e.g. `"Deployment"`.
    pub kind: String,
}

impl Gvk {
    /// Construct from the three identity strings.
    #[must_use]
    pub fn new(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        }
    }
}

impl From<&crate::resource::ResourceKey> for Gvk {
    fn from(k: &crate::resource::ResourceKey) -> Self {
        Self {
            group: k.group.clone(),
            version: k.version.clone(),
            kind: k.kind.clone(),
        }
    }
}

/// A dotted/indexed JSON path identifying a field within an object — the
/// second half of the strategic-merge schema-lookup key. Built up as the
/// strategic-merge recursion descends through object keys (array indices are
/// NOT recorded — list-merge strategy is a property of the list FIELD, not of
/// any element).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct JsonPath(Vec<String>);

impl JsonPath {
    /// The empty (root) path.
    #[must_use]
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// A path from dotted segments, e.g. `"spec.template.spec.containers"`.
    #[must_use]
    pub fn from_dotted(s: &str) -> Self {
        if s.is_empty() {
            return Self::root();
        }
        Self(s.split('.').map(str::to_string).collect())
    }

    /// Return a NEW path with `seg` appended (non-mutating — the recursion
    /// keeps the parent path intact for sibling fields).
    #[must_use]
    pub fn child(&self, seg: &str) -> Self {
        let mut v = self.0.clone();
        v.push(seg.to_string());
        Self(v)
    }

    /// The path segments.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    /// The dotted rendering, e.g. `"spec.template.spec.containers"`.
    #[must_use]
    pub fn dotted(&self) -> String {
        self.0.join(".")
    }
}

/// How strategic-merge combines two lists at a given field — the typed
/// resolution of the OpenAPI `x-kubernetes-*` list extensions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListMergeStrategy {
    /// `x-kubernetes-list-type: atomic` (or no merge metadata) — the patch
    /// list REPLACES the whole existing list. (e.g. `tolerations`.)
    Replace,
    /// `x-kubernetes-patch-strategy: merge` + `x-kubernetes-list-map-keys` /
    /// `x-kubernetes-patch-merge-key` — list elements are objects merged by
    /// the named key(s). New keys append; existing keys deep-merge. (THE
    /// `kubectl set image` case: `spec.template.spec.containers` by `name`.)
    MergeByKey(Vec<String>),
    /// `x-kubernetes-patch-strategy: merge,retainKeys` — same as
    /// [`Self::MergeByKey`] but the merge honors a `$retainKeys` directive
    /// (e.g. `volumes`).
    MergeByKeyRetainKeys(Vec<String>),
    /// `x-kubernetes-list-type: set` — set-union of primitive elements (no
    /// duplicates). (e.g. `metadata.finalizers`.)
    Set,
}

/// A strategic-merge `$patch` directive sentinel — the in-band instructions a
/// patch carries to control list/map merge behavior beyond the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatchDirective {
    /// `$patch: replace` — replace the target wholesale (don't merge).
    Replace,
    /// `$patch: delete` — delete the matched list element / map.
    Delete,
    /// `$patch: merge` — explicit merge (the default for maps anyway).
    Merge,
    /// `$patch: delete` applied to a primitive-list element via
    /// `$deleteFromPrimitiveList/<field>`.
    DeleteFromPrimitiveList,
}

impl PatchDirective {
    /// Parse a `$patch` sentinel value.
    fn from_value(v: &Value) -> Option<Self> {
        match v.as_str()? {
            "replace" => Some(Self::Replace),
            "delete" => Some(Self::Delete),
            "merge" => Some(Self::Merge),
            _ => None,
        }
    }
}

/// A typed failure from the patch interpreter. NO silent wrong answers — every
/// surface that can't honor the patch returns one of these (never a wrong `Ok`,
/// never a panic / `todo!` / `unimplemented!`).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PatchError {
    /// An RFC 7396 merge surface failed (currently unreachable — merge is
    /// total — but reserved so a future stricter merge has a typed home).
    #[error("merge patch failed: {0}")]
    RfcMerge(String),
    /// An RFC 6902 JSON-Pointer resolution failed — `remove`/`replace` on a
    /// path that doesn't exist, or a malformed pointer.
    #[error("json patch: bad pointer {path:?}")]
    JsonPointer { path: String },
    /// An RFC 6902 `test` op compared unequal — the WHOLE document is
    /// rejected (atomicity), no partial application.
    #[error("json patch: test failed at {path:?}")]
    TestFailed { path: String },
    /// A strategic-merge list field declared a merge-key the patch element
    /// lacks — the element can't be matched into the existing list.
    #[error("strategic merge: list at {path:?} is missing merge key {key:?}")]
    MissingMergeKey { path: String, key: String },
    /// A `$patch` sentinel value the interpreter doesn't recognize.
    #[error("strategic merge: bad $patch directive {raw:?}")]
    BadDirective { raw: String },
    /// A recognized-but-unimplemented patch surface — server-side apply.
    /// Maps to a typed 415/422 at the apiserver boundary, NEVER a silent
    /// strategic/merge fallback.
    #[error("unsupported patch: {what}")]
    Unsupported { what: String },
}

// ─── the Environment trait (the testability contract) ──────────────────────

/// The Environment trait for the strategic-merge algorithm — abstracts the
/// ONE side effect strategic-merge needs: the per-list-field schema lookup.
///
/// Per ★★ TYPED-SPEC + INTERPRETER TRIPLET: the trait IS the testability
/// contract. The interpreter is otherwise pure (current + patch in, `Value`
/// out), so RFC7386-merge and RFC6902-json-patch don't even touch the env —
/// only strategic does. Tests inject a [`MockPatchEnv`] pre-seeded with
/// strategies so the algorithm is unit-tested with ZERO OpenAPI parsing,
/// ZERO network, ZERO filesystem.
pub trait PatchSchemaEnv: Send + Sync {
    /// Resolve the strategic-merge list strategy for a list field at
    /// `json_path` within the object of kind `gvk`. Returns the typed
    /// [`ListMergeStrategy`]. When the field/schema isn't found the impl
    /// returns [`ListMergeStrategy::Replace`] (atomic) — the conservative
    /// K8s default for a list with no merge metadata.
    fn merge_strategy(&self, gvk: &Gvk, json_path: &JsonPath) -> ListMergeStrategy;
}

// ─── the typed patch payload ───────────────────────────────────────────────

/// The typed patch payload handed to [`apply`] — a typed pair of the
/// algorithm and its strongly-typed body. For `Json` the payload is the
/// already-typed `Vec<JsonPatchOp>`; for `Merge`/`Strategic` a raw `Value`
/// (these algorithms interpret the patch structurally, not as an op list);
/// `Apply` carries the declared body (interpreter rejects it as deferred).
#[derive(Clone, Debug)]
pub enum PatchBody {
    /// RFC 7396 merge patch body.
    Merge(Value),
    /// Strategic merge patch body.
    Strategic(Value),
    /// RFC 6902 typed op list.
    Json(Vec<JsonPatchOp>),
    /// Server-side apply declared body (typed-deferred).
    Apply(Value),
}

impl PatchBody {
    /// Decode a raw patch `Value` (as decoded at the apiserver boundary) into
    /// the typed [`PatchBody`] for the given [`PatchType`]. For `Json` the
    /// raw value is the op array, deserialized into the typed
    /// `Vec<JsonPatchOp>`; a malformed op array is a typed
    /// [`PatchError::BadDirective`].
    ///
    /// # Errors
    ///
    /// [`PatchError::BadDirective`] if a json-patch body isn't a valid
    /// RFC 6902 op array.
    pub fn from_raw(patch_type: PatchType, raw: Value) -> Result<Self, PatchError> {
        Ok(match patch_type {
            PatchType::Merge => Self::Merge(raw),
            PatchType::Strategic => Self::Strategic(raw),
            PatchType::Apply => Self::Apply(raw),
            PatchType::Json => {
                let ops: Vec<JsonPatchOp> =
                    serde_json::from_value(raw).map_err(|e| PatchError::BadDirective {
                        raw: format!("invalid RFC6902 op array: {e}"),
                    })?;
                Self::Json(ops)
            }
        })
    }
}

// ─── the interpreter ───────────────────────────────────────────────────────

/// Apply a typed patch to `current`, returning the new object.
///
/// Walks the typed phases per [`PatchType`]:
///
///   * [`PatchType::Merge`] → RFC 7396 (recursive object merge, `null`
///     deletes, arrays replace whole, non-object replaces).
///   * [`PatchType::Json`] → RFC 6902 (fold the typed ops over a JSON-Pointer
///     engine; `test` failure → whole-document rejection; missing pointer on
///     remove/replace → [`PatchError::JsonPointer`]).
///   * [`PatchType::Strategic`] → object-merge that, at each list field,
///     consults `env.merge_strategy(gvk, json_path)` + honors `$patch`
///     directives.
///   * [`PatchType::Apply`] → [`PatchError::Unsupported`] (typed-deferred SSA).
///
/// # Errors
///
/// Any [`PatchError`] from the algorithm — never a silent wrong `Ok`.
pub fn apply(
    patch_type: PatchType,
    current: &Value,
    patch: &PatchBody,
    env: &dyn PatchSchemaEnv,
    gvk: &Gvk,
) -> Result<Value, PatchError> {
    match (patch_type, patch) {
        (PatchType::Merge, PatchBody::Merge(p)) => {
            let mut out = current.clone();
            merge_rfc7396(&mut out, p);
            Ok(out)
        }
        (PatchType::Json, PatchBody::Json(ops)) => apply_json_patch(current, ops),
        (PatchType::Strategic, PatchBody::Strategic(p)) => {
            strategic_merge(current, p, env, gvk, &JsonPath::root())
        }
        (PatchType::Apply, _) => Err(PatchError::Unsupported {
            what: "server-side apply".to_string(),
        }),
        // The (type, body) pair must agree — a mismatch is a construction
        // bug at the boundary, surfaced as a typed error rather than a wrong
        // merge.
        (pt, _) => Err(PatchError::Unsupported {
            what: format!("patch type {pt:?} with mismatched body"),
        }),
    }
}

// ─── RFC 7396: JSON Merge Patch ────────────────────────────────────────────

/// Recursive JSON merge — fields in `patch` overwrite, `null` in patch
/// deletes a key, a non-object patch replaces the target, arrays replace
/// whole. The canonical RFC 7396 (JSON Merge Patch) implementation for the
/// fleet.
///
/// This is the same algorithm the store's `apply_patch` ran unconditionally
/// before this module existed — moved here as the canonical impl and now used
/// ONLY for the `Merge` arm (the other algorithms have correct semantics).
pub fn merge_rfc7396(target: &mut Value, patch: &Value) {
    if !patch.is_object() {
        *target = patch.clone();
        return;
    }
    if !target.is_object() {
        *target = Value::Object(serde_json::Map::new());
    }
    let target_obj = target.as_object_mut().expect("target made object above");
    for (k, v) in patch.as_object().expect("patch is object") {
        if v.is_null() {
            target_obj.remove(k);
        } else {
            let entry = target_obj.entry(k.clone()).or_insert(Value::Null);
            merge_rfc7396(entry, v);
        }
    }
}

// ─── RFC 6902: JSON Patch ──────────────────────────────────────────────────

/// Apply an RFC 6902 op list to `current`, atomically: the ops are folded
/// over a CLONE of `current`, and the clone is returned only if EVERY op
/// succeeds. A `test` mismatch or a bad pointer rejects the whole document.
fn apply_json_patch(current: &Value, ops: &[JsonPatchOp]) -> Result<Value, PatchError> {
    let mut doc = current.clone();
    for op in ops {
        apply_one_op(&mut doc, op)?;
    }
    Ok(doc)
}

fn apply_one_op(doc: &mut Value, op: &JsonPatchOp) -> Result<(), PatchError> {
    match op {
        JsonPatchOp::Add { path, value } => pointer_add(doc, path, value.clone()),
        JsonPatchOp::Remove { path } => pointer_remove(doc, path).map(|_| ()),
        JsonPatchOp::Replace { path, value } => {
            // RFC 6902: replace fails if the target location does not exist.
            pointer_get(doc, path).ok_or_else(|| PatchError::JsonPointer { path: path.clone() })?;
            pointer_remove(doc, path)?;
            pointer_add(doc, path, value.clone())
        }
        JsonPatchOp::Copy { from, path } => {
            let v = pointer_get(doc, from)
                .ok_or_else(|| PatchError::JsonPointer { path: from.clone() })?
                .clone();
            pointer_add(doc, path, v)
        }
        JsonPatchOp::Move { from, path } => {
            let v = pointer_remove(doc, from)?;
            pointer_add(doc, path, v)
        }
        JsonPatchOp::Test { path, value } => {
            let actual = pointer_get(doc, path)
                .ok_or_else(|| PatchError::TestFailed { path: path.clone() })?;
            if actual == value {
                Ok(())
            } else {
                Err(PatchError::TestFailed { path: path.clone() })
            }
        }
    }
}

/// Parse an RFC 6901 JSON Pointer into its decoded reference tokens.
/// `""` → no tokens (whole document). `"/a/b"` → `["a", "b"]`. `~1` → `/`,
/// `~0` → `~`.
fn pointer_tokens(path: &str) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }
    path.split('/')
        .skip(1) // leading "" before first '/'
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Resolve a JSON Pointer to a shared ref into `doc`.
fn pointer_get<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let tokens = pointer_tokens(path);
    let mut cur = doc;
    for t in &tokens {
        cur = match cur {
            Value::Object(m) => m.get(t)?,
            Value::Array(a) => {
                let idx: usize = t.parse().ok()?;
                a.get(idx)?
            }
            _ => return None,
        };
    }
    Some(cur)
}

/// Insert/replace at a JSON Pointer (RFC 6902 `add` semantics — into an
/// object key, or into an array at an index / `-` append).
fn pointer_add(doc: &mut Value, path: &str, value: Value) -> Result<(), PatchError> {
    let tokens = pointer_tokens(path);
    if tokens.is_empty() {
        *doc = value;
        return Ok(());
    }
    let (last, parents) = tokens.split_last().expect("non-empty checked above");
    let mut cur = doc;
    for t in parents {
        cur = match cur {
            Value::Object(m) => m.get_mut(t).ok_or_else(|| PatchError::JsonPointer {
                path: path.to_string(),
            })?,
            Value::Array(a) => {
                let idx: usize = t.parse().map_err(|_| PatchError::JsonPointer {
                    path: path.to_string(),
                })?;
                a.get_mut(idx).ok_or_else(|| PatchError::JsonPointer {
                    path: path.to_string(),
                })?
            }
            _ => {
                return Err(PatchError::JsonPointer {
                    path: path.to_string(),
                });
            }
        };
    }
    match cur {
        Value::Object(m) => {
            m.insert(last.clone(), value);
            Ok(())
        }
        Value::Array(a) => {
            if last == "-" {
                a.push(value);
                Ok(())
            } else {
                let idx: usize = last.parse().map_err(|_| PatchError::JsonPointer {
                    path: path.to_string(),
                })?;
                if idx > a.len() {
                    return Err(PatchError::JsonPointer {
                        path: path.to_string(),
                    });
                }
                a.insert(idx, value);
                Ok(())
            }
        }
        _ => Err(PatchError::JsonPointer {
            path: path.to_string(),
        }),
    }
}

/// Remove the value at a JSON Pointer, returning it. Errors if the location
/// doesn't exist.
fn pointer_remove(doc: &mut Value, path: &str) -> Result<Value, PatchError> {
    let tokens = pointer_tokens(path);
    if tokens.is_empty() {
        // Removing the whole document — replace with Null, return old.
        return Ok(std::mem::replace(doc, Value::Null));
    }
    let (last, parents) = tokens.split_last().expect("non-empty checked above");
    let mut cur = doc;
    for t in parents {
        cur = match cur {
            Value::Object(m) => m.get_mut(t).ok_or_else(|| PatchError::JsonPointer {
                path: path.to_string(),
            })?,
            Value::Array(a) => {
                let idx: usize = t.parse().map_err(|_| PatchError::JsonPointer {
                    path: path.to_string(),
                })?;
                a.get_mut(idx).ok_or_else(|| PatchError::JsonPointer {
                    path: path.to_string(),
                })?
            }
            _ => {
                return Err(PatchError::JsonPointer {
                    path: path.to_string(),
                });
            }
        };
    }
    match cur {
        Value::Object(m) => m.remove(last).ok_or_else(|| PatchError::JsonPointer {
            path: path.to_string(),
        }),
        Value::Array(a) => {
            let idx: usize = last.parse().map_err(|_| PatchError::JsonPointer {
                path: path.to_string(),
            })?;
            if idx >= a.len() {
                return Err(PatchError::JsonPointer {
                    path: path.to_string(),
                });
            }
            Ok(a.remove(idx))
        }
        _ => Err(PatchError::JsonPointer {
            path: path.to_string(),
        }),
    }
}

// ─── Strategic Merge Patch ─────────────────────────────────────────────────

const PATCH_SENTINEL: &str = "$patch";
const RETAIN_KEYS_SENTINEL: &str = "$retainKeys";

/// Strategic-merge `patch` into `current` at object path `path`. Object keys
/// recurse; list keys consult the env for their [`ListMergeStrategy`]; the
/// `$patch` directive sentinels override the default.
///
/// `pub` because server-side apply ([`crate::ssa::apply_ssa`]) REUSES this
/// exact engine — the apply config IS a strategic-merge patch over current,
/// so SSA's merge step calls back here rather than forking a second merge
/// engine.
///
/// # Errors
///
/// Any [`PatchError`] from the recursion (a key-less merge-by-key element /
/// an unrecognized `$patch` directive). An apply config carries neither, so
/// SSA's call is effectively total.
pub fn strategic_merge(
    current: &Value,
    patch: &Value,
    env: &dyn PatchSchemaEnv,
    gvk: &Gvk,
    path: &JsonPath,
) -> Result<Value, PatchError> {
    // A non-object patch replaces wholesale (same as RFC7396 leaf).
    let Some(patch_obj) = patch.as_object() else {
        return Ok(patch.clone());
    };

    // `$patch: replace` at the object level → replace the whole object with
    // the patch (minus the directive).
    if let Some(dir) = patch_obj.get(PATCH_SENTINEL) {
        match PatchDirective::from_value(dir) {
            Some(PatchDirective::Replace) => {
                let mut cloned = patch_obj.clone();
                cloned.remove(PATCH_SENTINEL);
                return Ok(Value::Object(cloned));
            }
            // `$patch: merge` / `delete` on an object are handled by the
            // parent (delete) or are the default (merge); fall through.
            Some(_) => {}
            None => {
                return Err(PatchError::BadDirective {
                    raw: dir.to_string(),
                });
            }
        }
    }

    let mut out = current.as_object().cloned().unwrap_or_default();
    // `$retainKeys` controls which keys survive the merge (retainKeys
    // strategy). When present, keys NOT listed are dropped before the merge.
    let retain_keys: Option<Vec<String>> = patch_obj.get(RETAIN_KEYS_SENTINEL).and_then(|v| {
        v.as_array().map(|a| {
            a.iter()
                .filter_map(|e| e.as_str().map(str::to_string))
                .collect()
        })
    });
    if let Some(retain) = &retain_keys {
        out.retain(|k, _| retain.contains(k));
    }

    for (k, pv) in patch_obj {
        if k == PATCH_SENTINEL || k == RETAIN_KEYS_SENTINEL {
            continue;
        }
        let field_path = path.child(k);
        if pv.is_null() {
            // null deletes a scalar/object field (RFC7396-compatible leaf).
            out.remove(k);
            continue;
        }
        let existing = out.get(k);
        if pv.is_array() {
            // List field — consult the env for the merge strategy.
            let strat = env.merge_strategy(gvk, &field_path);
            let existing_list = existing
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let patch_list = pv.as_array().expect("checked is_array");
            let merged = merge_list(&existing_list, patch_list, &strat, env, gvk, &field_path)?;
            out.insert(k.clone(), Value::Array(merged));
        } else if pv.is_object() {
            let base = existing
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));
            let merged = strategic_merge(&base, pv, env, gvk, &field_path)?;
            out.insert(k.clone(), merged);
        } else {
            // Scalar — overwrite.
            out.insert(k.clone(), pv.clone());
        }
    }
    Ok(Value::Object(out))
}

/// Merge two lists per the resolved [`ListMergeStrategy`].
fn merge_list(
    existing: &[Value],
    patch: &[Value],
    strat: &ListMergeStrategy,
    env: &dyn PatchSchemaEnv,
    gvk: &Gvk,
    path: &JsonPath,
) -> Result<Vec<Value>, PatchError> {
    match strat {
        ListMergeStrategy::Replace => Ok(patch.to_vec()),
        ListMergeStrategy::Set => Ok(set_union(existing, patch)),
        ListMergeStrategy::MergeByKey(keys) | ListMergeStrategy::MergeByKeyRetainKeys(keys) => {
            merge_list_by_key(existing, patch, keys, env, gvk, path)
        }
    }
}

/// Set-union of primitive lists — existing elements first, then patch
/// elements not already present.
fn set_union(existing: &[Value], patch: &[Value]) -> Vec<Value> {
    let mut out = existing.to_vec();
    for p in patch {
        if !out.contains(p) {
            out.push(p.clone());
        }
    }
    out
}

/// The strategic list-merge-by-key heart — THE `kubectl set image` case.
///
/// Each patch element is matched against an existing element by the merge
/// key(s). A matched element is strategic-merged in place (only the named
/// container changes); an unmatched patch element is appended; a patch
/// element carrying `$patch: delete` removes the matched existing element.
fn merge_list_by_key(
    existing: &[Value],
    patch: &[Value],
    keys: &[String],
    env: &dyn PatchSchemaEnv,
    gvk: &Gvk,
    path: &JsonPath,
) -> Result<Vec<Value>, PatchError> {
    let key_of = |el: &Value| -> Option<Vec<Value>> {
        let obj = el.as_object()?;
        keys.iter().map(|k| obj.get(k).cloned()).collect()
    };

    // Seed the result with the existing elements (preserving order).
    let mut out: Vec<Value> = existing.to_vec();

    for pel in patch {
        // A `$patch: delete` element removes the matched existing element.
        let directive = pel
            .as_object()
            .and_then(|o| o.get(PATCH_SENTINEL))
            .and_then(PatchDirective::from_value);

        let Some(pkey) = key_of(pel) else {
            // Primitive element OR object missing the merge key: if the patch
            // element is a primitive treat the list as a set-union append
            // (defensive); if it's an object missing the key it's a typed
            // error (we can't position it).
            if pel.is_object() {
                let missing = keys
                    .iter()
                    .find(|k| pel.as_object().and_then(|o| o.get(*k)).is_none())
                    .cloned()
                    .unwrap_or_default();
                return Err(PatchError::MissingMergeKey {
                    path: path.dotted(),
                    key: missing,
                });
            }
            if !out.contains(pel) {
                out.push(pel.clone());
            }
            continue;
        };

        let matched_idx = out.iter().position(|e| key_of(e).as_ref() == Some(&pkey));
        match (directive, matched_idx) {
            (Some(PatchDirective::Delete), Some(idx)) => {
                out.remove(idx);
            }
            (Some(PatchDirective::Delete), None) => {
                // delete of a non-existent element — no-op.
            }
            (_, Some(idx)) => {
                // Strategic-merge the patch element INTO the matched existing
                // element (recurse — only the named element changes).
                let merged = strategic_merge(&out[idx], pel, env, gvk, path)?;
                out[idx] = merged;
            }
            (_, None) => {
                // New element — append (minus any $patch sentinel).
                let mut cloned = pel.clone();
                if let Some(o) = cloned.as_object_mut() {
                    o.remove(PATCH_SENTINEL);
                }
                out.push(cloned);
            }
        }
    }
    Ok(out)
}

// ─── OpenAPI-backed real impl ──────────────────────────────────────────────

/// The production [`PatchSchemaEnv`] — resolves strategies from the vendored,
/// BLAKE3-pinned Kubernetes OpenAPI v3 documents (the SINGLE source of truth
/// per the Compounding Directive; a hand-curated strategy table would be a
/// second source that drifts).
///
/// Built once at store/apiserver boot (the documents are `&'static`
/// `include_str!` so there's no FS read at runtime). The per-`(gvk, path)`
/// lookup is cached so repeated patches don't re-walk the schema.
pub struct OpenApiPatchEnv {
    /// Cache of resolved strategies, keyed by `(gvk, dotted-path)`.
    cache: std::sync::Mutex<HashMap<(Gvk, String), ListMergeStrategy>>,
}

impl Default for OpenApiPatchEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenApiPatchEnv {
    /// Construct the OpenAPI-backed env (empty cache).
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Walk the vendored OpenAPI v3 schema for `gvk` along `json_path` and
    /// resolve the list strategy at the target field. Returns
    /// [`ListMergeStrategy::Replace`] (the conservative atomic default) when
    /// the schema/field isn't found.
    fn resolve(&self, gvk: &Gvk, json_path: &JsonPath) -> ListMergeStrategy {
        resolve_from_openapi(gvk, json_path).unwrap_or(ListMergeStrategy::Replace)
    }
}

impl PatchSchemaEnv for OpenApiPatchEnv {
    fn merge_strategy(&self, gvk: &Gvk, json_path: &JsonPath) -> ListMergeStrategy {
        let cache_key = (gvk.clone(), json_path.dotted());
        if let Ok(cache) = self.cache.lock() {
            if let Some(hit) = cache.get(&cache_key) {
                return hit.clone();
            }
        }
        let strat = self.resolve(gvk, json_path);
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(cache_key, strat.clone());
        }
        strat
    }
}

/// Resolve a list strategy by walking the vendored OpenAPI v3 document for
/// `gvk` along `json_path`. `None` when no document/schema/field is found.
fn resolve_from_openapi(gvk: &Gvk, json_path: &JsonPath) -> Option<ListMergeStrategy> {
    let doc_body = engenho_types::openapi_v3::document_for(&gvk.group, &gvk.version)?;
    let doc: Value = serde_json::from_str(doc_body).ok()?;
    let schemas = doc.get("components")?.get("schemas")?.as_object()?;
    let root_name = find_schema_name(schemas, gvk)?;
    let root = schemas.get(&root_name)?;

    // Walk each path segment through properties + $ref + items, reading the
    // x-kubernetes-* extensions at the FINAL segment.
    let mut node = root;
    let segs = json_path.segments();
    for (i, seg) in segs.iter().enumerate() {
        let resolved = deref(schemas, node)?;
        let props = resolved.get("properties")?.as_object()?;
        let field = props.get(seg)?;
        if i + 1 == segs.len() {
            // Final segment — read the list extensions HERE (on the field
            // schema, before deref, since x-kubernetes-* live on the array
            // property node).
            return strategy_from_field(field);
        }
        // Intermediate segment — descend into the field's (deref'd) schema.
        node = field;
    }
    None
}

/// Read the `x-kubernetes-*` list extensions on a list-field schema node and
/// resolve the typed [`ListMergeStrategy`].
fn strategy_from_field(field: &Value) -> Option<ListMergeStrategy> {
    let list_type = field.get("x-kubernetes-list-type").and_then(Value::as_str);
    let patch_strategy = field
        .get("x-kubernetes-patch-strategy")
        .and_then(Value::as_str);

    // Merge keys: prefer list-map-keys (the modern form); fall back to the
    // single patch-merge-key.
    let keys: Vec<String> = field
        .get("x-kubernetes-list-map-keys")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| e.as_str().map(str::to_string))
                .collect()
        })
        .or_else(|| {
            field
                .get("x-kubernetes-patch-merge-key")
                .and_then(Value::as_str)
                .map(|k| vec![k.to_string()])
        })
        .unwrap_or_default();

    // The patch-strategy string is the authority: `merge` / `merge,retainKeys`
    // vs absent (atomic). list-type `set` is a primitive-set union.
    match (patch_strategy, list_type) {
        (Some(ps), _) if ps.contains("retainKeys") && !keys.is_empty() => {
            Some(ListMergeStrategy::MergeByKeyRetainKeys(keys))
        }
        (Some(ps), _) if ps.contains("merge") && !keys.is_empty() => {
            Some(ListMergeStrategy::MergeByKey(keys))
        }
        (_, Some("set")) => Some(ListMergeStrategy::Set),
        (_, Some("map")) if !keys.is_empty() => Some(ListMergeStrategy::MergeByKey(keys)),
        // atomic, or a list with no usable merge metadata → replace whole.
        _ => Some(ListMergeStrategy::Replace),
    }
}

/// Resolve `allOf`/`$ref` indirection one level — return the concrete schema
/// node (with `properties`). Handles the common `{"allOf":[{"$ref":…}]}`
/// shape the vendored docs use, a bare `{"$ref":…}`, and an already-concrete
/// node.
fn deref<'a>(schemas: &'a serde_json::Map<String, Value>, node: &'a Value) -> Option<&'a Value> {
    if node.get("properties").is_some() {
        return Some(node);
    }
    if let Some(r) = node.get("$ref").and_then(Value::as_str) {
        let name = r.rsplit('/').next()?;
        return deref(schemas, schemas.get(name)?);
    }
    if let Some(all) = node.get("allOf").and_then(Value::as_array) {
        for sub in all {
            if let Some(r) = sub.get("$ref").and_then(Value::as_str) {
                let name = r.rsplit('/').next()?;
                if let Some(s) = schemas.get(name) {
                    return deref(schemas, s);
                }
            } else if sub.get("properties").is_some() {
                return Some(sub);
            }
        }
    }
    // An array field: descend into items (used when a path segment crosses a
    // list — not the common case for the patch paths we resolve).
    if let Some(items) = node.get("items") {
        return deref(schemas, items);
    }
    None
}

/// Find the schema component name for `gvk` via its
/// `x-kubernetes-group-version-kind` metadata.
fn find_schema_name(schemas: &serde_json::Map<String, Value>, gvk: &Gvk) -> Option<String> {
    for (name, schema) in schemas {
        let Some(gvks) = schema
            .get("x-kubernetes-group-version-kind")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for g in gvks {
            let group = g.get("group").and_then(Value::as_str).unwrap_or("");
            let version = g.get("version").and_then(Value::as_str).unwrap_or("");
            let kind = g.get("kind").and_then(Value::as_str).unwrap_or("");
            if group == gvk.group && version == gvk.version && kind == gvk.kind {
                return Some(name.clone());
            }
        }
    }
    None
}

// ─── Mock impl for tests ───────────────────────────────────────────────────

/// A test [`PatchSchemaEnv`] backed by a pre-seeded `(gvk, path) → strategy`
/// map — the Environment-trait testability contract. Lets the strategic-merge
/// algorithm be unit-tested with ZERO OpenAPI parsing, network, or FS. A
/// lookup miss returns the seeded `default` (commonly
/// [`ListMergeStrategy::Replace`] so unmodeled lists are atomic).
#[derive(Default)]
pub struct MockPatchEnv {
    map: HashMap<(Gvk, String), ListMergeStrategy>,
    default: Option<ListMergeStrategy>,
}

impl MockPatchEnv {
    /// Empty mock — every lookup returns [`ListMergeStrategy::Replace`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A mock that returns `default` for every unseeded lookup.
    #[must_use]
    pub fn with_default(default: ListMergeStrategy) -> Self {
        Self {
            map: HashMap::new(),
            default: Some(default),
        }
    }

    /// Seed `(gvk, dotted-path) → strategy`.
    #[must_use]
    pub fn seed(mut self, gvk: Gvk, dotted_path: &str, strat: ListMergeStrategy) -> Self {
        self.map.insert((gvk, dotted_path.to_string()), strat);
        self
    }
}

impl PatchSchemaEnv for MockPatchEnv {
    fn merge_strategy(&self, gvk: &Gvk, json_path: &JsonPath) -> ListMergeStrategy {
        self.map
            .get(&(gvk.clone(), json_path.dotted()))
            .cloned()
            .or_else(|| self.default.clone())
            .unwrap_or(ListMergeStrategy::Replace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn deploy_gvk() -> Gvk {
        Gvk::new("apps", "v1", "Deployment")
    }

    // ── RFC 7396 Merge ──────────────────────────────────────────────────

    fn merge(current: Value, patch: Value) -> Value {
        let env = MockPatchEnv::new();
        apply(
            PatchType::Merge,
            &current,
            &PatchBody::Merge(patch),
            &env,
            &deploy_gvk(),
        )
        .unwrap()
    }

    #[test]
    fn merge_scalar_replace() {
        let out = merge(
            json!({"spec": {"replicas": 1}}),
            json!({"spec": {"replicas": 3}}),
        );
        assert_eq!(out["spec"]["replicas"], 3);
    }

    #[test]
    fn merge_null_deletes_key() {
        let out = merge(json!({"a": 1, "b": 2}), json!({"b": null}));
        assert_eq!(out, json!({"a": 1}));
    }

    #[test]
    fn merge_preserves_unspecified_keys() {
        let out = merge(json!({"a": 1, "b": 2}), json!({"a": 9}));
        assert_eq!(out, json!({"a": 9, "b": 2}));
    }

    #[test]
    fn merge_array_replaces_whole() {
        let out = merge(json!({"l": [1, 2, 3]}), json!({"l": [9]}));
        assert_eq!(out["l"], json!([9]));
    }

    #[test]
    fn merge_nested_object() {
        let out = merge(
            json!({"meta": {"a": 1, "b": 2}}),
            json!({"meta": {"b": 3, "c": 4}}),
        );
        assert_eq!(out["meta"], json!({"a": 1, "b": 3, "c": 4}));
    }

    // ── RFC 6902 JSON Patch ─────────────────────────────────────────────

    fn json_patch(current: Value, ops: Vec<JsonPatchOp>) -> Result<Value, PatchError> {
        let env = MockPatchEnv::new();
        apply(
            PatchType::Json,
            &current,
            &PatchBody::Json(ops),
            &env,
            &deploy_gvk(),
        )
    }

    #[test]
    fn json_add() {
        let out = json_patch(
            json!({"spec": {}}),
            vec![JsonPatchOp::Add {
                path: "/spec/replicas".into(),
                value: json!(5),
            }],
        )
        .unwrap();
        assert_eq!(out["spec"]["replicas"], 5);
    }

    #[test]
    fn json_remove() {
        let out = json_patch(
            json!({"metadata": {"labels": {"env": "prod", "team": "x"}}}),
            vec![JsonPatchOp::Remove {
                path: "/metadata/labels/env".into(),
            }],
        )
        .unwrap();
        assert_eq!(out["metadata"]["labels"], json!({"team": "x"}));
    }

    #[test]
    fn json_replace() {
        let out = json_patch(
            json!({"spec": {"replicas": 1}}),
            vec![JsonPatchOp::Replace {
                path: "/spec/replicas".into(),
                value: json!(5),
            }],
        )
        .unwrap();
        assert_eq!(out["spec"]["replicas"], 5);
    }

    #[test]
    fn json_move() {
        let out = json_patch(
            json!({"a": 1, "b": 2}),
            vec![JsonPatchOp::Move {
                from: "/a".into(),
                path: "/c".into(),
            }],
        )
        .unwrap();
        assert_eq!(out, json!({"b": 2, "c": 1}));
    }

    #[test]
    fn json_copy() {
        let out = json_patch(
            json!({"a": 1}),
            vec![JsonPatchOp::Copy {
                from: "/a".into(),
                path: "/b".into(),
            }],
        )
        .unwrap();
        assert_eq!(out, json!({"a": 1, "b": 1}));
    }

    #[test]
    fn json_test_success_applies() {
        let out = json_patch(
            json!({"a": 1}),
            vec![
                JsonPatchOp::Test {
                    path: "/a".into(),
                    value: json!(1),
                },
                JsonPatchOp::Replace {
                    path: "/a".into(),
                    value: json!(2),
                },
            ],
        )
        .unwrap();
        assert_eq!(out["a"], 2);
    }

    #[test]
    fn json_test_failure_rejects_whole_document() {
        // The test fails → the WHOLE doc is rejected (atomic); the prior
        // replace MUST NOT have committed.
        let err = json_patch(
            json!({"a": 1}),
            vec![
                JsonPatchOp::Replace {
                    path: "/a".into(),
                    value: json!(99),
                },
                JsonPatchOp::Test {
                    path: "/a".into(),
                    value: json!(1), // 1 != 99 → fail
                },
            ],
        )
        .unwrap_err();
        assert_eq!(err, PatchError::TestFailed { path: "/a".into() });
    }

    #[test]
    fn json_remove_missing_pointer_errors() {
        let err = json_patch(
            json!({"a": 1}),
            vec![JsonPatchOp::Remove {
                path: "/nope".into(),
            }],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PatchError::JsonPointer {
                path: "/nope".into()
            }
        );
    }

    #[test]
    fn json_replace_missing_pointer_errors() {
        let err = json_patch(
            json!({"a": 1}),
            vec![JsonPatchOp::Replace {
                path: "/nope".into(),
                value: json!(2),
            }],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PatchError::JsonPointer {
                path: "/nope".into()
            }
        );
    }

    #[test]
    fn json_add_to_array_append() {
        let out = json_patch(
            json!({"l": [1, 2]}),
            vec![JsonPatchOp::Add {
                path: "/l/-".into(),
                value: json!(3),
            }],
        )
        .unwrap();
        assert_eq!(out["l"], json!([1, 2, 3]));
    }

    #[test]
    fn json_add_to_array_index() {
        let out = json_patch(
            json!({"l": [1, 3]}),
            vec![JsonPatchOp::Add {
                path: "/l/1".into(),
                value: json!(2),
            }],
        )
        .unwrap();
        assert_eq!(out["l"], json!([1, 2, 3]));
    }

    // ── Strategic Merge ─────────────────────────────────────────────────

    fn containers_env() -> MockPatchEnv {
        MockPatchEnv::new().seed(
            deploy_gvk(),
            "spec.template.spec.containers",
            ListMergeStrategy::MergeByKey(vec!["name".into()]),
        )
    }

    fn strategic(env: &MockPatchEnv, current: Value, patch: Value) -> Result<Value, PatchError> {
        apply(
            PatchType::Strategic,
            &current,
            &PatchBody::Strategic(patch),
            env,
            &deploy_gvk(),
        )
    }

    fn two_container_deploy() -> Value {
        json!({
            "spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.27"},
                {"name": "sidecar", "image": "envoy:1.30"}
            ]}}}
        })
    }

    #[test]
    fn strategic_list_merge_by_key_patches_only_named_container() {
        // THE kubectl set image proof: patch nginx's image; sidecar untouched.
        let env = containers_env();
        let out = strategic(
            &env,
            two_container_deploy(),
            json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.28"}
            ]}}}}),
        )
        .unwrap();
        let containers = out["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 2, "no container added/removed");
        let nginx = containers.iter().find(|c| c["name"] == "nginx").unwrap();
        let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
        assert_eq!(nginx["image"], "nginx:1.28", "nginx image updated");
        assert_eq!(sidecar["image"], "envoy:1.30", "SIDECAR UNCHANGED");
    }

    #[test]
    fn strategic_list_merge_by_key_appends_new_element() {
        let env = containers_env();
        let out = strategic(
            &env,
            two_container_deploy(),
            json!({"spec": {"template": {"spec": {"containers": [
                {"name": "metrics", "image": "prom:1.0"}
            ]}}}}),
        )
        .unwrap();
        let containers = out["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 3, "new container appended");
        assert!(containers.iter().any(|c| c["name"] == "metrics"));
    }

    #[test]
    fn strategic_patch_delete_removes_matched_element() {
        let env = containers_env();
        let out = strategic(
            &env,
            two_container_deploy(),
            json!({"spec": {"template": {"spec": {"containers": [
                {"name": "sidecar", "$patch": "delete"}
            ]}}}}),
        )
        .unwrap();
        let containers = out["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 1);
        assert_eq!(containers[0]["name"], "nginx");
    }

    #[test]
    fn strategic_atomic_list_replaces_whole() {
        // tolerations are atomic → whole-list replace.
        let env = MockPatchEnv::new().seed(
            deploy_gvk(),
            "spec.template.spec.tolerations",
            ListMergeStrategy::Replace,
        );
        let out = strategic(
            &env,
            json!({"spec": {"template": {"spec": {"tolerations": [
                {"key": "a"}, {"key": "b"}
            ]}}}}),
            json!({"spec": {"template": {"spec": {"tolerations": [
                {"key": "c"}
            ]}}}}),
        )
        .unwrap();
        let tol = out["spec"]["template"]["spec"]["tolerations"]
            .as_array()
            .unwrap();
        assert_eq!(tol.len(), 1);
        assert_eq!(tol[0]["key"], "c");
    }

    #[test]
    fn strategic_primitive_set_union() {
        let env =
            MockPatchEnv::new().seed(deploy_gvk(), "metadata.finalizers", ListMergeStrategy::Set);
        let out = strategic(
            &env,
            json!({"metadata": {"finalizers": ["a", "b"]}}),
            json!({"metadata": {"finalizers": ["b", "c"]}}),
        )
        .unwrap();
        assert_eq!(out["metadata"]["finalizers"], json!(["a", "b", "c"]));
    }

    #[test]
    fn strategic_map_merge_preserves_other_keys() {
        // kubectl annotate / label — map merge preserves sibling keys.
        let env = MockPatchEnv::new();
        let out = strategic(
            &env,
            json!({"metadata": {"annotations": {"keep": "yes"}}}),
            json!({"metadata": {"annotations": {"foo": "bar"}}}),
        )
        .unwrap();
        assert_eq!(out["metadata"]["annotations"]["keep"], "yes");
        assert_eq!(out["metadata"]["annotations"]["foo"], "bar");
    }

    #[test]
    fn strategic_patch_replace_directive_on_map() {
        let env = MockPatchEnv::new();
        let out = strategic(
            &env,
            json!({"data": {"old": "1", "keep": "2"}}),
            json!({"data": {"$patch": "replace", "new": "3"}}),
        )
        .unwrap();
        // $patch:replace → the whole `data` map becomes just {new:3}.
        assert_eq!(out["data"], json!({"new": "3"}));
    }

    #[test]
    fn strategic_retain_keys_directive() {
        let env = MockPatchEnv::new();
        let out = strategic(
            &env,
            json!({"spec": {"a": 1, "b": 2, "c": 3}}),
            json!({"spec": {"$retainKeys": ["a", "b"], "b": 9}}),
        )
        .unwrap();
        // c dropped (not retained), b updated, a kept.
        assert_eq!(out["spec"], json!({"a": 1, "b": 9}));
    }

    #[test]
    fn strategic_missing_merge_key_errors() {
        let env = containers_env();
        let err = strategic(
            &env,
            two_container_deploy(),
            json!({"spec": {"template": {"spec": {"containers": [
                {"image": "no-name:1.0"}
            ]}}}}),
        )
        .unwrap_err();
        assert_eq!(
            err,
            PatchError::MissingMergeKey {
                path: "spec.template.spec.containers".into(),
                key: "name".into()
            }
        );
    }

    // ── SSA deferral ────────────────────────────────────────────────────

    #[test]
    fn apply_returns_unsupported_never_silent_merge() {
        let env = MockPatchEnv::new();
        let err = apply(
            PatchType::Apply,
            &json!({"spec": {"replicas": 1}}),
            &PatchBody::Apply(json!({"spec": {"replicas": 9}})),
            &env,
            &deploy_gvk(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            PatchError::Unsupported {
                what: "server-side apply".into()
            }
        );
    }

    // ── PatchBody::from_raw ─────────────────────────────────────────────

    #[test]
    fn from_raw_json_decodes_typed_ops() {
        let raw = json!([{"op": "replace", "path": "/spec/replicas", "value": 5}]);
        let body = PatchBody::from_raw(PatchType::Json, raw).unwrap();
        match body {
            PatchBody::Json(ops) => {
                assert_eq!(ops.len(), 1);
                assert!(matches!(ops[0], JsonPatchOp::Replace { .. }));
            }
            _ => panic!("expected Json body"),
        }
    }

    #[test]
    fn from_raw_bad_json_patch_is_typed_error() {
        // A merge-shaped object sent as json-patch → typed BadDirective, not
        // a panic.
        let raw = json!({"spec": {"replicas": 5}});
        let err = PatchBody::from_raw(PatchType::Json, raw).unwrap_err();
        assert!(matches!(err, PatchError::BadDirective { .. }));
    }

    // ── OpenAPI-backed env (real impl, no network/FS — vendored docs) ───

    #[test]
    fn openapi_env_resolves_containers_as_merge_by_name() {
        let env = OpenApiPatchEnv::new();
        let strat = env.merge_strategy(
            &deploy_gvk(),
            &JsonPath::from_dotted("spec.template.spec.containers"),
        );
        assert_eq!(strat, ListMergeStrategy::MergeByKey(vec!["name".into()]));
    }

    #[test]
    fn openapi_env_resolves_volumes_as_retain_keys() {
        let env = OpenApiPatchEnv::new();
        let strat = env.merge_strategy(
            &deploy_gvk(),
            &JsonPath::from_dotted("spec.template.spec.volumes"),
        );
        assert_eq!(
            strat,
            ListMergeStrategy::MergeByKeyRetainKeys(vec!["name".into()])
        );
    }

    #[test]
    fn openapi_env_resolves_tolerations_as_atomic_replace() {
        let env = OpenApiPatchEnv::new();
        let strat = env.merge_strategy(
            &deploy_gvk(),
            &JsonPath::from_dotted("spec.template.spec.tolerations"),
        );
        assert_eq!(strat, ListMergeStrategy::Replace);
    }

    #[test]
    fn openapi_env_unknown_field_is_atomic_replace() {
        let env = OpenApiPatchEnv::new();
        let strat = env.merge_strategy(&deploy_gvk(), &JsonPath::from_dotted("spec.nope.list"));
        assert_eq!(strat, ListMergeStrategy::Replace);
    }

    #[test]
    fn openapi_env_drives_real_set_image_merge() {
        // End-to-end: the REAL OpenAPI env drives the strategic merge, and
        // only the named container changes (no MockEnv).
        let env = OpenApiPatchEnv::new();
        let out = apply(
            PatchType::Strategic,
            &two_container_deploy(),
            &PatchBody::Strategic(json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.28"}
            ]}}}})),
            &env,
            &deploy_gvk(),
        )
        .unwrap();
        let containers = out["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 2);
        let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
        assert_eq!(
            sidecar["image"], "envoy:1.30",
            "sidecar untouched via real OpenAPI env"
        );
    }
}
