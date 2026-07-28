//! `apply_ssa` — the server-side-apply (SSA) interpreter.
//!
//! ## Why this module exists
//!
//! Before this module, an `application/apply-patch+yaml` request resolved
//! to [`engenho_types::patch::PatchType::Apply`] and the store's patch
//! interpreter returned [`crate::patch_apply::PatchError::Unsupported`] — a
//! clean typed 415 stub. This module is the load-bearing implementation:
//! a PURE interpreter [`apply_ssa`] that
//!
//!   1. merges the apply config into the current object REUSING the exact
//!      strategic-merge engine [`crate::patch_apply::strategic_merge`] (THE
//!      associative-list-by-key semantics SSA needs ARE strategic merge —
//!      no second merge engine is forked), and
//!   2. tracks per-manager field ownership in `metadata.managedFields`
//!      (the typed [`FieldSet`] trie ↔ the wire `FieldsV1` projection), and
//!   3. detects conflicts against OTHER apply-managers' ownership (a path
//!      owned by a different manager whose value differs) — returning a
//!      typed [`ApplyConflicts`] (→ a 409 Status) unless `force`, and
//!   4. removes fields a manager previously owned but no longer applies
//!      (when no other manager owns them) — the "drop a key on re-apply
//!      removes it" semantics.
//!
//! ## The triplet (★★ TYPED-SPEC + INTERPRETER TRIPLET)
//!
//! 1. **Typed border** — [`FieldSet`] / [`PathElement`] (the internal
//!    ownership trie) + [`engenho_types::meta::FieldsV1`] /
//!    [`engenho_types::meta::ManagedFieldsEntry`] (the wire projection) +
//!    [`ApplyConflicts`] / [`Conflict`] / [`SsaOutcome`].
//! 2. **Authored Lisp spec** — the same TYPED FOLLOW-UP owed by
//!    `patch_apply` (the store crate intentionally does not pull tatara-lisp
//!    into the hot path); the phases authored here would author as Lisp
//!    data behind a feature.
//! 3. **Working interpreter** — [`apply_ssa`], below. Pure given the
//!    [`crate::patch_apply::PatchSchemaEnv`] (the SAME env strategic-merge
//!    consults — no new env). Tests inject
//!    [`crate::patch_apply::MockPatchEnv`].
//!
//! No silent wrong answers: a conflict without `force` is a typed
//! [`ApplyConflicts`], never a silent overwrite.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::patch_apply::{Gvk, JsonPath, ListMergeStrategy, PatchSchemaEnv, strategic_merge};

// ─── the internal typed ownership representation ────────────────────────────

/// One step in a field-ownership path — the typed analog of a
/// structured-merge-diff `PathElement`. The four variants map 1:1 to the
/// `f:` / `k:` / `v:` / `i:` `FieldsV1` key prefixes.
///
/// `Ord` is implemented over the deterministic `FieldsV1`-key encoding (not
/// derived — `serde_json::Value` is not `Ord`). That total order IS the
/// byte-deterministic ordering the [`FieldSet`] `BTreeMap` children obey,
/// so the serialized managedFields is identical on every Raft replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathElement {
    /// `f:<name>` — a struct/map field.
    Field(String),
    /// `k:{keys}` — an associative-list element addressed by its
    /// list-map-keys tuple. The `BTreeMap` keeps the key tuple ordered so
    /// the encoding is byte-deterministic.
    Key(BTreeMap<String, Value>),
    /// `v:<val>` — a set element addressed by its primitive value.
    Value(Value),
    /// `i:<n>` — a list element addressed by index.
    Index(usize),
}

impl Ord for PathElement {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by the deterministic FieldsV1 key encoding — a total order
        // over a value-carrying enum where serde_json::Value isn't Ord.
        self.to_fields_v1_key().cmp(&other.to_fields_v1_key())
    }
}

impl PartialOrd for PathElement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PathElement {
    /// The `FieldsV1` JSON-object KEY this element encodes to (`f:foo`,
    /// `k:{"name":"nginx"}`, `v:"x"`, `i:3`). The canonical
    /// (BTreeMap/serde) JSON render keeps `Key` deterministic.
    fn to_fields_v1_key(&self) -> String {
        match self {
            Self::Field(name) => format!("f:{name}"),
            Self::Key(keys) => {
                // serde over a BTreeMap renders keys in ascending order →
                // deterministic. (The map is already a BTreeMap.)
                let obj: Map<String, Value> =
                    keys.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                format!("k:{}", Value::Object(obj))
            }
            Self::Value(v) => format!("v:{v}"),
            Self::Index(i) => format!("i:{i}"),
        }
    }

    /// Parse a `FieldsV1` object key back into a typed [`PathElement`].
    fn from_fields_v1_key(key: &str) -> Option<Self> {
        if let Some(name) = key.strip_prefix("f:") {
            return Some(Self::Field(name.to_string()));
        }
        if let Some(rest) = key.strip_prefix("k:") {
            let v: Value = serde_json::from_str(rest).ok()?;
            let obj = v.as_object()?;
            let map: BTreeMap<String, Value> =
                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            return Some(Self::Key(map));
        }
        if let Some(rest) = key.strip_prefix("v:") {
            let v: Value = serde_json::from_str(rest).ok()?;
            return Some(Self::Value(v));
        }
        if let Some(rest) = key.strip_prefix("i:") {
            let i: usize = rest.parse().ok()?;
            return Some(Self::Index(i));
        }
        None
    }
}

/// A node in the field-ownership trie. `self_owned` is the `.` sentinel
/// ("this node itself is owned"); `children` maps each owned sub-path
/// element to its sub-trie.
///
/// `BTreeMap` (not `HashMap`) is mandatory: managedFields is replicated
/// Raft state and MUST serialize byte-identically on every replica (the
/// same determinism law `ObjectMeta`'s `BTreeMap` labels obey).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldSet {
    /// The `.` sentinel — this node itself is an owned leaf.
    pub self_owned: bool,
    /// Owned children, keyed by the typed path element.
    pub children: BTreeMap<PathElement, FieldSet>,
}

impl FieldSet {
    /// An empty fieldset (owns nothing).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` when this trie owns no path at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.self_owned && self.children.is_empty()
    }

    /// Collect every owned path (each a `Vec<PathElement>` ending at a
    /// `self_owned` node) into `out`, prefixed by `prefix`. The empty path
    /// (the root being `self_owned`) is included if the root owns itself.
    fn collect_paths(&self, prefix: &mut Vec<PathElement>, out: &mut Vec<Vec<PathElement>>) {
        if self.self_owned {
            out.push(prefix.clone());
        }
        for (el, child) in &self.children {
            prefix.push(el.clone());
            child.collect_paths(prefix, out);
            prefix.pop();
        }
    }

    /// Every owned path in this trie (a leaf-set view used by conflict
    /// detection + field removal).
    #[must_use]
    pub fn owned_paths(&self) -> Vec<Vec<PathElement>> {
        let mut out = Vec::new();
        let mut prefix = Vec::new();
        self.collect_paths(&mut prefix, &mut out);
        out
    }

    /// `true` iff this trie owns the exact path `elems` (as a `self_owned`
    /// leaf).
    #[must_use]
    pub fn owns(&self, elems: &[PathElement]) -> bool {
        match elems.split_first() {
            None => self.self_owned,
            Some((head, tail)) => self.children.get(head).is_some_and(|c| c.owns(tail)),
        }
    }

    /// Serialize this trie to the wire [`engenho_types::meta::FieldsV1`]
    /// `.`/`f:`/`k:`/`v:`/`i:` JSON object.
    #[must_use]
    pub fn to_fields_v1(&self) -> engenho_types::meta::FieldsV1 {
        engenho_types::meta::FieldsV1(self.to_value())
    }

    fn to_value(&self) -> Value {
        let mut obj = Map::new();
        if self.self_owned {
            obj.insert(".".to_string(), Value::Object(Map::new()));
        }
        for (el, child) in &self.children {
            obj.insert(el.to_fields_v1_key(), child.to_value());
        }
        Value::Object(obj)
    }

    /// Parse a wire [`engenho_types::meta::FieldsV1`] back into the typed
    /// trie. Unrecognized keys are skipped (forward-compat — never a
    /// panic).
    #[must_use]
    pub fn from_fields_v1(fields: &engenho_types::meta::FieldsV1) -> Self {
        Self::from_value(&fields.0)
    }

    fn from_value(v: &Value) -> Self {
        let mut set = Self::new();
        if let Some(obj) = v.as_object() {
            for (k, child) in obj {
                if k == "." {
                    set.self_owned = true;
                    continue;
                }
                if let Some(el) = PathElement::from_fields_v1_key(k) {
                    set.children.insert(el, Self::from_value(child));
                }
            }
        }
        set
    }
}

// ─── computing the applied-config fieldset ──────────────────────────────────

/// Metadata leaves that are apiserver-owned and EXCLUDED from any
/// manager's ownership (upstream strips these from every applied fieldset).
const APISERVER_OWNED_METADATA: &[&str] = &[
    "managedFields",
    "resourceVersion",
    "uid",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
];

/// Build the [`FieldSet`] a manager owns by APPLYING `config` — walk the
/// config recursively, marking ownership at the grain dictated by the
/// env's per-list merge strategy (the SAME `(gvk, path) → strategy`
/// resolution strategic-merge consults).
///
///   * object key → a [`PathElement::Field`] child (recurse), EXCEPT the
///     apiserver-owned `metadata` leaves which are skipped.
///   * scalar leaf → mark `self_owned`.
///   * list field → consult `env.merge_strategy(gvk, path)`:
///       - `MergeByKey(keys)` / `MergeByKeyRetainKeys(keys)` → each element
///         becomes a [`PathElement::Key`] child built from its key tuple
///         (recurse) — `containers[name=nginx]` is one owned path.
///       - `Set` → each primitive element becomes a [`PathElement::Value`]
///         child.
///       - `Replace` (atomic) → the whole list field is one owned leaf.
#[must_use]
pub fn fieldset_of(config: &Value, gvk: &Gvk, env: &dyn PatchSchemaEnv) -> FieldSet {
    let mut set = FieldSet::new();
    build_fieldset(config, gvk, env, &JsonPath::root(), &mut set);
    set
}

fn build_fieldset(
    node: &Value,
    gvk: &Gvk,
    env: &dyn PatchSchemaEnv,
    path: &JsonPath,
    out: &mut FieldSet,
) {
    match node {
        Value::Object(obj) => {
            // An empty object the operator explicitly declared still owns
            // itself (mirrors "this field is mine, with no sub-fields").
            if obj.is_empty() {
                out.self_owned = true;
                return;
            }
            for (k, v) in obj {
                // Skip apiserver-owned metadata leaves so a manager never
                // claims ownership of resourceVersion/uid/managedFields/etc.
                if path.segments().is_empty() && k == "metadata" {
                    let child = out
                        .children
                        .entry(PathElement::Field(k.clone()))
                        .or_default();
                    build_metadata_fieldset(v, gvk, env, &path.child(k), child);
                    if child.is_empty() {
                        out.children.remove(&PathElement::Field(k.clone()));
                    }
                    continue;
                }
                let child = out
                    .children
                    .entry(PathElement::Field(k.clone()))
                    .or_default();
                build_fieldset(v, gvk, env, &path.child(k), child);
            }
        }
        Value::Array(arr) => {
            let strat = env.merge_strategy(gvk, path);
            build_list_fieldset(arr, &strat, gvk, env, path, out);
        }
        // A scalar leaf — the manager owns this field's value.
        _ => out.self_owned = true,
    }
}

/// `metadata` sub-walk that drops the apiserver-owned leaves. Behaves like
/// [`build_fieldset`] for every other metadata field (name, namespace,
/// labels, annotations, finalizers, …).
fn build_metadata_fieldset(
    node: &Value,
    gvk: &Gvk,
    env: &dyn PatchSchemaEnv,
    path: &JsonPath,
    out: &mut FieldSet,
) {
    let Some(obj) = node.as_object() else {
        // A non-object metadata is malformed; own nothing (conservative).
        return;
    };
    for (k, v) in obj {
        if APISERVER_OWNED_METADATA.contains(&k.as_str()) {
            continue;
        }
        let child = out
            .children
            .entry(PathElement::Field(k.clone()))
            .or_default();
        build_fieldset(v, gvk, env, &path.child(k), child);
    }
}

fn build_list_fieldset(
    arr: &[Value],
    strat: &ListMergeStrategy,
    gvk: &Gvk,
    env: &dyn PatchSchemaEnv,
    path: &JsonPath,
    out: &mut FieldSet,
) {
    match strat {
        ListMergeStrategy::Replace => {
            // Atomic list — the whole field is one owned leaf; elements are
            // not individually addressed.
            out.self_owned = true;
        }
        ListMergeStrategy::Set => {
            for el in arr {
                // Primitive set element keyed by value. (A non-primitive in
                // a `set` list is malformed; address it by value anyway —
                // the encoding is total.)
                let child = out
                    .children
                    .entry(PathElement::Value(el.clone()))
                    .or_default();
                child.self_owned = true;
            }
        }
        ListMergeStrategy::MergeByKey(keys) | ListMergeStrategy::MergeByKeyRetainKeys(keys) => {
            for el in arr {
                let Some(obj) = el.as_object() else {
                    // A primitive in a merge-by-key list is malformed; fall
                    // back to value addressing (total, never a panic).
                    let child = out
                        .children
                        .entry(PathElement::Value(el.clone()))
                        .or_default();
                    child.self_owned = true;
                    continue;
                };
                // Build the key tuple. A missing key → the element is
                // addressed by whatever keys it DOES carry (conservative;
                // strategic_merge already rejects a truly key-less element
                // at merge time with MissingMergeKey, so the fieldset just
                // mirrors what it can).
                let mut tuple = BTreeMap::new();
                for key in keys {
                    if let Some(v) = obj.get(key) {
                        tuple.insert(key.clone(), v.clone());
                    }
                }
                let child = out.children.entry(PathElement::Key(tuple)).or_default();
                build_fieldset(el, gvk, env, path, child);
            }
        }
    }
}

// ─── conflict + outcome typed values ────────────────────────────────────────

/// One field-level apply conflict — a path the incoming apply wants to own
/// that a DIFFERENT apply-manager already owns with a differing value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    /// The manager that currently owns the conflicting path.
    pub manager: String,
    /// The conflicting field path, rendered as the upstream
    /// `.spec.x.containers[name=nginx].image` dotted/bracketed form for the
    /// 409 Status `details.causes[].field`.
    pub field: String,
}

/// The set of conflicts an unforced apply hit — the typed error
/// [`apply_ssa`] returns instead of silently overwriting. The apiserver
/// renders it as a 409 `Status` with reason `Conflict` + a `details.causes`
/// array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyConflicts {
    /// One cause per conflicting field.
    pub conflicts: Vec<Conflict>,
}

impl ApplyConflicts {
    /// Render the conflicts to the typed `details.causes` JSON array the
    /// 409 Status carries (`[{type, message, field}, …]`) — TYPED EMISSION
    /// (a serde value, never `format!()` of the wire object). The
    /// per-cause `message` strings are composed text that lands in the
    /// typed `message` field, mirroring upstream's "conflict with
    /// \"<mgr>\"".
    #[must_use]
    pub fn to_causes(&self) -> Value {
        let causes: Vec<Value> = self
            .conflicts
            .iter()
            .map(|c| {
                serde_json::json!({
                    "type": "FieldManagerConflict",
                    "message": format!("conflict with \"{}\"", c.manager),
                    "field": c.field,
                })
            })
            .collect();
        Value::Array(causes)
    }
}

/// What [`apply_ssa`] produces on success — the merged object plus the new
/// `managedFields` slice (as a wire JSON array, ready to write into
/// `metadata.managedFields`).
#[derive(Clone, Debug, PartialEq)]
pub enum SsaOutcome {
    /// The object did not exist — created from the apply config, with the
    /// manager recorded as the sole owner.
    Created {
        /// The merged object (apply config; apiserver metadata stamped by
        /// the store apply path).
        merged: Value,
        /// The new `metadata.managedFields` array.
        managed_fields: Value,
    },
    /// The object existed — merged, conflicts resolved, ownership updated.
    Updated {
        /// The merged object.
        merged: Value,
        /// The new `metadata.managedFields` array.
        managed_fields: Value,
    },
}

impl SsaOutcome {
    /// The merged object regardless of created/updated.
    #[must_use]
    pub fn merged(&self) -> &Value {
        match self {
            Self::Created { merged, .. } | Self::Updated { merged, .. } => merged,
        }
    }

    /// The new managedFields array regardless of created/updated.
    #[must_use]
    pub fn managed_fields(&self) -> &Value {
        match self {
            Self::Created { managed_fields, .. } | Self::Updated { managed_fields, .. } => {
                managed_fields
            }
        }
    }
}

// ─── the interpreter ────────────────────────────────────────────────────────

/// Apply `config` on behalf of `manager` (server-side apply), reusing the
/// strategic-merge engine + tracking field ownership.
///
/// `current` is the live object (`None` ⇒ create); `time` is the
/// boundary-frozen RFC3339 instant stamped onto the manager's
/// managedFields entry (threaded from the replicated command so every Raft
/// replica replays identical bytes); `env` is the SAME
/// [`PatchSchemaEnv`](crate::patch_apply::PatchSchemaEnv) strategic-merge
/// consults.
///
/// Phases (pure given `env`):
///   1. CREATE branch (`current` is None) — no conflicts possible; merged
///      = config; managedFields = one entry owning the applied fieldset.
///   2. compute the applied fieldset.
///   3. collect conflicts vs OTHER apply-managers (path owned by a
///      different manager + value differs).
///   4. conflicts && !force → `Err(ApplyConflicts)`.
///   5. force || no-conflict → strategic-merge config into current.
///   6. field removal — drop paths `manager` previously owned but no
///      longer applies (when no other manager owns them).
///   7. update managedFields (M gains applied, loses dropped; under force,
///      subtract from conflicting managers; drop emptied entries; stamp M's
///      `time`).
///
/// # Errors
///
/// [`ApplyConflicts`] when an unforced apply overlaps another manager's
/// ownership with a differing value — NEVER a silent overwrite.
pub fn apply_ssa(
    current: Option<&Value>,
    config: &Value,
    manager: &str,
    force: bool,
    gvk: &Gvk,
    env: &dyn PatchSchemaEnv,
    time: &str,
) -> Result<SsaOutcome, ApplyConflicts> {
    let applied_fs = fieldset_of(config, gvk, env);

    // ── CREATE branch ──────────────────────────────────────────────────
    let Some(current) = current else {
        let entry = make_entry(manager, &applied_fs, gvk, time);
        let managed_fields = Value::Array(vec![entry]);
        // Write managedFields INTO the created object so the stored object
        // carries the ownership ledger (the apply config never had it).
        let mut merged = config.clone();
        set_managed_fields(&mut merged, managed_fields.clone());
        return Ok(SsaOutcome::Created {
            merged,
            managed_fields,
        });
    };

    // ── existing-object branch ─────────────────────────────────────────
    let entries = read_managed_fields(current);

    // Phase 3 — collect conflicts against OTHER apply-managers.
    let mut conflicts = Vec::new();
    for path in applied_fs.owned_paths() {
        for entry in &entries {
            if entry.manager == manager {
                continue;
            }
            if entry.operation != ManagedOp::Apply {
                // Scope boundary: Update-operation ownership is typed-
                // deferred — only Apply managers participate in conflict
                // detection at this brick.
                continue;
            }
            if entry.fields.owns(&path) {
                // The other manager owns this path. A conflict exists iff
                // the applied value DIFFERS from the current value.
                let applied_val = value_at(config, &path);
                let current_val = value_at(current, &path);
                if applied_val != current_val {
                    conflicts.push(Conflict {
                        manager: entry.manager.clone(),
                        field: render_field_path(&path),
                    });
                }
            }
        }
    }

    if !conflicts.is_empty() && !force {
        // Stable order for a deterministic 409 body.
        conflicts.sort_by(|a, b| a.field.cmp(&b.field).then(a.manager.cmp(&b.manager)));
        conflicts.dedup();
        return Err(ApplyConflicts { conflicts });
    }

    // Phase 5 — MERGE (reuse the strategic-merge engine; the apply config
    // IS a strategic-merge patch over current).
    let mut merged = strategic_merge(current, config, env, gvk, &JsonPath::root())
        // strategic_merge only errors on a key-less merge element / bad
        // $patch directive; an apply config carries neither. A surfaced
        // error is conservatively treated as "no structural merge" — keep
        // current. (Total: never a panic.)
        .unwrap_or_else(|_| current.clone());

    // Phase 6 — FIELD REMOVAL: drop paths `manager` previously owned but
    // no longer applies, when no OTHER manager owns them.
    let prior_self = entries
        .iter()
        .find(|e| e.manager == manager && e.operation == ManagedOp::Apply)
        .map(|e| e.fields.clone())
        .unwrap_or_default();
    for path in prior_self.owned_paths() {
        if applied_fs.owns(&path) {
            continue; // still applied — keep.
        }
        let owned_by_other = entries.iter().any(|e| {
            e.manager != manager && e.operation == ManagedOp::Apply && e.fields.owns(&path)
        });
        if !owned_by_other {
            remove_at(&mut merged, &path);
        }
    }

    // Phase 7 — update managedFields ownership.
    let new_entries = update_ownership(&entries, manager, &applied_fs, force, gvk, time);
    let managed_fields = Value::Array(new_entries);

    // Write the new managedFields into the merged object (the store apply
    // path stamps resourceVersion/generation/uid; managedFields is SSA-
    // owned and written HERE so it survives the strategic merge — the
    // apply config never carried it).
    set_managed_fields(&mut merged, managed_fields.clone());

    Ok(SsaOutcome::Updated {
        merged,
        managed_fields,
    })
}

// ─── managedFields read/decode (store-side, raw-Value) ──────────────────────

/// The operation an entry records — the store-side mirror of
/// [`engenho_types::meta::ManagedFieldsOperation`] (kept local so the trait
/// types stay apiserver/types-side; the store works on raw `Value`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedOp {
    Apply,
    Update,
}

/// A decoded managedFields entry (store-internal typed view over the raw
/// `Value` array).
struct DecodedEntry {
    manager: String,
    operation: ManagedOp,
    fields: FieldSet,
    /// The raw entry value, retained so non-conflicting / untouched
    /// entries re-serialize byte-identically.
    raw: Value,
}

/// Decode `current.metadata.managedFields` into the typed view. A missing
/// / malformed slice decodes to empty (no ownership recorded).
fn read_managed_fields(current: &Value) -> Vec<DecodedEntry> {
    let Some(arr) = current
        .get("metadata")
        .and_then(|m| m.get("managedFields"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|e| {
            let manager = e.get("manager")?.as_str()?.to_string();
            let operation = match e.get("operation").and_then(Value::as_str) {
                Some("Update") => ManagedOp::Update,
                _ => ManagedOp::Apply,
            };
            let fields = e
                .get("fieldsV1")
                .map(|f| FieldSet::from_fields_v1(&engenho_types::meta::FieldsV1(f.clone())))
                .unwrap_or_default();
            Some(DecodedEntry {
                manager,
                operation,
                fields,
                raw: e.clone(),
            })
        })
        .collect()
}

/// Build a fresh managedFields entry `Value` for `manager` owning
/// `fieldset`, stamped with `time` + the object's apiVersion.
fn make_entry(manager: &str, fieldset: &FieldSet, gvk: &Gvk, time: &str) -> Value {
    let api_version = if gvk.group.is_empty() {
        gvk.version.clone()
    } else {
        format!("{}/{}", gvk.group, gvk.version)
    };
    serde_json::json!({
        "manager": manager,
        "operation": "Apply",
        "apiVersion": api_version,
        "time": time,
        "fieldsType": "FieldsV1",
        "fieldsV1": fieldset.to_fields_v1().0,
    })
}

/// Recompute the managedFields array after an apply by `manager` that took
/// `applied_fs`. M's entry's fieldset BECOMES `applied_fs` (replace
/// wholesale — that's what makes field-removal work). Under `force`, the
/// applied paths are SUBTRACTED from every OTHER apply-manager (ownership
/// transfer); without force there were no conflicts so no subtraction is
/// needed. Emptied entries are dropped.
fn update_ownership(
    entries: &[DecodedEntry],
    manager: &str,
    applied_fs: &FieldSet,
    force: bool,
    gvk: &Gvk,
    time: &str,
) -> Vec<Value> {
    let mut out = Vec::new();
    let mut saw_manager = false;

    for entry in entries {
        if entry.manager == manager && entry.operation == ManagedOp::Apply {
            // M's entry — replace its fieldset with the applied set.
            saw_manager = true;
            out.push(make_entry(manager, applied_fs, gvk, time));
            continue;
        }
        if force && entry.operation == ManagedOp::Apply {
            // Subtract the applied paths from the other apply-manager.
            let mut remaining = entry.fields.clone();
            subtract(&mut remaining, applied_fs);
            if remaining.is_empty() {
                // Emptied — drop the entry entirely.
                continue;
            }
            // Re-serialize the other manager's entry with the reduced
            // fieldset, preserving its other fields.
            let mut raw = entry.raw.clone();
            if let Some(obj) = raw.as_object_mut() {
                obj.insert("fieldsV1".to_string(), remaining.to_fields_v1().0);
            }
            out.push(raw);
            continue;
        }
        // Untouched entry (different manager, no force, or an Update
        // entry) — pass through byte-identically.
        out.push(entry.raw.clone());
    }

    if !saw_manager {
        // First apply by this manager — add its entry.
        out.push(make_entry(manager, applied_fs, gvk, time));
    }
    out
}

/// Remove every owned path of `remove` from `set` (set difference at the
/// leaf grain). A node whose children all drained + which is not itself
/// owned after the subtraction is pruned.
fn subtract(set: &mut FieldSet, remove: &FieldSet) {
    for path in remove.owned_paths() {
        remove_owned_path(set, &path);
    }
}

/// Drop ownership of the exact path `elems` from `set`, pruning empty
/// intermediate nodes.
fn remove_owned_path(set: &mut FieldSet, elems: &[PathElement]) {
    match elems.split_first() {
        None => set.self_owned = false,
        Some((head, tail)) => {
            if let Some(child) = set.children.get_mut(head) {
                remove_owned_path(child, tail);
                if child.is_empty() {
                    set.children.remove(head);
                }
            }
        }
    }
}

// ─── object navigation by PathElement path ──────────────────────────────────

/// Resolve the value at `path` within `root`, or `None` if any step is
/// absent. `Key` steps address an associative-list element by its
/// list-map-keys tuple; `Value` steps a set element; `Index` a list
/// position; `Field` an object key.
fn value_at<'a>(root: &'a Value, path: &[PathElement]) -> Option<&'a Value> {
    let mut cur = root;
    for el in path {
        cur = step(cur, el)?;
    }
    Some(cur)
}

fn step<'a>(node: &'a Value, el: &PathElement) -> Option<&'a Value> {
    match el {
        PathElement::Field(name) => node.get(name),
        PathElement::Index(i) => node.as_array()?.get(*i),
        PathElement::Value(v) => node.as_array()?.iter().find(|e| *e == v),
        PathElement::Key(keys) => node
            .as_array()?
            .iter()
            .find(|e| keys.iter().all(|(k, v)| e.get(k) == Some(v))),
    }
}

/// Remove the value at `path` within `root` (used by field removal),
/// PRUNING emptied associative-list elements on the way back up. `Field`
/// deletes the object key; `Key`/`Value` remove the matched list element;
/// `Index` removes by position. A missing path is a no-op.
///
/// The prune is the upstream behavior: when a manager stops declaring a
/// field inside an associative-list element (e.g. `containers[name=sidecar]`)
/// and that leaves the element an empty `{}`, the empty element is removed
/// from the list — not left as a phantom `{}`. Pruning happens recursively:
/// removing a leaf field that empties its containing Key/Index/Value
/// list-element drops the element too.
fn remove_at(root: &mut Value, path: &[PathElement]) {
    remove_rec(root, path);
}

/// Remove `path` from `node`, returning `true` if `node` itself became an
/// empty container (`{}` / `[]`) as a result — so the caller can prune it
/// when `node` is an associative-list element.
fn remove_rec(node: &mut Value, path: &[PathElement]) -> bool {
    let Some((head, tail)) = path.split_first() else {
        return false;
    };
    if tail.is_empty() {
        // Leaf step — perform the actual removal.
        match head {
            PathElement::Field(name) => {
                if let Some(obj) = node.as_object_mut() {
                    obj.remove(name);
                }
            }
            PathElement::Index(i) => {
                if let Some(arr) = node.as_array_mut().filter(|a| *i < a.len()) {
                    arr.remove(*i);
                }
            }
            PathElement::Value(v) => {
                if let Some(arr) = node.as_array_mut() {
                    arr.retain(|e| e != v);
                }
            }
            PathElement::Key(keys) => {
                if let Some(arr) = node.as_array_mut() {
                    arr.retain(|e| !keys.iter().all(|(k, val)| e.get(k) == Some(val)));
                }
            }
        }
        return is_empty_container(node);
    }
    // Intermediate step — for an associative-list / set / index element,
    // capture the element's index in the array FIRST (so we can prune it by
    // position after the descent — re-matching by key fails once the key
    // field itself is removed). Then descend; if the element emptied, drop
    // it from the array by that index. A struct field that emptied is left
    // as `{}` (matching upstream's struct semantics).
    match head {
        PathElement::Key(_) | PathElement::Value(_) | PathElement::Index(_) => {
            let Some(idx) = list_element_index(node, head) else {
                return false;
            };
            let Some(arr) = node.as_array_mut() else {
                return false;
            };
            let child = &mut arr[idx];
            let child_emptied = remove_rec(child, tail);
            if child_emptied {
                arr.remove(idx);
            }
        }
        PathElement::Field(name) => {
            let Some(child) = node.get_mut(name) else {
                return false;
            };
            // Descend; an emptied struct field stays as `{}` (no prune).
            let _ = remove_rec(child, tail);
        }
    }
    is_empty_container(node)
}

/// Resolve the array index of the element `head` addresses (`Key`/`Value`/
/// `Index`) within the array `node`, or `None`.
fn list_element_index(node: &Value, head: &PathElement) -> Option<usize> {
    let arr = node.as_array()?;
    match head {
        PathElement::Index(i) => (*i < arr.len()).then_some(*i),
        PathElement::Value(v) => arr.iter().position(|e| e == v),
        PathElement::Key(keys) => arr
            .iter()
            .position(|e| keys.iter().all(|(k, val)| e.get(k) == Some(val))),
        PathElement::Field(_) => None,
    }
}

/// `true` when `v` is an empty object `{}` or empty array `[]`.
fn is_empty_container(v: &Value) -> bool {
    match v {
        Value::Object(m) => m.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

/// Render a `PathElement` path into the upstream conflict-cause field
/// form: `.spec.template.spec.containers[name="nginx"].image`. Emission
/// goes through `write!` into the accumulator (the typed `fmt::Write`
/// surface) — not `format!()`-then-push.
fn render_field_path(path: &[PathElement]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for el in path {
        match el {
            PathElement::Field(name) => {
                out.push('.');
                out.push_str(name);
            }
            PathElement::Index(i) => {
                let _ = write!(out, "[{i}]");
            }
            PathElement::Value(v) => {
                let _ = write!(out, "[{v}]");
            }
            PathElement::Key(keys) => {
                let inner: Vec<String> = keys.iter().map(|(k, v)| format!("{k}={v}")).collect();
                let _ = write!(out, "[{}]", inner.join(","));
            }
        }
    }
    if out.is_empty() {
        out.push('.');
    }
    out
}

/// Write `managed_fields` into `obj.metadata.managedFields`, creating the
/// metadata object if absent.
fn set_managed_fields(obj: &mut Value, managed_fields: Value) {
    let Some(map) = obj.as_object_mut() else {
        return;
    };
    let metadata = map
        .entry("metadata".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(meta_obj) = metadata.as_object_mut() {
        meta_obj.insert("managedFields".to_string(), managed_fields);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch_apply::MockPatchEnv;
    use serde_json::json;

    const TIME: &str = "2026-06-08T00:00:00Z";

    fn cm_gvk() -> Gvk {
        Gvk::new("", "v1", "ConfigMap")
    }

    fn deploy_gvk() -> Gvk {
        Gvk::new("apps", "v1", "Deployment")
    }

    fn containers_env() -> MockPatchEnv {
        MockPatchEnv::new().seed(
            deploy_gvk(),
            "spec.template.spec.containers",
            ListMergeStrategy::MergeByKey(vec!["name".into()]),
        )
    }

    /// The merged object's managedFields entry for `manager`.
    fn entry_for<'a>(out: &'a SsaOutcome, manager: &str) -> &'a Value {
        out.managed_fields()
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["manager"] == manager)
            .expect("entry present")
    }

    fn owned(out: &SsaOutcome, manager: &str) -> FieldSet {
        FieldSet::from_fields_v1(&engenho_types::meta::FieldsV1(
            entry_for(out, manager)["fieldsV1"].clone(),
        ))
    }

    // ── CREATE ──────────────────────────────────────────────────────────

    #[test]
    fn create_records_ownership() {
        let env = MockPatchEnv::new();
        let out = apply_ssa(
            None,
            &json!({"apiVersion": "v1", "kind": "ConfigMap", "data": {"a": "1"}}),
            "mgr",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        assert!(matches!(out, SsaOutcome::Created { .. }));
        let entries = out.managed_fields().as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["manager"], "mgr");
        assert_eq!(entries[0]["operation"], "Apply");
        // owns .data.a
        let fs = owned(&out, "mgr");
        assert!(fs.owns(&[
            PathElement::Field("data".into()),
            PathElement::Field("a".into())
        ]));
    }

    // ── RE-APPLY same manager ───────────────────────────────────────────

    #[test]
    fn reapply_same_manager_gains_field_no_conflict() {
        let env = MockPatchEnv::new();
        // Seed an existing object where mgr owns .data.a.
        let created = apply_ssa(
            None,
            &json!({"data": {"a": "1"}}),
            "mgr",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let mut current = created.merged().clone();
        set_managed_fields(&mut current, created.managed_fields().clone());

        let out = apply_ssa(
            Some(&current),
            &json!({"data": {"a": "2", "b": "3"}}),
            "mgr",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        assert_eq!(out.merged()["data"]["a"], "2");
        assert_eq!(out.merged()["data"]["b"], "3");
        let fs = owned(&out, "mgr");
        assert!(fs.owns(&[
            PathElement::Field("data".into()),
            PathElement::Field("a".into())
        ]));
        assert!(fs.owns(&[
            PathElement::Field("data".into()),
            PathElement::Field("b".into())
        ]));
    }

    // ── FIELD REMOVAL ───────────────────────────────────────────────────

    #[test]
    fn dropping_field_removes_it() {
        let env = MockPatchEnv::new();
        let created = apply_ssa(
            None,
            &json!({"data": {"a": "1", "b": "2"}}),
            "mgr",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let mut current = created.merged().clone();
        set_managed_fields(&mut current, created.managed_fields().clone());

        // Re-apply dropping data.b.
        let out = apply_ssa(
            Some(&current),
            &json!({"data": {"a": "1"}}),
            "mgr",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        assert_eq!(out.merged()["data"]["a"], "1");
        assert!(
            out.merged()["data"].get("b").is_none(),
            "data.b removed on re-apply that drops it"
        );
        let fs = owned(&out, "mgr");
        assert!(!fs.owns(&[
            PathElement::Field("data".into()),
            PathElement::Field("b".into())
        ]));
    }

    // ── CONFLICT ────────────────────────────────────────────────────────

    #[test]
    fn second_manager_conflict_blocks_without_force() {
        let env = MockPatchEnv::new();
        let created = apply_ssa(
            None,
            &json!({"data": {"a": "1"}}),
            "mgr-a",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let mut current = created.merged().clone();
        set_managed_fields(&mut current, created.managed_fields().clone());

        // mgr-b applies data.a=2 — conflict (owned by mgr-a, value differs).
        let err = apply_ssa(
            Some(&current),
            &json!({"data": {"a": "2"}}),
            "mgr-b",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap_err();
        assert_eq!(err.conflicts.len(), 1);
        assert_eq!(err.conflicts[0].manager, "mgr-a");
        assert_eq!(err.conflicts[0].field, ".data.a");
        // And the current value is UNTOUCHED (no silent overwrite — the
        // caller never wrote merged).
        assert_eq!(current["data"]["a"], "1");
    }

    #[test]
    fn second_manager_same_value_is_not_a_conflict() {
        // mgr-b applying the SAME value mgr-a owns is a co-ownership, not a
        // conflict.
        let env = MockPatchEnv::new();
        let created = apply_ssa(
            None,
            &json!({"data": {"a": "1"}}),
            "mgr-a",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let mut current = created.merged().clone();
        set_managed_fields(&mut current, created.managed_fields().clone());

        let out = apply_ssa(
            Some(&current),
            &json!({"data": {"a": "1"}}),
            "mgr-b",
            false,
            &cm_gvk(),
            &env,
            TIME,
        );
        assert!(out.is_ok(), "same value ⇒ no conflict");
    }

    // ── FORCE ───────────────────────────────────────────────────────────

    #[test]
    fn force_transfers_ownership() {
        let env = MockPatchEnv::new();
        let created = apply_ssa(
            None,
            &json!({"data": {"a": "1"}}),
            "mgr-a",
            false,
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let mut current = created.merged().clone();
        set_managed_fields(&mut current, created.managed_fields().clone());

        let out = apply_ssa(
            Some(&current),
            &json!({"data": {"a": "2"}}),
            "mgr-b",
            true, // force
            &cm_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        assert_eq!(out.merged()["data"]["a"], "2", "force applied the value");
        // mgr-b now owns .data.a.
        let fs_b = owned(&out, "mgr-b");
        assert!(fs_b.owns(&[
            PathElement::Field("data".into()),
            PathElement::Field("a".into())
        ]));
        // mgr-a's entry was emptied (only owned .data.a) → dropped.
        assert!(
            out.managed_fields()
                .as_array()
                .unwrap()
                .iter()
                .all(|e| e["manager"] != "mgr-a"),
            "mgr-a entry removed after losing its only field"
        );
    }

    // ── ASSOCIATIVE-LIST MERGE BY KEY ──────────────────────────────────

    #[test]
    fn deployment_containers_merge_by_name_and_fieldset_encodes_key() {
        let env = containers_env();
        let current = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web"},
            "spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.27"},
                {"name": "sidecar", "image": "envoy:1.30"}
            ]}}}
        });
        // Apply via SSA the nginx image bump (create branch would also work;
        // here current has no managedFields so no conflict).
        let out = apply_ssa(
            Some(&current),
            &json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.28"}
            ]}}}}),
            "mgr",
            false,
            &deploy_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let containers = out.merged()["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 2, "merge-by-key: no container lost");
        let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
        assert_eq!(sidecar["image"], "envoy:1.30", "sidecar untouched");
        // The fieldset encodes the container by its key tuple
        // (containers[k:{name:nginx}]).
        let fs = owned(&out, "mgr");
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), json!("nginx"));
        assert!(fs.owns(&[
            PathElement::Field("spec".into()),
            PathElement::Field("template".into()),
            PathElement::Field("spec".into()),
            PathElement::Field("containers".into()),
            PathElement::Key(key),
            PathElement::Field("image".into()),
        ]));
    }

    #[test]
    fn reapply_dropping_a_container_removes_the_whole_element_not_an_empty_object() {
        // The Deployment SSA regression: manager owns BOTH nginx + sidecar;
        // a re-apply naming ONLY nginx must REMOVE the sidecar element
        // entirely (not leave a phantom `{}`).
        let env = containers_env();
        let created = apply_ssa(
            None,
            &json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.27"},
                {"name": "sidecar", "image": "envoy:1.30"}
            ]}}}}),
            "mgr",
            false,
            &deploy_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let current = created.merged().clone();

        let out = apply_ssa(
            Some(&current),
            &json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.28"}
            ]}}}}),
            "mgr",
            false,
            &deploy_gvk(),
            &env,
            TIME,
        )
        .unwrap();
        let containers = out.merged()["spec"]["template"]["spec"]["containers"]
            .as_array()
            .unwrap();
        assert_eq!(containers.len(), 1, "sidecar element removed, not emptied");
        assert_eq!(containers[0]["name"], "nginx");
        assert_eq!(containers[0]["image"], "nginx:1.28");
    }

    // ── FieldSet ↔ FieldsV1 round-trip ─────────────────────────────────

    #[test]
    fn fieldset_fields_v1_round_trips_byte_deterministic() {
        let env = containers_env();
        let fs = fieldset_of(
            &json!({"spec": {"template": {"spec": {"containers": [
                {"name": "nginx", "image": "nginx:1.28"}
            ]}}}, "data": {"z": "1", "a": "2"}}),
            &deploy_gvk(),
            &env,
        );
        let v1 = fs.to_fields_v1();
        let back = FieldSet::from_fields_v1(&v1);
        assert_eq!(fs, back, "round-trip preserves the trie");
        // Determinism: two serializations are byte-identical, and BTreeMap
        // ordering means `a` precedes `z` in the rendered JSON.
        let s1 = serde_json::to_string(&v1.0).unwrap();
        let s2 = serde_json::to_string(&fs.to_fields_v1().0).unwrap();
        assert_eq!(s1, s2, "byte-deterministic");
        let ia = s1.find("f:a").unwrap();
        let iz = s1.find("f:z").unwrap();
        assert!(ia < iz, "BTreeMap orders f:a before f:z");
    }

    #[test]
    fn apiserver_owned_metadata_is_excluded_from_ownership() {
        let env = MockPatchEnv::new();
        let fs = fieldset_of(
            &json!({"metadata": {
                "name": "x",
                "labels": {"k": "v"},
                "resourceVersion": "5",
                "uid": "abc",
                "managedFields": [],
                "creationTimestamp": "2026-01-01T00:00:00Z"
            }}),
            &cm_gvk(),
            &env,
        );
        // name + labels.k owned; the apiserver leaves are NOT.
        assert!(fs.owns(&[
            PathElement::Field("metadata".into()),
            PathElement::Field("name".into())
        ]));
        assert!(fs.owns(&[
            PathElement::Field("metadata".into()),
            PathElement::Field("labels".into()),
            PathElement::Field("k".into())
        ]));
        assert!(!fs.owns(&[
            PathElement::Field("metadata".into()),
            PathElement::Field("resourceVersion".into())
        ]));
        assert!(!fs.owns(&[
            PathElement::Field("metadata".into()),
            PathElement::Field("uid".into())
        ]));
    }

    #[test]
    fn conflict_causes_render_typed_array() {
        let c = ApplyConflicts {
            conflicts: vec![Conflict {
                manager: "other".into(),
                field: ".spec.replicas".into(),
            }],
        };
        let causes = c.to_causes();
        let arr = causes.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["type"], "FieldManagerConflict");
        assert_eq!(arr[0]["field"], ".spec.replicas");
        assert!(arr[0]["message"].as_str().unwrap().contains("other"));
    }
}
