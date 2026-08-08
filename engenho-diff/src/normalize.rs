//! The [`Normalizer`] — a typed, ordered list of [`Mask`]s that strips
//! legitimately-varying fields BEFORE the differ runs. Without it every
//! diff is ~100% noise (uid/resourceVersion/timestamps differ on every
//! object). The masks are auditable: strict mode (see [`crate::cotejo`])
//! re-diffs the RAW objects and re-reports whatever a mask suppressed as
//! `Severity::Cosmetic`, so a mask can never silently hide a real gap.

use serde_json::Value;

use crate::path::{JsonPath, PathSeg};

/// A pure canonicalization function applied at a path.
pub type CanonFn = fn(&Value) -> Value;

/// One normalization step.
#[derive(Clone)]
pub enum Mask {
    /// Remove the value at a path (drop the whole subtree).
    Drop(JsonPath),
    /// Replace the value at a path with a canonical form.
    Canonicalize(JsonPath, CanonFn),
    /// Sort the array at a path by an object key (stable ordering).
    Sort(JsonPath, String),
}

/// An ordered set of masks.
#[derive(Clone, Default)]
pub struct Normalizer {
    masks: Vec<Mask>,
}

impl Normalizer {
    /// An empty normalizer (identity).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a mask (builder style).
    #[must_use]
    pub fn with(mut self, m: Mask) -> Self {
        self.masks.push(m);
        self
    }

    /// The masks, in order.
    #[must_use]
    pub fn masks(&self) -> &[Mask] {
        &self.masks
    }

    /// Apply every mask in order, returning the normalized clone.
    #[must_use]
    pub fn apply(&self, v: &Value) -> Value {
        let mut out = v.clone();
        for m in &self.masks {
            match m {
                Mask::Drop(p) => walk_drop(&mut out, p.segs()),
                Mask::Canonicalize(p, f) => walk_canon(&mut out, p.segs(), *f),
                Mask::Sort(p, key) => walk_sort(&mut out, p.segs(), key),
            }
        }
        out
    }
}

fn walk_drop(v: &mut Value, segs: &[PathSeg]) {
    let Some((seg, rest)) = segs.split_first() else {
        return;
    };
    if rest.is_empty() {
        match seg {
            PathSeg::Key(k) => {
                if let Some(o) = v.as_object_mut() {
                    o.remove(k);
                }
            }
            PathSeg::Index(i) => {
                if let Some(a) = v.as_array_mut() {
                    if *i < a.len() {
                        a.remove(*i);
                    }
                }
            }
            PathSeg::Wildcard => {
                if let Some(a) = v.as_array_mut() {
                    a.clear();
                }
            }
        }
        return;
    }
    descend_each(v, seg, |child| walk_drop(child, rest));
}

fn walk_canon(v: &mut Value, segs: &[PathSeg], f: CanonFn) {
    let Some((seg, rest)) = segs.split_first() else {
        *v = f(v);
        return;
    };
    if rest.is_empty() {
        match seg {
            PathSeg::Key(k) => {
                if let Some(child) = v.as_object_mut().and_then(|o| o.get_mut(k)) {
                    *child = f(child);
                }
            }
            PathSeg::Index(i) => {
                if let Some(child) = v.as_array_mut().and_then(|a| a.get_mut(*i)) {
                    *child = f(child);
                }
            }
            PathSeg::Wildcard => {
                if let Some(a) = v.as_array_mut() {
                    for child in a.iter_mut() {
                        *child = f(child);
                    }
                }
            }
        }
        return;
    }
    descend_each(v, seg, |child| walk_canon(child, rest, f));
}

fn walk_sort(v: &mut Value, segs: &[PathSeg], key: &str) {
    let Some((seg, rest)) = segs.split_first() else {
        sort_array_by_key(v, key);
        return;
    };
    if rest.is_empty() {
        match seg {
            PathSeg::Key(k) => {
                if let Some(child) = v.as_object_mut().and_then(|o| o.get_mut(k)) {
                    sort_array_by_key(child, key);
                }
            }
            PathSeg::Index(i) => {
                if let Some(child) = v.as_array_mut().and_then(|a| a.get_mut(*i)) {
                    sort_array_by_key(child, key);
                }
            }
            PathSeg::Wildcard => {
                if let Some(a) = v.as_array_mut() {
                    for child in a.iter_mut() {
                        sort_array_by_key(child, key);
                    }
                }
            }
        }
        return;
    }
    descend_each(v, seg, |child| walk_sort(child, rest, key));
}

fn descend_each(v: &mut Value, seg: &PathSeg, mut f: impl FnMut(&mut Value)) {
    match seg {
        PathSeg::Key(k) => {
            if let Some(child) = v.as_object_mut().and_then(|o| o.get_mut(k)) {
                f(child);
            }
        }
        PathSeg::Index(i) => {
            if let Some(child) = v.as_array_mut().and_then(|a| a.get_mut(*i)) {
                f(child);
            }
        }
        PathSeg::Wildcard => {
            if let Some(a) = v.as_array_mut() {
                for child in a.iter_mut() {
                    f(child);
                }
            }
        }
    }
}

fn sort_array_by_key(v: &mut Value, key: &str) {
    if let Some(arr) = v.as_array_mut() {
        arr.sort_by(|a, b| {
            let ak = a.get(key).and_then(Value::as_str).unwrap_or_default();
            let bk = b.get(key).and_then(Value::as_str).unwrap_or_default();
            ak.cmp(bk)
        });
    }
}

/// The canonical profile: strip the fields that legitimately vary between
/// any two conformant apiservers, so what remains is genuine divergence.
///
/// Deliberately does NOT mask `spec.finalizers`, `metadata.finalizers`,
/// `metadata.labels`, or `data` — those carry the real conformance signal
/// (server-side defaulting).
///
/// `status` is **collapsed to a presence marker, not dropped** (corrected
/// 2026-08-08). Its contents are genuinely time-varying, so they stay out of
/// the diff; but dropping the subtree outright made the harness unable to see
/// that engenho returns **no `status` at all** — its single largest
/// divergence. A mask that hides the biggest gap is not a mask, it is a
/// blindfold. Strict mode still re-surfaces what any mask suppressed as
/// `Cosmetic`.
#[must_use]
pub fn volatile_meta() -> Normalizer {
    Normalizer::new()
        // Identity / bookkeeping metadata — opaque + server-assigned.
        .with(Mask::Drop(JsonPath::parse("metadata.uid")))
        .with(Mask::Drop(JsonPath::parse("metadata.resourceVersion")))
        .with(Mask::Drop(JsonPath::parse("metadata.generation")))
        .with(Mask::Drop(JsonPath::parse("metadata.creationTimestamp")))
        .with(Mask::Drop(JsonPath::parse("metadata.selfLink")))
        // Server-side-apply bookkeeping (engenho stamps it only on apply;
        // k3s stamps it on every write). The timestamps within are the
        // volatile part; the whole block is dropped for a clean diff.
        .with(Mask::Drop(JsonPath::parse("metadata.managedFields")))
        // Status: COLLAPSED to a presence marker, never dropped.
        //
        // This used to be `Mask::Drop(status)` with the note "time-varying by
        // construction". The volatility is real — podIP, conditions,
        // lastTransitionTime and phase all move under you — but dropping the
        // whole subtree made the harness blind to the single largest
        // divergence engenho has: **it returns no `status` key at all**
        // (measured 2026-08-08: a created Pod's top-level keys are
        // [apiVersion, kind, metadata, spec] against a conformant server's
        // [apiVersion, kind, metadata, spec, status]).
        //
        // An instrument that cannot see the biggest gap is not an instrument.
        // Collapsing to a constant keeps every volatile leaf out of the diff
        // while leaving PRESENCE observable: both sides having a status is
        // parity, one side missing it is a divergence.
        .with(Mask::Canonicalize(JsonPath::parse("status"), |_| {
            Value::String("<present>".to_owned())
        }))
        // Allocated network identity (Service).
        .with(Mask::Drop(JsonPath::parse("spec.clusterIP")))
        .with(Mask::Drop(JsonPath::parse("spec.clusterIPs")))
        // Scheduler-assigned placement (Pod).
        .with(Mask::Drop(JsonPath::parse("spec.nodeName")))
        // List envelope continuation token.
        .with(Mask::Drop(JsonPath::parse("metadata.continue")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn drop_removes_nested_key() {
        let mut norm = Normalizer::new().with(Mask::Drop(JsonPath::parse("metadata.uid")));
        let v = json!({"metadata": {"name": "x", "uid": "abc"}});
        let out = norm.apply(&v);
        assert_eq!(out, json!({"metadata": {"name": "x"}}));
        // untouched clone
        assert!(v["metadata"]["uid"].is_string());
        norm = norm.with(Mask::Drop(JsonPath::parse("status")));
        let out2 = norm.apply(&json!({"metadata": {"uid": "z"}, "status": {"phase": "Active"}}));
        assert_eq!(out2, json!({"metadata": {}}));
    }

    #[test]
    fn wildcard_drops_time_in_each_element() {
        let norm = Normalizer::new().with(Mask::Drop(JsonPath::parse(
            "metadata.managedFields[*].time",
        )));
        let v = json!({"metadata": {"managedFields": [
            {"manager": "a", "time": "t1"},
            {"manager": "b", "time": "t2"},
        ]}});
        let out = norm.apply(&v);
        assert_eq!(
            out,
            json!({"metadata": {"managedFields": [{"manager": "a"}, {"manager": "b"}]}})
        );
    }

    #[test]
    fn volatile_meta_leaves_finalizers_and_data() {
        let norm = volatile_meta();
        let v = json!({
            "metadata": {"name": "ns", "uid": "u", "finalizers": ["kubernetes"]},
            "spec": {"finalizers": ["kubernetes"]},
            "data": {"k": "v"},
            "status": {"phase": "Active"},
        });
        let out = norm.apply(&v);
        assert!(out["metadata"]["finalizers"].is_array());
        assert!(out["spec"]["finalizers"].is_array());
        assert_eq!(out["data"]["k"], json!("v"));
        // Status is COLLAPSED, not dropped: its volatile contents are gone but
        // its PRESENCE survives, so "engenho returned no status at all" is a
        // divergence the harness can see. Asserting `is_none()` here was the
        // unit-test half of the blindness — it pinned the very behaviour that
        // made the largest gap unobservable.
        assert_eq!(
            out["status"],
            json!("<present>"),
            "a present status must collapse to the marker, never vanish",
        );
        assert!(out["metadata"].get("uid").is_none());
    }

    /// The point of the collapse, stated as a test: an object WITHOUT a status
    /// must not normalize to the same value as one WITH a status.
    ///
    /// Measured 2026-08-08: engenho returns `[apiVersion, kind, metadata,
    /// spec]` for a created Pod where a conformant server returns those plus
    /// `status`. Under the old `Mask::Drop("status")` both sides normalized
    /// identically and the harness reported parity — it could not have failed.
    #[test]
    fn a_missing_status_is_distinguishable_from_a_present_one() {
        let norm = volatile_meta();
        let with = norm.apply(&json!({"kind":"Pod","status":{"phase":"Running"}}));
        let without = norm.apply(&json!({"kind":"Pod"}));
        assert_ne!(
            with, without,
            "an absent status MUST diverge from a present one — this is the \
             single largest engenho gap and the harness has to be able to see it",
        );
        assert!(without.get("status").is_none());
    }
}
