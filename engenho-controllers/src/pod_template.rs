//! Pod-template identity — upstream's `EqualIgnoreHash`.
//!
//! A Deployment finds the `ReplicaSet` that runs its template by COMPARING
//! the two templates, not by comparing a hash of one against a label on the
//! other. Upstream (`deploymentutil.FindNewReplicaSet`) does exactly this:
//! it deep-copies both `PodTemplateSpec`s, deletes the `pod-template-hash`
//! label from each, and runs `apiequality.Semantic.DeepEqual`. The hash is
//! only ever used to NAME a new `ReplicaSet`.
//!
//! ★ WHY NOT THE HASH. A hash over the raw template bytes changes whenever
//! the bytes change, including when the template means the same thing: a
//! `labels: null` the apiserver starts dropping (T4.4), an `env: []` a
//! client starts omitting, a change to the hash function itself. Each of
//! those, under a hash matcher, finds no `ReplicaSet` for EVERY Deployment
//! at once, creates a new one for each, and scales every running one to
//! zero — a cluster-wide rollout nobody asked for. Under this matcher none
//! of them can: two templates that decode to the same `PodTemplateSpec`
//! compare equal, whatever their bytes and whatever their hash.
//!
//! ★ WHAT "THE SAME" MEANS HERE — a SUBSET of upstream's equality, never a
//! superset. Every rule below is one upstream's typed decode + `Semantic`
//! comparison already applies, so this module never calls two templates
//! equal that upstream would call different (a missed rollout is worse
//! than a spurious one):
//!
//! * the template's `pod-template-hash` label is ignored (`EqualIgnoreHash`);
//! * a STRUCT field whose value is `null` is absent — `encoding/json`
//!   decodes a JSON `null` into a nil pointer/map/slice, or leaves any other
//!   field at its zero value, which is what absence does;
//! * a STRUCT field whose value is `[]` is absent — every array in a
//!   `PodTemplateSpec` decodes to a Go slice, and `Semantic.DeepEqual`
//!   treats a nil slice and an empty one as equal;
//! * a MAP field (the closed list [`MAP_FIELDS`]) whose value is `{}` is
//!   absent — nil and empty maps are equal under the same rule — and so is
//!   a `metadata` that is `{}`: both `ObjectMeta`s a pod template can carry
//!   are embedded by value, never by pointer.
//!
//! The ENTRIES of a map are kept verbatim: `labels: {"a": null}` decodes to
//! `{"a": ""}` upstream, which is NOT `{}`. An empty object anywhere else is
//! kept too: most of those are pointers to a struct (`emptyDir: {}`,
//! `securityContext: {}`), where `{}` and absent are different values
//! upstream — `emptyDir: {}` is the volume's whole source.
//!
//! Tier, stated plainly: SOUND but INCOMPLETE, and the soundness rests on
//! one list. Some pairs upstream calls equal still compare unequal here —
//! `hostNetwork: false` vs absent, a `Quantity` of `"1"` vs `"1000m"`, a
//! defaulted vs an undefaulted template. Such a pair rolls out, exactly as
//! it did under the hash, so nothing regresses. The defaulted-vs-undefaulted
//! case is the one that matters: it is closed only when the `apps/v1`
//! template-defaulting arm lands AND the same pod-spec defaulter runs inside
//! [`NormalizedTemplate::of`] on both sides, because a stored `ReplicaSet`
//! template is never re-defaulted. A map field missing from [`MAP_FIELDS`]
//! would have its `null` ENTRIES dropped — a false "equal" only for a map
//! value that is `null`, which T4.4 rejects at the border (422).

use std::fmt;
use std::io;

use serde_json::{Map, Value};

/// The label a `ReplicaSet` carries naming the hash of the template it was
/// created for. Upstream's `apps.DefaultDeploymentUniqueLabelKey`.
pub const POD_TEMPLATE_HASH_LABEL: &str = "pod-template-hash";

/// A pod template reduced to the form two templates are compared in.
///
/// The only constructor is [`NormalizedTemplate::of`], so a comparison of
/// two `NormalizedTemplate`s is always a comparison of normalized forms —
/// there is no way to hand this type raw template bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedTemplate(Value);

impl NormalizedTemplate {
    /// Normalize `template` (a `PodTemplateSpec`: `spec.template` of a
    /// Deployment or a `ReplicaSet`). Total: every JSON value has a
    /// normalized form, and a template that is not an object is compared
    /// as it stands.
    #[must_use]
    pub fn of(template: &Value) -> Self {
        let mut t = template.clone();
        if let Some(labels) = t
            .get_mut("metadata")
            .and_then(|m| m.get_mut("labels"))
            .and_then(Value::as_object_mut)
        {
            labels.remove(POD_TEMPLATE_HASH_LABEL);
        }
        normalize(None, &mut t);
        Self(t)
    }

    /// The `spec.template` of `owner` (a Deployment or a `ReplicaSet`),
    /// normalized. `None` when it declares no template — absent or `null`,
    /// which are the same thing.
    #[must_use]
    pub fn of_spec_template(owner: &Value) -> Option<Self> {
        owner
            .get("spec")
            .and_then(|s| s.get("template"))
            .filter(|t| !t.is_null())
            .map(Self::of)
    }

    /// The hash that NAMES a `ReplicaSet` created for this template (and
    /// fills its `pod-template-hash` label). It identifies nothing: two
    /// templates are the same when they compare equal, never because their
    /// hashes do. Computed over the normalized form, so equal templates
    /// also get equal names.
    ///
    /// `None` only if the normalized value fails to serialize, which a
    /// `serde_json::Value` with string keys does not do.
    #[must_use]
    pub fn naming_hash(&self) -> Option<TemplateHash> {
        let mut fnv = Fnv1a::new();
        serde_json::to_writer(&mut fnv, &self.0).ok()?;
        Some(TemplateHash(fnv.0))
    }
}

/// The name-only hash of a template: FNV-1a over the normalized template's
/// compact JSON, rendered as its top 40 bits in 10 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateHash(u64);

impl fmt::Display for TemplateHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Top 40 bits = the first 10 of the 16 zero-padded hex digits the
        // name has always carried.
        write!(f, "{:010x}", self.0 >> 24)
    }
}

/// FNV-1a, 64-bit, fed as a byte sink so the JSON never has to be
/// materialized to be hashed.
struct Fnv1a(u64);

impl Fnv1a {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }
}

impl io::Write for Fnv1a {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for b in buf {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The fields of a `PodTemplateSpec` that are Go MAPS rather than structs,
/// by name — every name that holds a map anywhere in the tree, and no name
/// that holds a struct object anywhere in it: `ObjectMeta` (the template's
/// and an ephemeral volume claim template's) `labels`/`annotations`;
/// `PodSpec.nodeSelector` and `overhead`; `ResourceRequirements` (a
/// container's, the pod's, a claim template's) `limits`/`requests`;
/// `LabelSelector.matchLabels` (affinity terms, spread constraints, claim
/// selectors); `CSIVolumeSource.volumeAttributes`; `FlexVolumeSource.options`
/// (`PodDNSConfig.options` is an array, never an object).
pub const MAP_FIELDS: &[&str] = &[
    "labels",
    "annotations",
    "nodeSelector",
    "overhead",
    "limits",
    "requests",
    "matchLabels",
    "volumeAttributes",
    "options",
];

fn is_map_field(field: &str) -> bool {
    MAP_FIELDS.contains(&field)
}

/// Normalize `v`, the value of `field` (`None` for the template root and
/// for array elements, which are structs or scalars). A map's entries are
/// kept verbatim; a struct loses every field [`is_absent_field`] names, at
/// every depth. Array elements are normalized but never removed: a slice's
/// length and order are part of its value.
fn normalize(field: Option<&str>, v: &mut Value) {
    match v {
        Value::Object(_) if field.is_some_and(is_map_field) => {}
        Value::Object(fields) => fields.retain(|name, member| {
            normalize(Some(name.as_str()), member);
            !is_absent_field(name, member)
        }),
        Value::Array(items) => items.iter_mut().for_each(|item| normalize(None, item)),
        _ => {}
    }
}

/// A struct field whose value decodes to what its absence decodes to, and
/// compares equal to it under `Semantic.DeepEqual`.
fn is_absent_field(name: &str, v: &Value) -> bool {
    v.is_null()
        || v.as_array().is_some_and(Vec::is_empty)
        || (v.as_object().is_some_and(Map::is_empty) && (is_map_field(name) || name == "metadata"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn web() -> Value {
        json!({
            "metadata": {"labels": {"app": "web"}},
            "spec": {
                "containers": [{"name": "c", "image": "web:1"}]
            }
        })
    }

    fn same(a: &Value, b: &Value) -> bool {
        NormalizedTemplate::of(a) == NormalizedTemplate::of(b)
    }

    #[test]
    fn the_pod_template_hash_label_is_ignored() {
        let mut hashed = web();
        hashed["metadata"]["labels"]["pod-template-hash"] = json!("0123456789");
        assert!(same(&web(), &hashed));
    }

    #[test]
    fn a_label_set_holding_only_the_hash_is_no_label_set() {
        let bare = json!({"spec": {"containers": [{"name": "c"}]}});
        let hashed = json!({
            "metadata": {"labels": {"pod-template-hash": "0123456789"}},
            "spec": {"containers": [{"name": "c"}]}
        });
        assert!(same(&bare, &hashed));
    }

    #[test]
    fn a_null_member_is_an_absent_member_at_any_depth() {
        let mut nulls = web();
        nulls["metadata"]["annotations"] = Value::Null;
        nulls["metadata"]["creationTimestamp"] = Value::Null;
        nulls["spec"]["nodeSelector"] = Value::Null;
        nulls["spec"]["containers"][0]["resources"] = Value::Null;
        assert!(same(&web(), &nulls));
    }

    #[test]
    fn an_empty_list_is_an_absent_list_at_any_depth() {
        let mut empties = web();
        empties["spec"]["volumes"] = json!([]);
        empties["spec"]["containers"][0]["env"] = json!([]);
        empties["spec"]["containers"][0]["args"] = json!([]);
        assert!(same(&web(), &empties));
    }

    #[test]
    fn empty_template_labels_annotations_and_metadata_are_absent() {
        let bare = json!({"spec": {"containers": [{"name": "c"}]}});
        let empty_maps = json!({
            "metadata": {"labels": {}, "annotations": {}},
            "spec": {"containers": [{"name": "c"}]}
        });
        let empty_meta = json!({"metadata": {}, "spec": {"containers": [{"name": "c"}]}});
        assert!(same(&bare, &empty_maps));
        assert!(same(&bare, &empty_meta));
    }

    #[test]
    fn an_empty_map_field_is_absent_at_any_depth() {
        let mut empties = web();
        empties["spec"]["nodeSelector"] = json!({});
        empties["spec"]["containers"][0]["resources"] = json!({"limits": {}, "requests": {}});
        let mut bare_resources = web();
        bare_resources["spec"]["containers"][0]["resources"] = json!({});
        assert!(same(&bare_resources, &empties));
    }

    #[test]
    fn a_null_map_entry_is_an_empty_string_not_an_absent_key() {
        // `map[string]string` decodes `{"tier": null}` to `{"tier": ""}` —
        // a label that exists, with an empty value. Dropping it would
        // call a relabel "no change".
        let mut null_entry = web();
        null_entry["metadata"]["labels"]["tier"] = Value::Null;
        assert!(!same(&web(), &null_entry));

        let mut null_selector = web();
        null_selector["spec"]["nodeSelector"] = json!({"disk": null});
        assert!(!same(&web(), &null_selector));
    }

    #[test]
    fn a_changed_image_is_a_different_template() {
        let mut v2 = web();
        v2["spec"]["containers"][0]["image"] = json!("web:2");
        assert!(!same(&web(), &v2));
    }

    #[test]
    fn a_restart_annotation_is_a_different_template() {
        // `kubectl rollout restart` works by exactly this edit.
        let mut restarted = web();
        restarted["metadata"]["annotations"] =
            json!({"kubectl.kubernetes.io/restartedAt": "2026-09-19T00:00:00Z"});
        assert!(!same(&web(), &restarted));
    }

    #[test]
    fn a_label_other_than_the_hash_is_part_of_the_template() {
        let mut relabelled = web();
        relabelled["metadata"]["labels"]["tier"] = json!("front");
        assert!(!same(&web(), &relabelled));
    }

    #[test]
    fn an_empty_object_that_is_a_pointer_upstream_is_kept() {
        // `emptyDir: {}` IS the volume's source: without it the volume has
        // none. Collapsing `{}` to absent here would hide a real edit.
        let mut with_source = web();
        with_source["spec"]["volumes"] = json!([{"name": "scratch", "emptyDir": {}}]);
        let mut without_source = web();
        without_source["spec"]["volumes"] = json!([{"name": "scratch"}]);
        assert!(!same(&with_source, &without_source));

        let mut sc = web();
        sc["spec"]["securityContext"] = json!({});
        assert!(!same(&web(), &sc));
    }

    #[test]
    fn list_order_and_length_are_part_of_the_template() {
        let two = json!({"spec": {"containers": [{"name": "a"}, {"name": "b"}]}});
        let swapped = json!({"spec": {"containers": [{"name": "b"}, {"name": "a"}]}});
        let with_null = json!({"spec": {"containers": [{"name": "a"}, {"name": "b"}, null]}});
        assert!(!same(&two, &swapped));
        assert!(!same(&two, &with_null));
    }

    #[test]
    fn normalizing_twice_changes_nothing() {
        let messy = json!({
            "metadata": {"labels": {"pod-template-hash": "x"}, "annotations": null},
            "spec": {"containers": [{"name": "c", "env": [], "resources": null}]}
        });
        let once = NormalizedTemplate::of(&messy);
        assert_eq!(NormalizedTemplate::of(&once.0), once);
    }

    #[test]
    fn of_spec_template_reads_absent_and_null_as_no_template() {
        assert!(NormalizedTemplate::of_spec_template(&json!({"spec": {}})).is_none());
        assert!(
            NormalizedTemplate::of_spec_template(&json!({"spec": {"template": null}})).is_none()
        );
        assert!(
            NormalizedTemplate::of_spec_template(&json!({"spec": {"template": web()}})).is_some()
        );
    }

    #[test]
    fn the_naming_hash_is_ten_hex_digits_and_follows_the_template() {
        let h1 = NormalizedTemplate::of(&web())
            .naming_hash()
            .map(|h| h.to_string());
        let mut v2 = web();
        v2["spec"]["containers"][0]["image"] = json!("web:2");
        let h2 = NormalizedTemplate::of(&v2)
            .naming_hash()
            .map(|h| h.to_string());
        let h1 = h1.expect("hashable");
        assert_eq!(h1.len(), 10);
        assert!(
            h1.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_ne!(Some(h1), h2);
    }

    #[test]
    fn equal_templates_get_equal_names() {
        let mut nulls = web();
        nulls["metadata"]["annotations"] = Value::Null;
        nulls["spec"]["volumes"] = json!([]);
        assert_eq!(
            NormalizedTemplate::of(&web()).naming_hash(),
            NormalizedTemplate::of(&nulls).naming_hash()
        );
    }

    #[test]
    fn the_naming_hash_keeps_the_names_it_always_gave() {
        // FNV-1a over the compact JSON, top 10 of 16 hex digits — the
        // rendering the ReplicaSet names have always had, so a template the
        // normalization leaves untouched keeps its old name.
        let t = json!({"spec": {}});
        let bytes = serde_json::to_vec(&t).expect("serializable");
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in &bytes {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        let old: String = format!("{h:016x}").chars().take(10).collect();
        assert_eq!(
            NormalizedTemplate::of(&t)
                .naming_hash()
                .map(|h| h.to_string()),
            Some(old)
        );
    }
}
