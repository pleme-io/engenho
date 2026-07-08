//! `cotejo` (Portuguese: *a collation / side-by-side comparison*) — the diff
//! interpreter. It executes an [`Operation`] against two [`DiffTarget`]s and
//! folds the paired [`Observation`]s through the [`Normalizer`] into a typed
//! [`Verdict`]. The core differ ([`diff_observations`]) is a pure function
//! over two `Observation`s + a `Normalizer` — unit-testable with zero
//! clusters.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::normalize::Normalizer;
use crate::observe::{DiffError, Observation};
use crate::op::{OpKind, Operation};
use crate::path::JsonPath;
use crate::target::DiffTarget;
use crate::verdict::{Divergence, Gvr, Severity, Side, Verdict};

/// Execute `op` on both targets and diff the results (HARD divergences
/// only — the masked comparison). `Parity` is returned iff the divergence
/// set is empty (proof-by-absence).
///
/// # Errors
///
/// Propagates a [`DiffError`] if either target's transport fails.
pub async fn run(
    op: &Operation,
    engenho: &dyn DiffTarget,
    k3s: &dyn DiffTarget,
    norm: &Normalizer,
) -> Result<Verdict, DiffError> {
    let eng_obs = op_observe(op, engenho).await?;
    let k3s_obs = op_observe(op, k3s).await?;
    let divs = diff_observations(op, &eng_obs, &k3s_obs, norm);
    Ok(if divs.is_empty() {
        Verdict::parity()
    } else {
        Verdict::Divergent(divs)
    })
}

/// Like [`run`] but also returns the masked/cosmetic divergences (strict
/// mode) so the masks stay auditable. Returns `(hard, cosmetic)`.
///
/// # Errors
///
/// Propagates a [`DiffError`] if either target's transport fails.
pub async fn run_strict(
    op: &Operation,
    engenho: &dyn DiffTarget,
    k3s: &dyn DiffTarget,
    norm: &Normalizer,
) -> Result<(Vec<Divergence>, Vec<Divergence>), DiffError> {
    let eng_obs = op_observe(op, engenho).await?;
    let k3s_obs = op_observe(op, k3s).await?;
    let hard = diff_observations(op, &eng_obs, &k3s_obs, norm);
    let cosmetic = cosmetic_divergences(op, &eng_obs, &k3s_obs, &hard);
    Ok((hard, cosmetic))
}

async fn op_observe(op: &Operation, target: &dyn DiffTarget) -> Result<Observation, DiffError> {
    target
        .raw(
            op.method,
            &op.path,
            op.body_bytes().as_deref(),
            op.content_type.as_deref(),
        )
        .await
}

/// The pure differ: HARD divergences between two observations under `norm`.
#[must_use]
pub fn diff_observations(
    op: &Operation,
    eng: &Observation,
    k3s: &Observation,
    norm: &Normalizer,
) -> Vec<Divergence> {
    // 1. Status-code layer — a mismatch short-circuits body diffing.
    if eng.status != k3s.status {
        return status_divergence(op, eng.status, k3s.status);
    }
    // Both sides agree on a non-success status ⇒ they agree (e.g. both 404).
    if !eng.is_success() {
        return Vec::new();
    }
    // 2. Body layer — dispatch on the surface class.
    match op.kind {
        OpKind::Object => diff_object_focused(
            &norm.apply(&eng.json),
            &norm.apply(&k3s.json),
            op.focus.as_ref(),
            Severity::Hard,
        ),
        OpKind::List => diff_list(&eng.json, &k3s.json, norm, Severity::Hard),
        OpKind::Discovery => diff_discovery(&eng.json, &k3s.json),
    }
}

/// Diff two (already-normalized) objects, optionally restricted to the
/// subtree at `focus`. FOCUS mode isolates a field-level invariant from a
/// kind's unrelated server-side-defaulting divergence: the diff runs on the
/// focused subtree of BOTH sides (an absent focus on one side surfaces
/// naturally as a `MissingDefault`/`ExtraField` at the focus root). Without a
/// focus it is exactly [`diff_object`] over the whole object.
fn diff_object_focused(
    eng: &Value,
    k3s: &Value,
    focus: Option<&JsonPath>,
    sev: Severity,
) -> Vec<Divergence> {
    match focus {
        None => diff_object(eng, k3s, sev),
        Some(p) => {
            let en = p.get(eng).cloned().unwrap_or(Value::Null);
            let kn = p.get(k3s).cloned().unwrap_or(Value::Null);
            // Re-root the divergence paths at the focus so the report + ratchet
            // signature name the real location, not `.`.
            diff_object(&en, &kn, sev)
                .into_iter()
                .map(|d| d.reroot(p))
                .collect()
        }
    }
}

/// Masked (cosmetic) divergences: those present in the RAW comparison but
/// suppressed by the normalizer. Surfaced so a mask can never silently hide
/// a real gap.
#[must_use]
fn cosmetic_divergences(
    op: &Operation,
    eng: &Observation,
    k3s: &Observation,
    hard: &[Divergence],
) -> Vec<Divergence> {
    if eng.status != k3s.status || !eng.is_success() {
        return Vec::new();
    }
    let raw = match op.kind {
        OpKind::Object => {
            diff_object_focused(&eng.json, &k3s.json, op.focus.as_ref(), Severity::Cosmetic)
        }
        OpKind::List => diff_list_raw(&eng.json, &k3s.json, Severity::Cosmetic),
        OpKind::Discovery => Vec::new(),
    };
    let hard_sigs: BTreeSet<String> = hard.iter().map(Divergence::signature).collect();
    raw.into_iter()
        .filter(|d| !hard_sigs.contains(&d.signature()))
        .collect()
}

fn status_divergence(op: &Operation, eng: u16, k3s: u16) -> Vec<Divergence> {
    let eng_ok = (200..300).contains(&eng);
    let k3s_ok = (200..300).contains(&k3s);
    // A verb probe whose two sides disagree on success ⇒ a refused verb.
    if let Some((gvr, verb)) = &op.verb_probe {
        if eng_ok != k3s_ok {
            let present_on = if k3s_ok { Side::K3s } else { Side::Engenho };
            return vec![Divergence::MissingVerb {
                gvr: gvr.clone(),
                verb: *verb,
                present_on,
            }];
        }
    }
    // A 404-vs-success (no verb probe) ⇒ a missing resource.
    if eng == 404 && k3s_ok {
        return vec![Divergence::MissingResource {
            path: JsonPath::root(),
            present_on: Side::K3s,
        }];
    }
    if k3s == 404 && eng_ok {
        return vec![Divergence::MissingResource {
            path: JsonPath::root(),
            present_on: Side::Engenho,
        }];
    }
    vec![Divergence::StatusCodeDiff {
        op: op.id.clone(),
        method: op.method,
        engenho: eng,
        k3s,
    }]
}

/// Recursive object differ. `k3s` is the oracle: a key on k3s that engenho
/// lacks is a `MissingDefault`; a key engenho adds that k3s lacks is an
/// `ExtraField`; a shared key with differing values is a `FieldDiff`.
fn diff_object(eng: &Value, k3s: &Value, sev: Severity) -> Vec<Divergence> {
    let mut out = Vec::new();
    walk(JsonPath::root(), eng, k3s, sev, &mut out);
    out
}

fn walk(path: JsonPath, eng: &Value, k3s: &Value, sev: Severity, out: &mut Vec<Divergence>) {
    match (eng, k3s) {
        (Value::Object(e), Value::Object(k)) => {
            for (key, kv) in k {
                match e.get(key) {
                    None => out.push(Divergence::MissingDefault {
                        path: path.clone().key(key.clone()),
                        k3s_value: kv.clone(),
                        severity: sev,
                    }),
                    Some(ev) => walk(path.clone().key(key.clone()), ev, kv, sev, out),
                }
            }
            for (key, ev) in e {
                if !k.contains_key(key) {
                    out.push(Divergence::ExtraField {
                        path: path.clone().key(key.clone()),
                        side: Side::Engenho,
                        value: ev.clone(),
                        severity: sev,
                    });
                }
            }
        }
        (Value::Array(e), Value::Array(k)) => {
            if e.len() == k.len() {
                for (i, (ev, kv)) in e.iter().zip(k.iter()).enumerate() {
                    walk(path.clone().index(i), ev, kv, sev, out);
                }
            } else {
                out.push(Divergence::FieldDiff {
                    path,
                    engenho: eng.clone(),
                    k3s: k3s.clone(),
                    severity: sev,
                });
            }
        }
        (e, k) if e == k => {}
        (e, k) => out.push(Divergence::FieldDiff {
            path,
            engenho: e.clone(),
            k3s: k.clone(),
            severity: sev,
        }),
    }
}

/// List differ: match items by `metadata.name`, normalize + diff each, and
/// flag names present on only one side. Isolates the comparison from
/// cluster-auto-created objects.
fn diff_list(eng: &Value, k3s: &Value, norm: &Normalizer, sev: Severity) -> Vec<Divergence> {
    let mut out = Vec::new();
    // Envelope kind/apiVersion.
    envelope_check(eng, k3s, sev, &mut out);
    let eng_items = items_by_name(eng);
    let k3s_items = items_by_name(k3s);
    let names: BTreeSet<&String> = eng_items.keys().chain(k3s_items.keys()).collect();
    for name in names {
        match (eng_items.get(name), k3s_items.get(name)) {
            (Some(e), Some(k)) => {
                out.extend(diff_object(&norm.apply(e), &norm.apply(k), sev));
            }
            (None, Some(_)) => out.push(Divergence::MissingResource {
                path: JsonPath::root().key("items").key((*name).clone()),
                present_on: Side::K3s,
            }),
            (Some(_), None) => out.push(Divergence::MissingResource {
                path: JsonPath::root().key("items").key((*name).clone()),
                present_on: Side::Engenho,
            }),
            (None, None) => {}
        }
    }
    out
}

/// Raw (unmasked) list diff for the cosmetic pass — per-item without
/// normalization.
fn diff_list_raw(eng: &Value, k3s: &Value, sev: Severity) -> Vec<Divergence> {
    let mut out = Vec::new();
    let eng_items = items_by_name(eng);
    let k3s_items = items_by_name(k3s);
    for (name, e) in &eng_items {
        if let Some(k) = k3s_items.get(name) {
            out.extend(diff_object(e, k, sev));
        }
    }
    out
}

fn envelope_check(eng: &Value, k3s: &Value, sev: Severity, out: &mut Vec<Divergence>) {
    for field in ["kind", "apiVersion"] {
        let ev = eng.get(field);
        let kv = k3s.get(field);
        if ev != kv {
            match (ev, kv) {
                (None, Some(k)) => out.push(Divergence::MissingDefault {
                    path: JsonPath::root().key(field),
                    k3s_value: k.clone(),
                    severity: sev,
                }),
                (Some(e), None) => out.push(Divergence::ExtraField {
                    path: JsonPath::root().key(field),
                    side: Side::Engenho,
                    value: e.clone(),
                    severity: sev,
                }),
                (Some(e), Some(k)) => out.push(Divergence::FieldDiff {
                    path: JsonPath::root().key(field),
                    engenho: e.clone(),
                    k3s: k.clone(),
                    severity: sev,
                }),
                (None, None) => {}
            }
        }
    }
}

fn items_by_name(list: &Value) -> std::collections::BTreeMap<String, Value> {
    let mut m = std::collections::BTreeMap::new();
    if let Some(items) = list.get("items").and_then(Value::as_array) {
        for it in items {
            let name = it
                .get("metadata")
                .and_then(|md| md.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            m.insert(name, it.clone());
        }
    }
    m
}

/// Discovery differ — compares the `configmaps` and `namespaces` entries in
/// a core/v1 `APIResourceList`, plus the namespace subresources.
fn diff_discovery(eng: &Value, k3s: &Value) -> Vec<Divergence> {
    let mut out = Vec::new();
    let eng_res = resources_by_name(eng);
    let k3s_res = resources_by_name(k3s);
    for name in ["configmaps", "namespaces"] {
        match (eng_res.get(name), k3s_res.get(name)) {
            (Some(e), Some(k)) => diff_resource_entry(name, e, k, &mut out),
            (None, Some(_)) => out.push(Divergence::MissingResource {
                path: JsonPath::root().key("resources").key(name),
                present_on: Side::K3s,
            }),
            (Some(_), None) => out.push(Divergence::MissingResource {
                path: JsonPath::root().key("resources").key(name),
                present_on: Side::Engenho,
            }),
            (None, None) => {}
        }
    }
    // Namespace subresources k3s exposes.
    for sub in ["namespaces/status", "namespaces/finalize"] {
        let on_eng = eng_res.contains_key(sub);
        let on_k3s = k3s_res.contains_key(sub);
        if on_k3s && !on_eng {
            out.push(Divergence::MissingSubresource {
                gvr: Gvr {
                    group: String::new(),
                    version: "v1".into(),
                    resource: "namespaces".into(),
                },
                sub: sub.rsplit('/').next().unwrap_or(sub).to_string(),
                present_on: Side::K3s,
            });
        }
    }
    out
}

fn resources_by_name(list: &Value) -> std::collections::BTreeMap<String, Value> {
    let mut m = std::collections::BTreeMap::new();
    if let Some(resources) = list.get("resources").and_then(Value::as_array) {
        for r in resources {
            if let Some(name) = r.get("name").and_then(Value::as_str) {
                m.insert(name.to_string(), r.clone());
            }
        }
    }
    m
}

fn diff_resource_entry(name: &str, eng: &Value, k3s: &Value, out: &mut Vec<Divergence>) {
    // namespaced flag.
    let e_ns = eng.get("namespaced").and_then(Value::as_bool);
    let k_ns = k3s.get("namespaced").and_then(Value::as_bool);
    if e_ns != k_ns {
        let mut detail = String::from(name);
        detail.push_str(": namespaced engenho=");
        detail.push_str(&fmt_opt_bool(e_ns));
        detail.push_str(" k3s=");
        detail.push_str(&fmt_opt_bool(k_ns));
        out.push(Divergence::DiscoveryDiff {
            group: String::new(),
            version: "v1".into(),
            detail,
        });
    }
    // verb set differences.
    let e_verbs = str_set(eng.get("verbs"));
    let k_verbs = str_set(k3s.get("verbs"));
    let missing: Vec<&String> = k_verbs.difference(&e_verbs).collect();
    let extra: Vec<&String> = e_verbs.difference(&k_verbs).collect();
    if !missing.is_empty() {
        let mut detail = String::from(name);
        detail.push_str(": engenho missing verbs ");
        detail.push_str(&join_refs(&missing));
        out.push(Divergence::DiscoveryDiff {
            group: String::new(),
            version: "v1".into(),
            detail,
        });
    }
    if !extra.is_empty() {
        let mut detail = String::from(name);
        detail.push_str(": engenho extra verbs ");
        detail.push_str(&join_refs(&extra));
        out.push(Divergence::DiscoveryDiff {
            group: String::new(),
            version: "v1".into(),
            detail,
        });
    }
    // shortNames set differences (informational).
    let e_short = str_set(eng.get("shortNames"));
    let k_short = str_set(k3s.get("shortNames"));
    if e_short != k_short {
        let mut detail = String::from(name);
        detail.push_str(": shortNames differ (engenho ");
        detail.push_str(&join_set(&e_short));
        detail.push_str(", k3s ");
        detail.push_str(&join_set(&k_short));
        detail.push(')');
        out.push(Divergence::DiscoveryDiff {
            group: String::new(),
            version: "v1".into(),
            detail,
        });
    }
}

fn fmt_opt_bool(b: Option<bool>) -> String {
    match b {
        Some(true) => "true".into(),
        Some(false) => "false".into(),
        None => "unset".into(),
    }
}

fn str_set(v: Option<&Value>) -> BTreeSet<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn join_refs(items: &[&String]) -> String {
    let mut out = String::from("[");
    for (i, s) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(s);
    }
    out.push(']');
    out
}

fn join_set(items: &BTreeSet<String>) -> String {
    let refs: Vec<&String> = items.iter().collect();
    join_refs(&refs)
}

/// The single crate-internal Parity constructor — see [`Verdict`].
impl Verdict {
    pub(crate) fn parity() -> Self {
        Self::Parity(crate::verdict::ParityWitness::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::volatile_meta;
    use serde_json::json;

    fn obs(status: u16, json: Value) -> Observation {
        Observation {
            status,
            content_type: Some("application/json".into()),
            json,
        }
    }

    fn get_cm() -> Operation {
        Operation::get_configmap("ns", "cm")
    }

    #[test]
    fn identical_objects_are_parity() {
        let norm = volatile_meta();
        let body = json!({"kind": "ConfigMap", "metadata": {"name": "cm"}, "data": {"a": "b"}});
        let d = diff_observations(&get_cm(), &obs(200, body.clone()), &obs(200, body), &norm);
        assert!(d.is_empty(), "identical bodies must be parity, got {d:?}");
    }

    #[test]
    fn masked_fields_do_not_diverge() {
        let norm = volatile_meta();
        let eng = json!({"metadata": {"name": "cm", "resourceVersion": "7"}, "data": {"a": "b"}});
        let k3s = json!({"metadata": {"name": "cm", "resourceVersion": "90210", "uid": "abc",
            "creationTimestamp": "2026-01-01T00:00:00Z"}, "data": {"a": "b"}});
        let d = diff_observations(&get_cm(), &obs(200, eng), &obs(200, k3s), &norm);
        assert!(d.is_empty(), "volatile meta must be masked, got {d:?}");
    }

    #[test]
    fn missing_default_and_extra_field_detected() {
        let norm = volatile_meta();
        // k3s has spec.finalizers; engenho has metadata.finalizers instead.
        let eng = json!({"metadata": {"name": "ns", "finalizers": ["kubernetes"]}});
        let k3s = json!({"metadata": {"name": "ns", "labels": {"kubernetes.io/metadata.name": "ns"}},
            "spec": {"finalizers": ["kubernetes"]}});
        let d = diff_observations(
            &Operation::get_namespace("ns"),
            &obs(200, eng),
            &obs(200, k3s),
            &norm,
        );
        let sigs: BTreeSet<String> = d.iter().map(Divergence::signature).collect();
        assert!(
            sigs.contains("MissingDefault:.spec"),
            "want MissingDefault .spec, got {sigs:?}"
        );
        assert!(sigs.contains("MissingDefault:.metadata.labels"));
        assert!(sigs.contains("ExtraField:.metadata.finalizers:engenho"));
    }

    #[test]
    fn put_verb_probe_becomes_missing_verb() {
        let norm = volatile_meta();
        let op = Operation::replace_configmap("ns", "cm", json!({}));
        // engenho 400 (router refuses PUT), k3s 200.
        let d = diff_observations(&op, &obs(400, json!({})), &obs(200, json!({})), &norm);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].signature(), "MissingVerb:v1/configmaps:update:k3s");
        assert_eq!(d[0].owning_crate(), "engenho-apiserver");
    }

    #[test]
    fn agreeing_status_is_parity() {
        let norm = volatile_meta();
        // Both 404 ⇒ they agree, no divergence.
        let d = diff_observations(&get_cm(), &obs(404, json!({})), &obs(404, json!({})), &norm);
        assert!(d.is_empty());
        // Different status ⇒ StatusCodeDiff.
        let op = Operation::merge_patch_configmap("ns", "cm", json!({}));
        let d = diff_observations(&op, &obs(500, json!({})), &obs(200, json!({})), &norm);
        assert_eq!(d.len(), 1);
        assert!(matches!(d[0], Divergence::StatusCodeDiff { .. }));
    }

    #[test]
    fn discovery_missing_verbs_detected() {
        let eng = json!({"resources": [
            {"name": "configmaps", "namespaced": true, "kind": "ConfigMap", "verbs": ["get", "list", "create"]},
            {"name": "namespaces", "namespaced": false, "kind": "Namespace", "verbs": ["get", "list"]},
        ]});
        let k3s = json!({"resources": [
            {"name": "configmaps", "namespaced": true, "kind": "ConfigMap", "verbs": ["get", "list", "create", "update", "patch", "delete"], "shortNames": ["cm"]},
            {"name": "namespaces", "namespaced": false, "kind": "Namespace", "verbs": ["get", "list"], "shortNames": ["ns"]},
            {"name": "namespaces/status", "namespaced": false, "kind": "Namespace"},
        ]});
        let op = Operation::discovery_core_v1();
        let d = diff_observations(&op, &obs(200, eng), &obs(200, k3s), &Normalizer::new());
        let sigs: Vec<String> = d.iter().map(Divergence::signature).collect();
        assert!(
            sigs.iter().any(|s| s.contains("engenho missing verbs")),
            "want a missing-verbs discovery diff, got {sigs:?}"
        );
        assert!(
            d.iter()
                .any(|x| matches!(x, Divergence::MissingSubresource { .. })),
            "want namespaces/status subresource gap, got {d:?}"
        );
    }

    // Two MockKubeClient instances (zero clusters) exercised through the
    // differ: identical objects ⇒ parity, a mutated field ⇒ FieldDiff.
    #[tokio::test]
    async fn two_mock_clients_feed_the_differ() {
        use engenho_kube_client::mock::MockKubeClient;
        use engenho_types::client::KubeClient;

        let a = MockKubeClient::new();
        let b = MockKubeClient::new();
        let pod = toy::pod("p", "default", "img:1");
        let ca = a.create(&pod).await.unwrap();
        let cb = b.create(&pod).await.unwrap();
        let norm = volatile_meta();
        let d = diff_observations(
            &get_cm(),
            &obs(200, serde_json::to_value(&ca).unwrap()),
            &obs(200, serde_json::to_value(&cb).unwrap()),
            &norm,
        );
        assert!(
            d.is_empty(),
            "identical mock objects must be parity, got {d:?}"
        );

        // Mutate b's image ⇒ a real FieldDiff at .spec.image.
        let cb2 = b.create(&toy::pod("p2", "default", "img:2")).await.unwrap();
        let ca2 = a.create(&toy::pod("p2", "default", "img:1")).await.unwrap();
        let d2 = diff_observations(
            &get_cm(),
            &obs(200, serde_json::to_value(&ca2).unwrap()),
            &obs(200, serde_json::to_value(&cb2).unwrap()),
            &norm,
        );
        assert!(
            d2.iter().any(|x| matches!(x, Divergence::FieldDiff { .. })),
            "want a FieldDiff on the mutated image, got {d2:?}"
        );
    }

    /// A minimal typed resource for the two-mock-clients test.
    mod toy {
        use engenho_types::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
        pub struct ToyPod {
            pub metadata: engenho_types::meta::ObjectMeta,
            pub spec: ToySpec,
        }
        #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
        pub struct ToySpec {
            pub image: String,
        }
        impl KubeResource for ToyPod {
            const GVK: GroupVersionKind = GroupVersionKind {
                group: "",
                version: "v1",
                kind: "ToyPod",
            };
            const GVR: GroupVersionResource = GroupVersionResource {
                group: "",
                version: "v1",
                resource: "toypods",
            };
            const SCOPE: Scope = Scope::Namespaced;
            fn name(&self) -> std::borrow::Cow<'_, str> {
                self.metadata.name.as_str().into()
            }
            fn namespace(&self) -> Option<std::borrow::Cow<'_, str>> {
                self.metadata.namespace.as_deref().map(Into::into)
            }
            fn resource_version(&self) -> Option<std::borrow::Cow<'_, str>> {
                if self.metadata.resource_version.is_empty() {
                    None
                } else {
                    Some(self.metadata.resource_version.as_str().into())
                }
            }
        }
        pub fn pod(name: &str, ns: &str, image: &str) -> ToyPod {
            ToyPod {
                metadata: engenho_types::meta::ObjectMeta {
                    name: name.into(),
                    namespace: Some(ns.into()),
                    ..Default::default()
                },
                spec: ToySpec {
                    image: image.into(),
                },
            }
        }
    }
}
