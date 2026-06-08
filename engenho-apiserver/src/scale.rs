//! The typed `autoscaling/v1` Scale projection.
//!
//! `/scale` is NOT a stored kind — it is a typed PROJECTION over the
//! parent's `spec.replicas` + `status.replicas` + selector. The same
//! upstream contract applies regardless of the parent's group: an apps/v1
//! Deployment's `/scale` is an `autoscaling/v1` `Scale`, never an apps/v1
//! object. So this type is hand-authored ONCE here (a view, not a cataloged
//! kind with a store), and GVK-tagged `autoscaling/v1`/`Scale` always.
//!
//! ## Round-trip
//!
//!   * `GET /scale`  → [`project_scale`] reads the parent's spec/status/
//!     metadata into a [`Scale`].
//!   * `PUT /scale`  → the handler deserializes the incoming [`Scale`],
//!     takes `spec.replicas`, writes it back to the parent's
//!     `spec.replicas` via a scoped `{"spec":{"replicas":N}}` merge patch,
//!     then re-projects the now-updated parent for the response.
//!
//! Typed serde end-to-end — no `format!()` of the wire (★★ TYPED EMISSION).
//! The selector string is built by the typed [`label_selector_to_string`]
//! helper (sorted, deterministic), never `format!`-concatenated ad hoc.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The `autoscaling/v1` `Scale` object — the projected replica view served
/// at `<plural>/<name>/scale` for scalable kinds (Deployment / ReplicaSet /
/// StatefulSet). GVK-tagged `autoscaling/v1`/`Scale` regardless of the
/// parent's group (the upstream contract).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scale {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    #[serde(default)]
    pub metadata: ScaleMeta,
    #[serde(default)]
    pub spec: ScaleSpec,
    #[serde(default)]
    pub status: ScaleStatus,
}

/// The ObjectMeta subset a `Scale` carries — projected from the parent so
/// the Scale's `resourceVersion` IS the parent's rv (kubectl's
/// `scale --resource-version` CAS works, and a subsequent `PUT /scale`
/// threads the projected rv back as the CAS `expected`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleMeta {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(
        rename = "resourceVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_version: String,
    #[serde(
        rename = "creationTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub creation_timestamp: Option<String>,
}

/// `Scale.spec` — only the desired replica count.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleSpec {
    #[serde(default)]
    pub replicas: i64,
}

/// `Scale.status` — the observed replica count + the selector string.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleStatus {
    #[serde(default)]
    pub replicas: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
}

impl Scale {
    /// The fixed `autoscaling/v1` apiVersion the Scale projection always
    /// carries (independent of the parent's group).
    pub const API_VERSION: &'static str = "autoscaling/v1";
    /// The fixed `Scale` kind.
    pub const KIND: &'static str = "Scale";
}

/// Read an `i64` off a JSON path `obj.<a>.<b>`, defaulting to `default`
/// when the path is absent or not an integer. K8s replica counts are
/// int32 on the wire but parse cleanly as `i64`.
fn read_i64(obj: &Value, a: &str, b: &str, default: i64) -> i64 {
    obj.get(a)
        .and_then(|v| v.get(b))
        .and_then(Value::as_i64)
        .unwrap_or(default)
}

/// Read an optional string off a JSON path `obj.<a>.<b>`.
fn read_str(obj: &Value, a: &str, b: &str) -> Option<String> {
    obj.get(a)
        .and_then(|v| v.get(b))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Project a parent object (a Deployment / ReplicaSet / StatefulSet) into
/// its `autoscaling/v1` Scale view:
///
///   * `spec.replicas`    ← parent `.spec.replicas`   (default `1`).
///   * `status.replicas`  ← parent `.status.replicas` (default `0`).
///   * `status.selector`  ← serialized parent `.spec.selector.matchLabels`
///     (e.g. `"app=web"`); `None` when there is no selector.
///   * `metadata`         ← parent's name / namespace / uid / rv /
///     creationTimestamp (so the Scale's rv is the PARENT's rv).
#[must_use]
pub fn project_scale(parent: &Value) -> Scale {
    let spec_replicas = read_i64(parent, "spec", "replicas", 1);
    let status_replicas = read_i64(parent, "status", "replicas", 0);

    // selector ← .spec.selector.matchLabels serialized as a label string.
    let selector = parent
        .get("spec")
        .and_then(|s| s.get("selector"))
        .and_then(|sel| sel.get("matchLabels"))
        .and_then(label_selector_to_string);

    let metadata = ScaleMeta {
        name: read_str(parent, "metadata", "name").unwrap_or_default(),
        namespace: read_str(parent, "metadata", "namespace"),
        uid: read_str(parent, "metadata", "uid"),
        resource_version: read_str(parent, "metadata", "resourceVersion").unwrap_or_default(),
        creation_timestamp: read_str(parent, "metadata", "creationTimestamp"),
    };

    Scale {
        api_version: Scale::API_VERSION.to_string(),
        kind: Scale::KIND.to_string(),
        metadata,
        spec: ScaleSpec {
            replicas: spec_replicas,
        },
        status: ScaleStatus {
            replicas: status_replicas,
            selector,
        },
    }
}

/// Serialize a `matchLabels` JSON object into the K8s label-selector string
/// form: `k1=v1,k2=v2` with keys sorted for determinism (the same shape
/// kube-apiserver renders into `Scale.status.selector`). Returns `None` for
/// a non-object or an empty selector.
///
/// Typed construction — the pieces are pushed into a `String` via a typed
/// builder loop, NOT a `format!()` of the whole wire string (★★ TYPED
/// EMISSION). Values that are non-strings (a malformed selector) are
/// rendered through their JSON scalar text so the output is never silently
/// wrong.
#[must_use]
pub fn label_selector_to_string(match_labels: &Value) -> Option<String> {
    let obj = match_labels.as_object()?;
    if obj.is_empty() {
        return None;
    }
    // BTreeMap gives a deterministic, sorted key order.
    let mut pairs: std::collections::BTreeMap<&str, String> = std::collections::BTreeMap::new();
    for (k, v) in obj {
        let val = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        pairs.insert(k.as_str(), val);
    }
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parent() -> Value {
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "uid-web-1",
                "resourceVersion": "42",
                "creationTimestamp": "2026-06-07T00:00:00Z"
            },
            "spec": {
                "replicas": 5,
                "selector": { "matchLabels": { "app": "web" } }
            },
            "status": { "replicas": 3 }
        })
    }

    #[test]
    fn project_reads_spec_status_selector_and_meta() {
        let s = project_scale(&parent());
        assert_eq!(s.api_version, "autoscaling/v1");
        assert_eq!(s.kind, "Scale");
        assert_eq!(s.spec.replicas, 5);
        assert_eq!(s.status.replicas, 3);
        assert_eq!(s.status.selector.as_deref(), Some("app=web"));
        assert_eq!(s.metadata.name, "web");
        assert_eq!(s.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(s.metadata.uid.as_deref(), Some("uid-web-1"));
        // The Scale's rv IS the parent's rv (CAS round-trips).
        assert_eq!(s.metadata.resource_version, "42");
        assert_eq!(
            s.metadata.creation_timestamp.as_deref(),
            Some("2026-06-07T00:00:00Z")
        );
    }

    #[test]
    fn project_defaults_replicas_when_absent() {
        // No spec.replicas → default 1; no status.replicas → default 0.
        let p = json!({ "metadata": { "name": "x" }, "spec": {}, "status": {} });
        let s = project_scale(&p);
        assert_eq!(s.spec.replicas, 1, "absent spec.replicas defaults to 1");
        assert_eq!(s.status.replicas, 0, "absent status.replicas defaults to 0");
        assert_eq!(s.status.selector, None, "no selector → None");
    }

    #[test]
    fn label_selector_single_pair() {
        assert_eq!(
            label_selector_to_string(&json!({ "app": "web" })),
            Some("app=web".to_string())
        );
    }

    #[test]
    fn label_selector_multi_pair_is_sorted_deterministic() {
        // Keys sorted so the rendered string is byte-stable across runs.
        let s = label_selector_to_string(&json!({ "tier": "fe", "app": "web" })).unwrap();
        assert_eq!(s, "app=web,tier=fe");
    }

    #[test]
    fn label_selector_empty_or_non_object_is_none() {
        assert_eq!(label_selector_to_string(&json!({})), None);
        assert_eq!(label_selector_to_string(&json!("notanobject")), None);
        assert_eq!(label_selector_to_string(&json!(null)), None);
    }

    #[test]
    fn scale_serializes_with_autoscaling_v1_gvk() {
        let s = project_scale(&parent());
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v.get("apiVersion").unwrap(), "autoscaling/v1");
        assert_eq!(v.get("kind").unwrap(), "Scale");
        assert_eq!(v.get("spec").unwrap().get("replicas").unwrap(), 5);
        assert_eq!(v.get("status").unwrap().get("replicas").unwrap(), 3);
        assert_eq!(v.get("status").unwrap().get("selector").unwrap(), "app=web");
    }

    #[test]
    fn scale_round_trips_through_serde() {
        // The incoming PUT /scale body deserializes to the same Scale.
        let s = project_scale(&parent());
        let wire = serde_json::to_string(&s).unwrap();
        let back: Scale = serde_json::from_str(&wire).unwrap();
        assert_eq!(back.spec.replicas, 5);
        assert_eq!(back.api_version, "autoscaling/v1");
    }
}
