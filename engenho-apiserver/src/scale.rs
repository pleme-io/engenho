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
//! ## A replica count has three states (T1.6)
//!
//! Both counts are read through [`engenho_controllers::int_at`], which is
//! [`engenho_types::SpecInt`] underneath: absent (or `null`), an integer,
//! or something else. The old reader folded the third into the first, so a
//! parent declaring `spec.replicas: "3"` projected as a Scale of `1` — an
//! answer about an object nobody declared. Now:
//!
//!   * absent → the field's API default. `spec.replicas` takes
//!     [`engenho_controllers::REPLICAS`], the one default every workload
//!     controller also reads, so the Scale and the controller acting on it
//!     cannot disagree about what an absent count means;
//!   * not an integer → [`UnprojectableScale`], a 500 naming the field.
//!     Upstream never stores such a parent (decoding into `int32` refuses
//!     it); where it can project from an untyped object — a custom
//!     resource's scale subresource — an accessor error there is a 500
//!     too.
//!
//! Tier-honest: the three states are a type, and this module cannot build a
//! [`Scale`] from a count it could not read. That no OTHER reader in the
//! crate calls `as_i64` directly is review, not a type.
//!
//! Typed serde end-to-end — no `format!()` of the wire (★★ TYPED EMISSION).
//! The selector string is built by the typed [`label_selector_to_string`]
//! helper (sorted, deterministic), never `format!`-concatenated ad hoc.

use engenho_controllers::{DefaultedInt, REPLICAS, ShapeError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

/// The `autoscaling/v1` `Scale` object — the projected replica view served
/// at `<plural>/<name>/scale` for scalable kinds (`Deployment` /
/// `ReplicaSet` / `StatefulSet`). GVK-tagged `autoscaling/v1`/`Scale` regardless of the
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

/// The `ObjectMeta` subset a `Scale` carries — projected from the parent so
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

/// `status.replicas` of a scalable parent: absent means no replica has been
/// observed yet, so `0` (upstream's `int32` status count, omitted when
/// zero).
const STATUS_REPLICAS: DefaultedInt = DefaultedInt::new(&["status", "replicas"], 0);

/// A parent whose replica counts cannot be read, so no [`Scale`] can be
/// projected from it. The [`ShapeError`] names the field and what stands
/// there.
///
/// Answered as a 500: the client sent nothing wrong, the server holds an
/// object it cannot project. Never answered with the field's default.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no Scale can be projected from this object: {0}")]
pub struct UnprojectableScale(ShapeError);

impl UnprojectableScale {
    /// The field that could not be read, and what stands there.
    #[must_use]
    pub fn shape(&self) -> &ShapeError {
        &self.0
    }
}

impl From<UnprojectableScale> for ApiError {
    fn from(e: UnprojectableScale) -> Self {
        ApiError::Internal(e.to_string())
    }
}

/// Read an optional string off a JSON path `obj.<a>.<b>`.
fn read_str(obj: &Value, a: &str, b: &str) -> Option<String> {
    obj.get(a)
        .and_then(|v| v.get(b))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Project a parent object (a `Deployment` / `ReplicaSet` / `StatefulSet`) into
/// its `autoscaling/v1` Scale view:
///
///   * `spec.replicas`    ← parent `.spec.replicas`; absent or `null` is
///     [`REPLICAS`]' API default (`1`).
///   * `status.replicas`  ← parent `.status.replicas`; absent or `null` is
///     `0`.
///   * `status.selector`  ← serialized parent `.spec.selector.matchLabels`
///     (e.g. `"app=web"`); `None` when there is no selector.
///   * `metadata`         ← parent's name / namespace / uid / rv /
///     creationTimestamp (so the Scale's rv is the PARENT's rv).
///
/// # Errors
///
/// [`UnprojectableScale`] when either count — or a step above it that is
/// not an object — holds something other than an integer (`"3"`, `2.5`,
/// `true`). No default is substituted for a value the parent declared.
pub fn project_scale(parent: &Value) -> Result<Scale, UnprojectableScale> {
    let spec_replicas = REPLICAS.read(parent).map_err(UnprojectableScale)?;
    let status_replicas = STATUS_REPLICAS.read(parent).map_err(UnprojectableScale)?;

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

    Ok(Scale {
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
    })
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
    use engenho_controllers::JsonKind;
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
        let s = project_scale(&parent()).expect("projectable");
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
        let s = project_scale(&p).expect("projectable");
        assert_eq!(s.spec.replicas, 1, "absent spec.replicas defaults to 1");
        assert_eq!(s.status.replicas, 0, "absent status.replicas defaults to 0");
        assert_eq!(s.status.selector, None, "no selector → None");
    }

    #[test]
    fn project_reads_null_replicas_as_absent() {
        // `null` decodes to a nil pointer upstream and is defaulted like a
        // missing key: the same answer, not a leniency.
        let p = json!({ "spec": { "replicas": null }, "status": { "replicas": null } });
        let s = project_scale(&p).expect("projectable");
        assert_eq!(s.spec.replicas, 1);
        assert_eq!(s.status.replicas, 0);
    }

    #[test]
    fn project_reads_zero_replicas_as_zero_not_as_the_default() {
        // A declared 0 is a scale-to-zero, never the absent default of 1.
        let p = json!({ "spec": { "replicas": 0 }, "status": { "replicas": 0 } });
        let s = project_scale(&p).expect("projectable");
        assert_eq!(s.spec.replicas, 0);
        assert_eq!(s.status.replicas, 0);
    }

    /// The field an [`UnprojectableScale`] names, dotted, and the JSON type
    /// it found — read through the typed error, not its message.
    fn refused(p: &Value) -> (String, JsonKind) {
        match project_scale(p) {
            Err(e) => match e.shape() {
                ShapeError::NotAnInteger { path, found } => (path.to_string(), *found),
                other @ ShapeError::Wrong { .. } => panic!("expected NotAnInteger, got {other:?}"),
            },
            Ok(s) => panic!("projected {s:?} from a count it cannot read"),
        }
    }

    #[test]
    fn project_refuses_a_spec_replicas_that_is_not_an_integer() {
        // The defect: `"3"` read as absent and projected as a Scale of 1.
        let mut p = parent();
        p["spec"]["replicas"] = json!("3");
        assert_eq!(refused(&p), ("spec.replicas".to_string(), JsonKind::String));
        for bad in [json!(2.5), json!(true), json!([3]), json!({ "n": 3 })] {
            let mut p = parent();
            p["spec"]["replicas"] = bad.clone();
            assert_eq!(refused(&p).0, "spec.replicas", "{bad}");
        }
    }

    #[test]
    fn project_refuses_a_status_replicas_that_is_not_an_integer() {
        let mut p = parent();
        p["status"]["replicas"] = json!("2");
        assert_eq!(
            refused(&p),
            ("status.replicas".to_string(), JsonKind::String)
        );
    }

    #[test]
    fn project_refuses_a_spec_that_is_not_an_object() {
        // `spec: "oops"` declared something; reading it as absent would
        // hand back the default for an object that said otherwise.
        let p = json!({ "metadata": { "name": "x" }, "spec": "oops" });
        assert_eq!(refused(&p), ("spec.replicas".to_string(), JsonKind::String));
    }

    #[test]
    fn unprojectable_scale_is_a_500_naming_the_field() {
        let mut p = parent();
        p["spec"]["replicas"] = json!("3");
        let err = project_scale(&p).expect_err("unprojectable");
        match ApiError::from(err) {
            ApiError::Internal(msg) => assert!(
                msg.contains("spec.replicas") && msg.contains("a string"),
                "{msg}"
            ),
            other => panic!("expected a 500, got {other:?}"),
        }
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
        let s = project_scale(&parent()).expect("projectable");
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
        let s = project_scale(&parent()).expect("projectable");
        let wire = serde_json::to_string(&s).unwrap();
        let back: Scale = serde_json::from_str(&wire).unwrap();
        assert_eq!(back.spec.replicas, 5);
        assert_eq!(back.api_version, "autoscaling/v1");
    }
}
