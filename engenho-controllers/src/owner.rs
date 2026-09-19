//! Owner-reference helpers.
//!
//! K8s controllers track parent/child relationships via
//! `metadata.ownerReferences[].controller=true`. The owner pattern
//! shows up in: ReplicaSet→Pod, Deployment→ReplicaSet,
//! Service→Endpoints, etc. — extract once + reuse.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::meta::{ObjectMeta, ShapeError, array_mut};

/// Where a child's owner references live.
pub const OWNER_REFERENCES: &[&str] = &["metadata", "ownerReferences"];

/// K8s `OwnerReference` shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerReference {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
    /// True if this owner is the "controller" of the resource —
    /// only one ownerRef per child can have controller=true.
    #[serde(default, skip_serializing_if = "is_false")]
    pub controller: bool,
    /// True if deletion of the owner should garbage-collect this
    /// child. Default true for controller-style ownership.
    #[serde(
        rename = "blockOwnerDeletion",
        default,
        skip_serializing_if = "is_false"
    )]
    pub block_owner_deletion: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl OwnerReference {
    /// The wire object: the same bytes `serde_json::to_value` produces for
    /// this type (pinned by `to_value_matches_serde_for_every_flag`), built
    /// without a fallible serializer so writing a reference cannot fail.
    #[must_use]
    pub fn to_value(&self) -> Value {
        let mut out = Map::new();
        out.insert("apiVersion".into(), Value::from(self.api_version.as_str()));
        out.insert("kind".into(), Value::from(self.kind.as_str()));
        out.insert("name".into(), Value::from(self.name.as_str()));
        out.insert("uid".into(), Value::from(self.uid.as_str()));
        if self.controller {
            out.insert("controller".into(), Value::Bool(true));
        }
        if self.block_owner_deletion {
            out.insert("blockOwnerDeletion".into(), Value::Bool(true));
        }
        Value::Object(out)
    }
}

/// Write an `ownerReferences` entry into `child`'s metadata,
/// pointing at `owner`. Idempotent — won't duplicate an existing
/// reference with the same uid.
///
/// Mutates `child` in place. Returns `Ok(true)` if a reference was added,
/// `Ok(false)` if one with the same uid was already there.
///
/// Total over the child's shape (see [`crate::meta::array_mut`]): an absent
/// or `null` `metadata` or `ownerReferences` is treated as empty.
///
/// # Errors
///
/// [`ShapeError`] when `child`, its `metadata`, or its `ownerReferences`
/// holds the wrong JSON type. The child is left unchanged, and the caller
/// skips that object; it used to panic here, taking every object after it
/// in the sweep down with it.
pub fn set_owner_reference(
    child: &mut Value,
    owner_ref: OwnerReference,
) -> Result<bool, ShapeError> {
    let refs = array_mut(child, OWNER_REFERENCES)?;
    let already_present = refs
        .iter()
        .any(|r| r.get("uid").and_then(Value::as_str) == Some(owner_ref.uid.as_str()));
    if already_present {
        return Ok(false);
    }
    refs.push(owner_ref.to_value());
    Ok(true)
}

/// Read the controlling owner of `child` (the ownerRef with
/// controller=true) if any.
#[must_use]
pub fn controlling_owner(child: &Value) -> Option<OwnerReference> {
    child
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|r| r.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|r| {
                let is_ctrl = r
                    .get("controller")
                    .and_then(|c| c.as_bool())
                    .unwrap_or(false);
                if is_ctrl {
                    serde_json::from_value::<OwnerReference>(r.clone()).ok()
                } else {
                    None
                }
            })
        })
}

/// Returns true if `child` has `owner_uid` as its controlling owner.
#[must_use]
pub fn is_owned_by(child: &Value, owner_uid: &str) -> bool {
    controlling_owner(child)
        .map(|o| o.uid == owner_uid)
        .unwrap_or(false)
}

/// Build a controller-style [`OwnerReference`] pointing at `parent`
/// for the given `api_version` + `kind`.
///
/// `controller` + `block_owner_deletion` are both `true` — the
/// standard "this object owns + garbage-collects its child" shape
/// every workload controller (ReplicaSet→Pod, Deployment→ReplicaSet,
/// StatefulSet→Pod, Job→Pod, Service→Endpoints) uses. Each controller
/// passes its own `api_version`/`kind` literals.
///
/// Returns `None` when `parent` lacks a `metadata.name` or
/// `metadata.uid` — a freshly minted parent the apiserver has not
/// finished stamping; the caller then skips this tick rather than
/// minting a child with a dangling owner ref.
#[must_use]
pub fn owner_ref_for(parent: &Value, api_version: &str, kind: &str) -> Option<OwnerReference> {
    Some(OwnerReference {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        name: parent.name()?.to_string(),
        uid: parent.uid()?.to_string(),
        controller: true,
        block_owner_deletion: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rs_owner() -> OwnerReference {
        OwnerReference {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            name: "podinfo-abc".into(),
            uid: "uid-rs-1".into(),
            controller: true,
            block_owner_deletion: true,
        }
    }

    #[test]
    fn set_owner_reference_adds_new_entry() {
        let mut pod = json!({"metadata": {"name": "p1"}, "spec": {}});
        let added = set_owner_reference(&mut pod, rs_owner()).unwrap();
        assert!(added);
        let refs = pod
            .get("metadata")
            .unwrap()
            .get("ownerReferences")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].get("uid").unwrap(), "uid-rs-1");
        assert_eq!(refs[0].get("controller").unwrap(), true);
    }

    #[test]
    fn set_owner_reference_is_idempotent_by_uid() {
        let mut pod = json!({"metadata": {"name": "p1"}});
        assert!(set_owner_reference(&mut pod, rs_owner()).unwrap());
        // Second call returns false (no-op).
        assert!(!set_owner_reference(&mut pod, rs_owner()).unwrap());
        let refs = pod
            .get("metadata")
            .unwrap()
            .get("ownerReferences")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn controlling_owner_returns_ctrl_ref() {
        let mut pod = json!({"metadata": {"name": "p"}});
        set_owner_reference(&mut pod, rs_owner()).unwrap();
        let owner = controlling_owner(&pod).expect("owner");
        assert_eq!(owner.uid, "uid-rs-1");
        assert_eq!(owner.kind, "ReplicaSet");
    }

    #[test]
    fn controlling_owner_skips_non_controller_refs() {
        let pod = json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "x",
                     "uid": "u1", "controller": false}
                ]
            }
        });
        assert!(controlling_owner(&pod).is_none());
    }

    #[test]
    fn is_owned_by_matches_uid() {
        let mut pod = json!({"metadata": {"name": "p"}});
        set_owner_reference(&mut pod, rs_owner()).unwrap();
        assert!(is_owned_by(&pod, "uid-rs-1"));
        assert!(!is_owned_by(&pod, "uid-other"));
    }

    #[test]
    fn owner_ref_for_builds_controller_ref() {
        let parent = json!({"metadata": {"name": "podinfo-abc", "uid": "uid-rs-1"}});
        let r = owner_ref_for(&parent, "apps/v1", "ReplicaSet").expect("ref");
        assert_eq!(r.api_version, "apps/v1");
        assert_eq!(r.kind, "ReplicaSet");
        assert_eq!(r.name, "podinfo-abc");
        assert_eq!(r.uid, "uid-rs-1");
        assert!(r.controller);
        assert!(r.block_owner_deletion);
    }

    #[test]
    fn owner_ref_for_none_without_name_or_uid() {
        let no_uid = json!({"metadata": {"name": "x"}});
        assert!(owner_ref_for(&no_uid, "apps/v1", "ReplicaSet").is_none());
        let no_name = json!({"metadata": {"uid": "u"}});
        assert!(owner_ref_for(&no_name, "apps/v1", "ReplicaSet").is_none());
    }

    /// `null` metadata or ownerReferences is the empty case: the reference
    /// is written. On HEAD before T4.3 both panicked ("ownerReferences must
    /// be array" / "metadata must be object").
    #[test]
    fn null_metadata_or_owner_references_is_written_not_a_panic() {
        for mut child in [
            json!({"metadata": {"name": "p", "ownerReferences": null}}),
            json!({"metadata": null}),
            json!({}),
        ] {
            assert_eq!(set_owner_reference(&mut child, rs_owner()), Ok(true));
            assert!(is_owned_by(&child, "uid-rs-1"), "{child}");
        }
    }

    /// The wrong type is an error naming the path, and the child is left
    /// exactly as it was.
    #[test]
    fn a_wrong_shape_is_an_error_and_writes_nothing() {
        for (child, path) in [
            (
                json!({"metadata": {"ownerReferences": "x"}}),
                "metadata.ownerReferences",
            ),
            (
                json!({"metadata": {"ownerReferences": {}}}),
                "metadata.ownerReferences",
            ),
            (json!({"metadata": 3}), "metadata"),
            (json!("not-an-object"), "the object itself"),
        ] {
            let mut written = child.clone();
            let err = set_owner_reference(&mut written, rs_owner()).unwrap_err();
            assert!(err.to_string().starts_with(path), "{err}");
            assert_eq!(written, child, "nothing written on a shape error");
        }
    }

    /// The hand-built wire object is byte-identical to serde's, for every
    /// combination of the two skip-if-false flags.
    #[test]
    fn to_value_matches_serde_for_every_flag() {
        for controller in [false, true] {
            for block_owner_deletion in [false, true] {
                let r = OwnerReference {
                    controller,
                    block_owner_deletion,
                    ..rs_owner()
                };
                assert_eq!(r.to_value(), serde_json::to_value(&r).unwrap());
            }
        }
    }

    #[test]
    fn owner_reference_serde_round_trips() {
        let o = rs_owner();
        let s = serde_json::to_string(&o).unwrap();
        let back: OwnerReference = serde_json::from_str(&s).unwrap();
        assert_eq!(back, o);
        assert!(s.contains("\"controller\":true"));
        assert!(s.contains("\"blockOwnerDeletion\":true"));
    }
}
