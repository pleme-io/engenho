//! Owner-reference helpers.
//!
//! K8s controllers track parent/child relationships via
//! `metadata.ownerReferences[].controller=true`. The owner pattern
//! shows up in: ReplicaSet→Pod, Deployment→ReplicaSet,
//! Service→Endpoints, etc. — extract once + reuse.

use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// Write an `ownerReferences` entry into `child`'s metadata,
/// pointing at `owner`. Idempotent — won't duplicate an existing
/// reference with the same uid.
///
/// Mutates `child` in place. Returns true if a reference was added.
pub fn set_owner_reference(child: &mut Value, owner_ref: OwnerReference) -> bool {
    let metadata = child
        .as_object_mut()
        .expect("child must be a JSON object")
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let metadata_obj = metadata.as_object_mut().expect("metadata must be object");
    let owner_refs = metadata_obj
        .entry("ownerReferences".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let arr = owner_refs
        .as_array_mut()
        .expect("ownerReferences must be array");
    // Idempotence: if a ref with the same uid exists, no-op.
    let already_present = arr.iter().any(|r| {
        r.get("uid")
            .and_then(|u| u.as_str())
            .map(|u| u == owner_ref.uid)
            .unwrap_or(false)
    });
    if already_present {
        return false;
    }
    arr.push(serde_json::to_value(&owner_ref).expect("OwnerReference serializes"));
    true
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
        let added = set_owner_reference(&mut pod, rs_owner());
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
        assert!(set_owner_reference(&mut pod, rs_owner()));
        // Second call returns false (no-op).
        assert!(!set_owner_reference(&mut pod, rs_owner()));
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
        set_owner_reference(&mut pod, rs_owner());
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
        set_owner_reference(&mut pod, rs_owner());
        assert!(is_owned_by(&pod, "uid-rs-1"));
        assert!(!is_owned_by(&pod, "uid-other"));
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
