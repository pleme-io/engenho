//! `ResourceCatalog` — the deterministic state machine over the
//! Raft log. Each committed [`ResourceCommand`] mutates this;
//! followers and leader converge by replay.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::command::{ResourceCommand, ResourceOp};
use crate::resource::{ResourceKey, ResourceValue};

/// In-memory K8s resource catalog. Keyed by [`ResourceKey`] (which
/// encodes group + version + kind + namespace + name), values are
/// opaque [`ResourceValue`] (serde_json::Value) at R6.
///
/// The catalog tracks `last_applied_index` so consumers (the
/// apiserver, watchers) can express read-after-write semantics by
/// waiting for a specific index.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceCatalog {
    pub resources: BTreeMap<ResourceKey, ResourceValue>,
    pub last_applied_term: u64,
    pub last_applied_index: u64,
}

// serde_json doesn't support struct map-keys directly. Custom impl
// flattens the BTreeMap into Vec<(ResourceKey, ResourceValue)> at
// the wire — preserves order (BTreeMap iter is sorted) so the
// resulting bytes are deterministic across nodes.
impl Serialize for ResourceCatalog {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = ser.serialize_struct("ResourceCatalog", 3)?;
        let entries: Vec<(&ResourceKey, &ResourceValue)> = self.resources.iter().collect();
        state.serialize_field("resources", &entries)?;
        state.serialize_field("last_applied_term", &self.last_applied_term)?;
        state.serialize_field("last_applied_index", &self.last_applied_index)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ResourceCatalog {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            resources: Vec<(ResourceKey, ResourceValue)>,
            last_applied_term: u64,
            last_applied_index: u64,
        }
        let h = Helper::deserialize(de)?;
        Ok(Self {
            resources: h.resources.into_iter().collect(),
            last_applied_term: h.last_applied_term,
            last_applied_index: h.last_applied_index,
        })
    }
}

/// Snapshot for serde — same shape as the catalog. Provided as a
/// distinct type for forward compat (R6.5 may store the snapshot
/// in a more compact compressed form on disk).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceCatalogSnapshot {
    pub catalog: ResourceCatalog,
}

impl ResourceCatalog {
    /// Apply a single committed command. Pure function. Returns
    /// the operation outcome the apiserver / client gets back.
    ///
    /// Resource versioning: every successful Put / Patch /
    /// Delete writes `metadata.resourceVersion = index` (the
    /// committed Raft log index). On Put, `metadata.uid` is set
    /// to a deterministic hash of `(key, index)` if it doesn't
    /// already exist. This mirrors etcd-key-versioning behavior
    /// at the apiserver boundary.
    pub fn apply(&mut self, cmd: &ResourceCommand, term: u64, index: u64) -> ResourceOp {
        let outcome = match cmd {
            ResourceCommand::Put { key, value, .. } => self.apply_put(key, value, index),
            ResourceCommand::Patch { key, patch, .. } => self.apply_patch(key, patch, index),
            ResourceCommand::Delete { key, .. } => self.apply_delete(key),
        };
        self.last_applied_term = term;
        self.last_applied_index = index;
        outcome
    }

    fn apply_put(&mut self, key: &ResourceKey, value: &ResourceValue, index: u64) -> ResourceOp {
        let mut new_value = value.clone();
        let already_existed = self.resources.contains_key(key);
        if let Some(obj) = new_value.as_object_mut() {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                meta_obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(index.to_string()),
                );
                // Set uid only if not already present + not already in store
                let needs_uid = !meta_obj.contains_key("uid")
                    && !self
                        .resources
                        .get(key)
                        .and_then(|v| v.get("metadata").and_then(|m| m.get("uid")))
                        .is_some();
                if needs_uid {
                    let uid = format!("uid-{}-{}", key.label().replace('/', "-"), index);
                    meta_obj.insert("uid".to_string(), serde_json::Value::String(uid));
                } else if let Some(prior_uid) = self
                    .resources
                    .get(key)
                    .and_then(|v| v.get("metadata"))
                    .and_then(|m| m.get("uid"))
                    .cloned()
                {
                    // Preserve uid across updates.
                    meta_obj.insert("uid".to_string(), prior_uid);
                }
            }
        }
        self.resources.insert(key.clone(), new_value);
        if already_existed {
            ResourceOp::Replaced
        } else {
            ResourceOp::Created
        }
    }

    fn apply_patch(&mut self, key: &ResourceKey, patch: &ResourceValue, index: u64) -> ResourceOp {
        let Some(existing) = self.resources.get_mut(key) else {
            return ResourceOp::NoOp;
        };
        merge_json(existing, patch);
        if let Some(obj) = existing.as_object_mut() {
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta_obj) = metadata.as_object_mut() {
                meta_obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(index.to_string()),
                );
            }
        }
        ResourceOp::Patched
    }

    fn apply_delete(&mut self, key: &ResourceKey) -> ResourceOp {
        if self.resources.remove(key).is_some() {
            ResourceOp::Deleted
        } else {
            ResourceOp::NoOp
        }
    }

    /// Read a single resource by key.
    #[must_use]
    pub fn get(&self, key: &ResourceKey) -> Option<&ResourceValue> {
        self.resources.get(key)
    }

    /// List resources matching (group, version, kind), optionally
    /// scoped to a namespace.
    #[must_use]
    pub fn list(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Vec<(&ResourceKey, &ResourceValue)> {
        self.resources
            .iter()
            .filter(|(k, _)| k.group == group && k.version == version && k.kind == kind)
            .filter(|(k, _)| match (namespace, k.namespace.as_deref()) {
                (None, _) => true,
                (Some(want), Some(have)) => want == have,
                (Some(_), None) => false,
            })
            .collect()
    }

    /// Total resource count (across all kinds + namespaces).
    #[must_use]
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

/// Recursive JSON merge — fields in `patch` overwrite + null in
/// patch deletes. Mirrors RFC 7396 (JSON Merge Patch).
fn merge_json(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if !patch.is_object() {
        *target = patch.clone();
        return;
    }
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let target_obj = target.as_object_mut().unwrap();
    for (k, v) in patch.as_object().unwrap() {
        if v.is_null() {
            target_obj.remove(k);
        } else {
            let entry = target_obj
                .entry(k.clone())
                .or_insert_with(|| serde_json::Value::Null);
            merge_json(entry, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Reason;

    fn pod_key(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Pod", "default", name)
    }

    #[test]
    fn put_creates_then_replaces() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        let v = serde_json::json!({"spec": {"image": "v1"}});
        let op = cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: v.clone(),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        assert_eq!(op, ResourceOp::Created);
        assert_eq!(cat.len(), 1);

        // Replace
        let v2 = serde_json::json!({"spec": {"image": "v2"}});
        let op = cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: v2,
                reason: Reason::Operator,
            },
            1,
            2,
        );
        assert_eq!(op, ResourceOp::Replaced);
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn put_sets_resource_version_and_uid() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: serde_json::json!({"spec": {}}),
                reason: Reason::Operator,
            },
            1,
            42,
        );
        let stored = cat.get(&k).unwrap();
        let metadata = stored.get("metadata").unwrap();
        assert_eq!(
            metadata.get("resourceVersion").unwrap(),
            &serde_json::json!("42")
        );
        // uid must be set + stable across replaces.
        let uid_1 = metadata.get("uid").unwrap().as_str().unwrap().to_string();
        assert!(uid_1.starts_with("uid-"));
        // Replace; uid should survive.
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: serde_json::json!({"spec": {"replaced": true}}),
                reason: Reason::Operator,
            },
            1,
            100,
        );
        let uid_2 = cat
            .get(&k)
            .unwrap()
            .get("metadata")
            .unwrap()
            .get("uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(uid_1, uid_2);
    }

    #[test]
    fn patch_merges_into_existing() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: serde_json::json!({"spec": {"image": "v1", "replicas": 2}}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        let op = cat.apply(
            &ResourceCommand::Patch {
                key: k.clone(),
                patch: serde_json::json!({"spec": {"image": "v2"}}),
                reason: Reason::Operator,
            },
            1,
            2,
        );
        assert_eq!(op, ResourceOp::Patched);
        let stored = cat.get(&k).unwrap();
        assert_eq!(stored.get("spec").unwrap().get("image").unwrap(), "v2");
        // replicas survived the merge
        assert_eq!(stored.get("spec").unwrap().get("replicas").unwrap(), 2);
    }

    #[test]
    fn patch_on_missing_is_noop() {
        let mut cat = ResourceCatalog::default();
        let op = cat.apply(
            &ResourceCommand::Patch {
                key: pod_key("ghost"),
                patch: serde_json::json!({"x": 1}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        assert_eq!(op, ResourceOp::NoOp);
    }

    #[test]
    fn delete_removes_then_noop() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("podinfo");
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: serde_json::json!({}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        assert_eq!(cat.len(), 1);
        let op = cat.apply(
            &ResourceCommand::Delete {
                key: k.clone(),
                reason: Reason::Operator,
            },
            1,
            2,
        );
        assert_eq!(op, ResourceOp::Deleted);
        assert_eq!(cat.len(), 0);
        // Second delete is no-op.
        let op = cat.apply(
            &ResourceCommand::Delete {
                key: k.clone(),
                reason: Reason::GarbageCollector,
            },
            1,
            3,
        );
        assert_eq!(op, ResourceOp::NoOp);
    }

    #[test]
    fn list_filters_by_gvk_and_namespace() {
        let mut cat = ResourceCatalog::default();
        for name in ["a", "b", "c"] {
            cat.apply(
                &ResourceCommand::Put {
                    key: ResourceKey::namespaced("", "v1", "Pod", "default", name),
                    value: serde_json::json!({}),
                    reason: Reason::Operator,
                },
                1,
                1,
            );
        }
        cat.apply(
            &ResourceCommand::Put {
                key: ResourceKey::namespaced("", "v1", "Pod", "kube-system", "coredns"),
                value: serde_json::json!({}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        cat.apply(
            &ResourceCommand::Put {
                key: ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm-1"),
                value: serde_json::json!({}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        // List all Pods in default → a, b, c
        let pods = cat.list("", "v1", "Pod", Some("default"));
        assert_eq!(pods.len(), 3);
        // List ConfigMaps in default → 1
        let cms = cat.list("", "v1", "ConfigMap", Some("default"));
        assert_eq!(cms.len(), 1);
        // List all Pods (any namespace) → 4
        let all_pods = cat.list("", "v1", "Pod", None);
        assert_eq!(all_pods.len(), 4);
    }

    #[test]
    fn json_merge_patch_null_deletes_field() {
        let mut cat = ResourceCatalog::default();
        let k = pod_key("p");
        cat.apply(
            &ResourceCommand::Put {
                key: k.clone(),
                value: serde_json::json!({"spec": {"image": "v1", "annotations": {"a": "b"}}}),
                reason: Reason::Operator,
            },
            1,
            1,
        );
        cat.apply(
            &ResourceCommand::Patch {
                key: k.clone(),
                patch: serde_json::json!({"spec": {"annotations": null}}),
                reason: Reason::Operator,
            },
            1,
            2,
        );
        let stored = cat.get(&k).unwrap();
        assert!(
            stored
                .get("spec")
                .unwrap()
                .as_object()
                .unwrap()
                .get("annotations")
                .is_none()
        );
        // Image survives
        assert_eq!(stored.get("spec").unwrap().get("image").unwrap(), "v1");
    }
}
