//! The KV semantics — etcd requests translated onto engenho's store.
//!
//! ★ THIS IS THE HALF THAT CAN BE WRONG. The tonic transport above it is
//! boilerplate; the mapping below decides whether a consumer reads the
//! right bytes. It is kept free of any network type so every rule here is
//! testable without a listener, which is also what makes the eventual
//! transport a thin shim rather than a place where semantics hide.
//!
//! ★ THE MAPPING IS EXACT WHERE IT MATTERS. engenho's store was already
//! etcd-shaped in the one way a façade cannot fake — [`Revision`] is a
//! single global monotonic counter, and [`VersionMeta`] is exactly etcd's
//! `create_revision` / `mod_revision` / `version` triple. So the
//! translation is field-for-field, not an approximation:
//!
//! | etcd `KeyValue` | engenho |
//! |---|---|
//! | `key` | the `/registry` rendering of a `ResourceKey` |
//! | `value` | the object's JSON, as bytes |
//! | `create_revision` | `VersionMeta::create_revision` |
//! | `mod_revision` | `VersionMeta::mod_revision` |
//! | `version` | `VersionMeta::version` |
//! | `lease` | always 0 — engenho attaches no leases to objects |
//!
//! ★ RANGE ORDER. etcd returns keys in byte-lexicographic order of the
//! full key. engenho's catalog is a `BTreeMap<ResourceKey, _>` ordered by
//! `(group, version, kind, namespace, name)`. Within ONE resource prefix
//! those coincide — every key shares the leading segments, so both reduce
//! to `(namespace, name)` — which covers every prefix scan a consumer
//! performs. A range SPANNING resources does not coincide, and this module
//! sorts the assembled result by key bytes rather than pretending the
//! orders agree.

use std::collections::BTreeMap;

use engenho_store::ResourceKey;
use engenho_store::revision::{Revision, VersionMeta};

use crate::keyspace::{self, ParsedKey};
use crate::pb::mvccpb::KeyValue;

/// One object as the store holds it, ready to be rendered onto the wire.
#[derive(Debug, Clone)]
pub struct StoredObject {
    pub key: ResourceKey,
    pub value: Vec<u8>,
    pub meta: VersionMeta,
}

/// Render a stored object as an etcd `KeyValue`.
///
/// `plural` and `namespaced` come from the catalog; they cannot be derived
/// from `ResourceKey` alone, which carries the KIND rather than the plural.
#[must_use]
pub fn to_key_value(obj: &StoredObject, plural: &str, namespaced: bool) -> KeyValue {
    let key = keyspace::object_key(
        &obj.key.group,
        plural,
        namespaced,
        obj.key.namespace.as_deref(),
        &obj.key.name,
    )
    .key;
    KeyValue {
        key: key.into_bytes(),
        create_revision: i64::try_from(obj.meta.create_revision.get()).unwrap_or(i64::MAX),
        mod_revision: i64::try_from(obj.meta.mod_revision.get()).unwrap_or(i64::MAX),
        version: i64::try_from(obj.meta.version).unwrap_or(i64::MAX),
        value: obj.value.clone(),
        // engenho attaches no lease to an API object. Reporting 0 is
        // etcd's own encoding for "no lease", not a placeholder.
        lease: 0,
    }
}

/// What a `Range` request is asking for, once decoded.
///
/// etcd has no prefix flag on the wire: a prefix scan IS
/// `range_end == prefix_range_end(key)`. Recognising that here rather than
/// in the transport is what lets the store answer with one collection scan
/// instead of a full keyspace walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeShape {
    /// A single key.
    Point(String),
    /// Every key under a prefix.
    Prefix(String),
    /// An explicit `[start, end)` interval that is not a clean prefix.
    Interval { start: String, end: String },
    /// `key == "\0"` and `range_end == "\0"` — etcd's "the whole keyspace".
    All,
}

/// Classify a raw `(key, range_end)` pair.
#[must_use]
pub fn range_shape(key: &[u8], range_end: &[u8]) -> RangeShape {
    // etcd spells "from here to the end of the keyspace" as a single zero
    // byte, and "everything" as key=\0 with that end.
    if range_end.is_empty() {
        return RangeShape::Point(String::from_utf8_lossy(key).into_owned());
    }
    if key == [0] && range_end == [0] {
        return RangeShape::All;
    }
    if range_end == keyspace::prefix_range_end(key).as_slice() {
        return RangeShape::Prefix(String::from_utf8_lossy(key).into_owned());
    }
    RangeShape::Interval {
        start: String::from_utf8_lossy(key).into_owned(),
        end: String::from_utf8_lossy(range_end).into_owned(),
    }
}

/// Assemble a `Range` response body from candidate objects.
///
/// Sorting is by KEY BYTES, deliberately: see the module note on range
/// order. `limit <= 0` means unlimited, which is etcd's encoding.
#[must_use]
pub fn assemble_range(mut kvs: Vec<KeyValue>, limit: i64) -> (Vec<KeyValue>, bool) {
    kvs.sort_by(|a, b| a.key.cmp(&b.key));
    if limit > 0 && kvs.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
        let n = usize::try_from(limit).unwrap_or(usize::MAX);
        kvs.truncate(n);
        // `more` tells the client another page exists — etcd's `RangeResponse.more`.
        return (kvs, true);
    }
    (kvs, false)
}

/// Resolve a `/registry` key to the store coordinates a lookup needs.
///
/// Returns `None` for a singleton (`/registry/health`, allocator ranges,
/// master leases): they are real keys with no GVK, and a caller must carry
/// them verbatim rather than invent coordinates.
#[must_use]
pub fn store_coords(key: &str) -> Option<(String, String, String, Option<String>, String)> {
    match keyspace::parse_key(key).ok()? {
        ParsedKey::Object {
            group,
            version,
            kind,
            namespace,
            name,
            ..
        } => Some((group, version, kind, namespace, name)),
        ParsedKey::Singleton { .. } => None,
    }
}

/// The header every etcd response carries.
#[must_use]
pub fn response_revision(rev: Revision) -> i64 {
    i64::try_from(rev.get()).unwrap_or(i64::MAX)
}

/// Group stored objects by their rendered key for deterministic assembly.
#[must_use]
pub fn index_by_key(kvs: Vec<KeyValue>) -> BTreeMap<Vec<u8>, KeyValue> {
    kvs.into_iter().map(|kv| (kv.key.clone(), kv)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(create: u64, modr: u64, version: u64) -> VersionMeta {
        VersionMeta {
            create_revision: Revision(create),
            mod_revision: Revision(modr),
            version,
        }
    }

    fn obj(ns: &str, name: &str) -> StoredObject {
        StoredObject {
            key: ResourceKey::namespaced("", "v1", "Pod", ns, name),
            value: br#"{"kind":"Pod"}"#.to_vec(),
            meta: meta(3, 7, 2),
        }
    }

    #[test]
    fn key_value_is_field_for_field_not_an_approximation() {
        let kv = to_key_value(&obj("default", "nginx"), "pods", true);
        assert_eq!(kv.key, b"/registry/pods/default/nginx".to_vec());
        assert_eq!(kv.create_revision, 3);
        assert_eq!(kv.mod_revision, 7);
        assert_eq!(kv.version, 2);
        assert_eq!(kv.value, br#"{"kind":"Pod"}"#.to_vec());
        // 0 is etcd's encoding for "no lease", not a placeholder.
        assert_eq!(kv.lease, 0);
    }

    #[test]
    fn a_cluster_scoped_object_renders_without_a_namespace_segment() {
        let o = StoredObject {
            key: ResourceKey::cluster_scoped("", "v1", "Node", "cid"),
            value: b"{}".to_vec(),
            meta: meta(1, 1, 1),
        };
        // And through the corrected segment map: nodes are minions.
        assert_eq!(
            to_key_value(&o, "nodes", false).key,
            b"/registry/minions/cid".to_vec()
        );
    }

    #[test]
    fn a_prefix_scan_is_recognised_from_range_end_alone() {
        // etcd has no --prefix on the wire; this IS how it is expressed.
        let key = b"/registry/pods/default/";
        let end = keyspace::prefix_range_end(key);
        assert_eq!(
            range_shape(key, &end),
            RangeShape::Prefix("/registry/pods/default/".into())
        );
    }

    #[test]
    fn an_empty_range_end_is_a_point_read() {
        assert_eq!(
            range_shape(b"/registry/pods/default/nginx", b""),
            RangeShape::Point("/registry/pods/default/nginx".into())
        );
    }

    #[test]
    fn the_whole_keyspace_and_a_plain_interval_are_distinguished() {
        assert_eq!(range_shape(&[0], &[0]), RangeShape::All);
        assert_eq!(
            range_shape(b"/registry/a", b"/registry/z"),
            RangeShape::Interval {
                start: "/registry/a".into(),
                end: "/registry/z".into(),
            }
        );
    }

    #[test]
    fn range_results_are_ordered_by_key_bytes_not_insertion() {
        // The order etcd promises. engenho's BTreeMap order coincides
        // within one prefix but this must not depend on that.
        let kvs = vec![
            to_key_value(&obj("default", "zeta"), "pods", true),
            to_key_value(&obj("default", "alpha"), "pods", true),
            to_key_value(&obj("beta", "mid"), "pods", true),
        ];
        let (out, more) = assemble_range(kvs, 0);
        let keys: Vec<String> = out
            .iter()
            .map(|k| String::from_utf8_lossy(&k.key).into_owned())
            .collect();
        assert_eq!(
            keys,
            vec![
                "/registry/pods/beta/mid",
                "/registry/pods/default/alpha",
                "/registry/pods/default/zeta",
            ]
        );
        assert!(!more, "limit 0 means unlimited — never truncated");
    }

    #[test]
    fn a_limit_truncates_and_reports_more() {
        let kvs = vec![
            to_key_value(&obj("default", "a"), "pods", true),
            to_key_value(&obj("default", "b"), "pods", true),
            to_key_value(&obj("default", "c"), "pods", true),
        ];
        let (out, more) = assemble_range(kvs, 2);
        assert_eq!(out.len(), 2);
        assert!(more, "the client must learn another page exists");
    }

    #[test]
    fn store_coords_round_trip_through_the_registry_key() {
        let (group, version, kind, ns, name) =
            store_coords("/registry/deployments/flux-system/source-controller").expect("resolves");
        assert_eq!(
            (group.as_str(), version.as_str(), kind.as_str()),
            ("apps", "v1", "Deployment")
        );
        assert_eq!(ns.as_deref(), Some("flux-system"));
        assert_eq!(name, "source-controller");
    }

    #[test]
    fn a_singleton_key_yields_no_store_coordinates() {
        // Real keys with no GVK. Inventing coordinates for them would make
        // a backup tool read a nonexistent object.
        for k in [
            "/registry/health",
            "/registry/masterleases/10.0.0.1",
            "/registry/ranges/serviceips",
        ] {
            assert!(store_coords(k).is_none(), "{k} must not resolve to a GVK");
        }
    }
}
