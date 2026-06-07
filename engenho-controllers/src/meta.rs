//! `ObjectMeta` accessor extension trait.
//!
//! Every workload controller (ReplicaSet / Deployment / StatefulSet /
//! Endpoints / Job / GC) reads the same handful of fields off the
//! opaque `serde_json::Value` it lists from the store:
//!
//!   * `metadata.uid`  — the object's stable identity (owner refs)
//!   * `metadata.name` — the object's name (child naming, owner refs)
//!   * `metadata.namespace` — scope
//!   * `spec.<key>` as an `i64` — replica/completion/parallelism counts
//!
//! Before this trait each controller hand-copied byte-identical
//! `*_uid` / `*_name` / `*_replicas` accessors. Per the Prime
//! Directive the shape is solved ONCE here: [`ObjectMeta`] is the
//! single accessor surface every controller consumes. Adding a new
//! controller no longer re-derives these four accessors.
//!
//! The trait is implemented for [`serde_json::Value`] (so it is also
//! available through any `&Value` deref); call sites read
//! `obj.uid()` / `obj.name()` / `obj.namespace()` /
//! `obj.spec_i64("replicas", 1)` directly.

use serde_json::Value;

/// Borrowed accessors over a K8s object's `metadata` + `spec`.
///
/// Implemented for [`serde_json::Value`]. The returned `&str` borrows
/// from the object, matching the byte-for-byte behavior of the
/// per-controller accessors this trait replaces.
pub trait ObjectMeta {
    /// `metadata.uid` — the object's stable identity. `None` when the
    /// field is absent (a freshly minted object the apiserver has not
    /// stamped yet).
    fn uid(&self) -> Option<&str>;

    /// `metadata.name` — the object's name. `None` when absent.
    fn name(&self) -> Option<&str>;

    /// `metadata.namespace` — the object's namespace scope. `None`
    /// for cluster-scoped objects or before the field is set.
    fn namespace(&self) -> Option<&str>;

    /// `spec.<key>` interpreted as an `i64`, falling back to `default`
    /// when the field is absent or not an integer. Mirrors the
    /// per-controller `replicas` / `desired_replicas` / `completions`
    /// / `parallelism` accessors (each was `…unwrap_or(<default>)`).
    fn spec_i64(&self, key: &str, default: i64) -> i64;
}

impl ObjectMeta for Value {
    fn uid(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
    }

    fn name(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
    }

    fn namespace(&self) -> Option<&str> {
        self.get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
    }

    fn spec_i64(&self, key: &str, default: i64) -> i64 {
        self.get("spec")
            .and_then(|s| s.get(key))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uid_reads_metadata_uid() {
        let obj = json!({"metadata": {"uid": "u-1"}});
        assert_eq!(obj.uid(), Some("u-1"));
    }

    #[test]
    fn uid_none_when_absent() {
        let obj = json!({"metadata": {"name": "x"}});
        assert_eq!(obj.uid(), None);
        let obj = json!({"spec": {}});
        assert_eq!(obj.uid(), None);
    }

    #[test]
    fn name_reads_metadata_name() {
        let obj = json!({"metadata": {"name": "podinfo"}});
        assert_eq!(obj.name(), Some("podinfo"));
    }

    #[test]
    fn name_none_when_absent() {
        let obj = json!({"metadata": {"uid": "u"}});
        assert_eq!(obj.name(), None);
    }

    #[test]
    fn namespace_reads_metadata_namespace() {
        let obj = json!({"metadata": {"namespace": "kube-system"}});
        assert_eq!(obj.namespace(), Some("kube-system"));
    }

    #[test]
    fn namespace_none_when_absent() {
        let obj = json!({"metadata": {"name": "x"}});
        assert_eq!(obj.namespace(), None);
    }

    #[test]
    fn spec_i64_reads_value() {
        let obj = json!({"spec": {"replicas": 3}});
        assert_eq!(obj.spec_i64("replicas", 1), 3);
    }

    #[test]
    fn spec_i64_falls_back_when_absent() {
        let obj = json!({"spec": {}});
        assert_eq!(obj.spec_i64("replicas", 1), 1);
        let obj = json!({"metadata": {}});
        assert_eq!(obj.spec_i64("completions", 7), 7);
    }

    #[test]
    fn spec_i64_falls_back_when_not_integer() {
        // A non-integer (string / float) yields the default, matching
        // the per-controller `as_i64().unwrap_or(default)` behavior.
        let obj = json!({"spec": {"replicas": "two"}});
        assert_eq!(obj.spec_i64("replicas", 1), 1);
        let obj = json!({"spec": {"parallelism": 2.5}});
        assert_eq!(obj.spec_i64("parallelism", 1), 1);
    }

    #[test]
    fn accessors_return_borrowed_str() {
        // The returned &str borrows from the object (no allocation) —
        // call sites that need an owned String do `.map(String::from)`.
        let obj = json!({"metadata": {"uid": "u", "name": "n", "namespace": "ns"}});
        let _: Option<&str> = obj.uid();
        let _: Option<&str> = obj.name();
        let _: Option<&str> = obj.namespace();
    }
}
