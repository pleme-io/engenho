//! THE `/registry` KEYSPACE — how a Kubernetes object is addressed in etcd.
//!
//! ★ WHY THIS IS THE LOAD-BEARING PIECE. The gRPC plumbing around it is
//! mechanical: `Range` in, `RangeResponse` out. This module is where the
//! façade is actually right or wrong, because every etcd-shaped consumer
//! addresses objects BY KEY and nothing validates the key for us. A wrong
//! prefix does not error — it returns an empty range, which reads exactly
//! like "the cluster has no pods". `etcdctl get /registry/pods/ --prefix`
//! returning nothing is indistinguishable from an empty cluster, so a
//! mistake here is silent in the one direction that matters.
//!
//! ★ THE LAYOUT IS UPSTREAM'S, NOT OURS. kube-apiserver derives an object's
//! etcd key from its `StorageStrategy`, and the result is NOT a uniform
//! `/registry/<group>/<plural>/…`. The irregularities below are real and
//! each one is load-bearing for a tool that already exists:
//!
//!   * **Core-group kinds carry NO group segment.** A Pod is
//!     `/registry/pods/<ns>/<name>`, not `/registry/core/pods/…`.
//!   * **Services are SPLIT.** `/registry/services/specs/<ns>/<name>` holds
//!     the Service; `/registry/services/endpoints/<ns>/<name>` holds its
//!     Endpoints. A reader that files Endpoints under `/registry/endpoints/`
//!     finds nothing — this is the single most-missed rule in the layout.
//!   * **Several `apps` and `batch` kinds are stored group-LESS**, because
//!     they predate their groups: deployments, replicasets, daemonsets,
//!     statefulsets, jobs, cronjobs all live at `/registry/<plural>/…`.
//!   * **Everything else is `/registry/<group>/<plural>/…`** — so
//!     `networking.k8s.io` Ingress is `/registry/ingress/<ns>/<name>`… no:
//!     it is `/registry/ingress/…` ONLY in old releases; current is
//!     `/registry/ingress/<ns>/<name>` for the extensions lineage and
//!     `/registry/ingressclasses/<name>` cluster-scoped. Where a lineage is
//!     ambiguous this module encodes the CURRENT v1.34 answer and the test
//!     names the release it was checked against.
//!
//! ★ CLUSTER-SCOPED KINDS HAVE NO NAMESPACE SEGMENT. `/registry/nodes/<name>`,
//! never `/registry/nodes//<name>`. An empty segment is a DIFFERENT key, and
//! the empty-vs-absent distinction is the same silent-empty-range failure.
//!
//! ★ PREFIXES END IN `/`. `etcdctl get /registry/pods --prefix` would also
//! match a hypothetical `/registry/podsecuritypolicies`; the trailing slash
//! is what makes a prefix scan mean "this resource" rather than "anything
//! starting with these letters".

/// Where every Kubernetes object lives. Upstream's default
/// `--etcd-prefix`; a cluster may override it, so it is a constant here
/// rather than a literal sprinkled through the module.
pub const REGISTRY_ROOT: &str = "/registry";

/// How a kind's key is shaped — the irregularities above, made typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyShape {
    /// `/registry/<plural>/<ns>/<name>` — core group, and the group-less
    /// legacy kinds from `apps`/`batch`.
    GrouplessNamespaced,
    /// `/registry/<plural>/<name>` — cluster-scoped, no group segment.
    GrouplessClusterScoped,
    /// `/registry/<group>/<plural>/<ns>/<name>`.
    GroupedNamespaced,
    /// `/registry/<group>/<plural>/<name>`.
    GroupedClusterScoped,
    /// `/registry/services/<discriminator>/<ns>/<name>` — the Service split.
    ServiceSubtree(&'static str),
}

/// A resolved object key plus the prefix that lists its whole collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKey {
    pub key: String,
    pub collection_prefix: String,
}

/// Kinds upstream stores WITHOUT a group segment despite having a group.
/// Sourced from kube-apiserver's storage registrations; these predate their
/// API groups and the keys were never migrated, because doing so would
/// strand every existing cluster's data.
const GROUPLESS_NON_CORE: &[&str] = &[
    "deployments",
    "replicasets",
    "daemonsets",
    "statefulsets",
    "controllerrevisions",
    "jobs",
    "cronjobs",
    "horizontalpodautoscalers",
    "ingress",
    "networkpolicies",
    "poddisruptionbudgets",
];

/// Core-group (`""`) plurals — no group segment by definition.
fn is_core(group: &str) -> bool {
    group.is_empty() || group == "v1" || group == "core"
}

/// Decide a kind's key shape.
///
/// `plural` is the lowercase resource plural (`pods`, `clusterroles`);
/// `group` is the API group (`""` for core, `apps`, `rbac.authorization.k8s.io`).
#[must_use]
pub fn shape_for(group: &str, plural: &str, namespaced: bool) -> KeyShape {
    // The Service split is checked FIRST: `endpoints` is a core plural that
    // would otherwise resolve to `/registry/endpoints/`, which holds nothing.
    match plural {
        "services" => return KeyShape::ServiceSubtree("specs"),
        "endpoints" => return KeyShape::ServiceSubtree("endpoints"),
        _ => {}
    }
    let groupless = is_core(group) || GROUPLESS_NON_CORE.contains(&plural);
    match (groupless, namespaced) {
        (true, true) => KeyShape::GrouplessNamespaced,
        (true, false) => KeyShape::GrouplessClusterScoped,
        (false, true) => KeyShape::GroupedNamespaced,
        (false, false) => KeyShape::GroupedClusterScoped,
    }
}

/// The etcd key for one object, and the prefix listing its collection.
///
/// `namespace` is ignored for cluster-scoped shapes rather than rejected:
/// a caller passing `Some("default")` for a Node has made a category error
/// the key must not encode, and silently dropping it is what upstream does.
#[must_use]
pub fn object_key(
    group: &str,
    plural: &str,
    namespaced: bool,
    namespace: Option<&str>,
    name: &str,
) -> ObjectKey {
    let shape = shape_for(group, plural, namespaced);
    let ns = namespace.unwrap_or_default();
    let collection_prefix = match shape {
        KeyShape::GrouplessNamespaced => {
            if ns.is_empty() {
                format!("{REGISTRY_ROOT}/{plural}/")
            } else {
                format!("{REGISTRY_ROOT}/{plural}/{ns}/")
            }
        }
        KeyShape::GrouplessClusterScoped => format!("{REGISTRY_ROOT}/{plural}/"),
        KeyShape::GroupedNamespaced => {
            if ns.is_empty() {
                format!("{REGISTRY_ROOT}/{group}/{plural}/")
            } else {
                format!("{REGISTRY_ROOT}/{group}/{plural}/{ns}/")
            }
        }
        KeyShape::GroupedClusterScoped => format!("{REGISTRY_ROOT}/{group}/{plural}/"),
        KeyShape::ServiceSubtree(sub) => {
            if ns.is_empty() {
                format!("{REGISTRY_ROOT}/services/{sub}/")
            } else {
                format!("{REGISTRY_ROOT}/services/{sub}/{ns}/")
            }
        }
    };
    ObjectKey {
        key: format!("{collection_prefix}{name}"),
        collection_prefix,
    }
}

/// The exclusive upper bound for a prefix scan — etcd's `range_end`
/// convention: increment the last byte of the prefix.
///
/// This is how `--prefix` is actually implemented on the wire; there is no
/// prefix flag in the protocol. An all-`0xFF` prefix has no successor and
/// scans to the end of the keyspace, which etcd spells as the single zero
/// byte — a real edge case, not a hypothetical, because `\0` is also how
/// "from here to infinity" is requested.
#[must_use]
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(group: &str, plural: &str, ns: Option<&str>, name: &str, namespaced: bool) -> String {
        object_key(group, plural, namespaced, ns, name).key
    }

    // Checked against Kubernetes v1.34 storage registrations.
    #[test]
    fn core_kinds_carry_no_group_segment() {
        assert_eq!(
            key("", "pods", Some("default"), "nginx", true),
            "/registry/pods/default/nginx"
        );
        assert_eq!(
            key("", "configmaps", Some("kube-system"), "coredns", true),
            "/registry/configmaps/kube-system/coredns"
        );
    }

    #[test]
    fn cluster_scoped_kinds_have_no_namespace_segment() {
        // An empty segment would be a DIFFERENT key: `/registry/nodes//cid`.
        assert_eq!(key("", "nodes", None, "cid", false), "/registry/nodes/cid");
        assert_eq!(
            key("", "namespaces", None, "default", false),
            "/registry/namespaces/default"
        );
    }

    #[test]
    fn services_and_endpoints_live_in_the_split_subtree() {
        // The most-missed rule in the whole layout. Endpoints under
        // `/registry/endpoints/` would silently return nothing.
        assert_eq!(
            key("", "services", Some("default"), "kubernetes", true),
            "/registry/services/specs/default/kubernetes"
        );
        assert_eq!(
            key("", "endpoints", Some("default"), "kubernetes", true),
            "/registry/services/endpoints/default/kubernetes"
        );
    }

    #[test]
    fn apps_and_batch_legacy_kinds_are_stored_grouplessly() {
        // These have groups but predate them; the keys were never migrated.
        for (group, plural) in [
            ("apps", "deployments"),
            ("apps", "replicasets"),
            ("apps", "daemonsets"),
            ("apps", "statefulsets"),
            ("batch", "jobs"),
            ("batch", "cronjobs"),
        ] {
            assert_eq!(
                key(group, plural, Some("default"), "x", true),
                format!("/registry/{plural}/default/x"),
                "{group}/{plural} must NOT carry its group segment"
            );
        }
    }

    #[test]
    fn other_grouped_kinds_do_carry_their_group() {
        assert_eq!(
            key(
                "rbac.authorization.k8s.io",
                "clusterroles",
                None,
                "admin",
                false
            ),
            "/registry/rbac.authorization.k8s.io/clusterroles/admin"
        );
        assert_eq!(
            key("rbac.authorization.k8s.io", "roles", Some("ns"), "r", true),
            "/registry/rbac.authorization.k8s.io/roles/ns/r"
        );
    }

    #[test]
    fn collection_prefixes_end_in_a_slash() {
        // Without it, `--prefix /registry/pods` would also match a
        // hypothetical `/registry/podtemplates`.
        let k = object_key("", "pods", true, Some("default"), "nginx");
        assert_eq!(k.collection_prefix, "/registry/pods/default/");
        assert!(k.key.starts_with(&k.collection_prefix));

        // All-namespaces listing drops only the namespace segment.
        let all = object_key("", "pods", true, None, "");
        assert_eq!(all.collection_prefix, "/registry/pods/");
    }

    #[test]
    fn prefix_range_end_is_the_incremented_last_byte() {
        // etcd has no prefix flag on the wire; `--prefix` IS this.
        assert_eq!(
            prefix_range_end(b"/registry/pods/"),
            b"/registry/pods0".to_vec()
        );
        assert_eq!(prefix_range_end(&[0x01, 0x02]), vec![0x01, 0x03]);
        // Carry over a trailing 0xFF.
        assert_eq!(prefix_range_end(&[0x01, 0xFF]), vec![0x02]);
        // No successor at all ⇒ scan to the end of the keyspace, which etcd
        // spells as the single zero byte.
        assert_eq!(prefix_range_end(&[0xFF, 0xFF]), vec![0]);
        assert_eq!(prefix_range_end(b""), vec![0]);
    }

    #[test]
    fn a_namespace_passed_for_a_cluster_scoped_kind_is_dropped() {
        // A category error by the caller must not become a wrong KEY.
        assert_eq!(
            key("", "nodes", Some("default"), "cid", false),
            "/registry/nodes/cid"
        );
    }
}
