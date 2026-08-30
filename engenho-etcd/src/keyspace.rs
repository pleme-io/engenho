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
//! ★ THIS MODULE IS MEASURED, NOT REASONED — and the distinction is not
//! academic. The first cut was written from careful reasoning about
//! upstream's storage registrations and was WRONG IN THREE WAYS. It was
//! corrected against a 699-key corpus taken from a live k3s
//! (`tests/fixtures/k3s-registry-keys.txt`, Kubernetes v1.34.3, captured
//! 2026-08-29). Each error is now a test:
//!
//!   1. **Nodes are stored as `minions`** — `/registry/minions/<name>`. The
//!      pre-1.0 name, never migrated, because migrating it would strand
//!      every existing cluster's data. No amount of reading the modern API
//!      surface reveals this.
//!   2. **Almost nothing carries a group segment.** rbac, storage,
//!      discovery, networking, coordination, scheduling, node and
//!      flowcontrol kinds are ALL stored grouplessly — `/registry/roles/…`,
//!      `/registry/csinodes/…`, `/registry/leases/…`.
//!   3. **Therefore `grouped` is the EXCEPTION.** The original code carried
//!      an allowlist of groupless kinds; the polarity was inverted. Only
//!      custom resources plus the two `apiextensions.k8s.io` /
//!      `apiregistration.k8s.io` built-ins are grouped.
//!
//! ★ THE SPLIT SUBTREE IS REAL and was the one original rule that held:
//! `/registry/services/specs/<ns>/<name>` holds a Service and
//! `/registry/services/endpoints/<ns>/<name>` holds its Endpoints. A reader
//! that files Endpoints under `/registry/endpoints/` finds nothing.
//!
//! ★ CLUSTER-SCOPED KINDS HAVE NO NAMESPACE SEGMENT. `/registry/minions/cid`,
//! never `/registry/minions//cid` — an empty segment is a DIFFERENT key, and
//! the empty-vs-absent distinction is the same silent-empty-range failure.
//!
//! ★ PREFIXES END IN `/`. `etcdctl get /registry/pods --prefix` would also
//! match `/registry/podtemplates`; the trailing slash is what makes a prefix
//! scan mean "this resource" rather than "anything starting with these
//! letters".

/// Where every Kubernetes object lives. Upstream's default `--etcd-prefix`;
/// a cluster may override it, so it is a constant rather than a literal
/// sprinkled through the module.
pub const REGISTRY_ROOT: &str = "/registry";

/// The ONLY built-in groups whose keys carry a group segment.
///
/// Measured, not assumed: in the 699-key corpus exactly two built-in groups
/// appear as a path segment. Every other built-in group's kinds are stored
/// grouplessly. Custom resources are always grouped, which is what makes
/// this a short closed list rather than an open-ended one.
pub const GROUPED_BUILTIN_GROUPS: &[&str] = &["apiextensions.k8s.io", "apiregistration.k8s.io"];

/// Built-in API groups engenho serves. Membership decides GROUPLESS vs
/// GROUPED: a group in this list (and not in [`GROUPED_BUILTIN_GROUPS`])
/// stores its kinds without a group segment; anything else is a custom
/// resource and is grouped.
///
/// Deliberately explicit rather than a `contains("k8s.io")` heuristic —
/// `cilium.io`, `helm.cattle.io` and `pangea.pleme.io` are all CRDs that a
/// suffix test would misfile, and all three are in the corpus.
pub const BUILTIN_GROUPS: &[&str] = &[
    "",
    "admissionregistration.k8s.io",
    "apps",
    "authentication.k8s.io",
    "authorization.k8s.io",
    "autoscaling",
    "batch",
    "certificates.k8s.io",
    "coordination.k8s.io",
    "discovery.k8s.io",
    "flowcontrol.apiserver.k8s.io",
    "networking.k8s.io",
    "node.k8s.io",
    "policy",
    "rbac.authorization.k8s.io",
    "scheduling.k8s.io",
    "storage.k8s.io",
];

/// Plurals whose `/registry` segment is NOT the plural.
///
/// One entry today, and it is the one nobody guesses: Nodes are `minions`.
const REGISTRY_SEGMENT_OVERRIDES: &[(&str, &str)] = &[("nodes", "minions")];

/// How a kind's key is shaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyShape {
    /// `/registry/<segment>/<ns>/<name>` — the common case.
    GrouplessNamespaced,
    /// `/registry/<segment>/<name>` — cluster-scoped, no group segment.
    GrouplessClusterScoped,
    /// `/registry/<group>/<plural>/<ns>/<name>` — custom resources.
    GroupedNamespaced,
    /// `/registry/<group>/<plural>/<name>` — CRDs, APIServices.
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

/// Non-object keys that live in the registry: allocator state, apiserver
/// endpoint leases, and the health sentinel.
///
/// They are NOT resources — no GVK, no namespace, no object semantics — and
/// a façade that tried to parse them as objects would produce nonsense GVKs
/// for real keys a backup tool must round-trip. Enumerated so they can be
/// recognised and passed through verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingletonKey {
    /// `/registry/health` — the apiserver's storage liveness sentinel.
    Health,
    /// `/registry/masterleases/<ip>` — apiserver endpoint leases.
    MasterLease,
    /// `/registry/ranges/serviceips` and `/registry/ranges/servicenodeports`
    /// — the Service VIP and NodePort allocator bitmaps.
    Range,
}

/// Classify a registry key that is not an ordinary object key.
#[must_use]
pub fn singleton_for(key: &str) -> Option<SingletonKey> {
    match key {
        "/registry/health" => Some(SingletonKey::Health),
        k if k.starts_with("/registry/masterleases/") => Some(SingletonKey::MasterLease),
        k if k.starts_with("/registry/ranges/") => Some(SingletonKey::Range),
        _ => None,
    }
}

/// The `/registry` path segment for a resource plural.
#[must_use]
pub fn registry_segment(plural: &str) -> &str {
    REGISTRY_SEGMENT_OVERRIDES
        .iter()
        .find(|(from, _)| *from == plural)
        .map_or(plural, |(_, to)| *to)
}

/// Is this group stored WITHOUT a group segment?
#[must_use]
pub fn is_groupless(group: &str) -> bool {
    BUILTIN_GROUPS.contains(&group) && !GROUPED_BUILTIN_GROUPS.contains(&group)
}

/// Decide a kind's key shape.
///
/// `plural` is the lowercase resource plural (`pods`, `clusterroles`);
/// `group` is the API group (`""` for core, `apps`, `cilium.io`).
#[must_use]
pub fn shape_for(group: &str, plural: &str, namespaced: bool) -> KeyShape {
    // The Service split is checked FIRST: `endpoints` is a core plural that
    // would otherwise resolve to `/registry/endpoints/`, which holds nothing.
    if is_groupless(group) {
        match plural {
            "services" => return KeyShape::ServiceSubtree("specs"),
            "endpoints" => return KeyShape::ServiceSubtree("endpoints"),
            _ => {}
        }
    }
    match (is_groupless(group), namespaced) {
        (true, true) => KeyShape::GrouplessNamespaced,
        (true, false) => KeyShape::GrouplessClusterScoped,
        (false, true) => KeyShape::GroupedNamespaced,
        (false, false) => KeyShape::GroupedClusterScoped,
    }
}

/// The etcd key for one object, and the prefix listing its collection.
///
/// `namespace` is ignored for cluster-scoped shapes rather than rejected: a
/// caller passing `Some("default")` for a Node has made a category error the
/// key must not encode, and dropping it is what upstream does.
#[must_use]
pub fn object_key(
    group: &str,
    plural: &str,
    namespaced: bool,
    namespace: Option<&str>,
    name: &str,
) -> ObjectKey {
    let seg = registry_segment(plural);
    let ns = namespace.unwrap_or_default();
    let collection_prefix = match shape_for(group, plural, namespaced) {
        KeyShape::GrouplessNamespaced => {
            if ns.is_empty() {
                format!("{REGISTRY_ROOT}/{seg}/")
            } else {
                format!("{REGISTRY_ROOT}/{seg}/{ns}/")
            }
        }
        KeyShape::GrouplessClusterScoped => format!("{REGISTRY_ROOT}/{seg}/"),
        KeyShape::GroupedNamespaced => {
            if ns.is_empty() {
                format!("{REGISTRY_ROOT}/{group}/{seg}/")
            } else {
                format!("{REGISTRY_ROOT}/{group}/{seg}/{ns}/")
            }
        }
        KeyShape::GroupedClusterScoped => format!("{REGISTRY_ROOT}/{group}/{seg}/"),
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
/// byte — a real edge case, because `\0` is also how "from here to infinity"
/// is requested.
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
mod tests_support {
    /// The oracle: every distinct key a real kube-apiserver wrote.
    pub const ORACLE: &str = include_str!("../tests/fixtures/k3s-registry-keys.txt");

    pub fn oracle_keys() -> impl Iterator<Item = &'static str> {
        ORACLE
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("/registry/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::tests_support::oracle_keys;

    fn key(group: &str, plural: &str, ns: Option<&str>, name: &str, namespaced: bool) -> String {
        object_key(group, plural, namespaced, ns, name).key
    }

    // ── The three measured corrections ────────────────────────────────

    #[test]
    fn nodes_are_stored_as_minions() {
        // The rule no amount of reading the modern API surface reveals.
        assert_eq!(
            key("", "nodes", None, "cid", false),
            "/registry/minions/cid"
        );
        assert!(
            oracle_keys().any(|k| k == "/registry/minions/rio"),
            "the oracle must contain the minions key this rule is derived from"
        );
        assert!(
            !oracle_keys().any(|k| k.starts_with("/registry/nodes/")),
            "a real apiserver never writes /registry/nodes/"
        );
    }

    #[test]
    fn builtin_non_core_groups_are_groupless() {
        // Every one of these was WRONG in the first cut.
        for (group, plural, ns, name) in [
            ("rbac.authorization.k8s.io", "clusterroles", None, "view"),
            (
                "rbac.authorization.k8s.io",
                "roles",
                Some("kube-system"),
                "r",
            ),
            ("storage.k8s.io", "csinodes", None, "rio"),
            ("storage.k8s.io", "storageclasses", None, "local-path"),
            (
                "coordination.k8s.io",
                "leases",
                Some("kube-node-lease"),
                "rio",
            ),
            ("discovery.k8s.io", "endpointslices", Some("default"), "es"),
            (
                "scheduling.k8s.io",
                "priorityclasses",
                None,
                "system-cluster-critical",
            ),
            ("node.k8s.io", "runtimeclasses", None, "rc"),
            ("flowcontrol.apiserver.k8s.io", "flowschemas", None, "fs"),
            ("networking.k8s.io", "ingressclasses", None, "traefik"),
            (
                "apps",
                "deployments",
                Some("flux-system"),
                "helm-controller",
            ),
            ("batch", "jobs", Some("default"), "j"),
        ] {
            let namespaced = ns.is_some();
            let seg = registry_segment(plural);
            let want = match ns {
                Some(n) => format!("/registry/{seg}/{n}/{name}"),
                None => format!("/registry/{seg}/{name}"),
            };
            assert_eq!(
                key(group, plural, ns, name, namespaced),
                want,
                "{group}/{plural} must NOT carry its group segment"
            );
        }
    }

    #[test]
    fn only_crds_and_two_builtins_are_grouped() {
        // Grouped is the EXCEPTION — the polarity the first cut inverted.
        assert_eq!(
            key(
                "apiextensions.k8s.io",
                "customresourcedefinitions",
                None,
                "x.cilium.io",
                false
            ),
            "/registry/apiextensions.k8s.io/customresourcedefinitions/x.cilium.io"
        );
        assert_eq!(
            key(
                "apiregistration.k8s.io",
                "apiservices",
                None,
                "v1.metrics",
                false
            ),
            "/registry/apiregistration.k8s.io/apiservices/v1.metrics"
        );
        // A custom resource — a suffix heuristic on "k8s.io" would misfile
        // every one of these, and all three groups are in the corpus.
        assert_eq!(
            key(
                "cilium.io",
                "ciliumnetworkpolicies",
                Some("kube-system"),
                "p",
                true
            ),
            "/registry/cilium.io/ciliumnetworkpolicies/kube-system/p"
        );
        for g in [
            "cilium.io",
            "helm.cattle.io",
            "pangea.pleme.io",
            "actions.github.com",
        ] {
            assert!(!is_groupless(g), "{g} is a CRD group and must be grouped");
        }
    }

    // ── The rule that survived ────────────────────────────────────────

    #[test]
    fn services_and_endpoints_live_in_the_split_subtree() {
        assert_eq!(
            key("", "services", Some("default"), "kubernetes", true),
            "/registry/services/specs/default/kubernetes"
        );
        assert_eq!(
            key("", "endpoints", Some("default"), "kubernetes", true),
            "/registry/services/endpoints/default/kubernetes"
        );
        assert!(oracle_keys().any(|k| k == "/registry/services/specs/default/kubernetes"));
        assert!(oracle_keys().any(|k| k == "/registry/services/endpoints/default/kubernetes"));
    }

    // ── The corpus itself ─────────────────────────────────────────────

    #[test]
    fn the_oracle_is_present_and_substantial() {
        let n = oracle_keys().count();
        assert!(
            n > 600,
            "expected the full captured corpus, got {n} keys — was the fixture truncated?"
        );
    }

    #[test]
    fn every_oracle_key_is_classified_and_none_is_orphaned() {
        // The anti-vacuity half: a classifier that recognised nothing would
        // pass a "no key is misclassified" test trivially. Assert coverage.
        let mut objects = 0usize;
        let mut singletons = 0usize;
        for k in oracle_keys() {
            if singleton_for(k).is_some() {
                singletons += 1;
                continue;
            }
            let segs: Vec<&str> = k.trim_start_matches("/registry/").split('/').collect();
            assert!(
                (2..=4).contains(&segs.len()),
                "unclassifiable key shape: {k} ({} segments)",
                segs.len()
            );
            objects += 1;
        }
        assert!(objects > 600, "too few object keys classified: {objects}");
        assert!(
            singletons >= 3,
            "expected health + masterleases + ranges, got {singletons}"
        );
    }

    #[test]
    fn singleton_families_are_recognised_not_parsed_as_objects() {
        assert_eq!(
            singleton_for("/registry/health"),
            Some(SingletonKey::Health)
        );
        assert_eq!(
            singleton_for("/registry/masterleases/10.248.52.137"),
            Some(SingletonKey::MasterLease)
        );
        assert_eq!(
            singleton_for("/registry/ranges/servicenodeports"),
            Some(SingletonKey::Range)
        );
        assert_eq!(singleton_for("/registry/pods/default/nginx"), None);
        // Each family must actually appear in the corpus, or the arm is dead
        // code justified by nothing.
        for probe in [
            "/registry/health",
            "/registry/masterleases/",
            "/registry/ranges/",
        ] {
            assert!(
                oracle_keys().any(|k| k.starts_with(probe)),
                "{probe} is not in the corpus — the arm is unjustified"
            );
        }
    }

    #[test]
    fn rendered_keys_round_trip_against_the_oracle() {
        // Render from (group, plural, ns, name) and require byte-identity
        // with what the real apiserver wrote. This is the whole point.
        let cases: &[(&str, &str, Option<&str>, &str)] = &[
            ("", "pods", Some("kube-system"), "coredns-5bd557ffb9-84wzh"),
            ("", "configmaps", Some("kube-system"), "coredns"),
            ("", "namespaces", None, "kube-system"),
            ("", "secrets", Some("kube-system"), "chart-values-traefik"),
            ("", "serviceaccounts", Some("default"), "default"),
            (
                "",
                "persistentvolumes",
                None,
                "pvc-eea63c9f-e3ba-4de9-abb3-138caa33b0be",
            ),
            (
                "apps",
                "deployments",
                Some("flux-system"),
                "source-controller",
            ),
            (
                "apps",
                "daemonsets",
                Some("kube-system"),
                "svclb-traefik-3504b541",
            ),
            (
                "rbac.authorization.k8s.io",
                "clusterroles",
                None,
                "system:kube-scheduler",
            ),
            ("storage.k8s.io", "csinodes", None, "rio"),
        ];
        for (g, p, ns, n) in cases {
            let rendered = key(g, p, *ns, n, ns.is_some());
            assert!(
                oracle_keys().any(|k| k == rendered),
                "rendered {rendered} is NOT a key the real apiserver wrote"
            );
        }
    }

    // ── Shape invariants ──────────────────────────────────────────────

    #[test]
    fn cluster_scoped_kinds_have_no_namespace_segment() {
        // An empty segment would be a DIFFERENT key: `/registry/minions//cid`.
        assert_eq!(
            key("", "nodes", None, "cid", false),
            "/registry/minions/cid"
        );
        assert_eq!(
            key("", "namespaces", None, "default", false),
            "/registry/namespaces/default"
        );
        // A category error by the caller must not become a wrong KEY.
        assert_eq!(
            key("", "nodes", Some("default"), "cid", false),
            "/registry/minions/cid"
        );
    }

    #[test]
    fn collection_prefixes_end_in_a_slash() {
        let k = object_key("", "pods", true, Some("default"), "nginx");
        assert_eq!(k.collection_prefix, "/registry/pods/default/");
        assert!(k.key.starts_with(&k.collection_prefix));
        // All-namespaces listing drops only the namespace segment.
        assert_eq!(
            object_key("", "pods", true, None, "").collection_prefix,
            "/registry/pods/"
        );
    }

    #[test]
    fn prefix_range_end_is_the_incremented_last_byte() {
        // etcd has no prefix flag on the wire; `--prefix` IS this.
        assert_eq!(
            prefix_range_end(b"/registry/pods/"),
            b"/registry/pods0".to_vec()
        );
        assert_eq!(prefix_range_end(&[0x01, 0x02]), vec![0x01, 0x03]);
        assert_eq!(prefix_range_end(&[0x01, 0xFF]), vec![0x02]);
        // No successor at all ⇒ scan to the end of the keyspace.
        assert_eq!(prefix_range_end(&[0xFF, 0xFF]), vec![0]);
        assert_eq!(prefix_range_end(b""), vec![0]);
    }
}

// ─────────────────────────────────────────────────────────────────────
// THE REVERSE DIRECTION — parsing a `/registry` key back to a GVK.
//
// ★ WHY BOTH DIRECTIONS ARE NEEDED. Rendering answers "where do I write
// this object". Parsing answers the question every etcd CONSUMER asks:
// `Range("/registry/pods/", …)` arrives as bytes and must become a store
// lookup. Without the reverse map the façade can only serve keys it was
// told about, which is to say it cannot serve etcdctl at all.
//
// ★ WHY IT IS NOT MERELY THE INVERSE FUNCTION. The forward map loses
// information: `/registry/deployments/…` does not say `apps/v1`, and
// `/registry/leases/…` does not say `coordination.k8s.io/v1`. Recovering
// the group requires the CATALOG — which is exactly why this lives against
// `engenho_types::RESOURCE_CATALOG` rather than a hand-built table that
// could drift from the kinds actually served.
//
// ★ AMBIGUITY IS REAL AND MUST NOT BE GUESSED. Two served kinds can share
// a plural across groups (upstream's historical `events` in core v1 and
// `events.k8s.io/v1` is the archetype). The catalog is scanned in order and
// the FIRST match wins, which is deterministic; where a genuine collision
// exists the resolver reports it rather than silently picking, because a
// silently-wrong GVK produces a valid-looking read of the wrong object.
// ─────────────────────────────────────────────────────────────────────

use engenho_types::generated_v1_34::{RESOURCE_CATALOG, ResourceDescriptor};

/// What a `/registry` key denotes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedKey {
    /// An ordinary API object.
    Object {
        /// Owned, not `&'static`: a custom resource's group and plural come
        /// from the KEY, not from the compiled-in catalog, and the façade
        /// must serve CRs it was never built against.
        group: String,
        version: String,
        kind: String,
        plural: String,
        namespace: Option<String>,
        name: String,
    },
    /// Allocator state / apiserver leases / the health sentinel. Carried
    /// verbatim — these are not objects and must not be given a GVK.
    Singleton { kind: SingletonKey, key: String },
}

/// Why a key could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseKeyError {
    #[error("key does not start with {REGISTRY_ROOT}/: {key}")]
    NotRegistry { key: String },
    #[error("no served kind has registry segment '{segment}' (key: {key})")]
    UnknownSegment { segment: String, key: String },
    #[error("segment '{segment}' is served by {count} kinds; refusing to guess (key: {key})")]
    AmbiguousSegment {
        segment: String,
        count: usize,
        key: String,
    },
    #[error("key has {segments} segments, which no shape produces: {key}")]
    UnexpectedShape { segments: usize, key: String },
}

/// Every descriptor whose `/registry` segment is `segment`, in an
/// unqualified (groupless) position.
fn groupless_candidates(segment: &str) -> Vec<&'static ResourceDescriptor> {
    RESOURCE_CATALOG
        .iter()
        .filter(|d| is_groupless(d.group) && registry_segment(d.plural) == segment)
        .collect()
}

/// Resolve a descriptor for a grouped key.
fn grouped_descriptor(group: &str, plural: &str) -> Option<&'static ResourceDescriptor> {
    RESOURCE_CATALOG
        .iter()
        .find(|d| d.group == group && d.plural == plural)
}

/// Parse a `/registry` key back to what it denotes.
///
/// Custom resources are not in the catalog, so a grouped key whose group is
/// unknown still resolves structurally — group and plural come from the key
/// itself, and only `kind` is unavailable. That is correct: a façade must
/// serve CRs it has never been compiled against.
pub fn parse_key(key: &str) -> Result<ParsedKey, ParseKeyError> {
    if let Some(kind) = singleton_for(key) {
        return Ok(ParsedKey::Singleton {
            kind,
            key: key.to_string(),
        });
    }
    let rest = key
        .strip_prefix(REGISTRY_ROOT)
        .and_then(|r| r.strip_prefix('/'))
        .ok_or_else(|| ParseKeyError::NotRegistry {
            key: key.to_string(),
        })?;
    let segs: Vec<&str> = rest.split('/').collect();

    // The Service split, checked first for the same reason as in rendering.
    if segs.len() == 4 && segs[0] == "services" && matches!(segs[1], "specs" | "endpoints") {
        let (kind, plural) = if segs[1] == "specs" {
            ("Service", "services")
        } else {
            ("Endpoints", "endpoints")
        };
        return Ok(ParsedKey::Object {
            group: String::new(),
            version: "v1".to_string(),
            kind: kind.to_string(),
            plural: plural.to_string(),
            namespace: Some(segs[2].to_string()),
            name: segs[3].to_string(),
        });
    }

    // A group segment always contains a dot; no built-in groupless segment
    // does. This is the one structural discriminator the key itself carries.
    let grouped = segs[0].contains('.');

    match (grouped, segs.len()) {
        // /registry/<segment>/<name>  — groupless cluster-scoped
        // /registry/<segment>/<ns>/<name> — groupless namespaced
        (false, 2 | 3) => {
            let cands = groupless_candidates(segs[0]);
            let d = match cands.len() {
                0 => {
                    return Err(ParseKeyError::UnknownSegment {
                        segment: segs[0].to_string(),
                        key: key.to_string(),
                    });
                }
                1 => cands[0],
                n => {
                    // Deterministic only if every candidate agrees on GVK;
                    // otherwise refuse rather than serve the wrong object.
                    let first = cands[0];
                    if cands
                        .iter()
                        .all(|c| c.group == first.group && c.kind == first.kind)
                    {
                        first
                    } else {
                        return Err(ParseKeyError::AmbiguousSegment {
                            segment: segs[0].to_string(),
                            count: n,
                            key: key.to_string(),
                        });
                    }
                }
            };
            let (namespace, name) = if segs.len() == 3 {
                (Some(segs[1].to_string()), segs[2].to_string())
            } else {
                (None, segs[1].to_string())
            };
            Ok(ParsedKey::Object {
                group: d.group.to_string(),
                version: d.version.to_string(),
                kind: d.kind.to_string(),
                plural: d.plural.to_string(),
                namespace,
                name,
            })
        }
        // /registry/<group>/<plural>/<name>        — grouped cluster-scoped
        // /registry/<group>/<plural>/<ns>/<name>   — grouped namespaced
        (true, 3 | 4) => {
            let d = grouped_descriptor(segs[0], segs[1]);
            let (namespace, name) = if segs.len() == 4 {
                (Some(segs[2].to_string()), segs[3].to_string())
            } else {
                (None, segs[2].to_string())
            };
            Ok(ParsedKey::Object {
                group: d.map_or(segs[0], |d| d.group).to_string(),
                version: d.map_or("", |d| d.version).to_string(),
                // A custom resource has no compiled-in kind; the empty
                // string says "structurally resolved, kind unknown" rather
                // than inventing one.
                kind: d.map_or("", |d| d.kind).to_string(),
                plural: d.map_or(segs[1], |d| d.plural).to_string(),
                namespace,
                name,
            })
        }
        (_, n) => Err(ParseKeyError::UnexpectedShape {
            segments: n,
            key: key.to_string(),
        }),
    }
}

#[cfg(test)]
mod parse_tests {
    use super::tests_support::oracle_keys;
    use super::*;

    fn obj(key: &str) -> (String, String, Option<String>, String) {
        match parse_key(key).expect("parses") {
            ParsedKey::Object {
                group,
                kind,
                namespace,
                name,
                ..
            } => (group, kind, namespace, name),
            other => panic!("expected an object, got {other:?}"),
        }
    }

    #[test]
    fn the_group_is_recovered_from_the_catalog_not_the_key() {
        // The forward map LOSES the group; only the catalog restores it.
        assert_eq!(
            obj("/registry/deployments/flux-system/source-controller"),
            (
                "apps".to_string(),
                "Deployment".to_string(),
                Some("flux-system".into()),
                "source-controller".into()
            )
        );
        assert_eq!(
            obj("/registry/leases/kube-node-lease/rio").0,
            "coordination.k8s.io"
        );
        assert_eq!(obj("/registry/csinodes/rio").0, "storage.k8s.io");
        assert_eq!(
            obj("/registry/clusterroles/view"),
            (
                "rbac.authorization.k8s.io".to_string(),
                "ClusterRole".to_string(),
                None,
                "view".into()
            )
        );
    }

    #[test]
    fn minions_parses_back_to_a_node() {
        assert_eq!(
            obj("/registry/minions/rio"),
            (String::new(), "Node".to_string(), None, "rio".into())
        );
    }

    #[test]
    fn the_service_split_parses_to_two_different_kinds() {
        assert_eq!(
            obj("/registry/services/specs/default/kubernetes").1,
            "Service"
        );
        assert_eq!(
            obj("/registry/services/endpoints/default/kubernetes").1,
            "Endpoints"
        );
    }

    #[test]
    fn custom_resources_resolve_structurally_without_being_compiled_in() {
        // A façade must serve CRs it was never built against. Group and
        // plural come from the key; kind is empty rather than invented.
        let p = parse_key("/registry/cilium.io/ciliumnetworkpolicies/kube-system/p").unwrap();
        assert_eq!(
            p,
            ParsedKey::Object {
                group: "cilium.io".to_string(),
                version: String::new(),
                kind: String::new(),
                plural: "ciliumnetworkpolicies".to_string(),
                namespace: Some("kube-system".into()),
                name: "p".into(),
            }
        );
    }

    #[test]
    fn singletons_are_carried_verbatim_never_given_a_gvk() {
        for k in [
            "/registry/health",
            "/registry/masterleases/10.248.52.137",
            "/registry/ranges/servicenodeports",
        ] {
            match parse_key(k).unwrap() {
                ParsedKey::Singleton { key, .. } => assert_eq!(key, k),
                other => panic!("{k} must not parse as an object: {other:?}"),
            }
        }
    }

    #[test]
    fn a_non_registry_key_is_refused_not_coerced() {
        assert!(matches!(
            parse_key("/other/pods/default/x"),
            Err(ParseKeyError::NotRegistry { .. })
        ));
        assert!(matches!(
            parse_key("/registry/definitelynotakind/x"),
            Err(ParseKeyError::UnknownSegment { .. })
        ));
    }

    /// Registry segments present in the oracle that engenho does NOT serve.
    ///
    /// These are real Kubernetes kinds (`networking.k8s.io` IPAddress and
    /// ServiceCIDR) that a v1.34 apiserver writes and engenho's catalog has
    /// no descriptor for. A key for an unserved kind CANNOT resolve to a
    /// GVK, and inventing one would be worse than refusing.
    ///
    /// Declared as a closed list rather than tolerated as "some failures are
    /// fine", so the gate works in BOTH directions: a NEW unserved kind
    /// appearing is a failure, and a listed kind that starts resolving is
    /// also a failure — the list must be tightened rather than left to rot.
    /// This is the `engenho-diff` xfail discipline applied at unit scale.
    const UNSERVED_SEGMENTS: &[&str] = &["ipaddresses", "servicecidrs"];

    #[test]
    fn every_oracle_key_parses_except_the_declared_unserved_kinds() {
        let mut unexpected = Vec::new();
        let mut unserved_seen: Vec<&str> = Vec::new();
        let mut objects = 0usize;
        for k in oracle_keys() {
            match parse_key(k) {
                Ok(ParsedKey::Object { .. }) => objects += 1,
                Ok(ParsedKey::Singleton { .. }) => {}
                Err(e) => {
                    let seg = k
                        .trim_start_matches("/registry/")
                        .split('/')
                        .next()
                        .unwrap_or_default();
                    if let Some(known) = UNSERVED_SEGMENTS.iter().find(|u| **u == seg) {
                        if !unserved_seen.contains(known) {
                            unserved_seen.push(known);
                        }
                    } else {
                        unexpected.push(format!("{k}: {e}"));
                    }
                }
            }
        }
        assert!(
            unexpected.is_empty(),
            "{} oracle keys failed to parse for an UNDECLARED reason:\n  {}",
            unexpected.len(),
            unexpected.join("\n  ")
        );
        // The other direction: a declared gap that has silently closed must
        // be removed from the list, or the list stops meaning anything.
        let stale: Vec<&&str> = UNSERVED_SEGMENTS
            .iter()
            .filter(|u| !unserved_seen.contains(*u))
            .collect();
        assert!(
            stale.is_empty(),
            "these segments are declared unserved but now resolve — tighten \
             UNSERVED_SEGMENTS: {stale:?}"
        );
        assert!(objects > 600, "too few objects parsed: {objects}");
    }

    #[test]
    fn round_trip_render_parse_render_is_stable_over_the_whole_oracle() {
        // The bijection property: parse a real key, render it back, get the
        // same bytes. Anything less means a Range would read a different
        // object than the one the caller addressed.
        let mut checked = 0usize;
        for k in oracle_keys() {
            let Ok(parsed) = parse_key(k) else {
                continue; // declared-unserved kinds, covered by the test above
            };
            let ParsedKey::Object {
                group,
                plural,
                namespace,
                name,
                ..
            } = parsed
            else {
                continue;
            };
            // CRs are not in the catalog, so `namespaced` cannot be derived
            // for them here; the structural round-trip still applies.
            let rendered = object_key(
                &group,
                &plural,
                namespace.is_some(),
                namespace.as_deref(),
                &name,
            )
            .key;
            assert_eq!(rendered, k, "round-trip changed the key");
            checked += 1;
        }
        assert!(checked > 600, "round-trip covered only {checked} keys");
    }
}
