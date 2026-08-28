//! Vendored Kubernetes OpenAPI v3 group documents, embedded into the
//! binary.
//!
//! The apiserver serves the K8s OpenAPI-v3 discovery surface
//! (`/openapi/v3` index + `/openapi/v3/api/v1` + `/openapi/v3/apis/<g>/<v>`)
//! that `kubectl explain` and client-side `--validate` consume. Those
//! consumers expect the FULL per-kind schemas with
//! `x-kubernetes-group-version-kind` metadata — exactly the bodies vendored
//! under `vendor/openapi/v1.34.0/` (BLAKE3-attested via `MANIFEST.yaml`,
//! CI-roundtrip-verified by `tests/vendored_openapi_blake3.rs`).
//!
//! Embedding via `include_str!` means the apiserver never reads the
//! filesystem at runtime — the documents are part of the binary, so the
//! served bytes can never drift from the attested source.
//!
//! ## Single source of truth
//!
//! [`SERVED`] is the ONE table that drives both the `/openapi/v3` index and
//! the per-group [`document_for`] lookup. It mirrors `MANIFEST.yaml`'s
//! `files` list, scoped to the schema-backed (non-opaque) cataloged groups.
//! Per the ★★ CATALOG REFLECTION invariant the set of `(group, version)`
//! pairs here MUST equal the distinct NON-OPAQUE pairs in
//! [`crate::generated_v1_34::RESOURCE_CATALOG`] — a test
//! (`tests/generated_catalog_invariants.rs`) asserts it, so adding a group is
//! one catalog change + one vendored file + one [`SERVED`] row, never a
//! hand-wired route.

use std::collections::BTreeSet;

/// The vendored core (`""`/v1) OpenAPI v3 document.
pub const CORE_V1: &str = include_str!("../vendor/openapi/v1.34.0/api__v1_openapi.json");

/// The vendored apps/v1 OpenAPI v3 document.
pub const APPS_V1: &str = include_str!("../vendor/openapi/v1.34.0/apis__apps__v1_openapi.json");

/// The vendored rbac.authorization.k8s.io/v1 OpenAPI v3 document.
pub const RBAC_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__rbac.authorization.k8s.io__v1_openapi.json");

/// The vendored batch/v1 OpenAPI v3 document (Job, CronJob).
pub const BATCH_V1: &str = include_str!("../vendor/openapi/v1.34.0/apis__batch__v1_openapi.json");

/// The vendored networking.k8s.io/v1 OpenAPI v3 document (Ingress,
/// IngressClass, NetworkPolicy).
pub const NETWORKING_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__networking.k8s.io__v1_openapi.json");

/// The vendored policy/v1 OpenAPI v3 document (PodDisruptionBudget).
pub const POLICY_V1: &str = include_str!("../vendor/openapi/v1.34.0/apis__policy__v1_openapi.json");

/// The vendored storage.k8s.io/v1 OpenAPI v3 document (StorageClass, CSINode,
/// CSIDriver, VolumeAttachment, CSIStorageCapacity).
pub const STORAGE_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__storage.k8s.io__v1_openapi.json");

/// The vendored scheduling.k8s.io/v1 OpenAPI v3 document (PriorityClass).
pub const SCHEDULING_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__scheduling.k8s.io__v1_openapi.json");

/// The vendored coordination.k8s.io/v1 OpenAPI v3 document (Lease).
pub const COORDINATION_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__coordination.k8s.io__v1_openapi.json");

/// The vendored node.k8s.io/v1 OpenAPI v3 document (RuntimeClass).
pub const NODE_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__node.k8s.io__v1_openapi.json");

/// The vendored autoscaling/v2 OpenAPI v3 document (HorizontalPodAutoscaler).
pub const AUTOSCALING_V2: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__autoscaling__v2_openapi.json");

/// One served OpenAPI v3 group document: the `(group, version)` it
/// describes, the embedded body, and the BLAKE3 digest from the manifest
/// (used as the `?hash=` cache key in the discovery index).
#[derive(Clone, Copy, Debug)]
pub struct ServedDoc {
    /// API group (`""` for core).
    pub group: &'static str,
    /// API version (`"v1"`).
    pub version: &'static str,
    /// The full vendored OpenAPI v3 document body.
    pub body: &'static str,
    /// The BLAKE3 digest of `body` per `MANIFEST.yaml` — a stable per-doc
    /// cache key kubectl appends as `?hash=`.
    pub blake3: &'static str,
}

impl ServedDoc {
    /// The discovery-index path key for this document: `api/v1` for the
    /// core group, `apis/<group>/<version>` for a named group. (Matches the
    /// upstream kube-apiserver `/openapi/v3` key convention.)
    #[must_use]
    pub fn index_key(&self) -> String {
        if self.group.is_empty() {
            ["api/", self.version].concat()
        } else {
            ["apis/", self.group, "/", self.version].concat()
        }
    }
}

/// Every served OpenAPI v3 group document — the SINGLE source the discovery
/// index + [`document_for`] both iterate. Scoped to exactly the schema-backed
/// (non-opaque) cataloged groups. BLAKE3 digests mirror `MANIFEST.yaml` (and
/// are re-verified against `body` by a test, so a manifest/body drift fails
/// CI). Per the ★★ CATALOG REFLECTION invariant the set of `(group, version)`
/// pairs here MUST equal the distinct pairs of the NON-OPAQUE rows in
/// [`crate::generated_v1_34::RESOURCE_CATALOG`].
pub const SERVED: &[ServedDoc] = &[
    ServedDoc {
        group: "",
        version: "v1",
        body: CORE_V1,
        blake3: "3ef14c1747e3d2cb04d45dfd8376e2ff639d48d8a335ce4d8dbe7daa828eafda",
    },
    ServedDoc {
        group: "apps",
        version: "v1",
        body: APPS_V1,
        blake3: "61ed5cd6c4197a656225ff8e69ae011b29a8bcdcc0b1b5394e5f4feea021a5f2",
    },
    ServedDoc {
        group: "rbac.authorization.k8s.io",
        version: "v1",
        body: RBAC_V1,
        blake3: "d4ee15fa2165bfa334aa39722f84f1cc0f80fbe9e4fc7190559e6de94e9f5309",
    },
    // ── M0.0.4 typed-promotion groups (previously served opaque) ──────────
    ServedDoc {
        group: "batch",
        version: "v1",
        body: BATCH_V1,
        blake3: "22cf4a19be81fe87ca2f81db66865ee8b9f22c8ba604d3f12df4d52733b15579",
    },
    ServedDoc {
        group: "networking.k8s.io",
        version: "v1",
        body: NETWORKING_V1,
        blake3: "9ec72a7c7eecbd5c9d979ae4b5fb96231b49cf8e94d5e6bcdc10cf2640723856",
    },
    ServedDoc {
        group: "policy",
        version: "v1",
        body: POLICY_V1,
        blake3: "51b97b1f84d1ace1928016e27ee23b3704aca19939d7476cd507ba7e735ff115",
    },
    ServedDoc {
        group: "storage.k8s.io",
        version: "v1",
        body: STORAGE_V1,
        blake3: "5e696a939eeea0817be84b881bab309fe982ce387074cf364c8cfa520e0ef60a",
    },
    ServedDoc {
        group: "scheduling.k8s.io",
        version: "v1",
        body: SCHEDULING_V1,
        blake3: "8e0fb9ed304a7195c504e6db33ddfb0d2c917a98dcc5704c7723ce7f5476cb92",
    },
    ServedDoc {
        group: "coordination.k8s.io",
        version: "v1",
        body: COORDINATION_V1,
        blake3: "47db299aebd28c76a39b3180def778184f92aa3edad2ad1c261ad971f70f47dc",
    },
    ServedDoc {
        group: "node.k8s.io",
        version: "v1",
        body: NODE_V1,
        blake3: "bcae475e24b86173dccf1c9fbe4212548cac2d24d8c8285608db5fab7628b529",
    },
    ServedDoc {
        group: "autoscaling",
        version: "v2",
        body: AUTOSCALING_V2,
        blake3: "c9985c686485a5d03bf250b32ac48d73d287e56e3493242f5b354565d8e5d396",
    },
];

/// The verbatim OpenAPI v3 document for `(group, version)`, or `None` if no
/// document is served for that pair. The core group is the empty string.
///
/// Driven by [`SERVED`] so the lookup can never advertise a pair the
/// discovery index doesn't list (and vice versa).
#[must_use]
pub fn document_for(group: &str, version: &str) -> Option<&'static str> {
    SERVED
        .iter()
        .find(|d| d.group == group && d.version == version)
        .map(|d| d.body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_for_apps_is_some_and_openapi_3() {
        let doc = document_for("apps", "v1").expect("apps/v1 served");
        let parsed: serde_json::Value =
            serde_json::from_str(doc).expect("apps/v1 doc is valid JSON");
        assert_eq!(
            parsed.get("openapi").and_then(|v| v.as_str()),
            Some("3.0.0"),
            "apps/v1 vendored doc is openapi 3.0.0"
        );
        assert!(
            parsed
                .get("components")
                .and_then(|c| c.get("schemas"))
                .and_then(|s| s.get("io.k8s.api.apps.v1.Deployment"))
                .is_some(),
            "apps/v1 doc carries the Deployment schema"
        );
    }

    #[test]
    fn document_for_core_carries_pod() {
        let doc = document_for("", "v1").expect("core/v1 served");
        let parsed: serde_json::Value =
            serde_json::from_str(doc).expect("core/v1 doc is valid JSON");
        assert!(
            parsed
                .get("components")
                .and_then(|c| c.get("schemas"))
                .and_then(|s| s.get("io.k8s.api.core.v1.Pod"))
                .is_some(),
            "core/v1 doc carries the Pod schema"
        );
    }

    #[test]
    fn document_for_uncataloged_is_none() {
        assert!(document_for("nope.example.com", "v1").is_none());
        assert!(document_for("apps", "v2").is_none());
    }

    #[test]
    fn index_keys_follow_core_vs_grouped_convention() {
        for d in SERVED {
            let key = d.index_key();
            if d.group.is_empty() {
                assert_eq!(key, "api/v1", "core index key is api/v1");
            } else {
                assert_eq!(
                    key,
                    format!("apis/{}/{}", d.group, d.version),
                    "named-group index key is apis/<g>/<v>"
                );
            }
        }
    }

    #[test]
    fn served_blake3_matches_embedded_body() {
        // The digest in SERVED is a cache key; it MUST equal the BLAKE3 of
        // the embedded body, else the manifest and the binary disagree.
        for d in SERVED {
            let actual = blake3::hash(d.body.as_bytes()).to_hex().to_string();
            assert_eq!(
                actual, d.blake3,
                "BLAKE3 drift for served {}/{} — body hashes to {actual}, SERVED says {}",
                d.group, d.version, d.blake3
            );
        }
    }
}

// ── Verb derivation — the contract, not a hand-list ───────────────────────

/// A Kubernetes API verb, as advertised in discovery's `verbs` array.
///
/// Closed by construction: the set of verbs upstream advertises is fixed by
/// the API contract, so a new one is a deliberate widening here rather than a
/// string appearing somewhere in a handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verb {
    /// Read one object by name.
    Get,
    /// Read the collection.
    List,
    /// Stream changes.
    Watch,
    /// Create into the collection.
    Create,
    /// Replace one object (HTTP PUT).
    Update,
    /// Merge into one object (HTTP PATCH).
    Patch,
    /// Remove one object by name.
    Delete,
    /// Remove the whole collection (HTTP DELETE on the collection path).
    DeleteCollection,
}

impl Verb {
    /// The discovery wire string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::List => "list",
            Self::Watch => "watch",
            Self::Create => "create",
            Self::Update => "update",
            Self::Patch => "patch",
            Self::Delete => "delete",
            Self::DeleteCollection => "deletecollection",
        }
    }
}

impl core::fmt::Display for Verb {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Derive the verb set upstream advertises for `plural` in `group`/`version`,
/// **read out of the vendored OpenAPI document** rather than hand-listed.
///
/// This is the L1 half of the conformance differential: the pinned document is
/// an oracle that is always present, needs no running cluster, and is the same
/// bytes upstream publishes. A hand-written verb table can drift from it
/// silently; a derivation cannot.
///
/// The mapping is upstream's own REST convention:
///
/// | OpenAPI path | method | verb |
/// |---|---|---|
/// | `…/{plural}` | `get` | `list` |
/// | `…/{plural}` | `post` | `create` |
/// | `…/{plural}` | `delete` | `deletecollection` |
/// | `…/{plural}/{name}` | `get` | `get` |
/// | `…/{plural}/{name}` | `put` | `update` |
/// | `…/{plural}/{name}` | `patch` | `patch` |
/// | `…/{plural}/{name}` | `delete` | `delete` |
/// | `…/watch/{plural}` | `get` | `watch` |
///
/// Returns `None` when the group/version has no vendored document — the eight
/// opaque groups. `None` is a *statement of ignorance*, never an empty set:
/// an empty set would read as "upstream advertises no verbs", which is the
/// vacuity failure this distinction exists to prevent.
///
/// Namespaced kinds appear under both `/namespaces/{namespace}/{plural}` and
/// a cluster-wide `…/{plural}` list path; both are scanned, and the verbs are
/// unioned, because discovery advertises one verb set per resource.
#[must_use]
pub fn verbs_for(group: &str, version: &str, plural: &str) -> Option<BTreeSet<Verb>> {
    let body = document_for(group, version)?;
    let doc: serde_json::Value = serde_json::from_str(body).ok()?;
    let paths = doc.get("paths")?.as_object()?;

    let mut verbs = BTreeSet::new();
    for (path, item) in paths {
        let Some(methods) = item.as_object() else {
            continue;
        };
        // Trailing segment analysis. `{name}` is upstream's path-parameter
        // spelling for the item path.
        let trimmed = path.trim_end_matches('/');
        let is_watch = trimmed.contains("/watch/");
        let ends_with_plural = trimmed.ends_with(&["/", plural].concat());
        let ends_with_item = trimmed.ends_with(&["/", plural, "/{name}"].concat());

        if !ends_with_plural && !ends_with_item {
            continue;
        }

        for method in methods.keys() {
            let verb = match (method.as_str(), is_watch, ends_with_item) {
                // The deprecated /watch/ paths are how the document spells
                // the watch capability; discovery still advertises `watch`.
                ("get", true, _) => Some(Verb::Watch),
                ("get", false, false) => Some(Verb::List),
                ("post", false, false) => Some(Verb::Create),
                ("delete", false, false) => Some(Verb::DeleteCollection),
                ("get", false, true) => Some(Verb::Get),
                ("put", false, true) => Some(Verb::Update),
                ("patch", false, true) => Some(Verb::Patch),
                ("delete", false, true) => Some(Verb::Delete),
                _ => None,
            };
            if let Some(v) = verb {
                verbs.insert(v);
            }
        }
    }

    // A document that mentions the plural but yields no verb is a parse
    // failure, not a resource with no verbs — report ignorance.
    if verbs.is_empty() { None } else { Some(verbs) }
}

#[cfg(test)]
mod verb_tests {
    use super::*;

    /// A full-CRUD kind derives the complete upstream verb set.
    #[test]
    fn clusterroles_derive_full_crud() {
        let v = verbs_for("rbac.authorization.k8s.io", "v1", "clusterroles")
            .expect("rbac/v1 is vendored");
        for want in [
            Verb::Get,
            Verb::List,
            Verb::Watch,
            Verb::Create,
            Verb::Update,
            Verb::Patch,
            Verb::Delete,
            Verb::DeleteCollection,
        ] {
            assert!(v.contains(&want), "clusterroles must advertise {want}");
        }
    }

    /// Namespaced kinds union their namespaced and cluster-wide list paths.
    #[test]
    fn namespaced_kind_derives_verbs() {
        let v = verbs_for("apps", "v1", "deployments").expect("apps/v1 is vendored");
        assert!(v.contains(&Verb::List) && v.contains(&Verb::Create) && v.contains(&Verb::Watch));
    }

    /// The core group derives too (group is the empty string).
    #[test]
    fn core_group_derives_verbs() {
        let v = verbs_for("", "v1", "configmaps").expect("core/v1 is vendored");
        assert!(v.contains(&Verb::DeleteCollection), "configmaps support deletecollection");
    }

    /// **Ignorance is not an empty set.** An unvendored group returns `None`,
    /// which callers must not read as "advertises nothing".
    #[test]
    fn unvendored_group_is_none_not_empty() {
        assert_eq!(
            verbs_for("authorization.k8s.io", "v1", "selfsubjectaccessreviews"),
            None,
            "the 8 opaque groups have no vendored document — report ignorance"
        );
        assert_eq!(verbs_for("no.such.group", "v1", "widgets"), None);
    }

    /// A plural that does not appear in a vendored doc is also ignorance.
    #[test]
    fn unknown_plural_in_a_vendored_group_is_none() {
        assert_eq!(verbs_for("apps", "v1", "notathing"), None);
    }

    #[test]
    fn verb_wire_strings_are_the_discovery_spellings() {
        assert_eq!(Verb::DeleteCollection.as_str(), "deletecollection");
        assert_eq!(Verb::List.to_string(), "list");
    }
}
