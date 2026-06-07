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
//! `files` list, scoped to the three cataloged groups. Per the ★★ CATALOG
//! REFLECTION invariant the set of `(group, version)` pairs here MUST equal
//! the distinct pairs in [`crate::generated_v1_34::RESOURCE_CATALOG`] — a
//! test (`tests/generated_catalog_invariants.rs`) asserts it, so adding a
//! group is one catalog change + one vendored file + one [`SERVED`] row,
//! never a hand-wired route.

/// The vendored core (`""`/v1) OpenAPI v3 document.
pub const CORE_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/api__v1_openapi.json");

/// The vendored apps/v1 OpenAPI v3 document.
pub const APPS_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__apps__v1_openapi.json");

/// The vendored rbac.authorization.k8s.io/v1 OpenAPI v3 document.
pub const RBAC_V1: &str =
    include_str!("../vendor/openapi/v1.34.0/apis__rbac.authorization.k8s.io__v1_openapi.json");

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
/// index + [`document_for`] both iterate. Scoped to exactly the three
/// cataloged groups. BLAKE3 digests mirror `MANIFEST.yaml` (and are
/// re-verified against `body` by a test, so a manifest/body drift fails CI).
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
