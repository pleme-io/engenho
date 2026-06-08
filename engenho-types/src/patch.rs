//! Typed patch operations. Maps to the apiserver's PATCH endpoint with
//! the correct `Content-Type` per variant.

use serde::{Deserialize, Serialize};

/// A patch operation against an existing resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Patch {
    /// JSON Merge Patch (RFC 7396). Content-Type:
    /// `application/merge-patch+json`. The simplest variant — replace
    /// fields named in the patch, leave others alone.
    Merge(serde_json::Value),

    /// Strategic Merge Patch — Kubernetes-specific, knows how to merge
    /// arrays of objects (e.g. `containers`) by key. Content-Type:
    /// `application/strategic-merge-patch+json`. Default for `kubectl
    /// patch` without `--type`.
    Strategic(serde_json::Value),

    /// JSON Patch (RFC 6902) — list of add/remove/replace ops.
    /// Content-Type: `application/json-patch+json`.
    Json(Vec<JsonPatchOp>),

    /// Server-Side Apply — declarative reconciliation with apiserver-
    /// side field-ownership tracking. Content-Type:
    /// `application/apply-patch+yaml`. Carries the field-manager id
    /// the caller speaks for.
    Apply {
        /// Identity of the caller doing the apply (e.g. `engenho-scheduler`).
        field_manager: String,
        /// Whether to take ownership of conflicting fields (rare).
        force: bool,
        /// The (possibly partial) resource state we're declaring.
        body: serde_json::Value,
    },
}

/// One operation in an RFC 6902 JSON Patch document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "op")]
pub enum JsonPatchOp {
    /// `{"op": "add", "path": "/spec/replicas", "value": 3}`
    Add {
        path: String,
        value: serde_json::Value,
    },
    /// `{"op": "remove", "path": "/spec/replicas"}`
    Remove { path: String },
    /// `{"op": "replace", "path": "/spec/replicas", "value": 5}`
    Replace {
        path: String,
        value: serde_json::Value,
    },
    /// `{"op": "copy", "from": "/a", "path": "/b"}`
    Copy { from: String, path: String },
    /// `{"op": "move", "from": "/a", "path": "/b"}`
    Move { from: String, path: String },
    /// `{"op": "test", "path": "/a", "value": 1}`
    Test {
        path: String,
        value: serde_json::Value,
    },
}

/// The type-only discriminant of [`Patch`] — which of the four patch
/// algorithms the client requested. Carried through the apiserver →
/// store path (alongside the decoded patch `Value`) so the store's
/// interpreter dispatches into the correct algorithm instead of
/// unconditionally RFC7386-merging. Without this, the Content-Type the
/// client sent is erased at the apiserver boundary and every patch is
/// (wrongly) executed as a merge patch.
///
/// The round-trip [`PatchType::from_content_type`] ∘ [`Patch::content_type`]
/// is the identity on the four variants (property-tested) — the two
/// functions are inverses by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchType {
    /// JSON Merge Patch (RFC 7396) — `application/merge-patch+json`.
    Merge,
    /// Strategic Merge Patch — Kubernetes list-merge-by-key semantics.
    /// `application/strategic-merge-patch+json`. The default for
    /// `kubectl patch`/`kubectl edit`/client-side `kubectl apply`.
    Strategic,
    /// JSON Patch (RFC 6902) — `application/json-patch+json`.
    Json,
    /// Server-Side Apply — `application/apply-patch+yaml`. Typed-deferred:
    /// the interpreter returns a typed error (mapped to 415/422) rather
    /// than silently falling back to a strategic/merge patch (which would
    /// lie about field-ownership / conflict semantics).
    Apply,
}

impl PatchType {
    /// The inverse of [`Patch::content_type`] — resolve a request's
    /// `Content-Type` media type into the typed patch algorithm.
    ///
    /// Accepts the bare media type (already stripped of `; charset=…`
    /// parameters). Returns `None` for a media type that is not one of the
    /// four K8s patch content-types; the caller maps that to a typed 415.
    ///
    /// An EMPTY media type defaults to [`PatchType::Strategic`] — matching
    /// kube-apiserver: `kubectl patch` without `--type` (and the default
    /// patch path) sends `strategic-merge-patch+json`, and a missing
    /// Content-Type is treated as the strategic default.
    #[must_use]
    pub fn from_content_type(ct: &str) -> Option<Self> {
        // Strip any `; charset=…` parameters + surrounding whitespace.
        let media = ct.split(';').next().unwrap_or("").trim();
        if media.is_empty() {
            // Default patch type when no Content-Type is sent.
            return Some(Self::Strategic);
        }
        match media {
            "application/merge-patch+json" => Some(Self::Merge),
            "application/strategic-merge-patch+json" => Some(Self::Strategic),
            "application/json-patch+json" => Some(Self::Json),
            "application/apply-patch+yaml" | "application/apply-patch+json" => Some(Self::Apply),
            _ => None,
        }
    }
}

impl Patch {
    /// The HTTP `Content-Type` header value the apiserver expects for
    /// this patch variant.
    #[must_use]
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Merge(_) => "application/merge-patch+json",
            Self::Strategic(_) => "application/strategic-merge-patch+json",
            Self::Json(_) => "application/json-patch+json",
            Self::Apply { .. } => "application/apply-patch+yaml",
        }
    }

    /// The type-only discriminant of this patch — the inverse target of
    /// [`PatchType::from_content_type`].
    #[must_use]
    pub fn patch_type(&self) -> PatchType {
        match self {
            Self::Merge(_) => PatchType::Merge,
            Self::Strategic(_) => PatchType::Strategic,
            Self::Json(_) => PatchType::Json,
            Self::Apply { .. } => PatchType::Apply,
        }
    }

    /// Serialize the patch body to a `Vec<u8>` ready for the request.
    ///
    /// # Errors
    ///
    /// Returns the underlying serde_json error if serialization fails
    /// (effectively unreachable — all our variants are owned-data).
    pub fn body_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Merge(v) | Self::Strategic(v) => serde_json::to_vec(v),
            Self::Json(ops) => serde_json::to_vec(ops),
            Self::Apply { body, .. } => serde_json::to_vec(body),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_content_type() {
        let p = Patch::Merge(json!({"spec": {"replicas": 3}}));
        assert_eq!(p.content_type(), "application/merge-patch+json");
    }

    #[test]
    fn strategic_content_type() {
        let p = Patch::Strategic(json!({}));
        assert_eq!(p.content_type(), "application/strategic-merge-patch+json");
    }

    #[test]
    fn json_patch_content_type() {
        let p = Patch::Json(vec![JsonPatchOp::Add {
            path: "/spec/replicas".into(),
            value: json!(3),
        }]);
        assert_eq!(p.content_type(), "application/json-patch+json");
    }

    #[test]
    fn apply_content_type_and_field_manager() {
        let p = Patch::Apply {
            field_manager: "engenho-scheduler".into(),
            force: false,
            body: json!({"apiVersion": "v1", "kind": "Pod"}),
        };
        assert_eq!(p.content_type(), "application/apply-patch+yaml");
    }

    #[test]
    fn patch_type_round_trips_through_content_type() {
        // PatchType::from_content_type(p.content_type()) == p.patch_type()
        // for every variant — the two functions are inverses.
        let variants = [
            Patch::Merge(json!({})),
            Patch::Strategic(json!({})),
            Patch::Json(vec![]),
            Patch::Apply {
                field_manager: "fm".into(),
                force: false,
                body: json!({}),
            },
        ];
        for p in variants {
            assert_eq!(
                PatchType::from_content_type(p.content_type()),
                Some(p.patch_type()),
                "round-trip failed for {:?}",
                p.patch_type()
            );
        }
    }

    #[test]
    fn empty_content_type_defaults_to_strategic() {
        // kubectl patch without --type / a missing Content-Type → strategic.
        assert_eq!(PatchType::from_content_type(""), Some(PatchType::Strategic));
        assert_eq!(
            PatchType::from_content_type("application/strategic-merge-patch+json; charset=utf-8"),
            Some(PatchType::Strategic)
        );
    }

    #[test]
    fn unknown_content_type_is_none() {
        assert_eq!(PatchType::from_content_type("text/plain"), None);
        assert_eq!(PatchType::from_content_type("application/xml"), None);
    }

    #[test]
    fn apply_patch_yaml_and_json_both_map_to_apply() {
        assert_eq!(
            PatchType::from_content_type("application/apply-patch+yaml"),
            Some(PatchType::Apply)
        );
        assert_eq!(
            PatchType::from_content_type("application/apply-patch+json"),
            Some(PatchType::Apply)
        );
    }

    #[test]
    fn json_patch_body_roundtrips() {
        let p = Patch::Json(vec![
            JsonPatchOp::Add {
                path: "/a".into(),
                value: json!(1),
            },
            JsonPatchOp::Remove { path: "/b".into() },
            JsonPatchOp::Replace {
                path: "/c".into(),
                value: json!("x"),
            },
            JsonPatchOp::Copy {
                from: "/d".into(),
                path: "/e".into(),
            },
            JsonPatchOp::Move {
                from: "/f".into(),
                path: "/g".into(),
            },
            JsonPatchOp::Test {
                path: "/h".into(),
                value: json!(true),
            },
        ]);
        let bytes = p.body_bytes().unwrap();
        // Round-trip
        let back: Vec<JsonPatchOp> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.len(), 6);
    }
}
