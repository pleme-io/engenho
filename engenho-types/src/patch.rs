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
