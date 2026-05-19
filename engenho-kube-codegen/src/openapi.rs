//! OpenAPI v3 parser — sufficient surface to extract a "kind shape"
//! per [`crate::catalog::KindEntry`].
//!
//! Intentionally minimal — full $ref-walking lives in M0.0.4. For
//! M0.0.1 we just need to confirm each cataloged kind EXISTS in the
//! vendored schema + has the expected `x-kubernetes-group-version-kind`
//! metadata. That's the "this catalog matches the schema" check that
//! gates `engenho-kube-codegen --check`.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Top-level OpenAPI v3 document. We only deserialize the components
/// section we use — the rest is left as raw JSON.
#[derive(Debug, Deserialize)]
pub struct OpenApiDoc {
    /// Sub-section with the resource type definitions.
    pub components: Components,
}

/// `components.schemas` from the OpenAPI doc.
#[derive(Debug, Deserialize)]
pub struct Components {
    /// Definition map: key is the full openapi-key
    /// (e.g. `io.k8s.api.core.v1.Pod`), value is the schema body.
    pub schemas: BTreeMap<String, KindShape>,
}

/// Per-kind schema body. We only extract what we need.
#[derive(Debug, Deserialize, Clone)]
pub struct KindShape {
    /// `x-kubernetes-group-version-kind` array. Most kinds have ONE
    /// entry; alpha kinds in transition can have multiple.
    #[serde(default, rename = "x-kubernetes-group-version-kind")]
    pub gvks: Vec<XGvk>,

    /// Human-readable description (drops into the generated rustdoc).
    #[serde(default)]
    pub description: String,

    /// Top-level properties. Used at M0.0.4 to emit typed fields.
    #[serde(default)]
    pub properties: BTreeMap<String, serde_json::Value>,

    /// Required properties. Used at M0.0.4 for `#[serde(skip_serializing_if)]`.
    #[serde(default)]
    pub required: Vec<String>,
}

/// `x-kubernetes-group-version-kind` entry.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct XGvk {
    /// API group (`""` for core/v1).
    pub group:   String,
    /// API version.
    pub version: String,
    /// Kind name.
    pub kind:    String,
}

impl OpenApiDoc {
    /// Load + parse an OpenAPI v3 JSON file.
    ///
    /// # Errors
    ///
    /// Forwards parse errors with context including the file path.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", path.display()))
    }

    /// Find the `KindShape` for `openapi_key`, or error.
    ///
    /// # Errors
    ///
    /// Returns an error mentioning the key when absent.
    pub fn shape(&self, openapi_key: &str) -> Result<&KindShape> {
        self.components.schemas.get(openapi_key)
            .ok_or_else(|| anyhow!("kind {openapi_key:?} not in OpenAPI components.schemas"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_openapi_doc() {
        let json = serde_json::json!({
            "components": {
                "schemas": {
                    "io.k8s.api.core.v1.Pod": {
                        "x-kubernetes-group-version-kind": [
                            {"group": "", "version": "v1", "kind": "Pod"}
                        ],
                        "description": "Pod is a collection of containers",
                        "properties": {"metadata": {}, "spec": {}, "status": {}},
                        "required": ["spec"]
                    }
                }
            }
        });
        let doc: OpenApiDoc = serde_json::from_value(json).unwrap();
        let pod = doc.shape("io.k8s.api.core.v1.Pod").unwrap();
        assert_eq!(pod.gvks.len(), 1);
        assert_eq!(pod.gvks[0], XGvk {
            group: "".into(), version: "v1".into(), kind: "Pod".into(),
        });
    }

    #[test]
    fn missing_shape_errors() {
        let doc = OpenApiDoc {
            components: Components { schemas: BTreeMap::new() },
        };
        let err = doc.shape("io.k8s.api.core.v1.Pod").unwrap_err();
        assert!(err.to_string().contains("not in OpenAPI"));
    }
}
