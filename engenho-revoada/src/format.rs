//! # FormatAdapter — operator formats ↔ Native CBOR envelope
//!
//! Operators send manifests in their preferred format: Kubernetes
//! YAML, JSON, Nomad HCL, etc. The fabric's internal store speaks
//! one shape (the [`crate::face::ResourceFormat::Native`] CBOR
//! envelope = `{ ref, payload }`). The [`FormatAdapter`] trait
//! lives at the boundary, doing the round-trip:
//!
//! - **In:** operator bytes (`Yaml` / `Json` / `Hcl`) → typed
//!   [`crate::face::ResourceRef`] + native bytes → store.
//! - **Out:** store bytes → operator's requested format.
//!
//! Faces hold an `Arc<dyn FormatAdapter>` so the same backend
//! store can serve operators in different formats. Each face's
//! `apply_resource` calls `adapter.to_native(format, body)?` then
//! stores the result; `get_resource` reverses the call.
//!
//! # Why this lives one layer above Face
//!
//! Per the prime directive's "third site" rule, format handling
//! was about to be duplicated in five face impls — each parsing
//! YAML/JSON/HCL to extract metadata + wrapping in envelopes. One
//! adapter trait + three impls collapses that to a single
//! load-bearing primitive every face composes with.
//!
//! # The three ship impls
//!
//! - [`K8sYamlAdapter`] — Kubernetes-style YAML. Extracts
//!   `apiVersion`/`kind`/`metadata.name`/`metadata.namespace`.
//! - [`K8sJsonAdapter`] — same shape, JSON instead of YAML.
//! - [`NativePassthroughAdapter`] — no-op; expects the body to
//!   already be a CBOR `NativeEnvelope`. Used by PureRaftFace.
//!
//! New format families (Nomad HCL, Terraform HCL, Cap'n Proto…)
//! land as additional `FormatAdapter` impls. The trait stays
//! stable.

use std::sync::Arc;

use crate::face::{ResourceFormat, ResourceRef};

/// Errors a [`FormatAdapter`] can surface during conversion.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("adapter does not support format {format:?}")]
    UnsupportedFormat { format: ResourceFormat },

    #[error("manifest missing required field: {field}")]
    MissingField { field: &'static str },

    #[error("manifest field {field} has wrong type (expected {expected})")]
    WrongType {
        field: &'static str,
        expected: &'static str,
    },

    #[error("parse error in format {format:?}: {reason}")]
    Parse {
        format: ResourceFormat,
        reason: String,
    },

    #[error("encode error to format {format:?}: {reason}")]
    Encode {
        format: ResourceFormat,
        reason: String,
    },
}

/// The contract every format adapter honors.
///
/// **Object-safe by design** — `Send + Sync + 'static` so faces
/// can hold `Arc<dyn FormatAdapter>` and swap implementations
/// behind a single trait object.
pub trait FormatAdapter: Send + Sync + 'static {
    /// Stable name for telemetry + diagnostics
    /// (`"k8s-yaml"`, `"k8s-json"`, `"native-passthrough"`).
    fn name(&self) -> &'static str;

    /// Which formats this adapter handles.
    fn supported_formats(&self) -> &'static [ResourceFormat];

    /// Extract a typed `ResourceRef` from an operator-authored
    /// manifest. Used by `apply_resource` to determine where to
    /// store the resource.
    ///
    /// # Errors
    ///
    /// - [`AdapterError::UnsupportedFormat`] when `format` isn't
    ///   in [`Self::supported_formats`].
    /// - [`AdapterError::Parse`] on parse failure.
    /// - [`AdapterError::MissingField`] / [`AdapterError::WrongType`]
    ///   when the manifest doesn't carry the metadata the adapter
    ///   expects.
    fn extract_ref(
        &self,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<ResourceRef, AdapterError>;

    /// Convert an operator-authored manifest to the Native CBOR
    /// envelope. The result is what the face's backend stores.
    ///
    /// # Errors
    ///
    /// As [`Self::extract_ref`], plus [`AdapterError::Encode`] on
    /// CBOR encode failure.
    fn to_native(&self, format: ResourceFormat, body: &[u8]) -> Result<Vec<u8>, AdapterError>;

    /// Convert a Native CBOR envelope back to the operator's
    /// requested format. Used by `get_resource` / `list_resources`
    /// / `watch_resources` to render results in the format the
    /// operator asked for.
    ///
    /// # Errors
    ///
    /// As [`Self::extract_ref`].
    fn from_native(
        &self,
        format: ResourceFormat,
        native: &[u8],
    ) -> Result<Vec<u8>, AdapterError>;
}

/// Decode the CBOR `NativeEnvelope` shape used internally. Helper
/// used by adapters that need to round-trip through Native.
fn decode_envelope(native: &[u8]) -> Result<(ResourceRef, Vec<u8>), AdapterError> {
    #[derive(serde::Deserialize)]
    struct Wire {
        #[serde(rename = "ref")]
        reference: ResourceRef,
        payload: Vec<u8>,
    }
    let w: Wire = ciborium::from_reader(native).map_err(|e| AdapterError::Parse {
        format: ResourceFormat::Native,
        reason: format!("envelope decode: {e}"),
    })?;
    Ok((w.reference, w.payload))
}

/// Encode a `NativeEnvelope`. Public helper for callers building
/// envelopes outside an adapter (tests, federation, mock faces).
///
/// # Errors
///
/// Returns the underlying CBOR encode error.
pub fn encode_envelope(reference: &ResourceRef, payload: &[u8]) -> Result<Vec<u8>, AdapterError> {
    #[derive(serde::Serialize)]
    struct Wire<'a> {
        #[serde(rename = "ref")]
        reference: &'a ResourceRef,
        payload: &'a [u8],
    }
    let mut out = Vec::new();
    ciborium::into_writer(
        &Wire {
            reference,
            payload,
        },
        &mut out,
    )
    .map_err(|e| AdapterError::Encode {
        format: ResourceFormat::Native,
        reason: e.to_string(),
    })?;
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────
// NativePassthroughAdapter — the no-op adapter
// ─────────────────────────────────────────────────────────────────

/// Expects the body to already be a CBOR `NativeEnvelope` (the
/// shape PureRaftFace stores). The "adapter" here is a contract
/// shim — the body bytes pass through unchanged on both sides.
pub struct NativePassthroughAdapter;

impl FormatAdapter for NativePassthroughAdapter {
    fn name(&self) -> &'static str {
        "native-passthrough"
    }

    fn supported_formats(&self) -> &'static [ResourceFormat] {
        &[ResourceFormat::Native]
    }

    fn extract_ref(
        &self,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<ResourceRef, AdapterError> {
        if format != ResourceFormat::Native {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let (reference, _payload) = decode_envelope(body)?;
        Ok(reference)
    }

    fn to_native(&self, format: ResourceFormat, body: &[u8]) -> Result<Vec<u8>, AdapterError> {
        if format != ResourceFormat::Native {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        Ok(body.to_vec())
    }

    fn from_native(
        &self,
        format: ResourceFormat,
        native: &[u8],
    ) -> Result<Vec<u8>, AdapterError> {
        if format != ResourceFormat::Native {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        Ok(native.to_vec())
    }
}

// ─────────────────────────────────────────────────────────────────
// K8sJsonAdapter — Kubernetes-style JSON manifests
// ─────────────────────────────────────────────────────────────────

/// Adapter for Kubernetes-style JSON manifests. Extracts
/// `kind` + `metadata.name` + `metadata.namespace`; wraps the
/// JSON body in a `NativeEnvelope`.
pub struct K8sJsonAdapter;

impl FormatAdapter for K8sJsonAdapter {
    fn name(&self) -> &'static str {
        "k8s-json"
    }

    fn supported_formats(&self) -> &'static [ResourceFormat] {
        &[ResourceFormat::Json]
    }

    fn extract_ref(
        &self,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<ResourceRef, AdapterError> {
        if format != ResourceFormat::Json {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let v: serde_json::Value =
            serde_json::from_slice(body).map_err(|e| AdapterError::Parse {
                format,
                reason: e.to_string(),
            })?;
        extract_k8s_ref(&v)
    }

    fn to_native(&self, format: ResourceFormat, body: &[u8]) -> Result<Vec<u8>, AdapterError> {
        let reference = self.extract_ref(format, body)?;
        encode_envelope(&reference, body)
    }

    fn from_native(
        &self,
        format: ResourceFormat,
        native: &[u8],
    ) -> Result<Vec<u8>, AdapterError> {
        if format != ResourceFormat::Json {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let (_, payload) = decode_envelope(native)?;
        Ok(payload)
    }
}

// ─────────────────────────────────────────────────────────────────
// HclAdapter — Nomad-style HCL job manifests
// ─────────────────────────────────────────────────────────────────

/// Adapter for HashiCorp HCL job manifests (Nomad's authoring
/// format). Parses the top-level `job "<name>" {}` block to extract
/// the resource reference; stores the HCL body verbatim in a
/// `NativeEnvelope`.
///
/// **What this proves about the trait abstraction:** YAML + JSON
/// are sibling formats (both serde-driven, both K8s-style). HCL is
/// a genuinely different family (HashiCorp's, block-oriented). The
/// fact that it fits the FormatAdapter trait with the same shape
/// is the structural proof that the trait abstracts over arbitrary
/// authoring formats, not just YAML-shaped ones.
///
/// **Resource shape:** Nomad jobs are typed as `kind = "Job"`,
/// `name = <block label>`, `namespace = <job namespace stanza, or
/// "default" if absent>`. This matches the `nomad job run` mental
/// model.
pub struct HclAdapter;

impl FormatAdapter for HclAdapter {
    fn name(&self) -> &'static str {
        "nomad-hcl"
    }

    fn supported_formats(&self) -> &'static [ResourceFormat] {
        &[ResourceFormat::Hcl]
    }

    fn extract_ref(
        &self,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<ResourceRef, AdapterError> {
        if format != ResourceFormat::Hcl {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let source = std::str::from_utf8(body).map_err(|e| AdapterError::Parse {
            format,
            reason: format!("hcl body is not utf-8: {e}"),
        })?;
        let body_value: hcl::Body =
            hcl::from_str(source).map_err(|e| AdapterError::Parse {
                format,
                reason: e.to_string(),
            })?;
        extract_nomad_job_ref(&body_value)
    }

    fn to_native(&self, format: ResourceFormat, body: &[u8]) -> Result<Vec<u8>, AdapterError> {
        let reference = self.extract_ref(format, body)?;
        encode_envelope(&reference, body)
    }

    fn from_native(
        &self,
        format: ResourceFormat,
        native: &[u8],
    ) -> Result<Vec<u8>, AdapterError> {
        if format != ResourceFormat::Hcl {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let (_, payload) = decode_envelope(native)?;
        Ok(payload)
    }
}

/// Walk an `hcl::Body` looking for `job "<name>" {}` with an
/// optional `namespace = "<ns>"` attribute; emit the typed
/// `ResourceRef`.
fn extract_nomad_job_ref(body: &hcl::Body) -> Result<ResourceRef, AdapterError> {
    use hcl::Structure;
    for structure in body.iter() {
        let Structure::Block(block) = structure else {
            continue;
        };
        if block.identifier.as_str() != "job" {
            continue;
        }
        let name = block
            .labels
            .first()
            .ok_or(AdapterError::MissingField {
                field: "job.<name>",
            })?
            .as_str()
            .to_string();
        // Look for an inner `namespace = "<ns>"` attribute.
        let namespace = block
            .body
            .iter()
            .find_map(|s| match s {
                Structure::Attribute(attr) if attr.key.as_str() == "namespace" => {
                    if let hcl::Expression::String(ns) = &attr.expr {
                        Some(ns.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            });
        return Ok(ResourceRef {
            kind: "Job".to_string(),
            name,
            namespace,
        });
    }
    Err(AdapterError::MissingField {
        field: "job block",
    })
}

// ─────────────────────────────────────────────────────────────────
// K8sYamlAdapter — Kubernetes-style YAML manifests
// ─────────────────────────────────────────────────────────────────

/// Adapter for Kubernetes-style YAML manifests. Extracts the
/// same fields as [`K8sJsonAdapter`]; stores the YAML body
/// verbatim in a `NativeEnvelope`.
pub struct K8sYamlAdapter;

impl FormatAdapter for K8sYamlAdapter {
    fn name(&self) -> &'static str {
        "k8s-yaml"
    }

    fn supported_formats(&self) -> &'static [ResourceFormat] {
        &[ResourceFormat::Yaml]
    }

    fn extract_ref(
        &self,
        format: ResourceFormat,
        body: &[u8],
    ) -> Result<ResourceRef, AdapterError> {
        if format != ResourceFormat::Yaml {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        // serde_yaml → serde_json::Value via the YAML's own Value;
        // we reuse the JSON walker so K8s YAML and K8s JSON share
        // the same metadata-extraction logic.
        let yaml_value: serde_yaml::Value =
            serde_yaml::from_slice(body).map_err(|e| AdapterError::Parse {
                format,
                reason: e.to_string(),
            })?;
        let json_value: serde_json::Value =
            serde_json::to_value(yaml_value).map_err(|e| AdapterError::Parse {
                format,
                reason: format!("yaml → json bridge: {e}"),
            })?;
        extract_k8s_ref(&json_value)
    }

    fn to_native(&self, format: ResourceFormat, body: &[u8]) -> Result<Vec<u8>, AdapterError> {
        let reference = self.extract_ref(format, body)?;
        encode_envelope(&reference, body)
    }

    fn from_native(
        &self,
        format: ResourceFormat,
        native: &[u8],
    ) -> Result<Vec<u8>, AdapterError> {
        if format != ResourceFormat::Yaml {
            return Err(AdapterError::UnsupportedFormat { format });
        }
        let (_, payload) = decode_envelope(native)?;
        Ok(payload)
    }
}

// ─────────────────────────────────────────────────────────────────
// AdapterRegistry — face-side adapter dispatch
// ─────────────────────────────────────────────────────────────────

/// Maps each [`ResourceFormat`] an operator can request to the
/// adapter that handles it. Faces hold one of these and route
/// verb calls through it; the format the operator asks for
/// selects the adapter, the adapter does the byte ↔ Native
/// conversion.
///
/// Built via [`AdapterRegistry::default`] (ships the three
/// standard adapters: `NativePassthroughAdapter` for `Native`,
/// `K8sJsonAdapter` for `Json`, `K8sYamlAdapter` for `Yaml`).
/// Operators with custom format families register additional
/// adapters via [`AdapterRegistry::register`].
pub struct AdapterRegistry {
    by_format: std::collections::HashMap<ResourceFormat, Arc<dyn FormatAdapter>>,
}

impl AdapterRegistry {
    /// Empty registry — every `select` returns
    /// `AdapterError::UnsupportedFormat`. Useful for tests that
    /// want to assert "no adapter is reachable for X."
    #[must_use]
    pub fn empty() -> Self {
        Self {
            by_format: std::collections::HashMap::new(),
        }
    }

    /// Register an adapter for every format it claims to support.
    /// If multiple adapters claim the same format, the last
    /// registered wins (last-writer-wins is the standard registry
    /// semantic; operators who care about ordering register
    /// explicitly in priority order).
    pub fn register(&mut self, adapter: Arc<dyn FormatAdapter>) -> &mut Self {
        for format in adapter.supported_formats() {
            self.by_format.insert(*format, Arc::clone(&adapter));
        }
        self
    }

    /// Resolve the adapter for `format`.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError::UnsupportedFormat`] when no adapter
    /// in the registry claims this format.
    pub fn select(
        &self,
        format: ResourceFormat,
    ) -> Result<Arc<dyn FormatAdapter>, AdapterError> {
        self.by_format
            .get(&format)
            .cloned()
            .ok_or(AdapterError::UnsupportedFormat { format })
    }

    /// True iff the registry has an adapter for `format`.
    #[must_use]
    pub fn handles(&self, format: ResourceFormat) -> bool {
        self.by_format.contains_key(&format)
    }
}

impl Default for AdapterRegistry {
    /// Ships the three standard adapters:
    /// - `Native` → `NativePassthroughAdapter`
    /// - `Json` → `K8sJsonAdapter`
    /// - `Yaml` → `K8sYamlAdapter`
    ///
    /// Faces that want only Native (e.g. an early prototype face)
    /// build a custom registry; faces that want HCL register an
    /// additional adapter later.
    fn default() -> Self {
        let mut r = Self::empty();
        r.register(Arc::new(NativePassthroughAdapter));
        r.register(Arc::new(K8sJsonAdapter));
        r.register(Arc::new(K8sYamlAdapter));
        r.register(Arc::new(HclAdapter));
        r
    }
}

impl std::fmt::Debug for AdapterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<(ResourceFormat, &str)> = self
            .by_format
            .iter()
            .map(|(k, v)| (*k, v.name()))
            .collect();
        names.sort_by_key(|(_, n)| *n);
        f.debug_struct("AdapterRegistry")
            .field("formats", &names)
            .finish()
    }
}

/// Shared extraction: walk a `serde_json::Value` looking like a
/// Kubernetes manifest; pull `kind` + `metadata.name` +
/// `metadata.namespace`.
fn extract_k8s_ref(v: &serde_json::Value) -> Result<ResourceRef, AdapterError> {
    let obj = v.as_object().ok_or(AdapterError::WrongType {
        field: "<root>",
        expected: "object",
    })?;
    let kind = obj
        .get("kind")
        .ok_or(AdapterError::MissingField { field: "kind" })?
        .as_str()
        .ok_or(AdapterError::WrongType {
            field: "kind",
            expected: "string",
        })?
        .to_string();
    let metadata = obj
        .get("metadata")
        .ok_or(AdapterError::MissingField { field: "metadata" })?
        .as_object()
        .ok_or(AdapterError::WrongType {
            field: "metadata",
            expected: "object",
        })?;
    let name = metadata
        .get("name")
        .ok_or(AdapterError::MissingField {
            field: "metadata.name",
        })?
        .as_str()
        .ok_or(AdapterError::WrongType {
            field: "metadata.name",
            expected: "string",
        })?
        .to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|v| v.as_str().map(str::to_string));
    Ok(ResourceRef {
        kind,
        name,
        namespace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ───────────────────────────────────────────────────

    fn pod_yaml(name: &str, ns: Option<&str>) -> Vec<u8> {
        let mut s = format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n"
        );
        if let Some(n) = ns {
            s.push_str(&format!("  namespace: {n}\n"));
        }
        s.push_str("spec:\n  containers:\n    - name: c\n      image: nginx\n");
        s.into_bytes()
    }

    fn pod_json(name: &str, ns: Option<&str>) -> Vec<u8> {
        let body = if let Some(n) = ns {
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": name, "namespace": n },
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            })
        } else {
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": name },
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            })
        };
        serde_json::to_vec(&body).unwrap()
    }

    fn cluster_role_json(name: &str) -> Vec<u8> {
        // No namespace — cluster-scoped resource.
        let body = serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": { "name": name },
            "rules": []
        });
        serde_json::to_vec(&body).unwrap()
    }

    // ── NativePassthroughAdapter ──────────────────────────────────

    #[test]
    fn native_passthrough_round_trips_envelope() {
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let env = encode_envelope(&r, b"payload").unwrap();
        let adapter = NativePassthroughAdapter;
        let extracted = adapter
            .extract_ref(ResourceFormat::Native, &env)
            .unwrap();
        assert_eq!(extracted, r);
        let nat = adapter.to_native(ResourceFormat::Native, &env).unwrap();
        assert_eq!(nat, env);
        let back = adapter.from_native(ResourceFormat::Native, &env).unwrap();
        assert_eq!(back, env);
    }

    #[test]
    fn native_passthrough_rejects_non_native_formats() {
        let adapter = NativePassthroughAdapter;
        assert!(matches!(
            adapter.extract_ref(ResourceFormat::Yaml, b""),
            Err(AdapterError::UnsupportedFormat { .. })
        ));
        assert!(matches!(
            adapter.to_native(ResourceFormat::Json, b""),
            Err(AdapterError::UnsupportedFormat { .. })
        ));
        assert!(matches!(
            adapter.from_native(ResourceFormat::Hcl, b""),
            Err(AdapterError::UnsupportedFormat { .. })
        ));
    }

    // ── K8sJsonAdapter ────────────────────────────────────────────

    #[test]
    fn k8s_json_extracts_ref_from_namespaced_pod() {
        let adapter = K8sJsonAdapter;
        let body = pod_json("nginx", Some("default"));
        let r = adapter.extract_ref(ResourceFormat::Json, &body).unwrap();
        assert_eq!(r.kind, "Pod");
        assert_eq!(r.name, "nginx");
        assert_eq!(r.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn k8s_json_extracts_ref_from_cluster_scoped() {
        let adapter = K8sJsonAdapter;
        let body = cluster_role_json("admin");
        let r = adapter.extract_ref(ResourceFormat::Json, &body).unwrap();
        assert_eq!(r.kind, "ClusterRole");
        assert_eq!(r.name, "admin");
        assert!(r.namespace.is_none());
    }

    #[test]
    fn k8s_json_to_native_round_trips_via_from_native() {
        let adapter = K8sJsonAdapter;
        let body = pod_json("nginx", Some("default"));
        let env = adapter.to_native(ResourceFormat::Json, &body).unwrap();
        let back = adapter.from_native(ResourceFormat::Json, &env).unwrap();
        // The payload returned is the operator's original JSON
        // body verbatim.
        assert_eq!(back, body);
    }

    #[test]
    fn k8s_json_missing_kind_errors_with_named_field() {
        let adapter = K8sJsonAdapter;
        let body = serde_json::to_vec(&serde_json::json!({
            "metadata": { "name": "x" }
        }))
        .unwrap();
        match adapter.extract_ref(ResourceFormat::Json, &body) {
            Err(AdapterError::MissingField { field }) => assert_eq!(field, "kind"),
            other => panic!("expected MissingField(kind), got {other:?}"),
        }
    }

    #[test]
    fn k8s_json_missing_metadata_name_errors_with_named_field() {
        let adapter = K8sJsonAdapter;
        let body = serde_json::to_vec(&serde_json::json!({
            "kind": "Pod",
            "metadata": {}
        }))
        .unwrap();
        match adapter.extract_ref(ResourceFormat::Json, &body) {
            Err(AdapterError::MissingField { field }) => {
                assert_eq!(field, "metadata.name");
            }
            other => panic!("expected MissingField(metadata.name), got {other:?}"),
        }
    }

    #[test]
    fn k8s_json_rejects_non_json_formats() {
        let adapter = K8sJsonAdapter;
        let body = pod_json("nginx", None);
        assert!(matches!(
            adapter.extract_ref(ResourceFormat::Yaml, &body),
            Err(AdapterError::UnsupportedFormat { .. })
        ));
    }

    #[test]
    fn k8s_json_parse_error_surfaces_clearly() {
        let adapter = K8sJsonAdapter;
        let body = b"not json at all";
        match adapter.extract_ref(ResourceFormat::Json, body) {
            Err(AdapterError::Parse { format, .. }) => {
                assert_eq!(format, ResourceFormat::Json);
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    // ── K8sYamlAdapter ────────────────────────────────────────────

    #[test]
    fn k8s_yaml_extracts_ref_from_namespaced_pod() {
        let adapter = K8sYamlAdapter;
        let body = pod_yaml("nginx", Some("default"));
        let r = adapter.extract_ref(ResourceFormat::Yaml, &body).unwrap();
        assert_eq!(r.kind, "Pod");
        assert_eq!(r.name, "nginx");
        assert_eq!(r.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn k8s_yaml_extracts_ref_from_cluster_scoped() {
        let adapter = K8sYamlAdapter;
        let body = b"apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: admin\nrules: []\n";
        let r = adapter.extract_ref(ResourceFormat::Yaml, body).unwrap();
        assert_eq!(r.kind, "ClusterRole");
        assert_eq!(r.name, "admin");
        assert!(r.namespace.is_none());
    }

    #[test]
    fn k8s_yaml_to_native_then_from_native_preserves_body() {
        let adapter = K8sYamlAdapter;
        let body = pod_yaml("nginx", Some("default"));
        let env = adapter.to_native(ResourceFormat::Yaml, &body).unwrap();
        let back = adapter.from_native(ResourceFormat::Yaml, &env).unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn k8s_yaml_missing_kind_errors() {
        let adapter = K8sYamlAdapter;
        let body = b"metadata:\n  name: x\n";
        assert!(matches!(
            adapter.extract_ref(ResourceFormat::Yaml, body),
            Err(AdapterError::MissingField { field: "kind" })
        ));
    }

    #[test]
    fn k8s_yaml_rejects_non_yaml_formats() {
        let adapter = K8sYamlAdapter;
        assert!(matches!(
            adapter.extract_ref(ResourceFormat::Json, b"{}"),
            Err(AdapterError::UnsupportedFormat { .. })
        ));
    }

    #[test]
    fn k8s_yaml_parse_error_surfaces_clearly() {
        let adapter = K8sYamlAdapter;
        // Invalid YAML — unclosed quote.
        let body = b"key: \"unclosed\n";
        match adapter.extract_ref(ResourceFormat::Yaml, body) {
            Err(AdapterError::Parse { format, .. }) => {
                assert_eq!(format, ResourceFormat::Yaml);
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    // ── Object-safety / dyn-compat ────────────────────────────────

    #[test]
    fn format_adapter_is_send_sync_static_dyn_compat() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<Box<dyn FormatAdapter>>();
        // Heterogeneous Vec compiles → trait is object-safe.
        let adapters: Vec<Box<dyn FormatAdapter>> = vec![
            Box::new(NativePassthroughAdapter),
            Box::new(K8sJsonAdapter),
            Box::new(K8sYamlAdapter),
        ];
        let names: Vec<&str> = adapters.iter().map(|a| a.name()).collect();
        assert_eq!(names, vec!["native-passthrough", "k8s-json", "k8s-yaml"]);
    }

    #[test]
    fn each_adapter_declares_its_supported_formats() {
        let n = NativePassthroughAdapter;
        let j = K8sJsonAdapter;
        let y = K8sYamlAdapter;
        assert_eq!(n.supported_formats(), &[ResourceFormat::Native]);
        assert_eq!(j.supported_formats(), &[ResourceFormat::Json]);
        assert_eq!(y.supported_formats(), &[ResourceFormat::Yaml]);
    }

    // ── encode_envelope public helper ─────────────────────────────

    #[test]
    fn encode_envelope_round_trips_through_decode() {
        let r = ResourceRef::namespaced("Pod", "nginx", "default");
        let env = encode_envelope(&r, b"payload").unwrap();
        let (decoded_ref, decoded_payload) = decode_envelope(&env).unwrap();
        assert_eq!(decoded_ref, r);
        assert_eq!(decoded_payload, b"payload");
    }

    // ── Cross-format consistency ─────────────────────────────────

    #[test]
    fn yaml_and_json_extract_the_same_ref_for_equivalent_manifests() {
        let json_body = pod_json("nginx", Some("default"));
        let yaml_body = pod_yaml("nginx", Some("default"));
        let json_ref = K8sJsonAdapter
            .extract_ref(ResourceFormat::Json, &json_body)
            .unwrap();
        let yaml_ref = K8sYamlAdapter
            .extract_ref(ResourceFormat::Yaml, &yaml_body)
            .unwrap();
        // Same logical resource → identical typed ref regardless
        // of which format the operator authored in.
        assert_eq!(json_ref, yaml_ref);
    }

    // ── HclAdapter ────────────────────────────────────────────────

    fn nomad_job_hcl(name: &str, ns: Option<&str>) -> Vec<u8> {
        let mut s = format!("job \"{name}\" {{\n");
        if let Some(n) = ns {
            s.push_str(&format!("  namespace = \"{n}\"\n"));
        }
        s.push_str("  group \"web\" {\n    count = 3\n  }\n}\n");
        s.into_bytes()
    }

    #[test]
    fn hcl_adapter_extracts_ref_from_namespaced_job() {
        let body = nomad_job_hcl("web", Some("team-a"));
        let r = HclAdapter
            .extract_ref(ResourceFormat::Hcl, &body)
            .unwrap();
        assert_eq!(r.kind, "Job");
        assert_eq!(r.name, "web");
        assert_eq!(r.namespace.as_deref(), Some("team-a"));
    }

    #[test]
    fn hcl_adapter_extracts_ref_from_no_namespace_job() {
        let body = nomad_job_hcl("web", None);
        let r = HclAdapter
            .extract_ref(ResourceFormat::Hcl, &body)
            .unwrap();
        assert_eq!(r.kind, "Job");
        assert_eq!(r.name, "web");
        assert!(r.namespace.is_none());
    }

    #[test]
    fn hcl_adapter_round_trips_body_through_native() {
        let body = nomad_job_hcl("web", Some("default"));
        let env = HclAdapter.to_native(ResourceFormat::Hcl, &body).unwrap();
        let back = HclAdapter
            .from_native(ResourceFormat::Hcl, &env)
            .unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn hcl_adapter_missing_job_block_errors() {
        let body = b"variable \"x\" {\n  default = 1\n}\n";
        match HclAdapter.extract_ref(ResourceFormat::Hcl, body) {
            Err(AdapterError::MissingField { field: "job block" }) => {}
            other => panic!("expected MissingField(\"job block\"), got {other:?}"),
        }
    }

    #[test]
    fn hcl_adapter_rejects_non_hcl_formats() {
        let body = nomad_job_hcl("web", None);
        match HclAdapter.extract_ref(ResourceFormat::Yaml, &body) {
            Err(AdapterError::UnsupportedFormat { .. }) => {}
            other => panic!("expected UnsupportedFormat, got {other:?}"),
        }
    }

    #[test]
    fn hcl_adapter_parse_error_surfaces_clearly() {
        let body = b"job \"unclosed {\n";
        match HclAdapter.extract_ref(ResourceFormat::Hcl, body) {
            Err(AdapterError::Parse { format, .. }) => {
                assert_eq!(format, ResourceFormat::Hcl);
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn default_registry_now_handles_hcl() {
        let reg = AdapterRegistry::default();
        assert!(reg.handles(ResourceFormat::Hcl));
        let adapter = reg.select(ResourceFormat::Hcl).unwrap();
        assert_eq!(adapter.name(), "nomad-hcl");
    }
}
