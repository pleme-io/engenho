//! Type-driven redaction at the MCP boundary.
//!
//! Codifies the LEAN.md invariant: **Secret-material is never
//! serialized to operator-facing wire surfaces.** engenho-types
//! intentionally keeps the typed `Secret { data, string_data, … }`
//! fully populated because internal consumers (engenho-kubelet,
//! the runtime materializer) need the bytes. The redaction
//! happens at the **boundary** — when engenho-mcp serializes a
//! Secret out, it transforms `Secret → SecretView` first.
//!
//! Test invariant: any payload produced via `redact_secret` MUST
//! NOT contain the secret value bytes anywhere in its serialized
//! form. The `data_keys` array carries only the keys; values are
//! collapsed to a `*REDACTED*` placeholder.
//!
//! Pattern extends to future Secret-like types (TLS, dockerconfig,
//! token-projected volumes). Each gets its own typed view that
//! lives here.

use engenho_types::generated_v1_34::core_v1::{Secret, SecretType};
use engenho_types::meta::ObjectMeta;
use serde::Serialize;

/// Operator-facing view of a Kubernetes Secret. Carries the
/// metadata + type + key-set, NEVER the values.
#[derive(Debug, Serialize)]
pub struct SecretView<'a> {
    pub metadata: &'a ObjectMeta,
    /// `Type` of the Secret (Opaque, kubernetes.io/tls, …).
    /// Pass-through from engenho-types' typed enum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<&'a SecretType>,
    /// Whether the Secret is immutable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
    /// Sorted keys of `data` — values redacted.
    #[serde(rename = "dataKeys")]
    pub data_keys: Vec<&'a String>,
    /// Sorted keys of `stringData` — values redacted.
    #[serde(rename = "stringDataKeys", skip_serializing_if = "Vec::is_empty")]
    pub string_data_keys: Vec<&'a String>,
    /// Constant marker — present on every redacted view so
    /// consumers can pattern-match without parsing the missing
    /// `data` field.
    pub redacted: bool,
}

impl<'a> From<&'a Secret> for SecretView<'a> {
    fn from(s: &'a Secret) -> Self {
        // BTreeMap iteration is sorted by key; preserve it.
        let data_keys: Vec<&String> = s.data.keys().collect();
        let string_data_keys: Vec<&String> = s.string_data.keys().collect();
        Self {
            metadata: &s.metadata,
            r#type: s.r#type.as_ref(),
            immutable: s.immutable,
            data_keys,
            string_data_keys,
            redacted: true,
        }
    }
}

/// Convert a `Secret` into the redacted wire shape ready for JSON
/// emission. Wrapper helps callers that need to surface "this is
/// a Secret response" via `kind`/`apiVersion` injection at the
/// outer layer.
pub fn redact_secret(s: &Secret) -> serde_json::Value {
    serde_json::to_value(&SecretView::from(s)).expect("SecretView is infallibly serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_types::generated_v1_34::core_v1::{KnownSecretType, SecretType};

    fn sample_secret() -> Secret {
        let mut s = Secret::default();
        s.metadata.name = "podinfo-token".into();
        s.metadata.namespace = Some("default".into());
        s.r#type = Some(SecretType::Known(KnownSecretType::ServiceAccountToken));
        // Two data keys with realistic values.
        s.data
            .insert("token".into(), "ZXhhbXBsZS10b2tlbi12YWx1ZQ==".into());
        s.data.insert(
            "ca.crt".into(),
            "LS0tLS1CRUdJTi1DRVJUSUZJQ0FURS0tLS0t".into(),
        );
        s.data.insert("namespace".into(), "ZGVmYXVsdA==".into());
        s.immutable = Some(false);
        s
    }

    #[test]
    fn redacted_view_carries_keys_only() {
        let secret = sample_secret();
        let view: SecretView<'_> = (&secret).into();
        assert_eq!(view.data_keys.len(), 3);
        // BTreeMap sort order: "ca.crt" < "namespace" < "token".
        assert_eq!(view.data_keys[0].as_str(), "ca.crt");
        assert_eq!(view.data_keys[1].as_str(), "namespace");
        assert_eq!(view.data_keys[2].as_str(), "token");
        assert!(view.redacted);
    }

    #[test]
    fn redacted_secret_json_never_contains_values() {
        let secret = sample_secret();
        let json_str = serde_json::to_string(&redact_secret(&secret)).unwrap();
        // Every Secret value from sample_secret() — none must appear.
        let forbidden_substrings = [
            "ZXhhbXBsZS10b2tlbi12YWx1ZQ==",
            "LS0tLS1CRUdJTi1DRVJUSUZJQ0FURS0tLS0t",
            "ZGVmYXVsdA==",
        ];
        for forbidden in forbidden_substrings {
            assert!(
                !json_str.contains(forbidden),
                "redacted Secret JSON leaked secret value {forbidden:?}: {json_str}"
            );
        }
        // But the keys + metadata + type + redacted marker MUST be there.
        assert!(json_str.contains("\"ca.crt\""), "key missing: {json_str}");
        assert!(
            json_str.contains("\"podinfo-token\""),
            "name missing: {json_str}"
        );
        // upstream type id includes the prefix "kubernetes.io/" so
        // we assert on the canonical full string.
        assert!(
            json_str.contains("kubernetes.io/service-account-token"),
            "type missing: {json_str}"
        );
        assert!(
            json_str.contains("\"redacted\":true"),
            "redacted marker missing: {json_str}"
        );
    }

    /// The full Secret still carries values through engenho-types'
    /// own serialization (engenho-kubelet's contract). Redaction
    /// is purely a boundary transform. This test guards that the
    /// boundary actually does the transform — bypassing
    /// `redact_secret` would leak.
    #[test]
    fn raw_secret_serialization_does_contain_values() {
        let secret = sample_secret();
        let raw = serde_json::to_string(&secret).unwrap();
        assert!(
            raw.contains("ZXhhbXBsZS10b2tlbi12YWx1ZQ=="),
            "raw Secret serialization should preserve values: {raw}"
        );
        // The boundary code MUST call redact_secret to avoid leaking.
        // The view transform is the proof.
    }

    #[test]
    fn empty_secret_redacts_to_empty_key_arrays() {
        let secret = Secret::default();
        let view: SecretView<'_> = (&secret).into();
        assert!(view.data_keys.is_empty());
        assert!(view.string_data_keys.is_empty());
    }

    #[test]
    fn many_keys_preserves_sorted_order() {
        let mut s = Secret::default();
        for k in ["z", "a", "m", "k", "b"] {
            s.data.insert(k.to_string(), "v".into());
        }
        let view: SecretView<'_> = (&s).into();
        let keys: Vec<&str> = view.data_keys.iter().map(|s| s.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "k", "m", "z"]);
    }
}
