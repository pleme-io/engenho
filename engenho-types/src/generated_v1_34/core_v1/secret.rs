//! `Secret` — M0.0.2 typed expansion #6.
//!
//! Wire-different from Pod-style kinds: Secret has NO spec/status.
//! It carries `data` (base64-encoded values), `stringData` (a
//! write-only convenience field that the apiserver merges into
//! `data`), `binaryData`, `immutable`, and a typed `type` enum.
//!
//! **Security note for engenho-mcp consumers:** the FULL Secret
//! is consumed only by engenho-kubelet (the runtime materializer).
//! At the MCP boundary, Secret routes through `SecretView` which
//! carries the KEYS but NEVER the values. See
//! `engenho-mcp::reader::redaction::SecretView` and the LEAN.md
//! invariant (`docs/LEAN.md` → "Secret-material-free by type").

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// `Secret` holds secret data of a certain type. Operator-facing
/// surfaces (engenho-mcp, kubectl describe with --show-secrets=false)
/// MUST NOT serialize the values directly; engenho-kubelet is the
/// canonical consumer that materializes the bytes for runtime mount.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Secret {
    /// Standard object metadata.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    /// `Data` contains base64-encoded byte arrays. Each key must be
    /// a valid DNS_SUBDOMAIN. Values are decoded by consumers.
    ///
    /// CAUTION: values are SECRET MATERIAL. Operator-facing wire
    /// surfaces must redact through `SecretView`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<String, String>,

    /// `StringData` is a write-only convenience field — clients
    /// provide values as raw UTF-8; the apiserver base64-encodes
    /// + merges into `data`. Read responses never carry it.
    #[serde(default, rename = "stringData", skip_serializing_if = "BTreeMap::is_empty")]
    pub string_data: BTreeMap<String, String>,

    /// `Immutable`, if set to true, ensures the Secret data is
    /// frozen — only metadata can change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,

    /// Typed Secret type — closed enum mirrors the upstream
    /// canonical types. Custom types fall through to `Opaque`
    /// (the safe default).
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<SecretType>,
}

/// Typed Secret type — upstream's canonical types as a closed
/// enum. Custom string types deserialize as `Other(s)` to keep
/// the wire faithful.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SecretType {
    /// Known upstream-canonical type identifiers.
    Known(KnownSecretType),
    /// Any other string passed through verbatim.
    Other(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnownSecretType {
    /// `Opaque` — default catch-all.
    Opaque,
    /// `kubernetes.io/service-account-token`
    #[serde(rename = "kubernetes.io/service-account-token")]
    ServiceAccountToken,
    /// `kubernetes.io/dockercfg`
    #[serde(rename = "kubernetes.io/dockercfg")]
    Dockercfg,
    /// `kubernetes.io/dockerconfigjson`
    #[serde(rename = "kubernetes.io/dockerconfigjson")]
    DockerConfigJson,
    /// `kubernetes.io/basic-auth`
    #[serde(rename = "kubernetes.io/basic-auth")]
    BasicAuth,
    /// `kubernetes.io/ssh-auth`
    #[serde(rename = "kubernetes.io/ssh-auth")]
    SshAuth,
    /// `kubernetes.io/tls`
    #[serde(rename = "kubernetes.io/tls")]
    Tls,
    /// `bootstrap.kubernetes.io/token`
    #[serde(rename = "bootstrap.kubernetes.io/token")]
    BootstrapToken,
}

impl KubeResource for Secret {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Secret",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "secrets",
    };
    const SCOPE: Scope = Scope::Namespaced;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.metadata.name.as_str())
    }
    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.metadata.namespace.as_deref().map(Cow::Borrowed)
    }
    fn resource_version(&self) -> Option<Cow<'_, str>> {
        if self.metadata.resource_version.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(self.metadata.resource_version.as_str()))
        }
    }
}

fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_type_serializes_canonical_strings() {
        let cases = [
            (SecretType::Known(KnownSecretType::Opaque), "\"Opaque\""),
            (
                SecretType::Known(KnownSecretType::ServiceAccountToken),
                "\"kubernetes.io/service-account-token\"",
            ),
            (
                SecretType::Known(KnownSecretType::Tls),
                "\"kubernetes.io/tls\"",
            ),
            (SecretType::Other("custom".into()), "\"custom\""),
        ];
        for (t, expected) in cases {
            let s = serde_json::to_string(&t).unwrap();
            assert_eq!(s, expected, "{t:?}");
        }
    }

    #[test]
    fn secret_round_trips_with_typed_type() {
        let mut s = Secret::default();
        s.metadata.name = "podinfo-token".into();
        s.metadata.namespace = Some("default".into());
        s.r#type = Some(SecretType::Known(KnownSecretType::ServiceAccountToken));
        s.data.insert(
            "token".into(),
            "ZXhhbXBsZS10b2tlbg==".into(),
        );
        s.data.insert("ca.crt".into(), "LS0t".into());
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            json.contains("\"kubernetes.io/service-account-token\""),
            "got: {json}"
        );
        let back: Secret = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    /// At engenho-types level, Secret IS allowed to serialize
    /// values — engenho-kubelet (a future internal consumer) needs
    /// them. The redaction happens at the engenho-mcp boundary
    /// via `SecretView`, not here.
    #[test]
    fn secret_values_survive_engenho_types_serialization() {
        let mut s = Secret::default();
        s.data.insert("k".into(), "secret-value".into());
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            json.contains("secret-value"),
            "engenho-types::Secret intentionally keeps values; redaction is engenho-mcp's job"
        );
    }

    #[test]
    fn secret_gvk_is_core_v1() {
        assert_eq!(Secret::GVK.kind, "Secret");
        assert_eq!(Secret::GVR.resource, "secrets");
        assert_eq!(Secret::SCOPE, Scope::Namespaced);
    }
}
