//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// Secret holds secret data of a certain type. The total bytes of the values in the Data field must be less than MaxSecretSize bytes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Secret {
    /// Data contains the secret data. Each key must consist of alphanumeric characters, '-', '_' or '.'. The serialized form of the secret data is a base64 encoded string, representing the arbitrary (possibly non-string) data value here. Described in https://tools.ietf.org/html/rfc4648#section-4
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub data: std::collections::BTreeMap<String, String>,
    /// Immutable, if set to true, ensures that data stored in the Secret cannot be updated (only object metadata can be modified). If not set to true, the field can be modified at any time. Defaulted to nil.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// stringData allows specifying non-binary secret data in string form. It is provided as a write-only input field for convenience. All keys and values are merged into the data field on write, overwriting any existing values. The stringData field is never output when reading from the API.
    #[serde(
        default,
        rename = "stringData",
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub string_data: std::collections::BTreeMap<String, String>,
    /// Used to facilitate programmatic handling of secret data. More info: https://kubernetes.io/docs/concepts/configuration/secret/#secret-types
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<crate::curated_enums::SecretType>,
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
