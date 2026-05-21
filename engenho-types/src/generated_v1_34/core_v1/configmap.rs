//! `ConfigMap` — M0.0.2 typed expansion #3.
//!
//! ConfigMap is wire-different from the Pod/Service shape: it has
//! NO spec/status. The data + binaryData + immutable fields live
//! directly on the top-level object. The generator's original
//! output had `spec` / `status` fields which were wire-wrong
//! (ConfigMap requests with those fields would be rejected by
//! the apiserver). M0.0.2 fixes the shape AND types the maps.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// `ConfigMap` holds configuration data for pods to consume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMap {
    /// Standard object metadata.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    /// `Data` contains the configuration data. Each key must be a
    /// valid DNS_SUBDOMAIN with an optional leading dot.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<String, String>,

    /// `BinaryData` contains the binary data. Each key must consist
    /// of alphanumeric characters, `-`, `_` or `.`. Values are
    /// base64-encoded.
    #[serde(default, rename = "binaryData", skip_serializing_if = "BTreeMap::is_empty")]
    pub binary_data: BTreeMap<String, String>,

    /// `Immutable`, if set to true, ensures that data stored in the
    /// ConfigMap cannot be updated (only object meta can be modified).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
}

impl KubeResource for ConfigMap {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "ConfigMap",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "configmaps",
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
    fn configmap_round_trips_with_data() {
        let mut cm = ConfigMap::default();
        cm.metadata.name = "kube-proxy".into();
        cm.metadata.namespace = Some("kube-system".into());
        cm.data.insert("config.conf".into(), "mode: iptables".into());
        cm.immutable = Some(true);
        let json = serde_json::to_string(&cm).unwrap();
        assert!(json.contains("\"config.conf\""));
        assert!(json.contains("\"immutable\":true"));
        // Wire-clean: no spec/status leaked.
        assert!(!json.contains("\"spec\""), "ConfigMap must not emit spec field: {json}");
        assert!(!json.contains("\"status\""), "ConfigMap must not emit status field: {json}");
        let back: ConfigMap = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cm);
    }

    #[test]
    fn configmap_gvk_is_core_v1() {
        assert_eq!(ConfigMap::GVK.kind, "ConfigMap");
        assert_eq!(ConfigMap::GVR.resource, "configmaps");
        assert_eq!(ConfigMap::SCOPE, Scope::Namespaced);
    }

    #[test]
    fn empty_configmap_serializes_minimally() {
        let s = serde_json::to_string(&ConfigMap::default()).unwrap();
        assert_eq!(s, "{}");
    }
}
