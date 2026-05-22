//! Typed `HelmChart` CRD (k3s' `helm.cattle.io/v1` resource).
//!
//! Closes the remaining 22 `formatdoc!` sites in `render.rs` per
//! `pleme-io/theory/TYPED-EMISSION.md`. Every emitted byte flows
//! through `serde_yaml::to_string` on a typed struct.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Top-level k3s HelmChart resource. Drops into
/// `/var/lib/rancher/k3s/server/manifests/` and is reconciled by
/// k3s' built-in helm-controller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelmChart {
    /// K8s API version — always `helm.cattle.io/v1` for k3s.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Resource kind — always `HelmChart` for k3s' helm-controller.
    pub kind: String,
    /// Resource metadata (name + namespace).
    pub metadata: HelmChartMetadata,
    /// Helm chart deployment spec.
    pub spec: HelmChartSpec,
}

/// HelmChart resource metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelmChartMetadata {
    /// Resource name (the HelmChart's identifier in the source namespace).
    pub name: String,
    /// Source namespace where the HelmChart resource lives.
    pub namespace: String,
}

/// Mirror of `helm.cattle.io/v1.HelmChart.spec`. Only the fields
/// engenho actually emits are typed; everything else (timeout,
/// jobImage, podSelector, etc.) lives in the `extra` flatten map
/// so operator extensions survive round-trip.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelmChartSpec {
    /// Helm chart repository URL.
    pub repo: String,
    /// Chart name within the repo.
    pub chart: String,
    /// Destination namespace where the chart's resources live.
    pub target_namespace: String,
    /// Whether to create the target namespace if absent. None = k3s default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_namespace: Option<bool>,
    /// Pinned chart version. Optional — k3s tracks "latest" when None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// `valuesContent` block — multi-line YAML that gets piped
    /// into `helm install --values -`. Stored as a raw string so
    /// the embedded YAML structure stays under operator control.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values_content: Option<String>,
    /// Forward-compat for future spec fields. Survives round-trip.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

impl HelmChart {
    /// Construct a minimal HelmChart manifest with the canonical
    /// k3s API version + kind.
    #[must_use]
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, spec: HelmChartSpec) -> Self {
        Self {
            api_version: "helm.cattle.io/v1".to_string(),
            kind: "HelmChart".to_string(),
            metadata: HelmChartMetadata {
                name: name.into(),
                namespace: namespace.into(),
            },
            spec,
        }
    }

    /// Serialize to k3s-compatible YAML. Replaces `formatdoc!`
    /// blocks across the 22 helm-wrapper sites in `render.rs`.
    /// **Typed emission.**
    #[must_use]
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).expect("HelmChart serializes")
    }

    /// Parse a HelmChart manifest. Tolerant of extra spec fields
    /// via [`HelmChartSpec::extra`].
    ///
    /// # Errors
    ///
    /// Returns `Err` on malformed YAML.
    pub fn from_yaml(s: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(s)
    }
}

/// Convenience builder for the most common shape:
/// `repo + chart + target_namespace` with optional createNamespace.
#[must_use]
pub fn simple_chart(
    name: impl Into<String>,
    namespace: impl Into<String>,
    repo: impl Into<String>,
    chart: impl Into<String>,
    target_namespace: impl Into<String>,
    create_namespace: bool,
) -> HelmChart {
    HelmChart::new(
        name,
        namespace,
        HelmChartSpec {
            repo: repo.into(),
            chart: chart.into(),
            target_namespace: target_namespace.into(),
            create_namespace: if create_namespace { Some(true) } else { None },
            version: None,
            values_content: None,
            extra: BTreeMap::new(),
        },
    )
}

/// Builder for charts that need `valuesContent`. Trailing newline
/// is added if absent — k3s' helm controller fails to parse
/// values-content blocks that don't end in a newline.
#[must_use]
pub fn chart_with_values(
    name: impl Into<String>,
    namespace: impl Into<String>,
    repo: impl Into<String>,
    chart: impl Into<String>,
    target_namespace: impl Into<String>,
    create_namespace: bool,
    values_content: String,
) -> HelmChart {
    let values = if values_content.ends_with('\n') {
        values_content
    } else {
        format!("{values_content}\n")
    };
    HelmChart::new(
        name,
        namespace,
        HelmChartSpec {
            repo: repo.into(),
            chart: chart.into(),
            target_namespace: target_namespace.into(),
            create_namespace: if create_namespace { Some(true) } else { None },
            version: None,
            values_content: Some(values),
            extra: BTreeMap::new(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_chart_serializes_with_expected_keys() {
        let c = simple_chart(
            "calico",
            "kube-system",
            "https://docs.tigera.io/calico/charts",
            "tigera-operator",
            "tigera-operator",
            true,
        );
        let yaml = c.to_yaml();
        assert!(yaml.contains("apiVersion: helm.cattle.io/v1"));
        assert!(yaml.contains("kind: HelmChart"));
        assert!(yaml.contains("name: calico"));
        assert!(yaml.contains("namespace: kube-system"));
        assert!(yaml.contains("repo: https://docs.tigera.io/calico/charts"));
        assert!(yaml.contains("chart: tigera-operator"));
        assert!(yaml.contains("targetNamespace: tigera-operator"));
        assert!(yaml.contains("createNamespace: true"));
    }

    #[test]
    fn simple_chart_skips_create_namespace_when_false() {
        let c = simple_chart(
            "calico",
            "kube-system",
            "https://example.com",
            "calico",
            "tigera-operator",
            false,
        );
        let yaml = c.to_yaml();
        assert!(!yaml.contains("createNamespace"));
    }

    #[test]
    fn chart_with_values_includes_values_content() {
        let c = chart_with_values(
            "cilium",
            "kube-system",
            "https://helm.cilium.io",
            "cilium",
            "kube-system",
            false,
            "kubeProxyReplacement: true\nk8sServiceHost: 127.0.0.1".to_string(),
        );
        let yaml = c.to_yaml();
        assert!(yaml.contains("valuesContent: "));
        assert!(yaml.contains("kubeProxyReplacement: true"));
        assert!(yaml.contains("k8sServiceHost: 127.0.0.1"));
    }

    #[test]
    fn chart_round_trips_through_yaml() {
        let original = chart_with_values(
            "cilium",
            "kube-system",
            "https://helm.cilium.io",
            "cilium",
            "kube-system",
            false,
            "kubeProxyReplacement: true\n".to_string(),
        );
        let yaml = original.to_yaml();
        let parsed = HelmChart::from_yaml(&yaml).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn chart_preserves_unknown_spec_fields_via_extra() {
        let yaml = "\
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: x
  namespace: kube-system
spec:
  repo: https://example.com
  chart: x
  targetNamespace: x
  timeout: 5m
  jobImage: my-registry/helm:3.14
";
        let c = HelmChart::from_yaml(yaml).unwrap();
        assert!(c.spec.extra.contains_key("timeout"));
        assert!(c.spec.extra.contains_key("jobImage"));
        // Round-trip preserves them.
        let again = c.to_yaml();
        let reparsed = HelmChart::from_yaml(&again).unwrap();
        assert_eq!(reparsed.spec.extra, c.spec.extra);
    }

    #[test]
    fn chart_with_values_appends_trailing_newline() {
        let c = chart_with_values(
            "x",
            "ns",
            "r",
            "ch",
            "tns",
            false,
            "key: value".to_string(), // no trailing newline
        );
        let values = c.spec.values_content.unwrap();
        assert!(values.ends_with('\n'));
    }

    #[test]
    fn chart_pinned_version_serializes() {
        let mut c = simple_chart("x", "ns", "r", "ch", "tns", false);
        c.spec.version = Some("1.2.3".to_string());
        let yaml = c.to_yaml();
        assert!(yaml.contains("version: 1.2.3"));
    }

    #[test]
    fn helm_chart_constructor_uses_canonical_api_version() {
        let c = HelmChart::new("x", "ns", HelmChartSpec::default());
        assert_eq!(c.api_version, "helm.cattle.io/v1");
        assert_eq!(c.kind, "HelmChart");
    }
}
