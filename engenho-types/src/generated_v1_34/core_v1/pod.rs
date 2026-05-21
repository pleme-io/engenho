//! `Pod` — M0.0.2 typed bullseye.
//!
//! Originally emitted by `engenho-kube-codegen` with opaque
//! `serde_json::Value` for spec + status. Hand-promoted to typed
//! `PodSpec` / `PodStatus` as the M0.0.2 bullseye per ENGENHO.md
//! §X. The generator at M0.0.3 reproduces this shape byte-for-byte;
//! until then this file is the source of truth.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds. To extend the typed
//! expansion for another kind (Deployment, Service, …) follow the
//! same pattern: add a `<kind>_spec.rs` module beside the kind's
//! own file with hand-authored typed shapes, then flip the
//! `spec` / `status` fields here to reference them.

#![allow(clippy::module_name_repetitions)]

use std::borrow::Cow;
use serde::{Deserialize, Serialize};

use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

use super::pod_spec::{PodSpec, PodStatus};

/// `Pod` is a collection of containers that can run on a host.
/// This resource is created by clients and scheduled onto hosts.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Pod {
    /// Standard object metadata.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: ObjectMeta,

    /// Specification of the desired behavior of the pod.
    #[serde(default, skip_serializing_if = "is_empty_spec")]
    pub spec: PodSpec,

    /// Most recently observed status of the pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PodStatus>,
}

impl KubeResource for Pod {
    const GVK: GroupVersionKind = GroupVersionKind {
        group:   "",
        version: "v1",
        kind:    "Pod",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group:    "",
        version:  "v1",
        resource: "pods",
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

fn is_empty_meta(m: &ObjectMeta) -> bool { m == &ObjectMeta::default() }
fn is_empty_spec(s: &PodSpec) -> bool { s == &PodSpec::default() }

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::pod_spec::{Container, ContainerPort, PodPhase};

    #[test]
    fn pod_round_trips_with_typed_spec_and_status() {
        let mut pod = Pod::default();
        pod.metadata.name = "podinfo-abc".into();
        pod.metadata.namespace = Some("default".into());
        pod.spec.containers.push(Container {
            name: "podinfod".into(),
            image: "ghcr.io/stefanprodan/podinfo:6.12.0".into(),
            ports: vec![ContainerPort {
                container_port: 9898,
                name: Some("http".into()),
                protocol: Some("TCP".into()),
                ..Default::default()
            }],
            ..Default::default()
        });
        pod.status = Some(PodStatus {
            phase: Some(PodPhase::Running),
            pod_ip: Some("10.42.0.20".into()),
            ..Default::default()
        });

        let json = serde_json::to_string(&pod).unwrap();
        assert!(json.contains("\"podinfod\""));
        assert!(json.contains("\"Running\""));
        let back: Pod = serde_json::from_str(&json).unwrap();
        assert_eq!(back, pod);
    }

    #[test]
    fn empty_pod_serializes_minimally() {
        let s = serde_json::to_string(&Pod::default()).unwrap();
        // metadata/spec are empty-skipped; status is None-skipped.
        assert_eq!(s, "{}");
    }

    #[test]
    fn pod_gvk_is_core_v1_pod() {
        assert_eq!(Pod::GVK.group, "");
        assert_eq!(Pod::GVK.version, "v1");
        assert_eq!(Pod::GVK.kind, "Pod");
        assert_eq!(Pod::GVR.resource, "pods");
        assert_eq!(Pod::SCOPE, Scope::Namespaced);
    }
}
