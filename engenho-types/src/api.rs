//! API discovery helpers — `GroupVersion` parsing + path construction.
//!
//! Tiny helper layer that turns the typed `GroupVersionKind` /
//! `GroupVersionResource` from `kind.rs` into the URL path segments
//! the apiserver router needs.

use crate::kind::{GroupVersionKind, GroupVersionResource, Scope};

/// Construct the REST path for a List/Watch on a resource.
///
/// - Core (`group=""`): `/api/v1/{resource}` or
///   `/api/v1/namespaces/{ns}/{resource}`.
/// - Non-core: `/apis/{group}/{version}/{resource}` or
///   `/apis/{group}/{version}/namespaces/{ns}/{resource}`.
#[must_use]
pub fn list_path(gvr: GroupVersionResource, scope: Scope, namespace: Option<&str>) -> String {
    let base = if gvr.group.is_empty() {
        format!("/api/{}", gvr.version)
    } else {
        format!("/apis/{}/{}", gvr.group, gvr.version)
    };
    match (scope, namespace) {
        (Scope::Namespaced, Some(ns)) => format!("{base}/namespaces/{ns}/{}", gvr.resource),
        (Scope::Namespaced, None) | (Scope::Cluster, _) => format!("{base}/{}", gvr.resource),
    }
}

/// Construct the REST path for a single-item GET / PUT / PATCH / DELETE.
#[must_use]
pub fn item_path(
    gvr: GroupVersionResource,
    scope: Scope,
    namespace: Option<&str>,
    name: &str,
) -> String {
    format!("{}/{name}", list_path(gvr, scope, namespace))
}

/// Build the `apiVersion` string from a GVK.
#[must_use]
pub fn api_version(gvk: GroupVersionKind) -> String {
    if gvk.group.is_empty() {
        gvk.version.to_string()
    } else {
        format!("{}/{}", gvk.group, gvk.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POD_GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "pods",
    };
    const DEPLOY_GVR: GroupVersionResource = GroupVersionResource {
        group: "apps",
        version: "v1",
        resource: "deployments",
    };
    const POD_GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Pod",
    };
    const DEPLOY_GVK: GroupVersionKind = GroupVersionKind {
        group: "apps",
        version: "v1",
        kind: "Deployment",
    };

    #[test]
    fn list_path_core_namespaced() {
        assert_eq!(
            list_path(POD_GVR, Scope::Namespaced, Some("default")),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn list_path_core_cluster() {
        assert_eq!(list_path(POD_GVR, Scope::Namespaced, None), "/api/v1/pods");
    }

    #[test]
    fn list_path_apps_namespaced() {
        assert_eq!(
            list_path(DEPLOY_GVR, Scope::Namespaced, Some("kube-system")),
            "/apis/apps/v1/namespaces/kube-system/deployments"
        );
    }

    #[test]
    fn item_path_appends_name() {
        assert_eq!(
            item_path(POD_GVR, Scope::Namespaced, Some("default"), "my-pod"),
            "/api/v1/namespaces/default/pods/my-pod"
        );
    }

    #[test]
    fn api_version_core() {
        assert_eq!(api_version(POD_GVK), "v1");
    }

    #[test]
    fn api_version_apps() {
        assert_eq!(api_version(DEPLOY_GVK), "apps/v1");
    }
}
