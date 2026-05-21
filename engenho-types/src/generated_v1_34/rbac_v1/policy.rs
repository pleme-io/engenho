//! Shared RBAC primitives — `PolicyRule` + `RoleRef` + `Subject`.
//!
//! Used by Role, ClusterRole, RoleBinding, ClusterRoleBinding.
//! Living in `policy` because each kind references these
//! independently — no duplication.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};

/// `PolicyRule` holds information that describes a policy rule
/// — what API groups + resources + verbs are allowed.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PolicyRule {
    /// `Verbs` — list of allowed verbs (e.g. `get`, `list`, `watch`,
    /// `create`, `update`, `patch`, `delete`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verbs: Vec<String>,

    /// `APIGroups` — list of API groups (`""` for core, `apps`, etc.).
    #[serde(default, rename = "apiGroups", skip_serializing_if = "Vec::is_empty")]
    pub api_groups: Vec<String>,

    /// `Resources` — list of resources (plural form, e.g. `pods`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<String>,

    /// `ResourceNames` — restrict to specific resource names.
    #[serde(default, rename = "resourceNames", skip_serializing_if = "Vec::is_empty")]
    pub resource_names: Vec<String>,

    /// `NonResourceURLs` — for non-resource endpoints like `/healthz`.
    #[serde(default, rename = "nonResourceURLs", skip_serializing_if = "Vec::is_empty")]
    pub non_resource_urls: Vec<String>,
}

/// `RoleRef` references a Role or ClusterRole in the current
/// namespace or cluster.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoleRef {
    /// `APIGroup` is the group for the resource being referenced
    /// (always `rbac.authorization.k8s.io`).
    #[serde(rename = "apiGroup")]
    pub api_group: String,
    /// `Kind` is the type of resource being referenced (`Role` or
    /// `ClusterRole`).
    pub kind: String,
    /// Name of the role.
    pub name: String,
}

/// `Subject` references a user, group, or service account.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Subject {
    /// `Kind` of subject — `User` | `Group` | `ServiceAccount`.
    pub kind: String,
    /// `APIGroup` is the group of the subject — empty for core
    /// (ServiceAccount).
    #[serde(default, rename = "apiGroup", skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    /// Subject name.
    pub name: String,
    /// Namespace of the subject (for ServiceAccount). Empty for
    /// User and Group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_rule_round_trips() {
        let p = PolicyRule {
            verbs: vec!["get".into(), "list".into(), "watch".into()],
            api_groups: vec!["".into(), "apps".into()],
            resources: vec!["pods".into(), "deployments".into()],
            ..Default::default()
        };
        let s = serde_json::to_string(&p).unwrap();
        assert!(s.contains("\"apiGroups\""), "got: {s}");
        let back: PolicyRule = serde_json::from_str(&s).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn role_ref_round_trips() {
        let r = RoleRef {
            api_group: "rbac.authorization.k8s.io".into(),
            kind: "Role".into(),
            name: "podinfo-reader".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("rbac.authorization.k8s.io"));
        let back: RoleRef = serde_json::from_str(&s).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn subject_round_trips() {
        let subj = Subject {
            kind: "ServiceAccount".into(),
            name: "default".into(),
            namespace: Some("default".into()),
            ..Default::default()
        };
        let s = serde_json::to_string(&subj).unwrap();
        assert!(s.contains("ServiceAccount"));
        let back: Subject = serde_json::from_str(&s).unwrap();
        assert_eq!(back, subj);
    }
}
