//! `rbac_v1` typed kinds. Kind structs (Role / ClusterRole / RoleBinding /
//! ClusterRoleBinding) are GENERATED (Source: engenho-kube-codegen); the shared
//! RBAC primitives (`PolicyRule` / `RoleRef` / `Subject`) are hand-authored in
//! [`policy`] because the codegen-emitted versions in
//! [`crate::generated_v1_34::types`] DROP the load-bearing `kind` discriminator
//! on `RoleRef` + `Subject` (and rename `nonResourceURLs` to a
//! double-underscore field), which makes them unusable for authorization (the
//! RBAC Authorizer dispatches on `Subject.kind` / `RoleRef.kind`). The `policy`
//! module is the single canonical source; the kind structs reference it.

mod clusterrole;
mod clusterrolebinding;
mod policy;
mod role;
mod rolebinding;

pub use clusterrole::ClusterRole;
pub use clusterrolebinding::ClusterRoleBinding;
pub use policy::{PolicyRule, RoleRef, Subject};
pub use role::Role;
pub use rolebinding::RoleBinding;

// Re-export the rest of the generated types EXCEPT the broken RBAC primitives
// (PolicyRule / RoleRef / Subject) — the explicit `pub use policy::{…}` above
// takes precedence over this glob for those three names, and the kind structs
// import them from `super::policy` directly. AggregationRule (referenced by
// ClusterRole) still comes from here.
pub use crate::generated_v1_34::types::*;
