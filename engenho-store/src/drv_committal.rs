//! Drv → Raft committal helper.
//!
//! Bridges `engenho_substrate::Drv` typed values into the
//! `engenho-store` apiserver path so a Drv becomes a first-class
//! Raft-committed object alongside Pods / Services / Deployments.
//!
//! ## What this does (and doesn't)
//!
//! - DOES: wrap a [`Drv`] as a `Derivation` resource (group
//!   `engenho.io`, version `v1`) + the canonical [`ResourceKey`]
//!   it lives under. Caller then submits via the normal
//!   `StoreMesh::propose` path.
//! - DOES: provide pure helpers for serializing the Drv into the
//!   resource's `spec` block (so the apiserver can validate +
//!   index by `spec.drvHash`).
//! - DOES NOT: hold a StoreMesh reference. This module is pure
//!   value construction; consumers pass it to their store.
//!
//! ## Why a separate module
//!
//! Keeps the substrate's typed Drv decoupled from the apiserver's
//! K8s-shaped resource form. The translation is the SINGLE place
//! that knows both shapes — extends easily to other formats
//! (CRD spec / OpenAPI / iroh blob ID / ...).

use engenho_substrate::Drv;
use serde_json::{json, Value};

use crate::command::{Reason, ResourceCommand};
use crate::resource::ResourceKey;

/// Group + version + kind for derivation resources in the store.
pub const DRV_GROUP: &str = "engenho.io";
/// API version.
pub const DRV_VERSION: &str = "v1";
/// Resource kind.
pub const DRV_KIND: &str = "Derivation";

/// Build the canonical [`ResourceKey`] for a Drv. By convention,
/// resource name = the drv hash hex (collision-free, derived).
#[must_use]
pub fn drv_resource_key(drv: &Drv, namespace: &str) -> ResourceKey {
    ResourceKey::namespaced(
        DRV_GROUP,
        DRV_VERSION,
        DRV_KIND,
        namespace,
        &drv.drv_hash.to_hex(),
    )
}

/// Render a [`Drv`] as a K8s-shape resource value. The full Drv
/// JSON lives under `spec.drv`; the drv hash hex is duplicated to
/// `spec.drvHash` for fast indexing without deserializing the full
/// drv.
///
/// # Errors
/// Returns `Err` if the Drv can't be serialized (should be
/// impossible for a valid Drv, but `serde_json` is technically
/// fallible).
pub fn render_drv_resource(drv: &Drv, namespace: &str) -> Result<Value, serde_json::Error> {
    let drv_json = serde_json::to_value(drv)?;
    Ok(json!({
        "apiVersion": format!("{DRV_GROUP}/{DRV_VERSION}"),
        "kind": DRV_KIND,
        "metadata": {
            "name": drv.drv_hash.to_hex(),
            "namespace": namespace,
        },
        "spec": {
            "drvHash": drv.drv_hash.to_hex(),
            "system": drv.system.clone(),
            "drv": drv_json,
        }
    }))
}

/// Build a `ResourceCommand::Put` for committing a Drv. Caller
/// passes it to `StoreMesh::propose`.
///
/// # Errors
/// Same as [`render_drv_resource`].
pub fn put_drv_command(
    drv: &Drv,
    namespace: &str,
    reason: Reason,
) -> Result<ResourceCommand, serde_json::Error> {
    let value = render_drv_resource(drv, namespace)?;
    let key = drv_resource_key(drv, namespace);
    Ok(ResourceCommand::Put { key, value, reason })
}

/// Build a `ResourceCommand::Delete` for removing a Drv.
#[must_use]
pub fn delete_drv_command(
    drv_hash_hex: &str,
    namespace: &str,
    reason: Reason,
) -> ResourceCommand {
    let key = ResourceKey::namespaced(
        DRV_GROUP,
        DRV_VERSION,
        DRV_KIND,
        namespace,
        drv_hash_hex,
    );
    ResourceCommand::Delete { key, reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_substrate::DrvHash;

    fn sample_drv() -> Drv {
        Drv::synthetic(DrvHash::from_bytes(b"sample-drv"), "x86_64-linux")
    }

    #[test]
    fn key_uses_drv_hash_as_resource_name() {
        let drv = sample_drv();
        let k = drv_resource_key(&drv, "default");
        assert_eq!(k.name, drv.drv_hash.to_hex());
        assert_eq!(k.group, "engenho.io");
        assert_eq!(k.version, "v1");
        assert_eq!(k.kind, "Derivation");
        assert_eq!(k.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn render_resource_includes_apiversion_and_metadata() {
        let drv = sample_drv();
        let v = render_drv_resource(&drv, "default").unwrap();
        assert_eq!(v.get("apiVersion").unwrap(), "engenho.io/v1");
        assert_eq!(v.get("kind").unwrap(), "Derivation");
        assert_eq!(
            v.get("metadata").unwrap().get("name").unwrap(),
            &drv.drv_hash.to_hex()
        );
        assert_eq!(
            v.get("metadata").unwrap().get("namespace").unwrap(),
            "default"
        );
    }

    #[test]
    fn render_resource_indexes_drv_hash_in_spec() {
        let drv = sample_drv();
        let v = render_drv_resource(&drv, "default").unwrap();
        assert_eq!(
            v.get("spec").unwrap().get("drvHash").unwrap(),
            &drv.drv_hash.to_hex()
        );
        assert_eq!(
            v.get("spec").unwrap().get("system").unwrap(),
            &drv.system
        );
    }

    #[test]
    fn render_resource_embeds_full_drv_under_spec() {
        let drv = sample_drv();
        let v = render_drv_resource(&drv, "default").unwrap();
        let embedded = v.get("spec").unwrap().get("drv").unwrap();
        // Round-trip: deserialize back into a Drv, assert equal.
        let back: Drv = serde_json::from_value(embedded.clone()).unwrap();
        assert_eq!(back, drv);
    }

    #[test]
    fn put_drv_command_constructs_put_variant() {
        let drv = sample_drv();
        let cmd = put_drv_command(&drv, "default", Reason::Operator).unwrap();
        match cmd {
            ResourceCommand::Put { key, value, reason } => {
                assert_eq!(key.kind, "Derivation");
                assert_eq!(reason, Reason::Operator);
                assert_eq!(value.get("kind").unwrap(), "Derivation");
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn delete_drv_command_constructs_delete_variant() {
        let cmd = delete_drv_command("abc123", "default", Reason::Operator);
        match cmd {
            ResourceCommand::Delete { key, reason } => {
                assert_eq!(key.name, "abc123");
                assert_eq!(reason, Reason::Operator);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn render_resource_round_trips_through_serde() {
        let drv = sample_drv();
        let v = render_drv_resource(&drv, "kube-system").unwrap();
        let bytes = serde_json::to_vec(&v).unwrap();
        let back: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn group_version_kind_constants_match_canonical() {
        assert_eq!(DRV_GROUP, "engenho.io");
        assert_eq!(DRV_VERSION, "v1");
        assert_eq!(DRV_KIND, "Derivation");
    }
}
