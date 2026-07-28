//! The PRODUCTION [`RbacStoreEnv`] — a thin wrapper over `Arc<StoreMesh>`.
//!
//! Each method lists/gets the typed RBAC kinds from the store's catalog
//! (synchronous local-replica reads, no Raft round-trip) and deserializes the
//! opaque `serde_json::Value` (= `ResourceValue`) into the GENERATED `rbac_v1`
//! typed structs via `serde_json::from_value` — NO parallel parser, per the
//! PRIME DIRECTIVE. A stored object that fails to deserialize is SKIPPED + a
//! `tracing::warn!` is emitted (a malformed Role contributes NoOpinion, never a
//! panic / silent wrong answer).
//!
//! The store keys: `("rbac.authorization.k8s.io", "v1", <Kind>, <ns?>, <name>)`.
//! Role/RoleBinding are namespaced; ClusterRole/ClusterRoleBinding are
//! cluster-scoped (catalog rows confirm namespaced=true/false respectively).

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{ResourceKey, StoreMesh};
use engenho_types::generated_v1_34::rbac_v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};

use super::RbacStoreEnv;

/// The RBAC group + version the store keys + lists use.
const RBAC_GROUP: &str = "rbac.authorization.k8s.io";
const RBAC_VERSION: &str = "v1";

/// Production [`RbacStoreEnv`] over the distributed store. Cheaply cloneable
/// (`Arc` clone of the store).
#[derive(Clone)]
pub struct StoreRbacEnv {
    store: Arc<StoreMesh>,
}

impl StoreRbacEnv {
    /// Wrap the store. The runtime threads `store.clone()` in.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self { store }
    }

    /// Deserialize one stored value into a typed `T`, logging + dropping a
    /// malformed object (NoOpinion contribution, never a panic). `kind` is for
    /// the warning text only.
    fn deserialize_or_warn<T: serde::de::DeserializeOwned>(
        value: serde_json::Value,
        kind: &str,
    ) -> Option<T> {
        match serde_json::from_value::<T>(value) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    kind = %kind,
                    error = %e,
                    "RBAC: stored {kind} failed to deserialize — skipped (contributes NoOpinion)"
                );
                None
            }
        }
    }
}

#[async_trait]
impl RbacStoreEnv for StoreRbacEnv {
    async fn list_cluster_role_bindings(&self) -> Vec<ClusterRoleBinding> {
        self.store
            .list(RBAC_GROUP, RBAC_VERSION, "ClusterRoleBinding", None)
            .await
            .into_iter()
            .filter_map(|(_k, v)| Self::deserialize_or_warn(v, "ClusterRoleBinding"))
            .collect()
    }

    async fn list_role_bindings(&self, ns: &str) -> Vec<RoleBinding> {
        self.store
            .list(RBAC_GROUP, RBAC_VERSION, "RoleBinding", Some(ns))
            .await
            .into_iter()
            .filter_map(|(_k, v)| Self::deserialize_or_warn(v, "RoleBinding"))
            .collect()
    }

    async fn get_cluster_role(&self, name: &str) -> Option<ClusterRole> {
        let key = ResourceKey::cluster_scoped(RBAC_GROUP, RBAC_VERSION, "ClusterRole", name);
        let v = self.store.get(&key).await?;
        Self::deserialize_or_warn(v, "ClusterRole")
    }

    async fn get_role(&self, ns: &str, name: &str) -> Option<Role> {
        let key = ResourceKey::namespaced(RBAC_GROUP, RBAC_VERSION, "Role", ns, name);
        let v = self.store.get(&key).await?;
        Self::deserialize_or_warn(v, "Role")
    }
}
