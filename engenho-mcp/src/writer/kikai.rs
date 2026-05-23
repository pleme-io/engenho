//! `KikaiClusterWriter` — production [`ClusterWriter`] backed by
//! [`engenho_kube_client::ReqwestKubeClient`].
//!
//! Same dispatch pattern as `KikaiClusterReader`: closed-enum
//! match on `ResourceKind` → typed `client.patch::<R>()` for
//! Apply, typed `client.delete::<R>()` for Delete. Compiler
//! enforces exhaustiveness — adding a kind to the catalog without
//! adding apply + delete arms here is a build error.
//!
//! Authority enforcement is the trait's contract; this impl checks
//! `authority.can_write()` as the very first thing in every method
//! and returns `WriterError::AuthorityRequired` if false.

use async_trait::async_trait;
use std::path::PathBuf;

use engenho_kube_client::{client::ReqwestKubeClient, config::Kubeconfig};
use engenho_types::client::{DeleteOptions, KubeClient};
use engenho_types::generated_v1_34::apps_v1::{Deployment, ReplicaSet};
use engenho_types::generated_v1_34::core_v1::{
    ConfigMap, Endpoints, Namespace, Node, PersistentVolumeClaim, Pod, Secret, Service,
    ServiceAccount,
};
use engenho_types::generated_v1_34::rbac_v1::{Role, RoleBinding};
use engenho_types::patch::Patch;

use crate::resource_kind::ResourceKind;
use crate::writer::{Authority, ClusterWriter, WriterError};

pub struct KikaiClusterWriter {
    home: PathBuf,
}

impl KikaiClusterWriter {
    #[must_use]
    pub fn new(home: PathBuf) -> Self {
        Self { home }
    }

    pub fn from_env() -> Result<Self, WriterError> {
        let home = std::env::var_os("HOME").ok_or_else(|| {
            WriterError::Invalid("HOME env var not set — cannot locate kikai state".into())
        })?;
        Ok(Self::new(PathBuf::from(home)))
    }

    fn kubeconfig_path(&self, cluster: &str) -> PathBuf {
        self.home.join(".kube/configs").join(cluster)
    }

    fn build_kube_client(&self, cluster: &str) -> Result<ReqwestKubeClient, WriterError> {
        let kc_path = self.kubeconfig_path(cluster);
        if !kc_path.exists() {
            return Err(WriterError::UnknownCluster(format!(
                "kubeconfig missing at {} — cluster '{cluster}' not bootstrapped",
                kc_path.display()
            )));
        }
        let kc = Kubeconfig::load(&kc_path)
            .map_err(|e| WriterError::Invalid(format!("kubeconfig parse for '{cluster}': {e}")))?;
        let conn = kc.resolve_connection().map_err(|e| {
            WriterError::Invalid(format!("kubeconfig resolve for '{cluster}': {e}"))
        })?;
        Ok(ReqwestKubeClient::new(conn))
    }
}

#[async_trait]
impl ClusterWriter for KikaiClusterWriter {
    async fn apply_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        field_manager: &str,
        force: bool,
        authority: &Authority,
    ) -> Result<serde_json::Value, WriterError> {
        if !authority.can_write() {
            return Err(WriterError::AuthorityRequired);
        }
        let client = self.build_kube_client(cluster)?;
        let ns_arg: Option<&str> = if kind.is_cluster_scoped() {
            None
        } else {
            Some(namespace)
        };
        let patch = Patch::Apply {
            body,
            field_manager: field_manager.to_string(),
            force,
        };
        // Compiler-exhaustive: adding a ResourceKind variant without
        // an arm here breaks the build (closed enum + exhaustive match).
        let result_json = match kind {
            ResourceKind::Pod => apply_typed::<Pod>(&client, ns_arg, name, &patch, "pod").await?,
            ResourceKind::Service => {
                apply_typed::<Service>(&client, ns_arg, name, &patch, "service").await?
            }
            ResourceKind::ConfigMap => {
                apply_typed::<ConfigMap>(&client, ns_arg, name, &patch, "configmap").await?
            }
            ResourceKind::Secret => {
                apply_typed::<Secret>(&client, ns_arg, name, &patch, "secret").await?
            }
            ResourceKind::ServiceAccount => {
                apply_typed::<ServiceAccount>(&client, ns_arg, name, &patch, "serviceaccount")
                    .await?
            }
            ResourceKind::Endpoints => {
                apply_typed::<Endpoints>(&client, ns_arg, name, &patch, "endpoints").await?
            }
            ResourceKind::PersistentVolumeClaim => {
                apply_typed::<PersistentVolumeClaim>(
                    &client,
                    ns_arg,
                    name,
                    &patch,
                    "persistentvolumeclaim",
                )
                .await?
            }
            ResourceKind::Namespace => {
                apply_typed::<Namespace>(&client, ns_arg, name, &patch, "namespace").await?
            }
            ResourceKind::Node => {
                apply_typed::<Node>(&client, ns_arg, name, &patch, "node").await?
            }
            ResourceKind::Deployment => {
                apply_typed::<Deployment>(&client, ns_arg, name, &patch, "deployment").await?
            }
            ResourceKind::ReplicaSet => {
                apply_typed::<ReplicaSet>(&client, ns_arg, name, &patch, "replicaset").await?
            }
            ResourceKind::Role => {
                apply_typed::<Role>(&client, ns_arg, name, &patch, "role").await?
            }
            ResourceKind::RoleBinding => {
                apply_typed::<RoleBinding>(&client, ns_arg, name, &patch, "rolebinding").await?
            }
        };
        Ok(result_json)
    }

    async fn delete_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        authority: &Authority,
    ) -> Result<(), WriterError> {
        if !authority.can_write() {
            return Err(WriterError::AuthorityRequired);
        }
        let client = self.build_kube_client(cluster)?;
        let ns_arg: Option<&str> = if kind.is_cluster_scoped() {
            None
        } else {
            Some(namespace)
        };
        let opts = DeleteOptions::default();
        match kind {
            ResourceKind::Pod => delete_typed::<Pod>(&client, ns_arg, name, &opts, "pod").await,
            ResourceKind::Service => {
                delete_typed::<Service>(&client, ns_arg, name, &opts, "service").await
            }
            ResourceKind::ConfigMap => {
                delete_typed::<ConfigMap>(&client, ns_arg, name, &opts, "configmap").await
            }
            ResourceKind::Secret => {
                delete_typed::<Secret>(&client, ns_arg, name, &opts, "secret").await
            }
            ResourceKind::ServiceAccount => {
                delete_typed::<ServiceAccount>(&client, ns_arg, name, &opts, "serviceaccount").await
            }
            ResourceKind::Endpoints => {
                delete_typed::<Endpoints>(&client, ns_arg, name, &opts, "endpoints").await
            }
            ResourceKind::PersistentVolumeClaim => {
                delete_typed::<PersistentVolumeClaim>(
                    &client,
                    ns_arg,
                    name,
                    &opts,
                    "persistentvolumeclaim",
                )
                .await
            }
            ResourceKind::Namespace => {
                delete_typed::<Namespace>(&client, ns_arg, name, &opts, "namespace").await
            }
            ResourceKind::Node => delete_typed::<Node>(&client, ns_arg, name, &opts, "node").await,
            ResourceKind::Deployment => {
                delete_typed::<Deployment>(&client, ns_arg, name, &opts, "deployment").await
            }
            ResourceKind::ReplicaSet => {
                delete_typed::<ReplicaSet>(&client, ns_arg, name, &opts, "replicaset").await
            }
            ResourceKind::Role => delete_typed::<Role>(&client, ns_arg, name, &opts, "role").await,
            ResourceKind::RoleBinding => {
                delete_typed::<RoleBinding>(&client, ns_arg, name, &opts, "rolebinding").await
            }
        }
    }
}

/// Shared apply helper — every match arm goes through this so
/// the URL / patch type / response wrapping logic lives in one
/// place.
async fn apply_typed<R>(
    client: &ReqwestKubeClient,
    namespace: Option<&str>,
    name: &str,
    patch: &Patch,
    label: &str,
) -> Result<serde_json::Value, WriterError>
where
    R: engenho_types::kind::KubeResource + Send + Sync + 'static,
{
    let updated = client
        .patch::<R>(namespace, name, patch)
        .await
        .map_err(|e| {
            let where_str = namespace.unwrap_or("<cluster-scope>");
            WriterError::Api {
                what: format!("{label}/{name} in {where_str}"),
                detail: e.to_string(),
            }
        })?;
    let mut item_json = serde_json::to_value(&updated)
        .map_err(|e| WriterError::Invalid(format!("serialize {label}: {e}")))?;
    if let Some(map) = item_json.as_object_mut() {
        map.insert(
            "kind".into(),
            serde_json::Value::String(R::GVK.kind.to_string()),
        );
        let api_version = if R::GVK.group.is_empty() {
            R::GVK.version.to_string()
        } else {
            format!("{}/{}", R::GVK.group, R::GVK.version)
        };
        map.insert("apiVersion".into(), serde_json::Value::String(api_version));
    }
    Ok(item_json)
}

async fn delete_typed<R>(
    client: &ReqwestKubeClient,
    namespace: Option<&str>,
    name: &str,
    opts: &DeleteOptions,
    label: &str,
) -> Result<(), WriterError>
where
    R: engenho_types::kind::KubeResource + Send + Sync + 'static,
{
    client
        .delete::<R>(namespace, name, opts)
        .await
        .map_err(|e| {
            let where_str = namespace.unwrap_or("<cluster-scope>");
            WriterError::Api {
                what: format!("{label}/{name} in {where_str}"),
                detail: e.to_string(),
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, KikaiClusterWriter) {
        let tmp = TempDir::new().unwrap();
        let writer = KikaiClusterWriter::new(tmp.path().to_path_buf());
        (tmp, writer)
    }

    /// Placeholder authority is always rejected — every kind, every method.
    /// This guards the trait contract: as long as `Authority::Placeholder`
    /// is the only variant, no path through `apply_resource` or
    /// `delete_resource` succeeds. P2 (saguão) lands the variant that
    /// can flip these to actual mutations.
    #[tokio::test]
    async fn placeholder_authority_rejects_every_kind() {
        let (_tmp, writer) = setup();
        let auth = Authority::Placeholder;
        for &kind in ResourceKind::all() {
            let apply_err = writer
                .apply_resource(
                    "demo",
                    kind,
                    "default",
                    "name",
                    serde_json::json!({}),
                    "engenho-mcp",
                    false,
                    &auth,
                )
                .await
                .unwrap_err();
            assert_eq!(
                apply_err.kind(),
                "authority_required",
                "{kind:?} apply should require authority"
            );
            let delete_err = writer
                .delete_resource("demo", kind, "default", "name", &auth)
                .await
                .unwrap_err();
            assert_eq!(
                delete_err.kind(),
                "authority_required",
                "{kind:?} delete should require authority"
            );
        }
    }

    /// Without authority, kubeconfig is never even looked at — proves
    /// the gate fires BEFORE any I/O.
    #[tokio::test]
    async fn rejection_happens_before_kubeconfig_lookup() {
        let (_tmp, writer) = setup();
        // No kubeconfig file exists in tmp; if the writer tried to
        // resolve it before checking authority we'd see an Io/Invalid
        // error. We get AuthorityRequired instead — proof the gate
        // is the first thing.
        let err = writer
            .apply_resource(
                "no-such-cluster",
                ResourceKind::Pod,
                "default",
                "name",
                serde_json::json!({}),
                "engenho-mcp",
                false,
                &Authority::Placeholder,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "authority_required");
    }
}
