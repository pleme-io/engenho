//! Per-kind CRUD trait.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};

use crate::error::ApiError;

/// Typed K8s-resource CRUD trait. Each registered kind implements
/// this; the router dispatches REST routes to the trait methods.
///
/// Default impl: [`StoreBackedHandler`] — works for any kind by
/// routing through the opaque-JSON [`StoreMesh`] catalog.
#[async_trait]
pub trait ResourceHandler: Send + Sync + 'static {
    fn group(&self) -> &str;
    fn version(&self) -> &str;
    fn kind(&self) -> &str;
    fn plural(&self) -> &str;
    fn namespaced(&self) -> bool;

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError>;

    async fn list(&self, namespace: Option<&str>) -> Result<Value, ApiError>;

    async fn create(&self, namespace: Option<&str>, body: Value) -> Result<Value, ApiError>;

    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<Value, ApiError>;

    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<(), ApiError>;
}

/// Default implementation backed by [`StoreMesh`]. Handles every
/// kind uniformly — the kind-specific intelligence (defaulters,
/// validators, finalizers) is left to controllers + admission
/// webhooks at R8+.
pub struct StoreBackedHandler {
    group: String,
    version: String,
    kind: String,
    plural: String,
    namespaced: bool,
    store: Arc<StoreMesh>,
}

impl StoreBackedHandler {
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        plural: impl Into<String>,
        namespaced: bool,
    ) -> Self {
        Self {
            store,
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
            plural: plural.into(),
            namespaced,
        }
    }

    /// Construct from a known K8s kind. The plural is derived from
    /// the kind by lowercasing + appending 's' — this matches the
    /// upstream K8s resource pluralization for the kinds we ship at R7.
    #[must_use]
    pub fn for_core_kind(store: Arc<StoreMesh>, kind: &str, namespaced: bool) -> Self {
        let plural = format!("{}s", kind.to_lowercase());
        Self::new(store, "", "v1", kind, plural, namespaced)
    }

    fn key(&self, namespace: Option<&str>, name: &str) -> Result<ResourceKey, ApiError> {
        if self.namespaced != namespace.is_some() {
            return Err(ApiError::BadRequest(format!(
                "{}/{} is {}; got namespace={:?}",
                self.kind,
                name,
                if self.namespaced {
                    "namespaced"
                } else {
                    "cluster-scoped"
                },
                namespace
            )));
        }
        Ok(match namespace {
            Some(ns) => ResourceKey::namespaced(&self.group, &self.version, &self.kind, ns, name),
            None => ResourceKey::cluster_scoped(&self.group, &self.version, &self.kind, name),
        })
    }
}

#[async_trait]
impl ResourceHandler for StoreBackedHandler {
    fn group(&self) -> &str {
        &self.group
    }
    fn version(&self) -> &str {
        &self.version
    }
    fn kind(&self) -> &str {
        &self.kind
    }
    fn plural(&self) -> &str {
        &self.plural
    }
    fn namespaced(&self) -> bool {
        self.namespaced
    }

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let v = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::NotFound(format!("{}/{}", self.kind, name)))?;
        Ok(inject_type_meta(&v, self.api_version(), &self.kind))
    }

    async fn list(&self, namespace: Option<&str>) -> Result<Value, ApiError> {
        if self.namespaced && namespace.is_none() {
            // List across all namespaces — common kubectl pattern
            // for kubectl get pods --all-namespaces.
        }
        let entries = self
            .store
            .list(&self.group, &self.version, &self.kind, namespace)
            .await;
        let items: Vec<Value> = entries
            .into_iter()
            .map(|(_, v)| inject_type_meta(&v, self.api_version(), &self.kind))
            .collect();
        Ok(serde_json::json!({
            "kind": format!("{}List", self.kind),
            "apiVersion": self.api_version(),
            "items": items,
            "metadata": { "resourceVersion": self.store.current_catalog().await.last_applied_index.to_string() },
        }))
    }

    async fn create(&self, namespace: Option<&str>, body: Value) -> Result<Value, ApiError> {
        let name = body
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| ApiError::BadRequest("missing metadata.name in request body".into()))?
            .to_string();
        let key = self.key(namespace, &name)?;
        // Reject if already exists (POST semantics).
        if self.store.get(&key).await.is_some() {
            return Err(ApiError::Conflict(
                format!("{}/{}", self.kind, name),
                "resource already exists".into(),
            ));
        }
        let result = self
            .store
            .propose(ResourceCommand::Put {
                key: key.clone(),
                value: body,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        // Read back the committed resource (with resourceVersion).
        let _ = result;
        let stored = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::Internal("created but not readable".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }

    async fn patch(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        if self.store.get(&key).await.is_none() {
            return Err(ApiError::NotFound(format!("{}/{}", self.kind, name)));
        }
        self.store
            .propose(ResourceCommand::Patch {
                key: key.clone(),
                patch,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        let stored = self
            .store
            .get(&key)
            .await
            .ok_or_else(|| ApiError::Internal("patch lost during commit".into()))?;
        Ok(inject_type_meta(&stored, self.api_version(), &self.kind))
    }

    async fn delete(&self, namespace: Option<&str>, name: &str) -> Result<(), ApiError> {
        let key = self.key(namespace, name)?;
        self.store
            .propose(ResourceCommand::Delete {
                key,
                reason: Reason::Operator,
            })
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        Ok(())
    }
}

impl StoreBackedHandler {
    fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
}

/// Add `kind` + `apiVersion` to a resource if missing. Matches
/// what kubectl expects in single-resource GET responses.
fn inject_type_meta(v: &Value, api_version: String, kind: &str) -> Value {
    let mut out = v.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.entry("kind".to_string())
            .or_insert_with(|| Value::String(kind.to_string()));
        obj.entry("apiVersion".to_string())
            .or_insert_with(|| Value::String(api_version));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Constructor coverage is exercised by the integration tests in
    // tests/r7_http_k8s_api.rs — they build a real StoreMesh + verify
    // each handler method end-to-end. Zero-cost mocking the StoreMesh
    // here would require introducing a trait for it; the integration
    // path is more honest.

    #[test]
    fn inject_type_meta_adds_missing_fields() {
        let v = serde_json::json!({"metadata": {"name": "x"}});
        let out = inject_type_meta(&v, "v1".into(), "Pod");
        assert_eq!(out.get("kind").unwrap(), "Pod");
        assert_eq!(out.get("apiVersion").unwrap(), "v1");
    }

    #[test]
    fn inject_type_meta_preserves_existing_fields() {
        let v = serde_json::json!({"kind": "Pod", "apiVersion": "v1"});
        let out = inject_type_meta(&v, "v99".into(), "WrongKind");
        // Existing kind / apiVersion survive.
        assert_eq!(out.get("kind").unwrap(), "Pod");
        assert_eq!(out.get("apiVersion").unwrap(), "v1");
    }
}
