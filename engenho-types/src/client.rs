//! `KubeClient` — the typed CRUD + watch surface against a kube apiserver.
//!
//! This trait is what every controller, scheduler, kubelet, and
//! operator binary in pleme-io speaks to talk to a cluster (engenho's
//! own apiserver eventually; today the k3s apiserver via the bridge).
//!
//! The shape is intentionally narrow — every op is generic over
//! `R: KubeResource` and returns `Result<R, KubeError>`. Patch + watch
//! diverge slightly to capture their wire-protocol particulars.
//!
//! The default impl in `engenho-kube-client` wraps reqwest + rustls;
//! tests can substitute a `MockKubeClient` (in-memory, deterministic).

use crate::error::KubeError;
use crate::kind::KubeResource;
use crate::patch::Patch;
use crate::watch::Watcher;

/// CRUD + watch operations on Kubernetes resources.
///
/// Every method is async-fn-with-trait (via `async_trait`) for
/// dyn-compat. Implementors must be `Send + Sync` so controllers
/// can clone them across tokio tasks.
#[async_trait::async_trait]
pub trait KubeClient: Send + Sync {
    /// GET a single resource by name.
    ///
    /// `namespace` is required for namespaced kinds + ignored for
    /// cluster-scoped kinds — the implementation derives the URL from
    /// `R::SCOPE` so callers don't have to branch.
    ///
    /// # Errors
    ///
    /// [`KubeError::NotFound`] if the resource doesn't exist;
    /// [`KubeError::ApiStatus`] for other apiserver responses;
    /// [`KubeError::Network`] for connection failures.
    async fn get<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<R, KubeError>;

    /// LIST resources, optionally filtered by selectors.
    ///
    /// Returns a [`List<R>`] with the items + the resourceVersion the
    /// caller should use to start a watch from.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::get`].
    async fn list<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        opts: &ListOptions,
    ) -> Result<List<R>, KubeError>;

    /// CREATE a new resource. The apiserver assigns `resourceVersion`,
    /// `uid`, and timestamps; the returned `R` carries them.
    ///
    /// # Errors
    ///
    /// [`KubeError::ApiStatus`] with [`ApiStatusKind::AlreadyExists`]
    /// if a same-named object already exists. Conflict, Invalid,
    /// Forbidden surface the same way.
    ///
    /// [`ApiStatusKind::AlreadyExists`]: crate::error::ApiStatusKind::AlreadyExists
    async fn create<R: KubeResource + Send + Sync + 'static>(&self, resource: &R) -> Result<R, KubeError>;

    /// REPLACE (PUT) an entire resource — must include current
    /// `resourceVersion` for optimistic concurrency.
    ///
    /// Prefer [`Self::patch`] over `replace` when you only know SOME
    /// fields — patch is the upstream-recommended path.
    ///
    /// # Errors
    ///
    /// [`KubeError::ApiStatus`] with `Conflict` on resourceVersion
    /// mismatch.
    async fn replace<R: KubeResource + Send + Sync + 'static>(&self, resource: &R) -> Result<R, KubeError>;

    /// PATCH an existing resource. The patch type is encoded in
    /// [`Patch`].
    ///
    /// Server-side-apply ([`Patch::Apply`]) is the upstream-recommended
    /// path for declarative reconciliation — it does field-ownership
    /// tracking + 3-way merge with the apiserver.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::get`]; SSA can also fail with
    /// `Conflict` carrying a typed FieldManagerConflict body.
    async fn patch<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: &Patch,
    ) -> Result<R, KubeError>;

    /// DELETE a resource by name. Returns Ok(()) on success (no
    /// content; the apiserver may stream the deleted object back but
    /// we don't bother).
    ///
    /// # Errors
    ///
    /// [`KubeError::NotFound`] is intentionally exposed — many
    /// controllers reconcile by attempting to delete + treating
    /// NotFound as success ("already gone").
    async fn delete<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        opts: &DeleteOptions,
    ) -> Result<(), KubeError>;

    /// Hand back the typed [`Watcher`] for `R`. The watcher itself
    /// is what drives a streaming watch loop; this method just
    /// constructs it bound to this client's connection state.
    fn watcher<R: KubeResource + Send + Sync + 'static>(&self) -> Box<dyn Watcher<R>>;
}

/// Result of a LIST operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct List<R> {
    /// The items the apiserver returned in this page.
    pub items: Vec<R>,
    /// `metadata.resourceVersion` of the LIST response — the
    /// safe starting point for a subsequent watch.
    pub resource_version: String,
    /// Token to fetch the next page when pagination is in effect.
    /// `None` ⇒ this is the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continue_token: Option<String>,
}

/// Selectors + pagination parameters for `KubeClient::list`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ListOptions {
    /// Label selector (`metadata.labels` match expression). Empty ⇒ all.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub label_selector: String,
    /// Field selector (`metadata.name=foo`, `metadata.namespace=bar`, …).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field_selector: String,
    /// Max items per page. `None` ⇒ apiserver default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Resume from the previous page's `continue_token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continue_token: Option<String>,
    /// Watch from this rv when listing as part of a watch+list.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resource_version: String,
}

/// Options for `KubeClient::delete`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DeleteOptions {
    /// Grace period before terminating the resource. `0` ⇒ immediate.
    /// `None` ⇒ apiserver default (often 30s for Pods).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<u64>,
    /// `Foreground` ⇒ wait for dependents; `Background` ⇒ orphan;
    /// `Orphan` ⇒ explicitly keep dependents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub propagation_policy: Option<PropagationPolicy>,
    /// Optimistic-concurrency guard. If set, the delete only proceeds
    /// when the live object's `resourceVersion` matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<DeletePreconditions>,
}

/// Cascading-delete policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PropagationPolicy {
    /// Wait for dependent objects to be deleted first.
    Foreground,
    /// Delete the target immediately; dependents are reaped async.
    Background,
    /// Don't delete dependents — they become orphans.
    Orphan,
}

/// Optimistic-concurrency preconditions for a delete.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeletePreconditions {
    /// Only delete when the live object's `resourceVersion` matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// Only delete when the live object's `uid` matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}
