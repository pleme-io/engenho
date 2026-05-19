//! `Informer` — cached state-sync for a single resource type.
//!
//! The Kubernetes pattern: list once + watch from then on, maintain a
//! local cache, hand callbacks Add/Update/Delete events derived from
//! cache state transitions (not raw watch events — the informer
//! handles reordering + relist deduplication).
//!
//! This trait is the substrate every controller's reconcile loop
//! reads from. Concrete impls live in `engenho-kube-client`
//! (SharedInformerImpl using ReqwestWatcher) and the future
//! `engenho-controller-runtime`.

use crate::error::KubeError;
use crate::kind::KubeResource;
use std::borrow::Cow;

/// One callback for the informer's event stream.
///
/// Implementors register themselves with [`Informer::add_event_handler`]
/// and receive callbacks for every cache-state transition.
#[async_trait::async_trait]
pub trait EventHandler<R: KubeResource>: Send + Sync {
    /// A new object entered the cache. Called on initial list +
    /// subsequent Added events.
    async fn on_add(&self, obj: &R);
    /// An existing cache entry was updated.
    async fn on_update(&self, old: &R, new: &R);
    /// A cache entry was deleted (either by a DELETED event or by
    /// a `final`-state relist that observed its absence).
    async fn on_delete(&self, obj: &R);
}

/// Local read-through cache of one resource type from the apiserver.
///
/// Snapshot-consistency is at the granularity of "last completed
/// list+watch cycle." A long-running informer maintains an
/// eventually-consistent view; callers needing stronger guarantees
/// should `KubeClient::get` directly.
pub trait Cache<R: KubeResource>: Send + Sync {
    /// Look up a resource by namespaced key. Cluster-scoped resources
    /// pass `namespace = None`.
    fn get(&self, namespace: Option<&str>, name: &str) -> Option<R>;

    /// List all cached objects. Implementations should return a stable
    /// order (typically the apiserver's listing order, preserved through
    /// the local cache's ordering structure).
    fn list(&self) -> Vec<R>;

    /// Best-effort estimate of cache size. May be inaccurate during
    /// a relist; callers should treat it as a hint, not a hard count.
    fn len(&self) -> usize;

    /// Convenience: `len() == 0`.
    fn is_empty(&self) -> bool { self.len() == 0 }
}

/// The full informer: cache + event multiplexing + watch lifecycle.
#[async_trait::async_trait]
pub trait Informer<R: KubeResource>: Cache<R> {
    /// Register an event handler. The handler receives all subsequent
    /// transitions; it does NOT replay history (callers needing
    /// initial-state visibility should call `list` first then register).
    async fn add_event_handler(&self, handler: Box<dyn EventHandler<R>>);

    /// Block until the informer has synced its initial list from the
    /// apiserver. Returns `Ok(())` on success, `Err(KubeError)` if the
    /// initial list failed (transient errors retry automatically).
    ///
    /// # Errors
    ///
    /// Forwards [`KubeError::Network`] / [`KubeError::Auth`] / etc.
    /// from the underlying client.
    async fn wait_for_initial_sync(&self) -> Result<(), KubeError>;

    /// Stop the informer's watch loop + flush state. After this, the
    /// cache is no longer maintained; further reads observe the
    /// last-known state.
    async fn shutdown(&self);
}

/// Identifier for a cache entry. Borrowed for cheap key construction
/// in the cache's internal storage; owned variant for trait-object
/// boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey<'a> {
    /// `metadata.namespace`, or empty for cluster-scoped.
    pub namespace: Cow<'a, str>,
    /// `metadata.name`.
    pub name:      Cow<'a, str>,
}

impl<'a> CacheKey<'a> {
    /// Build a key from a resource. Namespaced or cluster-scoped is
    /// inferred from `R::SCOPE`.
    #[must_use]
    pub fn from_resource<R: KubeResource>(r: &'a R) -> Self {
        Self {
            namespace: r.namespace().unwrap_or(Cow::Borrowed("")),
            name:      r.name(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_equality() {
        let a = CacheKey { namespace: "default".into(), name: "p1".into() };
        let b = CacheKey { namespace: "default".into(), name: "p1".into() };
        assert_eq!(a, b);
    }
}
