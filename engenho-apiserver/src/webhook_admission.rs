//! Apiserver-side wiring for dynamic mutating admission webhooks.
//!
//! The pure, mockable core (config parsing, rule matching, AdmissionReview
//! build, base64-JSONPatch apply, the `MutatingWebhookPlugin`) lives in
//! [`engenho_controllers::admission_webhook`]. THIS module supplies the two
//! production seams that core abstracts:
//!
//!   * [`StoreWebhookConfigSource`] — lists the live
//!     `MutatingWebhookConfiguration` objects out of the [`StoreMesh`].
//!   * [`ReqwestWebhookCaller`] — POSTs the `AdmissionReview` JSON to a webhook
//!     endpoint over HTTPS via reqwest.
//!   * [`catalog_pluralizer`] — the `(group, version, kind) -> plural`
//!     resolver, sourced from the generated `RESOURCE_CATALOG` (the single
//!     pluralization source of truth).
//!
//! Wire a `MutatingWebhookPlugin` into the apiserver via
//! [`mutating_webhook_plugin`] + add it to the [`engenho_controllers::AdmissionChain`]
//! the cataloged handlers carry.
//!
//! ## caBundle TLS verification — DEFERRED, surfaced not silent
//!
//! [`ReqwestWebhookCaller`] honors the per-webhook `timeoutSeconds`. The
//! `clientConfig.caBundle` (the webhook's serving-cert CA) is THREADED to this
//! caller by the core plugin but FULL caBundle verification is a typed
//! follow-up: the default client uses the platform/webpki roots. When a
//! `caBundle` is supplied AND the operator hasn't enabled the default-allow
//! escape hatch, the caller returns a typed error rather than silently
//! trusting an unverified endpoint — the security control is never dropped
//! without an explicit, logged decision (see [`ReqwestWebhookCaller::new`]).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use engenho_controllers::MutatingWebhookPlugin;
use engenho_controllers::admission_webhook::{
    Pluralizer, StoreServiceResolver, WebhookCaller, WebhookConfigSource,
};
use engenho_store::StoreMesh;
use engenho_types::generated_v1_34::RESOURCE_CATALOG;

/// `MutatingWebhookConfiguration` GVK — cluster-scoped, opaque JSON.
const MWC_GROUP: &str = "admissionregistration.k8s.io";
const MWC_VERSION: &str = "v1";
const MWC_KIND: &str = "MutatingWebhookConfiguration";
/// `ValidatingWebhookConfiguration` — same group/version, different kind.
/// It has always been in the catalog and therefore storable; nothing read
/// it, so a cluster could hold a policy configuration that was never
/// consulted and reported no error.
const VWC_KIND: &str = "ValidatingWebhookConfiguration";

/// Store-backed [`WebhookConfigSource`]: lists every live
/// `MutatingWebhookConfiguration` (cluster-scoped) from the mesh on each
/// review. Cheap — the configs are a small, slow-changing set; reading them
/// per-request keeps the plugin stateless + drift-free (a freshly-applied
/// config takes effect on the next admission with no cache to invalidate).
pub struct StoreWebhookConfigSource {
    store: Arc<StoreMesh>,
}

impl StoreWebhookConfigSource {
    /// New source over `store`.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl WebhookConfigSource for StoreWebhookConfigSource {
    async fn mutating_configs(&self) -> Vec<Value> {
        self.store
            .list(MWC_GROUP, MWC_VERSION, MWC_KIND, None)
            .await
            .into_iter()
            .map(|(_k, v)| v)
            .collect()
    }

    async fn validating_configs(&self) -> Vec<Value> {
        self.store
            .list(MWC_GROUP, MWC_VERSION, VWC_KIND, None)
            .await
            .into_iter()
            .map(|(_k, v)| v)
            .collect()
    }
}

/// reqwest-backed [`WebhookCaller`]: POST the `AdmissionReview` JSON to the
/// webhook over HTTPS, honoring the per-call timeout.
pub struct ReqwestWebhookCaller {
    client: reqwest::Client,
    /// When `true`, a webhook declaring a `caBundle` is called anyway against
    /// the default trust roots (caBundle verification is the typed follow-up).
    /// When `false` (the safe default), a `caBundle`-bearing webhook returns a
    /// typed error rather than being called against unverified roots.
    allow_unverified_ca_bundle: bool,
}

impl Default for ReqwestWebhookCaller {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ReqwestWebhookCaller {
    /// New caller.
    ///
    /// `allow_unverified_ca_bundle` is the EXPLICIT operator decision for the
    /// deferred caBundle control: `false` ⇒ refuse to call a `caBundle`-bearing
    /// webhook (fail-safe; the webhook's `failurePolicy` then governs); `true`
    /// ⇒ call it against the default trust roots (the caBundle is NOT yet
    /// verified — a documented, logged interim).
    #[must_use]
    pub fn new(allow_unverified_ca_bundle: bool) -> Self {
        Self {
            client: reqwest::Client::new(),
            allow_unverified_ca_bundle,
        }
    }
}

#[async_trait]
impl WebhookCaller for ReqwestWebhookCaller {
    async fn call(
        &self,
        endpoint: &str,
        ca_bundle: Option<&str>,
        timeout_seconds: u32,
        review_request: &Value,
    ) -> Result<Value, String> {
        // caBundle verification is DEFERRED. Rather than silently trust an
        // unverified endpoint, refuse unless the operator opted in.
        if ca_bundle.is_some() && !self.allow_unverified_ca_bundle {
            return Err(
                "webhook declares clientConfig.caBundle, but caBundle TLS verification is not \
                 yet implemented; set allow_unverified_ca_bundle to call against default trust \
                 roots (DEFERRED security control — never silently skipped)"
                    .to_string(),
            );
        }
        let resp = self
            .client
            .post(endpoint)
            .timeout(Duration::from_secs(u64::from(timeout_seconds.max(1))))
            .json(review_request)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("webhook returned HTTP {}", resp.status()));
        }
        resp.json::<Value>().await.map_err(|e| e.to_string())
    }
}

/// The `(group, version, kind) -> plural` resolver sourced from the generated
/// `RESOURCE_CATALOG` — the single pluralization source of truth (no `+s`
/// derivation). An uncataloged kind yields `None` (the plugin then admits it
/// unchanged — a webhook can't rule-match a kind the apiserver doesn't serve).
#[must_use]
pub fn catalog_pluralizer() -> Pluralizer {
    Box::new(|group, version, kind| {
        RESOURCE_CATALOG
            .iter()
            .find(|d| d.group == group && d.version == version && d.kind == kind)
            .map(|d| d.plural.to_string())
    })
}

/// Build a production [`MutatingWebhookPlugin`] wired to `store` (config
/// source + service resolver) + a reqwest caller + the catalog pluralizer.
/// The [`StoreServiceResolver`] resolves `clientConfig.service` refs to the
/// Service's allocated ClusterIP (the ClusterIP allocator stamps it), so a
/// mesh injector declared by `service` (not `url`) becomes callable. Add the
/// returned `Arc<dyn AdmissionWebhook>` to the apiserver's `AdmissionChain`.
#[must_use]
pub fn mutating_webhook_plugin(
    store: Arc<StoreMesh>,
    allow_unverified_ca_bundle: bool,
) -> MutatingWebhookPlugin {
    MutatingWebhookPlugin::new(
        Arc::new(StoreWebhookConfigSource::new(store.clone())),
        Arc::new(ReqwestWebhookCaller::new(allow_unverified_ca_bundle)),
        catalog_pluralizer(),
    )
    .with_resolver(Arc::new(StoreServiceResolver::new(store)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pluralizer_uses_catalog() {
        let p = catalog_pluralizer();
        assert_eq!(p("", "v1", "Pod").as_deref(), Some("pods"));
        assert_eq!(
            p("apps", "v1", "Deployment").as_deref(),
            Some("deployments")
        );
        // Irregular plural is curated, not `+s`.
        assert_eq!(p("", "v1", "Endpoints").as_deref(), Some("endpoints"));
        // The new admissionregistration kinds are cataloged.
        assert_eq!(
            p(MWC_GROUP, MWC_VERSION, MWC_KIND).as_deref(),
            Some("mutatingwebhookconfigurations")
        );
        // Uncataloged kind → None.
        assert_eq!(p("nope.io", "v1", "Nope"), None);
    }

    #[tokio::test]
    async fn caller_refuses_ca_bundle_without_opt_in() {
        let caller = ReqwestWebhookCaller::new(false);
        let err = caller
            .call("https://x/y", Some("Zm9v"), 5, &serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            err.contains("caBundle"),
            "caBundle refusal is surfaced: {err}"
        );
        assert!(err.contains("never silently skipped"));
    }
}
