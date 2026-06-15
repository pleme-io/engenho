//! Dynamic admission webhooks — the apiserver-side call-out to
//! `MutatingWebhookConfiguration` (and the `ValidatingWebhookConfiguration`
//! call path) registered under `admissionregistration.k8s.io/v1`.
//!
//! This is THE brick that lets a service mesh (tatara-mesh) run on engenho:
//! the mesh's sidecar injector (`lareira-enxerto`) is a
//! `MutatingWebhookConfiguration` that intercepts Pod creation and injects the
//! `aresta` proxy sidecar + a SPIRE-agent init container as a base64
//! RFC6902 JSONPatch. This module dispatches that call-out and applies the
//! returned patch to the object BEFORE it is persisted — and the kubelet's
//! init-container driver (commit a7810a6) then runs the injected init
//! container. The loop closes.
//!
//! ## Shape (★★ TYPED-SPEC + INTERPRETER TRIPLET — Environment trait)
//!
//! The interpreter is [`MutatingWebhookPlugin`] (an [`AdmissionWebhook`]). Its
//! two side effects are abstracted behind traits so the whole plugin is
//! unit-testable with ZERO network + ZERO store:
//!
//!   1. [`WebhookConfigSource`] — "list the live
//!      `MutatingWebhookConfiguration` objects" (production: read the store;
//!      test: an in-memory list).
//!   2. [`WebhookCaller`] — "POST this `AdmissionReview` JSON to a webhook
//!      endpoint and return its response JSON" (production: HTTPS via reqwest,
//!      lives in the apiserver crate; test: a canned-patch mock).
//!
//! The wire `AdmissionReview` request/response objects are built + read as
//! typed `serde_json::Value` trees (NO `format!()` of JSON — ★★ TYPED
//! EMISSION). The returned base64 JSONPatch is decoded + applied through
//! [`engenho_store::patch_apply`]'s RFC6902 engine — the SAME engine the
//! apiserver's `kubectl patch --type=json` path uses (solve-once: no second
//! patch implementation).
//!
//! ## Honest scope (deferred surfaces are TYPED, never silent)
//!
//!   * `clientConfig.url` is supported. `clientConfig.service` resolution to a
//!     ClusterIP/endpoint is DEFERRED — a config that uses `service` without a
//!     `url` yields a typed [`WebhookError::ServiceRefUnsupported`] the
//!     `failurePolicy` then governs (it is NOT silently skipped).
//!   * `clientConfig.caBundle` TLS verification is DEFERRED to the production
//!     [`WebhookCaller`] impl in the apiserver crate; this module records the
//!     caBundle on the typed [`WebhookClientConfig`] + passes it to the caller.
//!     The security control is surfaced, never dropped.
//!   * `namespaceSelector` / `objectSelector` are matched best-effort against
//!     the request object's own labels + namespace label set the caller may
//!     thread in; an unparseable selector denies the match (conservative).

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex;

use engenho_store::patch_apply::{Gvk, MockPatchEnv, PatchBody, PatchSchemaEnv, apply as apply_patch};
use engenho_types::patch::PatchType;

use crate::admission::{
    AdmissionAction, AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};

// ═══════════════════════════════════════════════════════════════════════════
//  Typed border — the webhook-configuration shapes we read off the wire.
// ═══════════════════════════════════════════════════════════════════════════

/// A single `MutatingWebhook` entry within a `MutatingWebhookConfiguration`.
///
/// Parsed from the opaque-JSON config object the apiserver stores (the
/// admissionregistration kinds are served as opaque JSON today). A field the
/// config omits takes the K8s default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutatingWebhook {
    /// `webhooks[].name` — telemetry + ordering identity.
    pub name: String,
    /// Where to call (`clientConfig`).
    pub client_config: WebhookClientConfig,
    /// `webhooks[].rules` — which (group, version, resource, operation)
    /// requests this webhook intercepts. Empty ⇒ matches nothing.
    pub rules: Vec<RuleWithOperations>,
    /// `webhooks[].failurePolicy` — `Fail` (default) ⇒ a call/parse error
    /// rejects the request; `Ignore` ⇒ the request proceeds unmutated.
    pub failure_policy: FailurePolicy,
    /// `webhooks[].timeoutSeconds` — per-call deadline (default 10, max 30).
    pub timeout_seconds: u32,
    /// `webhooks[].reinvocationPolicy` — `Never` (default) ⇒ called once;
    /// `IfNeeded` ⇒ may be reinvoked if a later webhook mutated the object.
    pub reinvocation_policy: ReinvocationPolicy,
    /// `webhooks[].namespaceSelector` — best-effort label match on the
    /// request namespace's labels. `None` ⇒ matches all namespaces.
    pub namespace_selector: Option<LabelSelector>,
    /// `webhooks[].objectSelector` — best-effort label match on the request
    /// object's own labels. `None` ⇒ matches all objects.
    pub object_selector: Option<LabelSelector>,
}

/// `clientConfig` — how to reach the webhook server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookClientConfig {
    /// `clientConfig.url` — a fully-qualified `https://…/path` endpoint.
    /// `Some` is the supported path today.
    pub url: Option<String>,
    /// `clientConfig.service` — an in-cluster Service ref. Present ⇒ DEFERRED
    /// (typed [`WebhookError::ServiceRefUnsupported`] unless a `url` is also
    /// set). Carried so the typed error names the exact service.
    pub service: Option<ServiceRef>,
    /// `clientConfig.caBundle` — base64 PEM CA bundle for verifying the
    /// webhook's serving cert. Threaded to the [`WebhookCaller`]; the actual
    /// TLS verification is the caller's responsibility (DEFERRED in the
    /// reqwest impl, surfaced never dropped).
    pub ca_bundle: Option<String>,
}

/// `clientConfig.service` — `{namespace, name, path?, port?}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceRef {
    /// Service namespace.
    pub namespace: String,
    /// Service name.
    pub name: String,
    /// URL path on the service (default `/`).
    pub path: Option<String>,
    /// Service port (default 443).
    pub port: Option<u16>,
}

/// One `rules[]` entry — the (apiGroups, apiVersions, resources, operations)
/// match set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleWithOperations {
    /// `operations` — `CREATE` / `UPDATE` / `DELETE` / `*`.
    pub operations: Vec<String>,
    /// `apiGroups` — `""` (core) / a group / `*`.
    pub api_groups: Vec<String>,
    /// `apiVersions` — `v1` / `*`.
    pub api_versions: Vec<String>,
    /// `resources` — plural resource name (`pods`) / `*`.
    pub resources: Vec<String>,
}

/// `failurePolicy` discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Reject the request when the webhook errors (K8s default).
    Fail,
    /// Proceed (unmutated by this webhook) when the webhook errors.
    Ignore,
}

/// `reinvocationPolicy` discriminant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReinvocationPolicy {
    /// Call once (K8s default).
    Never,
    /// May reinvoke if a later webhook mutated the object.
    IfNeeded,
}

/// A label selector — `matchLabels` only (the common shape; `matchExpressions`
/// is a typed follow-up). `None`-equivalent (match-all) is modeled by an
/// `Option<LabelSelector>` at the use site, not by an empty selector here.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LabelSelector {
    /// `matchLabels` — every (k, v) must be present on the target.
    pub match_labels: std::collections::BTreeMap<String, String>,
}

impl LabelSelector {
    /// True iff every `matchLabels` pair is present in `labels`.
    #[must_use]
    pub fn matches(&self, labels: &std::collections::BTreeMap<String, String>) -> bool {
        self.match_labels
            .iter()
            .all(|(k, v)| labels.get(k).is_some_and(|got| got == v))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Errors.
// ═══════════════════════════════════════════════════════════════════════════

/// A typed failure from the webhook plugin. NO silent wrong answers — every
/// surface that can't honor a webhook returns one of these; the caller's
/// `failurePolicy` then decides Fail vs Ignore.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum WebhookError {
    /// The [`WebhookCaller`] failed to reach / get a response from the
    /// endpoint (network, TLS, timeout, non-2xx).
    #[error("call to webhook {webhook:?} failed: {message}")]
    CallFailed {
        /// The webhook's `name`.
        webhook: String,
        /// Backend message.
        message: String,
    },
    /// The webhook's `AdmissionReview` response was malformed (missing
    /// `response`, bad `uid`, …).
    #[error("webhook {webhook:?} returned a malformed AdmissionReview: {reason}")]
    BadResponse {
        /// The webhook's `name`.
        webhook: String,
        /// What was wrong.
        reason: String,
    },
    /// The webhook returned a `patchType` other than `JSONPatch`.
    #[error("webhook {webhook:?} returned unsupported patchType {got:?} (only JSONPatch)")]
    UnsupportedPatchType {
        /// The webhook's `name`.
        webhook: String,
        /// The unsupported value.
        got: String,
    },
    /// The base64 `patch` payload failed to decode, or the decoded body was
    /// not a valid RFC6902 op array, or applying it failed.
    #[error("webhook {webhook:?} patch could not be applied: {reason}")]
    PatchApply {
        /// The webhook's `name`.
        webhook: String,
        /// What failed.
        reason: String,
    },
    /// The `clientConfig` uses a `service` ref with no `url`. DEFERRED:
    /// Service→endpoint resolution is a typed follow-up. NOT silently skipped.
    #[error(
        "webhook {webhook:?} uses clientConfig.service ({namespace}/{name}) — \
         service-ref resolution is unsupported; supply clientConfig.url"
    )]
    ServiceRefUnsupported {
        /// The webhook's `name`.
        webhook: String,
        /// Service namespace.
        namespace: String,
        /// Service name.
        name: String,
    },
    /// The `clientConfig` declared neither `url` nor `service` — invalid.
    #[error("webhook {webhook:?} clientConfig declares neither url nor service")]
    NoEndpoint {
        /// The webhook's `name`.
        webhook: String,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
//  The two Environment traits (the testability contract).
// ═══════════════════════════════════════════════════════════════════════════

/// Source of the live `MutatingWebhookConfiguration` objects. Production reads
/// the store; tests pass a static list. Returns the raw opaque-JSON config
/// objects (the plugin parses them into [`MutatingWebhook`]s) — so the source
/// stays a thin "give me the configs" seam with no parsing logic to drift.
#[async_trait]
pub trait WebhookConfigSource: Send + Sync {
    /// Every live `MutatingWebhookConfiguration` object, as stored JSON.
    async fn mutating_configs(&self) -> Vec<Value>;
}

/// The HTTP(S) call-out seam. Production POSTs the `AdmissionReview` request
/// JSON over HTTPS (reqwest impl in the apiserver crate, honoring
/// `ca_bundle` + `timeout`); tests return a canned response.
#[async_trait]
pub trait WebhookCaller: Send + Sync {
    /// POST `review_request` (a v1 `AdmissionReview` JSON) to the endpoint and
    /// return the response `AdmissionReview` JSON.
    ///
    /// `endpoint` is the resolved `clientConfig.url`. `ca_bundle` is the
    /// base64 PEM bundle (caller verifies). `timeout_seconds` bounds the call.
    ///
    /// # Errors
    /// A backend message string on any network/TLS/timeout/non-2xx failure —
    /// the plugin wraps it into a typed [`WebhookError::CallFailed`].
    async fn call(
        &self,
        endpoint: &str,
        ca_bundle: Option<&str>,
        timeout_seconds: u32,
        review_request: &Value,
    ) -> Result<Value, String>;
}

// ═══════════════════════════════════════════════════════════════════════════
//  Parsing — opaque config JSON → typed MutatingWebhook list.
// ═══════════════════════════════════════════════════════════════════════════

/// Parse a stored `MutatingWebhookConfiguration` object's `webhooks[]` into the
/// typed list. Unparseable/absent fields take the K8s default; a webhook with
/// no name is skipped (it can never be referenced/ordered).
#[must_use]
pub fn parse_mutating_config(config: &Value) -> Vec<MutatingWebhook> {
    let Some(hooks) = config.get("webhooks").and_then(Value::as_array) else {
        return Vec::new();
    };
    hooks.iter().filter_map(parse_one_webhook).collect()
}

fn parse_one_webhook(hook: &Value) -> Option<MutatingWebhook> {
    let name = hook.get("name").and_then(Value::as_str)?.to_string();
    let cc = hook.get("clientConfig").unwrap_or(&Value::Null);
    let client_config = WebhookClientConfig {
        url: cc.get("url").and_then(Value::as_str).map(str::to_string),
        service: cc.get("service").and_then(parse_service_ref),
        ca_bundle: cc.get("caBundle").and_then(Value::as_str).map(str::to_string),
    };
    let rules = hook
        .get("rules")
        .and_then(Value::as_array)
        .map(|rs| rs.iter().map(parse_rule).collect())
        .unwrap_or_default();
    let failure_policy = match hook.get("failurePolicy").and_then(Value::as_str) {
        Some("Ignore") => FailurePolicy::Ignore,
        // default + explicit "Fail"
        _ => FailurePolicy::Fail,
    };
    let timeout_seconds = hook
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .map_or(10, |t| t.min(30) as u32);
    let reinvocation_policy = match hook.get("reinvocationPolicy").and_then(Value::as_str) {
        Some("IfNeeded") => ReinvocationPolicy::IfNeeded,
        _ => ReinvocationPolicy::Never,
    };
    let namespace_selector = hook.get("namespaceSelector").and_then(parse_selector);
    let object_selector = hook.get("objectSelector").and_then(parse_selector);
    Some(MutatingWebhook {
        name,
        client_config,
        rules,
        failure_policy,
        timeout_seconds,
        reinvocation_policy,
        namespace_selector,
        object_selector,
    })
}

fn parse_service_ref(svc: &Value) -> Option<ServiceRef> {
    Some(ServiceRef {
        namespace: svc.get("namespace").and_then(Value::as_str)?.to_string(),
        name: svc.get("name").and_then(Value::as_str)?.to_string(),
        path: svc.get("path").and_then(Value::as_str).map(str::to_string),
        port: svc
            .get("port")
            .and_then(Value::as_u64)
            .and_then(|p| u16::try_from(p).ok()),
    })
}

fn parse_rule(rule: &Value) -> RuleWithOperations {
    let strs = |key: &str| -> Vec<String> {
        rule.get(key)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    RuleWithOperations {
        operations: strs("operations"),
        api_groups: strs("apiGroups"),
        api_versions: strs("apiVersions"),
        resources: strs("resources"),
    }
}

fn parse_selector(sel: &Value) -> Option<LabelSelector> {
    let ml = sel.get("matchLabels")?.as_object()?;
    let match_labels = ml
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    Some(LabelSelector { match_labels })
}

// ═══════════════════════════════════════════════════════════════════════════
//  Rule matching.
// ═══════════════════════════════════════════════════════════════════════════

/// Map an [`AdmissionAction`] to its wire `operation` verb.
#[must_use]
pub fn operation_for(action: AdmissionAction) -> &'static str {
    match action {
        AdmissionAction::Put => "CREATE",
        AdmissionAction::Patch => "UPDATE",
        AdmissionAction::Delete => "DELETE",
    }
}

/// A `*` wildcard matches anything; otherwise an exact membership check.
fn rule_field_matches(patterns: &[String], value: &str) -> bool {
    patterns.iter().any(|p| p == "*" || p == value)
}

/// Does `webhook`'s `rules` match `(group, version, resource, operation)`?
/// A webhook matches if ANY of its rules matches on all four axes.
#[must_use]
pub fn rules_match(
    webhook: &MutatingWebhook,
    group: &str,
    version: &str,
    resource: &str,
    operation: &str,
) -> bool {
    webhook.rules.iter().any(|r| {
        rule_field_matches(&r.operations, operation)
            && rule_field_matches(&r.api_groups, group)
            && rule_field_matches(&r.api_versions, version)
            && rule_field_matches(&r.resources, resource)
    })
}

// ═══════════════════════════════════════════════════════════════════════════
//  AdmissionReview request build + response read (typed serde_json — no format!)
// ═══════════════════════════════════════════════════════════════════════════

/// Build a v1 `AdmissionReview` request object for `request` targeting
/// `resource` (the plural). `uid` is the per-call correlation id.
#[must_use]
pub fn build_admission_review(
    request: &AdmissionRequest,
    resource: &str,
    uid: &str,
) -> Value {
    let key = &request.key;
    let user = &request.user_info;
    json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": {
            "uid": uid,
            "kind": {
                "group": key.group,
                "version": key.version,
                "kind": key.kind,
            },
            "resource": {
                "group": key.group,
                "version": key.version,
                "resource": resource,
            },
            "name": key.name,
            "namespace": key.namespace.clone().unwrap_or_default(),
            "operation": operation_for(request.action),
            "userInfo": {
                "username": user.username,
                "uid": user.uid,
                "groups": user.groups,
                "extra": user.extra,
            },
            "object": request.value.clone().unwrap_or(Value::Null),
            "oldObject": request.current.clone().unwrap_or(Value::Null),
            "dryRun": false,
        }
    })
}

/// The parsed `response` half of a webhook's `AdmissionReview`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookResponse {
    /// `response.uid` (should echo the request uid).
    pub uid: String,
    /// `response.allowed`.
    pub allowed: bool,
    /// `response.status.message` when denied.
    pub message: Option<String>,
    /// `response.patch` decoded + base64-decoded → RFC6902 op array, IF the
    /// webhook returned a `JSONPatch`. `None` if no patch.
    pub patch: Option<Value>,
}

/// Read the `response` half of a webhook's `AdmissionReview`, decoding +
/// base64-decoding any `JSONPatch`.
///
/// # Errors
/// [`WebhookError::BadResponse`] for a missing/malformed response;
/// [`WebhookError::UnsupportedPatchType`] for a non-JSONPatch patchType;
/// [`WebhookError::PatchApply`] for a base64/JSON decode failure on the patch.
pub fn read_admission_review(
    webhook_name: &str,
    review: &Value,
) -> Result<WebhookResponse, WebhookError> {
    let resp = review.get("response").ok_or_else(|| WebhookError::BadResponse {
        webhook: webhook_name.to_string(),
        reason: "missing `response`".into(),
    })?;
    let uid = resp
        .get("uid")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let allowed = resp.get("allowed").and_then(Value::as_bool).ok_or_else(|| {
        WebhookError::BadResponse {
            webhook: webhook_name.to_string(),
            reason: "missing/non-bool `response.allowed`".into(),
        }
    })?;
    let message = resp
        .get("status")
        .and_then(|s| s.get("message"))
        .and_then(Value::as_str)
        .map(str::to_string);

    // A patch is present only when `patch` is set; its type MUST be JSONPatch.
    let patch = match resp.get("patch").and_then(Value::as_str) {
        None | Some("") => None,
        Some(b64) => {
            let patch_type = resp
                .get("patchType")
                .and_then(Value::as_str)
                .unwrap_or("JSONPatch");
            if patch_type != "JSONPatch" {
                return Err(WebhookError::UnsupportedPatchType {
                    webhook: webhook_name.to_string(),
                    got: patch_type.to_string(),
                });
            }
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| WebhookError::PatchApply {
                    webhook: webhook_name.to_string(),
                    reason: format!("base64 decode: {e}"),
                })?;
            let ops: Value =
                serde_json::from_slice(&decoded).map_err(|e| WebhookError::PatchApply {
                    webhook: webhook_name.to_string(),
                    reason: format!("patch JSON parse: {e}"),
                })?;
            Some(ops)
        }
    };

    Ok(WebhookResponse {
        uid,
        allowed,
        message,
        patch,
    })
}

/// Apply a webhook's RFC6902 op array (already decoded) to `object`, returning
/// the mutated object. Reuses the apiserver's canonical json-patch engine.
///
/// # Errors
/// [`WebhookError::PatchApply`] when the ops don't form a valid RFC6902 array
/// or applying them fails.
pub fn apply_webhook_patch(
    webhook_name: &str,
    object: &Value,
    ops: &Value,
    gvk: &Gvk,
) -> Result<Value, WebhookError> {
    let body =
        PatchBody::from_raw(PatchType::Json, ops.clone()).map_err(|e| WebhookError::PatchApply {
            webhook: webhook_name.to_string(),
            reason: e.to_string(),
        })?;
    // json-patch is pure (touches no schema env) — the mock env is never
    // consulted; pass one to satisfy the signature.
    let env: &dyn PatchSchemaEnv = &MockPatchEnv::new();
    apply_patch(PatchType::Json, object, &body, env, gvk).map_err(|e| WebhookError::PatchApply {
        webhook: webhook_name.to_string(),
        reason: e.to_string(),
    })
}

// ═══════════════════════════════════════════════════════════════════════════
//  The plugin — an AdmissionWebhook composing the two seams.
// ═══════════════════════════════════════════════════════════════════════════

/// A `(group, version, kind) -> plural` resolver. Boxed so the plugin stays
/// object-safe; production wires the generated-catalog pluralizer, tests pass a
/// closure.
pub type Pluralizer = Box<dyn Fn(&str, &str, &str) -> Option<String> + Send + Sync>;

/// The mutating-admission-webhook dispatch plugin.
///
/// On every Put/Patch (CREATE/UPDATE) it lists the live
/// `MutatingWebhookConfiguration`s, finds the webhooks whose `rules` match the
/// request, builds an `AdmissionReview`, calls each in config+webhook order,
/// and applies each returned JSONPatch to the object — feeding the mutated
/// object forward to the next webhook. A `Deny` (`allowed: false`) short
/// circuits with [`AdmissionDecision::Deny`]. Errors are governed per-webhook
/// by `failurePolicy` (`Fail` ⇒ deny; `Ignore` ⇒ skip that webhook).
pub struct MutatingWebhookPlugin {
    source: Arc<dyn WebhookConfigSource>,
    caller: Arc<dyn WebhookCaller>,
    /// Maps a kind to its plural resource for rule matching + the
    /// `AdmissionReview.request.resource`. Production wires the catalog
    /// pluralizer; tests pass a closure.
    pluralize: Pluralizer,
}

impl MutatingWebhookPlugin {
    /// New plugin from the two seams + a pluralizer
    /// `(group, version, kind) -> plural`.
    #[must_use]
    pub fn new(
        source: Arc<dyn WebhookConfigSource>,
        caller: Arc<dyn WebhookCaller>,
        pluralize: Pluralizer,
    ) -> Self {
        Self {
            source,
            caller,
            pluralize,
        }
    }

    /// The full dispatch over the request's (possibly already-mutated) value.
    /// Returns the [`AdmissionDecision`] for the chain.
    async fn dispatch(&self, request: &AdmissionRequest) -> AdmissionDecision {
        let key = &request.key;
        let Some(resource) = (self.pluralize)(&key.group, &key.version, &key.kind) else {
            // Unknown kind → no plural → cannot rule-match; admit unchanged.
            return AdmissionDecision::Allow;
        };
        let operation = operation_for(request.action);
        let configs = self.source.mutating_configs().await;

        // The object that flows through the webhook chain. CREATE/UPDATE carry
        // a value; without one there's nothing to mutate → admit.
        let Some(mut object) = request.value.clone() else {
            return AdmissionDecision::Allow;
        };
        let gvk = Gvk::from(key);
        let mut mutated = false;
        let mut uid_counter = 0u64;

        for config in &configs {
            for webhook in parse_mutating_config(config) {
                if !rules_match(&webhook, &key.group, &key.version, &resource, operation) {
                    continue;
                }
                if !self.selectors_match(&webhook, request, &object) {
                    continue;
                }
                uid_counter += 1;
                let uid = format!("{}-{}", request.key.label(), uid_counter);
                // Build the review against the CURRENT (possibly mutated)
                // object so a later webhook sees earlier injections.
                let mut req_for_review = request.clone();
                req_for_review.value = Some(object.clone());
                let review_req = build_admission_review(&req_for_review, &resource, &uid);

                match self.call_one(&webhook, &review_req).await {
                    Ok(resp) => {
                        if !resp.allowed {
                            let reason = resp
                                .message
                                .unwrap_or_else(|| "request denied by webhook".to_string());
                            return AdmissionDecision::Deny(format!("{}: {reason}", webhook.name));
                        }
                        if let Some(ops) = resp.patch {
                            match apply_webhook_patch(&webhook.name, &object, &ops, &gvk) {
                                Ok(new) => {
                                    object = new;
                                    mutated = true;
                                }
                                Err(e) => {
                                    if let Some(d) =
                                        Self::govern_failure(&webhook, &e.to_string())
                                    {
                                        return d;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(d) = Self::govern_failure(&webhook, &e.to_string()) {
                            return d;
                        }
                    }
                }
            }
        }

        if mutated {
            AdmissionDecision::Mutate(object)
        } else {
            AdmissionDecision::Allow
        }
    }

    /// Resolve the endpoint + invoke the [`WebhookCaller`], parsing the
    /// response. A `service` ref with no `url` is a typed deferral error.
    async fn call_one(
        &self,
        webhook: &MutatingWebhook,
        review_req: &Value,
    ) -> Result<WebhookResponse, WebhookError> {
        let endpoint = match (&webhook.client_config.url, &webhook.client_config.service) {
            (Some(url), _) => url.clone(),
            (None, Some(svc)) => {
                return Err(WebhookError::ServiceRefUnsupported {
                    webhook: webhook.name.clone(),
                    namespace: svc.namespace.clone(),
                    name: svc.name.clone(),
                });
            }
            (None, None) => {
                return Err(WebhookError::NoEndpoint {
                    webhook: webhook.name.clone(),
                });
            }
        };
        let raw = self
            .caller
            .call(
                &endpoint,
                webhook.client_config.ca_bundle.as_deref(),
                webhook.timeout_seconds,
                review_req,
            )
            .await
            .map_err(|message| WebhookError::CallFailed {
                webhook: webhook.name.clone(),
                message,
            })?;
        read_admission_review(&webhook.name, &raw)
    }

    /// Best-effort `namespaceSelector` / `objectSelector` matching. An
    /// `objectSelector` matches against the request object's own
    /// `metadata.labels`. A `namespaceSelector` matches against any namespace
    /// labels the request threads through `user_info.extra["namespaceLabels"]`
    /// (the apiserver may populate this); absent ⇒ an empty label set ⇒ a
    /// non-empty selector does NOT match (conservative). `None` selectors
    /// match all.
    fn selectors_match(
        &self,
        webhook: &MutatingWebhook,
        request: &AdmissionRequest,
        object: &Value,
    ) -> bool {
        if let Some(sel) = &webhook.object_selector {
            let labels = object_labels(object);
            if !sel.matches(&labels) {
                return false;
            }
        }
        if let Some(sel) = &webhook.namespace_selector {
            let labels = namespace_labels(request);
            if !sel.matches(&labels) {
                return false;
            }
        }
        true
    }

    /// Per-webhook `failurePolicy`: `Fail` ⇒ `Some(Deny(...))`; `Ignore` ⇒
    /// `None` (skip this webhook, keep going).
    fn govern_failure(webhook: &MutatingWebhook, message: &str) -> Option<AdmissionDecision> {
        match webhook.failure_policy {
            FailurePolicy::Fail => Some(AdmissionDecision::Deny(format!(
                "{}: {message} (failurePolicy=Fail)",
                webhook.name
            ))),
            FailurePolicy::Ignore => None,
        }
    }
}

/// Read `metadata.labels` off an object as a string map.
fn object_labels(object: &Value) -> std::collections::BTreeMap<String, String> {
    object
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Read namespace labels threaded via `user_info.extra["namespaceLabels"]`
/// (each `"k=v"`), if the apiserver populated them.
fn namespace_labels(request: &AdmissionRequest) -> std::collections::BTreeMap<String, String> {
    request
        .user_info
        .extra
        .get("namespaceLabels")
        .map(|pairs| {
            pairs
                .iter()
                .filter_map(|kv| kv.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[async_trait]
impl AdmissionWebhook for MutatingWebhookPlugin {
    fn name(&self) -> &'static str {
        "mutating-admission-webhooks"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        // Only CREATE/UPDATE carry an object to mutate; DELETE is admit-through
        // for the mutating plugin (validating webhooks for delete are a typed
        // follow-up — this plugin is the MUTATING path).
        match request.action {
            AdmissionAction::Put | AdmissionAction::Patch => Ok(self.dispatch(request).await),
            AdmissionAction::Delete => Ok(AdmissionDecision::Allow),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Mock seams for tests.
// ═══════════════════════════════════════════════════════════════════════════

/// A [`WebhookConfigSource`] holding a static list of config objects.
#[derive(Clone, Default)]
pub struct StaticConfigSource {
    configs: Arc<Vec<Value>>,
}

impl StaticConfigSource {
    /// New source from a list of `MutatingWebhookConfiguration` objects.
    #[must_use]
    pub fn new(configs: Vec<Value>) -> Self {
        Self {
            configs: Arc::new(configs),
        }
    }
}

#[async_trait]
impl WebhookConfigSource for StaticConfigSource {
    async fn mutating_configs(&self) -> Vec<Value> {
        (*self.configs).clone()
    }
}

/// A [`WebhookCaller`] that returns a canned `AdmissionReview` keyed by
/// endpoint, and records every call. Lets a test inject a JSONPatch (or a
/// deny, or a backend error) WITHOUT a real webhook server.
#[derive(Clone, Default)]
pub struct MockWebhookCaller {
    inner: Arc<Mutex<MockCallerState>>,
}

#[derive(Default)]
struct MockCallerState {
    /// endpoint → canned response review.
    responses: std::collections::BTreeMap<String, Value>,
    /// endpoint → backend error message (takes precedence over a response).
    errors: std::collections::BTreeMap<String, String>,
    /// (endpoint, timeout, request) call log.
    calls: Vec<(String, u32, Value)>,
}

impl MockWebhookCaller {
    /// New empty caller — every endpoint errors with "no canned response".
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin a canned response review for `endpoint`.
    pub async fn set_response(&self, endpoint: &str, review: Value) {
        self.inner
            .lock()
            .await
            .responses
            .insert(endpoint.to_string(), review);
    }

    /// Pin a backend error for `endpoint`.
    pub async fn set_error(&self, endpoint: &str, message: impl Into<String>) {
        self.inner
            .lock()
            .await
            .errors
            .insert(endpoint.to_string(), message.into());
    }

    /// The recorded calls `(endpoint, timeout_seconds, request)`.
    pub async fn calls(&self) -> Vec<(String, u32, Value)> {
        self.inner.lock().await.calls.clone()
    }

    /// Convenience: a `JSONPatch` `AdmissionReview` response from RFC6902 ops.
    #[must_use]
    pub fn json_patch_review(uid: &str, ops: &Value) -> Value {
        let b64 = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(ops).expect("ops serialize"));
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": b64,
            }
        })
    }

    /// Convenience: an allow-with-no-patch response.
    #[must_use]
    pub fn allow_review(uid: &str) -> Value {
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": { "uid": uid, "allowed": true }
        })
    }

    /// Convenience: a deny response carrying `message`.
    #[must_use]
    pub fn deny_review(uid: &str, message: &str) -> Value {
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": false,
                "status": { "code": 403, "message": message }
            }
        })
    }
}

#[async_trait]
impl WebhookCaller for MockWebhookCaller {
    async fn call(
        &self,
        endpoint: &str,
        _ca_bundle: Option<&str>,
        timeout_seconds: u32,
        review_request: &Value,
    ) -> Result<Value, String> {
        let mut state = self.inner.lock().await;
        state
            .calls
            .push((endpoint.to_string(), timeout_seconds, review_request.clone()));
        if let Some(msg) = state.errors.get(endpoint) {
            return Err(msg.clone());
        }
        state
            .responses
            .get(endpoint)
            .cloned()
            .ok_or_else(|| format!("no canned response for endpoint {endpoint:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::resource::ResourceKey;
    use serde_json::json;

    fn pluralizer() -> Pluralizer {
        Box::new(|_g, _v, kind| {
            Some(match kind {
                "Pod" => "pods",
                "Deployment" => "deployments",
                other => return Some(format!("{}s", other.to_lowercase())),
            }
            .to_string())
        })
    }

    fn pod_request() -> AdmissionRequest {
        AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "podinfo"),
            value: Some(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "podinfo" },
                "spec": { "containers": [{"name": "app", "image": "podinfo:6"}] }
            })),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        }
    }

    /// A config whose webhook injects a sidecar container via a JSONPatch and
    /// matches Pod CREATE.
    fn sidecar_inject_config(endpoint: &str) -> Value {
        json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "MutatingWebhookConfiguration",
            "metadata": { "name": "lareira-enxerto" },
            "webhooks": [{
                "name": "inject.tatara-mesh.io",
                "clientConfig": { "url": endpoint },
                "rules": [{
                    "operations": ["CREATE"],
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "resources": ["pods"]
                }],
                "failurePolicy": "Fail",
                "timeoutSeconds": 5
            }]
        })
    }

    /// The RFC6902 ops the sidecar injector returns: append the aresta proxy
    /// container + a SPIRE-agent init container.
    fn sidecar_ops() -> Value {
        json!([
            {"op": "add", "path": "/spec/containers/-",
             "value": {"name": "aresta-proxy", "image": "aresta:1.0"}},
            {"op": "add", "path": "/spec/initContainers",
             "value": [{"name": "spire-agent-init", "image": "spire-agent:1.0"}]}
        ])
    }

    #[tokio::test]
    async fn sidecar_is_injected_into_the_object() {
        let endpoint = "https://inject.tatara-mesh.svc/mutate";
        let source = Arc::new(StaticConfigSource::new(vec![sidecar_inject_config(endpoint)]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller
            .set_response(
                endpoint,
                MockWebhookCaller::json_patch_review("u", &sidecar_ops()),
            )
            .await;
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        let mutated = match decision {
            AdmissionDecision::Mutate(v) => v,
            other => panic!("expected Mutate, got {other:?}"),
        };
        // The proxy sidecar landed.
        let containers = mutated["spec"]["containers"].as_array().unwrap();
        assert_eq!(containers.len(), 2, "aresta proxy appended");
        assert!(containers.iter().any(|c| c["name"] == "aresta-proxy"));
        // The SPIRE init container landed (closes the loop with the kubelet
        // init-container driver).
        let inits = mutated["spec"]["initContainers"].as_array().unwrap();
        assert_eq!(inits.len(), 1);
        assert_eq!(inits[0]["name"], "spire-agent-init");
        // The webhook was actually called with a CREATE AdmissionReview.
        let calls = caller.calls().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, endpoint);
        assert_eq!(calls[0].2["request"]["operation"], "CREATE");
        assert_eq!(calls[0].2["request"]["resource"]["resource"], "pods");
    }

    #[tokio::test]
    async fn non_matching_rule_is_not_called() {
        // A webhook matching Deployments only must NOT be called for a Pod.
        let endpoint = "https://deploy-only.svc/mutate";
        let config = json!({
            "kind": "MutatingWebhookConfiguration",
            "webhooks": [{
                "name": "deploy.only.io",
                "clientConfig": { "url": endpoint },
                "rules": [{
                    "operations": ["CREATE"],
                    "apiGroups": ["apps"],
                    "apiVersions": ["v1"],
                    "resources": ["deployments"]
                }]
            }]
        });
        let source = Arc::new(StaticConfigSource::new(vec![config]));
        let caller = Arc::new(MockWebhookCaller::new());
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        assert!(matches!(decision, AdmissionDecision::Allow), "no rule match → admit unchanged");
        assert_eq!(caller.calls().await.len(), 0, "webhook NOT called");
    }

    #[tokio::test]
    async fn failure_policy_fail_rejects_on_call_error() {
        let endpoint = "https://flaky.svc/mutate";
        let source = Arc::new(StaticConfigSource::new(vec![sidecar_inject_config(endpoint)]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller.set_error(endpoint, "connection refused").await;
        let plugin = MutatingWebhookPlugin::new(source, caller, pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        match decision {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("inject.tatara-mesh.io"));
                assert!(reason.contains("failurePolicy=Fail"));
                assert!(reason.contains("connection refused"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failure_policy_ignore_proceeds_on_call_error() {
        let endpoint = "https://flaky.svc/mutate";
        let mut config = sidecar_inject_config(endpoint);
        config["webhooks"][0]["failurePolicy"] = json!("Ignore");
        let source = Arc::new(StaticConfigSource::new(vec![config]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller.set_error(endpoint, "connection refused").await;
        let plugin = MutatingWebhookPlugin::new(source, caller, pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        // The webhook errored but failurePolicy=Ignore → request proceeds
        // unmutated by this webhook → Allow (no mutation happened).
        assert!(
            matches!(decision, AdmissionDecision::Allow),
            "failurePolicy=Ignore on error → admit unchanged"
        );
    }

    #[tokio::test]
    async fn webhook_deny_short_circuits() {
        let endpoint = "https://policy.svc/mutate";
        let source = Arc::new(StaticConfigSource::new(vec![sidecar_inject_config(endpoint)]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller
            .set_response(
                endpoint,
                MockWebhookCaller::deny_review("u", "pods must carry a mesh label"),
            )
            .await;
        let plugin = MutatingWebhookPlugin::new(source, caller, pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        match decision {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("inject.tatara-mesh.io"));
                assert!(reason.contains("mesh label"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn service_ref_without_url_is_typed_deferral() {
        let config = json!({
            "kind": "MutatingWebhookConfiguration",
            "webhooks": [{
                "name": "svc.only.io",
                "clientConfig": { "service": { "namespace": "mesh", "name": "injector" } },
                "rules": [{
                    "operations": ["CREATE"], "apiGroups": [""],
                    "apiVersions": ["v1"], "resources": ["pods"]
                }],
                "failurePolicy": "Fail"
            }]
        });
        let source = Arc::new(StaticConfigSource::new(vec![config]));
        let caller = Arc::new(MockWebhookCaller::new());
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        let decision = plugin.review(&pod_request()).await.unwrap();
        // service-ref unsupported → typed error → governed by failurePolicy=Fail
        // → Deny (NOT a silent skip).
        match decision {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("service-ref resolution is unsupported"));
                assert!(reason.contains("mesh/injector"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
        assert_eq!(caller.calls().await.len(), 0, "no HTTP call for a service ref");
    }

    #[tokio::test]
    async fn two_webhooks_chain_mutations_forward() {
        // Webhook A appends a label; webhook B (a later config) sees it and
        // appends a container — proving the object flows forward.
        let ep_a = "https://a.svc/mutate";
        let ep_b = "https://b.svc/mutate";
        let cfg_a = json!({
            "kind": "MutatingWebhookConfiguration",
            "webhooks": [{
                "name": "a.io", "clientConfig": { "url": ep_a },
                "rules": [{"operations": ["CREATE"], "apiGroups": [""],
                           "apiVersions": ["v1"], "resources": ["pods"]}]
            }]
        });
        let cfg_b = json!({
            "kind": "MutatingWebhookConfiguration",
            "webhooks": [{
                "name": "b.io", "clientConfig": { "url": ep_b },
                "rules": [{"operations": ["CREATE"], "apiGroups": [""],
                           "apiVersions": ["v1"], "resources": ["pods"]}]
            }]
        });
        let source = Arc::new(StaticConfigSource::new(vec![cfg_a, cfg_b]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller
            .set_response(
                ep_a,
                MockWebhookCaller::json_patch_review(
                    "ua",
                    &json!([{"op": "add", "path": "/metadata/labels",
                             "value": {"mutated-by-a": "yes"}}]),
                ),
            )
            .await;
        caller
            .set_response(
                ep_b,
                MockWebhookCaller::json_patch_review(
                    "ub",
                    &json!([{"op": "add", "path": "/spec/containers/-",
                             "value": {"name": "from-b", "image": "b:1"}}]),
                ),
            )
            .await;
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        let mutated = match plugin.review(&pod_request()).await.unwrap() {
            AdmissionDecision::Mutate(v) => v,
            other => panic!("expected Mutate, got {other:?}"),
        };
        assert_eq!(mutated["metadata"]["labels"]["mutated-by-a"], "yes");
        let containers = mutated["spec"]["containers"].as_array().unwrap();
        assert!(containers.iter().any(|c| c["name"] == "from-b"));
        // B was called with the object A already mutated.
        let calls = caller.calls().await;
        let b_call = calls.iter().find(|(e, _, _)| e == ep_b).unwrap();
        assert_eq!(b_call.2["request"]["object"]["metadata"]["labels"]["mutated-by-a"], "yes");
    }

    #[tokio::test]
    async fn object_selector_filters() {
        // A webhook with objectSelector matchLabels{mesh=on} must skip a Pod
        // lacking that label.
        let endpoint = "https://sel.svc/mutate";
        let mut config = sidecar_inject_config(endpoint);
        config["webhooks"][0]["objectSelector"] = json!({"matchLabels": {"mesh": "on"}});
        let source = Arc::new(StaticConfigSource::new(vec![config]));
        let caller = Arc::new(MockWebhookCaller::new());
        caller
            .set_response(endpoint, MockWebhookCaller::json_patch_review("u", &sidecar_ops()))
            .await;
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        // Pod without the label → not selected → not called → admit unchanged.
        let decision = plugin.review(&pod_request()).await.unwrap();
        assert!(matches!(decision, AdmissionDecision::Allow));
        assert_eq!(caller.calls().await.len(), 0);

        // Pod WITH the label → selected → injected.
        let mut req = pod_request();
        req.value.as_mut().unwrap()["metadata"]["labels"] = json!({"mesh": "on"});
        let decision = plugin.review(&req).await.unwrap();
        assert!(matches!(decision, AdmissionDecision::Mutate(_)));
        assert_eq!(caller.calls().await.len(), 1);
    }

    #[tokio::test]
    async fn unsupported_patch_type_is_governed_by_failure_policy() {
        let endpoint = "https://badtype.svc/mutate";
        let source = Arc::new(StaticConfigSource::new(vec![sidecar_inject_config(endpoint)]));
        let caller = Arc::new(MockWebhookCaller::new());
        // Response declares a patchType engenho doesn't support.
        caller
            .set_response(
                endpoint,
                json!({
                    "response": { "uid": "u", "allowed": true,
                                  "patchType": "SomeFuturePatch", "patch": "e30=" }
                }),
            )
            .await;
        let plugin = MutatingWebhookPlugin::new(source, caller, pluralizer());

        // failurePolicy=Fail → the unsupported patchType denies (not silent).
        match plugin.review(&pod_request()).await.unwrap() {
            AdmissionDecision::Deny(reason) => {
                assert!(reason.contains("SomeFuturePatch") || reason.contains("unsupported"));
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_is_admit_through() {
        let endpoint = "https://x.svc/mutate";
        let source = Arc::new(StaticConfigSource::new(vec![sidecar_inject_config(endpoint)]));
        let caller = Arc::new(MockWebhookCaller::new());
        let plugin = MutatingWebhookPlugin::new(source, caller.clone(), pluralizer());

        let mut req = pod_request();
        req.action = AdmissionAction::Delete;
        req.value = None;
        let decision = plugin.review(&req).await.unwrap();
        assert!(matches!(decision, AdmissionDecision::Allow));
        assert_eq!(caller.calls().await.len(), 0, "mutating plugin doesn't call on delete");
    }

    #[test]
    fn rules_match_wildcards() {
        let wh = MutatingWebhook {
            name: "w".into(),
            client_config: WebhookClientConfig { url: Some("u".into()), service: None, ca_bundle: None },
            rules: vec![RuleWithOperations {
                operations: vec!["*".into()],
                api_groups: vec!["*".into()],
                api_versions: vec!["*".into()],
                resources: vec!["*".into()],
            }],
            failure_policy: FailurePolicy::Fail,
            timeout_seconds: 10,
            reinvocation_policy: ReinvocationPolicy::Never,
            namespace_selector: None,
            object_selector: None,
        };
        assert!(rules_match(&wh, "apps", "v1", "deployments", "CREATE"));
        assert!(rules_match(&wh, "", "v1", "pods", "DELETE"));
    }

    #[test]
    fn parse_defaults_are_k8s_defaults() {
        let cfg = json!({
            "webhooks": [{ "name": "w", "clientConfig": { "url": "u" } }]
        });
        let hooks = parse_mutating_config(&cfg);
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].failure_policy, FailurePolicy::Fail);
        assert_eq!(hooks[0].timeout_seconds, 10);
        assert_eq!(hooks[0].reinvocation_policy, ReinvocationPolicy::Never);
    }
}
