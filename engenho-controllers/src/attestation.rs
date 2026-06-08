//! Attestation-gated admission — verify image / artifact signatures
//! before admission.
//!
//! Adapts a pluggable `SignatureVerifier` trait to the
//! `AdmissionWebhook` trait. Production deployments wire in a
//! tameshi / cosign / sigstore verifier; the substrate ships a
//! `FakeSignatureVerifier` for tests + a typed
//! `TameshiAttestationWebhook` that holds the verifier reference
//! + opts in to specific kinds (Pods, Jobs, CronJobs — anything
//! that names an image).
//!
//! ## Decision rules
//!
//! For Put / Patch on a kind that names images (Pod / Job / CronJob
//! / Deployment / StatefulSet / DaemonSet / ReplicaSet via their
//! `spec.template.spec.containers[].image`), extract every image
//! reference + ask the verifier. First unsigned image denies with
//! a typed reason.
//!
//! Other kinds + Delete → Allow.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::admission::{
    AdmissionAction, AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};

/// Verifier errors.
#[derive(Debug, Clone, Error)]
pub enum VerifierError {
    /// Backend (cosign, tameshi, sigstore) returned an error.
    #[error("backend: {0}")]
    Backend(String),
    /// Image reference malformed.
    #[error("invalid image ref: {0}")]
    InvalidRef(String),
}

engenho_substrate::impl_error_kind! {
    VerifierError {
        (Backend(_)) => "backend",
        (InvalidRef(_)) => "invalid_ref",
    }
}

/// Pluggable signature verifier — checks an image / artifact
/// reference has a valid signature against the verifier's trust
/// store.
#[async_trait]
pub trait SignatureVerifier: Send + Sync {
    /// Verifier identifier for telemetry.
    fn name(&self) -> &'static str;

    /// True if `image_ref` is verifiably signed. False if explicitly
    /// unsigned. Returns Err for backend failures the caller
    /// decides how to interpret.
    ///
    /// # Errors
    /// [`VerifierError::Backend`] on backend failure;
    /// [`VerifierError::InvalidRef`] on malformed input.
    async fn verify(&self, image_ref: &str) -> Result<bool, VerifierError>;
}

// =================================================================
// FakeSignatureVerifier — deterministic backend for tests
// =================================================================

/// In-memory verifier. Holds a set of "signed" image refs; anything
/// not in the set is unsigned. Records every verify call.
#[derive(Default, Clone)]
pub struct FakeSignatureVerifier {
    inner: Arc<Mutex<FakeVerifierState>>,
}

#[derive(Default)]
struct FakeVerifierState {
    signed: BTreeSet<String>,
    fail_with: Option<String>,
    calls: Vec<String>,
}

impl FakeSignatureVerifier {
    /// Fresh verifier — everything unsigned.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `image` as signed.
    pub async fn mark_signed(&self, image: impl Into<String>) {
        self.inner.lock().await.signed.insert(image.into());
    }

    /// Make the next verify call return `VerifierError::Backend`.
    pub async fn fail_next(&self, msg: impl Into<String>) {
        self.inner.lock().await.fail_with = Some(msg.into());
    }

    /// Snapshot of verify calls.
    pub async fn calls(&self) -> Vec<String> {
        self.inner.lock().await.calls.clone()
    }
}

#[async_trait]
impl SignatureVerifier for FakeSignatureVerifier {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn verify(&self, image_ref: &str) -> Result<bool, VerifierError> {
        let mut state = self.inner.lock().await;
        state.calls.push(image_ref.to_string());
        if let Some(msg) = state.fail_with.take() {
            return Err(VerifierError::Backend(msg));
        }
        if image_ref.is_empty() {
            return Err(VerifierError::InvalidRef("empty".into()));
        }
        Ok(state.signed.contains(image_ref))
    }
}

// =================================================================
// TameshiAttestationWebhook — adapts SignatureVerifier → AdmissionWebhook
// =================================================================

/// Kinds whose Pod template contains images we should verify.
fn workload_kinds() -> BTreeSet<&'static str> {
    [
        "Pod",
        "Deployment",
        "StatefulSet",
        "DaemonSet",
        "ReplicaSet",
        "Job",
        "CronJob",
    ]
    .into_iter()
    .collect()
}

/// Webhook backed by a [`SignatureVerifier`].
pub struct TameshiAttestationWebhook {
    verifier: Arc<dyn SignatureVerifier>,
    workload_kinds: BTreeSet<&'static str>,
}

impl TameshiAttestationWebhook {
    /// New webhook gating workload kinds (Pod / Deployment /
    /// StatefulSet / Job / CronJob / DaemonSet / ReplicaSet).
    #[must_use]
    pub fn new(verifier: Arc<dyn SignatureVerifier>) -> Self {
        Self {
            verifier,
            workload_kinds: workload_kinds(),
        }
    }

    /// Pure helper: extract every image reference from a workload
    /// manifest. Walks `spec.containers[].image` and (for owner-
    /// resources) `spec.template.spec.containers[].image`. Also
    /// walks `spec.jobTemplate.spec.template.spec.containers[].image`
    /// for CronJob.
    #[must_use]
    pub fn extract_image_refs(value: &Value) -> Vec<String> {
        let mut refs = Vec::new();
        fn from_containers(containers: &Value, out: &mut Vec<String>) {
            if let Some(arr) = containers.as_array() {
                for c in arr {
                    if let Some(img) = c.get("image").and_then(|i| i.as_str()) {
                        out.push(img.to_string());
                    }
                }
            }
        }
        // Pod directly: spec.containers[]
        if let Some(c) = value.get("spec").and_then(|s| s.get("containers")) {
            from_containers(c, &mut refs);
        }
        // Owner: spec.template.spec.containers[]
        if let Some(c) = value
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
        {
            from_containers(c, &mut refs);
        }
        // CronJob: spec.jobTemplate.spec.template.spec.containers[]
        if let Some(c) = value
            .get("spec")
            .and_then(|s| s.get("jobTemplate"))
            .and_then(|jt| jt.get("spec"))
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
        {
            from_containers(c, &mut refs);
        }
        refs
    }
}

#[async_trait]
impl AdmissionWebhook for TameshiAttestationWebhook {
    fn name(&self) -> &'static str {
        "tameshi-attestation"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        if request.action == AdmissionAction::Delete {
            return Ok(AdmissionDecision::Allow);
        }
        if !self.workload_kinds.contains(request.key.kind.as_str()) {
            return Ok(AdmissionDecision::Allow);
        }
        let Some(value) = &request.value else {
            return Ok(AdmissionDecision::Allow);
        };
        let images = Self::extract_image_refs(value);
        if images.is_empty() {
            // No images to verify (e.g. patch of unrelated field) → allow.
            return Ok(AdmissionDecision::Allow);
        }
        for image in &images {
            let signed = self
                .verifier
                .verify(image)
                .await
                .map_err(|e| AdmissionError::Backend(e.to_string()))?;
            if !signed {
                return Ok(AdmissionDecision::Deny(format!(
                    "image {image} is not signed (verifier={})",
                    self.verifier.name()
                )));
            }
        }
        Ok(AdmissionDecision::Allow)
    }
}

/// Convenience constructor.
#[must_use]
pub fn tameshi_attestation_webhook(
    verifier: Arc<dyn SignatureVerifier>,
) -> Arc<dyn AdmissionWebhook> {
    Arc::new(TameshiAttestationWebhook::new(verifier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_store::resource::ResourceKey;
    use serde_json::json;

    fn pod_req(image: &str) -> AdmissionRequest {
        AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
            value: Some(json!({
                "spec": {"containers": [{"image": image}]}
            })),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        }
    }

    fn deployment_req(image: &str) -> AdmissionRequest {
        AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("apps", "v1", "Deployment", "default", "x"),
            value: Some(json!({
                "spec": {
                    "template": {
                        "spec": {"containers": [{"image": image}]}
                    }
                }
            })),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        }
    }

    fn cronjob_req(image: &str) -> AdmissionRequest {
        AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("batch", "v1", "CronJob", "default", "x"),
            value: Some(json!({
                "spec": {
                    "jobTemplate": {
                        "spec": {
                            "template": {
                                "spec": {"containers": [{"image": image}]}
                            }
                        }
                    }
                }
            })),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        }
    }

    #[tokio::test]
    async fn signed_image_allowed() {
        let v = Arc::new(FakeSignatureVerifier::new());
        v.mark_signed("registry/podinfo:1.0").await;
        let w = TameshiAttestationWebhook::new(v);
        let r = w.review(&pod_req("registry/podinfo:1.0")).await.unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn unsigned_image_denied() {
        let v = Arc::new(FakeSignatureVerifier::new());
        let w = TameshiAttestationWebhook::new(v);
        let r = w.review(&pod_req("untrusted:latest")).await.unwrap();
        assert!(
            matches!(r, AdmissionDecision::Deny(reason) if reason.contains("untrusted:latest"))
        );
    }

    #[tokio::test]
    async fn deployment_walks_template_spec_containers() {
        let v = Arc::new(FakeSignatureVerifier::new());
        v.mark_signed("nginx:1.27").await;
        let w = TameshiAttestationWebhook::new(v.clone());
        let r = w.review(&deployment_req("nginx:1.27")).await.unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
        // Verifier saw the right image.
        assert!(v.calls().await.contains(&"nginx:1.27".to_string()));
    }

    #[tokio::test]
    async fn cronjob_walks_jobtemplate_template_spec_containers() {
        let v = Arc::new(FakeSignatureVerifier::new());
        v.mark_signed("alpine:3.20").await;
        let w = TameshiAttestationWebhook::new(v.clone());
        let r = w.review(&cronjob_req("alpine:3.20")).await.unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
        assert!(v.calls().await.contains(&"alpine:3.20".to_string()));
    }

    #[tokio::test]
    async fn non_workload_kind_skips_verification() {
        let v = Arc::new(FakeSignatureVerifier::new());
        let w = TameshiAttestationWebhook::new(v.clone());
        let r = w
            .review(&AdmissionRequest {
                action: AdmissionAction::Put,
                key: ResourceKey::namespaced("", "v1", "ConfigMap", "default", "cm"),
                value: Some(json!({})),
                current: None,
                user_info: engenho_types::auth::UserInfo::default(),
            })
            .await
            .unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
        assert!(
            v.calls().await.is_empty(),
            "verifier not called for ConfigMap"
        );
    }

    #[tokio::test]
    async fn delete_always_allowed() {
        let v = Arc::new(FakeSignatureVerifier::new());
        let w = TameshiAttestationWebhook::new(v.clone());
        let r = w
            .review(&AdmissionRequest {
                action: AdmissionAction::Delete,
                key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
                value: None,
                current: None,
                user_info: engenho_types::auth::UserInfo::default(),
            })
            .await
            .unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
        assert!(v.calls().await.is_empty());
    }

    #[tokio::test]
    async fn patch_without_images_allowed() {
        let v = Arc::new(FakeSignatureVerifier::new());
        let w = TameshiAttestationWebhook::new(v);
        let r = w
            .review(&AdmissionRequest {
                action: AdmissionAction::Patch,
                key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
                value: Some(json!({"metadata": {"labels": {"new": "label"}}})),
                current: None,
                user_info: engenho_types::auth::UserInfo::default(),
            })
            .await
            .unwrap();
        assert_eq!(r, AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn backend_failure_surfaces_as_admission_error() {
        let v = Arc::new(FakeSignatureVerifier::new());
        v.fail_next("verifier down").await;
        let w = TameshiAttestationWebhook::new(v);
        let err = w.review(&pod_req("x")).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn multiple_containers_all_must_be_signed() {
        let v = Arc::new(FakeSignatureVerifier::new());
        v.mark_signed("good:1").await;
        // "bad:1" intentionally unsigned.
        let w = TameshiAttestationWebhook::new(v);
        let req = AdmissionRequest {
            action: AdmissionAction::Put,
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
            value: Some(json!({
                "spec": {"containers": [{"image": "good:1"}, {"image": "bad:1"}]}
            })),
            current: None,
            user_info: engenho_types::auth::UserInfo::default(),
        };
        let r = w.review(&req).await.unwrap();
        assert!(matches!(r, AdmissionDecision::Deny(reason) if reason.contains("bad:1")));
    }

    #[test]
    fn extract_image_refs_handles_empty_value() {
        let refs = TameshiAttestationWebhook::extract_image_refs(&json!({}));
        assert!(refs.is_empty());
    }

    #[test]
    fn extract_image_refs_handles_pod_shape() {
        let refs = TameshiAttestationWebhook::extract_image_refs(&json!({
            "spec": {"containers": [{"image": "a"}, {"image": "b"}]}
        }));
        assert_eq!(refs, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn verifier_name_is_stable() {
        assert_eq!(FakeSignatureVerifier::new().name(), "fake");
    }

    #[tokio::test]
    async fn webhook_name_is_stable() {
        let v = Arc::new(FakeSignatureVerifier::new());
        let w = TameshiAttestationWebhook::new(v);
        assert_eq!(w.name(), "tameshi-attestation");
    }

    #[test]
    fn verifier_error_kinds_are_stable() {
        assert_eq!(VerifierError::Backend("x".into()).kind(), "backend");
        assert_eq!(VerifierError::InvalidRef("x".into()).kind(), "invalid_ref");
    }

    #[tokio::test]
    async fn fake_verifier_rejects_empty_ref() {
        let v = FakeSignatureVerifier::new();
        let err = v.verify("").await.unwrap_err();
        assert_eq!(err.kind(), "invalid_ref");
    }
}
