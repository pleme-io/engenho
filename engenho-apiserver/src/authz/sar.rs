//! `authorization.k8s.io/v1` SubjectAccessReview family — typed serde structs +
//! the three POST handlers `kubectl auth can-i` drives.
//!
//! These kinds do NOT exist in `engenho-types` (confirmed: only `rbac_v1` +
//! `authentication.k8s.io.SelfSubjectReview` are generated). Per TYPED EMISSION
//! they are authored here as serde structs mirroring upstream
//! `authorization/v1` field-for-field (NOT `json!()` of the wire), alongside the
//! existing `SelfSubjectReview` special-route pattern — they are discovery-light
//! special routes, not store-backed kinds.
//!
//!   * `POST .../subjectaccessreviews`      → authorize ANOTHER subject (the
//!     spec's user/groups). The caller must itself be authorized to create the
//!     SAR (the authz middleware gates the route).
//!   * `POST .../selfsubjectaccessreviews`  → authorize the CALLER (from
//!     `Extension<UserInfo>`); `kubectl auth can-i <verb> <resource>`.
//!   * `POST .../selfsubjectrulesreviews`   → enumerate the caller's applicable
//!     rules; `kubectl auth can-i --list`.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use engenho_kube_proto::{self as kube_proto, is_protobuf_content_type};
use engenho_types::auth::UserInfo;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{Attributes, Decision, EffectiveRules};
use crate::error::ApiError;
use crate::router::{ExtractUserInfo, RouterState};

// ── wire types (authorization.k8s.io/v1) ───────────────────────────────────

/// `ResourceAttributes` — the resource-targeting half of a SAR spec.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceAttributes {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// `NonResourceAttributes` — the non-resource-targeting half of a SAR spec.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NonResourceAttributes {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,
}

/// `SubjectAccessReviewSpec` — the inputs of a SAR (for ANOTHER subject).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectAccessReviewSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_attributes: Option<ResourceAttributes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_resource_attributes: Option<NonResourceAttributes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra: std::collections::BTreeMap<String, Vec<String>>,
}

/// `SelfSubjectAccessReviewSpec` — the inputs of a SELF SAR (the caller). No
/// user/groups (the subject is the authenticated caller).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectAccessReviewSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_attributes: Option<ResourceAttributes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_resource_attributes: Option<NonResourceAttributes>,
}

/// `SelfSubjectRulesReviewSpec` — the inputs of a rules review (a namespace).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SelfSubjectRulesReviewSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// `SubjectAccessReviewStatus` — the verdict of a (self) SAR.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectAccessReviewStatus {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub denied: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(
        default,
        rename = "evaluationError",
        skip_serializing_if = "String::is_empty"
    )]
    pub evaluation_error: String,
}

/// `ResourceRule` — one enumerated resource rule in a rules review status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verbs: Vec<String>,
    #[serde(default, rename = "apiGroups", skip_serializing_if = "Vec::is_empty")]
    pub api_groups: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<String>,
    #[serde(
        default,
        rename = "resourceNames",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub resource_names: Vec<String>,
}

/// `NonResourceRule` — one enumerated non-resource rule in a rules review status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonResourceRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verbs: Vec<String>,
    #[serde(
        default,
        rename = "nonResourceURLs",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub non_resource_urls: Vec<String>,
}

/// `SubjectRulesReviewStatus` — the result of `kubectl auth can-i --list`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRulesReviewStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_rules: Vec<ResourceRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_resource_rules: Vec<NonResourceRule>,
    pub incomplete: bool,
}

/// `SubjectAccessReview` — the full POST request/response envelope.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubjectAccessReview {
    #[serde(
        default,
        rename = "apiVersion",
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default)]
    pub spec: SubjectAccessReviewSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectAccessReviewStatus>,
}

/// `SelfSubjectAccessReview` — the full POST envelope for the caller's SAR.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SelfSubjectAccessReview {
    #[serde(
        default,
        rename = "apiVersion",
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default)]
    pub spec: SelfSubjectAccessReviewSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectAccessReviewStatus>,
}

/// `SelfSubjectRulesReview` — the full POST envelope for the rules review.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SelfSubjectRulesReview {
    #[serde(
        default,
        rename = "apiVersion",
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default)]
    pub spec: SelfSubjectRulesReviewSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectRulesReviewStatus>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !*b
}

// ── Attributes builders ─────────────────────────────────────────────────────

/// Build [`Attributes`] from a SAR's resource/non-resource attributes + the
/// SUBJECT (the user the SAR asks about). A SAR with neither resource nor
/// non-resource attributes resolves to an empty resource request (NoOpinion).
fn attributes_from_spec(
    user: UserInfo,
    resource: Option<&ResourceAttributes>,
    non_resource: Option<&NonResourceAttributes>,
) -> Attributes {
    if let Some(nr) = non_resource {
        return Attributes {
            user,
            verb: nr.verb.clone().unwrap_or_default(),
            group: String::new(),
            version: String::new(),
            resource: String::new(),
            subresource: None,
            namespace: None,
            name: None,
            non_resource_url: Some(nr.path.clone().unwrap_or_default()),
        };
    }
    let r = resource.cloned().unwrap_or_default();
    Attributes {
        user,
        verb: r.verb.unwrap_or_default(),
        group: r.group.unwrap_or_default(),
        version: r.version.unwrap_or_default(),
        resource: r.resource.unwrap_or_default(),
        subresource: r.subresource,
        namespace: r.namespace,
        name: r.name,
        non_resource_url: None,
    }
}

/// The subject a `SubjectAccessReview` asks about: the spec's `user` + `groups`
/// (NOT the caller). An empty username falls back to anonymous so the authorizer
/// never sees a nameless identity.
fn subject_user(spec: &SubjectAccessReviewSpec) -> UserInfo {
    let username = spec.user.clone().unwrap_or_default();
    if username.is_empty() && spec.groups.as_ref().map_or(true, Vec::is_empty) {
        return UserInfo::anonymous();
    }
    UserInfo {
        username,
        uid: spec.uid.clone().unwrap_or_default(),
        groups: spec.groups.clone().unwrap_or_default(),
        extra: spec.extra.clone(),
    }
}

/// Map a [`Decision`] to the SAR status (`allowed` / `denied` / `reason`).
fn status_for(decision: Decision) -> SubjectAccessReviewStatus {
    match decision {
        Decision::Allow => SubjectAccessReviewStatus {
            allowed: true,
            denied: false,
            reason: "RBAC: allowed by a matching rule".to_string(),
            evaluation_error: String::new(),
        },
        Decision::Deny => SubjectAccessReviewStatus {
            allowed: false,
            denied: true,
            reason: "RBAC: explicitly denied".to_string(),
            evaluation_error: String::new(),
        },
        Decision::NoOpinion => SubjectAccessReviewStatus {
            allowed: false,
            denied: false,
            reason: "RBAC: no rule grants this request".to_string(),
            evaluation_error: String::new(),
        },
    }
}

/// Convert the interpreter's [`EffectiveRules`] into the wire rules-review
/// status.
fn rules_status(rules: EffectiveRules) -> SubjectRulesReviewStatus {
    SubjectRulesReviewStatus {
        resource_rules: rules
            .resource_rules
            .into_iter()
            .map(|r| ResourceRule {
                verbs: r.verbs,
                api_groups: r.api_groups,
                resources: r.resources,
                resource_names: r.resource_names,
            })
            .collect(),
        non_resource_rules: rules
            .non_resource_rules
            .into_iter()
            .map(|r| NonResourceRule {
                verbs: r.verbs,
                non_resource_urls: r.non_resource_urls,
            })
            .collect(),
        incomplete: rules.incomplete,
    }
}

/// Decode a SAR-family request body into a typed `T`, dispatching on the
/// `Content-Type`:
///
///   * `application/json` (or absent) → `serde_json::from_slice`.
///   * `application/vnd.kubernetes.protobuf` → the typed `engenho-kube-proto`
///     codec (the authorization.k8s.io/v1 messages are vendored into the
///     descriptor pool, so the `runtime.Unknown` wrapper + per-kind message
///     decode to a `serde_json::Value`, then to `T`). This is the path kubectl's
///     typed clientset (`auth can-i`) uses.
///
/// An empty body decodes to `T::default()` (a SAR POST with no spec asks "can I
/// do nothing" → NoOpinion). A malformed body → a typed [`ApiError::BadRequest`].
fn decode_sar_body<T: DeserializeOwned + Default>(
    headers: &HeaderMap,
    raw: &[u8],
    kind: &str,
) -> Result<T, ApiError> {
    if raw.is_empty() {
        return Ok(T::default());
    }
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let media = content_type.split(';').next().unwrap_or("").trim();
    let value: serde_json::Value =
        if media.is_empty() || media.eq_ignore_ascii_case("application/json") {
            serde_json::from_slice(raw)
                .map_err(|e| ApiError::BadRequest(format!("invalid {kind} JSON body: {e}")))?
        } else if is_protobuf_content_type(content_type) {
            kube_proto::decode_protobuf(raw)?
        } else {
            return Err(ApiError::UnsupportedMediaType(format!(
                "the body of the {kind} request was in an unsupported format; got {media:?}"
            )));
        };
    serde_json::from_value(value)
        .map_err(|e| ApiError::BadRequest(format!("invalid {kind} shape: {e}")))
}

// ── handlers ────────────────────────────────────────────────────────────────

/// `POST /apis/authorization.k8s.io/v1/subjectaccessreviews` — authorize the
/// SPEC's subject (NOT the caller). The caller's authority to create the SAR is
/// enforced by the authz middleware wrapping this route. The body is decoded via
/// the content-negotiated [`decode_sar_body`] (JSON or kubectl's protobuf).
pub async fn subject_access_review(
    State(state): State<RouterState>,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let review: SubjectAccessReview = decode_sar_body(&headers, &raw, "SubjectAccessReview")?;
    let subject = subject_user(&review.spec);
    let attrs = attributes_from_spec(
        subject,
        review.spec.resource_attributes.as_ref(),
        review.spec.non_resource_attributes.as_ref(),
    );
    let decision = state.authorizer.authorize(&attrs).await;
    let out = SubjectAccessReview {
        api_version: "authorization.k8s.io/v1".to_string(),
        kind: "SubjectAccessReview".to_string(),
        spec: review.spec,
        status: Some(status_for(decision)),
    };
    Ok((StatusCode::CREATED, Json(out)).into_response())
}

/// `POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews` — authorize the
/// CALLER (`kubectl auth can-i <verb> <resource>`). The subject is the
/// authenticated caller from request extensions; the system:masters
/// short-circuit makes admin's self-review always `allowed: true`.
pub async fn self_subject_access_review(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let review: SelfSubjectAccessReview =
        decode_sar_body(&headers, &raw, "SelfSubjectAccessReview")?;
    let attrs = attributes_from_spec(
        user_info.0,
        review.spec.resource_attributes.as_ref(),
        review.spec.non_resource_attributes.as_ref(),
    );
    let decision = state.authorizer.authorize(&attrs).await;
    let out = SelfSubjectAccessReview {
        api_version: "authorization.k8s.io/v1".to_string(),
        kind: "SelfSubjectAccessReview".to_string(),
        spec: review.spec,
        status: Some(status_for(decision)),
    };
    Ok((StatusCode::CREATED, Json(out)).into_response())
}

/// `POST /apis/authorization.k8s.io/v1/selfsubjectrulesreviews` — enumerate the
/// caller's applicable rules in the spec's namespace (`kubectl auth can-i
/// --list`). For `system:masters` the authorizer returns the wildcard `*.*`.
pub async fn self_subject_rules_review(
    State(state): State<RouterState>,
    user_info: ExtractUserInfo,
    headers: HeaderMap,
    raw: Bytes,
) -> Result<Response, ApiError> {
    let review: SelfSubjectRulesReview = decode_sar_body(&headers, &raw, "SelfSubjectRulesReview")?;
    let rules = state
        .authorizer
        .rules_for(&user_info.0, review.spec.namespace.as_deref())
        .await;
    let out = SelfSubjectRulesReview {
        api_version: "authorization.k8s.io/v1".to_string(),
        kind: "SelfSubjectRulesReview".to_string(),
        spec: review.spec,
        status: Some(rules_status(rules)),
    };
    Ok((StatusCode::CREATED, Json(out)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_from_resource_spec() {
        let r = ResourceAttributes {
            namespace: Some("default".into()),
            verb: Some("get".into()),
            resource: Some("pods".into()),
            ..Default::default()
        };
        let a = attributes_from_spec(UserInfo::anonymous(), Some(&r), None);
        assert_eq!(a.verb, "get");
        assert_eq!(a.resource, "pods");
        assert_eq!(a.namespace.as_deref(), Some("default"));
        assert!(a.non_resource_url.is_none());
    }

    #[test]
    fn attributes_from_non_resource_spec() {
        let nr = NonResourceAttributes {
            path: Some("/healthz".into()),
            verb: Some("get".into()),
        };
        let a = attributes_from_spec(UserInfo::anonymous(), None, Some(&nr));
        assert_eq!(a.verb, "get");
        assert_eq!(a.non_resource_url.as_deref(), Some("/healthz"));
        assert!(a.is_non_resource());
    }

    #[test]
    fn subject_user_uses_spec_not_caller() {
        let spec = SubjectAccessReviewSpec {
            user: Some("test-user".into()),
            groups: Some(vec!["g1".into()]),
            ..Default::default()
        };
        let u = subject_user(&spec);
        assert_eq!(u.username, "test-user");
        assert_eq!(u.groups, vec!["g1".to_string()]);
    }

    #[test]
    fn status_for_maps_decisions() {
        assert!(status_for(Decision::Allow).allowed);
        assert!(!status_for(Decision::NoOpinion).allowed);
        assert!(!status_for(Decision::NoOpinion).denied);
        assert!(status_for(Decision::Deny).denied);
        assert!(!status_for(Decision::Deny).allowed);
    }

    #[test]
    fn sar_status_serializes_camel_case() {
        let s = SubjectAccessReviewStatus {
            allowed: true,
            denied: false,
            reason: "ok".into(),
            evaluation_error: String::new(),
        };
        let v: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert_eq!(v["allowed"], true);
        // denied=false + empty evaluationError are skip-if-empty.
        assert!(v.get("denied").is_none());
        assert!(v.get("evaluationError").is_none());
    }
}
