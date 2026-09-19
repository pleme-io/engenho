//! Brick B — RBAC enforcement integration tests.
//!
//! Boots an in-process store + an apiserver carrying the REAL
//! [`RbacAuthorizer`] over [`StoreRbacEnv`] (NOT the AllowAll default), seeds a
//! Role(get,list pods, ns default) + a RoleBinding to `test-user`, and drives it
//! with `reqwest` over plaintext.
//!
//! Proves the live-bar items at the HTTP boundary:
//!   * BOUND ROLE WORKS — a SubjectAccessReview{user:test-user, get pods,
//!     default} => allowed:true; {delete} => false; {secrets} => false.
//!   * DEFAULT-DENY IS REAL — a SelfSubjectAccessReview for an ungranted verb as
//!     an unbound identity => allowed:false.
//!   * ADMIN UNAFFECTED — a SelfSubjectAccessReview with the admin bearer
//!     (system:masters) => allowed:true; the rules-review shows *.*.
//!   * UNBOUND => 403 — an authenticated-but-unbound request to a protected
//!     resource returns HTTP 403 with the typed K8s Status (reason Forbidden,
//!     `forbidden: User ... cannot ...` message).
//!   * A GRANT REACHES WHAT IT NAMES (T4.2) — `create serviceaccounts` does not
//!     reach `serviceaccounts/token`, and `create serviceaccounts/token` does
//!     not reach `serviceaccounts`, as upstream's `ResourceMatches`.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{
    ApiServer, Authorizer, ChainAuthenticator, RbacAuthorizer, ResourceHandler, RouterState,
    StoreRbacEnv, handlers_from_catalog,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use engenho_types::generated_v1_34::rbac_v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use engenho_types::meta::ObjectMeta;

const ADMIN_TOKEN: &str = "test-admin-bootstrap-token";
const RBAC_GROUP: &str = "rbac.authorization.k8s.io";
const RBAC_VERSION: &str = "v1";

/// Boot a store, seed the bootstrap discovery/basic-user RBAC + a
/// Role(get,list pods)+RoleBinding(test-user), then start a PLAINTEXT apiserver
/// carrying the real RBAC authorizer + the admin bearer token. Returns the base
/// URL + the server handle.
async fn boot_rbac_server() -> (String, ApiServer) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-rbac").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    seed_bootstrap(&store).await;
    seed_test_role(&store).await;

    let handlers: Vec<Arc<dyn ResourceHandler>> = handlers_from_catalog(store.clone());
    let authorizer: Arc<dyn Authorizer> =
        Arc::new(RbacAuthorizer::new(StoreRbacEnv::new(store.clone())));
    let state = RouterState::new(handlers)
        .with_authenticator(Arc::new(ChainAuthenticator::bootstrap(Some(
            ADMIN_TOKEN.to_string(),
        ))))
        .with_authorizer(authorizer);

    // Plaintext (no TLS material) — these tests assert authz behavior, not TLS.
    let server = ApiServer::start_with_state("127.0.0.1:0".parse().unwrap(), state, None)
        .await
        .unwrap();
    let addr = server.local_addr();
    let base = format!("http://127.0.0.1:{}", addr.port());
    drop(store);
    (base, server)
}

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        ..Default::default()
    }
}

/// Seed the bootstrap policy the live bar relies on: system:basic-user (the
/// self-review surface) + system:public-info-viewer (anonymous discovery +
/// health). cluster-admin is unnecessary here (the short-circuit handles
/// system:masters) but the public-info binding is what lets anonymous `/api`
/// resolve through a REAL binding (TIER 2).
async fn seed_bootstrap(store: &StoreMesh) {
    use engenho_types::generated_v1_34::rbac_v1::{ClusterRole, ClusterRoleBinding};

    let group_subj = |name: &str| Subject {
        kind: "Group".into(),
        api_group: Some(RBAC_GROUP.into()),
        name: name.into(),
        namespace: None,
    };
    let cr_ref = |name: &str| RoleRef {
        api_group: RBAC_GROUP.into(),
        kind: "ClusterRole".into(),
        name: name.into(),
    };

    // system:basic-user — selfsubject* create surface.
    let basic = ClusterRole {
        metadata: meta("system:basic-user"),
        rules: vec![PolicyRule {
            verbs: vec!["create".into()],
            api_groups: vec!["authorization.k8s.io".into()],
            resources: vec![
                "selfsubjectaccessreviews".into(),
                "selfsubjectrulesreviews".into(),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    put(store, "ClusterRole", None, "system:basic-user", &basic).await;
    let basic_b = ClusterRoleBinding {
        metadata: meta("system:basic-user"),
        role_ref: cr_ref("system:basic-user"),
        subjects: vec![group_subj("system:authenticated")],
    };
    put(
        store,
        "ClusterRoleBinding",
        None,
        "system:basic-user",
        &basic_b,
    )
    .await;

    // system:public-info-viewer — anonymous discovery + health.
    let public = ClusterRole {
        metadata: meta("system:public-info-viewer"),
        rules: vec![PolicyRule {
            verbs: vec!["get".into()],
            non_resource_urls: vec![
                "/healthz".into(),
                "/livez".into(),
                "/readyz".into(),
                "/version".into(),
                "/version/*".into(),
                "/api".into(),
                "/api/*".into(),
                "/apis".into(),
                "/apis/*".into(),
                "/openapi".into(),
                "/openapi/*".into(),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    put(
        store,
        "ClusterRole",
        None,
        "system:public-info-viewer",
        &public,
    )
    .await;
    let public_b = ClusterRoleBinding {
        metadata: meta("system:public-info-viewer"),
        role_ref: cr_ref("system:public-info-viewer"),
        subjects: vec![
            group_subj("system:authenticated"),
            group_subj("system:unauthenticated"),
        ],
    };
    put(
        store,
        "ClusterRoleBinding",
        None,
        "system:public-info-viewer",
        &public_b,
    )
    .await;
}

/// Seed Role(get,list pods, ns default) + RoleBinding(test-user).
async fn seed_test_role(store: &StoreMesh) {
    let role = Role {
        metadata: meta("pod-reader"),
        rules: vec![PolicyRule {
            verbs: vec!["get".into(), "list".into()],
            api_groups: vec!["".into()],
            resources: vec!["pods".into()],
            ..Default::default()
        }],
    };
    put(store, "Role", Some("default"), "pod-reader", &role).await;

    let binding = RoleBinding {
        metadata: meta("bind-pod-reader"),
        role_ref: RoleRef {
            api_group: RBAC_GROUP.into(),
            kind: "Role".into(),
            name: "pod-reader".into(),
        },
        subjects: vec![Subject {
            kind: "User".into(),
            api_group: Some(RBAC_GROUP.into()),
            name: "test-user".into(),
            namespace: None,
        }],
    };
    put(
        store,
        "RoleBinding",
        Some("default"),
        "bind-pod-reader",
        &binding,
    )
    .await;
}

/// `Put` a typed RBAC value into the store (Reason::Operator).
async fn put<T: serde::Serialize>(
    store: &StoreMesh,
    kind: &str,
    ns: Option<&str>,
    name: &str,
    value: &T,
) {
    let key = match ns {
        Some(ns) => ResourceKey::namespaced(RBAC_GROUP, RBAC_VERSION, kind, ns, name),
        None => ResourceKey::cluster_scoped(RBAC_GROUP, RBAC_VERSION, kind, name),
    };
    store
        .propose(ResourceCommand::Put {
            key,
            value: serde_json::to_value(value).unwrap(),
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

fn sar_url(base: &str) -> String {
    format!("{base}/apis/authorization.k8s.io/v1/subjectaccessreviews")
}
fn self_sar_url(base: &str) -> String {
    format!("{base}/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
}
fn self_rules_url(base: &str) -> String {
    format!("{base}/apis/authorization.k8s.io/v1/selfsubjectrulesreviews")
}

/// POST a SubjectAccessReview for ANOTHER subject and return `status.allowed`.
async fn sar_allowed(client: &reqwest::Client, base: &str, spec: serde_json::Value) -> bool {
    let body = serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "spec": spec
    });
    let resp = client
        .post(sar_url(base))
        // The caller must be authorized to create the SAR — use the admin
        // bearer (system:masters) so the create itself is allowed.
        .bearer_auth(ADMIN_TOKEN)
        .json(&body)
        .send()
        .await
        .expect("SubjectAccessReview POST");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::CREATED,
        "SAR create returns 201"
    );
    let v: serde_json::Value = resp.json().await.unwrap();
    v["status"]["allowed"].as_bool().unwrap_or(false)
}

#[tokio::test]
async fn bound_role_subjectaccessreview() {
    // (3) BOUND ROLE WORKS.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    // get pods in default => allowed.
    assert!(
        sar_allowed(
            &client,
            &base,
            serde_json::json!({
                "user": "test-user",
                "resourceAttributes": {"verb": "get", "resource": "pods", "namespace": "default"}
            })
        )
        .await,
        "test-user CAN get pods in default"
    );
    // delete pods => NOT allowed.
    assert!(
        !sar_allowed(
            &client,
            &base,
            serde_json::json!({
                "user": "test-user",
                "resourceAttributes": {"verb": "delete", "resource": "pods", "namespace": "default"}
            })
        )
        .await,
        "test-user CANNOT delete pods"
    );
    // get secrets => NOT allowed.
    assert!(
        !sar_allowed(
            &client,
            &base,
            serde_json::json!({
                "user": "test-user",
                "resourceAttributes": {"verb": "get", "resource": "secrets", "namespace": "default"}
            })
        )
        .await,
        "test-user CANNOT get secrets"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn admin_self_review_is_allowed_and_rules_are_wildcard() {
    // (1) ADMIN UNAFFECTED — a self-review with the admin bearer => allowed; the
    // rules review shows *.*.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(self_sar_url(&base))
        .bearer_auth(ADMIN_TOKEN)
        .json(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "spec": {"resourceAttributes": {"verb": "create", "resource": "deployments", "group": "apps"}}
        }))
        .send()
        .await
        .expect("admin SelfSubjectAccessReview");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["status"]["allowed"], true,
        "admin can create deployments: {v}"
    );

    // can-i --list => *.* for admin.
    let resp = client
        .post(self_rules_url(&base))
        .bearer_auth(ADMIN_TOKEN)
        .json(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectRulesReview",
            "spec": {"namespace": "default"}
        }))
        .send()
        .await
        .expect("admin SelfSubjectRulesReview");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    let rr = &v["status"]["resourceRules"];
    let has_wildcard = rr
        .as_array()
        .map(|rules| {
            rules.iter().any(|r| {
                r["verbs"]
                    .as_array()
                    .is_some_and(|vs| vs.iter().any(|x| x == "*"))
                    && r["resources"]
                        .as_array()
                        .is_some_and(|rs| rs.iter().any(|x| x == "*"))
            })
        })
        .unwrap_or(false);
    assert!(has_wildcard, "admin rules review shows *.*: {v}");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_deny_for_unbound_user() {
    // (2) DEFAULT-DENY IS REAL — an admin-posted SubjectAccessReview asking about
    // an UNBOUND user (no binding grants it anything) => allowed:false. Using a
    // SAR (admin-posted) keeps the create itself authorized while proving the
    // authorizer default-denies a non-admin identity it has no rule for.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    assert!(
        !sar_allowed(
            &client,
            &base,
            serde_json::json!({
                "user": "unbound-user",
                "groups": ["system:authenticated"],
                "resourceAttributes": {"verb": "delete", "resource": "secrets", "namespace": "kube-system"}
            })
        )
        .await,
        "an authenticated-but-unbound user is default-denied"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unbound_request_to_protected_resource_is_403() {
    // (4) UNBOUND => 403 — an authenticated-but-unbound identity hitting a
    // protected resource returns HTTP 403 with the typed K8s Status. We use the
    // anonymous path (no binding grants it secrets), so the authz layer denies.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/v1/namespaces/kube-system/secrets"))
        .send()
        .await
        .expect("anonymous LIST secrets");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "unbound LIST secrets => 403"
    );
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["kind"], "Status", "typed K8s Status body: {v}");
    assert_eq!(v["reason"], "Forbidden", "reason Forbidden: {v}");
    let msg = v["message"].as_str().unwrap_or("");
    assert!(
        msg.starts_with("forbidden: User "),
        "standard RBAC forbidden message: {msg:?}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn admin_request_to_protected_resource_is_allowed() {
    // (1) ADMIN UNAFFECTED at the request layer — the admin bearer
    // (system:masters) LISTs secrets fine (short-circuit), proving existing
    // flows still pass.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/v1/namespaces/kube-system/secrets"))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .expect("admin LIST secrets");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "admin LIST secrets => 200 (system:masters short-circuit)"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn anonymous_discovery_and_health_open() {
    // (5) DISCOVERY + HEALTH STILL OPEN — anonymous /api, /apis, /version,
    // /healthz, /readyz => 200. /api,/apis via the public-info-viewer +
    // discovery bindings; /version,/healthz,/readyz via pre-authz (TIER 1).
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();

    // TIER 1 — pre-authz health/version (work even with an unseeded store).
    for path in ["/version", "/healthz", "/readyz"] {
        let resp = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "anonymous GET {path} => 200 (pre-authz)"
        );
    }
    // TIER 2 — discovery resolves THROUGH the public-info-viewer binding.
    for path in ["/api", "/apis"] {
        let resp = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "anonymous GET {path} => 200 (binding-driven discovery)"
        );
    }

    server.shutdown().await.unwrap();
}

/// POST `body` to `path` as the admin (system:masters) and require 201.
async fn admin_create(client: &reqwest::Client, base: &str, path: &str, body: serde_json::Value) {
    let resp = client
        .post([base, path].concat())
        .bearer_auth(ADMIN_TOKEN)
        .json(&body)
        .send()
        .await
        .expect("admin create");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "admin create {path}: {text}"
    );
}

/// A Role in `default` holding one rule: `verb` on core-group `resource`.
fn one_rule_role(name: &str, verb: &str, resource: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": name, "namespace": "default"},
        "rules": [{"verbs": [verb], "apiGroups": [""], "resources": [resource]}]
    })
}

/// A RoleBinding in `default` binding Role `role` to one subject.
fn bind(role: &str, subject_kind: &str, subject_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": role, "namespace": "default"},
        "roleRef": {"apiGroup": RBAC_GROUP, "kind": "Role", "name": role},
        "subjects": [{"apiGroup": RBAC_GROUP, "kind": subject_kind, "name": subject_name}]
    })
}

#[tokio::test]
async fn create_serviceaccounts_does_not_grant_the_token_subresource() {
    // (6) T4.2 — the live escalation: before it, holding `create
    // serviceaccounts` minted a token for any ServiceAccount in the namespace
    // through `serviceaccounts/token`.
    let (base, server) = boot_rbac_server().await;
    let client = reqwest::Client::new();
    let roles = "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles";
    let bindings = "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings";

    // Anonymous callers may create ServiceAccounts in `default`, nothing more.
    admin_create(
        &client,
        &base,
        roles,
        one_rule_role("sa-creator", "create", "serviceaccounts"),
    )
    .await;
    admin_create(
        &client,
        &base,
        bindings,
        bind("sa-creator", "Group", "system:unauthenticated"),
    )
    .await;

    // Positive control: the grant is live. Without this, the 403 below could
    // mean only that the binding was never seen.
    let resp = client
        .post(format!("{base}/api/v1/namespaces/default/serviceaccounts"))
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "foo", "namespace": "default"}
        }))
        .send()
        .await
        .expect("anonymous create ServiceAccount");
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "the bare grant reaches serviceaccounts: {text}"
    );

    // The token subresource of that ServiceAccount is refused, and the
    // refusal names what was judged.
    let resp = client
        .post(format!(
            "{base}/api/v1/namespaces/default/serviceaccounts/foo/token"
        ))
        .json(&serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": {}
        }))
        .send()
        .await
        .expect("anonymous POST serviceaccounts/foo/token");
    let status = resp.status();
    let v: serde_json::Value = resp.json().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "create serviceaccounts must not mint a token: {v}"
    );
    let msg = v["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("\"serviceaccounts/token\""),
        "the refusal names the subresource: {msg:?}"
    );

    // A SubjectAccessReview gives the same answer for the same identity.
    let anonymous = |attrs: serde_json::Value| {
        serde_json::json!({
            "user": "system:anonymous",
            "groups": ["system:unauthenticated"],
            "resourceAttributes": attrs
        })
    };
    assert!(
        sar_allowed(
            &client,
            &base,
            anonymous(serde_json::json!({
                "verb": "create", "resource": "serviceaccounts", "namespace": "default"
            }))
        )
        .await,
        "SAR: the bare grant reaches serviceaccounts"
    );
    assert!(
        !sar_allowed(
            &client,
            &base,
            anonymous(serde_json::json!({
                "verb": "create", "resource": "serviceaccounts", "subresource": "token",
                "namespace": "default", "name": "foo"
            }))
        )
        .await,
        "SAR: the bare grant does not reach serviceaccounts/token"
    );

    // The other direction: an exact subresource grant reaches the subresource
    // and not its parent.
    admin_create(
        &client,
        &base,
        roles,
        one_rule_role("token-minter", "create", "serviceaccounts/token"),
    )
    .await;
    admin_create(
        &client,
        &base,
        bindings,
        bind("token-minter", "User", "minter"),
    )
    .await;
    let minter = |attrs: serde_json::Value| {
        serde_json::json!({
            "user": "minter",
            "groups": ["system:authenticated"],
            "resourceAttributes": attrs
        })
    };
    assert!(
        sar_allowed(
            &client,
            &base,
            minter(serde_json::json!({
                "verb": "create", "resource": "serviceaccounts", "subresource": "token",
                "namespace": "default", "name": "foo"
            }))
        )
        .await,
        "SAR: serviceaccounts/token reaches serviceaccounts/token"
    );
    assert!(
        !sar_allowed(
            &client,
            &base,
            minter(serde_json::json!({
                "verb": "create", "resource": "serviceaccounts", "namespace": "default"
            }))
        )
        .await,
        "SAR: serviceaccounts/token does not reach serviceaccounts"
    );

    server.shutdown().await.unwrap();
}
