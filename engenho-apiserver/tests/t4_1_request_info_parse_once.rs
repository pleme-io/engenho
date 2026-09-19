//! T4.1 — the request is parsed ONCE, and what authz judges is what dispatch
//! acts on.
//!
//! Before T4.1 the authz layer classified `req.uri().path()` (the RAW,
//! still-percent-encoded path) while dispatch split axum's percent-DECODED
//! `*rest`. `POST .../serviceaccounts/foo%2Ftoken` was judged as a plain
//! `create` of a ServiceAccount literally named `foo%2Ftoken`, and dispatched
//! as a TokenRequest minted for `foo`. The watch flag had the same split: authz
//! read `?watch` off the raw query with one truth table, dispatch read it
//! through serde with another.
//!
//! Every test here records the [`Attributes`] the authorizer was handed and the
//! decision it returned, then checks the response the SAME request produced.
//! Each assertion is "authz and dispatch agree", never "this identity may do
//! this": the RBAC matcher's own reach is T4.2's business, and a test that
//! required a particular decision would pin whichever matcher shipped.
//!
//! Boots an in-process store + a plaintext apiserver, as `m0_5_rbac` does.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use engenho_apiserver::sa_token::SaIssuer;
use engenho_apiserver::{
    AllowAllAuthorizer, ApiServer, Attributes, Authorizer, Decision, RbacAuthorizer,
    ResourceHandler, RouterState, StoreRbacEnv, handlers_from_catalog,
};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use engenho_types::generated_v1_34::rbac_v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use engenho_types::meta::ObjectMeta;

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

/// The one request the plan names: a `/token` subresource whose separating
/// slash arrives percent-encoded.
const ENCODED_TOKEN_PATH: &str = "/api/v1/namespaces/default/serviceaccounts/foo%2Ftoken";

/// Wraps a real authorizer and records every `(Attributes, Decision)` pair it
/// returned, so a test can compare what was JUDGED with what was DISPATCHED.
struct Recorder {
    inner: Arc<dyn Authorizer>,
    seen: Mutex<Vec<(Attributes, Decision)>>,
}

impl Recorder {
    fn wrapping(inner: Arc<dyn Authorizer>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            seen: Mutex::new(Vec::new()),
        })
    }

    /// Everything recorded so far, oldest first.
    fn seen(&self) -> Vec<(Attributes, Decision)> {
        self.seen.lock().expect("recorder mutex").clone()
    }
}

#[async_trait::async_trait]
impl Authorizer for Recorder {
    async fn authorize(&self, attrs: &Attributes) -> Decision {
        let decision = self.inner.authorize(attrs).await;
        self.seen
            .lock()
            .expect("recorder mutex")
            .push((attrs.clone(), decision));
        decision
    }
}

/// Boot a store holding ServiceAccount `default/foo` and a Role that grants
/// ONLY `create` on `serviceaccounts`, bound to every unauthenticated caller.
/// Start an apiserver whose authorizer is `authorizer(store)` wrapped in a
/// [`Recorder`], with a token issuer installed so `/token` really mints.
async fn boot(
    authorizer: impl FnOnce(Arc<StoreMesh>) -> Arc<dyn Authorizer>,
) -> (String, ApiServer, Arc<Recorder>) {
    let router = InProcessRouter::new();
    let cfg = default_config("apiserver-t4-1").unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);

    put(
        &store,
        ResourceKey::namespaced("", "v1", "ServiceAccount", "default", "foo"),
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": "foo", "namespace": "default", "uid": "sa-foo-uid"},
        }),
    )
    .await;

    let role = Role {
        metadata: meta("sa-creator"),
        rules: vec![PolicyRule {
            verbs: vec!["create".into()],
            api_groups: vec!["".into()],
            resources: vec!["serviceaccounts".into()],
            ..Default::default()
        }],
    };
    put(
        &store,
        ResourceKey::namespaced(RBAC_GROUP, "v1", "Role", "default", "sa-creator"),
        serde_json::to_value(&role).unwrap(),
    )
    .await;
    let binding = RoleBinding {
        metadata: meta("bind-sa-creator"),
        role_ref: RoleRef {
            api_group: RBAC_GROUP.into(),
            kind: "Role".into(),
            name: "sa-creator".into(),
        },
        subjects: vec![Subject {
            kind: "Group".into(),
            api_group: Some(RBAC_GROUP.into()),
            name: "system:unauthenticated".into(),
            namespace: None,
        }],
    };
    put(
        &store,
        ResourceKey::namespaced(
            RBAC_GROUP,
            "v1",
            "RoleBinding",
            "default",
            "bind-sa-creator",
        ),
        serde_json::to_value(&binding).unwrap(),
    )
    .await;

    let recorder = Recorder::wrapping(authorizer(store.clone()));
    let handlers: Vec<Arc<dyn ResourceHandler>> = handlers_from_catalog(store.clone());
    let issuer = Arc::new(SaIssuer {
        signing: ed25519_dalek::SigningKey::from_bytes(&[41u8; 32]),
        issuer: "https://kubernetes.default.svc".into(),
        default_audience: "https://kubernetes.default.svc".into(),
    });
    let state = RouterState::new(handlers)
        .with_authorizer(recorder.clone())
        .with_token_issuer(issuer);
    let server = ApiServer::start_with_state("127.0.0.1:0".parse().unwrap(), state, None)
        .await
        .unwrap();
    let base = ["http://127.0.0.1:", &server.local_addr().port().to_string()].concat();
    (base, server, recorder)
}

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        ..Default::default()
    }
}

async fn put(store: &StoreMesh, key: ResourceKey, value: serde_json::Value) {
    store
        .propose(ResourceCommand::Put {
            key,
            value,
            expected: None,
            reason: Reason::Operator,
        })
        .await
        .unwrap();
}

/// `true` iff `attrs` is the token subresource of ServiceAccount `default/foo`
/// under `create` — the ONE reading of [`ENCODED_TOKEN_PATH`].
fn is_foo_token_create(attrs: &Attributes) -> bool {
    attrs.verb == "create"
        && attrs.group.is_empty()
        && attrs.resource == "serviceaccounts"
        && attrs.subresource.as_deref() == Some("token")
        && attrs.name.as_deref() == Some("foo")
        && attrs.namespace.as_deref() == Some("default")
        && attrs.non_resource_url.is_none()
}

/// POST [`ENCODED_TOKEN_PATH`] anonymously; returns `(status, body)`.
async fn post_encoded_token(base: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let url = [base, ENCODED_TOKEN_PATH].concat();
    // The client must put the `%2F` on the wire verbatim, or this test proves
    // nothing about decoding.
    assert!(
        reqwest::Url::parse(&url)
            .unwrap()
            .path()
            .contains("foo%2Ftoken"),
        "the request path keeps its percent-encoded slash"
    );
    let resp = reqwest::Client::new()
        .post(url)
        .json(&serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": {}
        }))
        .send()
        .await
        .expect("POST the encoded token path");
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// Assert the response is the TokenRequest minted for `default/foo`.
fn assert_minted_for_foo(status: reqwest::StatusCode, body: &serde_json::Value) {
    assert_eq!(status, reqwest::StatusCode::CREATED, "token minted: {body}");
    assert_eq!(body["kind"], "TokenRequest", "dispatched to /token: {body}");
    assert_eq!(body["metadata"]["name"], "foo", "minted for foo: {body}");
    assert_eq!(body["metadata"]["namespace"], "default", "{body}");
    assert!(
        body["status"]["token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "a real token came back: {body}"
    );
}

#[tokio::test]
async fn an_encoded_slash_is_judged_as_the_subresource_dispatch_serves() {
    let (base, server, recorder) = boot(|_| Arc::new(AllowAllAuthorizer)).await;

    let (status, body) = post_encoded_token(&base).await;

    // Dispatch serves the token subresource of `foo`...
    assert_minted_for_foo(status, &body);
    // ...so that, and nothing else, is what authz must have judged.
    let seen = recorder.seen();
    assert_eq!(seen.len(), 1, "authz ran exactly once: {seen:?}");
    assert!(
        is_foo_token_create(&seen[0].0),
        "authz judged (create, serviceaccounts, token, foo): {:?}",
        seen[0].0
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_create_serviceaccounts_role_is_judged_on_the_token_subresource() {
    // The plan's scenario: a Role granting ONLY `create` on `serviceaccounts`.
    // Whatever the RBAC matcher decides, it decides it about the request that
    // is actually served. Since T4.2 a bare parent rule no longer reaches a
    // subresource, so this takes the 403 arm. Both arms stay: this test pins
    // agreement between authz and dispatch, not the matcher's reach, which
    // `authz::tests::upstream_v1_34` and `m0_5_rbac` pin.
    let (base, server, recorder) = boot(|store| {
        Arc::new(RbacAuthorizer::new(StoreRbacEnv::new(store))) as Arc<dyn Authorizer>
    })
    .await;

    let (status, body) = post_encoded_token(&base).await;

    let seen = recorder.seen();
    assert_eq!(seen.len(), 1, "authz ran exactly once: {seen:?}");
    let (attrs, decision) = &seen[0];
    assert!(
        is_foo_token_create(attrs),
        "authz judged (create, serviceaccounts, token, foo): {attrs:?}"
    );
    match decision {
        Decision::Allow => assert_minted_for_foo(status, &body),
        Decision::Deny | Decision::NoOpinion => {
            assert_eq!(status, reqwest::StatusCode::FORBIDDEN, "{body}");
            let msg = body["message"].as_str().unwrap_or_default();
            assert!(
                msg.contains("\"serviceaccounts/token\""),
                "the refusal names the subresource that was judged: {msg:?}"
            );
        }
    }

    server.shutdown().await.unwrap();
}

/// GET `path_and_query` anonymously and return the response body text. A
/// watch is bounded by the `timeoutSeconds` the caller puts in the query.
async fn get_body(base: &str, path_and_query: &str) -> String {
    reqwest::Client::new()
        .get([base, path_and_query].concat())
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("GET configmaps")
        .text()
        .await
        .unwrap_or_default()
}

/// The verb authz judged for the most recent request.
fn last_judged_verb(recorder: &Recorder) -> Option<String> {
    recorder.seen().last().map(|(a, _)| a.verb.clone())
}

#[tokio::test]
async fn the_watch_flag_is_read_once_for_authz_and_dispatch() {
    let (base, server, recorder) = boot(|_| Arc::new(AllowAllAuthorizer)).await;

    // `?watch` with no value. It is not a watch request to the list/watch
    // dispatcher, so authz must not judge it as one either.
    let body = get_body(&base, "/api/v1/namespaces/default/configmaps?watch").await;
    assert_eq!(
        last_judged_verb(&recorder).as_deref(),
        Some("list"),
        "judged a list"
    );
    let listed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(listed["kind"], "ConfigMapList", "dispatched a list: {body}");

    // `?watch=yes` IS a watch to the dispatcher, so authz must judge `watch`,
    // not `list` (a list-only grant must not open a stream).
    let body = get_body(
        &base,
        "/api/v1/namespaces/default/configmaps?watch=yes&timeoutSeconds=1",
    )
    .await;
    assert_eq!(
        last_judged_verb(&recorder).as_deref(),
        Some("watch"),
        "judged a watch"
    );
    assert!(
        !body.contains("ConfigMapList"),
        "dispatched a watch stream, not a list: {body}"
    );

    // A watch flag on an INSTANCE path: dispatch serves a single GET, so authz
    // judges `get` (upstream's RequestInfo reads `watch` only on a collection).
    let body = get_body(
        &base,
        "/api/v1/namespaces/default/configmaps/cm1?watch=true",
    )
    .await;
    assert_eq!(
        last_judged_verb(&recorder).as_deref(),
        Some("get"),
        "judged a get"
    );
    let got: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(got["reason"], "NotFound", "dispatched a single GET: {body}");

    server.shutdown().await.unwrap();
}
