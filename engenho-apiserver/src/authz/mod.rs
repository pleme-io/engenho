//! Brick B — the typed RBAC Authorizer (TYPED-SPEC + INTERPRETER TRIPLET).
//!
//! ## The triplet shape
//!
//! This module is the TYPED BORDER + WORKING INTERPRETER + MOCKABLE ENV of the
//! typed-spec triplet for authorization. The author-facing Lisp spec lives at
//! `specs/rbac_authz.lisp`.
//!
//!   1. **Typed border** — [`Attributes`] (the request reduced to authz inputs)
//!      + [`Decision`] (`Allow` / `Deny` / `NoOpinion`). RBAC is an ALLOW-only
//!      authorizer: a matching rule → `Allow`, no match → `NoOpinion` (there are
//!      no deny rules in RBAC). The single-authorizer chain default-denies a
//!      `NoOpinion`.
//!   2. **Working interpreter** — [`RbacAuthorizer`] walks the phases:
//!      `system:masters` short-circuit → gather matching bindings → resolve roles
//!      → match PolicyRules. Every phase returns a typed [`Decision`]; a
//!      malformed stored object contributes `NoOpinion` + a `tracing::warn!`
//!      (never a panic / `todo!` / silent wrong answer).
//!   3. **Mockable env** — [`RbacStoreEnv`] abstracts the Role / ClusterRole /
//!      binding lookup so authorization is unit-testable WITHOUT a live store.
//!      The trait IS the testability contract. The production impl wraps
//!      `Arc<StoreMesh>` ([`crate::authz::store_env`]); the mock impl backs the
//!      same trait with in-memory `Vec`s.
//!
//! ## Behavior preservation (the load-bearing lever)
//!
//! The `system:masters` short-circuit ([`RbacAuthorizer::authorize`] phase a)
//! returns `Allow` for the admin identity (`O=system:masters` cert OR admin
//! bearer → [`UserInfo::admin`]) with ZERO store reads, so EVERY existing live
//! proof still passes. Default-deny applies only to NON-admin identities.
//!
//! The default authorizer installed by every legacy `RouterState::new` caller is
//! [`AllowAllAuthorizer`] — authorize-ALL retained until the runtime installs the
//! real RBAC authorizer via [`RouterState::with_authorizer`].

pub mod sar;
pub mod store_env;

use std::sync::Arc;

use async_trait::async_trait;
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};

/// The super-user group whose presence short-circuits the authorizer to
/// allow-all. The admin kubeconfig (`O=system:masters` cert OR admin bearer)
/// resolves to a [`UserInfo`] carrying this group.
pub const GROUP_MASTERS: &str = "system:masters";

/// A request reduced to the inputs RBAC authorizes on. Built from the HTTP
/// method + URL path (the shared [`crate::coords::parse_request_info`] helper)
/// + the resolved [`UserInfo`] (request extensions). One of `resource` /
/// `non_resource_url` is the discriminant: a resource request carries
/// `resource` (+ optional `subresource` / `namespace` / `name`); a non-resource
/// request (`/healthz`, `/metrics`, `/openapi/v3`) carries `non_resource_url`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attributes {
    /// The authenticated principal making the request.
    pub user: UserInfo,
    /// The RBAC verb (`get` / `list` / `watch` / `create` / `update` / `patch`
    /// / `delete` / `deletecollection` for resources; the lowercased HTTP method
    /// for non-resource paths).
    pub verb: String,
    /// The API group (`""` for core, `apps`, `rbac.authorization.k8s.io`, …).
    pub group: String,
    /// The API version (`v1`, …). Carried for completeness; RBAC matches on
    /// group + resource, not version.
    pub version: String,
    /// The resource plural (`pods`, `deployments`, …). Empty for a non-resource
    /// request.
    pub resource: String,
    /// The subresource (`status`, `scale`), if the request targets one. The
    /// RBAC `resource` match key becomes `resource/subresource`.
    pub subresource: Option<String>,
    /// The namespace, for a namespaced resource request. `None` for
    /// cluster-scoped or non-resource requests.
    pub namespace: Option<String>,
    /// The instance name, for an instance-targeting request. Matched against
    /// `PolicyRule.resource_names` when that field is non-empty.
    pub name: Option<String>,
    /// The non-resource URL (`/healthz`, `/api`, `/openapi/v3/...`), for a
    /// non-resource request. Matched against `PolicyRule.non_resource_urls`.
    pub non_resource_url: Option<String>,
}

impl Attributes {
    /// `true` iff this is a non-resource request (a `nonResourceURL` path, not
    /// a `/api`/`/apis` resource shape).
    #[must_use]
    pub fn is_non_resource(&self) -> bool {
        self.non_resource_url.is_some()
    }

    /// The RBAC resource match key: `resource` or `resource/subresource` when a
    /// subresource is targeted. Empty for a non-resource request.
    #[must_use]
    pub fn resource_key(&self) -> String {
        match &self.subresource {
            Some(sub) => {
                let mut s = String::with_capacity(self.resource.len() + 1 + sub.len());
                s.push_str(&self.resource);
                s.push('/');
                s.push_str(sub);
                s
            }
            None => self.resource.clone(),
        }
    }
}

/// The typed authorizer verdict. RBAC is an ALLOW-only authorizer:
///
///   * [`Decision::Allow`] — a matching rule granted the request.
///   * [`Decision::NoOpinion`] — no rule matched (RBAC has no deny rules; the
///     single-authorizer chain treats `NoOpinion` as a default-deny → 403).
///   * [`Decision::Deny`] — reserved for an explicit deny (no RBAC producer
///     today; present so the type is complete + the chain can carry an explicit
///     deny if a future authorizer emits one).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The request is authorized.
    Allow,
    /// Explicitly denied (no RBAC producer; reserved).
    Deny,
    /// No opinion — the chain default-denies this (the single-authorizer case).
    NoOpinion,
}

impl Decision {
    /// `true` iff this decision authorizes the request (`Allow`).
    #[must_use]
    pub fn is_allow(self) -> bool {
        matches!(self, Decision::Allow)
    }
}

/// The MOCKABLE store env — the testability contract. Abstracts the
/// Role / ClusterRole / binding lookup so [`RbacAuthorizer`] is unit-testable
/// with NO live store. Production impl: [`store_env::StoreRbacEnv`] over
/// `Arc<StoreMesh>`. Mock impl: in-memory `Vec`s (see this module's tests).
///
/// Async to match `StoreMesh`'s async catalog reads without blocking the tokio
/// runtime.
#[async_trait]
pub trait RbacStoreEnv: Send + Sync {
    /// All `ClusterRoleBinding`s in the cluster.
    async fn list_cluster_role_bindings(&self) -> Vec<ClusterRoleBinding>;
    /// All `RoleBinding`s in namespace `ns`.
    async fn list_role_bindings(&self, ns: &str) -> Vec<RoleBinding>;
    /// Resolve a `ClusterRole` by name (`None` if absent).
    async fn get_cluster_role(&self, name: &str) -> Option<ClusterRole>;
    /// Resolve a namespaced `Role` by `(ns, name)` (`None` if absent).
    async fn get_role(&self, ns: &str, name: &str) -> Option<Role>;
}

/// The typed authorizer trait. The router's authz middleware + the SAR handlers
/// share an `Arc<dyn Authorizer>` and call [`Authorizer::authorize`].
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Authorize `attrs`, returning a typed [`Decision`].
    async fn authorize(&self, attrs: &Attributes) -> Decision;

    /// Enumerate the rules that apply to `attrs.user` in `namespace` — the
    /// backing of `SelfSubjectRulesReview` (`kubectl auth can-i --list`).
    /// Default impl returns the empty rule set (the [`AllowAllAuthorizer`]
    /// overrides it with the wildcard); [`RbacAuthorizer`] enumerates the real
    /// rules.
    async fn rules_for(&self, user: &UserInfo, namespace: Option<&str>) -> EffectiveRules {
        let _ = (user, namespace);
        EffectiveRules::default()
    }
}

/// The rules that apply to a subject in a namespace — the typed result of
/// `SelfSubjectRulesReview` enumeration. Split into resource rules + non-resource
/// rules, mirroring `authorization/v1.SubjectRulesReviewStatus`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EffectiveRules {
    /// The resource-targeting PolicyRules (verbs × apiGroups × resources × …).
    pub resource_rules: Vec<PolicyRule>,
    /// The non-resource PolicyRules (verbs × nonResourceURLs).
    pub non_resource_rules: Vec<PolicyRule>,
    /// `true` iff enumeration was incomplete (e.g. a dangling roleRef). Maps to
    /// `SubjectRulesReviewStatus.incomplete`.
    pub incomplete: bool,
}

/// The default authorizer — authorize-ALL, byte-identical to the pre-Brick-B
/// behavior. Every legacy `RouterState::new` caller / test installs this so the
/// authz middleware is a no-op until the runtime installs the real RBAC
/// authorizer. Its `rules_for` returns the wildcard `*.*` so a
/// `SelfSubjectRulesReview` under allow-all shows full access.
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(&self, _attrs: &Attributes) -> Decision {
        Decision::Allow
    }

    async fn rules_for(&self, _user: &UserInfo, _namespace: Option<&str>) -> EffectiveRules {
        EffectiveRules {
            resource_rules: vec![PolicyRule {
                verbs: vec!["*".to_string()],
                api_groups: vec!["*".to_string()],
                resources: vec!["*".to_string()],
                ..Default::default()
            }],
            non_resource_rules: vec![PolicyRule {
                verbs: vec!["*".to_string()],
                non_resource_urls: vec!["*".to_string()],
                ..Default::default()
            }],
            incomplete: false,
        }
    }
}

/// The RBAC authorizer interpreter, generic over the [`RbacStoreEnv`] so the
/// same walk drives the production store + the in-memory mock.
pub struct RbacAuthorizer<E: RbacStoreEnv> {
    env: E,
}

impl<E: RbacStoreEnv> RbacAuthorizer<E> {
    /// Build the authorizer over a store env.
    #[must_use]
    pub fn new(env: E) -> Self {
        Self { env }
    }

    /// The wildcard `*.*` rule set returned for a `system:masters` subject in
    /// `rules_for` — so `kubectl auth can-i --list` shows `*.*` for admin.
    fn masters_rules() -> EffectiveRules {
        EffectiveRules {
            resource_rules: vec![PolicyRule {
                verbs: vec!["*".to_string()],
                api_groups: vec!["*".to_string()],
                resources: vec!["*".to_string()],
                ..Default::default()
            }],
            non_resource_rules: vec![PolicyRule {
                verbs: vec!["*".to_string()],
                non_resource_urls: vec!["*".to_string()],
                ..Default::default()
            }],
            incomplete: false,
        }
    }
}

#[async_trait]
impl<E: RbacStoreEnv> Authorizer for RbacAuthorizer<E> {
    async fn authorize(&self, attrs: &Attributes) -> Decision {
        // Phase a — system:masters short-circuit (ZERO store reads). THE
        // behavior-preservation lever: the admin kubeconfig keeps allow-all so
        // every existing live proof still passes.
        if attrs.user.groups.iter().any(|g| g == GROUP_MASTERS) {
            return Decision::Allow;
        }

        // Phase b — gather the bindings whose subjects match this identity
        // (a cheap filter before any role resolution).
        let mut matched_refs: Vec<(RoleRef, Option<String>)> = Vec::new();
        for crb in self.env.list_cluster_role_bindings().await {
            if subjects_match(&crb.subjects, &attrs.user, None) {
                // A ClusterRoleBinding's roleRef resolves cluster-wide; no
                // binding namespace.
                matched_refs.push((crb.role_ref, None));
            }
        }
        if let Some(ns) = attrs.namespace.as_deref() {
            for rb in self.env.list_role_bindings(ns).await {
                if subjects_match(&rb.subjects, &attrs.user, Some(ns)) {
                    // A RoleBinding's roleRef is resolved IN this namespace
                    // (a Role) or cluster-wide (a ClusterRole applied in-ns).
                    matched_refs.push((rb.role_ref, Some(ns.to_string())));
                }
            }
        }

        // Phase c + d — resolve each ref to its PolicyRules + match. First
        // matching rule → Allow.
        for (role_ref, binding_ns) in matched_refs {
            let rules = self.resolve_rules(&role_ref, binding_ns.as_deref()).await;
            for rule in &rules {
                if rule_grants(rule, attrs) {
                    return Decision::Allow;
                }
            }
        }

        // Phase e — no match → NoOpinion (the chain default-denies).
        Decision::NoOpinion
    }

    async fn rules_for(&self, user: &UserInfo, namespace: Option<&str>) -> EffectiveRules {
        // system:masters → the wildcard, so `kubectl auth can-i --list` shows
        // *.* for admin (no store reads).
        if user.groups.iter().any(|g| g == GROUP_MASTERS) {
            return Self::masters_rules();
        }

        let mut out = EffectiveRules::default();
        let mut matched_refs: Vec<(RoleRef, Option<String>)> = Vec::new();
        for crb in self.env.list_cluster_role_bindings().await {
            if subjects_match(&crb.subjects, user, None) {
                matched_refs.push((crb.role_ref, None));
            }
        }
        if let Some(ns) = namespace {
            for rb in self.env.list_role_bindings(ns).await {
                if subjects_match(&rb.subjects, user, Some(ns)) {
                    matched_refs.push((rb.role_ref, Some(ns.to_string())));
                }
            }
        }
        for (role_ref, binding_ns) in matched_refs {
            let rules = self.resolve_rules(&role_ref, binding_ns.as_deref()).await;
            if rules.is_empty() {
                // A dangling roleRef contributed no rules → enumeration is
                // incomplete (mirrors upstream's tolerant resolution).
                out.incomplete = true;
                continue;
            }
            for rule in rules {
                if rule.non_resource_urls.is_empty() {
                    out.resource_rules.push(rule);
                } else {
                    out.non_resource_rules.push(rule);
                }
            }
        }
        out
    }
}

impl<E: RbacStoreEnv> RbacAuthorizer<E> {
    /// Resolve a `roleRef` to its `PolicyRule`s. A `ClusterRole` ref resolves
    /// cluster-wide; a `Role` ref resolves in `binding_ns` (a `Role` ref is only
    /// valid from a `RoleBinding`, so `binding_ns` is always `Some` there). A
    /// dangling ref → an empty rule set (NoOpinion contribution + a warn — never
    /// a hard error, matching upstream's tolerant resolution).
    async fn resolve_rules(&self, role_ref: &RoleRef, binding_ns: Option<&str>) -> Vec<PolicyRule> {
        match role_ref.kind.as_str() {
            "ClusterRole" => match self.env.get_cluster_role(&role_ref.name).await {
                Some(cr) => cr.rules,
                None => {
                    tracing::warn!(
                        role = %role_ref.name,
                        "RBAC: dangling ClusterRole roleRef — skipped (no rules)"
                    );
                    Vec::new()
                }
            },
            "Role" => {
                let Some(ns) = binding_ns else {
                    tracing::warn!(
                        role = %role_ref.name,
                        "RBAC: Role roleRef from a non-namespaced binding — skipped"
                    );
                    return Vec::new();
                };
                match self.env.get_role(ns, &role_ref.name).await {
                    Some(r) => r.rules,
                    None => {
                        tracing::warn!(
                            role = %role_ref.name,
                            namespace = %ns,
                            "RBAC: dangling Role roleRef — skipped (no rules)"
                        );
                        Vec::new()
                    }
                }
            }
            other => {
                tracing::warn!(
                    kind = %other,
                    "RBAC: unknown roleRef kind — skipped"
                );
                Vec::new()
            }
        }
    }
}

/// `true` iff any subject in `subjects` matches the requesting `user`.
///
///   * `kind=="User"`           → `name == user.username`.
///   * `kind=="Group"`          → `user.groups` contains `name`.
///   * `kind=="ServiceAccount"` → matched as user
///     `system:serviceaccount:<ns>:<name>` (the upstream SA username). The SA
///     namespace comes from the subject's `namespace` (or, for a RoleBinding,
///     the binding namespace `default_sa_ns`).
fn subjects_match(subjects: &[Subject], user: &UserInfo, default_sa_ns: Option<&str>) -> bool {
    subjects.iter().any(|s| match s.kind.as_str() {
        "User" => s.name == user.username,
        "Group" => user.groups.iter().any(|g| g == &s.name),
        "ServiceAccount" => {
            let ns = s.namespace.as_deref().or(default_sa_ns).unwrap_or_default();
            let sa_user = ["system:serviceaccount:", ns, ":", &s.name].concat();
            sa_user == user.username
        }
        _ => false,
    })
}

/// `true` iff `rule` grants `attrs`.
///
///   * Non-resource request: the verb must match AND the path must match one of
///     the rule's `non_resource_urls` (exact OR a trailing `/*` prefix-glob).
///   * Resource request: the verb, apiGroup, and resource key must each match
///     (`*` wildcard accepted on each), AND (`resource_names` empty OR
///     `attrs.name ∈ resource_names`).
fn rule_grants(rule: &PolicyRule, attrs: &Attributes) -> bool {
    if attrs.is_non_resource() {
        // A non-resource rule has non_resource_urls; a resource rule never
        // matches a non-resource request.
        let url = attrs.non_resource_url.as_deref().unwrap_or("");
        verb_matches(&rule.verbs, &attrs.verb)
            && rule
                .non_resource_urls
                .iter()
                .any(|pat| non_resource_url_matches(pat, url))
    } else {
        verb_matches(&rule.verbs, &attrs.verb)
            && api_group_matches(&rule.api_groups, &attrs.group)
            && resource_matches(&rule.resources, attrs)
            && resource_name_matches(&rule.resource_names, attrs.name.as_deref())
    }
}

/// `true` iff `verb` is granted by `verbs` (`*` wildcard or exact).
fn verb_matches(verbs: &[String], verb: &str) -> bool {
    verbs.iter().any(|v| v == "*" || v == verb)
}

/// `true` iff `group` is granted by `api_groups` (`*` wildcard or exact).
fn api_group_matches(api_groups: &[String], group: &str) -> bool {
    api_groups.iter().any(|g| g == "*" || g == group)
}

/// `true` iff the request's resource key is granted by `resources`. A subresource
/// request (`resource/subresource`) is granted by `*`, by the exact
/// `resource/subresource`, by `resource/*`, OR by the bare `resource` (upstream
/// matches a subresource against the parent resource too). A base-resource
/// request is granted by `*` or the exact `resource`.
fn resource_matches(resources: &[String], attrs: &Attributes) -> bool {
    let key = attrs.resource_key();
    for r in resources {
        if r == "*" || r == &key {
            return true;
        }
        if let Some(sub) = &attrs.subresource {
            // `resource/*` matches any subresource of `resource`.
            let parent_glob = [attrs.resource.as_str(), "/*"].concat();
            if r == &parent_glob {
                return true;
            }
            // The bare parent `resource` matches the subresource too.
            if r == &attrs.resource {
                let _ = sub;
                return true;
            }
        }
    }
    false
}

/// `true` iff `name` (the requested instance, if any) is permitted by
/// `resource_names`. An empty `resource_names` permits all names; a non-empty
/// one restricts to listed names (a collection request — `name == None` —
/// against a name-restricted rule does NOT match).
fn resource_name_matches(resource_names: &[String], name: Option<&str>) -> bool {
    if resource_names.is_empty() {
        return true;
    }
    match name {
        Some(n) => resource_names.iter().any(|rn| rn == n),
        None => false,
    }
}

/// `true` iff the non-resource `url` matches the rule pattern `pat`. A pattern
/// ending in `/*` is a prefix-glob (`/apis/*` matches `/apis/apps/v1`); a bare
/// `*` matches anything; otherwise an exact match.
fn non_resource_url_matches(pat: &str, url: &str) -> bool {
    if pat == "*" {
        return true;
    }
    if let Some(prefix) = pat.strip_suffix("/*") {
        // `/apis/*` matches `/apis` itself AND any `/apis/...` subpath.
        return url == prefix || url.starts_with(&[prefix, "/"].concat());
    }
    pat == url
}

/// Wrap any [`Authorizer`] as `Arc<dyn Authorizer>` — the shape `RouterState`
/// + the SAR handlers store.
#[must_use]
pub fn into_dyn_authorizer<A: Authorizer + 'static>(a: A) -> Arc<dyn Authorizer> {
    Arc::new(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use engenho_types::meta::ObjectMeta;

    /// In-memory mock env — the testability contract. Backs the same trait with
    /// `Vec`s + `HashMap`s. A read-counter proves the `system:masters`
    /// short-circuit does ZERO store reads.
    #[derive(Default)]
    struct MockEnv {
        cluster_role_bindings: Vec<ClusterRoleBinding>,
        role_bindings: HashMap<String, Vec<RoleBinding>>,
        cluster_roles: HashMap<String, ClusterRole>,
        roles: HashMap<(String, String), Role>,
        reads: AtomicUsize,
    }

    impl MockEnv {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RbacStoreEnv for MockEnv {
        async fn list_cluster_role_bindings(&self) -> Vec<ClusterRoleBinding> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.cluster_role_bindings.clone()
        }
        async fn list_role_bindings(&self, ns: &str) -> Vec<RoleBinding> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.role_bindings.get(ns).cloned().unwrap_or_default()
        }
        async fn get_cluster_role(&self, name: &str) -> Option<ClusterRole> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.cluster_roles.get(name).cloned()
        }
        async fn get_role(&self, ns: &str, name: &str) -> Option<Role> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.roles.get(&(ns.to_string(), name.to_string())).cloned()
        }
    }

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            extra: Default::default(),
        }
    }

    fn attrs_resource(
        user: UserInfo,
        verb: &str,
        group: &str,
        resource: &str,
        ns: Option<&str>,
        name: Option<&str>,
    ) -> Attributes {
        Attributes {
            user,
            verb: verb.to_string(),
            group: group.to_string(),
            version: "v1".to_string(),
            resource: resource.to_string(),
            subresource: None,
            namespace: ns.map(str::to_string),
            name: name.map(str::to_string),
            non_resource_url: None,
        }
    }

    fn meta(name: &str) -> ObjectMeta {
        ObjectMeta {
            name: name.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn system_masters_allows_all_with_zero_store_reads() {
        let env = MockEnv::default();
        // Hold a raw ref to read the counter AFTER the authorize call.
        let authz = RbacAuthorizer::new(env);
        let admin = user("engenho-admin", &["system:masters", "system:authenticated"]);

        // Any verb/resource → Allow.
        let d = authz
            .authorize(&attrs_resource(
                admin.clone(),
                "delete",
                "",
                "secrets",
                Some("kube-system"),
                Some("anything"),
            ))
            .await;
        assert_eq!(d, Decision::Allow);

        // A wildcard `* *` self-review shape → Allow too.
        let d2 = authz
            .authorize(&attrs_resource(admin, "*", "*", "*", None, None))
            .await;
        assert_eq!(d2, Decision::Allow);

        // ZERO store reads — the short-circuit never touched the env.
        assert_eq!(
            authz.env.reads(),
            0,
            "system:masters short-circuit reads nothing"
        );
    }

    #[tokio::test]
    async fn bound_role_grants_exactly_its_verbs() {
        let mut env = MockEnv::default();
        env.roles.insert(
            ("default".to_string(), "pod-reader".to_string()),
            Role {
                metadata: meta("pod-reader"),
                rules: vec![PolicyRule {
                    verbs: vec!["get".into(), "list".into()],
                    api_groups: vec!["".into()],
                    resources: vec!["pods".into()],
                    ..Default::default()
                }],
            },
        );
        env.role_bindings.insert(
            "default".to_string(),
            vec![RoleBinding {
                metadata: meta("bind-pod-reader"),
                role_ref: RoleRef {
                    api_group: "rbac.authorization.k8s.io".into(),
                    kind: "Role".into(),
                    name: "pod-reader".into(),
                },
                subjects: vec![Subject {
                    kind: "User".into(),
                    name: "test-user".into(),
                    ..Default::default()
                }],
            }],
        );
        let authz = RbacAuthorizer::new(env);
        let u = || user("test-user", &["system:authenticated"]);

        // Granted verb → Allow.
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "get",
                    "",
                    "pods",
                    Some("default"),
                    Some("p1")
                ))
                .await,
            Decision::Allow
        );
        // Ungranted verb → NoOpinion (→ deny).
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "delete",
                    "",
                    "pods",
                    Some("default"),
                    Some("p1")
                ))
                .await,
            Decision::NoOpinion
        );
        // Ungranted resource → NoOpinion.
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "get",
                    "",
                    "secrets",
                    Some("default"),
                    None
                ))
                .await,
            Decision::NoOpinion
        );
    }

    #[tokio::test]
    async fn wildcard_verb_group_resource_match() {
        let mut env = MockEnv::default();
        env.cluster_roles.insert(
            "super".to_string(),
            ClusterRole {
                metadata: meta("super"),
                rules: vec![PolicyRule {
                    verbs: vec!["*".into()],
                    api_groups: vec!["*".into()],
                    resources: vec!["*".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        env.cluster_role_bindings.push(ClusterRoleBinding {
            metadata: meta("bind-super"),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "super".into(),
            },
            subjects: vec![Subject {
                kind: "Group".into(),
                name: "power-users".into(),
                ..Default::default()
            }],
        });
        let authz = RbacAuthorizer::new(env);
        let u = user("alice", &["power-users", "system:authenticated"]);
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u,
                    "patch",
                    "apps",
                    "deployments",
                    Some("x"),
                    Some("d")
                ))
                .await,
            Decision::Allow
        );
    }

    #[tokio::test]
    async fn resource_names_restriction() {
        let mut env = MockEnv::default();
        env.cluster_roles.insert(
            "named".to_string(),
            ClusterRole {
                metadata: meta("named"),
                rules: vec![PolicyRule {
                    verbs: vec!["get".into()],
                    api_groups: vec!["".into()],
                    resources: vec!["configmaps".into()],
                    resource_names: vec!["foo".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        env.cluster_role_bindings.push(ClusterRoleBinding {
            metadata: meta("bind-named"),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "named".into(),
            },
            subjects: vec![Subject {
                kind: "User".into(),
                name: "bob".into(),
                ..Default::default()
            }],
        });
        let authz = RbacAuthorizer::new(env);
        let u = || user("bob", &["system:authenticated"]);
        // get cm "foo" → Allow.
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "get",
                    "",
                    "configmaps",
                    Some("default"),
                    Some("foo")
                ))
                .await,
            Decision::Allow
        );
        // get cm "bar" → NoOpinion (not in resource_names).
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "get",
                    "",
                    "configmaps",
                    Some("default"),
                    Some("bar")
                ))
                .await,
            Decision::NoOpinion
        );
    }

    #[tokio::test]
    async fn subresource_match() {
        let mut env = MockEnv::default();
        env.cluster_roles.insert(
            "status-writer".to_string(),
            ClusterRole {
                metadata: meta("status-writer"),
                rules: vec![PolicyRule {
                    verbs: vec!["patch".into()],
                    api_groups: vec!["".into()],
                    resources: vec!["pods/status".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        env.cluster_role_bindings.push(ClusterRoleBinding {
            metadata: meta("bind-status"),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "status-writer".into(),
            },
            subjects: vec![Subject {
                kind: "User".into(),
                name: "carol".into(),
                ..Default::default()
            }],
        });
        let authz = RbacAuthorizer::new(env);
        let u = || user("carol", &["system:authenticated"]);
        // patch pods/status → Allow.
        let mut a = attrs_resource(u(), "patch", "", "pods", Some("default"), Some("p1"));
        a.subresource = Some("status".to_string());
        assert_eq!(authz.authorize(&a).await, Decision::Allow);
        // patch the base pods (no subresource) → NoOpinion (rule is on
        // pods/status only, which does NOT grant the base pods).
        let base = attrs_resource(u(), "patch", "", "pods", Some("default"), Some("p1"));
        assert_eq!(authz.authorize(&base).await, Decision::NoOpinion);
    }

    #[tokio::test]
    async fn non_resource_url_glob() {
        let mut env = MockEnv::default();
        env.cluster_roles.insert(
            "discovery".to_string(),
            ClusterRole {
                metadata: meta("discovery"),
                rules: vec![PolicyRule {
                    verbs: vec!["get".into()],
                    non_resource_urls: vec!["/api".into(), "/apis".into(), "/apis/*".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        env.cluster_role_bindings.push(ClusterRoleBinding {
            metadata: meta("bind-discovery"),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "discovery".into(),
            },
            subjects: vec![Subject {
                kind: "Group".into(),
                name: "system:authenticated".into(),
                ..Default::default()
            }],
        });
        let authz = RbacAuthorizer::new(env);
        let u = || user("dave", &["system:authenticated"]);
        let nr = |verb: &str, url: &str| Attributes {
            user: u(),
            verb: verb.to_string(),
            group: String::new(),
            version: String::new(),
            resource: String::new(),
            subresource: None,
            namespace: None,
            name: None,
            non_resource_url: Some(url.to_string()),
        };
        assert_eq!(authz.authorize(&nr("get", "/api")).await, Decision::Allow);
        // /apis/* glob matches /apis/apps/v1.
        assert_eq!(
            authz.authorize(&nr("get", "/apis/apps/v1")).await,
            Decision::Allow
        );
        // A resource URL is NOT granted by this non-resource rule.
        assert_eq!(
            authz
                .authorize(&attrs_resource(
                    u(),
                    "get",
                    "",
                    "secrets",
                    Some("kube-system"),
                    None
                ))
                .await,
            Decision::NoOpinion
        );
    }

    #[tokio::test]
    async fn default_deny_for_unbound_user() {
        let env = MockEnv::default();
        let authz = RbacAuthorizer::new(env);
        let u = user("nobody", &["system:authenticated"]);
        assert_eq!(
            authz
                .authorize(&attrs_resource(u, "get", "", "pods", Some("default"), None))
                .await,
            Decision::NoOpinion
        );
    }

    #[tokio::test]
    async fn rolebinding_to_clusterrole_resolution() {
        // A RoleBinding referencing a ClusterRole applies that cluster role's
        // rules IN the binding's namespace.
        let mut env = MockEnv::default();
        env.cluster_roles.insert(
            "view".to_string(),
            ClusterRole {
                metadata: meta("view"),
                rules: vec![PolicyRule {
                    verbs: vec!["get".into(), "list".into()],
                    api_groups: vec!["".into()],
                    resources: vec!["pods".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        env.role_bindings.insert(
            "team-a".to_string(),
            vec![RoleBinding {
                metadata: meta("bind-view"),
                role_ref: RoleRef {
                    api_group: "rbac.authorization.k8s.io".into(),
                    kind: "ClusterRole".into(),
                    name: "view".into(),
                },
                subjects: vec![Subject {
                    kind: "User".into(),
                    name: "erin".into(),
                    ..Default::default()
                }],
            }],
        );
        let authz = RbacAuthorizer::new(env);
        let u = user("erin", &["system:authenticated"]);
        assert_eq!(
            authz
                .authorize(&attrs_resource(u, "list", "", "pods", Some("team-a"), None))
                .await,
            Decision::Allow
        );
    }

    /// A malformed stored object that fails to deserialize is handled in the
    /// production store env (it skips + warns). At the interpreter level, a
    /// dangling roleRef (a binding pointing at a missing role) is the
    /// equivalent NoOpinion-contribution path — proven here: NO panic, just
    /// NoOpinion.
    #[tokio::test]
    async fn dangling_role_ref_is_no_opinion_not_panic() {
        let mut env = MockEnv::default();
        env.cluster_role_bindings.push(ClusterRoleBinding {
            metadata: meta("bind-ghost"),
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".into(),
                kind: "ClusterRole".into(),
                name: "does-not-exist".into(),
            },
            subjects: vec![Subject {
                kind: "User".into(),
                name: "frank".into(),
                ..Default::default()
            }],
        });
        let authz = RbacAuthorizer::new(env);
        let u = user("frank", &["system:authenticated"]);
        assert_eq!(
            authz
                .authorize(&attrs_resource(u, "get", "", "pods", Some("default"), None))
                .await,
            Decision::NoOpinion
        );
    }

    #[tokio::test]
    async fn rules_for_masters_is_wildcard() {
        let env = MockEnv::default();
        let authz = RbacAuthorizer::new(env);
        let admin = user("engenho-admin", &["system:masters"]);
        let rules = authz.rules_for(&admin, None).await;
        assert!(
            rules
                .resource_rules
                .iter()
                .any(|r| r.verbs.contains(&"*".to_string())
                    && r.resources.contains(&"*".to_string()))
        );
        assert!(
            rules
                .non_resource_rules
                .iter()
                .any(|r| r.non_resource_urls.contains(&"*".to_string()))
        );
    }
}
