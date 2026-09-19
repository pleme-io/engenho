//! T4.2's census: which subjects reached a subresource ONLY through a bare
//! parent resource.
//!
//! ## The rule that changed
//!
//! engenho's matcher used to let a rule naming a bare resource (`pods`) grant
//! every subresource of it (`pods/log`, `serviceaccounts/token`). Upstream's
//! `ResourceMatches` never did, and the shipped [`RbacAuthorizer`] no longer
//! does. The census asks, for every binding subject other than
//! `system:masters`: which requests did the OLD rule grant it that the
//! shipped authorizer refuses?
//!
//! ## How it asks without re-implementing the matcher
//!
//! The verdict comes from the shipped [`RbacAuthorizer`], unchanged, over the
//! listed roles and bindings. The census only builds the questions: for each
//! rule reachable from a subject that names a bare resource `R`, one probe per
//! request the old rule granted on a subresource of `R` that engenho SERVES.
//! Each probe the authorizer does not allow is a grant the subject lost.
//!
//! ## Which requests are probed
//!
//! * **Subresources and their groups**: exactly the ones served — every
//!   subresource the generated [`RESOURCE_CATALOG`] declares (the table the
//!   apiserver builds its handlers and its discovery from), and `status` for
//!   each served CRD version that declares it — in each group the rule's
//!   `apiGroups` admits. A subresource nothing serves (`pods/exec` today)
//!   cannot be requested, so no access to it can be lost.
//! * **Verbs and names**: the rule's own. Under `*` the old rule granted
//!   every verb, so the census probes each verb some rule in the cluster
//!   names and one no rule names ([`Dim::Other`]); upstream's matcher treats
//!   every unnamed verb alike (only `*` grants it), so one probe answers for
//!   all of them. An unrestricted rule granted every instance name, probed
//!   the same way. The concrete value sent for [`Dim::Other`] is built to be
//!   absent from every rule, so no rule can name it.
//! * **Who asks**: each binding subject, carrying the groups the
//!   authenticator that issues it puts on every one of its requests
//!   ([`identity`]). A subresource the subject reaches explicitly through
//!   one of those groups (a `pods/log` grant to `system:serviceaccounts`) was
//!   never lost, and a probe without the group would report it anyway.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use async_trait::async_trait;
use engenho_apiserver::authz::{
    Attributes, Authorizer as _, GROUP_MASTERS, RbacAuthorizer, RbacStoreEnv,
};
use engenho_apiserver::sa_token;
use engenho_controllers::CrdController;
use engenho_types::auth::{GROUP_AUTHENTICATED, UserInfo};
use engenho_types::generated_v1_34::catalog::{RESOURCE_CATALOG, Subresource};
use engenho_types::generated_v1_34::rbac_v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef,
};
use serde_json::Value;

use super::{Finding, Match, Subject};

/// The lists the RBAC census reads, as read.
pub(super) struct Listed {
    pub(super) cluster_role_bindings: Vec<Value>,
    pub(super) role_bindings: Vec<Value>,
    pub(super) cluster_roles: Vec<Value>,
    pub(super) roles: Vec<Value>,
    /// For the subresources each CRD serves.
    pub(super) crds: Vec<Value>,
}

/// The RBAC lists decoded into the typed kinds. A value that does not decode
/// is skipped, as the production store env skips it (it contributes no
/// opinion).
struct Typed {
    cluster_role_bindings: Vec<ClusterRoleBinding>,
    role_bindings: Vec<RoleBinding>,
    cluster_roles: BTreeMap<String, ClusterRole>,
    roles: BTreeMap<(String, String), Role>,
}

/// Decode each value as `$ty`, skipping one that does not decode.
macro_rules! decoded {
    ($values:expr, $ty:ty) => {
        $values
            .into_iter()
            .filter_map(|v| serde_json::from_value::<$ty>(v).ok())
    };
}

impl Typed {
    fn of(listed: Listed) -> Self {
        Self {
            cluster_role_bindings: decoded!(listed.cluster_role_bindings, ClusterRoleBinding)
                .collect(),
            role_bindings: decoded!(listed.role_bindings, RoleBinding).collect(),
            cluster_roles: decoded!(listed.cluster_roles, ClusterRole)
                .map(|cr| (cr.metadata.name.clone(), cr))
                .collect(),
            roles: decoded!(listed.roles, Role)
                .map(|r| {
                    let ns = r.metadata.namespace.clone().unwrap_or_default();
                    ((ns, r.metadata.name.clone()), r)
                })
                .collect(),
        }
    }

    /// The rules `role_ref` resolves to from a binding in `binding_ns`
    /// (`None` for a `ClusterRoleBinding`) — the resolution the authorizer
    /// performs: a `ClusterRole` by name, a `Role` only from a `RoleBinding`,
    /// in its namespace.
    fn rules(&self, role_ref: &RoleRef, binding_ns: Option<&str>) -> &[PolicyRule] {
        match (role_ref.kind.as_str(), binding_ns) {
            ("ClusterRole", _) => self
                .cluster_roles
                .get(&role_ref.name)
                .map_or(&[], |cr| cr.rules.as_slice()),
            ("Role", Some(ns)) => self
                .roles
                .get(&(ns.to_owned(), role_ref.name.clone()))
                .map_or(&[], |r| r.rules.as_slice()),
            _ => &[],
        }
    }

    fn every_rule(&self) -> impl Iterator<Item = &PolicyRule> {
        self.cluster_roles
            .values()
            .flat_map(|cr| &cr.rules)
            .chain(self.roles.values().flat_map(|r| &r.rules))
    }
}

/// The listed objects as the authorizer's store env: the same four reads the
/// production env makes against the store.
struct ListedEnv<'a>(&'a Typed);

#[async_trait]
impl RbacStoreEnv for ListedEnv<'_> {
    async fn list_cluster_role_bindings(&self) -> Vec<ClusterRoleBinding> {
        self.0.cluster_role_bindings.clone()
    }

    async fn list_role_bindings(&self, ns: &str) -> Vec<RoleBinding> {
        self.0
            .role_bindings
            .iter()
            .filter(|rb| rb.metadata.namespace.as_deref() == Some(ns))
            .cloned()
            .collect()
    }

    async fn get_cluster_role(&self, name: &str) -> Option<ClusterRole> {
        self.0.cluster_roles.get(name).cloned()
    }

    async fn get_role(&self, ns: &str, name: &str) -> Option<Role> {
        self.0.roles.get(&(ns.to_owned(), name.to_owned())).cloned()
    }
}

/// The URL segment of a catalog subresource. Exhaustive: a subresource the
/// catalog adds does not compile here until it is named.
const fn segment(sub: Subresource) -> &'static str {
    match sub {
        Subresource::Status => "status",
        Subresource::Scale => "scale",
        Subresource::Log => "log",
        Subresource::Token => "token",
    }
}

/// Every subresource engenho serves, by parent resource: `(group,
/// subresource)` pairs under each plural.
struct Served(BTreeMap<String, BTreeSet<(String, &'static str)>>);

impl Served {
    fn of(crds: &[Value]) -> Self {
        let mut served: BTreeMap<String, BTreeSet<(String, &'static str)>> = BTreeMap::new();
        for d in RESOURCE_CATALOG {
            for sub in d.subresources {
                served
                    .entry(d.plural.to_owned())
                    .or_default()
                    .insert((d.group.to_owned(), segment(*sub)));
            }
        }
        for entry in crds.iter().flat_map(CrdController::extract_entries) {
            if entry.serves_status {
                served
                    .entry(entry.plural.clone())
                    .or_default()
                    .insert((entry.group.clone(), segment(Subresource::Status)));
            }
        }
        Self(served)
    }

    /// The served subresources of `parent` in a group `api_groups` admits.
    fn of_parent<'a>(
        &'a self,
        parent: &str,
        api_groups: &'a [String],
    ) -> impl Iterator<Item = (&'a str, &'static str)> {
        let admits = |group: &str| api_groups.iter().any(|g| g == "*" || g == group);
        self.0
            .get(parent)
            .into_iter()
            .flatten()
            .filter(move |(group, _)| admits(group))
            .map(|(group, subresource)| (group.as_str(), *subresource))
    }
}

/// One dimension of a probe: a value some rule in the cluster names, or the
/// class of every value no rule names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Dim {
    /// A value some rule names.
    Named(String),
    /// Any value no rule names; they all get the same answer.
    Other,
}

impl fmt::Display for Dim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(v) => f.write_str(v),
            Self::Other => f.write_str("(any other)"),
        }
    }
}

/// An RBAC subject, as a binding names it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectRef {
    /// `User`, `Group` or `ServiceAccount`.
    pub kind: String,
    /// The subject's name.
    pub name: String,
    /// A `ServiceAccount`'s namespace.
    pub namespace: Option<String>,
}

impl fmt::Display for SubjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.kind)?;
        if let Some(ns) = &self.namespace {
            write!(f, "{ns}/")?;
        }
        f.write_str(&self.name)
    }
}

/// The role a lost grant came from: `ClusterRole edit`, `Role ns-reader`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Via {
    /// `ClusterRole` or `Role`.
    pub kind: String,
    /// The role's name.
    pub name: String,
}

impl fmt::Display for Via {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind, self.name)
    }
}

/// A request the bare-parent rule granted a subject and the shipped
/// authorizer refuses.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LostGrant {
    /// The verb.
    pub verb: Dim,
    /// The served subresource's API group (`""` for core).
    pub group: String,
    /// The bare parent resource the old rule named.
    pub parent: String,
    /// The served subresource.
    pub subresource: &'static str,
    /// The instance name: one the old rule restricted itself to, or, for an
    /// unrestricted rule, one some rule names or the class of all others.
    pub name: Dim,
    /// The namespace, for a grant through a `RoleBinding`; `None` for one
    /// through a `ClusterRoleBinding` (every namespace).
    pub namespace: Option<String>,
    /// The role whose bare-parent rule granted it.
    pub via: Via,
}

impl LostGrant {
    /// `parent/subresource`: what the finding is counted under.
    #[must_use]
    pub fn resource(&self) -> GrantedResource<'_> {
        GrantedResource(self)
    }
}

/// `parent/subresource`, rendered.
pub struct GrantedResource<'a>(&'a LostGrant);

impl fmt::Display for GrantedResource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.0.parent, self.0.subresource)
    }
}

impl fmt::Display for LostGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} (group {:?}, name {}) ",
            self.verb,
            self.resource(),
            self.group,
            self.name
        )?;
        match &self.namespace {
            Some(ns) => write!(f, "in namespace {ns}")?,
            None => f.write_str("cluster-wide")?,
        }
        write!(
            f,
            ": granted only by bare {:?} in {}",
            self.parent, self.via
        )
    }
}

/// Every verb and every instance name some rule in the cluster names.
struct Vocabulary {
    verbs: BTreeSet<String>,
    names: BTreeSet<String>,
}

impl Vocabulary {
    fn of<'a>(rules: impl Iterator<Item = &'a PolicyRule>) -> Self {
        let mut vocabulary = Self {
            verbs: BTreeSet::new(),
            names: BTreeSet::new(),
        };
        for rule in rules {
            vocabulary
                .verbs
                .extend(rule.verbs.iter().filter(|v| *v != "*").cloned());
            vocabulary.names.extend(rule.resource_names.iter().cloned());
        }
        vocabulary
    }
}

/// Each value of `vocabulary`, then the class of all others.
fn every(vocabulary: &BTreeSet<String>) -> Vec<Dim> {
    vocabulary
        .iter()
        .cloned()
        .map(Dim::Named)
        .chain(std::iter::once(Dim::Other))
        .collect()
}

/// A string no member of `named` equals — the concrete value a probe
/// carries for [`Dim::Other`]. Built, not chosen: it grows until it is
/// absent, so no rule can name it.
fn absent_from(named: &BTreeSet<String>) -> String {
    let mut sentinel = String::from("~census-other");
    while named.contains(&sentinel) {
        sentinel.push('~');
    }
    sentinel
}

/// The concrete values a probe sends for [`Dim::Other`].
struct Sentinels {
    verb: String,
    name: String,
}

impl Sentinels {
    fn value<'a>(dim: &'a Dim, sentinel: &'a str) -> &'a str {
        match dim {
            Dim::Named(v) => v,
            Dim::Other => sentinel,
        }
    }
}

/// The group every `ServiceAccount` token carries, whatever its namespace;
/// its namespace group is this, `:`, the namespace. [`sa_token::groups_for`]
/// spells both and the apiserver exports no constant for either, so this
/// copy is pinned to that function by the census test
/// `the_service_account_groups_are_the_authenticators`: a gate, not a type.
pub(super) const GROUP_SERVICEACCOUNTS: &str = "system:serviceaccounts";

/// The identity a probe authenticates as: the subject, carrying the groups
/// the authenticator that issues it puts on EVERY one of its requests. No
/// fewer: a group every request carries can grant the subresource
/// explicitly, and a probe without it reports a loss nobody sees. No more: a
/// group only some requests carry would hide a real one.
///
/// * `ServiceAccount`: its token's username and groups
///   ([`sa_token::subject_for`], [`sa_token::groups_for`]).
/// * `User` `system:anonymous`: the anonymous identity, which is NOT in
///   `system:authenticated`.
/// * any other `User`: a client certificate's, so `system:authenticated`.
///   The certificate's Organizations are invisible to a census, so a user
///   granted the subresource through one of them is still reported: an
///   over-count, on the side that keeps T4.2 in Shadow.
/// * `Group`: a member of it ([`member_of`]).
fn identity(subject: &SubjectRef) -> Option<UserInfo> {
    match subject.kind.as_str() {
        "ServiceAccount" => {
            let ns = subject.namespace.as_deref().unwrap_or_default();
            Some(UserInfo {
                username: sa_token::subject_for(ns, &subject.name),
                groups: sa_token::groups_for(ns),
                ..UserInfo::default()
            })
        }
        "User" => {
            let anonymous = UserInfo::anonymous();
            Some(if subject.name == anonymous.username {
                anonymous
            } else {
                UserInfo::from_client_cert(&subject.name, &[])
            })
        }
        "Group" => Some(member_of(&subject.name)),
        _ => None,
    }
}

/// A member of `group`, carrying the groups every member of it carries and
/// no username (no binding can name a member the census does not know).
pub(super) fn member_of(group: &str) -> UserInfo {
    let anonymous = UserInfo::anonymous();
    if anonymous.groups.iter().any(|g| g == group) {
        // `system:unauthenticated`: its one member is the anonymous user.
        return anonymous;
    }
    let sa_namespace = group
        .strip_prefix(GROUP_SERVICEACCOUNTS)
        .and_then(|rest| rest.strip_prefix(':'));
    let groups = match (group, sa_namespace) {
        // A namespace's ServiceAccounts: their tokens' groups, exactly.
        (_, Some(ns)) => sa_token::groups_for(ns),
        // Every ServiceAccount: the groups no namespace changes.
        (GROUP_SERVICEACCOUNTS, None) => vec![
            GROUP_AUTHENTICATED.to_owned(),
            GROUP_SERVICEACCOUNTS.to_owned(),
        ],
        (GROUP_AUTHENTICATED, None) => vec![GROUP_AUTHENTICATED.to_owned()],
        // Any other group is a client certificate's Organization.
        (other, None) => return UserInfo::from_client_cert("", &[other.to_owned()]),
    };
    UserInfo {
        groups,
        ..UserInfo::default()
    }
}

/// A binding subject as the census names it. A `ServiceAccount` with no
/// namespace of its own is the binding's, as the authorizer reads it.
fn subject_ref(
    s: &engenho_types::generated_v1_34::rbac_v1::Subject,
    binding_ns: Option<&str>,
) -> SubjectRef {
    let namespace = (s.kind == "ServiceAccount")
        .then(|| s.namespace.as_deref().or(binding_ns).map(str::to_owned))
        .flatten();
    SubjectRef {
        kind: s.kind.clone(),
        name: s.name.clone(),
        namespace,
    }
}

/// One binding, reduced to what the census walks.
struct Binding<'a> {
    namespace: Option<&'a str>,
    role_ref: &'a RoleRef,
    subjects: Vec<SubjectRef>,
}

/// T4.2: every grant a bare parent resource gave a subject that the shipped
/// [`RbacAuthorizer`] refuses. `system:masters` is skipped: the authorizer
/// allows it everything before it reads a rule.
pub(super) async fn bare_parent_only(listed: Listed) -> Vec<Match> {
    let served = Served::of(&listed.crds);
    let typed = Typed::of(listed);
    let vocabulary = Vocabulary::of(typed.every_rule());
    let sentinels = Sentinels {
        verb: absent_from(&vocabulary.verbs),
        name: absent_from(&vocabulary.names),
    };
    let authorizer = RbacAuthorizer::new(ListedEnv(&typed));

    let bindings = typed
        .cluster_role_bindings
        .iter()
        .map(|crb| Binding {
            namespace: None,
            role_ref: &crb.role_ref,
            subjects: crb.subjects.iter().map(|s| subject_ref(s, None)).collect(),
        })
        .chain(typed.role_bindings.iter().map(|rb| {
            let ns = rb.metadata.namespace.as_deref();
            Binding {
                namespace: ns,
                role_ref: &rb.role_ref,
                subjects: rb.subjects.iter().map(|s| subject_ref(s, ns)).collect(),
            }
        }));

    let mut lost: BTreeSet<(SubjectRef, LostGrant)> = BTreeSet::new();
    for binding in bindings {
        let rules = typed.rules(binding.role_ref, binding.namespace);
        let grants: Vec<LostGrant> = rules
            .iter()
            .flat_map(|rule| probes(rule, &served, &vocabulary, &binding))
            .collect();
        for subject in &binding.subjects {
            if subject.kind == "Group" && subject.name == GROUP_MASTERS {
                continue;
            }
            let Some(user) = identity(subject) else {
                continue;
            };
            for grant in &grants {
                let attrs = Attributes {
                    user: user.clone(),
                    verb: Sentinels::value(&grant.verb, &sentinels.verb).to_owned(),
                    group: grant.group.clone(),
                    version: String::new(),
                    resource: grant.parent.clone(),
                    subresource: Some(grant.subresource.to_owned()),
                    namespace: grant.namespace.clone(),
                    name: Some(Sentinels::value(&grant.name, &sentinels.name).to_owned()),
                    non_resource_url: None,
                };
                if !authorizer.authorize(&attrs).await.is_allow() {
                    lost.insert((subject.clone(), grant.clone()));
                }
            }
        }
    }
    lost.into_iter()
        .map(|(subject, grant)| Match {
            subject: Subject::Rbac(subject),
            finding: Finding::SubresourceLost(grant),
        })
        .collect()
}

/// Every request `rule`'s bare parents granted under the old matcher on a
/// served subresource: per bare resource, each served (group, subresource)
/// the rule's groups admit, each verb class, each name class.
fn probes(
    rule: &PolicyRule,
    served: &Served,
    vocabulary: &Vocabulary,
    binding: &Binding<'_>,
) -> Vec<LostGrant> {
    if !rule.non_resource_urls.is_empty() {
        return Vec::new();
    }
    let verbs = if rule.verbs.iter().any(|v| v == "*") {
        every(&vocabulary.verbs)
    } else {
        rule.verbs.iter().cloned().map(Dim::Named).collect()
    };
    // An unrestricted rule granted every name; a restricted one its own.
    let names = if rule.resource_names.is_empty() {
        every(&vocabulary.names)
    } else {
        rule.resource_names
            .iter()
            .cloned()
            .map(Dim::Named)
            .collect()
    };
    let via = Via {
        kind: binding.role_ref.kind.clone(),
        name: binding.role_ref.name.clone(),
    };
    let mut out = Vec::new();
    for parent in rule
        .resources
        .iter()
        .filter(|r| *r != "*" && !r.contains('/'))
    {
        for (group, subresource) in served.of_parent(parent, &rule.api_groups) {
            for verb in &verbs {
                for name in &names {
                    out.push(LostGrant {
                        verb: verb.clone(),
                        group: group.to_owned(),
                        parent: parent.clone(),
                        subresource,
                        name: name.clone(),
                        namespace: binding.namespace.map(str::to_owned),
                        via: via.clone(),
                    });
                }
            }
        }
    }
    out
}
