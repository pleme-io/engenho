//! The census (plan T0.10): one named check, run with no side effects over
//! live state, printing what it matched and how many.
//!
//! ## Why a census comes before a change
//!
//! Seven rows of the plan change a rule that already has data under it: RBAC
//! matching (T4.2), the capacity ledger (T1.5), the scheduler's filters
//! (T5.7), defaulting and validation of stored objects (T4.4, T4.5), the
//! Deployment's `ReplicaSet` matcher (T4.10) and the store's boot image
//! (T3.4). Each can strand a workload or reject an object that was accepted
//! yesterday, and none of them fails loudly when it does. The census answers
//! "how many live objects would this rule refuse?" before the rule ships, and
//! a non-zero answer keeps the rule in Shadow (T0.11).
//!
//! ## What the census calls
//!
//! Every [`Predicate`] calls, unchanged, the function the change ships:
//! [`engenho_scheduler::NodeLedger`], [`engenho_scheduler::FilterPlugin`],
//! [`engenho_controllers::current_replicaset`], the apiserver's
//! `defaulting`, `validation` and `schema_validation` stages, the
//! [`engenho_apiserver::authz::RbacAuthorizer`], and the store's own boot
//! (`StoreMesh::start_durable`) with its image tripwire. A census that
//! re-implemented a rule would count what its copy thinks, which is the one
//! number nobody needs.
//!
//! ## The catalog is closed
//!
//! [`Predicate`] is an enum and [`Predicate::ALL`] is generated from the same
//! list as its names, so a predicate cannot be declared without being
//! runnable and there is no way to hand the census an expression: `--predicate`
//! names a row of the catalog or is refused ([`Predicate::from_name`]).
//!
//! ## Two sources, one reading
//!
//! A [`Source`] is where the objects come from: [`ApiSource`], a LIST through
//! a running apiserver, or [`DataDirSource`], a copy of a node's data
//! directory booted privately. Both answer the same three questions, so a
//! predicate never knows which one it reads. A predicate that needs the store
//! image ([`Predicate::StoreImageInconsistent`]) refuses an apiserver rather
//! than report zero: a source that cannot see the thing is not a source that
//! saw none of it.
//!
//! ## What is a type and what is not
//!
//! The closed catalog and the read-only data directory are structural: no
//! string reaches a predicate as code, and [`DataDirSource`] never opens the
//! directory it was given, only a private copy of it. That the census calls
//! the SAME function the rule ships is not a type: it holds because each arm
//! below calls the shipped function by path, which a reviewer checks.

mod predicates;
mod rbac;
mod source;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use engenho_apiserver::schema_validation::SchemaViolation;
use engenho_apiserver::validation::Violation;
use engenho_scheduler::filter::Rejection;
use engenho_store::{ImageInconsistency, StoreError};
use engenho_types::error::KubeError;
use serde_json::Value;

pub use rbac::{Dim, GrantedResource, LostGrant, SubjectRef, Via};
pub use source::{ApiSource, DataDirSource, Source};

/// Declare the predicate enum, its run list, its names, its plan rows and
/// its summaries from ONE list, so a declared predicate is a runnable one.
macro_rules! census_predicates {
    ($( $(#[$doc:meta])* $variant:ident => $name:literal, $row:literal, $summary:literal; )+) => {
        /// A census check, by name. Closed: the census runs these and nothing
        /// else.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Predicate {
            $( $(#[$doc])* $variant, )+
        }

        impl Predicate {
            /// Every predicate, in catalog order.
            pub const ALL: &'static [Self] = &[ $( Self::$variant, )+ ];

            /// The name `--predicate` takes.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $( Self::$variant => $name, )+
                }
            }

            /// The plan row whose change this census gates.
            #[must_use]
            pub const fn plan_row(self) -> &'static str {
                match self {
                    $( Self::$variant => $row, )+
                }
            }

            /// What a match means, in one line.
            #[must_use]
            pub const fn summary(self) -> &'static str {
                match self {
                    $( Self::$variant => $summary, )+
                }
            }
        }
    };
}

census_predicates! {
    /// Nodes the capacity ledger already charges past their allocatable
    /// ([`engenho_scheduler::NodeLedger::headroom`], signed).
    NodeOvercommitted => "node-overcommitted", "T1.5",
        "nodes the capacity ledger charges past their allocatable";
    /// Bound pods, each judged by every
    /// [`engenho_scheduler::FilterPlugin`] against its own node, charged
    /// with every other pod on that node.
    BoundPodRejected => "bound-pod-rejected", "T5.7",
        "bound pods a filter plugin would refuse on their own node";
    /// Deployments no owned `ReplicaSet` runs the template of
    /// ([`engenho_controllers::current_replicaset`]): the controller would
    /// create one and scale the rest to zero.
    DeploymentWouldRoll => "deployment-would-roll", "T4.10",
        "Deployments whose template no owned ReplicaSet runs";
    /// Stored objects that the create path's defaulting, validation and CRD
    /// structural schema would refuse today.
    StoredWouldReject => "stored-would-reject", "T4.4/T4.5",
        "stored objects defaulting + validation + CRD schema would refuse";
    /// Disagreements the store's boot found in its own durable image
    /// ([`engenho_store::ImageTripwire`]). A data directory only.
    StoreImageInconsistent => "store-image-inconsistent", "T3.4",
        "ways the store's boot found its durable image disagreeing with itself";
    /// Subjects (other than `system:masters`) who reached a subresource only
    /// through a bare parent resource, which upstream's `ResourceMatches`
    /// never granted and the shipped authorizer no longer grants.
    RbacBareParentOnly => "rbac-bare-parent-only", "T4.2",
        "subjects that reached a subresource only through a bare parent";
}

impl Predicate {
    /// The catalog row named `name`, if there is one. A lookup, not a parse:
    /// nothing but a catalog name gets through.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|p| p.name() == name)
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The predicate catalog as `engenho census --list` prints it.
#[derive(Debug, Clone, Copy)]
pub struct Catalog;

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for p in Predicate::ALL {
            writeln!(f, "{:<26} {:<10} {}", p.name(), p.plan_row(), p.summary())?;
        }
        Ok(())
    }
}

/// A group, version and kind, owned (a CRD's kinds are only known at run
/// time).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gvk {
    /// The API group; empty for the core group.
    pub group: String,
    /// The version (`v1`).
    pub version: String,
    /// The kind (`Pod`).
    pub kind: String,
}

impl Gvk {
    /// A GVK from its three parts.
    #[must_use]
    pub fn new(group: &str, version: &str, kind: &str) -> Self {
        Self {
            group: group.to_owned(),
            version: version.to_owned(),
            kind: kind.to_owned(),
        }
    }
}

/// `v1/Pod`, `apps/v1/Deployment`.
impl fmt::Display for Gvk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}/{}", self.version, self.kind)
        } else {
            write!(f, "{}/{}/{}", self.group, self.version, self.kind)
        }
    }
}

/// The built-in kinds the predicates read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Builtin {
    Node,
    Pod,
    Lease,
    Deployment,
    ReplicaSet,
    ClusterRole,
    ClusterRoleBinding,
    Role,
    RoleBinding,
    CustomResourceDefinition,
}

impl Builtin {
    fn gvk(self) -> Gvk {
        let (group, version, kind) = match self {
            Self::Node => ("", "v1", "Node"),
            Self::Pod => ("", "v1", "Pod"),
            Self::Lease => ("coordination.k8s.io", "v1", "Lease"),
            Self::Deployment => ("apps", "v1", "Deployment"),
            Self::ReplicaSet => ("apps", "v1", "ReplicaSet"),
            Self::ClusterRole => (RBAC_GROUP, "v1", "ClusterRole"),
            Self::ClusterRoleBinding => (RBAC_GROUP, "v1", "ClusterRoleBinding"),
            Self::Role => (RBAC_GROUP, "v1", "Role"),
            Self::RoleBinding => (RBAC_GROUP, "v1", "RoleBinding"),
            Self::CustomResourceDefinition => {
                ("apiextensions.k8s.io", "v1", "CustomResourceDefinition")
            }
        };
        Gvk::new(group, version, kind)
    }
}

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

/// What a match is about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Subject {
    /// A stored object.
    Object {
        /// Its kind.
        gvk: Gvk,
        /// Its namespace, when namespaced.
        namespace: Option<String>,
        /// Its `metadata.name`; `None` when it has none.
        name: Option<String>,
    },
    /// The store's durable image as a whole.
    StoreImage,
    /// An RBAC subject named by a binding.
    Rbac(SubjectRef),
}

impl Subject {
    fn object(gvk: &Gvk, object: &Value) -> Self {
        Self::Object {
            gvk: gvk.clone(),
            namespace: text_at(object, "/metadata/namespace").map(str::to_owned),
            name: text_at(object, "/metadata/name").map(str::to_owned),
        }
    }

    /// The group a match is counted under: the object's kind, or the other
    /// two subjects by name.
    fn group(&self) -> SubjectGroup<'_> {
        match self {
            Self::Object { gvk, .. } => SubjectGroup::Kind(gvk),
            Self::StoreImage => SubjectGroup::StoreImage,
            Self::Rbac(_) => SubjectGroup::RbacSubject,
        }
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object {
                gvk,
                namespace,
                name,
            } => {
                write!(f, "{gvk} ")?;
                if let Some(ns) = namespace {
                    write!(f, "{ns}/")?;
                }
                f.write_str(name.as_deref().unwrap_or("(unnamed)"))
            }
            Self::StoreImage => f.write_str("store-image"),
            Self::Rbac(subject) => write!(f, "{subject}"),
        }
    }
}

/// The count key's first half.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SubjectGroup<'a> {
    Kind(&'a Gvk),
    StoreImage,
    RbacSubject,
}

impl fmt::Display for SubjectGroup<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kind(gvk) => write!(f, "{gvk}"),
            Self::StoreImage => f.write_str("store-image"),
            Self::RbacSubject => f.write_str("rbac-subject"),
        }
    }
}

/// Why a subject matched. One arm per kind of answer a predicate gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// [`Predicate::NodeOvercommitted`]: the node's signed headroom.
    Overcommitted {
        /// Remaining cpu, milli-cores; negative when overcommitted.
        cpu_milli: i128,
        /// Remaining memory, milli-bytes; negative when overcommitted.
        mem_milli: i128,
    },
    /// [`Predicate::BoundPodRejected`]: a plugin refused the pod's own node.
    Rejected(Rejection),
    /// [`Predicate::BoundPodRejected`]: the pod is bound to a node the node
    /// list does not hold, so no plugin can judge it.
    NodeNotObserved {
        /// The `spec.nodeName` the pod carries.
        node: String,
    },
    /// [`Predicate::DeploymentWouldRoll`].
    NoReplicaSetRunsTemplate,
    /// [`Predicate::StoredWouldReject`]: built-in validation.
    Invalid(Violation),
    /// [`Predicate::StoredWouldReject`]: the CRD's structural schema.
    SchemaInvalid(SchemaViolation),
    /// [`Predicate::StoreImageInconsistent`].
    ImageInconsistent {
        /// Which disagreement.
        kind: ImageInconsistency,
        /// How many times the boot hit it.
        hits: u64,
    },
    /// [`Predicate::RbacBareParentOnly`].
    SubresourceLost(LostGrant),
}

impl Finding {
    /// The count key's second half: the reason a match is counted under.
    fn reason(&self) -> Reason<'_> {
        Reason(self)
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overcommitted {
                cpu_milli,
                mem_milli,
            } => write!(
                f,
                "overcommitted: headroom cpu={cpu_milli}m memory={}B",
                mem_milli / 1000
            ),
            Self::Rejected(rejection) => write!(f, "{}: {rejection}", rejection.plugin()),
            Self::NodeNotObserved { node } => {
                write!(
                    f,
                    "bound to node {node:?}, which the node list does not hold"
                )
            }
            Self::NoReplicaSetRunsTemplate => f.write_str(
                "no owned ReplicaSet runs its template: the controller would create one \
                 and scale the others to zero",
            ),
            Self::Invalid(v) => write!(f, "invalid: {}: {}", v.field, v.message),
            Self::SchemaInvalid(v) => write!(f, "schema: {v}"),
            Self::ImageInconsistent { kind, hits } => write!(f, "{kind}: {hits} hit(s)"),
            Self::SubresourceLost(lost) => write!(f, "{lost}"),
        }
    }
}

/// The label a finding is counted under.
struct Reason<'a>(&'a Finding);

impl fmt::Display for Reason<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Finding::Overcommitted { .. } => f.write_str("overcommitted"),
            Finding::Rejected(rejection) => f.write_str(rejection.plugin().name()),
            Finding::NodeNotObserved { .. } => f.write_str("node_not_observed"),
            Finding::NoReplicaSetRunsTemplate => f.write_str("no_replicaset_runs_template"),
            Finding::Invalid(v) => f.write_str(&v.field),
            Finding::SchemaInvalid(v) => f.write_str(&v.path),
            Finding::ImageInconsistent { kind, .. } => f.write_str(kind.reason()),
            Finding::SubresourceLost(lost) => write!(f, "{}", lost.resource()),
        }
    }
}

/// One subject a predicate matched, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    /// What matched.
    pub subject: Subject,
    /// Why.
    pub finding: Finding,
}

impl fmt::Display for Match {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}  {}", self.subject, self.finding)
    }
}

/// What one census run read and found.
#[derive(Debug, Clone)]
pub struct Report {
    predicate: Predicate,
    source: String,
    examined: BTreeMap<Gvk, usize>,
    unlisted: BTreeMap<Gvk, String>,
    matches: Vec<Match>,
}

impl Report {
    fn new(predicate: Predicate, source: String) -> Self {
        Self {
            predicate,
            source,
            examined: BTreeMap::new(),
            unlisted: BTreeMap::new(),
            matches: Vec::new(),
        }
    }

    /// List `gvk` from `source`, counting what was read.
    async fn read<S: Source + ?Sized>(
        &mut self,
        source: &S,
        gvk: &Gvk,
    ) -> Result<Vec<Value>, CensusError> {
        let objects = source.list(gvk).await?;
        *self.examined.entry(gvk.clone()).or_default() += objects.len();
        Ok(objects)
    }

    /// The predicate that ran.
    #[must_use]
    pub fn predicate(&self) -> Predicate {
        self.predicate
    }

    /// Every match, in the order the predicate found them.
    #[must_use]
    pub fn matches(&self) -> &[Match] {
        &self.matches
    }

    /// How many objects of each kind were read.
    #[must_use]
    pub fn examined(&self) -> &BTreeMap<Gvk, usize> {
        &self.examined
    }

    /// The kinds the source listed as listable and then refused to LIST, with
    /// the URL that refused. Their objects were not judged: they are named
    /// here so the report never counts them as zero.
    #[must_use]
    pub fn unlisted(&self) -> &BTreeMap<Gvk, String> {
        &self.unlisted
    }

    /// How many distinct subjects matched. An object with two findings
    /// counts once.
    #[must_use]
    pub fn subjects_matched(&self) -> usize {
        self.matches
            .iter()
            .map(|m| &m.subject)
            .collect::<BTreeSet<_>>()
            .len()
    }

    /// Findings counted by what they are about (a kind, the store image, an
    /// RBAC subject) and the reason they are counted under.
    #[must_use]
    pub fn counts(&self) -> BTreeMap<(String, String), usize> {
        let mut counts = BTreeMap::new();
        for m in &self.matches {
            let key = (
                m.subject.group().to_string(),
                m.finding.reason().to_string(),
            );
            *counts.entry(key).or_default() += 1;
        }
        counts
    }
}

/// The census's whole answer: the header, every match, the counts by kind
/// and reason, and the totals. An empty answer says so: zero matches is a
/// finding.
impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "census {} ({}) over {}",
            self.predicate,
            self.predicate.plan_row(),
            self.source
        )?;
        for (gvk, n) in &self.examined {
            writeln!(f, "examined {gvk}: {n}")?;
        }
        for (gvk, url) in &self.unlisted {
            writeln!(
                f,
                "unlisted {gvk}: discovery lists it, but GET {url} answers 405; not judged"
            )?;
        }
        for m in &self.matches {
            writeln!(f, "match {m}")?;
        }
        for ((group, reason), n) in self.counts() {
            writeln!(f, "count {group} {reason}: {n}")?;
        }
        writeln!(
            f,
            "total: {} finding(s) over {} subject(s)",
            self.matches.len(),
            self.subjects_matched()
        )
    }
}

/// Why a census could not answer. Every arm is a census that did NOT run to
/// the end, which is never reported as zero matches.
#[derive(Debug, thiserror::Error)]
pub enum CensusError {
    /// The predicate reads the store image and the source is an apiserver.
    #[error("{predicate} reads the store's durable image, which only a data directory has")]
    NeedsDataDir {
        /// The predicate that was asked for.
        predicate: Predicate,
    },
    /// The apiserver's discovery does not list the kind.
    #[error("the apiserver serves no listable kind {gvk}")]
    KindNotServed {
        /// The kind that was asked for.
        gvk: Gvk,
    },
    /// Discovery says the kind can be listed, and the LIST answers 405
    /// Method Not Allowed: the server does not treat it as a collection.
    #[error("discovery lists {gvk} as listable, but GET {url} answers 405 Method Not Allowed")]
    NotListable {
        /// The kind.
        gvk: Gvk,
        /// The LIST URL that refused.
        url: String,
    },
    /// The request never got an answer.
    #[error("GET {url}: {source}")]
    Http {
        /// The URL requested.
        url: String,
        /// The transport's error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The apiserver answered with a status that is not success.
    #[error("GET {url}: HTTP {code}")]
    Status {
        /// The URL requested.
        url: String,
        /// The HTTP status code.
        code: u16,
    },
    /// The body is not JSON.
    #[error("GET {url}: the body is not JSON: {source}")]
    Decode {
        /// The URL requested.
        url: String,
        /// The decoder's error.
        #[source]
        source: serde_json::Error,
    },
    /// The body is JSON of the wrong shape.
    #[error("GET {url}: {shape}")]
    Shape {
        /// The URL requested.
        url: String,
        /// What was missing.
        shape: Shape,
    },
    /// The kubeconfig, the connection or a URL could not be built.
    #[error(transparent)]
    Kube(#[from] KubeError),
    /// The data directory holds no readable `store` directory.
    #[error("no store directory at {}: {source}", path.display())]
    NoStore {
        /// Where the store was looked for.
        path: std::path::PathBuf,
        /// Why it could not be read.
        #[source]
        source: std::io::Error,
    },
    /// Something in the store directory is neither a file nor a directory,
    /// so it cannot be copied faithfully.
    #[error("{} is neither a file nor a directory; the census copies only those", path.display())]
    UncopyableEntry {
        /// The entry.
        path: std::path::PathBuf,
    },
    /// The private copy could not be made.
    #[error("copy {} to {}: {source}", from.display(), to.display())]
    Copy {
        /// What was being copied.
        from: std::path::PathBuf,
        /// Where to.
        to: std::path::PathBuf,
        /// Why it failed.
        #[source]
        source: std::io::Error,
    },
    /// The store holds no raft state: nothing was ever written to it.
    #[error("the store at {} holds no raft state; nothing was ever written there", path.display())]
    EmptyStore {
        /// The data directory given.
        path: std::path::PathBuf,
    },
    /// The store could not be booted or stopped.
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// What a JSON body lacked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A LIST body with no `items` array.
    NoItems,
    /// A discovery body with no `versions`, `groups` or `resources` array.
    NoDiscovery,
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoItems => f.write_str("the LIST body has no `items` array"),
            Self::NoDiscovery => f.write_str("the discovery body has none of the arrays it names"),
        }
    }
}

engenho_substrate::impl_error_kind! {
    CensusError {
        { NeedsDataDir { .. } } => "needs_data_dir",
        { KindNotServed { .. } } => "kind_not_served",
        { NotListable { .. } } => "not_listable",
        { Http { .. } } => "http",
        { Status { .. } } => "status",
        { Decode { .. } } => "decode",
        { Shape { .. } } => "shape",
        (Kube(_)) => "kube",
        { NoStore { .. } } => "no_store",
        { UncopyableEntry { .. } } => "uncopyable_entry",
        { Copy { .. } } => "copy",
        { EmptyStore { .. } } => "empty_store",
        (Store(_)) => "store",
    }
}

/// Run `predicate` over `source`. Reads only: nothing is written anywhere
/// the source reads from.
///
/// # Errors
///
/// A [`CensusError`] when the source cannot answer, or cannot answer this
/// predicate at all ([`CensusError::NeedsDataDir`]); never a partial count.
pub async fn run<S: Source + ?Sized>(
    predicate: Predicate,
    source: &S,
) -> Result<Report, CensusError> {
    run_at(predicate, source, &engenho_types::time::now_rfc3339_utc()).await
}

/// [`run`] with the clock the node projection reads injected.
async fn run_at<S: Source + ?Sized>(
    predicate: Predicate,
    source: &S,
    now: &str,
) -> Result<Report, CensusError> {
    let mut report = Report::new(predicate, source.label());
    let found = match predicate {
        Predicate::NodeOvercommitted => {
            let nodes = report.read(source, &Builtin::Node.gvk()).await?;
            let pods = report.read(source, &Builtin::Pod.gvk()).await?;
            predicates::node_overcommitted(&nodes, &pods)
        }
        Predicate::BoundPodRejected => {
            let nodes = report.read(source, &Builtin::Node.gvk()).await?;
            let pods = report.read(source, &Builtin::Pod.gvk()).await?;
            let leases = report.read(source, &Builtin::Lease.gvk()).await?;
            predicates::bound_pod_rejected(&nodes, &pods, &leases, now)
        }
        Predicate::DeploymentWouldRoll => {
            let deployments = report.read(source, &Builtin::Deployment.gvk()).await?;
            let replicasets = report.read(source, &Builtin::ReplicaSet.gvk()).await?;
            predicates::deployment_would_roll(&deployments, &replicasets)
        }
        Predicate::StoredWouldReject => {
            // Read for their schemas only; the CRDs are judged, and counted,
            // with every other kind below.
            let crds = source
                .list(&Builtin::CustomResourceDefinition.gvk())
                .await?;
            let schemas = predicates::crd_schemas(&crds);
            let mut found = Vec::new();
            for gvk in source.kinds().await? {
                match report.read(source, &gvk).await {
                    Ok(objects) => {
                        found.extend(predicates::would_reject(&gvk, &objects, schemas.get(&gvk)));
                    }
                    // A kind the server will not LIST holds nothing a LIST
                    // can judge. It is named in the report, not counted.
                    Err(CensusError::NotListable { gvk, url }) => {
                        report.unlisted.insert(gvk, url);
                    }
                    Err(e) => return Err(e),
                }
            }
            found
        }
        Predicate::StoreImageInconsistent => {
            let Some(tripwire) = source.image().await else {
                return Err(CensusError::NeedsDataDir { predicate });
            };
            predicates::image_inconsistent(&tripwire)
        }
        Predicate::RbacBareParentOnly => {
            let listed = rbac::Listed {
                cluster_role_bindings: report
                    .read(source, &Builtin::ClusterRoleBinding.gvk())
                    .await?,
                role_bindings: report.read(source, &Builtin::RoleBinding.gvk()).await?,
                cluster_roles: report.read(source, &Builtin::ClusterRole.gvk()).await?,
                roles: report.read(source, &Builtin::Role.gvk()).await?,
                crds: source
                    .list(&Builtin::CustomResourceDefinition.gvk())
                    .await?,
            };
            rbac::bare_parent_only(listed).await
        }
    };
    report.matches = found;
    Ok(report)
}

/// The string at `pointer`, if it is one.
fn text_at<'a>(v: &'a Value, pointer: &str) -> Option<&'a str> {
    v.pointer(pointer).and_then(Value::as_str)
}

#[cfg(test)]
mod tests;
