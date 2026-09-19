//! ★ ONE WRITE PIPELINE (T4.5). Every main-object user write — POST, PUT,
//! PATCH (merge, strategic, JSON) and server-side apply — reaches the store
//! through [`StoreBackedHandler::write`], and nothing else on that path calls
//! `propose`.
//!
//! ★ WHY. Before this module the four verbs were four hand-written paths to
//! the store, and each had collected a different subset of the rules:
//!
//! | rule                         | POST | PUT | PATCH | apply |
//! |------------------------------|------|-----|-------|-------|
//! | defaulting                   | yes  | —   | —     | —     |
//! | validation                   | yes  | —   | —     | —     |
//! | CRD schema defaults + checks | yes  | —   | —     | —     |
//! | normalized stored object     | yes  | yes | —     | —     |
//! | name format, `NamespaceLifecycle`, create stamps | yes | n/a | n/a | — (on create) |
//! | admission sees the object    | yes  | as CREATE | the patch, not the object | as UPDATE, even when it creates |
//! | status kept off the main endpoint | n/a | yes | — | — |
//! | dryRun                       | yes  | yes | refused | refused |
//!
//! Server-side apply matters most: it is how Flux writes, so the objects a
//! `GitOps` cluster actually holds were the ones that skipped every check, and
//! a Namespace Flux created was born without its `kubernetes` finalizer.
//!
//! ★ THE PIPELINE. One function over one verb-free candidate:
//!
//! 1. read the prior object and its revision under one guard;
//! 2. decide the LIFECYCLE from what the store holds — create or update,
//!    never from the verb, because an apply is either;
//! 3. check the client's own precondition (`metadata.resourceVersion`);
//! 4. compute the CANDIDATE — the body (POST, PUT), the store's own patch
//!    algorithm (PATCH), the store's own server-side-apply merge (apply).
//!    These are the same pure functions the store's replay path calls
//!    ([`apply_patch_algorithm`], [`apply_ssa`]); nothing is re-implemented;
//! 5. normalize it (the object that will be stored — never the patch body);
//! 6. the name is the URL's;
//! 7. create-only refusals: the name's format, `NamespaceLifecycle`;
//! 8. default → validate → CRD-schema default → CRD-schema validate;
//! 9. admission, as CREATE or UPDATE by lifecycle, over the whole candidate;
//! 10. server-owned fields: namespace, name; on create the timestamp and the
//!     Namespace lifecycle stamps; on update `uid`/`creationTimestamp`/
//!     `deletionTimestamp`, `.status` on a status-bearing kind, and a
//!     Service's allocated cluster IP;
//! 11. an update that changes nothing answers without proposing;
//! 12. `dryRun` answers with the candidate;
//! 13. ONE compare-and-swap proposal: a create is `Txn{NotExists} → Put`,
//!     an update is a `Put` at the prior's revision. A race the client did
//!     not ask to lose is re-planned against the newer object, up to
//!     [`WRITE_ATTEMPTS`] times.
//!
//! ★ TIER, STATED PLAINLY.
//! * A verb skipping a rule: the rules are written once, after the verbs
//!   converge on a candidate, so no rule has a per-verb place to be skipped.
//!   The rules that do differ differ by LIFECYCLE, which is read from the
//!   store. `tests/w1_one_write_pipeline.rs` pins every verb against every
//!   rule — a GATE, not a type.
//! * A main-object write reaching the store without the pipeline: the only
//!   `propose` on that path is [`StoreBackedHandler::commit`], which takes a
//!   [`WritePlan`], and a `WritePlan` is constructed only at the end of
//!   [`StoreBackedHandler::plan`]. That is a module-private constructor, not
//!   a sealed store API: `propose` stays callable by anything holding
//!   `Arc<StoreMesh>`, and the `/status` and `/scale` subresource writes in
//!   this crate still propose a scoped `Patch` directly.
//! * Replay: the store's merge is called, not moved or rewritten, so the
//!   `Patch` entries already in a log replay through unchanged store code.
//!   What a PATCH or apply logs from now on is a `Put` of the merged,
//!   defaulted, admitted object.
//! * Concurrency: an unconditional PATCH used to be merged inside the state
//!   machine and could never conflict. It is now optimistic, like
//!   upstream's `GuaranteedUpdate`: a write that loses the race re-reads and
//!   re-plans. Every lost race means another write committed, so contending
//!   writers finish; the bound only turns a pathological stream of
//!   newcomers into a 409 every client retries.

use std::sync::LazyLock;

use serde_json::Value;

use engenho_controllers::admission::{AdmissionAction, AdmissionDecision};
use engenho_store::{
    ApplyConflicts, Gvk, OpenApiPatchEnv, PatchBody, PatchError, Revision, VersionMeta,
    apply_patch_algorithm, apply_ssa,
    command::{Reason, ResourceCommand, ResourceOp, TxnCompare, TxnOp},
    resource::ResourceKey,
    unchanged,
};
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::Subresource;
use engenho_types::name::{NameError, ResourceName};
use engenho_types::patch::PatchType;

use super::{
    ResourceHandler, StoreBackedHandler, admission_request, inject_type_meta,
    preserve_immutable_meta, preserve_status, stamp_creation_timestamp, stamp_namespace,
    stamp_namespace_create_defaults,
};
use crate::error::ApiError;
use crate::field_validation::{KindRef, PatchFields};
use crate::object_body::ObjectBody;
use crate::params::{ApplyOptions, DryRun, body_precondition};
use crate::schema_validation::SchemaViolation;
use crate::validation::Violation;

/// How many times one write is planned. Each lost compare-and-swap means
/// another write committed in between, so N contending writers need at most
/// N attempts; upstream's `GuaranteedUpdate` has no bound at all and relies
/// on the request deadline. Past this, the write answers 409, which every
/// client retries.
const WRITE_ATTEMPTS: usize = 32;

/// The strategic-merge schema a PATCH or apply candidate is computed under.
/// The store's catalog builds the same resolver — `OpenApiPatchEnv::new()`
/// over the same `&'static` vendored `OpenAPI` documents, with no other state —
/// so a candidate computed here and a `Patch` replayed there resolve every
/// list strategy identically. One per process, so its cache is shared.
static PATCH_SCHEMA: LazyLock<OpenApiPatchEnv> = LazyLock::new(OpenApiPatchEnv::new);

/// A main-object write, as the client asked for it.
pub(super) enum WriteRequest {
    /// POST: the body is the object to create.
    Create(ObjectBody),
    /// PUT: the body is the object that replaces the stored one.
    Replace(ObjectBody),
    /// PATCH with one of the three merge algorithms.
    Patch {
        patch: Value,
        algorithm: MergeAlgorithm,
    },
    /// Server-side apply: create-or-merge on behalf of a field manager.
    Apply { config: Value, opts: ApplyOptions },
}

/// The patch algorithms that are not server-side apply. [`PatchType::Apply`]
/// has no arm: an apply needs a field manager, so it is
/// [`WriteRequest::Apply`], and an apply without one has no representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MergeAlgorithm {
    /// RFC 7396 JSON merge patch.
    Merge,
    /// Kubernetes strategic merge patch.
    Strategic,
    /// RFC 6902 JSON patch.
    Json,
}

impl MergeAlgorithm {
    fn patch_type(self) -> PatchType {
        match self {
            Self::Merge => PatchType::Merge,
            Self::Strategic => PatchType::Strategic,
            Self::Json => PatchType::Json,
        }
    }
}

impl WriteRequest {
    /// Resolve a PATCH request. The router validates `fieldManager` before
    /// calling, so an apply without options only reaches here from a caller
    /// that skipped the router; it is refused rather than guessed.
    ///
    /// # Errors
    ///
    /// [`WriteRefusal::ApplyWithoutManager`] for an apply with no options.
    pub(super) fn patch(
        patch: Value,
        patch_type: PatchType,
        apply: Option<ApplyOptions>,
    ) -> Result<Self, WriteRefusal> {
        let algorithm = match patch_type {
            PatchType::Apply => {
                return apply
                    .map(|opts| Self::Apply {
                        config: patch,
                        opts,
                    })
                    .ok_or(WriteRefusal::ApplyWithoutManager);
            }
            PatchType::Merge => MergeAlgorithm::Merge,
            PatchType::Strategic => MergeAlgorithm::Strategic,
            PatchType::Json => MergeAlgorithm::Json,
        };
        Ok(Self::Patch { patch, algorithm })
    }

    /// The client's own optimistic-concurrency precondition: the
    /// `metadata.resourceVersion` its body or patch carries. A JSON patch is
    /// an op array with no `metadata`, so it never carries one.
    fn client_precondition(&self) -> Result<Option<Revision>, ApiError> {
        match self {
            Self::Create(body) | Self::Replace(body) => body_precondition(body.as_value()),
            Self::Patch { patch, .. } => body_precondition(patch),
            Self::Apply { config, .. } => body_precondition(config),
        }
    }

    /// What the store must hold for this verb to proceed.
    fn requires(&self) -> Existence {
        match self {
            Self::Create(_) => Existence::Absent,
            Self::Replace(_) | Self::Patch { .. } => Existence::Present,
            Self::Apply { .. } => Existence::Either,
        }
    }

    /// The field manager whose `managedFields` time an unchanged check
    /// ignores: the one instant every re-apply restamps.
    fn manager(&self) -> Option<&str> {
        match self {
            Self::Apply { opts, .. } => Some(opts.manager.as_str()),
            Self::Create(_) | Self::Replace(_) | Self::Patch { .. } => None,
        }
    }
}

/// What a verb needs the store to hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Existence {
    /// POST: the name must be free.
    Absent,
    /// PUT and PATCH: there must be an object to replace or patch.
    Present,
    /// Server-side apply: an upsert.
    Either,
}

/// Whether a write brings an object into existence or changes one that
/// exists. Decided from what the store holds, never from the verb.
#[derive(Clone, Debug)]
enum Lifecycle {
    /// Nothing is stored under the key.
    Create,
    /// `prior` is stored under the key, last modified at `at`.
    Update { prior: Value, at: Revision },
}

impl Lifecycle {
    fn of(prior: Option<(Value, VersionMeta)>) -> Self {
        match prior {
            None => Self::Create,
            Some((prior, meta)) => Self::Update {
                prior,
                at: meta.mod_revision,
            },
        }
    }

    fn prior(&self) -> Option<&Value> {
        match self {
            Self::Create => None,
            Self::Update { prior, .. } => Some(prior),
        }
    }

    fn revision(&self) -> Option<Revision> {
        match self {
            Self::Create => None,
            Self::Update { at, .. } => Some(*at),
        }
    }

    /// The admission operation: webhooks read [`AdmissionAction::Put`] as
    /// CREATE and [`AdmissionAction::Patch`] as UPDATE. A PUT that replaces
    /// is an UPDATE and an apply that creates is a CREATE, as upstream
    /// reports them.
    fn admission_action(&self) -> AdmissionAction {
        match self {
            Self::Create => AdmissionAction::Put,
            Self::Update { .. } => AdmissionAction::Patch,
        }
    }
}

/// A write that has passed every rule and is ready to propose. Constructed
/// only at the end of [`StoreBackedHandler::plan`]; the only thing
/// [`StoreBackedHandler::commit`] accepts.
pub(super) struct WritePlan {
    lifecycle: Lifecycle,
    candidate: Value,
}

/// What planning a write produced.
enum Planned {
    /// The update would store the object exactly as it is.
    Unchanged(Value),
    /// A write to propose.
    Write(WritePlan),
}

/// What the store did with a plan's one proposal.
enum Committed {
    /// Stored, or found identical and left as it was.
    Stored,
    /// The write emptied the finalizers of a Terminating object, and the
    /// store removed it (the finalizer release).
    Released,
    /// The object was not where the plan read it: created, changed or
    /// removed in between.
    Raced,
}

/// `<Kind>/<name>` — how a refusal names the object it is about.
#[derive(Debug)]
pub(super) struct ObjectRef {
    kind: String,
    name: String,
}

impl std::fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.kind, self.name)
    }
}

/// One reason an object is not legal, in upstream's `field: message` shape.
#[derive(Debug)]
pub(super) enum InvalidCause {
    /// `metadata.name` breaks the kind's naming rule.
    Name(NameError),
    /// A built-in field check.
    Field(Violation),
    /// A CRD structural-schema check.
    Schema(SchemaViolation),
}

impl std::fmt::Display for InvalidCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(e) => write!(f, "metadata.name: {e}"),
            Self::Field(v) => write!(f, "{v}"),
            Self::Schema(v) => write!(f, "{v}"),
        }
    }
}

/// Upstream's `NewInvalid` sentence: `<Kind> "<name>" is invalid: <cause>;
/// <cause>`. Every cause, not the first: a client that fixes one error only
/// to hit the next spends a round trip per mistake.
#[derive(Debug)]
pub(super) struct ObjectInvalid {
    kind: String,
    name: String,
    causes: Vec<InvalidCause>,
}

impl std::fmt::Display for ObjectInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} \"{}\" is invalid: ", self.kind, self.name)?;
        for (i, cause) in self.causes.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{cause}")?;
        }
        Ok(())
    }
}

/// Why the pipeline refused a write. Each variant maps to exactly one HTTP
/// status in [`From<WriteRefusal> for ApiError`].
#[derive(Debug, thiserror::Error)]
pub(super) enum WriteRefusal {
    /// POST of a name that is taken. 409 `AlreadyExists`.
    #[error("{0} already exists")]
    AlreadyExists(ObjectRef),
    /// PUT or PATCH of a name that is not stored. 404.
    #[error("{0}")]
    NotFound(ObjectRef),
    /// A create into a namespace that does not exist (`NamespaceLifecycle`).
    /// 404, naming the namespace, as upstream does.
    #[error("namespaces \"{0}\" not found")]
    NamespaceMissing(String),
    /// The object names itself differently from the URL (upstream
    /// `checkName`). 400.
    #[error("the name of the object ({body}) does not match the name on the URL ({url})")]
    NameMismatch { body: String, url: String },
    /// The patch algorithm refused the patch. 400.
    #[error("{0}")]
    PatchRejected(#[from] PatchError),
    /// An unforced apply overlaps another manager's fields. 409 with causes.
    #[error("apply conflicts with another field manager")]
    ApplyConflict(ApplyConflicts),
    /// An apply without a field manager. 400.
    #[error("server-side apply requires a fieldManager")]
    ApplyWithoutManager,
    /// The candidate is not a legal object. 422.
    #[error("{0}")]
    Invalid(ObjectInvalid),
    /// The store answered a `Put`/`Txn` with an outcome only a `Patch` or
    /// `Delete` produces. 500.
    #[error("the store answered {0:?} to a write that proposes only a Put")]
    UnexpectedOutcome(ResourceOp),
    /// The write committed and the object is not there to read back. 500.
    #[error("{0} was written but cannot be read back")]
    Unreadable(ObjectRef),
}

impl From<WriteRefusal> for ApiError {
    fn from(refusal: WriteRefusal) -> Self {
        match refusal {
            WriteRefusal::AlreadyExists(object) => {
                Self::Conflict(object.to_string(), "resource already exists".into())
            }
            WriteRefusal::NotFound(object) => Self::NotFound(object.to_string()),
            WriteRefusal::NamespaceMissing(_) => Self::NotFound(refusal.to_string()),
            WriteRefusal::ApplyConflict(conflicts) => Self::ApplyConflict(conflicts.to_causes()),
            WriteRefusal::Invalid(_) => Self::Invalid(refusal.to_string()),
            WriteRefusal::NameMismatch { .. }
            | WriteRefusal::PatchRejected(_)
            | WriteRefusal::ApplyWithoutManager => Self::BadRequest(refusal.to_string()),
            WriteRefusal::UnexpectedOutcome(_) | WriteRefusal::Unreadable(_) => {
                Self::Internal(refusal.to_string())
            }
        }
    }
}

impl StoreBackedHandler {
    /// ★ THE write path for a main object: plan, then propose once, re-planning
    /// only when the store moved between the read and the proposal and the
    /// client did not pin the revision it wrote against. See the module docs.
    pub(super) async fn write(
        &self,
        namespace: Option<&str>,
        name: &str,
        request: WriteRequest,
        user_info: &UserInfo,
        dry_run: DryRun,
        fields: &mut PatchFields,
    ) -> Result<Value, ApiError> {
        let key = self.key(namespace, name)?;
        let client_expected = request.client_precondition()?;
        // The write's ONE clock read, frozen before planning: the
        // creationTimestamp of an object it creates and the managedFields
        // time of an apply. The store never reads a clock, so this instant
        // is what every replica stores.
        let now = engenho_types::time::now_rfc3339_utc();
        for _ in 0..WRITE_ATTEMPTS {
            let prior = self.store.get_with_meta(&key).await;
            let plan = match self
                .plan(
                    &key,
                    namespace,
                    name,
                    &request,
                    prior,
                    client_expected,
                    &now,
                    user_info,
                    fields,
                )
                .await?
            {
                Planned::Unchanged(stored) => return Ok(self.typed(&stored)),
                Planned::Write(plan) => plan,
            };
            // DRY-RUN GATE. Every rule above ran — including admission and
            // the precondition — because that is what makes a dry run worth
            // asking for. Only the proposal is skipped, and the object that
            // would have been written is the answer. PATCH and apply used to
            // refuse here: their merge happened inside the store, so the
            // result could not be known without persisting it.
            if dry_run.is_dry() {
                return Ok(self.typed(&plan.candidate));
            }
            match self.commit(&key, plan).await? {
                Committed::Stored => return self.read_back(&key, name).await,
                Committed::Released => {
                    return Ok(crate::error::delete_status_success(name, &self.kind));
                }
                Committed::Raced if client_expected.is_some() => {
                    return Err(self.rv_conflict(name, client_expected));
                }
                Committed::Raced if request.requires() == Existence::Absent => {
                    return Err(WriteRefusal::AlreadyExists(self.object_ref(name)).into());
                }
                Committed::Raced => {}
            }
        }
        Err(self.rv_conflict(name, client_expected))
    }

    /// Plan one attempt of a write against `prior`, the object as the store
    /// holds it now (step 1, read by [`Self::write`]). Runs steps 2–11;
    /// proposes nothing — steps 12 and 13 are [`Self::write`]'s.
    #[allow(clippy::too_many_arguments)] // the request, its target, and what the attempt read
    async fn plan(
        &self,
        key: &ResourceKey,
        namespace: Option<&str>,
        name: &str,
        request: &WriteRequest,
        prior: Option<(Value, VersionMeta)>,
        client_expected: Option<Revision>,
        now: &str,
        user_info: &UserInfo,
        fields: &mut PatchFields,
    ) -> Result<Planned, ApiError> {
        // ── 2. LIFECYCLE, from what the store holds. ──────────────────────
        // A POST is a create whatever the store holds: a taken name is
        // refused after every rule has run, because upstream validates
        // before its storage create answers AlreadyExists (422 before 409).
        let name_taken = request.requires() == Existence::Absent && prior.is_some();
        let lifecycle = match (request.requires(), prior) {
            (Existence::Present, None) => {
                return Err(WriteRefusal::NotFound(self.object_ref(name)).into());
            }
            (Existence::Absent, _) => Lifecycle::Create,
            (Existence::Present | Existence::Either, prior) => Lifecycle::of(prior),
        };

        // ── 3. THE CLIENT'S PRECONDITION, before any work is done for it. ──
        // A revision on a create can never hold: there is nothing to CAS.
        if let Some(expected) = client_expected
            && lifecycle.revision() != Some(expected)
        {
            return Err(self.rv_conflict(name, client_expected));
        }

        // ── 4. THE CANDIDATE: the object this write would store. ──────────
        let gvk = Gvk::from(key);
        let candidate = match request {
            WriteRequest::Create(body) | WriteRequest::Replace(body) => body.as_value().clone(),
            WriteRequest::Patch { patch, algorithm } => {
                let Some(prior) = lifecycle.prior() else {
                    // Refused at step 2; answered again rather than assumed.
                    return Err(WriteRefusal::NotFound(self.object_ref(name)).into());
                };
                let pt = algorithm.patch_type();
                let body = PatchBody::from_raw(pt, patch.clone()).map_err(WriteRefusal::from)?;
                apply_patch_algorithm(pt, prior, &body, &*PATCH_SCHEMA, &gvk)
                    .map_err(WriteRefusal::from)?
            }
            WriteRequest::Apply { config, opts } => apply_ssa(
                lifecycle.prior(),
                config,
                &opts.manager,
                opts.force,
                &gvk,
                &*PATCH_SCHEMA,
                now,
            )
            .map_err(WriteRefusal::ApplyConflict)?
            .merged()
            .clone(),
        };

        // ── 4b. FIELD VALIDATION of a patched object (T4.6). ──────────────
        // A merge, strategic or JSON patch is not an object, so its unknown
        // fields exist only here, in what it would store. The patch answers
        // for the ones it introduced: upstream's strict decode of the patched
        // object. A POST, PUT or apply body was judged whole at the border.
        let mut candidate = candidate;
        if let (WriteRequest::Patch { algorithm, .. }, Some(prior)) = (request, lifecycle.prior()) {
            let api_version = self.api_version();
            fields.judge_patched(
                KindRef {
                    group: &self.group,
                    version: &self.version,
                    kind: &self.kind,
                    api_version: &api_version,
                },
                prior,
                &mut candidate,
                *algorithm == MergeAlgorithm::Strategic,
            )?;
        }

        // ── 5. NORMALIZE the object that will be stored. ──────────────────
        // A POST or PUT body was normalized at the border already (this is
        // idempotent on it). A PATCH or apply candidate is normalized here
        // for the first time — the patch itself never is, because in a
        // patch `null` means "delete" (plan edge 9).
        let mut candidate = ObjectBody::normalize(&self.group, &self.kind, candidate)
            .map_err(|e| e.request_error(&self.version, &self.kind))?
            .into_value();

        // ── 6. THE NAME IS THE URL'S. ─────────────────────────────────────
        settle_name(&mut candidate, name)?;

        // ── 7. CREATE-ONLY REFUSALS. ──────────────────────────────────────
        if matches!(lifecycle, Lifecycle::Create) {
            self.refuse_bad_create(namespace, name).await?;
        }

        // ── 8. DEFAULT → VALIDATE → CRD SCHEMA. ───────────────────────────
        self.default_and_validate(&mut candidate, name)?;

        // ── 9. ADMISSION, over the whole candidate. ───────────────────────
        let mut candidate = self.admit(&lifecycle, key, candidate, user_info).await?;

        // ── 10. SERVER-OWNED FIELDS, after admission so no webhook moves them.
        self.stamp_server_owned(&mut candidate, namespace, name, &lifecycle, now);

        if name_taken {
            return Err(WriteRefusal::AlreadyExists(self.object_ref(name)).into());
        }

        // ── 11. AN UPDATE THAT CHANGES NOTHING PROPOSES NOTHING. ───────────
        // `unchanged` is the store's own T3.5 rule, so the answer agrees with
        // what the store would decide — including that an apply's
        // managedFields `time` is not a change. A Terminating candidate is
        // left to the store, whose rule also covers the finalizer release.
        match lifecycle {
            Lifecycle::Update { prior, .. }
                if unchanged(&prior, &candidate, request.manager())
                    && !is_terminating(&candidate) =>
            {
                Ok(Planned::Unchanged(prior))
            }
            lifecycle => Ok(Planned::Write(WritePlan {
                lifecycle,
                candidate,
            })),
        }
    }

    /// The two refusals only a create can earn: a name the kind cannot
    /// carry (422), and a namespace that does not exist (404,
    /// `NamespaceLifecycle` — enabled by the assembled runtime).
    async fn refuse_bad_create(&self, namespace: Option<&str>, name: &str) -> Result<(), ApiError> {
        // ── NAME VALIDATION (DNS-1123). ───────────────────────────────────
        // Measured 2026-08-28: `UPPERCASE`, `has spaces`, `-leading-dash`,
        // `has_underscore` and a 64-char name were all accepted with 201. The
        // rule is chosen by KIND (Namespace/Service take the stricter DNS-1123
        // LABEL), so a call site cannot pick the wrong strictness. 422, not
        // 400: client-go's `IsInvalid` keys on it, and a permanently invalid
        // object must not look transient.
        if let Err(e) = ResourceName::parse_for_kind(name, &self.kind) {
            return Err(WriteRefusal::Invalid(ObjectInvalid {
                kind: self.kind.clone(),
                name: name.to_string(),
                causes: vec![InvalidCause::Name(e)],
            })
            .into());
        }
        // ── NamespaceLifecycle. ───────────────────────────────────────────
        // Measured 2026-08-28: a ConfigMap POSTed into a namespace that did
        // not exist returned 201, and was stored permanently orphaned —
        // namespace deletion is what collects a namespace's contents.
        if self.namespace_lifecycle
            && let Some(ns) = namespace
        {
            let ns_key = ResourceKey::cluster_scoped("", "v1", "Namespace", ns.to_string());
            if self.store.get(&ns_key).await.is_none() {
                return Err(WriteRefusal::NamespaceMissing(ns.to_string()).into());
            }
        }
        Ok(())
    }

    /// Upstream's `decode → DEFAULT → VALIDATE`, then the CRD's structural
    /// schema (defaults first, so a default can satisfy `required`).
    ///
    /// Defaulting runs before validation so a validator judges the object
    /// the cluster will actually store, and before admission so a mutating
    /// webhook can see and override a default. Measured on rio 2026-09-15:
    /// Flux's `GitRepository` CRD defaults `spec.timeout`; the object was
    /// stored without it and source-controller v1.8.5 panicked on the nil
    /// duration on every reconcile.
    fn default_and_validate(&self, candidate: &mut Value, name: &str) -> Result<(), ApiError> {
        crate::defaulting::apply(&self.group, &self.version, &self.kind, candidate);
        let violations =
            crate::validation::validate(&self.group, &self.version, &self.kind, candidate);
        if !violations.is_empty() {
            return Err(self
                .invalid(name, violations.into_iter().map(InvalidCause::Field))
                .into());
        }
        if let Some(schema) = &self.crd_schema {
            crate::schema_validation::apply_defaults(schema, candidate);
            let violations = crate::schema_validation::validate(schema, candidate);
            if !violations.is_empty() {
                return Err(self
                    .invalid(name, violations.into_iter().map(InvalidCause::Schema))
                    .into());
            }
        }
        Ok(())
    }

    /// Run the admission chain over the candidate. No chain admits it
    /// unchanged; `Deny` is 403. A mutating webhook returns a whole new body,
    /// which is normalized again (upstream decodes a webhook-patched object
    /// into its Go type), and a body the webhook left mis-shaped is the
    /// webhook's fault: upstream's 500, not the client's 400.
    async fn admit(
        &self,
        lifecycle: &Lifecycle,
        key: &ResourceKey,
        candidate: Value,
        user_info: &UserInfo,
    ) -> Result<Value, ApiError> {
        let Some(chain) = &self.admission else {
            return Ok(candidate);
        };
        let request = admission_request(
            lifecycle.admission_action(),
            key,
            Some(candidate.clone()),
            lifecycle.prior().cloned(),
            user_info,
        );
        match chain.review(request).await {
            AdmissionDecision::Allow => Ok(candidate),
            AdmissionDecision::Mutate(admitted) => {
                ObjectBody::normalize(&self.group, &self.kind, admitted)
                    .map(ObjectBody::into_value)
                    .map_err(|e| e.admission_error(&self.version, &self.kind))
            }
            AdmissionDecision::Deny(reason) => Err(ApiError::Forbidden(reason)),
        }
    }

    /// The fields no client and no webhook writes: the key's name and
    /// namespace; on create the timestamp and a Namespace's lifecycle stamps;
    /// on update the live object's identity and termination, its `.status`
    /// when the kind serves `/status`, and a Service's allocated cluster IP.
    fn stamp_server_owned(
        &self,
        candidate: &mut Value,
        namespace: Option<&str>,
        name: &str,
        lifecycle: &Lifecycle,
        now: &str,
    ) {
        stamp_name(candidate, name);
        if let Some(ns) = namespace {
            stamp_namespace(candidate, ns);
        }
        match lifecycle {
            Lifecycle::Create => {
                stamp_creation_timestamp(candidate, now);
                // Upstream's namespace-lifecycle plugin seeds these; engenho
                // stamps them so a Namespace is born Active, with the
                // `kubernetes` finalizer its deletion cascade waits on.
                if self.kind == "Namespace" {
                    stamp_namespace_create_defaults(candidate);
                }
            }
            Lifecycle::Update { prior, .. } => {
                preserve_immutable_meta(candidate, prior);
                if self.subresources.contains(&Subresource::Status) {
                    preserve_status(candidate, prior);
                }
                self.carry_allocated_values(candidate, prior);
            }
        }
    }

    /// Upstream's `patchAllocatedValues`, for the one value engenho allocates
    /// at admission: a Service's cluster IP. The allocator reviews CREATE
    /// only, so an update that leaves `clusterIP`/`clusterIPs` out keeps the
    /// allocated ones rather than storing a Service with no VIP. An
    /// `ExternalName` Service has none to keep.
    fn carry_allocated_values(&self, candidate: &mut Value, prior: &Value) {
        if !(self.group.is_empty() && self.version == "v1" && self.kind == "Service") {
            return;
        }
        if is_external_name(prior) || is_external_name(candidate) {
            return;
        }
        let Some(prior_spec) = prior.get("spec") else {
            return;
        };
        let Some(spec) = candidate.get_mut("spec").and_then(Value::as_object_mut) else {
            return;
        };
        let unset_ip = spec
            .get("clusterIP")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty);
        if unset_ip && let Some(ip) = prior_spec.get("clusterIP") {
            spec.insert("clusterIP".to_string(), ip.clone());
        }
        let unset_ips = spec
            .get("clusterIPs")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty);
        let prior_ips = prior_spec
            .get("clusterIPs")
            .filter(|ips| ips.as_array().is_some_and(|a| !a.is_empty()));
        if unset_ips && let Some(ips) = prior_ips {
            spec.insert("clusterIPs".to_string(), ips.clone());
        }
    }

    /// Propose a plan: the ONE `propose` on the main-object write path. A
    /// create commits only if the name is still free; an update only if the
    /// object is still at the revision the plan read.
    async fn commit(&self, key: &ResourceKey, plan: WritePlan) -> Result<Committed, ApiError> {
        let WritePlan {
            lifecycle,
            candidate,
        } = plan;
        let command = match lifecycle {
            Lifecycle::Create => ResourceCommand::Txn {
                compares: vec![TxnCompare::NotExists { key: key.clone() }],
                success: vec![TxnOp::Put {
                    key: key.clone(),
                    value: candidate,
                }],
                failure: Vec::new(),
                reason: Reason::Operator,
            },
            Lifecycle::Update { at, .. } => ResourceCommand::Put {
                key: key.clone(),
                value: candidate,
                expected: Some(at),
                reason: Reason::Operator,
            },
        };
        let result = self
            .store
            .propose(command)
            .await
            .map_err(|e| ApiError::StorageError(e.to_string()))?;
        match result.op {
            ResourceOp::Created | ResourceOp::Replaced | ResourceOp::Unchanged => {
                Ok(Committed::Stored)
            }
            ResourceOp::Deleted => Ok(Committed::Released),
            // A Put's CAS missed, or a create's `NotExists` compare failed.
            ResourceOp::Conflict | ResourceOp::NoOp => Ok(Committed::Raced),
            op @ (ResourceOp::Patched
            | ResourceOp::DeletionPending
            | ResourceOp::PatchRejected
            | ResourceOp::ApplyConflict) => Err(WriteRefusal::UnexpectedOutcome(op).into()),
        }
    }

    /// Read back the committed object, with the revision it committed at.
    async fn read_back(&self, key: &ResourceKey, name: &str) -> Result<Value, ApiError> {
        match self.store.get(key).await {
            Some(stored) => Ok(self.typed(&stored)),
            None => Err(WriteRefusal::Unreadable(self.object_ref(name)).into()),
        }
    }

    /// An object as the wire returns it: with `kind` and `apiVersion`.
    fn typed(&self, object: &Value) -> Value {
        inject_type_meta(object, self.api_version(), &self.kind)
    }

    fn object_ref(&self, name: &str) -> ObjectRef {
        ObjectRef {
            kind: self.kind.clone(),
            name: name.to_string(),
        }
    }

    fn invalid(&self, name: &str, causes: impl Iterator<Item = InvalidCause>) -> WriteRefusal {
        WriteRefusal::Invalid(ObjectInvalid {
            kind: self.kind.clone(),
            name: name.to_string(),
            causes: causes.collect(),
        })
    }
}

/// Upstream's `checkName`: the object's `metadata.name` is the one in the
/// URL. Absent, it is the URL's; different, the write is refused (400), so a
/// stored object can never disagree with the key it is stored under. Runs
/// after normalization, which guarantees `metadata` is an object if present.
fn settle_name(candidate: &mut Value, url: &str) -> Result<(), WriteRefusal> {
    match candidate
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
    {
        Some(body) if body != url => Err(WriteRefusal::NameMismatch {
            body: body.to_string(),
            url: url.to_string(),
        }),
        _ => {
            stamp_name(candidate, url);
            Ok(())
        }
    }
}

/// Set `metadata.name` to the key's name. After admission this overrides a
/// webhook that renamed the object: the key is the authority, as it is for
/// the namespace.
fn stamp_name(candidate: &mut Value, name: &str) {
    if let Some(obj) = candidate.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta) = metadata.as_object_mut() {
            meta.insert("name".to_string(), Value::String(name.to_string()));
        }
    }
}

/// A Terminating object: one carrying `metadata.deletionTimestamp`.
fn is_terminating(object: &Value) -> bool {
    object
        .get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .and_then(Value::as_str)
        .is_some_and(|ts| !ts.is_empty())
}

/// A Service of `type: ExternalName`, which has no cluster IP to keep.
fn is_external_name(service: &Value) -> bool {
    service
        .get("spec")
        .and_then(|s| s.get("type"))
        .and_then(Value::as_str)
        == Some("ExternalName")
}
