//! W1 (T4.5) — ONE WRITE PIPELINE, pinned as a matrix: every write verb
//! against every write rule.
//!
//! POST, PUT, PATCH and server-side apply used to be four paths to the store,
//! each carrying a different subset of the rules (the table is in
//! `src/handler/write_plan.rs`). This matrix sends each rule's intent through
//! each verb — over real HTTP, into a real `StoreMesh` — and asserts the
//! OUTCOME: the status code, the object the store now holds, the store's
//! revision, and what admission was shown. A verb that skips a rule fails
//! its cell. A rule that does not apply to a verb is a `NotApplicable` row
//! that says why, never a silent gap.
//!
//! An update verb's prior object is seeded straight into the store, past
//! every rule, so its cell measures what the VERB enforces rather than what
//! the object already had.
//!
//! A verb or rule cannot be added without running: each enum's `ALL` is
//! emitted with its variants, [`case`] and [`check`] match every rule, and
//! [`Verb::routable`] derives the PATCH verbs from `PatchType` exhaustively
//! and must equal `Verb::ALL`.
//!
//! Beside the matrix, two concurrency tests pin what the compare-and-swap
//! must keep: concurrent patches lose no update, and concurrent creates of
//! one name succeed exactly once.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_controllers::{
    AdmissionAction, AdmissionChain, AdmissionDecision, AdmissionError, AdmissionMode,
    AdmissionRequest, AdmissionWebhook,
};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, Revision, StoreMesh, default_config};
use engenho_types::patch::PatchType;

// ═══════════════════════════════════════════════════════════════════════════
//  The two axes.
// ═══════════════════════════════════════════════════════════════════════════

/// An enum whose `ALL` list is emitted from the same variant list, so a
/// variant cannot exist without being iterated: a new verb or rule runs in
/// the matrix the moment it compiles.
macro_rules! closed_enum {
    ($(#[$meta:meta])* enum $name:ident { $($(#[$vmeta:meta])* $variant:ident,)+ }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum $name { $($(#[$vmeta])* $variant,)+ }
        impl $name {
            const ALL: &'static [$name] = &[$($name::$variant,)+];
        }
    };
}

closed_enum! {
    /// A write verb. Server-side apply is an upsert, so it is two verbs: one
    /// that creates and one that updates.
    enum Verb {
        Post,
        ApplyCreate,
        Put,
        MergePatch,
        StrategicPatch,
        JsonPatch,
        ApplyUpdate,
    }
}

impl Verb {
    /// The verbs the router can produce: POST and PUT, then one per
    /// `PatchType` by an exhaustive match, so a new patch algorithm cannot
    /// compile without a verb here. The matrix asserts this is exactly
    /// [`Verb::ALL`].
    fn routable() -> Vec<Self> {
        let mut verbs = vec![Self::Post, Self::Put];
        for pt in [
            PatchType::Merge,
            PatchType::Strategic,
            PatchType::Json,
            PatchType::Apply,
        ] {
            match pt {
                PatchType::Merge => verbs.push(Self::MergePatch),
                PatchType::Strategic => verbs.push(Self::StrategicPatch),
                PatchType::Json => verbs.push(Self::JsonPatch),
                PatchType::Apply => {
                    verbs.push(Self::ApplyCreate);
                    verbs.push(Self::ApplyUpdate);
                }
            }
        }
        verbs
    }

    fn creates(self) -> bool {
        matches!(self, Self::Post | Self::ApplyCreate)
    }

    fn slug(self) -> &'static str {
        match self {
            Self::Post => "post",
            Self::ApplyCreate => "apc",
            Self::Put => "put",
            Self::MergePatch => "mrg",
            Self::StrategicPatch => "smp",
            Self::JsonPatch => "jsn",
            Self::ApplyUpdate => "apu",
        }
    }
}

closed_enum! {
    /// A write rule: something every write the rule applies to must obey.
    enum Rule {
        /// Built-in defaulting fills what the object omits.
        Defaulted,
        /// Built-in validation refuses an illegal object (422).
        Validated,
        /// A CRD schema's defaults are filled.
        SchemaDefaulted,
        /// A CRD schema's types are enforced (422).
        SchemaValidated,
        /// Admission reviews the whole candidate object, as CREATE or UPDATE
        /// by lifecycle, and its mutation is stored.
        AdmissionSeesTheObject,
        /// An admission deny is 403 and stores nothing.
        AdmissionDenies,
        /// Mis-shaped metadata in the object that would be stored is 400.
        MetadataShape,
        /// The stored object's name is the URL's (400 otherwise).
        NameIsTheUrl,
        /// A created object's name obeys the kind's naming rule (422).
        NameFormat,
        /// A create into a missing namespace is 404 (NamespaceLifecycle).
        NamespaceMustExist,
        /// A created object is born with its server stamps.
        CreateStamps,
        /// An update cannot move `uid` or `creationTimestamp`.
        IdentityIsServerOwned,
        /// An update cannot move `deletionTimestamp`.
        TerminationIsServerOwned,
        /// A main-object update does not write `.status` of a status-bearing
        /// kind.
        StatusNotWrittenHere,
        /// An update that omits a Service's allocated cluster IP keeps it.
        ClusterIpCarriesOver,
        /// A stale client `resourceVersion` is 409 and stores nothing.
        StalePreconditionConflicts,
        /// `dryRun=All` runs every rule and stores nothing.
        DryRun,
        /// Re-sending a write that is already in effect commits no revision.
        UnchangedCommitsNothing,
    }
}

impl Rule {
    fn slug(self) -> &'static str {
        match self {
            Self::Defaulted => "dflt",
            Self::Validated => "valid",
            Self::SchemaDefaulted => "sdflt",
            Self::SchemaValidated => "svalid",
            Self::AdmissionSeesTheObject => "adm",
            Self::AdmissionDenies => "deny",
            Self::MetadataShape => "shape",
            Self::NameIsTheUrl => "name",
            Self::NameFormat => "fmt",
            Self::NamespaceMustExist => "nsx",
            Self::CreateStamps => "stamp",
            Self::IdentityIsServerOwned => "ident",
            Self::TerminationIsServerOwned => "term",
            Self::StatusNotWrittenHere => "status",
            Self::ClusterIpCarriesOver => "cip",
            Self::StalePreconditionConflicts => "stale",
            Self::DryRun => "dry",
            Self::UnchangedCommitsNothing => "same",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Cells.
// ═══════════════════════════════════════════════════════════════════════════

/// The kinds the matrix writes.
#[derive(Clone, Copy, Debug)]
enum Target {
    /// Built-in defaulting + validation + a `/status` subresource.
    Pod,
    /// A Service: the kind whose cluster IP is allocated at admission.
    Service,
    /// Cluster-scoped, with create stamps of its own.
    Namespace,
    /// A custom resource with a structural schema.
    Widget,
}

impl Target {
    fn key(self, namespace: &str, name: &str) -> ResourceKey {
        match self {
            Self::Pod => ResourceKey::namespaced("", "v1", "Pod", namespace, name),
            Self::Service => ResourceKey::namespaced("", "v1", "Service", namespace, name),
            Self::Namespace => ResourceKey::cluster_scoped("", "v1", "Namespace", name),
            Self::Widget => ResourceKey::namespaced("example.com", "v1", "Widget", namespace, name),
        }
    }

    fn collection(self, addr: &str, namespace: &str) -> String {
        match self {
            Self::Pod => format!("http://{addr}/api/v1/namespaces/{namespace}/pods"),
            Self::Service => format!("http://{addr}/api/v1/namespaces/{namespace}/services"),
            Self::Namespace => format!("http://{addr}/api/v1/namespaces"),
            Self::Widget => {
                format!("http://{addr}/apis/example.com/v1/namespaces/{namespace}/widgets")
            }
        }
    }

    fn item(self, addr: &str, namespace: &str, name: &str) -> String {
        format!("{}/{name}", self.collection(addr, namespace))
    }
}

/// One cell's write.
struct Case {
    target: Target,
    namespace: &'static str,
    /// The name in the URL (and, for POST, in the body).
    name: String,
    /// Seeded straight into the store — past every rule — before the write.
    /// `None` for a create.
    prior: Option<Value>,
    /// The object the client wants.
    desired: Value,
    dry_run: bool,
    /// Send the write twice; the outcome is the second.
    repeat: bool,
}

enum Cell {
    Run(Case),
    NotApplicable(&'static str),
}

/// A legal, UNDEFAULTED Pod — what a raw store write holds.
fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "matrix"}},
        "spec": {"containers": [{"name": "app", "image": "registry.example/app:1"}]}
    })
}

fn service(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": "default", "labels": {"app": "matrix"}},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.96.0.10",
            "clusterIPs": ["10.96.0.10"],
            "ports": [{"port": 80, "protocol": "TCP"}]
        }
    })
}

fn namespace(name: &str) -> Value {
    json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
}

fn widget(name: &str) -> Value {
    json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"size": 1}
    })
}

/// The Widget CRD's schema: an integer `size` (required) and a `mode` that
/// defaults to `fast`.
fn widget_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "required": ["size"],
                "properties": {
                    "size": {"type": "integer"},
                    "mode": {"type": "string", "default": "fast"}
                }
            }
        }
    })
}

/// Split a JSON pointer into unescaped segments.
fn segments(pointer: &str) -> Vec<String> {
    pointer
        .split('/')
        .skip(1)
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// `value` with `pointer` set to `to`, creating objects on the way.
fn with(mut value: Value, pointer: &str, to: Value) -> Value {
    let segs = segments(pointer);
    let (last, parents) = segs.split_last().expect("a non-root pointer");
    let mut at = &mut value;
    for seg in parents {
        at = at
            .as_object_mut()
            .expect("an object on the path")
            .entry(seg.clone())
            .or_insert_with(|| json!({}));
    }
    at.as_object_mut()
        .expect("an object at the parent")
        .insert(last.clone(), to);
    value
}

/// `value` with `pointer` removed.
fn without(mut value: Value, pointer: &str) -> Value {
    let segs = segments(pointer);
    let (last, parents) = segs.split_last().expect("a non-root pointer");
    let mut at = &mut value;
    for seg in parents {
        at = at.get_mut(seg.as_str()).expect("the path exists");
    }
    at.as_object_mut()
        .expect("an object at the parent")
        .remove(last.as_str());
    value
}

fn touched(value: Value) -> Value {
    with(value, "/metadata/labels/touched", json!("yes"))
}

/// The cell for `rule` × `verb`: the write that expresses the rule's intent
/// through the verb, or why the rule does not apply to it.
fn case(rule: Rule, verb: Verb) -> Cell {
    let name = format!("{}-{}", rule.slug(), verb.slug());
    // An update verb patches a seeded prior; a create verb has none.
    let run = |target: Target, base: Value, desired: Value| {
        Cell::Run(Case {
            target,
            namespace: "default",
            name: name.clone(),
            prior: (!verb.creates()).then_some(base),
            desired,
            dry_run: false,
            repeat: false,
        })
    };
    match rule {
        Rule::Defaulted => {
            let base = pod(&name);
            run(Target::Pod, base.clone(), touched(base))
        }
        Rule::Validated => {
            let base = pod(&name);
            run(
                Target::Pod,
                base.clone(),
                with(base, "/spec/restartPolicy", json!("Sometimes")),
            )
        }
        Rule::SchemaDefaulted => {
            let base = widget(&name);
            run(
                Target::Widget,
                base.clone(),
                with(base, "/spec/size", json!(2)),
            )
        }
        Rule::SchemaValidated => {
            let base = widget(&name);
            run(
                Target::Widget,
                base.clone(),
                with(base, "/spec/size", json!("NOT-AN-INT")),
            )
        }
        Rule::AdmissionSeesTheObject => {
            let base = pod(&name);
            run(Target::Pod, base.clone(), touched(base))
        }
        Rule::AdmissionDenies => {
            let base = pod(&name);
            run(
                Target::Pod,
                base.clone(),
                with(base, "/metadata/labels/matrix~1deny", json!("yes")),
            )
        }
        Rule::MetadataShape => {
            let base = pod(&name);
            run(
                Target::Pod,
                base.clone(),
                with(base, "/metadata/labels", json!("oops")),
            )
        }
        Rule::NameIsTheUrl => match verb {
            Verb::Post => Cell::NotApplicable(
                "a POST names its object in the body; there is no URL name to disagree with",
            ),
            _ => {
                let base = pod(&name);
                let other = format!("{name}-other");
                run(
                    Target::Pod,
                    base.clone(),
                    with(base, "/metadata/name", json!(other)),
                )
            }
        },
        Rule::NameFormat => {
            if verb.creates() {
                let bad = format!("Bad-{}", verb.slug());
                Cell::Run(Case {
                    target: Target::Pod,
                    namespace: "default",
                    name: bad.clone(),
                    prior: None,
                    desired: pod(&bad),
                    dry_run: false,
                    repeat: false,
                })
            } else {
                Cell::NotApplicable(
                    "the name was judged when the object was created, and an update cannot \
                     change it (NameIsTheUrl)",
                )
            }
        }
        Rule::NamespaceMustExist => {
            if verb.creates() {
                Cell::Run(Case {
                    target: Target::Pod,
                    namespace: "ghost",
                    name: name.clone(),
                    prior: None,
                    desired: with(pod(&name), "/metadata/namespace", json!("ghost")),
                    dry_run: false,
                    repeat: false,
                })
            } else {
                Cell::NotApplicable("an existing object already lives in its namespace")
            }
        }
        Rule::CreateStamps => {
            if verb.creates() {
                Cell::Run(Case {
                    target: Target::Namespace,
                    namespace: "",
                    name: name.clone(),
                    prior: None,
                    desired: namespace(&name),
                    dry_run: false,
                    repeat: false,
                })
            } else {
                Cell::NotApplicable(
                    "stamped once, at create; IdentityIsServerOwned keeps them afterwards",
                )
            }
        }
        Rule::IdentityIsServerOwned => {
            if verb.creates() {
                Cell::NotApplicable("a create has no identity to keep yet (see CreateStamps)")
            } else {
                let base = with(
                    with(pod(&name), "/metadata/uid", json!("uid-original")),
                    "/metadata/creationTimestamp",
                    json!("2020-01-01T00:00:00Z"),
                );
                let desired = touched(with(
                    with(base.clone(), "/metadata/uid", json!("uid-forged")),
                    "/metadata/creationTimestamp",
                    json!("1999-01-01T00:00:00Z"),
                ));
                run(Target::Pod, base, desired)
            }
        }
        Rule::TerminationIsServerOwned => {
            if verb.creates() {
                Cell::NotApplicable("only DELETE starts termination; a create has none to keep")
            } else {
                let base = with(
                    with(
                        pod(&name),
                        "/metadata/deletionTimestamp",
                        json!("2026-01-01T00:00:00Z"),
                    ),
                    "/metadata/finalizers",
                    json!(["matrix/hold"]),
                );
                // An apply cannot remove a field it never owned, so it tries
                // to MOVE the timestamp instead; the other verbs remove it.
                let desired = if verb == Verb::ApplyUpdate {
                    with(
                        base.clone(),
                        "/metadata/deletionTimestamp",
                        json!("2030-01-01T00:00:00Z"),
                    )
                } else {
                    without(base.clone(), "/metadata/deletionTimestamp")
                };
                run(Target::Pod, base, touched(desired))
            }
        }
        Rule::StatusNotWrittenHere => {
            if verb.creates() {
                Cell::NotApplicable(
                    "engenho stores a created object's status as sent; upstream resets it per \
                     kind, which is not pinned here",
                )
            } else {
                let base = with(pod(&name), "/status", json!({"phase": "Running"}));
                let desired = touched(with(base.clone(), "/status/phase", json!("Failed")));
                run(Target::Pod, base, desired)
            }
        }
        Rule::ClusterIpCarriesOver => {
            if verb.creates() {
                Cell::NotApplicable(
                    "allocation is the ClusterIP admission plugin's, which reviews CREATE",
                )
            } else {
                let base = service(&name);
                // An apply cannot remove fields it never owned; it clears them.
                let desired = if verb == Verb::ApplyUpdate {
                    with(
                        with(base.clone(), "/spec/clusterIP", json!("")),
                        "/spec/clusterIPs",
                        json!([]),
                    )
                } else {
                    without(without(base.clone(), "/spec/clusterIP"), "/spec/clusterIPs")
                };
                run(Target::Service, base, touched(desired))
            }
        }
        Rule::StalePreconditionConflicts => match verb {
            Verb::Post | Verb::ApplyCreate => Cell::NotApplicable(
                "upstream refuses a resourceVersion on create, and not as a precondition \
                 conflict; not pinned here",
            ),
            Verb::JsonPatch => Cell::NotApplicable(
                "RFC 6902 carries no resourceVersion precondition; its `test` op is the \
                 precondition, and an op that adds one only sets a field",
            ),
            Verb::Put | Verb::MergePatch | Verb::StrategicPatch | Verb::ApplyUpdate => {
                let base = pod(&name);
                run(
                    Target::Pod,
                    base.clone(),
                    touched(with(base, "/metadata/resourceVersion", json!("999999"))),
                )
            }
        },
        Rule::DryRun => {
            let base = pod(&name);
            match run(Target::Pod, base.clone(), touched(base)) {
                Cell::Run(mut c) => {
                    c.dry_run = true;
                    Cell::Run(c)
                }
                na @ Cell::NotApplicable(_) => na,
            }
        }
        Rule::UnchangedCommitsNothing => {
            if verb.creates() {
                Cell::NotApplicable(
                    "a repeated POST is AlreadyExists, and a repeated create-apply is an \
                     update (the ApplyUpdate cell)",
                )
            } else {
                let base = pod(&name);
                match run(Target::Pod, base.clone(), touched(base)) {
                    Cell::Run(mut c) => {
                        c.repeat = true;
                        Cell::Run(c)
                    }
                    na @ Cell::NotApplicable(_) => na,
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Admission: record every review, deny on a label, mark what it admits.
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
struct Review {
    action: AdmissionAction,
    key: String,
    object: Option<Value>,
}

#[derive(Default)]
struct Recorder {
    reviews: Mutex<Vec<Review>>,
}

#[async_trait]
impl AdmissionWebhook for Recorder {
    fn name(&self) -> &'static str {
        "matrix"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        self.reviews.lock().await.push(Review {
            action: request.action,
            key: request.key.label(),
            object: request.value.clone(),
        });
        let Some(object) = &request.value else {
            return Ok(AdmissionDecision::Allow);
        };
        if object.pointer("/metadata/labels/matrix~1deny") == Some(&json!("yes")) {
            return Ok(AdmissionDecision::Deny("the matrix denies this".into()));
        }
        // Mark what was admitted. A constant, so a repeated write stays
        // unchanged.
        if object.pointer("/metadata").is_some_and(Value::is_object) {
            return Ok(AdmissionDecision::Mutate(with(
                object.clone(),
                "/metadata/annotations/matrix~1admitted",
                json!("yes"),
            )));
        }
        Ok(AdmissionDecision::Allow)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Running a cell.
// ═══════════════════════════════════════════════════════════════════════════

struct Harness {
    store: Arc<StoreMesh>,
    server: ApiServer,
    admission: Arc<Recorder>,
    client: reqwest::Client,
}

async fn boot_store(cluster: &str) -> Arc<StoreMesh> {
    let router = InProcessRouter::new();
    let cfg = default_config(cluster).unwrap();
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), router, cfg)
            .await
            .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

async fn seed(store: &StoreMesh, key: ResourceKey, value: Value) {
    store
        .propose(ResourceCommand::put(key, value, Reason::Operator))
        .await
        .unwrap();
}

async fn boot_matrix() -> Harness {
    let store = boot_store("apiserver-w1-matrix").await;
    seed(
        &store,
        Target::Namespace.key("", "default"),
        namespace("default"),
    )
    .await;
    let admission = Arc::new(Recorder::default());
    let chain = Arc::new(AdmissionChain::new(
        vec![admission.clone() as Arc<dyn AdmissionWebhook>],
        AdmissionMode::FailClosed,
    ));
    let cataloged = |kind: &str| -> Arc<dyn ResourceHandler> {
        Arc::new(
            StoreBackedHandler::for_kind(store.clone(), kind)
                .expect("a cataloged kind")
                .with_admission(chain.clone())
                .with_namespace_lifecycle(),
        )
    };
    let widgets: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::new(
            store.clone(),
            "example.com",
            "v1",
            "Widget",
            "widgets",
            true,
        )
        .with_admission(chain.clone())
        .with_crd_schema(widget_schema())
        .with_namespace_lifecycle(),
    );
    let handlers = vec![
        cataloged("Pod"),
        cataloged("Service"),
        cataloged("Namespace"),
        widgets,
    ];
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), handlers, None)
        .await
        .unwrap();
    Harness {
        store,
        server,
        admission,
        client: reqwest::Client::new(),
    }
}

/// The RFC 7396 patch that turns `prior` into `desired`.
fn merge_diff(prior: &Value, desired: &Value) -> Value {
    match (prior.as_object(), desired.as_object()) {
        (Some(p), Some(d)) => {
            let mut out = Map::new();
            for (k, dv) in d {
                match p.get(k) {
                    Some(pv) if pv == dv => {}
                    Some(pv) => {
                        out.insert(k.clone(), merge_diff(pv, dv));
                    }
                    None => {
                        out.insert(k.clone(), dv.clone());
                    }
                }
            }
            for k in p.keys() {
                if !d.contains_key(k) {
                    out.insert(k.clone(), Value::Null);
                }
            }
            Value::Object(out)
        }
        _ => desired.clone(),
    }
}

/// The RFC 6902 ops that turn `prior` into `desired`.
fn json_ops(prior: &Value, desired: &Value, path: &str, ops: &mut Vec<Value>) {
    let escape = |k: &str| k.replace('~', "~0").replace('/', "~1");
    match (prior.as_object(), desired.as_object()) {
        (Some(p), Some(d)) => {
            for (k, dv) in d {
                let child = format!("{path}/{}", escape(k));
                match p.get(k) {
                    Some(pv) if pv == dv => {}
                    Some(pv) if pv.is_object() && dv.is_object() => {
                        json_ops(pv, dv, &child, ops);
                    }
                    _ => ops.push(json!({"op": "add", "path": child, "value": dv})),
                }
            }
            for k in p.keys() {
                if !d.contains_key(k) {
                    ops.push(json!({"op": "remove", "path": format!("{path}/{}", escape(k))}));
                }
            }
        }
        _ => ops.push(json!({"op": "replace", "path": path, "value": desired})),
    }
}

impl Harness {
    fn addr(&self) -> String {
        self.server.local_addr().to_string()
    }

    /// Send `case` through `verb` once.
    async fn send(&self, verb: Verb, case: &Case) -> (u16, Value) {
        let addr = self.addr();
        let item = case.target.item(&addr, case.namespace, &case.name);
        let dry = if case.dry_run { "dryRun=All" } else { "" };
        let with_query = |url: String, q: &str| {
            if q.is_empty() {
                url
            } else {
                format!("{url}?{q}")
            }
        };
        let patch = |content_type: &str, body: String| {
            self.client
                .patch(with_query(item.clone(), dry))
                .header("Content-Type", content_type)
                .body(body)
        };
        let prior = case.prior.clone().unwrap_or_else(|| json!({}));
        let request = match verb {
            Verb::Post => self
                .client
                .post(with_query(
                    case.target.collection(&addr, case.namespace),
                    dry,
                ))
                .json(&case.desired),
            Verb::Put => self
                .client
                .put(with_query(item.clone(), dry))
                .json(&case.desired),
            Verb::MergePatch => patch(
                "application/merge-patch+json",
                merge_diff(&prior, &case.desired).to_string(),
            ),
            Verb::StrategicPatch => patch(
                "application/strategic-merge-patch+json",
                merge_diff(&prior, &case.desired).to_string(),
            ),
            Verb::JsonPatch => {
                let mut ops = Vec::new();
                json_ops(&prior, &case.desired, "", &mut ops);
                patch("application/json-patch+json", Value::Array(ops).to_string())
            }
            Verb::ApplyCreate | Verb::ApplyUpdate => {
                let q = if case.dry_run {
                    "fieldManager=matrix&dryRun=All"
                } else {
                    "fieldManager=matrix"
                };
                self.client
                    .patch(with_query(item.clone(), q))
                    .header("Content-Type", "application/apply-patch+yaml")
                    .body(case.desired.to_string())
            }
        };
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }
}

/// What a cell's write did.
struct Outcome {
    status: u16,
    body: Value,
    /// What the store holds under the key afterwards.
    stored: Option<Value>,
    /// The key's `resourceVersion` just before the (last) write.
    rv_before: Option<String>,
    /// The store's revision just before and after the (last) write.
    store_before: Revision,
    store_after: Revision,
    /// The last admission review of this key.
    review: Option<Review>,
}

fn rv(object: Option<&Value>) -> Option<String> {
    object
        .and_then(|o| o.pointer("/metadata/resourceVersion"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

async fn run(h: &Harness, verb: Verb, case: &Case) -> Result<Outcome, String> {
    let key = case.target.key(case.namespace, &case.name);
    if let Some(prior) = &case.prior {
        seed(&h.store, key.clone(), prior.clone()).await;
    }
    if case.repeat {
        let (status, body) = h.send(verb, case).await;
        if !(200..300).contains(&status) {
            return Err(format!("the first of two writes answered {status}: {body}"));
        }
        // An apply restamps its manager's `managedFields` time, at one-second
        // resolution. Inside the same second the re-apply's time equals the
        // first and nothing distinguishes "unchanged" from "changed only in
        // time"; crossing a second makes the cell exercise the rule that an
        // apply's own timestamp is not a change.
        if verb == Verb::ApplyUpdate {
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
    }
    let rv_before = rv(h.store.get(&key).await.as_ref());
    let store_before = h.store.current_revision().await;
    let (status, body) = h.send(verb, case).await;
    let store_after = h.store.current_revision().await;
    let stored = h.store.get(&key).await;
    let label = key.label();
    let review = h
        .admission
        .reviews
        .lock()
        .await
        .iter()
        .rev()
        .find(|r| r.key == label)
        .cloned();
    Ok(Outcome {
        status,
        body,
        stored,
        rv_before,
        store_before,
        store_after,
        review,
    })
}

fn at<'a>(object: Option<&'a Value>, pointer: &str) -> Option<&'a Value> {
    object.and_then(|o| o.pointer(pointer))
}

/// Refused, and nothing written: the key is as it was and the store
/// consumed no revision.
fn refused_with(out: &Outcome, code: u16) -> Result<(), String> {
    if out.status != code {
        return Err(format!("expected {code}, got {}: {}", out.status, out.body));
    }
    if out.store_after != out.store_before {
        return Err(format!(
            "a refused write consumed revisions {:?} -> {:?}",
            out.store_before, out.store_after
        ));
    }
    if rv(out.stored.as_ref()) != out.rv_before {
        return Err("a refused write changed the stored object".into());
    }
    Ok(())
}

fn succeeded(out: &Outcome) -> Result<(), String> {
    if (200..300).contains(&out.status) {
        Ok(())
    } else {
        Err(format!("expected 2xx, got {}: {}", out.status, out.body))
    }
}

fn expect_eq(what: &str, got: Option<&Value>, want: &Value) -> Result<(), String> {
    if got == Some(want) {
        Ok(())
    } else {
        Err(format!("{what}: expected {want}, got {got:?}"))
    }
}

/// Judge a cell's outcome against its rule.
fn check(rule: Rule, verb: Verb, case: &Case, out: &Outcome) -> Result<(), String> {
    let stored = out.stored.as_ref();
    match rule {
        Rule::Defaulted => {
            succeeded(out)?;
            expect_eq(
                "stored spec.restartPolicy",
                at(stored, "/spec/restartPolicy"),
                &json!("Always"),
            )
        }
        Rule::Validated | Rule::SchemaValidated => refused_with(out, 422),
        Rule::SchemaDefaulted => {
            succeeded(out)?;
            expect_eq("stored spec.mode", at(stored, "/spec/mode"), &json!("fast"))
        }
        Rule::AdmissionSeesTheObject => {
            succeeded(out)?;
            expect_eq(
                "stored admission mark",
                at(stored, "/metadata/annotations/matrix~1admitted"),
                &json!("yes"),
            )?;
            let review = out.review.as_ref().ok_or("admission never reviewed it")?;
            let want = if verb.creates() {
                AdmissionAction::Put
            } else {
                AdmissionAction::Patch
            };
            if review.action != want {
                return Err(format!(
                    "admission saw {:?}, expected {want:?} (CREATE is Put, UPDATE is Patch)",
                    review.action
                ));
            }
            let object = review.object.as_ref();
            expect_eq(
                "admission's object spec.containers[0].name",
                at(object, "/spec/containers/0/name"),
                &json!("app"),
            )?;
            expect_eq(
                "admission's object metadata.labels.touched",
                at(object, "/metadata/labels/touched"),
                &json!("yes"),
            )
        }
        Rule::AdmissionDenies => refused_with(out, 403),
        Rule::MetadataShape | Rule::NameIsTheUrl => refused_with(out, 400),
        Rule::NameFormat => refused_with(out, 422),
        Rule::NamespaceMustExist => refused_with(out, 404),
        Rule::CreateStamps => {
            succeeded(out)?;
            let created = at(stored, "/metadata/creationTimestamp")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if created.is_empty() {
                return Err("created without a creationTimestamp".into());
            }
            let finalizers = at(stored, "/spec/finalizers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if !finalizers.contains(&json!("kubernetes")) {
                return Err(format!(
                    "Namespace born without the kubernetes finalizer: {finalizers:?}"
                ));
            }
            expect_eq(
                "stored status.phase",
                at(stored, "/status/phase"),
                &json!("Active"),
            )?;
            expect_eq(
                "stored name label",
                at(stored, "/metadata/labels/kubernetes.io~1metadata.name"),
                &json!(case.name),
            )
        }
        Rule::IdentityIsServerOwned => {
            succeeded(out)?;
            expect_eq(
                "stored uid",
                at(stored, "/metadata/uid"),
                &json!("uid-original"),
            )?;
            expect_eq(
                "stored creationTimestamp",
                at(stored, "/metadata/creationTimestamp"),
                &json!("2020-01-01T00:00:00Z"),
            )?;
            expect_eq(
                "stored label (the write itself landed)",
                at(stored, "/metadata/labels/touched"),
                &json!("yes"),
            )
        }
        Rule::TerminationIsServerOwned => {
            succeeded(out)?;
            expect_eq(
                "stored deletionTimestamp",
                at(stored, "/metadata/deletionTimestamp"),
                &json!("2026-01-01T00:00:00Z"),
            )?;
            expect_eq(
                "stored label (the write itself landed)",
                at(stored, "/metadata/labels/touched"),
                &json!("yes"),
            )
        }
        Rule::StatusNotWrittenHere => {
            succeeded(out)?;
            expect_eq(
                "stored status.phase",
                at(stored, "/status/phase"),
                &json!("Running"),
            )?;
            expect_eq(
                "stored label (the write itself landed)",
                at(stored, "/metadata/labels/touched"),
                &json!("yes"),
            )
        }
        Rule::ClusterIpCarriesOver => {
            succeeded(out)?;
            expect_eq(
                "stored spec.clusterIP",
                at(stored, "/spec/clusterIP"),
                &json!("10.96.0.10"),
            )?;
            expect_eq(
                "stored spec.clusterIPs",
                at(stored, "/spec/clusterIPs"),
                &json!(["10.96.0.10"]),
            )
        }
        Rule::StalePreconditionConflicts => refused_with(out, 409),
        Rule::DryRun => {
            succeeded(out)?;
            let answer = Some(&out.body);
            expect_eq(
                "dry-run answer's label (the write was computed)",
                at(answer, "/metadata/labels/touched"),
                &json!("yes"),
            )?;
            expect_eq(
                "dry-run answer's spec.restartPolicy (defaulting ran)",
                at(answer, "/spec/restartPolicy"),
                &json!("Always"),
            )?;
            expect_eq(
                "dry-run answer's admission mark (admission ran)",
                at(answer, "/metadata/annotations/matrix~1admitted"),
                &json!("yes"),
            )?;
            if out.store_after != out.store_before {
                return Err("a dry run consumed a revision".into());
            }
            if rv(stored) != out.rv_before {
                return Err("a dry run changed the stored object".into());
            }
            Ok(())
        }
        Rule::UnchangedCommitsNothing => {
            succeeded(out)?;
            if out.store_after != out.store_before {
                return Err(format!(
                    "re-sending an applied write committed revisions {:?} -> {:?}",
                    out.store_before, out.store_after
                ));
            }
            if rv(Some(&out.body)) != rv(stored) {
                return Err("the answer's resourceVersion is not the stored one".into());
            }
            Ok(())
        }
    }
}

/// ★ THE MATRIX. Every verb × every rule; one failure list, so a single run
/// shows every cell a regression breaks.
#[tokio::test]
async fn every_write_verb_obeys_every_write_rule() {
    let h = boot_matrix().await;
    let mut failures = Vec::new();
    let mut ran = 0usize;
    let mut not_applicable = 0usize;
    let mut routable = Verb::routable();
    routable.sort_by_key(|v| v.slug());
    let mut all = Verb::ALL.to_vec();
    all.sort_by_key(|v| v.slug());
    assert_eq!(
        routable, all,
        "every routable verb is a column, and no other"
    );
    for &rule in Rule::ALL {
        for &verb in Verb::ALL {
            match case(rule, verb) {
                Cell::NotApplicable(why) => {
                    assert!(!why.is_empty(), "{rule:?} x {verb:?}: say why");
                    not_applicable += 1;
                }
                Cell::Run(c) => {
                    ran += 1;
                    let verdict = match run(&h, verb, &c).await {
                        Ok(out) => check(rule, verb, &c, &out),
                        Err(e) => Err(e),
                    };
                    if let Err(e) = verdict {
                        failures.push(format!("{rule:?} x {verb:?}: {e}"));
                    }
                }
            }
        }
    }
    assert_eq!(
        ran + not_applicable,
        Rule::ALL.len() * Verb::ALL.len(),
        "every cell is either run or explained"
    );
    assert!(
        failures.is_empty(),
        "{} of {ran} cells failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
    h.server.shutdown().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════════
//  What the compare-and-swap must keep.
// ═══════════════════════════════════════════════════════════════════════════

async fn boot_configmaps(cluster: &str) -> (Arc<StoreMesh>, ApiServer) {
    let store = boot_store(cluster).await;
    let cm: Arc<dyn ResourceHandler> = Arc::new(
        StoreBackedHandler::for_kind(store.clone(), "ConfigMap").expect("a cataloged kind"),
    );
    let server = ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![cm], None)
        .await
        .unwrap();
    (store, server)
}

const WRITERS: usize = 16;

/// Sixteen unconditional merge patches to one object, all at once. Each is
/// computed against the object it read and committed at that revision; a
/// patch that loses the race is re-planned against the newer object. None
/// may be lost, and none may fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_patches_lose_no_update() {
    let (store, server) = boot_configmaps("apiserver-w1-patches").await;
    let key = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "shared");
    seed(
        &store,
        key.clone(),
        json!({"apiVersion": "v1", "kind": "ConfigMap",
               "metadata": {"name": "shared", "namespace": "default"}, "data": {}}),
    )
    .await;
    let url = format!(
        "http://{}/api/v1/namespaces/default/configmaps/shared",
        server.local_addr()
    );
    let client = reqwest::Client::new();
    let mut writers = Vec::new();
    for i in 0..WRITERS {
        let (client, url) = (client.clone(), url.clone());
        writers.push(tokio::spawn(async move {
            client
                .patch(&url)
                .header("Content-Type", "application/merge-patch+json")
                .body(json!({"data": {format!("k{i}"): "v"}}).to_string())
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }));
    }
    for writer in writers {
        assert_eq!(writer.await.unwrap(), 200, "every patch lands");
    }
    let data = store
        .get(&key)
        .await
        .and_then(|cm| cm.get("data").cloned())
        .unwrap_or_default();
    for i in 0..WRITERS {
        assert_eq!(
            data.get(format!("k{i}")),
            Some(&json!("v")),
            "patch {i} was lost: {data}"
        );
    }
    server.shutdown().await.unwrap();
}

/// Sixteen POSTs of one name, all at once. Exactly one creates it; every
/// other is AlreadyExists; and the stored object is the winner's. A create
/// commits only if the name is still free when the store applies it, so a
/// second create can never overwrite the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_of_one_name_succeed_exactly_once() {
    let (store, server) = boot_configmaps("apiserver-w1-creates").await;
    let key = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "once");
    let url = format!(
        "http://{}/api/v1/namespaces/default/configmaps",
        server.local_addr()
    );
    let client = reqwest::Client::new();
    let mut writers = Vec::new();
    for i in 0..WRITERS {
        let (client, url) = (client.clone(), url.clone());
        writers.push(tokio::spawn(async move {
            let status = client
                .post(&url)
                .json(&json!({"apiVersion": "v1", "kind": "ConfigMap",
                              "metadata": {"name": "once"}, "data": {"writer": i.to_string()}}))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16();
            (i, status)
        }));
    }
    let mut created = Vec::new();
    for writer in writers {
        let (i, status) = writer.await.unwrap();
        match status {
            201 => created.push(i),
            409 => {}
            other => panic!("writer {i} answered {other}"),
        }
    }
    assert_eq!(created.len(), 1, "exactly one create wins: {created:?}");
    let stored = store.get(&key).await.expect("the winner is stored");
    assert_eq!(
        stored.pointer("/data/writer"),
        Some(&json!(created[0].to_string())),
        "the stored object is the winner's, never overwritten by a loser"
    );
    server.shutdown().await.unwrap();
}
