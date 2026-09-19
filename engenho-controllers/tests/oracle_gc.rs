//! Garbage-collector owner resolution against upstream (Kubernetes v1.34.0
//! `pkg/controller/garbagecollector`), row by row.
//!
//! The adapter drives engenho's collection core (`engenho_controllers::gc`)
//! over a recording [`GcEnv`] built from each row: the row's `rest_mapper`
//! becomes the served catalog (engenho's own compiled-in catalog when the
//! row names none), its `live` objects the store. Every read and write the
//! core makes is recorded in upstream's `clientActions` format, so
//! `api_calls` compares what engenho actually asked for.
//!
//! One modelling choice, stated once: engenho's store stamps
//! `metadata.resourceVersion` on every write, so every object the gc reads
//! carries one. The fixtures come from client-go's fake client, which does
//! not, so the fake stamps `"1"` where a row left it absent or empty.
//!
//! What is answered, per row kind, is engenho's value restricted to the keys
//! upstream recorded. Two upstream keys are never answered:
//!
//! - `delete` is the DeleteOptions a delete carries (propagationPolicy,
//!   uid/resourceVersion preconditions). engenho's store delete has no
//!   options body; its one precondition is the resourceVersion read.
//! - `patch` is answered only where upstream recorded just the decision
//!   (`remove_owner_uids`). Where it also recorded the wire form (the
//!   strategic-merge body and its 415 fallback), no engenho value is that
//!   object: engenho writes the surviving list as a JSON merge patch pinned
//!   to the resourceVersion, which is upstream's fallback form.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engenho_controllers::gc::{
    self, Candidate, Collected, GcEnv, OwnerState, ServedKind, ServedKinds, Unresolvable,
};
use engenho_controllers::{Controller, ControllerError, Effect, GcController, OwnerReference};
use engenho_oracle::{Answer, Case, Deviation, OutOfScope, Scope as Claim, Vector, assert_table};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand, ResourceOp},
    default_config,
    revision::Revision,
};
use engenho_types::kind::Scope;
use serde_json::{Map, Value, json};

// ── the served catalog and the store, from a row ──────────────────────────

fn text<'v>(v: &'v Value, key: &str) -> &'v str {
    v.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// `"apps/v1"` -> `("apps", "v1")`, `"v1"` -> `("", "v1")`.
fn group_version(gv: &str) -> (&str, &str) {
    gv.split_once('/').unwrap_or(("", gv))
}

/// The row's RESTMapper, or engenho's own catalog when it names none.
fn served_of(input: &Value) -> ServedKinds {
    let Some(rows) = input
        .get("rest_mapper")
        .and_then(|m| m.get("served"))
        .and_then(Value::as_array)
    else {
        return ServedKinds::builtin();
    };
    ServedKinds::from_kinds(rows.iter().map(|r| {
        let (group, version) = group_version(text(r, "groupVersion"));
        ServedKind {
            group: group.into(),
            version: version.into(),
            kind: text(r, "kind").into(),
            plural: text(r, "resource").into(),
            scope: if r["namespaced"].as_bool() == Some(true) {
                Scope::Namespaced
            } else {
                Scope::Cluster
            },
        }
    }))
}

/// The served kind with this plural at this group/version: the row's, then
/// engenho's catalog.
fn kind_for_plural(served: &ServedKinds, group: &str, version: &str, plural: &str) -> ServedKind {
    let builtin = ServedKinds::builtin();
    served
        .iter()
        .chain(builtin.iter())
        .find(|k| k.group == group && k.version == version && k.plural == plural)
        .cloned()
        .unwrap_or_else(|| panic!("fixture resource {group}/{version} {plural} is not served"))
}

fn key_of(group: &str, version: &str, kind: &str, namespace: &str, name: &str) -> ResourceKey {
    if namespace.is_empty() {
        ResourceKey::cluster_scoped(group, version, kind, name)
    } else {
        ResourceKey::namespaced(group, version, kind, namespace, name)
    }
}

fn api_version(group: &str, version: &str) -> String {
    if group.is_empty() {
        version.to_owned()
    } else {
        format!("{group}/{version}")
    }
}

/// One `live` entry as the stored object engenho would hold.
fn live_object(served: &ServedKinds, entry: &Value) -> (ResourceKey, Value) {
    let resource = text(entry, "resource");
    let (gv, plural) = resource
        .split_once(", Resource=")
        .expect("resource is '<gv>, Resource=<plural>'");
    let (group, version) = group_version(gv.trim_start_matches('/'));
    let kind = kind_for_plural(served, group, version, plural);
    let namespace = text(entry, "namespace");
    let name = text(entry, "name");
    let rv = match text(entry, "resourceVersion") {
        "" => "1",
        rv => rv,
    };
    let mut meta = json!({"name": name, "uid": text(entry, "uid"), "resourceVersion": rv});
    if !namespace.is_empty() {
        meta["namespace"] = json!(namespace);
    }
    for field in ["deletionTimestamp", "finalizers", "ownerReferences"] {
        if let Some(v) = entry.get(field).filter(|v| !v.is_null()) {
            meta[field] = v.clone();
        }
    }
    let value = json!({
        "apiVersion": api_version(group, version),
        "kind": kind.kind,
        "metadata": meta,
    });
    (key_of(group, version, &kind.kind, namespace, name), value)
}

// ── the recording environment ─────────────────────────────────────────────

struct Fake {
    served: ServedKinds,
    objects: Mutex<BTreeMap<ResourceKey, Value>>,
    calls: Mutex<Vec<String>>,
}

impl Fake {
    fn new(input: &Value) -> Self {
        let served = served_of(input);
        let objects = input
            .get("live")
            .and_then(Value::as_array)
            .map(|live| live.iter().map(|e| live_object(&served, e)).collect())
            .unwrap_or_default();
        Self {
            served,
            objects: Mutex::new(objects),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// `<verb> <GroupVersionResource> [ns=<ns>] name=<name>`, as upstream's
    /// `TestConflictingData` records client actions.
    fn record(&self, verb: &str, key: &ResourceKey) {
        let plural = [self.served.clone(), ServedKinds::builtin()]
            .iter()
            .find_map(|s| {
                s.find(&key.group, &key.version, &key.kind)
                    .map(|k| k.plural.clone())
            })
            .unwrap_or_else(|| panic!("no plural for {}", key.label()));
        let ns = key
            .namespace
            .as_deref()
            .map(|ns| format!(" ns={ns}"))
            .unwrap_or_default();
        self.calls.lock().expect("calls").push(format!(
            "{verb} {}/{}, Resource={plural}{ns} name={}",
            key.group, key.version, key.name
        ));
    }

    fn calls(&self) -> Value {
        json!(*self.calls.lock().expect("calls"))
    }

    fn snapshot(&self) -> BTreeMap<ResourceKey, Value> {
        self.objects.lock().expect("objects").clone()
    }

    /// Apply `write` to the object at `key` if it is still at `pinned`, the
    /// way the store answers a revision-pinned write.
    fn pinned_write(
        &self,
        key: &ResourceKey,
        pinned: Revision,
        write: impl FnOnce(&mut BTreeMap<ResourceKey, Value>) -> ResourceOp,
    ) -> Effect {
        let mut objects = self.objects.lock().expect("objects");
        let at = objects
            .get(key)
            .and_then(|o| o["metadata"]["resourceVersion"].as_str())
            .and_then(|rv| rv.parse::<u64>().ok());
        Effect::of(if at == Some(pinned.0) {
            write(&mut objects)
        } else {
            ResourceOp::Conflict
        })
    }
}

#[async_trait]
impl GcEnv for Fake {
    async fn get(&self, key: &ResourceKey) -> Option<Value> {
        self.record("get", key);
        self.objects.lock().expect("objects").get(key).cloned()
    }

    async fn delete(&self, key: &ResourceKey, pinned: Revision) -> Result<Effect, ControllerError> {
        self.record("delete", key);
        Ok(self.pinned_write(key, pinned, |objects| {
            objects.remove(key);
            ResourceOp::Deleted
        }))
    }

    async fn replace_owner_references(
        &self,
        key: &ResourceKey,
        references: Vec<Value>,
        pinned: Revision,
    ) -> Result<Effect, ControllerError> {
        self.record("patch", key);
        Ok(self.pinned_write(key, pinned, |objects| {
            if let Some(o) = objects.get_mut(key) {
                o["metadata"]["ownerReferences"] = Value::Array(references);
            }
            ResourceOp::Patched
        }))
    }
}

// ── engenho's answers, in upstream's vocabulary ───────────────────────────

fn error_type(why: &Unresolvable) -> &'static str {
    match why {
        Unresolvable::NoRestMapping { .. } => "restMappingError",
        Unresolvable::NamespacedOwnerOfClusterScoped => "namespacedOwnerOfClusterScopedObjectErr",
        Unresolvable::NotAList | Unresolvable::Malformed { .. } => "engenho:malformed",
    }
}

fn error_json(why: &Unresolvable) -> Value {
    json!({"type": error_type(why), "message": why.to_string()})
}

fn namespace_of(item: &Value) -> Option<&str> {
    Some(text(item, "namespace")).filter(|ns| !ns.is_empty())
}

fn owner_reference(raw: &Value) -> OwnerReference {
    serde_json::from_value(raw.clone()).expect("fixture owner reference")
}

fn gvr(kind: &ServedKind) -> String {
    format!("{}/{}, Resource={}", kind.group, kind.version, kind.plural)
}

/// engenho's answer restricted to the keys upstream recorded. A row whose
/// keys engenho answers none of is not checked, and must be declared.
///
/// Every key an answer builder inserts is inserted whatever engenho decided
/// (`null` where engenho has no value), so restricting to upstream's keys
/// can never drop a disagreement. The one exception is `return` for an item
/// that is gone: upstream's value is a graph event engenho has no
/// counterpart for, and the same rows record `api_calls` and `item_deleted`.
fn restrict(full: Map<String, Value>, expected: &Value) -> Answer {
    let kept: Map<String, Value> = full
        .into_iter()
        .filter(|(k, _)| expected.get(k).is_some())
        .collect();
    if kept.is_empty() {
        Answer::NotChecked
    } else {
        Answer::Checked(Value::Object(kept))
    }
}

fn answer_api_resource(input: &Value) -> Map<String, Value> {
    let served = served_of(input);
    let r = &input["owner_ref"];
    let out = match served.resolve(text(r, "apiVersion"), text(r, "kind")) {
        Ok(k) => json!({
            "ok": {"resource": gvr(k), "namespaced": k.scope == Scope::Namespaced},
            "error": null,
        }),
        Err(why) => json!({"ok": null, "error": error_json(&why)}),
    };
    out.as_object().cloned().unwrap_or_default()
}

async fn answer_is_dangling(input: &Value) -> Map<String, Value> {
    let fake = Fake::new(input);
    let owner = owner_reference(&input["owner_ref"]);
    let state = gc::owner_state(&fake, &fake.served, namespace_of(&input["item"]), &owner).await;
    let mut out = match state {
        Ok(OwnerState::Dangling) => json!({"dangling": true, "owner": null, "error": null}),
        Ok(OwnerState::Solid | OwnerState::WaitingForDependents) => {
            json!({"dangling": false, "owner": {"uid": owner.uid}, "error": null})
        }
        Err(why) => json!({"dangling": false, "owner": null, "error": error_json(&why)}),
    };
    out["api_calls"] = fake.calls();
    // engenho resolves the kind before anything else, on every lookup.
    out["rest_mapper_consulted"] = json!(true);
    out.as_object().cloned().unwrap_or_default()
}

async fn answer_is_dangling_sequence(input: &Value) -> Map<String, Value> {
    let fake = Fake::new(input);
    let mut results = Vec::new();
    for step in input["steps"].as_array().expect("steps") {
        let owner = owner_reference(&step["owner_ref"]);
        let state = gc::owner_state(&fake, &fake.served, namespace_of(&step["item"]), &owner).await;
        results.push(json!({"dangling": state == Ok(OwnerState::Dangling)}));
    }
    let mut out = Map::new();
    out.insert("results".into(), json!(results));
    out
}

async fn answer_classify(input: &Value) -> Map<String, Value> {
    let fake = Fake::new(input);
    let refs = input["owner_refs"].as_array().cloned().unwrap_or_default();
    let classified = gc::classify(&fake, &fake.served, namespace_of(&input["item"]), &refs).await;
    let out = match classified {
        Ok(c) => json!({
            "error": null,
            "solid": c.uids(OwnerState::Solid),
            "dangling": c.uids(OwnerState::Dangling),
            "waitingForDependentsDeletion": c.uids(OwnerState::WaitingForDependents),
            "api_calls": fake.calls(),
        }),
        Err(why) => json!({
            "error": error_json(&why),
            "solid": null,
            "dangling": null,
            "waitingForDependentsDeletion": null,
            "api_calls": fake.calls(),
        }),
    };
    out.as_object().cloned().unwrap_or_default()
}

async fn answer_attempt(input: &Value, expected: &Value) -> Map<String, Value> {
    let fake = Fake::new(input);
    let before = fake.snapshot();
    let item = &input["item"];
    let (group, version) = group_version(text(item, "apiVersion"));
    let candidate = Candidate {
        key: key_of(
            group,
            version,
            text(item, "kind"),
            text(item, "namespace"),
            text(item, "name"),
        ),
        uid: text(item, "uid").into(),
        // The graph's `beingDeleted` is the object's deletionTimestamp
        // (graph_builder.go), which is what engenho's scan reads.
        being_deleted: item["beingDeleted"].as_bool() == Some(true),
    };
    let collected = gc::collect(&fake, &fake.served, &candidate)
        .await
        .expect("the fake's writes cannot fail");
    let after = fake.snapshot();

    let mut out = Map::new();
    out.insert("api_calls".into(), fake.calls());
    out.insert(
        "item_deleted".into(),
        json!(matches!(collected, Collected::Deleted(_))),
    );
    match &collected {
        Collected::Unresolvable(why) => {
            out.insert("return".into(), json!({"type": error_type(why)}));
        }
        // Upstream answers with a virtual delete event for its graph;
        // engenho has no graph, so there is no return value to compare.
        Collected::ItemGone => {}
        _ => {
            out.insert("return".into(), Value::Null);
        }
    }
    // `patch` is comparable only where upstream recorded just the decision;
    // see the module header. Whether engenho patched is answered either way.
    let decision_only = expected["patch"]
        .as_object()
        .is_some_and(|p| p.keys().eq(["remove_owner_uids"].iter()));
    if decision_only {
        let patch = match &collected {
            Collected::ReferencesRemoved { uids, .. } => json!({"remove_owner_uids": uids}),
            _ => Value::Null,
        };
        out.insert("patch".into(), patch);
    }
    let final_refs = after.get(&candidate.key).map(|item_after| {
        gc::owner_references(item_after)
            .unwrap_or_default()
            .iter()
            .map(|r| text(r, "name"))
            .collect::<Vec<_>>()
    });
    out.insert("final_owner_refs".into(), json!(final_refs));
    let others_untouched = before
        .iter()
        .filter(|(k, _)| **k != candidate.key)
        .all(|(k, v)| after.get(k) == Some(v));
    out.insert("owner_in_ns2_untouched".into(), json!(others_untouched));
    out
}

async fn boot_store() -> Arc<StoreMesh> {
    let cfg = default_config("oracle-gc").expect("config");
    let store = Arc::new(
        StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
            .await
            .expect("store"),
    );
    store.initialize_singleton().await.expect("init");
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    store
}

/// The whole controller, over a real store: which of the row's objects one
/// gc tick deletes.
async fn answer_scenario(input: &Value) -> Map<String, Value> {
    let store = boot_store().await;
    let mut keys = Vec::new();
    for o in input["objects"].as_array().expect("objects") {
        let (group, version) = group_version(text(o, "apiVersion"));
        let key = key_of(
            group,
            version,
            text(o, "kind"),
            text(o, "namespace"),
            text(o, "name"),
        );
        let mut meta = json!({"name": text(o, "name"), "ownerReferences": o["ownerReferences"]});
        if !text(o, "namespace").is_empty() {
            meta["namespace"] = o["namespace"].clone();
        }
        let value = json!({"apiVersion": o["apiVersion"], "kind": o["kind"], "metadata": meta});
        store
            .propose(ResourceCommand::put(key.clone(), value, Reason::Operator))
            .await
            .expect("put");
        keys.push((key, o));
    }
    GcController::new(store.clone(), None)
        .tick()
        .await
        .expect("gc tick");
    let mut deleted = Vec::new();
    for (key, o) in keys {
        if store.get(&key).await.is_none() {
            let at = match text(o, "namespace") {
                "" => text(o, "name").to_owned(),
                ns => format!("{ns}/{}", text(o, "name")),
            };
            deleted.push(format!("{} {at}", text(o, "kind")));
        }
    }
    let mut out = Map::new();
    out.insert("deleted".into(), json!(deleted));
    out
}

async fn answer(case: &Case) -> Answer {
    let declared_out = OUT_OF_SCOPE
        .iter()
        .any(|o| matches!(o.scope, Claim::Case(name) if name == case.name));
    if declared_out {
        return Answer::NotChecked;
    }
    let input = &case.input;
    let full = match text(input, "fn") {
        "apiResource" => answer_api_resource(input),
        "isDangling" => answer_is_dangling(input).await,
        "isDangling.sequence" => answer_is_dangling_sequence(input).await,
        "classifyReferences" => answer_classify(input).await,
        "attemptToDeleteItem" => answer_attempt(input, &case.expected).await,
        "scenario" => answer_scenario(input).await,
        _ => return Answer::NotChecked,
    };
    restrict(full, &case.expected)
}

// ── what engenho does not answer, and where it deliberately differs ──────

const NO_GRAPH: &str = "engenho's gc keeps no dependency graph: owners are resolved per reference, \
     live, every tick, so there is no virtual node, alternate identity or OwnerRefInvalidNamespace event";

const DELETE_OPTIONS_ONLY: &str = "the row records only the DeleteOptions the delete carries \
     (propagationPolicy, uid/resourceVersion preconditions). engenho's store delete has no options \
     body: no deletion propagation exists anywhere in engenho (the apiserver ignores propagationPolicy; \
     nothing processes the orphan or foregroundDeletion finalizers), and its one precondition is the \
     resourceVersion read. That the item IS deleted is pinned by tests/gc_owner_resolution.rs";

const OUT_OF_SCOPE: &[OutOfScope] = &[
    OutOfScope::kind(
        "DeferredDiscoveryRESTMapper.RESTMapping",
        "engenho has no discovery cache to go stale: gc rebuilds its served set from RESOURCE_CATALOG \
         and the stored CRDs at the start of every tick",
    ),
    OutOfScope::kind(
        "validateOwnerReference",
        "write-time validation (objectmeta.go) belongs to the apiserver, and engenho-apiserver does \
         not validate ownerReferences yet (recorded for the apiserver lane). The gc does not lean on \
         it: an entry that does not parse is Unresolvable::Malformed and keeps its dependent",
    ),
    OutOfScope::kind(
        "ValidateOwnerReferences",
        "write-time validation (objectmeta.go) belongs to the apiserver; see validateOwnerReference",
    ),
    OutOfScope::kind(
        "absentOwnerCache.lru",
        "engenho's gc keeps no absent-owner cache: every tick re-reads each owner live. The cache's \
         observable effects are declared deviations on the rows that show them",
    ),
    OutOfScope::kind(
        "attemptToDeleteItem.sequence",
        "a revision-pinned write that loses a race is the store's Conflict; the gc does not re-read \
         and retry within the attempt. The next tick re-reads the item and re-derives the verdict",
    ),
    OutOfScope::kind(
        "deleteObject",
        "a revision-pinned delete that loses a race is the store's Conflict; the gc does not re-read \
         and retry within the attempt. The next tick re-reads the item and re-derives the verdict",
    ),
    OutOfScope::kind(
        "attemptToDeleteWorker",
        "engenho's gc has no work queue and no graph: a tick re-lists the scanned dependents, so \
         there is no queued or virtual item to forget or requeue",
    ),
    OutOfScope::kind("processGraphChanges", NO_GRAPH),
    OutOfScope::kind("processGraphChanges.sequence", NO_GRAPH),
    OutOfScope::kind("getAlternateOwnerIdentity", NO_GRAPH),
    OutOfScope::kind("addDependentToOwners", NO_GRAPH),
    OutOfScope::kind(
        "integration",
        "needs CRD deletion to cascade to its custom resources (crd.rs: orphaned CRs stay in the \
         store) and gc to collect ConfigMap dependents, a kind it does not scan (IMPROVEMENT-PLAN §9 \
         refuses widening the dependent scan to every served kind)",
    ),
    OutOfScope::kind(
        "kube_runtime::reflector::ObjectRef::eq/hash",
        "a contrast row about kube-rs's ObjectRef, which engenho's gc does not use: an owner's \
         identity is the full reference (apiVersion, kind, name, uid) at the dependent's namespace",
    ),
    OutOfScope::kind(
        "kube_runtime::reflector::ObjectRef::<Pod>::from_owner_ref",
        "a contrast row about kube-rs's ObjectRef, which engenho's gc does not use",
    ),
    OutOfScope::case(
        "isDangling: non-404 GET error propagates (not dangling, not cached)",
        "an owner read is a local read of engenho's replicated store, which cannot fail with an \
         HTTP status: there is no GET error to propagate",
    ),
    OutOfScope::case(
        "attemptToDeleteItem: all owners dangling, item carries orphan finalizer -> DELETE with Orphan policy",
        DELETE_OPTIONS_ONLY,
    ),
    OutOfScope::case(
        "attemptToDeleteItem: all owners dangling, item carries foregroundDeletion finalizer -> DELETE with Foreground policy",
        DELETE_OPTIONS_ONLY,
    ),
    OutOfScope::case(
        "attemptToDeleteItem: only a waiting owner, item HAS dependents -> DELETE with Foreground policy",
        DELETE_OPTIONS_ONLY,
    ),
    OutOfScope::case(
        "attemptToDeleteItem: only a waiting owner, item has NO dependents -> falls to default branch -> Background",
        DELETE_OPTIONS_ONLY,
    ),
];

const DEVIATIONS: &[Deviation] = &[
    Deviation {
        case: "apiResource: Kind+'List' resolves to a guessed resource that the server does not serve",
        why: "upstream's discovery mapper guesses the resource `podlists`, which nothing serves, and \
              what a GET on it returns is not verified by the table. engenho does not guess a \
              resource: the reference is Unresolvable and its dependent is kept (fail-closed)",
    },
    Deviation {
        case: "isDangling: cluster-key cache hit short-circuits BEFORE the RESTMapper, so even an unresolvable kind is dangling",
        why: "engenho keeps no absent-owner cache, so a kind no longer served is Unresolvable and the \
              dependent is kept; upstream reaches dangling only through a 404 it remembered from \
              before the kind went away",
    },
    Deviation {
        case: "isDangling: namespaced-key cache hit (key namespace == dependent namespace) -> dangling, no API call",
        why: "engenho keeps no absent-owner cache: it re-reads the owner (one GET) and reaches the \
              same verdict, dangling",
    },
    Deviation {
        case: "attemptToDeleteItem: absence cached via apiVersion v1 does NOT cover a ref to the same owner via v1beta1",
        why: "engenho keys a stored object by the version it was written at and does not convert on \
              read, so before calling an owner of a multi-version kind absent it also reads the \
              other served version (one extra GET, rbac v1 here). The verdict is the same: deleted",
    },
];

#[tokio::test]
async fn gc_owner_resolution_agrees_with_upstream() {
    let table = Vector::GcOwnerResolution.load();
    let mut answers = HashMap::new();
    for case in &table.cases {
        answers.insert(case.name.clone(), answer(case).await);
    }
    let report = assert_table(&table, OUT_OF_SCOPE, DEVIATIONS, |case| {
        answers.remove(&case.name).expect("answered")
    });
    assert!(
        report.agree >= 30,
        "the gc table must stay mostly checked, not mostly exempt: {report:?}"
    );
}
