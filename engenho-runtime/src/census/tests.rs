//! The census over an in-memory [`Source`]: each predicate's behaviour,
//! pinned on objects built to sit on each side of its rule.

use std::collections::BTreeMap;

use async_trait::async_trait;
use engenho_controllers::node_lease::lease_value;
use engenho_scheduler::filter::{FilterPlugin, Rejection};
use engenho_store::ImageTripwire;
use serde_json::{Value, json};

use super::*;

const NOW: &str = "2026-09-19T12:00:00Z";

/// The mockable seam: a map of kind to objects, kinds whose LIST fails,
/// and no store image.
#[derive(Default)]
struct Fixture {
    objects: BTreeMap<Gvk, Vec<Value>>,
    failing: BTreeMap<Gvk, Failure>,
}

/// How a fixture kind's LIST fails.
#[derive(Clone, Copy)]
enum Failure {
    /// The source says the kind is not a collection.
    NotListable,
    /// The source could not answer at all.
    Blind,
}

impl Fixture {
    fn with(mut self, gvk: Gvk, objects: Vec<Value>) -> Self {
        self.objects.entry(gvk).or_default().extend(objects);
        self
    }

    fn failing(mut self, gvk: Gvk, failure: Failure) -> Self {
        self.failing.insert(gvk, failure);
        self
    }
}

#[async_trait]
impl Source for Fixture {
    async fn list(&self, gvk: &Gvk) -> Result<Vec<Value>, CensusError> {
        match self.failing.get(gvk) {
            Some(Failure::NotListable) => Err(CensusError::NotListable {
                gvk: gvk.clone(),
                url: "fixture://list".into(),
            }),
            Some(Failure::Blind) => Err(CensusError::Status {
                url: "fixture://list".into(),
                code: 503,
            }),
            None => Ok(self.objects.get(gvk).cloned().unwrap_or_default()),
        }
    }

    async fn kinds(&self) -> Result<Vec<Gvk>, CensusError> {
        Ok(self
            .objects
            .keys()
            .chain(self.failing.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    async fn image(&self) -> Option<ImageTripwire> {
        None
    }

    fn label(&self) -> String {
        "fixture".to_owned()
    }
}

async fn census(predicate: Predicate, source: &Fixture) -> Report {
    run_at(predicate, source, NOW)
        .await
        .expect("an in-memory census answers")
}

fn names(report: &Report) -> Vec<String> {
    report
        .matches()
        .iter()
        .map(|m| m.subject.to_string())
        .collect()
}

fn node(name: &str, cpu: &str, memory: &str) -> Value {
    json!({
        "metadata": {"name": name},
        "status": {"allocatable": {"cpu": cpu, "memory": memory}}
    })
}

fn fresh_lease(node: &str) -> Value {
    lease_value(node, &engenho_types::time::now_rfc3339_utc(), 0)
}

fn pod(name: &str, node: &str, cpu: &str, memory: &str) -> Value {
    json!({
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "nodeName": node,
            "containers": [{
                "name": "c",
                "image": "i",
                "resources": {"requests": {"cpu": cpu, "memory": memory}}
            }]
        },
        "status": {"phase": "Running"}
    })
}

// ── the catalog ──────────────────────────────────────────────────────────

/// `--predicate` takes exactly the catalog's names: each finds its own row,
/// and no other string finds any.
#[test]
fn every_predicate_is_found_by_its_own_name_and_nothing_else_is() {
    for p in Predicate::ALL {
        assert_eq!(Predicate::from_name(p.name()), Some(*p));
    }
    for not_a_name in [
        "",
        "node",
        "NODE-OVERCOMMITTED",
        "node-overcommitted ",
        "true",
    ] {
        assert_eq!(Predicate::from_name(not_a_name), None, "{not_a_name:?}");
    }
    let unique: BTreeSet<&str> = Predicate::ALL.iter().map(|p| p.name()).collect();
    assert_eq!(
        unique.len(),
        Predicate::ALL.len(),
        "two predicates share a name"
    );
}

/// The printed catalog names every predicate and the plan row it gates.
#[test]
fn the_catalog_lists_every_predicate_with_its_plan_row() {
    let printed = Catalog.to_string();
    for p in Predicate::ALL {
        let line = printed
            .lines()
            .find(|l| l.starts_with(p.name()))
            .unwrap_or_else(|| panic!("the catalog omits {p}"));
        assert!(
            line.contains(p.plan_row()),
            "{p}'s line omits its row: {line}"
        );
    }
}

// ── node-overcommitted (T1.5) ────────────────────────────────────────────

/// A node charged past its allocatable matches, with its signed headroom;
/// one within it does not.
#[tokio::test]
async fn an_overcommitted_node_matches_with_its_signed_headroom() {
    let source = Fixture::default()
        .with(
            Builtin::Node.gvk(),
            vec![node("full", "1", "1Gi"), node("roomy", "4", "8Gi")],
        )
        .with(
            Builtin::Pod.gvk(),
            vec![
                pod("a", "full", "750m", "128Mi"),
                pod("b", "full", "500m", "128Mi"),
                pod("c", "roomy", "1", "1Gi"),
            ],
        );
    let report = census(Predicate::NodeOvercommitted, &source).await;
    assert_eq!(names(&report), ["v1/Node full"]);
    assert_eq!(
        report.matches()[0].finding,
        Finding::Overcommitted {
            cpu_milli: -250,
            mem_milli: (1024 - 256) * 1024 * 1024 * 1000,
        }
    );
}

// ── bound-pod-rejected (T5.7) ────────────────────────────────────────────

/// Every plugin judges the pod, not only the first to refuse it: a pod that
/// wants `gpu=true` on a `gpu=false` node with a `NoSchedule` taint is counted
/// under both plugins.
#[tokio::test]
async fn every_refusing_plugin_is_counted_not_only_the_first() {
    let mut gpu_node = node("n1", "4", "8Gi");
    gpu_node["metadata"]["labels"] = json!({"gpu": "false"});
    gpu_node["spec"] = json!({"taints": [{"key": "dedicated", "effect": "NoSchedule"}]});
    let mut p = pod("wants-gpu", "n1", "100m", "64Mi");
    p["spec"]["nodeSelector"] = json!({"gpu": "true"});
    let source = Fixture::default()
        .with(Builtin::Node.gvk(), vec![gpu_node])
        .with(Builtin::Pod.gvk(), vec![p])
        .with(Builtin::Lease.gvk(), vec![fresh_lease("n1")]);
    let report = census(Predicate::BoundPodRejected, &source).await;
    let findings: Vec<&Finding> = report.matches().iter().map(|m| &m.finding).collect();
    assert_eq!(
        findings,
        [
            &Finding::Rejected(Rejection::NodeSelectorMismatch),
            &Finding::Rejected(Rejection::UntoleratedTaint {
                key: "dedicated".into()
            }),
        ]
    );
    assert_eq!(report.subjects_matched(), 1, "one pod, two findings");
    let counts = report.counts();
    assert_eq!(
        counts.get(&("v1/Pod".into(), FilterPlugin::NodeSelector.name().into())),
        Some(&1)
    );
}

/// A pod that fills its node exactly fits there: the ledger charges every
/// OTHER pod, never the pod being judged, which is already on the node.
#[tokio::test]
async fn a_pod_is_not_charged_against_itself() {
    let source = Fixture::default()
        .with(Builtin::Node.gvk(), vec![node("n1", "1", "1Gi")])
        .with(Builtin::Pod.gvk(), vec![pod("exact", "n1", "1", "1Gi")])
        .with(Builtin::Lease.gvk(), vec![fresh_lease("n1")]);
    let report = census(Predicate::BoundPodRejected, &source).await;
    assert!(
        report.matches().is_empty(),
        "a pod that fits its node exactly was refused: {report}"
    );
}

/// Two pods that together overcommit their node: each is judged with the
/// other charged, so both are refused for resources.
#[tokio::test]
async fn each_pod_is_charged_every_other_pod_on_its_node() {
    let source = Fixture::default()
        .with(Builtin::Node.gvk(), vec![node("n1", "1", "1Gi")])
        .with(
            Builtin::Pod.gvk(),
            vec![
                pod("a", "n1", "600m", "64Mi"),
                pod("b", "n1", "600m", "64Mi"),
            ],
        )
        .with(Builtin::Lease.gvk(), vec![fresh_lease("n1")]);
    let report = census(Predicate::BoundPodRejected, &source).await;
    assert_eq!(names(&report), ["v1/Pod default/a", "v1/Pod default/b"]);
    assert!(
        report
            .matches()
            .iter()
            .all(|m| m.finding == Finding::Rejected(Rejection::InsufficientResources))
    );
}

/// A node with no Lease reads `NotReady`, by the same projection the
/// scheduler uses; a pod on a node the list lacks is reported, not judged;
/// a pod observed Succeeded is never placed again, so it is not judged.
#[tokio::test]
async fn readiness_absence_and_terminal_phase_are_each_their_own_answer() {
    let mut done = pod("done", "n1", "100m", "64Mi");
    done["status"]["phase"] = json!("Succeeded");
    let source = Fixture::default()
        .with(Builtin::Node.gvk(), vec![node("n1", "4", "8Gi")])
        .with(
            Builtin::Pod.gvk(),
            vec![
                pod("on-silent-node", "n1", "100m", "64Mi"),
                pod("orphan", "gone", "100m", "64Mi"),
                done,
            ],
        );
    let report = census(Predicate::BoundPodRejected, &source).await;
    let got: Vec<(String, Finding)> = report
        .matches()
        .iter()
        .map(|m| (m.subject.to_string(), m.finding.clone()))
        .collect();
    assert_eq!(
        got,
        [
            (
                "v1/Pod default/on-silent-node".into(),
                Finding::Rejected(Rejection::NotReady)
            ),
            (
                "v1/Pod default/orphan".into(),
                Finding::NodeNotObserved {
                    node: "gone".into()
                }
            ),
        ]
    );
}

// ── deployment-would-roll (T4.10) ────────────────────────────────────────

fn template(image: &str) -> Value {
    json!({
        "metadata": {"labels": {"app": "web"}},
        "spec": {"containers": [{"name": "c", "image": image}]}
    })
}

fn deployment(name: &str, uid: &str, image: &str) -> Value {
    json!({
        "metadata": {"name": name, "namespace": "default", "uid": uid},
        "spec": {"replicas": 1, "template": template(image)}
    })
}

fn replicaset(name: &str, owner_uid: &str, image: &str, hash: &str) -> Value {
    let mut t = template(image);
    t["metadata"]["labels"]["pod-template-hash"] = json!(hash);
    json!({
        "metadata": {
            "name": name,
            "namespace": "default",
            "labels": {"pod-template-hash": hash},
            "ownerReferences": [{
                "apiVersion": "apps/v1", "kind": "Deployment", "name": "x",
                "uid": owner_uid, "controller": true
            }]
        },
        "spec": {"replicas": 1, "template": t}
    })
}

/// Matched: a Deployment whose owned RS runs another template, and one whose
/// matching RS is owned by someone else. Not matched: one whose owned RS runs
/// its template under any hash label, and one with no uid (the controller
/// skips it).
#[tokio::test]
async fn a_deployment_matches_only_when_no_owned_replicaset_runs_its_template() {
    let mut no_uid = deployment("unstamped", "", "web:1");
    no_uid["metadata"]
        .as_object_mut()
        .expect("metadata is an object")
        .remove("uid");
    let source = Fixture::default()
        .with(
            Builtin::Deployment.gvk(),
            vec![
                deployment("steady", "u-steady", "web:1"),
                deployment("edited", "u-edited", "web:2"),
                deployment("adopted-elsewhere", "u-adopt", "web:1"),
                no_uid,
            ],
        )
        .with(
            Builtin::ReplicaSet.gvk(),
            vec![
                replicaset("steady-old-hash", "u-steady", "web:1", "0123456789"),
                replicaset("edited-1", "u-edited", "web:1", "aaaaaaaaaa"),
                replicaset("stranger", "u-someone-else", "web:1", "bbbbbbbbbb"),
            ],
        );
    let report = census(Predicate::DeploymentWouldRoll, &source).await;
    assert_eq!(
        names(&report),
        [
            "apps/v1/Deployment default/edited",
            "apps/v1/Deployment default/adopted-elsewhere",
        ]
    );
}

// ── stored-would-reject (T4.4 / T4.5) ────────────────────────────────────

/// Validation judges the DEFAULTED object, as the create path does: a pod
/// that omits `restartPolicy` is filled, not refused; one that says
/// `Sometimes` is refused, counted under its field.
#[tokio::test]
async fn stored_objects_are_judged_after_defaulting() {
    let mut sometimes = pod("sometimes", "n1", "100m", "64Mi");
    sometimes["spec"]["restartPolicy"] = json!("Sometimes");
    let omitted = pod("omitted", "n1", "100m", "64Mi");
    let source = Fixture::default().with(Builtin::Pod.gvk(), vec![sometimes, omitted]);
    let report = census(Predicate::StoredWouldReject, &source).await;
    assert_eq!(names(&report), ["v1/Pod default/sometimes"]);
    assert_eq!(
        report.counts(),
        BTreeMap::from([(("v1/Pod".into(), "spec.restartPolicy".into()), 1)])
    );
}

fn widget_crd() -> Value {
    json!({
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList"},
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "properties": {"spec": {
                        "type": "object",
                        "properties": {
                            "size": {"type": "integer"},
                            "mode": {"type": "string", "default": "fast"}
                        },
                        "required": ["mode"]
                    }}
                }}
            }]
        }
    })
}

/// A CR is judged against its CRD's schema after the schema's defaults: a
/// string where the schema says integer is refused; a required field the
/// schema defaults is not.
#[tokio::test]
async fn custom_resources_are_judged_by_their_crd_schema_after_its_defaults() {
    let widget = Gvk::new("example.com", "v1", "Widget");
    let source = Fixture::default()
        .with(Builtin::CustomResourceDefinition.gvk(), vec![widget_crd()])
        .with(
            widget.clone(),
            vec![
                json!({"metadata": {"name": "bad", "namespace": "default"},
                       "spec": {"size": "NOT-AN-INT"}}),
                json!({"metadata": {"name": "good", "namespace": "default"},
                       "spec": {"size": 3}}),
            ],
        );
    let report = census(Predicate::StoredWouldReject, &source).await;
    assert_eq!(names(&report), ["example.com/v1/Widget default/bad"]);
    assert!(matches!(
        &report.matches()[0].finding,
        Finding::SchemaInvalid(v) if v.path == "spec.size"
    ));
    assert_eq!(
        report.examined().get(&widget),
        Some(&2),
        "every CR of the kind was read"
    );
}

/// A kind the source will not LIST is named in the report and judged by no
/// one; a kind the source cannot answer for fails the whole census. Neither
/// is ever a zero.
#[tokio::test]
async fn a_kind_the_source_refuses_is_named_and_a_blind_one_fails_the_census() {
    let review = Gvk::new("authorization.k8s.io", "v1", "SelfSubjectAccessReview");
    let mut sometimes = pod("sometimes", "n1", "100m", "64Mi");
    sometimes["spec"]["restartPolicy"] = json!("Sometimes");
    let refusing = Fixture::default()
        .with(Builtin::Pod.gvk(), vec![sometimes])
        .failing(review.clone(), Failure::NotListable);
    let report = census(Predicate::StoredWouldReject, &refusing).await;
    assert_eq!(names(&report), ["v1/Pod default/sometimes"]);
    assert_eq!(
        report.unlisted().keys().collect::<Vec<_>>(),
        [&review],
        "the refused kind is named"
    );
    assert!(!report.examined().contains_key(&review), "and not counted");
    assert!(
        report.to_string().contains(
            "unlisted authorization.k8s.io/v1/SelfSubjectAccessReview: discovery lists it, but GET fixture://list answers 405; not judged\n"
        ),
        "{report}"
    );

    let blind = Fixture::default().failing(Builtin::Pod.gvk(), Failure::Blind);
    let err = run_at(Predicate::StoredWouldReject, &blind, NOW)
        .await
        .expect_err("a kind the source cannot answer for is not a zero");
    assert!(
        matches!(err, CensusError::Status { code: 503, .. }),
        "{err}"
    );
}

// ── store-image-inconsistent (T3.4) ──────────────────────────────────────

/// A source with no store image refuses the image census: it could not
/// see an image, which is not the same answer as an image with no fault.
#[tokio::test]
async fn a_source_without_an_image_refuses_the_image_census() {
    let err = run_at(Predicate::StoreImageInconsistent, &Fixture::default(), NOW)
        .await
        .expect_err("an apiserver-shaped source cannot answer for the image");
    assert!(
        matches!(
            err,
            CensusError::NeedsDataDir {
                predicate: Predicate::StoreImageInconsistent
            }
        ),
        "{err}"
    );
}

// ── rbac-bare-parent-only (T4.2) ─────────────────────────────────────────

fn cluster_role(name: &str, rules: &Value) -> Value {
    json!({"metadata": {"name": name}, "rules": rules})
}

fn crb(name: &str, role: &str, subjects: &Value) -> Value {
    json!({
        "metadata": {"name": name},
        "roleRef": {"apiGroup": RBAC_GROUP, "kind": "ClusterRole", "name": role},
        "subjects": subjects
    })
}

fn rbac_fixture(cluster_roles: Vec<Value>, bindings: Vec<Value>) -> Fixture {
    Fixture::default()
        .with(Builtin::ClusterRole.gvk(), cluster_roles)
        .with(Builtin::ClusterRoleBinding.gvk(), bindings)
}

fn lost(report: &Report) -> Vec<String> {
    report
        .matches()
        .iter()
        .map(|m| match &m.finding {
            Finding::SubresourceLost(g) => [
                m.subject.to_string(),
                " ".into(),
                g.verb.to_string(),
                " ".into(),
                g.resource().to_string(),
            ]
            .concat(),
            other => panic!("not an RBAC finding: {other}"),
        })
        .collect()
}

/// The live escalation T4.2 closed: `create serviceaccounts` also granted
/// `create serviceaccounts/token`. The census reports it, and only it: a
/// subresource nothing serves is not probed, however a rule names it.
#[tokio::test]
async fn a_bare_parent_grant_of_a_served_subresource_is_reported() {
    let source = rbac_fixture(
        vec![cluster_role(
            "sa-creator",
            &json!([
                {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["create"]},
                {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get"]}
            ]),
        )],
        vec![crb(
            "alice-sa",
            "sa-creator",
            &json!([{"kind": "User", "name": "alice"}]),
        )],
    );
    let report = census(Predicate::RbacBareParentOnly, &source).await;
    assert_eq!(
        lost(&report),
        ["User alice create serviceaccounts/token"],
        "configmaps has no subresource to lose"
    );
}

/// A subject whose rules grant the subresource explicitly lost nothing
/// there; `system:masters` is allowed everything and never reported. What
/// the explicit rules do not cover is still reported.
#[tokio::test]
async fn an_explicit_grant_and_system_masters_are_not_reported() {
    let source = rbac_fixture(
        vec![cluster_role(
            "full",
            &json!([
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]},
                {"apiGroups": ["", "apps"], "resources": ["pods/log", "*/status"], "verbs": ["get"]},
                {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}
            ]),
        )],
        vec![crb(
            "b",
            "full",
            &json!([
                {"kind": "User", "name": "bob"},
                {"kind": "Group", "name": "system:masters"}
            ]),
        )],
    );
    let report = census(Predicate::RbacBareParentOnly, &source).await;
    assert_eq!(
        lost(&report),
        ["User bob get deployments/scale"],
        "pods/log, pods/status and deployments/status are granted explicitly; masters is skipped"
    );
}

/// A grant through a `RoleBinding` is reported in its namespace, for the
/// `ServiceAccount` the binding names (defaulting to the binding's
/// namespace).
#[tokio::test]
async fn a_role_binding_loss_is_reported_in_its_namespace() {
    let source = Fixture::default()
        .with(
            Builtin::Role.gvk(),
            vec![json!({
                "metadata": {"name": "reader", "namespace": "team"},
                "rules": [{"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}]
            })],
        )
        .with(
            Builtin::RoleBinding.gvk(),
            vec![json!({
                "metadata": {"name": "rb", "namespace": "team"},
                "roleRef": {"apiGroup": RBAC_GROUP, "kind": "Role", "name": "reader"},
                "subjects": [{"kind": "ServiceAccount", "name": "bot"}]
            })],
        );
    let report = census(Predicate::RbacBareParentOnly, &source).await;
    assert_eq!(
        lost(&report),
        [
            "ServiceAccount team/bot get deployments/scale",
            "ServiceAccount team/bot get deployments/status",
        ]
    );
    let Finding::SubresourceLost(grant) = &report.matches()[0].finding else {
        panic!("not an RBAC finding");
    };
    assert_eq!(grant.namespace.as_deref(), Some("team"));
    assert_eq!(grant.via.to_string(), "Role reader");
}

/// A wildcard verb granted every verb: the census probes each verb some rule
/// names and the class of all others, and reports each one the shipped
/// authorizer refuses.
#[tokio::test]
async fn a_wildcard_verb_is_probed_per_verb_class() {
    let source = rbac_fixture(
        vec![
            cluster_role(
                "any-verb",
                &json!([{"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["*"]}]),
            ),
            cluster_role(
                "scaler",
                &json!([{"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["update"]}]),
            ),
        ],
        vec![
            crb(
                "c1",
                "any-verb",
                &json!([{"kind": "User", "name": "carol"}]),
            ),
            crb("c2", "scaler", &json!([{"kind": "User", "name": "carol"}])),
        ],
    );
    let report = census(Predicate::RbacBareParentOnly, &source).await;
    assert_eq!(
        lost(&report),
        [
            "User carol update deployments/status",
            "User carol (any other) deployments/scale",
            "User carol (any other) deployments/status",
        ],
        "update deployments/scale is granted explicitly"
    );
}

/// A CRD that serves `/status` makes `<plural>/status` a served subresource
/// in its group: a bare grant on the plural reached it.
#[tokio::test]
async fn a_crd_status_subresource_is_probed_in_its_group() {
    let mut crd = widget_crd();
    crd["spec"]["versions"][0]["subresources"] = json!({"status": {}});
    let source = rbac_fixture(
        vec![cluster_role(
            "widget-reader",
            &json!([{"apiGroups": ["example.com"], "resources": ["widgets"], "verbs": ["get"]}]),
        )],
        vec![crb(
            "d",
            "widget-reader",
            &json!([{"kind": "Group", "name": "widget-team"}]),
        )],
    )
    .with(Builtin::CustomResourceDefinition.gvk(), vec![crd]);
    let report = census(Predicate::RbacBareParentOnly, &source).await;
    assert_eq!(lost(&report), ["Group widget-team get widgets/status"]);
}

// ── the report ───────────────────────────────────────────────────────────

/// Zero matches is printed as a finding with its totals, and the header says
/// which predicate ran over which source.
#[tokio::test]
async fn an_empty_census_says_so() {
    let report = census(Predicate::DeploymentWouldRoll, &Fixture::default()).await;
    let printed = report.to_string();
    assert!(
        printed.starts_with("census deployment-would-roll (T4.10) over fixture\n"),
        "{printed}"
    );
    assert!(
        printed.contains("examined apps/v1/Deployment: 0\n"),
        "{printed}"
    );
    assert!(
        printed.ends_with("total: 0 finding(s) over 0 subject(s)\n"),
        "{printed}"
    );
}
