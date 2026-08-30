//! `NetworkPolicyController` — the PRODUCER `NetworkPolicyEnforcer` never had.
//!
//! ★ THE GAP THIS CLOSES. `network_policy.rs` shipped a typed rule model, a
//! trait, a `FakeNetworkPolicyEnforcer` and a `CiliumNetworkPolicyAdapter`,
//! all tested — and nothing anywhere constructed a rule from a real
//! `NetworkPolicy` object or called `upsert`. Measured 2026-08-30:
//!
//! ```text
//! $ grep -rn 'NetworkPolicyEnforcer' --include=*.rs . | grep -v network_policy.rs
//! engenho-controllers/src/lib.rs:136:   (the re-export)
//! ```
//!
//! Zero consumers. That is instance #8 of "type + backend + no producer" in
//! this codebase, and it is the most dangerous one so far: every other
//! instance was a missing feature, while this one is a POLICY OBJECT THAT
//! APPLIES SUCCESSFULLY AND RESTRICTS NOTHING. An operator writes a
//! default-deny policy, `kubectl` says `created`, and every packet still
//! flows.
//!
//! ★ TWO SEPARATE JOBS, DELIBERATELY NOT COLLAPSED.
//!
//! 1. **Translate + install.** `spec` → [`NetworkPolicyRule`]s → the
//!    enforcer. This is the feature.
//! 2. **Say which one happened.** If the enforcer's
//!    [`PolicyDatapath`] is `Computed` — the darwin topology, where there is
//!    no kernel to install a filter into — the policy is annotated and an
//!    event is emitted saying the traffic is unrestricted.
//!
//! Job 2 is not decoration and it does not wait for job 1 to be perfect. A
//! computed-only policy is indistinguishable from an enforced one by every
//! `kubectl` command there is, so the *only* thing standing between an
//! operator and a false sense of containment is this annotation.
//!
//! ★ WHY "ALLOW FROM ANYWHERE" IS ENCODED AS `0.0.0.0/0` AND NOT AS AN
//! EMPTY PEER LIST. Upstream's `from: []` (absent) means *allow all*, while
//! a policyType naming a direction with no rules at all means *deny all*.
//! [`NetworkPolicyRule`] already fixes empty `allowed_peers` to mean deny,
//! so the two cases would collapse onto the same value and the permissive
//! one would silently become a deny — or, read the other way, a deny would
//! become permissive. Rendering allow-all as an explicit
//! `IpBlock { cidr: "0.0.0.0/0" }` keeps them distinct without widening the
//! rule type, and it is what every real enforcer emits for that case anyway.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_store::StoreMesh;
use engenho_store::command::{Reason as CommandReason, ResourceCommand};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;
use crate::event_recorder::{EventRecord, EventSink, InvolvedObject, NullEventSink, Reason};
use crate::network_policy::{
    Direction, NetworkPolicyEnforcer, NetworkPolicyRule, PeerSelector, PolicyDatapath, PortSpec,
};

/// The annotation carrying the enforcement verdict, so the distinction is
/// machine-queryable (`kubectl get netpol -o json`) and not only visible in
/// an event that ages out.
pub const ENFORCEMENT_ANNOTATION: &str = "engenho.io/network-policy-enforcement";

/// Upstream's allow-all CIDR. See the module header for why allow-all is
/// encoded this way rather than as an empty peer list.
pub const ALLOW_ALL_CIDR: &str = "0.0.0.0/0";

// =====================================================================
// TRANSLATION — pure, and therefore the part that can be exhaustively
// tested without a store, an enforcer or a clock.
// =====================================================================

/// The `policyTypes` a `NetworkPolicy` is effectively subject to.
///
/// Upstream's defaulting rule, which is easy to get subtly wrong: when
/// `policyTypes` is absent it is `[Ingress]`, PLUS `Egress` if and only if
/// the policy has any `egress` rules. It is NOT "whichever sections are
/// present" — a policy with neither section is still an ingress deny-all.
#[must_use]
pub fn effective_policy_types(spec: &Value) -> (bool, bool) {
    if let Some(types) = spec.get("policyTypes").and_then(Value::as_array) {
        let has = |t: &str| types.iter().any(|v| v.as_str() == Some(t));
        return (has("Ingress"), has("Egress"));
    }
    let has_egress = spec
        .get("egress")
        .and_then(Value::as_array)
        .is_some_and(|a| !a.is_empty());
    (true, has_egress)
}

fn match_labels_of(v: Option<&Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(obj) = v
        .and_then(|s| s.get("matchLabels"))
        .and_then(Value::as_object)
    else {
        return out;
    };
    for (k, val) in obj {
        if let Some(s) = val.as_str() {
            out.insert(k.clone(), s.to_string());
        }
    }
    out
}

/// One `from`/`to` entry → a peer.
///
/// Returns `None` for an entry naming neither a selector nor a CIDR: an
/// unrecognised peer must never widen the rule to allow-all, so it is
/// dropped rather than defaulted.
fn peer_of(entry: &Value) -> Option<PeerSelector> {
    if let Some(ip) = entry.get("ipBlock") {
        let cidr = ip.get("cidr").and_then(Value::as_str)?.to_string();
        let except = ip
            .get("except")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        return Some(PeerSelector::IpBlock { cidr, except });
    }
    // A `namespaceSelector` present alongside a `podSelector` is upstream's
    // AND semantics. The flat rule model has no conjunction, so the
    // namespace selector — the WIDER of the two — is dropped and the pod
    // selector kept, which narrows rather than widens. Narrowing an allow
    // rule can break traffic loudly; widening it fails open silently, and
    // between those two this picks the one an operator can see.
    if entry.get("podSelector").is_some() {
        return Some(PeerSelector::PodSelector {
            match_labels: match_labels_of(entry.get("podSelector")),
        });
    }
    if entry.get("namespaceSelector").is_some() {
        return Some(PeerSelector::NamespaceSelector {
            match_labels: match_labels_of(entry.get("namespaceSelector")),
        });
    }
    None
}

fn ports_of(rule: &Value) -> Vec<PortSpec> {
    let Some(ports) = rule.get("ports").and_then(Value::as_array) else {
        return Vec::new();
    };
    ports
        .iter()
        .filter_map(|p| {
            // A named port (`port: "http"`) cannot be resolved without the
            // target pod's containerPort table, which translation does not
            // have. Dropping the ENTRY (not the port field) is the safe
            // direction: keeping it with a defaulted port number would
            // allow traffic on a port nobody asked for.
            let port = u16::try_from(p.get("port").and_then(Value::as_u64)?).ok()?;
            let end_port = p
                .get("endPort")
                .and_then(Value::as_u64)
                .and_then(|v| u16::try_from(v).ok());
            Some(PortSpec {
                port,
                end_port,
                protocol: p
                    .get("protocol")
                    .and_then(Value::as_str)
                    .unwrap_or("TCP")
                    .to_string(),
            })
        })
        .collect()
}

/// The stable id of one translated rule.
///
/// `<ns>/<name>#<direction>:<index>` — a policy produces several rules, and
/// the enforcer keys on this string, so it has to be unique per rule AND
/// prefix-derivable from the policy so deletion can find them all.
#[must_use]
pub fn rule_id(namespace: &str, name: &str, direction: Direction, index: usize) -> String {
    let d = match direction {
        Direction::Ingress => "ingress",
        Direction::Egress => "egress",
    };
    format!("{namespace}/{name}#{d}:{index}")
}

/// The prefix every rule of one policy shares. Used to reap on delete.
#[must_use]
pub fn policy_prefix(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}#")
}

/// Translate one `NetworkPolicy` object into flat enforcer rules.
///
/// Total: never panics, never returns an error. A malformed policy yields
/// fewer rules, never a wrong one.
#[must_use]
pub fn translate(policy: &Value) -> Vec<NetworkPolicyRule> {
    let meta = policy.get("metadata");
    let namespace = meta
        .and_then(|m| m.get("namespace"))
        .and_then(Value::as_str)
        .unwrap_or("default");
    let name = meta
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if name.is_empty() {
        return Vec::new();
    }
    // `get("spec")` on `{"spec": null}` returns `Some(Null)`, not `None` —
    // so requiring an OBJECT is what actually rejects a malformed policy.
    // Without this a null spec defaults to "policyTypes absent" and
    // translates to a deny-all ingress rule: engenho would invent a
    // restriction the operator never wrote. Found by T8, not by reading.
    let Some(spec) = policy.get("spec").filter(|s| s.is_object()) else {
        return Vec::new();
    };

    let pod_selector = match_labels_of(spec.get("podSelector"));
    let (ingress, egress) = effective_policy_types(spec);
    let mut out = Vec::new();

    for (enabled, direction, field, peer_field) in [
        (ingress, Direction::Ingress, "ingress", "from"),
        (egress, Direction::Egress, "egress", "to"),
    ] {
        if !enabled {
            continue;
        }
        let rules = spec.get(field).and_then(Value::as_array).map(Vec::as_slice);
        match rules {
            // A direction named in policyTypes with no rules is upstream's
            // DENY ALL. Empty `allowed_peers` is exactly that.
            None | Some(&[]) => out.push(NetworkPolicyRule {
                policy_id: rule_id(namespace, name, direction, 0),
                pod_selector: pod_selector.clone(),
                direction,
                allowed_peers: Vec::new(),
                allowed_ports: Vec::new(),
            }),
            Some(list) => {
                for (i, r) in list.iter().enumerate() {
                    let peers = match r
                        .get(peer_field)
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                    {
                        // Absent or empty `from`/`to` is ALLOW ALL — the
                        // case that must not collapse onto deny-all.
                        None | Some(&[]) => vec![PeerSelector::IpBlock {
                            cidr: ALLOW_ALL_CIDR.to_string(),
                            except: Vec::new(),
                        }],
                        Some(entries) => {
                            let peers: Vec<_> = entries.iter().filter_map(peer_of).collect();
                            // Every entry was unrecognised. Emitting a rule
                            // with no peers here would turn an ALLOW into a
                            // DENY; emitting allow-all would ignore the
                            // operator's intent entirely. Skip the rule and
                            // let the direction's other rules stand.
                            if peers.is_empty() {
                                continue;
                            }
                            peers
                        }
                    };
                    out.push(NetworkPolicyRule {
                        policy_id: rule_id(namespace, name, direction, i),
                        pod_selector: pod_selector.clone(),
                        direction,
                        allowed_peers: peers,
                        allowed_ports: ports_of(r),
                    });
                }
            }
        }
    }
    out
}

// =====================================================================
// HONESTY — the annotation, kept idempotent.
// =====================================================================

/// The annotation value for a datapath verdict.
#[must_use]
pub fn enforcement_value(datapath: PolicyDatapath) -> &'static str {
    match datapath {
        PolicyDatapath::Computed => "Computed",
        PolicyDatapath::Installed => "Installed",
    }
}

/// Whether the policy already carries the right verdict.
///
/// Keeps the controller idempotent: rewriting an unchanged annotation every
/// tick advances the store revision forever, which is the hot-loop class
/// the node lease already hit once in this codebase.
#[must_use]
pub fn already_annotated(policy: &Value, datapath: PolicyDatapath) -> bool {
    policy
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(ENFORCEMENT_ANNOTATION))
        .and_then(Value::as_str)
        == Some(enforcement_value(datapath))
}

/// The policy with the enforcement verdict annotated.
#[must_use]
pub fn annotated(policy: &Value, datapath: PolicyDatapath) -> Value {
    let mut out = policy.clone();
    let meta = out
        .as_object_mut()
        .expect("policy is an object")
        .entry("metadata")
        .or_insert_with(|| json!({}));
    let anns = meta
        .as_object_mut()
        .expect("metadata is an object")
        .entry("annotations")
        .or_insert_with(|| json!({}));
    anns.as_object_mut()
        .expect("annotations is an object")
        .insert(
            ENFORCEMENT_ANNOTATION.to_string(),
            json!(enforcement_value(datapath)),
        );
    out
}

/// The message an operator reads when a policy restricts nothing.
///
/// Names the CONSEQUENCE, not the absence — the same rule the inert-kind
/// messages follow.
#[must_use]
pub fn not_enforced_message(backend: &str) -> String {
    format!(
        "NetworkPolicy accepted and its rules computed, but the '{backend}' backend installs no \
         packet filter on this node: traffic this policy claims to restrict is NOT restricted"
    )
}

// =====================================================================
// THE PRODUCER
// =====================================================================

/// Translates every `NetworkPolicy` into enforcer rules, installs them, and
/// records whether they are actually enforced.
pub struct NetworkPolicyController {
    store: Arc<StoreMesh>,
    enforcer: Arc<dyn NetworkPolicyEnforcer>,
    events: Arc<dyn EventSink>,
}

impl NetworkPolicyController {
    /// New controller over `store`, installing through `enforcer`.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, enforcer: Arc<dyn NetworkPolicyEnforcer>) -> Self {
        Self {
            store,
            enforcer,
            events: Arc::new(NullEventSink),
        }
    }

    /// Builder: wire the event sink so a computed-only policy is visible in
    /// `kubectl describe`.
    #[must_use]
    pub fn with_event_sink(mut self, events: Arc<dyn EventSink>) -> Self {
        self.events = events;
        self
    }
}

#[async_trait]
impl Controller for NetworkPolicyController {
    fn name(&self) -> &'static str {
        "network-policy"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let datapath = self.enforcer.datapath();
        let now = engenho_types::time::now_rfc3339_utc();

        let policies = self
            .store
            .list("networking.k8s.io", "v1", "NetworkPolicy", None)
            .await;
        report.objects_examined = policies.len();

        // Every rule id this pass believes should exist. Anything the
        // enforcer holds that is NOT here belongs to a deleted policy and
        // is reaped below — which is how a `kubectl delete netpol` actually
        // stops enforcing, rather than leaving the filter installed forever.
        let mut desired_ids: Vec<String> = Vec::new();

        for (key, policy) in policies {
            for rule in translate(&policy) {
                desired_ids.push(rule.policy_id.clone());
                if self.enforcer.upsert(&rule).await.is_err() {
                    report.objects_skipped += 1;
                }
            }

            if already_annotated(&policy, datapath) {
                continue;
            }
            let desired = annotated(&policy, datapath);
            if self
                .store
                .propose(ResourceCommand::Put {
                    key,
                    value: desired,
                    expected: None,
                    reason: CommandReason::Controller,
                })
                .await
                .is_ok()
            {
                report.objects_changed += 1;
            } else {
                report.objects_skipped += 1;
                continue;
            }

            // The event fires only on the transition — the annotation check
            // above already returned for a policy seen on a previous tick —
            // so this does not spam one event per policy per tick.
            if datapath == PolicyDatapath::Computed {
                let meta = policy.get("metadata");
                self.events
                    .record(EventRecord {
                        involved: InvolvedObject {
                            api_version: "networking.k8s.io/v1".into(),
                            kind: "NetworkPolicy".into(),
                            namespace: meta
                                .and_then(|m| m.get("namespace"))
                                .and_then(Value::as_str)
                                .map(ToString::to_string),
                            name: meta
                                .and_then(|m| m.get("name"))
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            uid: meta
                                .and_then(|m| m.get("uid"))
                                .and_then(Value::as_str)
                                .map(ToString::to_string),
                        },
                        reason: Reason::NetworkPolicyNotEnforced,
                        message: not_enforced_message(self.enforcer.name()),
                        component: "network-policy-controller".into(),
                        timestamp: now.clone(),
                    })
                    .await;
            }
        }

        // Reap rules whose policy is gone.
        if let Ok(installed) = self.enforcer.list().await {
            for rule in installed {
                if !desired_ids.contains(&rule.policy_id) {
                    let _ = self.enforcer.remove(&rule.policy_id).await;
                    report.objects_changed += 1;
                }
            }
        }

        Ok(ReconcileOutcome::from(report))
    }
}

#[cfg(test)]
mod translate_tests {
    use super::*;

    fn policy(spec: Value) -> Value {
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": { "name": "p", "namespace": "ns" },
            "spec": spec,
        })
    }

    // T1 — the defaulting rule that is easy to get subtly wrong.
    #[test]
    fn absent_policy_types_means_ingress_plus_egress_only_if_egress_rules_exist() {
        assert_eq!(effective_policy_types(&json!({})), (true, false));
        assert_eq!(
            effective_policy_types(&json!({ "egress": [{ "to": [] }] })),
            (true, true)
        );
        // Present-but-empty egress does NOT turn egress on: upstream keys
        // the default off the presence of RULES, not of the field.
        assert_eq!(
            effective_policy_types(&json!({ "egress": [] })),
            (true, false)
        );
        // An explicit list wins over every inference.
        assert_eq!(
            effective_policy_types(&json!({ "policyTypes": ["Egress"], "ingress": [{}] })),
            (false, true)
        );
    }

    // T2 — THE load-bearing distinction. Deny-all and allow-all differ by
    // one absent field in the source and must never render the same.
    #[test]
    fn deny_all_and_allow_all_do_not_collapse_onto_each_other() {
        // No ingress section at all ⇒ deny all ingress.
        let deny = translate(&policy(json!({ "podSelector": {} })));
        assert_eq!(deny.len(), 1);
        assert!(
            deny[0].allowed_peers.is_empty(),
            "empty peers is this model's deny-all"
        );

        // One ingress rule with no `from` ⇒ allow from anywhere.
        let allow = translate(&policy(json!({ "ingress": [{}] })));
        assert_eq!(allow.len(), 1);
        assert_eq!(
            allow[0].allowed_peers,
            vec![PeerSelector::IpBlock {
                cidr: ALLOW_ALL_CIDR.into(),
                except: vec![]
            }]
        );
        assert_ne!(deny[0].allowed_peers, allow[0].allowed_peers);
    }

    // T3 — an explicitly empty `from: []` is also allow-all upstream.
    #[test]
    fn an_explicitly_empty_from_is_allow_all_not_deny_all() {
        let r = translate(&policy(json!({ "ingress": [{ "from": [] }] })));
        assert_eq!(r.len(), 1);
        assert!(!r[0].allowed_peers.is_empty(), "from: [] means allow all");
    }

    // T4 — every rule of one policy shares a reapable prefix and has a
    // unique id, or the enforcer's keyed map silently loses rules.
    #[test]
    fn rule_ids_are_unique_and_share_the_policy_prefix() {
        let r = translate(&policy(json!({
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [{ "from": [{ "ipBlock": { "cidr": "10.0.0.0/8" } }] },
                        { "from": [{ "ipBlock": { "cidr": "192.168.0.0/16" } }] }],
            "egress": [{ "to": [{ "ipBlock": { "cidr": "0.0.0.0/0" } }] }],
        })));
        assert_eq!(r.len(), 3);
        let ids: Vec<_> = r.iter().map(|x| x.policy_id.clone()).collect();
        let mut uniq = ids.clone();
        uniq.sort();
        uniq.dedup();
        assert_eq!(uniq.len(), ids.len(), "ids collide: {ids:?}");
        let prefix = policy_prefix("ns", "p");
        assert!(ids.iter().all(|i| i.starts_with(&prefix)), "{ids:?}");
    }

    // T5 — ports, including the named-port case that cannot be resolved.
    #[test]
    fn a_named_port_is_dropped_rather_than_defaulted_to_a_number() {
        let r = translate(&policy(json!({
            "ingress": [{ "from": [{ "ipBlock": { "cidr": "10.0.0.0/8" } }],
                          "ports": [{ "port": 80, "protocol": "TCP" },
                                    { "port": "http", "protocol": "TCP" },
                                    { "port": 8000, "endPort": 8100, "protocol": "UDP" }] }]
        })));
        assert_eq!(r[0].allowed_ports.len(), 2, "the named port is dropped");
        assert_eq!(r[0].allowed_ports[0].port, 80);
        assert_eq!(r[0].allowed_ports[1].end_port, Some(8100));
        assert_eq!(r[0].allowed_ports[1].protocol, "UDP");
    }

    // T6 — protocol defaults to TCP, as upstream does.
    #[test]
    fn protocol_defaults_to_tcp() {
        let r = translate(&policy(json!({
            "ingress": [{ "from": [{ "ipBlock": { "cidr": "10.0.0.0/8" } }],
                          "ports": [{ "port": 53 }] }]
        })));
        assert_eq!(r[0].allowed_ports[0].protocol, "TCP");
    }

    // T7 — a rule whose every peer is unrecognised is SKIPPED, because
    // both alternatives are wrong: no peers would flip an allow into a
    // deny, and allow-all would ignore the operator entirely.
    #[test]
    fn a_rule_with_only_unrecognised_peers_is_skipped_not_turned_into_a_deny() {
        let r = translate(&policy(json!({
            "policyTypes": ["Ingress"],
            "ingress": [{ "from": [{ "somethingFromTheFuture": {} }] }],
        })));
        assert!(r.is_empty(), "got {r:?}");
    }

    // T8 — totality. A malformed policy yields fewer rules, never a panic
    // and never a wrong one.
    #[test]
    fn malformed_policies_are_total() {
        assert!(translate(&json!({})).is_empty());
        assert!(translate(&json!({ "metadata": { "name": "p" } })).is_empty());
        assert!(translate(&json!({ "spec": {} })).is_empty(), "no name");
        assert!(translate(&json!(null)).is_empty());
        // A policy with a name but a null spec field.
        assert!(translate(&json!({ "metadata": { "name": "p" }, "spec": null })).is_empty());
    }

    // T9 — the pod selector reaches every rule, or the rules apply to the
    // wrong pods.
    #[test]
    fn the_pod_selector_is_carried_onto_every_rule() {
        let r = translate(&policy(json!({
            "podSelector": { "matchLabels": { "app": "db" } },
            "policyTypes": ["Ingress", "Egress"],
        })));
        assert_eq!(r.len(), 2);
        assert!(r.iter().all(|x| x.pod_selector["app"] == "db"));
    }

    // T10 — namespace defaulting, since a rule id keyed on the wrong
    // namespace collides across namespaces.
    #[test]
    fn a_policy_without_a_namespace_lands_in_default() {
        let r = translate(&json!({
            "metadata": { "name": "p" },
            "spec": { "policyTypes": ["Ingress"] },
        }));
        assert!(r[0].policy_id.starts_with("default/p#"));
    }
}

#[cfg(test)]
mod honesty_tests {
    use super::*;

    #[test]
    fn the_annotation_records_the_verdict_and_is_idempotent() {
        let p = json!({ "metadata": { "name": "p" } });
        assert!(!already_annotated(&p, PolicyDatapath::Computed));
        let once = annotated(&p, PolicyDatapath::Computed);
        assert!(already_annotated(&once, PolicyDatapath::Computed));
        // A rewrite every tick advances the store revision forever — the
        // hot-loop class the node lease already hit in this codebase.
        assert_eq!(once, annotated(&once, PolicyDatapath::Computed));
    }

    #[test]
    fn a_computed_annotation_does_not_satisfy_an_installed_check() {
        // The direction that matters: a policy that is NOT enforced must
        // never read as enforced.
        let p = annotated(&json!({}), PolicyDatapath::Computed);
        assert!(!already_annotated(&p, PolicyDatapath::Installed));
    }

    #[test]
    fn annotating_preserves_existing_metadata_and_annotations() {
        let p = json!({
            "metadata": { "name": "p", "namespace": "ns",
                          "annotations": { "keep": "me" } }
        });
        let out = annotated(&p, PolicyDatapath::Installed);
        assert_eq!(out["metadata"]["name"], "p");
        assert_eq!(out["metadata"]["namespace"], "ns");
        assert_eq!(out["metadata"]["annotations"]["keep"], "me");
    }

    #[test]
    fn the_message_names_what_is_not_restricted_not_merely_what_is_missing() {
        let m = not_enforced_message("fake");
        assert!(m.contains("NOT restricted"), "{m}");
        assert!(m.contains("fake"), "{m}");
    }

    #[test]
    fn a_new_backend_that_forgets_to_override_claims_enforcement() {
        // The default is deliberately the CLAIM, so a backend that forgets
        // is caught by an honesty test rather than defaulting to the
        // permissive answer nobody notices.
        struct Forgetful;
        #[async_trait]
        impl NetworkPolicyEnforcer for Forgetful {
            fn name(&self) -> &'static str {
                "forgetful"
            }
            async fn upsert(
                &self,
                _r: &NetworkPolicyRule,
            ) -> Result<(), crate::network_policy::NetworkPolicyError> {
                Ok(())
            }
            async fn remove(
                &self,
                _id: &str,
            ) -> Result<(), crate::network_policy::NetworkPolicyError> {
                Ok(())
            }
            async fn list(
                &self,
            ) -> Result<Vec<NetworkPolicyRule>, crate::network_policy::NetworkPolicyError>
            {
                Ok(vec![])
            }
        }
        assert_eq!(Forgetful.datapath(), PolicyDatapath::Installed);
        // And the test double, which installs nothing, says so.
        assert_eq!(
            crate::network_policy::FakeNetworkPolicyEnforcer::new().datapath(),
            PolicyDatapath::Computed
        );
    }
}
