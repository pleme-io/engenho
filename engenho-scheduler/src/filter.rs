//! The **Filter** stage: a closed set of plugins, and the only constructor of
//! [`Feasible`].
//!
//! ## Why a closed set
//!
//! Before T5.7 the scheduler's Filter stage was one inline resource-fit check,
//! and the strategy itself skipped cordoned and not-ready nodes. The node
//! selector and taint predicates in [`crate::predicates`] were written and
//! tested and called by nothing, so a pod with `nodeSelector: {gpu: "true"}`
//! was bound to a `gpu=false` node carrying a `NoSchedule` taint (measured
//! 2026-09-18). Each missing check was an omission nobody could see: every
//! binding still looked like a binding.
//!
//! [`FilterPlugin`] names every check the Filter stage runs, and
//! [`FilterPlugin::ALL`] is the order [`filter`] runs them in. Both come from
//! one list (`filter_plugins!`), so a declared plugin is a run plugin. Adding
//! a variant is a compile error in [`FilterPlugin::check`] until the check is
//! written, and in [`Rejection::plugin`] until its rejection is attributed.
//!
//! ## Why `Feasible`
//!
//! A strategy is handed a [`Feasible`] and returns a node name. `Feasible` has
//! private fields and one constructor, [`filter`], which puts a node in it only
//! after every plugin admitted it. So a strategy cannot be handed a node that
//! skipped a filter, and it cannot be handed an empty set: the head is a field,
//! not the first element of a `Vec` that might have none. `pick` therefore has
//! no `None` arm, and the "strategy found nothing" count the scheduler used to
//! keep (`skipped_no_node`) has no way to be non-zero, so it is gone.
//!
//! ## What this does NOT make impossible
//!
//! The type proves [`filter`] ran. It does not prove `filter` ran against the
//! same [`NodeLedger`] the scheduler debits afterwards; the scheduler holds one
//! ledger per tick at one call site. Pod affinity, anti-affinity and topology
//! spread ([`crate::affinity`]) are not plugins yet, and preemption
//! ([`crate::preemption`]) is not wired.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;

use serde_json::Value;

use crate::fit::PodRequests;
use crate::ledger::NodeLedger;
use crate::observed::ObservedNode;
use crate::predicates;

/// Declare the plugin enum, its run order and its names from ONE list.
///
/// [`FilterPlugin::ALL`] is what [`filter`] iterates. Were it a hand-written
/// array beside the enum, a new variant would get its `check` arm (the match
/// is exhaustive) and still never run: the "predicate with tests and no
/// caller" failure T5.7 fixes, one level up. Generated from the same list, a
/// variant cannot be declared without being run.
macro_rules! filter_plugins {
    ($( $(#[$doc:meta])* $variant:ident ),+ $(,)?) => {
        /// Every check the Filter stage runs, declared in the order it runs
        /// them.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum FilterPlugin {
            $( $(#[$doc])* $variant, )+
        }

        impl FilterPlugin {
            /// Every plugin, in the order [`filter`] runs them.
            pub const ALL: &'static [Self] = &[ $( Self::$variant, )+ ];

            /// Stable identifier, for logs and the T0.10 census.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $( Self::$variant => stringify!($variant), )+
                }
            }
        }
    };
}

// Cheapest first, and most specific first, so the reason a node is reported
// under is the one an operator can act on: a cordoned node says "cordoned",
// not "taint", and a node that is not ready says so before anything is
// judged against its labels or capacity.
filter_plugins! {
    /// The node's Lease-derived `Ready` condition is `True`.
    NodeReady,
    /// The node is not cordoned (`spec.unschedulable`).
    Cordon,
    /// The node has a name a pod can be bound to, and it is the one the pod
    /// is pinned to, if it is pinned.
    NodeName,
    /// The node's labels satisfy the pod's `spec.nodeSelector`.
    NodeSelector,
    /// The pod tolerates every `NoSchedule` / `NoExecute` taint on the node.
    TaintToleration,
    /// The pod's requests fit the node's remaining capacity in the ledger.
    Resources,
}

impl FilterPlugin {
    /// Run this one plugin for `pod` against `node`.
    ///
    /// `req` is the pod's effective requests ([`crate::pod_requests`]) and
    /// `ledger` the tick's capacity books; only [`Self::Resources`] reads
    /// them.
    ///
    /// # Errors
    ///
    /// The [`Rejection`] naming why the node is not a candidate.
    pub fn check(
        self,
        pod: &Value,
        req: &PodRequests,
        node: &ObservedNode,
        ledger: &NodeLedger,
    ) -> Result<(), Rejection> {
        match self {
            Self::NodeReady => refuse_unless(node.is_ready(), Rejection::NotReady),
            Self::Cordon => {
                refuse_unless(!predicates::is_cordoned(node.value()), Rejection::Cordoned)
            }
            Self::NodeName => bindable_name(pod, node).map(drop),
            Self::NodeSelector => refuse_unless(
                predicates::matches_node_selector(pod, node.value()),
                Rejection::NodeSelectorMismatch,
            ),
            Self::TaintToleration => match predicates::untolerated_taint(pod, node.value()) {
                Some(taint) => Err(Rejection::UntoleratedTaint { key: taint.key }),
                None => Ok(()),
            },
            Self::Resources => {
                if req.unparseable {
                    Err(Rejection::UnreadableRequests)
                } else {
                    refuse_unless(
                        node.name().is_some_and(|name| ledger.fits(name, req)),
                        Rejection::InsufficientResources,
                    )
                }
            }
        }
    }
}

impl fmt::Display for FilterPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

fn refuse_unless(admitted: bool, rejection: Rejection) -> Result<(), Rejection> {
    if admitted { Ok(()) } else { Err(rejection) }
}

/// The name `pod` would be bound to on `node`: the node's non-empty
/// `metadata.name`, which must equal the pod's pin when it has one.
///
/// A node with no name cannot be written into `spec.nodeName`, so it fails
/// the same plugin a pin to another node does.
fn bindable_name<'n>(pod: &Value, node: &'n ObservedNode) -> Result<&'n str, Rejection> {
    node.name()
        .filter(|name| !name.is_empty() && predicates::node_name_matches(pod, node.value()))
        .ok_or(Rejection::NodeNameMismatch)
}

/// Why a node is not a candidate for a pod.
///
/// Carried rather than collapsed to a bool, so an unschedulable pod's
/// `PodScheduled` condition can say which plugin excluded how many nodes:
/// "2 node(s) had untolerated taint {dedicated}" is actionable, "no node
/// fits" is not.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rejection {
    /// [`FilterPlugin::NodeReady`]: the Lease-derived `Ready` is not `True`.
    NotReady,
    /// [`FilterPlugin::Cordon`].
    Cordoned,
    /// [`FilterPlugin::NodeName`]: pinned elsewhere, or the node has no name.
    NodeNameMismatch,
    /// [`FilterPlugin::NodeSelector`].
    NodeSelectorMismatch,
    /// [`FilterPlugin::TaintToleration`]: the first blocking taint the pod
    /// does not tolerate.
    UntoleratedTaint {
        /// The taint's key.
        key: String,
    },
    /// [`FilterPlugin::Resources`]: the requests do not fit what is left.
    InsufficientResources,
    /// [`FilterPlugin::Resources`]: a request could not be read, so the pod
    /// fits no node (see [`crate::fit`]).
    UnreadableRequests,
}

impl Rejection {
    /// The plugin that produced this rejection.
    #[must_use]
    pub const fn plugin(&self) -> FilterPlugin {
        match self {
            Self::NotReady => FilterPlugin::NodeReady,
            Self::Cordoned => FilterPlugin::Cordon,
            Self::NodeNameMismatch => FilterPlugin::NodeName,
            Self::NodeSelectorMismatch => FilterPlugin::NodeSelector,
            Self::UntoleratedTaint { .. } => FilterPlugin::TaintToleration,
            Self::InsufficientResources | Self::UnreadableRequests => FilterPlugin::Resources,
        }
    }
}

/// The clause upstream puts after the count in a `FailedScheduling` message.
impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady => f.write_str("node(s) were not ready"),
            Self::Cordoned => f.write_str("node(s) were unschedulable"),
            Self::NodeNameMismatch => f.write_str("node(s) didn't match the requested node name"),
            Self::NodeSelectorMismatch => f.write_str("node(s) didn't match Pod's node selector"),
            Self::UntoleratedTaint { key } => {
                write!(f, "node(s) had untolerated taint {{{key}}}")
            }
            Self::InsufficientResources => f.write_str("node(s) had insufficient cpu/memory"),
            Self::UnreadableRequests => {
                f.write_str("node(s) could not fit the Pod's unreadable resource requests")
            }
        }
    }
}

/// A node every plugin admitted, with the name the pod would be bound to.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'n> {
    name: &'n str,
    node: &'n ObservedNode,
}

impl<'n> Candidate<'n> {
    /// The node's non-empty `metadata.name`.
    #[must_use]
    pub fn name(&self) -> &'n str {
        self.name
    }

    /// The node as observed this tick.
    #[must_use]
    pub fn node(&self) -> &'n ObservedNode {
        self.node
    }
}

/// The nodes a pod may be bound to: non-empty, and built only by [`filter`].
#[derive(Debug, Clone)]
pub struct Feasible<'n> {
    head: Candidate<'n>,
    tail: Vec<Candidate<'n>>,
}

impl<'n> Feasible<'n> {
    /// How many nodes passed every plugin. Never zero.
    #[must_use]
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::MIN.saturating_add(self.tail.len())
    }

    /// Every feasible node, in the order the nodes were observed.
    pub fn iter(&self) -> impl Iterator<Item = &Candidate<'n>> {
        std::iter::once(&self.head).chain(&self.tail)
    }

    /// The candidate at `index`, wrapping past the end. Total: there is no
    /// index without a candidate.
    #[must_use]
    pub fn nth_wrapping(&self, index: usize) -> &Candidate<'n> {
        let index = index % self.len();
        // `index` is below `len() = 1 + tail.len()`, so a non-zero index is
        // always in the tail and only zero reaches the head.
        match index.checked_sub(1).and_then(|i| self.tail.get(i)) {
            Some(candidate) => candidate,
            None => &self.head,
        }
    }
}

/// Why no observed node was feasible: how many nodes each rejection removed.
///
/// Every observed node is counted under exactly one rejection, the first
/// plugin in [`FilterPlugin::ALL`] that refused it, so the number of nodes
/// observed is the sum of the counts and cannot disagree with them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnosis {
    rejections: BTreeMap<Rejection, NonZeroUsize>,
}

impl Diagnosis {
    /// How many nodes were observed (and all rejected).
    #[must_use]
    pub fn nodes_observed(&self) -> usize {
        self.rejections.values().map(|n| n.get()).sum()
    }

    /// How many nodes `rejection` removed.
    #[must_use]
    pub fn count(&self, rejection: &Rejection) -> usize {
        self.rejections.get(rejection).map_or(0, |n| n.get())
    }

    /// How many nodes `plugin` removed, across all its rejections.
    #[must_use]
    pub fn count_by(&self, plugin: FilterPlugin) -> usize {
        self.rejections
            .iter()
            .filter(|(r, _)| r.plugin() == plugin)
            .map(|(_, n)| n.get())
            .sum()
    }

    /// Each rejection with its node count, in [`Rejection`]'s order.
    pub fn iter(&self) -> impl Iterator<Item = (&Rejection, usize)> {
        self.rejections.iter().map(|(r, n)| (r, n.get()))
    }
}

/// Upstream's `FailedScheduling` shape:
/// `0/3 nodes are available: 1 node(s) were unschedulable, 2 node(s) had
/// untolerated taint {dedicated}.`
impl fmt::Display for Diagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0/{} nodes are available: ", self.nodes_observed())?;
        for (i, (rejection, count)) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{count} {rejection}")?;
        }
        f.write_str(".")
    }
}

/// What the Filter stage concluded for one pod.
#[derive(Debug, Clone)]
pub enum Filtered<'n> {
    /// At least one node passed every plugin.
    Feasible(Feasible<'n>),
    /// Nodes were observed and every one was rejected. The pod is
    /// unschedulable, and the diagnosis says why.
    Infeasible(Diagnosis),
    /// No node was observed at all, so nothing was judged. Nothing is written
    /// for the pod: there is no reason to report, only an absence of nodes
    /// (a cluster still booting, before its kubelet registers).
    NoNodesObserved,
}

/// Run one plugin after another for `pod` against `node`, and return the name
/// the pod would be bound to.
///
/// # Errors
///
/// The first plugin's [`Rejection`], in [`FilterPlugin::ALL`] order.
pub fn admit<'n>(
    pod: &Value,
    req: &PodRequests,
    node: &'n ObservedNode,
    ledger: &NodeLedger,
) -> Result<Candidate<'n>, Rejection> {
    for plugin in FilterPlugin::ALL {
        plugin.check(pod, req, node, ledger)?;
    }
    let name = bindable_name(pod, node)?;
    Ok(Candidate { name, node })
}

/// The Filter stage: run every [`FilterPlugin`] for `pod` against every
/// observed node.
///
/// The only constructor of [`Feasible`].
#[must_use]
pub fn filter<'n>(
    pod: &Value,
    req: &PodRequests,
    nodes: &'n [ObservedNode],
    ledger: &NodeLedger,
) -> Filtered<'n> {
    let mut admitted = Vec::new();
    let mut rejections: BTreeMap<Rejection, NonZeroUsize> = BTreeMap::new();
    for node in nodes {
        match admit(pod, req, node, ledger) {
            Ok(candidate) => admitted.push(candidate),
            Err(rejection) => {
                rejections
                    .entry(rejection)
                    .and_modify(|n| *n = n.saturating_add(1))
                    .or_insert(NonZeroUsize::MIN);
            }
        }
    }
    let mut admitted = admitted.into_iter();
    match admitted.next() {
        Some(head) => Filtered::Feasible(Feasible {
            head,
            tail: admitted.collect(),
        }),
        None if rejections.is_empty() => Filtered::NoNodesObserved,
        None => Filtered::Infeasible(Diagnosis { rejections }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fit::pod_requests;
    use engenho_controllers::node_lease::lease_value;
    use serde_json::json;

    const NOW: &str = "2026-09-19T12:00:00Z";

    fn fresh(name: &str) -> Value {
        lease_value(name, &engenho_types::time::now_rfc3339_utc(), 0)
    }

    fn stale(name: &str) -> Value {
        lease_value(name, "2020-01-01T00:00:00Z", 0)
    }

    /// A 4-core / 8Gi node with the given `spec` and labels, storing
    /// `Ready=True` (which only a fresh Lease makes true).
    fn node_value(name: &str, spec: Value, labels: Value) -> Value {
        let mut node = json!({
            "metadata": { "name": name },
            "status": {
                "allocatable": { "cpu": "4", "memory": "8Gi" },
                "conditions": [{ "type": "Ready", "status": "True" }]
            }
        });
        node["metadata"]["labels"] = labels;
        node["spec"] = spec;
        node
    }

    /// Heartbeating, untainted, uncordoned, unlabelled.
    fn ready(name: &str) -> ObservedNode {
        ObservedNode::project(
            node_value(name, json!({}), json!({})),
            Some(&fresh(name)),
            NOW,
        )
    }

    fn ready_with(name: &str, spec: Value, labels: Value) -> ObservedNode {
        ObservedNode::project(node_value(name, spec, labels), Some(&fresh(name)), NOW)
    }

    fn pod(spec: Value) -> Value {
        let mut pod = json!({ "metadata": { "name": "p" } });
        pod["spec"] = spec;
        pod
    }

    fn ledger_of(nodes: &[ObservedNode]) -> NodeLedger {
        NodeLedger::seed(nodes.iter().map(ObservedNode::value), [])
    }

    /// Run `filter` and return the first rejection of a single node.
    fn only_rejection(pod: &Value, node: ObservedNode) -> Option<Rejection> {
        let nodes = [node];
        let ledger = ledger_of(&nodes);
        match filter(pod, &pod_requests(pod), &nodes, &ledger) {
            Filtered::Feasible(_) => None,
            Filtered::Infeasible(d) => d.iter().next().map(|(r, _)| r.clone()),
            Filtered::NoNodesObserved => panic!("one node was observed"),
        }
    }

    fn names(f: &Filtered<'_>) -> Vec<String> {
        match f {
            Filtered::Feasible(fs) => fs.iter().map(|c| c.name().to_owned()).collect(),
            _ => Vec::new(),
        }
    }

    #[test]
    fn plugins_run_in_the_documented_order_under_their_own_names() {
        use FilterPlugin::{Cordon, NodeName, NodeReady, NodeSelector, Resources, TaintToleration};
        assert_eq!(
            FilterPlugin::ALL,
            &[
                NodeReady,
                Cordon,
                NodeName,
                NodeSelector,
                TaintToleration,
                Resources
            ][..]
        );
        let names: Vec<String> = FilterPlugin::ALL.iter().map(ToString::to_string).collect();
        assert_eq!(
            names,
            [
                "NodeReady",
                "Cordon",
                "NodeName",
                "NodeSelector",
                "TaintToleration",
                "Resources"
            ]
        );
    }

    #[test]
    fn a_bare_pod_is_admitted_by_every_plugin() {
        // Anti-vacuity: a plugin set that rejected everything would pass
        // every negative test below.
        let n = ready("n");
        let ledger = ledger_of(std::slice::from_ref(&n));
        let p = pod(json!({}));
        for plugin in FilterPlugin::ALL {
            assert_eq!(
                plugin.check(&p, &pod_requests(&p), &n, &ledger),
                Ok(()),
                "{plugin}"
            );
        }
        assert_eq!(only_rejection(&p, n), None);
    }

    #[test]
    fn a_node_selector_mismatch_excludes_the_node() {
        // The measured incident: gpu=true bound to gpu=false.
        let wants_gpu = pod(json!({ "nodeSelector": { "gpu": "true" } }));
        let no_gpu = ready_with("n", json!({}), json!({ "gpu": "false" }));
        assert_eq!(
            only_rejection(&wants_gpu, no_gpu),
            Some(Rejection::NodeSelectorMismatch)
        );
        let gpu = ready_with("g", json!({}), json!({ "gpu": "true" }));
        assert_eq!(only_rejection(&wants_gpu, gpu), None);
    }

    #[test]
    fn an_untolerated_no_schedule_taint_excludes_the_node_and_names_its_key() {
        let tainted = ready_with(
            "n",
            json!({ "taints": [{ "key": "dedicated", "value": "db", "effect": "NoSchedule" }] }),
            json!({}),
        );
        assert_eq!(
            only_rejection(&pod(json!({})), tainted.clone()),
            Some(Rejection::UntoleratedTaint {
                key: "dedicated".to_owned()
            })
        );
        let tolerates = pod(json!({ "tolerations": [
            { "key": "dedicated", "operator": "Equal", "value": "db", "effect": "NoSchedule" }
        ] }));
        assert_eq!(only_rejection(&tolerates, tainted), None);
    }

    #[test]
    fn a_node_that_is_not_ready_is_rejected_as_not_ready() {
        let wedged = ObservedNode::project(
            node_value("n", json!({}), json!({})),
            Some(&stale("n")),
            NOW,
        );
        assert_eq!(
            only_rejection(&pod(json!({})), wedged),
            Some(Rejection::NotReady)
        );
        let silent = ObservedNode::project(node_value("n", json!({}), json!({})), None, NOW);
        assert_eq!(
            only_rejection(&pod(json!({})), silent),
            Some(Rejection::NotReady)
        );
    }

    #[test]
    fn a_cordoned_node_is_rejected_as_cordoned_not_as_tainted() {
        // Most specific first: the cordon is what the operator did.
        let n = ready_with(
            "n",
            json!({ "unschedulable": true,
                    "taints": [{ "key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule" }] }),
            json!({}),
        );
        assert_eq!(
            only_rejection(&pod(json!({})), n),
            Some(Rejection::Cordoned)
        );
    }

    #[test]
    fn a_node_name_pin_does_not_defeat_a_taint() {
        // Quietly honouring the pin would let it override a restriction the
        // operator set precisely to keep pods off.
        let tainted = ready_with(
            "target",
            json!({ "taints": [{ "key": "k", "effect": "NoSchedule" }] }),
            json!({}),
        );
        assert!(matches!(
            only_rejection(&pod(json!({ "nodeName": "target" })), tainted),
            Some(Rejection::UntoleratedTaint { .. })
        ));
        assert_eq!(
            only_rejection(&pod(json!({ "nodeName": "other" })), ready("target")),
            Some(Rejection::NodeNameMismatch)
        );
    }

    #[test]
    fn a_node_without_a_name_cannot_be_bound_to() {
        let nameless = ObservedNode::project(
            json!({ "metadata": {}, "status": { "allocatable": { "cpu": "4" } } }),
            Some(&fresh("x")),
            NOW,
        );
        assert_eq!(
            only_rejection(&pod(json!({})), nameless),
            Some(Rejection::NodeNameMismatch)
        );
    }

    #[test]
    fn resources_reject_a_pod_that_does_not_fit_and_one_that_cannot_be_read() {
        let greedy = pod(json!({ "containers": [{ "name": "c",
            "resources": { "requests": { "cpu": "64" } } }] }));
        assert_eq!(
            only_rejection(&greedy, ready("n")),
            Some(Rejection::InsufficientResources)
        );
        let garbage = pod(json!({ "containers": [{ "name": "c",
            "resources": { "requests": { "cpu": "lots" } } }] }));
        assert_eq!(
            only_rejection(&garbage, ready("n")),
            Some(Rejection::UnreadableRequests)
        );
    }

    #[test]
    fn filter_keeps_exactly_the_admitted_nodes_in_observed_order() {
        let nodes = [
            ready_with("a-plain", json!({}), json!({ "gpu": "false" })),
            ready_with("b-gpu", json!({}), json!({ "gpu": "true" })),
            ready_with("c-gpu", json!({}), json!({ "gpu": "true" })),
        ];
        let ledger = ledger_of(&nodes);
        let p = pod(json!({ "nodeSelector": { "gpu": "true" } }));
        let out = filter(&p, &pod_requests(&p), &nodes, &ledger);
        assert_eq!(names(&out), ["b-gpu", "c-gpu"]);
    }

    #[test]
    fn no_nodes_is_its_own_outcome_not_an_empty_diagnosis() {
        let ledger = NodeLedger::default();
        let p = pod(json!({}));
        assert!(matches!(
            filter(&p, &pod_requests(&p), &[], &ledger),
            Filtered::NoNodesObserved
        ));
    }

    #[test]
    fn the_diagnosis_counts_each_node_once_under_its_first_rejection() {
        let nodes = [
            ready_with("cordoned", json!({ "unschedulable": true }), json!({})),
            ready_with(
                "tainted-1",
                json!({ "taints": [{ "key": "dedicated", "effect": "NoSchedule" }] }),
                json!({}),
            ),
            ready_with(
                "tainted-2",
                json!({ "taints": [{ "key": "dedicated", "effect": "NoExecute" }] }),
                json!({}),
            ),
        ];
        let ledger = ledger_of(&nodes);
        let p = pod(json!({}));
        let Filtered::Infeasible(d) = filter(&p, &pod_requests(&p), &nodes, &ledger) else {
            panic!("every node is excluded");
        };
        assert_eq!(d.nodes_observed(), 3);
        assert_eq!(d.count(&Rejection::Cordoned), 1);
        assert_eq!(d.count_by(FilterPlugin::TaintToleration), 2);
        assert_eq!(d.count_by(FilterPlugin::Resources), 0);
        assert_eq!(
            d.to_string(),
            "0/3 nodes are available: 1 node(s) were unschedulable, \
             2 node(s) had untolerated taint {dedicated}."
        );
    }

    #[test]
    fn every_rejection_names_the_plugin_that_produced_it() {
        // `check` and `plugin` must agree: a rejection attributed to the
        // wrong plugin would make the census count the wrong filter.
        let tainted = ready_with(
            "n",
            json!({ "taints": [{ "key": "k", "effect": "NoSchedule" }] }),
            json!({ "gpu": "false" }),
        );
        let cases = [
            (
                FilterPlugin::NodeReady,
                ObservedNode::project(node_value("n", json!({}), json!({})), None, NOW),
                pod(json!({})),
            ),
            (
                FilterPlugin::Cordon,
                ready_with("n", json!({ "unschedulable": true }), json!({})),
                pod(json!({})),
            ),
            (
                FilterPlugin::NodeName,
                ready("n"),
                pod(json!({ "nodeName": "elsewhere" })),
            ),
            (
                FilterPlugin::NodeSelector,
                tainted.clone(),
                pod(json!({ "nodeSelector": { "gpu": "true" } })),
            ),
            (FilterPlugin::TaintToleration, tainted, pod(json!({}))),
            (
                FilterPlugin::Resources,
                ready("n"),
                pod(json!({ "containers": [{ "name": "c",
                    "resources": { "requests": { "memory": "1Ti" } } }] })),
            ),
        ];
        for (plugin, node, p) in cases {
            let ledger = ledger_of(std::slice::from_ref(&node));
            let rejection = plugin
                .check(&p, &pod_requests(&p), &node, &ledger)
                .expect_err(plugin.name());
            assert_eq!(rejection.plugin(), plugin, "{rejection}");
        }
    }

    #[test]
    fn nth_wrapping_visits_every_candidate_in_order_and_wraps() {
        let nodes = [ready("a"), ready("b"), ready("c")];
        let ledger = ledger_of(&nodes);
        let p = pod(json!({}));
        let Filtered::Feasible(fs) = filter(&p, &pod_requests(&p), &nodes, &ledger) else {
            panic!("all three are feasible");
        };
        assert_eq!(fs.len().get(), 3);
        let visited: Vec<&str> = (0..7).map(|i| fs.nth_wrapping(i).name()).collect();
        assert_eq!(visited, ["a", "b", "c", "a", "b", "c", "a"]);
        assert_eq!(fs.nth_wrapping(usize::MAX).name(), "a");
    }
}
