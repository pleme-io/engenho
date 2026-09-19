//! The object predicates: each one a pure function over listed objects that
//! calls the shipped rule it counts for.

use std::collections::BTreeMap;

use engenho_apiserver::{defaulting, schema_validation, validation};
use engenho_controllers::node_lease::LEASE_NAMESPACE;
use engenho_controllers::{
    CrdController, CrdHandlerSpec, NormalizedTemplate, ObjectMeta as _, REPLICAS,
    current_replicaset, is_owned_by,
};
use engenho_scheduler::{
    FilterPlugin, Headroom, NodeLedger, ObservedNode, holds_capacity, pod_requests,
};
use engenho_store::{ImageInconsistency, ImageTripwire, ResourceKey};
use serde_json::Value;

use super::{Builtin, Finding, Gvk, Match, Subject, text_at};

/// T1.5: every node [`NodeLedger::headroom`] reports overcommitted, from the
/// same [`NodeLedger::seed`] the scheduler opens its books with each tick.
pub(super) fn node_overcommitted(nodes: &[Value], pods: &[Value]) -> Vec<Match> {
    let gvk = Builtin::Node.gvk();
    NodeLedger::seed(nodes, pods)
        .headroom()
        .filter(Headroom::is_overcommitted)
        .map(|row| Match {
            subject: Subject::Object {
                gvk: gvk.clone(),
                namespace: None,
                name: Some(row.node.to_owned()),
            },
            finding: Finding::Overcommitted {
                cpu_milli: row.cpu_milli,
                mem_milli: row.mem_milli,
            },
        })
        .collect()
}

/// T5.7: every bound pod that still [`holds_capacity`], judged by EVERY
/// [`FilterPlugin`] (not only the first to refuse) against its own node.
///
/// The node is projected from its Lease by [`ObservedNode::project`], as the
/// scheduler does. The ledger charges every OTHER pod on the node: the pod is
/// already on it, so charging it too would refuse a pod that fits exactly —
/// the question is whether the filter would place it there now, not whether
/// it fits a second time.
///
/// A pod observed `Succeeded` or `Failed` is skipped: it is never placed
/// again, so no filter decides anything about it. A pod bound to a node the
/// list lacks is reported as such, not judged.
pub(super) fn bound_pod_rejected(
    nodes: &[Value],
    pods: &[Value],
    leases: &[Value],
    now: &str,
) -> Vec<Match> {
    let pod_gvk = Builtin::Pod.gvk();
    let mut found = Vec::new();
    for (i, pod) in pods.iter().enumerate() {
        let Some(node_name) = text_at(pod, "/spec/nodeName").filter(|n| !n.is_empty()) else {
            continue;
        };
        if !holds_capacity(pod).holds() {
            continue;
        }
        let subject = Subject::object(&pod_gvk, pod);
        let Some(node) = nodes
            .iter()
            .find(|n| text_at(n, "/metadata/name") == Some(node_name))
        else {
            found.push(Match {
                subject,
                finding: Finding::NodeNotObserved {
                    node: node_name.to_owned(),
                },
            });
            continue;
        };
        let lease = leases.iter().find(|l| {
            text_at(l, "/metadata/namespace") == Some(LEASE_NAMESPACE)
                && text_at(l, "/metadata/name") == Some(node_name)
        });
        let observed = ObservedNode::project(node.clone(), lease, now);
        let others = pods
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, p)| p);
        let ledger = NodeLedger::seed(std::iter::once(node), others);
        let req = pod_requests(pod);
        for plugin in FilterPlugin::ALL {
            if let Err(rejection) = plugin.check(pod, &req, &observed, &ledger) {
                found.push(Match {
                    subject: subject.clone(),
                    finding: Finding::Rejected(rejection),
                });
            }
        }
    }
    found
}

/// T4.10: every Deployment for which [`current_replicaset`] finds no owned
/// `ReplicaSet` — the one case in which the controller creates a new one.
///
/// Skipped exactly where the controller does nothing: a Deployment with no
/// `metadata.uid` (the owned-children skeleton skips it), one whose
/// `spec.replicas` cannot be read (it fails with an Event, creating
/// nothing), and one with no template.
pub(super) fn deployment_would_roll(deployments: &[Value], replicasets: &[Value]) -> Vec<Match> {
    let gvk = Builtin::Deployment.gvk();
    let mut found = Vec::new();
    for d in deployments {
        let Some(uid) = d.uid() else { continue };
        if REPLICAS.read(d).is_err() {
            continue;
        }
        let Some(template) = NormalizedTemplate::of_spec_template(d) else {
            continue;
        };
        let namespace = d.namespace();
        let owned: Vec<(ResourceKey, Value)> = replicasets
            .iter()
            .filter(|rs| rs.namespace() == namespace && is_owned_by(rs, uid))
            .map(|rs| {
                (
                    ResourceKey::namespaced(
                        "apps",
                        "v1",
                        "ReplicaSet",
                        namespace.unwrap_or_default(),
                        rs.name().unwrap_or_default(),
                    ),
                    rs.clone(),
                )
            })
            .collect();
        if current_replicaset(&template, &owned).is_none() {
            found.push(Match {
                subject: Subject::object(&gvk, d),
                finding: Finding::NoReplicaSetRunsTemplate,
            });
        }
    }
    found
}

/// Each served CRD version's structural schema, unwrapped exactly as the
/// apiserver's handler gets it: [`CrdController::extract_entries`], then
/// [`CrdHandlerSpec::from`]. A schema that is not an object carries no
/// constraint, so the handler keeps none (`with_crd_schema`) and neither
/// does this.
pub(super) fn crd_schemas(crds: &[Value]) -> BTreeMap<Gvk, Value> {
    crds.iter()
        .flat_map(CrdController::extract_entries)
        .map(|entry| CrdHandlerSpec::from(&entry))
        .filter(|spec| spec.schema.is_object())
        .map(|spec| {
            (
                Gvk::new(&spec.group, &spec.version, &spec.kind),
                spec.schema,
            )
        })
        .collect()
}

/// T4.4 / T4.5 (i): every finding the create path's stages would raise
/// against `objects` of `gvk`, in the create path's order: defaulting, then
/// validation, then the CRD schema's defaults, then the CRD schema.
pub(super) fn would_reject(gvk: &Gvk, objects: &[Value], schema: Option<&Value>) -> Vec<Match> {
    let mut found = Vec::new();
    for object in objects {
        let mut body = object.clone();
        defaulting::apply(&gvk.group, &gvk.version, &gvk.kind, &mut body);
        for violation in validation::validate(&gvk.group, &gvk.version, &gvk.kind, &body) {
            found.push(Match {
                subject: Subject::object(gvk, object),
                finding: Finding::Invalid(violation),
            });
        }
        if let Some(schema) = schema {
            schema_validation::apply_defaults(schema, &mut body);
            for violation in schema_validation::validate(schema, &body) {
                found.push(Match {
                    subject: Subject::object(gvk, object),
                    finding: Finding::SchemaInvalid(violation),
                });
            }
        }
    }
    found
}

/// Every [`ImageInconsistency`], in the store's declaration order.
///
/// The store's enum has no list of its variants, so this is one, held to it
/// at compile time: [`image_kind_index`] matches the enum exhaustively (a
/// variant the store adds stops it compiling until it has an index), and the
/// `const` block below proves each row sits at its own index, so no kind is
/// listed twice. That a new variant's row is ADDED here is left to whoever
/// gives it an index.
const IMAGE_KINDS: [ImageInconsistency; 5] = [
    ImageInconsistency::SnapshotNewerThanBlob,
    ImageInconsistency::SnapshotUnreadable,
    ImageInconsistency::BlobAheadOfLastApplied,
    ImageInconsistency::ReplayGap,
    ImageInconsistency::AlreadyApplied,
];

/// The position of `kind` in [`IMAGE_KINDS`].
const fn image_kind_index(kind: ImageInconsistency) -> usize {
    match kind {
        ImageInconsistency::SnapshotNewerThanBlob => 0,
        ImageInconsistency::SnapshotUnreadable => 1,
        ImageInconsistency::BlobAheadOfLastApplied => 2,
        ImageInconsistency::ReplayGap => 3,
        ImageInconsistency::AlreadyApplied => 4,
    }
}

const _: () = {
    let mut i = 0;
    while i < IMAGE_KINDS.len() {
        assert!(
            image_kind_index(IMAGE_KINDS[i]) == i,
            "IMAGE_KINDS is out of order"
        );
        i += 1;
    }
};

/// T3.4: each kind of disagreement the boot counted, with its hits.
pub(super) fn image_inconsistent(tripwire: &ImageTripwire) -> Vec<Match> {
    IMAGE_KINDS
        .iter()
        .filter_map(|&kind| {
            let hits = tripwire.count(kind);
            (hits > 0).then_some(Match {
                subject: Subject::StoreImage,
                finding: Finding::ImageInconsistent { kind, hits },
            })
        })
        .collect()
}
