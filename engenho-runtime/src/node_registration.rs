//! Node self-registration: a booting node CREATES its own Node when it is
//! absent, and otherwise MERGES only the fields its host is the authority on.
//!
//! ── ★ THE DEFECT (T1.3a) ─────────────────────────────────────────────────
//! Registration used to be one unconditional `Put` of a literal Node on every
//! boot: `spec: {unschedulable: false}`, the five well-known labels, host
//! capacity and a hardcoded `Ready=True`. A `Put` replaces the whole object,
//! so every restart
//!
//!   * UNCORDONED the node — `kubectl cordon` writes `spec.unschedulable`,
//!     and the literal wrote `false` over it;
//!   * dropped every TAINT — `spec.taints` was absent from the literal;
//!   * dropped every LABEL outside the well-known five — nothing evaluated
//!     `nodeSelector` yet, so no pod was stranded, but once the scheduler's
//!     `NodeSelector` filter (T5.7) did, a selector on an operator's label
//!     would have gone from matching to Pending on a reboot. T5.7 waited for
//!     this fix for that reason;
//!   * reasserted `Ready=True` without a reason, on a node that had not yet
//!     heartbeat — readiness stated, never derived.
//!
//! None of it errored. The object read correctly afterwards; it just no longer
//! carried what the operator had put on it.
//!
//! ── ★ THE SHAPE ──────────────────────────────────────────────────────────
//!   * **Absent → create, never overwrite.** One transaction whose only compare
//!     is "the key does not exist" (the compare kube-apiserver uses for every
//!     create) and whose failure branch is empty. A Node that appears between
//!     the read and the write takes the empty branch and is left as it is; the
//!     loop re-reads it and merges.
//!   * **Present → merge the host-owned fields, at the revision read.** The
//!     merge body is rendered from [`HostOwned`], which has a field for the
//!     well-known labels and one for the host's resources and nothing else. An
//!     RFC 7396 merge leaves every key the body does not name untouched, so
//!     `spec` (the cordon, the taints), every other label and every condition
//!     survive by construction. The patch carries the `resourceVersion` it was
//!     computed from; a concurrent write refuses it (`Conflict`) and the loop
//!     re-reads, a bounded number of times.
//!   * **Already current → no write.** Re-registering an unchanged host
//!     consumes no revision and wakes no watcher.
//!   * **A fresh Node is `Ready=Unknown`, reason `NodeStatusNeverUpdated`** —
//!     the value [`node_lease::project_ready_condition`] derives for a node
//!     with no lease, because at registration there is none. The kubelet's
//!     first lease renewal moves it to `Ready`.
//!
//! ── ★ TIER-HONEST ────────────────────────────────────────────────────────
//!   * The merge body cannot name `spec`, a foreign label or a condition:
//!     [`HostOwned`] has no field for them and is the only thing a merge is
//!     rendered from — a type, inside this module.
//!   * Create-never-overwrites is the store's replicated `NotExists` compare,
//!     deterministic on every replica.
//!   * Nothing here stops ANOTHER writer from `Put`ting a whole Node: the
//!     kubelet's readiness publish still does, unconditionally (T1.3b).

use std::collections::BTreeMap;

use engenho_controllers::status::resource_version_of;
use engenho_controllers::{Effect, Refusal, node_lease};
use engenho_kubelet::node_info::{kube_arch, kube_os};
use engenho_store::command::{Reason, ResourceCommand, TxnCompare, TxnOp};
use engenho_store::{ResourceKey, ResourceValue, StoreMesh};
use engenho_types::generated_v1_34::core_v1::Node;
use engenho_types::generated_v1_34::types::{
    DaemonEndpoint, NodeAddress, NodeDaemonEndpoints, NodeSpec, NodeStatus, NodeSystemInfo,
    Quantity,
};
use engenho_types::kind::KubeResource;
use engenho_types::meta::ObjectMeta;
use tracing::info;

use crate::error::RuntimeError;

/// How many read → write rounds registration makes before it gives up.
///
/// Each extra round is spent only when a concurrent writer moved the Node
/// between this node's read and its write. On a single node at boot that is
/// the operator racing the boot itself; five consecutive losses means
/// something is rewriting the Node continuously, which a sixth read would not
/// outrun.
const REGISTRATION_ATTEMPTS: u32 = 5;

/// Why this node's own Node object could not be registered.
#[derive(Debug, thiserror::Error)]
pub enum NodeRegistrationError {
    /// The Node, or the merge body, did not serialize. Effectively impossible
    /// for the concrete typed struct; boot fails loudly rather than skipping.
    #[error("could not serialize the registration of Node {node:?}: {source}")]
    Serialize {
        /// The node being registered.
        node: String,
        /// The serializer's error.
        #[source]
        source: serde_json::Error,
    },

    /// The stored Node carries no parseable `metadata.resourceVersion`, so
    /// there is no revision to make the merge conditional on. The store
    /// stamps one on every write; an object without it was not written
    /// through the store, and merging into it blind is the overwrite this
    /// module exists to prevent.
    #[error(
        "Node {node:?} has no parseable metadata.resourceVersion; refusing to merge into it \
         without a compare-and-swap"
    )]
    NoResourceVersion {
        /// The node being registered.
        node: String,
    },

    /// The store refused the registration write for a reason a re-read does
    /// not change.
    #[error("the store refused the registration of Node {node:?}: {refusal}")]
    Refused {
        /// The node being registered.
        node: String,
        /// What the store said.
        refusal: Refusal,
    },

    /// Every round lost its compare-and-swap to a concurrent writer.
    #[error(
        "Node {node:?} changed under every one of {attempts} registration attempts; \
         something is rewriting it continuously"
    )]
    Contended {
        /// The node being registered.
        node: String,
        /// How many rounds were made.
        attempts: u32,
    },
}

/// What registration did to this node's Node object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Registered {
    /// No Node existed; one was created.
    Created,
    /// A Node existed; its host-owned fields were brought up to date and
    /// nothing else was written.
    Merged,
    /// A Node existed and already carried this host's fields. Nothing was
    /// written.
    AlreadyCurrent,
}

/// The fields of a Node its own host is the authority on — and nothing else.
///
/// Two fields: the well-known labels and the host's resources, rendered into
/// both `status.capacity` and `status.allocatable` (no reservations are made,
/// so the two are equal). There is no field for `spec` (the cordon, the
/// taints), for any other label, or for a condition, so a merge rendered from
/// this value cannot write them.
///
/// The resources are LOAD-BEARING: engenho-scheduler's resource-fit predicate
/// uses a zero-on-absent allocatable policy (an un-sized node fits NO pod
/// that requests cpu/memory), so a Node without them leaves every pod that
/// declares a request Pending forever. They are the host's measured
/// logical-CPU count and total memory.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HostOwned {
    labels: BTreeMap<String, String>,
    resources: BTreeMap<String, Quantity>,
    endpoint: Option<KubeletEndpointFields>,
    node_info: Option<NodeSystemInfo>,
}

/// Where this node's kubelet can be reached: `status.addresses` and
/// `status.daemonEndpoints.kubeletEndpoint.Port`.
///
/// An apiserver on ANOTHER node reaches this node's pods (exec, attach,
/// port-forward, logs) only through these two fields; a Node without them
/// answers every such request with 503 "publishes no kubelet address". They
/// are derived from the address the kubelet listener actually bound, never
/// declared separately, so the published endpoint cannot name a port nothing
/// listens on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KubeletEndpointFields {
    addresses: Vec<(NodeAddressType, String)>,
    port: u16,
}

/// The `status.addresses[].type` values this node publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeAddressType {
    /// An address other nodes route to.
    InternalIp,
    /// The node's name, resolvable where the fleet's names are.
    Hostname,
}

impl NodeAddressType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InternalIp => "InternalIP",
            Self::Hostname => "Hostname",
        }
    }
}

impl KubeletEndpointFields {
    /// The endpoint of a kubelet bound at `bound` on node `node_name`, with
    /// `advertise_address` (`runtime.advertise_address`) as the routable
    /// address when the bind names none.
    ///
    /// * A bind on a specific IP publishes that IP: it is the only address
    ///   the listener answers on. Loopback included — that is the truth, and
    ///   it is what lets several nodes on one host reach each other.
    /// * A bind on every interface (`0.0.0.0`, `::`) publishes the advertised
    ///   host when it is an IP literal, and no InternalIP otherwise: an
    ///   unspecified address is not one anybody can dial.
    /// * `Hostname` is always the node name, as upstream's kubelet publishes.
    #[must_use]
    pub(crate) fn derive(
        node_name: &str,
        bound: std::net::SocketAddr,
        advertise_address: &str,
    ) -> Self {
        let internal = if bound.ip().is_unspecified() {
            crate::runtime::advertised_ip(advertise_address)
        } else {
            Some(bound.ip())
        };
        let mut addresses = Vec::with_capacity(2);
        if let Some(ip) = internal {
            addresses.push((NodeAddressType::InternalIp, ip.to_string()));
        }
        if !node_name.is_empty() {
            addresses.push((NodeAddressType::Hostname, node_name.to_owned()));
        }
        Self {
            addresses,
            port: bound.port(),
        }
    }

    fn typed_addresses(&self) -> Vec<NodeAddress> {
        self.addresses
            .iter()
            .map(|(ty, address)| NodeAddress {
                address: address.clone(),
                r#type: ty.as_str().to_owned(),
            })
            .collect()
    }

    fn typed_endpoints(&self) -> NodeDaemonEndpoints {
        NodeDaemonEndpoints {
            kubelet_endpoint: Some(DaemonEndpoint {
                port: i32::from(self.port),
            }),
        }
    }

    fn is_current_on(&self, status: Option<&ResourceValue>) -> bool {
        let want = serde_json::to_value(self.typed_addresses()).ok();
        let addresses_current = status.and_then(|s| s.get("addresses")) == want.as_ref();
        let port_current = status
            .and_then(|s| s.pointer("/daemonEndpoints/kubeletEndpoint/Port"))
            .and_then(serde_json::Value::as_u64)
            == Some(u64::from(self.port));
        addresses_current && port_current
    }
}

impl HostOwned {
    /// This host's fields for a Node named `node_name`, measured now.
    pub(crate) fn measured(node_name: &str) -> Self {
        let (cpu, memory) = crate::runtime::host_capacity();
        Self::of_host(node_name, cpu, memory)
    }

    pub(crate) fn advertising(
        mut self,
        caps: Option<&engenho_scheduler::NodeCapabilities>,
    ) -> Self {
        if let Some(caps) = caps {
            self.labels.extend(caps.runtime_labels());
        }
        self
    }

    pub(crate) fn reporting(mut self, node_info: NodeSystemInfo) -> Self {
        self.node_info = Some(node_info);
        self
    }

    /// These fields plus where the kubelet listens.
    pub(crate) fn reachable_at(mut self, endpoint: KubeletEndpointFields) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// The fields for a Node named `node_name` on a host with `cpu` logical
    /// CPUs and `memory` total memory (both as Kubernetes quantities).
    pub(crate) fn of_host(node_name: &str, cpu: String, memory: String) -> Self {
        Self {
            labels: well_known_node_labels(node_name),
            resources: BTreeMap::from([
                ("cpu".to_owned(), Quantity(cpu)),
                ("memory".to_owned(), Quantity(memory)),
            ]),
            endpoint: None,
            node_info: None,
        }
    }

    /// A typed Node carrying exactly these fields: no name, no `spec`, no
    /// conditions. Serialized, every absent part is omitted — which is what
    /// makes it a merge body that names nothing else.
    fn fields(&self) -> Node {
        Node {
            metadata: ObjectMeta {
                labels: self.labels.clone(),
                ..ObjectMeta::default()
            },
            spec: None,
            status: Some(NodeStatus {
                capacity: self.resources.clone(),
                allocatable: self.resources.clone(),
                addresses: self
                    .endpoint
                    .as_ref()
                    .map(KubeletEndpointFields::typed_addresses)
                    .unwrap_or_default(),
                daemon_endpoints: self
                    .endpoint
                    .as_ref()
                    .map(KubeletEndpointFields::typed_endpoints),
                node_info: self.node_info.clone(),
                ..NodeStatus::default()
            }),
        }
    }

    /// The RFC 7396 merge body for an existing Node.
    fn merge_patch(&self, node: &str) -> Result<ResourceValue, NodeRegistrationError> {
        serde_json::to_value(self.fields()).map_err(|source| NodeRegistrationError::Serialize {
            node: node.to_owned(),
            source,
        })
    }

    /// The whole Node to create when none exists: these fields, a schedulable
    /// `spec`, the readiness derived for a node that has never heartbeat, a
    /// `creationTimestamp` and the type meta every stored Node has carried.
    fn fresh_node(&self, node: &str, now: &str) -> Result<ResourceValue, NodeRegistrationError> {
        let mut typed = self.fields();
        node.clone_into(&mut typed.metadata.name);
        typed.spec = Some(NodeSpec {
            unschedulable: Some(false),
            ..NodeSpec::default()
        });
        let mut value =
            serde_json::to_value(typed).map_err(|source| NodeRegistrationError::Serialize {
                node: node.to_owned(),
                source,
            })?;
        // No lease exists yet: this process has not heartbeat. The projection
        // is the one derivation of Ready, so the stored value is what it says
        // for exactly that input — Unknown, NodeStatusNeverUpdated.
        node_lease::project_ready_condition(&mut value, None, now);
        if let Some(obj) = value.as_object_mut() {
            // Node is in the core group, whose apiVersion is the bare version.
            obj.insert("apiVersion".to_owned(), Node::GVK.version.into());
            obj.insert("kind".to_owned(), Node::GVK.kind.into());
        }
        crate::runtime::stamp_creation_timestamp_value(&mut value);
        Ok(value)
    }

    /// Whether `live` already carries every one of these fields.
    fn is_current_on(&self, live: &ResourceValue) -> bool {
        let labels = live.get("metadata").and_then(|m| m.get("labels"));
        let labels_current = self.labels.iter().all(|(k, v)| {
            labels.and_then(|l| l.get(k)).and_then(|x| x.as_str()) == Some(v.as_str())
        });
        let status = live.get("status");
        let resources_current = ["capacity", "allocatable"].iter().all(|field| {
            let map = status.and_then(|s| s.get(*field));
            self.resources.iter().all(|(k, q)| {
                map.and_then(|m| m.get(k)).and_then(|x| x.as_str()) == Some(q.0.as_str())
            })
        });
        let endpoint_current = self
            .endpoint
            .as_ref()
            .is_none_or(|e| e.is_current_on(status));
        let node_info_current = self.node_info.as_ref().is_none_or(|info| {
            serde_json::to_value(info).ok().as_ref() == status.and_then(|s| s.get("nodeInfo"))
        });
        labels_current && resources_current && endpoint_current && node_info_current
    }
}

/// The store key of a Node.
pub(crate) fn node_key(node: &str) -> ResourceKey {
    ResourceKey::cluster_scoped(Node::GVK.group, Node::GVK.version, Node::GVK.kind, node)
}

/// Register this node: create its Node if absent, otherwise merge the fields
/// in `host` into it at the revision read. Never writes `spec`, a label
/// outside `host`, or a condition of an existing Node.
///
/// # Errors
///
/// [`RuntimeError::Store`] on a transport failure; [`RuntimeError::NodeRegistration`]
/// when the store refuses the write, the stored Node has no revision to CAS
/// against, or every round loses its CAS.
pub(crate) async fn register_node(
    store: &StoreMesh,
    node: &str,
    host: &HostOwned,
) -> Result<Registered, RuntimeError> {
    let key = node_key(node);
    for _ in 0..REGISTRATION_ATTEMPTS {
        let Some(live) = store.get(&key).await else {
            let body = host.fresh_node(node, &engenho_types::time::now_rfc3339_utc())?;
            let applied = store
                .propose(ResourceCommand::Txn {
                    compares: vec![TxnCompare::NotExists { key: key.clone() }],
                    success: vec![TxnOp::Put {
                        key: key.clone(),
                        value: body,
                    }],
                    failure: Vec::new(),
                    reason: Reason::Operator,
                })
                .await?;
            match Effect::of(applied.op) {
                Effect::Written(_) => {
                    info!(
                        node,
                        "registered node (created, Ready=Unknown until its first heartbeat)"
                    );
                    return Ok(Registered::Created);
                }
                // The failure branch is empty, so a transaction that changed
                // nothing took it: the Node appeared after the read. Re-read
                // and merge into it rather than replace it.
                Effect::Unchanged => continue,
                Effect::Rejected(refusal) => {
                    return Err(refused(node, refusal));
                }
            }
        };

        if host.is_current_on(&live) {
            info!(node, "registered node (already current, nothing written)");
            return Ok(Registered::AlreadyCurrent);
        }
        let Some(expected) = resource_version_of(&live) else {
            return Err(NodeRegistrationError::NoResourceVersion {
                node: node.to_owned(),
            }
            .into());
        };
        let applied = store
            .propose(ResourceCommand::patch_cas(
                key.clone(),
                host.merge_patch(node)?,
                Some(expected),
                Reason::Operator,
            ))
            .await?;
        match Effect::of(applied.op) {
            Effect::Written(_) => {
                info!(node, "registered node (merged host-owned fields)");
                return Ok(Registered::Merged);
            }
            // Moved (or deleted) since the read: re-read and decide again.
            Effect::Rejected(Refusal::Conflict) => {}
            Effect::Rejected(refusal) => return Err(refused(node, refusal)),
            // The store accepted the merge and it changed nothing: the fields
            // were current by the time it applied.
            Effect::Unchanged => return Ok(Registered::AlreadyCurrent),
        }
    }
    Err(NodeRegistrationError::Contended {
        node: node.to_owned(),
        attempts: REGISTRATION_ATTEMPTS,
    }
    .into())
}

fn refused(node: &str, refusal: Refusal) -> RuntimeError {
    NodeRegistrationError::Refused {
        node: node.to_owned(),
        refusal,
    }
    .into()
}

/// The well-known labels upstream's kubelet self-applies at registration.
///
/// Without these, `metadata.labels` is ABSENT — not sparse (measured on the
/// live node 2026-08-30: `labels: None`). The scheduler's `NodeSelector`
/// filter (T5.7) is an exact-match AND of `spec.nodeSelector` over that map,
/// so a selector naming any of these keys fails on a node without them, and
/// the pod stays Pending (`PodScheduled=False`) until a node carrying the
/// label appears. Before T5.7 nothing shipped evaluated `nodeSelector`, so
/// the absent map of 2026-08-30 stranded no pod: every pod was bound whatever
/// it selected. Pinned end to end by
/// `tests/i21_config_read.rs::a_selector_in_go_vocabulary_matches_the_registered_node`.
///
/// The `beta.kubernetes.io/*` pair is deprecated upstream and still
/// emitted, because charts in the wild continue to select on it and a
/// missing label is a silent non-match rather than an error.
#[must_use]
fn well_known_node_labels(node_name: &str) -> BTreeMap<String, String> {
    [
        ("kubernetes.io/hostname", node_name),
        ("kubernetes.io/os", kube_os()),
        ("kubernetes.io/arch", kube_arch()),
        ("beta.kubernetes.io/os", kube_os()),
        ("beta.kubernetes.io/arch", kube_arch()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect()
}

#[cfg(test)]
mod node_label_tests {
    use super::well_known_node_labels;
    use engenho_kubelet::node_info::{kube_arch, kube_os};

    /// The labels speak Go's vocabulary, not Rust's.
    ///
    /// This is the whole point of the mapping: the reference pangea
    /// Postgres selects `kubernetes.io/arch: arm64`, and Rust's
    /// `std::env::consts::ARCH` is `aarch64` on the same machine. Emitting
    /// the Rust spelling yields a label that reads correctly in
    /// `kubectl get node -o yaml` and matches no selector anyone writes.
    #[test]
    fn labels_use_go_vocabulary_not_rust() {
        assert_ne!(
            kube_arch(),
            "aarch64",
            "kubernetes.io/arch must never be the Rust spelling"
        );
        assert_ne!(
            kube_os(),
            "macos",
            "kubernetes.io/os must never be the Rust spelling"
        );
        assert!(
            matches!(kube_arch(), "arm64" | "amd64" | "arm" | "ppc64le" | "s390x"),
            "unexpected GOARCH rendering: {}",
            kube_arch()
        );
        assert!(
            matches!(kube_os(), "linux" | "darwin" | "windows"),
            "unexpected GOOS rendering: {}",
            kube_os()
        );
    }

    /// Every well-known key upstream's kubelet self-applies is present and
    /// non-empty. Under the scheduler's `NodeSelector` filter (T5.7) an
    /// absent key fails every selector naming it; a present-but-partial map
    /// fails the same way, quietly.
    #[test]
    fn every_well_known_label_is_present_and_non_empty() {
        let labels = well_known_node_labels("cid");
        for key in [
            "kubernetes.io/hostname",
            "kubernetes.io/os",
            "kubernetes.io/arch",
            "beta.kubernetes.io/os",
            "beta.kubernetes.io/arch",
        ] {
            let v = labels
                .get(key)
                .unwrap_or_else(|| panic!("missing well-known label {key}"));
            assert!(!v.is_empty(), "{key} is empty, which matches nothing");
        }
        assert_eq!(labels["kubernetes.io/hostname"], "cid");
    }

    /// The deprecated beta aliases must agree with their replacements —
    /// charts in the wild still select on them, and a disagreement would
    /// make the same node match one selector and not its equivalent.
    #[test]
    fn beta_aliases_agree_with_their_replacements() {
        let labels = well_known_node_labels("cid");
        assert_eq!(labels["beta.kubernetes.io/os"], labels["kubernetes.io/os"]);
        assert_eq!(
            labels["beta.kubernetes.io/arch"],
            labels["kubernetes.io/arch"]
        );
    }
}

#[cfg(test)]
mod registration_tests {
    //! T1.3a: a boot creates the Node if absent and otherwise merges only the
    //! host-owned fields. Each test drives the real store through
    //! `register_node`, then reads the Node back.

    use std::sync::Arc;
    use std::time::Duration;

    use engenho_controllers::node_lease::find_ready_condition;
    use engenho_store::command::{Reason, ResourceCommand};
    use engenho_store::{InProcessRouter, ResourceValue, StoreMesh, default_config};
    use serde_json::json;

    use super::{HostOwned, Registered, node_key, register_node};

    const NODE: &str = "ryn";

    async fn boot() -> Arc<StoreMesh> {
        let router = InProcessRouter::new();
        let cfg = default_config("t1-3a").expect("config");
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .expect("store starts"),
        );
        store.initialize_singleton().await.expect("initialize");
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    fn host() -> HostOwned {
        HostOwned::of_host(NODE, "8".to_owned(), "17179869184".to_owned())
    }

    async fn node(store: &StoreMesh) -> ResourceValue {
        store.get(&node_key(NODE)).await.expect("the Node exists")
    }

    /// What `kubectl cordon`, `kubectl taint` and `kubectl label` write: a
    /// merge patch naming only their own field.
    async fn operator_patch(store: &StoreMesh, patch: ResourceValue) {
        let applied = store
            .propose(ResourceCommand::patch(
                node_key(NODE),
                patch,
                Reason::Operator,
            ))
            .await
            .expect("patch proposes");
        assert!(
            super::Effect::of(applied.op).landed(),
            "the operator's own write must land: {:?}",
            applied.op
        );
    }

    /// Register, apply the operator's change, register again (a restart).
    async fn registered_then_restarted_after(patch: ResourceValue) -> ResourceValue {
        let store = boot().await;
        register_node(&store, NODE, &host())
            .await
            .expect("first boot");
        operator_patch(&store, patch).await;
        register_node(&store, NODE, &host()).await.expect("restart");
        node(&store).await
    }

    #[tokio::test]
    async fn a_fresh_node_registers_ready_unknown_never_updated() {
        let store = boot().await;
        let outcome = register_node(&store, NODE, &host())
            .await
            .expect("register");
        assert_eq!(outcome, Registered::Created);

        let n = node(&store).await;
        let ready = find_ready_condition(&n).expect("a fresh Node carries a Ready condition");
        assert_eq!(
            ready["status"], "Unknown",
            "a node that has never heartbeat is not Ready: {n}"
        );
        assert_eq!(ready["reason"], "NodeStatusNeverUpdated", "{n}");
        let readies = n["status"]["conditions"]
            .as_array()
            .expect("conditions")
            .iter()
            .filter(|c| c["type"] == "Ready")
            .count();
        assert_eq!(readies, 1, "exactly one Ready condition: {n}");

        // The rest of what a created Node carries.
        assert_eq!(n["metadata"]["labels"]["kubernetes.io/hostname"], NODE);
        assert_eq!(n["status"]["capacity"]["cpu"], "8");
        assert_eq!(n["status"]["allocatable"]["memory"], "17179869184");
        assert_eq!(
            n["spec"]["unschedulable"], false,
            "a fresh node is not cordoned"
        );
        assert_eq!(n["kind"], "Node");
        assert_eq!(n["apiVersion"], "v1");
        assert!(
            n["metadata"]["creationTimestamp"].is_string(),
            "a created Node carries a creationTimestamp: {n}"
        );
    }

    #[tokio::test]
    async fn a_cordon_survives_a_re_registration() {
        let n = registered_then_restarted_after(json!({"spec": {"unschedulable": true}})).await;
        assert_eq!(
            n["spec"]["unschedulable"], true,
            "a restart must not uncordon the node: {n}"
        );
    }

    #[tokio::test]
    async fn a_custom_taint_survives_a_re_registration() {
        let taint = json!({"key": "gpu", "value": "true", "effect": "NoSchedule"});
        let n = registered_then_restarted_after(json!({"spec": {"taints": [taint.clone()]}})).await;
        assert_eq!(
            n["spec"]["taints"],
            json!([taint]),
            "a restart must not drop the node's taints: {n}"
        );
    }

    #[tokio::test]
    async fn a_node_advertises_the_runtime_it_runs_and_a_restart_on_another_backend_overwrites_it()
    {
        use engenho_scheduler::{NodeCapabilities, Runtime};
        let store = boot().await;
        let native = NodeCapabilities::running([Runtime::Native]);
        let oci = NodeCapabilities::running([Runtime::Oci]);

        register_node(&store, NODE, &host().advertising(Some(&native)))
            .await
            .expect("first boot");
        assert_eq!(NodeCapabilities::observe(&node(&store).await), native);

        register_node(&store, NODE, &host().advertising(Some(&oci)))
            .await
            .expect("restart on an OCI backend");
        assert_eq!(
            NodeCapabilities::observe(&node(&store).await),
            oci,
            "a runtime the node no longer runs is still advertised"
        );
    }

    #[tokio::test]
    async fn a_custom_label_survives_a_re_registration() {
        let n =
            registered_then_restarted_after(json!({"metadata": {"labels": {"team": "storage"}}}))
                .await;
        assert_eq!(
            n["metadata"]["labels"]["team"], "storage",
            "a restart must not drop an operator's label: {n}"
        );
        assert_eq!(
            n["metadata"]["labels"]["kubernetes.io/hostname"], NODE,
            "the well-known labels are still there beside it: {n}"
        );
    }

    #[tokio::test]
    async fn a_published_ready_condition_survives_a_re_registration() {
        // What the kubelet publishes once it has heartbeat. Registration owns
        // no condition, so a restart neither resets it to Unknown nor
        // replaces it with a reasonless literal.
        let ready = json!({"type": "Ready", "status": "True", "reason": "KubeletReady",
                           "lastTransitionTime": "2026-09-01T00:00:00Z"});
        let n = registered_then_restarted_after(json!({"status": {"conditions": [ready.clone()]}}))
            .await;
        assert_eq!(
            find_ready_condition(&n),
            Some(&ready),
            "registration must not touch a condition: {n}"
        );
    }

    #[tokio::test]
    async fn a_re_registration_refreshes_the_host_owned_fields() {
        // The merge half: create-if-absent alone would leave a Node written by
        // an older build — the Rust spelling of the arch label, a smaller
        // machine's capacity — wrong forever.
        let store = boot().await;
        register_node(&store, NODE, &host())
            .await
            .expect("first boot");
        operator_patch(
            &store,
            json!({
                "metadata": {"labels": {"kubernetes.io/arch": "aarch64"}},
                "status": {"capacity": {"cpu": "1", "pods": "110"},
                           "allocatable": {"cpu": "1"}}
            }),
        )
        .await;
        let outcome = register_node(&store, NODE, &host()).await.expect("restart");
        assert_eq!(outcome, Registered::Merged);

        let n = node(&store).await;
        assert_eq!(
            n["metadata"]["labels"]["kubernetes.io/arch"],
            engenho_kubelet::node_info::kube_arch(),
            "{n}"
        );
        assert_eq!(n["status"]["capacity"]["cpu"], "8", "{n}");
        assert_eq!(n["status"]["allocatable"]["cpu"], "8", "{n}");
        // A resource key the host does not report is not the host's to drop.
        assert_eq!(n["status"]["capacity"]["pods"], "110", "{n}");
    }

    #[tokio::test]
    async fn re_registering_an_unchanged_host_writes_nothing() {
        let store = boot().await;
        register_node(&store, NODE, &host())
            .await
            .expect("first boot");
        let before = node(&store).await["metadata"]["resourceVersion"].clone();
        let outcome = register_node(&store, NODE, &host()).await.expect("restart");
        assert_eq!(outcome, Registered::AlreadyCurrent);
        assert_eq!(
            node(&store).await["metadata"]["resourceVersion"],
            before,
            "an unchanged host must not consume a revision or wake a watcher"
        );
    }

    #[tokio::test]
    async fn the_bound_kubelet_endpoint_is_published_and_a_rebind_moves_it() {
        use super::KubeletEndpointFields;
        let store = boot().await;
        register_node(&store, NODE, &host()).await.expect("boot");
        let bound: std::net::SocketAddr = "10.0.0.7:10251".parse().expect("addr");
        let reachable = host().reachable_at(KubeletEndpointFields::derive(NODE, bound, ""));
        assert_eq!(
            register_node(&store, NODE, &reachable)
                .await
                .expect("publish"),
            Registered::Merged
        );
        let n = node(&store).await;
        assert_eq!(
            n["status"]["addresses"],
            json!([{"address": "10.0.0.7", "type": "InternalIP"},
                   {"address": NODE, "type": "Hostname"}]),
            "{n}"
        );
        assert_eq!(
            n["status"]["daemonEndpoints"]["kubeletEndpoint"]["Port"],
            10251
        );
        assert_eq!(
            engenho_apiserver::kubelet_authority_of_node(&n).as_deref(),
            Some("10.0.0.7:10251"),
            "the apiserver's resolver reads what registration wrote"
        );
        assert_eq!(
            register_node(&store, NODE, &reachable)
                .await
                .expect("again"),
            Registered::AlreadyCurrent
        );
        let moved: std::net::SocketAddr = "10.0.0.8:10250".parse().expect("addr");
        register_node(
            &store,
            NODE,
            &host().reachable_at(KubeletEndpointFields::derive(NODE, moved, "")),
        )
        .await
        .expect("rebind");
        let n = node(&store).await;
        assert_eq!(
            engenho_apiserver::kubelet_authority_of_node(&n).as_deref(),
            Some("10.0.0.8:10250")
        );
        assert_eq!(n["status"]["addresses"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn an_unspecified_bind_publishes_the_advertised_ip_or_no_internal_ip() {
        use super::KubeletEndpointFields;
        let any: std::net::SocketAddr = "0.0.0.0:10250".parse().expect("addr");
        let body = |advertise: &str| {
            host()
                .reachable_at(KubeletEndpointFields::derive(NODE, any, advertise))
                .merge_patch(NODE)
                .expect("serializes")["status"]["addresses"]
                .clone()
        };
        assert_eq!(
            body("100.64.0.9:6443"),
            json!([{"address": "100.64.0.9", "type": "InternalIP"},
                   {"address": NODE, "type": "Hostname"}])
        );
        assert_eq!(
            body("rio.pleme.internal"),
            json!([{"address": NODE, "type": "Hostname"}]),
            "a name is not an InternalIP; 0.0.0.0 is not an address"
        );
        let v6: std::net::SocketAddr = "[fd00::7]:10250".parse().expect("addr");
        let n = host()
            .reachable_at(KubeletEndpointFields::derive(NODE, v6, ""))
            .merge_patch(NODE)
            .expect("serializes");
        assert_eq!(
            engenho_apiserver::kubelet_authority_of_node(&n).as_deref(),
            Some("[fd00::7]:10250")
        );
    }

    #[test]
    fn the_merge_body_names_only_host_owned_fields() {
        let patch = host().merge_patch(NODE).expect("serializes");
        let top: Vec<&str> = patch
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            top,
            ["metadata", "status"],
            "no spec, no type meta: {patch}"
        );
        let meta: Vec<&str> = patch["metadata"]
            .as_object()
            .expect("metadata")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(meta, ["labels"], "{patch}");
        let status: Vec<&str> = patch["status"]
            .as_object()
            .expect("status")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            status,
            ["allocatable", "capacity"],
            "no conditions: {patch}"
        );
    }
}
