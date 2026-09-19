//! `PvBinderController` — the PersistentVolume / PersistentVolumeClaim
//! binder PLUS a minimal local-path dynamic provisioner.
//!
//! ## What it does (the Storage CNCF brick)
//!
//! Each tick, for every `PersistentVolumeClaim` in `status.phase == Pending`
//! (or unset) it tries, in order:
//!
//!   1. **Static bind** — find an `Available` `PersistentVolume` that
//!      *matches* the claim (capacity ≥ request, accessModes ⊇ requested,
//!      storageClassName equal incl. `""`/nil semantics, and — if the PVC
//!      pre-binds via `spec.volumeName` — exactly that PV). On a match it
//!      **bidirectionally binds**: `PVC.spec.volumeName = PV.name`,
//!      `PVC.status.phase = Bound`, `PV.status.phase = Bound`, and
//!      `PV.spec.claimRef → {namespace,name,uid}` of the PVC.
//!   2. **Dynamic provision** — if no static PV matches AND the PVC's
//!      effective StorageClass uses the local-path provisioner (or is the
//!      cluster default SC), it CREATES a `PersistentVolume` named
//!      `pvc-<uid>` with a node-local `hostPath` source under
//!      `<data_dir>/local-path/pvc-<uid>_<ns>_<name>`, `capacity = request`,
//!      the SC's `reclaimPolicy`, and a `claimRef` pre-pointing at the PVC,
//!      then binds it.
//!
//! ## Identity is the claim's uid
//!
//! A claim's name is reusable; its `metadata.uid` is not. Every volume a
//! claim is given is tied to its uid ([`ClaimUid`]):
//!
//!   * a dynamic PV's name and directory come from [`PvName::for_claim`],
//!     so a claim deleted and recreated under the same name gets a new PV
//!     and a new, empty directory — never the deleted claim's data;
//!   * a PV whose `claimRef` carries a different uid belongs to an earlier
//!     claim of the same name and is never bound to this one;
//!   * the PV is written create-if-absent, so a provision never overwrites
//!     a PV that already holds its name;
//!   * a claim with no usable uid stays Pending with a typed reason
//!     ([`NoVolumeIdentity`]).
//!
//! PVs provisioned before this rule keep their names (`pvc-<ns>-<name>`):
//! nothing renames a bound volume, and a claim whose PV was written but not
//! yet bound finds it again through its `claimRef` uid.
//!
//! ## volumeBindingMode
//!
//! `Immediate` (the default + only mode this brick provisions in) provisions
//! as soon as the PVC is seen. `WaitForFirstConsumer` is **typed-deferred**:
//! the binder leaves such a PVC Pending (it does NOT provision until a Pod
//! references it) and records that in the report — a named follow-up rather
//! than a silent wrong behavior.
//!
//! ## The filesystem seam (Environment-trait discipline)
//!
//! The ONLY host effect this controller performs is creating the local-path
//! data directory at provision time. That sits behind the
//! [`ProvisionerEnv`] trait so the whole binder is unit-testable WITHOUT
//! touching the real filesystem (the [`FakeProvisionerEnv`]). The real
//! [`HostProvisionerEnv`] `mkdir -p`s the dir. The directory is created here
//! at *provision* time (the provisioner's job) so the PV's `hostPath` exists
//! before any kubelet tries to bind-mount it — the clean seam, documented.
//!
//! ## No silent wrong answers
//!
//! Every store write goes through `ResourceCommand::Put` with
//! `Reason::Controller`. A PVC that matches nothing AND can't be dynamically
//! provisioned is left Pending (the K8s behavior) — never fake-Bound. There
//! is no `todo!()` / `unimplemented!()` / `panic!()` anywhere in the path.

mod identity;

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand, TxnCompare, TxnOp},
    resource::ResourceKey,
};
use engenho_types::primitives::quantity::Quantity;
use serde_json::{Value, json};
use std::str::FromStr;
use tracing::debug;

use crate::controller::{Controller, ReconcileOutcome};
use crate::csi_provisioner::{CsiCreateRequest, CsiProvisioner, NoCsiProvisioner, parse_quantity};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::event_recorder::Reason as EventReason;
use crate::reads::{DeclaresReads, Reads, gvk};
use crate::sweep::{ObjectOutcome, Sweep, impl_sweep_event_sink};
use crate::volume_snapshot::{SNAPSHOT_GROUP, SNAPSHOT_VERSION};

pub use identity::{ClaimUid, LocalPathDir, NoVolumeIdentity, PvName};

/// The local-path provisioner identifier. A StorageClass whose
/// `provisioner` is this string (the rancher.io/local-path de-facto
/// standard) OR engenho's own alias triggers dynamic provisioning.
pub const LOCAL_PATH_PROVISIONER: &str = "rancher.io/local-path";
/// engenho's own alias for the same node-local-hostPath provisioner.
pub const ENGENHO_LOCAL_PATH_PROVISIONER: &str = "engenho.io/local-path";

/// The annotation marking a StorageClass as the cluster default.
const DEFAULT_SC_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Upstream's `source.component` for the PV controller, which signs the
/// binder's Events.
const COMPONENT: &str = "persistentvolume-controller";

/// Why a local-path claim with no usable size cannot be provisioned.
///
/// Upstream's apiserver refuses such a claim at create time; engenho's does
/// not, and the binder used to provision it anyway — a local-path PV whose
/// `capacity.storage` was `null` or the unparseable string, bound as if it
/// were real. Now it is an Item failure on the claim: the claim stays
/// Pending and a `ProvisioningFailed` Event on it says why.
const UNUSABLE_REQUEST: &str = "spec.resources.requests.storage is missing or is not a quantity, \
     so the local-path provisioner has no size to give the volume; the claim stays Pending \
     until it names one";

/// The report note for a claim that restores from a snapshot not yet ready.
const SNAPSHOT_NOT_READY: &str = "PVC names a VolumeSnapshot dataSource that is not ready; left \
     Pending rather than provisioned empty";

/// The report note for a `WaitForFirstConsumer` claim.
const WAIT_FOR_FIRST_CONSUMER: &str = "WaitForFirstConsumer PVC left Pending (deferred)";

/// The report note for a claim whose derived PV name is already held by a
/// PV that is not bound to it. Nothing is overwritten and nothing is
/// provisioned into that PV's directory; the claim stays Pending.
const PV_NAME_TAKEN: &str = "a PersistentVolume already holds the name derived from this \
     claim's uid and is not bound to it; nothing was overwritten and the claim stays Pending";

/// One claim, as a PV's `claimRef` names it.
#[derive(Debug, Clone, Copy)]
struct ClaimId<'a> {
    namespace: &'a str,
    name: &'a str,
    uid: &'a ClaimUid,
}

/// Where a PV's `spec.claimRef` points, seen from one claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimRefTo {
    /// No claimRef: free for any claim it matches.
    Nobody,
    /// This claim's namespace and name, with no uid: reserved for the claim
    /// by name, the way an operator pre-binds a PV.
    ThisName,
    /// This claim's namespace, name and uid: already this claim's volume.
    ThisClaim,
    /// Some other claim — including an earlier claim of the same namespace
    /// and name, told apart by its uid.
    Other,
}

/// Everything a sweep reads once and matches each claim against.
struct TickView {
    pvs: Vec<(ResourceKey, Value)>,
    storage_classes: Vec<(ResourceKey, Value)>,
    snapshots: Vec<(ResourceKey, Value)>,
    snapshot_contents: Vec<(ResourceKey, Value)>,
}

/// The host-effect seam — the side-effecting half of the provisioner behind
/// a trait so binding + provisioning is unit-testable WITHOUT a real
/// filesystem. The real [`HostProvisionerEnv`] creates the local-path dir;
/// the [`FakeProvisionerEnv`] records the request + returns success.
pub trait ProvisionerEnv: Send + Sync {
    /// Idempotently ensure the host directory `path` exists (the local-path
    /// PV's `hostPath` backing dir). Created at provision time so the kubelet
    /// can bind-mount it.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error string on mkdir failure — surfaced as a
    /// [`ControllerError`] so provisioning fails loudly (the PVC stays Pending),
    /// never a silent fake-Bound.
    fn ensure_dir(&self, path: &str) -> Result<(), String>;

    /// Hydrate a freshly-provisioned backing dir FROM a snapshot directory.
    ///
    /// Added to this trait rather than to a second env so restore stays the
    /// same seam as provision: a PV whose data came from a snapshot is still
    /// just a local-path PV, and splitting the two would let a cluster
    /// provision without being able to restore.
    ///
    /// # Errors
    ///
    /// A human-readable message. A failed restore must leave the claim
    /// Pending — an EMPTY volume presented as a restored one is the silent
    /// wrong answer this whole controller refuses to give.
    fn restore_tree(&self, src: &str, dst: &str) -> Result<(), String>;
}

/// Production [`ProvisionerEnv`] — `std::fs::create_dir_all` rooted under the
/// configured `data_dir/local-path`.
pub struct HostProvisionerEnv;

impl ProvisionerEnv for HostProvisionerEnv {
    fn ensure_dir(&self, path: &str) -> Result<(), String> {
        std::fs::create_dir_all(path).map_err(|e| format!("mkdir {path}: {e}"))
    }

    /// Delegates to the snapshot controller's copy, so provision-side restore
    /// and snapshot-side capture are ONE implementation. Two copies of a
    /// recursive directory copy would be free to disagree about symlinks,
    /// permissions or partial failure — and only one of them would be tested.
    fn restore_tree(&self, src: &str, dst: &str) -> Result<(), String> {
        crate::volume_snapshot::SnapshotEnv::copy_tree(
            &crate::volume_snapshot::HostSnapshotEnv,
            src,
            dst,
        )
    }
}

/// The PV/PVC binder controller + local-path dynamic provisioner.
pub struct PvBinderController {
    store: Arc<StoreMesh>,
    namespace: Option<String>,
    /// Host data root under which dynamically-provisioned local-path PVs are
    /// backed: each PV's hostPath is
    /// `<local_path_root>/pvc-<uid>_<pvc-ns>_<pvc-name>` ([`PvName::local_path_dir`]).
    local_path_root: String,
    /// The filesystem seam (mockable).
    env: Arc<dyn ProvisionerEnv>,
    /// The CSI provisioning seam. Defaults to [`NoCsiProvisioner`], under
    /// which every CSI StorageClass stays Pending exactly as it did before
    /// this branch existed — so wiring it is opt-in and its absence is not
    /// a behaviour change.
    csi: Arc<dyn CsiProvisioner>,
    /// Per-claim isolation: one claim's failure costs only that claim.
    sweep: Sweep,
}

impl_sweep_event_sink!(PvBinderController);

impl PvBinderController {
    /// New binder with the production [`HostProvisionerEnv`]. `local_path_root`
    /// is typically `<data_dir>/local-path`.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        namespace: Option<String>,
        local_path_root: impl Into<String>,
    ) -> Self {
        Self {
            store,
            namespace,
            local_path_root: local_path_root.into(),
            env: Arc::new(HostProvisionerEnv),
            csi: Arc::new(NoCsiProvisioner),
            sweep: Sweep::new(COMPONENT, EventReason::ProvisioningFailed),
        }
    }

    /// Construct with an explicit [`ProvisionerEnv`] — the unit-test seam so
    /// the provisioner never touches the real filesystem.
    #[must_use]
    pub fn with_env(
        store: Arc<StoreMesh>,
        namespace: Option<String>,
        local_path_root: impl Into<String>,
        env: Arc<dyn ProvisionerEnv>,
    ) -> Self {
        Self {
            store,
            namespace,
            local_path_root: local_path_root.into(),
            env,
            csi: Arc::new(NoCsiProvisioner),
            sweep: Sweep::new(COMPONENT, EventReason::ProvisioningFailed),
        }
    }

    /// Builder: wire the CSI provisioning seam.
    #[must_use]
    pub fn with_csi(mut self, csi: Arc<dyn CsiProvisioner>) -> Self {
        self.csi = csi;
        self
    }

    /// A PVC's `status.phase`, defaulting to `"Pending"` when unset (a freshly
    /// created PVC has no status yet — it is Pending by definition).
    fn pvc_phase(pvc: &Value) -> &str {
        pvc.get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            .unwrap_or("Pending")
    }

    /// A PV's `status.phase`, defaulting to `"Available"` when unset (a fresh
    /// PV with no status is available to bind).
    fn pv_phase(pv: &Value) -> &str {
        pv.get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            .unwrap_or("Available")
    }

    /// The PVC's requested storage as a canonical byte count, or `None` when
    /// absent/unparseable (an unparseable request never matches — surfaced by
    /// leaving the PVC Pending rather than guessing).
    fn pvc_request_bytes(pvc: &Value) -> Option<i128> {
        let s = pvc
            .get("spec")
            .and_then(|sp| sp.get("resources"))
            .and_then(|r| r.get("requests"))
            .and_then(|r| r.get("storage"))
            .and_then(|q| q.as_str())?;
        Quantity::from_str(s).ok()?.milli_value()
    }

    /// A PV's `spec.capacity.storage` as a canonical byte count.
    fn pv_capacity_bytes(pv: &Value) -> Option<i128> {
        let s = pv
            .get("spec")
            .and_then(|sp| sp.get("capacity"))
            .and_then(|c| c.get("storage"))
            .and_then(|q| q.as_str())?;
        Quantity::from_str(s).ok()?.milli_value()
    }

    /// The requested access modes of a PVC (`spec.accessModes`), as a sorted
    /// owned vec. Empty when unset.
    fn access_modes(spec_obj: &Value) -> Vec<String> {
        spec_obj
            .get("spec")
            .and_then(|sp| sp.get("accessModes"))
            .and_then(|a| a.as_array())
            .map(|arr| {
                let mut v: Vec<String> = arr
                    .iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }

    /// A resource's `spec.storageClassName` as `Option<&str>`. `None` (nil) and
    /// `Some("")` are distinct in K8s but bind-compatible only with each other;
    /// this returns the raw value (nil → `None`, `""` → `Some("")`).
    fn storage_class<'a>(spec_obj: &'a Value) -> Option<&'a str> {
        spec_obj
            .get("spec")
            .and_then(|sp| sp.get("storageClassName"))
            .and_then(|s| s.as_str())
    }

    /// K8s storageClassName matching: nil and `""` are treated as equal (both
    /// mean "no class"); otherwise an exact string match.
    fn storage_class_matches(pvc: &Value, pv: &Value) -> bool {
        let norm = |o: Option<&str>| o.unwrap_or("").to_string();
        norm(Self::storage_class(pvc)) == norm(Self::storage_class(pv))
    }

    /// The PVC's `spec.volumeName` (a pre-bind to a named PV), if set + non-empty.
    fn pre_bound_volume(pvc: &Value) -> Option<&str> {
        pvc.get("spec")
            .and_then(|sp| sp.get("volumeName"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Where `pv`'s `spec.claimRef` points, seen from `claim`.
    ///
    /// The namespace and name must both match for the claimRef to be this
    /// claim's. When it also carries a uid, that uid decides: a different
    /// one is an earlier claim that happened to have the same name
    /// (upstream's `IsVolumeBoundToClaim`). An empty uid is no uid.
    fn claim_ref_to(pv: &Value, claim: &ClaimId<'_>) -> ClaimRefTo {
        let Some(cr) = pv
            .get("spec")
            .and_then(|s| s.get("claimRef"))
            .filter(|cr| !cr.is_null())
        else {
            return ClaimRefTo::Nobody;
        };
        let field = |k: &str| cr.get(k).and_then(Value::as_str).unwrap_or("");
        if field("namespace") != claim.namespace || field("name") != claim.name {
            return ClaimRefTo::Other;
        }
        match cr.get("uid") {
            None | Some(Value::Null) => ClaimRefTo::ThisName,
            Some(Value::String(uid)) if uid.is_empty() => ClaimRefTo::ThisName,
            Some(uid) if claim.uid.is(uid) => ClaimRefTo::ThisClaim,
            Some(_) => ClaimRefTo::Other,
        }
    }

    /// A PV is *available to bind to this PVC* iff it's `Available` AND (if it
    /// carries a `spec.claimRef`) that claimRef already points at THIS PVC
    /// (a pre-provisioned PV reserved for the claim). A PV claimRef'd to a
    /// DIFFERENT claim is not available, and neither is one claimRef'd to an
    /// earlier claim of the same namespace and name: the uid tells them apart.
    fn pv_claimref_compatible(pv: &Value, claim: &ClaimId<'_>) -> bool {
        match Self::claim_ref_to(pv, claim) {
            ClaimRefTo::Nobody | ClaimRefTo::ThisName | ClaimRefTo::ThisClaim => true,
            ClaimRefTo::Other => false,
        }
    }

    /// Is `pv` big enough for `pvc`'s request? A claim with no parseable
    /// request asks for nothing in particular (a CSI driver chose the
    /// size); a PV with no parseable capacity satisfies no request.
    fn holds_request(pv: &Value, pvc: &Value) -> bool {
        Self::pvc_request_bytes(pvc)
            .is_none_or(|req| Self::pv_capacity_bytes(pv).is_some_and(|cap| cap >= req))
    }

    /// Does `pv` satisfy `pvc`'s static-bind requirements? Capacity ≥ request,
    /// accessModes ⊇ requested, storageClassName equal, claimRef compatible.
    /// (volumeName pre-bind is checked by the caller — it narrows the candidate
    /// set to exactly one PV before this predicate runs.)
    fn pv_matches_pvc(pv: &Value, pvc: &Value, claim: &ClaimId<'_>) -> bool {
        // Capacity: PV must offer ≥ the request. A PVC with no parseable
        // request matches nothing (left Pending).
        let Some(req) = Self::pvc_request_bytes(pvc) else {
            return false;
        };
        let Some(cap) = Self::pv_capacity_bytes(pv) else {
            return false;
        };
        if cap < req {
            return false;
        }
        // Access modes: the PV must support every mode the PVC requests.
        let want = Self::access_modes(pvc);
        let have = Self::access_modes(pv);
        if !want.iter().all(|m| have.contains(m)) {
            return false;
        }
        // StorageClass equality (nil/"" normalized).
        if !Self::storage_class_matches(pvc, pv) {
            return false;
        }
        // claimRef must be unset or already point at this PVC.
        Self::pv_claimref_compatible(pv, claim)
    }

    /// The PVC's `volumeBindingMode` — resolved from its effective
    /// StorageClass. Returns `true` for `WaitForFirstConsumer` (provisioning
    /// deferred), `false` (default) for `Immediate` / absent.
    fn is_wait_for_first_consumer(sc: Option<&Value>) -> bool {
        sc.and_then(|c| c.get("volumeBindingMode"))
            .and_then(|m| m.as_str())
            .map(|m| m == "WaitForFirstConsumer")
            .unwrap_or(false)
    }

    /// Find the StorageClass object for a PVC: its explicit
    /// `spec.storageClassName`, else the cluster default SC (annotation).
    /// Returns the SC `Value` if one applies, `None` when the PVC has an
    /// empty/nil class and there is no default SC.
    fn effective_storage_class<'a>(
        pvc: &Value,
        storage_classes: &'a [(ResourceKey, Value)],
    ) -> Option<&'a Value> {
        match Self::storage_class(pvc) {
            Some(name) if !name.is_empty() => storage_classes
                .iter()
                .find(|(k, _)| k.name == name)
                .map(|(_, v)| v),
            // nil or "" → cluster default SC (if any).
            _ => storage_classes
                .iter()
                .find(|(_, v)| is_default_sc(v))
                .map(|(_, v)| v),
        }
    }

    /// Build the bound PVC value: stamp `spec.volumeName` + `status.phase=Bound`.
    fn bind_pvc(mut pvc: Value, pv_name: &str) -> Value {
        if let Some(spec) = pvc.get_mut("spec").and_then(Value::as_object_mut) {
            spec.insert("volumeName".into(), json!(pv_name));
        } else if let Some(obj) = pvc.as_object_mut() {
            obj.insert("spec".into(), json!({ "volumeName": pv_name }));
        }
        set_phase(&mut pvc, "Bound");
        pvc
    }

    /// The `spec.claimRef` naming `claim` — always with its uid, so the PV
    /// can never be mistaken for a later claim of the same name.
    fn claim_ref(claim: &ClaimId<'_>) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "namespace": claim.namespace,
            "name": claim.name,
            "uid": claim.uid.as_str(),
        })
    }

    /// Build the bound PV value: stamp `spec.claimRef` → the PVC +
    /// `status.phase=Bound`.
    fn bind_pv(mut pv: Value, claim: &ClaimId<'_>) -> Value {
        let claim_ref = Self::claim_ref(claim);
        if let Some(spec) = pv.get_mut("spec").and_then(Value::as_object_mut) {
            spec.insert("claimRef".into(), claim_ref);
        } else if let Some(obj) = pv.as_object_mut() {
            obj.insert("spec".into(), json!({ "claimRef": claim_ref }));
        }
        set_phase(&mut pv, "Bound");
        pv
    }

    /// Build a dynamically-provisioned local-path PV body for `pvc`, named
    /// `pv_name` and backed by `host_path` (both from [`PvName`]); capacity =
    /// the request; reclaimPolicy from the SC (default `Delete`); accessModes
    /// copied from the PVC; storageClassName from the PVC's effective class;
    /// claimRef pre-pointing at the PVC.
    fn build_dynamic_pv(
        pvc: &Value,
        claim: &ClaimId<'_>,
        pv_name: &str,
        host_path: &str,
        sc: &Value,
    ) -> Value {
        let capacity = pvc
            .get("spec")
            .and_then(|s| s.get("resources"))
            .and_then(|r| r.get("requests"))
            .and_then(|r| r.get("storage"))
            .cloned()
            .unwrap_or(Value::Null);
        let reclaim = sc
            .get("reclaimPolicy")
            .and_then(|r| r.as_str())
            .unwrap_or("Delete");
        let access_modes = pvc
            .get("spec")
            .and_then(|s| s.get("accessModes"))
            .cloned()
            .unwrap_or_else(|| json!(["ReadWriteOnce"]));
        let sc_name = sc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "name": pv_name,
                "annotations": {
                    "pv.kubernetes.io/provisioned-by": ENGENHO_LOCAL_PATH_PROVISIONER
                }
            },
            "spec": {
                "capacity": { "storage": capacity },
                "accessModes": access_modes,
                "persistentVolumeReclaimPolicy": reclaim,
                "storageClassName": sc_name,
                "hostPath": { "path": host_path },
                "claimRef": Self::claim_ref(claim)
            },
            "status": { "phase": "Bound" }
        })
    }

    /// The VolumeSnapshot name a PVC restores from, if any.
    ///
    /// Reads `spec.dataSource` (and accepts `spec.dataSourceRef`, which
    /// upstream added as the general form). The apiGroup is CHECKED: a
    /// dataSource naming a PVC clone or some other kind is not a snapshot and
    /// must not be silently treated as one.
    fn snapshot_data_source(pvc: &Value) -> Option<String> {
        let spec = pvc.get("spec")?;
        let ds = spec
            .get("dataSource")
            .or_else(|| spec.get("dataSourceRef"))?;
        if ds.get("kind").and_then(Value::as_str)? != "VolumeSnapshot" {
            return None;
        }
        let group = ds.get("apiGroup").and_then(Value::as_str).unwrap_or("");
        if group != crate::volume_snapshot::SNAPSHOT_GROUP {
            return None;
        }
        ds.get("name").and_then(Value::as_str).map(str::to_string)
    }

    /// Resolve a VolumeSnapshot to the directory holding its data.
    ///
    /// Returns `None` unless the snapshot is READY and its bound content
    /// carries a `snapshotHandle`. Every other outcome — missing snapshot,
    /// not-yet-ready, missing content — is `None` on purpose, and the caller
    /// leaves the claim Pending. A restore that silently produced an empty
    /// volume would pass every liveness check while losing all the data,
    /// which is the failure mode a drill is supposed to detect, not cause.
    fn resolve_snapshot_path(
        &self,
        namespace: &str,
        snapshot_name: &str,
        snapshots: &[(ResourceKey, Value)],
        contents: &[(ResourceKey, Value)],
    ) -> Option<String> {
        let (_, snap) = snapshots
            .iter()
            .find(|(k, _)| k.namespace.as_deref() == Some(namespace) && k.name == snapshot_name)?;
        let status = snap.get("status")?;
        if !status
            .get("readyToUse")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return None;
        }
        let content_name = status
            .get("boundVolumeSnapshotContentName")
            .and_then(Value::as_str)?;
        let (_, content) = contents.iter().find(|(k, _)| k.name == content_name)?;
        content
            .get("status")
            .and_then(|s| s.get("snapshotHandle"))
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    /// Provision one PVC through a registered CSI driver, then bind it.
    ///
    /// ★ THE ORDER IS CREATE-THEN-WRITE, AND IT MATTERS. The driver call
    /// happens first; only on success is a PV written. The reverse — write
    /// the PV, then create — leaves a PV referencing a volume handle that
    /// does not exist if the driver call fails, and a pod that mounts it
    /// gets a mount error rather than a Pending claim.
    ///
    /// The idempotency key is the PV NAME, derived from the claim's UID, so
    /// a retry after a transient failure returns the SAME volume instead of
    /// provisioning a second disk nobody will ever delete — and a claim
    /// recreated under the same name is a different key, so a driver that
    /// answers CreateVolume idempotently by name cannot hand it the deleted
    /// claim's disk.
    async fn provision_csi(
        &self,
        pvc_key: &ResourceKey,
        pvc: &Value,
        claim: &ClaimId<'_>,
        pv_name: &str,
        sc: &Value,
        provisioner: &str,
    ) -> Result<ObjectOutcome, ControllerError> {
        let requested = pvc
            .get("spec")
            .and_then(|s| s.get("resources"))
            .and_then(|r| r.get("requests"))
            .and_then(|r| r.get("storage"))
            .and_then(Value::as_str)
            .and_then(parse_quantity)
            .unwrap_or(0);

        let mut parameters = std::collections::BTreeMap::new();
        if let Some(params) = sc.get("parameters").and_then(Value::as_object) {
            for (k, v) in params {
                if let Some(v) = v.as_str() {
                    parameters.insert(k.clone(), v.to_string());
                }
            }
        }

        let access_modes = pvc
            .get("spec")
            .and_then(|s| s.get("accessModes"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let multi_node = access_modes
            .iter()
            .filter_map(Value::as_str)
            .any(|m| m == "ReadWriteMany" || m == "ReadOnlyMany");

        // A driver's refusal is this claim's alone: Item scope, retried on
        // the claim's own curve. Never a fake Bound — a claim bound to a
        // volume that was never created is worse than one honestly Pending.
        let created = self
            .csi
            .create_volume(&CsiCreateRequest {
                driver: provisioner.to_string(),
                name: pv_name.to_string(),
                capacity_bytes: requested,
                parameters,
                multi_node,
            })
            .await
            .map_err(|e| {
                ControllerError::Internal(
                    ["CSI CreateVolume through ", provisioner, " failed: ", &e].concat(),
                )
            })?;

        let sc_name = sc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let reclaim = sc
            .get("reclaimPolicy")
            .and_then(Value::as_str)
            .unwrap_or("Delete");
        let attributes: serde_json::Map<String, Value> = created
            .volume_attributes
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();

        let pv = json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "name": pv_name,
                "annotations": { "pv.kubernetes.io/provisioned-by": provisioner }
            },
            "spec": {
                // The DRIVER's capacity, which may exceed the request: a
                // driver rounds up to its allocation unit, and recording the
                // request instead would make a later binding check compare
                // against a size that does not exist.
                "capacity": { "storage": format!("{}", created.capacity_bytes) },
                "accessModes": if access_modes.is_empty() {
                    json!(["ReadWriteOnce"])
                } else {
                    json!(access_modes)
                },
                "persistentVolumeReclaimPolicy": reclaim,
                "storageClassName": sc_name,
                "csi": {
                    "driver": provisioner,
                    "volumeHandle": created.volume_handle,
                    "volumeAttributes": Value::Object(attributes),
                },
                "claimRef": Self::claim_ref(claim)
            },
            "status": { "phase": "Bound" }
        });

        // Store writes keep their own error, so they keep its Sweep scope.
        // They used to be flattened to a String with the driver's refusal,
        // which made a store that could not commit look like one claim's
        // problem and let the sweep carry on writing into it.
        self.create_and_bind(pvc_key, pvc, pv_name, pv).await
    }

    /// Write a freshly provisioned PV only if nothing holds its name, then
    /// bind the claim to it.
    ///
    /// ★ CREATE-IF-ABSENT, NEVER PUT. The PV is written by a transaction
    /// whose one compare is "this key does not exist" — the compare
    /// kube-apiserver uses for every create — with an empty failure branch.
    /// An unconditional Put here is how a recreated claim used to take over
    /// the deleted claim's PV: same name, overwritten claimRef, same
    /// directory underneath. A PV that already holds the name is left as it
    /// is, and the claim is not bound this pass; a PV that is this claim's
    /// is found on the next pass through its `claimRef` uid.
    async fn create_and_bind(
        &self,
        pvc_key: &ResourceKey,
        pvc: &Value,
        pv_name: &str,
        pv: Value,
    ) -> Result<ObjectOutcome, ControllerError> {
        let volume_key = ResourceKey::cluster_scoped("", "v1", "PersistentVolume", pv_name);
        let applied = self
            .store
            .propose(ResourceCommand::Txn {
                compares: vec![TxnCompare::NotExists {
                    key: volume_key.clone(),
                }],
                success: vec![TxnOp::Put {
                    key: volume_key,
                    value: pv,
                }],
                failure: Vec::new(),
                reason: Reason::Controller,
            })
            .await?;
        let volume = match Effect::of(applied.op) {
            written @ Effect::Written(_) => written,
            // The failure branch is empty, so a transaction that changed
            // nothing took it: the key was already there.
            Effect::Unchanged => return Ok(ObjectOutcome::skipped_because(PV_NAME_TAKEN)),
            refused @ Effect::Rejected(_) => return Ok(ObjectOutcome::from(refused)),
        };
        let claim = self
            .put(pvc_key.clone(), Self::bind_pvc(pvc.clone(), pv_name))
            .await?;
        Ok(ObjectOutcome::from(volume.and(claim)))
    }

    /// Write a Put for a resource value (Controller reason), and say what
    /// the store did with it.
    async fn put(&self, key: ResourceKey, value: Value) -> Result<Effect, ControllerError> {
        let applied = self
            .store
            .propose(ResourceCommand::Put {
                key,
                value,
                expected: None,
                reason: Reason::Controller,
            })
            .await?;
        Ok(Effect::of(applied.op))
    }
}

/// Set `status.phase` on a resource value, creating `status` if absent.
fn set_phase(v: &mut Value, phase: &str) {
    if let Some(status) = v.get_mut("status").and_then(Value::as_object_mut) {
        status.insert("phase".into(), json!(phase));
    } else if let Some(obj) = v.as_object_mut() {
        obj.insert("status".into(), json!({ "phase": phase }));
    }
}

/// Is this StorageClass the cluster default (`...is-default-class: "true"`)?
fn is_default_sc(sc: &Value) -> bool {
    sc.get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(DEFAULT_SC_ANNOTATION))
        .and_then(|v| v.as_str())
        .map(|s| s == "true")
        .unwrap_or(false)
}

/// Does a StorageClass use the local-path provisioner (either the
/// rancher.io standard or engenho's alias)?
fn sc_is_local_path(sc: &Value) -> bool {
    matches!(
        sc.get("provisioner").and_then(|p| p.as_str()),
        Some(LOCAL_PATH_PROVISIONER | ENGENHO_LOCAL_PATH_PROVISIONER)
    )
}

impl PvBinderController {
    /// The existing PV `pvc` binds to, if any. Candidates are the PVs not
    /// already claimed this tick; a pre-bound claim (`spec.volumeName`)
    /// narrows them to exactly that PV.
    ///
    /// First a PV whose claimRef carries this claim's uid, whatever its
    /// phase: it is already this claim's volume — a provision whose PV write
    /// landed and whose claim write did not, or an operator pre-bind by uid
    /// (upstream's `IsVolumeBoundToClaim` arm). Then any Available PV that
    /// matches.
    fn static_candidate<'v>(
        pvs: &'v [(ResourceKey, Value)],
        pvc: &Value,
        claim: &ClaimId<'_>,
        pre_bound: Option<&str>,
        claimed_this_tick: &[String],
    ) -> Option<&'v (ResourceKey, Value)> {
        let eligible = |key: &ResourceKey| {
            !claimed_this_tick.contains(&key.name) && pre_bound.is_none_or(|n| key.name == n)
        };
        pvs.iter()
            .find(|(key, pv)| {
                eligible(key)
                    && Self::claim_ref_to(pv, claim) == ClaimRefTo::ThisClaim
                    && Self::holds_request(pv, pvc)
            })
            .or_else(|| {
                pvs.iter().find(|(key, pv)| {
                    eligible(key)
                        && Self::pv_phase(pv) == "Available"
                        && Self::pv_matches_pvc(pv, pvc, claim)
                })
            })
    }

    /// Reconcile ONE claim. `?` in here leaves this claim only; whether the
    /// error then ends the sweep is decided by its scope in
    /// [`ObjectOutcome::settle`], never here.
    ///
    /// `claimed_this_tick` is shared across the sweep so two Pending claims
    /// do not bind the same available PV in one pass.
    async fn reconcile_claim(
        &self,
        pvc_key: &ResourceKey,
        pvc: &Value,
        view: &TickView,
        claimed_this_tick: &tokio::sync::Mutex<Vec<String>>,
    ) -> Result<ObjectOutcome, ControllerError> {
        // Held for this claim's whole reconcile. Claims are reconciled one
        // at a time, so this never waits; it is how the one list is shared
        // by per-claim futures that each outlive a single borrow.
        let mut claimed_this_tick = claimed_this_tick.lock().await;
        // Only Pending (or status-less) PVCs need binding. Already-Bound
        // PVCs are converged: idempotent no-ops.
        if Self::pvc_phase(pvc) != "Pending" {
            return Ok(ObjectOutcome::Unchanged);
        }
        let pvc_ns = pvc_key.namespace.as_deref().unwrap_or("default");
        let pvc_name = &pvc_key.name;

        // 0. IDENTITY. Every volume this claim is given — found or made —
        //    is tied to it by uid, so a claim with no usable uid has nothing
        //    to be matched by and stays Pending, saying why.
        let uid = match ClaimUid::of(pvc) {
            Ok(uid) => uid,
            Err(gap) => return Ok(ObjectOutcome::skipped_because(gap.note())),
        };
        let claim = ClaimId {
            namespace: pvc_ns,
            name: pvc_name,
            uid: &uid,
        };

        // 1. STATIC BIND.
        let pre_bound = Self::pre_bound_volume(pvc);
        let candidate =
            Self::static_candidate(&view.pvs, pvc, &claim, pre_bound, &claimed_this_tick);
        if let Some((pv_key, pv)) = candidate {
            let volume_name = pv_key.name.clone();
            let bound_pvc = Self::bind_pvc(pvc.clone(), &volume_name);
            let bound_pv = Self::bind_pv(pv.clone(), &claim);
            let claim = self.put(pvc_key.clone(), bound_pvc).await?;
            let volume = self.put(pv_key.clone(), bound_pv).await?;
            debug!(pvc = %pvc_key.label(), pv = %volume_name, "bound PVC to existing PV");
            claimed_this_tick.push(volume_name);
            return Ok(ObjectOutcome::from(claim.and(volume)));
        }

        // A pre-bound PVC whose named PV isn't available this pass waits;
        // we never dynamically provision over an explicit volumeName.
        if pre_bound.is_some() {
            return Ok(ObjectOutcome::SKIPPED);
        }

        // 2. DYNAMIC PROVISION via the local-path provisioner / default SC.
        let Some(sc) = Self::effective_storage_class(pvc, &view.storage_classes) else {
            // No class + no default SC → stay Pending (no provisioner).
            return Ok(ObjectOutcome::SKIPPED);
        };
        // The name this claim's volume gets, from its uid. If a PV already
        // holds it and was not bound to this claim above, it is not this
        // claim's to take: nothing is created over it, and nothing is
        // provisioned into what may be its directory.
        let volume = PvName::for_claim(&uid);
        let name_taken = view.pvs.iter().any(|(k, _)| volume.names(&k.name));
        let volume_name = volume.to_string();
        if !sc_is_local_path(sc) {
            // 2b. CSI DYNAMIC PROVISION. The class names some other
            // provisioner; if a registered CSI driver answers to that
            // name, engenho provisions through it.
            let provisioner = sc
                .get("provisioner")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // Asked BEFORE creating: an absent driver must leave the PVC
            // Pending ("waiting for your driver"), not fail a provision
            // ("your storage is broken"). Also what keeps a cluster with
            // an external provisioner working unchanged.
            if provisioner.is_empty() || !self.csi.can_provision(provisioner).await {
                return Ok(ObjectOutcome::SKIPPED);
            }
            if Self::is_wait_for_first_consumer(Some(sc)) {
                return Ok(ObjectOutcome::SKIPPED);
            }
            // Before the driver is asked: a CreateVolume idempotent by name
            // would hand back whatever volume holds it.
            if name_taken {
                return Ok(ObjectOutcome::skipped_because(PV_NAME_TAKEN));
            }
            return self
                .provision_csi(pvc_key, pvc, &claim, &volume_name, sc, provisioner)
                .await;
        }
        if Self::is_wait_for_first_consumer(Some(sc)) {
            // WaitForFirstConsumer: leave Pending until a Pod references the
            // PVC. TYPED-DEFERRED (named follow-up) — never provision early.
            return Ok(ObjectOutcome::skipped_because(WAIT_FOR_FIRST_CONSUMER));
        }

        // Immediate binding mode: provision now. The local-path PV copies
        // the claim's request as its capacity, so a claim with no usable
        // size has nothing to provision. That is its declaration being
        // unusable, not the world being unready: Declarative, reported on
        // the claim. (A CSI claim with no size is different: the driver
        // picks one and the PV records the driver's answer.)
        if Self::pvc_request_bytes(pvc).is_none() {
            return Err(ControllerError::InvalidResource(
                UNUSABLE_REQUEST.to_string(),
            ));
        }
        let host_path = match volume.local_path_dir(&self.local_path_root, pvc_ns, pvc_name) {
            Ok(dir) => dir.to_string(),
            Err(gap) => return Ok(ObjectOutcome::skipped_because(gap.note())),
        };
        // Before any host effect: the directory is named by the same uid,
        // so a PV holding the name may be using it.
        if name_taken {
            return Ok(ObjectOutcome::skipped_because(PV_NAME_TAKEN));
        }
        // The ONE host effect — create the backing dir before the PV is
        // visible so the kubelet can bind-mount it. A failure is this
        // claim's alone: the claims after it in the list still bind.
        self.env
            .ensure_dir(&host_path)
            .map_err(|e| ControllerError::Internal(["provision local-path dir: ", &e].concat()))?;
        // ── RESTORE-FROM-SNAPSHOT ────────────────────────────────────
        // A PVC whose `spec.dataSource` names a VolumeSnapshot is a
        // RESTORE, and this is the PITR drill's whole restore vector. The
        // hydrate happens AFTER the dir exists and BEFORE the PV is
        // visible, so a pod can never observe a half-filled volume.
        //
        // A named-but-unresolvable snapshot leaves the claim Pending
        // rather than provisioning an empty volume: presenting an empty
        // restore as a successful one is exactly the "clean receipt, no
        // data underneath" failure a drill exists to catch.
        if let Some(snap_name) = Self::snapshot_data_source(pvc) {
            let Some(src) = self.resolve_snapshot_path(
                pvc_ns,
                &snap_name,
                &view.snapshots,
                &view.snapshot_contents,
            ) else {
                return Ok(ObjectOutcome::skipped_because(SNAPSHOT_NOT_READY));
            };
            self.env
                .restore_tree(&src, &host_path)
                .map_err(|e| ControllerError::Internal(["restore from snapshot: ", &e].concat()))?;
            debug!(pvc = %pvc_key.label(), snapshot = %snap_name, "restored PV data from snapshot");
        }
        let dyn_pv = Self::build_dynamic_pv(pvc, &claim, &volume_name, &host_path, sc);
        let outcome = self
            .create_and_bind(pvc_key, pvc, &volume_name, dyn_pv)
            .await?;
        debug!(pvc = %pvc_key.label(), pv = %volume_name, ?outcome, "local-path provision");
        claimed_this_tick.push(volume_name);
        Ok(outcome)
    }
}

/// Claims, the volumes and classes that satisfy them, and the snapshots a
/// claim with a `dataSource` is restored from. A snapshot turning
/// `readyToUse` wakes the binder, so the claim waiting on it binds then.
impl DeclaresReads for PvBinderController {
    fn reads(&self) -> Reads {
        Reads::of(&[
            gvk("", "v1", "PersistentVolumeClaim"),
            gvk("", "v1", "PersistentVolume"),
            gvk("storage.k8s.io", "v1", "StorageClass"),
            gvk(SNAPSHOT_GROUP, SNAPSHOT_VERSION, "VolumeSnapshot"),
            gvk(SNAPSHOT_GROUP, SNAPSHOT_VERSION, "VolumeSnapshotContent"),
        ])
    }
}

#[async_trait]
impl Controller for PvBinderController {
    fn name(&self) -> &'static str {
        "pv-binder"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        // PVCs are namespaced; PVs + StorageClasses are cluster-scoped.
        let pvcs = self
            .store
            .list("", "v1", "PersistentVolumeClaim", self.namespace.as_deref())
            .await;
        let view = TickView {
            pvs: self.store.list("", "v1", "PersistentVolume", None).await,
            storage_classes: self
                .store
                .list("storage.k8s.io", "v1", "StorageClass", None)
                .await,
            // Snapshot kinds, for `spec.dataSource` restores. Listing them
            // unconditionally costs one store read per tick and keeps the
            // restore path from needing a second controller.
            snapshots: self
                .store
                .list(
                    crate::volume_snapshot::SNAPSHOT_GROUP,
                    crate::volume_snapshot::SNAPSHOT_VERSION,
                    "VolumeSnapshot",
                    None,
                )
                .await,
            snapshot_contents: self
                .store
                .list(
                    crate::volume_snapshot::SNAPSHOT_GROUP,
                    crate::volume_snapshot::SNAPSHOT_VERSION,
                    "VolumeSnapshotContent",
                    None,
                )
                .await,
        };

        // Track PVs claimed THIS tick so two Pending PVCs don't bind the same
        // available PV in one pass.
        let claimed_this_tick = tokio::sync::Mutex::new(Vec::new());

        let (this, view, claimed) = (self, &view, &claimed_this_tick);
        let report = self
            .sweep
            .run(&pvcs, |pvc_key, pvc| async move {
                ObjectOutcome::settle(this.reconcile_claim(pvc_key, pvc, view, claimed).await)
            })
            .await?;
        Ok(report.into())
    }
}

/// Deterministic mock [`ProvisionerEnv`] — records every dir it was asked to
/// ensure + always succeeds. The trait IS the testability contract: binding +
/// provisioning is fully exercised with ZERO host filesystem.
#[derive(Default)]
pub struct FakeProvisionerEnv {
    ensured: std::sync::Mutex<Vec<String>>,
    restored: std::sync::Mutex<Vec<(String, String)>>,
}

impl FakeProvisionerEnv {
    /// Fresh empty mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The dirs the mock was asked to ensure (in call order).
    #[must_use]
    pub fn ensured_dirs(&self) -> Vec<String> {
        self.ensured.lock().unwrap().clone()
    }

    /// The `(src, dst)` restores the mock was asked to perform, in call order.
    #[must_use]
    pub fn restored_trees(&self) -> Vec<(String, String)> {
        self.restored.lock().unwrap().clone()
    }
}

impl ProvisionerEnv for FakeProvisionerEnv {
    fn ensure_dir(&self, path: &str) -> Result<(), String> {
        self.ensured.lock().unwrap().push(path.to_string());
        Ok(())
    }

    /// Records `(src, dst)` instead of copying, so a test can assert that a
    /// restore was attempted FROM THE RIGHT SNAPSHOT — the pair is the whole
    /// claim, and a mock that only recorded "a restore happened" could not
    /// catch a restore from the wrong snapshot.
    fn restore_tree(&self, src: &str, dst: &str) -> Result<(), String> {
        self.restored
            .lock()
            .unwrap()
            .push((src.to_string(), dst.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn live_store() -> Arc<StoreMesh> {
        use engenho_store::{InProcessRouter, default_config};
        let router = InProcessRouter::new();
        let cfg = default_config("controllers-pv-binder").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        store
    }

    async fn put_op(store: &StoreMesh, key: ResourceKey, value: Value) {
        store
            .propose(ResourceCommand::put(key, value, Reason::Operator))
            .await
            .unwrap();
    }

    fn pvc_key(ns: &str, name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "PersistentVolumeClaim", ns, name)
    }
    fn pv_key(name: &str) -> ResourceKey {
        ResourceKey::cluster_scoped("", "v1", "PersistentVolume", name)
    }
    fn sc_key(name: &str) -> ResourceKey {
        ResourceKey::cluster_scoped("storage.k8s.io", "v1", "StorageClass", name)
    }

    fn uid(s: &str) -> ClaimUid {
        ClaimUid::of(&json!({"metadata": {"uid": s}})).unwrap()
    }

    /// The claim `ns/c` with uid `u-c`, as the pure predicates see it.
    fn with_claim<R>(f: impl FnOnce(&ClaimId<'_>) -> R) -> R {
        let u = uid("u-c");
        f(&ClaimId {
            namespace: "ns",
            name: "c",
            uid: &u,
        })
    }

    fn binder(store: Arc<StoreMesh>) -> PvBinderController {
        PvBinderController::with_env(
            store,
            None,
            "/data/local-path",
            Arc::new(FakeProvisionerEnv::new()),
        )
    }

    // ── pure-predicate tests (no store) ──────────────────────────────

    #[test]
    fn capacity_must_be_at_least_request() {
        let pvc = json!({"spec": {"resources": {"requests": {"storage": "1Gi"}},
                                  "accessModes": ["ReadWriteOnce"]}});
        let big = json!({"spec": {"capacity": {"storage": "2Gi"},
                                  "accessModes": ["ReadWriteOnce"]}});
        let small = json!({"spec": {"capacity": {"storage": "512Mi"},
                                    "accessModes": ["ReadWriteOnce"]}});
        with_claim(|c| {
            assert!(PvBinderController::pv_matches_pvc(&big, &pvc, c));
            assert!(!PvBinderController::pv_matches_pvc(&small, &pvc, c));
        });
    }

    #[test]
    fn access_modes_must_be_superset() {
        let pvc = json!({"spec": {"resources": {"requests": {"storage": "1Gi"}},
                                  "accessModes": ["ReadWriteMany"]}});
        // PV offers only RWO → cannot satisfy a RWX request.
        let rwo = json!({"spec": {"capacity": {"storage": "1Gi"},
                                  "accessModes": ["ReadWriteOnce"]}});
        let rwx = json!({"spec": {"capacity": {"storage": "1Gi"},
                                  "accessModes": ["ReadWriteOnce", "ReadWriteMany"]}});
        with_claim(|c| {
            assert!(!PvBinderController::pv_matches_pvc(&rwo, &pvc, c));
            assert!(PvBinderController::pv_matches_pvc(&rwx, &pvc, c));
        });
    }

    #[test]
    fn storage_class_must_match_nil_eq_empty() {
        let pvc_nil = json!({"spec": {}});
        let pv_empty = json!({"spec": {"storageClassName": ""}});
        assert!(PvBinderController::storage_class_matches(
            &pvc_nil, &pv_empty
        ));
        let pvc_fast = json!({"spec": {"storageClassName": "fast"}});
        let pv_slow = json!({"spec": {"storageClassName": "slow"}});
        assert!(!PvBinderController::storage_class_matches(
            &pvc_fast, &pv_slow
        ));
        let pv_fast = json!({"spec": {"storageClassName": "fast"}});
        assert!(PvBinderController::storage_class_matches(
            &pvc_fast, &pv_fast
        ));
    }

    #[test]
    fn claimref_to_other_claim_blocks_match() {
        let pvc = json!({"spec": {"resources": {"requests": {"storage": "1Gi"}}}});
        let pv = json!({"spec": {"capacity": {"storage": "1Gi"},
                                 "claimRef": {"namespace": "other", "name": "x"}}});
        // claimRef to THIS pvc is fine.
        let pv2 = json!({"spec": {"capacity": {"storage": "1Gi"},
                                  "claimRef": {"namespace": "ns", "name": "c"}}});
        with_claim(|c| {
            assert!(!PvBinderController::pv_matches_pvc(&pv, &pvc, c));
            assert!(PvBinderController::pv_matches_pvc(&pv2, &pvc, c));
        });
    }

    /// A claimRef naming this namespace and name but ANOTHER uid is an
    /// earlier claim of the same name: never compatible. With this claim's
    /// uid, or with none (a reservation by name), it is.
    #[test]
    fn a_claim_ref_uid_tells_two_claims_of_one_name_apart() {
        let pvc = json!({"spec": {"resources": {"requests": {"storage": "1Gi"}}}});
        let reserved = |uid: Value| {
            json!({"spec": {"capacity": {"storage": "1Gi"},
                            "claimRef": {"namespace": "ns", "name": "c", "uid": uid}}})
        };
        with_claim(|c| {
            let earlier = reserved(json!("u-earlier"));
            assert_eq!(
                PvBinderController::claim_ref_to(&earlier, c),
                ClaimRefTo::Other
            );
            assert!(!PvBinderController::pv_claimref_compatible(&earlier, c));
            assert!(!PvBinderController::pv_matches_pvc(&earlier, &pvc, c));

            let this = reserved(json!("u-c"));
            assert_eq!(
                PvBinderController::claim_ref_to(&this, c),
                ClaimRefTo::ThisClaim
            );
            assert!(PvBinderController::pv_matches_pvc(&this, &pvc, c));

            for by_name in [reserved(Value::Null), reserved(json!(""))] {
                assert_eq!(
                    PvBinderController::claim_ref_to(&by_name, c),
                    ClaimRefTo::ThisName
                );
                assert!(PvBinderController::pv_matches_pvc(&by_name, &pvc, c));
            }
            // A uid that is not a string cannot be checked, so it is not
            // this claim's.
            assert_eq!(
                PvBinderController::claim_ref_to(&reserved(json!(7)), c),
                ClaimRefTo::Other
            );
        });
    }

    // ── live-store binding tests ─────────────────────────────────────

    #[tokio::test]
    async fn pending_pvc_binds_to_matching_pv_bidirectionally() {
        let store = live_store().await;
        put_op(
            &store,
            pvc_key("ns1", "claim"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "claim", "namespace": "ns1", "uid": "claim-uid"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "1Gi"}},
                            "storageClassName": "manual"}}),
        )
        .await;
        put_op(
            &store,
            pv_key("vol-a"),
            json!({"apiVersion": "v1", "kind": "PersistentVolume",
                   "metadata": {"name": "vol-a"},
                   "spec": {"capacity": {"storage": "5Gi"},
                            "accessModes": ["ReadWriteOnce"],
                            "storageClassName": "manual",
                            "hostPath": {"path": "/mnt/a"}},
                   "status": {"phase": "Available"}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        let pvc = store.get(&pvc_key("ns1", "claim")).await.unwrap();
        assert_eq!(pvc["spec"]["volumeName"], "vol-a");
        assert_eq!(pvc["status"]["phase"], "Bound");
        let pv = store.get(&pv_key("vol-a")).await.unwrap();
        assert_eq!(pv["status"]["phase"], "Bound");
        assert_eq!(pv["spec"]["claimRef"]["name"], "claim");
        assert_eq!(pv["spec"]["claimRef"]["namespace"], "ns1");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "claim-uid");
    }

    #[tokio::test]
    async fn mismatched_pvs_do_not_bind() {
        let store = live_store().await;
        put_op(
            &store,
            pvc_key("ns1", "claim"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "claim", "namespace": "ns1"},
                   "spec": {"accessModes": ["ReadWriteMany"],
                            "resources": {"requests": {"storage": "10Gi"}},
                            "storageClassName": "manual"}}),
        )
        .await;
        // Too small.
        put_op(
            &store,
            pv_key("small"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"small"},
                   "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteMany"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;
        // Wrong class.
        put_op(
            &store,
            pv_key("wrongclass"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"wrongclass"},
                   "spec":{"capacity":{"storage":"100Gi"},"accessModes":["ReadWriteMany"],
                           "storageClassName":"other"},"status":{"phase":"Available"}}),
        )
        .await;
        // Wrong access mode (RWO can't serve RWX).
        put_op(
            &store,
            pv_key("wrongmode"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"wrongmode"},
                   "spec":{"capacity":{"storage":"100Gi"},"accessModes":["ReadWriteOnce"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        let pvc = store.get(&pvc_key("ns1", "claim")).await.unwrap();
        // Left untouched: phase never advanced to Bound (a Pending PVC the
        // binder can't satisfy is not re-stamped — phase stays unset).
        assert_ne!(pvc["status"]["phase"], "Bound");
        assert!(pvc["spec"].get("volumeName").is_none() || pvc["spec"]["volumeName"].is_null());
        // No PV was claimed.
        for n in ["small", "wrongclass", "wrongmode"] {
            let pv = store.get(&pv_key(n)).await.unwrap();
            assert_eq!(
                pv["status"]["phase"], "Available",
                "{n} must stay available"
            );
        }
    }

    #[tokio::test]
    async fn pvc_with_default_local_path_sc_dynamically_provisions() {
        let store = live_store().await;
        put_op(
            &store,
            sc_key("local-path"),
            json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                   "metadata": {"name": "local-path",
                                "annotations": {"storageclass.kubernetes.io/is-default-class": "true"}},
                   "provisioner": "rancher.io/local-path",
                   "reclaimPolicy": "Delete",
                   "volumeBindingMode": "Immediate"}),
        )
        .await;
        // PVC omits storageClassName → uses the default SC.
        put_op(
            &store,
            pvc_key("ns1", "dyn"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "dyn", "namespace": "ns1", "uid": "dyn-uid"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "2Gi"}}}}),
        )
        .await;

        let env = Arc::new(FakeProvisionerEnv::new());
        let c = PvBinderController::with_env(store.clone(), None, "/data/local-path", env.clone());
        c.tick().await.unwrap();

        // A PV was provisioned + the PVC bound to it.
        let pvc = store.get(&pvc_key("ns1", "dyn")).await.unwrap();
        assert_eq!(pvc["status"]["phase"], "Bound");
        let pv_name = pvc["spec"]["volumeName"].as_str().unwrap().to_string();
        let pv = store.get(&pv_key(&pv_name)).await.unwrap();
        assert_eq!(pv["status"]["phase"], "Bound");
        assert_eq!(pv["spec"]["capacity"]["storage"], "2Gi");
        assert_eq!(pv_name, "pvc-dyn-uid", "the PV is named by the claim's uid");
        assert_eq!(
            pv["spec"]["hostPath"]["path"],
            "/data/local-path/pvc-dyn-uid_ns1_dyn"
        );
        assert_eq!(pv["spec"]["claimRef"]["name"], "dyn");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "dyn-uid");
        assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Delete");
        // The host effect: the backing dir was ensured.
        assert_eq!(
            env.ensured_dirs(),
            vec!["/data/local-path/pvc-dyn-uid_ns1_dyn".to_string()]
        );
    }

    #[tokio::test]
    async fn wait_for_first_consumer_pvc_stays_pending() {
        let store = live_store().await;
        put_op(
            &store,
            sc_key("local-path-wfc"),
            json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                   "metadata": {"name": "local-path-wfc",
                                "annotations": {"storageclass.kubernetes.io/is-default-class": "true"}},
                   "provisioner": "rancher.io/local-path",
                   "volumeBindingMode": "WaitForFirstConsumer"}),
        )
        .await;
        put_op(
            &store,
            pvc_key("ns1", "wfc"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "wfc", "namespace": "ns1"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "1Gi"}}}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();
        let pvc = store.get(&pvc_key("ns1", "wfc")).await.unwrap();
        // WaitForFirstConsumer: never provisioned/bound — phase stays unset
        // (i.e. still Pending), and no volumeName was stamped.
        assert_ne!(
            pvc["status"]["phase"], "Bound",
            "WaitForFirstConsumer PVC must NOT be bound (no early provision)"
        );
        assert!(pvc["spec"].get("volumeName").is_none() || pvc["spec"]["volumeName"].is_null());
        // No PV was dynamically provisioned for it.
        let pvs = store.list("", "v1", "PersistentVolume", None).await;
        assert!(pvs.is_empty(), "no PV provisioned for a WFC claim");
    }

    #[tokio::test]
    async fn pre_bound_pvc_binds_only_to_named_pv() {
        let store = live_store().await;
        // PVC pre-binds to vol-named via spec.volumeName.
        put_op(
            &store,
            pvc_key("ns1", "claim"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "claim", "namespace": "ns1"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "1Gi"}},
                            "storageClassName": "manual",
                            "volumeName": "vol-named"}}),
        )
        .await;
        // A different available PV that ALSO matches on capacity/class —
        // must NOT be chosen.
        put_op(
            &store,
            pv_key("vol-other"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"vol-other"},
                   "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;
        put_op(
            &store,
            pv_key("vol-named"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"vol-named"},
                   "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        let pvc = store.get(&pvc_key("ns1", "claim")).await.unwrap();
        assert_eq!(pvc["spec"]["volumeName"], "vol-named");
        assert_eq!(pvc["status"]["phase"], "Bound");
        // The other PV was left untouched.
        let other = store.get(&pv_key("vol-other")).await.unwrap();
        assert_eq!(other["status"]["phase"], "Available");
    }

    #[tokio::test]
    async fn already_bound_pvc_is_idempotent_noop() {
        let store = live_store().await;
        put_op(
            &store,
            pvc_key("ns1", "bound"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "bound", "namespace": "ns1"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "1Gi"}},
                            "storageClassName": "manual",
                            "volumeName": "vol-x"},
                   "status": {"phase": "Bound"}}),
        )
        .await;
        // An Available PV that WOULD match if the PVC were Pending.
        put_op(
            &store,
            pv_key("vol-free"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"vol-free"},
                   "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;

        let outcome = binder(store.clone()).tick().await.unwrap();
        assert_eq!(
            outcome.objects_changed, 0,
            "no re-bind on an already-Bound PVC"
        );
        // The PVC's volumeName is unchanged; the free PV is untouched.
        let pvc = store.get(&pvc_key("ns1", "bound")).await.unwrap();
        assert_eq!(pvc["spec"]["volumeName"], "vol-x");
        let free = store.get(&pv_key("vol-free")).await.unwrap();
        assert_eq!(free["status"]["phase"], "Available");
    }

    #[tokio::test]
    async fn two_pending_pvcs_do_not_share_one_pv() {
        let store = live_store().await;
        for name in ["a", "b"] {
            put_op(
                &store,
                pvc_key("ns1", name),
                json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                       "metadata": {"name": name, "namespace": "ns1"},
                       "spec": {"accessModes": ["ReadWriteOnce"],
                                "resources": {"requests": {"storage": "1Gi"}},
                                "storageClassName": "manual"}}),
            )
            .await;
        }
        // Only ONE available PV.
        put_op(
            &store,
            pv_key("only"),
            json!({"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"only"},
                   "spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],
                           "storageClassName":"manual"},"status":{"phase":"Available"}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        let mut bound_count = 0;
        for n in ["a", "b"] {
            let pvc = store.get(&pvc_key("ns1", n)).await.unwrap();
            if pvc["status"]["phase"] == "Bound" {
                bound_count += 1;
            }
        }
        assert_eq!(bound_count, 1, "exactly one PVC binds the single PV");
    }

    // ── per-claim isolation (T2.3) ───────────────────────────────────

    /// A host seam whose directory creation fails for exactly one claim.
    struct PoisonedEnv {
        poisoned_dir: &'static str,
    }

    impl ProvisionerEnv for PoisonedEnv {
        fn ensure_dir(&self, path: &str) -> Result<(), String> {
            if path.ends_with(self.poisoned_dir) {
                Err("mkdir: permission denied".into())
            } else {
                Ok(())
            }
        }
        fn restore_tree(&self, _src: &str, _dst: &str) -> Result<(), String> {
            Ok(())
        }
    }

    async fn default_local_path_class(store: &StoreMesh) {
        put_op(
            store,
            sc_key("local-path"),
            json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                   "metadata": {"name": "local-path",
                                "annotations": {"storageclass.kubernetes.io/is-default-class": "true"}},
                   "provisioner": "rancher.io/local-path",
                   "volumeBindingMode": "Immediate"}),
        )
        .await;
    }

    async fn put_claim(store: &StoreMesh, name: &str, spec: Value) {
        put_claim_as(store, name, json!(["uid-", name].concat()), spec).await;
    }

    /// A claim `ns1/<name>` carrying exactly `uid` (which may be null or
    /// malformed — the store keeps whatever the body says).
    async fn put_claim_as(store: &StoreMesh, name: &str, uid: Value, spec: Value) {
        put_op(
            store,
            pvc_key("ns1", name),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": name, "namespace": "ns1", "uid": uid},
                   "spec": spec}),
        )
        .await;
    }

    fn sized() -> Value {
        json!({"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}})
    }

    async fn phase(store: &StoreMesh, name: &str) -> Value {
        store.get(&pvc_key("ns1", name)).await.unwrap()["status"]["phase"].clone()
    }

    /// The claim listed FIRST cannot get its directory. Before T2.3 its
    /// `?` ended the tick, so the two healthy claims after it were never
    /// reached — on that tick or any later one while the directory failed.
    #[tokio::test]
    async fn one_poisoned_claim_does_not_stop_the_others() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        for name in ["aaa-poisoned", "bbb", "ccc"] {
            put_claim(&store, name, sized()).await;
        }
        let listed = store.list("", "v1", "PersistentVolumeClaim", None).await;
        assert_eq!(
            listed[0].0.name, "aaa-poisoned",
            "precondition: listed first"
        );

        let c = PvBinderController::with_env(
            store.clone(),
            None,
            "/data/local-path",
            Arc::new(PoisonedEnv {
                poisoned_dir: "_ns1_aaa-poisoned",
            }),
        );
        let outcome = c
            .tick()
            .await
            .expect("one claim's directory is not the whole sweep's failure");

        for name in ["bbb", "ccc"] {
            assert_eq!(phase(&store, name).await, "Bound", "{name} was reconciled");
        }
        assert_ne!(phase(&store, "aaa-poisoned").await, "Bound");
        assert!(
            store.get(&pv_key("pvc-uid-aaa-poisoned")).await.is_none(),
            "no PV is written for a claim whose directory does not exist"
        );
        let sweep = outcome.sweep.expect("the binder reports through its sweep");
        assert_eq!((sweep.changed(), sweep.failed()), (2, 1));
        // A mkdir failure is Transient: isolated, but still retried on the
        // claim's own curve rather than left to the fallback timer.
        assert_eq!(
            outcome.result,
            crate::ReconcileResult::Requeue(crate::TRANSIENT_RETRY.base())
        );
    }

    /// examined = changed + unchanged + skipped + failed, on a real tick
    /// that produces every outcome.
    #[tokio::test]
    async fn the_report_identity_holds_on_a_real_sweep() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        // Changed: provisioned.
        put_claim(&store, "fresh", sized()).await;
        // Unchanged: already Bound.
        put_op(
            &store,
            pvc_key("ns1", "settled"),
            json!({"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                   "metadata": {"name": "settled", "namespace": "ns1"},
                   "spec": {"volumeName": "elsewhere"}, "status": {"phase": "Bound"}}),
        )
        .await;
        // Skipped: pre-bound to a PV that is not there.
        put_claim(
            &store,
            "waiting",
            json!({"accessModes": ["ReadWriteOnce"], "volumeName": "absent",
                   "resources": {"requests": {"storage": "1Gi"}}}),
        )
        .await;
        // Failed, Transient: its directory cannot be made.
        put_claim(&store, "poisoned", sized()).await;
        // Failed, Declarative: no size to provision.
        put_claim(
            &store,
            "sizeless",
            json!({"accessModes": ["ReadWriteOnce"]}),
        )
        .await;

        let c = PvBinderController::with_env(
            store.clone(),
            None,
            "/data/local-path",
            Arc::new(PoisonedEnv {
                poisoned_dir: "_ns1_poisoned",
            }),
        );
        let outcome = c.tick().await.unwrap();
        let sweep = outcome.sweep.unwrap();
        assert_eq!(
            (
                sweep.changed(),
                sweep.unchanged(),
                sweep.skipped(),
                sweep.failed()
            ),
            (1, 1, 1, 2)
        );
        assert_eq!(sweep.examined(), 5, "every listed claim was examined");
        assert_eq!(outcome.objects_examined, 5);
        assert_eq!(outcome.objects_changed, 1);
        assert_eq!(outcome.objects_skipped, 1);
    }

    /// A claim with no usable size is a broken declaration: it stays
    /// Pending, gets no PV, and says why on itself — once, not once per
    /// sweep. The claim beside it still provisions.
    #[tokio::test]
    async fn a_sizeless_claim_fails_on_itself_with_one_event() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        put_claim(
            &store,
            "sizeless",
            json!({"accessModes": ["ReadWriteOnce"]}),
        )
        .await;
        put_claim(&store, "sized", sized()).await;

        let events = Arc::new(crate::event_recorder::CollectingEventSink::new());
        let c = binder(store.clone()).with_event_sink(events.clone());
        let outcome = c.tick().await.unwrap();
        c.tick().await.unwrap();

        assert_eq!(phase(&store, "sized").await, "Bound");
        assert_ne!(phase(&store, "sizeless").await, "Bound");
        assert!(
            store.get(&pv_key("pvc-uid-sizeless")).await.is_none(),
            "no PV with a null or unparseable capacity"
        );
        let recorded = events.drain();
        assert_eq!(
            recorded.len(),
            1,
            "one Event across two sweeps: {recorded:?}"
        );
        let e = &recorded[0];
        assert_eq!(e.reason, crate::event_recorder::Reason::ProvisioningFailed);
        assert_eq!(e.involved.kind, "PersistentVolumeClaim");
        assert_eq!(e.involved.name, "sizeless");
        assert_eq!(e.involved.uid.as_deref(), Some("uid-sizeless"));
        assert_eq!(e.component, "persistentvolume-controller");
        // Declarative: no targeted retry; the Event is the answer.
        assert_eq!(outcome.result, crate::ReconcileResult::Done);
    }

    // ── identity is the claim's uid (T1.4) ───────────────────────────

    async fn delete_claim(store: &StoreMesh, name: &str) {
        store
            .propose(ResourceCommand::delete(
                pvc_key("ns1", name),
                Reason::Operator,
            ))
            .await
            .unwrap();
        assert!(store.get(&pvc_key("ns1", name)).await.is_none());
    }

    /// ★ THE DEFECT. A claim is provisioned, deleted, and created again
    /// under the same namespace and name. Its PV is not reclaimed (nothing
    /// reclaims yet), so it is still in the store. The new claim is a new
    /// object with a new uid, and must get a new volume: on HEAD it was
    /// handed the same `pvc-<ns>-<name>` PV, re-Put over itself, and the
    /// same directory with the deleted claim's data in it.
    #[tokio::test]
    async fn a_recreated_claim_does_not_inherit_the_deleted_claims_volume() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        let env = Arc::new(FakeProvisionerEnv::new());
        let c = PvBinderController::with_env(store.clone(), None, "/data/local-path", env.clone());

        put_claim_as(&store, "data", json!("uid-first"), sized()).await;
        c.tick().await.unwrap();
        let first = store.get(&pvc_key("ns1", "data")).await.unwrap();
        assert_eq!(first["status"]["phase"], "Bound");
        let first_pv_name = first["spec"]["volumeName"].as_str().unwrap().to_string();
        let first_pv = store.get(&pv_key(&first_pv_name)).await.unwrap();

        delete_claim(&store, "data").await;
        put_claim_as(&store, "data", json!("uid-second"), sized()).await;
        c.tick().await.unwrap();

        let second = store.get(&pvc_key("ns1", "data")).await.unwrap();
        assert_eq!(second["metadata"]["uid"], "uid-second", "a new incarnation");
        assert_eq!(second["status"]["phase"], "Bound");
        let second_pv_name = second["spec"]["volumeName"].as_str().unwrap();
        assert_ne!(
            second_pv_name, first_pv_name,
            "the recreated claim must not bind the deleted claim's PV"
        );
        let second_pv = store.get(&pv_key(second_pv_name)).await.unwrap();
        assert_ne!(
            second_pv["spec"]["hostPath"]["path"], first_pv["spec"]["hostPath"]["path"],
            "the recreated claim must not mount the deleted claim's directory"
        );
        assert_eq!(second_pv["spec"]["claimRef"]["uid"], "uid-second");
        assert_eq!(
            store.get(&pv_key(&first_pv_name)).await.unwrap(),
            first_pv,
            "the deleted claim's PV is left exactly as it was"
        );
        let dirs = env.ensured_dirs();
        assert_eq!(dirs.len(), 2, "{dirs:?}");
        assert_ne!(dirs[0], dirs[1], "two claims, two directories");
    }

    /// An Available PV whose claimRef names this namespace and name but an
    /// EARLIER uid is the earlier claim's (upstream would call it Released).
    /// A new claim of the same name must not bind it — on HEAD the claimRef
    /// was compared by namespace and name only.
    #[tokio::test]
    async fn a_pv_reserved_for_an_earlier_claim_of_the_same_name_is_not_bound() {
        let store = live_store().await;
        put_op(
            &store,
            pv_key("vol-old"),
            json!({"apiVersion": "v1", "kind": "PersistentVolume",
                   "metadata": {"name": "vol-old"},
                   "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                            "storageClassName": "manual",
                            "claimRef": {"namespace": "ns1", "name": "data", "uid": "uid-old"}},
                   "status": {"phase": "Available"}}),
        )
        .await;
        put_claim_as(
            &store,
            "data",
            json!("uid-new"),
            json!({"accessModes": ["ReadWriteOnce"], "storageClassName": "manual",
                   "resources": {"requests": {"storage": "1Gi"}}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        assert_ne!(phase(&store, "data").await, "Bound");
        let pv = store.get(&pv_key("vol-old")).await.unwrap();
        assert_eq!(pv["status"]["phase"], "Available");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "uid-old");
    }

    /// A PV reserved by namespace and name alone — no uid, the way an
    /// operator pre-binds one — still binds, and the bind records the uid.
    #[tokio::test]
    async fn a_pv_reserved_by_name_alone_binds_and_records_the_uid() {
        let store = live_store().await;
        put_op(
            &store,
            pv_key("vol-reserved"),
            json!({"apiVersion": "v1", "kind": "PersistentVolume",
                   "metadata": {"name": "vol-reserved"},
                   "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                            "storageClassName": "manual",
                            "claimRef": {"namespace": "ns1", "name": "data"}},
                   "status": {"phase": "Available"}}),
        )
        .await;
        put_claim_as(
            &store,
            "data",
            json!("uid-data"),
            json!({"accessModes": ["ReadWriteOnce"], "storageClassName": "manual",
                   "resources": {"requests": {"storage": "1Gi"}}}),
        )
        .await;

        binder(store.clone()).tick().await.unwrap();

        assert_eq!(phase(&store, "data").await, "Bound");
        let pv = store.get(&pv_key("vol-reserved")).await.unwrap();
        assert_eq!(pv["status"]["phase"], "Bound");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "uid-data");
    }

    /// A claim whose uid is missing, or cannot name a volume or a directory,
    /// stays Pending: no PV, no directory, and the sweep's note says which.
    #[tokio::test]
    async fn a_claim_with_no_usable_uid_stays_pending_with_a_typed_reason() {
        for (uid, gap) in [
            (Value::Null, NoVolumeIdentity::UidAbsent),
            (json!("../../escape"), NoVolumeIdentity::UidMalformed),
            (json!(12), NoVolumeIdentity::UidMalformed),
        ] {
            let store = live_store().await;
            default_local_path_class(&store).await;
            put_claim_as(&store, "data", uid.clone(), sized()).await;
            let env = Arc::new(FakeProvisionerEnv::new());
            let c =
                PvBinderController::with_env(store.clone(), None, "/data/local-path", env.clone());

            let outcome = c.tick().await.unwrap();

            assert_ne!(phase(&store, "data").await, "Bound", "{uid}");
            assert!(
                store
                    .list("", "v1", "PersistentVolume", None)
                    .await
                    .is_empty(),
                "no PV for a claim with uid {uid}"
            );
            assert!(env.ensured_dirs().is_empty(), "no directory for uid {uid}");
            let sweep = outcome.sweep.unwrap();
            assert_eq!((sweep.skipped(), sweep.failed()), (1, 0), "{uid}");
            assert_eq!(sweep.note(), Some(gap.note()), "{uid}");
        }
    }

    /// A provision whose PV write landed and whose claim write did not
    /// leaves a PV bound to the claim by uid and the claim Pending. The next
    /// pass binds the claim to THAT PV — it does not provision a second one
    /// or touch the directory again. The same holds for a PV provisioned
    /// under the old `pvc-<ns>-<name>` scheme: existing PVs keep their names.
    #[tokio::test]
    async fn a_pv_already_bound_to_the_claim_by_uid_is_found_not_reprovisioned() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        for (claim, pv_name) in [("data", "pvc-uid-data"), ("legacy", "pvc-ns1-legacy")] {
            put_claim(&store, claim, sized()).await;
            put_op(
                &store,
                pv_key(pv_name),
                json!({"apiVersion": "v1", "kind": "PersistentVolume",
                       "metadata": {"name": pv_name},
                       "spec": {"capacity": {"storage": "1Gi"},
                                "accessModes": ["ReadWriteOnce"],
                                "storageClassName": "local-path",
                                "hostPath": {"path": (["/data/local-path/", claim].concat())},
                                "claimRef": {"namespace": "ns1", "name": claim,
                                             "uid": (["uid-", claim].concat())}},
                       "status": {"phase": "Bound"}}),
            )
            .await;
        }
        let env = Arc::new(FakeProvisionerEnv::new());
        let c = PvBinderController::with_env(store.clone(), None, "/data/local-path", env.clone());

        c.tick().await.unwrap();

        for (claim, pv_name) in [("data", "pvc-uid-data"), ("legacy", "pvc-ns1-legacy")] {
            let pvc = store.get(&pvc_key("ns1", claim)).await.unwrap();
            assert_eq!(pvc["status"]["phase"], "Bound", "{claim}");
            assert_eq!(pvc["spec"]["volumeName"], pv_name, "{claim}");
        }
        assert_eq!(
            store.list("", "v1", "PersistentVolume", None).await.len(),
            2,
            "no second PV was provisioned"
        );
        assert!(
            env.ensured_dirs().is_empty(),
            "no directory was provisioned again: {:?}",
            env.ensured_dirs()
        );
    }

    /// A PV that already holds the name derived from this claim's uid but is
    /// bound to some other claim is never overwritten, and nothing is
    /// provisioned into what may be its directory.
    #[tokio::test]
    async fn a_pv_holding_the_derived_name_for_another_claim_is_left_alone() {
        let store = live_store().await;
        default_local_path_class(&store).await;
        let foreign = json!({"apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": "pvc-uid-data"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                     "storageClassName": "local-path",
                     "hostPath": {"path": "/foreign"},
                     "claimRef": {"namespace": "ns1", "name": "other", "uid": "uid-other"}},
            "status": {"phase": "Bound"}});
        put_op(&store, pv_key("pvc-uid-data"), foreign).await;
        let before = store.get(&pv_key("pvc-uid-data")).await.unwrap();
        put_claim(&store, "data", sized()).await;
        let env = Arc::new(FakeProvisionerEnv::new());
        let c = PvBinderController::with_env(store.clone(), None, "/data/local-path", env.clone());

        let outcome = c.tick().await.unwrap();

        assert_ne!(phase(&store, "data").await, "Bound");
        assert_eq!(store.get(&pv_key("pvc-uid-data")).await.unwrap(), before);
        assert!(env.ensured_dirs().is_empty(), "{:?}", env.ensured_dirs());
        assert_eq!(outcome.sweep.unwrap().note(), Some(PV_NAME_TAKEN));
    }

    /// The PV write itself is create-if-absent: a second create of the same
    /// name — a writer that raced this one between list and write — neither
    /// overwrites the PV nor binds the claim.
    #[tokio::test]
    async fn the_pv_write_is_create_if_absent() {
        let store = live_store().await;
        put_claim(&store, "data", sized()).await;
        let pvc = store.get(&pvc_key("ns1", "data")).await.unwrap();
        let c = binder(store.clone());
        let pv = |path: &str| {
            json!({"apiVersion": "v1", "kind": "PersistentVolume",
                   "metadata": {"name": "pvc-uid-data"},
                   "spec": {"hostPath": {"path": path}}})
        };

        let created = c
            .create_and_bind(&pvc_key("ns1", "data"), &pvc, "pvc-uid-data", pv("/first"))
            .await
            .unwrap();
        assert!(matches!(created, ObjectOutcome::Changed(_)), "{created:?}");
        let written = store.get(&pv_key("pvc-uid-data")).await.unwrap();

        // Unbind the claim, so only the create can say whether it binds.
        put_claim(&store, "data", sized()).await;
        let again = c
            .create_and_bind(&pvc_key("ns1", "data"), &pvc, "pvc-uid-data", pv("/second"))
            .await
            .unwrap();

        assert!(
            matches!(
                again,
                ObjectOutcome::Skipped {
                    note: Some(PV_NAME_TAKEN)
                }
            ),
            "{again:?}"
        );
        assert_eq!(
            store.get(&pv_key("pvc-uid-data")).await.unwrap(),
            written,
            "the existing PV is not overwritten"
        );
        assert_ne!(phase(&store, "data").await, "Bound");
    }

    /// A CSI driver that records every name it is asked to create under.
    #[derive(Default)]
    struct RecordingDriver {
        names: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CsiProvisioner for RecordingDriver {
        async fn can_provision(&self, _driver: &str) -> bool {
            true
        }
        async fn create_volume(
            &self,
            req: &CsiCreateRequest,
        ) -> Result<crate::csi_provisioner::CsiCreatedVolume, String> {
            self.names.lock().unwrap().push(req.name.clone());
            Ok(crate::csi_provisioner::CsiCreatedVolume {
                volume_handle: ["vol-", &req.name].concat(),
                capacity_bytes: req.capacity_bytes,
                volume_attributes: std::collections::BTreeMap::new(),
            })
        }
        async fn delete_volume(&self, _driver: &str, _handle: &str) -> Result<(), String> {
            Ok(())
        }
    }

    async fn csi_class(store: &StoreMesh) {
        put_op(
            store,
            sc_key("csi-sc"),
            json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                   "metadata": {"name": "csi-sc"}, "provisioner": "csi.example.com"}),
        )
        .await;
    }

    fn csi_claim_spec() -> Value {
        json!({"accessModes": ["ReadWriteOnce"], "storageClassName": "csi-sc",
               "resources": {"requests": {"storage": "1Gi"}}})
    }

    /// A PV holding the name derived from a CSI claim's uid, bound to some
    /// other claim: the driver is never asked, because a `CreateVolume`
    /// idempotent by name would answer with whatever volume holds it.
    #[tokio::test]
    async fn a_csi_claim_whose_name_is_taken_never_reaches_the_driver() {
        let store = live_store().await;
        csi_class(&store).await;
        put_op(
            &store,
            pv_key("pvc-uid-db"),
            json!({"apiVersion": "v1", "kind": "PersistentVolume",
                   "metadata": {"name": "pvc-uid-db"},
                   "spec": {"capacity": {"storage": "1Gi"},
                            "csi": {"driver": "csi.example.com", "volumeHandle": "theirs"},
                            "claimRef": {"namespace": "ns1", "name": "other", "uid": "uid-other"}},
                   "status": {"phase": "Bound"}}),
        )
        .await;
        put_claim(&store, "db", csi_claim_spec()).await;
        let driver = Arc::new(RecordingDriver::default());

        let outcome = binder(store.clone())
            .with_csi(driver.clone())
            .tick()
            .await
            .unwrap();

        assert!(driver.names.lock().unwrap().is_empty());
        assert_ne!(phase(&store, "db").await, "Bound");
        assert_eq!(outcome.sweep.unwrap().note(), Some(PV_NAME_TAKEN));
    }

    /// A CSI driver keys `CreateVolume` idempotency on the name it is given.
    /// A recreated claim must ask under a new name, or the driver hands it
    /// the deleted claim's disk.
    #[tokio::test]
    async fn a_recreated_csi_claim_asks_the_driver_for_a_new_volume() {
        let store = live_store().await;
        csi_class(&store).await;
        let csi_claim = csi_claim_spec();
        let driver = Arc::new(RecordingDriver::default());
        let c = binder(store.clone()).with_csi(driver.clone());

        put_claim_as(&store, "db", json!("uid-db-1"), csi_claim.clone()).await;
        c.tick().await.unwrap();
        delete_claim(&store, "db").await;
        put_claim_as(&store, "db", json!("uid-db-2"), csi_claim).await;
        c.tick().await.unwrap();

        assert_eq!(
            *driver.names.lock().unwrap(),
            vec!["pvc-uid-db-1".to_string(), "pvc-uid-db-2".to_string()]
        );
        let pvc = store.get(&pvc_key("ns1", "db")).await.unwrap();
        assert_eq!(pvc["status"]["phase"], "Bound");
        assert_eq!(pvc["spec"]["volumeName"], "pvc-uid-db-2");
        let pv = store.get(&pv_key("pvc-uid-db-2")).await.unwrap();
        assert_eq!(pv["spec"]["csi"]["volumeHandle"], "vol-pvc-uid-db-2");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "uid-db-2");
    }
}

#[cfg(test)]
mod snapshot_restore_source {
    use super::PvBinderController;
    use serde_json::json;

    fn pvc_with(ds: serde_json::Value) -> serde_json::Value {
        json!({ "spec": { "dataSource": ds } })
    }

    #[test]
    fn a_volume_snapshot_data_source_is_recognised() {
        let pvc = pvc_with(json!({
            "apiGroup": "snapshot.storage.k8s.io",
            "kind": "VolumeSnapshot",
            "name": "db-snap"
        }));
        assert_eq!(
            PvBinderController::snapshot_data_source(&pvc).as_deref(),
            Some("db-snap")
        );
    }

    #[test]
    fn data_source_ref_is_accepted_too() {
        // Upstream added `dataSourceRef` as the general form; a cluster that
        // only read `dataSource` would silently provision an EMPTY volume for
        // a restore written the modern way.
        let pvc = json!({ "spec": { "dataSourceRef": {
            "apiGroup": "snapshot.storage.k8s.io",
            "kind": "VolumeSnapshot",
            "name": "db-snap"
        }}});
        assert_eq!(
            PvBinderController::snapshot_data_source(&pvc).as_deref(),
            Some("db-snap")
        );
    }

    /// ★ NEGATIVE CONTROL — the group is CHECKED. A same-named kind in some
    /// other group is not our snapshot, and treating it as one would restore
    /// from a directory that has nothing to do with it.
    #[test]
    fn a_foreign_api_group_is_not_our_snapshot() {
        let pvc = pvc_with(json!({
            "apiGroup": "example.com",
            "kind": "VolumeSnapshot",
            "name": "db-snap"
        }));
        assert_eq!(PvBinderController::snapshot_data_source(&pvc), None);
    }

    /// ★ NEGATIVE CONTROL — a PVC CLONE (`kind: PersistentVolumeClaim`) is a
    /// different feature. Silently treating it as a snapshot restore would
    /// look up a snapshot that does not exist and leave the claim Pending
    /// with a misleading note.
    #[test]
    fn a_pvc_clone_data_source_is_not_a_snapshot() {
        let pvc = pvc_with(json!({
            "apiGroup": "",
            "kind": "PersistentVolumeClaim",
            "name": "other"
        }));
        assert_eq!(PvBinderController::snapshot_data_source(&pvc), None);
    }

    #[test]
    fn a_plain_pvc_has_no_data_source() {
        assert_eq!(
            PvBinderController::snapshot_data_source(&json!({"spec": {}})),
            None
        );
        assert_eq!(PvBinderController::snapshot_data_source(&json!({})), None);
    }
}
