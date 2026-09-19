//! The volume half of the persistentvolume-controller: a PV whose claim is
//! gone goes `Released`, and its reclaim policy decides what follows.
//!
//! ★ THE DEFECT. The binder only ever looked at claims. Delete a claim and
//! its PV stayed `Bound` forever to an object that no longer existed, and
//! its local-path directory stayed on the host with nothing left to say
//! whose it was — although the seeded default `StorageClass` says `Delete`,
//! nothing deleted.
//!
//! ★ THE SHAPE. Upstream's `syncVolume`, the subset engenho runs:
//!
//! ```text
//!   Bound ──claim gone──▶ Released ──Delete──▶ backing storage removed, PV deleted
//!                                   ──Retain──▶ stays Released
//!                                   ──(cannot)─▶ Failed, one VolumeFailedDelete Event
//!   Released | Failed ──operator clears claimRef (or its uid)──▶ Available
//! ```
//!
//! One step per pass: the write that releases a volume wakes the binder
//! again, and the reclaim runs on the volume as that write left it.
//!
//! ★ "THE CLAIM IS GONE" DESTROYS DATA, SO IT IS NEVER READ OFF A LIST.
//!
//!   * A claim is identified by the uid its PV's `claimRef` recorded. Its
//!     namespace and name alone are not the claim: a claim recreated under
//!     the same name is a different claim, and the old volume is Released.
//!   * The tick's claim list only rules the claim alive. When it says the
//!     claim is gone the store is asked again for that one claim before
//!     anything is written (upstream re-reads from the apiserver for the
//!     same reason).
//!   * A binder scoped to one namespace never judges a volume whose claim
//!     lives in another: it did not list that namespace's claims.
//!   * A claim that is Terminating still exists. pvc-protection holds it
//!     while a pod uses it, and its volume stays Bound until it is gone.
//!   * Before backing storage is removed the PV is read again; one that
//!     moved since it was listed waits for the next pass, and the PV delete
//!     itself is at the revision read.
//!
//! ★ WHAT MAY BE DELETED IS A TYPE. The local-path deleter removes a
//! [`LocalPathDir`], and the only constructor of one derives it from the
//! uid, namespace and name the claimRef recorded ([`ClaimUid::recorded`],
//! [`PvName::local_path_dir`]). A PV whose name or `hostPath` is not that
//! derivation — a legacy `pvc-<ns>-<name>` volume, or a hand-written PV
//! pointing at `/` — is not this provisioner's to delete: it goes `Failed`
//! and its directory is left alone. A PV provisioned by a CSI driver is
//! deleted through that driver when this node serves it; one provisioned
//! by anything else is left `Released` for its own deleter, as upstream
//! leaves an external provisioner's volume.
//!
//! Tier-honest: the directory being derivable is a type; the checks that
//! the claim is gone and the volume unmoved are reads, which narrow the
//! window a concurrent writer has but, as upstream's, cannot close it.
//! Not run: `Recycle` (deprecated upstream, and it needs a scrubber pod)
//! goes `Failed`; a claim that exists but is bound to another volume
//! leaves this one as it is.

use std::fmt;

use engenho_store::{
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};
use shigoto_types::failure::FailureKind;
use tracing::debug;

use super::{ClaimUid, ENGENHO_LOCAL_PATH_PROVISIONER, PvBinderController, PvName};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::meta::ObjectMeta;
use crate::status::resource_version_of;
use crate::sweep::ObjectOutcome;

/// The annotation a provisioner signs the PVs it creates with.
pub(super) const PROVISIONED_BY: &str = "pv.kubernetes.io/provisioned-by";

const RELEASED: &str = "Released";
const FAILED: &str = "Failed";
const AVAILABLE: &str = "Available";

/// The report note for a PV whose claim lives outside this binder's scope.
const OUT_OF_SCOPE: &str = "PV's claim lives in a namespace this binder does not watch; its \
     lifecycle is left to the binder that does";

/// The report note for a PV whose `claimRef` cannot be read as a claim.
const UNREADABLE_CLAIM_REF: &str = "PV's spec.claimRef is not an object naming a namespace, a \
     name and a string uid; nothing is released or reclaimed on an unreadable reference";

/// The report note for a PV that cannot be written at a known revision.
const NO_REVISION: &str = "PV carries no resourceVersion, so it cannot be written at the \
     revision it was read at; left for the next pass";

/// The report note for a PV that moved between its listing and its reclaim.
const VOLUME_MOVED: &str = "PV changed since it was listed; its reclaim waits for the next pass";

/// The report note for a Released PV another provisioner deletes.
const EXTERNAL_DELETER: &str = "PV was provisioned by a provisioner this node does not run; it \
     stays Released for that provisioner to delete";

/// A PV's `spec.persistentVolumeReclaimPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimPolicy {
    /// Keep the volume, Released, until an operator frees it.
    Retain,
    /// Remove the backing storage, then the PV.
    Delete,
    /// `Recycle`, or a policy with no meaning: nothing engenho runs.
    Unsupported,
}

impl ReclaimPolicy {
    /// The policy `pv` declares. Absent means `Retain`: the API's default
    /// for a PV, and the one reading of a missing field that never
    /// destroys data.
    #[must_use]
    pub fn of(pv: &Value) -> Self {
        match pv
            .get("spec")
            .and_then(|s| s.get("persistentVolumeReclaimPolicy"))
        {
            None | Some(Value::Null) => Self::Retain,
            Some(Value::String(p)) if p == "Retain" => Self::Retain,
            Some(Value::String(p)) if p == "Delete" => Self::Delete,
            Some(_) => Self::Unsupported,
        }
    }
}

/// Why a Released volume cannot be reclaimed at all. It goes `Failed`
/// with [`message`](Self::message) as its `status.message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unreclaimable {
    /// The reclaim policy is `Recycle`, or no policy engenho knows.
    UnsupportedPolicy,
    /// No provisioner signed the PV and engenho knows no deleter for its
    /// source.
    NoDeleter,
    /// Signed by the local-path provisioner, but its name or `hostPath` is
    /// not the one that provisioner derives from its `claimRef`.
    ForeignDirectory,
}

impl Unreclaimable {
    /// A fixed description: the volume's `status.message`, and the Event's.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::UnsupportedPolicy => {
                "persistentVolumeReclaimPolicy is neither Retain nor Delete; engenho does not \
                 recycle volumes, so the volume is left for an operator"
            }
            Self::NoDeleter => {
                "no provisioner signed this volume and engenho has no deleter for its source; \
                 the backing storage is left for an operator"
            }
            Self::ForeignDirectory => {
                "the volume's name or hostPath is not the directory the local-path provisioner \
                 derives from its claimRef, so it is not the provisioner's to delete"
            }
        }
    }
}

impl fmt::Display for Unreclaimable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

/// Why a Released volume was not reclaimed this pass.
#[derive(Debug, thiserror::Error)]
pub enum ReclaimError {
    /// It cannot be reclaimed at all; it has gone `Failed`.
    #[error("{0}")]
    Unreclaimable(Unreclaimable),
    /// Removing its local-path directory failed.
    #[error("removing the local-path directory failed: {0}")]
    RemoveDir(#[source] std::io::Error),
    /// The CSI driver refused `DeleteVolume`.
    #[error("CSI DeleteVolume through {driver} failed: {reason}")]
    CsiDelete { driver: String, reason: String },
}

impl ReclaimError {
    /// Its retry class, by variant: a volume that cannot be reclaimed stays
    /// that way until it is edited; a deleter that failed may not next time.
    #[must_use]
    pub const fn class(&self) -> FailureKind {
        match self {
            Self::Unreclaimable(_) => FailureKind::Declarative,
            Self::RemoveDir(_) | Self::CsiDelete { .. } => FailureKind::Transient,
        }
    }
}

/// Where a PV's `claimRef` points, read from the volume's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Holder<'v> {
    /// No claimRef, or one without a uid: reserved by name at most, bound
    /// to no claim object.
    Nobody,
    /// The claim the volume was bound to.
    Claim(RecordedClaim<'v>),
    /// A claimRef that does not read as a claim. Nothing is done on it.
    Unreadable,
}

/// A claim as a PV's `claimRef` recorded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedClaim<'v> {
    namespace: &'v str,
    name: &'v str,
    uid: &'v str,
}

impl<'v> Holder<'v> {
    fn of(pv: &'v Value) -> Self {
        let claim_ref = match pv.get("spec").and_then(|s| s.get("claimRef")) {
            None | Some(Value::Null) => return Self::Nobody,
            Some(Value::Object(claim_ref)) => claim_ref,
            Some(_) => return Self::Unreadable,
        };
        let uid = match claim_ref.get("uid") {
            None | Some(Value::Null) => return Self::Nobody,
            Some(Value::String(uid)) if uid.is_empty() => return Self::Nobody,
            Some(Value::String(uid)) => uid.as_str(),
            Some(_) => return Self::Unreadable,
        };
        let field = |k: &str| {
            claim_ref
                .get(k)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
        };
        match (field("namespace"), field("name")) {
            (Some(namespace), Some(name)) => Self::Claim(RecordedClaim {
                namespace,
                name,
                uid,
            }),
            _ => Self::Unreadable,
        }
    }
}

/// Is the recorded claim among `claims`, with its uid?
fn listed_alive(claims: &[(ResourceKey, Value)], claim: &RecordedClaim<'_>) -> bool {
    claims.iter().any(|(key, value)| {
        key.namespace.as_deref() == Some(claim.namespace)
            && key.name == claim.name
            && value.uid() == Some(claim.uid)
    })
}

/// What removing a volume's backing storage came to.
enum Removal {
    /// Removed (or already absent): the PV can go.
    Removed,
    /// Another provisioner's: left for it.
    External,
    /// Not removable by anyone engenho knows of.
    Unreclaimable(Unreclaimable),
}

/// The phases this module writes without a message.
#[derive(Debug, Clone, Copy)]
enum Phase {
    Available,
    Released,
}

impl Phase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Available => AVAILABLE,
            Self::Released => RELEASED,
        }
    }
}

impl PvBinderController {
    /// One step of `pv`'s lifecycle against the claims listed this tick.
    pub(super) async fn reconcile_volume(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        claims: &[(ResourceKey, Value)],
    ) -> Result<ObjectOutcome, ControllerError> {
        let released = matches!(Self::pv_phase(pv), RELEASED | FAILED);
        let claim = match Holder::of(pv) {
            // An operator freed a Released volume (upstream's manual
            // reclaim): it is available again, to the claim it still names
            // if it names one.
            Holder::Nobody if released => {
                return self.set_phase(pv_key, pv, Phase::Available).await;
            }
            Holder::Nobody => return Ok(ObjectOutcome::Unchanged),
            Holder::Unreadable => return Ok(ObjectOutcome::skipped_because(UNREADABLE_CLAIM_REF)),
            Holder::Claim(claim) => claim,
        };
        if self
            .namespace
            .as_deref()
            .is_some_and(|scope| scope != claim.namespace)
        {
            return Ok(ObjectOutcome::skipped_because(OUT_OF_SCOPE));
        }
        if listed_alive(claims, &claim) || self.claim_exists(&claim).await {
            return Ok(ObjectOutcome::Unchanged);
        }
        if !released {
            debug!(pv = %pv_key.name, claim = claim.name, "claim is gone; volume released");
            return self.set_phase(pv_key, pv, Phase::Released).await;
        }
        match ReclaimPolicy::of(pv) {
            ReclaimPolicy::Retain => Ok(ObjectOutcome::Unchanged),
            ReclaimPolicy::Delete => self.delete_volume(pv_key, pv, &claim).await,
            ReclaimPolicy::Unsupported => {
                self.fail(pv_key, pv, Unreclaimable::UnsupportedPolicy)
                    .await
            }
        }
    }

    /// Ask the store itself whether the recorded claim exists. The tick's
    /// list said it does not; that list is not allowed to be the reason a
    /// volume is released.
    async fn claim_exists(&self, claim: &RecordedClaim<'_>) -> bool {
        let key = ResourceKey::namespaced(
            "",
            "v1",
            "PersistentVolumeClaim",
            claim.namespace,
            claim.name,
        );
        self.store
            .get(&key)
            .await
            .is_some_and(|live| live.uid() == Some(claim.uid))
    }

    /// Remove a Released volume under `Delete`: its backing storage first,
    /// then the PV, at the revision it was listed at.
    async fn delete_volume(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        claim: &RecordedClaim<'_>,
    ) -> Result<ObjectOutcome, ControllerError> {
        let Some(revision) = resource_version_of(pv) else {
            return Ok(ObjectOutcome::skipped_because(NO_REVISION));
        };
        let live = self.store.get(pv_key).await;
        if live.as_ref().and_then(resource_version_of) != Some(revision) {
            return Ok(ObjectOutcome::skipped_because(VOLUME_MOVED));
        }
        match self.remove_backing(pv_key, pv, claim).await? {
            Removal::External => Ok(ObjectOutcome::skipped_because(EXTERNAL_DELETER)),
            Removal::Unreclaimable(why) => self.fail(pv_key, pv, why).await,
            Removal::Removed => {
                debug!(pv = %pv_key.name, "backing storage removed; deleting the volume");
                let applied = self
                    .store
                    .propose(ResourceCommand::delete_at(
                        pv_key.clone(),
                        Some(revision),
                        Reason::Controller,
                        Some(engenho_types::time::now_rfc3339_utc()),
                    ))
                    .await?;
                Ok(ObjectOutcome::from(Effect::of(applied.op)))
            }
        }
    }

    /// Remove the storage behind `pv`, through the deleter its provisioner
    /// names (upstream's `findDeletablePlugin`).
    async fn remove_backing(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        claim: &RecordedClaim<'_>,
    ) -> Result<Removal, ReclaimError> {
        let provisioner = pv
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(|a| a.get(PROVISIONED_BY))
            .and_then(Value::as_str);
        let spec = pv.get("spec");
        match provisioner {
            None => Ok(Removal::Unreclaimable(Unreclaimable::NoDeleter)),
            Some(ENGENHO_LOCAL_PATH_PROVISIONER) => {
                let host_path = spec
                    .and_then(|s| s.get("hostPath"))
                    .and_then(|h| h.get("path"))
                    .and_then(Value::as_str);
                let Ok(uid) = ClaimUid::recorded(claim.uid) else {
                    return Ok(Removal::Unreclaimable(Unreclaimable::ForeignDirectory));
                };
                let volume = PvName::for_claim(&uid);
                let Ok(dir) =
                    volume.local_path_dir(&self.local_path_root, claim.namespace, claim.name)
                else {
                    return Ok(Removal::Unreclaimable(Unreclaimable::ForeignDirectory));
                };
                if !volume.names(&pv_key.name) || host_path != Some(dir.to_string().as_str()) {
                    return Ok(Removal::Unreclaimable(Unreclaimable::ForeignDirectory));
                }
                self.env.remove_dir(&dir).map_err(ReclaimError::RemoveDir)?;
                Ok(Removal::Removed)
            }
            Some(provisioner) => {
                let csi = spec.and_then(|s| s.get("csi"));
                let driver = csi.and_then(|c| c.get("driver")).and_then(Value::as_str);
                let handle = csi
                    .and_then(|c| c.get("volumeHandle"))
                    .and_then(Value::as_str);
                match (driver, handle) {
                    (Some(driver), Some(handle))
                        if driver == provisioner && self.csi.can_provision(driver).await =>
                    {
                        self.csi
                            .delete_volume(driver, handle)
                            .await
                            .map_err(|reason| ReclaimError::CsiDelete {
                                driver: driver.to_owned(),
                                reason,
                            })?;
                        Ok(Removal::Removed)
                    }
                    _ => Ok(Removal::External),
                }
            }
        }
    }

    /// Write `phase` at the revision `pv` was read at, clearing any message
    /// a `Failed` left.
    async fn set_phase(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        phase: Phase,
    ) -> Result<ObjectOutcome, ControllerError> {
        let patch = json!({ "status": { "phase": phase.as_str(), "message": null } });
        self.patch_status(pv_key, pv, patch).await
    }

    /// Mark `pv` Failed with `why` as its message. The pass that writes it
    /// reports the failure, so the sweep puts one `VolumeFailedDelete`
    /// Event on the volume; a volume already Failed for the same reason is
    /// converged, and says nothing more.
    async fn fail(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        why: Unreclaimable,
    ) -> Result<ObjectOutcome, ControllerError> {
        let status = pv.get("status");
        let already = status.and_then(|s| s.get("phase")).and_then(Value::as_str) == Some(FAILED)
            && status
                .and_then(|s| s.get("message"))
                .and_then(Value::as_str)
                == Some(why.message());
        if already {
            return Ok(ObjectOutcome::Unchanged);
        }
        let patch = json!({ "status": { "phase": FAILED, "message": why.message() } });
        match self.patch_status(pv_key, pv, patch).await? {
            ObjectOutcome::Changed(_) => Err(ReclaimError::Unreclaimable(why).into()),
            other => Ok(other),
        }
    }

    async fn patch_status(
        &self,
        pv_key: &ResourceKey,
        pv: &Value,
        patch: Value,
    ) -> Result<ObjectOutcome, ControllerError> {
        let Some(revision) = resource_version_of(pv) else {
            return Ok(ObjectOutcome::skipped_because(NO_REVISION));
        };
        let applied = self
            .store
            .propose(ResourceCommand::patch_cas(
                pv_key.clone(),
                patch,
                Some(revision),
                Reason::Controller,
            ))
            .await?;
        Ok(ObjectOutcome::from(Effect::of(applied.op)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_policy_retains_and_only_delete_deletes() {
        let pv = |p: Value| json!({"spec": {"persistentVolumeReclaimPolicy": p}});
        assert_eq!(ReclaimPolicy::of(&json!({})), ReclaimPolicy::Retain);
        assert_eq!(ReclaimPolicy::of(&pv(Value::Null)), ReclaimPolicy::Retain);
        assert_eq!(
            ReclaimPolicy::of(&pv(json!("Retain"))),
            ReclaimPolicy::Retain
        );
        assert_eq!(
            ReclaimPolicy::of(&pv(json!("Delete"))),
            ReclaimPolicy::Delete
        );
        for other in [json!("Recycle"), json!("delete"), json!(""), json!(1)] {
            assert_eq!(
                ReclaimPolicy::of(&pv(other.clone())),
                ReclaimPolicy::Unsupported,
                "{other}"
            );
        }
    }

    #[test]
    fn a_claim_ref_is_a_claim_only_with_a_uid_namespace_and_name() {
        let pv = |cr: Value| json!({"spec": {"claimRef": cr}});
        assert_eq!(Holder::of(&json!({"spec": {}})), Holder::Nobody);
        assert_eq!(Holder::of(&pv(Value::Null)), Holder::Nobody);
        assert_eq!(
            Holder::of(&pv(json!({"namespace": "ns1", "name": "data"}))),
            Holder::Nobody,
            "reserved by name is bound to no claim object"
        );
        assert_eq!(
            Holder::of(&pv(json!({"namespace": "ns1", "name": "data", "uid": ""}))),
            Holder::Nobody
        );
        assert_eq!(
            Holder::of(&pv(
                json!({"namespace": "ns1", "name": "data", "uid": "u-1"})
            )),
            Holder::Claim(RecordedClaim {
                namespace: "ns1",
                name: "data",
                uid: "u-1"
            })
        );
        for unreadable in [
            json!("ns1/data"),
            json!({"namespace": "ns1", "name": "data", "uid": 7}),
            json!({"name": "data", "uid": "u-1"}),
            json!({"namespace": "ns1", "name": "", "uid": "u-1"}),
        ] {
            assert_eq!(
                Holder::of(&pv(unreadable.clone())),
                Holder::Unreadable,
                "{unreadable}"
            );
        }
    }
}
