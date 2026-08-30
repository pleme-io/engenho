//! Pod-volume resolution — the TYPED-SPEC + INTERPRETER TRIPLET for the
//! `emptyDir` / `configMap` / `secret` volume classes.
//!
//! This module is DISTINCT from [`crate::volume`] (the PVC / CSI storage
//! brick): that one owns `PersistentVolumeClaim` sources with an
//! independent lifecycle; THIS one owns the three *pod-lifetime* ephemeral
//! sources the kubelet materializes at pod-start and bind-mounts into the
//! pod's containers. No overlap, no fork.
//!
//! ## The triplet
//!
//! 1. **Typed border** — [`PodVolumeSource`] (one populated arm per
//!    Volume; >1 arm is rejected as [`VolumeResolveError::MultipleSources`])
//!    + [`ResolvedMount`] / [`MountSource`] (the thing stamped onto
//!    [`crate::backend::ContainerSpec::mounts`]) + [`VolumeResolveError`].
//! 2. **Pure resolver / interpreter** — [`resolve_pod_volumes`], which
//!    maps `(pod.spec.volumes[] + fetched ConfigMap/Secret data)` →
//!    a `BTreeMap<volName, MountSource>`. Mockable side effects (file
//!    writes, `podman volume create`) live behind the
//!    [`VolumeMaterializer`] trait — the trait IS the testability
//!    contract, so the WHOLE resolution is unit-testable WITHOUT real
//!    podman (the [`FakeVolumeMaterializer`]).
//! 3. **Working materializer** — [`PodmanVolumeMaterializer`] writes the
//!    decoded files under a `$HOME`-rooted data dir + creates a podman
//!    named volume for emptyDir. (The `$HOME` root is the ONE host-specific
//!    knob; the podman machine auto-shares `$HOME` but NOT `/tmp`, so the
//!    data root must live under `$HOME` for the bind mount to resolve.)
//!
//! ## No silent wrong answers
//!
//! A referenced configMap/secret that does not exist (and is not marked
//! `optional`) returns a typed [`VolumeResolveError`] whose
//! [`VolumeResolveError::pending_reason`] maps to a K8s-style
//! `containerStatuses[].state.waiting.reason` — the kubelet keeps the pod
//! `Pending` with that reason (e.g. `ConfigMapNotFound`), NEVER a fake
//! `Running` and NEVER a crash. There is no `todo!()` / `panic!()` /
//! placeholder `Ok` anywhere in the resolution path; every typed-deferred
//! source (hostPath / PVC / projected / downwardAPI) returns its own typed
//! Pending reason rather than mis-serving.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use engenho_types::generated_v1_34::core_v1::{ConfigMap, Secret};
use engenho_types::generated_v1_34::{
    EmptyDirVolumeSource, KeyToPath, SecretVolumeSource, Volume, VolumeMount,
};

/// Where a [`ResolvedMount`] sources its bytes from on the host. The two
/// arms map to the two podman `-v` shapes:
///
///   * [`MountSource::HostDir`] → `-v <abs host path>:<mountPath>[:ro]`
///     (configMap / secret materialized as files under the $HOME data
///     root; the podman machine shares $HOME so the bind resolves).
///   * [`MountSource::NamedVolume`] → `-v <volume name>:<mountPath>`
///     (emptyDir as a podman named volume, shared read-write across every
///     container in the pod that references it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MountSource {
    /// An absolute host-filesystem directory (or file) to bind-mount.
    /// configMap / secret sources — default read-only (K8s semantics).
    HostDir(PathBuf),
    /// A named podman volume (created via `podman volume create`).
    /// emptyDir sources — default read-write, shared across the pod.
    NamedVolume(String),
    /// A bound-PVC backing directory bind-mounted from the PV's node-local
    /// `hostPath`/`local` source — `-v <pv path>:<mountPath>`. Distinct from
    /// [`MountSource::HostDir`] only in its DEFAULT read-write semantics
    /// (a PVC is read-write unless the volume/volumeMount forces read-only),
    /// where configMap/secret default read-only. `read_only` carries the
    /// `persistentVolumeClaim.readOnly` flag (forces RO regardless of the
    /// volumeMount).
    PvcHostDir {
        /// Absolute host path of the bound PV's `hostPath`/`local` source dir.
        path: PathBuf,
        /// `persistentVolumeClaim.readOnly` — forces the mount read-only.
        read_only: bool,
    },
}

/// One fully-resolved mount: the materialized source + the container's
/// `mountPath` + read-only flag + optional subPath. Stamped onto
/// [`crate::backend::ContainerSpec::mounts`] by `pod_to_container_specs`
/// after resolution; [`crate::backend::PodmanBackend::run_argv`] turns each
/// into a `-v` argv pair. A pod with no volumes produces an empty
/// `Vec<ResolvedMount>` → byte-identical argv to before this brick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedMount {
    /// Materialized host source (a dir/file path or a named volume).
    pub source: MountSource,
    /// Path inside the container the source mounts at (`volumeMount.mountPath`).
    pub mount_path: String,
    /// Mount read-only (`-v src:dst:ro`). configMap/secret default to
    /// read-only (K8s semantics); emptyDir is read-write.
    pub read_only: bool,
    /// `volumeMount.subPath` — the single-element subdir/file the container
    /// sees. Folded into [`MountSource::HostDir`] at materialize time when
    /// easy; carried here for diagnostics + future complex-subPath handling.
    pub sub_path: Option<String>,
}

/// The typed volume source the resolver dispatches on — exactly one
/// populated arm per [`Volume`]. Built by [`PodVolumeSource::from_volume`],
/// which rejects a Volume carrying >1 source arm as
/// [`VolumeResolveError::MultipleSources`] (K8s forbids it).
///
/// IN-SCOPE arms ([`ConfigMap`](PodVolumeSource::ConfigMap) /
/// [`Secret`](PodVolumeSource::Secret) / [`EmptyDir`](PodVolumeSource::EmptyDir))
/// are live-provable on this host. The DEFERRED arms each carry a typed
/// Pending reason (never a fake success):
///
///   * [`PodVolumeSource::DownwardApi`] → `"DownwardApiUnsupported"`
///   * [`PodVolumeSource::Projected`] → `"ProjectedUnsupported"`
///   * [`PodVolumeSource::HostPath`] → `"HostPathUnsupported"` (host-fs
///     exposure risk; deferred until an explicit allowlist lands)
///
/// The [`PodVolumeSource::Pvc`] arm is LIVE: it resolves the PVC's bound PV
/// (via the `fetch` seam) to the PV's node-local `hostPath`/`local` source
/// path and produces a [`MountSource::HostDir`]. An unbound PVC keeps the
/// pod Pending (`"PvcNotBound"`); a PVC bound to a PV with an unsupported
/// source class (CSI/nfs/…) keeps it Pending (`"PvcSourceUnsupported"`) —
/// never a fake mount. See [`crate::volume`] for the CSI-style storage trait
/// (independent lifecycle).
///
/// `PartialEq` only (not `Eq`): the `items: Vec<KeyToPath>` payload mirrors
/// the upstream `KeyToPath` struct, which derives `PartialEq` but not `Eq`.
#[derive(Clone, Debug, PartialEq)]
pub enum PodVolumeSource {
    /// `configMap` — keys of `.data` (+ base64 `.binaryData`) → files.
    ConfigMap {
        /// Referenced `ConfigMap` name (`configMap.name`).
        name: String,
        /// `optional == true` ⇒ a missing source materializes empty.
        optional: bool,
        /// `items[]` `KeyToPath` subset; empty ⇒ all of `.data`/`.binaryData`.
        items: Vec<KeyToPath>,
    },
    /// `secret` — keys of `.data` (base64-decoded) → files.
    Secret {
        /// Referenced `Secret` name (`secret.secretName`).
        name: String,
        /// `optional == true` ⇒ a missing source materializes empty.
        optional: bool,
        /// `items[]` `KeyToPath` subset; empty ⇒ all of `.data`.
        items: Vec<KeyToPath>,
    },
    /// `emptyDir` — a per-pod scratch volume shared across containers.
    EmptyDir {
        /// `medium` (`""` = node default; `Memory` = tmpfs — typed-deferred
        /// nuance: served as a disk-backed named volume with a note).
        medium: Option<String>,
    },
    /// `downwardAPI` — typed-deferred (`"DownwardApiUnsupported"`).
    DownwardApi,
    /// `projected` — typed-deferred (`"ProjectedUnsupported"`).
    Projected,
    /// `persistentVolumeClaim` — resolved to the bound PV's node-local
    /// `hostPath`/`local` source dir (a [`MountSource::HostDir`]). An unbound
    /// PVC / unsupported-source PV stay Pending (never a fake mount).
    Pvc {
        /// Referenced `PersistentVolumeClaim` name (`persistentVolumeClaim.claimName`),
        /// in the pod's namespace.
        claim_name: String,
        /// `persistentVolumeClaim.readOnly` — forces the mount read-only.
        read_only: bool,
    },
    /// `hostPath` — typed-deferred (`"HostPathUnsupported"`; host-fs risk).
    HostPath,
}

impl PodVolumeSource {
    /// Project a typed [`Volume`] into its single populated source arm.
    ///
    /// # Errors
    ///
    /// [`VolumeResolveError::MultipleSources`] when more than one source
    /// field is populated; [`VolumeResolveError::NoSource`] when none is
    /// (a malformed Volume — never silently treated as empty).
    pub fn from_volume(vol: &Volume) -> Result<Self, VolumeResolveError> {
        // Count the populated source arms; >1 is illegal per K8s. We only
        // need to DISPATCH on the in-scope + named-deferred arms — any other
        // populated source field still counts toward the >1 guard via the
        // `populated` tally so we never silently pick one of two.
        let mut found: Option<PodVolumeSource> = None;
        let mut populated = 0usize;

        if let Some(cm) = &vol.config_map {
            populated += 1;
            found = Some(PodVolumeSource::ConfigMap {
                name: cm.name.clone().unwrap_or_default(),
                optional: cm.optional.unwrap_or(false),
                items: cm.items.clone(),
            });
        }
        if let Some(sec) = &vol.secret {
            populated += 1;
            found = Some(secret_arm(sec));
        }
        if let Some(ed) = &vol.empty_dir {
            populated += 1;
            found = Some(empty_dir_arm(ed));
        }
        if vol.downward_api.is_some() {
            populated += 1;
            found = Some(PodVolumeSource::DownwardApi);
        }
        if vol.projected.is_some() {
            populated += 1;
            found = Some(PodVolumeSource::Projected);
        }
        if let Some(pvc) = &vol.persistent_volume_claim {
            populated += 1;
            found = Some(PodVolumeSource::Pvc {
                claim_name: pvc.claim_name.clone(),
                read_only: pvc.read_only.unwrap_or(false),
            });
        }
        if vol.host_path.is_some() {
            populated += 1;
            found = Some(PodVolumeSource::HostPath);
        }

        if populated > 1 {
            return Err(VolumeResolveError::MultipleSources {
                vol: vol.name.clone(),
            });
        }
        found.ok_or_else(|| VolumeResolveError::NoSource {
            vol: vol.name.clone(),
        })
    }
}

fn secret_arm(sec: &SecretVolumeSource) -> PodVolumeSource {
    PodVolumeSource::Secret {
        name: sec.secret_name.clone().unwrap_or_default(),
        optional: sec.optional.unwrap_or(false),
        items: sec.items.clone(),
    }
}

fn empty_dir_arm(ed: &EmptyDirVolumeSource) -> PodVolumeSource {
    PodVolumeSource::EmptyDir {
        medium: ed.medium.clone(),
    }
}

/// Typed volume-resolution failures. Each maps to a K8s-style pod-Pending
/// `waiting.reason` via [`VolumeResolveError::pending_reason`] so a missing
/// or unsupported source surfaces mechanically — never a fake Running, a
/// crash, or a silent empty mount.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum VolumeResolveError {
    /// A referenced `ConfigMap` is absent + not `optional`. Pending reason
    /// `"ConfigMapNotFound"`.
    #[error("configMap not found: {name}")]
    ConfigMapNotFound {
        /// The missing `ConfigMap`'s name.
        name: String,
    },
    /// A referenced `Secret` is absent + not `optional`. Pending reason
    /// `"SecretNotFound"`.
    #[error("secret not found: {name}")]
    SecretNotFound {
        /// The missing Secret's name.
        name: String,
    },
    /// A listed `items[].key` is absent from a present source + the source
    /// is not `optional`. Pending reason `"InvalidVolumeKey"`.
    #[error("key {key:?} not found in {src}")]
    KeyNotFound {
        /// `<kind>/<name>` of the source the key was looked up in.
        src: String,
        /// The missing key.
        key: String,
    },
    /// A Volume populated >1 source arm (K8s forbids it). Pending reason
    /// `"InvalidVolumeSource"`.
    #[error("volume {vol} declares multiple sources")]
    MultipleSources {
        /// The offending volume's name.
        vol: String,
    },
    /// A Volume populated NO recognized source arm. Pending reason
    /// `"InvalidVolumeSource"`.
    #[error("volume {vol} declares no source")]
    NoSource {
        /// The offending volume's name.
        vol: String,
    },
    /// A typed-deferred source class (hostPath / projected / downwardAPI /
    /// emptyDir medium nuance). Carries the named K8s-style Pending reason
    /// verbatim — the source is NOT served, surfaced honestly rather than
    /// mis-materialized.
    #[error("volume {vol} uses unsupported source ({reason})")]
    Unsupported {
        /// The offending volume's name.
        vol: String,
        /// The typed Pending reason (e.g. `"HostPathUnsupported"`).
        reason: &'static str,
    },
    /// A `persistentVolumeClaim` volume references a PVC that is not yet
    /// `Bound` (absent, `status.phase != Bound`, or no `spec.volumeName`).
    /// The pod waits — like the `ConfigMapNotFound` path — until the
    /// PV/PVC binder converges it. Pending reason `"PvcNotBound"`.
    #[error("PVC not bound: {claim} (vol {vol})")]
    PvcNotBound {
        /// The offending volume's name.
        vol: String,
        /// The referenced claim name (in the pod's namespace).
        claim: String,
    },
    /// A `persistentVolumeClaim` is bound to a CSI PV, but this kubelet has
    /// no CSI plane wired (or the named driver never registered). Pending
    /// reason `"CsiUnavailable"` — distinct from `PvcSourceUnsupported`,
    /// which means engenho cannot serve the source class AT ALL. This one
    /// says the class is supported and THIS node cannot serve it, which is
    /// a completely different thing for an operator to act on.
    #[error("PVC volume {vol} needs CSI driver {driver}, which is not available on this node")]
    CsiUnavailable {
        /// The offending volume's name.
        vol: String,
        /// The driver the PV names.
        driver: String,
    },

    /// A `persistentVolumeClaim` is `Bound` but its PV carries a source class
    /// the kubelet can't mount node-locally (CSI / NFS / cloud disk / …; only
    /// `hostPath` + `local` are served). Pending reason
    /// `"PvcSourceUnsupported"` — never a fake mount.
    #[error("PVC {claim} bound PV {pv} has unsupported source (vol {vol})")]
    PvcSourceUnsupported {
        /// The offending volume's name.
        vol: String,
        /// The referenced claim name.
        claim: String,
        /// The bound PV's name.
        pv: String,
    },
    /// The materializer's host-side work (mkdir / file write / `podman
    /// volume create`) failed. Pending reason `"VolumeMaterializeError"`.
    #[error("materialize: {0}")]
    Materialize(String),
}

impl VolumeResolveError {
    /// The K8s-style `containerStatuses[].state.waiting.reason` this error
    /// surfaces on the pod. Stable strings — operators (and the live
    /// `kubectl get pod`) read these exactly.
    #[must_use]
    pub fn pending_reason(&self) -> &'static str {
        match self {
            VolumeResolveError::ConfigMapNotFound { .. } => "ConfigMapNotFound",
            VolumeResolveError::SecretNotFound { .. } => "SecretNotFound",
            VolumeResolveError::KeyNotFound { .. } => "InvalidVolumeKey",
            VolumeResolveError::MultipleSources { .. } | VolumeResolveError::NoSource { .. } => {
                "InvalidVolumeSource"
            }
            VolumeResolveError::Unsupported { reason, .. } => reason,
            VolumeResolveError::PvcNotBound { .. } => "PvcNotBound",
            VolumeResolveError::PvcSourceUnsupported { .. } => "PvcSourceUnsupported",
            VolumeResolveError::CsiUnavailable { .. } => "CsiUnavailable",
            VolumeResolveError::Materialize(_) => "VolumeMaterializeError",
        }
    }
}

engenho_substrate::impl_error_kind! {
    VolumeResolveError {
        { ConfigMapNotFound { .. } } => "config_map_not_found",
        { SecretNotFound { .. } } => "secret_not_found",
        { KeyNotFound { .. } } => "key_not_found",
        { MultipleSources { .. } } => "multiple_sources",
        { NoSource { .. } } => "no_source",
        { Unsupported { .. } } => "unsupported",
        { PvcNotBound { .. } } => "pvc_not_bound",
        { PvcSourceUnsupported { .. } } => "pvc_source_unsupported",
        { CsiUnavailable { .. } } => "csi_unavailable",
        (Materialize(_)) => "materialize",
    }
}

/// The materializer seam — the side-effecting half of the resolver, behind
/// a trait so resolution is unit-testable WITHOUT real podman. The real
/// [`PodmanVolumeMaterializer`] writes files + creates named volumes; the
/// [`FakeVolumeMaterializer`] records what it was asked to do + returns
/// deterministic [`MountSource`]s.
///
/// The two methods are the ONLY host effects volume resolution performs:
///
///   * [`materialize_files`](VolumeMaterializer::materialize_files) — write
///     a `(filename → bytes)` set into a per-pod-per-volume host dir, return
///     the [`MountSource::HostDir`] to bind-mount. configMap + secret share
///     this (secret decodes before calling; the materializer sees raw bytes).
///   * [`ensure_empty_dir`](VolumeMaterializer::ensure_empty_dir) —
///     idempotently create a per-pod-volume named volume, return the
///     [`MountSource::NamedVolume`]. Shared across the pod's containers.
/// The standard path upstream mounts a pod's ServiceAccount credentials at.
///
/// `kube-rs`, client-go and every other in-cluster client look here and
/// nowhere else. `Config::incluster()` reads `namespace` first, then `token`,
/// then `ca.crt` — which is why an engenho pod with the service env set but
/// no files reports `ReadDefaultNamespace(NotFound)` rather than a token
/// error.
pub const SA_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Supplies the files for a pod's projected ServiceAccount volume.
///
/// A SEAM, not a concrete type, because issuing a token needs the cluster's
/// ed25519 signing key which lives in `engenho-apiserver` — and
/// `engenho-kubelet` does not depend on it, deliberately. The runtime holds
/// both and implements this; tests inject a fake.
///
/// Returning `None` means this pod gets NO ServiceAccount projection, the
/// honest state for a runtime with no signing key. It must never mean
/// "project an empty token": a zero-byte token file is WORSE than an absent
/// one, because the client stops looking for a kubeconfig and then fails
/// authentication instead of falling back.
#[async_trait]
pub trait ServiceAccountProjector: Send + Sync {
    /// The files to place at [`SA_MOUNT_PATH`] — upstream projects exactly
    /// `token`, `ca.crt` and `namespace`.
    ///
    /// # Errors
    ///
    /// Any failure to mint a token. Surfaces as the pod-Pending reason, never
    /// a silent skip — a pod that cannot get its identity must not reach
    /// Running and then fail every API call it makes.
    async fn project(
        &self,
        namespace: &str,
        service_account: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<BTreeMap<String, Vec<u8>>>, String>;
}

/// A projector that supplies nothing — the honest default for a kubelet with
/// no signing key wired.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoServiceAccountProjection;

#[async_trait]
impl ServiceAccountProjector for NoServiceAccountProjection {
    async fn project(
        &self,
        _namespace: &str,
        _service_account: &str,
        _pod_name: &str,
        _pod_uid: &str,
    ) -> Result<Option<BTreeMap<String, Vec<u8>>>, String> {
        Ok(None)
    }
}

#[async_trait]
pub trait VolumeMaterializer: Send + Sync {
    /// Stable identifier (telemetry).
    fn name(&self) -> &'static str;

    /// Materialize `files` (`filename → bytes`) for `volume` of pod
    /// `<namespace>/<pod>` into a host dir; return the [`MountSource`] to
    /// bind-mount read-only into each referencing container.
    ///
    /// # Errors
    ///
    /// [`VolumeResolveError::Materialize`] on any host-side failure (mkdir /
    /// write) — surfaces as the pod-Pending reason, never a silent skip.
    async fn materialize_files(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<MountSource, VolumeResolveError>;

    /// Idempotently ensure a named volume backs the emptyDir `volume` of pod
    /// `<namespace>/<pod>`; return the [`MountSource::NamedVolume`] every
    /// referencing container shares read-write.
    ///
    /// # Errors
    ///
    /// [`VolumeResolveError::Materialize`] on `podman volume create` failure.
    async fn ensure_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<MountSource, VolumeResolveError>;

    /// Ask a CSI driver to publish `req`'s volume for pod
    /// `<namespace>/<pod>`, returning the [`MountSource`] to bind-mount.
    ///
    /// ★ THE DEFAULT REFUSES BY NAME rather than returning a plausible
    /// path. A materializer with no CSI wiring genuinely cannot mount a CSI
    /// volume, and the failure modes of the alternatives are both bad: an
    /// empty dir would present as a silently-empty volume (data loss that
    /// looks like an application bug), and a panic would take the kubelet
    /// down for one misconfigured pod. A typed Pending keeps the pod
    /// waiting with a reason an operator can read.
    ///
    /// # Errors
    ///
    /// [`VolumeResolveError::CsiUnavailable`] when this materializer has no
    /// CSI plane; whatever the driver returned otherwise.
    async fn publish_csi(
        &self,
        _namespace: &str,
        _pod: &str,
        volume: &str,
        req: &CsiPublishRequest,
    ) -> Result<MountSource, VolumeResolveError> {
        Err(VolumeResolveError::CsiUnavailable {
            vol: volume.to_string(),
            driver: req.driver.clone(),
        })
    }

    /// The pod-delete counterpart of
    /// [`publish_csi`](VolumeMaterializer::publish_csi).
    ///
    /// An already-unpublished volume is SUCCESS: teardown runs on a path
    /// that may already have partially completed, and an error here would
    /// wedge pod deletion behind a volume that is already gone.
    ///
    /// # Errors
    ///
    /// Whatever the driver returned. The default is a no-op success,
    /// because a materializer that never published has nothing to undo.
    async fn unpublish_csi(
        &self,
        _namespace: &str,
        _pod: &str,
        _volume: &str,
    ) -> Result<(), VolumeResolveError> {
        Ok(())
    }

    /// Idempotently remove the named volume backing the emptyDir `volume` of
    /// pod `<namespace>/<pod>` — the pod-delete counterpart of
    /// [`ensure_empty_dir`](VolumeMaterializer::ensure_empty_dir). emptyDir is
    /// pod-lifetime scratch, so its named volume is reaped alongside the pod's
    /// containers (after they stop+remove). An already-absent volume is
    /// SUCCESS (idempotent — a re-run never errors).
    ///
    /// # Errors
    ///
    /// [`VolumeResolveError::Materialize`] on a `podman volume rm` failure that
    /// is NOT "no such volume" (a genuine host error the cleanup must retry).
    async fn remove_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError>;
}

/// Read the typed `Vec<Volume>` from a Pod's raw `spec.volumes` JSON. Absent
/// / empty ⇒ `Ok(vec![])` (the no-volume fast path). A present-but-malformed
/// `spec.volumes` (not an array of Volume objects) is a typed deserialization
/// error mapped to [`VolumeResolveError::Materialize`] so the pod surfaces it
/// rather than silently dropping the volumes.
///
/// # Errors
///
/// [`VolumeResolveError::Materialize`] if `spec.volumes` is present but does
/// not deserialize into `Vec<Volume>` (typed-emission-aligned: we go through
/// the EXISTING engenho-types `Volume` struct, not ad-hoc field plucking).
pub fn pod_volumes(pod: &Value) -> Result<Vec<Volume>, VolumeResolveError> {
    let Some(vols) = pod.get("spec").and_then(|s| s.get("volumes")) else {
        return Ok(Vec::new());
    };
    if vols.is_null() {
        return Ok(Vec::new());
    }
    serde_json::from_value::<Vec<Volume>>(vols.clone())
        .map_err(|e| VolumeResolveError::Materialize(format!("parse spec.volumes: {e}")))
}

/// Read the typed `Vec<VolumeMount>` from a container's raw `volumeMounts`
/// JSON. Absent ⇒ empty (the container mounts nothing).
///
/// # Errors
///
/// [`VolumeResolveError::Materialize`] if `volumeMounts` is present but does
/// not deserialize into `Vec<VolumeMount>`.
pub fn container_volume_mounts(container: &Value) -> Result<Vec<VolumeMount>, VolumeResolveError> {
    let Some(vms) = container.get("volumeMounts") else {
        return Ok(Vec::new());
    };
    if vms.is_null() {
        return Ok(Vec::new());
    }
    serde_json::from_value::<Vec<VolumeMount>>(vms.clone())
        .map_err(|e| VolumeResolveError::Materialize(format!("parse volumeMounts: {e}")))
}

/// The PURE resolver / interpreter — fetch the referenced ConfigMaps/Secrets
/// (via `fetch`), materialize each volume's source (via `materializer`), and
/// return a `volName → MountSource` map.
///
/// `fetch` is the in-process store read seam: `fetch(kind, name) → Option<Value>`
/// where `kind` is `"ConfigMap"` or `"Secret"`. The kubelet passes a closure
/// over `self.store.get(ResourceKey::namespaced(...))` — the SAME read the
/// kubelet already does for Services; no new store mechanism.
///
/// Empty / absent `spec.volumes` ⇒ `Ok(empty map)` (the no-volume fast path —
/// the caller then stamps `mounts: vec![]` and gets byte-identical behavior to
/// before this brick).
///
/// # Errors
///
/// Any [`VolumeResolveError`]: a missing non-optional source, a missing
/// non-optional key, a multi/no-source volume, a typed-deferred source, or a
/// materializer failure. The caller maps the error's
/// [`VolumeResolveError::pending_reason`] onto every container's
/// `waiting.reason` and keeps the pod Pending.
pub async fn resolve_pod_volumes<F>(
    pod: &Value,
    namespace: &str,
    pod_name: &str,
    fetch: F,
    materializer: &dyn VolumeMaterializer,
) -> Result<BTreeMap<String, MountSource>, VolumeResolveError>
where
    F: Fn(&str, &str) -> Option<Value>,
{
    let volumes = pod_volumes(pod)?;
    let mut out: BTreeMap<String, MountSource> = BTreeMap::new();

    for vol in &volumes {
        let source = PodVolumeSource::from_volume(vol)?;
        let mount_source = match source {
            PodVolumeSource::ConfigMap {
                name,
                optional,
                items,
            } => {
                let fetched = fetch("ConfigMap", &name);
                let files = match fetched {
                    Some(raw) => {
                        let cm: ConfigMap = serde_json::from_value(raw).map_err(|e| {
                            VolumeResolveError::Materialize(format!("parse ConfigMap {name}: {e}"))
                        })?;
                        configmap_files(&name, &cm, &items)?
                    }
                    None if optional => BTreeMap::new(),
                    None => {
                        return Err(VolumeResolveError::ConfigMapNotFound { name });
                    }
                };
                materializer
                    .materialize_files(namespace, pod_name, &vol.name, &files)
                    .await?
            }
            PodVolumeSource::Secret {
                name,
                optional,
                items,
            } => {
                let fetched = fetch("Secret", &name);
                let files = match fetched {
                    Some(raw) => {
                        let sec: Secret = serde_json::from_value(raw).map_err(|e| {
                            VolumeResolveError::Materialize(format!("parse Secret {name}: {e}"))
                        })?;
                        secret_files(&name, &sec, &items)?
                    }
                    None if optional => BTreeMap::new(),
                    None => {
                        return Err(VolumeResolveError::SecretNotFound { name });
                    }
                };
                materializer
                    .materialize_files(namespace, pod_name, &vol.name, &files)
                    .await?
            }
            PodVolumeSource::EmptyDir { medium } => {
                // medium:Memory is served as a disk-backed named volume — the
                // SHARING semantics are correct; the tmpfs-backing is the
                // typed-deferred nuance (named here, not silently mis-served).
                if medium.as_deref() == Some("Memory") {
                    tracing::debug!(
                        volume = %vol.name,
                        "emptyDir medium:Memory served as disk-backed named volume (tmpfs deferred)"
                    );
                }
                materializer
                    .ensure_empty_dir(namespace, pod_name, &vol.name)
                    .await?
            }
            PodVolumeSource::DownwardApi => {
                return Err(VolumeResolveError::Unsupported {
                    vol: vol.name.clone(),
                    reason: "DownwardApiUnsupported",
                });
            }
            PodVolumeSource::Projected => {
                return Err(VolumeResolveError::Unsupported {
                    vol: vol.name.clone(),
                    reason: "ProjectedUnsupported",
                });
            }
            PodVolumeSource::Pvc {
                claim_name,
                read_only,
            } => match resolve_pvc_volume(&vol.name, &claim_name, read_only, &fetch)? {
                PvcBacking::HostDir(source) => source,
                // The side effect lives HERE, alongside the other
                // materializations, not inside the pure resolver.
                PvcBacking::Csi(req) => {
                    materializer
                        .publish_csi(namespace, pod_name, &vol.name, &req)
                        .await?
                }
            },
            PodVolumeSource::HostPath => {
                return Err(VolumeResolveError::Unsupported {
                    vol: vol.name.clone(),
                    reason: "HostPathUnsupported",
                });
            }
        };
        out.insert(vol.name.clone(), mount_source);
    }

    Ok(out)
}

/// Select the `(filename → bytes)` set a configMap volume materializes:
/// every key of `.data` (UTF-8) + `.binaryData` (base64-decoded), OR — when
/// `items[]` is set — only those listed keys, each renamed to its
/// `KeyToPath.path`. A listed key absent from BOTH maps is
/// [`VolumeResolveError::KeyNotFound`] (we don't know whether the caller
/// marked it optional at the key level — the source-level optional already
/// short-circuited a fully-missing source upstream).
fn configmap_files(
    name: &str,
    cm: &ConfigMap,
    items: &[KeyToPath],
) -> Result<BTreeMap<String, Vec<u8>>, VolumeResolveError> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if items.is_empty() {
        for (k, v) in &cm.data {
            files.insert(k.clone(), v.clone().into_bytes());
        }
        for (k, v) in &cm.binary_data {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(v)
                .map_err(|e| {
                    VolumeResolveError::Materialize(format!(
                        "decode binaryData[{k}] of ConfigMap {name}: {e}"
                    ))
                })?;
            files.insert(k.clone(), bytes);
        }
    } else {
        for it in items {
            let path = if it.path.is_empty() {
                it.key.clone()
            } else {
                it.path.clone()
            };
            if let Some(v) = cm.data.get(&it.key) {
                files.insert(path, v.clone().into_bytes());
            } else if let Some(v) = cm.binary_data.get(&it.key) {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(v)
                    .map_err(|e| {
                        VolumeResolveError::Materialize(format!(
                            "decode binaryData[{}] of ConfigMap {name}: {e}",
                            it.key
                        ))
                    })?;
                files.insert(path, bytes);
            } else {
                return Err(VolumeResolveError::KeyNotFound {
                    src: format!("ConfigMap/{name}"),
                    key: it.key.clone(),
                });
            }
        }
    }
    Ok(files)
}

/// Select the `(filename → bytes)` set a secret volume materializes: every
/// key of `.data` base64-DECODED (per K8s, secret `.data` is base64), OR —
/// when `items[]` is set — only those listed keys renamed to their
/// `KeyToPath.path`. `.stringData` is NEVER read (write-only per the type).
fn secret_files(
    name: &str,
    sec: &Secret,
    items: &[KeyToPath],
) -> Result<BTreeMap<String, Vec<u8>>, VolumeResolveError> {
    let decode = |key: &str, v: &str| -> Result<Vec<u8>, VolumeResolveError> {
        base64::engine::general_purpose::STANDARD
            .decode(v)
            .map_err(|e| {
                VolumeResolveError::Materialize(format!("decode data[{key}] of Secret {name}: {e}"))
            })
    };
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if items.is_empty() {
        for (k, v) in &sec.data {
            files.insert(k.clone(), decode(k, v)?);
        }
    } else {
        for it in items {
            let path = if it.path.is_empty() {
                it.key.clone()
            } else {
                it.path.clone()
            };
            match sec.data.get(&it.key) {
                Some(v) => {
                    files.insert(path, decode(&it.key, v)?);
                }
                None => {
                    return Err(VolumeResolveError::KeyNotFound {
                        src: format!("Secret/{name}"),
                        key: it.key.clone(),
                    });
                }
            }
        }
    }
    Ok(files)
}

/// What the kubelet must ask a CSI driver for, to make one PV visible on
/// this node.
///
/// ★ EVERY FIELD HERE IS LOAD-BEARING AND COMES OFF THE PV, NOT A DEFAULT.
/// `volume_handle` is the driver's own id for the volume and is what
/// `NodePublishVolume` keys on — inventing it, or reusing the PV name,
/// publishes the wrong volume or nothing at all. `volume_attributes` is the
/// opaque bag `CreateVolume` returned and the driver expects back verbatim;
/// dropping it is how a mount succeeds against the wrong backend path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsiPublishRequest {
    /// `spec.csi.driver` — which registered driver to call.
    pub driver: String,
    /// `spec.csi.volumeHandle` — the driver's id for this volume.
    pub volume_handle: String,
    /// `spec.csi.fsType`, when the PV names one.
    pub fs_type: Option<String>,
    /// `spec.csi.volumeAttributes`, passed back to the driver verbatim.
    pub volume_attributes: BTreeMap<String, String>,
    /// Read-only, from EITHER `spec.csi.readOnly` or the pod's
    /// `persistentVolumeClaim.readOnly`. Either one alone forces it: a
    /// volume declared read-only by the PV must not become writable
    /// because the pod forgot to say so.
    pub read_only: bool,
}

/// How a bound PV is backed on this node.
///
/// Introduced so `resolve_pvc_volume` stays PURE: deciding that a PV is CSI
/// is a computation, and CALLING the driver is a side effect. Collapsing
/// them would put a gRPC call inside the one function every volume test
/// exercises without a driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PvcBacking {
    /// A node-local `hostPath`/`local` directory: bind-mount it directly.
    HostDir(MountSource),
    /// A CSI volume: the driver must publish it before it can be mounted.
    Csi(CsiPublishRequest),
}

/// Resolve a `persistentVolumeClaim` volume to its bound PV's node-local
/// source dir, producing a [`MountSource::PvcHostDir`].
///
/// The resolution honors the PV/PVC binder's contract (commit 69c5415): a
/// dynamically-provisioned local-path PV carries `spec.hostPath.path =
/// <data_dir>/local-path/<ns>-<name>`; a statically-authored PV may carry
/// either `spec.hostPath.path` or `spec.local.path`. We read whichever is
/// present (preferring `hostPath`). The kubelet does NOT re-`mkdir` the dir —
/// the binder's `HostProvisionerEnv` created it at provision time, and a
/// statically-authored hostPath/local PV's path is the operator's contract;
/// re-ensuring it here would mask a genuinely-missing volume.
///
/// `fetch` is the SAME `(kind, name) → Option<Value>` store seam the
/// configMap/secret arms use — the kubelet pre-fetches the PVC (namespaced)
/// and the bound PV (cluster-scoped) into the lookup map; tests pass a
/// closure over in-memory objects (no real cluster).
///
/// # Errors
///
///   * [`VolumeResolveError::PvcNotBound`] — the PVC is absent, not `Bound`,
///     or has no `spec.volumeName` (the pod waits, like ConfigMapNotFound).
///   * [`VolumeResolveError::PvcNotBound`] — the bound PV named by the PVC is
///     itself absent from the store (binder hasn't created/written it yet).
///   * [`VolumeResolveError::PvcSourceUnsupported`] — the bound PV carries a
///     source class engenho serves through no plane at all (NFS, a cloud
///     disk), or a `csi` source missing its driver/handle. Never a fake
///     mount. A well-formed `csi` source is NOT this error any more: it
///     returns [`PvcBacking::Csi`] for the caller to publish.
fn resolve_pvc_volume<F>(
    vol_name: &str,
    claim_name: &str,
    pvc_read_only: bool,
    fetch: &F,
) -> Result<PvcBacking, VolumeResolveError>
where
    F: Fn(&str, &str) -> Option<Value>,
{
    let not_bound = || VolumeResolveError::PvcNotBound {
        vol: vol_name.to_string(),
        claim: claim_name.to_string(),
    };

    // 1. GET the PVC (in the pod's namespace) — absent ⇒ Pending (waits).
    let pvc = fetch("PersistentVolumeClaim", claim_name).ok_or_else(not_bound)?;

    // 2. PVC must be Bound with a spec.volumeName naming its PV.
    let phase = pvc
        .get("status")
        .and_then(|s| s.get("phase"))
        .and_then(Value::as_str);
    if phase != Some("Bound") {
        return Err(not_bound());
    }
    let pv_name = pvc
        .get("spec")
        .and_then(|s| s.get("volumeName"))
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty())
        .ok_or_else(not_bound)?;

    // 3. GET the bound PV (cluster-scoped) — absent ⇒ still Pending (the
    //    binder may not have written it yet; converges on a later tick).
    let pv = fetch("PersistentVolume", pv_name).ok_or_else(not_bound)?;

    // 4. Extract the node-local source path. Only hostPath + local are
    //    served; any other source class is a typed Pending (never faked).
    let spec = pv.get("spec");
    let host_path = spec
        .and_then(|s| s.get("hostPath"))
        .and_then(|h| h.get("path"))
        .and_then(Value::as_str);
    let local_path = spec
        .and_then(|s| s.get("local"))
        .and_then(|l| l.get("path"))
        .and_then(Value::as_str);

    if let Some(path) = host_path.or(local_path) {
        return Ok(PvcBacking::HostDir(MountSource::PvcHostDir {
            path: PathBuf::from(path),
            read_only: pvc_read_only,
        }));
    }

    // A `csi` source is not "unsupported" any more — it is the whole point
    // of the CSI contract. The driver publishes it; the kubelet bind-mounts
    // whatever path comes back.
    if let Some(csi) = spec.and_then(|s| s.get("csi")) {
        let driver = csi
            .get("driver")
            .and_then(Value::as_str)
            .filter(|d| !d.is_empty());
        let handle = csi
            .get("volumeHandle")
            .and_then(Value::as_str)
            .filter(|h| !h.is_empty());
        // A csi source missing either field is MALFORMED, not unsupported.
        // Publishing with an empty handle would ask the driver for "the
        // volume named nothing", and drivers differ on whether that is an
        // error or a silently-wrong mount.
        let (Some(driver), Some(volume_handle)) = (driver, handle) else {
            return Err(VolumeResolveError::PvcSourceUnsupported {
                vol: vol_name.to_string(),
                claim: claim_name.to_string(),
                pv: pv_name.to_string(),
            });
        };
        let mut volume_attributes = BTreeMap::new();
        if let Some(attrs) = csi.get("volumeAttributes").and_then(Value::as_object) {
            for (k, v) in attrs {
                if let Some(v) = v.as_str() {
                    volume_attributes.insert(k.clone(), v.to_string());
                }
            }
        }
        return Ok(PvcBacking::Csi(CsiPublishRequest {
            driver: driver.to_string(),
            volume_handle: volume_handle.to_string(),
            fs_type: csi
                .get("fsType")
                .and_then(Value::as_str)
                .filter(|f| !f.is_empty())
                .map(ToString::to_string),
            volume_attributes,
            // EITHER source of read-only forces it. A PV the cluster
            // declared read-only must not become writable because the pod
            // did not repeat the claim.
            read_only: pvc_read_only
                || csi
                    .get("readOnly")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
        }));
    }

    Err(VolumeResolveError::PvcSourceUnsupported {
        vol: vol_name.to_string(),
        claim: claim_name.to_string(),
        pv: pv_name.to_string(),
    })
}

/// Build the per-container [`ResolvedMount`] list from a container's
/// `volumeMounts[]` + the resolved `volName → MountSource` map.
///
/// A `volumeMount` naming a volume absent from `resolved` is a typed
/// [`VolumeResolveError::NoSource`] (an invalid pod — a mount references a
/// volume the pod never declared); the caller skips the pod. configMap +
/// secret sources default read-only; emptyDir (a [`MountSource::NamedVolume`])
/// defaults read-write — `volumeMount.readOnly == true` forces read-only
/// either way. `subPath`, when present and easy, is folded into the host
/// source path at materialize time by the caller; here it is recorded so the
/// argv builder / future complex handling sees it.
///
/// # Errors
///
/// [`VolumeResolveError::NoSource`] if a `volumeMount.name` has no resolved
/// volume.
pub fn container_mounts(
    container: &Value,
    resolved: &BTreeMap<String, MountSource>,
) -> Result<Vec<ResolvedMount>, VolumeResolveError> {
    let mounts = container_volume_mounts(container)?;
    let mut out = Vec::with_capacity(mounts.len());
    for vm in &mounts {
        let Some(source) = resolved.get(&vm.name) else {
            return Err(VolumeResolveError::NoSource {
                vol: vm.name.clone(),
            });
        };
        // configMap/secret are HostDir + default read-only; emptyDir is a
        // NamedVolume + default read-write; a bound-PVC (PvcHostDir) defaults
        // read-write but the PVC-source `readOnly` flag forces read-only. An
        // explicit volumeMount.readOnly:true forces read-only in every case.
        let (source_ro, source_default_ro) = match source {
            MountSource::HostDir(_) => (false, true),
            MountSource::NamedVolume(_) => (false, false),
            MountSource::PvcHostDir { read_only, .. } => (*read_only, false),
        };
        let read_only = vm.read_only.unwrap_or(false) || source_ro || source_default_ro;
        out.push(ResolvedMount {
            source: source.clone(),
            mount_path: vm.mount_path.clone(),
            read_only,
            sub_path: vm.sub_path.clone(),
        });
    }
    Ok(out)
}

// =================================================================
// PodmanVolumeMaterializer — the real host-side materializer
// =================================================================

/// The production [`VolumeMaterializer`]: writes decoded files under a
/// $HOME-rooted data dir + creates podman named volumes for emptyDir.
///
/// ## Why $HOME (the ONE host-specific knob)
///
/// The applehv podman machine auto-shares the user's $HOME but does NOT
/// share `/tmp` (live-probed: `-v /tmp/...:...` fails with `statfs: no such
/// directory`). So configMap/secret files MUST live under $HOME for the bind
/// mount to resolve. The data root defaults to
/// `$HOME/.local/share/engenho/volumes` (overridable via
/// [`PodmanVolumeMaterializer::with_data_root`]).
pub struct PodmanVolumeMaterializer {
    /// Binary name or path of the podman CLI (default `podman`).
    binary: String,
    /// Host data root under which per-pod-per-volume dirs are written.
    /// Defaults to `$HOME/.local/share/engenho/volumes`.
    data_root: PathBuf,
}

impl Default for PodmanVolumeMaterializer {
    fn default() -> Self {
        Self {
            binary: "podman".to_string(),
            data_root: default_data_root(),
        }
    }
}

/// Default host data root: `$HOME/.local/share/engenho/volumes`. Falls back
/// to a relative `./engenho/volumes` only if `$HOME` is unset (never panics).
fn default_data_root() -> PathBuf {
    let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("."), PathBuf::from);
    home.join(".local")
        .join("share")
        .join("engenho")
        .join("volumes")
}

impl PodmanVolumeMaterializer {
    /// New materializer with the host's `podman` from `$PATH` + the default
    /// `$HOME`-rooted data root.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: override the podman binary path (other fields default).
    #[must_use]
    pub fn with_binary(binary: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            ..Self::default()
        }
    }

    /// Builder: override the host data root (the $HOME-shared dir under which
    /// configMap/secret files are written).
    #[must_use]
    pub fn with_data_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.data_root = root.into();
        self
    }

    /// The per-pod-per-volume host dir: `<data_root>/<ns>_<pod>/<volume>`.
    fn volume_dir(&self, namespace: &str, pod: &str, volume: &str) -> PathBuf {
        self.data_root
            .join(format!("{namespace}_{pod}"))
            .join(volume)
    }

    /// `podman volume create <name>` argv (pure, unit-assertable).
    #[must_use]
    pub fn volume_create_argv(name: &str) -> Vec<String> {
        vec!["volume".to_string(), "create".to_string(), name.to_string()]
    }

    /// `podman volume rm <name>` argv (pure, unit-assertable). Used by
    /// [`remove_empty_dir`](VolumeMaterializer::remove_empty_dir) at pod-delete.
    #[must_use]
    pub fn volume_rm_argv(name: &str) -> Vec<String> {
        vec!["volume".to_string(), "rm".to_string(), name.to_string()]
    }

    /// The deterministic emptyDir named-volume name:
    /// `engenho-empty-<ns>_<pod>_<volume>`.
    #[must_use]
    pub fn empty_dir_volume_name(namespace: &str, pod: &str, volume: &str) -> String {
        format!("engenho-empty-{namespace}_{pod}_{volume}")
    }
}

#[async_trait]
impl VolumeMaterializer for PodmanVolumeMaterializer {
    fn name(&self) -> &'static str {
        "podman-volume"
    }

    async fn materialize_files(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<MountSource, VolumeResolveError> {
        let dir = self.volume_dir(namespace, pod, volume);
        // Each file is written atomically (reuses the substrate's
        // write_atomic — the load-bearing fix, not a hand-rolled write).
        // write_atomic mkdir's the parent, so the dir is created implicitly;
        // we also create the root dir explicitly so an EMPTY volume (optional
        // source missing) still produces a bind-mountable directory.
        std::fs::create_dir_all(&dir).map_err(|e| {
            VolumeResolveError::Materialize(format!("mkdir {}: {e}", dir.display()))
        })?;
        for (filename, bytes) in files {
            let path = dir.join(filename);
            engenho_substrate::write_atomic(&path, bytes).map_err(|e| {
                VolumeResolveError::Materialize(format!("write {}: {e}", path.display()))
            })?;
        }
        Ok(MountSource::HostDir(dir))
    }

    async fn ensure_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<MountSource, VolumeResolveError> {
        let vol_name = Self::empty_dir_volume_name(namespace, pod, volume);
        // `podman volume create` is idempotent in effect: creating an
        // existing volume returns its name with exit 0. We tolerate a
        // benign "already exists" stderr the same way ensure_network does.
        let argv = Self::volume_create_argv(&vol_name);
        let out = tokio::process::Command::new(&self.binary)
            .args(&argv)
            .output()
            .await
            .map_err(|e| {
                VolumeResolveError::Materialize(format!("podman volume create spawn: {e}"))
            })?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if !stderr.contains("already exists") && !stderr.contains("in use") {
                return Err(VolumeResolveError::Materialize(format!(
                    "podman volume create {vol_name}: {stderr}"
                )));
            }
        }
        Ok(MountSource::NamedVolume(vol_name))
    }

    async fn remove_empty_dir(
        &self,
        namespace: &str,
        pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError> {
        let vol_name = Self::empty_dir_volume_name(namespace, pod, volume);
        // `podman volume rm` of an absent volume exits non-zero with a "no
        // such volume" stderr — idempotent SUCCESS for the delete path (a
        // re-run after a partial cleanup, or a volume that never got created
        // because the pod failed to start, must NOT wedge cleanup).
        let argv = Self::volume_rm_argv(&vol_name);
        let out = tokio::process::Command::new(&self.binary)
            .args(&argv)
            .output()
            .await
            .map_err(|e| VolumeResolveError::Materialize(format!("podman volume rm spawn: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if !stderr.contains("no such volume") && !stderr.contains("not found") {
                return Err(VolumeResolveError::Materialize(format!(
                    "podman volume rm {vol_name}: {stderr}"
                )));
            }
        }
        Ok(())
    }
}

// =================================================================
// FakeVolumeMaterializer — deterministic mock for unit tests
// =================================================================

/// Deterministic mock [`VolumeMaterializer`] — records every
/// `(volName → filename → bytes)` it was asked to materialize + every
/// emptyDir it was asked to ensure, and returns deterministic
/// [`MountSource`]s (`HostDir("/fake/<vol>")` / `NamedVolume("fake-<vol>")`).
/// The trait IS the testability contract: resolution is fully exercised with
/// ZERO real podman + ZERO host filesystem.
#[derive(Default)]
pub struct FakeVolumeMaterializer {
    inner: tokio::sync::Mutex<FakeMatState>,
}

#[derive(Default)]
struct FakeMatState {
    /// `volume → (filename → bytes)` the materializer was asked to write.
    files: BTreeMap<String, BTreeMap<String, Vec<u8>>>,
    /// emptyDir volumes the materializer was asked to ensure.
    empty_dirs: Vec<String>,
    /// emptyDir volumes the materializer was asked to remove (delete path).
    removed_empty_dirs: Vec<String>,
}

impl FakeVolumeMaterializer {
    /// Fresh empty mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `(filename → bytes)` set recorded for `volume`, if any.
    pub async fn files_for(&self, volume: &str) -> Option<BTreeMap<String, Vec<u8>>> {
        self.inner.lock().await.files.get(volume).cloned()
    }

    /// The list of emptyDir volumes the mock was asked to ensure (in call
    /// order).
    pub async fn ensured_empty_dirs(&self) -> Vec<String> {
        self.inner.lock().await.empty_dirs.clone()
    }

    /// The list of emptyDir volumes the mock was asked to remove (in call
    /// order) — the pod-delete cleanup surface.
    pub async fn removed_empty_dirs(&self) -> Vec<String> {
        self.inner.lock().await.removed_empty_dirs.clone()
    }
}

#[async_trait]
impl VolumeMaterializer for FakeVolumeMaterializer {
    fn name(&self) -> &'static str {
        "fake-volume"
    }

    async fn materialize_files(
        &self,
        _namespace: &str,
        _pod: &str,
        volume: &str,
        files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<MountSource, VolumeResolveError> {
        self.inner
            .lock()
            .await
            .files
            .insert(volume.to_string(), files.clone());
        Ok(MountSource::HostDir(PathBuf::from(format!(
            "/fake/{volume}"
        ))))
    }

    async fn ensure_empty_dir(
        &self,
        _namespace: &str,
        _pod: &str,
        volume: &str,
    ) -> Result<MountSource, VolumeResolveError> {
        self.inner.lock().await.empty_dirs.push(volume.to_string());
        Ok(MountSource::NamedVolume(format!("fake-{volume}")))
    }

    async fn remove_empty_dir(
        &self,
        _namespace: &str,
        _pod: &str,
        volume: &str,
    ) -> Result<(), VolumeResolveError> {
        self.inner
            .lock()
            .await
            .removed_empty_dirs
            .push(volume.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    #[test]
    fn error_pending_reasons_are_stable() {
        assert_eq!(
            VolumeResolveError::ConfigMapNotFound { name: "x".into() }.pending_reason(),
            "ConfigMapNotFound"
        );
        assert_eq!(
            VolumeResolveError::SecretNotFound { name: "x".into() }.pending_reason(),
            "SecretNotFound"
        );
        assert_eq!(
            VolumeResolveError::KeyNotFound {
                src: "ConfigMap/x".into(),
                key: "k".into()
            }
            .pending_reason(),
            "InvalidVolumeKey"
        );
        assert_eq!(
            VolumeResolveError::Unsupported {
                vol: "v".into(),
                reason: "HostPathUnsupported"
            }
            .pending_reason(),
            "HostPathUnsupported"
        );
        // kind() (the impl_error_kind surface) is independent + stable.
        assert_eq!(
            VolumeResolveError::ConfigMapNotFound { name: "x".into() }.kind(),
            "config_map_not_found"
        );
    }

    #[test]
    fn from_volume_rejects_multiple_sources() {
        let vol: Volume = serde_json::from_value(json!({
            "name": "v",
            "configMap": { "name": "cm" },
            "emptyDir": {}
        }))
        .unwrap();
        let err = PodVolumeSource::from_volume(&vol).unwrap_err();
        assert!(matches!(err, VolumeResolveError::MultipleSources { .. }));
    }

    #[test]
    fn from_volume_dispatches_each_arm() {
        let cm: Volume = serde_json::from_value(json!({
            "name": "v", "configMap": { "name": "cm", "optional": true }
        }))
        .unwrap();
        assert!(matches!(
            PodVolumeSource::from_volume(&cm).unwrap(),
            PodVolumeSource::ConfigMap { optional: true, .. }
        ));
        let sec: Volume = serde_json::from_value(json!({
            "name": "v", "secret": { "secretName": "s" }
        }))
        .unwrap();
        assert!(matches!(
            PodVolumeSource::from_volume(&sec).unwrap(),
            PodVolumeSource::Secret { .. }
        ));
        let ed: Volume = serde_json::from_value(json!({
            "name": "v", "emptyDir": {}
        }))
        .unwrap();
        assert!(matches!(
            PodVolumeSource::from_volume(&ed).unwrap(),
            PodVolumeSource::EmptyDir { .. }
        ));
        let hp: Volume = serde_json::from_value(json!({
            "name": "v", "hostPath": { "path": "/etc" }
        }))
        .unwrap();
        assert!(matches!(
            PodVolumeSource::from_volume(&hp).unwrap(),
            PodVolumeSource::HostPath
        ));
    }

    #[tokio::test]
    async fn configmap_data_keys_become_files() {
        let pod = json!({
            "spec": { "volumes": [ { "name": "cfg", "configMap": { "name": "cm" } } ] }
        });
        let cm = json!({
            "kind": "ConfigMap", "apiVersion": "v1",
            "metadata": { "name": "cm" },
            "data": { "greeting": "hello", "other": "world" }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |kind: &str, name: &str| -> Option<Value> {
            if kind == "ConfigMap" && name == "cm" {
                Some(cm.clone())
            } else {
                None
            }
        };
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("cfg"),
            Some(&MountSource::HostDir(PathBuf::from("/fake/cfg")))
        );
        let files = mat.files_for("cfg").await.unwrap();
        assert_eq!(files.get("greeting"), Some(&b"hello".to_vec()));
        assert_eq!(files.get("other"), Some(&b"world".to_vec()));
    }

    #[tokio::test]
    async fn secret_data_is_base64_decoded_before_materialize() {
        let pod = json!({
            "spec": { "volumes": [ { "name": "sec", "secret": { "secretName": "s" } } ] }
        });
        let secret = json!({
            "kind": "Secret", "apiVersion": "v1",
            "metadata": { "name": "s" },
            "data": { "token": b64("s3cr3t-value") }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |kind: &str, name: &str| -> Option<Value> {
            if kind == "Secret" && name == "s" {
                Some(secret.clone())
            } else {
                None
            }
        };
        resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        // Proves the decode happened — the materializer saw the DECODED bytes.
        let files = mat.files_for("sec").await.unwrap();
        assert_eq!(files.get("token"), Some(&b"s3cr3t-value".to_vec()));
    }

    #[test]
    fn volume_create_and_rm_argv_are_exact() {
        assert_eq!(
            PodmanVolumeMaterializer::volume_create_argv("v"),
            vec!["volume".to_string(), "create".to_string(), "v".to_string()]
        );
        assert_eq!(
            PodmanVolumeMaterializer::volume_rm_argv("v"),
            vec!["volume".to_string(), "rm".to_string(), "v".to_string()]
        );
    }

    #[test]
    fn empty_dir_volume_name_is_deterministic() {
        assert_eq!(
            PodmanVolumeMaterializer::empty_dir_volume_name("default", "p1", "scratch"),
            "engenho-empty-default_p1_scratch"
        );
    }

    #[tokio::test]
    async fn fake_records_remove_empty_dir_calls() {
        let mat = FakeVolumeMaterializer::new();
        mat.ensure_empty_dir("default", "p1", "scratch")
            .await
            .unwrap();
        assert!(mat.removed_empty_dirs().await.is_empty());
        mat.remove_empty_dir("default", "p1", "scratch")
            .await
            .unwrap();
        assert_eq!(mat.removed_empty_dirs().await, vec!["scratch".to_string()]);
    }

    #[tokio::test]
    async fn empty_dir_yields_named_volume() {
        let pod = json!({
            "spec": { "volumes": [ { "name": "scratch", "emptyDir": {} } ] }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |_k: &str, _n: &str| None;
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("scratch"),
            Some(&MountSource::NamedVolume("fake-scratch".into()))
        );
        assert_eq!(mat.ensured_empty_dirs().await, vec!["scratch".to_string()]);
    }

    #[tokio::test]
    async fn missing_non_optional_configmap_errors() {
        let pod = json!({
            "spec": { "volumes": [ { "name": "cfg", "configMap": { "name": "absent" } } ] }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |_k: &str, _n: &str| None;
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "ConfigMapNotFound");
        assert!(matches!(err, VolumeResolveError::ConfigMapNotFound { .. }));
    }

    #[tokio::test]
    async fn missing_optional_configmap_materializes_empty() {
        let pod = json!({
            "spec": { "volumes": [ { "name": "cfg", "configMap": { "name": "absent", "optional": true } } ] }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |_k: &str, _n: &str| None;
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("cfg"),
            Some(&MountSource::HostDir(PathBuf::from("/fake/cfg")))
        );
        // Materialized with NO files (empty volume).
        assert_eq!(mat.files_for("cfg").await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn no_volumes_is_empty_map() {
        let pod = json!({ "spec": { "containers": [] } });
        let mat = FakeVolumeMaterializer::new();
        let fetch = |_k: &str, _n: &str| None;
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn container_mounts_maps_read_only_defaults() {
        let mut resolved = BTreeMap::new();
        resolved.insert(
            "cfg".to_string(),
            MountSource::HostDir(PathBuf::from("/fake/cfg")),
        );
        resolved.insert(
            "scratch".to_string(),
            MountSource::NamedVolume("fake-scratch".into()),
        );
        let container = json!({
            "name": "c",
            "volumeMounts": [
                { "name": "cfg", "mountPath": "/etc/cfg" },
                { "name": "scratch", "mountPath": "/data" }
            ]
        });
        let mounts = container_mounts(&container, &resolved).unwrap();
        assert_eq!(mounts.len(), 2);
        // configMap (HostDir) defaults read-only.
        let cfg = mounts.iter().find(|m| m.mount_path == "/etc/cfg").unwrap();
        assert!(cfg.read_only);
        // emptyDir (NamedVolume) defaults read-write.
        let scratch = mounts.iter().find(|m| m.mount_path == "/data").unwrap();
        assert!(!scratch.read_only);
    }

    #[test]
    fn container_mounts_rejects_unknown_volume() {
        let resolved = BTreeMap::new();
        let container = json!({
            "name": "c",
            "volumeMounts": [ { "name": "ghost", "mountPath": "/x" } ]
        });
        let err = container_mounts(&container, &resolved).unwrap_err();
        assert!(matches!(err, VolumeResolveError::NoSource { .. }));
    }

    #[test]
    fn configmap_items_subset_and_rename() {
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": { "name": "cm" },
            "data": { "a": "AAA", "b": "BBB" }
        }))
        .unwrap();
        let items = vec![KeyToPath {
            key: "a".into(),
            path: "renamed-a".into(),
            mode: None,
        }];
        let files = configmap_files("cm", &cm, &items).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.get("renamed-a"), Some(&b"AAA".to_vec()));
    }

    // ── PVC → bound PV → node-local hostPath resolution ───────────────

    /// `fetch` closure over a set of (kind, name) → Value objects — the same
    /// seam the kubelet pre-fetch builds, here in-memory (no cluster).
    fn pvc_fetch(
        objs: Vec<(&'static str, &'static str, Value)>,
    ) -> impl Fn(&str, &str) -> Option<Value> {
        move |kind: &str, name: &str| {
            objs.iter()
                .find(|(k, n, _)| *k == kind && *n == name)
                .map(|(_, _, v)| v.clone())
        }
    }

    #[tokio::test]
    async fn pvc_bound_to_local_path_pv_resolves_to_hostdir() {
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "myclaim" } }
            ] }
        });
        // Bound PVC → PV "pvc-xyz"; PV carries the binder's hostPath.
        let pvc = json!({
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": "myclaim", "namespace": "default" },
            "spec": { "volumeName": "pvc-xyz" },
            "status": { "phase": "Bound" }
        });
        let pv = json!({
            "kind": "PersistentVolume",
            "metadata": { "name": "pvc-xyz" },
            "spec": { "hostPath": { "path": "/data/local-path/default-myclaim" } }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "myclaim", pvc),
            ("PersistentVolume", "pvc-xyz", pv),
        ]);
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("data"),
            Some(&MountSource::PvcHostDir {
                path: PathBuf::from("/data/local-path/default-myclaim"),
                read_only: false,
            })
        );
        // The container sees the PV path at its mountPath, default read-write.
        let container = json!({
            "name": "c",
            "volumeMounts": [ { "name": "data", "mountPath": "/var/data" } ]
        });
        let mounts = container_mounts(&container, &map).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].mount_path, "/var/data");
        assert!(!mounts[0].read_only); // PVC defaults read-write
    }

    #[tokio::test]
    async fn pvc_bound_to_local_source_pv_resolves() {
        // A statically-authored PV may use `spec.local.path` instead of hostPath.
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c" } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv-local" }, "status": { "phase": "Bound" }
        });
        let pv = json!({
            "spec": { "local": { "path": "/mnt/disks/ssd1" } }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "c", pvc),
            ("PersistentVolume", "pv-local", pv),
        ]);
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("data"),
            Some(&MountSource::PvcHostDir {
                path: PathBuf::from("/mnt/disks/ssd1"),
                read_only: false,
            })
        );
    }

    #[tokio::test]
    async fn unbound_pvc_stays_pending_not_mounted() {
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "pending" } }
            ] }
        });
        // PVC present but not Bound (no volumeName, phase=Pending).
        let pvc = json!({
            "spec": {}, "status": { "phase": "Pending" }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![("PersistentVolumeClaim", "pending", pvc)]);
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "PvcNotBound");
        assert!(matches!(err, VolumeResolveError::PvcNotBound { .. }));
    }

    #[tokio::test]
    async fn absent_pvc_stays_pending() {
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "ghost" } }
            ] }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![]); // PVC not in store
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "PvcNotBound");
    }

    #[tokio::test]
    async fn bound_pvc_with_absent_pv_stays_pending() {
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c" } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv-missing" }, "status": { "phase": "Bound" }
        });
        let mat = FakeVolumeMaterializer::new();
        // PVC says Bound but the PV is not in the store yet.
        let fetch = pvc_fetch(vec![("PersistentVolumeClaim", "c", pvc)]);
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "PvcNotBound");
    }

    #[tokio::test]
    async fn a_csi_pv_now_says_csi_unavailable_not_source_unsupported() {
        // BEHAVIOUR CHANGE, deliberate. Before the CSI contract landed a
        // CSI-backed PV was `PvcSourceUnsupported` — "engenho cannot serve
        // this source class". That is no longer true: the class IS served,
        // and what is missing is a CSI plane on THIS node. The two are
        // completely different things for an operator to act on, which is
        // why they are different reasons rather than one shared string.
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c" } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv-csi" }, "status": { "phase": "Bound" }
        });
        let pv = json!({
            "spec": { "csi": { "driver": "ebs.csi.aws.com", "volumeHandle": "vol-123" } }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "c", pvc),
            ("PersistentVolume", "pv-csi", pv),
        ]);
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "CsiUnavailable");
        // And it names the driver, so an operator knows WHICH one to deploy.
        assert!(err.to_string().contains("ebs.csi.aws.com"), "{err}");
    }

    #[tokio::test]
    async fn a_genuinely_unservable_source_class_still_says_so() {
        // The `PvcSourceUnsupported` path must not have been lost: an NFS
        // PV is a source class engenho serves through no plane at all.
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c" } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv-nfs" }, "status": { "phase": "Bound" }
        });
        let pv = json!({
            "spec": { "nfs": { "server": "10.0.0.1", "path": "/exports/x" } }
        });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "c", pvc),
            ("PersistentVolume", "pv-nfs", pv),
        ]);
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "PvcSourceUnsupported");
    }

    #[tokio::test]
    async fn a_csi_pv_missing_its_handle_is_malformed_not_publishable() {
        // Publishing with an empty handle asks the driver for "the volume
        // named nothing"; drivers differ on whether that errors or mounts
        // something wrong, and neither is acceptable.
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c" } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv-csi" }, "status": { "phase": "Bound" }
        });
        let pv = json!({ "spec": { "csi": { "driver": "ebs.csi.aws.com" } } });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "c", pvc),
            ("PersistentVolume", "pv-csi", pv),
        ]);
        let err = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap_err();
        assert_eq!(err.pending_reason(), "PvcSourceUnsupported");
    }

    #[tokio::test]
    async fn pvc_read_only_propagates_to_mount() {
        let pod = json!({
            "spec": { "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "c", "readOnly": true } }
            ] }
        });
        let pvc = json!({
            "spec": { "volumeName": "pv1" }, "status": { "phase": "Bound" }
        });
        let pv = json!({ "spec": { "hostPath": { "path": "/data/ro" } } });
        let mat = FakeVolumeMaterializer::new();
        let fetch = pvc_fetch(vec![
            ("PersistentVolumeClaim", "c", pvc),
            ("PersistentVolume", "pv1", pv),
        ]);
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(
            map.get("data"),
            Some(&MountSource::PvcHostDir {
                path: PathBuf::from("/data/ro"),
                read_only: true,
            })
        );
        // readOnly on the PVC source forces the mount read-only even though
        // the volumeMount itself didn't request it.
        let container = json!({
            "name": "c",
            "volumeMounts": [ { "name": "data", "mountPath": "/var/data" } ]
        });
        let mounts = container_mounts(&container, &map).unwrap();
        assert!(mounts[0].read_only);
    }

    #[tokio::test]
    async fn volume_mount_read_only_forces_ro_on_writable_pvc() {
        // PVC is RW, but the volumeMount declares readOnly:true → RO mount.
        let mut resolved = BTreeMap::new();
        resolved.insert(
            "data".to_string(),
            MountSource::PvcHostDir {
                path: PathBuf::from("/data/rw"),
                read_only: false,
            },
        );
        let container = json!({
            "name": "c",
            "volumeMounts": [ { "name": "data", "mountPath": "/var/data", "readOnly": true } ]
        });
        let mounts = container_mounts(&container, &resolved).unwrap();
        assert!(mounts[0].read_only);
    }

    #[test]
    fn pvc_pending_reasons_and_kinds_are_stable() {
        assert_eq!(
            VolumeResolveError::PvcNotBound {
                vol: "v".into(),
                claim: "c".into()
            }
            .pending_reason(),
            "PvcNotBound"
        );
        assert_eq!(
            VolumeResolveError::PvcSourceUnsupported {
                vol: "v".into(),
                claim: "c".into(),
                pv: "p".into()
            }
            .pending_reason(),
            "PvcSourceUnsupported"
        );
        assert_eq!(
            VolumeResolveError::PvcNotBound {
                vol: "v".into(),
                claim: "c".into()
            }
            .kind(),
            "pvc_not_bound"
        );
        assert_eq!(
            VolumeResolveError::PvcSourceUnsupported {
                vol: "v".into(),
                claim: "c".into(),
                pv: "p".into()
            }
            .kind(),
            "pvc_source_unsupported"
        );
    }

    #[test]
    fn from_volume_dispatches_pvc_arm() {
        let v: Volume = serde_json::from_value(json!({
            "name": "data", "persistentVolumeClaim": { "claimName": "myclaim", "readOnly": true }
        }))
        .unwrap();
        assert!(matches!(
            PodVolumeSource::from_volume(&v).unwrap(),
            PodVolumeSource::Pvc {
                read_only: true,
                ..
            }
        ));
    }

    #[test]
    fn configmap_items_missing_key_errors() {
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": { "name": "cm" },
            "data": { "a": "AAA" }
        }))
        .unwrap();
        let items = vec![KeyToPath {
            key: "nope".into(),
            path: String::new(),
            mode: None,
        }];
        let err = configmap_files("cm", &cm, &items).unwrap_err();
        assert_eq!(err.pending_reason(), "InvalidVolumeKey");
    }
}
