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
    HostDir(PathBuf),
    /// A named podman volume (created via `podman volume create`).
    NamedVolume(String),
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
///   * [`PodVolumeSource::Pvc`] → `"PvcUnsupported"` (owned by the storage
///     brick — see [`crate::volume`])
///   * [`PodVolumeSource::HostPath`] → `"HostPathUnsupported"` (host-fs
///     exposure risk; deferred until an explicit allowlist lands)
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
    /// `persistentVolumeClaim` — owned by the storage brick
    /// (`"PvcUnsupported"` here; no fork of [`crate::volume`]).
    Pvc,
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
        if vol.persistent_volume_claim.is_some() {
            populated += 1;
            found = Some(PodVolumeSource::Pvc);
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
    /// A typed-deferred source class (hostPath / PVC / projected /
    /// downwardAPI / emptyDir medium nuance). Carries the named K8s-style
    /// Pending reason verbatim — the source is NOT served, surfaced
    /// honestly rather than mis-materialized.
    #[error("volume {vol} uses unsupported source ({reason})")]
    Unsupported {
        /// The offending volume's name.
        vol: String,
        /// The typed Pending reason (e.g. `"HostPathUnsupported"`).
        reason: &'static str,
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
            VolumeResolveError::MultipleSources { .. }
            | VolumeResolveError::NoSource { .. } => "InvalidVolumeSource",
            VolumeResolveError::Unsupported { reason, .. } => reason,
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
            PodVolumeSource::Pvc => {
                return Err(VolumeResolveError::Unsupported {
                    vol: vol.name.clone(),
                    reason: "PvcUnsupported",
                });
            }
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
                .map_err(|e| VolumeResolveError::Materialize(format!(
                    "decode binaryData[{k}] of ConfigMap {name}: {e}"
                )))?;
            files.insert(k.clone(), bytes);
        }
    } else {
        for it in items {
            let path = if it.path.is_empty() { it.key.clone() } else { it.path.clone() };
            if let Some(v) = cm.data.get(&it.key) {
                files.insert(path, v.clone().into_bytes());
            } else if let Some(v) = cm.binary_data.get(&it.key) {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(v)
                    .map_err(|e| VolumeResolveError::Materialize(format!(
                        "decode binaryData[{}] of ConfigMap {name}: {e}",
                        it.key
                    )))?;
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
            .map_err(|e| VolumeResolveError::Materialize(format!(
                "decode data[{key}] of Secret {name}: {e}"
            )))
    };
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    if items.is_empty() {
        for (k, v) in &sec.data {
            files.insert(k.clone(), decode(k, v)?);
        }
    } else {
        for it in items {
            let path = if it.path.is_empty() { it.key.clone() } else { it.path.clone() };
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
        // NamedVolume + default read-write. An explicit readOnly:true forces
        // read-only regardless.
        let default_ro = matches!(source, MountSource::HostDir(_));
        let read_only = vm.read_only.unwrap_or(false) || default_ro;
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
    home.join(".local").join("share").join("engenho").join("volumes")
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
        Ok(MountSource::HostDir(PathBuf::from(format!("/fake/{volume}"))))
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
            if kind == "ConfigMap" && name == "cm" { Some(cm.clone()) } else { None }
        };
        let map = resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        assert_eq!(map.get("cfg"), Some(&MountSource::HostDir(PathBuf::from("/fake/cfg"))));
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
            if kind == "Secret" && name == "s" { Some(secret.clone()) } else { None }
        };
        resolve_pod_volumes(&pod, "default", "p1", fetch, &mat)
            .await
            .unwrap();
        // Proves the decode happened — the materializer saw the DECODED bytes.
        let files = mat.files_for("sec").await.unwrap();
        assert_eq!(files.get("token"), Some(&b"s3cr3t-value".to_vec()));
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
        assert_eq!(map.get("cfg"), Some(&MountSource::HostDir(PathBuf::from("/fake/cfg"))));
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
        resolved.insert("cfg".to_string(), MountSource::HostDir(PathBuf::from("/fake/cfg")));
        resolved.insert("scratch".to_string(), MountSource::NamedVolume("fake-scratch".into()));
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
        let items = vec![KeyToPath { key: "a".into(), path: "renamed-a".into(), mode: None }];
        let files = configmap_files("cm", &cm, &items).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.get("renamed-a"), Some(&b"AAA".to_vec()));
    }

    #[test]
    fn configmap_items_missing_key_errors() {
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": { "name": "cm" },
            "data": { "a": "AAA" }
        }))
        .unwrap();
        let items = vec![KeyToPath { key: "nope".into(), path: String::new(), mode: None }];
        let err = configmap_files("cm", &cm, &items).unwrap_err();
        assert_eq!(err.pending_reason(), "InvalidVolumeKey");
    }
}
