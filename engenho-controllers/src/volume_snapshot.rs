//! `VolumeSnapshotController` — snapshot and restore for local-path volumes.
//!
//! ## Why this exists
//!
//! `snapshot.storage.k8s.io/v1` is not part of Kubernetes proper: upstream
//! ships it as CRDs plus the external-snapshotter controller, and every CSI
//! driver implements the actual copy. engenho serves CRDs and owns a
//! local-path provisioner ([`crate::pv_binder`]), so the honest local
//! implementation is to own the snapshot half of that same provisioner rather
//! than to pretend a CSI driver is present.
//!
//! The concrete driver: a PITR drill is snapshot → restore → verify, and the
//! restore vector on a real cluster is a CSI `VolumeSnapshot`. Without this,
//! that whole shape is unrunnable here and the drill logic can only ever be
//! exercised against a cloud.
//!
//! ## What it does, each tick
//!
//! For every `VolumeSnapshot` not yet `status.readyToUse`:
//!
//!   1. Resolve `spec.source.persistentVolumeClaimName` → the PVC in the same
//!      namespace → its bound PV → that PV's `hostPath.path`.
//!   2. Copy that directory to `<snapshot_root>/<ns>-<name>`.
//!   3. Create the `VolumeSnapshotContent` describing it.
//!   4. Stamp `status` — `readyToUse`, `boundVolumeSnapshotContentName`,
//!      `creationTime`, `restoreSize`.
//!
//! ## What it deliberately does NOT do
//!
//! **It does not quiesce.** The copy is of a live directory, so a writer
//! mid-write yields a torn file exactly as an un-quiesced disk snapshot does.
//! That is stated rather than hidden: a caller who needs consistency stops the
//! workload first (the same discipline camelot's real drill uses, where MySQL
//! is scaled to 0 so the volume is cold). Claiming crash-consistency we do not
//! implement would be the worse failure.
//!
//! **It snapshots ONLY local-path volumes.** A PV with no `hostPath` is
//! skipped with a typed note, never reported ready — a snapshot that claims
//! success while copying nothing is precisely the "ten clean receipts, real
//! residue underneath" failure the PITR programme already recorded once.
//!
//! ## The filesystem seam
//!
//! Every host effect sits behind [`SnapshotEnv`], so the controller is
//! unit-testable without touching a disk ([`FakeSnapshotEnv`]).

use std::sync::Arc;

use async_trait::async_trait;
use engenho_store::{
    StoreMesh,
    command::{Reason, ResourceCommand},
    resource::ResourceKey,
};
use serde_json::{Value, json};

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::effect::Effect;
use crate::error::ControllerError;
use crate::reads::{DeclaresReads, Reads, gvk};

/// The snapshot API group.
pub const SNAPSHOT_GROUP: &str = "snapshot.storage.k8s.io";
/// The served version.
pub const SNAPSHOT_VERSION: &str = "v1";
/// The driver name stamped into a `VolumeSnapshotContent`, matching the
/// provisioner that owns the underlying directory.
pub const SNAPSHOT_DRIVER: &str = "engenho.io/local-path";

/// The host-effect seam — the side-effecting half of snapshotting.
///
/// Mirrors [`crate::pv_binder::ProvisionerEnv`] deliberately: one trait, only
/// the effects, so the reconcile logic is pure and testable.
pub trait SnapshotEnv: Send + Sync {
    /// Recursively copy `src` to `dst`, replacing `dst` if it exists.
    ///
    /// # Errors
    ///
    /// A human-readable message on any failure. A failed copy leaves the
    /// snapshot NOT ready — never ready-with-no-data.
    fn copy_tree(&self, src: &str, dst: &str) -> Result<(), String>;

    /// Whether a path exists (used to refuse a snapshot of a missing source
    /// rather than creating an empty one).
    fn exists(&self, path: &str) -> bool;

    /// Total bytes under `path`, for `status.restoreSize`.
    ///
    /// # Errors
    ///
    /// A human-readable message on any failure.
    fn size_bytes(&self, path: &str) -> Result<u64, String>;
}

/// The real filesystem implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct HostSnapshotEnv;

impl SnapshotEnv for HostSnapshotEnv {
    fn copy_tree(&self, src: &str, dst: &str) -> Result<(), String> {
        let dst_path = std::path::Path::new(dst);
        if dst_path.exists() {
            std::fs::remove_dir_all(dst_path).map_err(|e| e.to_string())?;
        }
        copy_dir_recursive(std::path::Path::new(src), dst_path).map_err(|e| e.to_string())
    }

    fn exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }

    fn size_bytes(&self, path: &str) -> Result<u64, String> {
        dir_size(std::path::Path::new(path)).map_err(|e| e.to_string())
    }
}

/// Recursive directory copy. `std::fs` has no recursive copy, and shelling out
/// to `cp -r` is banned (★ NO SHELL) — this is the typed equivalent.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Recursive byte count, the size half of the same walk.
fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

/// The snapshot controller.
pub struct VolumeSnapshotController {
    store: Arc<StoreMesh>,
    /// Root under which snapshot copies live:
    /// `<snapshot_root>/<snapshot-ns>-<snapshot-name>`.
    snapshot_root: String,
    env: Arc<dyn SnapshotEnv>,
}

impl VolumeSnapshotController {
    /// Build the controller over a store, a snapshot root, and a host seam.
    #[must_use]
    pub fn new(store: Arc<StoreMesh>, snapshot_root: String, env: Arc<dyn SnapshotEnv>) -> Self {
        Self {
            store,
            snapshot_root,
            env,
        }
    }

    /// The directory a given snapshot's data lives in, derived from a root.
    ///
    /// Free of `self` so it is testable without standing up a store — the
    /// path derivation is the part with a rule in it (namespacing), and a
    /// helper that needs a whole cluster to test is a helper nobody tests.
    #[must_use]
    pub fn snapshot_path_in(root: &str, namespace: &str, name: &str) -> String {
        [root.trim_end_matches('/'), "/", namespace, "-", name].concat()
    }

    /// The directory this controller stores a given snapshot's data in.
    #[must_use]
    pub fn snapshot_path(&self, namespace: &str, name: &str) -> String {
        Self::snapshot_path_in(&self.snapshot_root, namespace, name)
    }

    /// Is this snapshot already done? Idempotence: a ready snapshot is never
    /// re-copied, so a tick over a settled cluster performs no host effect.
    fn is_ready(snap: &Value) -> bool {
        snap.get("status")
            .and_then(|s| s.get("readyToUse"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}

/// The snapshots it takes, and the claim and volume each one copies. The
/// `VolumeSnapshotContent` it writes is output, not input.
impl DeclaresReads for VolumeSnapshotController {
    fn reads(&self) -> Reads {
        Reads::of(&[
            gvk(SNAPSHOT_GROUP, SNAPSHOT_VERSION, "VolumeSnapshot"),
            gvk("", "v1", "PersistentVolumeClaim"),
            gvk("", "v1", "PersistentVolume"),
        ])
    }
}

#[async_trait]
impl Controller for VolumeSnapshotController {
    fn name(&self) -> &'static str {
        "volume-snapshot"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let snaps = self
            .store
            .list(SNAPSHOT_GROUP, SNAPSHOT_VERSION, "VolumeSnapshot", None)
            .await;
        let pvcs = self
            .store
            .list("", "v1", "PersistentVolumeClaim", None)
            .await;
        let pvs = self.store.list("", "v1", "PersistentVolume", None).await;

        let mut report = ReconcileReport::default();
        report.objects_examined = snaps.len();

        for (key, snap) in &snaps {
            if Self::is_ready(snap) {
                report.objects_skipped += 1;
                continue;
            }
            let ns = key.namespace.as_deref().unwrap_or("default");
            let name = &key.name;

            // ── Resolve source PVC → PV → hostPath ───────────────────────
            let Some(pvc_name) = snap
                .get("spec")
                .and_then(|s| s.get("source"))
                .and_then(|s| s.get("persistentVolumeClaimName"))
                .and_then(Value::as_str)
            else {
                report.objects_skipped += 1;
                report.note = Some(
                    "a VolumeSnapshot with no spec.source.persistentVolumeClaimName cannot be \
                     resolved; pre-provisioned content sources are not served"
                        .to_string(),
                );
                continue;
            };
            let Some((_, pvc)) = pvcs
                .iter()
                .find(|(k, _)| k.namespace.as_deref() == Some(ns) && k.name == pvc_name)
            else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(volume_name) = pvc
                .get("spec")
                .and_then(|s| s.get("volumeName"))
                .and_then(Value::as_str)
            else {
                // An unbound PVC has nothing to copy. Left not-ready.
                report.objects_skipped += 1;
                continue;
            };
            let Some((_, pv)) = pvs.iter().find(|(k, _)| k.name == volume_name) else {
                report.objects_skipped += 1;
                continue;
            };
            let Some(src) = pv
                .get("spec")
                .and_then(|s| s.get("hostPath"))
                .and_then(|h| h.get("path"))
                .and_then(Value::as_str)
            else {
                // Not a local-path volume. Skipped with a NOTE rather than
                // marked ready — a snapshot that copies nothing must never
                // report success.
                report.objects_skipped += 1;
                report.note = Some(
                    "only local-path (hostPath) volumes can be snapshotted by this controller; \
                     the source PV has no hostPath and was left not-ready"
                        .to_string(),
                );
                continue;
            };
            if !self.env.exists(src) {
                report.objects_skipped += 1;
                continue;
            }

            // ── Copy ─────────────────────────────────────────────────────
            let dst = self.snapshot_path(ns, name);
            self.env
                .copy_tree(src, &dst)
                .map_err(|e| ControllerError::Internal(["snapshot copy failed: ", &e].concat()))?;
            let size = self.env.size_bytes(&dst).unwrap_or(0);

            // ── The VolumeSnapshotContent ────────────────────────────────
            let content_name = ["snapcontent-", ns, "-", name].concat();
            let api_version = [SNAPSHOT_GROUP, "/", SNAPSHOT_VERSION].concat();
            let content = json!({
                "apiVersion": api_version,
                "kind": "VolumeSnapshotContent",
                "metadata": { "name": content_name },
                "spec": {
                    "driver": SNAPSHOT_DRIVER,
                    "deletionPolicy": "Delete",
                    "source": { "volumeHandle": volume_name },
                    "volumeSnapshotRef": {
                        "name": name,
                        "namespace": ns,
                    },
                },
                "status": {
                    "readyToUse": true,
                    "restoreSize": size,
                    "snapshotHandle": dst.clone(),
                },
            });
            self.store
                .propose(ResourceCommand::Put {
                    key: ResourceKey::cluster_scoped(
                        SNAPSHOT_GROUP,
                        SNAPSHOT_VERSION,
                        "VolumeSnapshotContent",
                        content_name.clone(),
                    ),
                    value: content,
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;

            // ── Stamp the snapshot's status ──────────────────────────────
            let mut updated = snap.clone();
            updated["status"] = json!({
                "readyToUse": true,
                "boundVolumeSnapshotContentName": content_name,
                "creationTime": engenho_types::time::now_rfc3339_utc(),
                "restoreSize": size,
            });
            let applied = self
                .store
                .propose(ResourceCommand::Put {
                    key: ResourceKey::namespaced(
                        SNAPSHOT_GROUP,
                        SNAPSHOT_VERSION,
                        "VolumeSnapshot",
                        ns.to_string(),
                        name.clone(),
                    ),
                    value: updated,
                    expected: None,
                    reason: Reason::Controller,
                })
                .await?;
            report.record(Effect::of(applied.op));
        }

        Ok(report.into())
    }
}

/// Deterministic mock [`SnapshotEnv`] — records every copy it was asked to
/// make and never touches a disk. The trait IS the testability contract.
#[derive(Default)]
pub struct FakeSnapshotEnv {
    copies: std::sync::Mutex<Vec<(String, String)>>,
    /// Paths the fake reports as existing. Empty ⇒ everything exists, which
    /// keeps the common case terse; push a path to test the missing-source
    /// refusal.
    missing: std::sync::Mutex<Vec<String>>,
}

impl FakeSnapshotEnv {
    /// The `(src, dst)` copies requested, in order.
    #[must_use]
    pub fn copies(&self) -> Vec<(String, String)> {
        self.copies.lock().unwrap().clone()
    }

    /// Mark a path as absent, so [`SnapshotEnv::exists`] reports false for it.
    pub fn mark_missing(&self, path: &str) {
        self.missing.lock().unwrap().push(path.to_string());
    }
}

impl SnapshotEnv for FakeSnapshotEnv {
    fn copy_tree(&self, src: &str, dst: &str) -> Result<(), String> {
        self.copies
            .lock()
            .unwrap()
            .push((src.to_string(), dst.to_string()));
        Ok(())
    }

    fn exists(&self, path: &str) -> bool {
        !self.missing.lock().unwrap().iter().any(|p| p == path)
    }

    fn size_bytes(&self, _path: &str) -> Result<u64, String> {
        Ok(0)
    }
}

#[cfg(test)]
mod snapshot_paths {
    use super::*;

    const ROOT: &str = "/data/snapshots";

    #[test]
    fn a_snapshot_path_is_namespaced_so_two_namespaces_cannot_collide() {
        // Without the namespace in the path, `a/snap` and `b/snap` would share
        // one directory and the second snapshot would silently overwrite the
        // first — two clusters' worth of data behind one name.
        assert_ne!(
            VolumeSnapshotController::snapshot_path_in(ROOT, "team-a", "nightly"),
            VolumeSnapshotController::snapshot_path_in(ROOT, "team-b", "nightly")
        );
    }

    #[test]
    fn the_path_is_rooted_where_configured() {
        assert!(
            VolumeSnapshotController::snapshot_path_in(ROOT, "default", "s")
                .starts_with("/data/snapshots/")
        );
    }

    #[test]
    fn a_trailing_slash_on_the_root_does_not_double_up() {
        assert!(
            !VolumeSnapshotController::snapshot_path_in("/data/snapshots/", "default", "s")
                .contains("//")
        );
    }

    #[test]
    fn a_ready_snapshot_is_not_recopied() {
        // Idempotence: without this a settled cluster would re-copy every
        // snapshot on every tick, which is unbounded disk IO proportional to
        // uptime rather than to change.
        let ready = json!({"status": {"readyToUse": true}});
        assert!(VolumeSnapshotController::is_ready(&ready));
    }

    /// ★ NEGATIVE CONTROL. `readyToUse` absent or false must both read as
    /// not-ready; treating a missing status as ready would skip the copy and
    /// leave an empty snapshot marked good.
    #[test]
    fn an_unfinished_snapshot_reads_as_not_ready() {
        assert!(!VolumeSnapshotController::is_ready(&json!({})));
        assert!(!VolumeSnapshotController::is_ready(&json!({"status": {}})));
        assert!(!VolumeSnapshotController::is_ready(
            &json!({"status": {"readyToUse": false}})
        ));
    }

    #[test]
    fn the_fake_env_reports_missing_paths_as_absent() {
        let env = FakeSnapshotEnv::default();
        assert!(env.exists("/anything"));
        env.mark_missing("/gone");
        assert!(!env.exists("/gone"));
        assert!(env.exists("/still-here"));
    }
}
