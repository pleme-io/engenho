//! `CniStatusController` — publishing which CNI this node actually has.
//!
//! ★ THE PRODUCER, LANDED WITH THE VOCABULARY. `engenho-cni` would
//! otherwise be instance #9 of "type + backend + no producer" — the exact
//! pattern this codebase has now hit eight times, where every symbol exists,
//! every test passes, and nothing calls it. So the contract ships with
//! something that reads it.
//!
//! ★ WHAT IT PUBLISHES, AND WHY EACH FIELD EARNS ITS PLACE.
//!
//! * `engenho.io/cni-install` — `Planned` or `Invoked`. Nothing else in the
//!   cluster distinguishes a computed chain from an executed one: the pod
//!   gets an IP either way, `kubectl get pod -o wide` shows it either way,
//!   and Endpoints lists it either way. On darwin the address comes from
//!   podman rather than from IPAM, and an operator debugging why a pod is
//!   unreachable from another node needs to know that BEFORE they start
//!   reading plugin logs that do not exist.
//! * `engenho.io/cni-network` — the network name from the winning config
//!   file, or `<none>`. `net.d` is a shared directory where first-lexical
//!   wins; when a cluster ends up on the wrong CNI it is almost always
//!   because a file nobody expected sorted first.
//! * `engenho.io/cni-config` — the file it came from, so the previous line
//!   is checkable rather than merely assertable.
//!
//! ★ SKIPPED FILES ARE LOGGED, NOT PUBLISHED. A half-written config is a
//! transient state that would otherwise churn the Node object on every
//! tick; the diagnostic value is in the log, and the annotation stays a
//! statement about what IS in effect.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use engenho_cni::config::load_conflist_dir;
use engenho_cni::exec::CniInstall;
use engenho_store::StoreMesh;
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::resource::ResourceKey;

use crate::controller::{Controller, ReconcileOutcome, ReconcileReport};
use crate::error::ControllerError;

/// Whether the plugin chain is executed or only computed.
pub const INSTALL_ANNOTATION: &str = "engenho.io/cni-install";
/// The effective network's name.
pub const NETWORK_ANNOTATION: &str = "engenho.io/cni-network";
/// The file the effective configuration came from.
pub const CONFIG_ANNOTATION: &str = "engenho.io/cni-config";
/// The value used when no configuration is present at all.
pub const NO_NETWORK: &str = "<none>";

/// The three annotations this controller maintains, computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CniStatus {
    /// `Planned` or `Invoked`.
    pub install: CniInstall,
    /// Network name, or [`NO_NETWORK`].
    pub network: String,
    /// Source file, or [`NO_NETWORK`].
    pub config: String,
}

impl CniStatus {
    /// Whether a Node already carries exactly this status.
    ///
    /// Idempotency is not cosmetic here: this controller runs on every tick
    /// and rewriting an unchanged Node would advance the store revision
    /// forever — the hot-loop class the node lease already hit once.
    #[must_use]
    pub fn already_on(&self, node: &Value) -> bool {
        let ann = |k: &str| {
            node.get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(k))
                .and_then(Value::as_str)
        };
        ann(INSTALL_ANNOTATION) == Some(self.install.as_str())
            && ann(NETWORK_ANNOTATION) == Some(self.network.as_str())
            && ann(CONFIG_ANNOTATION) == Some(self.config.as_str())
    }

    /// The Node with this status applied.
    #[must_use]
    pub fn applied_to(&self, node: &Value) -> Value {
        let mut out = node.clone();
        let Some(obj) = out.as_object_mut() else {
            return out;
        };
        let meta = obj.entry("metadata").or_insert_with(|| json!({}));
        let Some(meta) = meta.as_object_mut() else {
            return out;
        };
        let ann = meta.entry("annotations").or_insert_with(|| json!({}));
        if let Some(ann) = ann.as_object_mut() {
            ann.insert(INSTALL_ANNOTATION.into(), json!(self.install.as_str()));
            ann.insert(NETWORK_ANNOTATION.into(), json!(self.network));
            ann.insert(CONFIG_ANNOTATION.into(), json!(self.config));
        }
        out
    }
}

/// Reads `net.d` and publishes the verdict onto this node.
pub struct CniStatusController {
    store: Arc<StoreMesh>,
    node_name: String,
    net_d: PathBuf,
    install: CniInstall,
}

impl CniStatusController {
    /// New controller for `node_name`, reading `net_d`, reporting `install`.
    ///
    /// `install` is passed in rather than detected here: which backend the
    /// runtime chose is an assembly-time decision, and re-deriving it would
    /// give two places that can disagree about the one fact this controller
    /// exists to publish.
    #[must_use]
    pub fn new(
        store: Arc<StoreMesh>,
        node_name: impl Into<String>,
        net_d: impl Into<PathBuf>,
        install: CniInstall,
    ) -> Self {
        Self {
            store,
            node_name: node_name.into(),
            net_d: net_d.into(),
            install,
        }
    }

    /// The status this node should carry, read from disk.
    #[must_use]
    pub fn observe(&self) -> CniStatus {
        match load_conflist_dir(&self.net_d) {
            Ok((Some(cfg), skipped)) => {
                for (path, err) in &skipped {
                    // Logged, not published: a half-written file is a
                    // transient state, and annotating it would churn the
                    // Node every tick.
                    tracing::warn!(
                        file = %path.display(),
                        error = %err,
                        "skipping unreadable CNI config"
                    );
                }
                CniStatus {
                    install: self.install,
                    network: cfg.name.clone(),
                    config: cfg.source.display().to_string(),
                }
            }
            Ok((None, skipped)) => {
                for (path, err) in &skipped {
                    tracing::warn!(
                        file = %path.display(),
                        error = %err,
                        "skipping unreadable CNI config"
                    );
                }
                CniStatus {
                    install: self.install,
                    network: NO_NETWORK.to_string(),
                    config: NO_NETWORK.to_string(),
                }
            }
            Err(e) => {
                tracing::warn!(dir = %self.net_d.display(), error = %e, "cannot read CNI config dir");
                CniStatus {
                    install: self.install,
                    network: NO_NETWORK.to_string(),
                    config: NO_NETWORK.to_string(),
                }
            }
        }
    }
}

#[async_trait]
impl Controller for CniStatusController {
    fn name(&self) -> &'static str {
        "cni-status"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let key = ResourceKey::cluster_scoped("", "v1", "Node", &self.node_name);
        // No Node yet is not an error: the kubelet registers it, and on a
        // cold start this controller can legitimately run first.
        let Some(node) = self.store.get(&key).await else {
            return Ok(ReconcileOutcome::from(report));
        };
        report.objects_examined = 1;

        let status = self.observe();
        if status.already_on(&node) {
            report.objects_skipped = 1;
            return Ok(ReconcileOutcome::from(report));
        }

        match self
            .store
            .propose(ResourceCommand::Put {
                key,
                value: status.applied_to(&node),
                expected: None,
                reason: Reason::Controller,
            })
            .await
        {
            Ok(_) => report.objects_changed = 1,
            Err(_) => report.objects_skipped = 1,
        }
        Ok(ReconcileOutcome::from(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> Value {
        json!({ "apiVersion": "v1", "kind": "Node", "metadata": { "name": "n1" } })
    }

    fn status(install: CniInstall) -> CniStatus {
        CniStatus {
            install,
            network: "cbr0".into(),
            config: "/etc/cni/net.d/10-cbr0.conflist".into(),
        }
    }

    #[test]
    fn the_verdict_lands_on_the_node_and_is_idempotent() {
        let s = status(CniInstall::Planned);
        assert!(!s.already_on(&node()));
        let once = s.applied_to(&node());
        assert_eq!(
            once["metadata"]["annotations"][INSTALL_ANNOTATION],
            "Planned"
        );
        assert_eq!(once["metadata"]["annotations"][NETWORK_ANNOTATION], "cbr0");
        assert!(s.already_on(&once));
        // Rewriting an unchanged Node every tick advances the revision
        // forever — the hot-loop class the node lease already hit.
        assert_eq!(once, s.applied_to(&once));
    }

    #[test]
    fn a_planned_verdict_never_satisfies_an_invoked_check() {
        // The direction that matters: a node whose plugins did NOT run must
        // never read as one whose did.
        let planned = status(CniInstall::Planned).applied_to(&node());
        assert!(!status(CniInstall::Invoked).already_on(&planned));
    }

    #[test]
    fn a_changed_config_file_is_republished() {
        // net.d is a shared directory and first-lexical wins; when a
        // cluster ends up on the wrong CNI it is because a file nobody
        // expected sorted first. So the SOURCE has to be part of the
        // identity, not just the network name.
        let a = status(CniInstall::Planned).applied_to(&node());
        let b = CniStatus {
            install: CniInstall::Planned,
            network: "cbr0".into(),
            config: "/etc/cni/net.d/00-somebody-elses.conf".into(),
        };
        assert!(!b.already_on(&a), "a different source file is a change");
    }

    #[test]
    fn existing_annotations_survive() {
        let mut n = node();
        n["metadata"]["annotations"] = json!({ "keep": "me" });
        let out = status(CniInstall::Invoked).applied_to(&n);
        assert_eq!(out["metadata"]["annotations"]["keep"], "me");
        assert_eq!(
            out["metadata"]["annotations"][INSTALL_ANNOTATION],
            "Invoked"
        );
    }
}
