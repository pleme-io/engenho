//! Every destructive operation, as the daemon runs it (P7): preparing a
//! challenge, withdrawing one, and executing the operation it names.
//!
//! The handshake's rules are [`super::confirm`]'s; what each operation moves
//! is [`super::reinit`]'s; the supervisor runs the data-directory ones in its
//! loop, so nothing boots between the checks and the move. This module reads
//! the facts a challenge binds, says what an operation will cost before it is
//! confirmed, and reports what it did.

use chrono::Utc;
use engenho_apiserver::pki_inventory;
use engenho_control_types::types::{self, BlindReason, RefusalReason};
use engenho_control_types::{ControlError, Principal};

use super::confirm::{
    Binding, CaBound, EpochBound, IssueError, Mismatch, RedeemError, caller, digest,
};
use super::reinit::{self, Reinit, SaKey, WipeScope};
use super::service::{DaemonControl, internal};
use crate::boot::Timestamp;
use crate::boot_config::BootConfig;
use crate::layout::Area;
use crate::lifecycle::{DaemonEvent, LifecycleState, ReinitRefused, Snapshot};
use crate::publish::KubeconfigTarget;

/// What the operator does next, after each operation.
const NEXT_ADMIN_TOKEN: &str = "the running apiserver accepts the old token until it boots \
     again: `engenho ctl runtime restart` (or the next start) puts the new one in force";
const NEXT_RESEED: &str = "start the runtime (`engenho ctl runtime start`): it mints a new \
     seed, CA and admin credential and publishes the kubeconfigs again; every client of the \
     old CA needs the new kubeconfig";
const NEXT_WIPE: &str = "start the runtime (`engenho ctl runtime start`): its next boot is a \
     first boot, over an empty store";

impl DaemonControl {
    /// `reinit prepare`: bind a challenge to `request` as things are now.
    pub(super) fn prepare(
        &self,
        by: &Principal,
        request: &types::ReinitRequest,
    ) -> Result<types::Challenge, ControlError> {
        let binding = self.binding(by, request)?;
        let bound = binding.to_wire().map_err(internal)?;
        let blast_radius = self.blast_radius(request);
        let issued = self
            .confirmations
            .issue(binding, Utc::now())
            .map_err(|err| match err {
                IssueError::Full => {
                    ControlError::refused(RefusalReason::LifecycleBusy, err.to_string())
                }
                IssueError::Entropy(_) => internal(err),
            })?;
        Ok(types::Challenge {
            id: confirmation_id(&issued.id)?,
            phrase_hint: bound.cluster_name.clone(),
            expires_at: issued.expires_at,
            bound,
            blast_radius,
        })
    }

    /// `reinit cancel`.
    pub(super) fn cancel(
        &self,
        id: &types::ConfirmationId,
    ) -> Result<types::Cancelled, ControlError> {
        self.confirmations
            .cancel(id.as_str())
            .map_err(|err| redeem_refusal(&err))?;
        Ok(types::Cancelled {
            id: id.clone(),
            cancelled_at: Utc::now(),
        })
    }

    /// Execute `request` under the challenge `id`, confirmed by `phrase`.
    pub(super) async fn execute(
        &self,
        by: &Principal,
        id: &types::ConfirmationId,
        request: &types::ReinitRequest,
        phrase: &str,
    ) -> Result<types::ReinitReport, ControlError> {
        let bound = self
            .confirmations
            .redeem(id.as_str(), Utc::now())
            .map_err(|err| redeem_refusal(&err))?;
        let now = self.binding(by, request)?;
        bound
            .check(&now, phrase)
            .map_err(|m| mismatch_refusal(&m))?;
        let at = Timestamp::now();
        // Each announces itself on the event stream: the supervisor the
        // data-directory operations it runs, this the identity rotation.
        let (attic, stale, next) = match to_domain(request) {
            Some(op) => self.reinitialize(op, now.epoch).await?,
            None => self.rotate_identity(at)?,
        };
        Ok(types::ReinitReport {
            operation: bound.operation,
            executed_at: at.utc(),
            attic_path: attic.display().to_string(),
            stale_kubeconfigs: stale,
            next,
        })
    }

    async fn reinitialize(
        &self,
        op: Reinit,
        epoch: EpochBound,
    ) -> Result<(std::path::PathBuf, Vec<String>, String), ControlError> {
        // Read before the move: what is on disk now is what goes stale.
        let stale = if matches!(op, Reinit::ReseedPki { .. }) {
            self.published_kubeconfigs()
        } else {
            Vec::new()
        };
        let epoch = match epoch {
            EpochBound::Stopped(epoch) => Some(epoch),
            EpochBound::NotRequired => None,
        };
        let moved = self
            .p
            .supervisor
            .reinit(op, epoch)
            .await
            .map_err(|err| reinit_refusal(&err))?;
        let next = match op {
            Reinit::RotateAdminToken => NEXT_ADMIN_TOKEN,
            Reinit::ReseedPki { .. } => NEXT_RESEED,
            Reinit::WipeStore { .. } => NEXT_WIPE,
        };
        Ok((moved.attic, stale, next.to_owned()))
    }

    fn rotate_identity(
        &self,
        at: Timestamp,
    ) -> Result<(std::path::PathBuf, Vec<String>, String), ControlError> {
        let identity = self.p.remote.identity.as_ref().map_err(|why| {
            ControlError::blind(
                BlindReason::IoError,
                ["there is no control identity to rotate: ", why.as_str()].concat(),
            )
        })?;
        let attic = reinit::attic_dir(
            &self.p.data_dir,
            reinit::ReinitOp::RotateControlIdentity.name(),
            at,
        );
        let keep = attic
            .join(Area::Control.dir())
            .join(engenho_control_server::identity::DIR)
            .join(engenho_control_server::identity::KEY);
        let spki = identity
            .rotate(&keep)
            .map_err(|e| ControlError::blind(BlindReason::IoError, e.to_string()))?;
        self.p
            .supervisor
            .events()
            .push(DaemonEvent::ReinitExecuted {
                operation: reinit::ReinitOp::RotateControlIdentity,
            });
        let next = [
            "every remote client must now pin ",
            spki.to_string().as_str(),
            " as this daemon's server key (remotes.yaml `server_spki`; \
             pleme.engenho.remotes.<name>.serverSpki)",
        ]
        .concat();
        Ok((attic, Vec::new(), next))
    }

    /// Everything a challenge for `request` binds, read now.
    fn binding(
        &self,
        by: &Principal,
        request: &types::ReinitRequest,
    ) -> Result<Binding, ControlError> {
        let snapshot = self.snapshot();
        let (cluster_name, node_name) = self.names(&snapshot)?;
        let needs_stopped = to_domain(request).is_some_and(Reinit::needs_stopped);
        let epoch = if needs_stopped {
            if !resting(&snapshot.lifecycle) {
                return Err(ControlError::refused_with(
                    RefusalReason::RuntimeRunning,
                    [
                        op_of(request).to_string().as_str(),
                        " needs the runtime stopped",
                    ]
                    .concat(),
                    vec!["engenho ctl runtime stop".to_owned()],
                ));
            }
            EpochBound::Stopped(snapshot.epoch)
        } else {
            EpochBound::NotRequired
        };
        Ok(Binding {
            cluster_name,
            node_name,
            ca: CaBound::of(&pki_inventory::inventory(&self.p.data_dir).ca),
            epoch,
            operation: op_of(request),
            params: digest(request),
            principal: caller(by.attested()),
        })
    }

    /// The cluster's and this node's names: as configured, or — when the
    /// configuration does not resolve — as the first boot recorded them.
    fn names(&self, snapshot: &Snapshot) -> Result<(String, String), ControlError> {
        match (self.effective(), &snapshot.identity) {
            (Ok(resolved), _) => Ok((
                resolved.config.cluster.name,
                resolved.config.runtime.node_name,
            )),
            (Err(_), Some(first)) => Ok((first.cluster_name.clone(), first.node_name.clone())),
            (Err(err), None) => Err(err),
        }
    }

    /// Every kubeconfig the configuration publishes that is on disk now.
    fn published_kubeconfigs(&self) -> Vec<String> {
        let Some(boot) = self
            .effective()
            .ok()
            .and_then(|resolved| BootConfig::read(&resolved.config).ok())
        else {
            return Vec::new();
        };
        KubeconfigTarget::ALL
            .iter()
            .filter_map(|&target| crate::runtime::kubeconfig_path(&boot, target).ok())
            .filter(|path| path.exists())
            .map(|path| path.display().to_string())
            .collect()
    }

    /// What `request` will cost, one consequence a line.
    fn blast_radius(&self, request: &types::ReinitRequest) -> Vec<String> {
        let mut lines: Vec<String> = match request {
            types::ReinitRequest::RotateAdminToken => vec![
                "pki/admin.token is replaced; the old bearer token is refused once the runtime \
                 boots again"
                    .into(),
            ],
            types::ReinitRequest::ReseedPki { sa_key } => {
                let mut lines = vec![
                    "the cluster seed, CA and admin credential are replaced; the next boot mints \
                     new ones"
                        .into(),
                ];
                lines.extend(self.published_kubeconfigs().into_iter().map(|path| {
                    [
                        "kubeconfig ",
                        path.as_str(),
                        " goes stale until it is published again",
                    ]
                    .concat()
                }));
                if matches!(sa_key, types::SaKeyAction::Rotate) {
                    lines.push(
                        "the ServiceAccount signing key is replaced: every token minted so far \
                         stops verifying"
                            .into(),
                    );
                }
                lines
            }
            types::ReinitRequest::WipeStore { scope } => {
                let mut lines = vec![
                    "every object in the cluster is gone: the next boot starts from an empty \
                     store"
                        .into(),
                ];
                if matches!(scope, types::WipeScope::StoreAndNodeLocal) {
                    lines.push(
                        "pod volumes, local-path volumes, snapshots, plugin and pod state on this \
                         node go with it"
                            .into(),
                    );
                }
                lines
            }
            types::ReinitRequest::RotateControlIdentity => {
                let pin = self
                    .p
                    .remote
                    .identity
                    .as_ref()
                    .map(|id| id.spki().to_string())
                    .unwrap_or_default();
                vec![
                    [
                        "every remote client pinned to ",
                        pin.as_str(),
                        " is refused until it pins the new key",
                    ]
                    .concat(),
                ]
            }
        };
        lines.push("what is replaced is moved to data_dir/control/attic/, never deleted".into());
        lines
    }
}

/// Stopped, or a failed boot: no runtime holds the store.
const fn resting(lifecycle: &LifecycleState) -> bool {
    matches!(
        lifecycle,
        LifecycleState::Stopped { .. } | LifecycleState::Failed { .. }
    )
}

/// The data-directory operation `request` names; `None` for the control
/// identity, which is not one.
const fn to_domain(request: &types::ReinitRequest) -> Option<Reinit> {
    match request {
        types::ReinitRequest::RotateAdminToken => Some(Reinit::RotateAdminToken),
        types::ReinitRequest::ReseedPki { sa_key } => Some(Reinit::ReseedPki {
            sa_key: match sa_key {
                types::SaKeyAction::Keep => SaKey::Keep,
                types::SaKeyAction::Rotate => SaKey::Rotate,
            },
        }),
        types::ReinitRequest::WipeStore { scope } => Some(Reinit::WipeStore {
            scope: match scope {
                types::WipeScope::StoreOnly => WipeScope::StoreOnly,
                types::WipeScope::StoreAndNodeLocal => WipeScope::StoreAndNodeLocal,
            },
        }),
        types::ReinitRequest::RotateControlIdentity => None,
    }
}

/// The operation `request` names.
pub(super) const fn op_of(request: &types::ReinitRequest) -> types::ReinitOp {
    match request {
        types::ReinitRequest::RotateAdminToken => types::ReinitOp::RotateAdminToken,
        types::ReinitRequest::ReseedPki { .. } => types::ReinitOp::ReseedPki,
        types::ReinitRequest::WipeStore { .. } => types::ReinitOp::WipeStore,
        types::ReinitRequest::RotateControlIdentity => types::ReinitOp::RotateControlIdentity,
    }
}

fn confirmation_id(id: &str) -> Result<types::ConfirmationId, ControlError> {
    types::ConfirmationId::try_from(id).map_err(internal)
}

const PREPARE: &str = "engenho ctl reinit prepare";

fn redeem_refusal(err: &RedeemError) -> ControlError {
    match err {
        RedeemError::Unknown(_) => ControlError::refused_with(
            RefusalReason::ConfirmationRequired,
            err.to_string(),
            vec![PREPARE.to_owned()],
        ),
        RedeemError::Expired(_) => ControlError::refused_with(
            RefusalReason::ConfirmationExpired,
            err.to_string(),
            vec![PREPARE.to_owned()],
        ),
    }
}

fn mismatch_refusal(mismatch: &Mismatch) -> ControlError {
    ControlError::refused_with(
        RefusalReason::ConfirmationMismatch,
        [mismatch.to_string().as_str(), "; the challenge is used up"].concat(),
        vec![PREPARE.to_owned()],
    )
}

fn reinit_refusal(err: &ReinitRefused) -> ControlError {
    let because = err.to_string();
    match err {
        ReinitRefused::NotStopped => ControlError::refused_with(
            RefusalReason::RuntimeRunning,
            because,
            vec!["engenho ctl runtime stop".to_owned()],
        ),
        ReinitRefused::EpochMoved { .. } => ControlError::refused_with(
            RefusalReason::ConfirmationMismatch,
            because,
            vec![PREPARE.to_owned()],
        ),
        ReinitRefused::StoreHeld(_) => ControlError::refused(RefusalReason::LifecycleBusy, because),
        ReinitRefused::Failed(_) => ControlError::blind(BlindReason::IoError, because),
        ReinitRefused::Gone => ControlError::blind(BlindReason::Internal, because),
    }
}
