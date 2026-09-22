//! The daemon's answer to every control operation.
//!
//! [`DaemonControl`] implements the spec's [`EngenhoControl`] once, over the
//! supervisor: lifecycle and journal from its published [`Snapshot`] (never
//! waiting on the loop), the running runtime's children, store and
//! kubeconfigs from [`SupervisorHandle::inspect`], the PKI from disk
//! (read-only), and the streams from their rings.
//!
//! Domain types whose serde shape is the spec's (the lifecycle, the journal,
//! the publish records) cross by value through [`wire`]; a shape that drifts
//! from the spec fails there — the parity test exercises every variant.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::pki_inventory::{self, CaFact, CertFact, FileFact};
use engenho_config::{
    ConfigTierKind, EngenhoConfig, GroupTier, LeafPath, ProvenanceMap, SocketAccess, TieredConfig,
};
use engenho_control_server::AuditLog;
use engenho_control_types::ops::{
    CancelConfirmationRequest, ClearConfigOverridesRequest, CreateConfirmationRequest,
    DisableDriverRequest, EnableDriverRequest, ExitProcessRequest, GetBootRequest, GetChildRequest,
    GetConfigDriftRequest, GetConfigLeafRequest, GetConfigRequest, GetControlRequest,
    GetInitStateRequest, GetPkiRequest, GetRuntimeRequest, GetSpecRequest, GetStoreRequest,
    HelloRequest, ListAuditRequest, ListBootAttemptsRequest, ListChildrenRequest,
    ListConfigLeavesRequest, ListConfigOverridesRequest, ListEventsRequest, ListKubeconfigsRequest,
    ListLogsRequest, PublishKubeconfigRequest, ReloadConfigRequest, ReseedPkiRequest,
    RestartChildRequest, RestartRuntimeRequest, RetryBootRequest, RotateAdminTokenRequest,
    RotateControlIdentityRequest, SetConfigLeafRequest, StartRuntimeRequest, StopRuntimeRequest,
    UnsetConfigLeafRequest, WipeStoreRequest,
};
use engenho_control_types::types::{self, BlindReason, RefusalReason};
use engenho_control_types::{
    API_VERSION, AuthorityTier, ControlError, EngenhoControl, OperationId, Principal, SPEC_YAML,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::apply::ApplyEffect;
use super::configure::{
    ApplyOptions, leaf_path, reconfigure_refusal, restart_now, unreadable_overrides,
};
use super::confirm::{CaBound, ConfirmationBook};
use super::logs::LogEntry;
use super::names::{
    area_to_wire, child_from_wire, child_kind, child_to_wire, death_to_wire, driver_from_wire,
    driver_to_wire, reinit_op_to_wire, respawn_to_wire,
};
use super::overrides::Change;
use super::ring::{Page, Ring};
use crate::child::{Child, ChildState, RespawnError};
use crate::layout::Area;
use crate::lifecycle::journal::IdentityRecord;
use crate::lifecycle::supervisor::{CommandError, file_digest};
use crate::lifecycle::{
    ConfigSource, DaemonEvent, DataDirSource, ExitIntent, Hold, Inspection, LifecycleState,
    PendingApply, RefusedBecause, ResolvedConfig, RestartChildError, RuntimeFacts, Snapshot,
    StoreLock, SupervisorHandle,
};
use crate::publish::{KubeconfigTarget, PublishRecord, SkipReason};
use crate::runtime::STORE_DIR;

/// Carry a value whose serde shape is the spec's across to the spec's type.
///
/// # Errors
///
/// A blind (internal): the two shapes disagree, which the parity test
/// exists to catch before a caller can.
pub fn wire<T: Serialize, U: DeserializeOwned>(value: &T) -> Result<U, ControlError> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(|e| {
            ControlError::blind(
                BlindReason::Internal,
                format!("a daemon value does not match the control API's shape: {e}"),
            )
        })
}

/// The local socket, as the control API reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketFacts {
    /// Its path.
    pub path: PathBuf,
    /// Who may connect.
    pub access: SocketAccess,
    /// What a group member may do.
    pub group_tier: GroupTier,
}

/// The remote listener, as the control API reports it.
pub struct RemoteFacts {
    /// Where it is.
    pub state: tokio::sync::watch::Receiver<engenho_control_server::RemoteState>,
    /// The listener's own key — read at each request, since it can be
    /// rotated — or why there is none.
    pub identity: Result<Arc<engenho_control_server::ControlIdentity>, String>,
    /// Who it admits.
    pub pins: engenho_control_server::Pins,
}

/// What [`DaemonControl`] is built from.
pub struct DaemonControlParts {
    /// The supervisor.
    pub supervisor: SupervisorHandle,
    /// Where configuration comes from (for a runtime that is not running).
    pub source: ConfigSource,
    /// The data directory and how it was decided.
    pub data_dir: PathBuf,
    /// How the data directory was decided.
    pub data_dir_source: DataDirSource,
    /// The declared configuration file.
    pub declared: Option<PathBuf>,
    /// The local socket.
    pub socket: SocketFacts,
    /// The remote listener.
    pub remote: RemoteFacts,
    /// The daemon's recent log.
    pub logs: Arc<Ring<LogEntry>>,
    /// The audit chain.
    pub audit: Arc<AuditLog>,
    /// The source revision the binary was built from.
    pub git_rev: String,
}

/// The daemon's [`EngenhoControl`].
pub struct DaemonControl {
    pub(super) p: DaemonControlParts,
    /// One configuration change at a time, from planning to applied: two
    /// changes planned against one generation cannot both commit.
    pub(super) applying: tokio::sync::Mutex<()>,
    /// The destructive operations' pending challenges.
    pub(super) confirmations: ConfirmationBook,
}

/// The longest a long-poll may wait.
const MAX_WAIT: Duration = Duration::from_secs(30);
/// Items per page when the caller does not say.
const DEFAULT_LIMIT: usize = 256;

impl DaemonControl {
    /// The control surface over `parts`.
    #[must_use]
    pub fn new(parts: DaemonControlParts) -> Self {
        Self {
            p: parts,
            applying: tokio::sync::Mutex::new(()),
            confirmations: ConfirmationBook::default(),
        }
    }

    /// Whether this daemon serves `id` yet. Everything else is refused as
    /// unsupported (and left out of `hello`'s capabilities).
    #[must_use]
    pub const fn serves(id: OperationId) -> bool {
        use OperationId as O;
        match id {
            O::Hello
            | O::GetSpec
            | O::GetRuntime
            | O::GetBoot
            | O::ListBootAttempts
            | O::GetInitState
            | O::GetConfig
            | O::GetConfigDrift
            | O::ListChildren
            | O::GetChild
            | O::GetPki
            | O::GetStore
            | O::ListKubeconfigs
            | O::ListEvents
            | O::ListLogs
            | O::ListAudit
            | O::GetControl
            | O::StartRuntime
            | O::StopRuntime
            | O::RestartRuntime
            | O::RetryBoot
            | O::ExitProcess
            | O::ListConfigLeaves
            | O::GetConfigLeaf
            | O::ListConfigOverrides
            | O::SetConfigLeaf
            | O::UnsetConfigLeaf
            | O::ClearConfigOverrides
            | O::ReloadConfig
            | O::PublishKubeconfig
            | O::RestartChild
            | O::EnableDriver
            | O::DisableDriver
            | O::CreateConfirmation
            | O::CancelConfirmation
            | O::RotateAdminToken
            | O::ReseedPki
            | O::WipeStore
            | O::RotateControlIdentity => true,
        }
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        self.p.supervisor.snapshot()
    }

    fn lifecycle(snapshot: &Snapshot) -> Result<types::LifecycleState, ControlError> {
        wire(&snapshot.lifecycle)
    }

    fn daemon_info(&self, snapshot: &Snapshot) -> types::DaemonInfo {
        types::DaemonInfo {
            version: snapshot.daemon.version.clone(),
            git_rev: self.p.git_rev.clone(),
            pid: snapshot.daemon.pid,
            started_at: snapshot.daemon.started_at.utc(),
        }
    }

    async fn inspect(&self) -> Result<Inspection, ControlError> {
        self.p.supervisor.inspect().await.map_err(gone)
    }

    /// The effective configuration: the declared file with the override
    /// tier folded over it, resolved now.
    pub(super) fn effective(&self) -> Result<ResolvedConfig, ControlError> {
        self.p.source.resolve().map_err(|e| {
            ControlError::refused_with(
                RefusalReason::ConfigRejected,
                format!("the configuration does not resolve: {e}"),
                vec![
                    "fix the declared file (the daemon retries a held boot by itself)".into(),
                    "engenho ctl config set <leaf> --value <v>".into(),
                ],
            )
        })
    }

    /// The configuration to describe the daemon by: the effective one, or
    /// the running runtime's when the effective one does not resolve.
    fn configuration(
        &self,
        facts: Option<&RuntimeFacts>,
    ) -> Result<(EngenhoConfig, Option<ProvenanceMap>), ControlError> {
        match (self.effective(), facts) {
            (Ok(resolved), _) => Ok((resolved.config, resolved.provenance)),
            (Err(_), Some(facts)) => Ok((facts.config.clone(), facts.provenance.clone())),
            (Err(err), None) => Err(err),
        }
    }

    fn pending(snapshot: &Snapshot) -> PendingApply {
        match &snapshot.lifecycle {
            LifecycleState::Running { pending, .. } => pending.clone(),
            _ => PendingApply::InSync,
        }
    }

    fn ca_binding(&self) -> types::CaBinding {
        CaBound::of(&pki_inventory::inventory(&self.p.data_dir).ca).to_wire()
    }

    fn store_facts(inspection: &Inspection) -> types::StoreFacts {
        let state = match &inspection.runtime {
            Some(facts) if facts.config.runtime.durable => types::StoreState::Live {
                kind: wire(&facts.boot_kind).unwrap_or(types::BootKind::Resume),
                revision: facts.revision,
                leader: facts.leader,
            },
            Some(_) => types::StoreState::Ephemeral,
            None if inspection.store_present => types::StoreState::PresentOffline,
            None => types::StoreState::Absent,
        };
        let lock = match inspection.store_lock {
            StoreLock::HeldByThisDaemon => types::LockState::HeldByThisDaemon,
            StoreLock::HeldByOtherProcess => types::LockState::HeldByOtherProcess,
            StoreLock::Free => types::LockState::Free,
            StoreLock::Unknown => types::LockState::Unknown,
        };
        types::StoreFacts { state, lock }
    }

    fn publish_records(inspection: &Inspection) -> Result<Vec<types::PublishRecord>, ControlError> {
        let records = inspection.runtime.as_ref().map_or_else(
            || PublishRecord::all_skipped(SkipReason::RuntimeDown),
            |facts| facts.publish.clone(),
        );
        records.iter().map(wire).collect()
    }

    fn pki(&self, inspection: &Inspection) -> Result<types::PkiInventory, ControlError> {
        let inv = pki_inventory::inventory(&self.p.data_dir);
        let server_cert = match &inspection.runtime {
            Some(facts) => match &facts.server_sans {
                Some(sans) => types::ServerCertFact::Issued { sans: sans.clone() },
                None => types::ServerCertFact::NotIssued {
                    reason: types::ServerCertFactReason::TlsDisabled,
                },
            },
            None => types::ServerCertFact::NotIssued {
                reason: types::ServerCertFactReason::RuntimeDown,
            },
        };
        Ok(types::PkiInventory {
            ca: ca_fact(inv.ca)?,
            cluster_seed: file_fact(inv.cluster_seed)?,
            sa_key: file_fact(inv.sa_key)?,
            admin_token: file_fact(inv.admin_token)?,
            admin_cert: cert_fact(inv.admin_cert),
            server_cert,
        })
    }

    fn children(facts: &RuntimeFacts) -> Vec<types::ChildView> {
        Child::all()
            .map(|child| {
                let fact = facts.children.iter().find(|f| f.child == child);
                let state = fact.map_or(types::ChildState::Disabled, |f| match f.state {
                    ChildState::Running => types::ChildState::Running {
                        since: f.spawned_at.utc(),
                    },
                    ChildState::Dead(cause) => types::ChildState::Dead {
                        at: f.ended_at.unwrap_or(f.spawned_at).utc(),
                        cause: death_to_wire(cause),
                    },
                });
                let last_death =
                    fact.and_then(|f| f.last_death)
                        .map_or(types::LastDeath::Never, |death| {
                            types::LastDeath::Recorded {
                                at: death.at.utc(),
                                cause: death_to_wire(death.cause),
                            }
                        });
                types::ChildView {
                    child: child_to_wire(child),
                    kind: child_kind(child),
                    state,
                    generation: fact.map_or(0, |f| f.generation),
                    last_death,
                    respawn: respawn_to_wire(child.respawn()),
                }
            })
            .collect()
    }

    /// `children enable|disable`: the driver's `controllers.enable` switch,
    /// set as a persisted override through the one apply pipeline — so it is
    /// gated, audited and reported exactly as `config set` of that leaf is.
    async fn toggle_driver(
        &self,
        by: &Principal,
        name: types::DriverName,
        enabled: bool,
    ) -> Result<types::DriverToggleReport, ControlError> {
        let driver = driver_from_wire(name);
        let Some(switch) = driver.switch() else {
            return Err(ControlError::refused_with(
                RefusalReason::InvalidValue,
                [
                    name.to_string().as_str(),
                    " always runs: no controllers.enable switch turns it on or off",
                ]
                .concat(),
                vec![["engenho ctl children restart ", name.to_string().as_str()].concat()],
            ));
        };
        let path = LeafPath::parse(switch.leaf()).map_err(internal)?;
        let body = types::SetLeafRequest {
            dry_run: false,
            persist: true,
            precondition_generation: None,
            restart_policy: None,
            value: serde_json::Value::Bool(enabled),
        };
        let apply = self.set(by, path, body).await?;
        Ok(types::DriverToggleReport {
            driver: name,
            enabled,
            apply,
        })
    }

    fn identity(
        &self,
        snapshot: &Snapshot,
        config: &EngenhoConfig,
        provenance: Option<&ProvenanceMap>,
    ) -> types::IdentityFacts {
        let overrides = self.p.source.overrides().map(|store| store.path());
        let sourced = |path: &[&str], value: &str| types::SourcedValue {
            value: value.to_owned(),
            tier: tier_of(provenance, path, overrides),
        };
        let first_boot = match &snapshot.identity {
            None => types::FirstBootIdentity::NotRecorded,
            Some(IdentityRecord {
                cluster_name,
                node_name,
                recorded_at,
            }) if *cluster_name == config.cluster.name
                && *node_name == config.runtime.node_name =>
            {
                types::FirstBootIdentity::Matches {
                    recorded_at: recorded_at.utc(),
                }
            }
            Some(record) => types::FirstBootIdentity::Drifted {
                recorded_at: record.recorded_at.utc(),
                recorded_cluster_name: record.cluster_name.clone(),
                recorded_node_name: record.node_name.clone(),
            },
        };
        types::IdentityFacts {
            cluster_name: sourced(&["cluster", "name"], &config.cluster.name),
            node_name: sourced(&["runtime", "node_name"], &config.runtime.node_name),
            first_boot,
        }
    }

    fn layout(&self) -> Vec<types::LayoutEntry> {
        Area::ALL
            .iter()
            .map(|&area| types::LayoutEntry {
                name: area_to_wire(area),
                presence: if area.path(&self.p.data_dir).exists() {
                    types::Presence::Present
                } else {
                    types::Presence::Absent
                },
            })
            .collect()
    }

    fn declared_source(&self) -> Result<types::DeclaredSource, ControlError> {
        let Some(path) = self.p.declared.as_deref() else {
            return Ok(types::DeclaredSource::None);
        };
        let Some(digest) = file_digest(path) else {
            return Ok(types::DeclaredSource::None);
        };
        Ok(types::DeclaredSource::File {
            path: path.display().to_string(),
            digest: types::Blake3Hex::try_from(digest).map_err(internal)?,
        })
    }

    async fn command<T, U>(
        &self,
        run: impl std::future::Future<Output = Result<T, CommandError>>,
    ) -> Result<U, ControlError>
    where
        T: Serialize,
        U: DeserializeOwned,
    {
        match run.await {
            Ok(done) => wire(&done),
            Err(err) => Err(refusal(err)),
        }
    }
}

fn gone(err: CommandError) -> ControlError {
    refusal(err)
}

fn supervisor_gone() -> ControlError {
    ControlError::blind(BlindReason::Internal, "the daemon's supervisor has ended")
}

/// A child restart's refusal in the control API's vocabulary.
fn restart_refusal(err: &RestartChildError) -> ControlError {
    let because = err.to_string();
    let legal = |hint: &str| vec![hint.to_owned()];
    match *err {
        RestartChildError::Gone => supervisor_gone(),
        RestartChildError::NotRunning => ControlError::refused_with(
            RefusalReason::RuntimeNotRunning,
            because,
            legal("engenho ctl runtime start"),
        ),
        RestartChildError::Refused(RespawnError::RuntimeRestartOnly(_)) => {
            ControlError::refused_with(
                RefusalReason::RespawnRefused,
                because,
                legal("engenho ctl runtime restart"),
            )
        }
        RestartChildError::Refused(RespawnError::NotSpawned(child)) => {
            let enable = match child {
                Child::Driver(driver) if driver.switch().is_some() => {
                    let name = driver_to_wire(driver).to_string();
                    legal(&["engenho ctl children enable ", name.as_str()].concat())
                }
                Child::Driver(_) | Child::Listener(_) | Child::NodeLease => Vec::new(),
            };
            ControlError::refused_with(RefusalReason::RespawnRefused, because, enable)
        }
        RestartChildError::Refused(RespawnError::Build { .. }) => {
            ControlError::blind(BlindReason::Internal, because)
        }
    }
}

/// A supervisor refusal in the control API's vocabulary.
fn refusal(err: CommandError) -> ControlError {
    match err {
        CommandError::Gone => supervisor_gone(),
        CommandError::Refused(r) => {
            let (reason, legal): (RefusalReason, &[&str]) = match r.reason {
                RefusedBecause::RuntimeRunning => (
                    RefusalReason::RuntimeRunning,
                    &["engenho ctl runtime restart"],
                ),
                RefusedBecause::RuntimeNotRunning => (
                    RefusalReason::RuntimeNotRunning,
                    &["engenho ctl runtime start"],
                ),
                RefusedBecause::RuntimeNotFailed => (RefusalReason::RuntimeNotFailed, &[]),
                RefusedBecause::RuntimeNotStopped => (
                    RefusalReason::RuntimeNotStopped,
                    &["engenho ctl runtime retry"],
                ),
                RefusedBecause::LifecycleBusy | RefusedBecause::Unexpected => {
                    (RefusalReason::LifecycleBusy, &[])
                }
                RefusedBecause::Wedged => (
                    RefusalReason::Wedged,
                    &["engenho ctl runtime exit --body '{\"intent\":\"relaunch\"}'"],
                ),
            };
            ControlError::refused_with(
                reason,
                r.to_string(),
                legal.iter().map(|s| (*s).to_owned()).collect(),
            )
        }
    }
}

pub(super) fn internal(err: impl std::fmt::Display) -> ControlError {
    ControlError::blind(BlindReason::Internal, err.to_string())
}

/// Which tier gave a leaf its value. Both the declared file and the override
/// tier are shikumi `Custom` file layers; the file they came from tells them
/// apart.
pub(super) fn tier_of(
    provenance: Option<&ProvenanceMap>,
    path: &[&str],
    overrides: Option<&std::path::Path>,
) -> types::ConfigTierName {
    let Some(p) = provenance.and_then(|p| p.provenance_of(path)) else {
        return types::ConfigTierName::Default;
    };
    match p.tier() {
        ConfigTierKind::Bare => types::ConfigTierName::Bare,
        ConfigTierKind::Discovered => types::ConfigTierName::Discovered,
        ConfigTierKind::Custom
            if p.source().as_path().is_some() && p.source().as_path() == overrides =>
        {
            types::ConfigTierName::Override
        }
        ConfigTierKind::Custom => types::ConfigTierName::Declared,
        // shikumi's tier kind is non-exhaustive; the tiers engenho folds are
        // the four above, and a leaf no tier claims is the compiled default.
        ConfigTierKind::Default | _ => types::ConfigTierName::Default,
    }
}

fn unix_to_utc(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default()
}

fn ca_fact(fact: CaFact) -> Result<types::CaFact, ControlError> {
    Ok(match fact {
        CaFact::Absent => types::CaFact::Absent,
        CaFact::Present {
            sha256,
            not_before,
            not_after,
            publicly_derivable,
        } => types::CaFact::Present {
            sha256: types::Sha256Hex::try_from(sha256).map_err(internal)?,
            not_before: unix_to_utc(not_before),
            not_after: unix_to_utc(not_after),
            publicly_derivable,
        },
        CaFact::Unreadable(error) => types::CaFact::Unreadable { error },
    })
}

fn file_fact(fact: FileFact) -> Result<types::SecretFileFact, ControlError> {
    Ok(match fact {
        FileFact::Absent => types::SecretFileFact::Absent,
        FileFact::Present { bytes, mode } => types::SecretFileFact::Present {
            bytes,
            mode: types::SecretFileFactMode::try_from(format!("{mode:04o}")).map_err(internal)?,
        },
        FileFact::Unreadable(error) => types::SecretFileFact::Unreadable { error },
    })
}

fn cert_fact(fact: CertFact) -> types::AdminCertFact {
    match fact {
        CertFact::Absent => types::AdminCertFact::Absent,
        CertFact::Present {
            common_name,
            organizations,
            not_before,
            not_after,
        } => types::AdminCertFact::Present {
            common_name,
            organizations,
            not_before: unix_to_utc(not_before),
            not_after: unix_to_utc(not_after),
        },
        CertFact::Unreadable(error) => types::AdminCertFact::Unreadable { error },
    }
}

fn replay<T>(page: &Page<T>) -> types::Replay {
    page.truncated
        .map_or(types::Replay::Complete, types::Replay::Truncated)
}

fn limit(requested: Option<u32>) -> usize {
    requested.map_or(DEFAULT_LIMIT, |n| {
        usize::try_from(n).unwrap_or(DEFAULT_LIMIT)
    })
}

fn wait(requested: Option<u32>) -> Duration {
    Duration::from_millis(u64::from(requested.unwrap_or(0))).min(MAX_WAIT)
}

fn event_kind(event: &DaemonEvent) -> Result<types::ControlEventKind, ControlError> {
    Ok(match event {
        DaemonEvent::Lifecycle(to) => types::ControlEventKind::Lifecycle { to: wire(to)? },
        DaemonEvent::BootPhase { attempt, record } => types::ControlEventKind::BootPhase {
            attempt: *attempt,
            phase: wire(&record.phase)?,
            result: wire(&record.result)?,
        },
        DaemonEvent::ChildDied { child, cause } => types::ControlEventKind::ChildDied {
            child: child_to_wire(*child),
            cause: death_to_wire(*cause),
        },
        DaemonEvent::ChildRespawned { child, generation } => {
            types::ControlEventKind::ChildRespawned {
                child: child_to_wire(*child),
                generation: *generation,
            }
        }
        DaemonEvent::ReinitExecuted { operation } => types::ControlEventKind::ReinitExecuted {
            operation: reinit_op_to_wire(*operation),
        },
        DaemonEvent::ConfigApplied {
            generation,
            leaves,
            effect,
        } => types::ControlEventKind::ConfigApplied {
            generation: *generation,
            leaves: leaves.iter().map(wire).collect::<Result<_, _>>()?,
            effect: effect_to_wire(effect)?,
        },
        DaemonEvent::KubeconfigPublished { record } => {
            types::ControlEventKind::KubeconfigPublished {
                record: wire(record)?,
            }
        }
    })
}

/// The control API's spelling of what an applied change did.
pub(super) fn effect_to_wire(effect: &ApplyEffect) -> Result<types::ApplyEffect, ControlError> {
    Ok(match effect {
        ApplyEffect::NoChange => types::ApplyEffect::NoChange,
        ApplyEffect::PlannedOnly => types::ApplyEffect::PlannedOnly,
        ApplyEffect::AppliedLive => types::ApplyEffect::AppliedLive,
        ApplyEffect::Respawned { children } => types::ApplyEffect::Respawned {
            children: children.iter().copied().map(child_to_wire).collect(),
        },
        ApplyEffect::RestartScheduled => types::ApplyEffect::RestartScheduled,
        ApplyEffect::RestartDeferred { leaves } => types::ApplyEffect::RestartDeferred {
            leaves: leaves.iter().map(wire).collect::<Result<_, _>>()?,
        },
        ApplyEffect::NextBoot => types::ApplyEffect::NextBoot,
    })
}

fn log_level(level: tracing::Level) -> types::LogLevel {
    match level {
        tracing::Level::ERROR => types::LogLevel::Error,
        tracing::Level::WARN => types::LogLevel::Warn,
        tracing::Level::INFO => types::LogLevel::Info,
        tracing::Level::DEBUG => types::LogLevel::Debug,
        tracing::Level::TRACE => types::LogLevel::Trace,
    }
}

fn tracing_level(level: types::LogLevel) -> tracing::Level {
    match level {
        types::LogLevel::Error => tracing::Level::ERROR,
        types::LogLevel::Warn => tracing::Level::WARN,
        types::LogLevel::Info => tracing::Level::INFO,
        types::LogLevel::Debug => tracing::Level::DEBUG,
        types::LogLevel::Trace => tracing::Level::TRACE,
    }
}

#[async_trait::async_trait]
impl EngenhoControl for DaemonControl {
    async fn hello(&self, by: &Principal, _: HelloRequest) -> Result<types::Hello, ControlError> {
        let snapshot = self.snapshot();
        let inspection = self.inspect().await?;
        let (config, _) = self
            .configuration(inspection.runtime.as_ref())
            .unwrap_or_else(|_| (EngenhoConfig::prescribed_default(), None));
        Ok(types::Hello {
            api_version: types::HelloApiVersion::EngenhoControlV1,
            daemon: self.daemon_info(&snapshot),
            capabilities: OperationId::ALL
                .iter()
                .filter(|id| Self::serves(**id))
                .map(|id| id.as_str().to_owned())
                .collect(),
            transport: match by.view().attested {
                types::AttestedView::LocalUid { .. } => types::Transport::Uds,
                types::AttestedView::RemotePin { .. } => types::Transport::Mtls,
            },
            principal: by.view(),
            grant: by.grant().clone(),
            cluster: types::ClusterIdentity {
                cluster_name: config.cluster.name,
                node_name: config.runtime.node_name,
                ca: self.ca_binding(),
            },
            lifecycle: Self::lifecycle(&snapshot)?,
        })
    }

    async fn get_spec(&self, _: &Principal, _: GetSpecRequest) -> Result<String, ControlError> {
        debug_assert!(API_VERSION.starts_with("engenho.control/"));
        Ok(SPEC_YAML.to_owned())
    }

    async fn get_runtime(
        &self,
        _: &Principal,
        _: GetRuntimeRequest,
    ) -> Result<types::RuntimeStatus, ControlError> {
        let snapshot = self.snapshot();
        Ok(types::RuntimeStatus {
            lifecycle: Self::lifecycle(&snapshot)?,
            daemon: self.daemon_info(&snapshot),
        })
    }

    async fn get_boot(
        &self,
        _: &Principal,
        _: GetBootRequest,
    ) -> Result<types::BootView, ControlError> {
        let snapshot = self.snapshot();
        Ok(types::BootView {
            latest: match snapshot.attempts.last() {
                Some(attempt) => types::LatestBootAttempt::Recorded(wire(attempt)?),
                None => types::LatestBootAttempt::None,
            },
            previous_run: wire(&snapshot.previous_run)?,
        })
    }

    async fn list_boot_attempts(
        &self,
        _: &Principal,
        _: ListBootAttemptsRequest,
    ) -> Result<types::BootAttemptList, ControlError> {
        Ok(types::BootAttemptList {
            attempts: self
                .snapshot()
                .attempts
                .iter()
                .map(wire)
                .collect::<Result<_, _>>()?,
        })
    }

    async fn get_init_state(
        &self,
        _: &Principal,
        _: GetInitStateRequest,
    ) -> Result<types::InitState, ControlError> {
        let snapshot = self.snapshot();
        let inspection = self.inspect().await?;
        let (config, provenance) = self
            .configuration(inspection.runtime.as_ref())
            .unwrap_or_else(|_| (EngenhoConfig::prescribed_default(), None));
        let source = match &self.p.data_dir_source {
            DataDirSource::Resolved => types::DataDirSource::Resolved,
            DataDirSource::LenientFallback { error } => {
                types::DataDirSource::LenientFallback(error.clone())
            }
            DataDirSource::CompiledDefault => types::DataDirSource::CompiledDefault,
        };
        Ok(types::InitState {
            identity: self.identity(&snapshot, &config, provenance.as_ref()),
            data_dir: types::DataDirFacts {
                path: self.p.data_dir.display().to_string(),
                source,
            },
            pki: self.pki(&inspection)?,
            store: Self::store_facts(&inspection),
            kubeconfigs: Self::publish_records(&inspection)?,
            layout: self.layout(),
            previous_run: wire(&snapshot.previous_run)?,
        })
    }

    async fn get_config(
        &self,
        _: &Principal,
        _: GetConfigRequest,
    ) -> Result<types::ConfigView, ControlError> {
        let snapshot = self.snapshot();
        let effective = self.effective()?;
        let overrides = self.override_set()?;
        Ok(types::ConfigView {
            effective_yaml: serde_yaml::to_string(&effective.config).map_err(internal)?,
            declared: self.declared_source()?,
            generation: overrides.generation(),
            override_count: u32::try_from(overrides.len()).unwrap_or(u32::MAX),
            pending: wire(&Self::pending(&snapshot))?,
        })
    }

    async fn get_config_drift(
        &self,
        _: &Principal,
        _: GetConfigDriftRequest,
    ) -> Result<types::DriftReport, ControlError> {
        let snapshot = self.snapshot();
        let inspection = self.inspect().await?;
        let on_disk = self.p.declared.as_deref().and_then(file_digest);
        let changed = inspection
            .runtime
            .as_ref()
            .is_some_and(|facts| facts.declared_digest != on_disk);
        let (unified_diff, leaves) = self.drift()?;
        Ok(types::DriftReport {
            unified_diff,
            leaves,
            applied: wire(&Self::pending(&snapshot))?,
            declared_on_disk: if changed {
                types::DriftReportDeclaredOnDisk::ChangedSinceLoad
            } else {
                types::DriftReportDeclaredOnDisk::Current
            },
        })
    }

    async fn list_config_leaves(
        &self,
        _: &Principal,
        req: ListConfigLeavesRequest,
    ) -> Result<types::ConfigLeafList, ControlError> {
        Ok(types::ConfigLeafList {
            leaves: self.leaves(req.prefix.as_deref())?,
        })
    }

    async fn get_config_leaf(
        &self,
        _: &Principal,
        req: GetConfigLeafRequest,
    ) -> Result<types::ConfigLeaf, ControlError> {
        self.leaf(&leaf_path(&req.leaf)?)
    }

    async fn list_config_overrides(
        &self,
        _: &Principal,
        _: ListConfigOverridesRequest,
    ) -> Result<types::OverrideList, ControlError> {
        let overrides = self.override_set()?;
        if let Some(why) = overrides.unreadable() {
            return Err(unreadable_overrides(why));
        }
        Ok(types::OverrideList {
            generation: overrides.generation(),
            entries: overrides
                .entries()
                .into_iter()
                .map(|entry| {
                    Ok(types::OverrideEntry {
                        path: wire(&entry.path)?,
                        value: entry.stored.value,
                        set_by: entry.stored.set_by,
                        set_at: entry.stored.set_at,
                        durability: wire(&entry.durability)?,
                    })
                })
                .collect::<Result<_, ControlError>>()?,
        })
    }

    async fn set_config_leaf(
        &self,
        by: &Principal,
        req: SetConfigLeafRequest,
    ) -> Result<types::ApplyReport, ControlError> {
        let path = leaf_path(&req.leaf)?;
        self.set(by, path, req.body).await
    }

    async fn unset_config_leaf(
        &self,
        _: &Principal,
        req: UnsetConfigLeafRequest,
    ) -> Result<types::ApplyReport, ControlError> {
        let path = leaf_path(&req.leaf)?;
        self.configure(
            Some(Change::Unset { path }),
            ApplyOptions {
                dry_run: req.dry_run.unwrap_or(false),
                restart_now: restart_now(req.restart_policy),
                precondition: req.precondition_generation,
            },
        )
        .await
    }

    async fn clear_config_overrides(
        &self,
        _: &Principal,
        req: ClearConfigOverridesRequest,
    ) -> Result<types::ApplyReport, ControlError> {
        let body = req.body;
        self.configure(
            Some(Change::Clear {
                prefix: body.prefix,
            }),
            ApplyOptions {
                dry_run: body.dry_run,
                restart_now: restart_now(body.restart_policy),
                precondition: body.precondition_generation,
            },
        )
        .await
    }

    async fn reload_config(
        &self,
        _: &Principal,
        req: ReloadConfigRequest,
    ) -> Result<types::ApplyReport, ControlError> {
        self.configure(
            None,
            ApplyOptions {
                dry_run: req.body.dry_run,
                restart_now: restart_now(req.body.restart_policy),
                precondition: None,
            },
        )
        .await
    }

    async fn list_children(
        &self,
        _: &Principal,
        _: ListChildrenRequest,
    ) -> Result<types::ChildrenView, ControlError> {
        let snapshot = self.snapshot();
        Ok(match self.inspect().await?.runtime {
            Some(facts) => types::ChildrenView::Up {
                children: Self::children(&facts),
            },
            None => types::ChildrenView::Down {
                lifecycle: Self::lifecycle(&snapshot)?,
            },
        })
    }

    async fn get_child(
        &self,
        _: &Principal,
        req: GetChildRequest,
    ) -> Result<types::ChildLookup, ControlError> {
        let snapshot = self.snapshot();
        let child = child_from_wire(req.child);
        Ok(match self.inspect().await?.runtime {
            Some(facts) => Self::children(&facts)
                .into_iter()
                .find(|view| view.child == child_to_wire(child))
                .map_or_else(
                    || Err(internal("every child has a view")),
                    |child| Ok(types::ChildLookup::Up { child }),
                )?,
            None => types::ChildLookup::Down {
                lifecycle: Self::lifecycle(&snapshot)?,
            },
        })
    }

    async fn restart_child(
        &self,
        _: &Principal,
        req: RestartChildRequest,
    ) -> Result<types::RespawnReport, ControlError> {
        let respawned = self
            .p
            .supervisor
            .restart_child(child_from_wire(req.child))
            .await
            .map_err(|e| restart_refusal(&e))?;
        Ok(types::RespawnReport {
            child: child_to_wire(respawned.child),
            generation: respawned.generation,
            rebuilt: respawned.rebuilt.into_iter().map(child_to_wire).collect(),
        })
    }

    async fn enable_driver(
        &self,
        by: &Principal,
        req: EnableDriverRequest,
    ) -> Result<types::DriverToggleReport, ControlError> {
        self.toggle_driver(by, req.driver, true).await
    }

    async fn disable_driver(
        &self,
        by: &Principal,
        req: DisableDriverRequest,
    ) -> Result<types::DriverToggleReport, ControlError> {
        self.toggle_driver(by, req.driver, false).await
    }

    async fn get_pki(
        &self,
        _: &Principal,
        _: GetPkiRequest,
    ) -> Result<types::PkiInventory, ControlError> {
        let inspection = self.inspect().await?;
        self.pki(&inspection)
    }

    async fn get_store(
        &self,
        _: &Principal,
        _: GetStoreRequest,
    ) -> Result<types::StoreView, ControlError> {
        let inspection = self.inspect().await?;
        Ok(types::StoreView {
            path: self.p.data_dir.join(STORE_DIR).display().to_string(),
            facts: Self::store_facts(&inspection),
        })
    }

    async fn list_kubeconfigs(
        &self,
        _: &Principal,
        _: ListKubeconfigsRequest,
    ) -> Result<types::KubeconfigList, ControlError> {
        let inspection = self.inspect().await?;
        Ok(types::KubeconfigList {
            records: Self::publish_records(&inspection)?,
        })
    }

    async fn publish_kubeconfig(
        &self,
        _: &Principal,
        req: PublishKubeconfigRequest,
    ) -> Result<types::PublishRecord, ControlError> {
        let target: KubeconfigTarget = wire(&req.target)?;
        let records = self
            .p
            .supervisor
            .republish()
            .await
            .map_err(reconfigure_refusal)?;
        records
            .iter()
            .find(|record| record.target == target)
            .map_or_else(|| Err(internal("every target has a record")), wire)
    }

    async fn list_events(
        &self,
        _: &Principal,
        req: ListEventsRequest,
    ) -> Result<types::EventPage, ControlError> {
        let page = self
            .p
            .supervisor
            .events()
            .wait_page(
                req.after.unwrap_or(0),
                limit(req.limit),
                wait(req.wait_ms),
                |_| true,
            )
            .await;
        Ok(types::EventPage {
            events: page
                .items
                .iter()
                .map(|e| {
                    Ok(types::ControlEvent {
                        seq: e.seq,
                        at: e.at.utc(),
                        kind: event_kind(&e.item)?,
                    })
                })
                .collect::<Result<_, ControlError>>()?,
            next_cursor: page.next_cursor,
            replay: replay(&page),
        })
    }

    async fn list_logs(
        &self,
        _: &Principal,
        req: ListLogsRequest,
    ) -> Result<types::LogPage, ControlError> {
        let floor = req.level.map_or(tracing::Level::TRACE, tracing_level);
        let page = self
            .p
            .logs
            .wait_page(
                req.after.unwrap_or(0),
                limit(req.limit),
                wait(req.wait_ms),
                |entry: &LogEntry| entry.level <= floor,
            )
            .await;
        Ok(types::LogPage {
            lines: page
                .items
                .iter()
                .map(|e| types::LogLine {
                    seq: e.seq,
                    at: e.at.utc(),
                    level: log_level(e.item.level),
                    target: e.item.target.clone(),
                    message: e.item.message.clone(),
                    fields: e.item.fields.clone().into_iter().collect(),
                })
                .collect(),
            next_cursor: page.next_cursor,
            replay: replay(&page),
        })
    }

    async fn list_audit(
        &self,
        _: &Principal,
        req: ListAuditRequest,
    ) -> Result<types::AuditPage, ControlError> {
        let (records, next_cursor, replay) =
            self.p.audit.page(req.after.unwrap_or(0), limit(req.limit));
        Ok(types::AuditPage {
            records,
            next_cursor,
            replay,
        })
    }

    async fn get_control(
        &self,
        _: &Principal,
        _: GetControlRequest,
    ) -> Result<types::ControlView, ControlError> {
        let remote = &self.p.remote;
        Ok(types::ControlView {
            uds: types::UdsListenerState {
                path: self.p.socket.path.display().to_string(),
                access: match self.p.socket.access {
                    SocketAccess::Owner => types::SocketAccess::Owner,
                    SocketAccess::Group => types::SocketAccess::Group,
                },
                group_tier: match self.p.socket.group_tier {
                    GroupTier::Observe => AuthorityTier::Observe,
                    GroupTier::Mutate => AuthorityTier::Mutate,
                },
            },
            remote: remote.state.borrow().view(),
            identity: match &remote.identity {
                Ok(identity) => types::ControlIdentityView::Present {
                    spki: types::SpkiSha256::try_from(identity.spki().to_string())
                        .map_err(internal)?,
                    created_at: identity.created_at(),
                },
                Err(detail) => types::ControlIdentityView::Unavailable {
                    detail: detail.clone(),
                },
            },
            authorized_clients: remote.pins.current().views(),
        })
    }

    async fn start_runtime(
        &self,
        _: &Principal,
        _: StartRuntimeRequest,
    ) -> Result<types::Accepted, ControlError> {
        self.command(self.p.supervisor.start()).await
    }

    async fn stop_runtime(
        &self,
        _: &Principal,
        req: StopRuntimeRequest,
    ) -> Result<types::StopDone, ControlError> {
        let hold = match req.body.hold {
            types::Hold::None => Hold::None,
            types::Hold::AcrossRelaunch => Hold::AcrossRelaunch,
        };
        self.command(self.p.supervisor.stop(hold)).await
    }

    async fn restart_runtime(
        &self,
        _: &Principal,
        _: RestartRuntimeRequest,
    ) -> Result<types::Accepted, ControlError> {
        self.command(self.p.supervisor.restart()).await
    }

    async fn retry_boot(
        &self,
        _: &Principal,
        _: RetryBootRequest,
    ) -> Result<types::Accepted, ControlError> {
        self.command(self.p.supervisor.retry()).await
    }

    async fn exit_process(
        &self,
        _: &Principal,
        req: ExitProcessRequest,
    ) -> Result<types::Accepted, ControlError> {
        let intent = match req.body.intent {
            types::ExitIntent::Halt => ExitIntent::Halt,
            types::ExitIntent::Relaunch => ExitIntent::Relaunch,
        };
        self.command(self.p.supervisor.exit(intent)).await
    }

    async fn create_confirmation(
        &self,
        by: &Principal,
        req: CreateConfirmationRequest,
    ) -> Result<types::Challenge, ControlError> {
        self.prepare(by, &req.body.request)
    }

    async fn cancel_confirmation(
        &self,
        _: &Principal,
        req: CancelConfirmationRequest,
    ) -> Result<types::Cancelled, ControlError> {
        self.cancel(&req.confirmation)
    }

    async fn rotate_admin_token(
        &self,
        by: &Principal,
        req: RotateAdminTokenRequest,
    ) -> Result<types::ReinitReport, ControlError> {
        let request = types::ReinitRequest::RotateAdminToken;
        self.execute(
            by,
            &req.engenho_confirmation,
            &request,
            &req.body.confirm_phrase,
        )
        .await
    }

    async fn reseed_pki(
        &self,
        by: &Principal,
        req: ReseedPkiRequest,
    ) -> Result<types::ReinitReport, ControlError> {
        let request = types::ReinitRequest::ReseedPki {
            sa_key: req.body.sa_key,
        };
        self.execute(
            by,
            &req.engenho_confirmation,
            &request,
            &req.body.confirm_phrase,
        )
        .await
    }

    async fn wipe_store(
        &self,
        by: &Principal,
        req: WipeStoreRequest,
    ) -> Result<types::ReinitReport, ControlError> {
        let request = types::ReinitRequest::WipeStore {
            scope: req.body.scope,
        };
        self.execute(
            by,
            &req.engenho_confirmation,
            &request,
            &req.body.confirm_phrase,
        )
        .await
    }

    async fn rotate_control_identity(
        &self,
        by: &Principal,
        req: RotateControlIdentityRequest,
    ) -> Result<types::ReinitReport, ControlError> {
        let request = types::ReinitRequest::RotateControlIdentity;
        self.execute(
            by,
            &req.engenho_confirmation,
            &request,
            &req.body.confirm_phrase,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::boot::{BootKind, BootPhase, Timestamp};
    use crate::lifecycle::journal::{
        AttemptResult, BootAttempt, BootKindObservation, LastSeen, PhaseRecord, PhaseResult,
        PreviousRun,
    };
    use crate::publish::KubeconfigTarget;

    fn round_trip<T, U>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
        U: Serialize + DeserializeOwned,
    {
        let spec: U = wire(value).unwrap_or_else(|e| panic!("{value:?}: {e}"));
        let back: T = wire(&spec).unwrap_or_else(|e| panic!("{value:?} back: {e}"));
        assert_eq!(&back, value);
    }

    fn at() -> Timestamp {
        Timestamp::parse("2026-09-22T00:00:00.250Z").expect("literal")
    }

    /// Every variant of every journal and publish shape the daemon hands the
    /// control API is the spec's, and survives the trip there and back.
    #[test]
    fn every_journal_and_publish_shape_is_the_specs() {
        let results = [
            PhaseResult::InProgress,
            PhaseResult::Completed { elapsed_ms: 3 },
            PhaseResult::Failed {
                elapsed_ms: 4,
                error: "e".into(),
            },
            PhaseResult::Cancelled { elapsed_ms: 5 },
        ];
        let kinds = [
            BootKindObservation::NotYetKnown,
            BootKindObservation::Known {
                kind: BootKind::FirstBoot,
            },
            BootKindObservation::Known {
                kind: BootKind::Resume,
            },
            BootKindObservation::Known {
                kind: BootKind::Ephemeral,
            },
        ];
        let outcomes = [
            AttemptResult::InProgress,
            AttemptResult::Succeeded,
            AttemptResult::Failed,
            AttemptResult::Cancelled,
        ];
        for (i, kind) in kinds.iter().enumerate() {
            let attempt = BootAttempt {
                attempt: NonZeroU32::MIN,
                started_at: at(),
                kind: *kind,
                result: outcomes[i % outcomes.len()],
                phases: BootPhase::ALL
                    .iter()
                    .zip(results.iter().cycle())
                    .map(|(phase, result)| PhaseRecord {
                        phase: *phase,
                        started_at: at(),
                        result: result.clone(),
                    })
                    .collect(),
            };
            round_trip::<_, types::BootAttempt>(&attempt);
        }
        for previous in [
            PreviousRun::FirstEver,
            PreviousRun::CleanStop { at: at() },
            PreviousRun::Unclean {
                last_seen: LastSeen::Booting {
                    phase: BootPhase::AwaitLeadership,
                },
            },
            PreviousRun::Unclean {
                last_seen: LastSeen::Running,
            },
            PreviousRun::Unclean {
                last_seen: LastSeen::Draining,
            },
        ] {
            round_trip::<_, types::PreviousRun>(&previous);
        }
        let path = std::path::Path::new("/k");
        let mut records = vec![
            PublishRecord::written(KubeconfigTarget::DataDir, path, 0o600),
            PublishRecord::failed(KubeconfigTarget::Operator, path, &"denied"),
        ];
        for reason in [
            SkipReason::NotConfigured,
            SkipReason::TlsDisabled,
            SkipReason::NoAdvertiseAddress,
            SkipReason::NoPodAddress,
            SkipReason::NoHome,
            SkipReason::RuntimeDown,
        ] {
            records.extend(PublishRecord::all_skipped(reason));
        }
        for record in &records {
            round_trip::<_, types::PublishRecord>(record);
        }
        for kind in [BootKind::FirstBoot, BootKind::Resume, BootKind::Ephemeral] {
            round_trip::<_, types::BootKind>(&kind);
        }
        for phase in BootPhase::ALL {
            round_trip::<_, types::BootPhase>(phase);
        }
    }

    /// `hello`'s capabilities are exactly the operations this daemon serves
    /// — since P7, every operation the spec has.
    #[test]
    fn capabilities_name_only_what_is_served() {
        let unserved: Vec<_> = OperationId::ALL
            .iter()
            .filter(|id| !DaemonControl::serves(**id))
            .collect();
        assert!(unserved.is_empty(), "not served: {unserved:?}");
    }
}
