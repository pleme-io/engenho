//! The runtime's boot, one named phase at a time.
//!
//! A boot used to be one straight-line function whose only record was log
//! lines: when it failed, "which step?" was answered by reading the error
//! and guessing, and nothing outside the process could ask. Now every step
//! of [`crate::Runtime::boot_recorded`] enters a [`BootPhase`] first, through
//! a [`BootRecorder`] that reports the entry to whoever supervises the boot
//! and is also where a boot is asked to stop.
//!
//! The phases are a closed list in boot order. A successful boot enters
//! every one of them, in exactly [`BootPhase::ALL`] order — pinned by
//! `tests/boot_phases.rs`, so a step added to the boot without a phase, or a
//! phase that is never entered, fails the suite.

use chrono::{DateTime, SecondsFormat, Utc};
use engenho_serve::StopSignal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::RuntimeError;
use crate::release::{BootFailed, BootUnwind};

engenho_substrate::closed_enum! {
    /// One step of the boot, in the order the boot runs them — which is also
    /// the order `Ord` compares them in. The serde spelling is the control
    /// API's (`spec/engenho-control.openapi.yaml`, `BootPhase`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum BootPhase {
        /// Resolve the configuration from its tiers.
        ResolveConfig,
        /// Read every field into the boot's typed view, refusing what this
        /// runtime cannot honour.
        ReadBootConfig,
        /// Reach the configured container runtime and build the backend.
        PreflightBackend,
        /// Validate the whole configuration, cross-section.
        ValidateConfig,
        /// Open (or create) the store.
        OpenStore,
        /// Wait for raft leadership.
        AwaitLeadership,
        /// Build the scheduler, the tick windows and health.
        BuildScheduler,
        /// Register this node.
        RegisterNode,
        /// Seed namespaces, the `kubernetes` Service, RBAC, the default
        /// StorageClass and the snapshot CRDs.
        SeedCluster,
        /// Load or generate the cluster CA; issue the server and admin certs.
        IssuePki,
        /// Load or generate the bootstrap admin bearer token.
        LoadAdminToken,
        /// Build admission, authentication, authorization and the router.
        BuildAuth,
        /// Bind the Kubernetes apiserver.
        BindApiserver,
        /// Write the boot kubeconfig and publish its copies.
        PublishKubeconfigs,
        /// Spawn every child: drivers, listeners, the node lease.
        SpawnChildren,
        /// Hand health the spawned set and wire the Pod `/log` reader.
        AdoptHealth,
    }
}

impl BootPhase {
    /// The first phase of every boot.
    pub const FIRST: Self = Self::ResolveConfig;

    /// The phase's wire spelling (what logs and the control API show).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResolveConfig => "resolve_config",
            Self::ReadBootConfig => "read_boot_config",
            Self::PreflightBackend => "preflight_backend",
            Self::ValidateConfig => "validate_config",
            Self::OpenStore => "open_store",
            Self::AwaitLeadership => "await_leadership",
            Self::BuildScheduler => "build_scheduler",
            Self::RegisterNode => "register_node",
            Self::SeedCluster => "seed_cluster",
            Self::IssuePki => "issue_pki",
            Self::LoadAdminToken => "load_admin_token",
            Self::BuildAuth => "build_auth",
            Self::BindApiserver => "bind_apiserver",
            Self::PublishKubeconfigs => "publish_kubeconfigs",
            Self::SpawnChildren => "spawn_children",
            Self::AdoptHealth => "adopt_health",
        }
    }
}

impl std::fmt::Display for BootPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a boot created its store, resumed one, or runs in memory. Read
/// from the store's own initialized state; it is not a marker file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootKind {
    /// The durable store was created by this boot.
    FirstBoot,
    /// The durable store existed; this boot resumed it.
    Resume,
    /// The store is in memory and lives only as long as the runtime.
    Ephemeral,
}

/// A wall-clock moment in UTC — what the lifecycle, the boot journal and the
/// control API record. On the wire, RFC 3339 with milliseconds and a `Z`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Now.
    #[must_use]
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// A timestamp from its RFC 3339 text.
    ///
    /// # Errors
    ///
    /// The text is not RFC 3339.
    pub fn parse(text: &str) -> Result<Self, chrono::ParseError> {
        DateTime::parse_from_rfc3339(text).map(|t| Self(t.with_timezone(&Utc)))
    }

    /// This moment plus `delay`, saturating at the far end of the calendar.
    #[must_use]
    pub fn after(self, delay: std::time::Duration) -> Self {
        chrono::Duration::from_std(delay)
            .ok()
            .and_then(|d| self.0.checked_add_signed(d))
            .map_or(Self(DateTime::<Utc>::MAX_UTC), Self)
    }

    /// The RFC 3339 text, as serialized.
    #[must_use]
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_rfc3339())
    }
}

impl Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// What a boot reports while it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootProgress {
    /// The boot entered a phase.
    Entered {
        /// Which phase.
        phase: BootPhase,
        /// When.
        at: Timestamp,
    },
    /// The store is open, and this is what the boot found there.
    Kind(BootKind),
}

/// Where a boot reports the phases it enters, and where it is asked to
/// stop.
///
/// A stop is honoured at phase boundaries up to [`BootPhase::BindApiserver`]
/// — the boot returns [`RuntimeError::BootCancelled`] instead of entering the
/// next phase — and inside the one wait that is not bounded by the boot's own
/// work (raft leadership). The boot then unwinds like any other failure, so a
/// cancelled boot releases the store it had opened. Once the apiserver is
/// bound the boot finishes, and the stop is honoured by a shutdown.
#[derive(Debug)]
pub struct BootRecorder {
    progress: Option<mpsc::UnboundedSender<BootProgress>>,
    cancel: Option<StopSignal>,
    current: BootPhase,
}

impl BootRecorder {
    /// Reports to nobody and is never cancelled: an unsupervised boot
    /// ([`crate::Runtime::start`]).
    #[must_use]
    pub fn silent() -> Self {
        Self {
            progress: None,
            cancel: None,
            current: BootPhase::FIRST,
        }
    }

    /// Reports each phase entry (and the store's [`BootKind`]) on
    /// `progress`; stops when `cancel` is stopped, or its handle dropped.
    #[must_use]
    pub fn new(progress: mpsc::UnboundedSender<BootProgress>, cancel: StopSignal) -> Self {
        Self {
            progress: Some(progress),
            cancel: Some(cancel),
            current: BootPhase::FIRST,
        }
    }

    /// The phase the boot is in (the one it failed in, if it failed).
    #[must_use]
    pub const fn current(&self) -> BootPhase {
        self.current
    }

    /// Enter `phase`, unless the boot has been asked to stop.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::BootCancelled`], naming the phase the boot had
    /// reached, when a stop was requested.
    pub(crate) fn enter(&mut self, phase: BootPhase) -> Result<(), RuntimeError> {
        if self.is_cancelled() {
            return Err(RuntimeError::BootCancelled {
                phase: self.current,
            });
        }
        self.current = phase;
        self.report(BootProgress::Entered {
            phase,
            at: Timestamp::now(),
        });
        Ok(())
    }

    /// Enter `phase` past the point where a stop can end the boot (the
    /// apiserver is bound): the boot finishes, and the stop is honoured by
    /// shutting the runtime down.
    pub(crate) fn enter_committed(&mut self, phase: BootPhase) {
        self.current = phase;
        self.report(BootProgress::Entered {
            phase,
            at: Timestamp::now(),
        });
    }

    /// A boot that failed in the current phase, before it opened the store.
    pub(crate) fn failed(&self, error: RuntimeError) -> BootFailed {
        BootFailed {
            error,
            phase: self.current,
            unwind: BootUnwind::NeverOpened,
        }
    }

    /// Report what the boot found in the store it opened.
    pub(crate) fn observe(&self, kind: BootKind) {
        self.report(BootProgress::Kind(kind));
    }

    fn report(&self, progress: BootProgress) {
        if let Some(sink) = &self.progress {
            // A supervisor that went away no longer needs the report.
            let _ = sink.send(progress);
        }
    }

    /// Whether a stop has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(StopSignal::is_stopped)
    }

    /// Completes when a stop is requested; never, for an uncancellable boot.
    /// For racing a wait the boot does not otherwise bound.
    pub(crate) async fn cancelled(&self) {
        match &self.cancel {
            Some(signal) => signal.clone().stopped().await,
            None => std::future::pending().await,
        }
    }
}
