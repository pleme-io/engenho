//! Typed runtime errors — `thiserror`, no `anyhow` in the lib.

use std::fmt;

use engenho_apiserver::ServerError;
use engenho_config::ConfigError;
use engenho_store::StoreError;

/// Everything that can go wrong booting or shutting down a [`crate::Runtime`].
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Config failed `validate()` or a section was incoherent.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    /// A config field asks for something this runtime does not do (I21):
    /// an operator PKI, a multi-master formation, a consistency tier the
    /// store does not serve. Refused before anything is probed or written.
    #[error(transparent)]
    Unhonoured(#[from] crate::Unhonoured),

    /// The scheduler could not be built from `scheduler.*` (T5.8): an
    /// unimplemented strategy or a zero tick. `validate()` refuses both
    /// first; this is the constructor's own guard.
    #[error(transparent)]
    Scheduler(#[from] engenho_scheduler::SchedulerError),

    /// The store mesh failed to start, initialize, or take leadership; or,
    /// at a stop, to write its durable image
    /// ([`engenho_store::StoreError::Persist`], from the stop's flush or
    /// from `terminate`). A failed flush leaves every applied entry in the
    /// log, so the next boot replays it: nothing acknowledged is lost.
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// The apiserver failed to bind or serve.
    #[error("apiserver error: {0}")]
    Server(#[from] ServerError),

    /// The configured kubelet backend is refused at construction (T5.9): CRI
    /// until it sets mounts, pod IPs and confinement.
    #[error(transparent)]
    BackendRefused(#[from] engenho_kubelet::BackendRefused),

    /// Raft leadership wasn't reached within the configured timeout.
    /// The store started but never elected a leader, so no `propose`
    /// (Node registration, apiserver writes) could ever succeed.
    #[error("store did not reach leadership within {seconds}s")]
    LeadershipTimeout {
        /// The configured leadership timeout that elapsed.
        seconds: u32,
    },

    /// `listen_addr` couldn't be parsed into a `SocketAddr`.
    #[error("invalid listen_addr {addr:?}: {source}")]
    ListenAddr {
        /// The unparseable address string.
        addr: String,
        /// The parse error.
        #[source]
        source: std::net::AddrParseError,
    },

    /// The cluster's CA is reproducible from public source AND the apiserver
    /// was asked to listen somewhere other than loopback.
    ///
    /// ── ★ WHY THIS REFUSES TO START ────────────────────────────────────────
    /// Before the per-cluster PKI seed, every engenho derived its CA and its
    /// `O=system:masters` admin client key from a constant in a PUBLIC
    /// repository. Anyone who can read the source can reconstruct that CA and
    /// mint a super-user certificate such a cluster will accept.
    ///
    /// Bound to `127.0.0.1` that is inert: nothing off-host can reach it, and
    /// refusing to start would break every working local cluster over a risk
    /// they do not carry. Bound to a tailnet address, a LAN interface or
    /// `0.0.0.0`, it is an unauthenticated path to cluster-admin — and one that
    /// fails OPEN, with a successful handshake, a valid certificate and no log
    /// line anywhere to notice.
    ///
    /// So the pairing is the check: a public CA is tolerated exactly as long as
    /// it is unreachable. Refusing here makes "expose a cluster whose admin key
    /// is public knowledge" a startup failure rather than a silent posture.
    #[error(
        "refusing to serve {listen_addr} with a CA whose private key is derivable from public \
         source. This cluster's PKI predates the per-cluster seed, so its CA and its \
         system:masters admin certificate are identical on every engenho ever built and can be \
         reconstructed by anyone. Loopback-only is safe; this address is not. Remove \
         {pki_dir} and restart to mint a private cluster identity (every kubeconfig for this \
         cluster must then be re-fetched)"
    )]
    PublicCaOnReachableAddress {
        /// The address that would have been served.
        listen_addr: String,
        /// The directory to delete to regenerate the PKI.
        pki_dir: String,
    },

    /// An entry in `runtime.tls.extra_sans` is not a usable SAN.
    ///
    /// Refused BEFORE the apiserver binds, deliberately. A SAN is only ever
    /// consulted by a remote client during a TLS handshake, so a malformed one
    /// costs nothing locally and the node comes up looking entirely healthy —
    /// the failure lands on whoever tries to connect, as a verification error
    /// that reads like their kubeconfig is wrong. Worse, the certificate is
    /// persisted on first boot and reloaded thereafter, so the mistake outlives
    /// the fix until the PKI directory is removed. Failing the unit at start,
    /// naming the value, is the loud version of a fault that is otherwise
    /// silent and sticky.
    #[error("invalid runtime.tls.extra_sans entry: {source}")]
    ExtraSan {
        /// The classification failure.
        #[source]
        source: engenho_apiserver::SanParseError,
    },

    /// At shutdown, the Runtime could not become the sole owner of the
    /// store `Arc`: something still holds a clone. `terminate` consumes
    /// `StoreMesh` and requires the only strong ref; this surfaces the leak
    /// rather than hanging.
    ///
    /// `after` names the first stage of shutdown that owed sole ownership
    /// and did not have it (see [`ShutdownStage::owes_sole_ownership`]), so
    /// the holder is something that outlived that stage.
    ///
    /// The stop has already flushed the store by then
    /// ([`ShutdownStage::StoreFlushed`] runs before the unwrap), so the next
    /// boot replays nothing applied before the flush even though
    /// `terminate` never ran.
    #[error(
        "could not acquire sole store ownership for terminate ({strong_count} strong refs remain \
         after {after})"
    )]
    StoreStillShared {
        /// How many strong refs remained when `try_unwrap` failed.
        strong_count: usize,
        /// The first stage after which the store should have had one holder
        /// and had more.
        after: ShutdownStage,
    },

    /// The kubelet is configured to drive a container runtime whose binary
    /// could not be resolved at boot.
    ///
    /// This exists because the failure it replaces was SILENT for hours.
    /// Measured 2026-08-28: an unresolvable `podman` produced one WARN per
    /// reconcile tick (`spawn: No such file or directory`) while every pod sat
    /// with no status, and the operator's only symptom was an empty k9s
    /// screen. A control plane that cannot run a container must say so once,
    /// loudly, at boot — not whisper it forever into a log nobody tails.
    #[error(
        "kubelet backend {backend:?} is configured but its binary {binary:?} could not be \
         resolved or executed: {source}. Set runtime.podman_binary to an absolute path (the \
         nix module derives it from runtime.podmanPackage), or set runtime.kubelet_backend \
         to \"fake\" on a node that runs no containers."
    )]
    ContainerRuntimeUnavailable {
        /// The configured backend name.
        backend: String,
        /// The binary path (or bare name) that failed to resolve.
        binary: String,
        /// The underlying spawn error.
        source: std::io::Error,
    },

    /// Failed to build or persist the boot-time kubeconfig
    /// (`data_dir/kubeconfig`). Carries the emitter / io message.
    #[error("kubeconfig emission failed: {0}")]
    Kubeconfig(String),

    /// A filesystem operation while writing the kubeconfig failed.
    #[error("kubeconfig io error at {path}: {source}")]
    KubeconfigIo {
        /// The path the operation targeted.
        path: std::path::PathBuf,
        /// The underlying io error.
        #[source]
        source: std::io::Error,
    },

    /// This node's own Node object could not be registered at boot.
    #[error(transparent)]
    NodeRegistration(#[from] crate::node_registration::NodeRegistrationError),
}

engenho_substrate::impl_error_kind! {
    RuntimeError {
        (Config(_)) => "config",
        (Unhonoured(_)) => "unhonoured_config",
        (Scheduler(_)) => "scheduler",
        (Store(_)) => "store",
        (Server(_)) => "server",
        (BackendRefused(_)) => "backend_refused",
        { LeadershipTimeout { .. } } => "leadership_timeout",
        { ListenAddr { .. } } => "listen_addr",
        { ExtraSan { .. } } => "extra_san",
        { PublicCaOnReachableAddress { .. } } => "public_ca_on_reachable_address",
        { StoreStillShared { .. } } => "store_still_shared",
        { ContainerRuntimeUnavailable { .. } } => "container_runtime_unavailable",
        (Kubeconfig(_)) => "kubeconfig",
        { KubeconfigIo { .. } } => "kubeconfig_io",
        (NodeRegistration(_)) => "node_registration",
    }
}

/// A stage of [`crate::Runtime::shutdown`], in the order the stages run.
///
/// The runtime reads the store's strong count after each one. A
/// [`RuntimeError::StoreStillShared`] names the first stage that owed sole
/// ownership and did not have it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShutdownStage {
    /// Every child (the drivers and both listeners) has been aborted and
    /// awaited, so each task's store clone has been dropped.
    DriversAwaited,
    /// The apiserver has stopped: its serve task ended, or was aborted once
    /// the grace ran out.
    ApiserverStopped,
    /// The store's own background tasks (the raft RPC pump and the bookmark
    /// ticker) have been aborted and awaited by
    /// [`engenho_store::StoreMesh::quiesce`].
    StoreQuiesced,
    /// The durable image has been brought up to the applied state by
    /// [`engenho_store::StoreMesh::flush`], while the store is still behind
    /// the `Arc`. From here the next boot replays nothing applied before the
    /// flush, whether or not the store can then be terminated.
    StoreFlushed,
}

impl ShutdownStage {
    /// Every stage, in the order shutdown runs them.
    pub const ALL: [Self; 4] = [
        Self::DriversAwaited,
        Self::ApiserverStopped,
        Self::StoreQuiesced,
        Self::StoreFlushed,
    ];

    /// Whether the runtime must be the store's only strong holder once this
    /// stage has run.
    ///
    /// Not once the drivers are awaited: every apiserver handler holds a
    /// clone until the apiserver stops, so a count above one there is
    /// expected and names nothing. That is why a `StoreStillShared` never
    /// names [`Self::DriversAwaited`]. From the apiserver's stop on, nothing
    /// the runtime started may hold the store. Quiescing releases no
    /// `Arc<StoreMesh>` (the tasks it stops hold the store's inner state and
    /// a raft clone), and neither does flushing, so both owe the same thing:
    /// that nothing took a new reference in the meantime.
    #[must_use]
    pub const fn owes_sole_ownership(self) -> bool {
        match self {
            Self::DriversAwaited => false,
            Self::ApiserverStopped | Self::StoreQuiesced | Self::StoreFlushed => true,
        }
    }
}

impl fmt::Display for ShutdownStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DriversAwaited => "the drivers were awaited",
            Self::ApiserverStopped => "the apiserver stopped",
            Self::StoreQuiesced => "the store was quiesced",
            Self::StoreFlushed => "the store was flushed",
        })
    }
}

/// The store's strong count, read after each [`ShutdownStage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StrongCounts {
    pub(crate) drivers_awaited: usize,
    pub(crate) apiserver_stopped: usize,
    pub(crate) store_quiesced: usize,
    pub(crate) store_flushed: usize,
}

impl StrongCounts {
    /// The count read after `stage`.
    pub(crate) const fn after(&self, stage: ShutdownStage) -> usize {
        match stage {
            ShutdownStage::DriversAwaited => self.drivers_awaited,
            ShutdownStage::ApiserverStopped => self.apiserver_stopped,
            ShutdownStage::StoreQuiesced => self.store_quiesced,
            ShutdownStage::StoreFlushed => self.store_flushed,
        }
    }

    /// The stage a failed `try_unwrap` is charged to: the first stage that
    /// owed sole ownership and read more than one holder.
    ///
    /// When every owed reading was one, the holder took its reference after
    /// the last reading, which is still after the store was flushed.
    pub(crate) fn blame(&self) -> ShutdownStage {
        ShutdownStage::ALL
            .into_iter()
            .find(|stage| stage.owes_sole_ownership() && self.after(*stage) > 1)
            .unwrap_or(ShutdownStage::StoreFlushed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn counts(
        drivers: usize,
        apiserver: usize,
        quiesced: usize,
        flushed: usize,
    ) -> StrongCounts {
        StrongCounts {
            drivers_awaited: drivers,
            apiserver_stopped: apiserver,
            store_quiesced: quiesced,
            store_flushed: flushed,
        }
    }

    /// A holder that outlived the apiserver's stop is charged to that stage,
    /// however many the apiserver held before it.
    #[test]
    fn a_holder_that_outlives_the_apiserver_is_charged_to_its_stop() {
        assert_eq!(counts(57, 2, 2, 2).blame(), ShutdownStage::ApiserverStopped);
        assert_eq!(
            counts(57, 57, 57, 57).blame(),
            ShutdownStage::ApiserverStopped
        );
    }

    /// Sole ownership after the apiserver stopped, then a second holder:
    /// something took a new reference while the store was being quiesced.
    #[test]
    fn a_holder_taken_after_the_apiserver_stopped_is_charged_to_quiesce() {
        assert_eq!(counts(57, 1, 2, 2).blame(), ShutdownStage::StoreQuiesced);
    }

    /// A reference taken while the store was being flushed, or after the
    /// last reading, is charged to the flush: the last stage before the
    /// unwrap.
    #[test]
    fn a_holder_taken_during_or_after_the_flush_is_charged_to_it() {
        assert_eq!(counts(57, 1, 1, 2).blame(), ShutdownStage::StoreFlushed);
        // Every owed reading was one, yet the unwrap failed: the reference
        // was taken after the last reading.
        assert_eq!(counts(57, 1, 1, 1).blame(), ShutdownStage::StoreFlushed);
    }

    /// The apiserver's handlers hold the store until it stops, so many
    /// holders after the drivers are awaited is never the stage named.
    #[test]
    fn the_drivers_stage_is_never_charged() {
        for drivers in [1, 2, 57] {
            for apiserver in [1, 2] {
                for quiesced in [1, 2] {
                    for flushed in [1, 2] {
                        assert_ne!(
                            counts(drivers, apiserver, quiesced, flushed).blame(),
                            ShutdownStage::DriversAwaited
                        );
                    }
                }
            }
        }
        assert!(!ShutdownStage::DriversAwaited.owes_sole_ownership());
    }

    /// The error says after which stage the store was still shared, in
    /// words.
    #[test]
    fn store_still_shared_names_the_stage_in_its_message() {
        let err = RuntimeError::StoreStillShared {
            strong_count: 2,
            after: ShutdownStage::ApiserverStopped,
        };
        assert_eq!(
            err.to_string(),
            "could not acquire sole store ownership for terminate (2 strong refs remain after \
             the apiserver stopped)"
        );
        assert_eq!(
            ShutdownStage::DriversAwaited.to_string(),
            "the drivers were awaited"
        );
        assert_eq!(
            ShutdownStage::StoreQuiesced.to_string(),
            "the store was quiesced"
        );
        assert_eq!(
            ShutdownStage::StoreFlushed.to_string(),
            "the store was flushed"
        );
    }
}
