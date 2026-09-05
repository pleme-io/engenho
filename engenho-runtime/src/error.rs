//! Typed runtime errors — `thiserror`, no `anyhow` in the lib.

use engenho_apiserver::ServerError;
use engenho_config::ConfigError;
use engenho_store::StoreError;

/// Everything that can go wrong booting or shutting down a [`crate::Runtime`].
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Config failed `validate()` or a section was incoherent.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    /// The store mesh failed to start, initialize, or take leadership.
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// The apiserver failed to bind or serve.
    #[error("apiserver error: {0}")]
    Server(#[from] ServerError),

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
    /// store `Arc` (a driver task or apiserver handler still holds a
    /// clone). `terminate` consumes `StoreMesh` and requires the only
    /// strong ref; this surfaces the leak rather than hanging.
    #[error(
        "could not acquire sole store ownership for terminate ({strong_count} strong refs remain)"
    )]
    StoreStillShared {
        /// How many strong refs remained when `try_unwrap` failed.
        strong_count: usize,
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
}

engenho_substrate::impl_error_kind! {
    RuntimeError {
        (Config(_)) => "config",
        (Store(_)) => "store",
        (Server(_)) => "server",
        { LeadershipTimeout { .. } } => "leadership_timeout",
        { ListenAddr { .. } } => "listen_addr",
        { ExtraSan { .. } } => "extra_san",
        { StoreStillShared { .. } } => "store_still_shared",
        { ContainerRuntimeUnavailable { .. } } => "container_runtime_unavailable",
        (Kubeconfig(_)) => "kubeconfig",
        { KubeconfigIo { .. } } => "kubeconfig_io",
    }
}
