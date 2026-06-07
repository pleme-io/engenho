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

    /// At shutdown, the Runtime could not become the sole owner of the
    /// store `Arc` (a driver task or apiserver handler still holds a
    /// clone). `terminate` consumes `StoreMesh` and requires the only
    /// strong ref; this surfaces the leak rather than hanging.
    #[error("could not acquire sole store ownership for terminate ({strong_count} strong refs remain)")]
    StoreStillShared {
        /// How many strong refs remained when `try_unwrap` failed.
        strong_count: usize,
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
        { StoreStillShared { .. } } => "store_still_shared",
        (Kubeconfig(_)) => "kubeconfig",
        { KubeconfigIo { .. } } => "kubeconfig_io",
    }
}
