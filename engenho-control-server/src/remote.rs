//! The remote control listener: TLS 1.3, each client admitted by the pin of
//! its key ([`crate::pins`]), the listener known to clients by the pin of its
//! own ([`crate::identity`]).
//!
//! Starting it never fails the daemon: what keeps it from serving is its
//! state — [`RemoteState::Absent`] with a typed reason — which the control
//! API reports. A bind that fails (the tailnet address not up yet) is retried
//! on a doubling backoff until the daemon stops.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use engenho_config::RemoteControlConfig;
use engenho_control_types::pin::server_config;
use engenho_control_types::types;
use engenho_serve::StopSignal;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::identity::ControlIdentity;
use crate::pins::Pins;
use crate::router::Router;
use crate::serve::{GRACE, serve_tls};

/// The first retry of a failed bind, doubling to [`BIND_RETRY_MAX`].
pub const BIND_RETRY_FIRST: Duration = Duration::from_secs(1);
/// The longest wait between bind attempts.
pub const BIND_RETRY_MAX: Duration = Duration::from_secs(60);

/// Why the listener is not serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Absence {
    /// `control.remote.enable` is off.
    Disabled,
    /// No client is pinned, so nobody could connect.
    NoAuthorizedClients,
    /// The remote section does not validate.
    ControlConfigInvalid(String),
    /// The listener's own key could not be loaded or created.
    IdentityUnavailable(String),
    /// The address could not be bound; retried after `retry_in`.
    BindFailed {
        /// Where.
        addr: String,
        /// Why.
        detail: String,
        /// When the next attempt is.
        retry_in: Duration,
    },
}

/// Where the listener is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteState {
    /// Accepting connections.
    Serving {
        /// The address it bound.
        addr: SocketAddr,
        /// Since when.
        since: DateTime<Utc>,
    },
    /// Not accepting connections.
    Absent {
        /// Since when.
        since: DateTime<Utc>,
        /// Why.
        absence: Absence,
    },
}

impl RemoteState {
    fn absent(absence: Absence) -> Self {
        Self::Absent {
            since: Utc::now(),
            absence,
        }
    }

    /// The control API's view of it.
    #[must_use]
    pub fn view(&self) -> types::RemoteListenerState {
        match self {
            Self::Serving { addr, since } => types::RemoteListenerState::Serving {
                addr: addr.to_string(),
                since: *since,
            },
            Self::Absent { since, absence } => types::RemoteListenerState::Absent {
                since: *since,
                absence: match absence {
                    Absence::Disabled => types::RemoteAbsence::Disabled,
                    Absence::NoAuthorizedClients => types::RemoteAbsence::NoAuthorizedClients,
                    Absence::ControlConfigInvalid(detail) => {
                        types::RemoteAbsence::ControlConfigInvalid {
                            detail: detail.clone(),
                        }
                    }
                    Absence::IdentityUnavailable(detail) => {
                        types::RemoteAbsence::IdentityUnavailable {
                            detail: detail.clone(),
                        }
                    }
                    Absence::BindFailed {
                        addr,
                        detail,
                        retry_in,
                    } => types::RemoteAbsence::BindFailed {
                        addr: addr.clone(),
                        detail: detail.clone(),
                        retry_in_ms: u64::try_from(retry_in.as_millis()).unwrap_or(u64::MAX),
                    },
                },
            },
        }
    }
}

/// A started (or deliberately absent) listener.
#[derive(Debug)]
pub struct RemoteListener {
    state: watch::Receiver<RemoteState>,
    task: Option<JoinHandle<()>>,
}

impl RemoteListener {
    /// A listener's state channel, made before the listener so what reports
    /// the state can exist before the router the listener serves.
    #[must_use]
    pub fn channel() -> (watch::Sender<RemoteState>, watch::Receiver<RemoteState>) {
        watch::channel(RemoteState::absent(Absence::Disabled))
    }

    /// Start the listener `config` describes, serving `router` until `stop`
    /// and reporting on `state`. Returns at once; the state says whether and
    /// where it serves.
    #[must_use]
    pub fn spawn(
        config: &RemoteControlConfig,
        identity: Result<Arc<ControlIdentity>, String>,
        pins: Pins,
        router: Arc<Router>,
        stop: StopSignal,
        state: watch::Sender<RemoteState>,
    ) -> Self {
        let absent = |absence| {
            state.send_replace(RemoteState::absent(absence));
            Self {
                state: state.subscribe(),
                task: None,
            }
        };
        if !config.enable {
            return absent(Absence::Disabled);
        }
        if let Err(err) = config.validate() {
            return absent(Absence::ControlConfigInvalid(err.to_string()));
        }
        let addr: SocketAddr = match config.listen_addr.parse() {
            Ok(addr) => addr,
            Err(err) => return absent(Absence::ControlConfigInvalid(err.to_string())),
        };
        if pins.current().is_empty() {
            return absent(Absence::NoAuthorizedClients);
        }
        let identity = match identity {
            Ok(identity) => identity,
            Err(why) => return absent(Absence::IdentityUnavailable(why)),
        };
        let tls = match server_config(identity.presented(), Arc::new(pins.clone())) {
            Ok(tls) => Arc::new(tls),
            Err(err) => return absent(Absence::IdentityUnavailable(err.to_string())),
        };
        state.send_replace(RemoteState::absent(Absence::BindFailed {
            addr: addr.to_string(),
            detail: "not bound yet".into(),
            retry_in: Duration::ZERO,
        }));
        let receiver = state.subscribe();
        let task = tokio::spawn(run(addr, tls, pins, router, stop, state));
        Self {
            state: receiver,
            task: Some(task),
        }
    }

    /// Where the listener is now.
    #[must_use]
    pub fn state(&self) -> RemoteState {
        self.state.borrow().clone()
    }

    /// A receiver that sees every state from now on.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<RemoteState> {
        self.state.clone()
    }

    /// Wait for the listener to end (after its stop).
    pub async fn stopped(self) {
        if let Some(task) = self.task {
            let _ = task.await;
        }
    }
}

async fn run(
    addr: SocketAddr,
    tls: Arc<rustls::ServerConfig>,
    pins: Pins,
    router: Arc<Router>,
    mut stop: StopSignal,
    state: watch::Sender<RemoteState>,
) {
    let mut delay = BIND_RETRY_FIRST;
    loop {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                let bound = listener.local_addr().unwrap_or(addr);
                state.send_replace(RemoteState::Serving {
                    addr: bound,
                    since: Utc::now(),
                });
                tracing::info!(addr = %bound, "remote control listener serving");
                serve_tls(listener, tls, pins, router, stop, GRACE).await;
                return;
            }
            Err(err) => {
                tracing::warn!(%addr, error = %err, retry_in = ?delay, "remote control listener cannot bind");
                state.send_replace(RemoteState::absent(Absence::BindFailed {
                    addr: addr.to_string(),
                    detail: err.to_string(),
                    retry_in: delay,
                }));
                tokio::select! {
                    () = stop.stopped() => return,
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(BIND_RETRY_MAX);
            }
        }
    }
}
