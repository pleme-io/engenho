//! Plugin registration — how a driver announces itself to the kubelet.
//!
//! ★ THE CONTRACT, IN THE ORDER IT ACTUALLY HAPPENS. A CSI driver is
//! deployed as a `DaemonSet` with a `node-driver-registrar` sidecar. On
//! startup:
//!
//! 1. The driver creates its own socket at
//!    `<root>/plugins/<driver>/csi.sock`.
//! 2. The registrar creates a SECOND socket at
//!    `<root>/plugins_registry/<driver>-reg.sock`, serving
//!    `pluginregistration.Registration`.
//! 3. The kubelet watches `plugins_registry/`, and on seeing a new socket
//!    calls `GetInfo` on it — which returns the FIRST socket's path.
//! 4. The kubelet dials that first socket, interrogates the driver, and
//!    calls `NotifyRegistrationStatus` back on the registration socket to
//!    say whether it accepted it.
//!
//! ★ THE TWO SOCKETS ARE NOT REDUNDANT and collapsing them is the mistake
//! this module header exists to prevent. The registration socket is owned
//! by the registrar sidecar, the driver socket by the driver container.
//! They have different lifetimes: a driver can restart without the
//! registrar noticing, which is exactly why step 4's callback exists.
//!
//! ★ `NotifyRegistrationStatus` IS NOT OPTIONAL POLITENESS. A registrar
//! that never hears back assumes failure and re-registers in a loop; some
//! restart the driver container. Skipping the callback produces a driver
//! that appears to flap for no reason. It is called on the failure path
//! too, with the error, which is how `kubectl logs` on the registrar can
//! explain a rejection.
//!
//! ★ WHAT THIS MODULE DELIBERATELY DOES NOT DO. It does not watch with
//! inotify/FSEvents. A directory scan on the kubelet's existing tick is
//! sufficient — registration is a startup event measured in seconds, not a
//! hot path — and it avoids a second platform-specific code path for a
//! problem that does not need one. Named here so the absence reads as a
//! decision.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::client::{CsiClient, CsiError, DriverInfo};
use crate::reg::registration_client::RegistrationClient;

/// The plugin type string a CSI driver registers as. A device plugin uses
/// the same directory and the same protocol with a different type, so this
/// is the discriminator that keeps engenho from dialing a GPU plugin as if
/// it were a filesystem.
pub const CSI_PLUGIN_TYPE: &str = "CSIPlugin";

/// The CSI spec version engenho speaks.
pub const SUPPORTED_CSI_VERSION: &str = "1.0.0";

/// Errors discovering or registering a plugin.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// The registry directory could not be read.
    #[error("reading plugin registry {dir}: {source}")]
    ReadDir {
        /// The directory.
        dir: String,
        /// The io error.
        #[source]
        source: std::io::Error,
    },
    /// The plugin's registration socket refused.
    #[error("GetInfo on {socket}: {status}")]
    GetInfo {
        /// The registration socket.
        socket: String,
        /// The driver's status.
        status: Box<tonic::Status>,
    },
    /// The plugin is not a CSI driver.
    #[error("plugin at {socket} is type {found:?}, not {CSI_PLUGIN_TYPE}")]
    NotCsi {
        /// The registration socket.
        socket: String,
        /// What it said it was.
        found: String,
    },
    /// The plugin speaks no CSI version engenho supports.
    #[error("plugin {name} supports {versions:?}, engenho speaks {SUPPORTED_CSI_VERSION}")]
    VersionMismatch {
        /// Driver name.
        name: String,
        /// What it offered.
        versions: Vec<String>,
    },
    /// The driver socket itself failed.
    #[error(transparent)]
    Driver(#[from] CsiError),
}

engenho_substrate::impl_error_kind! {
    RegistrationError {
        { ReadDir { .. } } => "read_dir",
        { GetInfo { .. } } => "get_info",
        { NotCsi { .. } } => "not_csi",
        { VersionMismatch { .. } } => "version_mismatch",
        (Driver(_)) => "driver",
    }
}

/// A driver that completed registration.
#[derive(Debug, Clone)]
pub struct DiscoveredPlugin {
    /// What the driver told us about itself.
    pub info: DriverInfo,
    /// The driver's own socket (from `GetInfo`), not the registration one.
    pub endpoint: String,
    /// The registration socket, kept so the status callback can be re-sent
    /// and so a vanished registrar can be detected.
    pub registration_socket: PathBuf,
}

/// Whether a plugin's offered versions include one engenho speaks.
///
/// Empty is ACCEPTED: the field is documented as optional and older
/// registrars omit it. Refusing on empty would reject working drivers to
/// enforce a check that upstream does not make either.
#[must_use]
pub fn version_supported(versions: &[String]) -> bool {
    versions.is_empty() || versions.iter().any(|v| v == SUPPORTED_CSI_VERSION)
}

/// Whether a directory entry looks like a registration socket.
///
/// Upstream's convention is `<driver>-reg.sock`, but the kubelet does not
/// actually enforce the name — it tries every socket in the directory. This
/// filters on the EXTENSION only, for the same reason: a driver that names
/// its socket differently is conformant, and rejecting it would be
/// engenho inventing a rule.
#[must_use]
pub fn looks_like_a_socket(path: &Path) -> bool {
    path.extension().is_some_and(|e| e == "sock")
}

/// Discovers CSI drivers by scanning the kubelet's `plugins_registry`
/// directory.
#[derive(Debug, Clone)]
pub struct PluginRegistry {
    dir: PathBuf,
}

impl PluginRegistry {
    /// A registry over `<kubelet-root>/plugins_registry`.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The conventional path under a kubelet root.
    #[must_use]
    pub fn under_kubelet_root(root: impl AsRef<Path>) -> Self {
        Self::new(root.as_ref().join("plugins_registry"))
    }

    /// The directory being scanned.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Every socket currently in the registry directory.
    ///
    /// A MISSING directory is an empty list, not an error: on a node with
    /// no CSI driver deployed the directory legitimately does not exist,
    /// and erroring would put a permanent failure in the kubelet's log for
    /// a completely normal state.
    ///
    /// # Errors
    /// [`RegistrationError::ReadDir`] if the directory exists but cannot be
    /// read — a real problem, unlike its absence.
    pub fn sockets(&self) -> Result<Vec<PathBuf>, RegistrationError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(RegistrationError::ReadDir {
                    dir: self.dir.display().to_string(),
                    source,
                });
            }
        };
        let mut out: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| looks_like_a_socket(p))
            .collect();
        // Deterministic order so a scan is reproducible and a test can
        // assert on it.
        out.sort();
        Ok(out)
    }

    /// Register the plugin behind one registration socket.
    ///
    /// Performs the full four-step handshake including the status callback.
    ///
    /// # Errors
    /// Any [`RegistrationError`]; the callback is still sent on failure so
    /// the registrar learns why.
    pub async fn register_one(&self, socket: &Path) -> Result<DiscoveredPlugin, RegistrationError> {
        let result = self.interrogate(socket).await;
        // The callback happens on BOTH paths. A registrar that never hears
        // back re-registers in a loop, so a silent rejection looks like a
        // flapping driver.
        let (ok, err) = match &result {
            Ok(_) => (true, String::new()),
            Err(e) => (false, e.to_string()),
        };
        self.notify(socket, ok, err).await;
        result
    }

    async fn interrogate(&self, socket: &Path) -> Result<DiscoveredPlugin, RegistrationError> {
        let channel = crate::client::connect(socket).await?;
        let plugin = RegistrationClient::new(channel)
            .get_info(crate::reg::InfoRequest {})
            .await
            .map_err(|status| RegistrationError::GetInfo {
                socket: socket.display().to_string(),
                status: Box::new(status),
            })?
            .into_inner();

        if plugin.r#type != CSI_PLUGIN_TYPE {
            return Err(RegistrationError::NotCsi {
                socket: socket.display().to_string(),
                found: plugin.r#type,
            });
        }
        if !version_supported(&plugin.supported_versions) {
            return Err(RegistrationError::VersionMismatch {
                name: plugin.name,
                versions: plugin.supported_versions,
            });
        }

        // `endpoint` is optional: when empty the driver socket is the
        // registration socket itself. Defaulting it to the registration
        // socket rather than failing is what upstream does, and a
        // single-socket driver is a legal deployment.
        let endpoint = if plugin.endpoint.is_empty() {
            socket.display().to_string()
        } else {
            plugin.endpoint
        };

        let client = CsiClient::dial(&endpoint).await?;
        let info = client.info().await?;

        Ok(DiscoveredPlugin {
            info,
            endpoint,
            registration_socket: socket.to_path_buf(),
        })
    }

    /// Best-effort `NotifyRegistrationStatus`.
    ///
    /// Failing to deliver the callback is logged and dropped: the plugin is
    /// either already registered or already gone, and turning a callback
    /// failure into a registration failure would discard a working driver.
    async fn notify(&self, socket: &Path, plugin_registered: bool, error: String) {
        let Ok(channel) = crate::client::connect(socket).await else {
            tracing::debug!(socket = %socket.display(), "registrar gone before the status callback");
            return;
        };
        if let Err(e) = RegistrationClient::new(channel)
            .notify_registration_status(crate::reg::RegistrationStatus {
                plugin_registered,
                error,
            })
            .await
        {
            tracing::debug!(socket = %socket.display(), error = %e, "status callback refused");
        }
    }

    /// Scan and register everything present.
    ///
    /// Returns the drivers by name plus the per-socket failures, because a
    /// caller needs BOTH: one broken driver must not hide three working
    /// ones, and a silently-skipped driver is how a storage outage becomes
    /// unexplainable.
    ///
    /// # Errors
    /// [`RegistrationError::ReadDir`] only — per-plugin failures are
    /// returned in the second element, not raised.
    pub async fn scan(
        &self,
    ) -> Result<
        (
            BTreeMap<String, DiscoveredPlugin>,
            Vec<(PathBuf, RegistrationError)>,
        ),
        RegistrationError,
    > {
        let mut found = BTreeMap::new();
        let mut failed = Vec::new();
        for socket in self.sockets()? {
            match self.register_one(&socket).await {
                Ok(p) => {
                    found.insert(p.info.name.clone(), p);
                }
                Err(e) => failed.push((socket, e)),
            }
        }
        Ok((found, failed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_registry_directory_is_empty_not_an_error() {
        // A node with no CSI driver deployed is a completely normal state.
        // Erroring here would put a permanent failure in the kubelet log
        // for every such node.
        let r = PluginRegistry::new("/nonexistent/plugins_registry");
        assert_eq!(r.sockets().unwrap(), Vec::<PathBuf>::new());
    }

    #[test]
    fn only_sockets_are_scanned_and_the_order_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        for f in ["b-reg.sock", "a-reg.sock", "README.md", "notes.txt"] {
            std::fs::write(dir.path().join(f), b"").unwrap();
        }
        let got = PluginRegistry::new(dir.path()).sockets().unwrap();
        let names: Vec<_> = got
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(names, ["a-reg.sock", "b-reg.sock"]);
    }

    #[test]
    fn an_unconventionally_named_socket_is_still_scanned() {
        // The kubelet does not enforce the `-reg.sock` suffix, so neither
        // does engenho: rejecting a conformant driver over a filename
        // would be a rule we invented.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("weird.sock"), b"").unwrap();
        assert_eq!(PluginRegistry::new(dir.path()).sockets().unwrap().len(), 1);
    }

    #[test]
    fn an_empty_version_list_is_accepted_and_a_wrong_one_is_not() {
        // Older registrars omit the field; upstream does not reject them.
        assert!(version_supported(&[]));
        assert!(version_supported(&["1.0.0".into()]));
        assert!(version_supported(&["0.3.0".into(), "1.0.0".into()]));
        assert!(!version_supported(&["0.3.0".into()]));
    }

    #[test]
    fn under_kubelet_root_uses_upstreams_directory_name() {
        // A typo here means engenho watches a directory no driver writes
        // to, and every driver silently never registers.
        let r = PluginRegistry::under_kubelet_root("/var/lib/kubelet");
        assert_eq!(r.dir(), Path::new("/var/lib/kubelet/plugins_registry"));
    }
}
