//! The daemon's own control plane: where it listens, and who may do what.
//!
//! Read by the supervisor, never by the runtime: a control listener is up
//! before the first boot and stays up across every boot, restart and failure.
//! The daemon and `engenho ctl` both derive the socket path from this
//! section ([`ControlSocketConfig::resolved_path`]), so they agree on it.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

use crate::error::ConfigError;

/// Who may connect to the local control socket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketAccess {
    /// The daemon's own user (and root): the socket is `0600` in a directory
    /// the daemon's user owns.
    #[default]
    Owner,
    /// Also the members of the socket directory's group: the socket is
    /// `0660`, and its group is the directory's (which the deployment
    /// manages — `/run/engenho` on NixOS).
    Group,
}

/// The most a member of the socket's group may do. Destructive operations
/// are never granted through the group: they need the daemon's user or root.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupTier {
    /// Read everything.
    #[default]
    Observe,
    /// Also operate: start, stop, set config, restart children.
    Mutate,
}

/// The local control socket.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlSocketConfig {
    /// Where the socket is. Absent: the default for the daemon's user (see
    /// [`SocketDefaults::default_path`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// Who may connect.
    #[serde(default)]
    pub access: SocketAccess,
    /// The most a group member may do, when `access` is `group`.
    #[serde(default)]
    pub group_tier: GroupTier,
}

/// What a remote client may do: declared on the server, per pin. A client's
/// certificate never carries a tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteTier {
    /// Read everything.
    Observe,
    /// Also operate.
    Mutate,
    /// Also the confirmation-gated re-initializations.
    Destructive,
}

/// One client the remote listener admits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedClientConfig {
    /// Its name: recorded as who acted.
    pub name: String,
    /// The SHA-256 of its key's DER `SubjectPublicKeyInfo`, as
    /// `sha256:<64 lowercase hex>` (`engenho remote fingerprint <name>`
    /// prints it on the client).
    pub spki_sha256: String,
    /// What it may do.
    pub tier: RemoteTier,
}

/// Where the remote listener binds by default. A deployment restricts it to
/// the tailnet with its firewall; the pins are what admit a client.
pub const DEFAULT_REMOTE_LISTEN: &str = "0.0.0.0:7443";

/// The remote control listener: TLS 1.3, each client admitted by the pin of
/// its key, and the listener known to clients by the pin of its own
/// (`data_dir/control/identity/`) — never by engenho's cluster CA.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteControlConfig {
    /// Listen at all. Off by default: the local socket is always on.
    #[serde(default)]
    pub enable: bool,
    /// Where.
    #[serde(default = "default_remote_listen")]
    pub listen_addr: String,
    /// Who may connect, and what each may do.
    #[serde(default)]
    pub authorized_clients: Vec<AuthorizedClientConfig>,
}

fn default_remote_listen() -> String {
    DEFAULT_REMOTE_LISTEN.to_owned()
}

impl Default for RemoteControlConfig {
    fn default() -> Self {
        Self {
            enable: false,
            listen_addr: default_remote_listen(),
            authorized_clients: Vec::new(),
        }
    }
}

/// Whether `s` is spelled `sha256:<64 lowercase hex>`.
fn is_pin(s: &str) -> bool {
    s.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

impl RemoteControlConfig {
    /// Validate the remote section: a listen address that parses, and
    /// clients with names and pins that are well formed and unique.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidField`] naming the offending field.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |field: &str, reason: String| ConfigError::InvalidField {
            field: ["control.remote.", field].concat(),
            reason,
        };
        if self.listen_addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(invalid(
                "listen_addr",
                format!("{:?} is not an address:port", self.listen_addr),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut pins = std::collections::BTreeSet::new();
        for client in &self.authorized_clients {
            if client.name.trim().is_empty() {
                return Err(invalid("authorized_clients", "a client has no name".into()));
            }
            if !is_pin(&client.spki_sha256) {
                return Err(invalid(
                    "authorized_clients",
                    format!(
                        "{}: {:?} is not a pin (sha256:<64 lowercase hex>)",
                        client.name, client.spki_sha256
                    ),
                ));
            }
            if !names.insert(client.name.as_str()) {
                return Err(invalid(
                    "authorized_clients",
                    format!("{} is named twice", client.name),
                ));
            }
            if !pins.insert(client.spki_sha256.as_str()) {
                return Err(invalid(
                    "authorized_clients",
                    format!("{}'s pin is another client's too", client.name),
                ));
            }
        }
        Ok(())
    }
}

/// The control plane.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    /// The local control socket: always on, the recovery path.
    #[serde(default)]
    pub socket: ControlSocketConfig,
    /// The remote listener.
    #[serde(default)]
    pub remote: RemoteControlConfig,
}

/// The longest path a Unix socket address holds, NUL excluded: `sun_path` is
/// 104 bytes on macOS and 108 on Linux.
#[cfg(target_os = "macos")]
pub const SOCKET_PATH_MAX: usize = 103;
/// The longest path a Unix socket address holds, NUL excluded: `sun_path` is
/// 104 bytes on macOS and 108 on Linux.
#[cfg(not(target_os = "macos"))]
pub const SOCKET_PATH_MAX: usize = 107;

/// Where a daemon running as root puts its socket.
#[cfg(target_os = "macos")]
pub const SYSTEM_SOCKET_PATH: &str = "/var/run/engenho/control.sock";
/// Where a daemon running as root puts its socket.
#[cfg(not(target_os = "macos"))]
pub const SYSTEM_SOCKET_PATH: &str = "/run/engenho/control.sock";

/// What the default socket path depends on: the user, and where that user
/// keeps state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketDefaults {
    /// The effective uid.
    pub euid: u32,
    /// `$XDG_STATE_HOME`, when set.
    pub xdg_state_home: Option<PathBuf>,
    /// `$HOME`, when set.
    pub home: Option<PathBuf>,
}

impl SocketDefaults {
    /// The defaults for this process, running as `euid`.
    #[must_use]
    pub fn for_process(euid: u32) -> Self {
        let dir = |var: &str| {
            std::env::var_os(var)
                .map(PathBuf::from)
                .filter(|p| p.is_absolute())
        };
        Self {
            euid,
            xdg_state_home: dir("XDG_STATE_HOME"),
            home: dir("HOME"),
        }
    }

    /// Root: [`SYSTEM_SOCKET_PATH`]. Anyone else: `engenho/control.sock` under
    /// `$XDG_STATE_HOME`, else under `$HOME/.local/state`, else under
    /// `/tmp/engenho-<uid>`.
    #[must_use]
    pub fn default_path(&self) -> PathBuf {
        if self.euid == 0 {
            return PathBuf::from(SYSTEM_SOCKET_PATH);
        }
        let state = self
            .xdg_state_home
            .clone()
            .or_else(|| self.home.as_ref().map(|h| h.join(".local").join("state")));
        match state {
            Some(state) => state.join("engenho").join("control.sock"),
            None => PathBuf::from(format!("/tmp/engenho-{}", self.euid)).join("control.sock"),
        }
    }
}

impl ControlSocketConfig {
    /// The socket path: the configured one, or the default.
    #[must_use]
    pub fn resolved_path(&self, defaults: &SocketDefaults) -> PathBuf {
        self.path.clone().unwrap_or_else(|| defaults.default_path())
    }

    /// Validate the socket section.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidField`] for a relative path, or one too long to
    /// be a socket address.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let invalid = |reason: String| ConfigError::InvalidField {
            field: "control.socket.path".into(),
            reason,
        };
        if !path.is_absolute() {
            return Err(invalid(format!("{} is not absolute", path.display())));
        }
        check_socket_path_len(path).map_err(invalid)
    }
}

/// Refuse a path too long for a socket address, naming the limit.
///
/// # Errors
///
/// The reason, when the path is too long.
pub fn check_socket_path_len(path: &Path) -> Result<(), String> {
    let len = path.as_os_str().len();
    if len > SOCKET_PATH_MAX {
        return Err(format!(
            "{} is {len} bytes; a socket path holds at most {SOCKET_PATH_MAX} here",
            path.display()
        ));
    }
    Ok(())
}

impl TieredConfig for ControlConfig {
    fn bare() -> Self {
        Self::default()
    }

    fn prescribed_default() -> Self {
        Self::default()
    }

    fn extend(self, base: &Self) -> Self {
        // `access` and `group_tier` carry an explicit value on every tier
        // (their serde defaults are the prescribed ones), so the overlay's
        // wins; an absent `path` inherits.
        // `remote` likewise: every field carries its serde default.
        Self {
            socket: ControlSocketConfig {
                path: self.socket.path.or_else(|| base.socket.path.clone()),
                access: self.socket.access,
                group_tier: self.socket.group_tier,
            },
            remote: self.remote,
        }
    }
}

impl ControlConfig {
    /// Validate the control section.
    ///
    /// # Errors
    ///
    /// See [`ControlSocketConfig::validate`] and
    /// [`RemoteControlConfig::validate`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.socket.validate()?;
        self.remote.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults(euid: u32, xdg: Option<&str>, home: Option<&str>) -> SocketDefaults {
        SocketDefaults {
            euid,
            xdg_state_home: xdg.map(PathBuf::from),
            home: home.map(PathBuf::from),
        }
    }

    #[test]
    fn root_uses_the_system_path_and_a_user_its_state_dir() {
        assert_eq!(
            defaults(0, Some("/x"), Some("/root")).default_path(),
            PathBuf::from(SYSTEM_SOCKET_PATH)
        );
        assert_eq!(
            defaults(501, Some("/Users/o/.state"), Some("/Users/o")).default_path(),
            PathBuf::from("/Users/o/.state/engenho/control.sock")
        );
        assert_eq!(
            defaults(501, None, Some("/Users/o")).default_path(),
            PathBuf::from("/Users/o/.local/state/engenho/control.sock")
        );
        assert_eq!(
            defaults(501, None, None).default_path(),
            PathBuf::from("/tmp/engenho-501/control.sock")
        );
    }

    #[test]
    fn a_configured_path_wins_and_is_checked() {
        let cfg = ControlSocketConfig {
            path: Some(PathBuf::from("/srv/e/ctl.sock")),
            ..ControlSocketConfig::default()
        };
        assert_eq!(
            cfg.resolved_path(&defaults(0, None, None)),
            PathBuf::from("/srv/e/ctl.sock")
        );
        cfg.validate().expect("valid");

        let relative = ControlSocketConfig {
            path: Some(PathBuf::from("ctl.sock")),
            ..ControlSocketConfig::default()
        };
        assert!(relative.validate().is_err());
        let long = ControlSocketConfig {
            path: Some(PathBuf::from(format!("/{}", "a".repeat(SOCKET_PATH_MAX)))),
            ..ControlSocketConfig::default()
        };
        assert!(long.validate().is_err());
    }

    #[test]
    fn remote_clients_need_names_and_pins_both_well_formed_and_unique() {
        let pin = |c: char| ["sha256:", &c.to_string().repeat(64)].concat();
        let client = |name: &str, spki: String| AuthorizedClientConfig {
            name: name.into(),
            spki_sha256: spki,
            tier: RemoteTier::Observe,
        };
        let with = |clients: Vec<AuthorizedClientConfig>| RemoteControlConfig {
            enable: true,
            authorized_clients: clients,
            ..RemoteControlConfig::default()
        };
        with(vec![client("a", pin('a')), client("b", pin('b'))])
            .validate()
            .expect("valid");
        for bad in [
            with(vec![client("", pin('a'))]),
            with(vec![client("a", "sha256:ABC".into())]),
            with(vec![client("a", pin('A'))]),
            with(vec![client("a", pin('a')), client("a", pin('b'))]),
            with(vec![client("a", pin('a')), client("b", pin('a'))]),
            RemoteControlConfig {
                listen_addr: "tailnet:7443".into(),
                ..RemoteControlConfig::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        let parsed: ControlConfig = serde_yaml::from_str(
            "remote:\n  enable: true\n  authorized_clients:\n    - {name: ryn, spki_sha256: sha256:0000000000000000000000000000000000000000000000000000000000000000, tier: destructive}\n",
        )
        .expect("parse");
        assert_eq!(parsed.remote.listen_addr, DEFAULT_REMOTE_LISTEN);
        assert_eq!(
            parsed.remote.authorized_clients[0].tier,
            RemoteTier::Destructive
        );
    }

    #[test]
    fn the_section_parses_and_rejects_unknown_fields() {
        let cfg: ControlConfig =
            serde_yaml::from_str("socket:\n  access: group\n  group_tier: mutate\n")
                .expect("parse");
        assert_eq!(cfg.socket.access, SocketAccess::Group);
        assert_eq!(cfg.socket.group_tier, GroupTier::Mutate);
        assert!(serde_yaml::from_str::<ControlConfig>("socket:\n  nope: 1\n").is_err());
    }
}
