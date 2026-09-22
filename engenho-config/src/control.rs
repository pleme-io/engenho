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

/// The control plane.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    /// The local control socket: always on, the recovery path.
    #[serde(default)]
    pub socket: ControlSocketConfig,
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
        Self {
            socket: ControlSocketConfig {
                path: self.socket.path.or_else(|| base.socket.path.clone()),
                access: self.socket.access,
                group_tier: self.socket.group_tier,
            },
        }
    }
}

impl ControlConfig {
    /// Validate the control section.
    ///
    /// # Errors
    ///
    /// See [`ControlSocketConfig::validate`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.socket.validate()
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
    fn the_section_parses_and_rejects_unknown_fields() {
        let cfg: ControlConfig =
            serde_yaml::from_str("socket:\n  access: group\n  group_tier: mutate\n")
                .expect("parse");
        assert_eq!(cfg.socket.access, SocketAccess::Group);
        assert_eq!(cfg.socket.group_tier, GroupTier::Mutate);
        assert!(serde_yaml::from_str::<ControlConfig>("socket:\n  nope: 1\n").is_err());
    }
}
