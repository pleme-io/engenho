//! Where the runner looks on disk — and the one seam that makes the whole
//! pipeline testable.
//!
//! systemd's paths are absolute constants: `/var/lib`, `/var/cache`,
//! `/var/log`, `/run`, `/etc`, `/etc/passwd`, `/run/credentials/<unit>`. A
//! runner that hard-codes them can only be tested as root on a machine it is
//! allowed to write to, which means it is not tested. Every path this crate
//! touches is therefore resolved through a [`HostRoot`]: `/` in production,
//! a `tempfile::TempDir` in tests, and the same code runs against both.

use std::path::{Path, PathBuf};

/// The filesystem the runner acts on. `/` on a node; a temporary directory
/// in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRoot(PathBuf);

impl Default for HostRoot {
    fn default() -> Self {
        Self::system()
    }
}

impl HostRoot {
    /// The real filesystem.
    #[must_use]
    pub fn system() -> Self {
        Self(PathBuf::from("/"))
    }

    /// A filesystem rooted at `root` — for tests.
    #[must_use]
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self(root.into())
    }

    /// Where an absolute unit-file path lands under this root.
    ///
    /// A relative path is returned unchanged: the caller (the exec path,
    /// which resolves a bare program name through `PATH`) owns that case.
    #[must_use]
    pub fn resolve(&self, path: &Path) -> PathBuf {
        match path.strip_prefix("/") {
            Ok(relative) => self.0.join(relative),
            Err(_) => path.to_path_buf(),
        }
    }

    /// The root itself.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// The well-known directories, as absolute host paths (what the service
/// sees), together with the root they are created under.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Layout {
    root: HostRoot,
}

impl Layout {
    /// A layout over `root`.
    #[must_use]
    pub const fn new(root: HostRoot) -> Self {
        Self { root }
    }

    /// The root the layout creates under.
    #[must_use]
    pub const fn root(&self) -> &HostRoot {
        &self.root
    }

    /// `/var/lib`, `/var/cache`, `/var/log`, `/run`, `/etc` — what `%S %C %L
    /// %t %E` expand to, ALREADY under the root.
    ///
    /// ★ A path this layout DERIVES is real; a path the UNIT wrote is not.
    /// The service is told these paths, runs commands against them and has
    /// its directories created at them, so under a test root they must all be
    /// the same rooted path — otherwise a test would prepare one directory
    /// and hand the service another. An absolute path that came out of the
    /// unit file (`WorkingDirectory=`, `EnvironmentFile=`, a credential's
    /// source) is rooted separately, by [`Self::on_disk`], exactly once.
    #[must_use]
    pub fn base(&self, kind: BaseDir) -> PathBuf {
        self.root.resolve(Path::new(kind.path()))
    }

    /// Where an absolute path written in the unit file actually lives.
    #[must_use]
    pub fn on_disk(&self, path: &Path) -> PathBuf {
        self.root.resolve(path)
    }

    /// The per-unit credentials directory, `<root>/run/credentials/<unit>`.
    #[must_use]
    pub fn credentials_dir(&self, unit: &str) -> PathBuf {
        self.base(BaseDir::Runtime).join("credentials").join(unit)
    }

    /// `/etc/passwd`.
    #[must_use]
    pub fn passwd(&self) -> PathBuf {
        self.on_disk(Path::new("/etc/passwd"))
    }

    /// `/etc/group`.
    #[must_use]
    pub fn group(&self) -> PathBuf {
        self.on_disk(Path::new("/etc/group"))
    }
}

/// One of the five bases a `*Directory=` directive is relative to, plus the
/// specifier and environment variable systemd attaches to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BaseDir {
    /// `StateDirectory=` → `/var/lib`, `%S`, `$STATE_DIRECTORY`.
    State,
    /// `CacheDirectory=` → `/var/cache`, `%C`, `$CACHE_DIRECTORY`.
    Cache,
    /// `LogsDirectory=` → `/var/log`, `%L`, `$LOGS_DIRECTORY`.
    Logs,
    /// `RuntimeDirectory=` → `/run`, `%t`, `$RUNTIME_DIRECTORY`.
    Runtime,
    /// `ConfigurationDirectory=` → `/etc`, `%E`, `$CONFIGURATION_DIRECTORY`.
    Configuration,
}

impl BaseDir {
    /// Every base, in directive order.
    pub const ALL: [Self; 5] = [
        Self::State,
        Self::Cache,
        Self::Logs,
        Self::Runtime,
        Self::Configuration,
    ];

    /// The absolute base path.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::State => "/var/lib",
            Self::Cache => "/var/cache",
            Self::Logs => "/var/log",
            Self::Runtime => "/run",
            Self::Configuration => "/etc",
        }
    }

    /// The directive that declares directories under it.
    #[must_use]
    pub const fn directive(self) -> &'static str {
        match self {
            Self::State => "StateDirectory",
            Self::Cache => "CacheDirectory",
            Self::Logs => "LogsDirectory",
            Self::Runtime => "RuntimeDirectory",
            Self::Configuration => "ConfigurationDirectory",
        }
    }

    /// The directive that sets their mode.
    #[must_use]
    pub const fn mode_directive(self) -> &'static str {
        match self {
            Self::State => "StateDirectoryMode",
            Self::Cache => "CacheDirectoryMode",
            Self::Logs => "LogsDirectoryMode",
            Self::Runtime => "RuntimeDirectoryMode",
            Self::Configuration => "ConfigurationDirectoryMode",
        }
    }

    /// The environment variable systemd exports with their paths.
    #[must_use]
    pub const fn env_var(self) -> &'static str {
        match self {
            Self::State => "STATE_DIRECTORY",
            Self::Cache => "CACHE_DIRECTORY",
            Self::Logs => "LOGS_DIRECTORY",
            Self::Runtime => "RUNTIME_DIRECTORY",
            Self::Configuration => "CONFIGURATION_DIRECTORY",
        }
    }

    /// Whether the innermost directory is owned by the service's user.
    ///
    /// systemd.exec: "Except in case of `ConfigurationDirectory=`, the
    /// innermost specified directories will be owned by the user and group
    /// specified in `User=` and `Group=`."
    #[must_use]
    pub const fn owned_by_service(self) -> bool {
        !matches!(self, Self::Configuration)
    }

    /// systemd's default mode for the directive.
    #[must_use]
    pub const fn default_mode(self) -> u32 {
        0o755
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absolute_path_lands_under_the_root() {
        let layout = Layout::new(HostRoot::at("/tmp/x"));
        assert_eq!(
            layout.on_disk(Path::new("/var/lib/hass")),
            PathBuf::from("/tmp/x/var/lib/hass")
        );
        assert_eq!(layout.passwd(), PathBuf::from("/tmp/x/etc/passwd"));
        assert_eq!(
            layout.base(BaseDir::State),
            PathBuf::from("/tmp/x/var/lib"),
            "a derived base is already rooted — the service is told THIS path"
        );
        assert_eq!(
            layout.credentials_dir("zwave-js.service"),
            PathBuf::from("/tmp/x/run/credentials/zwave-js.service")
        );
    }

    #[test]
    fn the_system_root_is_a_no_op() {
        let layout = Layout::new(HostRoot::system());
        assert_eq!(
            layout.on_disk(Path::new("/etc/passwd")),
            PathBuf::from("/etc/passwd")
        );
        assert_eq!(layout.base(BaseDir::Runtime), PathBuf::from("/run"));
        assert_eq!(
            layout.credentials_dir("u.service"),
            PathBuf::from("/run/credentials/u.service")
        );
    }

    #[test]
    fn every_base_has_a_directive_a_mode_and_an_env_var() {
        for base in BaseDir::ALL {
            assert!(base.path().starts_with('/'));
            assert!(base.mode_directive().starts_with(base.directive()));
            assert!(base.env_var().ends_with("_DIRECTORY"));
        }
        assert!(!BaseDir::Configuration.owned_by_service());
        assert!(BaseDir::State.owned_by_service());
    }
}
