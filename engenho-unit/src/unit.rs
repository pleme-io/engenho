//! The typed unit: every directive classified against a CLOSED catalog, and
//! the ones this runner applies turned into typed values.
//!
//! ## Why every directive is classified
//!
//! A runner that reads the directives it knows and ignores the rest reports
//! success while running a service that is missing half of what its unit
//! asked for — the failure shape engenho keeps finding (see
//! `engenho-kubelet/src/native_backend.rs`'s header). So every entry lands in
//! exactly one class:
//!
//! * [`Class::Applied`] — this runner does it.
//! * [`Class::Acknowledged`] — a sandboxing, namespace or resource directive
//!   it does NOT enforce. Kept with its value and logged once at start, so
//!   the operator reads "these are not in force" rather than nothing.
//! * [`Class::SupervisorOwned`] — restart policy, stop and reload commands,
//!   watchdogs, logging: engenho's kubelet owns the lifecycle, and the pod
//!   spec is where those decisions live.
//! * [`Class::Inert`] — unit ordering, conditions, `[Install]`, `X-`
//!   extensions: nothing to do when one unit is started directly.
//! * [`Class::Unknown`] — not in the catalog: a typed warning, never a crash
//!   and never silence.
//!
//! ## List semantics
//!
//! systemd's list-valued directives accumulate over repeated lines, and an
//! EMPTY assignment resets the list (`AmbientCapabilities=` on its own is how
//! nixpkgs clears one before setting it). Both are implemented here for
//! `Environment=`, `EnvironmentFile=`, every `Exec*=`, the `*Directory=`
//! family, `SupplementaryGroups=`, `AmbientCapabilities=`, `LoadCredential=`
//! and `SetCredential=`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::capability::{Capability, CapabilitySet};
use crate::credentials::CredentialSpec;
use crate::env::{self, EnvError, EnvironmentFile};
use crate::exec::{ExecCommand, ExecError};
use crate::layout::BaseDir;
use crate::specifier::{Context, SpecifierError};
use crate::syntax::{self, Entry, RawUnit, SyntaxError};
use crate::words::{self, WordError};

/// What this runner does with a directive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Applied by this runner.
    Applied,
    /// Parsed, kept, logged — not enforced.
    Acknowledged(Ack),
    /// The kubelet's business, not a unit-run one.
    SupervisorOwned,
    /// Nothing to do when a single unit is started directly.
    Inert,
    /// Not in the catalog.
    Unknown,
}

/// Which kind of thing an acknowledged directive would have done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ack {
    /// Seccomp, capability bounding, `NoNewPrivileges`, `Protect*`,
    /// `Restrict*`, device policy.
    Sandbox,
    /// A mount or user namespace: `PrivateTmp`, `RootDirectory`, `BindPaths`,
    /// `ReadWritePaths`, `PrivateUsers`, …
    Namespace,
    /// cgroup limits and rlimits: `MemoryMax`, `TasksMax`, `Limit*`, …
    Resource,
    /// Scheduling and priority: `Nice`, `CPUSchedulingPolicy`, `IOWeight`, …
    Scheduling,
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sandbox => "sandbox",
            Self::Namespace => "namespace",
            Self::Resource => "resource limit",
            Self::Scheduling => "scheduling",
        })
    }
}

/// A directive that was read but not applied, kept for the start-up log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Noted {
    /// Its class.
    pub class: Class,
    /// The key.
    pub key: String,
    /// The value, as written.
    pub value: String,
    /// The 1-based line.
    pub line: usize,
}

impl fmt::Display for Noted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={} (line {})", self.key, self.value, self.line)
    }
}

/// `Type=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServiceType {
    /// `simple` — the default.
    #[default]
    Simple,
    /// `exec`.
    Exec,
    /// `forking` — the main process is expected to fork and exit.
    Forking,
    /// `oneshot`.
    Oneshot,
    /// `notify` — no `$NOTIFY_SOCKET` is provided.
    Notify,
    /// `notify-reload`.
    NotifyReload,
    /// `dbus`.
    Dbus,
    /// `idle`.
    Idle,
}

impl ServiceType {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "simple" => Self::Simple,
            "exec" => Self::Exec,
            "forking" => Self::Forking,
            "oneshot" => Self::Oneshot,
            "notify" => Self::Notify,
            "notify-reload" => Self::NotifyReload,
            "dbus" => Self::Dbus,
            "idle" => Self::Idle,
            _ => return None,
        })
    }

    /// Whether the type expects something this runner cannot provide, and
    /// what: no `sd_notify` socket, and no fork tracking.
    #[must_use]
    pub const fn caveat(self) -> Option<&'static str> {
        match self {
            Self::Simple | Self::Exec | Self::Oneshot | Self::Idle => None,
            Self::Notify | Self::NotifyReload => Some(
                "Type=notify: no $NOTIFY_SOCKET is provided, so readiness is what the pod's \
                 probes say, not what the service notifies",
            ),
            Self::Dbus => Some(
                "Type=dbus: nothing waits for the service's bus name; readiness is the pod's \
                 probes",
            ),
            Self::Forking => Some(
                "Type=forking: the main process is expected to fork and exit, which reads as the \
                 workload exiting. Run the service in the foreground (most take a flag) or give \
                 the pod a restart policy that tolerates it",
            ),
        }
    }
}

/// `WorkingDirectory=`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingDirectory {
    /// `~` means the service user's home.
    pub home: bool,
    /// The path, when not `~`.
    pub path: Option<PathBuf>,
    /// `-`: a missing directory is not an error.
    pub optional: bool,
}

/// One directory a `*Directory=` directive declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectorySpec {
    /// Which base it hangs under.
    pub base: BaseDir,
    /// The relative path below the base.
    pub path: String,
}

/// `RuntimeDirectoryPreserve=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimePreserve {
    /// `no` — the default: systemd removes the runtime directory when the
    /// service stops, so this runner recreates it EMPTY at start, which is
    /// the state a fresh start would see.
    #[default]
    No,
    /// `yes` / `restart` — the directory's contents survive.
    Yes,
}

/// What `User=`, `Group=`, `SupplementaryGroups=` and `DynamicUser=` ask for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityRequest {
    /// `User=`.
    pub user: Option<String>,
    /// `Group=`.
    pub group: Option<String>,
    /// `SupplementaryGroups=`.
    pub supplementary: Vec<String>,
    /// `DynamicUser=`.
    pub dynamic: bool,
}

/// The typed unit this runner acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceUnit {
    /// The unit name, e.g. `zwave-js.service`.
    pub name: String,
    /// The file it was read from.
    pub fragment: PathBuf,
    /// `Description=`.
    pub description: Option<String>,
    /// `Type=`.
    pub service_type: ServiceType,
    /// Who it runs as.
    pub identity: IdentityRequest,
    /// `ExecStartPre=`, in order.
    pub exec_start_pre: Vec<ExecCommand>,
    /// `ExecStart=` — more than one only for `Type=oneshot`.
    pub exec_start: Vec<ExecCommand>,
    /// `Environment=`, in order.
    pub environment: Vec<(String, String)>,
    /// `EnvironmentFile=`, in order.
    pub environment_files: Vec<EnvironmentFile>,
    /// `WorkingDirectory=`.
    pub working_directory: Option<WorkingDirectory>,
    /// `UMask=`, systemd's `0022` when unset.
    pub umask: u32,
    /// Every `*Directory=` entry, in directive order.
    pub directories: Vec<DirectorySpec>,
    /// `*DirectoryMode=` per base, defaulting to `0755`.
    pub directory_modes: BTreeMap<BaseDir, u32>,
    /// `RuntimeDirectoryPreserve=`.
    pub runtime_preserve: RuntimePreserve,
    /// `LoadCredential=` and `SetCredential=`, in order.
    pub credentials: Vec<CredentialSpec>,
    /// `AmbientCapabilities=`.
    pub ambient: CapabilitySet,
    /// Every directive read but not applied.
    pub noted: Vec<Noted>,
}

impl ServiceUnit {
    /// The acknowledged directives, by class.
    #[must_use]
    pub fn acknowledged(&self) -> Vec<&Noted> {
        self.noted
            .iter()
            .filter(|n| matches!(n.class, Class::Acknowledged(_)))
            .collect()
    }

    /// The directives the catalog does not know.
    #[must_use]
    pub fn unknown(&self) -> Vec<&Noted> {
        self.noted
            .iter()
            .filter(|n| n.class == Class::Unknown)
            .collect()
    }

    /// The mode a base's directories are created with.
    #[must_use]
    pub fn directory_mode(&self, base: BaseDir) -> u32 {
        self.directory_modes
            .get(&base)
            .copied()
            .unwrap_or_else(|| base.default_mode())
    }
}

/// Why a unit file did not become a [`ServiceUnit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitError {
    /// The file could not be read.
    Unreadable {
        /// The path.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// The file does not lex.
    Syntax(SyntaxError),
    /// The path does not name a `.service` unit.
    NotAService {
        /// The path.
        path: PathBuf,
    },
    /// The unit has no `[Service]` section.
    NoServiceSection,
    /// The unit declares no `ExecStart=`.
    NoExecStart,
    /// A specifier in a value did not expand.
    Specifier {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// Why.
        source: SpecifierError,
    },
    /// An `Exec*=` line did not parse.
    Exec {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// Why.
        source: ExecError,
    },
    /// An environment directive did not parse.
    Env {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// Why.
        source: EnvError,
    },
    /// A value did not split into words.
    Words {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// Why.
        source: WordError,
    },
    /// A value this runner cannot honour, with what to do instead.
    Unsupported {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// The value.
        value: String,
        /// Why it cannot be honoured.
        why: &'static str,
    },
    /// A value that is not of the directive's type.
    BadValue {
        /// The directive.
        key: String,
        /// The 1-based line.
        line: usize,
        /// The value.
        value: String,
        /// What the directive takes.
        expected: &'static str,
    },
}

impl fmt::Display for UnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { path, detail } => {
                write!(f, "{} cannot be read: {detail}", path.display())
            }
            Self::Syntax(e) => write!(f, "{e}"),
            Self::NotAService { path } => write!(
                f,
                "{} is not a .service unit: unit-run runs services, and the unit's name is \
                 what %n, %N and the credentials directory are built from",
                path.display()
            ),
            Self::NoServiceSection => f.write_str("the unit has no [Service] section"),
            Self::NoExecStart => f.write_str("the unit declares no ExecStart="),
            Self::Specifier { key, line, source } => write!(f, "{key}= (line {line}): {source}"),
            Self::Exec { key, line, source } => write!(f, "{key}= (line {line}): {source}"),
            Self::Env { key, line, source } => write!(f, "{key}= (line {line}): {source}"),
            Self::Words { key, line, source } => write!(f, "{key}= (line {line}): {source}"),
            Self::Unsupported {
                key,
                line,
                value,
                why,
            } => write!(f, "{key}={value} (line {line}) is not supported: {why}"),
            Self::BadValue {
                key,
                line,
                value,
                expected,
            } => write!(f, "{key}={value} (line {line}) is not {expected}"),
        }
    }
}

impl std::error::Error for UnitError {}

impl From<SyntaxError> for UnitError {
    fn from(e: SyntaxError) -> Self {
        Self::Syntax(e)
    }
}

/// A lexed unit file, before specifiers are expanded.
#[derive(Debug, Clone)]
pub struct UnitFile {
    /// The unit name, from the file name.
    pub name: String,
    /// The file.
    pub path: PathBuf,
    raw: RawUnit,
}

impl UnitFile {
    /// Read and lex `path`.
    ///
    /// # Errors
    ///
    /// [`UnitError::Unreadable`], [`UnitError::NotAService`] or
    /// [`UnitError::Syntax`].
    pub fn read(path: &Path) -> Result<Self, UnitError> {
        let text = std::fs::read_to_string(path).map_err(|e| UnitError::Unreadable {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })?;
        Self::parse(path, &text)
    }

    /// Lex `text` as the unit file at `path`.
    ///
    /// # Errors
    ///
    /// [`UnitError::NotAService`] or [`UnitError::Syntax`].
    pub fn parse(path: &Path, text: &str) -> Result<Self, UnitError> {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".service"))
            .ok_or_else(|| UnitError::NotAService {
                path: path.to_path_buf(),
            })?;
        Ok(Self {
            name,
            path: path.to_path_buf(),
            raw: syntax::lex(text)?,
        })
    }

    fn service_entries(&self) -> Result<&[Entry], UnitError> {
        self.raw
            .section("Service")
            .map(|s| s.entries.as_slice())
            .ok_or(UnitError::NoServiceSection)
    }

    /// Who the unit asks to run as — read FIRST, because the account it names
    /// is what `%u`, `%h`, `%U` … expand to everywhere else.
    ///
    /// # Errors
    ///
    /// [`UnitError`] when one of the four directives does not parse.
    pub fn identity_request(&self, ctx: &Context<'_>) -> Result<IdentityRequest, UnitError> {
        let mut request = IdentityRequest::default();
        for entry in self.service_entries()? {
            let expand = || expand(ctx, entry);
            match entry.key.as_str() {
                "User" => request.user = Some(expand()?).filter(|v| !v.is_empty()),
                "Group" => request.group = Some(expand()?).filter(|v| !v.is_empty()),
                "SupplementaryGroups" => {
                    let value = expand()?;
                    if value.is_empty() {
                        request.supplementary.clear();
                    } else {
                        request.supplementary.extend(split(entry, &value)?);
                    }
                }
                "DynamicUser" => request.dynamic = boolean(entry, &expand()?)?,
                _ => {}
            }
        }
        Ok(request)
    }

    /// Build the typed unit. `ctx` must carry the resolved account, so that
    /// `%u`, `%h` and friends expand.
    ///
    /// # Errors
    ///
    /// [`UnitError`] naming the directive and line that failed.
    #[allow(clippy::too_many_lines)] // one arm per directive: a catalog, not a function
    pub fn service(&self, ctx: &Context<'_>) -> Result<ServiceUnit, UnitError> {
        let mut unit = ServiceUnit {
            name: self.name.clone(),
            fragment: self.path.clone(),
            description: None,
            service_type: ServiceType::default(),
            identity: self.identity_request(ctx)?,
            exec_start_pre: Vec::new(),
            exec_start: Vec::new(),
            environment: Vec::new(),
            environment_files: Vec::new(),
            working_directory: None,
            umask: 0o022,
            directories: Vec::new(),
            directory_modes: BTreeMap::new(),
            runtime_preserve: RuntimePreserve::default(),
            credentials: Vec::new(),
            ambient: CapabilitySet::default(),
            noted: Vec::new(),
        };

        for section in &self.raw.sections {
            for entry in &section.entries {
                let class = classify(&section.name, &entry.key);
                match class {
                    Class::Applied => {}
                    Class::Acknowledged(_)
                    | Class::SupervisorOwned
                    | Class::Inert
                    | Class::Unknown => {
                        unit.noted.push(Noted {
                            class,
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            line: entry.line,
                        });
                        continue;
                    }
                }
                if section.name == "Unit" {
                    if entry.key == "Description" {
                        unit.description = Some(entry.value.clone());
                    }
                    continue;
                }
                let value = expand(ctx, entry)?;
                let empty = value.is_empty();
                match entry.key.as_str() {
                    // Identity: already read by `identity_request`.
                    "User" | "Group" | "SupplementaryGroups" | "DynamicUser" => {}
                    "Type" => {
                        unit.service_type =
                            ServiceType::parse(&value).ok_or_else(|| UnitError::BadValue {
                                key: entry.key.clone(),
                                line: entry.line,
                                value: value.clone(),
                                expected: "one of simple, exec, forking, oneshot, notify, \
                                           notify-reload, dbus, idle",
                            })?;
                    }
                    "ExecStartPre" => {
                        if empty {
                            unit.exec_start_pre.clear();
                        } else {
                            unit.exec_start_pre.push(exec(entry, &value)?);
                        }
                    }
                    "ExecStart" => {
                        if empty {
                            unit.exec_start.clear();
                        } else {
                            unit.exec_start.push(exec(entry, &value)?);
                        }
                    }
                    "Environment" => {
                        if empty {
                            unit.environment.clear();
                        } else {
                            unit.environment
                                .extend(env::parse_assignments(&value).map_err(|source| {
                                    UnitError::Env {
                                        key: entry.key.clone(),
                                        line: entry.line,
                                        source,
                                    }
                                })?);
                        }
                    }
                    "EnvironmentFile" => {
                        if empty {
                            unit.environment_files.clear();
                        } else {
                            let (optional, path) = match value.strip_prefix('-') {
                                Some(path) => (true, path),
                                None => (false, value.as_str()),
                            };
                            unit.environment_files.push(EnvironmentFile {
                                path: PathBuf::from(path),
                                optional,
                            });
                        }
                    }
                    "WorkingDirectory" => {
                        unit.working_directory = if empty {
                            None
                        } else {
                            let (optional, rest) = match value.strip_prefix('-') {
                                Some(rest) => (true, rest),
                                None => (false, value.as_str()),
                            };
                            if rest == "~" {
                                Some(WorkingDirectory {
                                    home: true,
                                    path: None,
                                    optional,
                                })
                            } else {
                                if !rest.starts_with('/') {
                                    return Err(UnitError::BadValue {
                                        key: entry.key.clone(),
                                        line: entry.line,
                                        value: value.clone(),
                                        expected: "an absolute path or ~",
                                    });
                                }
                                Some(WorkingDirectory {
                                    home: false,
                                    path: Some(PathBuf::from(rest)),
                                    optional,
                                })
                            }
                        };
                    }
                    "UMask" => unit.umask = mode(entry, &value)?,
                    "RuntimeDirectoryPreserve" => {
                        unit.runtime_preserve = match value.as_str() {
                            "no" => RuntimePreserve::No,
                            "yes" | "restart" => RuntimePreserve::Yes,
                            _ => {
                                return Err(UnitError::BadValue {
                                    key: entry.key.clone(),
                                    line: entry.line,
                                    value: value.clone(),
                                    expected: "one of no, yes, restart",
                                });
                            }
                        };
                    }
                    "AmbientCapabilities" => {
                        if empty {
                            unit.ambient.clear();
                        } else {
                            for name in split(entry, &value)? {
                                if name.starts_with('~') {
                                    return Err(UnitError::Unsupported {
                                        key: entry.key.clone(),
                                        line: entry.line,
                                        value: value.clone(),
                                        why: "an inverted (~) ambient capability set names every \
                                              capability EXCEPT these, which this runner does not \
                                              build: list the capabilities the service needs",
                                    });
                                }
                                let capability = Capability::parse(&name).ok_or_else(|| {
                                    UnitError::BadValue {
                                        key: entry.key.clone(),
                                        line: entry.line,
                                        value: name.clone(),
                                        expected: "a CAP_* name the kernel defines",
                                    }
                                })?;
                                unit.ambient.insert(capability);
                            }
                        }
                    }
                    "LoadCredential" | "SetCredential" => {
                        if empty {
                            unit.credentials.clear();
                        } else {
                            unit.credentials
                                .push(CredentialSpec::parse(&entry.key, &value, entry.line)?);
                        }
                    }
                    "LoadCredentialEncrypted" | "ImportCredential" | "SetCredentialEncrypted" => {
                        return Err(UnitError::Unsupported {
                            key: entry.key.clone(),
                            line: entry.line,
                            value: value.clone(),
                            why: "an encrypted or imported credential needs systemd's credential \
                                  store (TPM or host key); pass the secret through LoadCredential \
                                  with a plaintext source instead",
                        });
                    }
                    key => {
                        if let Some(base) = directory_base(key) {
                            if empty {
                                unit.directories.retain(|d| d.base != base);
                            } else {
                                for path in split(entry, &value)? {
                                    if path.contains(':') {
                                        return Err(UnitError::Unsupported {
                                            key: entry.key.clone(),
                                            line: entry.line,
                                            value: path,
                                            why: "the `dir:symlink` form is not created by this \
                                                  runner",
                                        });
                                    }
                                    if path.starts_with('/') || path.split('/').any(|c| c == "..") {
                                        return Err(UnitError::BadValue {
                                            key: entry.key.clone(),
                                            line: entry.line,
                                            value: path,
                                            expected: "a relative path below the directive's base, \
                                                       without `..`",
                                        });
                                    }
                                    unit.directories.push(DirectorySpec { base, path });
                                }
                            }
                        } else if let Some(base) = directory_mode_base(key) {
                            unit.directory_modes.insert(base, mode(entry, &value)?);
                        }
                    }
                }
            }
        }

        if unit.exec_start.is_empty() {
            return Err(UnitError::NoExecStart);
        }
        if unit.exec_start.len() > 1 && unit.service_type != ServiceType::Oneshot {
            return Err(UnitError::Unsupported {
                key: "ExecStart".into(),
                line: 0,
                value: unit.exec_start.len().to_string(),
                why: "several ExecStart= lines are only legal for Type=oneshot; the last one is \
                      the process this runner would exec",
            });
        }
        Ok(unit)
    }
}

fn expand(ctx: &Context<'_>, entry: &Entry) -> Result<String, UnitError> {
    ctx.expand(&entry.value)
        .map_err(|source| UnitError::Specifier {
            key: entry.key.clone(),
            line: entry.line,
            source,
        })
}

fn split(entry: &Entry, value: &str) -> Result<Vec<String>, UnitError> {
    words::split(value).map_err(|source| UnitError::Words {
        key: entry.key.clone(),
        line: entry.line,
        source,
    })
}

fn exec(entry: &Entry, value: &str) -> Result<ExecCommand, UnitError> {
    ExecCommand::parse(value).map_err(|source| UnitError::Exec {
        key: entry.key.clone(),
        line: entry.line,
        source,
    })
}

fn boolean(entry: &Entry, value: &str) -> Result<bool, UnitError> {
    match value {
        "1" | "yes" | "true" | "on" => Ok(true),
        "0" | "no" | "false" | "off" => Ok(false),
        _ => Err(UnitError::BadValue {
            key: entry.key.clone(),
            line: entry.line,
            value: value.to_string(),
            expected: "a boolean (yes/no, true/false, on/off, 1/0)",
        }),
    }
}

fn mode(entry: &Entry, value: &str) -> Result<u32, UnitError> {
    u32::from_str_radix(value, 8)
        .ok()
        .filter(|m| *m <= 0o7777)
        .ok_or_else(|| UnitError::BadValue {
            key: entry.key.clone(),
            line: entry.line,
            value: value.to_string(),
            expected: "an octal mode such as 0750",
        })
}

fn directory_base(key: &str) -> Option<BaseDir> {
    BaseDir::ALL.into_iter().find(|b| b.directive() == key)
}

fn directory_mode_base(key: &str) -> Option<BaseDir> {
    BaseDir::ALL.into_iter().find(|b| b.mode_directive() == key)
}

/// Directives this runner applies, in `[Service]`.
const APPLIED: &[&str] = &[
    "Type",
    "User",
    "Group",
    "SupplementaryGroups",
    "DynamicUser",
    "ExecStart",
    "ExecStartPre",
    "Environment",
    "EnvironmentFile",
    "WorkingDirectory",
    "UMask",
    "StateDirectory",
    "CacheDirectory",
    "LogsDirectory",
    "RuntimeDirectory",
    "ConfigurationDirectory",
    "StateDirectoryMode",
    "CacheDirectoryMode",
    "LogsDirectoryMode",
    "RuntimeDirectoryMode",
    "ConfigurationDirectoryMode",
    "RuntimeDirectoryPreserve",
    "LoadCredential",
    "SetCredential",
    "LoadCredentialEncrypted",
    "SetCredentialEncrypted",
    "ImportCredential",
    "AmbientCapabilities",
];

/// Sandboxing directives: parsed, listed, NOT enforced.
const SANDBOX: &[&str] = &[
    "CapabilityBoundingSet",
    "NoNewPrivileges",
    "SecureBits",
    "SystemCallFilter",
    "SystemCallArchitectures",
    "SystemCallErrorNumber",
    "SystemCallLog",
    "RestrictAddressFamilies",
    "RestrictNamespaces",
    "RestrictRealtime",
    "RestrictSUIDSGID",
    "RestrictFileSystems",
    "RestrictNetworkInterfaces",
    "LockPersonality",
    "MemoryDenyWriteExecute",
    "ProtectClock",
    "ProtectControlGroups",
    "ProtectHostname",
    "ProtectKernelLogs",
    "ProtectKernelModules",
    "ProtectKernelTunables",
    "ProtectProc",
    "ProcSubset",
    "DeviceAllow",
    "DevicePolicy",
    "KeyringMode",
    "RemoveIPC",
    "IPAddressAllow",
    "IPAddressDeny",
    "SocketBindAllow",
    "SocketBindDeny",
    "UMask2",
    "PAMName",
    "SELinuxContext",
    "AppArmorProfile",
    "SmackProcessLabel",
];

/// Namespace directives: the service sees the host's filesystem instead.
const NAMESPACE: &[&str] = &[
    "PrivateTmp",
    "PrivateDevices",
    "PrivateNetwork",
    "PrivateUsers",
    "PrivateMounts",
    "PrivateIPC",
    "ProtectHome",
    "ProtectSystem",
    "ReadWritePaths",
    "ReadOnlyPaths",
    "InaccessiblePaths",
    "ExecPaths",
    "NoExecPaths",
    "BindPaths",
    "BindReadOnlyPaths",
    "TemporaryFileSystem",
    "RootDirectory",
    "RootImage",
    "RootImageOptions",
    "MountAPIVFS",
    "MountFlags",
    "ExtensionDirectories",
    "ExtensionImages",
    "NetworkNamespacePath",
    "JoinsNamespaceOf",
];

/// Resource-control directives: cgroup limits and rlimits.
const RESOURCE: &[&str] = &[
    "MemoryMax",
    "MemoryHigh",
    "MemoryLow",
    "MemoryMin",
    "MemorySwapMax",
    "MemoryZSwapMax",
    "MemoryAccounting",
    "CPUAccounting",
    "CPUQuota",
    "CPUWeight",
    "StartupCPUWeight",
    "AllowedCPUs",
    "AllowedMemoryNodes",
    "TasksMax",
    "TasksAccounting",
    "IOAccounting",
    "IOWeight",
    "IOReadBandwidthMax",
    "IOWriteBandwidthMax",
    "LimitCPU",
    "LimitFSIZE",
    "LimitDATA",
    "LimitSTACK",
    "LimitCORE",
    "LimitRSS",
    "LimitNOFILE",
    "LimitAS",
    "LimitNPROC",
    "LimitMEMLOCK",
    "LimitLOCKS",
    "LimitSIGPENDING",
    "LimitMSGQUEUE",
    "LimitNICE",
    "LimitRTPRIO",
    "LimitRTTIME",
    "Slice",
    "Delegate",
    "DeviceAllowAccounting",
    "OOMPolicy",
    "OOMScoreAdjust",
    "ManagedOOMSwap",
    "ManagedOOMMemoryPressure",
];

/// Scheduling directives.
const SCHEDULING: &[&str] = &[
    "Nice",
    "CPUSchedulingPolicy",
    "CPUSchedulingPriority",
    "CPUSchedulingResetOnFork",
    "CPUAffinity",
    "NUMAPolicy",
    "NUMAMask",
    "IOSchedulingClass",
    "IOSchedulingPriority",
    "TimerSlackNSec",
];

/// Directives the kubelet owns: the lifecycle, the logs, the identity of the
/// unit inside systemd.
const SUPERVISOR: &[&str] = &[
    "Restart",
    "RestartSec",
    "RestartSteps",
    "RestartMaxDelaySec",
    "RestartPreventExitStatus",
    "RestartForceExitStatus",
    "SuccessExitStatus",
    "TimeoutSec",
    "TimeoutStartSec",
    "TimeoutStopSec",
    "TimeoutAbortSec",
    "TimeoutStartFailureMode",
    "TimeoutStopFailureMode",
    "RuntimeMaxSec",
    "WatchdogSec",
    "ExecCondition",
    "ExecStartPost",
    "ExecReload",
    "ExecStop",
    "ExecStopPost",
    "RemainAfterExit",
    "GuessMainPID",
    "PIDFile",
    "BusName",
    "NotifyAccess",
    "FileDescriptorStoreMax",
    "KillMode",
    "KillSignal",
    "RestartKillSignal",
    "FinalKillSignal",
    "WatchdogSignal",
    "SendSIGKILL",
    "SendSIGHUP",
    "StandardInput",
    "StandardOutput",
    "StandardError",
    "StandardInputText",
    "StandardInputData",
    "TTYPath",
    "TTYReset",
    "TTYVHangup",
    "TTYVTDisallocate",
    "SyslogIdentifier",
    "SyslogFacility",
    "SyslogLevel",
    "SyslogLevelPrefix",
    "LogLevelMax",
    "LogExtraFields",
    "LogRateLimitIntervalSec",
    "LogRateLimitBurst",
    "LogNamespace",
    "LogFilterPatterns",
    "Sockets",
    "OpenFile",
];

/// `[Unit]` directives that order, condition or document a unit.
const UNIT_INERT: &[&str] = &[
    "After",
    "Before",
    "Wants",
    "WantedBy",
    "Requires",
    "Requisite",
    "BindsTo",
    "PartOf",
    "Upholds",
    "Conflicts",
    "PropagatesReloadTo",
    "ReloadPropagatedFrom",
    "PropagatesStopTo",
    "StopPropagatedFrom",
    "JoinsNamespaceOf",
    "RequiresMountsFor",
    "OnFailure",
    "OnSuccess",
    "OnFailureJobMode",
    "IgnoreOnIsolate",
    "StopWhenUnneeded",
    "RefuseManualStart",
    "RefuseManualStop",
    "AllowIsolate",
    "DefaultDependencies",
    "CollectMode",
    "FailureAction",
    "SuccessAction",
    "JobTimeoutSec",
    "JobRunningTimeoutSec",
    "StartLimitIntervalSec",
    "StartLimitBurst",
    "StartLimitAction",
    "RebootArgument",
    "Documentation",
    "SourcePath",
    "Description",
];

/// Where a directive belongs.
#[must_use]
pub fn classify(section: &str, key: &str) -> Class {
    if key.starts_with("X-") {
        // NixOS's own unit annotations (X-Restart-Triggers, X-StopIfChanged):
        // they steer `nixos-rebuild`, never the service.
        return Class::Inert;
    }
    match section {
        "Install" => Class::Inert,
        "Unit" => {
            if key == "Description" {
                Class::Applied
            } else if UNIT_INERT.contains(&key)
                || key.starts_with("Condition")
                || key.starts_with("Assert")
            {
                Class::Inert
            } else {
                Class::Unknown
            }
        }
        "Service" => {
            if APPLIED.contains(&key) {
                Class::Applied
            } else if SANDBOX.contains(&key) {
                Class::Acknowledged(Ack::Sandbox)
            } else if NAMESPACE.contains(&key) {
                Class::Acknowledged(Ack::Namespace)
            } else if RESOURCE.contains(&key) {
                Class::Acknowledged(Ack::Resource)
            } else if SCHEDULING.contains(&key) {
                Class::Acknowledged(Ack::Scheduling)
            } else if SUPERVISOR.contains(&key) {
                Class::SupervisorOwned
            } else {
                Class::Unknown
            }
        }
        _ => Class::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{HostRoot, Layout};
    use crate::specifier::{AccountFacts, HostFacts};

    fn parse(text: &str) -> Result<ServiceUnit, UnitError> {
        let layout = Layout::new(HostRoot::system());
        let host = HostFacts::default();
        let account = AccountFacts::root();
        let file = UnitFile::parse(Path::new("/etc/systemd/system/t.service"), text)?;
        let ctx = Context {
            unit: &file.name,
            fragment: Some(&file.path),
            account: Some(&account),
            layout: &layout,
            host: &host,
        };
        file.service(&ctx)
    }

    #[test]
    fn every_class_is_recorded_and_nothing_is_dropped() {
        let unit = parse(
            "[Unit]\nDescription=d\nAfter=network.target\n\
             [Service]\nExecStart=/bin/a\nProtectHome=true\nPrivateTmp=true\nMemoryMax=1G\n\
             Nice=5\nRestart=always\nNoSuchDirective=x\n[Install]\nWantedBy=multi-user.target\n",
        )
        .unwrap();
        assert_eq!(unit.description.as_deref(), Some("d"));
        let acknowledged: Vec<_> = unit.acknowledged().iter().map(|n| n.key.clone()).collect();
        assert_eq!(
            acknowledged,
            ["ProtectHome", "PrivateTmp", "MemoryMax", "Nice"]
        );
        assert_eq!(unit.unknown().len(), 1);
        assert_eq!(unit.unknown()[0].key, "NoSuchDirective");
        assert!(
            unit.noted
                .iter()
                .any(|n| n.key == "Restart" && n.class == Class::SupervisorOwned),
            "the restart policy is the kubelet's, and is recorded as such"
        );
        assert!(
            unit.noted
                .iter()
                .any(|n| n.key == "After" && n.class == Class::Inert)
        );
        assert!(
            unit.noted
                .iter()
                .any(|n| n.key == "WantedBy" && n.class == Class::Inert)
        );
    }

    #[test]
    fn lists_accumulate_and_an_empty_assignment_resets_them() {
        let unit = parse(
            "[Service]\nExecStart=/bin/a\n\
             AmbientCapabilities=\nAmbientCapabilities=CAP_NET_ADMIN CAP_NET_RAW\n\
             Environment=A=1\nEnvironment=B=2\n\
             StateDirectory=one\nStateDirectory=two three\n\
             ExecStartPre=/bin/pre1\nExecStartPre=\nExecStartPre=/bin/pre2\n",
        )
        .unwrap();
        assert_eq!(unit.ambient.to_string(), "CAP_NET_ADMIN CAP_NET_RAW");
        assert_eq!(unit.environment.len(), 2);
        assert_eq!(
            unit.directories
                .iter()
                .map(|d| d.path.clone())
                .collect::<Vec<_>>(),
            ["one", "two", "three"]
        );
        assert_eq!(unit.exec_start_pre.len(), 1);
        assert_eq!(unit.exec_start_pre[0].program, "/bin/pre2");
    }

    #[test]
    fn modes_umask_working_directory_and_type() {
        let unit = parse(
            "[Service]\nType=notify\nExecStart=/bin/a\nUMask=0077\n\
             StateDirectory=x\nStateDirectoryMode=0750\nWorkingDirectory=-/var/lib/x\n",
        )
        .unwrap();
        assert_eq!(unit.service_type, ServiceType::Notify);
        assert!(unit.service_type.caveat().is_some());
        assert_eq!(unit.umask, 0o077);
        assert_eq!(unit.directory_mode(BaseDir::State), 0o750);
        assert_eq!(unit.directory_mode(BaseDir::Cache), 0o755);
        let wd = unit.working_directory.unwrap();
        assert!(wd.optional && !wd.home);
        assert_eq!(wd.path.unwrap(), PathBuf::from("/var/lib/x"));
    }

    #[test]
    fn bad_values_are_typed_errors_naming_the_line() {
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nType=magic\n"),
            Err(UnitError::BadValue { line: 3, .. })
        ));
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nStateDirectory=/absolute\n"),
            Err(UnitError::BadValue { line: 3, .. })
        ));
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nAmbientCapabilities=CAP_NONSENSE\n"),
            Err(UnitError::BadValue { line: 3, .. })
        ));
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nUMask=999\n"),
            Err(UnitError::BadValue { line: 3, .. })
        ));
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nLoadCredentialEncrypted=x:/y\n"),
            Err(UnitError::Unsupported { line: 3, .. })
        ));
        assert_eq!(parse("[Service]\nUser=x\n"), Err(UnitError::NoExecStart));
        assert_eq!(
            parse("[Unit]\nDescription=d\n"),
            Err(UnitError::NoServiceSection)
        );
        assert!(matches!(
            parse("[Service]\nExecStart=/bin/a\nExecStart=/bin/b\n"),
            Err(UnitError::Unsupported { .. })
        ));
    }

    #[test]
    fn identity_is_read_before_anything_else_expands() {
        let unit = parse(
            "[Service]\nUser=hass\nGroup=hass\nSupplementaryGroups=dialout render\n\
             DynamicUser=true\nExecStart=/bin/a\n",
        )
        .unwrap();
        assert_eq!(unit.identity.user.as_deref(), Some("hass"));
        assert_eq!(unit.identity.group.as_deref(), Some("hass"));
        assert_eq!(unit.identity.supplementary, ["dialout", "render"]);
        assert!(unit.identity.dynamic);
    }
}
