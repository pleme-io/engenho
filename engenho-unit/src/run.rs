//! The runner: plan a unit, prepare what it needs, run its `ExecStartPre=`
//! steps, and `exec` its `ExecStart=`.
//!
//! Split into a PLAN (pure: reads the unit, the account databases and the
//! host facts, decides everything) and the acts that follow it (creating
//! directories, installing credentials, running commands). The plan is what
//! `engenho unit-run --check` prints, and what the tests assert on: a bug
//! that would run the wrong thing is visible before anything is run.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;

use crate::accounts::{AccountError, Database};
use crate::credentials::{self, CredentialError};
use crate::directories::{self, DirectoryError, PlannedDirectory};
use crate::env::{self, EnvError, EnvironmentBuilder};
use crate::exec::{ExecCommand, Privilege};
pub use crate::identity::Overrides;
use crate::identity::{Identity, IdentityError};
use crate::layout::Layout;
use crate::privilege::{self, PrivilegeError};
use crate::specifier::{AccountFacts, Context, HostFacts};
use crate::unit::{Class, ServiceUnit, UnitError, UnitFile};

/// Why a unit could not be planned or run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitRunError {
    /// The unit file itself.
    Unit(UnitError),
    /// `/etc/passwd` or `/etc/group`.
    Accounts(AccountError),
    /// Who to run as.
    Identity(IdentityError),
    /// A `*Directory=`.
    Directory(DirectoryError),
    /// A credential.
    Credential(CredentialError),
    /// The environment.
    Env(EnvError),
    /// The privilege drop.
    Privilege(PrivilegeError),
    /// `WorkingDirectory=` does not exist (and carried no `-`).
    WorkingDirectory {
        /// The directory, as the unit names it.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// A command could not be spawned.
    Spawn {
        /// The program.
        program: String,
        /// What the OS said.
        detail: String,
    },
    /// An `ExecStartPre=` exited non-zero and did not carry `-`.
    StepFailed {
        /// Its 1-based position in the unit.
        step: usize,
        /// The program.
        program: String,
        /// Its exit code, when it exited rather than died on a signal.
        code: Option<i32>,
    },
    /// The thread running a dropped `ExecStartPre=` panicked.
    StepPanicked {
        /// Its 1-based position in the unit.
        step: usize,
    },
    /// `execve` of `ExecStart=` failed — the only way this function returns.
    Exec {
        /// The program.
        program: String,
        /// What the OS said.
        detail: String,
    },
}

impl fmt::Display for UnitRunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unit(e) => write!(f, "{e}"),
            Self::Accounts(e) => write!(f, "{e}"),
            Self::Identity(e) => write!(f, "{e}"),
            Self::Directory(e) => write!(f, "{e}"),
            Self::Credential(e) => write!(f, "{e}"),
            Self::Env(e) => write!(f, "{e}"),
            Self::Privilege(e) => write!(f, "{e}"),
            Self::WorkingDirectory { path, detail } => write!(
                f,
                "WorkingDirectory={} is not usable: {detail}",
                path.display()
            ),
            Self::Spawn { program, detail } => write!(f, "{program} could not be run: {detail}"),
            Self::StepFailed {
                step,
                program,
                code,
            } => {
                write!(f, "ExecStartPre #{step} ({program}) failed")?;
                match code {
                    Some(code) => write!(f, " with status {code}"),
                    None => f.write_str(" on a signal"),
                }
            }
            Self::StepPanicked { step } => {
                write!(f, "the thread running ExecStartPre #{step} panicked")
            }
            Self::Exec { program, detail } => {
                write!(f, "exec of {program} failed: {detail}")
            }
        }
    }
}

impl std::error::Error for UnitRunError {}

macro_rules! from_error {
    ($($from:ty => $variant:ident),* $(,)?) => {
        $(impl From<$from> for UnitRunError {
            fn from(e: $from) -> Self {
                Self::$variant(e)
            }
        })*
    };
}

from_error! {
    UnitError => Unit,
    AccountError => Accounts,
    IdentityError => Identity,
    DirectoryError => Directory,
    CredentialError => Credential,
    EnvError => Env,
    PrivilegeError => Privilege,
}

impl UnitRunError {
    /// The process exit code this error ends `engenho unit-run` with.
    ///
    /// `78` (`EX_CONFIG`) for anything the unit or the node's declaration got
    /// wrong, `71` (`EX_OSERR`) for a refused syscall, and a failed
    /// `ExecStartPre=`'s own status for that step — so a supervisor reading
    /// the code can tell "this unit can never work here" from "the host said
    /// no this time".
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::Unit(_) | Self::Accounts(_) | Self::Identity(_) | Self::Env(_) => 78,
            Self::Directory(_)
            | Self::Credential(_)
            | Self::Privilege(_)
            | Self::WorkingDirectory { .. }
            | Self::Spawn { .. }
            | Self::Exec { .. }
            | Self::StepPanicked { .. } => 71,
            Self::StepFailed { code, .. } => match code {
                Some(code) => *code,
                None => 1,
            },
        }
    }
}

/// Everything the runner decided, before it does anything.
#[derive(Debug, Clone)]
pub struct Plan {
    /// The typed unit.
    pub unit: ServiceUnit,
    /// Who it runs as.
    pub identity: Identity,
    /// Where it acts.
    pub layout: Layout,
    /// The directories to create.
    pub directories: Vec<PlannedDirectory>,
    /// `$CREDENTIALS_DIRECTORY`, when the unit declares credentials.
    pub credentials_dir: Option<PathBuf>,
    /// The working directory the commands run in, as the service sees it.
    pub working_directory: Option<PathBuf>,
}

/// Read `path` and decide everything, touching nothing.
///
/// # Errors
///
/// A [`UnitRunError`] from the unit file, the account databases or the
/// identity resolution.
pub fn plan(path: &Path, overrides: &Overrides, layout: Layout) -> Result<Plan, UnitRunError> {
    let file = UnitFile::read(path)?;
    let host = HostFacts::read(&layout);
    let without_account = Context {
        unit: &file.name,
        fragment: Some(&file.path),
        account: None,
        layout: &layout,
        host: &host,
    };
    let request = file.identity_request(&without_account)?;
    let db = Database::read(&layout.passwd(), &layout.group())?;
    let identity = crate::identity::resolve(&request, overrides, &db)?;

    let facts: AccountFacts = identity.facts();
    let ctx = Context {
        account: Some(&facts),
        ..without_account
    };
    let unit = file.service(&ctx)?;

    let owner = identity.account().map(|a| (a.uid, a.gid));
    let directories = directories::plan(&unit, &layout, owner);
    let credentials_dir =
        (!unit.credentials.is_empty()).then(|| layout.credentials_dir(&unit.name));
    // `WorkingDirectory=` and the home it may name come from the unit and the
    // account database, so they are rooted here, once.
    let working_directory = unit.working_directory.as_ref().map(|wd| {
        let written = if wd.home {
            PathBuf::from(&facts.home)
        } else {
            wd.path.clone().unwrap_or_else(|| PathBuf::from("/"))
        };
        layout.on_disk(&written)
    });

    Ok(Plan {
        unit,
        identity,
        layout,
        directories,
        credentials_dir,
        working_directory,
    })
}

impl Plan {
    /// Say once, at start, what the unit asked for that is NOT in force —
    /// every sandboxing, namespace, resource and scheduling directive, the
    /// ones the kubelet owns, the unknown ones, and the `Type=` caveat.
    ///
    /// Silence here would be the failure this runner is built to avoid: a
    /// service that looks started and is missing half its unit.
    pub fn log_what_is_not_enforced(&self) {
        let mut acknowledged: Vec<&str> = self
            .unit
            .acknowledged()
            .iter()
            .map(|n| n.key.as_str())
            .collect();
        acknowledged.dedup();
        if !acknowledged.is_empty() {
            tracing::warn!(
                unit = %self.unit.name,
                count = acknowledged.len(),
                directives = %Names(&acknowledged),
                "these sandboxing/resource directives are PARSED BUT NOT ENFORCED: the service \
                 runs with the host's filesystem and network view"
            );
        }
        let supervisor: Vec<&str> = self
            .unit
            .noted
            .iter()
            .filter(|n| n.class == Class::SupervisorOwned)
            .map(|n| n.key.as_str())
            .collect();
        if !supervisor.is_empty() {
            tracing::info!(
                unit = %self.unit.name,
                directives = %Names(&supervisor),
                "these directives belong to the pod's lifecycle, not to unit-run"
            );
        }
        for unknown in self.unit.unknown() {
            tracing::warn!(unit = %self.unit.name, directive = %unknown, "unknown directive, ignored");
        }
        if let Some(caveat) = self.unit.service_type.caveat() {
            tracing::warn!(unit = %self.unit.name, "{caveat}");
        }
    }

    /// Create the directories, install the credentials, set the umask.
    ///
    /// # Errors
    ///
    /// A [`UnitRunError`] naming the directory or credential that failed.
    pub fn prepare(&self) -> Result<(), UnitRunError> {
        directories::create(&self.directories)?;
        if self.credentials_dir.is_some() {
            credentials::install(
                &self.unit.credentials,
                &self.layout,
                &self.unit.name,
                self.identity.account().map(|a| (a.uid, a.gid)),
            )?;
        }
        // The previous umask is not restored: this process is about to
        // become the service.
        let _previous = privilege::set_umask(self.unit.umask);
        Ok(())
    }

    /// The environment the next command runs with.
    ///
    /// Built afresh per command, because an `ExecStartPre=` is allowed to
    /// write the `EnvironmentFile=` its `ExecStart=` reads.
    ///
    /// # Errors
    ///
    /// [`UnitRunError::Env`] for a required environment file that cannot be
    /// read.
    pub fn environment(&self) -> Result<BTreeMap<String, String>, UnitRunError> {
        let mut builder = EnvironmentBuilder::with_default_path();
        let facts = self.identity.facts();
        if self.identity.account().is_some() {
            builder.set("USER", &facts.user);
            builder.set("LOGNAME", &facts.user);
            builder.set("HOME", &facts.home);
            builder.set("SHELL", &facts.shell);
        }
        builder.extend(directories::environment(&self.directories));
        if let Some(dir) = &self.credentials_dir {
            builder.set("CREDENTIALS_DIRECTORY", dir.to_string_lossy().into_owned());
        }
        builder.extend(self.unit.environment.iter().cloned());
        for file in &self.unit.environment_files {
            let on_disk = self.layout.on_disk(&file.path);
            builder.extend(env::read_file(file, &on_disk)?);
        }
        Ok(builder.build())
    }

    /// Run every `ExecStartPre=`, in order, with its prefix semantics.
    ///
    /// # Errors
    ///
    /// A [`UnitRunError`] for the first step that fails without `-`.
    pub fn run_pre_steps(&self) -> Result<(), UnitRunError> {
        let steps = self.unit.exec_start_pre.clone();
        for (index, command) in steps.iter().enumerate() {
            self.run_step(command, index + 1)?;
        }
        // `Type=oneshot` may declare several ExecStart= lines; all but the
        // last run like a pre step, and the last is the process we become.
        let extra = self.unit.exec_start.len().saturating_sub(1);
        for (index, command) in self.unit.exec_start.iter().take(extra).enumerate() {
            self.run_step(command, self.unit.exec_start_pre.len() + index + 1)?;
        }
        Ok(())
    }

    fn run_step(&self, command: &ExecCommand, step: usize) -> Result<(), UnitRunError> {
        let env = self.environment()?;
        let mut built = self.command(command, &env)?;
        let drop_to = match (command.privilege, self.identity.account()) {
            (Privilege::Drop, Some(account)) if privilege::needs_drop(account) => Some(account),
            _ => None,
        };
        let status = match drop_to {
            None => built.status().map_err(|e| UnitRunError::Spawn {
                program: command.program.clone(),
                detail: e.to_string(),
            })?,
            Some(account) => {
                // The drop is thread-scoped, so it happens on a thread of its
                // own: the child inherits the dropped credentials, and this
                // runner keeps the privileges it still needs.
                let ambient = &self.unit.ambient;
                let program = command.program.clone();
                let mut built = built;
                let joined = std::thread::scope(|scope| {
                    scope
                        .spawn(move || -> Result<ExitStatus, UnitRunError> {
                            privilege::drop_to(account, ambient)?;
                            built.status().map_err(|e| UnitRunError::Spawn {
                                program,
                                detail: e.to_string(),
                            })
                        })
                        .join()
                });
                joined.map_err(|_| UnitRunError::StepPanicked { step })??
            }
        };
        if status.success() || command.ignore_failure {
            Ok(())
        } else {
            Err(UnitRunError::StepFailed {
                step,
                program: command.program.clone(),
                code: status.code(),
            })
        }
    }

    /// Become the service: drop privileges and `execve` `ExecStart=`.
    ///
    /// Returns ONLY when the exec failed — on success this process IS the
    /// service.
    ///
    /// # Errors
    ///
    /// A [`UnitRunError`] from the drop or the exec.
    #[must_use]
    pub fn exec_main(&self) -> UnitRunError {
        let Some(command) = self.unit.exec_start.last() else {
            return UnitRunError::Unit(UnitError::NoExecStart);
        };
        let env = match self.environment() {
            Ok(env) => env,
            Err(e) => return e,
        };
        let mut built = match self.command(command, &env) {
            Ok(built) => built,
            Err(e) => return e,
        };
        if command.privilege == Privilege::Drop
            && let Some(account) = self.identity.account()
            && privilege::needs_drop(account)
            && let Err(e) = privilege::drop_to(account, &self.unit.ambient)
        {
            return UnitRunError::Privilege(e);
        }
        // `exec` replaces this process; it returns only its error.
        let failure = std::os::unix::process::CommandExt::exec(&mut built);
        UnitRunError::Exec {
            program: command.program.clone(),
            detail: failure.to_string(),
        }
    }

    /// Prepare, run the pre steps, and become the service.
    ///
    /// Returns only on failure.
    #[must_use]
    pub fn run(&self) -> UnitRunError {
        self.log_what_is_not_enforced();
        if let Err(e) = self.prepare() {
            return e;
        }
        if let Err(e) = self.run_pre_steps() {
            return e;
        }
        self.exec_main()
    }

    /// One command, built but not spawned: argv (substituted), environment,
    /// working directory.
    ///
    /// # Errors
    ///
    /// [`UnitRunError::WorkingDirectory`] for a missing required working
    /// directory.
    pub fn command(
        &self,
        command: &ExecCommand,
        env: &BTreeMap<String, String>,
    ) -> Result<std::process::Command, UnitRunError> {
        use std::os::unix::process::CommandExt;

        let argv = command.resolved_argv(env);
        // The PROGRAM is not rooted: a test root holds the service's state,
        // never the binaries, which live in the Nix store on the real host.
        let mut built = std::process::Command::new(&command.program);
        if let Some(argv0) = argv.first() {
            built.arg0(argv0);
        }
        built.args(argv.iter().skip(1));
        built.env_clear();
        built.envs(env);
        if let Some(working) = &self.working_directory {
            let optional = self
                .unit
                .working_directory
                .as_ref()
                .is_some_and(|wd| wd.optional);
            match std::fs::metadata(working) {
                Ok(meta) if meta.is_dir() => {
                    built.current_dir(working);
                }
                Ok(_) => {
                    if !optional {
                        return Err(UnitRunError::WorkingDirectory {
                            path: working.clone(),
                            detail: "not a directory".into(),
                        });
                    }
                }
                Err(e) => {
                    if !optional {
                        return Err(UnitRunError::WorkingDirectory {
                            path: working.clone(),
                            detail: e.to_string(),
                        });
                    }
                }
            }
        } else {
            // systemd's default for a system service.
            built.current_dir(self.layout.on_disk(Path::new("/")));
        }
        Ok(built)
    }

    /// What `--check` prints: everything decided, nothing done.
    #[must_use]
    pub const fn report(&self) -> Report<'_> {
        Report(self)
    }
}

/// A space-separated directive list, rendered once.
struct Names<'a>(&'a [&'a str]);

impl fmt::Display for Names<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, name) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            f.write_str(name)?;
        }
        Ok(())
    }
}

/// The rendering of a [`Plan`].
pub struct Report<'a>(&'a Plan);

impl fmt::Display for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plan = self.0;
        writeln!(f, "unit: {}", plan.unit.name)?;
        if let Some(description) = &plan.unit.description {
            writeln!(f, "description: {description}")?;
        }
        writeln!(f, "type: {:?}", plan.unit.service_type)?;
        match plan.identity.account() {
            Some(account) => {
                writeln!(
                    f,
                    "runs as: {} ({}) group {} ({}) groups {:?}",
                    account.name, account.uid, account.group, account.gid, account.groups
                )?;
            }
            None => writeln!(f, "runs as: the daemon's own user (no User=)")?,
        }
        writeln!(f, "umask: {:04o}", plan.unit.umask)?;
        if let Some(working) = &plan.working_directory {
            writeln!(f, "working directory: {}", working.display())?;
        }
        for directory in &plan.directories {
            writeln!(
                f,
                "{}: {} mode {:04o}{}",
                directory.base.directive(),
                directory.path.display(),
                directory.mode,
                if directory.recreate {
                    " (recreated)"
                } else {
                    ""
                }
            )?;
        }
        if let Some(dir) = &plan.credentials_dir {
            writeln!(f, "credentials: {} holds", dir.display())?;
            for credential in &plan.unit.credentials {
                writeln!(f, "  {}", credential.id)?;
            }
        }
        if !plan.unit.ambient.is_empty() {
            writeln!(f, "ambient capabilities: {}", plan.unit.ambient)?;
        }
        for (index, step) in plan.unit.exec_start_pre.iter().enumerate() {
            writeln!(
                f,
                "ExecStartPre #{}: {} argv {:?}{}{}",
                index + 1,
                step.program,
                step.argv,
                if step.privilege == Privilege::Full {
                    " (full privileges)"
                } else {
                    ""
                },
                if step.ignore_failure {
                    " (failure ignored)"
                } else {
                    ""
                }
            )?;
        }
        if let Some(main) = plan.unit.exec_start.last() {
            writeln!(f, "ExecStart: {} argv {:?}", main.program, main.argv)?;
        }
        let acknowledged: Vec<&str> = plan
            .unit
            .acknowledged()
            .iter()
            .map(|n| n.key.as_str())
            .collect();
        if !acknowledged.is_empty() {
            writeln!(f, "not enforced: {}", Names(&acknowledged))?;
        }
        let unknown: Vec<&str> = plan.unit.unknown().iter().map(|n| n.key.as_str()).collect();
        if !unknown.is_empty() {
            writeln!(f, "unknown directives: {}", Names(&unknown))?;
        }
        if let Some(caveat) = plan.unit.service_type.caveat() {
            writeln!(f, "caveat: {caveat}")?;
        }
        Ok(())
    }
}
