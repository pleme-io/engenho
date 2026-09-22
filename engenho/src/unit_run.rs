//! `engenho unit-run` — run a rendered systemd `.service` file as this pod's
//! workload.
//!
//! ## Why the binary has this verb
//!
//! nixpkgs already knows how to run Home Assistant, mosquitto, zigbee2mqtt,
//! zwave-js and the rest: each NixOS module renders a unit that names the
//! user, the directories, the environment, the credentials and the pre-start
//! steps. engenho's native backend runs a pod as a host process from a
//! realised Nix closure — `command` and `env`, nothing else. `unit-run` is
//! the bridge: the pod's command becomes
//!
//! ```text
//! engenho unit-run --unit ${config.systemd.units."x.service".unit}/x.service
//! ```
//!
//! and everything the unit asks for happens before the service is `exec`ed.
//! The whole of it lives in `engenho-unit`; this module is the argv border.
//!
//! ## Why it runs before the tokio runtime exists
//!
//! The privilege drop is a THREAD-scoped syscall
//! (`engenho_unit::privilege`), and the last thing this verb does is
//! `execve`. Both are cleanest in a single-threaded process, so `main`
//! dispatches `unit-run` before it builds a runtime — this verb never
//! returns on success, so nothing is lost by doing so.

use std::fmt;
use std::path::PathBuf;

use engenho_unit::layout::{HostRoot, Layout};
use engenho_unit::{Overrides, UnitRunError, plan};

/// The verb's arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    /// `--unit <path>`: the rendered unit file.
    pub unit: PathBuf,
    /// `--user <name>`: override `User=`. REQUIRED for a `DynamicUser=yes`
    /// unit, which is otherwise refused.
    pub user: Option<String>,
    /// `--group <name>`: override `Group=`.
    pub group: Option<String>,
    /// `--check`: print the plan and exit, touching nothing.
    pub check: bool,
    /// `--root <dir>`: act against a filesystem root other than `/`. Every
    /// directory, credential, environment file and account lookup the RUNNER
    /// performs moves under it, which is what makes a dry run on a
    /// workstation possible.
    ///
    /// It does not rewrite paths written inside the unit's own command line
    /// (`--config /run/zwave-js/config.json` stays exactly that), and it
    /// never moves the programs, which live in the Nix store either way. A
    /// dry run under `--root` therefore shows a service whose own arguments
    /// still name the host's paths: that is the truth of the unit, not a bug
    /// in the run.
    pub root: Option<PathBuf>,
}

/// Why the arguments were not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgsError {
    /// No `--unit`.
    MissingUnit,
    /// A flag with no value after it.
    MissingValue {
        /// The flag.
        flag: String,
    },
    /// An argument that is not a flag of this verb.
    Unknown {
        /// The argument.
        argument: String,
    },
}

impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUnit => {
                f.write_str("unit-run needs --unit <path-to-a-rendered-.service-file>")
            }
            Self::MissingValue { flag } => write!(f, "{flag} needs a value"),
            Self::Unknown { argument } => write!(
                f,
                "unit-run does not take {argument:?} (it takes --unit <path> \
                 [--user <name>] [--group <name>] [--root <dir>] [--check])"
            ),
        }
    }
}

impl std::error::Error for ArgsError {}

impl Args {
    /// Classify the verb's argv.
    ///
    /// # Errors
    ///
    /// An [`ArgsError`] for a missing `--unit`, a flag with no value, or an
    /// argument this verb does not take.
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, ArgsError> {
        let mut unit = None;
        let mut user = None;
        let mut group = None;
        let mut root = None;
        let mut check = false;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            let mut value = |flag: &str| {
                args.next().ok_or_else(|| ArgsError::MissingValue {
                    flag: flag.to_string(),
                })
            };
            match argument.as_str() {
                "--unit" => unit = Some(PathBuf::from(value("--unit")?)),
                "--user" => user = Some(value("--user")?),
                "--group" => group = Some(value("--group")?),
                "--root" => root = Some(PathBuf::from(value("--root")?)),
                "--check" => check = true,
                _ => return Err(ArgsError::Unknown { argument }),
            }
        }
        Ok(Self {
            unit: unit.ok_or(ArgsError::MissingUnit)?,
            user,
            group,
            check,
            root,
        })
    }

    /// The overrides the unit is resolved with.
    #[must_use]
    pub fn overrides(&self) -> Overrides {
        Overrides {
            user: self.user.clone(),
            group: self.group.clone(),
        }
    }

    /// The filesystem the runner acts on.
    #[must_use]
    pub fn layout(&self) -> Layout {
        Layout::new(match &self.root {
            Some(root) => HostRoot::at(root.clone()),
            None => HostRoot::system(),
        })
    }
}

/// Run the verb. Returns the process exit code; on a successful start it does
/// not return at all, because the process has become the service.
#[must_use]
pub fn run(args: Vec<String>) -> i32 {
    let args = match Args::parse(args) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("engenho unit-run: {e}");
            // EX_USAGE: the command line, not the unit.
            return 64;
        }
    };
    init_tracing();
    let planned = match plan(&args.unit, &args.overrides(), args.layout()) {
        Ok(planned) => planned,
        Err(e) => return report(&e),
    };
    if args.check {
        print!("{}", planned.report());
        return 0;
    }
    // `run` returns only when something failed: on success this process has
    // been replaced by the service.
    report(&planned.run())
}

fn report(error: &UnitRunError) -> i32 {
    tracing::error!(error = %error, "unit-run failed");
    eprintln!("engenho unit-run: {error}");
    error.exit_code()
}

/// Logs to stderr, so a pod's log carries what the runner did before the
/// service took over the process.
fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> Result<Args, ArgsError> {
        Args::parse(argv.iter().map(|a| (*a).to_string()))
    }

    #[test]
    fn the_contract_the_nix_side_calls() {
        let args = parse(&[
            "--unit",
            "/nix/store/x-unit-zwave-js.service/zwave-js.service",
        ])
        .unwrap();
        assert_eq!(
            args.unit,
            PathBuf::from("/nix/store/x-unit-zwave-js.service/zwave-js.service")
        );
        assert_eq!(args.overrides(), Overrides::default());
        assert!(!args.check);
    }

    #[test]
    fn the_overrides_a_dynamic_user_unit_needs() {
        let args = parse(&[
            "--unit",
            "/x/zwave-js.service",
            "--user",
            "zwave-js",
            "--group",
            "zwave-js",
        ])
        .unwrap();
        assert_eq!(args.user.as_deref(), Some("zwave-js"));
        assert_eq!(args.group.as_deref(), Some("zwave-js"));
        assert_eq!(
            args.overrides(),
            Overrides {
                user: Some("zwave-js".into()),
                group: Some("zwave-js".into())
            }
        );
    }

    #[test]
    fn a_missing_unit_a_dangling_flag_and_a_stray_argument_are_typed_errors() {
        assert_eq!(parse(&["--user", "x"]), Err(ArgsError::MissingUnit));
        assert_eq!(
            parse(&["--unit"]),
            Err(ArgsError::MissingValue {
                flag: "--unit".into()
            })
        );
        assert_eq!(
            parse(&["--unit", "/x/a.service", "--sandbox"]),
            Err(ArgsError::Unknown {
                argument: "--sandbox".into()
            })
        );
    }

    #[test]
    fn a_root_makes_the_layout_relative_to_it() {
        let args = parse(&["--unit", "/x/a.service", "--root", "/tmp/r", "--check"]).unwrap();
        assert!(args.check);
        assert_eq!(args.layout().root().path(), std::path::Path::new("/tmp/r"));
        let default = parse(&["--unit", "/x/a.service"]).unwrap();
        assert_eq!(default.layout().root().path(), std::path::Path::new("/"));
    }
}
