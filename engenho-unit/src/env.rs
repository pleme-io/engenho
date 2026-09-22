//! The service's environment: `Environment=`, `EnvironmentFile=`, and what
//! systemd exports on its own.
//!
//! Precedence, lowest first — systemd's `build_environment` order:
//!
//! 1. the manager's own `PATH` (NixOS units always set their own, so this is
//!    only a floor);
//! 2. what systemd exports: `HOME`, `LOGNAME`, `USER`, `SHELL` for a unit
//!    with `User=`, the `*_DIRECTORY` variables, `CREDENTIALS_DIRECTORY`;
//! 3. `Environment=` assignments, in file order;
//! 4. `EnvironmentFile=` contents, in file order — these override
//!    `Environment=`, as systemd documents.
//!
//! The files are read again before EVERY command, because an
//! `ExecStartPre=` is allowed to write the file its `ExecStart=` reads.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::exec::is_env_name;
use crate::words::{self, WordError};

/// The `PATH` a service gets when nothing sets one: systemd's compiled-in
/// default for the system manager.
pub const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// One `EnvironmentFile=` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentFile {
    /// The path, as the unit names it (absolute, host-visible).
    pub path: PathBuf,
    /// Whether a missing file is ignored — the `-` prefix.
    pub optional: bool,
}

/// Why the environment could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvError {
    /// An `Environment=` value did not split.
    Words(WordError),
    /// A word that is not `NAME=value`, or whose name is not a valid
    /// environment-variable name.
    BadAssignment {
        /// The offending word.
        word: String,
    },
    /// A required `EnvironmentFile=` could not be read.
    FileUnreadable {
        /// The file, as the unit names it.
        path: PathBuf,
        /// What the OS said.
        detail: String,
    },
    /// An `EnvironmentFile=` line that is not `NAME=value`.
    BadFileLine {
        /// The file.
        path: PathBuf,
        /// The 1-based line.
        line: usize,
    },
    /// An `EnvironmentFile=` path that is not absolute.
    RelativeFile {
        /// The path as written.
        path: PathBuf,
    },
}

impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Words(e) => write!(f, "Environment=: {e}"),
            Self::BadAssignment { word } => write!(
                f,
                "Environment={word:?} is not a NAME=value assignment with a valid name"
            ),
            Self::FileUnreadable { path, detail } => write!(
                f,
                "EnvironmentFile={} cannot be read: {detail} (write EnvironmentFile=-{} to make it optional)",
                path.display(),
                path.display()
            ),
            Self::BadFileLine { path, line } => {
                write!(f, "{}: line {line} is not NAME=value", path.display())
            }
            Self::RelativeFile { path } => write!(
                f,
                "EnvironmentFile={} must be an absolute path",
                path.display()
            ),
        }
    }
}

impl std::error::Error for EnvError {}

impl From<WordError> for EnvError {
    fn from(e: WordError) -> Self {
        Self::Words(e)
    }
}

/// Parse one `Environment=` value into assignments, in order.
///
/// A value holds any number of `NAME=value` words, each of which may be
/// quoted — NixOS writes one quoted assignment per line
/// (`Environment="PATH=/nix/store/…"`), but `Environment=A=1 B=2` is equally
/// valid and both arrive here as words.
///
/// # Errors
///
/// [`EnvError::Words`] or [`EnvError::BadAssignment`].
pub fn parse_assignments(value: &str) -> Result<Vec<(String, String)>, EnvError> {
    let mut out = Vec::new();
    for word in words::split(value)? {
        let (name, assigned) = word
            .split_once('=')
            .ok_or_else(|| EnvError::BadAssignment { word: word.clone() })?;
        if !is_env_name(name) {
            return Err(EnvError::BadAssignment { word: word.clone() });
        }
        out.push((name.to_string(), assigned.to_string()));
    }
    Ok(out)
}

/// Read an `EnvironmentFile=`, in systemd's shell-like dialect.
///
/// * `#` and `;` at the start of a line are comments;
/// * `NAME=value`, with whitespace around the name ignored;
/// * the value may be `'single'` (literal), `"double"` (backslash escapes
///   `"`, `\`, `` ` ``, `$` and a newline) or bare (backslash escapes the
///   next character, and trailing whitespace is dropped);
/// * a backslash at end of line continues onto the next.
///
/// # Errors
///
/// [`EnvError::FileUnreadable`] for a required file that is missing, or
/// [`EnvError::BadFileLine`].
pub fn read_file(
    file: &EnvironmentFile,
    on_disk: &Path,
) -> Result<Vec<(String, String)>, EnvError> {
    if !file.path.is_absolute() {
        return Err(EnvError::RelativeFile {
            path: file.path.clone(),
        });
    }
    let text = match std::fs::read_to_string(on_disk) {
        Ok(text) => text,
        Err(e) if file.optional && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(e) => {
            return Err(EnvError::FileUnreadable {
                path: file.path.clone(),
                detail: e.to_string(),
            });
        }
    };
    parse_file(&text, &file.path)
}

/// Parse `EnvironmentFile=` text. See [`read_file`].
///
/// # Errors
///
/// [`EnvError::BadFileLine`] naming the first line that is not `NAME=value`.
pub fn parse_file(text: &str, path: &Path) -> Result<Vec<(String, String)>, EnvError> {
    let mut out = Vec::new();
    let mut lines = text.lines().enumerate().map(|(i, l)| (i + 1, l));
    while let Some((number, first)) = lines.next() {
        let trimmed = first.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        let mut logical = trimmed.to_string();
        while logical.ends_with('\\') {
            logical.pop();
            match lines.next() {
                Some((_, next)) => {
                    logical.push('\n');
                    logical.push_str(next.trim_end());
                }
                None => break,
            }
        }
        let bad = || EnvError::BadFileLine {
            path: path.to_path_buf(),
            line: number,
        };
        let (name, value) = logical.split_once('=').ok_or_else(bad)?;
        let name = name.trim().trim_start_matches("export ").trim();
        if !is_env_name(name) {
            return Err(bad());
        }
        out.push((name.to_string(), unquote_value(value.trim_start())));
    }
    Ok(out)
}

/// The shell-ish unquoting of one `EnvironmentFile=` value.
fn unquote_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    // Trailing whitespace is dropped only when it is UNQUOTED: `A='x '` keeps
    // its space, `A=x  ` does not.
    let mut ended_quoted = false;
    while let Some(c) = chars.next() {
        ended_quoted = matches!(c, '\'' | '"');
        match c {
            '\'' => {
                for c in chars.by_ref() {
                    if c == '\'' {
                        break;
                    }
                    out.push(c);
                }
            }
            '"' => {
                while let Some(c) = chars.next() {
                    match c {
                        '"' => break,
                        '\\' => match chars.next() {
                            Some('\n') => {}
                            Some(escaped @ ('"' | '\\' | '`' | '$')) => out.push(escaped),
                            Some(other) => {
                                out.push('\\');
                                out.push(other);
                            }
                            None => out.push('\\'),
                        },
                        other => out.push(other),
                    }
                }
            }
            '\\' => match chars.next() {
                Some('\n') => {}
                Some(escaped) => out.push(escaped),
                None => out.push('\\'),
            },
            other => out.push(other),
        }
    }
    if ended_quoted {
        out
    } else {
        out.trim_end().to_string()
    }
}

/// An environment being assembled, lowest precedence first.
#[derive(Debug, Clone, Default)]
pub struct EnvironmentBuilder(BTreeMap<String, String>);

impl EnvironmentBuilder {
    /// Start from systemd's floor: just `PATH`.
    #[must_use]
    pub fn with_default_path() -> Self {
        let mut map = BTreeMap::new();
        map.insert("PATH".to_string(), DEFAULT_PATH.to_string());
        Self(map)
    }

    /// Set one variable, overriding what is there.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    /// Apply assignments in order; later wins.
    pub fn extend(&mut self, assignments: impl IntoIterator<Item = (String, String)>) {
        for (name, value) in assignments {
            self.0.insert(name, value);
        }
    }

    /// The finished environment.
    #[must_use]
    pub fn build(self) -> BTreeMap<String, String> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nixpkgs_writes_one_quoted_assignment_per_line() {
        let got = parse_assignments(r#""PATH=/nix/store/a/bin:/nix/store/b/bin""#).unwrap();
        assert_eq!(
            got,
            [(
                "PATH".to_string(),
                "/nix/store/a/bin:/nix/store/b/bin".to_string()
            )]
        );
        assert_eq!(
            parse_assignments("NSNCD_HANDOFF_TIMEOUT=10").unwrap(),
            [("NSNCD_HANDOFF_TIMEOUT".to_string(), "10".to_string())]
        );
    }

    #[test]
    fn one_line_may_carry_several_assignments_quoted_or_not() {
        let got = parse_assignments(r#"A=1 "B=two words" C="x""#).unwrap();
        assert_eq!(
            got,
            [
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "two words".to_string()),
                ("C".to_string(), "x".to_string()),
            ]
        );
    }

    #[test]
    fn a_word_that_is_not_an_assignment_is_refused() {
        assert_eq!(
            parse_assignments("NOTANASSIGNMENT"),
            Err(EnvError::BadAssignment {
                word: "NOTANASSIGNMENT".into()
            })
        );
        assert_eq!(
            parse_assignments("1BAD=x"),
            Err(EnvError::BadAssignment {
                word: "1BAD=x".into()
            })
        );
    }

    #[test]
    fn environment_files_parse_like_a_shell_fragment() {
        let text = "# comment\n\
                    ; comment\n\
                    \n\
                    A=1\n\
                    B = two words  \n\
                    C='keep  $these \\ '\n\
                    D=\"quoted \\\"x\\\" $y\"\n\
                    E=a\\ b\n\
                    export F=g\n";
        let got = parse_file(text, Path::new("/etc/x.env")).unwrap();
        assert_eq!(
            got,
            [
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "two words".to_string()),
                ("C".to_string(), "keep  $these \\ ".to_string()),
                ("D".to_string(), "quoted \"x\" $y".to_string()),
                ("E".to_string(), "a b".to_string()),
                ("F".to_string(), "g".to_string()),
            ]
        );
    }

    #[test]
    fn a_missing_file_is_fatal_unless_the_dash_prefix_made_it_optional() {
        let missing = Path::new("/nonexistent/engenho-unit-test.env");
        let required = EnvironmentFile {
            path: missing.to_path_buf(),
            optional: false,
        };
        assert!(matches!(
            read_file(&required, missing),
            Err(EnvError::FileUnreadable { .. })
        ));
        let optional = EnvironmentFile {
            optional: true,
            ..required
        };
        assert_eq!(read_file(&optional, missing).unwrap(), []);
    }

    #[test]
    fn precedence_is_lowest_first() {
        let mut builder = EnvironmentBuilder::with_default_path();
        builder.set("HOME", "/var/lib/hass");
        builder.extend(parse_assignments(r#""PATH=/unit/bin" "HOME=/elsewhere""#).unwrap());
        builder.extend(parse_file("HOME=/from-file\n", Path::new("/x")).unwrap());
        let env = builder.build();
        assert_eq!(env["PATH"], "/unit/bin");
        assert_eq!(env["HOME"], "/from-file");
    }
}
