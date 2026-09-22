//! `ExecStart=` / `ExecStartPre=`: the prefix characters, the argv, and the
//! environment substitution systemd does at exec time.
//!
//! ## Prefixes
//!
//! Any of `@`, `-`, `:` and one of `+` / `!` / `!!` may lead the command, in
//! any order (systemd.service(5) "Command lines"):
//!
//! * `@` — the next word is argv[0], the first is the program to execute.
//! * `-` — a non-zero exit is ignored.
//! * `:` — no environment-variable substitution in the argv.
//! * `+` — run with full privileges: no `User=`/`Group=` drop (and, in
//!   systemd, no sandboxing, which this runner never applies anyway).
//! * `!` — systemd changes only the credential handling; since this runner
//!   enforces no sandbox, it is exactly `+` here.
//! * `!!` — a fallback for kernels without ambient capabilities. Linux has
//!   them, so systemd ignores it, and so does this: the command drops
//!   privileges like an unprefixed one.
//!
//! ## Substitution
//!
//! `${FOO}` inside a word is replaced by the variable's exact value, always
//! yielding ONE argument. A word that is exactly `$FOO` is replaced by the
//! value split at whitespace, yielding zero or more. `$$` is a literal `$`.
//! Anything else starting with `$` is left alone — which is what lets
//! `sh -c 'echo $HOME'` still mean what it says.

use std::collections::BTreeMap;
use std::fmt;

use crate::words::{self, WordError};

/// How a command treats `User=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    /// The default: drop to the unit's user, group and capabilities.
    Drop,
    /// `+` or `!`: keep the daemon's privileges.
    Full,
}

/// One `Exec*=` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecCommand {
    /// The program to execute.
    pub program: String,
    /// The argv, including argv[0] (which `@` may set to something else).
    pub argv: Vec<String>,
    /// Whether a non-zero exit is ignored (`-`).
    pub ignore_failure: bool,
    /// Whether the argv is substituted against the environment (`:` turns it
    /// off).
    pub substitute: bool,
    /// Whether privileges are dropped for it.
    pub privilege: Privilege,
}

/// Why an `Exec*=` line did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecError {
    /// The value split into no words at all.
    Empty,
    /// `@` with nothing after the program.
    MissingArgv0 {
        /// The program the `@` applied to.
        program: String,
    },
    /// A `;` word: systemd's (deprecated) two-commands-on-one-line form.
    /// Write a second `ExecStartPre=` line instead.
    SemicolonSeparator,
    /// The value did not split.
    Words(WordError),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("the command line is empty"),
            Self::MissingArgv0 { program } => write!(
                f,
                "@{program} sets argv[0] from the NEXT word, and there is none"
            ),
            Self::SemicolonSeparator => f.write_str(
                "a `;` word separates two commands on one line, which this runner does not \
                 accept: write one Exec line per command",
            ),
            Self::Words(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ExecError {}

impl From<WordError> for ExecError {
    fn from(e: WordError) -> Self {
        Self::Words(e)
    }
}

impl ExecCommand {
    /// Parse one `Exec*=` value, whose specifiers are already expanded.
    ///
    /// # Errors
    ///
    /// An [`ExecError`] for an empty line, a dangling `@` or a `;` word.
    pub fn parse(value: &str) -> Result<Self, ExecError> {
        let words = words::split(value)?;
        let mut words = words.into_iter();
        let first = words.next().ok_or(ExecError::Empty)?;

        let mut ignore_failure = false;
        let mut substitute = true;
        let mut privilege = Privilege::Drop;
        let mut argv0_from_next = false;
        let mut rest = first.as_str();
        loop {
            let mut chars = rest.chars();
            match chars.next() {
                Some('@') => argv0_from_next = true,
                Some('-') => ignore_failure = true,
                Some(':') => substitute = false,
                Some('+') => privilege = Privilege::Full,
                Some('!') => {
                    // `!!` is the ambient-capability fallback: ignored on a
                    // kernel that has them, which is every kernel engenho
                    // runs on. A single `!` is `+` here.
                    if chars.as_str().starts_with('!') {
                        rest = &rest[1..];
                    } else {
                        privilege = Privilege::Full;
                    }
                }
                _ => break,
            }
            rest = &rest[1..];
        }
        let program = rest.to_string();
        if program.is_empty() {
            return Err(ExecError::Empty);
        }

        let mut argv = Vec::new();
        if argv0_from_next {
            argv.push(words.next().ok_or_else(|| ExecError::MissingArgv0 {
                program: program.clone(),
            })?);
        } else {
            argv.push(program.clone());
        }
        for word in words {
            if word == ";" {
                return Err(ExecError::SemicolonSeparator);
            }
            argv.push(word);
        }
        Ok(Self {
            program,
            argv,
            ignore_failure,
            substitute,
            privilege,
        })
    }

    /// The argv with `${FOO}` / `$FOO` / `$$` resolved against `env`, unless
    /// the command carries `:`.
    #[must_use]
    pub fn resolved_argv(&self, env: &BTreeMap<String, String>) -> Vec<String> {
        if !self.substitute {
            return self.argv.clone();
        }
        let mut out = Vec::with_capacity(self.argv.len());
        for word in &self.argv {
            // argv[0] is substituted like any other word, as systemd does.
            match whole_word_variable(word) {
                // An unset variable as a whole word contributes NO argument.
                Some(name) => {
                    if let Some(value) = env.get(name) {
                        out.extend(value.split_whitespace().map(ToString::to_string));
                    }
                }
                None => out.push(substitute_braced(word, env)),
            }
        }
        out
    }
}

/// `$FOO` as an entire word → the variable's name.
fn whole_word_variable(word: &str) -> Option<&str> {
    let name = word.strip_prefix('$')?;
    (!name.is_empty() && is_env_name(name)).then_some(name)
}

/// Replace `${FOO}` and `$$` inside one word.
fn substitute_braced(word: &str, env: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(word.len());
    let mut rest = word;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
        } else if let Some(open) = after.strip_prefix('{') {
            if let Some((name, tail)) = open.split_once('}') {
                if let Some(value) = env.get(name) {
                    out.push_str(value);
                }
                rest = tail;
            } else {
                out.push('$');
                rest = after;
            }
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// A valid environment-variable name: letters, digits and `_`, not starting
/// with a digit.
#[must_use]
pub fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn a_plain_command_is_its_own_argv0() {
        let c = ExecCommand::parse("/nix/store/x/bin/mosquitto -c /nix/store/y/mosquitto.conf")
            .unwrap();
        assert_eq!(c.program, "/nix/store/x/bin/mosquitto");
        assert_eq!(
            c.argv,
            [
                "/nix/store/x/bin/mosquitto",
                "-c",
                "/nix/store/y/mosquitto.conf"
            ]
        );
        assert!(!c.ignore_failure);
        assert!(c.substitute);
        assert_eq!(c.privilege, Privilege::Drop);
    }

    #[test]
    fn dhcpcd_at_prefix_takes_argv0_from_the_next_word() {
        let c = ExecCommand::parse("@/nix/store/x/sbin/dhcpcd dhcpcd --quiet").unwrap();
        assert_eq!(c.program, "/nix/store/x/sbin/dhcpcd");
        assert_eq!(c.argv, ["dhcpcd", "--quiet"]);
        assert_eq!(
            ExecCommand::parse("@/bin/only"),
            Err(ExecError::MissingArgv0 {
                program: "/bin/only".into()
            })
        );
    }

    #[test]
    fn every_prefix_and_combination_is_read() {
        let plus = ExecCommand::parse("+/nix/store/x-migrate-dhcpcd").unwrap();
        assert_eq!(plus.privilege, Privilege::Full);
        assert_eq!(plus.program, "/nix/store/x-migrate-dhcpcd");

        let bang = ExecCommand::parse("!/bin/a").unwrap();
        assert_eq!(bang.privilege, Privilege::Full, "! is + for this runner");

        let double = ExecCommand::parse("!!/bin/a").unwrap();
        assert_eq!(
            double.privilege,
            Privilege::Drop,
            "!! is the no-ambient fallback, ignored where ambient caps exist"
        );

        let all = ExecCommand::parse("-@:+/bin/a arg0 x").unwrap();
        assert!(all.ignore_failure);
        assert!(!all.substitute);
        assert_eq!(all.privilege, Privilege::Full);
        assert_eq!(all.program, "/bin/a");
        assert_eq!(all.argv, ["arg0", "x"]);
    }

    #[test]
    fn an_empty_line_and_a_semicolon_word_are_typed_errors() {
        assert_eq!(ExecCommand::parse("   "), Err(ExecError::Empty));
        assert_eq!(ExecCommand::parse("-"), Err(ExecError::Empty));
        assert_eq!(
            ExecCommand::parse("/bin/a ; /bin/b"),
            Err(ExecError::SemicolonSeparator)
        );
    }

    #[test]
    fn braced_variables_stay_one_argument_and_bare_ones_split() {
        let env = env(&[("ARGS", "-a  -b"), ("DIR", "/var/lib/x"), ("EMPTY", "")]);
        let c = ExecCommand::parse("/bin/a --dir=${DIR}/sub $ARGS $NOPE 'echo $HOME'").unwrap();
        assert_eq!(
            c.resolved_argv(&env),
            ["/bin/a", "--dir=/var/lib/x/sub", "-a", "-b", "echo $HOME"],
            "$NOPE is unset so it contributes nothing; a $ inside a word is left to the shell \
             (quoting does NOT stop substitution — systemd substitutes after unquoting, so a \
             word that IS $HOME would be substituted)"
        );
        let literal = ExecCommand::parse(":/bin/a ${DIR}").unwrap();
        assert_eq!(literal.resolved_argv(&env), ["/bin/a", "${DIR}"]);
        let dollars = ExecCommand::parse("/bin/a a$$b ${EMPTY}x").unwrap();
        assert_eq!(dollars.resolved_argv(&env), ["/bin/a", "a$b", "x"]);
    }

    #[test]
    fn env_names_are_validated() {
        assert!(is_env_name("MAINPID"));
        assert!(is_env_name("_x1"));
        assert!(!is_env_name("1x"));
        assert!(!is_env_name("a-b"));
        assert!(!is_env_name(""));
    }
}
