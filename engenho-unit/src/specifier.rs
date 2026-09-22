//! systemd specifiers — `%d`, `%S`, `%t`, `%n`, … — expanded once, BEFORE a
//! value is split into words, as systemd's `unit_full_printf` does.
//!
//! nixpkgs depends on them: zwave-js runs
//! `ExecStartPre=/bin/sh -c "jq … %d/secrets.json > /run/zwave-js/config.json"`
//! with `LoadCredential=secrets.json:…`, and matter-server's `ExecStart=`
//! links `%S/matter-server/` into `%t/matter-server/root/data`. A specifier
//! left literal would hand the service a path containing `%d`, so an unknown
//! specifier is an ERROR, never passed through.
//!
//! Supported (systemd.unit(5) "Specifiers"):
//!
//! | spec | meaning |
//! |---|---|
//! | `%%` | a literal `%` |
//! | `%n` `%N` | the full unit name / without its type suffix |
//! | `%p` `%P` | the prefix (before `@`) / unescaped |
//! | `%i` `%I` | the instance (after `@`) / unescaped |
//! | `%j` `%J` | the prefix's last `-` component / unescaped |
//! | `%f` | `/` + the unescaped instance, or prefix |
//! | `%d` | the credentials directory, `/run/credentials/<unit>` |
//! | `%S` `%C` `%L` `%t` `%E` | `/var/lib` `/var/cache` `/var/log` `/run` `/etc` |
//! | `%T` `%V` | `/tmp`, `/var/tmp` |
//! | `%u` `%U` `%g` `%G` `%h` `%s` | the service's user, uid, group, gid, home, shell |
//! | `%H` `%l` | the hostname / its first label |
//! | `%m` `%b` `%v` `%a` | machine id, boot id, kernel release, architecture |
//! | `%y` `%Y` | the unit file's path / its directory |
//!
//! Everything else (`%o %w %W %B %M %A %q %R %Z`) is refused by name.

use std::fmt;
use std::path::Path;

use crate::layout::{BaseDir, Layout};

/// Why a value's specifiers did not expand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecifierError {
    /// `%x` for an `x` this runner does not know.
    Unknown {
        /// The character after `%`.
        spec: char,
    },
    /// A `%` as the last character.
    Trailing,
    /// A known specifier whose value is not available here.
    Unavailable {
        /// The character after `%`.
        spec: char,
        /// What it needs.
        needs: Needs,
    },
}

/// What an unavailable specifier needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Needs {
    /// The service's account — `User=` itself cannot name `%u`.
    Identity,
    /// A host fact that could not be read (hostname, machine id, boot id,
    /// kernel release).
    HostFact,
    /// The unit file's path, when the unit was not read from one.
    FragmentPath,
}

impl fmt::Display for SpecifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { spec } => write!(
                f,
                "%{spec} is not a specifier this runner expands (it is never passed through literally)"
            ),
            Self::Trailing => {
                f.write_str("a trailing % is not a specifier (write %% for a literal %)")
            }
            Self::Unavailable { spec, needs } => {
                let what = match needs {
                    Needs::Identity => {
                        "the service's account, which is not resolved where it is used"
                    }
                    Needs::HostFact => "a host fact that could not be read",
                    Needs::FragmentPath => "the unit file's path",
                };
                write!(f, "%{spec} needs {what}")
            }
        }
    }
}

impl std::error::Error for SpecifierError {}

/// The service account's facts the user specifiers expand to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountFacts {
    /// `%u`.
    pub user: String,
    /// `%U`.
    pub uid: u32,
    /// `%g`.
    pub group: String,
    /// `%G`.
    pub gid: u32,
    /// `%h`.
    pub home: String,
    /// `%s`.
    pub shell: String,
}

impl AccountFacts {
    /// The account a unit without `User=` runs as: systemd's own, root.
    #[must_use]
    pub fn root() -> Self {
        Self {
            user: "root".into(),
            uid: 0,
            group: "root".into(),
            gid: 0,
            home: "/root".into(),
            shell: "/bin/sh".into(),
        }
    }
}

/// Facts about the host, read once from the [`Layout`]'s root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostFacts {
    /// `%H`.
    pub hostname: Option<String>,
    /// `%m`.
    pub machine_id: Option<String>,
    /// `%b`.
    pub boot_id: Option<String>,
    /// `%v`.
    pub kernel_release: Option<String>,
}

impl HostFacts {
    /// Read the facts from `/proc` and `/etc` under the layout's root. A fact
    /// that cannot be read is `None`, and only a specifier that NEEDS it
    /// fails.
    #[must_use]
    pub fn read(layout: &Layout) -> Self {
        let first = |paths: &[&str]| {
            paths.iter().find_map(|p| {
                std::fs::read_to_string(layout.on_disk(Path::new(p)))
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
        };
        Self {
            hostname: first(&["/proc/sys/kernel/hostname", "/etc/hostname"]),
            machine_id: first(&["/etc/machine-id"]),
            boot_id: first(&["/proc/sys/kernel/random/boot_id"]).map(|id| id.replace('-', "")),
            kernel_release: first(&["/proc/sys/kernel/osrelease"]),
        }
    }
}

/// Everything a specifier can expand to.
#[derive(Debug, Clone)]
pub struct Context<'a> {
    /// The full unit name, e.g. `zwave-js.service`.
    pub unit: &'a str,
    /// The unit file the unit was read from.
    pub fragment: Option<&'a Path>,
    /// The service's account; `None` while `User=` itself is being expanded.
    pub account: Option<&'a AccountFacts>,
    /// The directory layout.
    pub layout: &'a Layout,
    /// Host facts.
    pub host: &'a HostFacts,
}

impl Context<'_> {
    /// Expand every specifier in `value`.
    ///
    /// # Errors
    ///
    /// A [`SpecifierError`] for an unknown, trailing or unavailable one.
    pub fn expand(&self, value: &str) -> Result<String, SpecifierError> {
        let mut out = String::with_capacity(value.len());
        let mut chars = value.chars();
        while let Some(c) = chars.next() {
            if c != '%' {
                out.push(c);
                continue;
            }
            let spec = chars.next().ok_or(SpecifierError::Trailing)?;
            out.push_str(&self.one(spec)?);
        }
        Ok(out)
    }

    fn one(&self, spec: char) -> Result<String, SpecifierError> {
        let name = UnitName::parse(self.unit);
        let account = || {
            self.account.ok_or(SpecifierError::Unavailable {
                spec,
                needs: Needs::Identity,
            })
        };
        let host = |fact: &Option<String>| {
            fact.clone().ok_or(SpecifierError::Unavailable {
                spec,
                needs: Needs::HostFact,
            })
        };
        let fragment = || {
            self.fragment.ok_or(SpecifierError::Unavailable {
                spec,
                needs: Needs::FragmentPath,
            })
        };
        let base = |b: BaseDir| path_string(&self.layout.base(b));
        Ok(match spec {
            '%' => "%".into(),
            'n' => self.unit.into(),
            'N' => name.without_suffix.into(),
            'p' => name.prefix.into(),
            'P' => unescape(name.prefix),
            'i' => name.instance.unwrap_or_default().into(),
            'I' => unescape(name.instance.unwrap_or_default()),
            'j' => last_dash_component(name.prefix).into(),
            'J' => unescape(last_dash_component(name.prefix)),
            'f' => {
                let mut f = String::from("/");
                f.push_str(&unescape(name.instance.unwrap_or(name.prefix)));
                f
            }
            'd' => path_string(&self.layout.credentials_dir(self.unit)),
            'S' => base(BaseDir::State),
            'C' => base(BaseDir::Cache),
            'L' => base(BaseDir::Logs),
            't' => base(BaseDir::Runtime),
            'E' => base(BaseDir::Configuration),
            'T' => "/tmp".into(),
            'V' => "/var/tmp".into(),
            'u' => account()?.user.clone(),
            'U' => account()?.uid.to_string(),
            'g' => account()?.group.clone(),
            'G' => account()?.gid.to_string(),
            'h' => account()?.home.clone(),
            's' => account()?.shell.clone(),
            'H' => host(&self.host.hostname)?,
            'l' => host(&self.host.hostname)?
                .split('.')
                .next()
                .unwrap_or_default()
                .to_string(),
            'm' => host(&self.host.machine_id)?,
            'b' => host(&self.host.boot_id)?,
            'v' => host(&self.host.kernel_release)?,
            'a' => architecture().into(),
            'y' => path_string(fragment()?),
            'Y' => path_string(fragment()?.parent().unwrap_or_else(|| Path::new("/"))),
            _ => return Err(SpecifierError::Unknown { spec }),
        })
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The parts of a unit name: `prefix@instance.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitName<'a> {
    /// The name without its `.type` suffix.
    pub without_suffix: &'a str,
    /// Before `@`, or the whole name without its suffix.
    pub prefix: &'a str,
    /// After `@`, if the name has one.
    pub instance: Option<&'a str>,
}

impl<'a> UnitName<'a> {
    /// Split `unit` into its parts.
    #[must_use]
    pub fn parse(unit: &'a str) -> Self {
        let without_suffix = unit.rsplit_once('.').map_or(unit, |(stem, _)| stem);
        let (prefix, instance) = match without_suffix.split_once('@') {
            Some((prefix, instance)) => (prefix, Some(instance)),
            None => (without_suffix, None),
        };
        Self {
            without_suffix,
            prefix,
            instance,
        }
    }
}

fn last_dash_component(prefix: &str) -> &str {
    prefix.rsplit('-').next().unwrap_or(prefix)
}

/// systemd's unit-name unescaping: `-` is `/`, `\xHH` is a byte.
#[must_use]
pub fn unescape(escaped: &str) -> String {
    let mut bytes = Vec::with_capacity(escaped.len());
    let raw = escaped.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hex = (raw[i] == b'\\' && raw.get(i + 1) == Some(&b'x'))
            .then(|| escaped.get(i + 2..i + 4))
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if let Some(byte) = hex {
            bytes.push(byte);
            i += 4;
        } else {
            bytes.push(if raw[i] == b'-' { b'/' } else { raw[i] });
            i += 1;
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// `%a`, in systemd's architecture vocabulary.
#[must_use]
pub fn architecture() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x86-64",
        "aarch64" => "arm64",
        "powerpc64" => "ppc64",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::HostRoot;

    fn ctx_expand(
        unit: &str,
        account: Option<&AccountFacts>,
        value: &str,
    ) -> Result<String, SpecifierError> {
        let layout = Layout::new(HostRoot::system());
        let host = HostFacts {
            hostname: Some("plo.pleme.internal".into()),
            ..HostFacts::default()
        };
        Context {
            unit,
            fragment: Some(Path::new("/nix/store/abc-unit-x/x.service")),
            account,
            layout: &layout,
            host: &host,
        }
        .expand(value)
    }

    #[test]
    fn zwave_js_credentials_specifier_is_the_credentials_directory() {
        let got = ctx_expand(
            "zwave-js.service",
            None,
            "/bin/sh -c \"jq -s '.[0] * .[1]' cfg.json %d/secrets.json > /run/zwave-js/config.json\"",
        )
        .unwrap();
        assert!(
            got.contains("/run/credentials/zwave-js.service/secrets.json"),
            "{got}"
        );
    }

    #[test]
    fn matter_server_directory_specifiers_expand_inside_quotes() {
        let got = ctx_expand(
            "matter-server.service",
            None,
            "sh -c ' ln -s %S/matter-server/ %t/matter-server/root/data '",
        )
        .unwrap();
        assert_eq!(
            got,
            "sh -c ' ln -s /var/lib/matter-server/ /run/matter-server/root/data '"
        );
    }

    #[test]
    fn unit_name_specifiers() {
        let e = |v| ctx_expand("wyoming-piper@main-x.service", None, v).unwrap();
        assert_eq!(e("%n"), "wyoming-piper@main-x.service");
        assert_eq!(e("%N"), "wyoming-piper@main-x");
        assert_eq!(e("%p"), "wyoming-piper");
        assert_eq!(e("%i"), "main-x");
        assert_eq!(e("%I"), "main/x");
        assert_eq!(e("%j"), "piper");
        assert_eq!(e("%f"), "/main/x");
        assert_eq!(e("100%% %y"), "100% /nix/store/abc-unit-x/x.service");
        assert_eq!(e("%Y"), "/nix/store/abc-unit-x");
        assert_eq!(e("%l %H"), "plo plo.pleme.internal");
    }

    #[test]
    fn user_specifiers_need_the_account() {
        assert_eq!(
            ctx_expand("a.service", None, "%h"),
            Err(SpecifierError::Unavailable {
                spec: 'h',
                needs: Needs::Identity
            })
        );
        let account = AccountFacts {
            user: "hass".into(),
            uid: 286,
            group: "hass".into(),
            gid: 286,
            home: "/var/lib/hass".into(),
            shell: "/run/current-system/sw/bin/nologin".into(),
        };
        assert_eq!(
            ctx_expand("a.service", Some(&account), "%u:%U:%g:%G:%h").unwrap(),
            "hass:286:hass:286:/var/lib/hass"
        );
    }

    #[test]
    fn unknown_trailing_and_unavailable_are_errors_never_literal() {
        assert_eq!(
            ctx_expand("a.service", None, "x%q"),
            Err(SpecifierError::Unknown { spec: 'q' })
        );
        assert_eq!(
            ctx_expand("a.service", None, "x%"),
            Err(SpecifierError::Trailing)
        );
        assert_eq!(
            ctx_expand("a.service", None, "%m"),
            Err(SpecifierError::Unavailable {
                spec: 'm',
                needs: Needs::HostFact
            })
        );
    }
}
