//! `engenho ctl` — talk to a running engenho over its control socket.
//!
//! Table-driven over the spec's operation catalog: every operation's
//! `x-engenho-cli` spelling is a `<resource> <verb>` here, and nothing about
//! any one operation is written in this file. Arguments become the
//! operation's HTTP pieces the same way for all of them:
//!
//! * positional arguments fill the path parameters, in template order;
//! * `--<param> <value>` fills a query or header parameter by its spec name
//!   (`-` and `_` interchangeable);
//! * any other `--<field> <value>` sets a field of the JSON body (the value
//!   parsed as JSON when it is JSON, a string otherwise); `--body <json>`
//!   gives the whole body.
//!
//! The request is then parsed by the same generated parser the daemon uses,
//! so a malformed argument is a usage error here, before anything is sent.
//!
//! Exit codes: 0 answered, 2 usage, 3 refused, 4 blind or unreachable.

use std::fmt;
use std::path::PathBuf;

use engenho_control_client::{ClientError, ControlClient, Reply, resolve_socket};
use engenho_control_types::wire::{HttpParts, HttpRequest, OperationRequest};
use engenho_control_types::{
    AuthorityTier, CATALOG, ControlError, MediaType, Operation, OperationId, OperationSpec,
    OperationVisitor, ParamLocation, visit,
};

/// The call was answered.
pub const EXIT_OK: u8 = 0;
/// The arguments were wrong.
pub const EXIT_USAGE: u8 = 2;
/// The daemon refused.
pub const EXIT_REFUSED: u8 = 3;
/// The daemon could not answer, or could not be reached.
pub const EXIT_BLIND: u8 = 4;

/// A parsed `engenho ctl` invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct CtlCommand {
    /// `--socket`.
    pub socket: Option<PathBuf>,
    /// `--json`: print the response body as JSON.
    pub json: bool,
    /// `--actor`.
    pub actor: Option<String>,
    /// `--ceiling`.
    pub ceiling: Option<AuthorityTier>,
    /// What to do.
    pub action: CtlAction,
}

/// What `engenho ctl` was asked to do.
#[derive(Debug, Clone, PartialEq)]
pub enum CtlAction {
    /// Print every `<resource> <verb>`.
    List,
    /// Call one operation with these pieces.
    Call {
        /// Which.
        id: OperationId,
        /// Its request, as the daemon will parse it.
        parts: HttpParts,
    },
}

/// A usage error: what was wrong, and what is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtlUsage {
    /// A global flag this command does not take.
    UnknownFlag(String),
    /// A flag given no value.
    NeedsValue(String),
    /// `--ceiling` named no tier.
    BadCeiling(String),
    /// No operation is spelled with this resource.
    UnknownResource(String),
    /// A resource without a verb.
    MissingVerb {
        /// The resource.
        resource: String,
        /// Its verbs.
        verbs: Vec<&'static str>,
    },
    /// A verb the resource does not have.
    UnknownVerb {
        /// The resource.
        resource: String,
        /// What was given.
        verb: String,
        /// Its verbs.
        verbs: Vec<&'static str>,
    },
    /// A positional argument past the operation's path parameters.
    UnexpectedArgument {
        /// The operation.
        op: OperationId,
        /// The argument.
        arg: String,
    },
    /// A path parameter not given.
    MissingPathParam {
        /// The operation.
        op: OperationId,
        /// The parameter.
        name: &'static str,
    },
    /// A flag that is neither a parameter nor (for an operation with none)
    /// a body field.
    NoSuchParameter {
        /// The operation.
        op: OperationId,
        /// The flag, without its dashes.
        flag: String,
    },
    /// `--body` is not JSON.
    BodyNotJson(String),
    /// The daemon's own parser refused the request.
    Invalid {
        /// The operation.
        op: OperationId,
        /// Why.
        detail: String,
    },
}

impl fmt::Display for CtlUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let spelled = |op: &OperationId| op.spec().cli;
        match self {
            Self::UnknownFlag(flag) => write!(f, "unknown flag {flag}\n\n{Usage}"),
            Self::NeedsValue(flag) => write!(f, "{flag} needs a value"),
            Self::BadCeiling(tier) => {
                write!(
                    f,
                    "--ceiling {tier:?}: expected observe, mutate or destructive"
                )
            }
            Self::UnknownResource(resource) => {
                write!(f, "unknown resource {resource:?}\n\n{Usage}")
            }
            Self::MissingVerb { resource, verbs } => {
                write!(f, "{resource} needs a verb: {}", verbs.join(", "))
            }
            Self::UnknownVerb {
                resource,
                verb,
                verbs,
            } => write!(f, "{resource} has no verb {verb:?}: {}", verbs.join(", ")),
            Self::UnexpectedArgument { op, arg } => {
                let cli = spelled(op);
                write!(f, "{} {} takes no argument {arg:?}", cli.resource, cli.verb)
            }
            Self::MissingPathParam { op, name } => {
                let cli = spelled(op);
                write!(f, "{} {} needs <{name}>", cli.resource, cli.verb)
            }
            Self::NoSuchParameter { op, flag } => {
                let names: Vec<&str> = op.spec().params.iter().map(|p| p.name).collect();
                write!(
                    f,
                    "{} has no parameter --{flag}; it takes: {}",
                    op.as_str(),
                    names.join(", ")
                )
            }
            Self::BodyNotJson(err) => write!(f, "--body is not JSON: {err}"),
            Self::Invalid { op, detail } => write!(f, "{}: {detail}", op.as_str()),
        }
    }
}

impl std::error::Error for CtlUsage {}

impl CtlCommand {
    /// Parse `engenho ctl`'s arguments (after `ctl`).
    ///
    /// # Errors
    ///
    /// [`CtlUsage`] naming what is accepted.
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, CtlUsage> {
        let mut args = args.into_iter().peekable();
        let mut command = Self {
            socket: None,
            json: false,
            actor: None,
            ceiling: None,
            action: CtlAction::List,
        };
        // Global flags, until the resource.
        while let Some(arg) = args.peek().cloned() {
            if !arg.starts_with("--") {
                break;
            }
            args.next();
            match arg.as_str() {
                "--json" => command.json = true,
                "--list" | "--help" => return Ok(command),
                "--socket" => command.socket = Some(PathBuf::from(value(&mut args, &arg)?)),
                "--actor" => command.actor = Some(value(&mut args, &arg)?),
                "--ceiling" => {
                    let tier = value(&mut args, &arg)?;
                    command.ceiling = Some(tier.parse().map_err(|_| CtlUsage::BadCeiling(tier))?);
                }
                _ => return Err(CtlUsage::UnknownFlag(arg)),
            }
        }
        let Some(resource) = args.next() else {
            return Ok(command);
        };
        if resource == "help" || resource == "list" {
            return Ok(command);
        }
        let rows: Vec<&OperationSpec> = CATALOG
            .iter()
            .filter(|r| r.cli.resource == resource)
            .collect();
        if rows.is_empty() {
            return Err(CtlUsage::UnknownResource(resource));
        }
        let verbs: Vec<&'static str> = rows.iter().map(|r| r.cli.verb).collect();
        let Some(verb) = args.next() else {
            return Err(CtlUsage::MissingVerb { resource, verbs });
        };
        let Some(row) = rows.iter().find(|r| r.cli.verb == verb) else {
            return Err(CtlUsage::UnknownVerb {
                resource,
                verb,
                verbs,
            });
        };
        let parts = request_parts(row, args, &mut command.json)?;
        command.action = CtlAction::Call { id: row.id, parts };
        Ok(command)
    }
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, CtlUsage> {
    args.next()
        .ok_or_else(|| CtlUsage::NeedsValue(flag.to_owned()))
}

/// The operation's HTTP pieces from the rest of the arguments.
fn request_parts(
    row: &OperationSpec,
    args: impl Iterator<Item = String>,
    json: &mut bool,
) -> Result<HttpParts, CtlUsage> {
    let mut path_params = row
        .params
        .iter()
        .filter(|p| p.location == ParamLocation::Path);
    let mut parts = HttpParts::default();
    let mut body = serde_json::Map::new();
    let mut whole_body: Option<serde_json::Value> = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        let Some(flag) = arg.strip_prefix("--") else {
            let Some(param) = path_params.next() else {
                return Err(CtlUsage::UnexpectedArgument { op: row.id, arg });
            };
            parts.path_params.push((param.name.to_owned(), arg));
            continue;
        };
        if flag == "json" {
            *json = true;
            continue;
        }
        let text = value(&mut args, &arg)?;
        if flag == "body" {
            whole_body = Some(
                serde_json::from_str(&text).map_err(|e| CtlUsage::BodyNotJson(e.to_string()))?,
            );
            continue;
        }
        let name = flag.replace('-', "_");
        match row.params.iter().find(|p| {
            p.location != ParamLocation::Path
                && p.name.replace('-', "_").eq_ignore_ascii_case(&name)
        }) {
            Some(p) if p.location == ParamLocation::Query => {
                parts.query.push((p.name.to_owned(), text));
            }
            Some(p) => parts.headers.push((p.name.to_owned(), text)),
            None if row.body => {
                let v = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
                body.insert(name, v);
            }
            None => {
                return Err(CtlUsage::NoSuchParameter {
                    op: row.id,
                    flag: flag.to_owned(),
                });
            }
        }
    }
    if let Some(missing) = path_params.next() {
        return Err(CtlUsage::MissingPathParam {
            op: row.id,
            name: missing.name,
        });
    }
    if row.body {
        let mut value =
            whole_body.unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
        if let serde_json::Value::Object(map) = &mut value {
            map.extend(body);
        }
        parts.body = Some(value);
    }
    Ok(parts)
}

/// The usage summary: every `<resource> <verb>`, its path parameters, its
/// tier and its operation.
pub struct Usage;

impl fmt::Display for Usage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "usage: engenho ctl [--socket PATH] [--json] [--actor A] [--ceiling TIER] \
             <resource> <verb> [args] [--param value]…\n"
        )?;
        let width = CATALOG
            .iter()
            .map(|r| r.cli.resource.len() + r.cli.verb.len() + 1)
            .max()
            .unwrap_or(0);
        for row in &CATALOG {
            let pad = width.saturating_sub(row.cli.resource.len() + row.cli.verb.len() + 1);
            write!(f, "  {} {}{:pad$}  ", row.cli.resource, row.cli.verb, "")?;
            let mut path_width = 0;
            for p in row
                .params
                .iter()
                .filter(|p| p.location == ParamLocation::Path)
            {
                write!(f, "<{}> ", p.name)?;
                path_width += p.name.len() + 3;
            }
            let pad = 13usize.saturating_sub(path_width);
            writeln!(f, "{:pad$}{:<11} {}", "", row.tier, row.id.as_str())?;
        }
        Ok(())
    }
}

/// Check the pieces with the daemon's own parser, then render them.
struct Render(HttpParts);

impl OperationVisitor for Render {
    type Output = Result<HttpRequest, CtlUsage>;

    fn visit<O: Operation>(self) -> Self::Output {
        O::Request::from_http(&self.0)
            .map(|req| req.to_http())
            .map_err(|bad| CtlUsage::Invalid {
                op: O::ID,
                detail: bad.to_string(),
            })
    }
}

/// Run `engenho ctl`.
pub async fn run(args: impl IntoIterator<Item = String>) -> u8 {
    let command = match CtlCommand::parse(args) {
        Ok(command) => command,
        Err(usage) => {
            eprintln!("engenho ctl: {usage}");
            return EXIT_USAGE;
        }
    };
    let CtlAction::Call { id, parts } = command.action else {
        print!("{Usage}");
        return EXIT_OK;
    };
    let request = match visit(id, Render(parts)) {
        Ok(request) => request,
        Err(usage) => {
            eprintln!("engenho ctl: {usage}");
            return EXIT_USAGE;
        }
    };
    let socket = resolve_socket(command.socket.clone());
    let client = match ControlClient::uds(&socket.path) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("engenho ctl: {err}");
            return EXIT_BLIND;
        }
    };
    let client = client.with_actor(command.actor.clone().unwrap_or_else(|| "human".into()));
    let client = match command.ceiling {
        Some(tier) => client.with_ceiling(tier),
        None => client,
    };
    match client.send(id, request).await {
        Ok(reply) => {
            print_reply(id, &reply, command.json);
            EXIT_OK
        }
        Err(ClientError::Control(ControlError::Refused(r))) => {
            eprintln!("refused ({}): {}", r.reason, r.because);
            for legal in &r.legal {
                eprintln!("  instead: {legal}");
            }
            EXIT_REFUSED
        }
        Err(ClientError::Control(ControlError::Blind(b))) => {
            eprintln!("blind ({}): {}", b.reason, b.because);
            EXIT_BLIND
        }
        Err(err @ ClientError::Unreachable { .. }) => {
            eprintln!("engenho ctl: {err}");
            let looked: Vec<String> = socket
                .considered
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            eprintln!("  looked at: {}", looked.join(", "));
            eprintln!("  (--socket PATH or $ENGENHO_CONTROL_SOCKET names it outright)");
            EXIT_BLIND
        }
        Err(err) => {
            eprintln!("engenho ctl: {err}");
            EXIT_BLIND
        }
    }
}

/// Print a success body: YAML text as it is; JSON as JSON with `--json`,
/// else as YAML, which reads better at a terminal.
fn print_reply(id: OperationId, reply: &Reply, json: bool) {
    let text = String::from_utf8_lossy(&reply.body);
    if id.spec().response_media == MediaType::Yaml {
        print!("{text}");
        return;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&reply.body) else {
        println!("{text}");
        return;
    };
    let rendered = if json {
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| text.into_owned())
    } else {
        serde_yaml::to_string(&value).unwrap_or_else(|_| text.into_owned())
    };
    println!("{}", rendered.trim_end());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<CtlCommand, CtlUsage> {
        CtlCommand::parse(line.split_whitespace().map(str::to_owned))
    }

    fn call(line: &str) -> (OperationId, HttpParts) {
        match parse(line).expect("parses").action {
            CtlAction::Call { id, parts } => (id, parts),
            CtlAction::List => panic!("{line}: parsed as a listing"),
        }
    }

    /// `<resource> <verb>` of `id`, then `rest`.
    fn spelled(id: OperationId, rest: &[&str]) -> String {
        let cli = id.spec().cli;
        [&[cli.resource, cli.verb][..], rest].concat().join(" ")
    }

    /// Every operation is reachable by exactly one `<resource> <verb>`.
    #[test]
    fn every_operation_has_exactly_one_spelling() {
        for row in &CATALOG {
            let same: Vec<_> = CATALOG
                .iter()
                .filter(|r| r.cli == row.cli)
                .map(|r| r.id)
                .collect();
            assert_eq!(same, [row.id], "{} {}", row.cli.resource, row.cli.verb);
        }
    }

    #[test]
    fn globals_resource_verb_and_parameters_are_read() {
        let command = parse("--socket /x.sock --json runtime show").expect("parses");
        assert_eq!(command.socket, Some(PathBuf::from("/x.sock")));
        assert!(command.json);
        assert!(matches!(
            command.action,
            CtlAction::Call {
                id: OperationId::GetRuntime,
                ..
            }
        ));

        let (id, parts) = call("logs list --after 5 --wait-ms 100 --level warn");
        assert_eq!(id, OperationId::ListLogs);
        assert_eq!(
            parts.query,
            vec![
                ("after".to_string(), "5".to_string()),
                ("wait_ms".to_string(), "100".to_string()),
                ("level".to_string(), "warn".to_string()),
            ]
        );
    }

    #[test]
    fn a_path_parameter_is_positional_and_body_fields_are_flags() {
        let (id, parts) = call(&spelled(OperationId::GetChild, &["kubelet"]));
        assert_eq!(id, OperationId::GetChild);
        assert_eq!(
            parts.path_params,
            vec![("child".to_string(), "kubelet".to_string())]
        );

        let (_, parts) = call(&spelled(
            OperationId::StopRuntime,
            &["--hold", "across_relaunch"],
        ));
        assert_eq!(
            parts.body,
            Some(serde_json::json!({"hold": "across_relaunch"}))
        );
        let (_, bare) = call(&spelled(OperationId::StopRuntime, &[]));
        assert_eq!(
            bare.body,
            Some(serde_json::json!({})),
            "a body op always sends one"
        );
    }

    #[test]
    fn mistakes_are_usage_errors_that_say_what_is_accepted() {
        assert!(matches!(
            parse("nonsense show"),
            Err(CtlUsage::UnknownResource(_))
        ));
        let err = parse("runtime frobnicate").expect_err("no such verb");
        assert!(err.to_string().contains("show"), "{err}");
        assert!(matches!(
            parse("--ceiling god runtime show"),
            Err(CtlUsage::BadCeiling(_))
        ));
        let err = parse(&spelled(OperationId::GetChild, &[])).expect_err("needs a child");
        assert!(err.to_string().contains("<child>"), "{err}");
        let usage = Usage.to_string();
        for row in &CATALOG {
            assert!(
                usage.contains(row.id.as_str()),
                "{} unlisted",
                row.id.as_str()
            );
        }
    }

    #[test]
    fn a_bad_parameter_is_caught_by_the_daemons_own_parser_before_sending() {
        let (id, parts) = call("logs list --after notanumber");
        assert!(visit(id, Render(parts)).is_err());
        let (id, parts) = call("logs list --after 7");
        let rendered = visit(id, Render(parts)).expect("renders");
        assert_eq!(rendered.query, vec![("after", "7".to_string())]);
    }
}
