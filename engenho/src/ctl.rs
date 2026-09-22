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
//! A destructive operation (its catalog row is [`ConfirmGate::Executes`])
//! run without `--engenho-confirmation` goes through the handshake here:
//! prepare a challenge, show what it binds and costs, take the cluster's
//! name — `--confirm-phrase`, or typed at a terminal — and execute under it.
//! Nothing about any one operation is written for this either.
//!
//! Exit codes: 0 answered, 2 usage, 3 refused, 4 blind or unreachable,
//! 5 confirmation aborted (nothing executed).

use std::fmt;
use std::io::IsTerminal as _;
use std::path::PathBuf;

use engenho_control_client::{
    ClientError, ControlClient, RemotesConfig, Reply, remote, resolve_socket,
};
use engenho_control_types::ops::{
    CancelConfirmation, CancelConfirmationRequest, CreateConfirmation, CreateConfirmationRequest,
};
use engenho_control_types::types;
use engenho_control_types::wire::{HttpParts, HttpRequest, OperationRequest};
use engenho_control_types::{
    AuthorityTier, CATALOG, ConfirmGate, ControlError, MediaType, Operation, OperationId,
    OperationSpec, OperationVisitor, ParamLocation, visit,
};

/// The call was answered.
pub const EXIT_OK: u8 = 0;
/// The arguments were wrong.
pub const EXIT_USAGE: u8 = 2;
/// The daemon refused.
pub const EXIT_REFUSED: u8 = 3;
/// The daemon could not answer, or could not be reached.
pub const EXIT_BLIND: u8 = 4;
/// A destructive operation's confirmation was not given or did not match:
/// nothing was executed, and the challenge was withdrawn.
pub const EXIT_ABORTED: u8 = 5;

/// The header a destructive operation carries its challenge in.
const CONFIRMATION_HEADER: &str = "Engenho-Confirmation";
/// The body field it carries the typed phrase in.
const PHRASE_FIELD: &str = "confirm_phrase";

/// Which daemon `engenho ctl` talks to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// This machine's, over its socket: `--socket PATH`, else the one
    /// [`resolve_socket`] finds.
    Local(Option<PathBuf>),
    /// `--remote NAME`: a daemon listed in `remotes.yaml`, over mTLS.
    Remote(String),
}

/// A parsed `engenho ctl` invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct CtlCommand {
    /// `--socket` or `--remote`.
    pub endpoint: Endpoint,
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
    /// `--socket` and `--remote` together: one daemon at a time.
    TwoEndpoints,
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
            Self::TwoEndpoints => f.write_str("--socket and --remote name two daemons; give one"),
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
            endpoint: Endpoint::Local(None),
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
                "--socket" | "--remote" => {
                    let given = value(&mut args, &arg)?;
                    if command.endpoint != Endpoint::Local(None) {
                        return Err(CtlUsage::TwoEndpoints);
                    }
                    command.endpoint = if arg == "--socket" {
                        Endpoint::Local(Some(PathBuf::from(given)))
                    } else {
                        Endpoint::Remote(given)
                    };
                }
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
            "usage: engenho ctl [--socket PATH | --remote NAME] [--json] [--actor A] \
             [--ceiling TIER] <resource> <verb> [args] [--param value]…\n"
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
    let CtlAction::Call { id, mut parts } = command.action else {
        print!("{Usage}");
        return EXIT_OK;
    };
    // A destructive operation without a challenge: the handshake below
    // fills the challenge in, so what is checked now is the request the
    // challenge will bind.
    let handshake = match id.spec().gate {
        ConfirmGate::Executes(op) if parts.header(CONFIRMATION_HEADER).is_none() => {
            match reinit_request(op, &parts) {
                Ok(request) => Some(request),
                Err(usage) => {
                    eprintln!("engenho ctl: {usage}");
                    return EXIT_USAGE;
                }
            }
        }
        ConfirmGate::Executes(_) | ConfirmGate::Issue | ConfirmGate::Cancel | ConfirmGate::None => {
            if let Err(usage) = visit(id, Render(parts.clone())) {
                eprintln!("engenho ctl: {usage}");
                return EXIT_USAGE;
            }
            None
        }
    };
    // Where it looked, for an unreachable local daemon; a remote one names
    // its own address.
    let mut looked: Vec<PathBuf> = Vec::new();
    let client = match &command.endpoint {
        Endpoint::Local(explicit) => {
            let socket = resolve_socket(explicit.clone());
            looked = socket.considered;
            match ControlClient::uds(&socket.path) {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("engenho ctl: {err}");
                    return EXIT_BLIND;
                }
            }
        }
        Endpoint::Remote(name) => match remote_client(name) {
            Ok(client) => client,
            Err(why) => {
                eprintln!("engenho ctl: --remote {name}: {why}");
                return EXIT_USAGE;
            }
        },
    };
    let client = client.with_actor(command.actor.clone().unwrap_or_else(|| "human".into()));
    let client = match command.ceiling {
        Some(tier) => client.with_ceiling(tier),
        None => client,
    };
    if let Some(request) = handshake
        && let Err(code) = confirm(&client, request, &mut parts, &looked).await
    {
        return code;
    }
    let request = match visit(id, Render(parts)) {
        Ok(request) => request,
        Err(usage) => {
            eprintln!("engenho ctl: {usage}");
            return EXIT_USAGE;
        }
    };
    match client.send(id, request).await {
        Ok(reply) => {
            print_reply(id, &reply, command.json);
            EXIT_OK
        }
        Err(err) => report(&err, &looked),
    }
}

/// Say why a call got no answer; the exit code that says so.
fn report(err: &ClientError, looked: &[PathBuf]) -> u8 {
    match err {
        ClientError::Control(ControlError::Refused(r)) => {
            eprintln!("refused ({}): {}", r.reason, r.because);
            for legal in &r.legal {
                eprintln!("  instead: {legal}");
            }
            EXIT_REFUSED
        }
        ClientError::Control(ControlError::Blind(b)) => {
            eprintln!("blind ({}): {}", b.reason, b.because);
            EXIT_BLIND
        }
        ClientError::Unreachable { .. } => {
            eprintln!("engenho ctl: {err}");
            if !looked.is_empty() {
                let looked: Vec<String> = looked.iter().map(|p| p.display().to_string()).collect();
                eprintln!("  looked at: {}", looked.join(", "));
                eprintln!("  (--socket PATH or $ENGENHO_CONTROL_SOCKET names it outright)");
            }
            EXIT_BLIND
        }
        ClientError::Protocol(_) => {
            eprintln!("engenho ctl: {err}");
            EXIT_BLIND
        }
    }
}

/// The re-initialization `op` names with the parameters in `parts`' body —
/// what the challenge binds.
fn reinit_request(
    op: types::ReinitOp,
    parts: &HttpParts,
) -> Result<types::ReinitRequest, CtlUsage> {
    let mut fields = match &parts.body {
        Some(serde_json::Value::Object(fields)) => fields.clone(),
        Some(_) | None => serde_json::Map::new(),
    };
    fields.remove(PHRASE_FIELD);
    fields.insert(
        "operation".into(),
        serde_json::Value::String(op.to_string()),
    );
    serde_json::from_value(serde_json::Value::Object(fields)).map_err(|e| CtlUsage::Invalid {
        op: OperationId::CreateConfirmation,
        detail: e.to_string(),
    })
}

/// The handshake: prepare a challenge for `request`, show what it binds and
/// costs, take the phrase, and fill the challenge and the phrase into
/// `parts`. A phrase that is not the cluster's name, or none, withdraws the
/// challenge.
async fn confirm(
    client: &ControlClient,
    request: types::ReinitRequest,
    parts: &mut HttpParts,
    looked: &[PathBuf],
) -> Result<(), u8> {
    let challenge = client
        .call::<CreateConfirmation>(&CreateConfirmationRequest {
            body: types::ConfirmationRequest { request },
        })
        .await
        .map_err(|err| report(&err, looked))?;
    show(&challenge, client.endpoint());
    let given = parts
        .body
        .as_ref()
        .and_then(|body| body.get(PHRASE_FIELD))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let phrase = match given {
        Some(phrase) => Some(phrase),
        None if std::io::stdin().is_terminal() => prompt(&challenge.phrase_hint).await,
        None => {
            eprintln!(
                "stdin is not a terminal: pass --confirm-phrase {} to confirm",
                challenge.phrase_hint
            );
            None
        }
    };
    if phrase.as_deref() != Some(challenge.phrase_hint.as_str()) {
        let _ = client
            .call::<CancelConfirmation>(&CancelConfirmationRequest {
                confirmation: challenge.id.clone(),
            })
            .await;
        eprintln!("aborted: nothing was done");
        return Err(EXIT_ABORTED);
    }
    parts
        .headers
        .push((CONFIRMATION_HEADER.to_owned(), challenge.id.to_string()));
    if let Some(serde_json::Value::Object(body)) = &mut parts.body {
        body.insert(
            PHRASE_FIELD.into(),
            serde_json::Value::String(challenge.phrase_hint),
        );
    }
    Ok(())
}

/// What a challenge binds and what executing it costs, on stderr.
fn show(challenge: &types::Challenge, endpoint: &str) {
    let bound = &challenge.bound;
    eprintln!(
        "{} on cluster {:?}, node {:?}, at {endpoint}",
        bound.operation, bound.cluster_name, bound.node_name
    );
    let ca = match &bound.ca {
        types::CaBinding::Present(sha256) => ["sha256:", sha256.as_str()].concat(),
        types::CaBinding::Absent => "none".to_owned(),
        types::CaBinding::Unreadable => "unreadable".to_owned(),
    };
    eprintln!("  CA: {ca}");
    for line in &challenge.blast_radius {
        eprintln!("  - {line}");
    }
    eprintln!("  (this challenge stands until {})", challenge.expires_at);
}

/// Ask for the phrase at the terminal; `None` at end of input.
async fn prompt(hint: &str) -> Option<String> {
    eprint!("type the cluster's name ({hint}) to confirm: ");
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .ok()
            .filter(|n| *n > 0)
            .map(|_| line.trim().to_owned())
    })
    .await
    .ok()
    .flatten()
}

/// A client for the remote daemon `name`: its address and pins from
/// `remotes.yaml`, this machine's key for it.
fn remote_client(name: &str) -> Result<ControlClient, String> {
    let dir = remote::config_dir().map_err(|e| e.to_string())?;
    let remotes = RemotesConfig::load(&dir).map_err(|e| e.to_string())?;
    let endpoint = remotes.get(&dir, name).map_err(|e| e.to_string())?;
    let key = remote::read_key(&remote::key_path(&dir, name, Some(endpoint)))
        .map_err(|e| e.to_string())?;
    ControlClient::remote(endpoint, &key).map_err(|e| e.to_string())
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
        assert_eq!(
            command.endpoint,
            Endpoint::Local(Some(PathBuf::from("/x.sock")))
        );
        assert!(command.json);
        let remote = parse("--remote plo runtime show").expect("parses");
        assert_eq!(remote.endpoint, Endpoint::Remote("plo".into()));
        assert_eq!(
            parse("--remote plo --socket /x.sock runtime show"),
            Err(CtlUsage::TwoEndpoints),
            "one daemon at a time"
        );
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
