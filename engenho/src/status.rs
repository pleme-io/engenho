#![allow(
    clippy::disallowed_macros,
    reason = "human-facing report text, never wire or syntax"
)]

use engenho_control_client::{ClientError, ControlClient};
use engenho_control_types::ops;
use engenho_control_types::types::{
    AttemptResult, BootView, ChildState, ChildrenView, ControlEvent, ControlEventKind, ControlView,
    DriftReport, Hello, KubeconfigList, LastDeath, LatestBootAttempt, LifecycleState, LockState,
    PendingApply, PhaseResult, PkiInventory, PreviousRun, PublishOutcome, StoreState, StoreView,
};
use kazari::prelude::*;
use serde::Serialize;

use crate::ctl::{self, CtlUsage, Endpoint};
use crate::face::{self, ago, detail, tag, word};

pub const EXIT_HEALTHY: u8 = 0;
pub const EXIT_DEGRADED: u8 = 1;

const USAGE: &str = "usage: engenho status [--socket PATH | --remote NAME] [--json] [--events N]";
const DEFAULT_EVENTS: usize = 8;
const EVENT_PAGES: usize = 64;

#[derive(Debug, PartialEq, Eq)]
pub struct StatusCommand {
    pub endpoint: Endpoint,
    pub json: bool,
    pub events: usize,
    pub help: bool,
}

impl StatusCommand {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, CtlUsage> {
        let mut command = Self {
            endpoint: Endpoint::Local(None),
            json: false,
            events: DEFAULT_EVENTS,
            help: false,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--json" => command.json = true,
                "--help" | "-h" | "help" => command.help = true,
                "--socket" | "--remote" => {
                    let given = ctl::value(&mut args, &arg)?;
                    command.endpoint.set(&arg, given)?;
                }
                "--events" => {
                    let given = ctl::value(&mut args, &arg)?;
                    command.events = given
                        .parse()
                        .map_err(|_| CtlUsage::NeedsValue("--events <count>".to_owned()))?;
                }
                _ => return Err(CtlUsage::UnknownFlag(arg)),
            }
        }
        Ok(command)
    }
}

struct Report {
    endpoint: String,
    hello: Hello,
    store: Section<StoreView>,
    children: Section<ChildrenView>,
    boot: Section<BootView>,
    pki: Section<PkiInventory>,
    kubeconfigs: Section<KubeconfigList>,
    drift: Section<DriftReport>,
    control: Section<ControlView>,
    events: Section<Vec<ControlEvent>>,
}

type Section<T> = Result<T, String>;

fn section<T>(result: Result<T, ClientError>) -> Section<T> {
    result.map_err(|err| err.to_string())
}

pub async fn run(args: impl IntoIterator<Item = String>) -> u8 {
    let command = match StatusCommand::parse(args) {
        Ok(command) => command,
        Err(usage) => {
            eprintln!("engenho status: {usage}\n{USAGE}");
            return ctl::EXIT_USAGE;
        }
    };
    if command.help {
        println!("{USAGE}");
        return ctl::EXIT_OK;
    }
    let (client, looked) = match ctl::connect(&command.endpoint, "engenho status") {
        Ok(connected) => connected,
        Err(code) => return code,
    };
    let client = client.with_actor("engenho-status");
    let report = match gather(&client, command.events).await {
        Ok(report) => report,
        Err(err) => return ctl::report_as("engenho status", &err, &looked),
    };
    let issues = issues(&report);
    if command.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json(&report, &issues)).unwrap_or_default()
        );
    } else {
        let _ = render(&report, &issues);
    }
    if issues.iter().any(|i| i.severity != CalloutSeverity::Note) {
        EXIT_DEGRADED
    } else {
        EXIT_HEALTHY
    }
}

async fn gather(client: &ControlClient, events: usize) -> Result<Report, ClientError> {
    let (hello, store, children, boot, pki, kubeconfigs, drift, control, recent) = tokio::join!(
        client.call::<ops::Hello>(&ops::HelloRequest {}),
        client.call::<ops::GetStore>(&ops::GetStoreRequest {}),
        client.call::<ops::ListChildren>(&ops::ListChildrenRequest {}),
        client.call::<ops::GetBoot>(&ops::GetBootRequest {}),
        client.call::<ops::GetPki>(&ops::GetPkiRequest {}),
        client.call::<ops::ListKubeconfigs>(&ops::ListKubeconfigsRequest {}),
        client.call::<ops::GetConfigDrift>(&ops::GetConfigDriftRequest {}),
        client.call::<ops::GetControl>(&ops::GetControlRequest {}),
        recent_events(client, events),
    );
    Ok(Report {
        endpoint: client.endpoint().to_owned(),
        hello: hello?,
        store: section(store),
        children: section(children),
        boot: section(boot),
        pki: section(pki),
        kubeconfigs: section(kubeconfigs),
        drift: section(drift),
        control: section(control),
        events: section(recent),
    })
}

async fn recent_events(
    client: &ControlClient,
    keep: usize,
) -> Result<Vec<ControlEvent>, ClientError> {
    let mut kept: Vec<ControlEvent> = Vec::new();
    let mut after = None;
    for _ in 0..EVENT_PAGES {
        let page = client
            .call::<ops::ListEvents>(&ops::ListEventsRequest {
                after,
                wait_ms: Some(0),
                limit: None,
            })
            .await?;
        if page.events.is_empty() || after == Some(page.next_cursor) {
            break;
        }
        after = Some(page.next_cursor);
        kept.extend(page.events);
        let excess = kept.len().saturating_sub(keep);
        kept.drain(..excess);
    }
    Ok(kept)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Issue {
    severity: CalloutSeverity,
    text: String,
}

impl Issue {
    fn new(severity: CalloutSeverity, text: impl Into<String>) -> Self {
        Self {
            severity,
            text: text.into(),
        }
    }
}

fn issues(r: &Report) -> Vec<Issue> {
    let mut out = Vec::new();
    match &r.hello.lifecycle {
        LifecycleState::Running {
            pending: PendingApply::RestartNeeded(leaves),
            ..
        } => out.push(Issue::new(
            CalloutSeverity::Warn,
            format!("{} config leaves wait for a runtime restart", leaves.len()),
        )),
        LifecycleState::Running { .. } => {}
        other => out.push(Issue::new(
            lifecycle_severity(other),
            format!("runtime is {}", tag(other, "state")),
        )),
    }
    if let Ok(store) = &r.store {
        if store.facts.lock != LockState::HeldByThisDaemon {
            out.push(Issue::new(
                CalloutSeverity::Warn,
                format!("store lock is {}", store.facts.lock),
            ));
        }
        if !matches!(store.facts.state, StoreState::Live { .. }) {
            out.push(Issue::new(
                CalloutSeverity::Warn,
                format!("store is {}", tag(&store.facts.state, "store")),
            ));
        }
    }
    if let Ok(ChildrenView::Up { children }) = &r.children {
        for child in children {
            if let ChildState::Dead { cause, .. } = &child.state {
                out.push(Issue::new(
                    CalloutSeverity::Error,
                    format!("child {} is dead ({cause})", child.child),
                ));
            }
        }
    }
    if let Ok(boot) = &r.boot
        && let LatestBootAttempt::Recorded(attempt) = &boot.latest
        && attempt.result == AttemptResult::Failed
    {
        out.push(Issue::new(
            CalloutSeverity::Error,
            format!("boot attempt {} failed", attempt.attempt),
        ));
    }
    if let Ok(list) = &r.kubeconfigs {
        for record in &list.records {
            if let PublishOutcome::Failed { error, .. } = &record.outcome {
                out.push(Issue::new(
                    CalloutSeverity::Error,
                    format!("kubeconfig {} not published: {error}", record.target),
                ));
            }
        }
    }
    if let Ok(drift) = &r.drift
        && tag(&drift.declared_on_disk, "") == "changed_since_load"
    {
        out.push(Issue::new(
            CalloutSeverity::Warn,
            "declared config changed on disk since it was loaded",
        ));
    }
    for (name, err) in [
        ("store", r.store.as_ref().err()),
        ("children", r.children.as_ref().err()),
        ("boot", r.boot.as_ref().err()),
        ("pki", r.pki.as_ref().err()),
        ("kubeconfigs", r.kubeconfigs.as_ref().err()),
        ("config drift", r.drift.as_ref().err()),
        ("control", r.control.as_ref().err()),
        ("events", r.events.as_ref().err()),
    ] {
        if let Some(err) = err {
            out.push(Issue::new(
                CalloutSeverity::Warn,
                format!("{name} not read: {err}"),
            ));
        }
    }
    out
}

fn lifecycle_severity(state: &LifecycleState) -> CalloutSeverity {
    face::severity(&tag(state, "state")).unwrap_or(CalloutSeverity::Warn)
}

fn render(r: &Report, issues: &[Issue]) -> std::io::Result<()> {
    let cluster = &r.hello.cluster;
    banner(["engenho · ", &cluster.cluster_name].concat()).print()?;
    verdict(issues).print()?;
    for issue in issues {
        callout(issue.severity, issue.text.clone()).print()?;
    }
    println!();
    rule_labelled("daemon").print()?;
    daemon(r).print()?;
    if let Ok(store) = &r.store {
        rule_labelled("store").print()?;
        store_panel(store).print()?;
    }
    if let Ok(children) = &r.children {
        children_section(children)?;
    }
    if let Ok(boot) = &r.boot {
        rule_labelled("boot").print()?;
        boot_panel(boot).print()?;
    }
    if let Ok(pki) = &r.pki {
        rule_labelled("pki").print()?;
        pki_panel(&serde_json::to_value(pki).unwrap_or_default()).print()?;
    }
    if let Ok(list) = &r.kubeconfigs {
        rule_labelled("kubeconfigs").print()?;
        kubeconfig_panel(list).print()?;
    }
    if let Ok(drift) = &r.drift {
        rule_labelled("config").print()?;
        drift_panel(drift).print()?;
    }
    if let Ok(control) = &r.control {
        rule_labelled("control").print()?;
        control_panel(control).print()?;
    }
    if let Ok(events) = &r.events
        && !events.is_empty()
    {
        rule_labelled("recent events").print()?;
        events_table(events).print()?;
    }
    Ok(())
}

fn verdict(issues: &[Issue]) -> Callout {
    let worst = issues
        .iter()
        .map(|i| i.severity)
        .filter(|s| *s != CalloutSeverity::Note)
        .max_by_key(|s| match s {
            CalloutSeverity::Error => 3,
            CalloutSeverity::Warn => 2,
            _ => 1,
        });
    match worst {
        None => ok("healthy"),
        Some(severity) => callout(severity, "degraded"),
    }
}

fn daemon(r: &Report) -> Panel {
    let h = &r.hello;
    let mut version = vec![Fragment::styled(h.daemon.version.clone(), Role::Ident)];
    if h.daemon.git_rev != "unknown" {
        version.push(detail([" · ", &h.daemon.git_rev].concat()));
    }
    let state = tag(&h.lifecycle, "state");
    let mut lifecycle = vec![word(state)];
    match &h.lifecycle {
        LifecycleState::Running {
            apiserver_addr,
            attempt,
            pending,
            since,
        } => {
            lifecycle.push(detail(format!(
                " · attempt {attempt} · apiserver {apiserver_addr} · {}",
                ago(*since)
            )));
            lifecycle.push(detail(" · config "));
            lifecycle.push(word(tag(pending, "kind")));
        }
        LifecycleState::Booting {
            attempt,
            phase,
            since,
        } => {
            lifecycle.push(detail(format!(
                " · attempt {attempt} · {phase} · {}",
                ago(*since)
            )));
        }
        LifecycleState::Failed {
            attempt,
            report,
            retry,
        } => {
            lifecycle.push(detail(format!(
                " · attempt {attempt} · {}: {} · retry {}",
                report.phase,
                report.error,
                tag(retry, "retry")
            )));
        }
        LifecycleState::Wedged { cause, since } => {
            lifecycle.push(detail(format!(" · {cause} · {}", ago(*since))));
        }
        LifecycleState::Stopped { reason, since, .. } => {
            lifecycle.push(detail(format!(" · {reason} · {}", ago(*since))));
        }
        _ => {}
    }
    panel()
        .row_fragments(
            "endpoint",
            vec![
                Fragment::styled(r.endpoint.clone(), Role::Text),
                detail([" (", &h.transport.to_string(), ")"].concat()),
            ],
        )
        .row("node", h.cluster.node_name.clone())
        .row_fragments("version", version)
        .row_fragments(
            "pid",
            vec![Fragment::styled(h.daemon.pid.to_string(), Role::Ident)],
        )
        .row_fragments(
            "up",
            vec![
                Fragment::styled(ago(h.daemon.started_at), Role::Text),
                detail([" · since ", &stamp(h.daemon.started_at)].concat()),
            ],
        )
        .row_fragments("lifecycle", lifecycle)
        .row_fragments("grant", grant(h))
}

fn grant(h: &Hello) -> Vec<Fragment> {
    let mut line = vec![Fragment::styled(h.grant.effective.to_string(), Role::Ident)];
    if h.grant.effective != h.grant.granted {
        line.push(detail(
            [" · granted ", &h.grant.granted.to_string()].concat(),
        ));
    }
    line
}

fn stamp(at: chrono::DateTime<chrono::Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn store_panel(store: &StoreView) -> Panel {
    let mut state = vec![word(tag(&store.facts.state, "store"))];
    if let StoreState::Live {
        kind,
        leader,
        revision,
    } = &store.facts.state
    {
        let role = if *leader { "leader" } else { "follower" };
        state.push(detail(format!(" · {kind} · {role} · revision {revision}")));
    }
    panel()
        .row("path", store.path.clone())
        .row_fragments("lock", vec![word(store.facts.lock.to_string())])
        .row_fragments("state", state)
}

fn children_section(children: &ChildrenView) -> std::io::Result<()> {
    match children {
        ChildrenView::Down { lifecycle } => {
            rule_labelled("children").print()?;
            note(["runtime is ", &tag(lifecycle, "state"), "; no children"].concat()).print()
        }
        ChildrenView::Up { children } => {
            let running = children
                .iter()
                .filter(|c| matches!(c.state, ChildState::Running { .. }))
                .count();
            rule_labelled(format!("children · {running}/{} running", children.len())).print()?;
            let mut table =
                kazari::table(["child", "kind", "state", "for", "respawn", "last death"])
                    .indented(2);
            for child in children {
                let (state, age) = match &child.state {
                    ChildState::Running { since } => ("running", ago(*since)),
                    ChildState::Dead { at, cause } => {
                        ("dead", [ago(*at), " · ".into(), cause.to_string()].concat())
                    }
                    ChildState::Disabled => ("disabled", String::new()),
                };
                let death = match &child.last_death {
                    LastDeath::Never => word("never"),
                    LastDeath::Recorded { at, cause } => Fragment::styled(
                        [cause.to_string(), " · ".into(), ago(*at), " ago".into()].concat(),
                        Role::Warn,
                    ),
                };
                table = table.row_fragments([
                    Fragment::styled(child.child.to_string(), Role::Text),
                    detail(child.kind.to_string()),
                    word(state),
                    detail(age),
                    detail(child.respawn.to_string()),
                    death,
                ]);
            }
            table.print()
        }
    }
}

fn boot_panel(boot: &BootView) -> Panel {
    let mut p = panel();
    match &boot.latest {
        LatestBootAttempt::None => p = p.row_fragments("latest", vec![word("none")]),
        LatestBootAttempt::Recorded(attempt) => {
            let done = attempt
                .phases
                .iter()
                .filter(|ph| matches!(ph.result, PhaseResult::Completed { .. }))
                .count();
            let elapsed: u64 = attempt
                .phases
                .iter()
                .map(|ph| match ph.result {
                    PhaseResult::Completed { elapsed_ms }
                    | PhaseResult::Failed { elapsed_ms, .. }
                    | PhaseResult::Cancelled { elapsed_ms } => elapsed_ms,
                    PhaseResult::InProgress => 0,
                })
                .sum();
            p = p
                .row_fragments(
                    "latest",
                    vec![
                        word(attempt.result.to_string()),
                        detail(format!(
                            " · attempt {} · {} · {}",
                            attempt.attempt,
                            tag(&attempt.kind, "kind"),
                            ago(attempt.started_at)
                        )),
                    ],
                )
                .row_fragments(
                    "phases",
                    vec![
                        Fragment::styled(format!("{done}/{}", attempt.phases.len()), Role::Ident),
                        detail(format!(" completed · {elapsed} ms summed")),
                    ],
                );
            for phase in &attempt.phases {
                if let PhaseResult::Failed { error, .. } = &phase.result {
                    p = p.row_role(
                        ["failed ", &phase.phase.to_string()].concat(),
                        error.clone(),
                        Role::Error,
                    );
                }
            }
        }
    }
    let previous = match &boot.previous_run {
        PreviousRun::FirstEver => vec![word("first_ever")],
        PreviousRun::CleanStop { at } => vec![
            word("clean_stop"),
            detail([" · ", &ago(*at), " ago"].concat()),
        ],
        PreviousRun::Unclean { last_seen } => vec![
            word("unclean"),
            detail([" · last seen ", &tag(last_seen, "")].concat()),
        ],
    };
    p.row_fragments("previous run", previous)
}

fn pki_panel(pki: &serde_json::Value) -> Panel {
    let mut p = panel();
    let Some(entries) = pki.as_object() else {
        return p;
    };
    for (name, facts) in entries {
        let status = ["ca", "cert", "file", "status", "state"]
            .iter()
            .find_map(|k| facts.get(*k).and_then(serde_json::Value::as_str))
            .unwrap_or("unknown");
        let mut line = vec![word(status)];
        if let Some(sha) = facts.get("sha256").and_then(serde_json::Value::as_str) {
            line.push(detail(
                [" · sha256:", sha.get(..12).unwrap_or(sha), "…"].concat(),
            ));
        }
        if let Some(until) = facts.get("not_after").and_then(serde_json::Value::as_str) {
            line.push(detail([" · until ", until].concat()));
        }
        if let Some(sans) = facts.get("sans").and_then(serde_json::Value::as_array) {
            line.push(detail(format!(" · {} SANs", sans.len())));
        }
        if let Some(mode) = facts.get("mode").and_then(serde_json::Value::as_str) {
            line.push(detail([" · ", mode].concat()));
        }
        p = p.row_fragments(name.clone(), line);
    }
    p
}

fn kubeconfig_panel(list: &KubeconfigList) -> Panel {
    list.records.iter().fold(panel(), |p, record| {
        let line = match &record.outcome {
            PublishOutcome::Written { path, mode } => vec![
                word("written"),
                detail([" · ", path.as_str(), " · ", mode.as_str()].concat()),
            ],
            PublishOutcome::Failed { path, error } => vec![
                word("failed"),
                detail([" · ", path.as_str(), " · ", error.as_str()].concat()),
            ],
            PublishOutcome::Skipped { reason } => vec![
                word("skipped"),
                detail([" · ", &reason.to_string()].concat()),
            ],
        };
        p.row_fragments(record.target.to_string(), line)
    })
}

fn drift_panel(drift: &DriftReport) -> Panel {
    let applied = match &drift.applied {
        PendingApply::InSync => vec![word("in_sync")],
        PendingApply::RestartNeeded(leaves) => vec![
            word("restart_needed"),
            detail(format!(" · {} leaves", leaves.len())),
        ],
    };
    panel()
        .row_fragments("applied", applied)
        .row_fragments(
            "declared on disk",
            vec![word(drift.declared_on_disk.to_string())],
        )
        .row_fragments(
            "drifted leaves",
            vec![Fragment::styled(
                drift.leaves.len().to_string(),
                Role::Ident,
            )],
        )
}

fn control_panel(control: &ControlView) -> Panel {
    let value = serde_json::to_value(control).unwrap_or_default();
    let remote = &value["remote"];
    let mut remote_line = vec![word(remote["listener"].as_str().unwrap_or("unknown"))];
    if let Some(addr) = remote["addr"].as_str() {
        remote_line.push(detail([" · ", addr].concat()));
    }
    if let Some(absence) = remote["absence"]["absence"].as_str() {
        remote_line.push(detail([" · ", absence].concat()));
    }
    let identity = &value["identity"];
    let mut identity_line = vec![word(identity["identity"].as_str().unwrap_or("unknown"))];
    if let Some(spki) = identity["spki"].as_str() {
        identity_line.push(detail(
            [" · ", spki.get(..19).unwrap_or(spki), "…"].concat(),
        ));
    }
    panel()
        .row_fragments(
            "socket",
            vec![
                Fragment::styled(control.uds.path.clone(), Role::Text),
                detail(
                    [
                        " · access ",
                        value["uds"]["access"].as_str().unwrap_or("?"),
                        " · group ",
                        &control.uds.group_tier.to_string(),
                    ]
                    .concat(),
                ),
            ],
        )
        .row_fragments("remote", remote_line)
        .row_fragments("identity", identity_line)
        .row_fragments(
            "authorized",
            vec![
                Fragment::styled(control.authorized_clients.len().to_string(), Role::Ident),
                detail(" clients"),
            ],
        )
}

fn events_table(events: &[ControlEvent]) -> Table {
    events.iter().rev().fold(
        kazari::table(["seq", "ago", "event", "detail"]).indented(2),
        |t, event| {
            let (name, line) = event_line(&event.kind);
            t.row_fragments([
                Fragment::styled(event.seq.to_string(), Role::TextDim),
                detail(ago(event.at)),
                Fragment::styled(name, Role::Text),
                line,
            ])
        },
    )
}

fn event_line(kind: &ControlEventKind) -> (&'static str, Fragment) {
    match kind {
        ControlEventKind::Lifecycle { to } => ("lifecycle", word(tag(to, "state"))),
        ControlEventKind::BootPhase {
            attempt,
            phase,
            result,
        } => {
            let r = tag(result, "result");
            (
                "boot_phase",
                Fragment::styled(format!("{phase} {r} (attempt {attempt})"), face::role(&r)),
            )
        }
        ControlEventKind::ChildDied { cause, child } => (
            "child_died",
            Fragment::styled(format!("{child} {cause}"), Role::Error),
        ),
        ControlEventKind::ChildRespawned { child, generation } => (
            "child_respawned",
            Fragment::styled(format!("{child} generation {generation}"), Role::Warn),
        ),
        ControlEventKind::ConfigApplied {
            effect,
            generation,
            leaves,
        } => (
            "config_applied",
            detail(format!(
                "{} · generation {generation} · {} leaves",
                tag(effect, "effect"),
                leaves.len()
            )),
        ),
        ControlEventKind::ReinitExecuted { operation } => (
            "reinit_executed",
            Fragment::styled(operation.to_string(), Role::Warn),
        ),
        ControlEventKind::KubeconfigPublished { record } => {
            let s = tag(&record.outcome, "status");
            (
                "kubeconfig_published",
                Fragment::styled(format!("{} {s}", record.target), face::role(&s)),
            )
        }
        ControlEventKind::RemoteListener { listener } => {
            ("remote_listener", word(tag(listener, "listener")))
        }
    }
}

fn json(r: &Report, issues: &[Issue]) -> serde_json::Value {
    fn part<T: Serialize>(section: &Section<T>) -> serde_json::Value {
        match section {
            Ok(v) => serde_json::to_value(v).unwrap_or_default(),
            Err(err) => serde_json::json!({ "error": err }),
        }
    }
    serde_json::json!({
        "endpoint": r.endpoint,
        "healthy": issues.iter().all(|i| i.severity == CalloutSeverity::Note),
        "issues": issues.iter().map(|i| serde_json::json!({ "severity": i.severity.tag().to_lowercase(), "text": i.text })).collect::<Vec<_>>(),
        "hello": serde_json::to_value(&r.hello).unwrap_or_default(),
        "store": part(&r.store),
        "children": part(&r.children),
        "boot": part(&r.boot),
        "pki": part(&r.pki),
        "kubeconfigs": part(&r.kubeconfigs),
        "config_drift": part(&r.drift),
        "control": part(&r.control),
        "events": part(&r.events),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<StatusCommand, CtlUsage> {
        StatusCommand::parse(line.split_whitespace().map(str::to_owned))
    }

    #[test]
    fn flags_select_the_daemon_and_the_output() {
        assert_eq!(parse("").unwrap().endpoint, Endpoint::Local(None));
        let c = parse("--remote plo --json --events 3").unwrap();
        assert_eq!(c.endpoint, Endpoint::Remote("plo".into()));
        assert!(c.json);
        assert_eq!(c.events, 3);
        assert_eq!(
            parse("--socket /x.sock").unwrap().endpoint,
            Endpoint::Local(Some("/x.sock".into()))
        );
    }

    #[test]
    fn mistakes_are_usage_errors() {
        assert_eq!(
            parse("--remote plo --socket /x"),
            Err(CtlUsage::TwoEndpoints)
        );
        assert!(matches!(parse("--bogus"), Err(CtlUsage::UnknownFlag(_))));
        assert!(matches!(
            parse("--events many"),
            Err(CtlUsage::NeedsValue(_))
        ));
    }

    #[test]
    fn the_verdict_is_the_worst_issue() {
        assert_eq!(verdict(&[]).severity, CalloutSeverity::Ok);
        let issues = [
            Issue::new(CalloutSeverity::Warn, "w"),
            Issue::new(CalloutSeverity::Error, "e"),
        ];
        assert_eq!(verdict(&issues).severity, CalloutSeverity::Error);
        assert_eq!(
            verdict(&[Issue::new(CalloutSeverity::Note, "n")]).severity,
            CalloutSeverity::Ok
        );
    }
}
