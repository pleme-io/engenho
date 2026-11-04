//! AUDIT LOGGING — the record of who did what to the API.
//!
//! ★ WHY ITS ABSENCE IS A COMPLIANCE GAP AND NOT A MISSING NICETY. Every
//! serious deployment has to answer "who deleted that Secret, and when".
//! Without an audit trail the answer is unavailable — not hard to find,
//! genuinely absent — and no amount of after-the-fact investigation
//! recovers it. It is also the one control that most compliance regimes
//! name explicitly, so a distribution without it is disqualified from
//! environments it is otherwise perfectly capable of running.
//!
//! ★ NO BODY CAN REACH THE LOG. The request body of a Secret create IS the
//! Secret, and the response of a `serviceaccounts/<n>/token` create IS a
//! bearer token. An [`Event`] has no field that could hold either, and
//! [`Level`] has no body-capturing variant, so a policy cannot ask for one.
//! The query string stays out as well (`requestURI` is the path alone), and
//! so does the `Authorization` header.
//!
//! ★ AN AUDIT EVENT IS EMITTED EVEN WHEN THE REQUEST FAILED. A denied
//! delete is precisely the thing an investigator needs to see, and a log
//! that only records successes answers the wrong question. A request refused
//! at the boundary (unclassifiable, unauthenticated or unauthorized) is
//! recorded whatever it asked for.
//!
//! ★ THE LEVEL IS DECIDED IN BLUE (`blue/audit/audit.b`). It is pure data, so
//! the program loads under an empty posture and reaches nothing. When it
//! cannot answer, the level is [`Level::Metadata`]: a failure of the policy
//! records more, never less.
//!
//! ★ THE LOG NEVER FAILS A REQUEST. [`AuditLog::record`] hands the line to one
//! writer thread over a bounded queue, so a slow disk cannot stall the API. A
//! line that cannot be queued or written is counted, warned about and lost.
//! Rotation bounds the disk at `(kept_files + 1) * max_file_bytes`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{self, Write as _};
use std::os::unix::fs::{
    DirBuilderExt as _, FileExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, SecondsFormat, Utc};
use engenho_blue::BlueWorker;
use engenho_types::auth::UserInfo;
use serde::{Deserialize, Serialize};

const FILES: &[(&str, &str)] = &[("audit/audit.b", include_str!("../blue/audit/audit.b"))];
const ENTRY: &str = "use(\"audit\")\n";
const LOG_FILE: &str = "audit.log";
const QUEUE_LINES: usize = 4096;
const LEVEL_CACHE_ENTRIES: usize = 1024;

/// How much is recorded about one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Level {
    /// Do not record this request.
    None,
    /// Record who, what, when and the outcome.
    Metadata,
}

/// The stage of request handling an event describes, as upstream names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Stage {
    /// The response has begun and stays open: a watch or an upgraded session.
    ResponseStarted,
    /// The response is complete.
    ResponseComplete,
}

/// What the authorizer said, under `authorization.k8s.io/decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authorization {
    /// The request was authorized.
    Allow,
    /// The request was refused.
    Forbid,
}

impl Authorization {
    fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Forbid => "forbid",
        }
    }
}

/// The resource a request names.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef<'a> {
    /// The resource plural.
    pub resource: &'a str,
    /// The subresource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<&'a str>,
    /// The namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<&'a str>,
    /// The object's name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
    /// The API group; empty for core.
    pub api_group: &'a str,
    /// The API version.
    pub api_version: &'a str,
}

/// What is being audited.
#[derive(Debug, Clone)]
pub struct AuditRequest<'a> {
    /// The RBAC verb, or the lowercased method of an unclassifiable request.
    pub verb: &'a str,
    /// The request path, without its query string.
    pub path: &'a str,
    /// The resource, for a resource request.
    pub object: Option<ObjectRef<'a>>,
    /// Who asked. `None` when authentication refused the request.
    pub user: Option<&'a UserInfo>,
    /// What authorization said. `None` when it never ran.
    pub authorization: Option<Authorization>,
    /// Whether the request could be classified at all.
    pub classified: bool,
    /// HTTP status the request produced.
    pub response_code: u16,
    /// Which stage this record describes.
    pub stage: Stage,
    /// When the request arrived.
    pub received: DateTime<Utc>,
}

impl AuditRequest<'_> {
    fn refused(&self) -> bool {
        !self.classified
            || self.authorization == Some(Authorization::Forbid)
            || matches!(self.response_code, 401 | 403)
    }
}

#[derive(Serialize)]
struct LevelFacts<'a> {
    verb: &'a str,
    resource: &'a str,
    subresource: Option<&'a str>,
    refused: bool,
}

#[derive(Serialize)]
struct EventUser<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uid: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<&'a [String]>,
}

#[derive(Serialize)]
struct ResponseStatus {
    code: u16,
}

#[derive(Serialize)]
struct Annotations {
    #[serde(rename = "authorization.k8s.io/decision")]
    decision: &'static str,
}

/// One `audit.k8s.io/v1` Event. It has no field for a body.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event<'a> {
    kind: &'static str,
    api_version: &'static str,
    level: Level,
    #[serde(rename = "auditID")]
    audit_id: &'a str,
    stage: Stage,
    #[serde(rename = "requestURI")]
    request_uri: &'a str,
    verb: &'a str,
    user: EventUser<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_ref: Option<ObjectRef<'a>>,
    response_status: ResponseStatus,
    request_received_timestamp: String,
    stage_timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<Annotations>,
}

fn timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Render one audit event.
///
/// Returns `None` at [`Level::None`] — the caller must not write a line, and
/// returning an empty event would put an unlabelled record in the log.
#[must_use]
pub fn event<'a>(
    level: Level,
    req: &AuditRequest<'a>,
    audit_id: &'a str,
    finished: DateTime<Utc>,
) -> Option<Event<'a>> {
    if level == Level::None {
        return None;
    }
    Some(Event {
        kind: "Event",
        api_version: "audit.k8s.io/v1",
        level,
        audit_id,
        stage: req.stage,
        request_uri: req.path,
        verb: req.verb,
        user: EventUser {
            username: req.user.map(|u| u.username.as_str()),
            uid: req.user.map(|u| u.uid.as_str()),
            groups: req.user.map(|u| u.groups.as_slice()),
        },
        object_ref: req.object,
        response_status: ResponseStatus {
            code: req.response_code,
        },
        request_received_timestamp: timestamp(req.received),
        stage_timestamp: timestamp(finished),
        annotations: req.authorization.map(|a| Annotations {
            decision: a.as_str(),
        }),
    })
}

/// How large the log may grow.
#[derive(Debug, Clone, Copy)]
pub struct AuditLimits {
    /// The size at which `audit.log` rotates.
    pub max_file_bytes: u64,
    /// How many rotated files are kept.
    pub kept_files: u32,
}

impl Default for AuditLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 64 * 1024 * 1024,
            kept_files: 4,
        }
    }
}

#[derive(Default)]
struct Counters {
    written: AtomicU64,
    write_failures: AtomicU64,
    dropped: AtomicU64,
    decision_failures: AtomicU64,
}

/// A reading of the log's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditCounts {
    /// Events written to the file.
    pub written: u64,
    /// Events lost because the write failed.
    pub write_failures: u64,
    /// Events lost because the queue was full.
    pub dropped: u64,
    /// Requests whose level the policy could not decide, recorded at
    /// [`Level::Metadata`].
    pub decision_failures: u64,
}

fn warn_on_powers_of_two(count: u64, what: &str, why: &dyn std::fmt::Display) {
    if count.is_power_of_two() {
        tracing::warn!(count, reason = %why, "api audit log: {what}");
    }
}

/// The `audit` program, and what it has already answered.
struct Levels {
    program: Result<BlueWorker, String>,
    answered: RwLock<HashMap<String, Level>>,
}

impl Levels {
    fn start(files: &'static [(&'static str, &'static str)]) -> Self {
        let program = BlueWorker::start("audit", ENTRY, files);
        if let Err(e) = &program {
            tracing::error!(error = %e, "api audit policy did not load; recording every request");
        }
        Self {
            program,
            answered: RwLock::new(HashMap::new()),
        }
    }

    fn cached(&self, facts: &str) -> Option<Level> {
        self.answered
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(facts)
            .copied()
    }

    fn remember(&self, facts: String, level: Level) {
        let mut answered = self
            .answered
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if answered.len() >= LEVEL_CACHE_ENTRIES {
            answered.clear();
        }
        answered.insert(facts, level);
    }

    async fn decide(&self, facts: &LevelFacts<'_>) -> Result<Level, String> {
        let text = serde_json::to_string(facts).map_err(|e| e.to_string())?;
        if let Some(level) = self.cached(&text) {
            return Ok(level);
        }
        let program = self.program.as_ref().map_err(Clone::clone)?;
        let level = program.ask::<Level>("audit::level", text.clone()).await?;
        self.remember(text, level);
        Ok(level)
    }
}

enum Message {
    Line(String),
    Flush(mpsc::Sender<()>),
}

struct Writer {
    dir: PathBuf,
    limits: AuditLimits,
    file: Option<File>,
    size: u64,
}

impl Writer {
    fn path(&self) -> PathBuf {
        self.dir.join(LOG_FILE)
    }

    fn rotated(&self, n: u32) -> PathBuf {
        self.dir.join(format!("{LOG_FILE}.{n}"))
    }

    fn open(&mut self) -> io::Result<()> {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.dir)?;
        std::fs::set_permissions(&self.dir, Permissions::from_mode(0o700))?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .open(self.path())?;
        file.set_permissions(Permissions::from_mode(0o600))?;
        let mut size = file.metadata()?.len();
        if size > 0 {
            let mut last = [0u8; 1];
            file.read_exact_at(&mut last, size - 1)?;
            if last != *b"\n" {
                file.write_all(b"\n")?;
                size += 1;
            }
        }
        self.file = Some(file);
        self.size = size;
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.file = None;
        for n in (1..self.limits.kept_files).rev() {
            rename_if_present(&self.rotated(n), &self.rotated(n + 1))?;
        }
        if self.limits.kept_files == 0 {
            std::fs::remove_file(self.path())?;
        } else {
            rename_if_present(&self.path(), &self.rotated(1))?;
        }
        self.open()
    }

    fn write(&mut self, line: &str) -> io::Result<()> {
        let bytes = line.len() as u64 + 1;
        if bytes > self.limits.max_file_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the event is larger than a whole log file",
            ));
        }
        if self.file.is_none() {
            self.open()?;
        }
        if self.size + bytes > self.limits.max_file_bytes {
            self.rotate()?;
        }
        let mut buffer = Vec::with_capacity(line.len() + 1);
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
        let Some(file) = self.file.as_mut() else {
            return Err(io::Error::other("the log file is not open"));
        };
        file.write_all(&buffer)?;
        self.size += bytes;
        Ok(())
    }

    fn run(mut self, inbox: &mpsc::Receiver<Message>, counters: &Counters) {
        for message in inbox {
            match message {
                Message::Line(line) => match self.write(&line) {
                    Ok(()) => {
                        counters.written.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.file = None;
                        let count = counters.write_failures.fetch_add(1, Ordering::Relaxed) + 1;
                        warn_on_powers_of_two(count, "an event could not be written", &e);
                    }
                },
                Message::Flush(done) => {
                    let _ = done.send(());
                }
            }
        }
    }
}

fn rename_if_present(from: &Path, to: &Path) -> io::Result<()> {
    match std::fs::rename(from, to) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

fn process_id_prefix() -> String {
    let mut bytes = [0u8; 8];
    if getrandom::getrandom(&mut bytes).is_err() {
        bytes = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_be_bytes();
    }
    format!("{:016x}", u64::from_be_bytes(bytes))
}

/// The API audit log: JSON lines of `audit.k8s.io/v1` Events under one
/// directory, mode 0600 in a directory of mode 0700.
pub struct AuditLog {
    lines: SyncSender<Message>,
    levels: Levels,
    counters: Arc<Counters>,
    id_prefix: String,
    next_id: AtomicU64,
}

impl AuditLog {
    /// Start a log under `dir`. Nothing here fails: a directory that cannot be
    /// written yet is counted at each write, and retried at the next.
    #[must_use]
    pub fn open(dir: PathBuf, limits: AuditLimits) -> Self {
        Self::with_policy(dir, limits, FILES)
    }

    fn with_policy(
        dir: PathBuf,
        limits: AuditLimits,
        policy: &'static [(&'static str, &'static str)],
    ) -> Self {
        let (lines, inbox) = mpsc::sync_channel(QUEUE_LINES);
        let counters = Arc::new(Counters::default());
        let writer = Writer {
            dir,
            limits,
            file: None,
            size: 0,
        };
        let writer_counters = Arc::clone(&counters);
        let spawned = std::thread::Builder::new()
            .name("api-audit".into())
            .spawn(move || writer.run(&inbox, &writer_counters));
        if let Err(e) = spawned {
            tracing::error!(error = %e, "api audit writer did not start");
        }
        Self {
            lines,
            levels: Levels::start(policy),
            counters,
            id_prefix: process_id_prefix(),
            next_id: AtomicU64::new(0),
        }
    }

    async fn level(&self, req: &AuditRequest<'_>) -> Level {
        let facts = LevelFacts {
            verb: req.verb,
            resource: req.object.map_or("", |o| o.resource),
            subresource: req.object.and_then(|o| o.subresource),
            refused: req.refused(),
        };
        match self.levels.decide(&facts).await {
            Ok(level) => level,
            Err(e) => {
                let count = self
                    .counters
                    .decision_failures
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                warn_on_powers_of_two(count, "the policy could not decide a level", &e);
                Level::Metadata
            }
        }
    }

    /// Record one request, at the level the policy gives it. Never fails the
    /// caller: what cannot be recorded is counted.
    pub async fn record(&self, req: &AuditRequest<'_>) {
        let level = self.level(req).await;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let audit_id = format!("{}-{id:016x}", self.id_prefix);
        let Some(event) = event(level, req, &audit_id, Utc::now()) else {
            return;
        };
        let line = match serde_json::to_string(&event) {
            Ok(line) => line,
            Err(e) => {
                let count = self.counters.write_failures.fetch_add(1, Ordering::Relaxed) + 1;
                warn_on_powers_of_two(count, "an event could not be rendered", &e);
                return;
            }
        };
        match self.lines.try_send(Message::Line(line)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.count_dropped(&"the queue is full"),
            Err(TrySendError::Disconnected(_)) => self.count_dropped(&"the writer stopped"),
        }
    }

    fn count_dropped(&self, why: &dyn std::fmt::Display) {
        let count = self.counters.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        warn_on_powers_of_two(count, "an event was dropped", why);
    }

    /// Block until everything queued so far has been written or counted.
    pub fn flush(&self) {
        let (done, finished) = mpsc::channel();
        if self.lines.send(Message::Flush(done)).is_ok() {
            let _ = finished.recv();
        }
    }

    /// The counters, for `/metrics`.
    #[must_use]
    pub fn counts(&self) -> AuditCounts {
        AuditCounts {
            written: self.counters.written.load(Ordering::Relaxed),
            write_failures: self.counters.write_failures.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            decision_failures: self.counters.decision_failures.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const BROKEN_POLICY: &[(&str, &str)] = &[("audit/audit.b", "def level(")];

    fn user() -> UserInfo {
        UserInfo {
            username: "alice".into(),
            uid: "u1".into(),
            groups: vec!["system:authenticated".into()],
            extra: std::collections::BTreeMap::new(),
        }
    }

    fn object(resource: &'static str, subresource: Option<&'static str>) -> ObjectRef<'static> {
        ObjectRef {
            resource,
            subresource,
            namespace: Some("default"),
            name: Some("x"),
            api_group: "",
            api_version: "v1",
        }
    }

    fn req<'a>(verb: &'a str, object: ObjectRef<'a>, user: &'a UserInfo) -> AuditRequest<'a> {
        AuditRequest {
            verb,
            path: "/api/v1/namespaces/default/things/x",
            object: Some(object),
            user: Some(user),
            authorization: Some(Authorization::Allow),
            classified: true,
            response_code: 200,
            stage: Stage::ResponseComplete,
            received: Utc::now(),
        }
    }

    fn log_in(dir: &Path) -> AuditLog {
        AuditLog::open(dir.join("audit"), AuditLimits::default())
    }

    fn lines_in(dir: &Path) -> Vec<Value> {
        std::fs::read_to_string(dir.join("audit").join(LOG_FILE))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).expect("every line is one JSON object"))
            .collect()
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("exists")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn an_event_has_the_shape_of_an_upstream_event() {
        let u = user();
        let r = req("create", object("pods", Some("status")), &u);
        let e = event(Level::Metadata, &r, "aid-1", Utc::now()).expect("recorded");
        let ev = serde_json::to_value(&e).expect("renders");
        assert_eq!(ev["kind"], "Event");
        assert_eq!(ev["apiVersion"], "audit.k8s.io/v1");
        assert_eq!(ev["level"], "Metadata");
        assert_eq!(ev["auditID"], "aid-1");
        assert_eq!(ev["stage"], "ResponseComplete");
        assert_eq!(ev["requestURI"], r.path);
        assert_eq!(ev["verb"], "create");
        assert_eq!(ev["user"]["username"], "alice");
        assert_eq!(ev["user"]["uid"], "u1");
        assert_eq!(ev["user"]["groups"][0], "system:authenticated");
        assert_eq!(ev["objectRef"]["resource"], "pods");
        assert_eq!(ev["objectRef"]["subresource"], "status");
        assert_eq!(ev["objectRef"]["namespace"], "default");
        assert_eq!(ev["objectRef"]["name"], "x");
        assert_eq!(ev["objectRef"]["apiGroup"], "");
        assert_eq!(ev["objectRef"]["apiVersion"], "v1");
        assert_eq!(ev["responseStatus"]["code"], 200);
        assert_eq!(ev["annotations"]["authorization.k8s.io/decision"], "allow");
        assert!(
            ev["requestReceivedTimestamp"]
                .as_str()
                .is_some_and(|t| t.ends_with('Z'))
        );
        assert!(ev["stageTimestamp"].is_string());
    }

    #[test]
    fn level_none_renders_no_event() {
        let u = user();
        let r = req("get", object("pods", None), &u);
        assert!(event(Level::None, &r, "aid", Utc::now()).is_none());
    }

    #[test]
    fn a_request_authentication_refused_names_no_user_and_no_decision() {
        let u = user();
        let mut r = req("delete", object("secrets", None), &u);
        r.user = None;
        r.authorization = None;
        r.response_code = 401;
        let e = event(Level::Metadata, &r, "aid", Utc::now()).expect("recorded");
        let ev = serde_json::to_value(&e).expect("renders");
        assert_eq!(ev["user"], serde_json::json!({}));
        assert!(ev.get("annotations").is_none());
    }

    #[tokio::test]
    async fn the_policy_program_records_writes_credentials_sessions_and_refusals() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_in(dir.path());
        let u = user();
        let at = |verb, resource, sub| req(verb, object(resource, sub), &u);
        let mut denied = at("get", "pods", None);
        denied.authorization = Some(Authorization::Forbid);
        denied.response_code = 403;
        let mut unclassified = at("get", "pods", None);
        unclassified.classified = false;
        let mut unauthenticated = at("get", "pods", None);
        unauthenticated.authorization = None;
        unauthenticated.response_code = 401;
        for (r, want) in [
            (at("get", "pods", None), Level::None),
            (at("list", "pods", None), Level::None),
            (at("watch", "pods", None), Level::None),
            (at("create", "pods", None), Level::Metadata),
            (at("patch", "widgets", None), Level::Metadata),
            (at("get", "secrets", None), Level::Metadata),
            (at("list", "secrets", None), Level::Metadata),
            (at("get", "pods", Some("exec")), Level::Metadata),
            (at("get", "pods", Some("portforward")), Level::Metadata),
            (
                at("create", "serviceaccounts", Some("token")),
                Level::Metadata,
            ),
            (denied, Level::Metadata),
            (unclassified, Level::Metadata),
            (unauthenticated, Level::Metadata),
        ] {
            assert_eq!(log.level(&r).await, want, "{r:?}");
        }
        assert_eq!(log.counts().decision_failures, 0);
    }

    #[tokio::test]
    async fn a_policy_that_cannot_answer_records_the_request_rather_than_losing_it() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::with_policy(
            dir.path().join("audit"),
            AuditLimits::default(),
            BROKEN_POLICY,
        );
        let u = user();
        log.record(&req("get", object("pods", None), &u)).await;
        log.flush();
        assert_eq!(lines_in(dir.path()).len(), 1);
        assert_eq!(log.counts().decision_failures, 1);
    }

    #[tokio::test]
    async fn a_decision_is_answered_once_and_the_cache_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_in(dir.path());
        let facts = LevelFacts {
            verb: "create",
            resource: "pods",
            subresource: None,
            refused: false,
        };
        assert_eq!(log.levels.decide(&facts).await, Ok(Level::Metadata));
        let key = serde_json::to_string(&facts).unwrap();
        assert_eq!(log.levels.cached(&key), Some(Level::Metadata));
        for n in 0..=(LEVEL_CACHE_ENTRIES * 2) {
            log.levels.remember(format!("resource-{n}"), Level::None);
        }
        let held = log.levels.answered.read().unwrap().len();
        assert!(held <= LEVEL_CACHE_ENTRIES, "held {held}");
    }

    #[tokio::test]
    async fn the_file_is_private_and_a_loose_one_is_tightened() {
        let dir = tempfile::tempdir().unwrap();
        let audit = dir.path().join("audit");
        std::fs::create_dir(&audit).unwrap();
        std::fs::set_permissions(&audit, Permissions::from_mode(0o755)).unwrap();
        std::fs::write(audit.join(LOG_FILE), "").unwrap();
        std::fs::set_permissions(audit.join(LOG_FILE), Permissions::from_mode(0o644)).unwrap();
        let log = log_in(dir.path());
        let u = user();
        log.record(&req("create", object("pods", None), &u)).await;
        log.flush();
        assert_eq!(mode_of(&audit), 0o700);
        assert_eq!(mode_of(&audit.join(LOG_FILE)), 0o600);
        assert_eq!(lines_in(dir.path()).len(), 1);
    }

    #[tokio::test]
    async fn a_fresh_file_is_created_private() {
        let dir = tempfile::tempdir().unwrap();
        let log = log_in(dir.path());
        let u = user();
        log.record(&req("create", object("pods", None), &u)).await;
        log.flush();
        assert_eq!(mode_of(&dir.path().join("audit").join(LOG_FILE)), 0o600);
    }

    #[tokio::test]
    async fn rotation_bounds_the_disk_and_keeps_the_newest_events() {
        let dir = tempfile::tempdir().unwrap();
        let audit = dir.path().join("audit");
        let limits = AuditLimits {
            max_file_bytes: 4096,
            kept_files: 2,
        };
        let log = AuditLog::open(audit.clone(), limits);
        let u = user();
        for _ in 0..500 {
            log.record(&req("create", object("pods", None), &u)).await;
        }
        log.flush();
        assert_eq!(log.counts().written, 500);
        let mut total = 0;
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&audit).unwrap() {
            let entry = entry.unwrap();
            let size = entry.metadata().unwrap().len();
            assert!(size <= limits.max_file_bytes, "{entry:?} is {size} bytes");
            assert_eq!(mode_of(&entry.path()), 0o600);
            total += size;
            names.push(entry.file_name().into_string().unwrap());
        }
        names.sort();
        assert_eq!(names, ["audit.log", "audit.log.1", "audit.log.2"]);
        assert!(total <= limits.max_file_bytes * 3);
        let newest = std::fs::read_to_string(audit.join(LOG_FILE)).unwrap();
        assert!(newest.ends_with('\n'));
        assert!(newest.contains(&format!("{:016x}", 499)));
    }

    #[tokio::test]
    async fn a_write_failure_is_counted_and_the_log_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let audit = dir.path().join("audit");
        std::fs::write(&audit, "a file where the directory goes").unwrap();
        let log = AuditLog::open(audit.clone(), AuditLimits::default());
        let u = user();
        log.record(&req("create", object("pods", None), &u)).await;
        log.flush();
        assert_eq!(log.counts().write_failures, 1);
        assert_eq!(log.counts().written, 0);
        std::fs::remove_file(&audit).unwrap();
        log.record(&req("create", object("pods", None), &u)).await;
        log.flush();
        assert_eq!(log.counts().write_failures, 1);
        assert_eq!(log.counts().written, 1);
        assert_eq!(lines_in(dir.path()).len(), 1);
    }

    #[tokio::test]
    async fn a_torn_last_line_does_not_swallow_the_next_event() {
        let dir = tempfile::tempdir().unwrap();
        let audit = dir.path().join("audit");
        std::fs::create_dir(&audit).unwrap();
        std::fs::write(audit.join(LOG_FILE), "{\"kind\":\"Ev").unwrap();
        let log = log_in(dir.path());
        let u = user();
        log.record(&req("create", object("pods", None), &u)).await;
        log.flush();
        let text = std::fs::read_to_string(audit.join(LOG_FILE)).unwrap();
        let last = text.lines().last().unwrap();
        let ev: Value = serde_json::from_str(last).expect("the new event is its own line");
        assert_eq!(ev["verb"], "create");
    }

    #[test]
    fn an_event_larger_than_a_file_is_refused_rather_than_breaking_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = Writer {
            dir: dir.path().join("audit"),
            limits: AuditLimits {
                max_file_bytes: 8,
                kept_files: 1,
            },
            file: None,
            size: 0,
        };
        assert!(writer.write("longer than eight bytes").is_err());
        assert!(writer.write("short").is_ok());
    }
}
