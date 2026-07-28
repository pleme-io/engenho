//! # AuditLog — typed event stream for every mutation
//!
//! Every apply/get/list/delete/watch verb call against a wrapped
//! backend emits a typed [`AuditEvent`] to an [`AuditLog`] sink.
//! Universal observability without per-backend instrumentation —
//! ANY [`StoreBackend`] gets audit-trail behavior by wrapping in
//! [`AuditingBackend`].
//!
//! ## Why a separate primitive
//!
//! Three concerns each backend would otherwise re-implement:
//!
//! 1. **Compliance:** regulators want a tamper-evident log of
//!    every write against the cluster (who applied what when).
//! 2. **Debugging:** "what state changes did the operator make in
//!    the last hour?" needs a unified event stream.
//! 3. **Rollback:** the audit log is the source-of-truth for
//!    replay-based recovery.
//!
//! Per the prime-directive third-site rule: instead of every
//! backend tracking its own audit state, one decorator wraps
//! any backend + any sink.
//!
//! ## Architecture
//!
//! ```text
//!  AuditingBackend<B, L>      composes any backend B + any sink L
//!         │
//!         ├── B: StoreBackend  (in-memory / filesystem / raft / etc.)
//!         └── L: AuditLog      (Noop / InMemory / File)
//! ```
//!
//! ## Operator pattern
//!
//! ```rust,ignore
//! use engenho_revoada::audit::{AuditingBackend, FileAuditLog};
//! use engenho_revoada::FileSystemBackend;
//!
//! let storage = FileSystemBackend::open("/var/lib/engenho", "prod")?;
//! let audit = FileAuditLog::open("/var/log/engenho/audit.jsonl")?;
//! let backend: Box<dyn StoreBackend> =
//!     Box::new(AuditingBackend::new(storage, audit));
//! ```

use std::sync::Mutex;
use std::time::SystemTime;

use shigoto_types::sink::{AuditFileSink, NullSink, Sink};

use crate::backend::StoreBackend;
use crate::face::{FaceError, FaceWatchStream, ResourceFormat, ResourceRef};

/// Which verb was called. Maps 1:1 onto the [`StoreBackend`]
/// trait's verb surface plus snapshot/restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerbKind {
    Apply,
    Get,
    List,
    Delete,
    Watch,
    Snapshot,
    Restore,
}

impl VerbKind {
    /// True if this verb mutates state (apply / delete / restore).
    /// Audit pipelines often filter to mutations-only.
    #[must_use]
    pub fn is_mutation(self) -> bool {
        matches!(self, VerbKind::Apply | VerbKind::Delete | VerbKind::Restore)
    }
}

/// A single audited verb invocation.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AuditEvent {
    /// Wall-clock time the verb was invoked (epoch seconds).
    pub timestamp_s: u64,
    /// Wall-clock fractional nanoseconds (for ordering events that
    /// happen in the same second).
    pub timestamp_ns: u32,
    /// Which verb.
    pub verb: VerbKind,
    /// Optional resource ref (None for list/watch/snapshot).
    pub reference: Option<ResourceRef>,
    /// Format the operator requested.
    pub format: Option<ResourceFormat>,
    /// For list/watch: the kind + namespace filter.
    pub kind_filter: Option<String>,
    pub namespace_filter: Option<String>,
    /// True iff the verb returned Ok.
    pub success: bool,
    /// Error message (when `success = false`).
    pub error_message: Option<String>,
    /// Body byte length, when applicable. Don't log the body itself
    /// (operator might apply secrets); length is enough for
    /// "did the operator write a 0-byte something" debugging.
    pub body_bytes: Option<usize>,
}

impl AuditEvent {
    /// Build an event with the current wall-clock timestamp.
    ///
    /// Preserves nanosecond precision via `SystemTime`. For typed
    /// substrate-Clock construction (loses ns precision in exchange
    /// for FrozenClock determinism in tests), use [`Self::at`].
    #[must_use]
    pub fn now(verb: VerbKind) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        Self::with_timestamp(now.as_secs(), now.subsec_nanos(), verb)
    }

    /// Build an event using a typed `engenho_substrate::Clock` for the
    /// timestamp source. `timestamp_ns` is set to 0 — substrate Clock
    /// is ms-precision. Tests should reach for this with a
    /// `FrozenClock` to get byte-deterministic audit-log payloads.
    #[must_use]
    pub fn at<C: engenho_substrate::Clock + ?Sized>(clock: &C, verb: VerbKind) -> Self {
        Self::with_timestamp(clock.unix_secs(), 0, verb)
    }

    /// Shared constructor — both `now()` and `at()` route through here.
    fn with_timestamp(timestamp_s: u64, timestamp_ns: u32, verb: VerbKind) -> Self {
        Self {
            timestamp_s,
            timestamp_ns,
            verb,
            reference: None,
            format: None,
            kind_filter: None,
            namespace_filter: None,
            success: true,
            error_message: None,
            body_bytes: None,
        }
    }

    /// Attach a resource reference.
    #[must_use]
    pub fn with_ref(mut self, r: ResourceRef) -> Self {
        self.reference = Some(r);
        self
    }

    /// Attach a format.
    #[must_use]
    pub fn with_format(mut self, f: ResourceFormat) -> Self {
        self.format = Some(f);
        self
    }

    /// Attach a list/watch filter.
    #[must_use]
    pub fn with_filter(mut self, kind: &str, namespace: Option<&str>) -> Self {
        self.kind_filter = Some(kind.to_string());
        self.namespace_filter = namespace.map(str::to_string);
        self
    }

    /// Attach a body length.
    #[must_use]
    pub fn with_body_bytes(mut self, n: usize) -> Self {
        self.body_bytes = Some(n);
        self
    }

    /// Mark success.
    #[must_use]
    pub fn ok(mut self) -> Self {
        self.success = true;
        self.error_message = None;
        self
    }

    /// Mark failure + attach the error's message.
    #[must_use]
    pub fn err(mut self, e: &FaceError) -> Self {
        self.success = false;
        self.error_message = Some(e.to_string());
        self
    }
}

/// The sink. Implementations write events to wherever audit logs
/// live (in-memory ring, append-only file, syslog, OTLP exporter,
/// kafka topic, etc.).
///
/// **Object-safe by design** — `Send + Sync + 'static` so backends
/// can hold `Box<dyn AuditLog>` and swap implementations behind
/// a trait object.
///
/// The write half IS the fleet [`shigoto_types::sink::Sink<AuditEvent>`]
/// (`record(&self, &AuditEvent)`) — `AuditLog` extends it with a `recent`
/// read for queryable sinks. Any `Sink<AuditEvent>` (the fleet
/// `NullSink` / `AuditFileSink` / `MultiSink`) is therefore an `AuditLog`
/// the moment it `impl AuditLog {}`, and the audit sinks compose with the
/// rest of the fleet's sink ecosystem.
pub trait AuditLog: Sink<AuditEvent> + 'static {
    /// Optional: surface recent events for inspection. Default returns
    /// an empty Vec; sinks that retain history (the in-memory ring)
    /// override.
    fn recent(&self, _limit: usize) -> Vec<AuditEvent> {
        Vec::new()
    }
}

// ─────────────────────────────────────────────────────────────────
// NoopAuditLog — drops every event (the fleet NullSink<AuditEvent>)
// ─────────────────────────────────────────────────────────────────

/// Discards every event. Use when audit isn't required (dev /
/// tests / production-without-audit-tier). This is the fleet
/// [`shigoto_types::sink::NullSink`] specialized to `AuditEvent` —
/// no hand-rolled impl.
pub type NoopAuditLog = NullSink<AuditEvent>;

impl AuditLog for NullSink<AuditEvent> {}

// ─────────────────────────────────────────────────────────────────
// InMemoryAuditLog — bounded ring buffer
// ─────────────────────────────────────────────────────────────────

/// Bounded in-memory ring buffer. Last N events retained; older
/// events drop. Useful for tests + debug inspection via the
/// telemetry surface.
pub struct InMemoryAuditLog {
    capacity: usize,
    events: Mutex<std::collections::VecDeque<AuditEvent>>,
}

impl InMemoryAuditLog {
    /// New ring with the given capacity. Capacity 0 is treated as 1.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            events: Mutex::new(std::collections::VecDeque::with_capacity(capacity.max(1))),
        }
    }

    /// Current number of retained events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// True iff no events retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All retained events, in insertion order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .map(|e| e.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Clear the buffer.
    pub fn clear(&self) {
        if let Ok(mut e) = self.events.lock() {
            e.clear();
        }
    }
}

impl Sink<AuditEvent> for InMemoryAuditLog {
    fn record(&self, event: &AuditEvent) {
        let Ok(mut events) = self.events.lock() else {
            return;
        };
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event.clone());
    }
}

impl AuditLog for InMemoryAuditLog {
    fn recent(&self, limit: usize) -> Vec<AuditEvent> {
        self.events
            .lock()
            .map(|e| e.iter().rev().take(limit).cloned().collect::<Vec<_>>())
            .unwrap_or_default()
            .into_iter()
            .rev()
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────
// FileAuditLog — append-only JSONL file
// ─────────────────────────────────────────────────────────────────

/// Appends every event to a file as JSONL (one JSON object per
/// line). Survives process restart. A thin newtype over the fleet
/// [`shigoto_types::sink::AuditFileSink`] — the canonical
/// append-JSONL-per-event sink — so the file-write path is no longer
/// hand-rolled. R6+ replaces with a typed segmented log (rotation,
/// compaction) but the trait + on-disk format are stable.
pub struct FileAuditLog(AuditFileSink<AuditEvent>);

impl FileAuditLog {
    /// Open the file in append mode (creates if absent, with parent dirs).
    ///
    /// # Errors
    ///
    /// Returns the underlying io::Error wrapped in [`FaceError::Unsupported`].
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, FaceError> {
        AuditFileSink::new(path.as_ref())
            .map(Self)
            .map_err(|e| FaceError::Unsupported(format!("audit log open: {e}")))
    }
}

impl Sink<AuditEvent> for FileAuditLog {
    fn record(&self, event: &AuditEvent) {
        self.0.record(event);
    }
}

impl AuditLog for FileAuditLog {}

// ─────────────────────────────────────────────────────────────────
// AuditingBackend — wraps any StoreBackend with any AuditLog
// ─────────────────────────────────────────────────────────────────

/// Decorator that adds audit-event emission to any
/// [`StoreBackend`] without backend-specific code. Every verb
/// call:
///
/// 1. Calls the inner backend's verb.
/// 2. Builds an [`AuditEvent`] from (verb, args, success/error).
/// 3. Records the event to the audit sink.
/// 4. Returns the inner backend's result.
///
/// Composition is the killer — works with InMemoryStore,
/// FileSystemBackend, RaftBackend, any future backend.
pub struct AuditingBackend<B: StoreBackend> {
    inner: B,
    log: Box<dyn AuditLog>,
}

impl<B: StoreBackend> AuditingBackend<B> {
    /// Wrap an inner backend with an audit sink.
    pub fn new(inner: B, log: impl AuditLog) -> Self {
        Self {
            inner,
            log: Box::new(log),
        }
    }

    /// Wrap with a boxed sink (e.g. when the sink is built at
    /// runtime + can't be moved into a generic).
    #[must_use]
    pub fn with_boxed_log(inner: B, log: Box<dyn AuditLog>) -> Self {
        Self { inner, log }
    }

    /// Borrow the inner backend — telemetry + advanced flows.
    #[must_use]
    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Borrow the audit sink.
    #[must_use]
    pub fn log(&self) -> &dyn AuditLog {
        self.log.as_ref()
    }
}

impl<B: StoreBackend> StoreBackend for AuditingBackend<B> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        let result = self.inner.apply(format, body);
        let mut event = AuditEvent::now(VerbKind::Apply)
            .with_format(format)
            .with_body_bytes(body.len());
        event = match &result {
            Ok(()) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        let result = self.inner.get(reference, format);
        let mut event = AuditEvent::now(VerbKind::Get)
            .with_ref(reference.clone())
            .with_format(format);
        event = match &result {
            Ok(_) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        let result = self.inner.list(kind, namespace, format);
        let mut event = AuditEvent::now(VerbKind::List)
            .with_filter(kind, namespace)
            .with_format(format);
        event = match &result {
            Ok(_) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        let result = self.inner.delete(reference);
        let mut event = AuditEvent::now(VerbKind::Delete).with_ref(reference.clone());
        event = match &result {
            Ok(()) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        let result = self.inner.watch(kind, namespace, format);
        let mut event = AuditEvent::now(VerbKind::Watch)
            .with_filter(kind, namespace)
            .with_format(format);
        event = match &result {
            Ok(_) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn resource_count(&self) -> usize {
        self.inner.resource_count()
    }

    fn subscriber_count(&self) -> usize {
        self.inner.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        let result = self.inner.snapshot();
        let mut event = AuditEvent::now(VerbKind::Snapshot);
        event = match &result {
            Ok(_) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }

    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        let result = self.inner.restore(snapshot_bytes);
        let mut event = AuditEvent::now(VerbKind::Restore).with_body_bytes(snapshot_bytes.len());
        event = match &result {
            Ok(()) => event.ok(),
            Err(e) => event.err(e),
        };
        self.log.record(&event);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::face::encode_native_envelope;
    use crate::face_store::InMemoryStore;

    fn pod_ref() -> ResourceRef {
        ResourceRef::namespaced("Pod", "nginx", "default")
    }

    fn envelope() -> Vec<u8> {
        encode_native_envelope(&pod_ref(), b"payload").unwrap()
    }

    fn yaml() -> Vec<u8> {
        b"apiVersion: v1\nkind: Pod\nmetadata:\n  name: nginx\n  namespace: default\nspec: {}\n"
            .to_vec()
    }

    // ── VerbKind ─────────────────────────────────────────────────

    #[test]
    fn verb_kind_classifies_mutations() {
        assert!(VerbKind::Apply.is_mutation());
        assert!(VerbKind::Delete.is_mutation());
        assert!(VerbKind::Restore.is_mutation());
        assert!(!VerbKind::Get.is_mutation());
        assert!(!VerbKind::List.is_mutation());
        assert!(!VerbKind::Watch.is_mutation());
        assert!(!VerbKind::Snapshot.is_mutation());
    }

    // ── AuditEvent builder ──────────────────────────────────────

    #[test]
    fn audit_event_now_populates_timestamp() {
        let ev = AuditEvent::now(VerbKind::Apply);
        assert!(ev.timestamp_s > 0);
        assert_eq!(ev.verb, VerbKind::Apply);
        assert!(ev.success);
        assert!(ev.error_message.is_none());
    }

    #[test]
    fn audit_event_builder_chains_cleanly() {
        let ev = AuditEvent::now(VerbKind::Apply)
            .with_ref(pod_ref())
            .with_format(ResourceFormat::Yaml)
            .with_body_bytes(123);
        assert_eq!(ev.reference.unwrap().name, "nginx");
        assert_eq!(ev.format, Some(ResourceFormat::Yaml));
        assert_eq!(ev.body_bytes, Some(123));
    }

    #[test]
    fn audit_event_err_attaches_message() {
        let err = FaceError::Unsupported("test failure".into());
        let ev = AuditEvent::now(VerbKind::Apply).err(&err);
        assert!(!ev.success);
        assert!(ev.error_message.unwrap().contains("test failure"));
    }

    // ── NoopAuditLog ─────────────────────────────────────────────

    #[test]
    fn noop_log_drops_every_event_silently() {
        let log = NoopAuditLog::new();
        for _ in 0..1000 {
            log.record(&AuditEvent::now(VerbKind::Apply));
        }
        // No retention; recent() returns empty.
        assert_eq!(log.recent(10).len(), 0);
    }

    // ── InMemoryAuditLog ────────────────────────────────────────

    #[test]
    fn in_memory_log_retains_events_up_to_capacity() {
        let log = InMemoryAuditLog::with_capacity(3);
        for i in 0..5 {
            log.record(&AuditEvent::now(VerbKind::Apply).with_body_bytes(i));
        }
        assert_eq!(log.len(), 3);
        let snap = log.snapshot();
        // Oldest 2 dropped; bodies = 2,3,4.
        assert_eq!(snap[0].body_bytes, Some(2));
        assert_eq!(snap[1].body_bytes, Some(3));
        assert_eq!(snap[2].body_bytes, Some(4));
    }

    #[test]
    fn in_memory_log_recent_returns_most_recent_n_in_order() {
        let log = InMemoryAuditLog::with_capacity(10);
        for i in 0..5 {
            log.record(&AuditEvent::now(VerbKind::Apply).with_body_bytes(i));
        }
        let recent = log.recent(3);
        assert_eq!(recent.len(), 3);
        // Most-recent-N preserved in insertion order.
        assert_eq!(recent[0].body_bytes, Some(2));
        assert_eq!(recent[1].body_bytes, Some(3));
        assert_eq!(recent[2].body_bytes, Some(4));
    }

    #[test]
    fn in_memory_log_clear_empties_buffer() {
        let log = InMemoryAuditLog::with_capacity(10);
        log.record(&AuditEvent::now(VerbKind::Apply));
        assert_eq!(log.len(), 1);
        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn in_memory_log_with_capacity_zero_normalizes_to_one() {
        let log = InMemoryAuditLog::with_capacity(0);
        log.record(&AuditEvent::now(VerbKind::Apply));
        log.record(&AuditEvent::now(VerbKind::Get));
        assert_eq!(log.len(), 1);
    }

    // ── FileAuditLog ─────────────────────────────────────────────

    #[test]
    fn file_log_appends_events_as_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&path).unwrap();
        log.record(&AuditEvent::now(VerbKind::Apply).with_body_bytes(10));
        log.record(&AuditEvent::now(VerbKind::Get).with_ref(pod_ref()));
        drop(log);
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        // Each line is valid JSON.
        let line0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(line0["verb"], "Apply");
        let line1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(line1["verb"], "Get");
    }

    #[test]
    fn file_log_survives_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        {
            let log = FileAuditLog::open(&path).unwrap();
            log.record(&AuditEvent::now(VerbKind::Apply).with_body_bytes(1));
        }
        // Reopen + append more.
        let log = FileAuditLog::open(&path).unwrap();
        log.record(&AuditEvent::now(VerbKind::Delete).with_ref(pod_ref()));
        drop(log);
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 2);
    }

    // ── AuditingBackend ─────────────────────────────────────────

    #[test]
    fn auditing_backend_records_every_verb() {
        let inner = InMemoryStore::new("inner");
        let log = InMemoryAuditLog::with_capacity(100);
        let backend = AuditingBackend::new(inner, log);

        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        let r = pod_ref();
        backend.get(&r, ResourceFormat::Native).unwrap();
        backend
            .list("Pod", Some("default"), ResourceFormat::Native)
            .unwrap();
        let _ = backend.watch("Pod", None, ResourceFormat::Native).unwrap();
        backend.delete(&r).unwrap();
        let _ = backend.snapshot().unwrap();

        let events = backend.log().recent(100);
        let verbs: Vec<VerbKind> = events.iter().map(|e| e.verb).collect();
        // The 5 verbs we called + the snapshot.
        assert!(verbs.contains(&VerbKind::Apply));
        assert!(verbs.contains(&VerbKind::Get));
        assert!(verbs.contains(&VerbKind::List));
        assert!(verbs.contains(&VerbKind::Watch));
        assert!(verbs.contains(&VerbKind::Delete));
        assert!(verbs.contains(&VerbKind::Snapshot));
    }

    #[test]
    fn auditing_backend_records_success_flag_correctly() {
        let inner = InMemoryStore::new("inner");
        let log = InMemoryAuditLog::with_capacity(10);
        let backend = AuditingBackend::new(inner, log);

        // Apply succeeds.
        backend.apply(ResourceFormat::Yaml, &yaml()).unwrap();
        // Get on a missing resource fails.
        let missing = ResourceRef::namespaced("Pod", "missing", "default");
        let _ = backend.get(&missing, ResourceFormat::Yaml);

        let events = backend.log().recent(10);
        let apply_ev = events.iter().find(|e| e.verb == VerbKind::Apply).unwrap();
        let get_ev = events.iter().find(|e| e.verb == VerbKind::Get).unwrap();
        assert!(apply_ev.success, "apply should succeed");
        assert!(!get_ev.success, "get on missing should fail");
        assert!(get_ev.error_message.is_some());
    }

    #[test]
    fn auditing_backend_records_body_length_not_content() {
        let inner = InMemoryStore::new("inner");
        let log = InMemoryAuditLog::with_capacity(10);
        let backend = AuditingBackend::new(inner, log);

        let body = yaml();
        let body_len = body.len();
        backend.apply(ResourceFormat::Yaml, &body).unwrap();

        let events = backend.log().recent(10);
        let apply_ev = events.iter().find(|e| e.verb == VerbKind::Apply).unwrap();
        assert_eq!(apply_ev.body_bytes, Some(body_len));
    }

    #[test]
    fn auditing_backend_preserves_inner_backend_name() {
        let inner = InMemoryStore::new("inner");
        let backend = AuditingBackend::new(inner, NoopAuditLog::new());
        // Inner is "in-memory" via the blanket StoreBackend impl
        // on InMemoryStore.
        assert_eq!(backend.name(), "in-memory");
    }

    #[test]
    fn auditing_backend_inner_borrow_for_telemetry() {
        let inner = InMemoryStore::new("inner");
        let backend = AuditingBackend::new(inner, NoopAuditLog::new());
        backend.apply(ResourceFormat::Yaml, &yaml()).unwrap();
        // Access the inner backend through inner() for telemetry.
        assert_eq!(backend.inner().len(), 1);
    }

    #[test]
    fn auditing_backend_wraps_filesystem_backend() {
        // Compose with a real persistent backend — proves the
        // decorator works with any StoreBackend impl.
        let dir = tempfile::tempdir().unwrap();
        let inner = crate::FileSystemBackend::open(dir.path(), "fs").unwrap();
        let log = InMemoryAuditLog::with_capacity(10);
        let backend = AuditingBackend::new(inner, log);
        backend.apply(ResourceFormat::Yaml, &yaml()).unwrap();
        // Backend name reflects the inner backend (filesystem).
        assert_eq!(backend.name(), "filesystem");
        let events = backend.log().recent(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].verb, VerbKind::Apply);
    }

    #[test]
    fn auditing_backend_dispatches_through_store_backend_trait_object() {
        let inner = InMemoryStore::new("inner");
        let backend: Box<dyn StoreBackend> =
            Box::new(AuditingBackend::new(inner, NoopAuditLog::new()));
        backend.apply(ResourceFormat::Yaml, &yaml()).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    // ── Event serde round-trip ──────────────────────────────────

    #[test]
    fn audit_event_serde_round_trips_through_json() {
        let ev = AuditEvent::now(VerbKind::Apply)
            .with_ref(pod_ref())
            .with_format(ResourceFormat::Yaml)
            .with_body_bytes(42);
        let json = serde_json::to_string(&ev).unwrap();
        let back: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    // ── Typed Clock construction (substrate relógio adoption) ────

    #[test]
    fn audit_event_at_frozen_clock_is_deterministic() {
        // Two events built from the same FrozenClock → byte-identical
        // timestamps, deterministically reproducible across runs.
        let clock = engenho_substrate::FrozenClock::at(1_700_000_000_000); // ms
        let e1 = AuditEvent::at(&clock, VerbKind::Apply);
        let e2 = AuditEvent::at(&clock, VerbKind::Apply);
        assert_eq!(e1.timestamp_s, e2.timestamp_s);
        assert_eq!(e1.timestamp_s, 1_700_000_000);
        assert_eq!(e1.timestamp_ns, 0); // ms-precision substrate Clock
    }

    #[test]
    fn audit_event_at_advances_with_clock() {
        let clock = engenho_substrate::FrozenClock::at(1_700_000_000_000);
        let e1 = AuditEvent::at(&clock, VerbKind::Get);
        clock.advance(5_000); // +5 sec
        let e2 = AuditEvent::at(&clock, VerbKind::Get);
        assert_eq!(e2.timestamp_s - e1.timestamp_s, 5);
    }

    // ── Object safety ────────────────────────────────────────────

    #[test]
    fn audit_log_is_object_safe() {
        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn AuditLog>();
        let _heterogeneous: Vec<Box<dyn AuditLog>> = vec![
            Box::new(NoopAuditLog::new()),
            Box::new(InMemoryAuditLog::with_capacity(10)),
        ];
    }

    // ── Fleet Sink ecosystem ─────────────────────────────────────

    /// The audit sinks are now real `shigoto_types::sink::Sink<AuditEvent>`,
    /// so they compose through the fleet `MultiSink` — one audit event fans
    /// out to several sinks (e.g. a durable file + an in-memory ring). This
    /// is the payoff of `AuditLog: Sink<AuditEvent>`.
    #[test]
    fn audit_sinks_compose_via_fleet_multisink() {
        use shigoto_types::sink::MultiSink;
        use std::sync::Arc;

        let ring = Arc::new(InMemoryAuditLog::with_capacity(16));
        let multi = MultiSink::<AuditEvent>::new()
            .with(ring.clone() as Arc<dyn Sink<AuditEvent>>)
            .with(Arc::new(NoopAuditLog::new()) as Arc<dyn Sink<AuditEvent>>);

        multi.record(&AuditEvent::now(VerbKind::Apply));
        multi.record(&AuditEvent::now(VerbKind::Delete));

        // The in-memory child captured both; the null child dropped them.
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.recent(16).len(), 2);
    }
}
