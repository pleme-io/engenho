//! # InMemoryStore — the shared verb-impl backend
//!
//! Every face needs the same shape of in-memory state to back its
//! 5-verb contract: a HashMap of stored envelopes, a Vec of watch
//! subscribers, and an AdapterRegistry for format conversion. This
//! module ships that shape as one shared helper so all five faces
//! compose with it.
//!
//! Per the prime-directive third-site rule: PureRaftFace shipped
//! verbs first; KubernetesFace + NomadFace + SystemdFace +
//! BareMetalSupervisorFace would have each re-implemented the same
//! ~70-line pattern. Lifting to one helper means each face is
//! 5 lines of delegation + its own lifecycle hooks. Future R6
//! per-face backends (kube-apiserver / nomad HTTP / dbus / etc.)
//! REPLACE the InMemoryStore in their face; the operator-facing
//! contract stays identical.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::face::{
    FaceError, FaceWatchEvent, FaceWatchEventKind, FaceWatchStream, ResourceFormat, ResourceRef,
};
use crate::format::AdapterRegistry;

/// The shared verb backend. Owns the in-memory state +
/// subscribers + adapter registry; honors the same byte-level
/// semantics every face's verb impls promise.
pub struct InMemoryStore {
    /// Face name (used for diagnostic messages in adapter errors).
    face_name: String,
    /// Resource → envelope bytes (CBOR-encoded NativeEnvelope when
    /// applied via Native; raw operator bytes when applied via
    /// Yaml/Json/Hcl — the adapter wraps either case to match
    /// `Native` round-tripping).
    store: Mutex<HashMap<ResourceRef, Vec<u8>>>,
    /// Watch fan-out subscribers. Apply/delete iterate this list +
    /// send to each matching subscriber.
    subscribers: Mutex<Vec<WatchSubscriber>>,
    /// Format adapters supported by this face. Defaults to the
    /// 3-adapter standard set; custom registries land via
    /// [`Self::with_adapters`].
    adapters: AdapterRegistry,
}

struct WatchSubscriber {
    sender: Option<std::sync::mpsc::Sender<FaceWatchEvent>>,
    kind_filter: String,
    namespace_filter: Option<String>,
}

impl InMemoryStore {
    /// Build an empty store with the default adapter registry.
    /// `face_name` flows into adapter error messages so multi-face
    /// federations can identify which face raised an error.
    #[must_use]
    pub fn new(face_name: impl Into<String>) -> Self {
        Self {
            face_name: face_name.into(),
            store: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(Vec::new()),
            adapters: AdapterRegistry::default(),
        }
    }

    /// Replace the adapter registry. Builder hook for tests + for
    /// faces that want a non-default format set.
    pub fn set_adapters(&mut self, adapters: AdapterRegistry) {
        self.adapters = adapters;
    }

    /// Borrow the adapter registry — diagnostics + tests.
    #[must_use]
    pub fn adapters(&self) -> &AdapterRegistry {
        &self.adapters
    }

    /// Helper: select the registered adapter for `format`.
    fn select_adapter(
        &self,
        format: ResourceFormat,
        verb: &'static str,
    ) -> Result<std::sync::Arc<dyn crate::format::FormatAdapter>, FaceError> {
        self.adapters
            .select(format)
            .map_err(|e| FaceError::Unsupported(format!("{verb}: {e}")))
    }

    /// Fan an event to every subscriber whose filter matches.
    fn broadcast(&self, reference: &ResourceRef, event_kind: FaceWatchEventKind, body: Vec<u8>) {
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        subs.retain_mut(|sub| {
            if sub.kind_filter != reference.kind {
                return true;
            }
            if let Some(ns) = &sub.namespace_filter
                && reference.namespace.as_deref() != Some(ns.as_str())
            {
                return true;
            }
            let Some(sender) = &sub.sender else {
                return false;
            };
            let event = FaceWatchEvent {
                kind: event_kind,
                body: body.clone(),
            };
            sender.send(event).is_ok()
        });
    }

    // ── Verb impls ────────────────────────────────────────────────

    /// Apply: adapter extracts ref + converts to envelope; store
    /// + broadcast.
    pub fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        let adapter = self.select_adapter(format, "apply_resource")?;
        let reference = adapter
            .extract_ref(format, body)
            .map_err(|e| FaceError::Unsupported(format!("apply_resource: {e}")))?;
        let envelope = adapter
            .to_native(format, body)
            .map_err(|e| FaceError::Unsupported(format!("apply_resource: {e}")))?;
        let mut store = self.store.lock().expect("store mutex poisoned");
        let event_kind = if store.contains_key(&reference) {
            FaceWatchEventKind::Modified
        } else {
            FaceWatchEventKind::Added
        };
        store.insert(reference.clone(), envelope.clone());
        drop(store);
        self.broadcast(&reference, event_kind, envelope);
        Ok(())
    }

    /// Get: lookup + adapter.from_native rendering.
    pub fn get(
        &self,
        reference: &ResourceRef,
        format: ResourceFormat,
    ) -> Result<Vec<u8>, FaceError> {
        let adapter = self.select_adapter(format, "get_resource")?;
        let store = self.store.lock().expect("store mutex poisoned");
        let envelope = store.get(reference).cloned().ok_or_else(|| {
            FaceError::Unsupported(format!(
                "get_resource on {}: no resource at {reference:?}",
                self.face_name
            ))
        })?;
        drop(store);
        adapter
            .from_native(format, &envelope)
            .map_err(|e| FaceError::Unsupported(format!("get_resource: {e}")))
    }

    /// List: filter by kind+namespace; render each via adapter.
    pub fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        let adapter = self.select_adapter(format, "list_resources")?;
        let store = self.store.lock().expect("store mutex poisoned");
        let envelopes: Vec<Vec<u8>> = store
            .iter()
            .filter(|(r, _)| r.kind == kind)
            .filter(|(r, _)| match namespace {
                Some(ns) => r.namespace.as_deref() == Some(ns),
                None => true,
            })
            .map(|(_, bytes)| bytes.clone())
            .collect();
        drop(store);
        let mut out = Vec::with_capacity(envelopes.len());
        for env in envelopes {
            out.push(
                adapter
                    .from_native(format, &env)
                    .map_err(|e| FaceError::Unsupported(format!("list_resources: {e}")))?,
            );
        }
        Ok(out)
    }

    /// Delete: remove from store; broadcast Deleted event with
    /// prior envelope body.
    pub fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        let mut store = self.store.lock().expect("store mutex poisoned");
        let body = store.remove(reference).ok_or_else(|| {
            FaceError::Unsupported(format!(
                "delete_resource on {}: no resource at {reference:?}",
                self.face_name
            ))
        })?;
        drop(store);
        self.broadcast(reference, FaceWatchEventKind::Deleted, body);
        Ok(())
    }

    // ── Snapshot / Restore ────────────────────────────────────────
    //
    // Deterministic state capture: emit a single CBOR-serialized
    // payload that holds every `ResourceRef` + envelope bytes the
    // store currently owns. Restore replays into a fresh store.
    // Foundation for: backup-on-shutdown, restore-on-startup,
    // in-memory cluster cloning, federation member migration.
    //
    // Pure-data primitive — no I/O. Operators wire to disk / object
    // store / network via the bytes the snapshot returns.

    /// Serialize every (ResourceRef, envelope) pair the store
    /// currently holds into a CBOR-encoded byte buffer. Stable
    /// ordering: entries are sorted by (kind, namespace, name) so
    /// two snapshots of the same logical state produce byte-
    /// identical output.
    ///
    /// # Errors
    ///
    /// Returns [`FaceError::Unsupported`] on CBOR encode failure
    /// (effectively unreachable — `Vec<(ResourceRef, Vec<u8>)>` is
    /// always serializable).
    pub fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        let store = self.store.lock().expect("store mutex poisoned");
        let mut entries: Vec<(ResourceRef, Vec<u8>)> = store
            .iter()
            .map(|(r, body)| (r.clone(), body.clone()))
            .collect();
        drop(store);
        entries.sort_by(|a, b| {
            a.0.kind
                .cmp(&b.0.kind)
                .then(a.0.namespace.cmp(&b.0.namespace))
                .then(a.0.name.cmp(&b.0.name))
        });
        let mut out = Vec::new();
        ciborium::into_writer(&entries, &mut out)
            .map_err(|e| FaceError::Unsupported(format!("snapshot: cbor encode failed: {e}")))?;
        Ok(out)
    }

    /// Replay a snapshot into this store. **Replaces** the entire
    /// store contents (any existing entries are dropped). Emits no
    /// watch events — restore is a state-resync, not a sequence of
    /// applies.
    ///
    /// # Errors
    ///
    /// Returns [`FaceError::Unsupported`] on CBOR decode failure
    /// (malformed snapshot bytes).
    pub fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        let entries: Vec<(ResourceRef, Vec<u8>)> = ciborium::from_reader(snapshot_bytes)
            .map_err(|e| FaceError::Unsupported(format!("restore: cbor decode failed: {e}")))?;
        let mut store = self.store.lock().expect("store mutex poisoned");
        store.clear();
        for (r, body) in entries {
            store.insert(r, body);
        }
        Ok(())
    }

    /// Number of resources currently stored. Used by ClusterHealth.
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// True iff the store has no resources.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of active watch subscribers. Used by ClusterHealth.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// Watch: register subscriber, replay current state as Added.
    /// Events emit raw envelope bytes (subscribers adapt as needed).
    pub fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        // Verify the format is supported (consistent with apply).
        let _ = self.select_adapter(format, "watch_resources")?;
        let (tx, rx) = std::sync::mpsc::channel();
        // Replay current matching state.
        let store = self.store.lock().expect("store mutex poisoned");
        for (r, body) in store.iter() {
            if r.kind != kind {
                continue;
            }
            if let Some(ns) = namespace
                && r.namespace.as_deref() != Some(ns)
            {
                continue;
            }
            let _ = tx.send(FaceWatchEvent {
                kind: FaceWatchEventKind::Added,
                body: body.clone(),
            });
        }
        drop(store);
        let mut subs = self.subscribers.lock().expect("subscribers mutex poisoned");
        subs.push(WatchSubscriber {
            sender: Some(tx),
            kind_filter: kind.to_string(),
            namespace_filter: namespace.map(str::to_string),
        });
        Ok(Box::new(MpscWatchStream { rx }))
    }
}

impl std::fmt::Debug for InMemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStore")
            .field("face_name", &self.face_name)
            .field(
                "store_len",
                &self.store.lock().map(|s| s.len()).unwrap_or(0),
            )
            .field(
                "subscriber_count",
                &self.subscribers.lock().map(|s| s.len()).unwrap_or(0),
            )
            .finish()
    }
}

/// mpsc-based watch stream used by every face's `watch_resources`.
struct MpscWatchStream {
    rx: std::sync::mpsc::Receiver<FaceWatchEvent>,
}

impl FaceWatchStream for MpscWatchStream {
    fn next_event(&mut self) -> Result<Option<FaceWatchEvent>, FaceError> {
        match self.rx.recv() {
            Ok(event) => Ok(Some(event)),
            Err(_) => Ok(None),
        }
    }
}
