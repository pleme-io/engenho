//! Watch events — K8s-style change notifications.
//!
//! Every successful apply on the state machine emits a
//! [`WatchEvent`] over a tokio broadcast channel. The apiserver's
//! /watch endpoint forwards these as SSE / chunked-JSON to clients
//! (controllers, kubectl --watch, etc.).
//!
//! At R7.5 we emit FULL resource objects per event (matches the
//! default K8s watch behavior). R7.6 may add bookmark events +
//! resourceVersion-based resume.

use serde::{Deserialize, Serialize};

use crate::resource::{ResourceKey, ResourceValue};

/// One change notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchEvent {
    #[serde(rename = "type")]
    pub kind: WatchEventKind,
    /// The full resource object at the moment of the change. For
    /// `Deleted`, the object is the LAST KNOWN state (the value
    /// that was just removed).
    pub object: ResourceValue,
    /// The resource key (group/version/kind/ns/name) — convenience
    /// for clients filtering by kind without parsing `object.kind`.
    pub key: ResourceKey,
    /// The Raft log index at which this change was committed.
    /// Clients can use it to seek/resume.
    pub resource_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventKind {
    Added,
    Modified,
    Deleted,
    /// Periodic marker (R7.6) — included for forward compat with
    /// resumable watches.
    Bookmark,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_event_serializes_as_k8s_format() {
        let ev = WatchEvent {
            kind: WatchEventKind::Added,
            object: serde_json::json!({"kind": "Pod"}),
            key: ResourceKey::namespaced("", "v1", "Pod", "default", "x"),
            resource_version: 42,
        };
        let s = serde_json::to_string(&ev).unwrap();
        // K8s wire format uses uppercase ADDED/MODIFIED/DELETED.
        assert!(s.contains("\"type\":\"ADDED\""), "got: {s}");
        let back: WatchEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(back.kind, WatchEventKind::Added);
        assert_eq!(back.resource_version, 42);
    }

    #[test]
    fn watch_event_kinds_round_trip() {
        for k in [
            WatchEventKind::Added,
            WatchEventKind::Modified,
            WatchEventKind::Deleted,
            WatchEventKind::Bookmark,
        ] {
            let s = serde_json::to_string(&k).unwrap();
            let back: WatchEventKind = serde_json::from_str(&s).unwrap();
            assert_eq!(back, k);
        }
    }
}
