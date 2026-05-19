//! `WatchEvent<R>` — typed stream events the apiserver emits over the
//! `?watch=true` channel.
//!
//! Matches upstream's `metav1.WatchEvent` shape: a `type` tag +
//! the resource payload. The fifth variant (`Error`) carries a
//! `Status` body — represented here as the typed [`KubeError`] so
//! consumers don't have to re-classify.

use serde::{Deserialize, Serialize};

use crate::error::KubeError;
use crate::kind::KubeResource;

/// One event from a `Watcher` stream.
///
/// Generic over `R: KubeResource` so the same enum carries `Pod`
/// events on a Pod watch and `Deployment` events on a Deployment
/// watch.
///
/// NB: `WatchEvent` deliberately does not implement `Clone`. Watch
/// events are moved through the stream and consumed by a handler;
/// duplicating an event with the same `metadata.uid` would violate
/// the informer's at-most-once-per-uid contract.
#[derive(Debug)]
pub enum WatchEvent<R> {
    /// A new object appeared. Either created server-side or visible
    /// to this watcher for the first time after a relist.
    Added(R),
    /// An existing object changed.
    Modified(R),
    /// The object was deleted. After this event the apiserver will
    /// not emit further events for the same `metadata.uid`.
    Deleted(R),
    /// A bookmark/cursor — the resourceVersion has advanced but no
    /// object changed. Used to keep informer state fresh while the
    /// watched set is quiet.
    Bookmark { resource_version: String },
    /// The apiserver reported an error in-stream. The watcher should
    /// usually close + relist.
    Error(KubeError),
}

/// Wire-format `metav1.WatchEvent`. Used internally by the watcher
/// impl to deserialize one line of a chunked watch response, then
/// converted to the typed [`WatchEvent<R>`] above.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawWatchEvent {
    /// One of `ADDED`, `MODIFIED`, `DELETED`, `BOOKMARK`, `ERROR`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// JSON of the typed resource (for ADDED/MODIFIED/DELETED) or
    /// a Status object (for ERROR/BOOKMARK).
    pub object:    serde_json::Value,
}

impl RawWatchEvent {
    /// Decode this wire event into a typed `WatchEvent<R>`.
    ///
    /// # Errors
    ///
    /// Returns [`KubeError::Decode`] if the `object` field can't be
    /// deserialized into `R` for object events, or if the event_type
    /// is unrecognized.
    pub fn into_typed<R: KubeResource>(self) -> Result<WatchEvent<R>, KubeError> {
        match self.event_type.as_str() {
            "ADDED" => {
                let r: R = serde_json::from_value(self.object)
                    .map_err(|e| KubeError::Decode(format!("ADDED object: {e}")))?;
                Ok(WatchEvent::Added(r))
            }
            "MODIFIED" => {
                let r: R = serde_json::from_value(self.object)
                    .map_err(|e| KubeError::Decode(format!("MODIFIED object: {e}")))?;
                Ok(WatchEvent::Modified(r))
            }
            "DELETED" => {
                let r: R = serde_json::from_value(self.object)
                    .map_err(|e| KubeError::Decode(format!("DELETED object: {e}")))?;
                Ok(WatchEvent::Deleted(r))
            }
            "BOOKMARK" => {
                let rv = self
                    .object
                    .get("metadata")
                    .and_then(|m| m.get("resourceVersion"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        KubeError::Decode(
                            "BOOKMARK missing metadata.resourceVersion".into(),
                        )
                    })?
                    .to_string();
                Ok(WatchEvent::Bookmark { resource_version: rv })
            }
            "ERROR" => {
                let code = self.object.get("code").and_then(|c| c.as_u64()).unwrap_or(0) as u16;
                let message = self
                    .object
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                Ok(WatchEvent::Error(KubeError::ApiStatus {
                    code,
                    kind: crate::error::ApiStatusKind::from_code(code),
                    message,
                }))
            }
            other => Err(KubeError::Decode(format!(
                "unknown watch event type {other:?}"
            ))),
        }
    }
}

/// Streaming source of [`WatchEvent<R>`] from the apiserver.
///
/// `async-trait` adds the `Send` bound automatically for async fns;
/// the returned stream is `Send + 'static` so callers can spawn on
/// any tokio runtime.
#[async_trait::async_trait]
pub trait Watcher<R: KubeResource>: Send + Sync {
    /// Open a watch starting at `resource_version`. Pass an empty
    /// string to watch from "now" (latest).
    ///
    /// The returned stream yields events until the apiserver closes
    /// it (typically every ~5min) or an error terminates it.
    /// Consumers should re-list + re-watch from the last seen
    /// resourceVersion + bookmark for correctness.
    ///
    /// # Errors
    ///
    /// Returns [`KubeError`] if the initial connection fails. Errors
    /// AFTER connection are surfaced as [`WatchEvent::Error`] inside
    /// the stream.
    async fn watch(
        &self,
        namespace: Option<&str>,
        resource_version: &str,
    ) -> Result<
        std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<WatchEvent<R>, KubeError>> + Send>,
        >,
        KubeError,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn raw_added_decodes_to_typed() {
        // Minimal Pod-like shape: we just need ANY KubeResource
        // implementor for this test, but engenho-types doesn't have
        // one yet. The into_typed call's failure path is what we
        // exercise here.
        let raw = RawWatchEvent {
            event_type: "ADDED".into(),
            object:     json!({"metadata": {"name": "p1"}}),
        };
        // Decoding to a non-existent type would fail; just verify
        // the parse + branch logic works for BOOKMARK + ERROR which
        // don't need a typed payload.
        let bookmark = RawWatchEvent {
            event_type: "BOOKMARK".into(),
            object:     json!({"metadata": {"resourceVersion": "12345"}}),
        };
        // Using `()` as a stand-in is unsound but exercising the
        // bookmark branch which never touches the payload-as-R is fine.
        // We use a wrapper test type instead:
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        struct Stub {
            #[serde(default)]
            metadata: serde_json::Value,
        }
        impl crate::kind::KubeResource for Stub {
            const GVK: crate::kind::GroupVersionKind = crate::kind::GroupVersionKind {
                group: "", version: "v1", kind: "Stub",
            };
            const GVR: crate::kind::GroupVersionResource = crate::kind::GroupVersionResource {
                group: "", version: "v1", resource: "stubs",
            };
            const SCOPE: crate::kind::Scope = crate::kind::Scope::Namespaced;
            fn name(&self) -> std::borrow::Cow<'_, str> { "".into() }
            fn namespace(&self) -> Option<std::borrow::Cow<'_, str>> { None }
            fn resource_version(&self) -> Option<std::borrow::Cow<'_, str>> { None }
        }
        let typed: WatchEvent<Stub> = raw.into_typed().unwrap();
        assert!(matches!(typed, WatchEvent::Added(_)));
        let typed: WatchEvent<Stub> = bookmark.into_typed().unwrap();
        assert!(matches!(typed, WatchEvent::Bookmark { .. }));
    }

    #[test]
    fn unknown_event_type_errors() {
        let raw = RawWatchEvent {
            event_type: "GARBAGE".into(),
            object:     json!({}),
        };
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        struct Stub;
        impl crate::kind::KubeResource for Stub {
            const GVK: crate::kind::GroupVersionKind = crate::kind::GroupVersionKind {
                group: "", version: "v1", kind: "Stub",
            };
            const GVR: crate::kind::GroupVersionResource = crate::kind::GroupVersionResource {
                group: "", version: "v1", resource: "stubs",
            };
            const SCOPE: crate::kind::Scope = crate::kind::Scope::Namespaced;
            fn name(&self) -> std::borrow::Cow<'_, str> { "".into() }
            fn namespace(&self) -> Option<std::borrow::Cow<'_, str>> { None }
            fn resource_version(&self) -> Option<std::borrow::Cow<'_, str>> { None }
        }
        let r: Result<WatchEvent<Stub>, _> = raw.into_typed();
        assert!(r.is_err());
        assert!(matches!(r.unwrap_err(), KubeError::Decode(_)));
    }

    #[test]
    fn error_event_decodes_to_apistatus() {
        let raw = RawWatchEvent {
            event_type: "ERROR".into(),
            object:     json!({"code": 410, "message": "too old resource version"}),
        };
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        struct Stub;
        impl crate::kind::KubeResource for Stub {
            const GVK: crate::kind::GroupVersionKind = crate::kind::GroupVersionKind {
                group: "", version: "v1", kind: "Stub",
            };
            const GVR: crate::kind::GroupVersionResource = crate::kind::GroupVersionResource {
                group: "", version: "v1", resource: "stubs",
            };
            const SCOPE: crate::kind::Scope = crate::kind::Scope::Namespaced;
            fn name(&self) -> std::borrow::Cow<'_, str> { "".into() }
            fn namespace(&self) -> Option<std::borrow::Cow<'_, str>> { None }
            fn resource_version(&self) -> Option<std::borrow::Cow<'_, str>> { None }
        }
        let typed: WatchEvent<Stub> = raw.into_typed().unwrap();
        match typed {
            WatchEvent::Error(KubeError::ApiStatus { code, kind, .. }) => {
                assert_eq!(code, 410);
                assert_eq!(kind, crate::error::ApiStatusKind::Gone);
            }
            other => panic!("expected Error event, got {other:?}"),
        }
    }
}
