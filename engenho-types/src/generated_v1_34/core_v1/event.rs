//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Regenerate via `cargo run -p engenho-kube-codegen -- \
//!     --schema engenho-types/vendor/openapi/v1.34.0 \
//!     --output engenho-types/src/generated_v1_34`.
//!
//! Edit src/catalog.rs to add or remove kinds.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::generated_v1_34::types::*;
use crate::kind::{GroupVersionKind, GroupVersionResource, KubeResource, Scope};
use crate::meta::ObjectMeta;

/// Event is a report of an event somewhere in the cluster.  Events have a limited retention time and triggers and messages may evolve with time.  Event consumers should not rely on the timing of an event with a given Reason reflecting a consistent underlying trigger, or the continued existence of events with that Reason.  Events should be treated as informative, best-effort, supplemental data.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// What action was taken/failed regarding to the Regarding object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    /// The number of times this event has occurred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,
    /// Time when this Event was first observed.
    #[serde(default, rename = "eventTime", skip_serializing_if = "Option::is_none")]
    pub event_time: Option<MicroTime>,
    /// The time at which the event was first recorded. (Time of server receipt is in TypeMeta.)
    #[serde(
        default,
        rename = "firstTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub first_timestamp: Option<Time>,
    /// The object that this event is about.
    #[serde(default, rename = "involvedObject")]
    pub involved_object: ObjectReference,
    /// The time at which the most recent occurrence of this event was recorded.
    #[serde(
        default,
        rename = "lastTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_timestamp: Option<Time>,
    /// A human-readable description of the status of this operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// This should be a short, machine understandable string that gives the reason for the transition into the object's current status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Optional secondary object for more complex actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related: Option<ObjectReference>,
    /// Name of the controller that emitted this Event, e.g. `kubernetes.io/kubelet`.
    #[serde(
        default,
        rename = "reportingComponent",
        skip_serializing_if = "Option::is_none"
    )]
    pub reporting_component: Option<String>,
    /// ID of the controller instance, e.g. `kubelet-xyzf`.
    #[serde(
        default,
        rename = "reportingInstance",
        skip_serializing_if = "Option::is_none"
    )]
    pub reporting_instance: Option<String>,
    /// Data about the Event series this event represents or nil if it's a singleton Event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub series: Option<EventSeries>,
    /// The component reporting this event. Should be a short machine understandable string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<EventSource>,
    /// Type of this event (Normal, Warning), new types could be added in the future
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

impl KubeResource for Event {
    const GVK: GroupVersionKind = GroupVersionKind {
        group: "",
        version: "v1",
        kind: "Event",
    };
    const GVR: GroupVersionResource = GroupVersionResource {
        group: "",
        version: "v1",
        resource: "events",
    };
    const SCOPE: Scope = Scope::Namespaced;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.metadata.name.as_str())
    }
    fn namespace(&self) -> Option<Cow<'_, str>> {
        self.metadata.namespace.as_deref().map(Cow::Borrowed)
    }
    fn resource_version(&self) -> Option<Cow<'_, str>> {
        if self.metadata.resource_version.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(self.metadata.resource_version.as_str()))
        }
    }
}

fn is_empty_meta(m: &ObjectMeta) -> bool {
    m == &ObjectMeta::default()
}
