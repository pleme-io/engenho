//! The k9s request matrix, as data.
//!
//! Mined from k9s v0.50.16 (`internal/dao`, `internal/view`, `internal/client`,
//! `internal/watch`) and confirmed against a recorded k9s session on ryn
//! (2026-10-13). Each [`K9sAction`] is something a k9s user does; each [`Row`]
//! is one request that action puts on the wire, with the exact shape k9s
//! sends and what an upstream apiserver answers. `docs/K9S-CONFORMANCE.md`
//! carries the prose.

use serde_json::{Value, json};

/// A k9s user action. [`K9sAction::ALL`] is checked against an exhaustive
/// `match` so a new variant cannot be added without being listed, and every
/// listed variant must own at least one [`Row`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum K9sAction {
    /// Startup and every view switch: `/version`, `/api`, `/apis`, aggregated
    /// discovery.
    Discovery,
    /// Every table view: an informer's LIST then WATCH.
    ListWatch,
    /// `:ns` and the namespace favourites row.
    NamespaceSwitch,
    /// `:ctx` — local kubeconfig plus a `/version` probe of the new context.
    ContextSwitch,
    /// `d` — `kubectl describe`: GET plus an events LIST by field selector.
    Describe,
    /// `y` — GET rendered as YAML.
    Yaml,
    /// `e` — GET then a full UPDATE.
    Edit,
    /// `ctrl-d` — DELETE with propagationPolicy and gracePeriodSeconds.
    Delete,
    /// `ctrl-k` — DELETE with gracePeriodSeconds=0.
    Kill,
    /// `l` — follow the default container's log (always `timestamps=true`).
    Logs,
    /// `l` on a multi-container pod — one follow per container, init ones
    /// included.
    LogsAllContainers,
    /// `p` — the previous instance's log.
    LogsPrevious,
    /// Logs view `1` (head) — `limitBytes=5000`, no follow.
    LogsHead,
    /// Logs view `2`..`6` — `sinceSeconds`.
    LogsSince,
    /// `s` — `kubectl exec` (WebSocket v5).
    Shell,
    /// `a` — attach (WebSocket v5).
    Attach,
    /// `shift-f` — port-forward (SPDY-over-WebSocket tunnel, SPDY fallback).
    PortForward,
    /// Port-forward benchmark — rides on [`K9sAction::PortForward`].
    Benchmark,
    /// `s` on a workload — the scale subresource, GET then PUT.
    Scale,
    /// `r` — strategic-merge patch of `kubectl.kubernetes.io/restartedAt`.
    Restart,
    /// `i` — strategic-merge patch of container images.
    SetImage,
    /// `c` on a node — strategic-merge patch of `spec.unschedulable`.
    Cordon,
    /// `u` on a node.
    Uncordon,
    /// `r` on a node — cordon, then evict (or delete when eviction is not
    /// discovered) every pod.
    Drain,
    /// Every action is gated on a `SelfSubjectAccessReview`.
    AccessReview,
    /// `:policy` / rules view.
    RulesReview,
    /// The CPU/MEM columns and pulses: `metrics.k8s.io`.
    Metrics,
    /// `:events`.
    Events,
    /// `:helm` — releases are Secrets labelled `owner=helm`.
    Helm,
    /// `:xray` / `:pulses` — LISTs across many kinds.
    Xray,
    /// `:crds` and browsing a CRD's instances.
    CrdBrowse,
}

impl K9sAction {
    /// Every action, in declaration order.
    pub const ALL: &'static [Self] = &[
        Self::Discovery,
        Self::ListWatch,
        Self::NamespaceSwitch,
        Self::ContextSwitch,
        Self::Describe,
        Self::Yaml,
        Self::Edit,
        Self::Delete,
        Self::Kill,
        Self::Logs,
        Self::LogsAllContainers,
        Self::LogsPrevious,
        Self::LogsHead,
        Self::LogsSince,
        Self::Shell,
        Self::Attach,
        Self::PortForward,
        Self::Benchmark,
        Self::Scale,
        Self::Restart,
        Self::SetImage,
        Self::Cordon,
        Self::Uncordon,
        Self::Drain,
        Self::AccessReview,
        Self::RulesReview,
        Self::Metrics,
        Self::Events,
        Self::Helm,
        Self::Xray,
        Self::CrdBrowse,
    ];

    /// The variant's position in [`Self::ALL`]; exhaustive, so a new variant
    /// is a compile error here until it is placed.
    pub const fn ordinal(self) -> usize {
        match self {
            Self::Discovery => 0,
            Self::ListWatch => 1,
            Self::NamespaceSwitch => 2,
            Self::ContextSwitch => 3,
            Self::Describe => 4,
            Self::Yaml => 5,
            Self::Edit => 6,
            Self::Delete => 7,
            Self::Kill => 8,
            Self::Logs => 9,
            Self::LogsAllContainers => 10,
            Self::LogsPrevious => 11,
            Self::LogsHead => 12,
            Self::LogsSince => 13,
            Self::Shell => 14,
            Self::Attach => 15,
            Self::PortForward => 16,
            Self::Benchmark => 17,
            Self::Scale => 18,
            Self::Restart => 19,
            Self::SetImage => 20,
            Self::Cordon => 21,
            Self::Uncordon => 22,
            Self::Drain => 23,
            Self::AccessReview => 24,
            Self::RulesReview => 25,
            Self::Metrics => 26,
            Self::Events => 27,
            Self::Helm => 28,
            Self::Xray => 29,
            Self::CrdBrowse => 30,
        }
    }
}

/// The request a row puts on the wire.
#[derive(Debug, Clone, Copy)]
pub enum Req {
    /// GET with an optional `Accept`.
    Get {
        path: &'static str,
        accept: Option<&'static str>,
    },
    /// POST a JSON body.
    Post {
        path: &'static str,
        body: fn() -> Value,
    },
    /// GET the object at `path`, apply `edit` to it, PUT it back.
    GetThenPut {
        path: &'static str,
        edit: fn(&mut Value),
    },
    /// PATCH with a content type.
    Patch {
        path: &'static str,
        content_type: &'static str,
        body: fn() -> Value,
    },
    /// DELETE with a `DeleteOptions` body.
    Delete {
        path: &'static str,
        body: fn() -> Value,
    },
    /// An informer watch: read until the first event frame.
    Watch { path: &'static str },
    /// A followed log: read the first chunk, then cancel.
    Follow { path: &'static str },
    /// A WebSocket upgrade offering `protocols`.
    Upgrade {
        path: &'static str,
        protocols: &'static str,
    },
}

/// What upstream answers, and so what engenho must answer.
#[derive(Debug, Clone, Copy)]
pub enum Expected {
    /// This status with a JSON body of this `kind`.
    Object { status: u16, kind: &'static str },
    /// 200 `text/plain` of at most this many bytes.
    TextAtMost { bytes: usize },
    /// This status with a K8s `Status` body.
    Refused { status: u16 },
    /// A watch stream whose first frame is a typed event.
    WatchEvent { event: &'static str },
    /// A followed log whose first chunk contains this text.
    FollowChunk { contains: &'static str },
    /// 200 log text containing `contains` and never `absent`; with
    /// `stamped`, every line opens with one RFC3339 token, a space, then the
    /// line's own text (the token k9s strips).
    Lines {
        contains: &'static str,
        absent: Option<&'static str>,
        stamped: bool,
    },
    /// 101 with this negotiated subprotocol.
    Switched { protocol: &'static str },
    /// Not served. The row asserts the graceful-degradation answer k9s needs
    /// (absent from discovery, a clean 404 / refusal), and the reason is the
    /// gap's ledger entry.
    Absent {
        reason: &'static str,
        degrade: Degrade,
    },
}

/// How an [`Expected::Absent`] row degrades. Every shape stays, whether or
/// not a row uses it today: the next gap found is a row, not new harness.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code, reason = "degrade shapes kept for the next Absent row")]
pub enum Degrade {
    /// A 404 `Status`, and `group` is not in `/apis`.
    NotDiscovered { group: &'static str },
    /// A 404 `Status`, and `pods/<sub>` is not in `/api/v1` discovery.
    NoSubresource { subresource: &'static str },
    /// No 101 negotiating `protocol`: the client sees an upgrade failure.
    NoUpgrade { protocol: &'static str },
    /// Answered, but with this known divergence (served, not honoured).
    Ignored { status: u16 },
}

/// One request of one action.
#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub action: K9sAction,
    pub what: &'static str,
    pub req: Req,
    pub expect: Expected,
}

/// The namespace every fixture lives in.
pub const NS: &str = "k9s";

const AGG: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,application/json";

fn ssar_pod_log() -> Value {
    json!({"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectAccessReview",
        "spec":{"resourceAttributes":{"namespace":NS,"verb":"get","group":"","version":"v1",
        "resource":"pods","subresource":"log","name":"web"}}})
}
fn ssar_list_ns() -> Value {
    json!({"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectAccessReview",
        "spec":{"resourceAttributes":{"verb":"list","version":"v1","resource":"namespaces"}}})
}
fn ssrr() -> Value {
    json!({"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectRulesReview",
        "spec":{"namespace":NS}})
}
fn background_delete() -> Value {
    json!({"kind":"DeleteOptions","apiVersion":"v1","propagationPolicy":"Background"})
}
fn kill() -> Value {
    json!({"kind":"DeleteOptions","apiVersion":"v1","propagationPolicy":"Background",
        "gracePeriodSeconds":0})
}
fn restart() -> Value {
    json!({"spec":{"template":{"metadata":{"annotations":{
        "kubectl.kubernetes.io/restartedAt":"2026-10-13T12:00:00-03:00"}}}}})
}
fn set_image() -> Value {
    json!({"spec":{"template":{"spec":{"containers":[{"name":"main","image":"busybox:2"}]}}}})
}
fn cordon() -> Value {
    json!({"spec":{"unschedulable":true}})
}
fn uncordon() -> Value {
    json!({"spec":{"unschedulable":null}})
}
fn eviction() -> Value {
    json!({"apiVersion":"policy/v1","kind":"Eviction",
        "metadata":{"name":"doomed","namespace":NS}})
}
fn guarded_eviction() -> Value {
    json!({"apiVersion":"policy/v1","kind":"Eviction",
        "metadata":{"name":"guarded","namespace":NS}})
}
fn label_edit(v: &mut Value) {
    v["metadata"]["labels"]["edited-by"] = json!("k9s");
}
fn scale_to_three(v: &mut Value) {
    v["spec"]["replicas"] = json!(3);
}

const SMP: &str = "application/strategic-merge-patch+json";
const POD: &str = "/api/v1/namespaces/k9s/pods/web";

/// The matrix.
pub const ROWS: &[Row] = &[
    // ── discovery ────────────────────────────────────────────────────────
    Row {
        action: K9sAction::Discovery,
        what: "server version",
        req: Req::Get {
            path: "/version",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "",
        },
    },
    Row {
        action: K9sAction::Discovery,
        what: "legacy core discovery",
        req: Req::Get {
            path: "/api",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIVersions",
        },
    },
    Row {
        action: K9sAction::Discovery,
        what: "core/v1 resource list",
        req: Req::Get {
            path: "/api/v1",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIResourceList",
        },
    },
    Row {
        action: K9sAction::Discovery,
        what: "group list",
        req: Req::Get {
            path: "/apis",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIGroupList",
        },
    },
    Row {
        action: K9sAction::Discovery,
        what: "aggregated discovery v2",
        req: Req::Get {
            path: "/apis",
            accept: Some(AGG),
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIGroupDiscoveryList",
        },
    },
    // ── informers ────────────────────────────────────────────────────────
    Row {
        action: K9sAction::ListWatch,
        what: "informer list (rv=0, limit=500)",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "PodList",
        },
    },
    Row {
        action: K9sAction::ListWatch,
        what: "informer watch with bookmarks",
        req: Req::Watch {
            path: "/api/v1/namespaces/k9s/pods?allowWatchBookmarks=true&resourceVersion=0&timeoutSeconds=300&watch=true",
        },
        expect: Expected::WatchEvent { event: "ADDED" },
    },
    Row {
        action: K9sAction::ListWatch,
        what: "single-object informer (fieldSelector metadata.name)",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods?fieldSelector=metadata.name%3Dweb&limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "PodList",
        },
    },
    Row {
        action: K9sAction::NamespaceSwitch,
        what: "namespaces list",
        req: Req::Get {
            path: "/api/v1/namespaces?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "NamespaceList",
        },
    },
    Row {
        action: K9sAction::ContextSwitch,
        what: "new context's version probe",
        req: Req::Get {
            path: "/version?timeout=32s",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "",
        },
    },
    // ── read views ───────────────────────────────────────────────────────
    Row {
        action: K9sAction::Describe,
        what: "describe: get",
        req: Req::Get {
            path: POD,
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Pod",
        },
    },
    Row {
        action: K9sAction::Describe,
        what: "describe: events by involvedObject",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/events?fieldSelector=involvedObject.name%3Dweb%2CinvolvedObject.namespace%3Dk9s&limit=500",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "EventList",
        },
    },
    Row {
        action: K9sAction::Yaml,
        what: "yaml: get",
        req: Req::Get {
            path: POD,
            accept: Some("application/json"),
        },
        expect: Expected::Object {
            status: 200,
            kind: "Pod",
        },
    },
    Row {
        action: K9sAction::Edit,
        what: "edit: get then update",
        req: Req::GetThenPut {
            path: "/api/v1/namespaces/k9s/configmaps/cfg",
            edit: label_edit,
        },
        expect: Expected::Object {
            status: 200,
            kind: "ConfigMap",
        },
    },
    Row {
        action: K9sAction::Delete,
        what: "delete, background propagation",
        req: Req::Delete {
            path: "/api/v1/namespaces/k9s/configmaps/doomed",
            body: background_delete,
        },
        expect: Expected::Object {
            status: 200,
            kind: "ConfigMap",
        },
    },
    Row {
        action: K9sAction::Kill,
        what: "kill, gracePeriodSeconds=0",
        req: Req::Delete {
            path: "/api/v1/namespaces/k9s/pods/victim",
            body: kill,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Pod",
        },
    },
    // ── logs ─────────────────────────────────────────────────────────────
    Row {
        action: K9sAction::Logs,
        what: "follow default container, timestamps",
        req: Req::Follow {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&follow=true&tailLines=100&timestamps=true",
        },
        expect: Expected::FollowChunk {
            contains: "main says hi",
        },
    },
    Row {
        action: K9sAction::LogsAllContainers,
        what: "follow the init container",
        req: Req::Follow {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=init&follow=true&tailLines=100&timestamps=true",
        },
        expect: Expected::FollowChunk {
            contains: "init done",
        },
    },
    Row {
        action: K9sAction::LogsAllContainers,
        what: "follow the app container",
        req: Req::Follow {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&follow=true&tailLines=100&timestamps=true",
        },
        expect: Expected::FollowChunk {
            contains: "main says hi",
        },
    },
    Row {
        action: K9sAction::LogsPrevious,
        what: "previous instance (none kept)",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&follow=true&previous=true&tailLines=100&timestamps=true",
            accept: None,
        },
        expect: Expected::Refused { status: 400 },
    },
    Row {
        action: K9sAction::LogsHead,
        what: "head: limitBytes=5000",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&limitBytes=5000&timestamps=true",
            accept: None,
        },
        expect: Expected::TextAtMost { bytes: 5000 },
    },
    Row {
        action: K9sAction::LogsSince,
        what: "sinceSeconds=300",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&sinceSeconds=300&tailLines=100&timestamps=true",
            accept: None,
        },
        expect: Expected::Lines {
            contains: "main says hi",
            absent: Some("old news"),
            stamped: true,
        },
    },
    Row {
        action: K9sAction::LogsSince,
        what: "sinceTime",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&sinceTime=2000-01-01T00%3A00%3A00Z",
            accept: None,
        },
        expect: Expected::Lines {
            contains: "old news",
            absent: None,
            stamped: false,
        },
    },
    Row {
        action: K9sAction::Logs,
        what: "timestamps prefix each line with one RFC3339 token",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=main&tailLines=3&timestamps=true",
            accept: None,
        },
        expect: Expected::Lines {
            contains: "main says hi",
            absent: None,
            stamped: true,
        },
    },
    Row {
        action: K9sAction::Logs,
        what: "no container: the default-container annotation",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?tailLines=1",
            accept: None,
        },
        expect: Expected::Lines {
            contains: "main says hi",
            absent: Some("sidecar"),
            stamped: false,
        },
    },
    Row {
        action: K9sAction::Logs,
        what: "no container, no annotation: the first in spec order",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/multi/log?tailLines=1",
            accept: None,
        },
        expect: Expected::Lines {
            contains: "zeta speaks",
            absent: None,
            stamped: false,
        },
    },
    Row {
        action: K9sAction::Logs,
        what: "unknown container is a clean 404",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/pods/web/log?container=nope&tailLines=100",
            accept: None,
        },
        expect: Expected::Refused { status: 404 },
    },
    // ── streams ──────────────────────────────────────────────────────────
    Row {
        action: K9sAction::Shell,
        what: "exec over WebSocket v5",
        req: Req::Upgrade {
            path: "/api/v1/namespaces/k9s/pods/web/exec?command=sh&container=main&stdin=true&stdout=true&stderr=true&tty=true",
            protocols: "v5.channel.k8s.io, v4.channel.k8s.io",
        },
        expect: Expected::Switched {
            protocol: "v5.channel.k8s.io",
        },
    },
    Row {
        action: K9sAction::Shell,
        what: "exec with no container: the first in spec order",
        req: Req::Upgrade {
            path: "/api/v1/namespaces/k9s/pods/multi/exec?command=sh&stdin=true&stdout=true&tty=true",
            protocols: "v5.channel.k8s.io, v4.channel.k8s.io",
        },
        expect: Expected::Switched {
            protocol: "v5.channel.k8s.io",
        },
    },
    Row {
        action: K9sAction::Attach,
        what: "attach over WebSocket v5",
        req: Req::Upgrade {
            path: "/api/v1/namespaces/k9s/pods/web/attach?container=main&stdin=true&stdout=true&tty=true",
            protocols: "v5.channel.k8s.io, v4.channel.k8s.io",
        },
        expect: Expected::Switched {
            protocol: "v5.channel.k8s.io",
        },
    },
    Row {
        action: K9sAction::PortForward,
        what: "SPDY-over-WebSocket tunnel",
        req: Req::Upgrade {
            path: "/api/v1/namespaces/k9s/pods/web/portforward",
            protocols: "SPDY/3.1+portforward.k8s.io",
        },
        expect: Expected::Absent {
            reason: "k9s's port-forward dialer speaks SPDY tunnelled in WebSocket (then plain SPDY); engenho relays only portforward.k8s.io WebSocket channels",
            degrade: Degrade::NoUpgrade {
                protocol: "SPDY/3.1+portforward.k8s.io",
            },
        },
    },
    Row {
        action: K9sAction::Benchmark,
        what: "benchmark needs a port-forward",
        req: Req::Upgrade {
            path: "/api/v1/namespaces/k9s/pods/web/portforward",
            protocols: "SPDY/3.1+portforward.k8s.io",
        },
        expect: Expected::Absent {
            reason: "rides on the port-forward gap",
            degrade: Degrade::NoUpgrade {
                protocol: "SPDY/3.1+portforward.k8s.io",
            },
        },
    },
    // ── workload mutations ───────────────────────────────────────────────
    Row {
        action: K9sAction::Scale,
        what: "scale: get scale",
        req: Req::Get {
            path: "/apis/apps/v1/namespaces/k9s/deployments/web/scale",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Scale",
        },
    },
    Row {
        action: K9sAction::Scale,
        what: "scale: update scale",
        req: Req::GetThenPut {
            path: "/apis/apps/v1/namespaces/k9s/deployments/web/scale",
            edit: scale_to_three,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Scale",
        },
    },
    Row {
        action: K9sAction::Restart,
        what: "restartedAt strategic-merge patch",
        req: Req::Patch {
            path: "/apis/apps/v1/namespaces/k9s/deployments/web",
            content_type: SMP,
            body: restart,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Deployment",
        },
    },
    Row {
        action: K9sAction::SetImage,
        what: "image strategic-merge patch",
        req: Req::Patch {
            path: "/apis/apps/v1/namespaces/k9s/deployments/web",
            content_type: SMP,
            body: set_image,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Deployment",
        },
    },
    Row {
        action: K9sAction::Cordon,
        what: "spec.unschedulable=true",
        req: Req::Patch {
            path: "/api/v1/nodes/n1",
            content_type: SMP,
            body: cordon,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Node",
        },
    },
    Row {
        action: K9sAction::Uncordon,
        what: "spec.unschedulable cleared",
        req: Req::Patch {
            path: "/api/v1/nodes/n1",
            content_type: SMP,
            body: uncordon,
        },
        expect: Expected::Object {
            status: 200,
            kind: "Node",
        },
    },
    Row {
        action: K9sAction::Drain,
        what: "pods/eviction",
        req: Req::Post {
            path: "/api/v1/namespaces/k9s/pods/doomed/eviction",
            body: eviction,
        },
        expect: Expected::Object {
            status: 201,
            kind: "Status",
        },
    },
    Row {
        action: K9sAction::Drain,
        what: "pods/eviction refused by a spent disruption budget",
        req: Req::Post {
            path: "/api/v1/namespaces/k9s/pods/guarded/eviction",
            body: guarded_eviction,
        },
        expect: Expected::Refused { status: 429 },
    },
    Row {
        action: K9sAction::Drain,
        what: "drain's fallback pod list",
        req: Req::Get {
            path: "/api/v1/pods?fieldSelector=spec.nodeName%3Dn1",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "PodList",
        },
    },
    // ── authz ────────────────────────────────────────────────────────────
    Row {
        action: K9sAction::AccessReview,
        what: "can-i get pods/log",
        req: Req::Post {
            path: "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            body: ssar_pod_log,
        },
        expect: Expected::Object {
            status: 201,
            kind: "SelfSubjectAccessReview",
        },
    },
    Row {
        action: K9sAction::AccessReview,
        what: "can-i list namespaces",
        req: Req::Post {
            path: "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            body: ssar_list_ns,
        },
        expect: Expected::Object {
            status: 201,
            kind: "SelfSubjectAccessReview",
        },
    },
    Row {
        action: K9sAction::AccessReview,
        what: "authorization.k8s.io/v1 discovered",
        req: Req::Get {
            path: "/apis/authorization.k8s.io/v1",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIResourceList",
        },
    },
    Row {
        action: K9sAction::AccessReview,
        what: "OpenAPI v3 for kubectl create -f ssar.yaml",
        req: Req::Get {
            path: "/openapi/v3/apis/authorization.k8s.io/v1",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "",
        },
    },
    Row {
        action: K9sAction::RulesReview,
        what: "rules in namespace",
        req: Req::Post {
            path: "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            body: ssrr,
        },
        expect: Expected::Object {
            status: 201,
            kind: "SelfSubjectRulesReview",
        },
    },
    // ── metrics ──────────────────────────────────────────────────────────
    Row {
        action: K9sAction::Metrics,
        what: "pod metrics",
        req: Req::Get {
            path: "/apis/metrics.k8s.io/v1beta1/namespaces/k9s/pods",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "PodMetricsList",
        },
    },
    Row {
        action: K9sAction::Metrics,
        what: "metrics.k8s.io discovered",
        req: Req::Get {
            path: "/apis/metrics.k8s.io/v1beta1",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "APIResourceList",
        },
    },
    Row {
        action: K9sAction::Metrics,
        what: "one pod's metrics",
        req: Req::Get {
            path: "/apis/metrics.k8s.io/v1beta1/namespaces/k9s/pods/web",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "PodMetrics",
        },
    },
    Row {
        action: K9sAction::Metrics,
        what: "an unmeasurable pod is a 404, never zeros",
        req: Req::Get {
            path: "/apis/metrics.k8s.io/v1beta1/namespaces/k9s/pods/blind",
            accept: None,
        },
        expect: Expected::Refused { status: 404 },
    },
    Row {
        action: K9sAction::Metrics,
        what: "node metrics",
        req: Req::Get {
            path: "/apis/metrics.k8s.io/v1beta1/nodes",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "NodeMetricsList",
        },
    },
    // ── other views ──────────────────────────────────────────────────────
    Row {
        action: K9sAction::Events,
        what: "core events",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/events?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "EventList",
        },
    },
    Row {
        action: K9sAction::Events,
        what: "events.k8s.io events",
        req: Req::Get {
            path: "/apis/events.k8s.io/v1/namespaces/k9s/events?limit=500",
            accept: None,
        },
        expect: Expected::Absent {
            reason: "events.k8s.io/v1 is not served (it would be a second projection of core Events, regarding/note for involvedObject/message); k9s resolves :events through discovery and falls back to v1 events",
            degrade: Degrade::NotDiscovered {
                group: "events.k8s.io",
            },
        },
    },
    Row {
        action: K9sAction::Helm,
        what: "helm releases (secrets owner=helm)",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/secrets?labelSelector=owner%3Dhelm",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "SecretList",
        },
    },
    Row {
        action: K9sAction::Xray,
        what: "deployments",
        req: Req::Get {
            path: "/apis/apps/v1/namespaces/k9s/deployments?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "DeploymentList",
        },
    },
    Row {
        action: K9sAction::Xray,
        what: "replicasets",
        req: Req::Get {
            path: "/apis/apps/v1/namespaces/k9s/replicasets?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "ReplicaSetList",
        },
    },
    Row {
        action: K9sAction::Xray,
        what: "services",
        req: Req::Get {
            path: "/api/v1/namespaces/k9s/services?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "ServiceList",
        },
    },
    Row {
        action: K9sAction::CrdBrowse,
        what: "crds",
        req: Req::Get {
            path: "/apis/apiextensions.k8s.io/v1/customresourcedefinitions?limit=500&resourceVersion=0",
            accept: None,
        },
        expect: Expected::Object {
            status: 200,
            kind: "CustomResourceDefinitionList",
        },
    },
];
