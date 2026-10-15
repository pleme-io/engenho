use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::Serialize;
use serde_json::{Value, json};

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: Cow<'static, str>,
    #[serde(rename = "type")]
    pub type_: Cow<'static, str>,
    pub format: Cow<'static, str>,
    pub description: Cow<'static, str>,
    pub priority: i32,
}

impl ColumnDefinition {
    const fn fixed(
        name: &'static str,
        type_: &'static str,
        format: &'static str,
        description: &'static str,
        priority: i32,
    ) -> Self {
        Self {
            name: Cow::Borrowed(name),
            type_: Cow::Borrowed(type_),
            format: Cow::Borrowed(format),
            description: Cow::Borrowed(description),
            priority,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Cell {
    Name,
    Age,
    Int(&'static str, i64),
    Count(&'static [&'static str]),
    Ratio(&'static str, &'static str, i64),
    Text(&'static str),
    TextOr(&'static str, &'static str),
    Containers(&'static str),
    Images(&'static str),
    Selector(&'static str),
    SelectorOrNone(&'static str),
    Labels(&'static str),
    Computed(fn(&Value, i64) -> Value),
}

#[derive(Debug)]
pub struct Column {
    pub definition: ColumnDefinition,
    pub cell: Cell,
}

#[derive(Debug)]
pub struct KindPrinter {
    pub group: &'static str,
    pub kind: &'static str,
    pub columns: &'static [Column],
    pub conditions: fn(&Value) -> Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustomColumn {
    pub definition: ColumnDefinition,
    pub json_path: String,
}

#[derive(Clone, Debug)]
pub enum Printer {
    Kind(&'static KindPrinter),
    Custom(Arc<[CustomColumn]>),
    Default,
}

const NAME_DESC: &str = "Name must be unique within a namespace. Is required when creating resources, although some resources may allow a client to request the generation of an appropriate name automatically. Name is primarily intended for creation idempotence and configuration definition. Cannot be updated. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names";
const AGE_DESC: &str = "CreationTimestamp is a timestamp representing the server time when this object was created. It is not guaranteed to be set in happens-before order across separate operations. Clients may not set this value. It is represented in RFC3339 form and is in UTC.\n\nPopulated by the system. Read-only. Null for lists. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata";
const CONTAINERS_DESC: &str = "Names of each container in the template.";
const IMAGES_DESC: &str = "Images referenced by each container in the template.";

const fn col(
    name: &'static str,
    type_: &'static str,
    description: &'static str,
    priority: i32,
    cell: Cell,
) -> Column {
    Column {
        definition: ColumnDefinition::fixed(name, type_, "", description, priority),
        cell,
    }
}

const NAME: Column = Column {
    definition: ColumnDefinition::fixed("Name", "string", "name", NAME_DESC, 0),
    cell: Cell::Name,
};
const AGE: Column = col("Age", "string", AGE_DESC, 0, Cell::Age);

fn no_conditions(_: &Value) -> Vec<Value> {
    Vec::new()
}

pub static KIND_PRINTERS: &[KindPrinter] = &[
    KindPrinter {
        group: "",
        kind: "Pod",
        columns: &[
            NAME,
            col(
                "Ready",
                "string",
                "The aggregate readiness state of this pod for accepting traffic.",
                0,
                Cell::Computed(pod_ready),
            ),
            col(
                "Status",
                "string",
                "The aggregate status of the containers in this pod.",
                0,
                Cell::Computed(pod_status),
            ),
            col(
                "Restarts",
                "string",
                "The number of times the containers in this pod have been restarted and when the last container in this pod has restarted.",
                0,
                Cell::Computed(pod_restarts),
            ),
            AGE,
            col(
                "IP",
                "string",
                "podIP address allocated to the pod. Routable at least within the cluster. Empty if not yet allocated.",
                1,
                Cell::Computed(pod_ip),
            ),
            col(
                "Node",
                "string",
                "NodeName indicates in which node this pod is scheduled.",
                1,
                Cell::TextOr("/spec/nodeName", "<none>"),
            ),
            col(
                "Nominated Node",
                "string",
                "nominatedNodeName is set only when this pod preempts other pods on the node.",
                1,
                Cell::TextOr("/status/nominatedNodeName", "<none>"),
            ),
            col(
                "Readiness Gates",
                "string",
                "If specified, all readiness gates will be evaluated for pod readiness.",
                1,
                Cell::Computed(pod_readiness_gates),
            ),
        ],
        conditions: pod_row_conditions,
    },
    KindPrinter {
        group: "",
        kind: "Node",
        columns: &[
            NAME,
            col(
                "Status",
                "string",
                "The status of the node",
                0,
                Cell::Computed(node_status),
            ),
            col(
                "Roles",
                "string",
                "The roles of the node",
                0,
                Cell::Computed(node_roles),
            ),
            AGE,
            col(
                "Version",
                "string",
                "Kubelet Version reported by the node.",
                0,
                Cell::Text("/status/nodeInfo/kubeletVersion"),
            ),
            col(
                "Internal-IP",
                "string",
                "List of addresses reachable to the node.",
                1,
                Cell::Computed(node_internal_ip),
            ),
            col(
                "External-IP",
                "string",
                "List of addresses reachable to the node.",
                1,
                Cell::Computed(node_external_ip),
            ),
            col(
                "OS-Image",
                "string",
                "OS Image reported by the node from /etc/os-release (e.g. Debian GNU/Linux 7 (wheezy)).",
                1,
                Cell::TextOr("/status/nodeInfo/osImage", "<unknown>"),
            ),
            col(
                "Kernel-Version",
                "string",
                "Kernel Version reported by the node from 'uname -r' (e.g. 3.16.0-0.bpo.4-amd64).",
                1,
                Cell::TextOr("/status/nodeInfo/kernelVersion", "<unknown>"),
            ),
            col(
                "Container-Runtime",
                "string",
                "ContainerRuntime Version reported by the node through runtime remote API (e.g. containerd://1.4.2).",
                1,
                Cell::TextOr("/status/nodeInfo/containerRuntimeVersion", "<unknown>"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "apps",
        kind: "Deployment",
        columns: &[
            NAME,
            col(
                "Ready",
                "string",
                "Number of the pod with ready state",
                0,
                Cell::Ratio("/status/readyReplicas", "/spec/replicas", 1),
            ),
            col(
                "Up-to-date",
                "string",
                "Total number of non-terminating pods targeted by this deployment that have the desired template spec.",
                0,
                Cell::Int("/status/updatedReplicas", 0),
            ),
            col(
                "Available",
                "string",
                "Total number of available non-terminating pods (ready for at least minReadySeconds) targeted by this deployment.",
                0,
                Cell::Int("/status/availableReplicas", 0),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/template/spec/containers"),
            ),
            col(
                "Selector",
                "string",
                "Label selector for pods. Existing ReplicaSets whose pods are selected by this will be the ones affected by this deployment.",
                1,
                Cell::Selector("/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "apps",
        kind: "ReplicaSet",
        columns: &[
            NAME,
            col(
                "Desired",
                "integer",
                "Replicas is the number of desired pods.",
                0,
                Cell::Int("/spec/replicas", 1),
            ),
            col(
                "Current",
                "integer",
                "Replicas is the most recently observed number of non-terminating pods.",
                0,
                Cell::Int("/status/replicas", 0),
            ),
            col(
                "Ready",
                "integer",
                "The number of non-terminating pods targeted by this ReplicaSet with a Ready Condition.",
                0,
                Cell::Int("/status/readyReplicas", 0),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/template/spec/containers"),
            ),
            col(
                "Selector",
                "string",
                "Selector is a label query over pods that should match the replica count.",
                1,
                Cell::SelectorOrNone("/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "apps",
        kind: "StatefulSet",
        columns: &[
            NAME,
            col(
                "Ready",
                "string",
                "Number of the pod with ready state",
                0,
                Cell::Ratio("/status/readyReplicas", "/spec/replicas", 1),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/template/spec/containers"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "apps",
        kind: "DaemonSet",
        columns: &[
            NAME,
            col(
                "Desired",
                "integer",
                "The total number of nodes that should be running the daemon pod (including nodes correctly running the daemon pod).",
                0,
                Cell::Int("/status/desiredNumberScheduled", 0),
            ),
            col(
                "Current",
                "integer",
                "The number of nodes that are running at least 1 daemon pod and are supposed to run the daemon pod.",
                0,
                Cell::Int("/status/currentNumberScheduled", 0),
            ),
            col(
                "Ready",
                "integer",
                "numberReady is the number of nodes that should be running the daemon pod and have one or more of the daemon pod running with a Ready Condition.",
                0,
                Cell::Int("/status/numberReady", 0),
            ),
            col(
                "Up-to-date",
                "integer",
                "The total number of nodes that are running updated daemon pod",
                0,
                Cell::Int("/status/updatedNumberScheduled", 0),
            ),
            col(
                "Available",
                "integer",
                "The number of nodes that should be running the daemon pod and have one or more of the daemon pod running and available (ready for at least spec.minReadySeconds)",
                0,
                Cell::Int("/status/numberAvailable", 0),
            ),
            col(
                "Node Selector",
                "string",
                "NodeSelector is a selector which must be true for the pod to fit on a node.",
                0,
                Cell::Labels("/spec/template/spec/nodeSelector"),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/template/spec/containers"),
            ),
            col(
                "Selector",
                "string",
                "A label query over pods that are managed by the daemon set.",
                1,
                Cell::SelectorOrNone("/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "batch",
        kind: "Job",
        columns: &[
            NAME,
            col(
                "Status",
                "string",
                "Status of the job.",
                0,
                Cell::Computed(job_status),
            ),
            col(
                "Completions",
                "string",
                "The number of pods which reached phase Succeeded.",
                0,
                Cell::Computed(job_completions),
            ),
            col(
                "Duration",
                "string",
                "Time required to complete the job.",
                0,
                Cell::Computed(job_duration),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/template/spec/containers"),
            ),
            col(
                "Selector",
                "string",
                "A label query over pods that should match the pod count.",
                1,
                Cell::SelectorOrNone("/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "batch",
        kind: "CronJob",
        columns: &[
            NAME,
            col(
                "Schedule",
                "string",
                "The schedule in Cron format, see https://en.wikipedia.org/wiki/Cron.",
                0,
                Cell::Text("/spec/schedule"),
            ),
            col(
                "Timezone",
                "string",
                "The time zone name for the given schedule.",
                0,
                Cell::TextOr("/spec/timeZone", "<none>"),
            ),
            col(
                "Suspend",
                "boolean",
                "This flag tells the controller to suspend subsequent executions.",
                0,
                Cell::Computed(cronjob_suspend),
            ),
            col(
                "Active",
                "integer",
                "A list of pointers to currently running jobs.",
                0,
                Cell::Count(&["/status/active"]),
            ),
            col(
                "Last Schedule",
                "string",
                "Information when was the last time the job was successfully scheduled.",
                0,
                Cell::Computed(cronjob_last_schedule),
            ),
            AGE,
            col(
                "Containers",
                "string",
                CONTAINERS_DESC,
                1,
                Cell::Containers("/spec/jobTemplate/spec/template/spec/containers"),
            ),
            col(
                "Images",
                "string",
                IMAGES_DESC,
                1,
                Cell::Images("/spec/jobTemplate/spec/template/spec/containers"),
            ),
            col(
                "Selector",
                "string",
                "A label query over pods that should match the pod count.",
                1,
                Cell::SelectorOrNone("/spec/jobTemplate/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "",
        kind: "Service",
        columns: &[
            NAME,
            col(
                "Type",
                "string",
                "type determines how the Service is exposed.",
                0,
                Cell::Computed(service_type),
            ),
            col(
                "Cluster-IP",
                "string",
                "clusterIP is the IP address of the service and is usually assigned randomly.",
                0,
                Cell::Computed(service_cluster_ip),
            ),
            col(
                "External-IP",
                "string",
                "externalIPs is a list of IP addresses for which nodes in the cluster will also accept traffic for this service.",
                0,
                Cell::Computed(service_external_ip),
            ),
            col(
                "Port(s)",
                "string",
                "The list of ports that are exposed by this service.",
                0,
                Cell::Computed(service_ports),
            ),
            AGE,
            col(
                "Selector",
                "string",
                "Route service traffic to pods with label keys and values matching this selector.",
                1,
                Cell::Labels("/spec/selector"),
            ),
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "",
        kind: "Namespace",
        columns: &[
            NAME,
            col(
                "Status",
                "string",
                "The status of the namespace",
                0,
                Cell::Text("/status/phase"),
            ),
            AGE,
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "",
        kind: "ConfigMap",
        columns: &[
            NAME,
            col(
                "Data",
                "string",
                "Data contains the configuration data.",
                0,
                Cell::Count(&["/data", "/binaryData"]),
            ),
            AGE,
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "",
        kind: "Secret",
        columns: &[
            NAME,
            col(
                "Type",
                "string",
                "Used to facilitate programmatic handling of secret data.",
                0,
                Cell::Text("/type"),
            ),
            col(
                "Data",
                "string",
                "Data contains the secret data.",
                0,
                Cell::Count(&["/data"]),
            ),
            AGE,
        ],
        conditions: no_conditions,
    },
    KindPrinter {
        group: "",
        kind: "Event",
        columns: &[
            col(
                "Last Seen",
                "string",
                "The time at which the most recent occurrence of this event was recorded.",
                0,
                Cell::Computed(event_last_seen),
            ),
            col(
                "Type",
                "string",
                "Type of this event (Normal, Warning), new types could be added in the future",
                0,
                Cell::Text("/type"),
            ),
            col(
                "Reason",
                "string",
                "This should be a short, machine understandable string that gives the reason for the transition into the object's current status.",
                0,
                Cell::Text("/reason"),
            ),
            col(
                "Object",
                "string",
                "The object that this event is about.",
                0,
                Cell::Computed(event_object),
            ),
            col(
                "Subobject",
                "string",
                "If referring to a piece of an object instead of an entire object, this string should contain a valid JSON/Go field access statement.",
                1,
                Cell::Text("/involvedObject/fieldPath"),
            ),
            col(
                "Source",
                "string",
                "The component reporting this event.",
                1,
                Cell::Computed(event_source),
            ),
            col(
                "Message",
                "string",
                "A human-readable description of the status of this operation.",
                0,
                Cell::Computed(event_message),
            ),
            col(
                "First Seen",
                "string",
                "The time at which the event was first recorded.",
                1,
                Cell::Computed(event_first_seen),
            ),
            col(
                "Count",
                "string",
                "The number of times this event has occurred.",
                1,
                Cell::Computed(event_count),
            ),
            Column {
                definition: ColumnDefinition::fixed("Name", "string", "name", NAME_DESC, 1),
                cell: Cell::Name,
            },
        ],
        conditions: no_conditions,
    },
];

const DEFAULT_COLUMNS: &[Column] = &[NAME, AGE];

impl Printer {
    #[must_use]
    pub fn for_kind(group: &str, kind: &str) -> Self {
        KIND_PRINTERS
            .iter()
            .find(|p| p.group == group && p.kind == kind)
            .map_or(Self::Default, Self::Kind)
    }

    #[must_use]
    pub fn for_crd(additional_printer_columns: &[Value]) -> Self {
        let mut columns: Vec<CustomColumn> = additional_printer_columns
            .iter()
            .filter_map(|c| {
                let json_path = c.get("jsonPath")?.as_str()?.to_string();
                let text = |field: &str| c.get(field).and_then(Value::as_str).unwrap_or_default();
                let description = match text("description") {
                    "" => format!(
                        "Custom resource definition column (in JSONPath format): {json_path}"
                    ),
                    d => d.to_string(),
                };
                Some(CustomColumn {
                    definition: ColumnDefinition {
                        name: Cow::Owned(c.get("name")?.as_str()?.to_string()),
                        type_: Cow::Owned(match text("type") {
                            "" => "string".to_string(),
                            t => t.to_string(),
                        }),
                        format: Cow::Owned(text("format").to_string()),
                        description: Cow::Owned(description),
                        priority: c
                            .get("priority")
                            .and_then(Value::as_i64)
                            .and_then(|p| i32::try_from(p).ok())
                            .unwrap_or(0),
                    },
                    json_path,
                })
            })
            .collect();
        if columns.is_empty() {
            columns.push(CustomColumn {
                definition: ColumnDefinition::fixed("Age", "date", "", AGE_DESC, 0),
                json_path: ".metadata.creationTimestamp".to_string(),
            });
        }
        Self::Custom(columns.into())
    }

    #[must_use]
    pub fn columns(&self) -> Vec<ColumnDefinition> {
        match self {
            Self::Kind(p) => p.columns.iter().map(|c| c.definition.clone()).collect(),
            Self::Default => DEFAULT_COLUMNS
                .iter()
                .map(|c| c.definition.clone())
                .collect(),
            Self::Custom(cols) => std::iter::once(NAME.definition.clone())
                .chain(cols.iter().map(|c| c.definition.clone()))
                .collect(),
        }
    }

    #[must_use]
    pub fn cells(&self, obj: &Value, now: i64) -> Vec<Value> {
        match self {
            Self::Kind(p) => p.columns.iter().map(|c| render(c.cell, obj, now)).collect(),
            Self::Default => DEFAULT_COLUMNS
                .iter()
                .map(|c| render(c.cell, obj, now))
                .collect(),
            Self::Custom(cols) => std::iter::once(render(Cell::Name, obj, now))
                .chain(cols.iter().map(|c| custom_cell(c, obj, now)))
                .collect(),
        }
    }

    #[must_use]
    pub fn conditions(&self, obj: &Value) -> Vec<Value> {
        match self {
            Self::Kind(p) => (p.conditions)(obj),
            Self::Custom(_) | Self::Default => Vec::new(),
        }
    }
}

fn text_at<'a>(obj: &'a Value, pointer: &str) -> Option<&'a str> {
    obj.pointer(pointer).and_then(Value::as_str)
}

fn int_at(obj: &Value, pointer: &str) -> Option<i64> {
    obj.pointer(pointer).and_then(Value::as_i64)
}

fn len_at(obj: &Value, pointer: &str) -> usize {
    match obj.pointer(pointer) {
        Some(Value::Array(a)) => a.len(),
        Some(Value::Object(o)) => o.len(),
        _ => 0,
    }
}

fn render(cell: Cell, obj: &Value, now: i64) -> Value {
    match cell {
        Cell::Name => obj
            .pointer("/metadata/name")
            .cloned()
            .unwrap_or(Value::Null),
        Cell::Age => json!(age(text_at(obj, "/metadata/creationTimestamp"), now)),
        Cell::Int(pointer, default) => json!(int_at(obj, pointer).unwrap_or(default)),
        Cell::Count(pointers) => json!(pointers.iter().map(|p| len_at(obj, p)).sum::<usize>()),
        Cell::Ratio(num, den, den_default) => json!(format!(
            "{}/{}",
            int_at(obj, num).unwrap_or(0),
            int_at(obj, den).unwrap_or(den_default)
        )),
        Cell::Text(pointer) => json!(text_at(obj, pointer).unwrap_or_default()),
        Cell::TextOr(pointer, default) => json!(
            text_at(obj, pointer)
                .filter(|s| !s.is_empty())
                .unwrap_or(default)
        ),
        Cell::Containers(pointer) => json!(container_field(obj, pointer, "name")),
        Cell::Images(pointer) => json!(container_field(obj, pointer, "image")),
        Cell::Selector(pointer) => {
            json!(selector_string(obj.pointer(pointer)).unwrap_or_else(|| "<invalid>".to_string()))
        }
        Cell::SelectorOrNone(pointer) => json!(match selector_string(obj.pointer(pointer)) {
            Some(s) if s.is_empty() => "<none>".to_string(),
            Some(s) => s,
            None => "<error>".to_string(),
        }),
        Cell::Labels(pointer) => json!(format_labels(obj.pointer(pointer))),
        Cell::Computed(f) => f(obj, now),
    }
}

fn container_field(obj: &Value, pointer: &str, field: &str) -> String {
    obj.pointer(pointer)
        .and_then(Value::as_array)
        .map(|cs| {
            cs.iter()
                .map(|c| c.get(field).and_then(Value::as_str).unwrap_or_default())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn format_labels(map: Option<&Value>) -> String {
    let pairs: Vec<String> = map
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();
    if pairs.is_empty() {
        "<none>".to_string()
    } else {
        pairs.join(",")
    }
}

fn selector_string(selector: Option<&Value>) -> Option<String> {
    let Some(selector) = selector.filter(|s| !s.is_null()) else {
        return Some(String::new());
    };
    let mut requirements: Vec<(String, String)> = Vec::new();
    if let Some(labels) = selector.get("matchLabels").and_then(Value::as_object) {
        for (k, v) in labels {
            requirements.push((k.clone(), format!("{k}={}", v.as_str().unwrap_or_default())));
        }
    }
    for expr in selector
        .get("matchExpressions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let key = expr.get("key").and_then(Value::as_str)?.to_string();
        let values: BTreeSet<&str> = expr
            .get("values")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let joined = values.into_iter().collect::<Vec<_>>().join(",");
        let rendered = match expr.get("operator").and_then(Value::as_str)? {
            "In" => format!("{key} in ({joined})"),
            "NotIn" => format!("{key} notin ({joined})"),
            "Exists" => key.clone(),
            "DoesNotExist" => format!("!{key}"),
            _ => return None,
        };
        requirements.push((key, rendered));
    }
    requirements.sort_by(|a, b| a.0.cmp(&b.0));
    Some(
        requirements
            .into_iter()
            .map(|(_, r)| r)
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn unix(stamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(stamp)
        .ok()
        .map(|t| t.timestamp())
}

#[must_use]
pub fn age(stamp: Option<&str>, now: i64) -> String {
    match stamp.filter(|s| !s.is_empty()).and_then(unix) {
        Some(t) => human_duration(now - t),
        None => "<unknown>".to_string(),
    }
}

#[must_use]
pub fn human_duration(seconds: i64) -> String {
    if seconds < -1 {
        return "<invalid>".to_string();
    }
    if seconds < 0 {
        return "0s".to_string();
    }
    if seconds < 60 * 2 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 10 {
        let s = seconds % 60;
        return if s == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m{s}s")
        };
    }
    if minutes < 60 * 3 {
        return format!("{minutes}m");
    }
    let hours = seconds / 3600;
    if hours < 8 {
        let m = minutes % 60;
        return if m == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h{m}m")
        };
    }
    if hours < 48 {
        return format!("{hours}h");
    }
    if hours < 24 * 8 {
        let h = hours % 24;
        return if h == 0 {
            format!("{}d", hours / 24)
        } else {
            format!("{}d{h}h", hours / 24)
        };
    }
    if hours < 24 * 365 * 2 {
        return format!("{}d", hours / 24);
    }
    if hours < 24 * 365 * 8 {
        let dy = (hours / 24) % 365;
        return if dy == 0 {
            format!("{}y", hours / 24 / 365)
        } else {
            format!("{}y{dy}d", hours / 24 / 365)
        };
    }
    format!("{}y", hours / 24 / 365)
}

struct PodSummary {
    ready: usize,
    total: usize,
    reason: String,
    restarts: String,
}

fn statuses<'a>(pod: &'a Value, field: &str) -> &'a [Value] {
    pod.get("status")
        .and_then(|s| s.get(field))
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn condition_true(conditions: &[Value], type_: &str) -> Option<bool> {
    conditions
        .iter()
        .find(|c| c.get("type").and_then(Value::as_str) == Some(type_))
        .map(|c| c.get("status").and_then(Value::as_str) == Some("True"))
}

fn terminated_reason(terminated: &Value, prefix: &str) -> String {
    match terminated.get("reason").and_then(Value::as_str) {
        Some(r) if !r.is_empty() => format!("{prefix}{r}"),
        _ => match terminated.get("signal").and_then(Value::as_i64) {
            Some(sig) if sig != 0 => format!("{prefix}Signal:{sig}"),
            _ => format!(
                "{prefix}ExitCode:{}",
                terminated
                    .get("exitCode")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
            ),
        },
    }
}

fn last_termination(status: &Value) -> Option<i64> {
    status
        .pointer("/lastState/terminated/finishedAt")
        .and_then(Value::as_str)
        .and_then(unix)
}

fn restart_count(status: &Value) -> i64 {
    status
        .get("restartCount")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

#[allow(clippy::too_many_lines)]
fn pod_summary(pod: &Value, now: i64) -> PodSummary {
    let spec_containers = pod
        .pointer("/spec/containers")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let init_specs: Vec<&Value> = pod
        .pointer("/spec/initContainers")
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    let restartable = |name: &str| {
        init_specs.iter().any(|c| {
            c.get("name").and_then(Value::as_str) == Some(name)
                && c.get("restartPolicy").and_then(Value::as_str) == Some("Always")
        })
    };
    let total = spec_containers
        + init_specs
            .iter()
            .filter(|c| c.get("restartPolicy").and_then(Value::as_str) == Some("Always"))
            .count();
    let conditions = statuses(pod, "conditions");
    let mut reason = pod
        .pointer("/status/reason")
        .and_then(Value::as_str)
        .filter(|r| !r.is_empty())
        .or_else(|| pod.pointer("/status/phase").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    if conditions.iter().any(|c| {
        c.get("type").and_then(Value::as_str) == Some("PodScheduled")
            && c.get("reason").and_then(Value::as_str) == Some("SchedulingGated")
    }) {
        reason = "SchedulingGated".to_string();
    }

    let mut ready = 0;
    let mut restarts = 0;
    let mut sidecar_restarts = 0;
    let mut last_restart: Option<i64> = None;
    let mut last_sidecar_restart: Option<i64> = None;
    let mut initializing = false;
    let init_statuses = statuses(pod, "initContainerStatuses");
    for (i, status) in init_statuses.iter().enumerate() {
        let name = status
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        restarts += restart_count(status);
        let finished = last_termination(status);
        last_restart = last_restart.max(finished);
        if restartable(name) {
            sidecar_restarts += restart_count(status);
            last_sidecar_restart = last_sidecar_restart.max(finished);
        }
        let state = status.get("state");
        let terminated = state.and_then(|s| s.get("terminated"));
        let waiting_reason = state
            .and_then(|s| s.pointer("/waiting/reason"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if terminated.is_some_and(|t| t.get("exitCode").and_then(Value::as_i64) == Some(0)) {
            continue;
        }
        if restartable(name) && status.get("started").and_then(Value::as_bool) == Some(true) {
            if status.get("ready").and_then(Value::as_bool) == Some(true) {
                ready += 1;
            }
            continue;
        }
        if let Some(terminated) = terminated {
            reason = terminated_reason(terminated, "Init:");
        } else if !waiting_reason.is_empty() && waiting_reason != "PodInitializing" {
            reason = format!("Init:{waiting_reason}");
        } else {
            reason = format!("Init:{i}/{}", init_specs.len());
        }
        initializing = true;
        break;
    }

    if !initializing || condition_true(conditions, "Initialized") == Some(true) {
        restarts = sidecar_restarts;
        last_restart = last_sidecar_restart;
        let mut has_running = false;
        for status in statuses(pod, "containerStatuses").iter().rev() {
            restarts += restart_count(status);
            last_restart = last_restart.max(last_termination(status));
            let state = status.get("state");
            let waiting = state
                .and_then(|s| s.pointer("/waiting/reason"))
                .and_then(Value::as_str)
                .filter(|r| !r.is_empty());
            let terminated = state.and_then(|s| s.get("terminated"));
            if let Some(waiting) = waiting {
                reason = waiting.to_string();
            } else if let Some(terminated) = terminated {
                reason = terminated_reason(terminated, "");
            } else if status.get("ready").and_then(Value::as_bool) == Some(true)
                && state.and_then(|s| s.get("running")).is_some()
            {
                has_running = true;
                ready += 1;
            }
        }
        if reason == "Completed" && has_running {
            reason = if condition_true(conditions, "Ready") == Some(true) {
                "Running".to_string()
            } else {
                "NotReady".to_string()
            };
        }
    }

    let deleting = pod
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|d| !d.is_null());
    let phase = pod
        .pointer("/status/phase")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if deleting && pod.pointer("/status/reason").and_then(Value::as_str) == Some("NodeLost") {
        reason = "Unknown".to_string();
    } else if deleting && phase != "Succeeded" && phase != "Failed" {
        reason = "Terminating".to_string();
    }

    let restarts = match last_restart {
        Some(at) if restarts != 0 => format!("{restarts} ({} ago)", human_duration(now - at)),
        _ => restarts.to_string(),
    };
    PodSummary {
        ready,
        total,
        reason,
        restarts,
    }
}

fn pod_ready(pod: &Value, now: i64) -> Value {
    let s = pod_summary(pod, now);
    json!(format!("{}/{}", s.ready, s.total))
}

fn pod_status(pod: &Value, now: i64) -> Value {
    json!(pod_summary(pod, now).reason)
}

fn pod_restarts(pod: &Value, now: i64) -> Value {
    json!(pod_summary(pod, now).restarts)
}

fn pod_ip(pod: &Value, _: i64) -> Value {
    json!(
        pod.pointer("/status/podIPs/0/ip")
            .and_then(Value::as_str)
            .filter(|ip| !ip.is_empty())
            .unwrap_or("<none>")
    )
}

fn pod_readiness_gates(pod: &Value, _: i64) -> Value {
    let gates = pod
        .pointer("/spec/readinessGates")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    if gates.is_empty() {
        return json!("<none>");
    }
    let conditions = statuses(pod, "conditions");
    let met = gates
        .iter()
        .filter_map(|g| g.get("conditionType").and_then(Value::as_str))
        .filter(|t| condition_true(conditions, t) == Some(true))
        .count();
    json!(format!("{met}/{}", gates.len()))
}

fn pod_row_conditions(pod: &Value) -> Vec<Value> {
    let completed = |reason: &str, message: &str| {
        vec![json!({"type": "Completed", "status": "True", "reason": reason, "message": message})]
    };
    match pod.pointer("/status/phase").and_then(Value::as_str) {
        Some("Succeeded") => completed("Succeeded", "The pod has completed successfully."),
        Some("Failed") => completed("Failed", "The pod failed."),
        _ => Vec::new(),
    }
}

fn node_status(node: &Value, _: i64) -> Value {
    let mut status = vec![
        match condition_true(statuses(node, "conditions"), "Ready") {
            Some(true) => "Ready",
            Some(false) => "NotReady",
            None => "Unknown",
        }
        .to_string(),
    ];
    if node.pointer("/spec/unschedulable").and_then(Value::as_bool) == Some(true) {
        status.push("SchedulingDisabled".to_string());
    }
    json!(status.join(","))
}

fn node_roles(node: &Value, _: i64) -> Value {
    let roles: BTreeSet<&str> = node
        .pointer("/metadata/labels")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| {
            if let Some(role) = k.strip_prefix("node-role.kubernetes.io/") {
                (!role.is_empty()).then_some(role)
            } else if k == "kubernetes.io/role" {
                v.as_str().filter(|r| !r.is_empty())
            } else {
                None
            }
        })
        .collect();
    if roles.is_empty() {
        json!("<none>")
    } else {
        json!(roles.into_iter().collect::<Vec<_>>().join(","))
    }
}

fn node_address(node: &Value, type_: &str) -> Value {
    json!(
        statuses(node, "addresses")
            .iter()
            .find(|a| a.get("type").and_then(Value::as_str) == Some(type_))
            .and_then(|a| a.get("address").and_then(Value::as_str))
            .unwrap_or("<none>")
    )
}

fn node_internal_ip(node: &Value, _: i64) -> Value {
    node_address(node, "InternalIP")
}

fn node_external_ip(node: &Value, _: i64) -> Value {
    node_address(node, "ExternalIP")
}

fn job_condition(job: &Value, type_: &str) -> bool {
    condition_true(statuses(job, "conditions"), type_) == Some(true)
}

fn job_status(job: &Value, _: i64) -> Value {
    let deleting = job
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|d| !d.is_null());
    json!(if job_condition(job, "Complete") {
        "Complete"
    } else if job_condition(job, "Failed") {
        "Failed"
    } else if deleting {
        "Terminating"
    } else if job_condition(job, "Suspended") {
        "Suspended"
    } else if job_condition(job, "FailureTarget") {
        "FailureTarget"
    } else if job_condition(job, "SuccessCriteriaMet") {
        "SuccessCriteriaMet"
    } else {
        "Running"
    })
}

fn job_completions(job: &Value, _: i64) -> Value {
    let succeeded = int_at(job, "/status/succeeded").unwrap_or(0);
    json!(match int_at(job, "/spec/completions") {
        Some(completions) => format!("{succeeded}/{completions}"),
        None => match int_at(job, "/spec/parallelism").unwrap_or(0) {
            p if p > 1 => format!("{succeeded}/1 of {p}"),
            _ => format!("{succeeded}/1"),
        },
    })
}

fn job_duration(job: &Value, now: i64) -> Value {
    let stamp = |p: &str| text_at(job, p).and_then(unix);
    json!(
        match (stamp("/status/startTime"), stamp("/status/completionTime")) {
            (None, _) => String::new(),
            (Some(start), None) => human_duration(now - start),
            (Some(start), Some(end)) => human_duration(end - start),
        }
    )
}

fn cronjob_suspend(cj: &Value, _: i64) -> Value {
    json!(match cj.pointer("/spec/suspend").and_then(Value::as_bool) {
        Some(true) => "True",
        Some(false) => "False",
        None => "<unset>",
    })
}

fn cronjob_last_schedule(cj: &Value, now: i64) -> Value {
    json!(match text_at(cj, "/status/lastScheduleTime") {
        Some(t) => age(Some(t), now),
        None => "<none>".to_string(),
    })
}

fn service_type(svc: &Value, _: i64) -> Value {
    json!(
        text_at(svc, "/spec/type")
            .filter(|t| !t.is_empty())
            .unwrap_or("ClusterIP")
    )
}

fn service_cluster_ip(svc: &Value, _: i64) -> Value {
    json!(
        text_at(svc, "/spec/clusterIPs/0")
            .or_else(|| text_at(svc, "/spec/clusterIP"))
            .filter(|ip| !ip.is_empty())
            .unwrap_or("<none>")
    )
}

fn strings_at<'a>(obj: &'a Value, pointer: &str) -> Vec<&'a str> {
    obj.pointer(pointer)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn service_external_ip(svc: &Value, _: i64) -> Value {
    let external = strings_at(svc, "/spec/externalIPs");
    let ty = service_type(svc, 0);
    json!(match ty.as_str().unwrap_or_default() {
        "ClusterIP" | "NodePort" if external.is_empty() => "<none>".to_string(),
        "ClusterIP" | "NodePort" => external.join(","),
        "LoadBalancer" => {
            let ingress: BTreeSet<&str> = svc
                .pointer("/status/loadBalancer/ingress")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|i| {
                    text_at(i, "/ip")
                        .filter(|s| !s.is_empty())
                        .or_else(|| text_at(i, "/hostname").filter(|s| !s.is_empty()))
                })
                .collect();
            let mut all: Vec<&str> = ingress.into_iter().collect();
            all.extend(external);
            if all.is_empty() {
                "<pending>".to_string()
            } else {
                all.join(",")
            }
        }
        "ExternalName" => text_at(svc, "/spec/externalName")
            .unwrap_or_default()
            .to_string(),
        _ => "<unknown>".to_string(),
    })
}

fn service_ports(svc: &Value, _: i64) -> Value {
    let ports: Vec<String> = svc
        .pointer("/spec/ports")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|p| {
            let port = p.get("port").and_then(Value::as_i64).unwrap_or(0);
            let protocol = p.get("protocol").and_then(Value::as_str).unwrap_or("TCP");
            match p.get("nodePort").and_then(Value::as_i64) {
                Some(node_port) if node_port > 0 => format!("{port}:{node_port}/{protocol}"),
                _ => format!("{port}/{protocol}"),
            }
        })
        .collect();
    json!(if ports.is_empty() {
        "<none>".to_string()
    } else {
        ports.join(",")
    })
}

fn event_first_seen(ev: &Value, now: i64) -> Value {
    json!(
        match text_at(ev, "/firstTimestamp").filter(|t| !t.is_empty()) {
            Some(t) => age(Some(t), now),
            None => age(text_at(ev, "/eventTime"), now),
        }
    )
}

fn event_last_seen(ev: &Value, now: i64) -> Value {
    if let Some(series) = text_at(ev, "/series/lastObservedTime") {
        return json!(age(Some(series), now));
    }
    match text_at(ev, "/lastTimestamp").filter(|t| !t.is_empty()) {
        Some(t) => json!(age(Some(t), now)),
        None => event_first_seen(ev, now),
    }
}

fn event_object(ev: &Value, _: i64) -> Value {
    let kind = text_at(ev, "/involvedObject/kind")
        .unwrap_or_default()
        .to_lowercase();
    json!(
        match text_at(ev, "/involvedObject/name").filter(|n| !n.is_empty()) {
            Some(name) => format!("{kind}/{name}"),
            None => kind,
        }
    )
}

fn event_source(ev: &Value, _: i64) -> Value {
    let first = |a: &str, b: &str| {
        text_at(ev, a)
            .filter(|s| !s.is_empty())
            .or_else(|| text_at(ev, b))
            .unwrap_or_default()
            .to_string()
    };
    let component = first("/source/component", "/reportingComponent");
    let instance = first("/source/host", "/reportingInstance");
    json!(if instance.is_empty() {
        component
    } else {
        format!("{component}, {instance}")
    })
}

fn event_message(ev: &Value, _: i64) -> Value {
    json!(text_at(ev, "/message").unwrap_or_default().trim())
}

fn event_count(ev: &Value, _: i64) -> Value {
    json!(match ev.pointer("/series/count").and_then(Value::as_i64) {
        Some(n) => n,
        None => match int_at(ev, "/count") {
            Some(0) | None => 1,
            Some(n) => n,
        },
    })
}

fn custom_cell(column: &CustomColumn, obj: &Value, now: i64) -> Value {
    let Some(value) = resolve_json_path(obj, &column.json_path) else {
        return Value::Null;
    };
    match column.definition.type_.as_ref() {
        "string" => match value {
            Value::Null => Value::Null,
            Value::String(s) => json!(s),
            other => json!(other.to_string()),
        },
        "integer" => value
            .as_i64()
            .or_else(|| value.as_f64().map(|f| f as i64))
            .map_or(Value::Null, |i| json!(i)),
        "number" => value.as_f64().map_or(Value::Null, |f| json!(f)),
        "boolean" => value.as_bool().map_or(Value::Null, |b| json!(b)),
        "date" => match value.as_str() {
            Some(t) => json!(match unix(t) {
                Some(at) => human_duration(now - at),
                None => "<invalid>".to_string(),
            }),
            None => Value::Null,
        },
        _ => Value::Null,
    }
}

enum Step {
    Field(String),
    Index(usize),
    Filter(String, String),
}

fn parse_json_path(path: &str) -> Option<Vec<Step>> {
    let path = path.trim();
    let path = path
        .strip_prefix('{')
        .and_then(|p| p.strip_suffix('}'))
        .unwrap_or(path);
    let path = path.strip_prefix('$').unwrap_or(path);
    let mut steps = Vec::new();
    let mut rest = path;
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            rest = r;
            continue;
        }
        if let Some(r) = rest.strip_prefix('[') {
            let end = r.find(']')?;
            let inner = r[..end].trim();
            rest = &r[end + 1..];
            if let Some(filter) = inner.strip_prefix("?(").and_then(|f| f.strip_suffix(')')) {
                let (lhs, rhs) = filter.split_once("==")?;
                let field = lhs.trim().strip_prefix("@.")?.to_string();
                let value = rhs
                    .trim()
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_string();
                steps.push(Step::Filter(field, value));
            } else if let Ok(i) = inner.parse::<usize>() {
                steps.push(Step::Index(i));
            } else {
                steps.push(Step::Field(
                    inner.trim_matches(|c| c == '"' || c == '\'').to_string(),
                ));
            }
            continue;
        }
        let end = rest.find(['.', '[']).unwrap_or(rest.len());
        steps.push(Step::Field(rest[..end].to_string()));
        rest = &rest[end..];
    }
    Some(steps)
}

#[must_use]
pub fn resolve_json_path(obj: &Value, path: &str) -> Option<Value> {
    let mut cur = obj;
    for step in parse_json_path(path)? {
        cur = match step {
            Step::Field(f) => cur.get(f.as_str())?,
            Step::Index(i) => cur.get(i)?,
            Step::Filter(field, want) => cur.as_array()?.iter().find(|item| {
                item.get(field.as_str()).is_some_and(|v| match v {
                    Value::String(s) => *s == want,
                    other => other.to_string() == want,
                })
            })?,
        };
    }
    Some(cur.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_790_000_000;

    fn at(seconds_ago: i64) -> String {
        chrono::DateTime::from_timestamp(NOW - seconds_ago, 0)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    fn header(printer: &Printer) -> Vec<(String, String, i32)> {
        printer
            .columns()
            .into_iter()
            .map(|c| (c.name.into_owned(), c.type_.into_owned(), c.priority))
            .collect()
    }

    fn cols(spec: &[(&str, &str, i32)]) -> Vec<(String, String, i32)> {
        spec.iter()
            .map(|(n, t, p)| ((*n).to_string(), (*t).to_string(), *p))
            .collect()
    }

    #[test]
    fn human_duration_follows_kubectl() {
        for (secs, want) in [
            (-2, "<invalid>"),
            (-1, "0s"),
            (0, "0s"),
            (59, "59s"),
            (119, "119s"),
            (120, "2m"),
            (125, "2m5s"),
            (599, "9m59s"),
            (600, "10m"),
            (179 * 60 + 59, "179m"),
            (3 * 3600, "3h"),
            (3 * 3600 + 59 * 60, "3h59m"),
            (8 * 3600, "8h"),
            (47 * 3600, "47h"),
            (48 * 3600, "2d"),
            (49 * 3600, "2d1h"),
            (8 * 86400, "8d"),
            (44 * 86400, "44d"),
            (729 * 86400, "729d"),
            (730 * 86400, "2y"),
            (731 * 86400, "2y1d"),
            (8 * 365 * 86400, "8y"),
        ] {
            assert_eq!(human_duration(secs), want, "{secs}s");
        }
        assert_eq!(age(None, NOW), "<unknown>");
        assert_eq!(age(Some("garbage"), NOW), "<unknown>");
        assert_eq!(age(Some(&at(44 * 86400)), NOW), "44d");
    }

    #[test]
    fn pods_print_name_ready_status_restarts_age_and_the_wide_columns() {
        let printer = Printer::for_kind("", "Pod");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Ready", "string", 0),
                ("Status", "string", 0),
                ("Restarts", "string", 0),
                ("Age", "string", 0),
                ("IP", "string", 1),
                ("Node", "string", 1),
                ("Nominated Node", "string", 1),
                ("Readiness Gates", "string", 1),
            ])
        );
        let pod = json!({
            "metadata": {"name": "web-0", "creationTimestamp": at(3 * 86400)},
            "spec": {"nodeName": "ryn", "containers": [{"name": "a"}, {"name": "b"}]},
            "status": {
                "phase": "Running",
                "podIPs": [{"ip": "10.42.0.7"}],
                "containerStatuses": [
                    {"name": "a", "ready": true, "restartCount": 2, "state": {"running": {}},
                     "lastState": {"terminated": {"finishedAt": at(300)}}},
                    {"name": "b", "ready": false, "restartCount": 0,
                     "state": {"waiting": {"reason": "CrashLoopBackOff"}}}
                ]
            }
        });
        assert_eq!(
            printer.cells(&pod, NOW),
            vec![
                json!("web-0"),
                json!("1/2"),
                json!("CrashLoopBackOff"),
                json!("2 (5m ago)"),
                json!("3d"),
                json!("10.42.0.7"),
                json!("ryn"),
                json!("<none>"),
                json!("<none>"),
            ]
        );
        let pending = json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "c"}]},
            "status": {"phase": "Pending"}});
        assert_eq!(
            printer.cells(&pending, NOW)[1..4],
            [json!("0/1"), json!("Pending"), json!("0")]
        );
        let done = json!({"metadata": {"name": "d"}, "status": {"phase": "Succeeded"}});
        assert_eq!(printer.conditions(&done)[0]["reason"], "Succeeded");
    }

    #[test]
    fn a_pod_in_its_init_containers_and_a_terminating_pod_say_so() {
        let printer = Printer::for_kind("", "Pod");
        let init = json!({"metadata": {"name": "i"},
            "spec": {"initContainers": [{"name": "setup"}], "containers": [{"name": "c"}]},
            "status": {"phase": "Pending", "initContainerStatuses": [
                {"name": "setup", "state": {"running": {}}}]}});
        assert_eq!(printer.cells(&init, NOW)[2], json!("Init:0/1"));
        let failed_init = json!({"metadata": {"name": "i"},
            "spec": {"initContainers": [{"name": "setup"}], "containers": [{"name": "c"}]},
            "status": {"phase": "Pending", "initContainerStatuses": [
                {"name": "setup", "state": {"terminated": {"exitCode": 3}}}]}});
        assert_eq!(
            printer.cells(&failed_init, NOW)[2],
            json!("Init:ExitCode:3")
        );
        let going = json!({"metadata": {"name": "t", "deletionTimestamp": at(1)},
            "spec": {"containers": [{"name": "c"}]}, "status": {"phase": "Running"}});
        assert_eq!(printer.cells(&going, NOW)[2], json!("Terminating"));
    }

    #[test]
    fn nodes_print_name_status_roles_age_version() {
        let printer = Printer::for_kind("", "Node");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Status", "string", 0),
                ("Roles", "string", 0),
                ("Age", "string", 0),
                ("Version", "string", 0),
                ("Internal-IP", "string", 1),
                ("External-IP", "string", 1),
                ("OS-Image", "string", 1),
                ("Kernel-Version", "string", 1),
                ("Container-Runtime", "string", 1),
            ])
        );
        let node = json!({
            "metadata": {"name": "ryn", "creationTimestamp": at(44 * 86400),
                "labels": {"node-role.kubernetes.io/control-plane": "", "node-role.kubernetes.io/worker": "",
                           "kubernetes.io/hostname": "ryn"}},
            "spec": {"unschedulable": true},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "addresses": [{"type": "InternalIP", "address": "192.168.1.10"}, {"type": "Hostname", "address": "ryn"}],
                "nodeInfo": {"kubeletVersion": "v1.34.0+engenho.0.53.133", "osImage": "NixOS 25.11 (Xantusia)",
                             "kernelVersion": "6.12.1", "containerRuntimeVersion": ""}
            }
        });
        assert_eq!(
            printer.cells(&node, NOW),
            vec![
                json!("ryn"),
                json!("Ready,SchedulingDisabled"),
                json!("control-plane,worker"),
                json!("44d"),
                json!("v1.34.0+engenho.0.53.133"),
                json!("192.168.1.10"),
                json!("<none>"),
                json!("NixOS 25.11 (Xantusia)"),
                json!("6.12.1"),
                json!("<unknown>"),
            ]
        );
        let silent = json!({"metadata": {"name": "n"}, "status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}});
        assert_eq!(
            printer.cells(&silent, NOW)[1..3],
            [json!("NotReady"), json!("<none>")]
        );
    }

    fn template() -> Value {
        json!({"spec": {"containers": [{"name": "app", "image": "nginx:1"}, {"name": "side", "image": "envoy:2"}]}})
    }

    #[test]
    fn deployments_print_ready_up_to_date_available() {
        let printer = Printer::for_kind("apps", "Deployment");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Ready", "string", 0),
                ("Up-to-date", "string", 0),
                ("Available", "string", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
                ("Selector", "string", 1),
            ])
        );
        let d = json!({"metadata": {"name": "web", "creationTimestamp": at(90)},
            "spec": {"replicas": 3, "template": template(),
                "selector": {"matchLabels": {"app": "web"}, "matchExpressions": [{"key": "tier", "operator": "In", "values": ["b", "a"]}]}},
            "status": {"readyReplicas": 2, "updatedReplicas": 3, "availableReplicas": 2}});
        assert_eq!(
            printer.cells(&d, NOW),
            vec![
                json!("web"),
                json!("2/3"),
                json!(3),
                json!(2),
                json!("90s"),
                json!("app,side"),
                json!("nginx:1,envoy:2"),
                json!("app=web,tier in (a,b)"),
            ]
        );
    }

    #[test]
    fn replicasets_print_desired_current_ready() {
        let printer = Printer::for_kind("apps", "ReplicaSet");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Desired", "integer", 0),
                ("Current", "integer", 0),
                ("Ready", "integer", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
                ("Selector", "string", 1),
            ])
        );
        let rs = json!({"metadata": {"name": "web-5d8", "creationTimestamp": at(7200)},
            "spec": {"replicas": 2, "template": template(), "selector": {"matchLabels": {"app": "web"}}},
            "status": {"replicas": 2}});
        assert_eq!(
            printer.cells(&rs, NOW),
            vec![
                json!("web-5d8"),
                json!(2),
                json!(2),
                json!(0),
                json!("120m"),
                json!("app,side"),
                json!("nginx:1,envoy:2"),
                json!("app=web"),
            ]
        );
    }

    #[test]
    fn statefulsets_print_ready_ratio() {
        let printer = Printer::for_kind("apps", "StatefulSet");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Ready", "string", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
            ])
        );
        let sts = json!({"metadata": {"name": "db", "creationTimestamp": at(600)},
            "spec": {"replicas": 3, "template": template()}, "status": {"readyReplicas": 3}});
        assert_eq!(
            printer.cells(&sts, NOW),
            vec![
                json!("db"),
                json!("3/3"),
                json!("10m"),
                json!("app,side"),
                json!("nginx:1,envoy:2")
            ]
        );
    }

    #[test]
    fn daemonsets_print_the_scheduling_counts_and_node_selector() {
        let printer = Printer::for_kind("apps", "DaemonSet");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Desired", "integer", 0),
                ("Current", "integer", 0),
                ("Ready", "integer", 0),
                ("Up-to-date", "integer", 0),
                ("Available", "integer", 0),
                ("Node Selector", "string", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
                ("Selector", "string", 1),
            ])
        );
        let mut t = template();
        t["spec"]["nodeSelector"] = json!({"kubernetes.io/os": "linux"});
        let ds = json!({"metadata": {"name": "agent", "creationTimestamp": at(30)},
            "spec": {"template": t, "selector": {"matchLabels": {"app": "agent"}}},
            "status": {"desiredNumberScheduled": 1, "currentNumberScheduled": 1, "numberReady": 1,
                       "updatedNumberScheduled": 1, "numberAvailable": 1}});
        assert_eq!(
            printer.cells(&ds, NOW),
            vec![
                json!("agent"),
                json!(1),
                json!(1),
                json!(1),
                json!(1),
                json!(1),
                json!("kubernetes.io/os=linux"),
                json!("30s"),
                json!("app,side"),
                json!("nginx:1,envoy:2"),
                json!("app=agent"),
            ]
        );
    }

    #[test]
    fn jobs_print_status_completions_duration() {
        let printer = Printer::for_kind("batch", "Job");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Status", "string", 0),
                ("Completions", "string", 0),
                ("Duration", "string", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
                ("Selector", "string", 1),
            ])
        );
        let job = json!({"metadata": {"name": "sync-1791936000", "creationTimestamp": at(400)},
            "spec": {"completions": 1, "template": template()},
            "status": {"succeeded": 1, "startTime": at(400), "completionTime": at(388),
                "conditions": [{"type": "Complete", "status": "True"}]}});
        assert_eq!(
            printer.cells(&job, NOW),
            vec![
                json!("sync-1791936000"),
                json!("Complete"),
                json!("1/1"),
                json!("12s"),
                json!("6m40s"),
                json!("app,side"),
                json!("nginx:1,envoy:2"),
                json!("<none>"),
            ]
        );
        let running = json!({"metadata": {"name": "r"}, "spec": {"parallelism": 3},
            "status": {"startTime": at(5)}});
        assert_eq!(
            printer.cells(&running, NOW)[1..4],
            [json!("Running"), json!("0/1 of 3"), json!("5s")]
        );
        let failed = json!({"metadata": {"name": "f"}, "status": {"conditions": [{"type": "Failed", "status": "True"}]}});
        assert_eq!(printer.cells(&failed, NOW)[1], json!("Failed"));
    }

    #[test]
    fn cronjobs_print_schedule_suspend_active_last_schedule() {
        let printer = Printer::for_kind("batch", "CronJob");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Schedule", "string", 0),
                ("Timezone", "string", 0),
                ("Suspend", "boolean", 0),
                ("Active", "integer", 0),
                ("Last Schedule", "string", 0),
                ("Age", "string", 0),
                ("Containers", "string", 1),
                ("Images", "string", 1),
                ("Selector", "string", 1),
            ])
        );
        let cj = json!({"metadata": {"name": "sync", "creationTimestamp": at(10 * 86400)},
            "spec": {"schedule": "0 */6 * * *", "suspend": false, "jobTemplate": {"spec": {"template": template()}}},
            "status": {"active": [{"name": "sync-1"}], "lastScheduleTime": at(3600)}});
        assert_eq!(
            printer.cells(&cj, NOW),
            vec![
                json!("sync"),
                json!("0 */6 * * *"),
                json!("<none>"),
                json!("False"),
                json!(1),
                json!("60m"),
                json!("10d"),
                json!("app,side"),
                json!("nginx:1,envoy:2"),
                json!("<none>"),
            ]
        );
        let fresh = json!({"metadata": {"name": "n"}, "spec": {"schedule": "* * * * *"}});
        assert_eq!(
            printer.cells(&fresh, NOW)[3..6],
            [json!("<unset>"), json!(0), json!("<none>")]
        );
    }

    #[test]
    fn services_print_type_ips_ports() {
        let printer = Printer::for_kind("", "Service");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Type", "string", 0),
                ("Cluster-IP", "string", 0),
                ("External-IP", "string", 0),
                ("Port(s)", "string", 0),
                ("Age", "string", 0),
                ("Selector", "string", 1),
            ])
        );
        let svc = json!({"metadata": {"name": "web", "creationTimestamp": at(86400)},
            "spec": {"type": "NodePort", "clusterIP": "10.43.0.10", "selector": {"app": "web"},
                "ports": [{"port": 80, "nodePort": 30080, "protocol": "TCP"}, {"port": 53, "protocol": "UDP"}]}});
        assert_eq!(
            printer.cells(&svc, NOW),
            vec![
                json!("web"),
                json!("NodePort"),
                json!("10.43.0.10"),
                json!("<none>"),
                json!("80:30080/TCP,53/UDP"),
                json!("24h"),
                json!("app=web"),
            ]
        );
        let lb = json!({"metadata": {"name": "lb"}, "spec": {"type": "LoadBalancer"}});
        assert_eq!(printer.cells(&lb, NOW)[3], json!("<pending>"));
        let headless = json!({"metadata": {"name": "h"}, "spec": {"clusterIP": "None"}});
        assert_eq!(
            printer.cells(&headless, NOW)[1..5],
            [
                json!("ClusterIP"),
                json!("None"),
                json!("<none>"),
                json!("<none>")
            ]
        );
    }

    #[test]
    fn namespaces_configmaps_and_secrets_print_their_summaries() {
        assert_eq!(
            header(&Printer::for_kind("", "Namespace")),
            cols(&[
                ("Name", "string", 0),
                ("Status", "string", 0),
                ("Age", "string", 0)
            ])
        );
        let ns = json!({"metadata": {"name": "default", "creationTimestamp": at(61)}, "status": {"phase": "Active"}});
        assert_eq!(
            Printer::for_kind("", "Namespace").cells(&ns, NOW),
            vec![json!("default"), json!("Active"), json!("61s")]
        );

        assert_eq!(
            header(&Printer::for_kind("", "ConfigMap")),
            cols(&[
                ("Name", "string", 0),
                ("Data", "string", 0),
                ("Age", "string", 0)
            ])
        );
        let cm = json!({"metadata": {"name": "cfg", "creationTimestamp": at(5)},
            "data": {"a": "1", "b": "2"}, "binaryData": {"c": "AA=="}});
        assert_eq!(
            Printer::for_kind("", "ConfigMap").cells(&cm, NOW),
            vec![json!("cfg"), json!(3), json!("5s")]
        );

        assert_eq!(
            header(&Printer::for_kind("", "Secret")),
            cols(&[
                ("Name", "string", 0),
                ("Type", "string", 0),
                ("Data", "string", 0),
                ("Age", "string", 0)
            ])
        );
        let secret = json!({"metadata": {"name": "tls", "creationTimestamp": at(5)},
            "type": "kubernetes.io/tls", "data": {"tls.crt": "", "tls.key": ""}});
        assert_eq!(
            Printer::for_kind("", "Secret").cells(&secret, NOW),
            vec![
                json!("tls"),
                json!("kubernetes.io/tls"),
                json!(2),
                json!("5s")
            ]
        );
    }

    #[test]
    fn events_print_last_seen_type_reason_object_message() {
        let printer = Printer::for_kind("", "Event");
        assert_eq!(
            header(&printer),
            cols(&[
                ("Last Seen", "string", 0),
                ("Type", "string", 0),
                ("Reason", "string", 0),
                ("Object", "string", 0),
                ("Subobject", "string", 1),
                ("Source", "string", 1),
                ("Message", "string", 0),
                ("First Seen", "string", 1),
                ("Count", "string", 1),
                ("Name", "string", 1),
            ])
        );
        let ev = json!({"metadata": {"name": "web-0.17f"},
            "type": "Warning", "reason": "BackOff",
            "involvedObject": {"kind": "Pod", "name": "web-0", "fieldPath": "spec.containers{app}"},
            "source": {"component": "kubelet", "host": "ryn"},
            "message": "Back-off restarting failed container\n",
            "firstTimestamp": at(600), "lastTimestamp": at(30), "count": 7});
        assert_eq!(
            printer.cells(&ev, NOW),
            vec![
                json!("30s"),
                json!("Warning"),
                json!("BackOff"),
                json!("pod/web-0"),
                json!("spec.containers{app}"),
                json!("kubelet, ryn"),
                json!("Back-off restarting failed container"),
                json!("10m"),
                json!(7),
                json!("web-0.17f"),
            ]
        );
        let singleton = json!({"metadata": {"name": "e"}, "eventTime": at(3),
            "involvedObject": {"kind": "Node"}, "reportingComponent": "engenho"});
        assert_eq!(printer.cells(&singleton, NOW)[0], json!("3s"));
        assert_eq!(printer.cells(&singleton, NOW)[3], json!("node"));
        assert_eq!(printer.cells(&singleton, NOW)[5], json!("engenho"));
        assert_eq!(printer.cells(&singleton, NOW)[8], json!(1));
    }

    #[test]
    fn a_kind_with_no_printer_gets_name_and_a_rendered_age() {
        let printer = Printer::for_kind("rbac.authorization.k8s.io", "Role");
        assert!(matches!(printer, Printer::Default));
        assert_eq!(
            header(&printer),
            cols(&[("Name", "string", 0), ("Age", "string", 0)])
        );
        let role = json!({"metadata": {"name": "reader", "creationTimestamp": at(44 * 86400)}});
        assert_eq!(
            printer.cells(&role, NOW),
            vec![json!("reader"), json!("44d")]
        );
    }

    #[test]
    fn every_printer_names_its_kind_once_and_renders_a_cell_per_column() {
        let mut seen = BTreeSet::new();
        for p in KIND_PRINTERS {
            assert!(
                seen.insert((p.group, p.kind)),
                "{}/{} twice",
                p.group,
                p.kind
            );
            let printer = Printer::Kind(p);
            assert_eq!(
                printer.cells(&json!({}), NOW).len(),
                printer.columns().len(),
                "{}",
                p.kind
            );
        }
    }

    #[test]
    fn a_crd_prints_its_additional_printer_columns() {
        let printer = Printer::for_crd(&[
            json!({"name": "Ready", "type": "string", "jsonPath": ".status.conditions[?(@.type==\"Ready\")].status"}),
            json!({"name": "Status", "type": "string", "jsonPath": ".status.conditions[?(@.type=='Ready')].message", "priority": 1}),
            json!({"name": "Replicas", "type": "integer", "jsonPath": ".spec.replicas"}),
            json!({"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp"}),
            json!({"name": "Broken", "type": "string"}),
        ]);
        assert_eq!(
            header(&printer),
            cols(&[
                ("Name", "string", 0),
                ("Ready", "string", 0),
                ("Status", "string", 1),
                ("Replicas", "integer", 0),
                ("Age", "date", 0),
            ])
        );
        let ks = json!({"metadata": {"name": "infra", "creationTimestamp": at(7 * 86400)},
            "spec": {"replicas": 2},
            "status": {"conditions": [
                {"type": "Reconciling", "status": "False"},
                {"type": "Ready", "status": "True", "message": "Applied revision: main@sha1:abc"}]}});
        assert_eq!(
            printer.cells(&ks, NOW),
            vec![
                json!("infra"),
                json!("True"),
                json!("Applied revision: main@sha1:abc"),
                json!(2),
                json!("7d"),
            ]
        );
        assert_eq!(
            printer.cells(&json!({"metadata": {"name": "bare"}}), NOW),
            vec![
                json!("bare"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null
            ]
        );
    }

    #[test]
    fn a_crd_with_no_columns_prints_name_and_age() {
        let printer = Printer::for_crd(&[]);
        assert_eq!(
            header(&printer),
            cols(&[("Name", "string", 0), ("Age", "date", 0)])
        );
        let cr = json!({"metadata": {"name": "x", "creationTimestamp": at(130)}});
        assert_eq!(printer.cells(&cr, NOW), vec![json!("x"), json!("2m10s")]);
    }

    #[test]
    fn json_paths_resolve_fields_indexes_and_filters() {
        let obj =
            json!({"a": {"b": [{"k": "x", "v": 1}, {"k": "y", "v": 2}]}, "m": {"dotted.key": 5}});
        assert_eq!(resolve_json_path(&obj, ".a.b[1].v"), Some(json!(2)));
        assert_eq!(resolve_json_path(&obj, "a.b[0].k"), Some(json!("x")));
        assert_eq!(
            resolve_json_path(&obj, ".a.b[?(@.k==\"y\")].v"),
            Some(json!(2))
        );
        assert_eq!(resolve_json_path(&obj, ".m['dotted.key']"), Some(json!(5)));
        assert_eq!(resolve_json_path(&obj, "{.a.b[0].v}"), Some(json!(1)));
        assert_eq!(resolve_json_path(&obj, ".a.missing"), None);
        assert_eq!(resolve_json_path(&obj, ".a.b[9]"), None);
    }
}
