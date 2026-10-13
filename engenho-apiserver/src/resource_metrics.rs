//! `metrics.k8s.io/v1beta1`: `PodMetrics` and `NodeMetrics`, read-only.
//!
//! What metrics-server serves upstream, served from the node runtime's own
//! readings through the [`ResourceMetricsReader`] seam (the runtime's
//! kubelet adapter implements it). k9s shows its CPU/MEM columns and
//! `kubectl top` works once discovery lists the group.
//!
//! A pod or node the runtime cannot measure is ABSENT from the list and a
//! 404 to GET, with the reason in the 404's message, as metrics-server
//! omits what it has no complete sample for. Never a zero: a zero reads as
//! an idle workload.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use engenho_store::{ContinueToken, Revision, StoreMesh};
use engenho_types::auth::UserInfo;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::field_validation::PatchFields;
use crate::handler::ResourceHandler;
use crate::object_body::ObjectBody;
use crate::params::{DryRun, ResumePoint, Selectors};
use crate::watch_start::WatchStart;

/// The group these kinds are served in.
pub const GROUP: &str = "metrics.k8s.io";
/// Its one version.
pub const VERSION: &str = "v1beta1";
/// `<group>/<version>`.
pub const API_VERSION: &str = "metrics.k8s.io/v1beta1";

/// One container's usage over a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSample {
    /// The container's name.
    pub name: String,
    /// CPU, in nanocores.
    pub cpu_nanocores: u64,
    /// Working set, in bytes.
    pub memory_bytes: u64,
}

/// A measurement window: when it ended, and how long it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// When it ended.
    pub timestamp: SystemTime,
    /// Its length.
    pub length: Duration,
}

/// One pod's usage, or why it has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodSample {
    /// The pod's namespace.
    pub namespace: String,
    /// The pod's name.
    pub name: String,
    /// Its containers' usage over one window, or why it is unmeasurable.
    pub usage: Result<(Window, Vec<ContainerSample>), String>,
}

/// One node's usage, or why it has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSample {
    /// The node's name.
    pub name: String,
    /// `(window, cpu nanocores, working-set bytes)`, or why unmeasurable.
    pub usage: Result<(Window, u64, u64), String>,
}

/// Where `metrics.k8s.io` reads from: the runtime's per-node usage.
#[async_trait]
pub trait ResourceMetricsReader: Send + Sync {
    /// Every pod this reader can see.
    async fn pods(&self) -> Vec<PodSample>;
    /// Every node this reader can see.
    async fn nodes(&self) -> Vec<NodeSample>;
}

/// Which of the two kinds a [`MetricsHandler`] serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsKind {
    /// `pods` → `PodMetrics`, namespaced.
    Pod,
    /// `nodes` → `NodeMetrics`, cluster-scoped.
    Node,
}

/// The read-only handler for one `metrics.k8s.io` kind.
pub struct MetricsHandler {
    kind: MetricsKind,
    reader: std::sync::Arc<dyn ResourceMetricsReader>,
    store: std::sync::Arc<StoreMesh>,
}

impl MetricsHandler {
    /// A handler for `kind`, reading usage from `reader` and the labels of
    /// the measured objects from `store`.
    #[must_use]
    pub fn new(
        kind: MetricsKind,
        reader: std::sync::Arc<dyn ResourceMetricsReader>,
        store: std::sync::Arc<StoreMesh>,
    ) -> Self {
        Self {
            kind,
            reader,
            store,
        }
    }

    /// Every measurable object as its metrics body, with the reasons the
    /// rest are not, keyed by `(namespace, name)`.
    async fn objects(
        &self,
        namespace: Option<&str>,
    ) -> Vec<((Option<String>, String), Result<Value, String>)> {
        match self.kind {
            MetricsKind::Pod => {
                let mut out = Vec::new();
                for p in self.reader.pods().await {
                    if namespace.is_some_and(|ns| ns != p.namespace) {
                        continue;
                    }
                    let key = engenho_store::resource::ResourceKey::namespaced(
                        "", "v1", "Pod", &p.namespace, &p.name,
                    );
                    let labels = self.labels_of(&key).await;
                    let body = p.usage.map(|(w, cs)| pod_metrics(&p.namespace, &p.name, &labels, w, &cs));
                    out.push(((Some(p.namespace), p.name), body));
                }
                out
            }
            MetricsKind::Node => {
                let mut out = Vec::new();
                for n in self.reader.nodes().await {
                    let key = engenho_store::resource::ResourceKey::cluster_scoped(
                        "", "v1", "Node", &n.name,
                    );
                    let labels = self.labels_of(&key).await;
                    let body = n
                        .usage
                        .map(|(w, cpu, mem)| node_metrics(&n.name, &labels, w, cpu, mem));
                    out.push(((None, n.name), body));
                }
                out
            }
        }
    }

    async fn labels_of(&self, key: &engenho_store::resource::ResourceKey) -> Value {
        self.store
            .get(key)
            .await
            .and_then(|v| v.pointer("/metadata/labels").cloned())
            .unwrap_or_else(|| json!({}))
    }

    async fn items(&self, namespace: Option<&str>, sel: &Selectors) -> Vec<Value> {
        self.objects(namespace)
            .await
            .into_iter()
            .filter_map(|(_, body)| body.ok())
            .filter(|v| sel.matches(v))
            .collect()
    }

    fn read_only(&self, verb: &str) -> ApiError {
        ApiError::MethodNotAllowed(format!(
            "{verb} is not allowed on {}.{GROUP}: the resource is read-only",
            self.plural()
        ))
    }
}

/// `PodMetrics` as metrics-server renders it.
#[must_use]
pub fn pod_metrics(
    namespace: &str,
    name: &str,
    labels: &Value,
    window: Window,
    containers: &[ContainerSample],
) -> Value {
    json!({
        "kind": "PodMetrics",
        "apiVersion": API_VERSION,
        "metadata": {"name": name, "namespace": namespace, "labels": labels,
            "creationTimestamp": stamp(SystemTime::now())},
        "timestamp": stamp(window.timestamp),
        "window": go_duration(window.length),
        "containers": containers.iter().map(|c| json!({
            "name": c.name,
            "usage": {"cpu": cpu_quantity(c.cpu_nanocores), "memory": memory_quantity(c.memory_bytes)},
        })).collect::<Vec<_>>(),
    })
}

/// `NodeMetrics` as metrics-server renders it.
#[must_use]
pub fn node_metrics(name: &str, labels: &Value, window: Window, cpu: u64, memory: u64) -> Value {
    json!({
        "kind": "NodeMetrics",
        "apiVersion": API_VERSION,
        "metadata": {"name": name, "labels": labels,
            "creationTimestamp": stamp(SystemTime::now())},
        "timestamp": stamp(window.timestamp),
        "window": go_duration(window.length),
        "usage": {"cpu": cpu_quantity(cpu), "memory": memory_quantity(memory)},
    })
}

fn stamp(t: SystemTime) -> String {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    i64::try_from(secs)
        .ok()
        .and_then(engenho_types::time::epoch_to_rfc3339_utc)
        .unwrap_or_default()
}

/// CPU in nanocores, metrics-server's `n` suffix.
#[must_use]
pub fn cpu_quantity(nanocores: u64) -> String {
    [nanocores.to_string().as_str(), "n"].concat()
}

/// Bytes, in `Ki` when whole kibibytes (metrics-server's `BinarySI` form).
#[must_use]
pub fn memory_quantity(bytes: u64) -> String {
    if bytes != 0 && bytes.is_multiple_of(1024) {
        [(bytes / 1024).to_string().as_str(), "Ki"].concat()
    } else {
        bytes.to_string()
    }
}

/// A duration as Go's `time.Duration.String` renders seconds: `1s`,
/// `1.5s`, `0.25s`.
#[must_use]
pub fn go_duration(d: Duration) -> String {
    let nanos = d.subsec_nanos();
    if nanos == 0 {
        return [d.as_secs().to_string().as_str(), "s"].concat();
    }
    let frac = format!("{nanos:09}");
    [
        d.as_secs().to_string().as_str(),
        ".",
        frac.trim_end_matches('0'),
        "s",
    ]
    .concat()
}

#[async_trait]
impl ResourceHandler for MetricsHandler {
    fn group(&self) -> &str {
        GROUP
    }
    fn version(&self) -> &str {
        VERSION
    }
    fn kind(&self) -> &str {
        match self.kind {
            MetricsKind::Pod => "PodMetrics",
            MetricsKind::Node => "NodeMetrics",
        }
    }
    fn plural(&self) -> &str {
        match self.kind {
            MetricsKind::Pod => "pods",
            MetricsKind::Node => "nodes",
        }
    }
    fn namespaced(&self) -> bool {
        self.kind == MetricsKind::Pod
    }
    fn read_only(&self) -> bool {
        true
    }
    fn supports_delete_collection(&self) -> bool {
        false
    }

    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, ApiError> {
        let found = self
            .objects(namespace)
            .await
            .into_iter()
            .find(|((_, n), _)| n == name);
        match found {
            Some((_, Ok(v))) => Ok(v),
            Some((_, Err(why))) => Err(ApiError::NotFound(format!(
                "{}.{GROUP} {name:?} not found: its usage cannot be measured: {why}",
                self.plural()
            ))),
            None => Err(ApiError::NotFound(format!(
                "{}.{GROUP} {name:?} not found",
                self.plural()
            ))),
        }
    }

    async fn list(&self, namespace: Option<&str>) -> Result<Value, ApiError> {
        let items = self.items(namespace, &Selectors::default()).await;
        Ok(self.list_response(items, Revision::ZERO, None, None))
    }

    async fn current_revision(&self) -> Revision {
        Revision::ZERO
    }

    async fn list_at(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
    ) -> Result<(Vec<Value>, Revision), ApiError> {
        Ok((self.items(namespace, sel).await, Revision::ZERO))
    }

    async fn list_page(
        &self,
        namespace: Option<&str>,
        sel: &Selectors,
        _limit: usize,
        _continue_token: Option<ContinueToken>,
    ) -> Result<(Vec<Value>, Revision, Option<String>, Option<u64>), ApiError> {
        Ok((self.items(namespace, sel).await, Revision::ZERO, None, None))
    }

    async fn watch_stream(
        &self,
        _namespace: Option<&str>,
        _from: ResumePoint,
        _allow_bookmarks: bool,
    ) -> Result<WatchStart, ApiError> {
        Err(self.read_only("watch"))
    }

    async fn create(
        &self,
        _namespace: Option<&str>,
        _body: ObjectBody,
        _user_info: &UserInfo,
        _dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        Err(self.read_only("create"))
    }

    async fn patch(
        &self,
        _namespace: Option<&str>,
        _name: &str,
        _patch: Value,
        _patch_type: engenho_types::patch::PatchType,
        _apply_opts: Option<crate::params::ApplyOptions>,
        _user_info: &UserInfo,
        _dry_run: DryRun,
        _fields: &mut PatchFields,
    ) -> Result<Value, ApiError> {
        Err(self.read_only("patch"))
    }

    async fn delete(
        &self,
        _namespace: Option<&str>,
        _name: &str,
        _user_info: &UserInfo,
        _dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        Err(self.read_only("delete"))
    }

    async fn delete_with_precondition(
        &self,
        _namespace: Option<&str>,
        _name: &str,
        _expected: Option<Revision>,
        _user_info: &UserInfo,
        _dry_run: DryRun,
    ) -> Result<Value, ApiError> {
        Err(self.read_only("delete"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities_and_windows_render_as_metrics_server_does() {
        assert_eq!(cpu_quantity(1_500_000), "1500000n");
        assert_eq!(memory_quantity(2048), "2Ki");
        assert_eq!(memory_quantity(1000), "1000");
        assert_eq!(memory_quantity(0), "0");
        assert_eq!(go_duration(Duration::from_secs(1)), "1s");
        assert_eq!(go_duration(Duration::from_millis(1500)), "1.5s");
    }
}
