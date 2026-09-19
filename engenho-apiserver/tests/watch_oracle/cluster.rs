//! An engenho store and apiserver for oracle rows.
//!
//! A row names revisions ("a pod at 12", "the cache relisted at 9"), so the
//! harness places every write at an exact revision: it writes ConfigMap
//! fillers until the store stands one below, then the row's Pod. The pod
//! watch filters the fillers out, so they are the gaps a per-kind upstream
//! cache sees between its events.
//!
//! A compaction floor comes from a durable reload: a store that loads from
//! disk starts with its floor at its current revision and an empty replay
//! ring (T3.3), which is what an upstream watch cache is after a relist.

use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_store::command::{Reason, ResourceCommand};
use engenho_store::{InProcessRouter, ResourceKey, StoreMesh, default_config};
use serde_json::{Value, json};

/// How long a store waits to become leader.
const LEADERSHIP: Duration = Duration::from_secs(10);

/// The bookmark cadence a row that counts bookmarks runs with. The runtime's
/// is five seconds; the store emits a bookmark only when the revision moved,
/// so a short cadence changes how soon one arrives, not whether.
pub const FAST_BOOKMARKS: Duration = Duration::from_millis(100);

/// The longest any single watch read may take before the harness calls it a
/// hang.
const READ_GUARD: Duration = Duration::from_secs(15);

/// Which bookmark cadence the Pod handler runs with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bookmarks {
    /// The runtime's cadence ([`StoreBackedHandler`]'s default, 5 s).
    Production,
    /// [`FAST_BOOKMARKS`].
    Fast,
}

/// One store and one apiserver serving Pods and ConfigMaps.
pub struct Cluster {
    pub store: Arc<StoreMesh>,
    server: ApiServer,
    client: reqwest::Client,
    _dir: Option<tempfile::TempDir>,
}

impl Cluster {
    /// A fresh in-memory store at revision 0, floor 0.
    pub async fn boot(bookmarks: Bookmarks) -> Self {
        let store = StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("oracle-watch").expect("raft config"),
        )
        .await
        .expect("start the store");
        store.initialize_singleton().await.expect("initialize");
        assert!(store.wait_for_leadership(LEADERSHIP).await, "leader");
        Self::serve(Arc::new(store), bookmarks, None).await
    }

    /// A durable store reloaded at `floor`: its revision and its compaction
    /// floor are both `floor`, and its replay ring is empty.
    pub async fn reloaded_at(floor: u64, bookmarks: Bookmarks) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("store");
        let first = path.clone();
        // The first lifetime runs on its own runtime, so dropping that runtime
        // ends every task holding the store: a kill after a clean flush.
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the first lifetime");
            rt.block_on(async {
                let store = StoreMesh::start_durable(
                    1,
                    "in-process://1".into(),
                    InProcessRouter::new(),
                    default_config("oracle-watch-reload").expect("raft config"),
                    &first,
                )
                .await
                .expect("first boot");
                store.initialize_singleton().await.expect("initialize");
                assert!(store.wait_for_leadership(LEADERSHIP).await, "leader");
                for rev in 1..=floor {
                    put(&store, filler(rev), filler_body(rev), rev).await;
                }
                let flushed = store.flush().await.expect("flush before the kill");
                assert!(
                    matches!(flushed, engenho_store::MeshFlushed::Durable(_)),
                    "HARNESS PRECONDITION: the first lifetime reached disk: {flushed:?}"
                );
            });
            drop(rt);
        })
        .await
        .expect("the first lifetime");

        let store = StoreMesh::start_durable(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config("oracle-watch-reload").expect("raft config"),
            &path,
        )
        .await
        .expect("reload");
        assert!(store.wait_for_leadership(LEADERSHIP).await, "leader");
        assert_eq!(
            (
                store.current_revision().await.get(),
                store.compacted_revision().await.get()
            ),
            (floor, floor),
            "HARNESS PRECONDITION: a reload puts the revision and the floor at {floor}"
        );
        Self::serve(Arc::new(store), bookmarks, Some(dir)).await
    }

    async fn serve(
        store: Arc<StoreMesh>,
        bookmarks: Bookmarks,
        dir: Option<tempfile::TempDir>,
    ) -> Self {
        let mut pods = StoreBackedHandler::for_core_kind(store.clone(), "Pod", true)
            .expect("Pod is cataloged");
        if bookmarks == Bookmarks::Fast {
            pods = pods.with_bookmark_every(FAST_BOOKMARKS);
        }
        let configmaps = StoreBackedHandler::for_core_kind(store.clone(), "ConfigMap", true)
            .expect("ConfigMap is cataloged");
        let handlers: Vec<Arc<dyn ResourceHandler>> = vec![Arc::new(pods), Arc::new(configmaps)];
        let server = ApiServer::start("127.0.0.1:0".parse().expect("addr"), handlers, None)
            .await
            .expect("start the apiserver");
        Self {
            store,
            server,
            client: reqwest::Client::new(),
            _dir: dir,
        }
    }

    /// The store's revision.
    pub async fn current(&self) -> u64 {
        self.store.current_revision().await.get()
    }

    /// Write ConfigMap fillers until the store stands at `rev`.
    pub async fn advance_to(&self, rev: u64) {
        let mut at = self.current().await;
        assert!(
            at <= rev,
            "HARNESS: the store is at {at}, past the {rev} a row needs"
        );
        while at < rev {
            at += 1;
            put(&self.store, filler(at), filler_body(at), at).await;
        }
    }

    /// Create or replace Pod `name` with `labels` at exactly revision `rev`.
    pub async fn pod_at(&self, name: &str, labels: Value, rev: u64) {
        self.advance_to(rev - 1).await;
        put(&self.store, pod(name), pod_body(name, labels), rev).await;
    }

    /// Delete Pod `name` at exactly revision `rev`.
    pub async fn delete_pod_at(&self, name: &str, rev: u64) {
        self.advance_to(rev - 1).await;
        let res = self
            .store
            .propose(ResourceCommand::delete(pod(name), Reason::Operator))
            .await
            .expect("delete");
        assert_eq!(res.revision, rev, "HARNESS: the delete landed at {rev}");
    }

    fn url(&self, path_and_query: &str) -> String {
        let addr = self.server.local_addr();
        let mut url = String::from("http://");
        url.push_str(&addr.to_string());
        url.push_str(path_and_query);
        url
    }

    /// `GET /api/v1/namespaces/default/pods?<query>` as a LIST.
    pub async fn list(&self, query: &str) -> Http {
        let resp = self
            .client
            .get(self.url(&pods_path(query)))
            .send()
            .await
            .expect("LIST sent");
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = resp.json().await.unwrap_or(Value::Null);
        Http {
            status,
            retry_after,
            body,
        }
    }

    /// Open `GET /api/v1/namespaces/default/pods?watch=true&<query>`
    /// (`watch=true` is not added again to a query that starts with it).
    pub async fn open(&self, query: &str) -> Open {
        let mut q = String::new();
        if !query.starts_with("watch=") {
            q.push_str("watch=true");
            if !query.is_empty() {
                q.push('&');
            }
        }
        q.push_str(query);
        let resp = self
            .client
            .get(self.url(&pods_path(&q)))
            .send()
            .await
            .expect("WATCH sent");
        Open {
            status: resp.status().as_u16(),
            resp,
            buf: Vec::new(),
            ended: false,
        }
    }

    /// A watch read to its end: the query must bound it (`timeoutSeconds`),
    /// or the server must end it (a refusal).
    pub async fn watch(&self, query: &str) -> Wire {
        let mut open = self.open(query).await;
        let mut lines = Vec::new();
        while let Some(line) = open.next_line(READ_GUARD).await {
            lines.push(line);
        }
        Wire {
            status: open.status,
            lines,
            ended: open.ended,
        }
    }
}

/// A LIST's answer.
#[derive(Debug)]
pub struct Http {
    pub status: u16,
    pub retry_after: Option<String>,
    pub body: Value,
}

/// A watch read to its end.
#[derive(Debug)]
pub struct Wire {
    pub status: u16,
    pub lines: Vec<Value>,
    /// The server ended the stream (not the harness's guard).
    pub ended: bool,
}

impl Wire {
    /// The lines of one `type`.
    pub fn of_type(&self, kind: &str) -> Vec<&Value> {
        self.lines.iter().filter(|l| l["type"] == kind).collect()
    }
}

/// A watch still open.
pub struct Open {
    pub status: u16,
    resp: reqwest::Response,
    buf: Vec<u8>,
    /// The server ended the stream.
    pub ended: bool,
}

impl Open {
    /// The next line, `None` at the end of the stream, or `None` when
    /// nothing arrives `within`.
    pub async fn next_line(&mut self, within: Duration) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                if line.len() > 1 {
                    return Some(
                        serde_json::from_slice(&line[..line.len() - 1]).expect("an NDJSON line"),
                    );
                }
                continue;
            }
            match tokio::time::timeout_at(deadline, self.resp.chunk()).await {
                Ok(Ok(Some(bytes))) => self.buf.extend_from_slice(&bytes),
                Ok(Ok(None) | Err(_)) => {
                    self.ended = true;
                    return None;
                }
                Err(_) => return None,
            }
        }
    }

    /// Every line that arrives within `window`, or until the stream ends.
    pub async fn lines_for(&mut self, window: Duration) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + window;
        let mut lines = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return lines;
            }
            match self.next_line(left).await {
                Some(line) => lines.push(line),
                None => return lines,
            }
        }
    }

    /// Read until a line satisfies `until` (returned last), the stream
    /// ends, or `within` passes.
    pub async fn lines_until(
        &mut self,
        within: Duration,
        until: impl Fn(&Value) -> bool,
    ) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + within;
        let mut lines = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next_line(left).await {
                Some(line) => {
                    let done = until(&line);
                    lines.push(line);
                    if done {
                        return lines;
                    }
                }
                None => return lines,
            }
        }
    }
}

fn pods_path(query: &str) -> String {
    let mut p = String::from("/api/v1/namespaces/default/pods");
    if !query.is_empty() {
        p.push('?');
        p.push_str(query);
    }
    p
}

fn pod(name: &str) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "Pod", "default", name)
}

fn pod_body(name: &str, labels: Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": labels},
        "spec": {"containers": [{"name": "c", "image": "busybox:1.36"}]}
    })
}

fn filler(rev: u64) -> ResourceKey {
    ResourceKey::namespaced("", "v1", "ConfigMap", "default", filler_name(rev))
}

fn filler_name(rev: u64) -> String {
    let mut name = String::from("filler-");
    name.push_str(&rev.to_string());
    name
}

fn filler_body(rev: u64) -> Value {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": filler_name(rev)}})
}

async fn put(store: &StoreMesh, key: ResourceKey, value: Value, rev: u64) {
    let res = store
        .propose(ResourceCommand::put(key, value, Reason::Operator))
        .await
        .expect("write");
    assert_eq!(res.revision, rev, "HARNESS: the write landed at {rev}");
}

/// The revision a watch line carries (its object's
/// `metadata.resourceVersion`), when it carries one: an `ERROR` line's
/// Status does not.
pub fn rv_of(line: &Value) -> Option<u64> {
    line["object"]["metadata"]["resourceVersion"]
        .as_str()
        .and_then(|s| s.parse().ok())
}

/// The Pod name a line carries.
pub fn line_name(line: &Value) -> &str {
    line["object"]["metadata"]["name"].as_str().unwrap_or("")
}
