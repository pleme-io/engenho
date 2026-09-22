//! Node-declared workloads: the `node-manifests` driver.
//!
//! Every `*.yaml` / `*.yml` file in `runtime.node_manifests_dir` (default
//! `/etc/engenho/manifests.d`) is SERVER-SIDE APPLIED through this node's own
//! apiserver, and every object this driver applied that no file declares any
//! more is DELETED. The node's configuration management (NixOS
//! `environment.etc`, a darwin activation) writes the directory; engenho
//! converges the cluster onto it.
//!
//! ## ★ THROUGH THE APISERVER, NOT AROUND IT
//!
//! Each object goes through the apiserver's own router
//! ([`engenho_apiserver::build`]) — the same authn → authz → admission →
//! defaulting → SSA write pipeline `kubectl apply --server-side` reaches over
//! the wire, driven in-process with `tower::ServiceExt::oneshot`:
//!
//! * `PATCH <item path>?fieldManager=engenho-node-manifests&force=true`,
//!   `Content-Type: application/apply-patch+yaml`;
//! * authenticated as the bootstrap admin: the bearer is read from
//!   `data_dir/pki/admin.token` on every pass, so a rotated token is followed
//!   rather than captured.
//!
//! A write straight into the store would skip admission and defaulting —
//! the unkept-promise class the repo CLAUDE.md measures (a `Secret` with no
//! `type`, a Service with no `ClusterIP`).
//!
//! ## What an applied object carries
//!
//! * label `engenho.pleme.io/declared-by: node-manifests` — what prune
//!   selects on. An object without it is never pruned by this driver;
//! * annotation `engenho.pleme.io/declared-in: <file name>` — which file
//!   declared it, so a file that cannot be read HOLDS its objects (below).
//!
//! ## Prune semantics
//!
//! After the applies, every served kind is listed with the label selector
//! above. A labelled object is deleted when no file declares it
//! (`(group, kind, namespace, name)` — version-insensitive), EXCEPT:
//!
//! * its `declared-in` file exists but failed to read or parse this pass —
//!   a typo in one file must not delete that file's workloads. It is held
//!   until the file parses again;
//! * the directory itself could not be listed (anything but "not found") —
//!   no apply and no prune at all that pass;
//! * the kind's list failed — nothing of that kind is deleted.
//!
//! A directory that does not exist declares nothing, like an empty one:
//! removing it prunes everything it once declared.
//!
//! ## When it runs
//!
//! No store event wakes it ([`Reads::nothing`](engenho_controllers::Reads)):
//! the directory is a filesystem input, polled by the driver's fallback tick
//! ([`POLL_INTERVAL`]). A pass is skipped when the directory's fingerprint
//! (BLAKE3 over every file name and byte) is unchanged, the last pass was
//! clean, and it ran less than [`RESYNC_INTERVAL`] ago — so an object deleted
//! by hand comes back within the resync window, and an apply that failed is
//! retried on the next tick. The memory is only that skip cache: a panic that
//! loses it costs one full pass, which is why the driver is Stateless.
//!
//! ## Tier-honest
//!
//! * Typed per-file failure ([`NodeManifestError`]); one bad file never stops
//!   the others — a test.
//! * Object ordering within a pass: `Namespace`s first, then
//!   `CustomResourceDefinition`s, then declaration order. A custom resource
//!   whose CRD is applied in the same pass is served only once the CRD
//!   controller registers it; until then its apply fails with
//!   [`NodeManifestError::UnservedKind`] and is retried next tick.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use engenho_apiserver::{PkiFile, ResourceHandler, RouterState};
use engenho_controllers::{
    Controller, ControllerError, DeclaresReads, Effect, Reads, ReconcileOutcome, ReconcileReport,
};
use serde_json::Value;
use tower::ServiceExt;
use tracing::{debug, info, warn};

/// The field manager every apply names.
pub(crate) const FIELD_MANAGER: &str = "engenho-node-manifests";
/// The label every applied object carries, and the one prune selects on.
pub(crate) const DECLARED_BY_LABEL: &str = "engenho.pleme.io/declared-by";
/// [`DECLARED_BY_LABEL`]'s value.
pub(crate) const DECLARED_BY_VALUE: &str = "node-manifests";
/// The annotation naming the file that declared an object.
pub(crate) const DECLARED_IN_ANNOTATION: &str = "engenho.pleme.io/declared-in";

/// How often the directory is looked at: the driver's fallback tick.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(10);
/// How long an unchanged, cleanly applied directory goes without being
/// re-applied and re-pruned.
pub(crate) const RESYNC_INTERVAL: Duration = Duration::from_secs(300);

/// The largest response body read back from the router.
const RESPONSE_LIMIT: usize = 64 * 1024 * 1024;

/// What one pass could not do, per file or per object. Rendered once, here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeManifestError {
    /// The directory exists and could not be listed.
    ReadDir { dir: PathBuf, detail: String },
    /// A manifest file could not be read.
    ReadFile { file: String, detail: String },
    /// A document in a file is not YAML.
    Parse {
        file: String,
        document: usize,
        detail: String,
    },
    /// A document is not an object with `apiVersion`, `kind` and
    /// `metadata.name` strings.
    Shape {
        file: String,
        document: usize,
        missing: &'static str,
    },
    /// Two documents declare the same object; the first wins.
    Duplicate {
        key: Box<ObjectKey>,
        file: String,
        first: String,
    },
    /// No handler serves the declared `apiVersion` + `kind`.
    UnservedKind {
        file: String,
        api_version: String,
        kind: String,
    },
    /// The admin bearer could not be read.
    Token { path: PathBuf, detail: String },
    /// The apiserver answered an apply with an error.
    Apply {
        key: Box<ObjectKey>,
        status: u16,
        message: String,
    },
    /// The apiserver answered a prune delete with an error.
    Delete {
        key: Box<ObjectKey>,
        status: u16,
        message: String,
    },
}

impl fmt::Display for NodeManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadDir { dir, detail } => write!(
                f,
                "cannot list node manifests directory {}: {detail} (nothing applied or pruned)",
                dir.display()
            ),
            Self::ReadFile { file, detail } => {
                write!(f, "cannot read node manifest {file}: {detail}")
            }
            Self::Parse {
                file,
                document,
                detail,
            } => write!(
                f,
                "node manifest {file} document {document} is not YAML: {detail} \
                 (the file's objects are held, not pruned)"
            ),
            Self::Shape {
                file,
                document,
                missing,
            } => write!(
                f,
                "node manifest {file} document {document} has no string {missing} \
                 (the file's objects are held, not pruned)"
            ),
            Self::Duplicate { key, file, first } => {
                write!(f, "{file} declares {key} again; {first} declared it first")
            }
            Self::UnservedKind {
                file,
                api_version,
                kind,
            } => write!(
                f,
                "{file} declares {api_version} {kind}, which this apiserver does not \
                 serve (yet — retried next tick)"
            ),
            Self::Token { path, detail } => write!(
                f,
                "cannot read the admin bearer at {}: {detail}",
                path.display()
            ),
            Self::Apply {
                key,
                status,
                message,
            } => write!(f, "apply of {key} answered {status}: {message}"),
            Self::Delete {
                key,
                status,
                message,
            } => write!(f, "prune of {key} answered {status}: {message}"),
        }
    }
}

impl std::error::Error for NodeManifestError {}

/// Which object a declaration names, for prune: version-insensitive, since
/// one object is served under every version of its kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub group: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let group = if self.group.is_empty() {
            "core"
        } else {
            &self.group
        };
        match &self.namespace {
            Some(ns) => write!(f, "{group}/{} {ns}/{}", self.kind, self.name),
            None => write!(f, "{group}/{} {}", self.kind, self.name),
        }
    }
}

/// One declared document, parsed.
#[derive(Debug, Clone, PartialEq)]
pub struct RawObject {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub body: Value,
}

/// `apiVersion` split into its group and version (`v1` is the core group).
fn split_api_version(api_version: &str) -> (&str, &str) {
    api_version.rsplit_once('/').unwrap_or(("", api_version))
}

/// Split one file into its objects. Empty documents (a trailing `---`, a
/// comment-only document) declare nothing and are skipped.
///
/// # Errors
///
/// The first document that is not YAML or not an object shape.
pub fn parse_file(file: &str, text: &str) -> Result<Vec<RawObject>, NodeManifestError> {
    let mut objects = Vec::new();
    for (document, de) in serde_yaml::Deserializer::from_str(text).enumerate() {
        let parse = |detail: String| NodeManifestError::Parse {
            file: file.to_owned(),
            document,
            detail,
        };
        let yaml = <serde_yaml::Value as serde::Deserialize>::deserialize(de)
            .map_err(|e| parse(e.to_string()))?;
        if yaml.is_null() {
            continue;
        }
        let body: Value = serde_json::to_value(yaml).map_err(|e| parse(e.to_string()))?;
        let shape = |missing| NodeManifestError::Shape {
            file: file.to_owned(),
            document,
            missing,
        };
        let text_at = |path: &[&str]| {
            path.iter()
                .try_fold(&body, |v, k| v.get(k))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let api_version = text_at(&["apiVersion"]).ok_or_else(|| shape("apiVersion"))?;
        let kind = text_at(&["kind"]).ok_or_else(|| shape("kind"))?;
        let name = text_at(&["metadata", "name"]).ok_or_else(|| shape("metadata.name"))?;
        let namespace = text_at(&["metadata", "namespace"]).filter(|ns| !ns.is_empty());
        objects.push(RawObject {
            api_version,
            kind,
            namespace,
            name,
            body,
        });
    }
    Ok(objects)
}

/// One file's result.
#[derive(Debug, Clone, PartialEq)]
pub struct FileScan {
    pub name: String,
    pub outcome: Result<Vec<RawObject>, NodeManifestError>,
}

/// What the directory declares, and its fingerprint.
#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub fingerprint: blake3::Hash,
    pub files: Vec<FileScan>,
}

/// Whether `path` names a manifest by its extension.
fn is_manifest(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml" | "yml")
    )
}

/// Read every manifest in `dir`, sorted by file name. A directory that does
/// not exist is an empty scan.
///
/// # Errors
///
/// [`NodeManifestError::ReadDir`] when the directory exists and cannot be
/// listed. A file that cannot be read is that file's outcome, not an error.
pub fn scan_dir(dir: &Path) -> Result<Scan, NodeManifestError> {
    let read_dir = |detail: String| NodeManifestError::ReadDir {
        dir: dir.to_owned(),
        detail,
    };
    let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| read_dir(e.to_string()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(read_dir(e.to_string())),
    };
    // `is_file` follows symlinks: NixOS `environment.etc` installs links
    // into the store.
    paths.retain(|p| is_manifest(p) && p.is_file());
    paths.sort();
    let mut hasher = blake3::Hasher::new();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
        let outcome = match std::fs::read(&path) {
            Ok(bytes) => {
                hasher.update(&(bytes.len() as u64).to_le_bytes());
                hasher.update(&bytes);
                String::from_utf8(bytes)
                    .map_err(|e| NodeManifestError::ReadFile {
                        file: name.clone(),
                        detail: e.to_string(),
                    })
                    .and_then(|text| parse_file(&name, &text))
            }
            Err(e) => {
                hasher.update(b"unreadable");
                Err(NodeManifestError::ReadFile {
                    file: name.clone(),
                    detail: e.to_string(),
                })
            }
        };
        files.push(FileScan { name, outcome });
    }
    Ok(Scan {
        fingerprint: hasher.finalize(),
        files,
    })
}

/// A REST address on this apiserver, rendered once.
struct ApiPath<'a> {
    group: &'a str,
    version: &'a str,
    plural: &'a str,
    namespace: Option<&'a str>,
    name: Option<&'a str>,
}

impl fmt::Display for ApiPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "/api/{}", self.version)?;
        } else {
            write!(f, "/apis/{}/{}", self.group, self.version)?;
        }
        if let Some(ns) = self.namespace {
            write!(f, "/namespaces/{ns}")?;
        }
        write!(f, "/{}", self.plural)?;
        if let Some(name) = self.name {
            write!(f, "/{name}")?;
        }
        Ok(())
    }
}

/// A request's query string, rendered once.
enum Query {
    /// The apply parameters.
    Apply,
    /// The prune list selector.
    Declared,
    /// None.
    Bare,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Apply => write!(f, "?fieldManager={FIELD_MANAGER}&force=true"),
            // `/` is a legal query character; `=` inside a selector value is
            // what the apiserver parses, so the selector needs no escaping.
            Self::Declared => write!(
                f,
                "?labelSelector={DECLARED_BY_LABEL}%3D{DECLARED_BY_VALUE}"
            ),
            Self::Bare => Ok(()),
        }
    }
}

/// What one pass did.
#[derive(Debug, Default)]
pub(crate) struct PassReport {
    pub(crate) applied: Vec<ObjectKey>,
    pub(crate) pruned: Vec<ObjectKey>,
    pub(crate) held: Vec<ObjectKey>,
    pub(crate) errors: Vec<NodeManifestError>,
}

impl PassReport {
    fn clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// The skip cache: see the module docs.
struct Memory {
    fingerprint: blake3::Hash,
    clean: bool,
    at: Instant,
}

/// The `node-manifests` controller.
pub struct NodeManifestsController {
    dir: PathBuf,
    token_path: PathBuf,
    state: RouterState,
    router: axum::Router,
    memory: Mutex<Option<Memory>>,
}

impl NodeManifestsController {
    /// Apply `dir` through `state`'s router, as the admin whose bearer is at
    /// `data_dir`'s `pki/admin.token`.
    #[must_use]
    pub fn new(dir: PathBuf, data_dir: &Path, state: RouterState) -> Self {
        Self {
            dir,
            token_path: PkiFile::AdminToken.path(data_dir),
            router: engenho_apiserver::build(state.clone()),
            state,
            memory: Mutex::new(None),
        }
    }

    /// The handler serving `api_version` + `kind`, if any.
    fn handler(
        &self,
        api_version: &str,
        kind: &str,
    ) -> Option<std::sync::Arc<dyn ResourceHandler>> {
        let (group, version) = split_api_version(api_version);
        self.state
            .handler_set()
            .into_iter()
            .find(|h| h.group() == group && h.version() == version && h.kind() == kind)
    }

    async fn send(
        &self,
        token: &str,
        method: Method,
        uri: String,
        body: Option<Vec<u8>>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, ["Bearer ", token].concat());
        if body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/apply-patch+yaml");
        }
        let request = match request.body(body.map_or_else(Body::empty, Body::from)) {
            Ok(request) => request,
            Err(e) => return (StatusCode::BAD_REQUEST, Value::String(e.to_string())),
        };
        let response = match self.router.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), RESPONSE_LIMIT)
            .await
            .unwrap_or_default();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    /// One full pass over `scan`: apply, then prune.
    pub(crate) async fn pass(&self, scan: &Scan) -> PassReport {
        let mut report = PassReport::default();
        let token = match std::fs::read_to_string(&self.token_path) {
            Ok(token) => token.trim().to_owned(),
            Err(e) => {
                report.errors.push(NodeManifestError::Token {
                    path: self.token_path.clone(),
                    detail: e.to_string(),
                });
                return report;
            }
        };

        // Collect every declaration, in apply order; files that failed hold.
        let mut declared: BTreeMap<ObjectKey, String> = BTreeMap::new();
        let mut failed_files: BTreeSet<&str> = BTreeSet::new();
        let mut to_apply: Vec<(&str, &RawObject, ObjectKey)> = Vec::new();
        for file in &scan.files {
            match &file.outcome {
                Ok(objects) => {
                    for object in objects {
                        let (group, _) = split_api_version(&object.api_version);
                        let key = ObjectKey {
                            group: group.to_owned(),
                            kind: object.kind.clone(),
                            namespace: object.namespace.clone(),
                            name: object.name.clone(),
                        };
                        to_apply.push((&file.name, object, key));
                    }
                }
                Err(e) => {
                    failed_files.insert(&file.name);
                    report.errors.push(e.clone());
                }
            }
        }
        to_apply.sort_by_key(|(_, object, _)| match object.kind.as_str() {
            "Namespace" => 0u8,
            "CustomResourceDefinition" => 1,
            _ => 2,
        });

        for (file, object, mut key) in to_apply {
            let Some(handler) = self.handler(&object.api_version, &object.kind) else {
                report.errors.push(NodeManifestError::UnservedKind {
                    file: file.to_owned(),
                    api_version: object.api_version.clone(),
                    kind: object.kind.clone(),
                });
                // Held: an unserved kind is not a withdrawn declaration.
                declared.entry(key).or_insert_with(|| file.to_owned());
                continue;
            };
            // kubectl's defaulting: a namespaced object with no namespace is
            // in `default`; a cluster-scoped one has none.
            key.namespace = if handler.namespaced() {
                Some(key.namespace.unwrap_or_else(|| "default".to_owned()))
            } else {
                None
            };
            if let Some(first) = declared.get(&key) {
                report.errors.push(NodeManifestError::Duplicate {
                    key: Box::new(key),
                    file: file.to_owned(),
                    first: first.clone(),
                });
                continue;
            }
            declared.insert(key.clone(), file.to_owned());
            let body = stamped(&object.body, key.namespace.as_deref(), file);
            let path = ApiPath {
                group: handler.group(),
                version: handler.version(),
                plural: handler.plural(),
                namespace: key.namespace.as_deref(),
                name: Some(&key.name),
            };
            let (status, answer) = self
                .send(
                    &token,
                    Method::PATCH,
                    [path.to_string(), Query::Apply.to_string()].concat(),
                    Some(body.to_string().into_bytes()),
                )
                .await;
            if status.is_success() {
                report.applied.push(key);
            } else {
                report.errors.push(NodeManifestError::Apply {
                    key: Box::new(key),
                    status: status.as_u16(),
                    message: message_of(&answer),
                });
            }
        }

        self.prune(&token, &declared, &failed_files, &mut report)
            .await;
        report
    }

    /// Delete every labelled object no file declares, except those held.
    async fn prune(
        &self,
        token: &str,
        declared: &BTreeMap<ObjectKey, String>,
        failed_files: &BTreeSet<&str>,
        report: &mut PassReport,
    ) {
        let mut seen: BTreeSet<ObjectKey> = BTreeSet::new();
        let mut handlers = self.state.handler_set();
        handlers.sort_by(|a, b| {
            (a.group(), a.version(), a.plural()).cmp(&(b.group(), b.version(), b.plural()))
        });
        for handler in handlers {
            let list = ApiPath {
                group: handler.group(),
                version: handler.version(),
                plural: handler.plural(),
                namespace: None,
                name: None,
            };
            let (status, answer) = self
                .send(
                    token,
                    Method::GET,
                    [list.to_string(), Query::Declared.to_string()].concat(),
                    None,
                )
                .await;
            if !status.is_success() {
                // A kind that cannot be listed (create-only, a virtual kind)
                // has nothing to prune; nothing of it is deleted.
                debug!(kind = handler.kind(), %status, "node-manifests: kind not listable");
                continue;
            }
            let items = answer
                .get("items")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            for item in items {
                let meta = item.get("metadata");
                let text = |k: &str| meta.and_then(|m| m.get(k)).and_then(Value::as_str);
                let labelled = meta
                    .and_then(|m| m.get("labels"))
                    .and_then(|l| l.get(DECLARED_BY_LABEL))
                    .and_then(Value::as_str)
                    == Some(DECLARED_BY_VALUE);
                let Some(name) = text("name").filter(|_| labelled) else {
                    continue;
                };
                let key = ObjectKey {
                    group: handler.group().to_owned(),
                    kind: handler.kind().to_owned(),
                    namespace: text("namespace").map(str::to_owned),
                    name: name.to_owned(),
                };
                if declared.contains_key(&key) || !seen.insert(key.clone()) {
                    continue;
                }
                let source = meta
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.get(DECLARED_IN_ANNOTATION))
                    .and_then(Value::as_str);
                if source.is_some_and(|file| failed_files.contains(file)) {
                    report.held.push(key);
                    continue;
                }
                let item_path = ApiPath {
                    name: Some(&key.name),
                    namespace: key.namespace.as_deref(),
                    ..list
                };
                let (status, answer) = self
                    .send(
                        token,
                        Method::DELETE,
                        [item_path.to_string(), Query::Bare.to_string()].concat(),
                        None,
                    )
                    .await;
                if status.is_success() || status == StatusCode::NOT_FOUND {
                    report.pruned.push(key);
                } else {
                    report.errors.push(NodeManifestError::Delete {
                        key: Box::new(key),
                        status: status.as_u16(),
                        message: message_of(&answer),
                    });
                }
            }
        }
    }
}

/// `body` with the driver's label and annotation, and the namespace the
/// apply lands in (none for a cluster-scoped kind).
fn stamped(body: &Value, namespace: Option<&str>, file: &str) -> Value {
    let mut body = body.clone();
    if let Some(meta) = body.get_mut("metadata").and_then(Value::as_object_mut) {
        match namespace {
            Some(ns) => {
                meta.insert("namespace".to_owned(), Value::String(ns.to_owned()));
            }
            None => {
                meta.remove("namespace");
            }
        }
        for (field, key, value) in [
            ("labels", DECLARED_BY_LABEL, DECLARED_BY_VALUE),
            ("annotations", DECLARED_IN_ANNOTATION, file),
        ] {
            let map = meta
                .entry(field)
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            if !map.is_object() {
                *map = Value::Object(serde_json::Map::new());
            }
            if let Some(map) = map.as_object_mut() {
                map.insert(key.to_owned(), Value::String(value.to_owned()));
            }
        }
    }
    body
}

/// A K8s `Status` body's message, or the whole body when it has none.
fn message_of(answer: &Value) -> String {
    answer
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| answer.to_string(), str::to_owned)
}

/// It reads nothing from the store: its input is the directory, and what it
/// lists for prune it lists through the apiserver. No store event wakes it.
impl DeclaresReads for NodeManifestsController {
    fn reads(&self) -> Reads {
        Reads::nothing()
    }
}

#[async_trait::async_trait]
impl Controller for NodeManifestsController {
    fn name(&self) -> &'static str {
        "node-manifests"
    }

    async fn tick(&self) -> Result<ReconcileOutcome, ControllerError> {
        let mut report = ReconcileReport::default();
        let scan = match scan_dir(&self.dir) {
            Ok(scan) => scan,
            Err(e) => {
                warn!(error = %e, "node-manifests");
                return Ok(ReconcileOutcome::from(report));
            }
        };
        let unchanged = {
            let memory = self
                .memory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            memory.as_ref().is_some_and(|m| {
                m.fingerprint == scan.fingerprint && m.clean && m.at.elapsed() < RESYNC_INTERVAL
            })
        };
        if unchanged {
            return Ok(ReconcileOutcome::from(report));
        }
        let pass = self.pass(&scan).await;
        for error in &pass.errors {
            warn!(error = %error, "node-manifests");
        }
        if !pass.pruned.is_empty() || !pass.held.is_empty() {
            info!(
                pruned = ?pass.pruned.iter().map(ToString::to_string).collect::<Vec<_>>(),
                held = ?pass.held.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "node-manifests prune"
            );
        }
        report.objects_examined =
            pass.applied.len() + pass.pruned.len() + pass.held.len() + pass.errors.len();
        // Every count goes through an [`Effect`] (T1.8). The answer read is
        // the apiserver's status: a 2xx apply or delete landed. It does NOT
        // say whether the object's content moved, so an apply that wrote the
        // same bytes still counts as a change — which is bounded rather than
        // churning, because a pass only runs when the directory's
        // fingerprint moved or the resync window elapsed.
        for _ in pass.applied.iter().chain(&pass.pruned) {
            report.record(Effect::answered(true));
        }
        // A held object is a SKIP with no write behind it at all: nothing was
        // sent, because its file did not parse. `Effect::Rejected` names a
        // refusal by the STORE ([`Refusal`] has an arm per store answer), so
        // there is no effect to record here and the count is written
        // directly — the one field the T1.8 gate covers is `objects_changed`,
        // which is recorded above.
        report.objects_skipped = pass.held.len();
        *self
            .memory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Memory {
            fingerprint: scan.fingerprint,
            clean: pass.clean(),
            at: Instant::now(),
        });
        Ok(ReconcileOutcome::from(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use engenho_apiserver::{
        ChainAuthenticator, RbacAuthorizer, StoreRbacEnv, handlers_from_catalog,
    };
    use engenho_store::{InProcessRouter, StoreMesh, default_config};

    const TOKEN: &str = "node-manifests-test-admin-token";

    #[test]
    fn a_multi_document_file_splits_and_skips_empty_documents() {
        let text = "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n# only a comment\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: b\n  namespace: web\n---\n";
        let objects = parse_file("f.yaml", text).unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(
            (objects[0].kind.as_str(), objects[0].namespace.as_deref()),
            ("ConfigMap", None)
        );
        assert_eq!(objects[1].api_version, "apps/v1");
        assert_eq!(objects[1].namespace.as_deref(), Some("web"));
    }

    #[test]
    fn a_document_without_a_name_is_a_typed_shape_error() {
        let err =
            parse_file("f.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n").unwrap_err();
        assert_eq!(
            err,
            NodeManifestError::Shape {
                file: "f.yaml".into(),
                document: 0,
                missing: "metadata.name",
            }
        );
        assert!(err.to_string().contains("held, not pruned"));
    }

    #[test]
    fn bad_yaml_is_a_parse_error_naming_the_document() {
        let err = parse_file("f.yaml", "apiVersion: v1\n---\nkind: [unclosed\n").unwrap_err();
        assert!(matches!(err, NodeManifestError::Shape { document: 0, .. }));
        let err = parse_file("f.yaml", "kind: [unclosed\n").unwrap_err();
        assert!(matches!(err, NodeManifestError::Parse { document: 0, .. }));
    }

    #[test]
    fn one_bad_file_does_not_stop_the_others() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.yaml"), "kind: [broken\n").unwrap();
        std::fs::write(
            dir.path().join("b.yml"),
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("c.txt"), "not a manifest").unwrap();
        let scan = scan_dir(dir.path()).unwrap();
        let names: Vec<_> = scan.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a.yaml", "b.yml"]);
        assert!(scan.files[0].outcome.is_err());
        assert_eq!(scan.files[1].outcome.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn the_fingerprint_moves_with_content_and_names_only() {
        let dir = tempfile::tempdir().unwrap();
        let empty = scan_dir(dir.path()).unwrap().fingerprint;
        let absent = scan_dir(&dir.path().join("absent")).unwrap();
        assert!(absent.files.is_empty());
        assert_eq!(
            absent.fingerprint, empty,
            "absent declares what empty declares"
        );
        std::fs::write(dir.path().join("a.yaml"), "x: 1\n").unwrap();
        let one = scan_dir(dir.path()).unwrap().fingerprint;
        assert_eq!(one, scan_dir(dir.path()).unwrap().fingerprint, "stable");
        std::fs::write(dir.path().join("a.yaml"), "x: 2\n").unwrap();
        let two = scan_dir(dir.path()).unwrap().fingerprint;
        std::fs::rename(dir.path().join("a.yaml"), dir.path().join("b.yaml")).unwrap();
        let renamed = scan_dir(dir.path()).unwrap().fingerprint;
        assert!(empty != one && one != two && two != renamed);
    }

    #[test]
    fn paths_and_queries_render_once() {
        let core = ApiPath {
            group: "",
            version: "v1",
            plural: "configmaps",
            namespace: Some("default"),
            name: Some("x"),
        };
        assert_eq!(core.to_string(), "/api/v1/namespaces/default/configmaps/x");
        let grouped = ApiPath {
            group: "apps",
            version: "v1",
            plural: "deployments",
            namespace: None,
            name: None,
        };
        assert_eq!(grouped.to_string(), "/apis/apps/v1/deployments");
        assert_eq!(
            Query::Apply.to_string(),
            "?fieldManager=engenho-node-manifests&force=true"
        );
    }

    /// A store + the router the runtime builds: the admin bearer resolves
    /// through the real chain and authz is the real RBAC authorizer.
    async fn harness() -> (Arc<StoreMesh>, RouterState, tempfile::TempDir) {
        let cfg = default_config("node-manifests").unwrap();
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), InProcessRouter::new(), cfg)
                .await
                .unwrap(),
        );
        store.initialize_singleton().await.unwrap();
        assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
        let state = RouterState::new(handlers_from_catalog(store.clone()))
            .with_authenticator(Arc::new(ChainAuthenticator::bootstrap(Some(TOKEN.into()))))
            .with_authorizer(Arc::new(RbacAuthorizer::new(StoreRbacEnv::new(
                store.clone(),
            ))));
        let data_dir = tempfile::tempdir().unwrap();
        let token = PkiFile::AdminToken.path(data_dir.path());
        std::fs::create_dir_all(token.parent().unwrap()).unwrap();
        std::fs::write(&token, TOKEN).unwrap();
        (store, state, data_dir)
    }

    fn cm(name: &str, value: &str) -> String {
        [
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: ",
            name,
            "\ndata:\n  v: \"",
            value,
            "\"\n",
        ]
        .concat()
    }

    async fn get(c: &NodeManifestsController, name: &str) -> (StatusCode, Value) {
        c.send(
            TOKEN,
            Method::GET,
            ["/api/v1/namespaces/default/configmaps/", name].concat(),
            None,
        )
        .await
    }

    /// Apply, change, prune — and a broken file holds its objects.
    #[tokio::test]
    async fn apply_change_prune_and_hold_through_the_apiserver() {
        let (_store, state, data_dir) = harness().await;
        let dir = tempfile::tempdir().unwrap();
        let c = NodeManifestsController::new(dir.path().to_owned(), data_dir.path(), state);

        // 1. Apply: two objects from one multi-document file, one from another.
        std::fs::write(
            dir.path().join("a.yaml"),
            [cm("one", "1"), "---\n".into(), cm("two", "2")].concat(),
        )
        .unwrap();
        std::fs::write(dir.path().join("b.yaml"), cm("three", "3")).unwrap();
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert!(pass.clean(), "{:?}", pass.errors);
        assert_eq!(pass.applied.len(), 3);
        let (status, one) = get(&c, "one").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(one["data"]["v"], "1");
        assert_eq!(
            one["metadata"]["labels"][DECLARED_BY_LABEL],
            DECLARED_BY_VALUE
        );
        assert_eq!(
            one["metadata"]["annotations"][DECLARED_IN_ANNOTATION],
            "a.yaml"
        );
        assert!(
            one["metadata"]["managedFields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["manager"] == FIELD_MANAGER && m["operation"] == "Apply"),
            "server-side applied under the driver's field manager"
        );

        // 2. Change: one's value moves, two is withdrawn → pruned.
        std::fs::write(dir.path().join("a.yaml"), cm("one", "9")).unwrap();
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert!(pass.clean(), "{:?}", pass.errors);
        assert_eq!(get(&c, "one").await.1["data"]["v"], "9");
        assert_eq!(get(&c, "two").await.0, StatusCode::NOT_FOUND);
        assert_eq!(pass.pruned.len(), 1);

        // 3. Hold: b.yaml breaks — three is held, not pruned; a.yaml still
        //    applies beside it.
        std::fs::write(dir.path().join("b.yaml"), "kind: [broken\n").unwrap();
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert_eq!(pass.errors.len(), 1);
        assert_eq!(pass.held.len(), 1);
        assert_eq!(get(&c, "three").await.0, StatusCode::OK);
        assert_eq!(get(&c, "one").await.0, StatusCode::OK);

        // 4. An object nobody labelled is never touched.
        let foreign = c
            .send(
                TOKEN,
                Method::PATCH,
                "/api/v1/namespaces/default/configmaps/foreign?fieldManager=op".into(),
                Some(
                    br#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"foreign"}}"#
                        .to_vec(),
                ),
            )
            .await;
        assert!(foreign.0.is_success(), "{foreign:?}");

        // 5. The whole directory goes: everything declared is pruned.
        std::fs::remove_file(dir.path().join("a.yaml")).unwrap();
        std::fs::remove_file(dir.path().join("b.yaml")).unwrap();
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert!(pass.clean(), "{:?}", pass.errors);
        assert_eq!(get(&c, "one").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(&c, "three").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(&c, "foreign").await.0, StatusCode::OK);
    }

    /// Red run of the credential: without the admin bearer the same pass is
    /// refused by RBAC, never applied — the driver is not a back door.
    #[tokio::test]
    async fn a_wrong_bearer_is_refused_by_authz() {
        let (_store, state, data_dir) = harness().await;
        std::fs::write(PkiFile::AdminToken.path(data_dir.path()), "not-the-token").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.yaml"), cm("one", "1")).unwrap();
        let c = NodeManifestsController::new(dir.path().to_owned(), data_dir.path(), state);
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert!(pass.applied.is_empty());
        assert!(
            pass.errors
                .iter()
                .any(|e| matches!(e, NodeManifestError::Apply { status, .. } if *status == 401 || *status == 403)),
            "{:?}",
            pass.errors
        );
    }

    #[tokio::test]
    async fn an_unserved_kind_is_typed_and_held() {
        let (_store, state, data_dir) = harness().await;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.yaml"),
            "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w\n",
        )
        .unwrap();
        let c = NodeManifestsController::new(dir.path().to_owned(), data_dir.path(), state);
        let pass = c.pass(&scan_dir(dir.path()).unwrap()).await;
        assert!(matches!(
            pass.errors.as_slice(),
            [NodeManifestError::UnservedKind { kind, .. }] if kind == "Widget"
        ));
    }
}
