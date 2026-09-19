//! Where a census reads from: a running apiserver, or a copy of a node's
//! data directory booted privately.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use engenho_kube_client::{Connection, KubeUrlBuilder, Kubeconfig};
use engenho_store::{ImageTripwire, InProcessRouter, StoreMesh, default_config};
use serde_json::Value;

use super::{CensusError, Gvk, Shape, text_at};
use crate::runtime::STORE_DIR;

/// Where a census reads objects from. Read-only: no method writes to what
/// the source reads.
#[async_trait]
pub trait Source: Send + Sync {
    /// Every object of `gvk`, in every namespace.
    ///
    /// # Errors
    ///
    /// A [`CensusError`] when the source cannot answer. An answer with no
    /// objects is `Ok(vec![])`, never an error.
    async fn list(&self, gvk: &Gvk) -> Result<Vec<Value>, CensusError>;

    /// Every kind the source can list.
    ///
    /// # Errors
    ///
    /// A [`CensusError`] when the source cannot answer.
    async fn kinds(&self) -> Result<Vec<Gvk>, CensusError>;

    /// The store image's tripwire, when the source is a store; `None` for a
    /// source that has no image to read (an apiserver).
    async fn image(&self) -> Option<ImageTripwire>;

    /// What the source is, for the report's header.
    fn label(&self) -> String;
}

// ── the apiserver ────────────────────────────────────────────────────────

/// A running apiserver, read by LIST. Kinds are resolved to their resource
/// paths through the server's own discovery, once, at [`ApiSource::connect`].
pub struct ApiSource {
    conn: Connection,
    served: BTreeMap<Gvk, Served>,
}

/// HTTP 405: the route exists and does not take this method.
const METHOD_NOT_ALLOWED: u16 = 405;

/// How the apiserver serves one kind.
#[derive(Debug, Clone)]
struct Served {
    plural: String,
}

/// A path on the apiserver.
#[derive(Debug, Clone, Copy)]
enum ApiPath<'a> {
    /// `/api`: the core versions.
    Core,
    /// `/api/<version>`: the core resources.
    CoreVersion(&'a str),
    /// `/apis`: the groups.
    Groups,
    /// `/apis/<group>/<version>`: one group version's resources.
    GroupVersion { group: &'a str, version: &'a str },
    /// A kind's collection, every namespace.
    List { gvk: &'a Gvk, plural: &'a str },
}

impl fmt::Display for ApiPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core => f.write_str("/api"),
            Self::CoreVersion(version) => write!(f, "/api/{version}"),
            Self::Groups => f.write_str("/apis"),
            Self::GroupVersion { group, version } => write!(f, "/apis/{group}/{version}"),
            Self::List { gvk, plural } if gvk.group.is_empty() => {
                write!(f, "/api/{}/{plural}", gvk.version)
            }
            Self::List { gvk, plural } => {
                write!(f, "/apis/{}/{}/{plural}", gvk.group, gvk.version)
            }
        }
    }
}

/// A full URL: the server, then a path.
struct Url<'a> {
    server: &'a str,
    path: ApiPath<'a>,
}

impl fmt::Display for Url<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.server, self.path)
    }
}

/// A JSON body and the URL it came from, so a shape error can name it.
struct Fetched {
    url: String,
    body: Value,
}

impl Fetched {
    /// The array under `key` in `within` (a part of this body), or a shape
    /// error naming this body's URL.
    fn array<'a>(
        &self,
        within: &'a Value,
        key: &str,
        shape: Shape,
    ) -> Result<&'a Vec<Value>, CensusError> {
        within
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| CensusError::Shape {
                url: self.url.clone(),
                shape,
            })
    }
}

impl ApiSource {
    /// Connect with the kubeconfig at `path` (its current context).
    ///
    /// # Errors
    ///
    /// [`CensusError::Kube`] when the kubeconfig cannot be read or resolved;
    /// any discovery error from [`Self::connect`].
    pub async fn from_kubeconfig(path: &Path) -> Result<Self, CensusError> {
        let conn = Kubeconfig::load(path)?.resolve_connection()?;
        Self::connect(conn).await
    }

    /// Run discovery over `conn` and keep the kinds it can LIST.
    ///
    /// Every version of every group the server lists is read. One that
    /// cannot be read fails the connect: a kind the census cannot resolve is
    /// a kind it cannot count, and a census that skipped it would report
    /// fewer objects than exist.
    ///
    /// # Errors
    ///
    /// A [`CensusError`] for any discovery request that fails.
    pub async fn connect(conn: Connection) -> Result<Self, CensusError> {
        let mut source = Self {
            conn,
            served: BTreeMap::new(),
        };
        let core = source.get(ApiPath::Core, None).await?;
        for version in core.array(&core.body, "versions", Shape::NoDiscovery)? {
            let Some(version) = version.as_str() else {
                continue;
            };
            let list = source.get(ApiPath::CoreVersion(version), None).await?;
            source.learn("", version, &list)?;
        }
        let groups = source.get(ApiPath::Groups, None).await?;
        for group in groups.array(&groups.body, "groups", Shape::NoDiscovery)? {
            let name = text_at(group, "/name").unwrap_or_default();
            for version in groups.array(group, "versions", Shape::NoDiscovery)? {
                let Some(version) = text_at(version, "/version") else {
                    continue;
                };
                let list = source
                    .get(
                        ApiPath::GroupVersion {
                            group: name,
                            version,
                        },
                        None,
                    )
                    .await?;
                source.learn(name, version, &list)?;
            }
        }
        Ok(source)
    }

    /// Record the listable resources of one discovery document. A
    /// subresource (`pods/log`) and a resource without the `list` verb are
    /// not collections, so they are not kinds a census can read.
    fn learn(&mut self, group: &str, version: &str, list: &Fetched) -> Result<(), CensusError> {
        for resource in list.array(&list.body, "resources", Shape::NoDiscovery)? {
            let (Some(plural), Some(kind)) =
                (text_at(resource, "/name"), text_at(resource, "/kind"))
            else {
                continue;
            };
            let listable = resource
                .get("verbs")
                .and_then(Value::as_array)
                .is_some_and(|verbs| verbs.iter().any(|v| v.as_str() == Some("list")));
            if plural.contains('/') || !listable {
                continue;
            }
            self.served.insert(
                Gvk::new(group, version, kind),
                Served {
                    plural: plural.to_owned(),
                },
            );
        }
        Ok(())
    }

    /// GET `path` (with a `continue` token, when paging) as JSON.
    async fn get(&self, path: ApiPath<'_>, page: Option<&str>) -> Result<Fetched, CensusError> {
        let base = Url {
            server: self.conn.server(),
            path,
        }
        .to_string();
        let url = KubeUrlBuilder::new(&base)?
            .opt_pair("continue", page)
            .finish();
        let request = self.conn.auth_header(self.conn.http().get(&url))?;
        let response = request.send().await.map_err(|e| CensusError::Http {
            url: url.clone(),
            source: Box::new(e),
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(CensusError::Status {
                url,
                code: status.as_u16(),
            });
        }
        let bytes = response.bytes().await.map_err(|e| CensusError::Http {
            url: url.clone(),
            source: Box::new(e),
        })?;
        match serde_json::from_slice(&bytes) {
            Ok(body) => Ok(Fetched { url, body }),
            Err(source) => Err(CensusError::Decode { url, source }),
        }
    }
}

#[async_trait]
impl Source for ApiSource {
    async fn list(&self, gvk: &Gvk) -> Result<Vec<Value>, CensusError> {
        let served = self
            .served
            .get(gvk)
            .ok_or_else(|| CensusError::KindNotServed { gvk: gvk.clone() })?;
        let path = ApiPath::List {
            gvk,
            plural: &served.plural,
        };
        let mut items = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut fetched = match self.get(path, page.as_deref()).await {
                Ok(fetched) => fetched,
                Err(CensusError::Status {
                    url,
                    code: METHOD_NOT_ALLOWED,
                }) => {
                    return Err(CensusError::NotListable {
                        gvk: gvk.clone(),
                        url,
                    });
                }
                Err(e) => return Err(e),
            };
            let Some(Value::Array(batch)) = fetched.body.get_mut("items").map(Value::take) else {
                return Err(CensusError::Shape {
                    url: fetched.url,
                    shape: Shape::NoItems,
                });
            };
            items.extend(batch);
            page = text_at(&fetched.body, "/metadata/continue")
                .filter(|token| !token.is_empty())
                .map(str::to_owned);
            if page.is_none() {
                return Ok(items);
            }
        }
    }

    async fn kinds(&self) -> Result<Vec<Gvk>, CensusError> {
        Ok(self.served.keys().cloned().collect())
    }

    async fn image(&self) -> Option<ImageTripwire> {
        None
    }

    fn label(&self) -> String {
        ["apiserver ", self.conn.server()].concat()
    }
}

// ── a data directory ─────────────────────────────────────────────────────

/// The cluster name the private store is booted under. The store keys
/// nothing by it; openraft only labels its metrics with it.
const CENSUS_CLUSTER: &str = "engenho-census";

/// A copy of a node's data directory, booted privately.
///
/// ★ READ-ONLY BY CONSTRUCTION. The directory given is only ever READ: its
/// `store` is copied into a private scratch directory and the store is booted
/// there, through the same [`StoreMesh::start_durable`] the daemon boots
/// through, so the objects listed are the ones the daemon would serve after
/// its own boot (committed entries the image had not yet caught up with are
/// replayed first). Whatever the boot writes — its lock, openraft's vote, a
/// leader's blank entry — lands in the copy, which is deleted when this is
/// dropped.
///
/// Copy the directory from a STOPPED node: the census cannot tell a copy
/// taken mid-write from a damaged store.
pub struct DataDirSource {
    // Field order is drop order: the store stops before its files go.
    mesh: StoreMesh,
    scratch: Scratch,
    origin: PathBuf,
}

impl DataDirSource {
    /// Copy `data_dir/store` into a private scratch directory and boot the
    /// store there.
    ///
    /// # Errors
    ///
    /// [`CensusError::NoStore`] when `data_dir` has no readable store;
    /// [`CensusError::Copy`] / [`CensusError::UncopyableEntry`] when it
    /// cannot be copied; [`CensusError::Store`] when the copy does not boot;
    /// [`CensusError::EmptyStore`] when it holds no raft state.
    pub async fn open(data_dir: &Path) -> Result<Self, CensusError> {
        let store = data_dir.join(STORE_DIR);
        let scratch = Scratch::copy_of(&store)?;
        let mesh = StoreMesh::start_durable(
            1,
            "in-process://census".to_owned(),
            InProcessRouter::new(),
            default_config(CENSUS_CLUSTER)?,
            scratch.store(),
        )
        .await?;
        if !mesh.is_initialized().await {
            mesh.terminate().await?;
            return Err(CensusError::EmptyStore {
                path: data_dir.to_owned(),
            });
        }
        Ok(Self {
            mesh,
            scratch,
            origin: data_dir.to_owned(),
        })
    }

    /// Stop the private store, then delete the copy.
    ///
    /// # Errors
    ///
    /// [`CensusError::Store`] when the store does not stop cleanly. The copy
    /// is deleted either way.
    pub async fn close(self) -> Result<(), CensusError> {
        let Self { mesh, scratch, .. } = self;
        let stopped = mesh.terminate().await;
        drop(scratch);
        Ok(stopped?)
    }
}

#[async_trait]
impl Source for DataDirSource {
    async fn list(&self, gvk: &Gvk) -> Result<Vec<Value>, CensusError> {
        Ok(self
            .mesh
            .list(&gvk.group, &gvk.version, &gvk.kind, None)
            .await
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }

    async fn kinds(&self) -> Result<Vec<Gvk>, CensusError> {
        // The store visits keys in key order, which sorts by group, version
        // and kind first: each kind's keys are contiguous, so a kind is new
        // exactly when it differs from the last one kept.
        let mut kinds: Vec<Gvk> = Vec::new();
        self.mesh
            .for_each_resource(|key, _, _| {
                let seen = kinds.last().is_some_and(|last| {
                    last.group == key.group && last.version == key.version && last.kind == key.kind
                });
                if !seen {
                    kinds.push(Gvk::new(&key.group, &key.version, &key.kind));
                }
            })
            .await;
        Ok(kinds)
    }

    async fn image(&self) -> Option<ImageTripwire> {
        self.mesh.image_tripwire().await
    }

    fn label(&self) -> String {
        ["data directory ", &self.origin.display().to_string()].concat()
    }
}

/// A private scratch directory holding a copy of a store, deleted on drop.
struct Scratch {
    root: PathBuf,
}

/// The scratch directory's name: unique per process and instant.
struct ScratchName {
    pid: u32,
    nanos: u128,
}

impl fmt::Display for ScratchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "engenho-census-{}-{}", self.pid, self.nanos)
    }
}

impl Scratch {
    /// Copy the store directory `store` into a fresh scratch directory.
    fn copy_of(store: &Path) -> Result<Self, CensusError> {
        std::fs::read_dir(store).map_err(|source| CensusError::NoStore {
            path: store.to_owned(),
            source,
        })?;
        let name = ScratchName {
            pid: std::process::id(),
            nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        };
        let root = std::env::temp_dir().join(name.to_string());
        std::fs::create_dir(&root).map_err(|source| CensusError::Copy {
            from: store.to_owned(),
            to: root.clone(),
            source,
        })?;
        // From here the directory exists, so a failed copy still deletes it.
        let scratch = Self { root };
        copy_tree(store, &scratch.store())?;
        Ok(scratch)
    }

    fn store(&self) -> PathBuf {
        self.root.join(STORE_DIR)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.root) {
            tracing::warn!(
                path = %self.root.display(),
                %error,
                "census: could not delete the private store copy"
            );
        }
    }
}

/// Copy the directory `from` to `to` (which must not exist): directories
/// and regular files only. Anything else refuses the copy rather than be
/// skipped, so the copy is never silently missing part of the store.
fn copy_tree(from: &Path, to: &Path) -> Result<(), CensusError> {
    let copy_err = |source| CensusError::Copy {
        from: from.to_owned(),
        to: to.to_owned(),
        source,
    };
    std::fs::create_dir(to).map_err(copy_err)?;
    for entry in std::fs::read_dir(from).map_err(copy_err)? {
        let entry = entry.map_err(copy_err)?;
        let kind = entry.file_type().map_err(copy_err)?;
        let (src, dst) = (entry.path(), to.join(entry.file_name()));
        if kind.is_dir() {
            copy_tree(&src, &dst)?;
        } else if kind.is_file() {
            std::fs::copy(&src, &dst).map_err(|source| CensusError::Copy {
                from: src.clone(),
                to: dst.clone(),
                source,
            })?;
        } else {
            return Err(CensusError::UncopyableEntry { path: src });
        }
    }
    Ok(())
}
