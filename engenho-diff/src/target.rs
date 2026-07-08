//! The two ends of the diff — the [`DiffTarget`] trait and its concrete
//! impls: [`EngenhoTarget`] (booted in-process, the r7 seam) and
//! [`K3sTarget`] (the live reference oracle from a kubeconfig).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use engenho_apiserver::{ApiServer, ResourceHandler, StoreBackedHandler};
use engenho_kube_client::{Connection, Kubeconfig, ReqwestKubeClient};
use engenho_store::{InProcessRouter, StoreMesh, default_config};
use engenho_types::auth::KubeAuth;
use serde_json::Value;

use crate::observe::{DiffError, HttpMethod, Observation};
use crate::verdict::{Side, Verdict};

/// A cluster the harness can fire operations at.
///
/// **Why `raw()` and not a typed `client()`:** the typed CRUD trait
/// `engenho_types::client::KubeClient` is NOT object-safe — its methods are
/// generic over `R: KubeResource`, so `&dyn KubeClient` does not compile
/// (there are zero `dyn KubeClient` uses in the whole engenho tree). A
/// trait-level `client() -> &dyn KubeClient`, as the design sketched, is
/// therefore not expressible in Rust. The reuse of the client is instead
/// STRUCTURAL: every real target is built over the same [`Connection`] that
/// also powers `raw()`, and exposes a concrete [`ReqwestKubeClient`] via
/// `kube_client()`. The differ's workhorse is `raw()` because it captures
/// the status code + full JSON body that the typed CRUD surface erases into
/// `Result<R, KubeError>`.
#[async_trait::async_trait]
pub trait DiffTarget: Send + Sync {
    /// Which side this target is.
    fn side(&self) -> Side;

    /// Issue `method path` (+ optional body) and return the typed
    /// observation. Reaches the discovery / status-code / error-body
    /// surfaces the typed CRUD trait cannot.
    ///
    /// # Errors
    ///
    /// [`DiffError::Transport`] if the request fails, or [`DiffError::Kube`]
    /// if the auth header can't be built.
    async fn raw(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<&[u8]>,
        content_type: Option<&str>,
    ) -> Result<Observation, DiffError>;
}

async fn http_raw(
    conn: &Connection,
    side: Side,
    method: HttpMethod,
    path: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
) -> Result<Observation, DiffError> {
    let mut url = String::from(conn.server());
    url.push_str(path);
    let mut rb = conn.http().request(method.as_reqwest(), &url);
    if let Some(ct) = content_type {
        rb = rb.header("Content-Type", ct);
    }
    if let Some(b) = body {
        rb = rb.body(b.to_vec());
    }
    let rb = conn
        .auth_header(rb)
        .map_err(|e| DiffError::Kube(e.to_string()))?;
    let resp = rb.send().await.map_err(|source| DiffError::Transport {
        side: side.label(),
        source,
    })?;
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = resp.text().await.map_err(|source| DiffError::Transport {
        side: side.label(),
        source,
    })?;
    let json = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    };
    Ok(Observation {
        status,
        content_type,
        json,
    })
}

/// engenho booted IN-PROCESS: a real `StoreMesh` (Raft-of-1 + MVCC) plus an
/// `ApiServer` on an ephemeral loopback port, plaintext (no TLS, no VM) —
/// milliseconds to stand up. Copies the r7 boot seam.
pub struct EngenhoTarget {
    _store: Arc<StoreMesh>,
    server: ApiServer,
    conn: Connection,
    client: ReqwestKubeClient,
}

impl EngenhoTarget {
    /// Boot engenho in-process with store-backed handlers for `kinds`
    /// (scope auto-derived from the resource catalog via `for_kind`).
    ///
    /// # Errors
    ///
    /// [`DiffError::Boot`] if the store, leadership, a handler, or the
    /// server fails to come up.
    pub async fn boot_in_process(kinds: &[&str]) -> Result<Self, DiffError> {
        let router = InProcessRouter::new();
        let cfg = default_config("engenho-diff").map_err(|e| DiffError::Boot(format!("{e:?}")))?;
        let store = Arc::new(
            StoreMesh::start(1, "in-process://1".into(), router, cfg)
                .await
                .map_err(|e| DiffError::Boot(format!("{e:?}")))?,
        );
        store
            .initialize_singleton()
            .await
            .map_err(|e| DiffError::Boot(format!("{e:?}")))?;
        if !store.wait_for_leadership(Duration::from_secs(5)).await {
            return Err(DiffError::Boot("store did not acquire leadership".into()));
        }
        let mut handlers: Vec<Arc<dyn ResourceHandler>> = Vec::with_capacity(kinds.len());
        for k in kinds {
            let h = StoreBackedHandler::for_kind(store.clone(), k)
                .ok_or_else(|| DiffError::Boot(format!("for_kind {k}: not a cataloged kind")))?;
            handlers.push(Arc::new(h));
        }
        let addr = "127.0.0.1:0"
            .parse()
            .map_err(|e| DiffError::Boot(format!("addr parse: {e}")))?;
        let server = ApiServer::start(addr, handlers, None)
            .await
            .map_err(|e| DiffError::Boot(format!("{e:?}")))?;
        let mut base = String::from("http://");
        base.push_str(&server.local_addr().to_string());
        let conn = Connection::new(&base, KubeAuth::Anonymous, None)
            .map_err(|e| DiffError::Boot(format!("{e:?}")))?;
        let client = ReqwestKubeClient::new(conn.clone());
        Ok(Self {
            _store: store,
            server,
            conn,
            client,
        })
    }

    /// The concrete typed CRUD client over the same connection (structural
    /// reuse — see [`DiffTarget`]).
    #[must_use]
    pub fn kube_client(&self) -> &ReqwestKubeClient {
        &self.client
    }

    /// Gracefully stop the in-process server.
    ///
    /// # Errors
    ///
    /// Propagates the server's shutdown error (currently never `Err`).
    pub async fn shutdown(self) -> Result<(), DiffError> {
        self.server
            .shutdown()
            .await
            .map_err(|e| DiffError::Boot(e.to_string()))
    }
}

#[async_trait::async_trait]
impl DiffTarget for EngenhoTarget {
    fn side(&self) -> Side {
        Side::Engenho
    }

    async fn raw(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<&[u8]>,
        content_type: Option<&str>,
    ) -> Result<Observation, DiffError> {
        http_raw(&self.conn, Side::Engenho, method, path, body, content_type).await
    }
}

/// The live k3s reference oracle, resolved from a kubeconfig.
pub struct K3sTarget {
    conn: Connection,
    client: ReqwestKubeClient,
}

impl K3sTarget {
    /// Build from a kubeconfig file (its current-context cluster + user).
    /// Honors client-cert auth + the CA; the server URL's host (127.0.0.1)
    /// is verified against the cert's IP SAN — so the CLI's
    /// `--tls-server-name 127.0.0.1` need not be threaded (the SNI host IS
    /// 127.0.0.1).
    ///
    /// # Errors
    ///
    /// [`DiffError::Kube`] if the kubeconfig can't be parsed or resolved.
    pub fn from_kubeconfig(path: &Path) -> Result<Self, DiffError> {
        let kc = Kubeconfig::load(path).map_err(|e| DiffError::Kube(e.to_string()))?;
        let conn = kc
            .resolve_connection()
            .map_err(|e| DiffError::Kube(e.to_string()))?;
        let client = ReqwestKubeClient::new(conn.clone());
        Ok(Self { conn, client })
    }

    /// The concrete typed CRUD client (structural reuse — see [`DiffTarget`]).
    #[must_use]
    pub fn kube_client(&self) -> &ReqwestKubeClient {
        &self.client
    }

    /// Preflight: confirm the oracle answers `GET /api/v1` with 2xx. Returns
    /// `Some(Verdict::ReferenceUnreachable)` on failure — the fail-loud
    /// signal the runner must honor, never a silent skip.
    pub async fn preflight(&self) -> Option<Verdict> {
        match self.raw(HttpMethod::Get, "/api/v1", None, None).await {
            Ok(obs) if obs.is_success() => None,
            _ => Some(Verdict::ReferenceUnreachable),
        }
    }
}

#[async_trait::async_trait]
impl DiffTarget for K3sTarget {
    fn side(&self) -> Side {
        Side::K3s
    }

    async fn raw(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<&[u8]>,
        content_type: Option<&str>,
    ) -> Result<Observation, DiffError> {
        http_raw(&self.conn, Side::K3s, method, path, body, content_type).await
    }
}
