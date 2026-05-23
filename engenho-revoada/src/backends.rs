//! # backends — the 5 R5/R6 real-system StoreBackend impls
//!
//! One backend per face kind. Each carries the domain-specific
//! configuration its real-system integration will need (raft
//! peers, kube-apiserver endpoint, nomad address, etc.) and wraps
//! an [`InMemoryStore`] for verb dispatch. The wrapping pattern is
//! the same one [`FileSystemBackend`](crate::FileSystemBackend)
//! established — overlay a real backing system on top of an
//! InMemoryStore cache for watch fan-out + fast reads; real I/O
//! lives in the wrapping methods.
//!
//! ## What ships today vs what R5/R6 fills in
//!
//! Today's impl uses InMemoryStore for all 5 backends — the typed
//! shape ships now so operator code can take a dependency on it +
//! tests cover the wiring end-to-end. R5/R6 PRs fill in the
//! real-system wiring (raft replication / kube-apiserver POST /
//! nomad HTTP / dbus / supervised containers) by overriding the
//! verb impls. Operators using these backends in test/dev today
//! get in-memory behavior with the right typed identity.
//!
//! ## Operator pattern
//!
//! ```rust,ignore
//! use engenho_revoada::backends::{RaftBackend, RaftConfig};
//! use engenho_revoada::{Cluster, FabricStrategy};
//!
//! let backend = RaftBackend::new(RaftConfig {
//!     node_id: 1,
//!     peers: vec!["10.0.0.1:7000".into(), "10.0.0.2:7000".into()],
//!     log_dir: "/var/lib/engenho/raft".into(),
//! });
//!
//! let cluster = Cluster::builder()
//!     .strategy(FabricStrategy::prescribed_homelab())
//!     .face_pure_raft("prod")
//!     .topology(Quorum3M)
//!     .with_face_backend(Box::new(backend))
//!     .start()?;
//! ```

use crate::backend::StoreBackend;
use crate::face::{FaceError, FaceWatchStream, ResourceFormat, ResourceRef};
use crate::face_store::InMemoryStore;
use crate::format::AdapterRegistry;

// ─────────────────────────────────────────────────────────────────
// RaftBackend — openraft-replicated (R5)
// ─────────────────────────────────────────────────────────────────

/// Configuration for [`RaftBackend`]. Operators set these per-node
/// at boot; the backend's `name()` reflects the node identity.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Stable numeric ID for this node in the raft cluster.
    pub node_id: u64,
    /// Initial peer addresses (host:port). The first boot uses
    /// these to bootstrap; subsequent boots load the cluster
    /// membership from the persistent log.
    pub peers: Vec<String>,
    /// Directory for the raft log + state machine snapshots.
    pub log_dir: std::path::PathBuf,
}

/// Openraft-replicated store backend. Wraps
/// [`crate::consensus::store`] under a [`StoreBackend`] trait
/// surface so [`Face`](crate::Face) impls can use raft replication
/// without depending on openraft directly.
///
/// **R5 wiring:** today the verbs route through InMemoryStore;
/// the R5 PR overlays raft client_write on every apply (followers
/// see writes through the state-machine apply path).
pub struct RaftBackend {
    config: RaftConfig,
    store: InMemoryStore,
}

impl RaftBackend {
    /// Construct from a config. The face's name is derived from
    /// the node ID (`"raft-node-{id}"`).
    #[must_use]
    pub fn new(config: RaftConfig) -> Self {
        let face_name = format!("raft-node-{}", config.node_id);
        let mut store = InMemoryStore::new(face_name);
        store.set_adapters(AdapterRegistry::default());
        Self { config, store }
    }

    /// Construct with a custom adapter registry.
    #[must_use]
    pub fn with_adapters(mut self, adapters: AdapterRegistry) -> Self {
        self.store.set_adapters(adapters);
        self
    }

    /// Borrow the config — telemetry + debug.
    #[must_use]
    pub fn config(&self) -> &RaftConfig {
        &self.config
    }
}

impl StoreBackend for RaftBackend {
    fn name(&self) -> &str {
        "openraft"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        // R5: call raft.client_write(envelope) here, which fans to
        // followers + commits via consensus. The state machine
        // apply path then mirrors into InMemoryStore.
        // Today: direct apply for the prototype.
        self.store.apply(format, body)
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        // Reads are local — operators tolerate eventual consistency
        // via raft's read-index or linearized via raft.client_read.
        self.store.get(reference, format)
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        // R5: raft.client_write(delete-op); state machine deletes
        // on apply.
        self.store.delete(reference)
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }

    fn resource_count(&self) -> usize {
        self.store.len()
    }

    fn subscriber_count(&self) -> usize {
        self.store.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.store.snapshot()
    }

    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.store.restore(snapshot_bytes)
    }
}

// ─────────────────────────────────────────────────────────────────
// KubeApiServerBackend — bridge to a real kube-apiserver (R6)
// ─────────────────────────────────────────────────────────────────

/// Configuration for [`KubeApiServerBackend`].
#[derive(Clone, Debug)]
pub struct KubeApiServerConfig {
    /// HTTPS URL of the apiserver (e.g.
    /// `"https://kubernetes.default.svc"`).
    pub endpoint: String,
    /// Path to the kubeconfig file with credentials. Operators
    /// can also supply a service-account token directly via
    /// [`Self::bearer_token`].
    pub kubeconfig: Option<std::path::PathBuf>,
    /// Bearer token alternative to kubeconfig.
    pub bearer_token: Option<String>,
    /// API version this backend targets (e.g. `"1.34"`).
    pub api_version: String,
}

/// Bridge to a real kube-apiserver. Wraps
/// [`engenho-kube-client`](https://github.com/pleme-io/engenho)'s
/// client + an InMemoryStore mirror for fast reads + watch
/// fan-out.
///
/// **R6 wiring:** verbs proxy to the apiserver over HTTPS using
/// the configured credentials; the watch stream subscribes to
/// `/api/v1/.../?watch=true&resourceVersion=...`. The InMemoryStore
/// mirror keeps fresh from the watch stream so reads don't hit
/// the apiserver on hot paths.
pub struct KubeApiServerBackend {
    config: KubeApiServerConfig,
    store: InMemoryStore,
}

impl KubeApiServerBackend {
    /// Construct from a config.
    #[must_use]
    pub fn new(config: KubeApiServerConfig) -> Self {
        let face_name = format!("kube-{}", config.api_version);
        let mut store = InMemoryStore::new(face_name);
        store.set_adapters(AdapterRegistry::default());
        Self { config, store }
    }

    /// Borrow the config.
    #[must_use]
    pub fn config(&self) -> &KubeApiServerConfig {
        &self.config
    }
}

impl StoreBackend for KubeApiServerBackend {
    fn name(&self) -> &str {
        "kube-apiserver"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        // R6: POST /api/v1/{namespace}/{kind} → wait for apiserver
        // 201/200 → mirror into store via watch stream.
        self.store.apply(format, body)
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        // R6: DELETE /api/v1/{ns}/{kind}/{name}
        self.store.delete(reference)
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }

    fn resource_count(&self) -> usize {
        self.store.len()
    }

    fn subscriber_count(&self) -> usize {
        self.store.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.store.snapshot()
    }

    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.store.restore(snapshot_bytes)
    }
}

// ─────────────────────────────────────────────────────────────────
// NomadHttpBackend — bridge to a real Nomad HTTP server (R6)
// ─────────────────────────────────────────────────────────────────

/// Configuration for [`NomadHttpBackend`].
#[derive(Clone, Debug)]
pub struct NomadHttpConfig {
    /// HTTP URL of the Nomad server (e.g. `"http://127.0.0.1:4646"`).
    pub address: String,
    /// Optional ACL token.
    pub token: Option<String>,
    /// Nomad region.
    pub region: String,
}

/// Bridge to a real Nomad HTTP API. Wraps a future Nomad client +
/// an InMemoryStore mirror.
///
/// **R6 wiring:** verbs proxy through POST `/v1/jobs/{name}`,
/// GET `/v1/job/{name}`, list `/v1/jobs?prefix=...`. Watch via
/// blocking-query `index=...`.
pub struct NomadHttpBackend {
    config: NomadHttpConfig,
    store: InMemoryStore,
}

impl NomadHttpBackend {
    #[must_use]
    pub fn new(config: NomadHttpConfig) -> Self {
        let face_name = format!("nomad-{}", config.region);
        let mut store = InMemoryStore::new(face_name);
        store.set_adapters(AdapterRegistry::default());
        Self { config, store }
    }

    #[must_use]
    pub fn config(&self) -> &NomadHttpConfig {
        &self.config
    }
}

impl StoreBackend for NomadHttpBackend {
    fn name(&self) -> &str {
        "nomad-http"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }

    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }

    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }

    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }

    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }

    fn resource_count(&self) -> usize {
        self.store.len()
    }

    fn subscriber_count(&self) -> usize {
        self.store.subscriber_count()
    }

    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.store.snapshot()
    }

    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.store.restore(snapshot_bytes)
    }
}

// ─────────────────────────────────────────────────────────────────
// SystemdDbusBackend — systemd unit-file render + dbus (R6)
// ─────────────────────────────────────────────────────────────────

/// Configuration for [`SystemdDbusBackend`].
#[derive(Clone, Debug)]
pub struct SystemdDbusConfig {
    /// Whether to write user units (`~/.config/systemd/user/`)
    /// vs system units (`/etc/systemd/system/`).
    pub user_units: bool,
    /// Whether to run `systemctl daemon-reload` after writes.
    pub auto_reload: bool,
}

/// Bridge to systemd via dbus + unit-file renderer.
///
/// **R6 wiring:** apply → render unit file to the right path +
/// dbus reload. delete → stop unit + remove file. watch → subscribe
/// to PropertiesChanged on the systemd Unit interface.
pub struct SystemdDbusBackend {
    config: SystemdDbusConfig,
    store: InMemoryStore,
}

impl SystemdDbusBackend {
    #[must_use]
    pub fn new(config: SystemdDbusConfig) -> Self {
        let face_name = if config.user_units {
            "systemd-user".to_string()
        } else {
            "systemd-system".to_string()
        };
        let mut store = InMemoryStore::new(face_name);
        store.set_adapters(AdapterRegistry::default());
        Self { config, store }
    }

    #[must_use]
    pub fn config(&self) -> &SystemdDbusConfig {
        &self.config
    }
}

impl StoreBackend for SystemdDbusBackend {
    fn name(&self) -> &str {
        "systemd-dbus"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
    fn resource_count(&self) -> usize {
        self.store.len()
    }
    fn subscriber_count(&self) -> usize {
        self.store.subscriber_count()
    }
    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.store.snapshot()
    }
    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.store.restore(snapshot_bytes)
    }
}

// ─────────────────────────────────────────────────────────────────
// SupervisedSystemdBackend — bare-metal supervised containers (R6)
// ─────────────────────────────────────────────────────────────────

/// Configuration for [`SupervisedSystemdBackend`].
#[derive(Clone, Debug)]
pub struct SupervisedSystemdConfig {
    /// Hostname this engenho instance is running on. Embedded in
    /// the face name for telemetry.
    pub hostname: String,
    /// Container runtime (e.g. `"podman"`, `"docker"`).
    pub runtime: String,
}

/// Bare-metal supervisor: render each Pod into a systemd unit
/// that runs the container via podman/docker, supervised by the
/// host's existing systemd. No Kubernetes apiserver, no raft —
/// just typed manifests → systemd units.
pub struct SupervisedSystemdBackend {
    config: SupervisedSystemdConfig,
    store: InMemoryStore,
}

impl SupervisedSystemdBackend {
    #[must_use]
    pub fn new(config: SupervisedSystemdConfig) -> Self {
        let face_name = format!("bms-{}", config.hostname);
        let mut store = InMemoryStore::new(face_name);
        store.set_adapters(AdapterRegistry::default());
        Self { config, store }
    }

    #[must_use]
    pub fn config(&self) -> &SupervisedSystemdConfig {
        &self.config
    }
}

impl StoreBackend for SupervisedSystemdBackend {
    fn name(&self) -> &str {
        "supervised-systemd"
    }

    fn apply(&self, format: ResourceFormat, body: &[u8]) -> Result<(), FaceError> {
        self.store.apply(format, body)
    }
    fn get(&self, reference: &ResourceRef, format: ResourceFormat) -> Result<Vec<u8>, FaceError> {
        self.store.get(reference, format)
    }
    fn list(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Vec<Vec<u8>>, FaceError> {
        self.store.list(kind, namespace, format)
    }
    fn delete(&self, reference: &ResourceRef) -> Result<(), FaceError> {
        self.store.delete(reference)
    }
    fn watch(
        &self,
        kind: &str,
        namespace: Option<&str>,
        format: ResourceFormat,
    ) -> Result<Box<dyn FaceWatchStream>, FaceError> {
        self.store.watch(kind, namespace, format)
    }
    fn resource_count(&self) -> usize {
        self.store.len()
    }
    fn subscriber_count(&self) -> usize {
        self.store.subscriber_count()
    }
    fn snapshot(&self) -> Result<Vec<u8>, FaceError> {
        self.store.snapshot()
    }
    fn restore(&self, snapshot_bytes: &[u8]) -> Result<(), FaceError> {
        self.store.restore(snapshot_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::face::encode_native_envelope;

    fn pod_ref() -> ResourceRef {
        ResourceRef::namespaced("Pod", "nginx", "default")
    }

    fn envelope() -> Vec<u8> {
        encode_native_envelope(&pod_ref(), b"payload").unwrap()
    }

    // ── RaftBackend ───────────────────────────────────────────────

    #[test]
    fn raft_backend_constructs_from_config() {
        let backend = RaftBackend::new(RaftConfig {
            node_id: 7,
            peers: vec!["10.0.0.1:7000".into(), "10.0.0.2:7000".into()],
            log_dir: "/var/lib/engenho/raft".into(),
        });
        assert_eq!(backend.name(), "openraft");
        assert_eq!(backend.config().node_id, 7);
        assert_eq!(backend.config().peers.len(), 2);
    }

    #[test]
    fn raft_backend_dispatches_5_verbs() {
        let backend = RaftBackend::new(RaftConfig {
            node_id: 1,
            peers: vec![],
            log_dir: "/tmp".into(),
        });
        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        let r = pod_ref();
        assert_eq!(backend.get(&r, ResourceFormat::Native).unwrap(), envelope());
        assert_eq!(
            backend
                .list("Pod", Some("default"), ResourceFormat::Native)
                .unwrap()
                .len(),
            1
        );
        let _ = backend.watch("Pod", None, ResourceFormat::Native).unwrap();
        backend.delete(&r).unwrap();
    }

    // ── KubeApiServerBackend ─────────────────────────────────────

    #[test]
    fn kube_apiserver_backend_constructs_from_config() {
        let backend = KubeApiServerBackend::new(KubeApiServerConfig {
            endpoint: "https://k.svc:6443".into(),
            kubeconfig: Some("/etc/kubernetes/admin.conf".into()),
            bearer_token: None,
            api_version: "1.34".into(),
        });
        assert_eq!(backend.name(), "kube-apiserver");
        assert_eq!(backend.config().api_version, "1.34");
    }

    #[test]
    fn kube_apiserver_backend_dispatches_verbs() {
        let backend = KubeApiServerBackend::new(KubeApiServerConfig {
            endpoint: "https://k:6443".into(),
            kubeconfig: None,
            bearer_token: Some("tok".into()),
            api_version: "1.34".into(),
        });
        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    // ── NomadHttpBackend ─────────────────────────────────────────

    #[test]
    fn nomad_http_backend_constructs_from_config() {
        let backend = NomadHttpBackend::new(NomadHttpConfig {
            address: "http://127.0.0.1:4646".into(),
            token: None,
            region: "global".into(),
        });
        assert_eq!(backend.name(), "nomad-http");
        assert_eq!(backend.config().region, "global");
    }

    #[test]
    fn nomad_http_backend_dispatches_verbs() {
        let backend = NomadHttpBackend::new(NomadHttpConfig {
            address: "http://n:4646".into(),
            token: None,
            region: "us-east".into(),
        });
        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    // ── SystemdDbusBackend ──────────────────────────────────────

    #[test]
    fn systemd_dbus_backend_constructs_from_config() {
        let backend = SystemdDbusBackend::new(SystemdDbusConfig {
            user_units: false,
            auto_reload: true,
        });
        assert_eq!(backend.name(), "systemd-dbus");
        assert!(!backend.config().user_units);
    }

    #[test]
    fn systemd_dbus_backend_dispatches_verbs() {
        let backend = SystemdDbusBackend::new(SystemdDbusConfig {
            user_units: true,
            auto_reload: false,
        });
        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    // ── SupervisedSystemdBackend ────────────────────────────────

    #[test]
    fn supervised_systemd_backend_constructs_from_config() {
        let backend = SupervisedSystemdBackend::new(SupervisedSystemdConfig {
            hostname: "host01".into(),
            runtime: "podman".into(),
        });
        assert_eq!(backend.name(), "supervised-systemd");
        assert_eq!(backend.config().runtime, "podman");
    }

    #[test]
    fn supervised_systemd_backend_dispatches_verbs() {
        let backend = SupervisedSystemdBackend::new(SupervisedSystemdConfig {
            hostname: "h".into(),
            runtime: "docker".into(),
        });
        backend.apply(ResourceFormat::Native, &envelope()).unwrap();
        assert_eq!(backend.resource_count(), 1);
    }

    // ── Cross-backend snapshot interop ──────────────────────────

    #[test]
    fn snapshot_round_trips_across_every_backend_pair() {
        // Apply same data to every backend; snapshot; restore each
        // snapshot into every OTHER backend; verify state matches.
        let backends: Vec<Box<dyn StoreBackend>> = vec![
            Box::new(InMemoryStore::new("mem")),
            Box::new(RaftBackend::new(RaftConfig {
                node_id: 1,
                peers: vec![],
                log_dir: "/tmp".into(),
            })),
            Box::new(KubeApiServerBackend::new(KubeApiServerConfig {
                endpoint: "https://k:6443".into(),
                kubeconfig: None,
                bearer_token: None,
                api_version: "1.34".into(),
            })),
            Box::new(NomadHttpBackend::new(NomadHttpConfig {
                address: "http://n:4646".into(),
                token: None,
                region: "g".into(),
            })),
            Box::new(SystemdDbusBackend::new(SystemdDbusConfig {
                user_units: false,
                auto_reload: false,
            })),
            Box::new(SupervisedSystemdBackend::new(SupervisedSystemdConfig {
                hostname: "h".into(),
                runtime: "podman".into(),
            })),
        ];

        // Seed each backend with the same envelope.
        for b in &backends {
            b.apply(ResourceFormat::Native, &envelope()).unwrap();
        }

        // Snapshot each; assert all snapshots are byte-identical
        // (every backend uses the same InMemoryStore CBOR codec).
        let snaps: Vec<Vec<u8>> = backends.iter().map(|b| b.snapshot().unwrap()).collect();
        for i in 1..snaps.len() {
            assert_eq!(
                snaps[i],
                snaps[0],
                "backend {} snapshot diverged from backend 0",
                backends[i].name(),
            );
        }

        // Cross-restore: take backend 0's snapshot, restore into
        // every other backend, verify state.
        let snap = snaps.into_iter().next().unwrap();
        for b in &backends[1..] {
            // Wipe then restore.
            let r = pod_ref();
            let _ = b.delete(&r);
            assert_eq!(b.resource_count(), 0);
            b.restore(&snap).unwrap();
            assert_eq!(b.resource_count(), 1, "backend {} restore failed", b.name());
        }
    }

    #[test]
    fn every_backend_implements_send_sync_static() {
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<RaftBackend>();
        assert_send_sync_static::<KubeApiServerBackend>();
        assert_send_sync_static::<NomadHttpBackend>();
        assert_send_sync_static::<SystemdDbusBackend>();
        assert_send_sync_static::<SupervisedSystemdBackend>();
    }

    #[test]
    fn every_backend_carries_named_identity() {
        let backends: Vec<(&'static str, Box<dyn StoreBackend>)> = vec![
            (
                "openraft",
                Box::new(RaftBackend::new(RaftConfig {
                    node_id: 1,
                    peers: vec![],
                    log_dir: "/tmp".into(),
                })),
            ),
            (
                "kube-apiserver",
                Box::new(KubeApiServerBackend::new(KubeApiServerConfig {
                    endpoint: "x".into(),
                    kubeconfig: None,
                    bearer_token: None,
                    api_version: "1".into(),
                })),
            ),
            (
                "nomad-http",
                Box::new(NomadHttpBackend::new(NomadHttpConfig {
                    address: "x".into(),
                    token: None,
                    region: "g".into(),
                })),
            ),
            (
                "systemd-dbus",
                Box::new(SystemdDbusBackend::new(SystemdDbusConfig {
                    user_units: false,
                    auto_reload: false,
                })),
            ),
            (
                "supervised-systemd",
                Box::new(SupervisedSystemdBackend::new(SupervisedSystemdConfig {
                    hostname: "h".into(),
                    runtime: "p".into(),
                })),
            ),
        ];
        for (expected_name, backend) in backends {
            assert_eq!(backend.name(), expected_name);
        }
    }
}
