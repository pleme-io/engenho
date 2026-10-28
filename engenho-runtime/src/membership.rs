use std::path::{Path, PathBuf};

use engenho_apiserver::{ClusterCa, PkiError, load_or_generate_ca, load_shared_ca};
use engenho_control_types::pin::KeyMaterial;
use engenho_store::RaftNodeId;

use crate::boot_config::{BootConfig, Membership};
use crate::error::RuntimeError;

pub(crate) const SINGLETON: RaftNodeId = 1;

pub(crate) struct NodeIdentity {
    #[cfg_attr(not(feature = "mesh-quic"), allow(dead_code))]
    key: KeyMaterial,
    id: RaftNodeId,
}

impl NodeIdentity {
    pub(crate) fn load_or_mint(path: &Path) -> Result<Self, RuntimeError> {
        let refused = |reason: String| RuntimeError::Identity {
            path: path.to_path_buf(),
            reason,
        };
        let key = match std::fs::read_to_string(path) {
            Ok(pem) => KeyMaterial::from_pem(&pem).map_err(|e| refused(e.to_string()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let key = KeyMaterial::generate().map_err(|e| refused(e.to_string()))?;
                if let Some(dir) = path.parent() {
                    crate::runtime::create_pki_dir(dir)?;
                }
                crate::runtime::write_at_mode(path, &key.to_pem(), 0o600)?;
                key
            }
            Err(e) => return Err(refused(e.to_string())),
        };
        Ok(Self {
            id: key.spki().mesh_node_id(),
            key,
        })
    }

    pub(crate) const fn id(&self) -> RaftNodeId {
        self.id
    }

    #[cfg(feature = "mesh-quic")]
    pub(crate) fn into_key(self) -> KeyMaterial {
        self.key
    }
}

pub(crate) fn identity(boot: &BootConfig) -> Result<Option<NodeIdentity>, RuntimeError> {
    boot.membership
        .shared()
        .map(|shared| NodeIdentity::load_or_mint(&shared.identity_key))
        .transpose()
}

pub(crate) fn cluster_ca(boot: &BootConfig) -> Result<ClusterCa, PkiError> {
    match &boot.membership {
        Membership::Own => load_or_generate_ca(&boot.data_dir),
        Membership::Shared(shared) => {
            load_shared_ca(&shared.ca_cert, &shared.ca_key, &boot.data_dir)
        }
    }
}

pub(crate) fn ca_dir(boot: &BootConfig) -> PathBuf {
    match &boot.membership {
        Membership::Own => engenho_apiserver::PkiFile::dir(&boot.data_dir),
        Membership::Shared(shared) => shared
            .ca_key
            .parent()
            .map_or_else(|| shared.ca_key.clone(), Path::to_path_buf),
    }
}

#[cfg(feature = "mesh-quic")]
pub(crate) struct BoundMesh {
    pub(crate) router: engenho_store::QuicRouter,
    pub(crate) listen: String,
    pub(crate) socket: std::net::SocketAddr,
}

#[cfg(feature = "mesh-quic")]
pub(crate) fn bind_mesh(
    boot: &BootConfig,
    bind: std::net::SocketAddr,
    identity: NodeIdentity,
) -> Result<BoundMesh, RuntimeError> {
    use std::sync::Arc;

    use engenho_store::{JoinGate, JoinKey, MeshAddr, PeerTable, QuicRouter};

    let mesh = |reason: String| RuntimeError::Mesh { reason };
    let shared = boot
        .membership
        .shared()
        .ok_or(crate::Unhonoured::MeshWithoutIdentity)?;
    let ca_key_pem = std::fs::read(&shared.ca_key).map_err(|e| {
        mesh(format!(
            "read the cluster CA key {}: {e}",
            shared.ca_key.display()
        ))
    })?;
    let gate = JoinGate {
        cluster: boot.cluster_name.clone(),
        key: JoinKey::derive(&ca_key_pem),
    };
    let table = PeerTable::open(shared.identity_key.with_file_name(PeerTable::FILE))
        .map_err(|e| mesh(e.to_string()))?;
    let router = QuicRouter::bind_joinable(bind, identity.into_key(), Arc::new(table), gate)
        .map_err(|e| mesh(e.to_string()))?;
    let bound = router.local_addr().map_err(|e| mesh(e.to_string()))?;
    let socket = advertised(bound, &boot.advertise_address).ok_or_else(|| {
        mesh(format!(
            "the mesh binds {bound}, which peers cannot dial; bind a concrete address or set \
             runtime.advertise_address"
        ))
    })?;
    let listen = MeshAddr::pinned(socket, router.spki()).to_string();
    Ok(BoundMesh {
        router,
        listen,
        socket,
    })
}

#[cfg(feature = "mesh-quic")]
pub(crate) async fn join(
    boot: &BootConfig,
    store: &engenho_store::StoreMesh,
    router: &engenho_store::QuicRouter,
    seeds: &[std::net::SocketAddr],
    me: std::net::SocketAddr,
) -> Result<(), RuntimeError> {
    let within = std::time::Duration::from_secs(u64::from(boot.leadership_timeout_seconds));
    let peers = router
        .join(seeds, me, within)
        .await
        .map_err(|e| RuntimeError::Mesh {
            reason: e.to_string(),
        })?;
    tracing::info!(
        node_id = store.node_id(),
        members = peers.len(),
        "admitted as a learner"
    );
    let deadline = tokio::time::Instant::now() + within;
    let mut metrics = store.metrics();
    while !store.is_member(store.node_id()).await {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero()
            || !matches!(
                tokio::time::timeout(left, metrics.changed()).await,
                Ok(Ok(()))
            )
        {
            return Err(RuntimeError::Mesh {
                reason: format!(
                    "admitted, and no membership naming node {} replicated within {within:?}",
                    store.node_id()
                ),
            });
        }
    }
    Ok(())
}

#[cfg(feature = "mesh-quic")]
fn advertised(bound: std::net::SocketAddr, advertise: &str) -> Option<std::net::SocketAddr> {
    if !bound.ip().is_unspecified() {
        return Some(bound);
    }
    let host = advertise
        .parse::<std::net::SocketAddr>()
        .map(|s| s.ip())
        .or_else(|_| advertise.parse::<std::net::IpAddr>())
        .ok()?;
    Some(std::net::SocketAddr::new(host, bound.port()))
}

#[cfg(all(test, feature = "mesh-quic"))]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use engenho_config::{EngenhoConfig, Fabric, SharedCluster, TieredConfig as _};
    use engenho_store::{MeshAddr, Reason, ResourceCommand, ResourceKey};

    use crate::boot::BootKind;
    use crate::boot_config::BootConfig;

    fn boot(data_dir: &Path, ca: &Path, seeds: Vec<std::net::SocketAddr>) -> BootConfig {
        let mut config = EngenhoConfig::prescribed_default();
        let pki = engenho_apiserver::PkiFile::dir(ca);
        config.cluster.shared = Some(SharedCluster {
            name: "aldeia-join".into(),
            ca_cert_path: pki.join("ca.crt"),
            ca_key_path: pki.join("ca.key"),
            identity_key_path: data_dir.join("mesh").join("node.key"),
            seeds,
        });
        config.fabric = Fabric::Quic {
            bind: "127.0.0.1:0".parse().unwrap(),
        };
        config.runtime.data_dir = data_dir.to_path_buf();
        config.runtime.durable = true;
        config.runtime.leadership_timeout_seconds = 10;
        BootConfig::read(&config).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_booted_with_seeds_joins_the_formed_store_as_a_learner_and_replicates() {
        let (ca, one, two) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        engenho_apiserver::load_or_generate_ca(ca.path()).unwrap();
        let (first, kind) = crate::runtime::boot_store(&boot(one.path(), ca.path(), Vec::new()))
            .await
            .unwrap();
        assert_eq!(kind, BootKind::FirstBoot);
        assert!(first.wait_for_leadership(Duration::from_secs(10)).await);
        let seed: MeshAddr = first.listen_addr().parse().unwrap();

        let (second, kind) =
            crate::runtime::boot_store(&boot(two.path(), ca.path(), vec![seed.socket]))
                .await
                .unwrap();
        assert_eq!(kind, BootKind::FirstBoot);
        let members = first.members().await;
        assert!(
            members.contains(&(second.node_id(), second.listen_addr().to_owned(), false)),
            "{members:?}"
        );

        let key = ResourceKey::namespaced("", "v1", "ConfigMap", "default", "joined");
        first
            .propose(ResourceCommand::put(
                key.clone(),
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "joined", "namespace": "default"}}),
                Reason::Operator,
            ))
            .await
            .unwrap();
        let at = first.current_revision().await;
        assert!(second.wait_for_revision(at, Duration::from_secs(10)).await);
        assert!(second.get(&key).await.is_some());
        assert!(!second.is_leader().await);

        for store in [second, first] {
            std::sync::Arc::try_unwrap(store)
                .ok()
                .unwrap()
                .terminate()
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn seeds_without_the_quic_fabric_are_refused() {
        let mut config = EngenhoConfig::prescribed_default();
        config.cluster.shared = Some(SharedCluster {
            name: "aldeia-join".into(),
            ca_cert_path: "/ca.crt".into(),
            ca_key_path: "/ca.key".into(),
            identity_key_path: "/node.key".into(),
            seeds: vec!["127.0.0.1:7946".parse().unwrap()],
        });
        assert_eq!(
            BootConfig::read(&config).map(|_| ()).unwrap_err(),
            crate::Unhonoured::SeedsWithoutMesh
        );
    }
}
