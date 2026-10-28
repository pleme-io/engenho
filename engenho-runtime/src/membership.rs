use std::path::{Path, PathBuf};

use engenho_apiserver::{ClusterCa, PkiError, load_or_generate_ca, load_shared_ca};
use engenho_control_types::pin::KeyMaterial;
use engenho_store::RaftNodeId;

use crate::boot_config::{BootConfig, Membership};
use crate::error::RuntimeError;

pub(crate) const SINGLETON: RaftNodeId = 1;

pub(crate) struct NodeIdentity {
    #[allow(dead_code)]
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
