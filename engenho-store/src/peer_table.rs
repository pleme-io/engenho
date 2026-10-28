use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use engenho_control_types::pin::Spki;
use serde::{Deserialize, Serialize};

use crate::type_config::RaftNodeId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub node_id: RaftNodeId,
    pub address: SocketAddr,
    pub spki: Spki,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerTableError {
    #[error("read the peer table {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("the peer table {path} is not a peer table: {source}")]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("write the peer table {path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnDisk {
    peers: Vec<Peer>,
}

#[derive(Debug)]
pub struct PeerTable {
    path: PathBuf,
    peers: Mutex<BTreeMap<RaftNodeId, Peer>>,
}

impl PeerTable {
    pub const FILE: &'static str = "peers.json";

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, PeerTableError> {
        let path = path.into();
        let peers = match std::fs::read(&path) {
            Ok(bytes) => {
                let OnDisk { peers } =
                    serde_json::from_slice(&bytes).map_err(|source| PeerTableError::Parse {
                        path: path.clone(),
                        source,
                    })?;
                peers.into_iter().map(|p| (p.node_id, p)).collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(source) => return Err(PeerTableError::Read { path, source }),
        };
        Ok(Self {
            path,
            peers: Mutex::new(peers),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn peers(&self) -> Vec<Peer> {
        self.lock().values().copied().collect()
    }

    pub fn get(&self, node: RaftNodeId) -> Option<Peer> {
        self.lock().get(&node).copied()
    }

    pub fn record(&self, peer: Peer) -> Result<bool, PeerTableError> {
        let mut peers = self.lock();
        if peers.get(&peer.node_id) == Some(&peer) {
            return Ok(false);
        }
        let mut next = peers.clone();
        next.insert(peer.node_id, peer);
        self.persist(&next)?;
        *peers = next;
        Ok(true)
    }

    fn persist(&self, peers: &BTreeMap<RaftNodeId, Peer>) -> Result<(), PeerTableError> {
        let failed = |source| PeerTableError::Write {
            path: self.path.clone(),
            source,
        };
        let body = serde_json::to_vec_pretty(&OnDisk {
            peers: peers.values().copied().collect(),
        })
        .map_err(|e| failed(std::io::Error::other(e)))?;
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(failed)?;
        }
        let staged = self.path.with_extension("json.next");
        std::fs::write(&staged, body).map_err(failed)?;
        std::fs::rename(&staged, &self.path).map_err(failed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<RaftNodeId, Peer>> {
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use engenho_control_types::pin::KeyMaterial;

    use super::*;

    fn peer(id: RaftNodeId, port: u16) -> Peer {
        Peer {
            node_id: id,
            address: ([127, 0, 0, 1], port).into(),
            spki: KeyMaterial::generate().unwrap().spki(),
        }
    }

    #[test]
    fn a_missing_table_is_empty_and_a_record_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mesh").join(PeerTable::FILE);
        let table = PeerTable::open(&path).unwrap();
        assert_eq!(table.peers(), Vec::new());
        assert!(!path.exists(), "opening writes nothing");
        let (one, two) = (peer(7, 7001), peer(9, 7002));
        assert!(table.record(one).unwrap());
        assert!(table.record(two).unwrap());
        assert!(!table.record(one).unwrap());
        let moved = Peer {
            address: ([127, 0, 0, 1], 7003).into(),
            ..one
        };
        assert!(table.record(moved).unwrap());
        let reopened = PeerTable::open(&path).unwrap();
        assert_eq!(reopened.peers(), vec![moved, two]);
        assert_eq!(reopened.get(9), Some(two));
    }

    #[test]
    fn a_table_that_does_not_parse_is_refused_not_read_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(PeerTable::FILE);
        for body in [
            "not json",
            r#"{"peers":[{"node_id":1}]}"#,
            r#"{"peers":[],"voters":3}"#,
        ] {
            std::fs::write(&path, body).unwrap();
            assert!(
                matches!(PeerTable::open(&path), Err(PeerTableError::Parse { .. })),
                "{body}"
            );
        }
    }
}
