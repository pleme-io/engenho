//! Layer C — content-addressed workload sync via iroh / BitTorrent
//! mainline DHT.
//!
//! Pod manifests, ConfigMaps, image layers are addressed by their
//! BLAKE3 hash + served peer-to-peer. Any node that has the content
//! can serve it; workers fetch from nearest peer instead of always
//! through the central apiserver.
//!
//! R0: typed surface only. R5 wires `n0-computer/iroh`
//! (`iroh-mainline-content-discovery`) for the actual sync.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::NodeId;

engenho_substrate::define_hash_newtype! {
    #[derive(Copy, Default)]
    #[serde(transparent)]
    /// Content address — a BLAKE3 hash of canonical-form bytes (Pod
    /// manifest, ConfigMap data, image layer). Matches the existing
    /// tameshi hashing scheme.
    ContentHash
}

/// Per-content advertisement gossiped to the mainline DHT.
/// "I have content X; reach me at this NodeId."
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAdvertisement {
    pub hash: ContentHash,
    pub holder: NodeId,
    /// Optional kind hint — speeds up replication for hot kinds.
    #[serde(default)]
    pub hint: ContentKind,
}

/// Hint about what kind of content this is — purely for telemetry +
/// replication policy. The bytes ARE the content; this enum is
/// non-load-bearing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    /// Pod manifest.
    PodSpec,
    /// ConfigMap data blob.
    ConfigMapData,
    /// Helm chart tarball.
    HelmChart,
    /// OCI image layer.
    OciLayer,
    /// Unspecified content.
    #[default]
    Other,
}

/// Local view of "who has what content right now" — populated from
/// DHT queries + direct gossip. R5 implementation will own the
/// concrete cache; this type is the operator-facing query result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentLocations {
    pub hash: ContentHash,
    pub holders: BTreeSet<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_kind_default_is_other() {
        assert_eq!(ContentKind::default(), ContentKind::Other);
    }

    #[test]
    fn content_advertisement_round_trips() {
        let adv = ContentAdvertisement {
            hash: ContentHash([0xaa; 32]),
            holder: NodeId::new([0xbb; 32]),
            hint: ContentKind::PodSpec,
        };
        let json = serde_json::to_string(&adv).unwrap();
        assert!(json.contains("\"pod_spec\""));
        let back: ContentAdvertisement = serde_json::from_str(&json).unwrap();
        assert_eq!(back, adv);
    }

    #[test]
    fn content_hash_hex_is_64_chars() {
        let h = ContentHash([0; 32]);
        assert_eq!(h.to_hex().len(), 64);
    }
}
