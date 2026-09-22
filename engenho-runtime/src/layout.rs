//! The data directory's layout: every top-level area engenho keeps under
//! `runtime.data_dir`, named once.
//!
//! What reads or writes an area names it through [`Area`], so the control
//! plane's `init` view, the store's boot, the volume materializers and a
//! re-initialization that moves an area aside cannot disagree about where it
//! is. What a wipe may touch is a row here ([`Area::node_local`]), not a list
//! written beside the wipe.

use std::path::{Path, PathBuf};

engenho_controllers::closed_enum! {
    /// One top-level area of the data directory.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Area {
        /// The durable store.
        Store,
        /// The cluster PKI: seed, CA, admin credentials, the `ServiceAccount`
        /// signing key.
        Pki,
        /// Volumes the local-path provisioner created.
        LocalPath,
        /// Pod volumes the kubelet materialized (configMaps, secrets,
        /// emptyDirs).
        Volumes,
        /// CSI plugin sockets and their registration.
        Plugins,
        /// The kubelet's per-pod state.
        Pods,
        /// Volume snapshots.
        Snapshots,
        /// The control plane's own state: the journal, the overrides, the
        /// audit chain, the control identity. Never wiped.
        Control,
    }
}

impl Area {
    /// Its directory, relative to the data directory.
    #[must_use]
    pub const fn dir(self) -> &'static str {
        match self {
            Self::Store => "store",
            // The apiserver owns the PKI's files, and names their directory.
            Self::Pki => engenho_apiserver::PkiFile::DIR,
            Self::LocalPath => "local-path",
            Self::Volumes => "volumes",
            Self::Plugins => "plugins",
            Self::Pods => "pods",
            Self::Snapshots => "snapshots",
            Self::Control => "control",
        }
    }

    /// Where it is under `data_dir`.
    #[must_use]
    pub fn path(self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.dir())
    }

    /// Whether it holds what workloads left on this node — what a wipe of
    /// the store "and node-local state" moves aside with it. The store, the
    /// PKI and the control plane's state are not node-local in this sense:
    /// the first two have their own re-initialization, the last is never
    /// wiped.
    #[must_use]
    pub const fn node_local(self) -> bool {
        match self {
            Self::LocalPath | Self::Volumes | Self::Plugins | Self::Pods | Self::Snapshots => true,
            Self::Store | Self::Pki | Self::Control => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_area_has_its_own_directory() {
        let dirs: BTreeSet<&str> = Area::ALL.iter().map(|a| a.dir()).collect();
        assert_eq!(dirs.len(), Area::ALL.len(), "two areas share a directory");
        assert!(Area::ALL.iter().all(|a| !a.dir().contains('/')));
    }

    /// The configuration crate cannot depend on the apiserver, so it spells
    /// the CA's default paths itself; this holds the two spellings together.
    #[test]
    fn the_configured_default_ca_paths_are_the_pki_files() {
        use engenho_config::TieredConfig as _;
        let config = engenho_config::EngenhoConfig::prescribed_default();
        let data_dir = &config.runtime.data_dir;
        assert_eq!(
            config.runtime.ca_cert_path(),
            engenho_apiserver::PkiFile::CaCert.path(data_dir)
        );
        assert_eq!(
            config.runtime.ca_key_path(),
            engenho_apiserver::PkiFile::CaKey.path(data_dir)
        );
    }

    #[test]
    fn neither_the_control_state_nor_the_pki_is_node_local() {
        assert!(!Area::Control.node_local());
        assert!(!Area::Pki.node_local());
        assert!(!Area::Store.node_local());
        assert_eq!(
            Area::ALL.iter().filter(|a| a.node_local()).count(),
            5,
            "local-path, volumes, plugins, pods, snapshots"
        );
    }
}
