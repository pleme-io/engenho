//! The files under `data_dir/pki/`, named once.
//!
//! Every reader and writer of the cluster's PKI on disk — the CA and seed
//! loaders, the `ServiceAccount` key, the admin credentials, the inventory the
//! control plane reports, and a re-initialization that moves them aside —
//! names a file through [`PkiFile`], so none of them can look for a file
//! under a name another one does not write.

use std::path::{Path, PathBuf};

/// One file of the cluster PKI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PkiFile {
    /// The random seed every derived key comes from.
    ClusterSeed,
    /// The CA certificate.
    CaCert,
    /// The CA key.
    CaKey,
    /// The admin client certificate.
    AdminCert,
    /// The admin client key.
    AdminKey,
    /// The bootstrap admin bearer token.
    AdminToken,
    /// The `ServiceAccount` token signing key.
    SaKey,
}

impl PkiFile {
    /// The directory, under the data directory.
    pub const DIR: &'static str = "pki";

    /// Every file.
    pub const ALL: [Self; 7] = [
        Self::ClusterSeed,
        Self::CaCert,
        Self::CaKey,
        Self::AdminCert,
        Self::AdminKey,
        Self::AdminToken,
        Self::SaKey,
    ];

    /// Its file name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ClusterSeed => "cluster-seed",
            Self::CaCert => "ca.crt",
            Self::CaKey => "ca.key",
            Self::AdminCert => "admin.crt",
            Self::AdminKey => "admin.key",
            Self::AdminToken => "admin.token",
            Self::SaKey => "sa.key",
        }
    }

    /// The PKI directory under `data_dir`.
    #[must_use]
    pub fn dir(data_dir: &Path) -> PathBuf {
        data_dir.join(Self::DIR)
    }

    /// Where it is under `data_dir`.
    #[must_use]
    pub fn path(self, data_dir: &Path) -> PathBuf {
        Self::dir(data_dir).join(self.name())
    }

    /// Whether the cluster seed decides it: the CA and the admin credential
    /// are derived from the seed, so they are replaced with it. The token and
    /// the `ServiceAccount` key are random on their own.
    #[must_use]
    pub const fn seed_derived(self) -> bool {
        match self {
            Self::ClusterSeed | Self::CaCert | Self::CaKey | Self::AdminCert | Self::AdminKey => {
                true
            }
            Self::AdminToken | Self::SaKey => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_file_has_its_own_name() {
        let names: BTreeSet<&str> = PkiFile::ALL.iter().map(|f| f.name()).collect();
        assert_eq!(names.len(), PkiFile::ALL.len());
        assert_eq!(
            PkiFile::CaCert.path(Path::new("/d")),
            Path::new("/d/pki/ca.crt")
        );
    }

    #[test]
    fn the_seed_decides_the_ca_and_the_admin_credential_only() {
        let derived: Vec<PkiFile> = PkiFile::ALL
            .iter()
            .copied()
            .filter(|f| f.seed_derived())
            .collect();
        assert_eq!(
            derived,
            [
                PkiFile::ClusterSeed,
                PkiFile::CaCert,
                PkiFile::CaKey,
                PkiFile::AdminCert,
                PkiFile::AdminKey
            ]
        );
    }
}
