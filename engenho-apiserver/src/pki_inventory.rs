//! What is in `data_dir/pki/`, read without changing anything.
//!
//! [`crate::load_or_generate_ca`] is the boot's path: it CREATES a CA and a
//! seed when they are absent. The control plane must be able to ask "is
//! there a CA, and what is it?" of a stopped or failed daemon without that
//! question minting one, so [`inventory`] only ever reads: every fact is a
//! state — absent, present (with what can be said without revealing a
//! secret), or unreadable (with why) — and no file is written.
//!
//! Secrets are reported by presence, size and mode, never by value.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use rcgen::{CertificateParams, DnType, KeyPair};
use sha2::{Digest, Sha256};

use crate::pki::ca_key_is_publicly_derivable;

/// A moment, in seconds since the Unix epoch (UTC).
pub type UnixSeconds = i64;

/// The cluster CA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaFact {
    /// No `ca.crt`.
    Absent,
    /// A CA certificate, and what its key says about it.
    Present {
        /// SHA-256 of the certificate's DER, lowercase hex.
        sha256: String,
        /// Validity start.
        not_before: UnixSeconds,
        /// Validity end.
        not_after: UnixSeconds,
        /// Its key is the one every pre-seed engenho derived from public
        /// source.
        publicly_derivable: bool,
    },
    /// Present but not readable as a CA (or its key is missing).
    Unreadable(String),
}

/// A secret file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFact {
    /// Not there.
    Absent,
    /// There; its size and permission bits.
    Present {
        /// Its size.
        bytes: u64,
        /// Its permission bits.
        mode: u32,
    },
    /// There, but it could not be examined.
    Unreadable(String),
}

/// The persisted admin client certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertFact {
    /// No `admin.crt`.
    Absent,
    /// Its subject and validity.
    Present {
        /// Subject CN.
        common_name: String,
        /// Subject O values (the Kubernetes groups).
        organizations: Vec<String>,
        /// Validity start.
        not_before: UnixSeconds,
        /// Validity end.
        not_after: UnixSeconds,
    },
    /// There, but not readable as a certificate.
    Unreadable(String),
}

/// Everything under `data_dir/pki/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkiInventory {
    /// `ca.crt` (+ `ca.key`).
    pub ca: CaFact,
    /// `cluster-seed`.
    pub cluster_seed: FileFact,
    /// `sa.key`.
    pub sa_key: FileFact,
    /// `admin.token`.
    pub admin_token: FileFact,
    /// `admin.crt`.
    pub admin_cert: CertFact,
}

/// Read `data_dir/pki/`. Never writes.
#[must_use]
pub fn inventory(data_dir: &Path) -> PkiInventory {
    let pki = data_dir.join("pki");
    PkiInventory {
        ca: ca_fact(&pki),
        cluster_seed: file_fact(&pki.join("cluster-seed")),
        sa_key: file_fact(&pki.join("sa.key")),
        admin_token: file_fact(&pki.join("admin.token")),
        admin_cert: cert_fact(&pki.join("admin.crt")),
    }
}

fn file_fact(path: &Path) -> FileFact {
    match std::fs::metadata(path) {
        Ok(meta) => FileFact::Present {
            bytes: meta.len(),
            mode: meta.permissions().mode() & 0o7777,
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => FileFact::Absent,
        Err(err) => FileFact::Unreadable(err.to_string()),
    }
}

/// The first certificate in a PEM file, as DER.
fn first_cert_der(path: &Path) -> Result<Option<Vec<u8>>, String> {
    let text = match std::fs::read(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.to_string()),
    };
    let mut reader = std::io::BufReader::new(text.as_slice());
    match rustls_pemfile::certs(&mut reader).next() {
        Some(Ok(der)) => Ok(Some(der.to_vec())),
        Some(Err(err)) => Err(format!("not PEM: {err}")),
        None => Err("no certificate in the file".into()),
    }
}

fn validity(params: &CertificateParams) -> (UnixSeconds, UnixSeconds) {
    (
        params.not_before.unix_timestamp(),
        params.not_after.unix_timestamp(),
    )
}

fn ca_fact(pki: &Path) -> CaFact {
    let der = match first_cert_der(&pki.join("ca.crt")) {
        Ok(Some(der)) => der,
        Ok(None) => return CaFact::Absent,
        Err(err) => return CaFact::Unreadable(format!("ca.crt: {err}")),
    };
    let params = match CertificateParams::from_ca_cert_der(&der.clone().into()) {
        Ok(params) => params,
        Err(err) => return CaFact::Unreadable(format!("ca.crt: {err}")),
    };
    let key_pem = match std::fs::read_to_string(pki.join("ca.key")) {
        Ok(pem) => pem,
        Err(err) => return CaFact::Unreadable(format!("ca.key: {err}")),
    };
    let key = match KeyPair::from_pem(&key_pem) {
        Ok(key) => key,
        Err(err) => return CaFact::Unreadable(format!("ca.key: {err}")),
    };
    let (not_before, not_after) = validity(&params);
    CaFact::Present {
        sha256: hex(&Sha256::digest(&der)),
        not_before,
        not_after,
        publicly_derivable: ca_key_is_publicly_derivable(&key.serialize_pem()),
    }
}

fn cert_fact(path: &Path) -> CertFact {
    let der = match first_cert_der(path) {
        Ok(Some(der)) => der,
        Ok(None) => return CertFact::Absent,
        Err(err) => return CertFact::Unreadable(err),
    };
    // rcgen reads a leaf's subject through its CA parser (it does not
    // require BasicConstraints), as `parse_client_cert` does.
    let params = match CertificateParams::from_ca_cert_der(&der.into()) {
        Ok(params) => params,
        Err(err) => return CertFact::Unreadable(err.to_string()),
    };
    let dn = &params.distinguished_name;
    let text = |t: &DnType| {
        dn.iter()
            .filter(|(ty, _)| *ty == t)
            .filter_map(|(_, v)| crate::pki::dn_value_str(v))
            .collect::<Vec<_>>()
    };
    let (not_before, not_after) = validity(&params);
    CertFact::Present {
        common_name: text(&DnType::CommonName)
            .into_iter()
            .next()
            .unwrap_or_default(),
        organizations: text(&DnType::OrganizationName),
        not_before,
        not_after,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_data_dir_has_nothing_and_stays_empty() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let inv = inventory(tmp.path());
        assert_eq!(inv.ca, CaFact::Absent);
        assert_eq!(inv.cluster_seed, FileFact::Absent);
        assert_eq!(inv.admin_cert, CertFact::Absent);
        assert!(!tmp.path().join("pki").exists(), "reading created nothing");
    }

    #[test]
    fn a_booted_pki_is_described_without_revealing_it() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ca = crate::load_or_generate_ca(tmp.path()).expect("ca");
        let admin = crate::issue_admin_client_material(&ca).expect("admin");
        std::fs::write(tmp.path().join("pki/admin.crt"), &admin.cert_pem).expect("write");
        let inv = inventory(tmp.path());
        let CaFact::Present {
            sha256,
            not_before,
            not_after,
            publicly_derivable,
        } = inv.ca
        else {
            panic!("{:?}", inv.ca);
        };
        assert_eq!(sha256.len(), 64);
        assert!(not_before < not_after);
        assert!(!publicly_derivable, "a seeded CA is private");
        assert!(matches!(
            inv.cluster_seed,
            FileFact::Present {
                bytes: 32,
                mode: 0o600
            }
        ));
        let CertFact::Present {
            common_name,
            organizations,
            ..
        } = inv.admin_cert
        else {
            panic!("{:?}", inv.admin_cert);
        };
        assert_eq!(common_name, "engenho-admin");
        assert_eq!(organizations, ["system:masters"]);
    }

    #[test]
    fn a_ca_without_its_key_is_unreadable_not_absent() {
        let tmp = tempfile::tempdir().expect("tempdir");
        crate::load_or_generate_ca(tmp.path()).expect("ca");
        std::fs::remove_file(tmp.path().join("pki/ca.key")).expect("remove");
        assert!(matches!(inventory(tmp.path()).ca, CaFact::Unreadable(_)));
    }
}
