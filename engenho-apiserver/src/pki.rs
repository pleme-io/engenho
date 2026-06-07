//! Cluster PKI — the typed surface that gives engenho a stable,
//! self-generated TLS identity so real kubectl trusts it over HTTPS.
//!
//! Two artifacts, two lifetimes:
//!
//!   * **The cluster CA** (`ca.crt` + `ca.key`). Generated ONCE, PERSISTED
//!     under `data_dir/pki/`, reloaded on every subsequent boot. Stable
//!     across restarts is the whole point — an already-distributed
//!     kubeconfig embeds this CA's cert in `certificate-authority-data`
//!     and keeps trusting the server because the CA's keypair (and thus
//!     the certs it signs) never changes. That is why the CA lives in
//!     `data_dir` (the durable root), NOT a tempdir.
//!
//!   * **The apiserver server cert.** Issued FROM the CA on EVERY boot
//!     (cheap; it need not persist, only the CA must). `CN=engenho-apiserver`,
//!     EKU `serverAuth`, with SANs covering `localhost` / `kubernetes` /
//!     `kubernetes.default` / the node name / `127.0.0.1` / the configured
//!     listen IP. kubectl validates the presented server cert against the
//!     SAN list, so a missing `127.0.0.1` SAN = handshake failure for a
//!     loopback kubeconfig.
//!
//! NO SHELL — pure rcgen, no `openssl` subprocess. Errors are typed
//! [`PkiError`]; nothing here panics or returns a placeholder Ok.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};

/// Everything that can go wrong building or loading the cluster PKI.
#[derive(Debug, thiserror::Error)]
pub enum PkiError {
    /// A filesystem operation (create dir, read/write PEM, chmod) failed.
    #[error("pki io error at {path}: {source}")]
    Io {
        /// The path the operation targeted.
        path: PathBuf,
        /// The underlying io error.
        #[source]
        source: std::io::Error,
    },

    /// rcgen failed to generate, parse, or sign a certificate / key.
    #[error("pki crypto error: {0}")]
    Crypto(#[source] rcgen::Error),

    /// A SAN entry (DNS name) was not a valid IA5 string.
    #[error("invalid SAN {value:?}: {source}")]
    San {
        /// The offending SAN value.
        value: String,
        /// The rcgen parse error.
        #[source]
        source: rcgen::Error,
    },
}

impl From<rcgen::Error> for PkiError {
    fn from(e: rcgen::Error) -> Self {
        Self::Crypto(e)
    }
}

/// ~10-year CA validity in days. The CA is the long-lived root that an
/// operator's kubeconfig pins; a decade keeps it from silently expiring
/// under a local single-node engenho.
const CA_VALIDITY_DAYS: i64 = 3650;
/// Server-cert validity. Re-issued every boot, so a year is generous.
const SERVER_VALIDITY_DAYS: i64 = 365;

/// The CA the cluster signs server certs with. Holds the parsed params
/// (DN + key-usages, needed to act as an issuer) + the keypair (the
/// signing material) + the canonical `ca.crt` PEM bytes (exactly what a
/// kubeconfig embeds in `certificate-authority-data`).
pub struct ClusterCa {
    params: CertificateParams,
    key: KeyPair,
    /// The persisted CA certificate PEM — the bytes that go into a
    /// kubeconfig's `certificate-authority-data`.
    ca_cert_pem: String,
}

impl ClusterCa {
    /// The CA certificate PEM bytes (for kubeconfig embedding + tests).
    #[must_use]
    pub fn cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// Reconstruct an issuer [`rcgen::Certificate`] from the CA params +
    /// key. `signed_by` only reads the issuer's DN / key-identifier /
    /// key-usages from this cert (never its serialized bytes) and signs
    /// with the keypair — so a freshly self-signed issuer cert from the
    /// SAME params + key produces certs that chain to the persisted CA
    /// (identical public key, identical subject DN).
    fn issuer(&self) -> Result<rcgen::Certificate, PkiError> {
        Ok(self.params.clone().self_signed(&self.key)?)
    }
}

/// The TLS material the server boots with: the server-cert chain (leaf +
/// CA) PEM and the server private key PEM, plus the CA cert PEM for
/// kubeconfig emission. `from_pem` for axum-server's `RustlsConfig`
/// consumes `cert_chain_pem` + `key_pem`.
pub struct TlsMaterial {
    /// Server leaf cert followed by the CA cert, PEM-concatenated — the
    /// chain rustls presents at handshake.
    pub cert_chain_pem: String,
    /// Server private key PEM.
    pub key_pem: String,
    /// The cluster CA cert PEM (== the kubeconfig's
    /// `certificate-authority-data`, base64'd at emit time).
    pub ca_cert_pem: String,
}

/// SAN inputs for the server cert. The listen IP is `Some` only when the
/// configured `listen_addr` parsed to a concrete, non-unspecified IP
/// (`0.0.0.0` / `::` are NOT valid SAN IPs — loopback access then rides
/// on the always-present `127.0.0.1` + `localhost` SANs).
pub struct ServerSanInputs<'a> {
    /// This node's name (a DNS SAN so `server: https://<node>` works).
    pub node_name: &'a str,
    /// The concrete listen IP, if `listen_addr` resolved to one.
    pub listen_ip: Option<IpAddr>,
}

/// Generate-or-load the cluster CA rooted at `data_dir/pki/`.
///
/// If BOTH `ca.crt` and `ca.key` exist under `data_dir/pki/`, they are
/// loaded + parsed (restart-stable path). Otherwise a fresh self-signed
/// CA is generated (`CN=engenho-ca`, `IsCa::Ca(Unconstrained)`,
/// keyCertSign+crlSign, ~10y) and PERSISTED with `ca.key` mode 0600 and
/// the `pki` dir mode 0700.
///
/// # Errors
///
/// [`PkiError::Io`] on any filesystem failure; [`PkiError::Crypto`] if
/// rcgen can't generate / parse / serialize the CA.
pub fn load_or_generate_ca(data_dir: &Path) -> Result<ClusterCa, PkiError> {
    let pki_dir = data_dir.join("pki");
    let ca_cert_path = pki_dir.join("ca.crt");
    let ca_key_path = pki_dir.join("ca.key");

    if ca_cert_path.exists() && ca_key_path.exists() {
        load_ca(&ca_cert_path, &ca_key_path)
    } else {
        generate_and_persist_ca(&pki_dir, &ca_cert_path, &ca_key_path)
    }
}

/// Load an existing CA from its persisted `ca.crt` + `ca.key` PEM.
fn load_ca(ca_cert_path: &Path, ca_key_path: &Path) -> Result<ClusterCa, PkiError> {
    let ca_cert_pem = read_to_string(ca_cert_path)?;
    let ca_key_pem = read_to_string(ca_key_path)?;
    let key = KeyPair::from_pem(&ca_key_pem)?;
    let params = CertificateParams::from_ca_cert_pem(&ca_cert_pem)?;
    Ok(ClusterCa {
        params,
        key,
        ca_cert_pem,
    })
}

/// Generate a fresh self-signed CA + persist it under `pki_dir`.
fn generate_and_persist_ca(
    pki_dir: &Path,
    ca_cert_path: &Path,
    ca_key_path: &Path,
) -> Result<ClusterCa, PkiError> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "engenho-ca");
    set_validity(&mut params, CA_VALIDITY_DAYS);

    let key = KeyPair::generate()?;
    let cert = params.clone().self_signed(&key)?;
    let ca_cert_pem = cert.pem();
    let ca_key_pem = key.serialize_pem();

    // Persist: dir 0700, ca.crt 0644 (a cert is public), ca.key 0600.
    create_dir_all_mode(pki_dir, 0o700)?;
    write_pem(ca_cert_path, &ca_cert_pem, 0o644)?;
    write_pem(ca_key_path, &ca_key_pem, 0o600)?;

    Ok(ClusterCa {
        params,
        key,
        ca_cert_pem,
    })
}

/// Issue the apiserver SERVER cert from the CA + assemble the full
/// [`TlsMaterial`]. The leaf is `CN=engenho-apiserver`, EKU `serverAuth`,
/// signed by the CA. SANs: DNS `localhost`, `kubernetes`,
/// `kubernetes.default`, the node name; IP `127.0.0.1`, plus the listen
/// IP when concrete.
///
/// # Errors
///
/// [`PkiError::San`] if a DNS SAN isn't a valid IA5 string;
/// [`PkiError::Crypto`] if rcgen can't generate / sign the leaf.
pub fn issue_server_material(
    ca: &ClusterCa,
    san: &ServerSanInputs<'_>,
) -> Result<TlsMaterial, PkiError> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::NoCa;
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "engenho-apiserver");
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    set_validity(&mut params, SERVER_VALIDITY_DAYS);
    params.subject_alt_names = build_sans(san)?;

    let leaf_key = KeyPair::generate()?;
    let issuer = ca.issuer()?;
    let leaf_cert = params.signed_by(&leaf_key, &issuer, &ca.key)?;

    // rustls presents leaf-then-CA; kubectl pins the CA via
    // certificate-authority-data so the chain verifies.
    let mut cert_chain_pem = leaf_cert.pem();
    cert_chain_pem.push('\n');
    cert_chain_pem.push_str(&ca.ca_cert_pem);

    Ok(TlsMaterial {
        cert_chain_pem,
        key_pem: leaf_key.serialize_pem(),
        ca_cert_pem: ca.ca_cert_pem.clone(),
    })
}

/// Build the server cert SAN list. DNS SANs are deduped-by-construction
/// (we never push the node name twice even if it's `localhost`).
fn build_sans(san: &ServerSanInputs<'_>) -> Result<Vec<SanType>, PkiError> {
    let mut sans: Vec<SanType> = Vec::new();

    // Standard DNS SANs every K8s apiserver carries.
    let mut dns_names: Vec<&str> = vec!["localhost", "kubernetes", "kubernetes.default"];
    // The node name is a DNS SAN so `server: https://<node>:port` works;
    // skip it if it duplicates one already present (e.g. node "localhost").
    if !san.node_name.is_empty() && !dns_names.contains(&san.node_name) {
        dns_names.push(san.node_name);
    }
    for name in dns_names {
        let ia5 = name
            .try_into()
            .map_err(|source| PkiError::San {
                value: name.to_string(),
                source,
            })?;
        sans.push(SanType::DnsName(ia5));
    }

    // IP SANs: loopback is always present (carries the loopback
    // kubeconfig). The concrete listen IP is added when it isn't loopback
    // (avoids a duplicate) and isn't unspecified (filtered by the caller).
    let loopback: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    sans.push(SanType::IpAddress(loopback));
    if let Some(ip) = san.listen_ip {
        if ip != loopback {
            sans.push(SanType::IpAddress(ip));
        }
    }

    Ok(sans)
}

/// Set `not_before` = now, `not_after` = now + `days`.
fn set_validity(params: &mut CertificateParams, days: i64) {
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(days);
}

// ── filesystem helpers (typed io errors, unix perms) ───────────────────

fn read_to_string(path: &Path) -> Result<String, PkiError> {
    std::fs::read_to_string(path).map_err(|source| PkiError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn create_dir_all_mode(dir: &Path, mode: u32) -> Result<(), PkiError> {
    std::fs::create_dir_all(dir).map_err(|source| PkiError::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    set_mode(dir, mode)
}

fn write_pem(path: &Path, pem: &str, mode: u32) -> Result<(), PkiError> {
    std::fs::write(path, pem.as_bytes()).map_err(|source| PkiError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    set_mode(path, mode)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<(), PkiError> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|source| {
        PkiError::Io {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<(), PkiError> {
    // Non-unix: file modes don't apply. The CA still persists; the 0600
    // hardening is a unix-only guarantee.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca_in_tempdir() -> (tempfile::TempDir, ClusterCa) {
        let dir = tempfile::tempdir().unwrap();
        let ca = load_or_generate_ca(dir.path()).unwrap();
        (dir, ca)
    }

    #[test]
    fn generated_ca_is_a_ca_with_expected_cn() {
        let (_dir, ca) = ca_in_tempdir();
        // Round-trip the emitted ca.crt through rcgen's parser and assert
        // it parsed as a CA with the expected CN.
        let params = CertificateParams::from_ca_cert_pem(ca.cert_pem()).unwrap();
        assert!(
            matches!(params.is_ca, IsCa::Ca(_)),
            "generated CA must have BasicConstraints CA"
        );
        let cn = params
            .distinguished_name
            .get(&DnType::CommonName)
            .expect("CA has a CN");
        // DnValue Display renders the printable-string value.
        assert!(
            format!("{cn:?}").contains("engenho-ca"),
            "CA CN should be engenho-ca, got {cn:?}"
        );
    }

    #[test]
    fn generated_ca_persists_with_locked_down_key() {
        let dir = tempfile::tempdir().unwrap();
        let _ca = load_or_generate_ca(dir.path()).unwrap();
        let pki = dir.path().join("pki");
        assert!(pki.join("ca.crt").exists(), "ca.crt persisted");
        assert!(pki.join("ca.key").exists(), "ca.key persisted");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let key_mode = std::fs::metadata(pki.join("ca.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(key_mode, 0o600, "ca.key must be 0600");
            let dir_mode = std::fs::metadata(&pki).unwrap().permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700, "pki dir must be 0700");
        }
    }

    #[test]
    fn load_is_idempotent_same_ca_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_generate_ca(dir.path()).unwrap();
        let first_pem = first.cert_pem().to_string();
        // Second call MUST LOAD the persisted CA (not regenerate) →
        // byte-identical cert PEM proves restart-stability.
        let second = load_or_generate_ca(dir.path()).unwrap();
        assert_eq!(
            first_pem,
            second.cert_pem(),
            "reload must return the SAME CA (restart-stable)"
        );
    }

    #[test]
    fn fresh_tempdir_generates_a_different_ca() {
        let (_d1, ca1) = ca_in_tempdir();
        let (_d2, ca2) = ca_in_tempdir();
        assert_ne!(
            ca1.cert_pem(),
            ca2.cert_pem(),
            "independent data_dirs must mint independent CAs"
        );
    }

    #[test]
    fn preseeded_ca_is_loaded_unchanged() {
        // Pre-seed a CA in dir A, copy its pki/ into dir B, load from B →
        // the loader returns the seeded CA verbatim.
        let dir_a = tempfile::tempdir().unwrap();
        let seeded = load_or_generate_ca(dir_a.path()).unwrap();
        let seeded_pem = seeded.cert_pem().to_string();

        let dir_b = tempfile::tempdir().unwrap();
        let pki_b = dir_b.path().join("pki");
        std::fs::create_dir_all(&pki_b).unwrap();
        std::fs::copy(
            dir_a.path().join("pki/ca.crt"),
            pki_b.join("ca.crt"),
        )
        .unwrap();
        std::fs::copy(
            dir_a.path().join("pki/ca.key"),
            pki_b.join("ca.key"),
        )
        .unwrap();

        let loaded = load_or_generate_ca(dir_b.path()).unwrap();
        assert_eq!(loaded.cert_pem(), seeded_pem);
    }

    #[test]
    fn server_cert_carries_required_sans_and_eku() {
        let (_dir, ca) = ca_in_tempdir();
        let listen_ip: IpAddr = "192.168.64.10".parse().unwrap();
        let mat = issue_server_material(
            &ca,
            &ServerSanInputs {
                node_name: "engenho-node",
                listen_ip: Some(listen_ip),
            },
        )
        .unwrap();

        // Parse the leaf (first PEM block in the chain) back through rcgen.
        let leaf_pem = first_pem_block(&mat.cert_chain_pem);
        let leaf = CertificateParams::from_ca_cert_pem(&leaf_pem).unwrap();

        // EKU serverAuth.
        assert!(
            leaf.extended_key_usages
                .contains(&ExtendedKeyUsagePurpose::ServerAuth),
            "server cert must have EKU serverAuth; got {:?}",
            leaf.extended_key_usages
        );

        // SAN assertions.
        let sans = &leaf.subject_alt_names;
        assert!(
            sans.iter().any(|s| is_dns(s, "localhost")),
            "must contain DNS localhost"
        );
        assert!(
            sans.iter().any(|s| is_dns(s, "kubernetes")),
            "must contain DNS kubernetes"
        );
        assert!(
            sans.iter().any(|s| is_dns(s, "engenho-node")),
            "must contain DNS node_name"
        );
        let loopback: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        assert!(
            sans.iter().any(|s| is_ip(s, loopback)),
            "must contain IP 127.0.0.1"
        );
        assert!(
            sans.iter().any(|s| is_ip(s, listen_ip)),
            "must contain the concrete listen IP"
        );
    }

    #[test]
    fn server_chain_includes_the_ca() {
        // The chain is leaf-then-CA so a verifying client that trusts the
        // CA can build the path. cert_chain_pem ends with ca_cert_pem.
        let (_dir, ca) = ca_in_tempdir();
        let mat = issue_server_material(
            &ca,
            &ServerSanInputs {
                node_name: "n",
                listen_ip: None,
            },
        )
        .unwrap();
        assert!(
            mat.cert_chain_pem.contains(ca.cert_pem().trim()),
            "the server chain must include the CA cert"
        );
        assert_eq!(mat.ca_cert_pem, ca.cert_pem());
        // Two PEM blocks: leaf + CA.
        assert_eq!(
            mat.cert_chain_pem.matches("BEGIN CERTIFICATE").count(),
            2,
            "chain must be leaf + CA"
        );
    }

    // ── small parsing helpers for the tests above ──────────────────────

    fn first_pem_block(chain: &str) -> String {
        let begin = "-----BEGIN CERTIFICATE-----";
        let end = "-----END CERTIFICATE-----";
        let start = chain.find(begin).expect("a cert block");
        let stop = chain[start..].find(end).expect("a cert end") + start + end.len();
        chain[start..stop].to_string()
    }

    fn is_dns(s: &SanType, name: &str) -> bool {
        match s {
            SanType::DnsName(ia5) => ia5.as_str() == name,
            _ => false,
        }
    }

    fn is_ip(s: &SanType, ip: IpAddr) -> bool {
        matches!(s, SanType::IpAddress(got) if *got == ip)
    }
}
