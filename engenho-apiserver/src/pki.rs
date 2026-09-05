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
//!     `kubernetes.default` / the container host gateway / the node name /
//!     `127.0.0.1` / the configured listen IP, PLUS whatever
//!     `runtime.tls.extra_sans` declares. kubectl validates the presented
//!     server cert against the SAN list, so a missing `127.0.0.1` SAN =
//!     handshake failure for a loopback kubeconfig.
//!
//!     The derived half answers for the NODE; the declared half answers for
//!     the names the cluster is REACHED BY — a tailnet address, a LAN alias,
//!     a VIP — which are facts about the deployment rather than the process.
//!     Note that binding `0.0.0.0` contributes NO listen-IP SAN (an
//!     unspecified address is not a valid SAN), so on a node reached by
//!     address the declared list is the only thing naming it.
//!
//! NO SHELL — pure rcgen, no `openssl` subprocess. Errors are typed
//! [`PkiError`]; nothing here panics or returns a placeholder Ok.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, DnValue,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;

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
    Crypto(#[from] rcgen::Error),

    /// A SAN entry (DNS name) was not a valid IA5 string.
    #[error("invalid SAN {value:?}: {source}")]
    San {
        /// The offending SAN value.
        value: String,
        /// The rcgen parse error.
        #[source]
        source: rcgen::Error,
    },

    /// Building the OPTIONAL client-cert verifier failed — the CA cert PEM
    /// could not be parsed into a [`rustls::RootCertStore`], or the
    /// [`WebPkiClientVerifier`] builder rejected the roots. Typed, never a
    /// silent insecure fall-through to no-client-auth.
    #[error("client verifier build error: {0}")]
    ClientVerifier(String),

    /// Deriving the deterministic ed25519 key from its BLAKE3 seed failed at
    /// the PKCS#8 encode step. Should never happen for a valid 32-byte seed.
    #[error("deterministic key derivation error: {0}")]
    DeterministicKey(#[from] ed25519_dalek::pkcs8::Error),
}

engenho_substrate::impl_error_kind! {
    PkiError {
        { Io { .. } } => "io",
        (Crypto(_)) => "crypto",
        { San { .. } } => "san",
        (ClientVerifier(_)) => "client_verifier",
        (DeterministicKey(_)) => "deterministic_key",
    }
}

/// Fixed per-role BLAKE3 derivation contexts — distinct so the CA, server,
/// and admin-client keys are independent yet each fully deterministic.
const CTX_CA: &str = "engenho cluster-ca v1";
const CTX_SERVER: &str = "engenho apiserver-cert v1";
const CTX_CLIENT: &str = "engenho admin-client-cert v1";

/// The base key material every role-seed is derived from. For a LOCAL
/// single-node engenho this is a fixed constant, so a fresh cluster's CA /
/// kubeconfig is byte-identical (the operator's "a new engenho can't result in
/// a different kubectl configuration"). An EXPOSED cluster MUST mix in a
/// per-cluster secret here (a cofre value) so the admin key isn't derivable
/// from public knowledge — tracked as a follow-up.
const PKI_BASE: &[u8] = b"engenho deterministic cluster identity v1";

/// A deterministic ed25519 [`KeyPair`] for a PKI role. The seed is
/// `BLAKE3::derive_key(ctx, PKI_BASE)`, so the same role always yields the same
/// key — hence a byte-identical CA / server / client cert (and kubeconfig)
/// across fresh boots, replacing rcgen's random `KeyPair::generate`.
fn deterministic_keypair(ctx: &str) -> Result<KeyPair, PkiError> {
    let seed = blake3::derive_key(ctx, PKI_BASE);
    let signing = SigningKey::from_bytes(&seed);
    let der = signing.to_pkcs8_der()?;
    let pkcs8 = rustls::pki_types::PrivatePkcs8KeyDer::from(der.as_bytes().to_vec());
    Ok(KeyPair::try_from(&pkcs8)?)
}

/// ~10-year CA validity in days. The CA is the long-lived root that an
/// operator's kubeconfig pins; a decade keeps it from silently expiring
/// under a local single-node engenho.
const CA_VALIDITY_DAYS: i64 = 3650;
/// Server-cert validity. Re-issued every boot, so a year is generous.
const SERVER_VALIDITY_DAYS: i64 = 365;
/// Admin CLIENT-cert validity. Re-issued each boot (cheap) but persisted so
/// the operator's kubeconfig stays stable; matches the server-cert horizon.
const CLIENT_VALIDITY_DAYS: i64 = 365;

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
    /// The OPTIONAL client-cert verifier. When `Some`, the server requests a
    /// client cert (verified against the cluster CA) but still completes the
    /// handshake for a no-cert client (`allow_unauthenticated`) — so existing
    /// token/anonymous kubectl keeps connecting. When `None`, the server runs
    /// with no client auth (`with_no_client_auth`) — the pre-authn behavior,
    /// kept for the plaintext-floor + tests that don't exercise mTLS.
    pub client_verifier: Option<std::sync::Arc<dyn ClientCertVerifier>>,
}

/// The admin CLIENT-cert material: a leaf cert + private key PEM the
/// operator's kubeconfig embeds as `client-certificate-data` +
/// `client-key-data`. Leaf-only (the client need NOT present the CA — the
/// server already trusts it via [`client_verifier`]'s root store).
pub struct ClientMaterial {
    /// The client leaf cert PEM (NO CA appended — leaf only).
    pub cert_pem: String,
    /// The client private key PEM.
    pub key_pem: String,
}

/// A verified peer client certificate's identity, parsed AFTER rustls
/// completed (and verified) the TLS handshake. The X509 authenticator stage
/// turns this into a `UserInfo` (`username = common_name`, `groups =
/// organizations + system:authenticated`). Injected into each request's
/// extensions by the custom TLS acceptor; ABSENT when the client presented
/// no cert (the `allow_unauthenticated` path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClientCert {
    /// The cert's Subject Common Name (the authenticated username).
    pub common_name: String,
    /// The cert's Subject Organization values (mapped to K8s groups).
    pub organizations: Vec<String>,
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
    /// Operator-declared additional SANs, in config order.
    ///
    /// The derived set — loopback, `kubernetes`, the container host gateway,
    /// the node name, the listen IP — answers only for the node itself. It
    /// cannot answer for the names a cluster is reached BY: a tailnet name, a
    /// LAN alias, a `*.quero.cloud` record, a load-balancer VIP. Those are
    /// facts about the deployment, so the deployment declares them.
    ///
    /// Already typed by the time they arrive: [`SanEntry`] has done the
    /// IP-versus-DNS decision, so this list cannot carry a string that turns
    /// out not to be a SAN at issuance time.
    pub extra_sans: &'a [SanEntry],
}

/// One operator-declared Subject Alternative Name, after classification.
///
/// A SAN is either an IP address or a DNS name and the encodings are not
/// interchangeable: a certificate carrying `10.0.0.1` as a *DNS* SAN verifies
/// against nothing, because a client connecting to that address checks the IP
/// list. Rather than ask an operator to say which kind each entry is — a second
/// field to get wrong, and one whose right answer is always derivable — the
/// string is parsed: anything that parses as an IP address is an IP SAN, and
/// everything else is a DNS name. This matches how `kubeadm`'s `certSANs`
/// behaves, so an operator's existing intuition transfers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SanEntry {
    /// A DNS name, e.g. `plo.tail1234.ts.net`.
    Dns(String),
    /// An IP address, e.g. `100.64.0.1`.
    Ip(IpAddr),
}

impl std::fmt::Display for SanEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dns(name) => f.write_str(name),
            Self::Ip(ip) => write!(f, "{ip}"),
        }
    }
}

impl std::str::FromStr for SanEntry {
    type Err = SanParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let value = raw.trim();
        if value.is_empty() {
            return Err(SanParseError::Empty);
        }
        // The three mistakes worth naming rather than encoding verbatim into a
        // certificate nobody will think to inspect. Each produces a cert that
        // builds, serves, and silently fails to verify for the address the
        // operator believed they had covered.
        if value.contains("://") {
            return Err(SanParseError::HasScheme {
                value: value.to_string(),
            });
        }
        if value.chars().any(char::is_whitespace) {
            return Err(SanParseError::HasWhitespace {
                value: value.to_string(),
            });
        }
        if let Ok(ip) = value.parse::<IpAddr>() {
            return Ok(Self::Ip(ip));
        }
        // A bare `host:port` is the other common paste. An IPv6 literal has
        // colons legitimately, but it would have parsed as an IP above, so any
        // colon still here is a port.
        if value.contains(':') {
            return Err(SanParseError::HasPort {
                value: value.to_string(),
            });
        }
        Ok(Self::Dns(value.to_string()))
    }
}

/// Why a declared SAN string is not a SAN.
///
/// Separate from [`PkiError`] because these are rejected when configuration is
/// read, long before any certificate is issued — the point of the split is that
/// a typo in a config file never reaches cert issuance.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SanParseError {
    /// The entry was empty or whitespace only.
    #[error("a SAN entry is empty")]
    Empty,
    /// A URL was given where a name was wanted.
    #[error("SAN {value:?} looks like a URL — give the host alone, with no scheme")]
    HasScheme {
        /// The offending entry.
        value: String,
    },
    /// The entry contains whitespace.
    #[error("SAN {value:?} contains whitespace")]
    HasWhitespace {
        /// The offending entry.
        value: String,
    },
    /// A `host:port` was given where a host was wanted.
    #[error("SAN {value:?} carries a port — a certificate names hosts, not ports")]
    HasPort {
        /// The offending entry.
        value: String,
    },
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
    // Fixed serial (rcgen randomizes by default) — the last randomness source
    // after the key + validity, so the CA cert is fully deterministic.
    params.serial_number = Some(SerialNumber::from(1u64));

    let key = deterministic_keypair(CTX_CA)?;
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
    params.serial_number = Some(SerialNumber::from(2u64));

    let leaf_key = deterministic_keypair(CTX_SERVER)?;
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
        // No client verifier by default — the runtime attaches the OPTIONAL
        // verifier via [`TlsMaterial::with_client_verifier`] when it wants mTLS.
        client_verifier: None,
    })
}

impl TlsMaterial {
    /// Attach the OPTIONAL client-cert verifier (builder style). The runtime
    /// builds it from the SAME cluster CA via [`client_verifier`] and attaches
    /// it here so the server requests-but-does-not-require a client cert.
    #[must_use]
    pub fn with_client_verifier(
        mut self,
        verifier: std::sync::Arc<dyn ClientCertVerifier>,
    ) -> Self {
        self.client_verifier = Some(verifier);
        self
    }
}

/// Issue the bootstrap ADMIN client cert from the SAME cluster CA. Mirrors
/// [`issue_server_material`] exactly but for a CLIENT identity:
/// `CN=engenho-admin`, `O=system:masters`, EKU `clientAuth`, NO SANs (a
/// client cert needs none). Returns the leaf cert + key PEM (leaf only — the
/// client never presents the CA; the server trusts it via the CA root store
/// in [`client_verifier`]).
///
/// This is the load-bearing material behind `kubectl auth whoami →
/// engenho-admin / system:masters`: the operator's kubeconfig embeds this
/// cert and the X509 authenticator maps `CN` + `O` to `UserInfo::admin()`.
///
/// # Errors
///
/// [`PkiError::Crypto`] if rcgen can't generate / sign the client leaf.
pub fn issue_admin_client_material(ca: &ClusterCa) -> Result<ClientMaterial, PkiError> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::NoCa;
    params.distinguished_name = DistinguishedName::new();
    // CN = the authenticated username; O = the K8s super-user group.
    params
        .distinguished_name
        .push(DnType::CommonName, "engenho-admin");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "system:masters");
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    // clientAuth (NOT serverAuth) — this cert authenticates a CLIENT.
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    set_validity(&mut params, CLIENT_VALIDITY_DAYS);
    // No SANs: a client cert is identified by its Subject, not a SAN.
    params.serial_number = Some(SerialNumber::from(3u64));

    let leaf_key = deterministic_keypair(CTX_CLIENT)?;
    let issuer = ca.issuer()?;
    let leaf_cert = params.signed_by(&leaf_key, &issuer, &ca.key)?;

    Ok(ClientMaterial {
        cert_pem: leaf_cert.pem(),
        key_pem: leaf_key.serialize_pem(),
    })
}

/// Build the OPTIONAL client-cert verifier rooted at the cluster CA.
///
/// `allow_unauthenticated()` is the LOAD-BEARING call: a client that presents
/// NO certificate still completes the TLS handshake (so the existing
/// token/anonymous kubectl + the CA-only `curl` keep connecting). A client
/// that DOES present a cert has it verified against the CA root store; the
/// verified leaf's Subject is then read post-handshake by the custom acceptor
/// into a [`VerifiedClientCert`].
///
/// The CA DER comes from re-parsing `ca.ca_cert_pem` via `rustls-pemfile`
/// (already a dep). A malformed CA PEM or a builder rejection is a typed
/// [`PkiError::ClientVerifier`] — never a silent downgrade to no-client-auth.
///
/// # Errors
///
/// [`PkiError::ClientVerifier`] if the CA PEM can't be parsed into a root
/// store or the `WebPkiClientVerifier` builder rejects the roots.
pub fn client_verifier(ca: &ClusterCa) -> Result<Arc<dyn ClientCertVerifier>, PkiError> {
    // `WebPkiClientVerifier::builder(...).build()` reads the process-level
    // crypto provider; install `ring` idempotently first (same provider the
    // server config + reqwest use) so a fresh process that builds the verifier
    // before any TLS handshake doesn't panic with "no process-level
    // CryptoProvider". An Err means another component already installed one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = RootCertStore::empty();
    let mut pem = ca.ca_cert_pem.as_bytes();
    for der in rustls_pemfile::certs(&mut pem) {
        let der: CertificateDer<'static> =
            der.map_err(|e| PkiError::ClientVerifier(format!("parse CA PEM: {e}")))?;
        roots
            .add(der)
            .map_err(|e| PkiError::ClientVerifier(format!("add CA to root store: {e}")))?;
    }
    if roots.is_empty() {
        return Err(PkiError::ClientVerifier(
            "no CA certificate found in ca_cert_pem".to_string(),
        ));
    }
    WebPkiClientVerifier::builder(Arc::new(roots))
        // OPTIONAL: a no-cert client still handshakes (existing kubectl works).
        .allow_unauthenticated()
        .build()
        .map_err(|e| PkiError::ClientVerifier(format!("build client verifier: {e}")))
}

/// Extract the inner string of a [`DnValue`], whatever ASN.1 string encoding
/// it carries. Our own certs push CN/O via `&str` → `Utf8String`, but a
/// re-parsed cert may surface other encodings (printable / ia5); this folds
/// every string variant to its text so the X509 authenticator never misses an
/// identity over an encoding mismatch. Returns `None` for the BMP/Universal
/// variants we never emit (kept total, no panic).
fn dn_value_str(v: &DnValue) -> Option<String> {
    match v {
        DnValue::Utf8String(s) => Some(s.clone()),
        DnValue::PrintableString(s) => Some(s.as_str().to_string()),
        DnValue::Ia5String(s) => Some(s.as_str().to_string()),
        DnValue::TeletexString(s) => Some(s.as_str().to_string()),
        // BMP / Universal are UCS-2/UTF-32 wide encodings we never emit for
        // CN/O — not reachable for engenho-minted certs; skip rather than
        // mis-decode. `DnValue` is #[non_exhaustive]; the wildcard also folds
        // any future variant to None (skip, never a wrong identity).
        _ => None,
    }
}

/// Parse a verified peer client cert's leaf DER into a [`VerifiedClientCert`]
/// (Subject CN + Organizations). Reuses rcgen's `from_ca_cert_der` purely as
/// a DN extractor — it reads the Subject regardless of BasicConstraints (it
/// does NOT require the cert to be a CA), so a client leaf parses cleanly.
///
/// Returns `None` when the cert has no CN (an unusable identity — the X509
/// stage then declines, falling through to the next authenticator).
///
/// # Errors
///
/// [`PkiError::Crypto`] if the DER can't be parsed at all.
pub fn parse_client_cert(leaf_der: &[u8]) -> Result<Option<VerifiedClientCert>, PkiError> {
    let der = CertificateDer::from(leaf_der.to_vec());
    let params = CertificateParams::from_ca_cert_der(&der)?;
    let dn = &params.distinguished_name;
    let Some(common_name) = dn.get(&DnType::CommonName).and_then(dn_value_str) else {
        return Ok(None);
    };
    let organizations: Vec<String> = dn
        .iter()
        .filter(|(ty, _)| **ty == DnType::OrganizationName)
        .filter_map(|(_, v)| dn_value_str(v))
        .collect();
    Ok(Some(VerifiedClientCert {
        common_name,
        organizations,
    }))
}

/// The container-host gateway name pods dial the apiserver on when no service
/// datapath serves the cluster VIP.
///
/// Must stay equal to `engenho_runtime`'s `PODMAN_HOST_GATEWAY`. It is
/// duplicated rather than shared because engenho-apiserver deliberately does
/// not depend on engenho-runtime; a test pins the equality.
pub const CONTAINER_HOST_GATEWAY_SAN: &str = "host.containers.internal";

/// Build the server cert SAN list. DNS SANs are deduped-by-construction
/// (we never push the node name twice even if it's `localhost`).
fn build_sans(san: &ServerSanInputs<'_>) -> Result<Vec<SanType>, PkiError> {
    let mut sans: Vec<SanType> = Vec::new();

    // Standard DNS SANs every K8s apiserver carries, plus the container
    // host gateway.
    //
    // ── ★ THE ADVERTISED ADDRESS AND THE SAN LIST ARE A PAIR ──────────────
    // `host.containers.internal` is what the runtime TELLS pods to dial (see
    // `ApiserverReachability::HostGateway` in engenho-runtime): on darwin the
    // apiserver binds host loopback while pods live in a VM, so the cluster
    // Service VIP has no datapath and the gateway name is the only route.
    //
    // Advertising a name that is NOT in this list is a slower version of the
    // same bug it was meant to fix. Measured 2026-09-01: after the runtime
    // began advertising the gateway, in-cluster clients still failed with
    // `client error (Connect)` — the name now ROUTED, and TLS verification
    // against the projected CA rejected it, because the serving cert did not
    // name it. Two layers, one symptom, and the second is invisible from the
    // first.
    //
    // A test asserts the pair, so changing one side alone fails the build
    // rather than the cluster.
    let mut dns_names: Vec<&str> = vec![
        "localhost",
        "kubernetes",
        "kubernetes.default",
        CONTAINER_HOST_GATEWAY_SAN,
    ];
    // The node name is a DNS SAN so `server: https://<node>:port` works;
    // skip it if it duplicates one already present (e.g. node "localhost").
    if !san.node_name.is_empty() && !dns_names.contains(&san.node_name) {
        dns_names.push(san.node_name);
    }
    // Operator-declared DNS SANs, deduped against the derived ones. Declaring a
    // name the runtime already derives is not an error — it is the natural thing
    // to write when you do not know which names are automatic — so it collapses
    // rather than producing a certificate with the same name listed twice.
    for entry in san.extra_sans {
        if let SanEntry::Dns(name) = entry
            && !dns_names.contains(&name.as_str())
        {
            dns_names.push(name.as_str());
        }
    }
    for name in dns_names {
        let ia5 = name.try_into().map_err(|source| PkiError::San {
            value: name.to_string(),
            source,
        })?;
        sans.push(SanType::DnsName(ia5));
    }

    // IP SANs: loopback is always present (carries the loopback
    // kubeconfig). The concrete listen IP is added when it isn't loopback
    // (avoids a duplicate) and isn't unspecified (filtered by the caller).
    let loopback: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let mut ips: Vec<IpAddr> = vec![loopback];
    if let Some(ip) = san.listen_ip
        && !ips.contains(&ip)
    {
        ips.push(ip);
    }
    // Declared IP SANs, deduped the same way. A tailnet address or a VIP the
    // apiserver does not itself bind belongs here: the cert must name every
    // address a client dials, not only the one the socket sits on.
    for entry in san.extra_sans {
        if let SanEntry::Ip(ip) = entry
            && !ips.contains(ip)
        {
            ips.push(*ip);
        }
    }
    for ip in ips {
        sans.push(SanType::IpAddress(ip));
    }

    Ok(sans)
}

/// Set `not_before` = now, `not_after` = now + `days`.
fn set_validity(params: &mut CertificateParams, _days: i64) {
    // Deterministic FIXED window — NOT `now()`-relative — so the emitted certs
    // (and therefore the kubeconfig) are byte-identical on every fresh boot.
    // A wide fixed span [2020-01-01 .. 2100-01-01] keeps every cert valid for
    // the realistic life of a local cluster. The per-role `_days` horizon is
    // intentionally unused: a deterministic local cluster re-derives identical
    // certs each boot, so staggered expiry would only break the determinism
    // (a `now()+days` not_after differs every boot). An EXPOSED cluster that
    // wants rotation drives it through cofre + a re-seed, not a clock here.
    let not_before = time::OffsetDateTime::from_unix_timestamp(1_577_836_800)
        .expect("2020-01-01T00:00:00Z is a valid timestamp");
    let not_after = time::OffsetDateTime::from_unix_timestamp(4_102_444_800)
        .expect("2100-01-01T00:00:00Z is a valid timestamp");
    params.not_before = not_before;
    params.not_after = not_after;
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

    /// ★ The advertised address must be authenticable. The runtime tells pods
    /// to dial the container host gateway; if the serving cert does not name
    /// it, TLS verification against the projected CA fails and every
    /// in-cluster call dies with a connect error that looks like a network
    /// fault. Measured 2026-09-01 — the routing fix alone was not enough.
    #[test]
    fn the_serving_cert_names_the_container_host_gateway_pods_are_told_to_dial() {
        let san = ServerSanInputs {
            node_name: "ryn",
            listen_ip: None,
            extra_sans: &[],
        };
        let sans = super::build_sans(&san).expect("build_sans");
        assert!(
            sans.iter().any(|s| matches!(
                s,
                SanType::DnsName(n) if n.as_str() == super::CONTAINER_HOST_GATEWAY_SAN
            )),
            "serving cert must carry a SAN for {}, the name pods are told to \
             dial; without it in-cluster TLS fails against a routable address",
            super::CONTAINER_HOST_GATEWAY_SAN
        );
    }

    /// The two sides of the pair are separate constants in separate crates
    /// (engenho-apiserver does not depend on engenho-runtime). Pin the value
    /// so they cannot drift silently.
    #[test]
    fn the_gateway_san_matches_the_runtime_constant_value() {
        assert_eq!(
            super::CONTAINER_HOST_GATEWAY_SAN,
            "host.containers.internal",
            "engenho_runtime::PODMAN_HOST_GATEWAY carries the same literal; \
             changing one alone breaks in-cluster auth"
        );
    }
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
    fn pki_is_deterministic_across_fresh_clusters() {
        // Two brand-new clusters (separate data dirs) must produce a
        // BYTE-IDENTICAL CA + server cert + admin client cert — so a fresh
        // engenho yields the same kubeconfig ("a new engenho can't result in
        // a different kubectl configuration").
        let (_d1, ca1) = ca_in_tempdir();
        let (_d2, ca2) = ca_in_tempdir();
        assert_eq!(
            ca1.cert_pem(),
            ca2.cert_pem(),
            "CA cert must be deterministic"
        );

        let san = ServerSanInputs {
            node_name: "engenho-local",
            listen_ip: None,
            extra_sans: &[],
        };
        let s1 = issue_server_material(&ca1, &san).unwrap();
        let s2 = issue_server_material(&ca2, &san).unwrap();
        assert_eq!(
            s1.cert_chain_pem, s2.cert_chain_pem,
            "server cert must be deterministic"
        );

        let c1 = issue_admin_client_material(&ca1).unwrap();
        let c2 = issue_admin_client_material(&ca2).unwrap();
        assert_eq!(
            c1.cert_pem, c2.cert_pem,
            "admin client cert must be deterministic"
        );
        assert_eq!(
            c1.key_pem, c2.key_pem,
            "admin client key must be deterministic"
        );

        // Same role → same key; distinct roles → distinct keys.
        assert_eq!(
            deterministic_keypair(CTX_CA).unwrap().serialize_pem(),
            deterministic_keypair(CTX_CA).unwrap().serialize_pem(),
        );
        assert_ne!(
            deterministic_keypair(CTX_CA).unwrap().serialize_pem(),
            deterministic_keypair(CTX_SERVER).unwrap().serialize_pem(),
            "CA and server keys must be independent",
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
    fn fresh_tempdir_generates_the_same_ca() {
        // Determinism (changed 2026-06): independent data_dirs now mint a
        // BYTE-IDENTICAL CA — a fresh engenho yields the same kubeconfig. (Was
        // `assert_ne!` under the old random `KeyPair::generate` keygen.)
        let (_d1, ca1) = ca_in_tempdir();
        let (_d2, ca2) = ca_in_tempdir();
        assert_eq!(
            ca1.cert_pem(),
            ca2.cert_pem(),
            "deterministic PKI: independent data_dirs mint the SAME CA"
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
        std::fs::copy(dir_a.path().join("pki/ca.crt"), pki_b.join("ca.crt")).unwrap();
        std::fs::copy(dir_a.path().join("pki/ca.key"), pki_b.join("ca.key")).unwrap();

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
                extra_sans: &[],
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
                extra_sans: &[],
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

    #[test]
    fn admin_client_cert_carries_cn_o_and_client_auth() {
        let (_dir, ca) = ca_in_tempdir();
        let mat = issue_admin_client_material(&ca).unwrap();

        // Parse the leaf back through rcgen's DN extractor.
        let der = pem_block_to_der(&mat.cert_pem);
        let leaf = CertificateParams::from_ca_cert_der(&der.into()).unwrap();

        // EKU clientAuth (NOT serverAuth).
        assert!(
            leaf.extended_key_usages
                .contains(&ExtendedKeyUsagePurpose::ClientAuth),
            "admin client cert must have EKU clientAuth; got {:?}",
            leaf.extended_key_usages
        );
        assert!(
            !leaf
                .extended_key_usages
                .contains(&ExtendedKeyUsagePurpose::ServerAuth),
            "admin client cert must NOT carry serverAuth"
        );

        // CN=engenho-admin, O=system:masters.
        let cn = leaf.distinguished_name.get(&DnType::CommonName).unwrap();
        assert!(format!("{cn:?}").contains("engenho-admin"), "CN: {cn:?}");
        let o = leaf
            .distinguished_name
            .get(&DnType::OrganizationName)
            .unwrap();
        assert!(format!("{o:?}").contains("system:masters"), "O: {o:?}");

        // It is leaf-ONLY (one PEM block, no CA appended).
        assert_eq!(
            mat.cert_pem.matches("BEGIN CERTIFICATE").count(),
            1,
            "admin client material is leaf-only (no CA appended)"
        );
        assert!(mat.key_pem.contains("PRIVATE KEY"), "key PEM present");
    }

    #[test]
    fn parse_client_cert_reads_cn_and_orgs() {
        // The acceptor-side parse: minted admin cert → VerifiedClientCert with
        // CN=engenho-admin + O=[system:masters].
        let (_dir, ca) = ca_in_tempdir();
        let mat = issue_admin_client_material(&ca).unwrap();
        let der = pem_block_to_der(&mat.cert_pem);
        let parsed = parse_client_cert(&der)
            .unwrap()
            .expect("admin cert has a CN");
        assert_eq!(parsed.common_name, "engenho-admin");
        assert_eq!(parsed.organizations, vec!["system:masters".to_string()]);
    }

    #[test]
    fn client_verifier_builds_from_ca() {
        // The OPTIONAL verifier builds from the cluster CA without error. The
        // allow_unauthenticated posture is exercised end-to-end by the live
        // proof (no-cert curl still connects); here we assert construction.
        let (_dir, ca) = ca_in_tempdir();
        let verifier = client_verifier(&ca).expect("verifier builds from CA");
        // offer_client_auth() is true (the verifier requests a cert) but the
        // allow_unauthenticated() path makes presenting one OPTIONAL.
        assert!(
            verifier.offer_client_auth(),
            "verifier offers (optional) client auth"
        );
    }

    #[test]
    fn admin_cert_chains_to_the_cluster_ca() {
        // The minted admin client cert verifies against the CA root store —
        // proves the leaf actually chains to the cluster CA the verifier
        // trusts (the same CA the server presents).
        let (_dir, ca) = ca_in_tempdir();
        let mat = issue_admin_client_material(&ca).unwrap();
        let leaf_der = CertificateDer::from(pem_block_to_der(&mat.cert_pem));

        let mut roots = RootCertStore::empty();
        let mut ca_pem = ca.cert_pem().as_bytes();
        for der in rustls_pemfile::certs(&mut ca_pem) {
            roots.add(der.unwrap()).unwrap();
        }
        // webpki verification needs a crypto provider; install ring.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let now = rustls::pki_types::UnixTime::now();
        // No intermediates: the admin leaf chains directly to the CA root.
        verifier
            .verify_client_cert(&leaf_der, &[], now)
            .expect("admin leaf verifies against the cluster CA root");
    }

    // ── small parsing helpers for the tests above ──────────────────────

    /// Decode the first PEM certificate block of `pem` to its DER bytes.
    fn pem_block_to_der(pem: &str) -> Vec<u8> {
        let mut bytes = pem.as_bytes();
        rustls_pemfile::certs(&mut bytes)
            .next()
            .expect("a cert block")
            .expect("valid PEM cert")
            .to_vec()
    }

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

    // ── Operator-declared SANs ────────────────────────────────────────────
    //
    // The derived set answers for the NODE. These prove it can be made to
    // answer for the names the cluster is REACHED BY, which is a fact about
    // the deployment that the runtime cannot know.

    #[test]
    fn a_declared_dns_name_reaches_the_certificate() {
        let extra = vec![SanEntry::Dns("plo.natal.quero.cloud".to_string())];
        let san = ServerSanInputs {
            node_name: "plo",
            listen_ip: None,
            extra_sans: &extra,
        };
        let sans = super::build_sans(&san).expect("build_sans");
        assert!(
            sans.iter().any(|s| matches!(
                s,
                SanType::DnsName(n) if n.as_str() == "plo.natal.quero.cloud"
            )),
            "a declared DNS SAN must appear in the serving cert; without it a \
             client dialing that name gets a verification failure that reads \
             like a bad kubeconfig"
        );
    }

    #[test]
    fn a_declared_ip_reaches_the_certificate_even_when_nothing_binds_it() {
        // The case the field exists for: a tailnet address the apiserver does
        // NOT bind (it binds 0.0.0.0, which is not a usable SAN) but which
        // every remote client dials.
        let tailnet: IpAddr = "100.64.0.7".parse().unwrap();
        let extra = vec![SanEntry::Ip(tailnet)];
        let san = ServerSanInputs {
            node_name: "plo",
            listen_ip: None,
            extra_sans: &extra,
        };
        let sans = super::build_sans(&san).expect("build_sans");
        assert!(
            sans.iter()
                .any(|s| matches!(s, SanType::IpAddress(ip) if *ip == tailnet)),
            "a declared IP SAN must appear even though listen_ip is None — \
             binding 0.0.0.0 yields no listen_ip, and that is exactly when a \
             declared address is the only thing naming the reachable one"
        );
    }

    #[test]
    fn declaring_a_name_the_runtime_already_derives_does_not_duplicate_it() {
        // An operator listing every name they can think of is the expected
        // usage; they should not have to first learn which are automatic.
        let extra = vec![
            SanEntry::Dns("localhost".to_string()),
            SanEntry::Dns("plo".to_string()),
            SanEntry::Ip("127.0.0.1".parse().unwrap()),
        ];
        let san = ServerSanInputs {
            node_name: "plo",
            listen_ip: None,
            extra_sans: &extra,
        };
        let sans = super::build_sans(&san).expect("build_sans");
        let count = |want: &str| {
            sans.iter()
                .filter(|s| match s {
                    SanType::DnsName(n) => n.as_str() == want,
                    SanType::IpAddress(ip) => ip.to_string() == want,
                    _ => false,
                })
                .count()
        };
        assert_eq!(count("localhost"), 1, "localhost duplicated");
        assert_eq!(count("plo"), 1, "the node name duplicated");
        assert_eq!(count("127.0.0.1"), 1, "loopback duplicated");
    }

    #[test]
    fn an_empty_declaration_leaves_the_derived_set_exactly_as_it_was() {
        // The regression guard for every existing deployment: adding this
        // field must not perturb a cert issued without it.
        let with = ServerSanInputs {
            node_name: "plo",
            listen_ip: None,
            extra_sans: &[],
        };
        let sans = super::build_sans(&with).expect("build_sans");
        let dns: Vec<String> = sans
            .iter()
            .filter_map(|s| match s {
                SanType::DnsName(n) => Some(n.as_str().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            dns,
            vec![
                "localhost".to_string(),
                "kubernetes".to_string(),
                "kubernetes.default".to_string(),
                super::CONTAINER_HOST_GATEWAY_SAN.to_string(),
                "plo".to_string(),
            ],
            "the derived DNS SAN set and its ORDER must be unchanged by the \
             introduction of extra_sans"
        );
    }

    // ── Classification, and the four mistakes it refuses ──────────────────

    #[test]
    fn an_address_is_classified_as_an_ip_and_a_name_as_dns() {
        // The whole reason the operator does not declare which kind an entry
        // is: the answer is always derivable, and a wrong answer produces a
        // cert that verifies for nothing.
        assert_eq!(
            "100.64.0.7".parse::<SanEntry>().unwrap(),
            SanEntry::Ip("100.64.0.7".parse().unwrap())
        );
        assert_eq!(
            "::1".parse::<SanEntry>().unwrap(),
            SanEntry::Ip("::1".parse().unwrap()),
            "an IPv6 literal is an IP SAN, not a DNS name with colons in it"
        );
        assert_eq!(
            "plo.tail1234.ts.net".parse::<SanEntry>().unwrap(),
            SanEntry::Dns("plo.tail1234.ts.net".to_string())
        );
    }

    #[test]
    fn the_four_pastes_that_would_silently_produce_a_useless_cert_are_refused() {
        // Each of these is a plausible thing to copy out of a kubeconfig or a
        // browser bar, and each would be encoded verbatim as a DNS SAN that
        // matches no connection anyone ever makes.
        assert_eq!("".parse::<SanEntry>(), Err(SanParseError::Empty));
        assert_eq!("   ".parse::<SanEntry>(), Err(SanParseError::Empty));
        assert!(matches!(
            "https://plo:6443".parse::<SanEntry>(),
            Err(SanParseError::HasScheme { .. })
        ));
        assert!(matches!(
            "plo:6443".parse::<SanEntry>(),
            Err(SanParseError::HasPort { .. })
        ));
        assert!(matches!(
            "plo natal".parse::<SanEntry>(),
            Err(SanParseError::HasWhitespace { .. })
        ));
    }

    #[test]
    fn surrounding_whitespace_is_trimmed_rather_than_refused() {
        // YAML list items pick up spaces; that is a formatting artifact, not
        // an operator error, and refusing it would be hostile.
        assert_eq!(
            "  plo.quero.cloud  ".parse::<SanEntry>().unwrap(),
            SanEntry::Dns("plo.quero.cloud".to_string())
        );
    }
}
