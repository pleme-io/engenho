//! SPKI pins: how the remote control listener and its clients recognise each
//! other without a certificate authority.
//!
//! Each side holds an ed25519 key and presents a certificate self-signed with
//! it, minted when it is needed. What is trusted is the SHA-256 of the key's
//! DER `SubjectPublicKeyInfo` — a pin, the `authorized_keys` model — never a
//! certificate's issuer, names or validity. Revoking a client is removing its
//! pin; rotating a server's key is adding its new pin beside the old one.
//!
//! Why not engenho's cluster CA: see docs/CONTROL-PLANE.md § Remote trust. In
//! short, its leaves never expire and the apiserver accepts any of them as a
//! Kubernetes identity; with pins the two planes reject each other's
//! certificates by construction.
//!
//! Both verifiers still verify the handshake's signature against the key in
//! the presented certificate (delegating to rustls), so presenting a pinned
//! certificate without its private key fails the handshake.

use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, Error, SignatureScheme};
use sha2::{Digest, Sha256};

/// SHA-256 of a DER `SubjectPublicKeyInfo`, spelled `sha256:<64 lowercase hex>`
/// (the control API's `SpkiSha256`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Spki([u8; 32]);

/// A string that is not a pin, or bytes that are not a certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpkiError {
    /// Not `sha256:<64 lowercase hex>`.
    NotAPin(String),
    /// Not a DER X.509 certificate.
    NotACertificate(String),
}

impl fmt::Display for SpkiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAPin(s) => write!(f, "{s:?} is not a pin (sha256:<64 lowercase hex>)"),
            Self::NotACertificate(why) => write!(f, "not a DER certificate: {why}"),
        }
    }
}

impl std::error::Error for SpkiError {}

impl Spki {
    /// The pin of a DER `SubjectPublicKeyInfo`.
    #[must_use]
    pub fn of_spki_der(der: &[u8]) -> Self {
        Self(Sha256::digest(der).into())
    }

    /// The pin of the key a DER certificate carries.
    ///
    /// # Errors
    ///
    /// [`SpkiError::NotACertificate`].
    pub fn of_certificate(der: &[u8]) -> Result<Self, SpkiError> {
        let (_, cert) = x509_parser::parse_x509_certificate(der)
            .map_err(|e| SpkiError::NotACertificate(e.to_string()))?;
        Ok(Self::of_spki_der(cert.tbs_certificate.subject_pki.raw))
    }

    /// Parse `sha256:<64 lowercase hex>`.
    ///
    /// # Errors
    ///
    /// [`SpkiError::NotAPin`].
    pub fn parse(s: &str) -> Result<Self, SpkiError> {
        let bad = || SpkiError::NotAPin(s.to_owned());
        let hex = s.strip_prefix("sha256:").ok_or_else(bad)?;
        if hex.len() != 64 {
            return Err(bad());
        }
        let nibble = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        let (pairs, _) = hex.as_bytes().as_chunks::<2>();
        let mut out = [0u8; 32];
        for (byte, [hi, lo]) in out.iter_mut().zip(pairs) {
            *byte = (nibble(*hi).ok_or_else(bad)? << 4) | nibble(*lo).ok_or_else(bad)?;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for Spki {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sha256:")?;
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

impl fmt::Debug for Spki {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::str::FromStr for Spki {
    type Err = SpkiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl serde::Serialize for Spki {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Spki {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// A key or certificate could not be made or read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError(pub String);

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for KeyError {}

/// An ed25519 key: what one side of the remote control plane is.
pub struct KeyMaterial {
    key: rcgen::KeyPair,
}

impl fmt::Debug for KeyMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the private key.
        f.debug_struct("KeyMaterial")
            .field("spki", &self.spki())
            .finish_non_exhaustive()
    }
}

impl KeyMaterial {
    /// A new random key.
    ///
    /// # Errors
    ///
    /// The platform's randomness failed.
    pub fn generate() -> Result<Self, KeyError> {
        rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)
            .map(|key| Self { key })
            .map_err(|e| KeyError(e.to_string()))
    }

    /// A key read from PKCS#8 PEM.
    ///
    /// # Errors
    ///
    /// It is not an ed25519 PKCS#8 PEM key.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let key = rcgen::KeyPair::from_pem(pem).map_err(|e| KeyError(e.to_string()))?;
        if !key.is_compatible(&rcgen::PKCS_ED25519) {
            return Err(KeyError("the key is not an ed25519 key".into()));
        }
        Ok(Self { key })
    }

    /// The key as PKCS#8 PEM — the private key: keep it `0600`.
    #[must_use]
    pub fn to_pem(&self) -> String {
        self.key.serialize_pem()
    }

    /// Its pin.
    #[must_use]
    pub fn spki(&self) -> Spki {
        Spki::of_spki_der(&self.key.public_key_der())
    }

    /// A certificate self-signed with it, naming `name`, and the key rustls
    /// presents it with.
    ///
    /// # Errors
    ///
    /// The certificate could not be signed.
    pub fn certificate(
        &self,
        name: &str,
    ) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), KeyError> {
        let params = rcgen::CertificateParams::new(vec![name.to_owned()])
            .map_err(|e| KeyError(e.to_string()))?;
        let cert = params
            .self_signed(&self.key)
            .map_err(|e| KeyError(e.to_string()))?;
        Ok((
            cert.der().clone(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key.serialize_der())),
        ))
    }
}

/// The crypto both sides speak: ring, as the rest of engenho does.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// TLS 1.3 only: nothing older is ever negotiated on the control plane.
const VERSIONS: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// The pins a server admits, looked up per handshake — so a set that changes
/// (a pin added or revoked) is seen by the next connection.
pub trait PinSet: Send + Sync + fmt::Debug {
    /// Whether `spki` is admitted.
    fn admits(&self, spki: &Spki) -> bool;
}

/// Pin a certificate: its key's pin, when `admits` it.
fn pinned(cert: &CertificateDer<'_>, admits: impl Fn(&Spki) -> bool) -> Result<Spki, Error> {
    let spki = Spki::of_certificate(cert)
        .map_err(|_| Error::InvalidCertificate(CertificateError::BadEncoding))?;
    if admits(&spki) {
        Ok(spki)
    } else {
        Err(Error::InvalidCertificate(
            CertificateError::ApplicationVerificationFailure,
        ))
    }
}

/// The handshake-signature half both verifiers delegate to rustls.
#[derive(Debug, Clone, Copy)]
struct Signatures(WebPkiSupportedAlgorithms);

impl Signatures {
    fn tls12(
        self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn tls13(
        self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn schemes(self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// The server's verifier: a client is admitted iff its certificate's key is
/// pinned. A client certificate is mandatory.
#[derive(Debug)]
struct ClientPins {
    pins: Arc<dyn PinSet>,
    signatures: Signatures,
}

impl ClientCertVerifier for ClientPins {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        pinned(end_entity, |spki| self.pins.admits(spki)).map(|_| ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.signatures.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.signatures.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.signatures.schemes()
    }
}

/// The client's verifier: the server is trusted iff its certificate's key is
/// one of the pins (several, so a rotation can overlap).
#[derive(Debug)]
struct ServerPins {
    pins: Vec<Spki>,
    signatures: Signatures,
}

impl ServerCertVerifier for ServerPins {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        pinned(end_entity, |spki| self.pins.contains(spki)).map(|_| ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.signatures.tls12(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.signatures.tls13(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.signatures.schemes()
    }
}

/// The name a control certificate carries; nothing checks it.
pub const CERT_NAME: &str = "engenho-control";

/// The certificate a server presents, replaceable while it serves: the next
/// handshake after [`Self::present`] presents the new key, and connections
/// already made keep the one they were made with.
#[derive(Debug)]
pub struct Presented {
    current: RwLock<Arc<CertifiedKey>>,
}

impl Presented {
    /// Present `identity`.
    ///
    /// # Errors
    ///
    /// Its certificate could not be made.
    pub fn new(identity: &KeyMaterial) -> Result<Self, KeyError> {
        Ok(Self {
            current: RwLock::new(certified(identity)?),
        })
    }

    /// Present `identity` from the next handshake on.
    ///
    /// # Errors
    ///
    /// Its certificate could not be made; the old one is still presented.
    pub fn present(&self, identity: &KeyMaterial) -> Result<(), KeyError> {
        let next = certified(identity)?;
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = next;
        Ok(())
    }
}

impl ResolvesServerCert for Presented {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(
            &self.current.read().unwrap_or_else(PoisonError::into_inner),
        ))
    }
}

fn certified(identity: &KeyMaterial) -> Result<Arc<CertifiedKey>, KeyError> {
    let (cert, key) = identity.certificate(CERT_NAME)?;
    CertifiedKey::from_der(vec![cert], key, &provider())
        .map(Arc::new)
        .map_err(|e| KeyError(e.to_string()))
}

/// The remote listener's TLS: TLS 1.3, presenting what `identity` presents
/// at each handshake, admitting only clients whose key `clients` pins.
///
/// # Errors
///
/// rustls refused the configuration.
pub fn server_config(
    identity: Arc<Presented>,
    clients: Arc<dyn PinSet>,
) -> Result<rustls::ServerConfig, KeyError> {
    let provider = provider();
    let signatures = Signatures(provider.signature_verification_algorithms);
    Ok(rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(VERSIONS)
        .map_err(|e| KeyError(e.to_string()))?
        .with_client_cert_verifier(Arc::new(ClientPins {
            pins: clients,
            signatures,
        }))
        .with_cert_resolver(identity))
}

/// A client's TLS: TLS 1.3, presenting `key`, trusting only a server whose
/// key is one of `server`.
///
/// # Errors
///
/// As [`server_config`].
pub fn client_config(
    key: &KeyMaterial,
    server: Vec<Spki>,
) -> Result<rustls::ClientConfig, KeyError> {
    let provider = provider();
    let signatures = Signatures(provider.signature_verification_algorithms);
    let (cert, private) = key.certificate(CERT_NAME)?;
    rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(VERSIONS)
        .map_err(|e| KeyError(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ServerPins {
            pins: server,
            signatures,
        }))
        .with_client_auth_cert(vec![cert], private)
        .map_err(|e| KeyError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pin_is_spelled_sha256_hex_and_round_trips() {
        let key = KeyMaterial::generate().unwrap();
        let spki = key.spki();
        let text = spki.to_string();
        assert!(text.starts_with("sha256:") && text.len() == 71, "{text}");
        assert_eq!(Spki::parse(&text), Ok(spki));
        assert_eq!(
            serde_json::from_value::<Spki>(serde_json::to_value(spki).unwrap()).unwrap(),
            spki
        );
        for bad in ["", "sha256:", "sha1:00", &text.to_uppercase(), &text[..70]] {
            assert!(Spki::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_pin_of_a_key_is_the_pin_of_its_certificate() {
        let key = KeyMaterial::generate().unwrap();
        let (cert, _) = key.certificate(CERT_NAME).unwrap();
        assert_eq!(Spki::of_certificate(&cert), Ok(key.spki()));
        let reread = KeyMaterial::from_pem(&key.to_pem()).unwrap();
        assert_eq!(reread.spki(), key.spki());
        assert!(
            !format!("{key:?}").contains("PRIVATE"),
            "Debug never shows the key"
        );
    }

    #[test]
    fn a_non_ed25519_key_is_refused() {
        let p256 = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        assert!(KeyMaterial::from_pem(&p256.serialize_pem()).is_err());
    }
}
