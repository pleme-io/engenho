//! Renewal of a node's leaf certificates while it serves.
//!
//! `blue/pki/pki.b` names each leaf's lease; this module issues what it names
//! and swaps the server leaf under the live listener. A failed renewal keeps the
//! leaf already held, warns and counts: nothing here can stop the API.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use engenho_blue::BlueWorker;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use serde::{Deserialize, Serialize};

use crate::pki::{
    ClientMaterial, ClusterCa, PkiError, SanEntry, ServerSanInputs, TlsMaterial,
    issue_admin_client_material_leased, issue_server_material_leased,
};

const FILES: &[(&str, &str)] = &[("pki/pki.b", include_str!("../blue/pki/pki.b"))];
const ENTRY: &str = "use(\"pki\")\n";

/// 2020-01-01T00:00:00Z.
const STANDING_NOT_BEFORE: i64 = 1_577_836_800;
/// 2100-01-01T00:00:00Z.
const STANDING_NOT_AFTER: i64 = 4_102_444_800;

/// The span of one certificate, and when it is renewed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub struct Lease {
    /// Unix seconds it is valid from.
    pub not_before: i64,
    /// Unix seconds it is valid to.
    pub not_after: i64,
    /// Unix seconds a renewal is due.
    pub renew_at: i64,
    /// Its serial number; a new lease has a new one.
    pub serial: u64,
}

impl Lease {
    /// The span of the CA, and of every leaf on a node with rotation off.
    #[must_use]
    pub const fn standing(serial: u64) -> Self {
        Self {
            not_before: STANDING_NOT_BEFORE,
            not_after: STANDING_NOT_AFTER,
            renew_at: STANDING_NOT_AFTER,
            serial,
        }
    }
}

/// A kind of leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeafKind {
    /// The apiserver's server certificate.
    Server,
    /// The admin client certificate a kubeconfig embeds.
    Client,
}

impl LeafKind {
    const fn standing_serial(self) -> u64 {
        match self {
            Self::Server => 2,
            Self::Client => 3,
        }
    }
}

#[derive(Serialize)]
struct LeaseFacts {
    kind: LeafKind,
    now: i64,
}

/// The `pki` program.
pub struct Leases(Result<BlueWorker, String>);

impl Leases {
    /// Load the program. A failed load is held, and each ask answers with it.
    #[must_use]
    pub fn start() -> Self {
        Self(BlueWorker::start("pki", ENTRY, FILES))
    }

    /// `pki::lease`.
    ///
    /// # Errors
    /// The program failed to load, or its answer does not read.
    pub async fn lease(&self, kind: LeafKind, now: i64) -> Result<Lease, String> {
        let program = self.0.as_ref().map_err(Clone::clone)?;
        let facts = serde_json::to_string(&LeaseFacts { kind, now }).map_err(|e| e.to_string())?;
        program.ask("pki::lease", facts).await
    }
}

/// The server leaf a listener presents, swapped in place.
#[derive(Debug)]
pub struct LiveCert(ArcSwap<CertifiedKey>);

impl LiveCert {
    /// The leaf `material` holds.
    ///
    /// # Errors
    /// The material does not parse into a certified key.
    pub fn new(material: &TlsMaterial) -> Result<Self, String> {
        Ok(Self(ArcSwap::from_pointee(certified_key(material)?)))
    }

    /// Present `material` on every later handshake. An error leaves the
    /// present leaf in place.
    ///
    /// # Errors
    /// The material does not parse into a certified key.
    pub fn store(&self, material: &TlsMaterial) -> Result<(), String> {
        self.0.store(Arc::new(certified_key(material)?));
        Ok(())
    }
}

impl ResolvesServerCert for LiveCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.load_full())
    }
}

fn certified_key(material: &TlsMaterial) -> Result<CertifiedKey, String> {
    let chain = rustls_pemfile::certs(&mut material.cert_chain_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parse cert chain PEM: {e}"))?;
    if chain.is_empty() {
        return Err("no certificate found in cert_chain_pem".to_string());
    }
    let key = rustls_pemfile::private_key(&mut material.key_pem.as_bytes())
        .map_err(|e| format!("parse server key PEM: {e}"))?
        .ok_or_else(|| "no private key found in key_pem".to_string())?;
    let signer = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|e| format!("server key: {e}"))?;
    Ok(CertifiedKey::new(chain, signer))
}

/// What became of renewals, for `engenho_cert_renewals_total`.
#[derive(Debug, Default)]
pub struct RenewalCounts {
    renewed: AtomicU64,
    failed: AtomicU64,
}

impl RenewalCounts {
    /// Leaves renewed.
    #[must_use]
    pub fn renewed(&self) -> u64 {
        self.renewed.load(Ordering::Relaxed)
    }

    /// Renewals that failed, each keeping the leaf already held.
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    fn fail(&self, leaf: LeafKind, why: &str) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            ?leaf,
            error = why,
            "certificate renewal failed; the leaf already held stays in use"
        );
    }
}

/// The names a server leaf answers for.
#[derive(Clone, Debug)]
pub struct OwnedSans {
    /// The node's name.
    pub node_name: String,
    /// The concrete listen IP, when there is one.
    pub listen_ip: Option<IpAddr>,
    /// What `runtime.tls.extra_sans` declares.
    pub extra_sans: Vec<SanEntry>,
}

impl OwnedSans {
    fn inputs(&self) -> ServerSanInputs<'_> {
        ServerSanInputs {
            node_name: &self.node_name,
            listen_ip: self.listen_ip,
            extra_sans: &self.extra_sans,
        }
    }
}

/// Renews a node's two leaves from its CA.
pub struct CertRenewer {
    ca: Arc<ClusterCa>,
    sans: OwnedSans,
    leases: Leases,
    live: Arc<LiveCert>,
    server_serial: AtomicU64,
    client_serial: AtomicU64,
    counts: Arc<RenewalCounts>,
}

/// What a renewer starts a node serving with.
pub struct FirstLeaves {
    /// The server material, carrying the live leaf.
    pub server: TlsMaterial,
    /// The admin client material.
    pub client: ClientMaterial,
    /// The renewer of both.
    pub renewer: CertRenewer,
}

impl CertRenewer {
    /// Issue a node's first leaves under their leases at `now`. A lease that
    /// cannot be read issues a standing leaf, warns and counts, because a node
    /// that cannot read its lease must still serve; a later tick replaces it.
    ///
    /// # Errors
    /// The leaves could not be issued at all.
    pub async fn issue_first(
        ca: Arc<ClusterCa>,
        sans: OwnedSans,
        leases: Leases,
        now: i64,
    ) -> Result<FirstLeaves, PkiError> {
        let counts = Arc::new(RenewalCounts::default());
        let lease_or_standing = async |kind: LeafKind| {
            leases.lease(kind, now).await.unwrap_or_else(|why| {
                counts.fail(kind, &why);
                Lease::standing(kind.standing_serial())
            })
        };
        let server_lease = lease_or_standing(LeafKind::Server).await;
        let client_lease = lease_or_standing(LeafKind::Client).await;
        let server = issue_server_material_leased(&ca, &sans.inputs(), &server_lease)?;
        let client = issue_admin_client_material_leased(&ca, &client_lease)?;
        let live = Arc::new(LiveCert::new(&server).map_err(PkiError::Renewal)?);
        let server = server.with_live_cert(live.clone());
        Ok(FirstLeaves {
            server,
            client,
            renewer: Self {
                ca,
                sans,
                leases,
                live,
                server_serial: AtomicU64::new(server_lease.serial),
                client_serial: AtomicU64::new(client_lease.serial),
                counts,
            },
        })
    }

    /// What became of renewals.
    #[must_use]
    pub fn counts(&self) -> Arc<RenewalCounts> {
        self.counts.clone()
    }

    /// Renew whichever leaf has a new lease at `now`, each apart from the
    /// other. `publish` persists and publishes a new client leaf; its error
    /// counts as a failed renewal and the next tick tries again.
    pub async fn tick(&self, now: i64, publish: impl Fn(&ClientMaterial) -> Result<(), String>) {
        if let Err(why) = self.renew_server(now).await {
            self.counts.fail(LeafKind::Server, &why);
        }
        if let Err(why) = self.renew_client(now, publish).await {
            self.counts.fail(LeafKind::Client, &why);
        }
    }

    async fn renew_server(&self, now: i64) -> Result<(), String> {
        let lease = self.leases.lease(LeafKind::Server, now).await?;
        if lease.serial == self.server_serial.load(Ordering::Relaxed) {
            return Ok(());
        }
        let material = issue_server_material_leased(&self.ca, &self.sans.inputs(), &lease)
            .map_err(|e| e.to_string())?;
        self.live.store(&material)?;
        self.server_serial.store(lease.serial, Ordering::Relaxed);
        self.counts.renewed.fetch_add(1, Ordering::Relaxed);
        tracing::info!(not_after = lease.not_after, "server certificate renewed");
        Ok(())
    }

    async fn renew_client(
        &self,
        now: i64,
        publish: impl Fn(&ClientMaterial) -> Result<(), String>,
    ) -> Result<(), String> {
        let lease = self.leases.lease(LeafKind::Client, now).await?;
        if lease.serial == self.client_serial.load(Ordering::Relaxed) {
            return Ok(());
        }
        let material =
            issue_admin_client_material_leased(&self.ca, &lease).map_err(|e| e.to_string())?;
        publish(&material)?;
        self.client_serial.store(lease.serial, Ordering::Relaxed);
        self.counts.renewed.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            not_after = lease.not_after,
            "admin client certificate renewed"
        );
        Ok(())
    }
}

/// `engenho_cert_renewals_total{result}`.
#[must_use]
pub fn renewals_family(counts: &RenewalCounts) -> crate::metrics::MetricFamily {
    #[allow(clippy::cast_precision_loss)]
    let sample = |result: &str, count: u64| crate::metrics::Sample {
        labels: vec![("result".to_string(), result.to_string())],
        value: count as f64,
    };
    crate::metrics::MetricFamily {
        name: "engenho_cert_renewals_total".into(),
        help: "Leaf certificate renewals, by what became of them.".into(),
        kind: crate::metrics::MetricType::Counter,
        samples: vec![
            sample("renewed", counts.renewed()),
            sample("failed", counts.failed()),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pki::{ServerSanInputs, client_verifier, load_or_generate_ca};
    use crate::{ApiServer, RouterState};
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

    const NOW: i64 = 1_790_000_000;
    const YEAR: i64 = 365 * 86_400;

    fn sans() -> OwnedSans {
        OwnedSans {
            node_name: "node-a".to_string(),
            listen_ip: None,
            extra_sans: Vec::new(),
        }
    }

    async fn first(dir: &std::path::Path) -> FirstLeaves {
        let ca = Arc::new(load_or_generate_ca(dir).unwrap());
        CertRenewer::issue_first(ca, sans(), Leases::start(), NOW)
            .await
            .unwrap()
    }

    fn not_after(pem: &str) -> i64 {
        let der = rustls_pemfile::certs(&mut pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        rcgen::CertificateParams::from_ca_cert_der(&der)
            .unwrap()
            .not_after
            .unix_timestamp()
    }

    async fn presented(addr: std::net::SocketAddr, ca_pem: &str) -> CertificateDer<'static> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
            .expect("the client pins the CA alone, and the leaf chains to it");
        tls.get_ref().1.peer_certificates().unwrap()[0]
            .clone()
            .into_owned()
    }

    #[tokio::test]
    async fn every_leaf_has_a_bounded_span_and_the_ca_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let leaves = first(dir.path()).await;
        for pem in [&leaves.server.cert_chain_pem, &leaves.client.cert_pem] {
            let end = not_after(pem);
            assert!(end > NOW, "{end}");
            assert!(end <= NOW + YEAR, "{end}");
        }
        assert_eq!(leaves.renewer.counts().failed(), 0);
        assert_eq!(not_after(&leaves.server.ca_cert_pem), STANDING_NOT_AFTER);
    }

    #[tokio::test]
    async fn a_restart_inside_a_period_issues_the_same_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let one = first(dir.path()).await;
        let two = first(dir.path()).await;
        assert_eq!(one.server.cert_chain_pem, two.server.cert_chain_pem);
        assert_eq!(one.client.cert_pem, two.client.cert_pem);
    }

    #[tokio::test]
    async fn the_listener_presents_a_renewed_leaf_with_no_restart_and_the_same_ca() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let leaves = first(dir.path()).await;
        let ca_pem = leaves.server.ca_cert_pem.clone();
        let renewer = leaves.renewer;
        let server = ApiServer::start_with_state(
            "127.0.0.1:0".parse().unwrap(),
            RouterState::new(vec![]),
            Some(leaves.server),
        )
        .await
        .unwrap();
        let addr = server.local_addr();
        let before = presented(addr, &ca_pem).await;

        renewer
            .tick(NOW, |_| panic!("no new client lease yet"))
            .await;
        assert_eq!(
            renewer.counts().renewed(),
            0,
            "nothing is due inside a period"
        );
        assert_eq!(presented(addr, &ca_pem).await, before);

        renewer.tick(NOW + 31 * 86_400, |_| Ok(())).await;
        assert!(renewer.counts().renewed() >= 1);
        assert_eq!(renewer.counts().failed(), 0);
        assert_ne!(presented(addr, &ca_pem).await, before);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_new_client_leaf_is_published_and_a_failed_publish_is_retried() {
        let dir = tempfile::tempdir().unwrap();
        let renewer = first(dir.path()).await.renewer;
        let later = NOW + 91 * 86_400;

        renewer.tick(later, |_| Err("disk full".to_string())).await;
        assert_eq!(renewer.counts().failed(), 1, "counted");

        let got = std::sync::Mutex::new(None);
        renewer
            .tick(later, |admin| {
                *got.lock().unwrap() = Some(admin.cert_pem.clone());
                Ok(())
            })
            .await;
        let pem = got.lock().unwrap().clone().expect("retried and published");
        assert!(not_after(&pem) > later);
        assert_eq!(renewer.counts().failed(), 1);
    }

    #[tokio::test]
    async fn a_failed_server_renewal_keeps_the_leaf_serving_and_counts() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let mut leaves = first(dir.path()).await;
        let ca_pem = leaves.server.ca_cert_pem.clone();
        leaves.renewer.sans.node_name = "not\u{1F600}a-name".to_string();
        let renewer = leaves.renewer;
        let server = ApiServer::start_with_state(
            "127.0.0.1:0".parse().unwrap(),
            RouterState::new(vec![]),
            Some(leaves.server),
        )
        .await
        .unwrap();
        let addr = server.local_addr();
        let before = presented(addr, &ca_pem).await;

        renewer.tick(NOW + 31 * 86_400, |_| Ok(())).await;

        assert_eq!(renewer.counts().failed(), 1);
        assert_eq!(
            presented(addr, &ca_pem).await,
            before,
            "the old leaf serves"
        );
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_lease_that_cannot_be_read_still_serves_and_counts() {
        let dir = tempfile::tempdir().unwrap();
        let ca = Arc::new(load_or_generate_ca(dir.path()).unwrap());
        let broken = Leases(Err("no program".to_string()));
        let leaves = CertRenewer::issue_first(ca, sans(), broken, NOW)
            .await
            .unwrap();
        assert_eq!(not_after(&leaves.client.cert_pem), STANDING_NOT_AFTER);
        assert_eq!(leaves.renewer.counts().failed(), 2);
        leaves.renewer.tick(NOW, |_| Ok(())).await;
        assert_eq!(leaves.renewer.counts().failed(), 4, "each tick says so");
    }

    #[tokio::test]
    async fn an_old_data_dir_is_read_as_it_is_and_its_leaves_keep_verifying() {
        let dir = tempfile::tempdir().unwrap();
        let ca = load_or_generate_ca(dir.path()).unwrap();
        let old_client = crate::pki::issue_admin_client_material(&ca).unwrap();
        let old_server = crate::pki::issue_server_material(
            &ca,
            &ServerSanInputs {
                node_name: "node-a",
                listen_ip: None,
                extra_sans: &[],
            },
        )
        .unwrap();
        assert_eq!(not_after(&old_client.cert_pem), STANDING_NOT_AFTER);
        assert_eq!(not_after(&old_server.cert_chain_pem), STANDING_NOT_AFTER);
        let ca_before = std::fs::read(crate::PkiFile::CaCert.path(dir.path())).unwrap();

        let leaves = first(dir.path()).await;

        assert_eq!(
            std::fs::read(crate::PkiFile::CaCert.path(dir.path())).unwrap(),
            ca_before,
            "the CA is kept"
        );
        assert!(not_after(&leaves.client.cert_pem) < STANDING_NOT_AFTER);
        let verifier = client_verifier(&load_or_generate_ca(dir.path()).unwrap()).unwrap();
        let der = rustls_pemfile::certs(&mut old_client.cert_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        verifier
            .verify_client_cert(&der, &[], rustls::pki_types::UnixTime::now())
            .expect("a kubeconfig copied before rotation keeps authenticating");
    }

    #[test]
    fn the_metric_names_each_result() {
        let counts = RenewalCounts::default();
        counts.renewed.fetch_add(2, Ordering::Relaxed);
        counts.failed.fetch_add(1, Ordering::Relaxed);
        let family = renewals_family(&counts);
        assert_eq!(family.name, "engenho_cert_renewals_total");
        assert_eq!(family.samples[0].value, 2.0);
        assert_eq!(family.samples[1].value, 1.0);
    }

    #[tokio::test]
    async fn material_that_does_not_parse_leaves_the_present_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let mut leaves = first(dir.path()).await;
        let live = leaves.renewer.live.clone();
        let held = live.0.load_full();
        leaves.server.cert_chain_pem = String::new();
        assert!(live.store(&leaves.server).is_err());
        assert!(Arc::ptr_eq(&held, &live.0.load_full()));
    }
}
