//! I23 / T0.11 — one would-reject ledger for the whole daemon.
//!
//! Every rollout gate in `Rollout::Shadow` counts what `Enforce` would have
//! refused into a `WouldRejectLedger`, and the apiserver's `/metrics` renders
//! the ledger its `RouterState` holds as
//! `engenho_would_reject_total{gate,reason}`. `RouterState::new` gives the
//! router a private ledger of its own, so unless the runtime installs the ONE
//! ledger it hands every other gate-owning component (the store's image
//! tripwire, the scheduler's filters), those components count into a ledger
//! no scrape ever reads: the operator sees zero would-rejects and flips a
//! gate to Enforce that is refusing real objects.
//!
//! What is pinned here is the observable half: a refusal a Shadow gate
//! records into the ledger the runtime exposes is on the running daemon's
//! `/metrics`, and the scrape follows the ledger live rather than a copy.
//!
//! Tier: only-mitigated. `RouterState::new` still builds a private default,
//! so a second ledger is representable; this test is what catches the
//! runtime forgetting to replace it.

use std::time::Duration;

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::Runtime;
use engenho_substrate::{Gate, Proceed, RejectReason, Rollout};
use shikumi::TieredConfig;

/// A gate no production code declares, so its series can only come from
/// this test.
const PROBE_GATE: Gate = Gate::new("i23_one_ledger_probe", Rollout::Shadow);

/// The probe gate's one failure reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeFailed;

impl RejectReason for ProbeFailed {
    fn label(&self) -> &'static str {
        "probe_failed"
    }
}

/// The exact sample line the scrape carries for the probe gate at `count`.
fn probe_sample(count: u64) -> String {
    format!(
        "engenho_would_reject_total{{gate=\"{}\",reason=\"probe_failed\"}} {count}\n",
        PROBE_GATE.name()
    )
}

/// Ephemeral store, fake kubelet backend, plaintext, every listener on an
/// ephemeral loopback port so this binary runs beside the others.
fn config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    // Writable: the bootstrap admin bearer token is minted under data_dir/pki.
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg
}

/// A client that authenticates as the bootstrap admin, so the scrape is
/// never refused by authorization.
fn admin_client(data_dir: &std::path::Path) -> reqwest::Client {
    let token = std::fs::read_to_string(data_dir.join("pki/admin.token"))
        .expect("the runtime minted the admin bearer token")
        .trim()
        .to_string();
    let mut auth = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
        .expect("a valid bearer header");
    auth.set_sensitive(true);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, auth);
    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("the admin client builds")
}

/// One `GET /metrics` against the running daemon.
async fn scrape(client: &reqwest::Client, rt: &Runtime) -> String {
    let resp = client
        .get(format!("http://{}/metrics", rt.local_addr()))
        .send()
        .await
        .expect("GET /metrics reaches the daemon");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET /metrics");
    resp.text().await.expect("the scrape body reads")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shadow_refusal_counted_in_the_runtime_ledger_is_on_the_daemons_scrape() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = Runtime::start(config(dir.path()))
        .await
        .expect("the runtime boots");
    let client = admin_client(dir.path());

    let before = scrape(&client, &rt).await;
    // An empty family renders nothing, so the precondition is that this is
    // the apiserver's exposition at all, not that the family is present.
    assert!(
        before.contains("# TYPE apiserver_registered_resources gauge"),
        "HARNESS PRECONDITION: this is the apiserver's /metrics; got:\n{before}"
    );
    assert!(
        !before.contains(PROBE_GATE.name()),
        "nothing has judged the probe gate yet; got:\n{before}"
    );

    // A gate owned by some component other than the apiserver, judged
    // against the ledger the runtime hands such components.
    let first = PROBE_GATE.judge(Err(ProbeFailed), rt.would_reject(), &"default/probe-a");
    assert_eq!(first, Ok(Proceed::Shadowed(ProbeFailed)));

    let after_one = scrape(&client, &rt).await;
    assert!(
        after_one.contains(&probe_sample(1)),
        "a refusal a Shadow gate counted into the runtime's ledger must be on the \
         daemon's /metrics: the apiserver must render the ONE ledger, not a private \
         one; got:\n{after_one}"
    );

    // The scrape follows the ledger itself, not a copy taken at boot.
    let second = PROBE_GATE.judge(Err(ProbeFailed), rt.would_reject(), &"default/probe-b");
    assert_eq!(second, Ok(Proceed::Shadowed(ProbeFailed)));
    let after_two = scrape(&client, &rt).await;
    assert!(
        after_two.contains(&probe_sample(2)),
        "the second refusal is on the next scrape; got:\n{after_two}"
    );

    rt.shutdown().await.expect("clean shutdown");
}
