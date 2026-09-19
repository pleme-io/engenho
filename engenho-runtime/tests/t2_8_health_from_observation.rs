//! I10 / T2.8 — a running daemon's health and metrics are derived from what
//! its children did, not asserted.
//!
//! The apiserver's `/livez`, `/healthz` and `/readyz` answered the constant
//! `"ok"` and `/metrics` a literal store revision of `0`. The apiserver half
//! of T2.8 made each of them a read through a `LivenessSource` /
//! `MetricsSource`, and a router with neither installed fails every health
//! check (`[-]liveness-source`) and omits the runtime's metric families. What
//! is pinned here is that the runtime installs both, over the children it
//! actually spawned:
//!
//! * `/livez` and `/healthz` list every spawned child by name and pass once
//!   each has beaten; `/readyz` adds a store read and the drain check.
//! * `/metrics` carries the store's real revision, the panic count, and for
//!   every driver its reconciles by result and its last tick.
//!
//! Tier: only-mitigated. `RouterState` still builds without the sources (and
//! then fails every check), so forgetting them is caught here, not made
//! unrepresentable. Stalled and dead children are pinned in the runtime's
//! `health` unit tests, which can stop a child's heartbeat; a booted daemon's
//! drivers cannot be made to stall from outside.

use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::{Child, Runtime};
use engenho_substrate::Liveness;
use reqwest::StatusCode;
use shikumi::TieredConfig;

/// How long a freshly booted daemon may take for every child to beat once.
/// Each driver ticks within one fallback (1 s here) of subscribing; the rest
/// is boot and CI slack.
const ALL_BEATEN: Duration = Duration::from_secs(30);

/// Ephemeral store, fake kubelet backend, plaintext, every listener on an
/// ephemeral loopback port so this binary runs beside the others, and a 1 s
/// fallback so an idle driver's first tick comes quickly.
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
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.controllers.fallback_interval_seconds = 1;
    cfg
}

/// A client that authenticates as the bootstrap admin, so no request here
/// is refused by authorization.
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

async fn get(client: &reqwest::Client, rt: &Runtime, path: &str) -> (StatusCode, String) {
    let resp = client
        .get(format!("http://{}{path}", rt.local_addr()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {path} reaches the daemon: {e}"));
    let status = resp.status();
    (status, resp.text().await.expect("the body reads"))
}

/// Every child the runtime spawned, by the name its check carries.
fn spawned(rt: &Runtime) -> Vec<&'static str> {
    rt.children()
        .iter()
        .map(|(child, _)| child.name())
        .collect()
}

/// Poll `/livez?verbose` until it passes, or fail with its last answer.
async fn until_live(client: &reqwest::Client, rt: &Runtime) -> String {
    let deadline = Instant::now() + ALL_BEATEN;
    loop {
        let (status, body) = get(client, rt, "/livez?verbose").await;
        if status == StatusCode::OK {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "/livez never passed within {ALL_BEATEN:?}; last answer {status}:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn boot(dir: &tempfile::TempDir) -> (Runtime, reqwest::Client) {
    let rt = Runtime::start(config(dir.path()))
        .await
        .expect("the runtime boots");
    let client = admin_client(dir.path());
    (rt, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_health_endpoint_is_derived_from_the_children_the_daemon_spawned() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (rt, client) = boot(&dir).await;
    let names = spawned(&rt);
    assert!(
        names.len() > 3,
        "HARNESS PRECONDITION: the daemon spawned its catalog: {names:?}"
    );

    let livez = until_live(&client, &rt).await;
    for name in &names {
        assert!(
            livez.contains(&format!("[+]{name} ok\n")),
            "/livez carries a passing check for the spawned child {name}; got:\n{livez}"
        );
    }
    assert_eq!(
        livez.lines().filter(|l| l.starts_with("[+]")).count(),
        names.len(),
        "one check per spawned child, and nothing else; got:\n{livez}"
    );
    assert!(livez.ends_with("livez check passed\n"), "got:\n{livez}");

    let (status, healthz) = get(&client, &rt, "/healthz?verbose").await;
    assert_eq!(status, StatusCode::OK, "/healthz:\n{healthz}");
    assert!(healthz.contains("[+]kubelet ok\n"), "got:\n{healthz}");

    let (status, readyz) = get(&client, &rt, "/readyz?verbose").await;
    assert_eq!(status, StatusCode::OK, "/readyz:\n{readyz}");
    assert!(
        readyz.starts_with("[+]store ok\n"),
        "readiness reads the store first; got:\n{readyz}"
    );
    assert!(readyz.contains("[+]shutdown ok\n"), "got:\n{readyz}");

    // The rows the endpoints rendered, read directly.
    let rows = rt.health().liveness();
    assert_eq!(rows.len(), names.len());
    assert!(
        rows.iter().all(|(_, l)| *l == Liveness::Alive),
        "every child alive once /livez passed: {rows:?}"
    );
    assert!(
        rows.iter().any(|(c, _)| matches!(c, Child::Listener(_))),
        "listeners are judged too: {rows:?}"
    );

    drop(client);
    rt.shutdown().await.expect("clean shutdown");
}

/// The value of the sample whose series (name plus any labels) is exactly
/// `series` on the scrape.
fn sample(scrape: &str, series: &str) -> Option<f64> {
    scrape
        .lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|v| v.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_daemons_scrape_carries_the_stores_revision_and_every_drivers_ticks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (rt, client) = boot(&dir).await;
    // Every driver has ticked once /livez passes.
    until_live(&client, &rt).await;

    let before = rt.store().current_revision().await.get();
    let (status, scrape) = get(&client, &rt, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let after = rt.store().current_revision().await.get();

    #[allow(clippy::cast_precision_loss)]
    let (before, after) = (before as f64, after as f64);
    let revision = sample(&scrape, "engenho_store_revision")
        .unwrap_or_else(|| panic!("engenho_store_revision is on the scrape; got:\n{scrape}"));
    assert!(
        revision > 0.0 && (before..=after).contains(&revision),
        "the scrape reads the store's revision (between {before} and {after}), not a literal; \
         got {revision}"
    );
    assert!(
        sample(&scrape, "engenho_panics_total").is_some(),
        "the panic count is on the scrape; got:\n{scrape}"
    );

    let drivers: Vec<&str> = rt
        .children()
        .iter()
        .filter(|(c, _)| matches!(c, Child::Driver(_)))
        .map(|(c, _)| c.name())
        .collect();
    assert!(!drivers.is_empty(), "HARNESS PRECONDITION: drivers spawned");
    for name in drivers {
        let reconciles: f64 = ["success", "error", "requeue_after"]
            .iter()
            .map(|result| {
                let series =
                    format!("controller_runtime_reconcile_total{{controller=\"{name}\",result=\"{result}\"}}");
                sample(&scrape, &series).unwrap_or_else(|| {
                    panic!("{series} is on the scrape; got:\n{scrape}")
                })
            })
            .sum();
        assert!(
            reconciles >= 1.0,
            "{name} has ticked, so it has counted a reconcile; got:\n{scrape}"
        );
        let last_tick =
            format!("engenho_controller_last_tick_timestamp_seconds{{controller=\"{name}\"}}");
        let at = sample(&scrape, &last_tick)
            .unwrap_or_else(|| panic!("{last_tick} is on the scrape; got:\n{scrape}"));
        assert!(
            at > 1_700_000_000.0,
            "{name}'s last tick is a wall-clock Unix time; got {at}"
        );
    }

    drop(client);
    rt.shutdown().await.expect("clean shutdown");
}
