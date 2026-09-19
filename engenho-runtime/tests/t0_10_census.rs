//! T0.10 — the census over its two real sources.
//!
//! | source | what must hold |
//! |---|---|
//! | a data directory | the objects are the ones the store boots with; the directory given is byte-for-byte untouched; the image census answers (zero is an answer) |
//! | a running apiserver | discovery resolves every kind; a LIST across namespaces is judged; a kind the server does not serve is refused, not counted as zero; the image census is refused |
//!
//! Each node lifetime runs on its own tokio runtime ([`lifetime`]) and ends by
//! dropping it, so no task of one lifetime (a store's state-machine worker, a
//! listener) can still be writing while the next one reads the directory.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_kube_client::Connection;
use engenho_runtime::Runtime;
use engenho_runtime::census::{
    self, ApiSource, CensusError, DataDirSource, Finding, Gvk, Predicate, Source as _, Subject,
};
use engenho_store::{
    InProcessRouter, Reason, ResourceCommand, ResourceKey, StoreMesh, default_config,
};
use engenho_types::auth::{KubeAuth, TokenSource};
use serde_json::{Value, json};
use shikumi::TieredConfig;

const LEADERSHIP: Duration = Duration::from_secs(10);

/// One lifetime on its own runtime; dropping the runtime at the end drops
/// every task it spawned.
fn lifetime<F: Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build the runtime for one lifetime");
    let out = rt.block_on(f);
    rt.shutdown_timeout(Duration::from_secs(5));
    out
}

fn put(key: ResourceKey, value: Value) -> ResourceCommand {
    ResourceCommand::put(key, value, Reason::Operator)
}

/// A pod no create path would store today: `restartPolicy: Sometimes`.
fn sometimes_pod() -> (ResourceKey, Value) {
    (
        ResourceKey::namespaced("", "v1", "Pod", "team", "sometimes"),
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "sometimes", "namespace": "team"},
            "spec": {
                "restartPolicy": "Sometimes",
                "containers": [{"name": "c", "image": "busybox"}]
            }
        }),
    )
}

/// A Deployment no `ReplicaSet` runs: its controller would create one.
fn orphan_deployment() -> (ResourceKey, Value) {
    (
        ResourceKey::namespaced("apps", "v1", "Deployment", "team", "web"),
        json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "team", "uid": "u-web"},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "web:1"}]}
                }
            }
        }),
    )
}

/// Write `objects` into a durable store under `data_dir/store`, then stop
/// it cleanly — a node's data directory as a census would be handed it.
fn seed_data_dir(data_dir: &Path, objects: Vec<(ResourceKey, Value)>) {
    lifetime(async {
        let (mesh, _) = StoreMesh::start_or_resume(
            1,
            "in-process://seed".to_owned(),
            InProcessRouter::new(),
            default_config("engenho-census-seed").expect("config"),
            data_dir.join("store"),
        )
        .await
        .expect("boot the seed store");
        assert!(
            mesh.wait_for_leadership(LEADERSHIP).await,
            "seed leadership"
        );
        for (key, value) in objects {
            mesh.propose(put(key, value)).await.expect("seed an object");
        }
        mesh.terminate().await.expect("stop the seed store");
    });
}

/// Every file under `dir`, by relative path, with its bytes.
fn tree(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).expect("read a directory of the tree") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let bytes = std::fs::read(&path).expect("read a file of the tree");
                let rel = path.strip_prefix(root).expect("under the root").to_owned();
                out.insert(rel, bytes);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

fn subjects(report: &census::Report) -> Vec<String> {
    report
        .matches()
        .iter()
        .map(|m| m.subject.to_string())
        .collect()
}

// ── a data directory ─────────────────────────────────────────────────────

/// The census reads a data directory's objects as the store boots them, and
/// leaves the directory it was given exactly as it found it: every file,
/// every byte. The boot's own writes (its lock, openraft's vote, a leader's
/// blank entry) land in a private copy.
#[test]
fn a_data_dir_census_reads_the_booted_store_and_writes_nothing_there() {
    let tmp = tempfile::tempdir().expect("tempdir");
    seed_data_dir(tmp.path(), vec![sometimes_pod(), orphan_deployment()]);
    let before = tree(tmp.path());
    assert!(
        before.keys().any(|p| p.starts_with("store")),
        "HARNESS PRECONDITION: the seed wrote a store: {:?}",
        before.keys().collect::<Vec<_>>()
    );

    let (rejects, rolls, image) = lifetime(async {
        let source = DataDirSource::open(tmp.path())
            .await
            .expect("the copy of a stopped node's store boots");
        let rejects = census::run(Predicate::StoredWouldReject, &source).await;
        let rolls = census::run(Predicate::DeploymentWouldRoll, &source).await;
        let image = census::run(Predicate::StoreImageInconsistent, &source).await;
        source.close().await.expect("the private store stops");
        (rejects, rolls, image)
    });

    let rejects = rejects.expect("stored-would-reject answers");
    assert_eq!(subjects(&rejects), ["v1/Pod team/sometimes"]);
    assert!(matches!(
        &rejects.matches()[0].finding,
        Finding::Invalid(v) if v.field == "spec.restartPolicy"
    ));
    assert!(
        rejects
            .examined()
            .contains_key(&Gvk::new("apps", "v1", "Deployment")),
        "every stored kind is judged, not only the ones a validator covers"
    );

    let rolls = rolls.expect("deployment-would-roll answers");
    assert_eq!(subjects(&rolls), ["apps/v1/Deployment team/web"]);

    let image = image.expect("a data directory answers for its image");
    assert!(
        image.matches().is_empty(),
        "a cleanly stopped store's image agrees with itself: {image}"
    );

    assert_eq!(
        tree(tmp.path()),
        before,
        "the census wrote into the data directory it was given"
    );
}

/// A directory with no store is refused, not counted as an empty cluster.
#[test]
fn a_data_dir_without_a_store_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let err = lifetime(async { DataDirSource::open(tmp.path()).await.err() })
        .expect("a directory with no store does not open");
    assert!(matches!(err, CensusError::NoStore { .. }), "{err}");
}

// ── a running apiserver ──────────────────────────────────────────────────

fn node_config(data_dir: &Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.cluster.name = "engenho-census".into();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-census".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg
}

/// Over LIST: discovery resolves the kinds, every namespace is read, and
/// an object stored without passing today's validation is found. A kind
/// the server does not serve is refused; so is the image census.
#[test]
fn an_apiserver_census_lists_every_namespace_through_discovery() {
    let tmp = tempfile::tempdir().expect("tempdir");
    lifetime(async {
        let rt = Runtime::start(node_config(tmp.path()))
            .await
            .expect("the runtime boots");
        let bad_service = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "bogus", "namespace": "elsewhere"},
            "spec": {"type": "Bogus", "ports": [{"port": 80}]}
        });
        rt.store()
            .propose(put(
                ResourceKey::namespaced("", "v1", "Service", "elsewhere", "bogus"),
                bad_service,
            ))
            .await
            .expect("store a service the create path would refuse");

        let base = ["http://", &rt.local_addr().to_string()].concat();
        let auth = KubeAuth::BearerToken(TokenSource::File {
            path: tmp.path().join("pki/admin.token"),
        });
        let source = ApiSource::connect(Connection::new(&base, auth, None).expect("connection"))
            .await
            .expect("discovery answers");

        let report = census::run(Predicate::StoredWouldReject, &source)
            .await
            .expect("stored-would-reject answers over LIST");
        let services: Vec<&Subject> = report
            .matches()
            .iter()
            .map(|m| &m.subject)
            .filter(|s| s.to_string().starts_with("v1/Service "))
            .collect();
        assert_eq!(
            services.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["v1/Service elsewhere/bogus"]
        );
        assert!(
            report.examined().contains_key(&Gvk::new(
                "rbac.authorization.k8s.io",
                "v1",
                "ClusterRole"
            )),
            "discovery resolved the group kinds, not only the core ones"
        );
        // Every kind discovery lists is either judged or named as refused:
        // none drops out of the report as if it held nothing.
        for gvk in source.kinds().await.expect("the discovered kinds") {
            assert!(
                report.examined().contains_key(&gvk) != report.unlisted().contains_key(&gvk),
                "{gvk} is neither judged nor named as refused, or is both"
            );
        }

        let not_served = source
            .list(&Gvk::new("example.com", "v1", "Nope"))
            .await
            .expect_err("an unserved kind is refused");
        assert!(
            matches!(not_served, CensusError::KindNotServed { .. }),
            "{not_served}"
        );
        let image = census::run(Predicate::StoreImageInconsistent, &source)
            .await
            .expect_err("an apiserver has no store image to read");
        assert!(matches!(image, CensusError::NeedsDataDir { .. }), "{image}");

        // RBAC over the bootstrap roles: it must answer, whatever it finds.
        census::run(Predicate::RbacBareParentOnly, &source)
            .await
            .expect("rbac-bare-parent-only answers over LIST");

        drop(source);
        rt.shutdown().await.expect("the runtime stops");
    });
}
