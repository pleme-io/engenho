//! A boot, phase by phase.
//!
//! * A successful boot enters every [`BootPhase`], each once, in exactly
//!   [`BootPhase::ALL`] order — so a step added to the boot without a phase,
//!   or a phase no step enters, fails here.
//! * A boot stopped before it opens the store stops at once and opened
//!   nothing; one stopped after it opened the store stops at the next phase
//!   boundary and releases it.
//! * A failed boot names the phase it failed in.

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_runtime::boot::{BootKind, BootPhase, BootProgress, BootRecorder};
use engenho_runtime::{BootUnwind, Runtime, RuntimeError};
use engenho_serve::stop_channel;
use shikumi::TieredConfig;
use tokio::sync::mpsc;

fn config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = false;
    cfg.runtime.node_name = "node-phases".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

fn drain(rx: &mut mpsc::UnboundedReceiver<BootProgress>) -> (Vec<BootPhase>, Vec<BootKind>) {
    let mut phases = Vec::new();
    let mut kinds = Vec::new();
    while let Ok(report) = rx.try_recv() {
        match report {
            BootProgress::Entered { phase, .. } => phases.push(phase),
            BootProgress::Kind(kind) => kinds.push(kind),
        }
    }
    (phases, kinds)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successful_boot_enters_every_phase_once_in_order() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (_cancel, signal) = stop_channel();
    let mut rec = BootRecorder::new(tx, signal);
    let rt = Runtime::boot_recorded(config(tmp.path()), &mut rec)
        .await
        .expect("boots");
    let (phases, kinds) = drain(&mut rx);
    assert_eq!(phases, BootPhase::ALL.to_vec());
    assert_eq!(kinds, [BootKind::Ephemeral]);
    assert_eq!(rt.boot_kind(), BootKind::Ephemeral);
    assert_eq!(rec.current(), BootPhase::AdoptHealth);
    rt.shutdown().await.expect("stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_stopped_before_it_starts_opens_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (cancel, signal) = stop_channel();
    cancel.stop();
    let mut rec = BootRecorder::new(tx, signal);
    let Err(failed) = Runtime::boot_recorded(config(tmp.path()), &mut rec).await else {
        panic!("a stopped boot booted");
    };
    assert!(
        matches!(
            failed.error,
            RuntimeError::BootCancelled {
                phase: BootPhase::ResolveConfig
            }
        ),
        "{:?}",
        failed.error
    );
    assert_eq!(failed.phase, BootPhase::ResolveConfig);
    assert!(matches!(failed.unwind, BootUnwind::NeverOpened));
    assert!(drain(&mut rx).0.is_empty(), "no phase was entered");
}

/// Stopped as soon as it reports opening the store: the boot stops at the
/// next boundary it reaches, before the apiserver binds, and gives the store
/// back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_stopped_after_opening_the_store_releases_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = config(tmp.path());
    cfg.runtime.durable = true;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (cancel, signal) = stop_channel();
    let boot = tokio::spawn(async move {
        let mut rec = BootRecorder::new(tx, signal);
        Runtime::boot_recorded(cfg, &mut rec).await
    });
    loop {
        match rx.recv().await {
            Some(BootProgress::Entered {
                phase: BootPhase::OpenStore,
                ..
            }) => break,
            Some(_) => {}
            None => panic!("the boot ended before it opened the store"),
        }
    }
    cancel.stop();
    let Err(failed) = boot.await.expect("the boot task") else {
        panic!("the boot finished despite the stop");
    };
    let RuntimeError::BootCancelled { phase } = failed.error else {
        panic!("{:?}", failed.error);
    };
    assert!(
        phase >= BootPhase::OpenStore && phase < BootPhase::BindApiserver,
        "stopped at {phase}"
    );
    assert!(
        failed.unwind.store_released(),
        "the cancelled boot kept its store: {:?}",
        failed.unwind
    );
    engenho_runtime::StoreReleased::probe(tmp.path(), true).expect("nothing holds the store");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_that_cannot_bind_says_so() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let squatter = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let mut cfg = config(tmp.path());
    cfg.runtime.listen_addr = squatter.local_addr().expect("addr").to_string();
    let Err(failed) = Runtime::boot(cfg).await else {
        panic!("booted on a taken port");
    };
    assert_eq!(failed.phase, BootPhase::BindApiserver);
    assert!(failed.unwind.store_released());
}
