//! The supervisor, end to end: a daemon that stays up above its runtime.
//!
//! Each test runs a real [`Supervisor`] over a throwaway data directory and
//! drives it the way an operator (or a service manager's signal) would,
//! reading the published [`Snapshot`] — never the loop — to see where it is.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use engenho_config::{ConfigError, EngenhoConfig, KubeletBackendKind};
use engenho_runtime::boot::BootPhase;
use engenho_runtime::lifecycle::{
    AttemptResult, CommandError, ControlDir, ExitIntent, Hold, LifecycleState, PreviousRun,
    RefusedBecause, ResolvedConfig, RetryClass, RunMarker, Snapshot, StopReason, Supervisor,
    SupervisorConfig, SupervisorError, SupervisorHandle,
};
use shikumi::TieredConfig;
use tokio::task::JoinHandle;

const PATIENCE: Duration = Duration::from_secs(30);

fn config(data_dir: &Path, durable: bool) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.kubelet_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.etcd_listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = durable;
    cfg.runtime.node_name = "node-supervised".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

/// A config source the test can change between boots: `None` does not
/// resolve.
#[derive(Clone)]
struct Switch(Arc<Mutex<Option<EngenhoConfig>>>);

impl Switch {
    fn new(config: Option<EngenhoConfig>) -> Self {
        Self(Arc::new(Mutex::new(config)))
    }

    fn set(&self, config: Option<EngenhoConfig>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = config;
    }

    fn source(&self) -> engenho_runtime::lifecycle::ConfigSource {
        let cell = Arc::clone(&self.0);
        engenho_runtime::lifecycle::ConfigSource::new(move || {
            cell.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
                .map(ResolvedConfig::untracked)
                .ok_or_else(|| ConfigError::Parse("the test's config does not resolve".into()))
        })
    }
}

fn supervise(
    data_dir: &Path,
    source: engenho_runtime::lifecycle::ConfigSource,
    declared: Option<PathBuf>,
) -> (JoinHandle<ExitIntent>, SupervisorHandle) {
    let (supervisor, handle) = Supervisor::new(SupervisorConfig {
        data_dir: data_dir.to_path_buf(),
        source,
        backend: None,
        declared,
    })
    .expect("the supervisor starts");
    (tokio::spawn(supervisor.run()), handle)
}

/// Wait until the published snapshot satisfies `pred`.
async fn until(
    handle: &SupervisorHandle,
    what: &str,
    pred: impl Fn(&Snapshot) -> bool,
) -> Snapshot {
    let mut rx = handle.watch();
    let waited = tokio::time::timeout(PATIENCE, async {
        loop {
            {
                let snapshot = rx.borrow_and_update();
                if pred(&snapshot) {
                    return snapshot.clone();
                }
            }
            rx.changed().await.expect("the supervisor is alive");
        }
    })
    .await;
    waited.unwrap_or_else(|_| {
        panic!(
            "timed out waiting for {what}; now {:?}",
            handle.snapshot().lifecycle
        )
    })
}

fn running(s: &Snapshot) -> bool {
    matches!(s.lifecycle, LifecycleState::Running { .. })
}

async fn exit(run: JoinHandle<ExitIntent>, handle: &SupervisorHandle, intent: ExitIntent) {
    handle.exit(intent).await.expect("exit accepted");
    let ended = tokio::time::timeout(PATIENCE, run)
        .await
        .expect("the supervisor ends")
        .expect("the supervisor task");
    assert_eq!(ended, intent);
}

/// A port someone else holds is waited out: the boot fails at the bind,
/// is retried on backoff, and comes up once the port is free — no process
/// exit, no service manager involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boot_that_cannot_bind_is_retried_until_the_port_frees() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let squatter = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let mut cfg = config(tmp.path(), false);
    cfg.runtime.listen_addr = squatter.local_addr().expect("addr").to_string();
    let (run, handle) = supervise(tmp.path(), Switch::new(Some(cfg)).source(), None);

    let failed = until(&handle, "a failed bind", |s| {
        matches!(s.lifecycle, LifecycleState::Failed { .. })
    })
    .await;
    let LifecycleState::Failed { report, retry, .. } = failed.lifecycle else {
        unreachable!()
    };
    assert_eq!(report.phase, BootPhase::BindApiserver);
    assert!(
        matches!(retry, RetryClass::Backoff { delay_ms: 1000, .. }),
        "{retry:?}"
    );

    drop(squatter);
    let up = until(&handle, "running", running).await;
    assert!(up.attempts.len() >= 2);
    let first = &up.attempts[0];
    assert_eq!(first.result, AttemptResult::Failed);
    assert_eq!(
        first.phases.last().map(|p| p.phase),
        Some(BootPhase::BindApiserver)
    );
    let last = up.attempts.last().expect("an attempt");
    assert_eq!(last.result, AttemptResult::Succeeded);
    assert_eq!(
        last.phases.iter().map(|p| p.phase).collect::<Vec<_>>(),
        BootPhase::ALL.to_vec()
    );

    exit(run, &handle, ExitIntent::Halt).await;
    let control = ControlDir::under(tmp.path());
    assert_eq!(control.read_journal().attempts().len(), up.attempts.len());
    assert!(matches!(
        control.read_run(),
        Some(RunMarker::Released { .. })
    ));
}

/// A config that does not resolve is held — no timer retries it — until an
/// operator retries after fixing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_that_does_not_resolve_is_held_until_retried() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let switch = Switch::new(None);
    let (run, handle) = supervise(tmp.path(), switch.source(), None);

    let held = until(&handle, "a held failure", |s| {
        matches!(s.lifecycle, LifecycleState::Failed { .. })
    })
    .await;
    assert!(matches!(
        &held.lifecycle,
        LifecycleState::Failed {
            report,
            retry: RetryClass::Hold,
            ..
        } if report.phase == BootPhase::ResolveConfig
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        handle.snapshot().attempts.len(),
        1,
        "a held failure is not retried on a timer"
    );

    switch.set(Some(config(tmp.path(), false)));
    handle.retry().await.expect("retry accepted");
    let up = until(&handle, "running", running).await;
    assert_eq!(up.attempts.len(), 2);
    exit(run, &handle, ExitIntent::Halt).await;
}

/// Stop and start without leaving the process: the store is released between
/// them, and the second boot is the next attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runtime_stops_and_starts_again_in_the_same_daemon() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (run, handle) = supervise(
        tmp.path(),
        Switch::new(Some(config(tmp.path(), true))).source(),
        None,
    );
    until(&handle, "running", running).await;

    let refused = handle.start().await.expect_err("already running");
    assert!(
        matches!(refused, CommandError::Refused(r) if r.reason == RefusedBecause::RuntimeRunning)
    );

    let stopped = handle.stop(Hold::None).await.expect("stop");
    assert_eq!(stopped.epoch, 1);
    assert!(matches!(
        stopped.lifecycle,
        LifecycleState::Stopped {
            reason: StopReason::OperatorRequest,
            epoch: 1,
            ..
        }
    ));
    engenho_runtime::StoreReleased::probe(tmp.path(), true).expect("the stop released the store");

    handle.start().await.expect("start");
    let up = until(&handle, "running again", running).await;
    assert!(matches!(up.lifecycle, LifecycleState::Running { attempt, .. } if attempt.get() == 2));
    exit(run, &handle, ExitIntent::Relaunch).await;
}

/// A stop held across relaunch survives the process: the next daemon comes
/// up Stopped, knows the last one ended cleanly, and boots when started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_keeps_a_relaunched_daemon_stopped_until_started() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let source = Switch::new(Some(config(tmp.path(), true))).source();
    {
        let (run, handle) = supervise(tmp.path(), source.clone(), None);
        let up = until(&handle, "running", running).await;
        assert_eq!(up.previous_run, PreviousRun::FirstEver);
        assert!(up.identity.is_some(), "the first boot records its identity");
        handle.stop(Hold::AcrossRelaunch).await.expect("stop");
        exit(run, &handle, ExitIntent::Relaunch).await;
    }
    let control = ControlDir::under(tmp.path());
    assert!(control.is_held());

    let (run, handle) = supervise(tmp.path(), source, None);
    let held = until(&handle, "held at startup", |s| {
        matches!(
            s.lifecycle,
            LifecycleState::Stopped {
                reason: StopReason::HeldAtStartup,
                ..
            }
        )
    })
    .await;
    assert!(matches!(held.previous_run, PreviousRun::CleanStop { .. }));
    assert!(held.identity.is_some(), "the identity outlives the process");

    handle.start().await.expect("start");
    assert!(!control.is_held(), "starting clears the hold");
    until(&handle, "running", running).await;
    exit(run, &handle, ExitIntent::Halt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_daemon_per_data_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let source = Switch::new(None).source();
    let (_first, _handle) = Supervisor::new(SupervisorConfig {
        data_dir: tmp.path().to_path_buf(),
        source: source.clone(),
        backend: None,
        declared: None,
    })
    .expect("the first daemon");
    let second = Supervisor::new(SupervisorConfig {
        data_dir: tmp.path().to_path_buf(),
        source,
        backend: None,
        declared: None,
    });
    assert!(matches!(second, Err(SupervisorError::Locked(_))));
}

/// The data directory is fixed for the life of the daemon: a config that
/// moves it is a held failure, not a boot over a store the control state
/// knows nothing about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_that_moves_the_data_dir_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let (run, handle) = supervise(
        tmp.path(),
        Switch::new(Some(config(elsewhere.path(), false))).source(),
        None,
    );
    let failed = until(&handle, "a held failure", |s| {
        matches!(s.lifecycle, LifecycleState::Failed { .. })
    })
    .await;
    let LifecycleState::Failed { report, retry, .. } = failed.lifecycle else {
        unreachable!()
    };
    assert_eq!(report.phase, BootPhase::ResolveConfig);
    assert!(report.error.contains("data_dir"), "{}", report.error);
    assert_eq!(retry, RetryClass::Hold);
    exit(run, &handle, ExitIntent::Halt).await;
}

/// A boot held on its config is retried when the declared file changes —
/// the fix lands through the file (a Nix rebuild, an edit), and the daemon
/// notices without being asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_to_the_declared_file_retries_a_held_boot() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let declared = tmp.path().join("engenho.yaml");
    std::fs::write(&declared, "broken\n").expect("write");
    let good = config(tmp.path(), false);
    let file = declared.clone();
    let source = engenho_runtime::lifecycle::ConfigSource::new(move || {
        let text = std::fs::read_to_string(&file).map_err(|e| ConfigError::Parse(e.to_string()))?;
        if text.trim() == "fixed" {
            Ok(ResolvedConfig::untracked(good.clone()))
        } else {
            Err(ConfigError::Parse(format!("not fixed: {text:?}")))
        }
    });
    let (run, handle) = supervise(tmp.path(), source, Some(declared.clone()));
    until(&handle, "a held failure", |s| {
        matches!(
            s.lifecycle,
            LifecycleState::Failed {
                retry: RetryClass::Hold,
                ..
            }
        )
    })
    .await;

    std::fs::write(&declared, "fixed\n").expect("fix the file");
    until(&handle, "running", running).await;
    exit(run, &handle, ExitIntent::Halt).await;
}

/// The same fix, landing WHILE the boot that will fail is still reading —
/// the case the watcher cannot report, because `ConfigChanged` is refused in
/// `Booting`. The supervisor must settle against the file as it is when the
/// failure lands, not against the events it happened to receive.
///
/// This is a real shape, not a contrived one: on Linux `std::fs::write`
/// truncates and then writes, and inotify reports the truncation on its own,
/// so a boot woken by it reads a file the writer has not finished. That is
/// how `control_uds` failed on ubuntu while every darwin run passed.
///
/// TIER — this is a LINUX-side gate, and measured to be vacuous on darwin:
/// with the fix reverted it still passes here, because FSEvents coalesces the
/// write and delivers one event after the writer has closed, late enough that
/// the edge-triggered path accepts it. On Linux the same revert holds the
/// daemon and this times out. Do not read a green run on a Mac as evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_that_lands_during_a_failing_boot_is_not_lost() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let declared = tmp.path().join("engenho.yaml");
    std::fs::write(&declared, "broken\n").expect("write");
    let good = config(tmp.path(), false);
    let file = declared.clone();
    let (reading_tx, reading_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    // Taken by the FIRST resolve only; every later one runs straight through.
    let gate = Mutex::new(Some((reading_tx, go_rx)));
    let source = engenho_runtime::lifecycle::ConfigSource::new(move || {
        let text = std::fs::read_to_string(&file).map_err(|e| ConfigError::Parse(e.to_string()))?;
        if let Some((reading, go)) = gate.lock().unwrap_or_else(PoisonError::into_inner).take() {
            let _ = reading.send(());
            let _ = go.recv();
        }
        if text.trim() == "fixed" {
            Ok(ResolvedConfig::untracked(good.clone()))
        } else {
            Err(ConfigError::Parse(format!("not fixed: {text:?}")))
        }
    });
    let (run, handle) = supervise(tmp.path(), source, Some(declared.clone()));

    tokio::task::spawn_blocking(move || reading_rx.recv())
        .await
        .expect("join")
        .expect("the first attempt read the declared file");
    // Mid-boot: this write's event reaches a supervisor in `Booting`, which
    // refuses it. Nothing else will arrive.
    std::fs::write(&declared, "fixed\n").expect("fix the file mid-boot");
    let _ = go_tx.send(());

    until(&handle, "running", running).await;
    exit(run, &handle, ExitIntent::Halt).await;
}
