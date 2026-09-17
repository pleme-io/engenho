//! The goal, in miniature: Postgres under engenho's kubelet, NATIVELY.
//!
//! ryn runs Postgres today as `docker.io/library/postgres:16-alpine` inside a
//! Fedora VM (`podman info` → `linux/arm64 kernel=6.17.7-300.fc43.aarch64`).
//! This test runs the same database, same major version, as a Mach-O process on
//! the host — driven through the same `ContainerRuntime` trait the kubelet
//! calls, from a pod spec with a volume.
//!
//! It is deliberately the WHOLE path and not a unit of it: image classification,
//! mount verification, spawn, liveness, logs, and a real client query. A
//! backend that starts a process is not the same claim as a backend that runs a
//! database.
//!
//! ## The 103-byte socket limit is why the paths here are short
//!
//! A unix socket path is capped at 103 bytes by `sockaddr_un`. Measured while
//! building this: a pod state directory under a long path silently exceeds it,
//! `pg_ctl` returns 1 and `psql` reports "Unix-domain socket path ... is too
//! long". Hence `/tmp/eng-pg-*` rather than a nested temp dir.

use engenho_kubelet::backend::{ContainerRuntime, ContainerSpec, LogOptions, PodIdentity};
use engenho_kubelet::native_backend::{Isolation, NativeBackend};
use engenho_kubelet::pod_volume::{MountSource, ResolvedMount};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn closure(attr: &str) -> PathBuf {
    let out = std::process::Command::new("nix")
        .args(["build", "--no-link", "--print-out-paths", attr])
        .output()
        .unwrap_or_else(|e| panic!("could not run nix for {attr}: {e}"));
    assert!(
        out.status.success(),
        "realising {attr} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // ★ A multi-output derivation prints SEVERAL paths, and the first is not
    // the package. postgresql_16 prints its `-man` output first; taking
    // `.next()` yielded a store path with no bin/ and an initdb that "does not
    // exist", which reads exactly like a broken closure. Select by what the
    // output actually CONTAINS rather than by position.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let path = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .find(|l| Path::new(l).join("bin").is_dir())
        .unwrap_or_else(|| panic!("no output of {attr} contains a bin/ directory; got:\n{stdout}"));
    PathBuf::from(path)
}

/// `initdb` the data directory, as an init container would.
fn initdb(pg: &Path, data: &Path) {
    if data.join("PG_VERSION").exists() {
        return;
    }
    let out = std::process::Command::new(pg.join("bin/initdb"))
        .args([
            "-D",
            &data.to_string_lossy(),
            "-U",
            "postgres",
            "--auth=trust",
        ])
        .output()
        .expect("run initdb");
    assert!(
        out.status.success(),
        "initdb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn postgres_runs_natively_under_the_kubelet_from_a_nix_closure() {
    let pg = closure("nixpkgs#postgresql_16");

    // Short paths on purpose — see the module docs on the 103-byte cap.
    let data = PathBuf::from("/tmp/eng-pg-data");
    let sock = PathBuf::from("/tmp/eng-pg-sock");
    let _ = std::fs::remove_dir_all(&data);
    std::fs::create_dir_all(&sock).expect("socket dir");
    initdb(&pg, &data);

    let backend = NativeBackend::new(Isolation::HostProcess, "/tmp/eng-pg-logs");

    let mut image = String::from("nix:");
    image.push_str(&pg.to_string_lossy());

    let spec = ContainerSpec {
        name: "postgres".to_string(),
        image,
        // The data directory is a VOLUME, declared at the path the workload
        // already expects — which is the only shape a native process can
        // honour, and which `verify_mounts` enforces.
        mounts: vec![ResolvedMount {
            source: MountSource::HostDir(data.clone()),
            mount_path: data.to_string_lossy().into_owned(),
            read_only: false,
            sub_path: None,
        }],
        command: vec![
            "postgres".to_string(),
            "-D".to_string(),
            data.to_string_lossy().into_owned(),
            "-p".to_string(),
            "15433".to_string(),
            "-k".to_string(),
            sock.to_string_lossy().into_owned(),
        ],
        env: BTreeMap::new(),
        pod: PodIdentity {
            namespace: "pangea-system".to_string(),
            name: "pangea-postgres-native-0".to_string(),
            container_name: "postgres".to_string(),
            ..PodIdentity::default()
        },
        ..ContainerSpec::default()
    };

    let started = backend.start(&spec).await.expect("postgres must start");
    assert!(started.running);

    // Wait for it to accept connections, bounded — never an unbounded wait, and
    // never a fixed sleep that passes on a fast machine and flakes on a slow one.
    let psql = pg.join("bin/psql");
    let mut answered = None;
    for _ in 0..100 {
        let out = std::process::Command::new(&psql)
            .args([
                "-h",
                &sock.to_string_lossy(),
                "-p",
                "15433",
                "-U",
                "postgres",
                "-tAc",
                "select version()",
            ])
            .output();
        if let Ok(o) = out {
            if o.status.success() {
                answered = Some(String::from_utf8_lossy(&o.stdout).trim().to_string());
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let version = answered.unwrap_or_else(|| {
        let logs = futures_lite_block(&backend, &started.container_id);
        panic!("postgres never answered a query. container logs:\n{logs}")
    });

    // ★ THE CLAIM. Not "a process started" — a real PostgreSQL answered a real
    // query, and it is a darwin binary, so nothing here went through a VM.
    assert!(
        version.contains("PostgreSQL 16"),
        "expected PostgreSQL 16, got: {version}"
    );
    assert!(
        version.contains("darwin"),
        "the server must be a DARWIN build — if this says linux, the workload \
         is in a VM and the whole point of this backend is unmet: {version}"
    );

    // Still tracked as running while it serves.
    let live = backend
        .status(&started.container_id)
        .await
        .expect("status")
        .expect("tracked");
    assert!(
        live.running,
        "postgres must still be running while it serves"
    );

    // SIGTERM is Postgres's "smart shutdown"; the process must actually go away.
    backend.stop(&started.container_id).await.expect("stop");
    let mut stopped = false;
    for _ in 0..100 {
        let s = backend
            .status(&started.container_id)
            .await
            .expect("status")
            .expect("tracked");
        if !s.running {
            stopped = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(stopped, "SIGTERM must actually stop the database");

    backend.remove(&started.container_id).await.expect("remove");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&sock);
}

/// Read a container's logs from a sync context, for the panic path above.
fn futures_lite_block(backend: &NativeBackend, id: &str) -> String {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            backend
                .logs(id, &LogOptions::default())
                .await
                .unwrap_or_else(|e| e.to_string())
        })
    })
}
