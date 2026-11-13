//! How a `native` workload outlives the daemon that started it, and how the
//! next daemon takes it back.
//!
//! Under [`WorkloadLifetime::Outlive`] every workload is recorded on disk
//! ([`Record`]) with the identity of its process: the pid, the kernel's start
//! time for that pid, and the boot it ran in. Its stdout and stderr are FIFOs
//! the workload holds open for reading as well as writing, so the stream
//! never loses its last reader and a write never fails while no daemon reads
//! it. The next backend adopts a record only when a pidfd opened on the pid
//! names a process with the recorded start time in the recorded boot, so a
//! reused pid is never adopted. Linux only: the identity and the pidfd are
//! Linux facts, and every other platform refuses the lifetime at
//! construction ([`crate::config_bridge::BackendRefused::OutliveUnsupported`]).
//!
//! A record keeps a digest of the container's spec, never the spec: the spec's
//! `env` holds resolved Secret values, and a Secret stays sealed at rest. It
//! keeps the [`Verdict`] its process entered under, so a command run into an
//! adopted workload joins the same enclosure, and a workload the node would now
//! enclose differently is replaced rather than claimed. The directory is `0700`
//! and each record `0600`, since an enclosure can leave `/var/lib` visible.

use crate::backend::{Confinement, ContainerSpec, ContainerStdio, Resources};
use crate::cri_log::LogStream;
use crate::kakoi::Verdict;
use crate::pod_volume::ResolvedMount;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Whether a native workload outlives the daemon that started it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum WorkloadLifetime {
    /// The workload ends with the daemon and is started again by the next
    /// one.
    #[default]
    Bound,
    /// The workload outlives the daemon and is adopted by the next one.
    /// `state` holds one record per workload.
    Outlive {
        /// Where the records and the stdio FIFOs live.
        state: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Projection {
    image: String,
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    mounts: Vec<ResolvedMount>,
    confinement: Confinement,
    resources: Resources,
    stdio: ContainerStdio,
}

impl Projection {
    pub(crate) fn of(spec: &ContainerSpec) -> Self {
        Self {
            image: spec.image.clone(),
            argv: spec.argv(),
            env: spec.env.clone(),
            mounts: spec.mounts.clone(),
            confinement: spec.confinement.clone(),
            resources: spec.resources.clone(),
            stdio: spec.stdio,
        }
    }

    pub(crate) fn digest(&self) -> serde_json::Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub(crate) id: String,
    pub(crate) pod_uid: String,
    pub(crate) spec: String,
    pub(crate) verdict: Verdict,
    pub(crate) pid: u32,
    pub(crate) start_ticks: u64,
    pub(crate) boot_id: String,
    pub(crate) program: PathBuf,
    pub(crate) log_path: PathBuf,
    pub(crate) grace_ms: u64,
    pub(crate) leaf: Option<String>,
    pub(crate) adoptable: bool,
}

impl Record {
    pub(crate) fn runs(&self, spec: &ContainerSpec) -> bool {
        self.pod_uid == spec.pod.uid
            && Projection::of(spec)
                .digest()
                .is_ok_and(|digest| digest == self.spec)
    }

    pub(crate) fn grace(&self) -> Duration {
        Duration::from_millis(self.grace_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) start_ticks: u64,
    pub(crate) boot_id: String,
}

pub(crate) fn identity(pid: u32) -> std::io::Result<Identity> {
    let stat = std::fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("stat"))?;
    let start_ticks = start_ticks_of(&stat).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "/proc/<pid>/stat carries no start time",
        )
    })?;
    Ok(Identity {
        start_ticks,
        boot_id: boot_id()?,
    })
}

fn boot_id() -> std::io::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned())
}

fn start_ticks_of(stat: &str) -> Option<u64> {
    let (_, after_comm) = stat.rsplit_once(')')?;
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[derive(Debug)]
pub(crate) struct Store {
    dir: PathBuf,
}

pub(crate) struct Fifos {
    pub(crate) stdout: std::fs::File,
    pub(crate) stderr: std::fs::File,
}

const PIPE_CAPACITY: usize = 1 << 20;

impl Store {
    pub(crate) fn open(dir: PathBuf) -> std::io::Result<Self> {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::create_dir_all(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        Ok(Self { dir })
    }

    fn record_path(&self, id: &str) -> PathBuf {
        self.dir.join([id, ".json"].concat())
    }

    fn fifo_path(&self, id: &str, stream: LogStream) -> PathBuf {
        self.dir.join([id, ".", stream.as_str()].concat())
    }

    pub(crate) fn write(&self, record: &Record) -> std::io::Result<()> {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let path = self.record_path(&record.id);
        let staged = self.dir.join([&record.id, ".json.new"].concat());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&staged)?;
        file.write_all(&serde_json::to_vec(record).map_err(std::io::Error::other)?)?;
        file.sync_all()?;
        std::fs::rename(&staged, &path)
    }

    pub(crate) fn forget(&self, id: &str) {
        for path in [
            self.record_path(id),
            self.fifo_path(id, LogStream::Stdout),
            self.fifo_path(id, LogStream::Stderr),
        ] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "a native workload's record was not removed");
                }
            }
        }
    }

    pub(crate) fn records(&self) -> std::io::Result<Vec<(PathBuf, Result<Record, String>)>> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
                continue;
            }
            let read = std::fs::read(&path)
                .map_err(|e| e.to_string())
                .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|e| e.to_string()));
            found.push((path, read));
        }
        Ok(found)
    }

    pub(crate) fn discard(path: &Path) {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(path = %path.display(), error = %e, "an unreadable native workload record was not removed");
        }
    }

    pub(crate) fn writers(&self, id: &str) -> std::io::Result<Fifos> {
        Ok(Fifos {
            stdout: self.writer(id, LogStream::Stdout)?,
            stderr: self.writer(id, LogStream::Stderr)?,
        })
    }

    pub(crate) fn readers(&self, id: &str) -> std::io::Result<Fifos> {
        Ok(Fifos {
            stdout: self.reader(id, LogStream::Stdout)?,
            stderr: self.reader(id, LogStream::Stderr)?,
        })
    }

    fn writer(&self, id: &str, stream: LogStream) -> std::io::Result<std::fs::File> {
        let path = self.fifo_path(id, stream);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        sys::mkfifo(&path)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        sys::widen(&file);
        Ok(file)
    }

    fn reader(&self, id: &str, stream: LogStream) -> std::io::Result<std::fs::File> {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(sys::nonblock())
            .open(self.fifo_path(id, stream))
    }
}

pub(crate) use sys::Handle;

#[cfg(target_os = "linux")]
mod sys {
    use super::{Record, identity};
    use rustix::fd::OwnedFd;
    use rustix::process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    pub(crate) fn nonblock() -> i32 {
        i32::try_from(rustix::fs::OFlags::NONBLOCK.bits()).unwrap_or(0)
    }

    pub(crate) fn mkfifo(path: &Path) -> std::io::Result<()> {
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            path,
            rustix::fs::Mode::from_raw_mode(0o600),
        )
        .map_err(std::io::Error::from)
    }

    pub(crate) fn widen(file: &std::fs::File) {
        if let Err(e) = rustix::pipe::fcntl_setpipe_size(file, super::PIPE_CAPACITY) {
            tracing::debug!(error = %e, "a native workload's stdio pipe keeps the kernel's default capacity");
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct Handle {
        pid: u32,
        fd: Arc<OwnedFd>,
    }

    impl Handle {
        pub(crate) fn of(record: &Record) -> Option<Self> {
            let raw = i32::try_from(record.pid).ok()?;
            let fd = pidfd_open(Pid::from_raw(raw)?, PidfdFlags::empty()).ok()?;
            let now = identity(record.pid).ok()?;
            let handle = Self {
                pid: record.pid,
                fd: Arc::new(fd),
            };
            (now.start_ticks == record.start_ticks
                && now.boot_id == record.boot_id
                && !handle.exited())
            .then_some(handle)
        }

        pub(crate) const fn pid(&self) -> u32 {
            self.pid
        }

        pub(crate) fn exited(&self) -> bool {
            let mut fds = [rustix::event::PollFd::new(
                self.fd.as_ref(),
                rustix::event::PollFlags::IN,
            )];
            let zero = rustix::event::Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            matches!(rustix::event::poll(&mut fds, Some(&zero)), Ok(n) if n > 0)
        }

        pub(crate) fn terminate_now(&self) -> bool {
            pidfd_send_signal(self.fd.as_ref(), Signal::TERM).is_ok()
        }

        pub(crate) fn kill_now(&self) -> bool {
            pidfd_send_signal(self.fd.as_ref(), Signal::KILL).is_ok()
        }

        pub(crate) async fn exit(&self) {
            while !self.exited() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    use super::Record;
    use std::path::Path;

    pub(crate) const fn nonblock() -> i32 {
        0
    }

    pub(crate) fn mkfifo(_: &Path) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "a native workload outlives the daemon on Linux only",
        ))
    }

    pub(crate) fn widen(_: &std::fs::File) {}

    #[derive(Debug, Clone)]
    pub(crate) enum Handle {}

    impl Handle {
        pub(crate) fn of(_: &Record) -> Option<Self> {
            None
        }

        pub(crate) const fn pid(&self) -> u32 {
            match *self {}
        }

        pub(crate) fn exited(&self) -> bool {
            match *self {}
        }

        pub(crate) fn terminate_now(&self) -> bool {
            match *self {}
        }

        pub(crate) fn kill_now(&self) -> bool {
            match *self {}
        }

        pub(crate) async fn exit(&self) {
            match *self {}
        }
    }
}

pub(crate) async fn terminate(handle: Handle, grace: Duration) {
    if !handle.terminate_now() {
        tracing::debug!(
            pid = handle.pid(),
            "SIGTERM not delivered to an adopted workload; the grace period still bounds the wait"
        );
    }
    if tokio::time::timeout(grace, handle.exit()).await.is_err() {
        if !handle.kill_now() {
            tracing::warn!(
                pid = handle.pid(),
                "SIGKILL after the grace period was not delivered to an adopted workload"
            );
        }
        handle.exit().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_start_time_is_the_twenty_second_field_even_when_the_name_holds_spaces_and_parens() {
        let stat =
            "4242 (a) b (c)) S 1 4242 4242 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 10";
        assert_eq!(start_ticks_of(stat), Some(987_654));
        assert_eq!(start_ticks_of("4242 (truncated"), None);
    }

    #[test]
    fn a_record_runs_a_spec_only_with_the_same_pod_and_the_same_projection() {
        let spec = ContainerSpec {
            image: "nix:/nix/store/a-x".into(),
            command: vec!["x".into()],
            pod: crate::backend::PodIdentity {
                uid: "u-1".into(),
                ..crate::backend::PodIdentity::default()
            },
            ..ContainerSpec::default()
        };
        let record = Record {
            id: "ns_pod_c".into(),
            pod_uid: "u-1".into(),
            spec: Projection::of(&spec).digest().unwrap(),
            verdict: Verdict::default(),
            pid: 1,
            start_ticks: 1,
            boot_id: "b".into(),
            program: PathBuf::from("/nix/store/a-x/bin/x"),
            log_path: PathBuf::from("/l"),
            grace_ms: 30_000,
            leaf: None,
            adoptable: true,
        };
        assert!(record.runs(&spec));
        let mut grace_only = spec.clone();
        grace_only.termination_grace = crate::backend::TerminationGrace::default();
        grace_only.network_aliases = vec!["svc".into()];
        assert!(record.runs(&grace_only));
        let mut recreated = spec.clone();
        recreated.pod.uid = "u-2".into();
        assert!(!record.runs(&recreated));
        let mut changed = spec.clone();
        changed.env.insert("K".into(), "V".into());
        assert!(!record.runs(&changed));
        let mut unreadable = record.clone();
        unreadable.spec = "another digest".into();
        assert!(!unreadable.runs(&spec));
    }

    #[test]
    fn a_record_on_disk_holds_no_env_value_and_only_its_owner_reads_it() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("pods/native");
        let store = Store::open(state.clone()).unwrap();
        let spec = ContainerSpec {
            image: "nix:/nix/store/a-x".into(),
            env: BTreeMap::from([("DB_PASSWORD".to_string(), "hunter2-sealed".to_string())]),
            pod: crate::backend::PodIdentity {
                uid: "u-1".into(),
                ..crate::backend::PodIdentity::default()
            },
            ..ContainerSpec::default()
        };
        let record = Record {
            id: "ns_pod_c".into(),
            pod_uid: "u-1".into(),
            spec: Projection::of(&spec).digest().unwrap(),
            verdict: Verdict {
                uid: Some(65532),
                ..Verdict::default()
            },
            pid: 1,
            start_ticks: 1,
            boot_id: "b".into(),
            program: PathBuf::from("/nix/store/a-x/bin/x"),
            log_path: PathBuf::from("/l"),
            grace_ms: 30_000,
            leaf: None,
            adoptable: true,
        };
        store.write(&record).unwrap();
        let path = state.join("ns_pod_c.json");
        let bytes = std::fs::read_to_string(&path).unwrap();
        assert!(!bytes.contains("hunter2-sealed"), "{bytes}");
        assert!(!bytes.contains("DB_PASSWORD"), "{bytes}");
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(&state), 0o700);
        let (_, read) = store.records().unwrap().pop().unwrap();
        let read = read.unwrap();
        assert_eq!(read.verdict.uid, Some(65532));
        assert!(read.runs(&spec), "the digest reads back");
        let mut other = spec.clone();
        other.env.insert("DB_PASSWORD".into(), "rotated".into());
        assert!(!read.runs(&other), "a rotated value changes the digest");
    }

    #[test]
    fn an_opened_store_tightens_a_directory_that_already_exists() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("native");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
        Store::open(state.clone()).unwrap();
        assert_eq!(
            std::fs::metadata(&state).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn this_process_has_an_identity_and_a_pidfd_that_names_it() {
        let pid = std::process::id();
        let seen = identity(pid).expect("this process is in /proc");
        let record = Record {
            id: "self".into(),
            pod_uid: String::new(),
            spec: String::new(),
            verdict: Verdict::default(),
            pid,
            start_ticks: seen.start_ticks,
            boot_id: seen.boot_id.clone(),
            program: PathBuf::new(),
            log_path: PathBuf::new(),
            grace_ms: 0,
            leaf: None,
            adoptable: true,
        };
        let handle = Handle::of(&record).expect("a live process with its own start time");
        assert!(!handle.exited());
        let reused = Record {
            start_ticks: seen.start_ticks + 1,
            ..record.clone()
        };
        assert!(
            Handle::of(&reused).is_none(),
            "the same pid with another start time is another process"
        );
        let other_boot = Record {
            boot_id: "another-boot".into(),
            ..record
        };
        assert!(Handle::of(&other_boot).is_none());
    }
}
