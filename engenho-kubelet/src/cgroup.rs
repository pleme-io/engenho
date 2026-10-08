#![allow(missing_docs, clippy::missing_errors_doc)]

use std::ffi::OsStr;
use std::fmt;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use engenho_config::NativeCgroups;
use engenho_substrate::HostRoot;

use crate::backend::{CFS_PERIOD_US, Resources};

pub const CGROUP_MOUNT: &str = "/sys/fs/cgroup";
pub const SELF_CGROUP: &str = "/proc/self/cgroup";
pub const SUPERVISOR: &str = "supervisor";
pub const WORKLOADS: &str = "workloads";
pub const PROCS: &str = "cgroup.procs";
pub const CONTROLLERS: &str = "cgroup.controllers";
pub const SUBTREE_CONTROL: &str = "cgroup.subtree_control";
pub const DELEGATION_MARKERS: [&str; 2] = ["user.delegate", "trusted.delegate"];
pub const REQUIRED_CONTROLLERS: [&str; 2] = ["cpu", "memory"];
pub const ENABLED_CONTROLLERS: [&str; 4] = ["cpu", "memory", "pids", "io"];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResourceEnforcement {
    #[default]
    Enforced,
    NotEnforced(NotEnforced),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotEnforced {
    PlatformUnsupported,
    ConfiguredOff,
    NotDelegated,
}

impl fmt::Display for NotEnforced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlatformUnsupported => write!(
                f,
                "this node's native backend runs on a host with no cgroup v2 \
                 (macOS), so nothing bounds the process"
            ),
            Self::ConfiguredOff => write!(
                f,
                "runtime.native_cgroups is off on this node, so nothing \
                 bounds the process"
            ),
            Self::NotDelegated => write!(
                f,
                "the daemon's cgroup is not delegated (systemd Delegate=yes), \
                 so its requests are not applied; a declared limit would be \
                 refused"
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Knob {
    MemoryMax,
    MemorySwapMax,
    MemoryOomGroup,
    CpuMax,
    CpuWeight,
    MemoryMin,
}

impl Knob {
    #[must_use]
    pub const fn file(self) -> &'static str {
        match self {
            Self::MemoryMax => "memory.max",
            Self::MemorySwapMax => "memory.swap.max",
            Self::MemoryOomGroup => "memory.oom.group",
            Self::CpuMax => "cpu.max",
            Self::CpuWeight => "cpu.weight",
            Self::MemoryMin => "memory.min",
        }
    }

    const fn page_rounded(self) -> bool {
        matches!(self, Self::MemoryMax | Self::MemoryMin)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuMax {
    pub quota_us: u64,
}

impl fmt::Display for CpuMax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {CFS_PERIOD_US}", self.quota_us)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CgroupPlan {
    pub memory_max: Option<u64>,
    pub cpu_max: Option<CpuMax>,
    pub cpu_weight: Option<u64>,
    pub memory_min: Option<u64>,
}

impl CgroupPlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    #[must_use]
    pub const fn bounds(&self) -> bool {
        self.memory_max.is_some() || self.cpu_max.is_some()
    }

    #[must_use]
    pub fn writes(&self) -> Vec<(Knob, String)> {
        let mut writes = Vec::new();
        if let Some(bytes) = self.memory_max {
            writes.push((Knob::MemoryMax, bytes.to_string()));
            writes.push((Knob::MemorySwapMax, 0.to_string()));
            writes.push((Knob::MemoryOomGroup, 1.to_string()));
        }
        if let Some(cpu_max) = self.cpu_max {
            writes.push((Knob::CpuMax, cpu_max.to_string()));
        }
        if let Some(weight) = self.cpu_weight {
            writes.push((Knob::CpuWeight, weight.to_string()));
        }
        if let Some(bytes) = self.memory_min {
            writes.push((Knob::MemoryMin, bytes.to_string()));
        }
        writes
    }
}

impl TryFrom<&Resources> for CgroupPlan {
    type Error = UnparseableResources;

    fn try_from(r: &Resources) -> Result<Self, Self::Error> {
        let unparseable = r.unparseable();
        if !unparseable.is_empty() {
            return Err(UnparseableResources {
                fields: unparseable
                    .into_iter()
                    .map(|(field, text)| (field, text.to_string()))
                    .collect(),
            });
        }
        let positive = |v: i64| u64::try_from(v).ok().filter(|v| *v > 0);
        Ok(Self {
            memory_max: r.memory_limit_bytes.value().and_then(positive),
            cpu_max: r
                .cpu_quota_us()
                .and_then(positive)
                .map(|quota_us| CpuMax { quota_us }),
            cpu_weight: r.cpu_weight(),
            memory_min: r.memory_request_bytes.value().and_then(positive),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnparseableResources {
    pub fields: Vec<(&'static str, String)>,
}

impl fmt::Display for UnparseableResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the container declares resources that cannot be parsed ("
        )?;
        for (i, (field, text)) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}={text:?}")?;
        }
        write!(
            f,
            "), so it is refused rather than run with a bound the kernel never \
             saw"
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CgroupOp {
    Read,
    Write,
    CreateDir,
    RemoveDir,
    List,
    Open,
    Xattr,
}

impl fmt::Display for CgroupOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::CreateDir => "create",
            Self::RemoveDir => "remove",
            Self::List => "list",
            Self::Open => "open",
            Self::Xattr => "read the delegation marker of",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Undelegated {
    Unmarked,
    ReadOnly,
    MissingControllers(Vec<&'static str>),
    Shared(Vec<u32>),
}

impl fmt::Display for Undelegated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unmarked => write!(
                f,
                "it carries no user.delegate or trusted.delegate marker, so \
                 systemd still owns its subtree"
            ),
            Self::ReadOnly => write!(f, "its {SUBTREE_CONTROL} is not writable"),
            Self::MissingControllers(missing) => {
                write!(f, "its {CONTROLLERS} lacks {}", missing.join(" and "))
            }
            Self::Shared(pids) => write!(
                f,
                "{} other process(es) share it, and cgroup v2 forbids \
                 controllers in a cgroup that holds processes",
                pids.len()
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CgroupError {
    NoUnifiedHierarchy,
    NotDelegated {
        cgroup: PathBuf,
        why: Undelegated,
    },
    Io {
        op: CgroupOp,
        path: PathBuf,
        detail: String,
    },
    Unexpected {
        path: PathBuf,
        read: String,
    },
    Readback {
        path: PathBuf,
        wrote: String,
        read: String,
    },
}

impl CgroupError {
    fn io(op: CgroupOp, path: &Path, e: &io::Error) -> Self {
        Self::Io {
            op,
            path: path.to_path_buf(),
            detail: e.to_string(),
        }
    }
}

impl fmt::Display for CgroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUnifiedHierarchy => write!(
                f,
                "{SELF_CGROUP} names no cgroup v2 (0::) membership, so there \
                 is no unified hierarchy to bound a workload in"
            ),
            Self::NotDelegated { cgroup, why } => write!(
                f,
                "the daemon's cgroup {} is not delegated: {why}",
                cgroup.display()
            ),
            Self::Io { op, path, detail } => {
                write!(f, "cannot {op} {}: {detail}", path.display())
            }
            Self::Unexpected { path, read } => {
                write!(
                    f,
                    "{} holds {read:?}, which is not a byte count",
                    path.display()
                )
            }
            Self::Readback { path, wrote, read } => write!(
                f,
                "wrote {wrote:?} to {} and the kernel reads back {read:?}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for CgroupError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unenforceable {
    NotDelegated(CgroupError),
    Leaf(CgroupError),
}

impl fmt::Display for Unenforceable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotDelegated(e) => write!(
                f,
                "the container declares a resource limit this node cannot \
                 enforce ({e}). Run the daemon with systemd Delegate=yes (the \
                 engenho NixOS module sets it for the native backend), or set \
                 runtime.native_cgroups: off to run every limit unenforced"
            ),
            Self::Leaf(e) => write!(f, "the container's cgroup could not be prepared: {e}"),
        }
    }
}

pub trait CgroupFs: fmt::Debug + Send + Sync {
    fn read(&self, path: &Path) -> io::Result<String>;
    fn write(&self, path: &Path, value: &str) -> io::Result<()>;
    fn create_dir(&self, path: &Path) -> io::Result<()>;
    fn remove_dir(&self, path: &Path) -> io::Result<()>;
    fn subdirs(&self, path: &Path) -> io::Result<Vec<String>>;
    fn writable(&self, path: &Path) -> bool;
    fn delegation_marked(&self, path: &Path) -> io::Result<bool>;
    fn open_procs(&self, path: &Path) -> io::Result<std::fs::File>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostFs {
    root: HostRoot,
}

impl HostFs {
    #[must_use]
    pub fn system() -> Self {
        Self::at(HostRoot::system())
    }

    #[must_use]
    pub const fn at(root: HostRoot) -> Self {
        Self { root }
    }
}

impl CgroupFs for HostFs {
    fn read(&self, path: &Path) -> io::Result<String> {
        std::fs::read_to_string(self.root.resolve(path))
    }

    fn write(&self, path: &Path, value: &str) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.root.resolve(path))?;
        file.write_all(value.as_bytes())
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(self.root.resolve(path))
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir(self.root.resolve(path))
    }

    fn subdirs(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(self.root.resolve(path))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    fn writable(&self, path: &Path) -> bool {
        std::fs::OpenOptions::new()
            .write(true)
            .open(self.root.resolve(path))
            .is_ok()
    }

    fn delegation_marked(&self, path: &Path) -> io::Result<bool> {
        let target = self.root.resolve(path);
        for marker in DELEGATION_MARKERS {
            let mut value = [0_u8; 8];
            match rustix::fs::getxattr(target.as_path(), marker, &mut value[..]) {
                Ok(len) if value[..len] == *b"1" => return Ok(true),
                Err(rustix::io::Errno::NOENT) => {
                    return Err(rustix::io::Errno::NOENT.into());
                }
                Ok(_) | Err(_) => {}
            }
        }
        Ok(false)
    }

    fn open_procs(&self, path: &Path) -> io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.resolve(path))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Enable(Vec<&'static str>);

impl fmt::Display for Enable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, controller) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(f, "+{controller}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PreparedLeaf {
    pub name: String,
    pub procs: std::fs::File,
}

#[derive(Debug)]
pub struct CgroupTree {
    fs: Box<dyn CgroupFs>,
    unit: PathBuf,
}

impl CgroupTree {
    pub fn adopt(fs: Box<dyn CgroupFs>) -> Result<Self, CgroupError> {
        let unit = unit_cgroup(fs.as_ref())?;
        let enable = delegated_controllers(fs.as_ref(), &unit)?.to_string();
        let supervisor = unit.join(SUPERVISOR);
        let workloads = unit.join(WORKLOADS);
        ensure_dir(fs.as_ref(), &supervisor)?;
        write(
            fs.as_ref(),
            &supervisor.join(PROCS),
            &std::process::id().to_string(),
        )?;
        write(fs.as_ref(), &unit.join(SUBTREE_CONTROL), &enable)?;
        ensure_dir(fs.as_ref(), &workloads)?;
        write(fs.as_ref(), &workloads.join(SUBTREE_CONTROL), &enable)?;
        let tree = Self { fs, unit };
        tree.sweep()?;
        Ok(tree)
    }

    #[must_use]
    pub fn unit(&self) -> &Path {
        &self.unit
    }

    #[must_use]
    pub fn workloads(&self) -> PathBuf {
        self.unit.join(WORKLOADS)
    }

    #[must_use]
    pub fn leaf(&self, name: &str) -> PathBuf {
        self.workloads().join(name)
    }

    pub fn prepare(&self, name: &str, plan: &CgroupPlan) -> Result<PreparedLeaf, CgroupError> {
        let dir = self.leaf(name);
        ensure_dir(self.fs.as_ref(), &dir)?;
        match self.configure(&dir, plan) {
            Ok(procs) => Ok(PreparedLeaf {
                name: name.to_string(),
                procs,
            }),
            Err(e) => {
                if let Err(cleanup) = self.release(name) {
                    tracing::warn!(error = %cleanup, leaf = %dir.display(), "a leaf that failed to prepare could not be removed");
                }
                Err(e)
            }
        }
    }

    pub fn release(&self, name: &str) -> Result<(), CgroupError> {
        let dir = self.leaf(name);
        match self.fs.remove_dir(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(CgroupError::io(CgroupOp::RemoveDir, &dir, &e)),
        }
        self.rebalance_memory_min()
    }

    fn configure(&self, dir: &Path, plan: &CgroupPlan) -> Result<std::fs::File, CgroupError> {
        for (knob, value) in plan.writes() {
            let path = dir.join(knob.file());
            write(self.fs.as_ref(), &path, &value)?;
            let read = read(self.fs.as_ref(), &path)?;
            verify(knob, &path, &value, read.trim())?;
        }
        self.rebalance_memory_min()?;
        let procs = dir.join(PROCS);
        self.fs
            .open_procs(&procs)
            .map_err(|e| CgroupError::io(CgroupOp::Open, &procs, &e))
    }

    fn sweep(&self) -> Result<(), CgroupError> {
        let workloads = self.workloads();
        for name in subdirs(self.fs.as_ref(), &workloads)? {
            let leaf = workloads.join(&name);
            if let Err(e) = self.fs.remove_dir(&leaf) {
                tracing::info!(leaf = %leaf.display(), error = %e, "a previous daemon's leaf is still in use; kept");
            }
        }
        self.rebalance_memory_min()
    }

    fn rebalance_memory_min(&self) -> Result<(), CgroupError> {
        let workloads = self.workloads();
        let mut sum: u64 = 0;
        for name in subdirs(self.fs.as_ref(), &workloads)? {
            let path = workloads.join(&name).join(Knob::MemoryMin.file());
            let bytes = match self.fs.read(&path) {
                Ok(text) => parse_bytes(&path, text.trim())?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
                Err(e) => return Err(CgroupError::io(CgroupOp::Read, &path, &e)),
            };
            sum = sum.saturating_add(bytes);
        }
        let path = workloads.join(Knob::MemoryMin.file());
        let value = sum.to_string();
        write(self.fs.as_ref(), &path, &value)?;
        let read = read(self.fs.as_ref(), &path)?;
        verify(Knob::MemoryMin, &path, &value, read.trim())
    }
}

#[derive(Debug)]
pub enum Cgroups {
    Delegated(Arc<CgroupTree>),
    NotDelegated(CgroupError),
    Off,
    Unsupported,
}

impl Cgroups {
    #[must_use]
    pub fn select(mode: NativeCgroups) -> Self {
        match mode {
            NativeCgroups::Off => Self::Off,
            NativeCgroups::Delegated => Self::on_this_host(),
        }
    }

    fn on_this_host() -> Self {
        if cfg!(target_os = "linux") {
            Self::adopt(Box::new(HostFs::system()))
        } else {
            Self::Unsupported
        }
    }

    #[must_use]
    pub fn adopt(fs: Box<dyn CgroupFs>) -> Self {
        match CgroupTree::adopt(fs) {
            Ok(tree) => {
                tracing::info!(
                    unit = %tree.unit().display(),
                    "native workloads are bounded in the daemon's delegated cgroup v2 subtree"
                );
                Self::Delegated(Arc::new(tree))
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "native workloads that declare a resource limit will be refused on this node"
                );
                Self::NotDelegated(e)
            }
        }
    }

    #[must_use]
    pub const fn enforcement(&self) -> ResourceEnforcement {
        match self {
            Self::Delegated(_) => ResourceEnforcement::Enforced,
            Self::NotDelegated(_) => ResourceEnforcement::NotEnforced(NotEnforced::NotDelegated),
            Self::Off => ResourceEnforcement::NotEnforced(NotEnforced::ConfiguredOff),
            Self::Unsupported => ResourceEnforcement::NotEnforced(NotEnforced::PlatformUnsupported),
        }
    }
}

fn unit_cgroup(fs: &dyn CgroupFs) -> Result<PathBuf, CgroupError> {
    let membership = read(fs, Path::new(SELF_CGROUP))?;
    let own = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or(CgroupError::NoUnifiedHierarchy)?;
    let own = Path::new(own.trim());
    let unit = if own.file_name() == Some(OsStr::new(SUPERVISOR)) {
        own.parent().unwrap_or(own)
    } else {
        own
    };
    Ok(HostRoot::at(CGROUP_MOUNT).resolve(unit))
}

fn delegated_controllers(fs: &dyn CgroupFs, unit: &Path) -> Result<Enable, CgroupError> {
    let undelegated = |why| CgroupError::NotDelegated {
        cgroup: unit.to_path_buf(),
        why,
    };
    let marked = fs
        .delegation_marked(unit)
        .map_err(|e| CgroupError::io(CgroupOp::Xattr, unit, &e))?;
    if !marked {
        return Err(undelegated(Undelegated::Unmarked));
    }
    if !fs.writable(&unit.join(SUBTREE_CONTROL)) {
        return Err(undelegated(Undelegated::ReadOnly));
    }
    let available = read(fs, &unit.join(CONTROLLERS))?;
    let available: Vec<&str> = available.split_whitespace().collect();
    let missing: Vec<&'static str> = REQUIRED_CONTROLLERS
        .into_iter()
        .filter(|c| !available.contains(c))
        .collect();
    if !missing.is_empty() {
        return Err(undelegated(Undelegated::MissingControllers(missing)));
    }
    let me = std::process::id();
    let others: Vec<u32> = read(fs, &unit.join(PROCS))?
        .split_whitespace()
        .filter_map(|pid| pid.parse().ok())
        .filter(|pid| *pid != me)
        .collect();
    if !others.is_empty() {
        return Err(undelegated(Undelegated::Shared(others)));
    }
    Ok(Enable(
        ENABLED_CONTROLLERS
            .into_iter()
            .filter(|c| available.contains(c))
            .collect(),
    ))
}

fn read(fs: &dyn CgroupFs, path: &Path) -> Result<String, CgroupError> {
    fs.read(path)
        .map_err(|e| CgroupError::io(CgroupOp::Read, path, &e))
}

fn write(fs: &dyn CgroupFs, path: &Path, value: &str) -> Result<(), CgroupError> {
    fs.write(path, value)
        .map_err(|e| CgroupError::io(CgroupOp::Write, path, &e))
}

fn subdirs(fs: &dyn CgroupFs, path: &Path) -> Result<Vec<String>, CgroupError> {
    fs.subdirs(path)
        .map_err(|e| CgroupError::io(CgroupOp::List, path, &e))
}

fn ensure_dir(fs: &dyn CgroupFs, path: &Path) -> Result<(), CgroupError> {
    match fs.create_dir(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(CgroupError::io(CgroupOp::CreateDir, path, &e)),
    }
}

fn parse_bytes(path: &Path, text: &str) -> Result<u64, CgroupError> {
    text.parse().map_err(|_| CgroupError::Unexpected {
        path: path.to_path_buf(),
        read: text.to_string(),
    })
}

fn page_size() -> u64 {
    u64::try_from(rustix::param::page_size()).unwrap_or(4096)
}

fn verify(knob: Knob, path: &Path, wrote: &str, read: &str) -> Result<(), CgroupError> {
    let agrees = if knob.page_rounded() {
        match (wrote.parse::<u64>(), read.parse::<u64>()) {
            (Ok(wrote), Ok(read)) => read <= wrote && wrote - read < page_size(),
            _ => false,
        }
    } else {
        wrote == read
    };
    if agrees {
        Ok(())
    } else {
        Err(CgroupError::Readback {
            path: path.to_path_buf(),
            wrote: wrote.to_string(),
            read: read.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const UNIT: &str = "/sys/fs/cgroup/system.slice/engenho-daemon.service";
    const FAKE_PAGE: u64 = 4096;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Op {
        Write(PathBuf, String),
        CreateDir(PathBuf),
        RemoveDir(PathBuf),
    }

    #[derive(Debug)]
    struct Kernel {
        host: HostFs,
        root: PathBuf,
        log: Arc<Mutex<Vec<Op>>>,
        lie: Option<(&'static str, &'static str)>,
    }

    impl Kernel {
        fn on(root: &Path) -> (Self, Arc<Mutex<Vec<Op>>>) {
            let log = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    host: HostFs::at(HostRoot::at(root)),
                    root: root.to_path_buf(),
                    log: Arc::clone(&log),
                    lie: None,
                },
                log,
            )
        }

        fn on_disk(&self, path: &Path) -> PathBuf {
            HostRoot::at(&self.root).resolve(path)
        }

        fn procs_in(&self, dir: &Path) -> String {
            std::fs::read_to_string(self.on_disk(&dir.join(PROCS))).unwrap_or_default()
        }
    }

    fn migrate(from: &Path, pid: &str) {
        for entry in std::fs::read_dir(from).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                migrate(&path, pid);
            } else if path.file_name() == Some(OsStr::new(PROCS)) {
                let kept: Vec<String> = std::fs::read_to_string(&path)
                    .unwrap_or_default()
                    .split_whitespace()
                    .filter(|p| *p != pid)
                    .map(str::to_string)
                    .collect();
                std::fs::write(&path, kept.join("\n")).unwrap();
            }
        }
    }

    fn busy() -> io::Error {
        rustix::io::Errno::BUSY.into()
    }

    impl CgroupFs for Kernel {
        fn read(&self, path: &Path) -> io::Result<String> {
            match self.lie {
                Some((file, value)) if path.file_name() == Some(OsStr::new(file)) => {
                    Ok(value.to_string())
                }
                _ => self.host.read(path),
            }
        }

        fn write(&self, path: &Path, value: &str) -> io::Result<()> {
            let dir = path.parent().expect("a cgroup file has a directory");
            let file = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
            let stored = match file {
                SUBTREE_CONTROL => {
                    if !self.procs_in(dir).trim().is_empty() {
                        return Err(busy());
                    }
                    let available = self.host.read(&dir.join(CONTROLLERS))?;
                    for token in value.split_whitespace() {
                        let name = token.trim_start_matches('+');
                        if !available.split_whitespace().any(|c| c == name) {
                            return Err(rustix::io::Errno::NOENT.into());
                        }
                    }
                    value.to_string()
                }
                PROCS => {
                    let subtree = self
                        .host
                        .read(&dir.join(SUBTREE_CONTROL))
                        .unwrap_or_default();
                    if !subtree.trim().is_empty() {
                        return Err(busy());
                    }
                    migrate(&self.on_disk(Path::new(CGROUP_MOUNT)), value.trim());
                    let mut procs = self.procs_in(dir);
                    procs.push_str(value.trim());
                    procs.push('\n');
                    procs
                }
                "memory.max" | "memory.min" => {
                    let bytes: u64 = value
                        .parse()
                        .map_err(|_| io::Error::from(rustix::io::Errno::INVAL))?;
                    (bytes / FAKE_PAGE * FAKE_PAGE).to_string()
                }
                _ => value.to_string(),
            };
            self.log
                .lock()
                .unwrap()
                .push(Op::Write(path.to_path_buf(), value.to_string()));
            std::fs::write(self.on_disk(path), stored)
        }

        fn create_dir(&self, path: &Path) -> io::Result<()> {
            self.host.create_dir(path)?;
            let parent = path.parent().expect("a cgroup has a parent");
            let enabled: Vec<String> = self
                .host
                .read(&parent.join(SUBTREE_CONTROL))
                .unwrap_or_default()
                .split_whitespace()
                .map(|t| t.trim_start_matches('+').to_string())
                .collect();
            std::fs::write(self.on_disk(&path.join(PROCS)), "")?;
            std::fs::write(self.on_disk(&path.join(SUBTREE_CONTROL)), "")?;
            std::fs::write(self.on_disk(&path.join(CONTROLLERS)), enabled.join(" "))?;
            if enabled.iter().any(|c| c == "memory") {
                std::fs::write(self.on_disk(&path.join("memory.min")), "0")?;
            }
            self.log
                .lock()
                .unwrap()
                .push(Op::CreateDir(path.to_path_buf()));
            Ok(())
        }

        fn remove_dir(&self, path: &Path) -> io::Result<()> {
            if !self.on_disk(path).exists() {
                return Err(rustix::io::Errno::NOENT.into());
            }
            if !self.procs_in(path).trim().is_empty() || !self.host.subdirs(path)?.is_empty() {
                return Err(busy());
            }
            std::fs::remove_dir_all(self.on_disk(path))?;
            self.log
                .lock()
                .unwrap()
                .push(Op::RemoveDir(path.to_path_buf()));
            Ok(())
        }

        fn subdirs(&self, path: &Path) -> io::Result<Vec<String>> {
            self.host.subdirs(path)
        }

        fn writable(&self, path: &Path) -> bool {
            self.host.writable(path)
        }

        fn delegation_marked(&self, path: &Path) -> io::Result<bool> {
            self.host.delegation_marked(path)
        }

        fn open_procs(&self, path: &Path) -> io::Result<std::fs::File> {
            self.host.open_procs(path)
        }
    }

    struct Node {
        dir: tempfile::TempDir,
    }

    impl Node {
        fn new(membership: &str, controllers: &str, procs: &str, marker: Option<&[u8]>) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let node = Self { dir };
            let unit = node.path(UNIT);
            std::fs::create_dir_all(&unit).unwrap();
            std::fs::create_dir_all(node.path("/proc/self")).unwrap();
            std::fs::write(node.path(SELF_CGROUP), membership).unwrap();
            std::fs::write(unit.join(CONTROLLERS), controllers).unwrap();
            std::fs::write(unit.join(SUBTREE_CONTROL), "").unwrap();
            std::fs::write(unit.join(PROCS), procs).unwrap();
            if let Some(value) = marker {
                rustix::fs::setxattr(
                    unit.as_path(),
                    "user.delegate",
                    value,
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            node
        }

        fn delegated() -> Self {
            Self::new(
                "0::/system.slice/engenho-daemon.service\n",
                "cpuset cpu io memory hugetlb pids",
                &me(),
                Some(b"1"),
            )
        }

        fn path(&self, host: &str) -> PathBuf {
            HostRoot::at(self.dir.path()).resolve(Path::new(host))
        }

        fn read(&self, host: &str) -> String {
            std::fs::read_to_string(self.path(host))
                .unwrap()
                .trim()
                .to_string()
        }

        fn kernel(&self) -> (Box<dyn CgroupFs>, Arc<Mutex<Vec<Op>>>) {
            let (kernel, log) = Kernel::on(self.dir.path());
            (Box::new(kernel), log)
        }

        fn adopt(&self) -> (Result<CgroupTree, CgroupError>, Arc<Mutex<Vec<Op>>>) {
            let (fs, log) = self.kernel();
            (CgroupTree::adopt(fs), log)
        }
    }

    fn me() -> String {
        std::process::id().to_string()
    }

    fn unit(rel: &str) -> PathBuf {
        Path::new(UNIT).join(rel)
    }

    fn resources(json: &serde_json::Value) -> Resources {
        Resources::from_container_json(&serde_json::json!({ "resources": json }))
    }

    fn plan(json: &serde_json::Value) -> CgroupPlan {
        CgroupPlan::try_from(&resources(json)).expect("parses")
    }

    #[test]
    fn each_declared_bound_lowers_to_its_cgroup_knob() {
        let table: [(serde_json::Value, CgroupPlan); 7] = [
            (
                serde_json::json!({ "limits": { "memory": "512Mi" } }),
                CgroupPlan {
                    memory_max: Some(536_870_912),
                    ..CgroupPlan::default()
                },
            ),
            (
                serde_json::json!({ "limits": { "cpu": "500m" } }),
                CgroupPlan {
                    cpu_max: Some(CpuMax { quota_us: 50_000 }),
                    ..CgroupPlan::default()
                },
            ),
            (
                serde_json::json!({ "limits": { "cpu": "1m" } }),
                CgroupPlan {
                    cpu_max: Some(CpuMax { quota_us: 1_000 }),
                    ..CgroupPlan::default()
                },
            ),
            (
                serde_json::json!({ "limits": { "cpu": "0", "memory": "0" } }),
                CgroupPlan::default(),
            ),
            (
                serde_json::json!({ "requests": { "cpu": "1" } }),
                CgroupPlan {
                    cpu_weight: Some(39),
                    ..CgroupPlan::default()
                },
            ),
            (
                serde_json::json!({ "requests": { "memory": "64Mi" } }),
                CgroupPlan {
                    memory_min: Some(67_108_864),
                    ..CgroupPlan::default()
                },
            ),
            (serde_json::json!({}), CgroupPlan::default()),
        ];
        for (declared, want) in table {
            assert_eq!(plan(&declared), want, "{declared}");
        }
    }

    #[test]
    fn a_memory_limit_turns_swap_off_and_groups_the_oom_kill() {
        let writes =
            plan(&serde_json::json!({ "limits": { "memory": "512Mi", "cpu": "500m" } })).writes();
        assert_eq!(
            writes,
            vec![
                (Knob::MemoryMax, "536870912".to_string()),
                (Knob::MemorySwapMax, "0".to_string()),
                (Knob::MemoryOomGroup, "1".to_string()),
                (Knob::CpuMax, "50000 100000".to_string()),
            ]
        );
        assert!(
            writes.iter().all(|(knob, _)| !knob.file().contains("high")),
            "memory.high throttles instead of bounding; it is never written"
        );
    }

    #[test]
    fn an_empty_plan_bounds_nothing_and_writes_nothing() {
        let empty = plan(&serde_json::json!({}));
        assert!(empty.is_empty());
        assert!(!empty.bounds());
        assert!(empty.writes().is_empty());
        let requests_only = plan(&serde_json::json!({ "requests": { "cpu": "1" } }));
        assert!(!requests_only.is_empty());
        assert!(
            !requests_only.bounds(),
            "a request is a weight, not a ceiling"
        );
    }

    #[test]
    fn an_unparseable_bound_refuses_the_pod_and_names_the_field() {
        let refused = CgroupPlan::try_from(&resources(&serde_json::json!({
            "limits": { "memory": "512Quatloos" }
        })))
        .expect_err("an unparseable bound is never planned around");
        assert_eq!(
            refused.fields,
            vec![("limits.memory", "512Quatloos".to_string())]
        );
        let shown = refused.to_string();
        assert!(shown.contains("limits.memory=\"512Quatloos\""), "{shown}");
        assert!(!shown.contains("  "), "{shown}");
    }

    #[test]
    fn adopt_moves_itself_out_before_any_controller_is_enabled() {
        let node = Node::delegated();
        let (adopted, log) = node.adopt();
        let tree = adopted.expect("a delegated unit is adopted");
        assert_eq!(tree.unit(), Path::new(UNIT));
        let enable = "+cpu +memory +pids +io".to_string();
        let writes: Vec<Op> = log
            .lock()
            .unwrap()
            .iter()
            .filter(|op| !matches!(op, Op::Write(p, _) if p.ends_with("memory.min")))
            .cloned()
            .collect();
        assert_eq!(
            writes,
            vec![
                Op::CreateDir(unit(SUPERVISOR)),
                Op::Write(unit(SUPERVISOR).join(PROCS), me()),
                Op::Write(unit(SUBTREE_CONTROL), enable.clone()),
                Op::CreateDir(unit(WORKLOADS)),
                Op::Write(unit(WORKLOADS).join(SUBTREE_CONTROL), enable),
            ]
        );
        assert_eq!(node.read(&format!("{UNIT}/{PROCS}")), "");
        assert_eq!(node.read(&format!("{UNIT}/{SUPERVISOR}/{PROCS}")), me());
    }

    #[test]
    fn adopt_enables_only_the_controllers_it_was_delegated() {
        let node = Node::new(
            "0::/system.slice/engenho-daemon.service\n",
            "cpu memory",
            &me(),
            Some(b"1"),
        );
        node.adopt().0.expect("cpu and memory are enough");
        assert_eq!(
            node.read(&format!("{UNIT}/{WORKLOADS}/{SUBTREE_CONTROL}")),
            "+cpu +memory"
        );
    }

    #[test]
    fn an_unmarked_cgroup_is_refused_and_nothing_is_written() {
        for marker in [None, Some(&b"0"[..])] {
            let node = Node::new(
                "0::/system.slice/engenho-daemon.service\n",
                "cpu io memory pids",
                &me(),
                marker,
            );
            let (adopted, log) = node.adopt();
            assert_eq!(
                adopted.expect_err("refused"),
                CgroupError::NotDelegated {
                    cgroup: PathBuf::from(UNIT),
                    why: Undelegated::Unmarked
                }
            );
            assert!(log.lock().unwrap().is_empty(), "{:?}", log.lock().unwrap());
        }
    }

    #[test]
    fn a_cgroup_another_process_shares_is_refused_and_nothing_is_written() {
        let mut procs = me();
        procs.push_str("\n4242\n");
        let node = Node::new(
            "0::/system.slice/engenho-daemon.service\n",
            "cpu io memory pids",
            &procs,
            Some(b"1"),
        );
        let (adopted, log) = node.adopt();
        assert_eq!(
            adopted.expect_err("refused"),
            CgroupError::NotDelegated {
                cgroup: PathBuf::from(UNIT),
                why: Undelegated::Shared(vec![4242])
            }
        );
        assert!(log.lock().unwrap().is_empty());
    }

    #[test]
    fn a_unit_without_cpu_or_memory_is_refused() {
        let node = Node::new(
            "0::/system.slice/engenho-daemon.service\n",
            "io pids",
            &me(),
            Some(b"1"),
        );
        assert_eq!(
            node.adopt().0.expect_err("refused"),
            CgroupError::NotDelegated {
                cgroup: PathBuf::from(UNIT),
                why: Undelegated::MissingControllers(vec!["cpu", "memory"])
            }
        );
    }

    #[test]
    fn a_host_with_no_unified_hierarchy_is_refused() {
        let node = Node::new(
            "1:name=systemd:/system.slice/engenho-daemon.service\n",
            "cpu memory",
            &me(),
            Some(b"1"),
        );
        assert_eq!(
            node.adopt().0.expect_err("refused"),
            CgroupError::NoUnifiedHierarchy
        );
    }

    #[test]
    fn a_second_adopt_in_one_process_finds_the_unit_from_its_supervisor() {
        let node = Node::delegated();
        node.adopt().0.expect("first boot");
        std::fs::write(
            node.path(SELF_CGROUP),
            "0::/system.slice/engenho-daemon.service/supervisor\n",
        )
        .unwrap();
        let tree = node
            .adopt()
            .0
            .expect("a re-boot inside one process adopts again");
        assert_eq!(tree.unit(), Path::new(UNIT));
    }

    #[test]
    fn adopt_sweeps_empty_leaves_a_previous_daemon_left_and_keeps_busy_ones() {
        let node = Node::delegated();
        let first = node.adopt().0.expect("first daemon");
        first
            .prepare(
                "default_idle_app",
                &plan(&serde_json::json!({ "requests": { "memory": "1Mi" } })),
            )
            .unwrap();
        first
            .prepare(
                "default_busy_app",
                &plan(&serde_json::json!({ "requests": { "memory": "2Mi" } })),
            )
            .unwrap();
        std::fs::write(
            node.path(&format!("{UNIT}/{WORKLOADS}/default_busy_app/{PROCS}")),
            "777\n",
        )
        .unwrap();
        std::fs::write(
            node.path(SELF_CGROUP),
            "0::/system.slice/engenho-daemon.service/supervisor\n",
        )
        .unwrap();
        node.adopt().0.expect("next daemon");
        assert!(
            !node
                .path(&format!("{UNIT}/{WORKLOADS}/default_idle_app"))
                .exists()
        );
        assert!(
            node.path(&format!("{UNIT}/{WORKLOADS}/default_busy_app"))
                .exists()
        );
        assert_eq!(
            node.read(&format!("{UNIT}/{WORKLOADS}/memory.min")),
            "2097152"
        );
    }

    #[test]
    fn prepare_writes_the_plan_and_reads_every_value_back() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        let declared = serde_json::json!({
            "limits": { "memory": "64Mi", "cpu": "250m" },
            "requests": { "cpu": "1", "memory": "32Mi" }
        });
        let leaf = tree.prepare("default_hog_hog", &plan(&declared)).unwrap();
        assert_eq!(leaf.name, "default_hog_hog");
        let at = |knob: &str| node.read(&format!("{UNIT}/{WORKLOADS}/default_hog_hog/{knob}"));
        assert_eq!(at("memory.max"), "67108864");
        assert_eq!(at("memory.swap.max"), "0");
        assert_eq!(at("memory.oom.group"), "1");
        assert_eq!(at("cpu.max"), "25000 100000");
        assert_eq!(at("cpu.weight"), "39");
        assert_eq!(at("memory.min"), "33554432");
        assert!(
            !node
                .path(&format!("{UNIT}/{WORKLOADS}/default_hog_hog/memory.high"))
                .exists()
        );
    }

    #[test]
    fn a_value_the_kernel_reads_back_differently_is_a_readback_error_and_no_leaf_is_left() {
        let node = Node::delegated();
        let (kernel, _) = Kernel::on(node.dir.path());
        let tree = CgroupTree::adopt(Box::new(Kernel {
            lie: Some(("cpu.max", "max 100000")),
            ..kernel
        }))
        .unwrap();
        let refused = tree
            .prepare(
                "default_spin_spin",
                &plan(&serde_json::json!({ "limits": { "cpu": "250m" } })),
            )
            .expect_err("a ceiling the kernel did not take is not a ceiling");
        assert_eq!(
            refused,
            CgroupError::Readback {
                path: unit(WORKLOADS).join("default_spin_spin").join("cpu.max"),
                wrote: "25000 100000".to_string(),
                read: "max 100000".to_string(),
            }
        );
        assert!(
            !node
                .path(&format!("{UNIT}/{WORKLOADS}/default_spin_spin"))
                .exists()
        );
    }

    #[test]
    fn page_rounding_of_a_byte_bound_is_not_a_mismatch() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        tree.prepare(
            "default_odd_odd",
            &CgroupPlan {
                memory_max: Some(1_000_000),
                ..CgroupPlan::default()
            },
        )
        .expect("the kernel rounds down to a page and that is still the bound");
        assert_eq!(
            node.read(&format!("{UNIT}/{WORKLOADS}/default_odd_odd/memory.max")),
            "999424"
        );
        assert!(verify(Knob::MemoryMax, Path::new("m"), "1000000", "999424").is_ok());
        assert!(verify(Knob::MemoryMax, Path::new("m"), "1000000", "1000001").is_err());
        assert!(
            verify(
                Knob::MemoryMax,
                Path::new("m"),
                "1000000",
                &(1_000_000 - page_size()).to_string()
            )
            .is_err()
        );
        assert!(verify(Knob::CpuWeight, Path::new("w"), "39", "40").is_err());
    }

    #[test]
    fn workloads_memory_min_is_the_sum_of_its_leaves() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        tree.prepare(
            "a",
            &plan(&serde_json::json!({ "requests": { "memory": "64Mi" } })),
        )
        .unwrap();
        tree.prepare(
            "b",
            &plan(&serde_json::json!({ "requests": { "memory": "32Mi" } })),
        )
        .unwrap();
        assert_eq!(
            node.read(&format!("{UNIT}/{WORKLOADS}/memory.min")),
            "100663296"
        );
        tree.release("a").unwrap();
        assert_eq!(
            node.read(&format!("{UNIT}/{WORKLOADS}/memory.min")),
            "33554432"
        );
    }

    #[test]
    fn release_removes_the_leaf_and_a_leaf_still_holding_a_process_stays() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        let declared = plan(&serde_json::json!({ "limits": { "memory": "8Mi" } }));
        tree.prepare("gone", &declared).unwrap();
        tree.release("gone").expect("an empty leaf is removed");
        assert!(!node.path(&format!("{UNIT}/{WORKLOADS}/gone")).exists());
        tree.release("gone")
            .expect("releasing twice is not an error");

        tree.prepare("held", &declared).unwrap();
        std::fs::write(
            node.path(&format!("{UNIT}/{WORKLOADS}/held/{PROCS}")),
            "99\n",
        )
        .unwrap();
        assert!(matches!(
            tree.release("held"),
            Err(CgroupError::Io {
                op: CgroupOp::RemoveDir,
                ..
            })
        ));
        assert!(node.path(&format!("{UNIT}/{WORKLOADS}/held")).exists());
    }

    #[test]
    fn the_delegation_marker_is_the_xattr_and_only_a_one_counts() {
        let dir = tempfile::tempdir().unwrap();
        let fs = HostFs::at(HostRoot::at(dir.path()));
        for (name, value, want) in [
            ("marked", Some(&b"1"[..]), true),
            ("zero", Some(&b"0"[..]), false),
            ("bare", None, false),
        ] {
            let path = dir.path().join(name);
            std::fs::create_dir(&path).unwrap();
            if let Some(value) = value {
                rustix::fs::setxattr(
                    path.as_path(),
                    "user.delegate",
                    value,
                    rustix::fs::XattrFlags::empty(),
                )
                .unwrap();
            }
            let host = Path::new("/").join(name);
            assert_eq!(fs.delegation_marked(&host).unwrap(), want, "{name}");
        }
        assert!(fs.delegation_marked(Path::new("/absent")).is_err());
    }

    #[test]
    fn enforcement_names_why_each_mode_does_not_enforce() {
        assert_eq!(
            Cgroups::Off.enforcement(),
            ResourceEnforcement::NotEnforced(NotEnforced::ConfiguredOff)
        );
        assert_eq!(
            Cgroups::Unsupported.enforcement(),
            ResourceEnforcement::NotEnforced(NotEnforced::PlatformUnsupported)
        );
        assert_eq!(
            Cgroups::NotDelegated(CgroupError::NoUnifiedHierarchy).enforcement(),
            ResourceEnforcement::NotEnforced(NotEnforced::NotDelegated)
        );
        let node = Node::delegated();
        let (fs, _) = node.kernel();
        assert_eq!(
            Cgroups::adopt(fs).enforcement(),
            ResourceEnforcement::Enforced
        );
        assert!(matches!(Cgroups::select(NativeCgroups::Off), Cgroups::Off));
        if !cfg!(target_os = "linux") {
            assert!(matches!(
                Cgroups::select(NativeCgroups::Delegated),
                Cgroups::Unsupported
            ));
        }
    }

    #[test]
    fn every_unparseable_field_is_named_once_in_order() {
        let refused = CgroupPlan::try_from(&resources(&serde_json::json!({
            "limits": { "cpu": "lots", "memory": "512Quatloos" }
        })))
        .unwrap_err();
        assert_eq!(
            refused.to_string(),
            "the container declares resources that cannot be parsed \
             (limits.cpu=\"lots\", limits.memory=\"512Quatloos\"), so it is \
             refused rather than run with a bound the kernel never saw"
        );
    }

    #[test]
    fn each_refusal_reads_as_what_went_wrong_and_where() {
        let unit = PathBuf::from(UNIT);
        let not_delegated = |why| CgroupError::NotDelegated {
            cgroup: unit.clone(),
            why,
        };
        let shown = [
            (
                CgroupError::NoUnifiedHierarchy,
                "names no cgroup v2 (0::) membership",
            ),
            (
                not_delegated(Undelegated::Unmarked),
                "is not delegated: it carries no user.delegate",
            ),
            (
                not_delegated(Undelegated::ReadOnly),
                "is not delegated: its cgroup.subtree_control is not writable",
            ),
            (
                not_delegated(Undelegated::MissingControllers(vec!["cpu", "memory"])),
                "is not delegated: its cgroup.controllers lacks cpu and memory",
            ),
            (
                not_delegated(Undelegated::Shared(vec![7, 8])),
                "is not delegated: 2 other process(es) share it",
            ),
            (
                CgroupError::Io {
                    op: CgroupOp::Write,
                    path: unit.join("cpu.max"),
                    detail: "Device or resource busy".to_string(),
                },
                "cannot write /sys/fs/cgroup/system.slice/engenho-daemon.service/cpu.max: Device or resource busy",
            ),
            (
                CgroupError::Unexpected {
                    path: unit.join("memory.min"),
                    read: "max".to_string(),
                },
                "memory.min holds \"max\", which is not a byte count",
            ),
            (
                CgroupError::Readback {
                    path: unit.join("cpu.max"),
                    wrote: "25000 100000".to_string(),
                    read: "max 100000".to_string(),
                },
                "wrote \"25000 100000\" to /sys/fs/cgroup/system.slice/engenho-daemon.service/cpu.max and the kernel reads back \"max 100000\"",
            ),
        ];
        for (error, phrase) in shown {
            let text = error.to_string();
            assert!(text.contains(phrase), "{text:?} lacks {phrase:?}");
            assert!(!text.contains("  "), "{text:?}");
        }
        let ops = [
            (CgroupOp::Read, "read"),
            (CgroupOp::Write, "write"),
            (CgroupOp::CreateDir, "create"),
            (CgroupOp::RemoveDir, "remove"),
            (CgroupOp::List, "list"),
            (CgroupOp::Open, "open"),
            (CgroupOp::Xattr, "read the delegation marker of"),
        ];
        for (op, verb) in ops {
            assert_eq!(op.to_string(), verb);
        }
        let leaf = Unenforceable::Leaf(CgroupError::NoUnifiedHierarchy).to_string();
        assert!(
            leaf.starts_with("the container's cgroup could not be prepared: "),
            "{leaf}"
        );
    }

    #[test]
    fn the_host_filesystem_acts_under_its_root() {
        let dir = tempfile::tempdir().unwrap();
        let fs = HostFs::at(HostRoot::at(dir.path()));
        let cg = Path::new("/sys/fs/cgroup");
        std::fs::create_dir_all(dir.path().join("sys/fs/cgroup")).unwrap();
        fs.create_dir(&cg.join("leaf")).unwrap();
        fs.create_dir(&cg.join("other")).unwrap();
        assert!(dir.path().join("sys/fs/cgroup/leaf").is_dir());
        fs.write(&cg.join("leaf/cpu.weight"), "39").unwrap();
        fs.write(&cg.join("leaf/cpu.weight"), "7").unwrap();
        assert_eq!(fs.read(&cg.join("leaf/cpu.weight")).unwrap(), "7");
        assert_eq!(
            fs.subdirs(cg).unwrap(),
            vec!["leaf".to_string(), "other".to_string()]
        );
        assert!(fs.writable(&cg.join("leaf/cpu.weight")));
        assert!(!fs.writable(&cg.join("leaf/absent")));
        let mut procs = fs.open_procs(&cg.join("leaf/cgroup.procs")).unwrap();
        procs.write_all(b"0").unwrap();
        assert_eq!(fs.read(&cg.join("leaf/cgroup.procs")).unwrap(), "0");
        fs.remove_dir(&cg.join("other")).unwrap();
        assert!(!dir.path().join("sys/fs/cgroup/other").exists());
        assert!(
            fs.remove_dir(&cg.join("leaf")).is_err(),
            "a directory with files is not an empty cgroup"
        );
    }

    #[test]
    fn a_unit_whose_subtree_control_cannot_be_written_is_refused() {
        let node = Node::delegated();
        std::fs::remove_file(node.path(&format!("{UNIT}/{SUBTREE_CONTROL}"))).unwrap();
        let (adopted, log) = node.adopt();
        assert_eq!(
            adopted.expect_err("refused"),
            CgroupError::NotDelegated {
                cgroup: PathBuf::from(UNIT),
                why: Undelegated::ReadOnly
            }
        );
        assert!(log.lock().unwrap().is_empty());
    }

    #[test]
    fn a_leaf_whose_memory_min_cannot_be_read_fails_the_rebalance() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        let odd = node.path(&format!("{UNIT}/{WORKLOADS}/odd"));
        std::fs::create_dir_all(odd.join("memory.min")).unwrap();
        assert!(matches!(
            tree.release("gone"),
            Err(CgroupError::Io {
                op: CgroupOp::Read,
                ..
            })
        ));
        std::fs::remove_dir(odd.join("memory.min")).unwrap();
        std::fs::write(odd.join("memory.min"), "max").unwrap();
        assert!(matches!(
            tree.release("gone"),
            Err(CgroupError::Unexpected { .. })
        ));
    }

    #[test]
    fn a_leaf_that_cannot_be_created_is_an_error_not_a_silent_skip() {
        let node = Node::delegated();
        let tree = node.adopt().0.unwrap();
        std::fs::remove_dir_all(node.path(&format!("{UNIT}/{WORKLOADS}"))).unwrap();
        assert!(matches!(
            tree.prepare(
                "default_x_x",
                &plan(&serde_json::json!({ "limits": { "memory": "8Mi" } }))
            ),
            Err(CgroupError::Io {
                op: CgroupOp::CreateDir,
                ..
            })
        ));
    }
}
