//! The enclosure of a native container, decided by the embedded `kakoi` blue
//! program (`blue/kakoi/kakoi.b`).
//!
//! Rust is the seam: it states the facts, reads the verdict, and applies it to
//! the child in [`enter_on_exec`].

use engenho_controllers::admission::{
    AdmissionDecision, AdmissionError, AdmissionRequest, AdmissionWebhook,
};
use serde::{Deserialize, Serialize};

use crate::backend::Confinement;
use crate::blue_worker::BlueWorker;

/// How a node treats a native container's security.
pub use engenho_config::NativeSecurity;

const FILES: &[(&str, &str)] = &[("kakoi/kakoi.b", include_str!("../blue/kakoi/kakoi.b"))];
const ENTRY: &str = "use(\"kakoi\")\n";

/// The pod annotation that grants root; its value is the reason.
pub const GRANT_ANNOTATION: &str = "kakoi.pleme.io/root";

/// The pod annotation that carries the blue `access` value; it only narrows.
pub const ACCESS_ANNOTATION: &str = "kakoi.pleme.io/access";

#[derive(Serialize)]
struct Declared<'a> {
    run_as_user: Option<i64>,
    run_as_group: Option<i64>,
    run_as_non_root: Option<bool>,
    privileged: bool,
    allow_escalation: Option<bool>,
    cap_add: &'a [String],
    cap_drop: &'a [String],
    read_only_root_fs: bool,
    no_new_privileges: bool,
    host_pid: bool,
    host_ipc: bool,
    host_network: bool,
    seccomp_profile: Option<&'a str>,
    sysctls: &'a [String],
    devices: &'a [String],
    volume_devices: &'a [String],
    not_applied: &'a [String],
    memory_limit: Option<i64>,
    memory_request: Option<i64>,
}

#[derive(Serialize)]
struct Facts<'a> {
    arch: &'a str,
    namespace: &'a str,
    volumes: &'a [String],
    mode: NativeSecurity,
    can_enforce: bool,
    bounds: &'a [&'static str],
    node_memory_bytes: u64,
    runtime_gate: bool,
    landlock_abi: u32,
    read_only_volumes: &'a [String],
    kernel_decides: bool,
    grant: Option<&'a str>,
    access: Option<serde_json::Value>,
    access_invalid: bool,
    declared: Declared<'a>,
}

/// A call refused with `EPERM` for what its first argument asks.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct Guard {
    /// The syscall number.
    pub number: u32,
    /// Refused when the first argument has any of these bits.
    #[serde(default)]
    pub mask: Option<u32>,
    /// Refused when the first argument is one of these.
    #[serde(default)]
    pub values: Vec<u32>,
}

/// What this node can do for a pod, as `kakoi::enclose` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    /// How this node treats a native container's security.
    pub mode: NativeSecurity,
    /// Whether this daemon can change a child's identity.
    pub can_enforce: bool,
    /// The default bounds this node can write: `memory`, `pids`.
    pub bounds: Vec<&'static str>,
    /// The node's memory in bytes, 0 where it is not known.
    pub memory_bytes: u64,
    /// A container runtime applies what the pod declares and this decides only
    /// who may ask, so what the native seam cannot apply is the runtime's to judge.
    pub runtime_gate: bool,
    /// The kernel's Landlock ABI, 0 where it has none.
    pub landlock_abi: u32,
}

impl Node {
    /// The node this daemon runs on, whose cgroups can write `bounds`.
    #[must_use]
    pub fn measured(mode: NativeSecurity, bounds: Vec<&'static str>) -> Self {
        Self {
            mode,
            can_enforce: can_enforce(),
            bounds,
            memory_bytes: node_memory_bytes(),
            runtime_gate: false,
            landlock_abi: landlock_abi(),
        }
    }
}

/// What one container asks of its enclosure.
#[derive(Clone, Copy, Debug)]
pub struct Asked<'a> {
    /// Its declared security.
    pub confinement: &'a Confinement,
    /// Its pod's namespace.
    pub namespace: &'a str,
    /// Its volume paths.
    pub volumes: &'a [String],
    /// Those of its volumes mounted read-only.
    pub read_only_volumes: &'a [String],
    /// Its declared resources.
    pub resources: &'a crate::backend::Resources,
}

/// The bounds a pod gets for what it does not declare.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct DefaultLimits {
    /// `pids.max`, where this node can write it.
    #[serde(default)]
    pub pids_max: Option<u64>,
    /// `memory.max`, where this node can write it and the pod declares no limit.
    #[serde(default)]
    pub memory_max: Option<u64>,
}

/// One path of a Landlock domain.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct LandlockRule {
    /// The path; one absent at the start is skipped.
    pub path: String,
    /// The rights it allows, by blue's words.
    pub rights: Vec<String>,
}

/// The Landlock domain blue decided for a container.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct Landlock {
    /// Every right this domain mediates, by blue's words.
    pub handled: Vec<String>,
    /// What it allows.
    pub rules: Vec<LandlockRule>,
    /// What the main process is cut off from.
    pub scopes: Vec<String>,
    /// What a command run into the pod is cut off from.
    pub join_scopes: Vec<String>,
}

/// What `kakoi::enclose` answered.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct Verdict {
    /// Why this container may not start.
    pub refused: Option<String>,
    /// The uid to run as; `None` keeps the daemon's.
    pub uid: Option<u32>,
    /// The gid to run as; `None` keeps the daemon's.
    pub gid: Option<u32>,
    /// Set `no_new_privs` on the child.
    pub no_new_privs: bool,
    /// Clear the child's capability sets, but for [`Self::keep`].
    pub clear_caps: bool,
    /// Its Landlock domain, `None` on a kernel with none or a pod with no enclosure.
    #[serde(default)]
    pub landlock: Option<Landlock>,
    /// The bounds it gets for what it does not declare.
    #[serde(default)]
    pub limits: DefaultLimits,
    /// The capabilities the child keeps across its uid change, by blue's words.
    #[serde(default)]
    pub keep: Vec<String>,
    /// Give the child its own mount namespace with a read-only host.
    #[serde(default)]
    pub enclosed: bool,
    /// Paths covered with an empty read-only filesystem.
    #[serde(default)]
    pub hide: Vec<String>,
    /// Directories given a fresh private filesystem.
    #[serde(default)]
    pub scratch: Vec<String>,
    /// Syscall numbers denied with `EPERM`, for this architecture.
    #[serde(default)]
    pub deny: Vec<u32>,
    /// Syscall numbers answered `ENOSYS`, so a C library falls back.
    #[serde(default)]
    pub absent: Vec<u32>,
    /// The highest syscall number allowed; anything above answers `ENOSYS`.
    #[serde(default)]
    pub newest: Option<u32>,
    /// Calls refused for what their first argument asks.
    #[serde(default)]
    pub guards: Vec<Guard>,
    /// The kernel namespaces the child gets of its own.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Volumes that stay read-only, because the pod's access does not write them.
    #[serde(default)]
    pub narrowed: Vec<String>,
    /// What the node could not enforce, for an event a reader sees.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// The `kakoi` program.
#[derive(Clone)]
pub struct Kakoi(BlueWorker);

impl Kakoi {
    /// Load the program.
    ///
    /// # Errors
    /// The load error, as text.
    pub fn start() -> Result<Self, String> {
        BlueWorker::start("kakoi", ENTRY, FILES).map(Self)
    }

    /// `kakoi::enclose`.
    ///
    /// # Errors
    /// The program's error, or an answer that does not read.
    pub async fn enclose(&self, asked: Asked<'_>, node: &Node) -> Result<Verdict, String> {
        let Asked {
            confinement,
            namespace,
            volumes,
            read_only_volumes,
            resources,
        } = asked;
        let Confinement {
            privileged,
            read_only_root_fs,
            no_new_privileges,
            cap_drop,
            cap_add,
            run_as_user,
            run_as_group,
            run_as_non_root,
            allow_escalation,
            root_grant,
            access,
            host_pid,
            host_ipc,
            host_network,
            seccomp_profile,
            sysctls,
            devices,
            volume_devices,
            not_applied,
        } = confinement;
        let parsed = access
            .as_deref()
            .map(serde_json::from_str::<serde_json::Value>);
        let facts = Facts {
            arch: std::env::consts::ARCH,
            namespace,
            volumes,
            mode: node.mode,
            can_enforce: node.can_enforce,
            bounds: &node.bounds,
            node_memory_bytes: node.memory_bytes,
            runtime_gate: node.runtime_gate,
            landlock_abi: node.landlock_abi,
            read_only_volumes,
            kernel_decides: cfg!(target_os = "linux"),
            grant: root_grant.as_deref(),
            access_invalid: matches!(parsed, Some(Err(_))),
            access: parsed.and_then(Result::ok),
            declared: Declared {
                run_as_user: *run_as_user,
                run_as_group: *run_as_group,
                run_as_non_root: *run_as_non_root,
                privileged: *privileged,
                allow_escalation: *allow_escalation,
                cap_add,
                cap_drop,
                read_only_root_fs: *read_only_root_fs,
                no_new_privileges: *no_new_privileges,
                host_pid: *host_pid,
                host_ipc: *host_ipc,
                host_network: *host_network,
                seccomp_profile: seccomp_profile.as_deref(),
                sysctls,
                devices,
                volume_devices,
                not_applied,
                memory_limit: resources.memory_limit_bytes.value(),
                memory_request: resources.memory_request_bytes.value(),
            },
        };
        let facts = serde_json::to_string(&facts).map_err(|e| e.to_string())?;
        self.0.ask("kakoi::enclose", facts).await
    }
}

#[derive(Serialize)]
struct AdmitFacts<'a> {
    mode: NativeSecurity,
    groups: &'a [String],
    object: &'a serde_json::Value,
    current: Option<&'a serde_json::Value>,
}

#[derive(Deserialize)]
struct Admitted {
    refused: Option<String>,
    warning: Option<String>,
}

/// Who may write a root grant: `kakoi::admit`, at the API boundary. Only a
/// write that names the annotation reaches blue, so a program that failed to
/// load refuses grants and nothing else.
pub struct RootGrantAdmission {
    kakoi: Result<Kakoi, String>,
    mode: NativeSecurity,
}

impl RootGrantAdmission {
    /// The rule for a node in `mode`.
    #[must_use]
    pub fn new(mode: NativeSecurity) -> Self {
        Self {
            kakoi: Kakoi::start(),
            mode,
        }
    }
}

#[async_trait::async_trait]
impl AdmissionWebhook for RootGrantAdmission {
    fn name(&self) -> &'static str {
        "kakoi-root-grant"
    }

    async fn review(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, AdmissionError> {
        let Some(object) = &request.value else {
            return Ok(AdmissionDecision::Allow);
        };
        if !object.to_string().contains(GRANT_ANNOTATION) {
            return Ok(AdmissionDecision::Allow);
        }
        let kakoi = self
            .kakoi
            .as_ref()
            .map_err(|e| AdmissionError::Backend(e.clone()))?;
        let facts = AdmitFacts {
            mode: self.mode,
            groups: &request.user_info.groups,
            object,
            current: request.current.as_ref(),
        };
        let facts =
            serde_json::to_string(&facts).map_err(|e| AdmissionError::Backend(e.to_string()))?;
        let answer: Admitted = kakoi
            .0
            .ask("kakoi::admit", facts)
            .await
            .map_err(AdmissionError::Backend)?;
        if let Some(warning) = answer.warning {
            tracing::warn!(user = %request.user_info.username, key = ?request.key, "{warning}");
        }
        Ok(answer
            .refused
            .map_or(AdmissionDecision::Allow, AdmissionDecision::Deny))
    }
}

/// Whether this daemon can change a child's identity: Linux, as root.
#[must_use]
pub fn can_enforce() -> bool {
    cfg!(target_os = "linux") && rustix::process::geteuid().is_root()
}

/// The kernel's Landlock ABI, 0 where it has none or Landlock is not enabled.
#[must_use]
pub fn landlock_abi() -> u32 {
    #[cfg(target_os = "linux")]
    {
        const CREATE_RULESET_VERSION: libc::c_ulong = 1;
        #[allow(unsafe_code)]
        let abi = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<u8>(),
                0_usize,
                CREATE_RULESET_VERSION,
            )
        };
        u32::try_from(abi).unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// The node's memory in bytes, 0 where it is not known.
#[must_use]
pub fn node_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let info = rustix::system::sysinfo();
        u64::try_from(info.totalram)
            .unwrap_or(0)
            .saturating_mul(u64::from(info.mem_unit))
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// A filesystem the host already has, as `/proc/self/mountinfo` states it.
#[cfg(target_os = "linux")]
struct HostMount {
    target: std::ffi::CString,
    keep: rustix::mount::MountFlags,
}

/// The mounts to make read-only in an enclosure: everything but what the kernel
/// serves for the process itself.
#[cfg(target_os = "linux")]
fn host_mounts(mountinfo: &str) -> Vec<HostMount> {
    use rustix::mount::MountFlags;
    mountinfo
        .lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let mut fields = left.split(' ');
            let target = fields.nth(4)?;
            let options = fields.next()?;
            let fstype = right.split(' ').next()?;
            let kernel = matches!(fstype, "proc" | "devtmpfs" | "devpts" | "mqueue")
                || target == "/dev"
                || target.starts_with("/dev/")
                || target.starts_with("/proc/");
            if kernel {
                return None;
            }
            let mut keep = MountFlags::empty();
            for option in options.split(',') {
                keep |= match option {
                    "nosuid" => MountFlags::NOSUID,
                    "nodev" => MountFlags::NODEV,
                    "noexec" => MountFlags::NOEXEC,
                    "noatime" => MountFlags::NOATIME,
                    "nodiratime" => MountFlags::NODIRATIME,
                    "relatime" => MountFlags::RELATIME,
                    _ => MountFlags::empty(),
                };
            }
            Some(HostMount {
                target: std::ffi::CString::new(mountinfo_path(target)).ok()?,
                keep,
            })
        })
        .collect()
}

/// A path as the kernel meant it: `mountinfo` writes a space, tab, newline or
/// backslash as a backslash and three octal digits.
#[cfg(target_os = "linux")]
fn mountinfo_path(field: &str) -> Vec<u8> {
    let bytes = field.as_bytes();
    let mut path = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let octal = bytes
            .get(i + 1..i + 4)
            .filter(|_| bytes[i] == b'\\')
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        if let Some(byte) = octal {
            path.push(byte);
            i += 4;
        } else {
            path.push(bytes[i]);
            i += 1;
        }
    }
    path
}

#[cfg(target_os = "linux")]
fn cstrings(paths: impl IntoIterator<Item = String>) -> Vec<std::ffi::CString> {
    paths
        .into_iter()
        .filter_map(|p| std::ffi::CString::new(p).ok())
        .collect()
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const AUDIT_ARCH: u32 = 0xc000_003e;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const AUDIT_ARCH: u32 = 0xc000_00b7;
#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
const AUDIT_ARCH: u32 = 0;

#[cfg(target_os = "linux")]
fn stmt(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

/// The filter: another architecture's numbers mean other calls, so a foreign
/// call is killed; a number above the newest allowed or a withheld one answers
/// `ENOSYS`; a denied number, or a guarded call whose first argument asks what
/// its guard refuses, answers `EPERM`; the rest run.
#[cfg(target_os = "linux")]
fn seccomp_filter(verdict: &Verdict) -> Vec<libc::sock_filter> {
    const LD_W_ABS: u16 = 0x20;
    const JEQ_K: u16 = 0x15;
    const JGT_K: u16 = 0x25;
    const JSET_K: u16 = 0x45;
    const RET_K: u16 = 0x06;
    const RET_KILL_PROCESS: u32 = 0x8000_0000;
    const RET_ERRNO_EPERM: u32 = 0x0005_0001;
    const RET_ERRNO_ENOSYS: u32 = 0x0005_0026;
    const RET_ALLOW: u32 = 0x7fff_0000;
    const OFFSET_NR: u32 = 0;
    const OFFSET_ARCH: u32 = 4;
    const OFFSET_ARG0: u32 = 16;
    let jump = |code, k, jf| libc::sock_filter { code, jt: 0, jf, k };
    let mut program = vec![
        stmt(LD_W_ABS, OFFSET_ARCH),
        libc::sock_filter {
            code: JEQ_K,
            jt: 1,
            jf: 0,
            k: AUDIT_ARCH,
        },
        stmt(RET_K, RET_KILL_PROCESS),
        stmt(LD_W_ABS, OFFSET_NR),
    ];
    if let Some(newest) = verdict.newest {
        program.push(jump(JGT_K, newest, 1));
        program.push(stmt(RET_K, RET_ERRNO_ENOSYS));
    }
    for number in &verdict.absent {
        program.push(jump(JEQ_K, *number, 1));
        program.push(stmt(RET_K, RET_ERRNO_ENOSYS));
    }
    for number in &verdict.deny {
        program.push(jump(JEQ_K, *number, 1));
        program.push(stmt(RET_K, RET_ERRNO_EPERM));
    }
    for guard in &verdict.guards {
        let mut body = vec![stmt(LD_W_ABS, OFFSET_ARG0)];
        if let Some(mask) = guard.mask {
            body.push(jump(JSET_K, mask, 1));
            body.push(stmt(RET_K, RET_ERRNO_EPERM));
        }
        for value in &guard.values {
            body.push(jump(JEQ_K, *value, 1));
            body.push(stmt(RET_K, RET_ERRNO_EPERM));
        }
        body.push(stmt(LD_W_ABS, OFFSET_NR));
        let skip = u8::try_from(body.len()).unwrap_or(u8::MAX);
        program.push(jump(JEQ_K, guard.number, skip));
        program.extend(body);
    }
    program.push(stmt(RET_K, RET_ALLOW));
    program
}

/// A capability blue names, as the kernel knows it. A word with no row refuses
/// the pod rather than running it with less than blue decided.
#[cfg(target_os = "linux")]
fn kept_capability(word: &str) -> std::io::Result<rustix::thread::CapabilitySet> {
    match word {
        "NET_BIND_SERVICE" => Ok(rustix::thread::CapabilitySet::NET_BIND_SERVICE),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("kakoi named a capability this seam does not know: {other}"),
        )),
    }
}

/// How a process comes into a pod's enclosure.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
enum Door {
    Start,
    Join,
}

/// A right blue names, as the kernel's bit. A word with no row refuses the pod
/// rather than running it with less than blue decided.
#[cfg(target_os = "linux")]
fn landlock_right(word: &str) -> std::io::Result<u64> {
    let bit = match word {
        "execute" => 0,
        "write-file" => 1,
        "read-file" => 2,
        "read-dir" => 3,
        "remove-dir" => 4,
        "remove-file" => 5,
        "make-char" => 6,
        "make-dir" => 7,
        "make-reg" => 8,
        "make-sock" => 9,
        "make-fifo" => 10,
        "make-block" => 11,
        "make-sym" => 12,
        "refer" => 13,
        "truncate" => 14,
        "ioctl-dev" => 15,
        "resolve-unix" => 16,
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("kakoi named a Landlock right this seam does not know: {other}"),
            ));
        }
    };
    Ok(1_u64 << bit)
}

/// A scope blue names, as the kernel's bit.
#[cfg(target_os = "linux")]
fn landlock_scope(word: &str) -> std::io::Result<u64> {
    match word {
        "abstract-unix" => Ok(1),
        "signal" => Ok(2),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("kakoi named a Landlock scope this seam does not know: {other}"),
        )),
    }
}

/// The rights the kernel accepts on a rule whose path is not a directory.
/// `resolve-unix` is left out: no rule names a socket itself, and a file holds
/// nothing to resolve.
#[cfg(target_os = "linux")]
const LANDLOCK_FILE_RIGHTS: u64 = 1 | (1 << 1) | (1 << 2) | (1 << 14) | (1 << 15);

/// A Landlock domain as the kernel takes it, built before the fork.
#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
struct LandlockCalls {
    handled: u64,
    scoped: u64,
    rules: Vec<(std::ffi::CString, u64)>,
}

#[cfg(target_os = "linux")]
impl LandlockCalls {
    fn of(landlock: &Landlock, scopes: &[String]) -> std::io::Result<Self> {
        let rights = |words: &[String]| {
            words.iter().try_fold(0_u64, |all, word| {
                Ok::<_, std::io::Error>(all | landlock_right(word)?)
            })
        };
        let mut rules = Vec::with_capacity(landlock.rules.len());
        for rule in &landlock.rules {
            let path = std::ffi::CString::new(rule.path.as_str())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            rules.push((path, rights(&rule.rights)?));
        }
        Ok(Self {
            handled: rights(&landlock.handled)?,
            scoped: scopes.iter().try_fold(0_u64, |all, word| {
                Ok::<_, std::io::Error>(all | landlock_scope(word)?)
            })?,
            rules,
        })
    }

    /// Open each path and build the ruleset. It runs after the mounts, so a rule
    /// holds the enclosure's own filesystems, and before the uid change, so a
    /// path under a directory the account cannot walk still gets its rule.
    fn ruleset(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        use std::os::fd::{AsRawFd, FromRawFd};

        #[repr(C)]
        struct RulesetAttr {
            handled_access_fs: u64,
            handled_access_net: u64,
            scoped: u64,
        }
        #[repr(C, packed)]
        struct PathBeneath {
            allowed_access: u64,
            parent_fd: i32,
        }
        const RULE_PATH_BENEATH: libc::c_long = 1;

        let attr = RulesetAttr {
            handled_access_fs: self.handled,
            handled_access_net: 0,
            scoped: self.scoped,
        };
        #[allow(unsafe_code)]
        let raw = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::from_ref(&attr),
                std::mem::size_of::<RulesetAttr>(),
                0_usize,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        #[allow(unsafe_code)]
        let ruleset = unsafe {
            std::os::fd::OwnedFd::from_raw_fd(
                i32::try_from(raw).map_err(|_| std::io::Error::from_raw_os_error(libc::EBADF))?,
            )
        };
        for (path, rights) in &self.rules {
            let opened = rustix::fs::open(
                path.as_c_str(),
                rustix::fs::OFlags::PATH | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            );
            let parent = match opened {
                Ok(fd) => fd,
                Err(rustix::io::Errno::NOENT | rustix::io::Errno::ACCESS) => continue,
                Err(e) => return Err(e.into()),
            };
            let directory =
                rustix::fs::FileType::from_raw_mode(rustix::fs::fstat(&parent)?.st_mode)
                    == rustix::fs::FileType::Directory;
            let allowed = if directory {
                *rights
            } else {
                *rights & LANDLOCK_FILE_RIGHTS
            };
            if allowed == 0 {
                continue;
            }
            let beneath = PathBeneath {
                allowed_access: allowed,
                parent_fd: parent.as_raw_fd(),
            };
            #[allow(unsafe_code)]
            let added = unsafe {
                libc::syscall(
                    libc::SYS_landlock_add_rule,
                    ruleset.as_raw_fd(),
                    RULE_PATH_BENEATH,
                    std::ptr::from_ref(&beneath),
                    0_usize,
                )
            };
            if added != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(ruleset)
    }
}

/// Enter the domain, which needs `no_new_privs` first.
#[cfg(target_os = "linux")]
fn landlock_restrict(ruleset: &std::os::fd::OwnedFd) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    #[allow(unsafe_code)]
    let entered = unsafe {
        libc::syscall(
            libc::SYS_landlock_restrict_self,
            ruleset.as_raw_fd(),
            0_usize,
        )
    };
    if entered != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// What the child's last steps need, built before the fork so the hook allocates
/// nothing.
#[cfg(target_os = "linux")]
struct Steps {
    filter: Vec<libc::sock_filter>,
    kept: Vec<rustix::thread::CapabilitySet>,
    landlock: Option<LandlockCalls>,
}

#[cfg(target_os = "linux")]
impl Steps {
    fn of(verdict: &Verdict, door: Door) -> std::io::Result<Self> {
        Ok(Self {
            landlock: verdict
                .landlock
                .as_ref()
                .map(|l| {
                    LandlockCalls::of(
                        l,
                        match door {
                            Door::Start => &l.scopes,
                            Door::Join => &l.join_scopes,
                        },
                    )
                })
                .transpose()?,
            filter: seccomp_filter(verdict),
            kept: verdict
                .keep
                .iter()
                .map(|word| kept_capability(word))
                .collect::<std::io::Result<_>>()?,
        })
    }
}

/// A container's own volume path, and whether it is mounted read-only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Volume {
    /// The path, the same on the host and in the container.
    pub path: String,
    /// Keep it read-only inside the enclosure.
    pub read_only: bool,
}

/// Arrange for the child to enter its enclosure just before `exec`.
///
/// `std` would change the uid before any hook runs, and mounting needs root, so
/// an enclosed child changes identity here, in this one order: namespace, mounts,
/// groups, gid, uid, capabilities, `no_new_privs`. Everything the hook touches
/// is prepared before the fork.
#[cfg(target_os = "linux")]
pub fn enter_on_exec(
    cmd: &mut tokio::process::Command,
    verdict: &Verdict,
    volumes: &[Volume],
) -> std::io::Result<()> {
    use rustix::mount::{MountFlags, MountPropagationFlags};
    use rustix::thread::UnshareFlags;

    let v = verdict.clone();
    if !v.enclosed && !v.no_new_privs && !v.clear_caps {
        if let Some(uid) = v.uid {
            cmd.uid(uid);
        }
        if let Some(gid) = v.gid {
            cmd.gid(gid);
        }
        return Ok(());
    }
    let hosts = if v.enclosed {
        host_mounts(&std::fs::read_to_string("/proc/self/mountinfo")?)
    } else {
        Vec::new()
    };
    let hide = cstrings(
        v.hide
            .iter()
            .filter(|p| std::path::Path::new(p).is_dir())
            .cloned(),
    );
    let stays_read_only = |m: &&Volume| m.read_only || v.narrowed.contains(&m.path);
    let writable = cstrings(
        volumes
            .iter()
            .filter(|m| !stays_read_only(m))
            .map(|m| m.path.clone()),
    );
    let readable = cstrings(
        volumes
            .iter()
            .filter(stays_read_only)
            .map(|m| m.path.clone()),
    );
    let unshare = v
        .namespaces
        .iter()
        .try_fold(UnshareFlags::empty(), |flags, word| {
            own_namespace(word).map(|(flag, _, _)| flags | flag)
        })?;
    let steps = Steps::of(&v, Door::Start)?;
    let empty = c"";
    let tmpfs = c"tmpfs";
    let root = c"/";
    let scratch = cstrings(
        v.scratch
            .iter()
            .filter(|p| std::path::Path::new(p).is_dir())
            .cloned(),
    );
    #[allow(unsafe_code)]
    unsafe {
        cmd.pre_exec(move || {
            if v.enclosed {
                rustix::thread::unshare_unsafe(unshare)?;
                rustix::mount::mount_change(
                    root,
                    MountPropagationFlags::PRIVATE | MountPropagationFlags::REC,
                )?;
                for path in writable.iter().chain(readable.iter()) {
                    rustix::mount::mount_bind(path.as_c_str(), path.as_c_str())?;
                }
                for host in &hosts {
                    if writable.iter().any(|w| w == &host.target) {
                        continue;
                    }
                    let flags = MountFlags::BIND | MountFlags::RDONLY | host.keep;
                    match rustix::mount::mount_remount(host.target.as_c_str(), flags, empty) {
                        Ok(()) | Err(rustix::io::Errno::NOENT | rustix::io::Errno::ACCESS) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                for path in &readable {
                    rustix::mount::mount_remount(
                        path.as_c_str(),
                        MountFlags::BIND | MountFlags::RDONLY,
                        empty,
                    )?;
                }
                for path in &scratch {
                    rustix::mount::mount(
                        tmpfs,
                        path.as_c_str(),
                        tmpfs,
                        MountFlags::NOSUID | MountFlags::NODEV,
                        c"mode=1777,size=256m",
                    )?;
                }
                rustix::mount::mount(
                    c"proc",
                    c"/proc",
                    c"proc",
                    MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOEXEC,
                    c"hidepid=invisible",
                )?;
                for path in &hide {
                    rustix::mount::mount(
                        tmpfs,
                        path.as_c_str(),
                        tmpfs,
                        MountFlags::RDONLY
                            | MountFlags::NOSUID
                            | MountFlags::NODEV
                            | MountFlags::NOEXEC,
                        c"mode=000,size=4k",
                    )?;
                }
            }
            become_declared(&v, &steps)
        });
    }
    Ok(())
}

/// The child's last steps before `exec`, in this one order: the Landlock ruleset,
/// groups, gid, uid, capabilities, `no_new_privs`, the Landlock domain, the
/// filter. A kept capability survives the uid change through `PR_SET_KEEPCAPS`,
/// and `exec` through the ambient set.
#[cfg(target_os = "linux")]
fn become_declared(v: &Verdict, steps: &Steps) -> std::io::Result<()> {
    use rustix::thread::{CapabilitySet, CapabilitySets};

    let ruleset = steps
        .landlock
        .as_ref()
        .map(LandlockCalls::ruleset)
        .transpose()?;
    if !steps.kept.is_empty() {
        rustix::thread::set_keep_capabilities(true)?;
    }
    if let Some(gid) = v.gid {
        rustix::thread::set_thread_groups(&[])?;
        rustix::thread::set_thread_gid(rustix::fs::Gid::from_raw(gid))?;
    }
    if let Some(uid) = v.uid {
        rustix::thread::set_thread_uid(rustix::fs::Uid::from_raw(uid))?;
    }
    if v.clear_caps {
        let kept = steps
            .kept
            .iter()
            .fold(CapabilitySet::empty(), |all, one| all | *one);
        rustix::thread::set_capabilities(
            None,
            CapabilitySets {
                effective: kept,
                permitted: kept,
                inheritable: kept,
            },
        )?;
        for one in &steps.kept {
            rustix::thread::configure_capability_in_ambient_set(*one, true)?;
        }
    }
    if v.no_new_privs {
        rustix::thread::set_no_new_privs(true)?;
    }
    if let Some(ruleset) = &ruleset {
        landlock_restrict(ruleset)?;
    }
    if !v.deny.is_empty() {
        let program = libc::sock_fprog {
            len: steps.filter.len() as u16,
            filter: steps.filter.as_ptr().cast_mut(),
        };
        #[allow(unsafe_code)]
        let installed = unsafe {
            libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                std::ptr::from_ref(&program),
            )
        };
        if installed != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Arrange for a command run in a live container (an exec or an exec probe) to
/// enter that container's enclosure and take its identity, filter included.
///
/// # Errors
/// An enclosed container whose process is gone, since nothing can be joined,
/// or the namespace that could not be opened.
#[cfg(target_os = "linux")]
pub fn join_on_exec(
    cmd: &mut tokio::process::Command,
    verdict: &Verdict,
    container_pid: Option<u32>,
) -> std::io::Result<()> {
    use rustix::thread::{LinkNameSpaceType, move_into_link_name_space};
    use std::os::fd::AsFd;

    let v = verdict.clone();
    if !v.enclosed && !v.no_new_privs && !v.clear_caps {
        if let Some(uid) = v.uid {
            cmd.uid(uid);
        }
        if let Some(gid) = v.gid {
            cmd.gid(gid);
        }
        return Ok(());
    }
    let mut joined = Vec::new();
    if v.enclosed {
        let pid = container_pid.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "the container's process is gone, so its enclosure cannot be joined",
            )
        })?;
        for word in &v.namespaces {
            let (_, kind, file) = own_namespace(word)?;
            joined.push((std::fs::File::open(format!("/proc/{pid}/ns/{file}"))?, kind));
        }
    }
    let steps = Steps::of(&v, Door::Join)?;
    #[allow(unsafe_code)]
    unsafe {
        cmd.pre_exec(move || {
            for (namespace, kind) in &joined {
                move_into_link_name_space(namespace.as_fd(), Some(*kind))?;
            }
            if v.enclosed {
                rustix::process::chdir(c"/")?;
            }
            become_declared(&v, &steps)
        });
    }
    Ok(())
}

/// A namespace blue named, as the kernel knows it: the flag that makes one, the
/// type that joins one, and its name under `/proc/<pid>/ns`. A word with no row
/// refuses the pod rather than running it with less than blue decided.
#[cfg(target_os = "linux")]
fn own_namespace(
    word: &str,
) -> std::io::Result<(
    rustix::thread::UnshareFlags,
    rustix::thread::LinkNameSpaceType,
    &'static str,
)> {
    use rustix::thread::{LinkNameSpaceType, UnshareFlags};
    match word {
        "mount" => Ok((UnshareFlags::NEWNS, LinkNameSpaceType::Mount, "mnt")),
        "ipc" => Ok((
            UnshareFlags::NEWIPC,
            LinkNameSpaceType::InterProcessCommunication,
            "ipc",
        )),
        "uts" => Ok((
            UnshareFlags::NEWUTS,
            LinkNameSpaceType::HostNameAndNISDomainName,
            "uts",
        )),
        "cgroup" => Ok((
            UnshareFlags::NEWCGROUP,
            LinkNameSpaceType::ControlGroup,
            "cgroup",
        )),
        "net" => Ok((UnshareFlags::NEWNET, LinkNameSpaceType::Network, "net")),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("kakoi named a namespace this seam does not know: {other}"),
        )),
    }
}

/// No enclosure can be entered off Linux; [`can_enforce`] said so first.
#[cfg(not(target_os = "linux"))]
pub fn enter_on_exec(
    cmd: &mut tokio::process::Command,
    verdict: &Verdict,
    _volumes: &[Volume],
) -> std::io::Result<()> {
    if let Some(uid) = verdict.uid {
        cmd.uid(uid);
    }
    if let Some(gid) = verdict.gid {
        cmd.gid(gid);
    }
    Ok(())
}

/// No enclosure exists off Linux, so a command in a container takes its identity.
#[cfg(not(target_os = "linux"))]
pub fn join_on_exec(
    cmd: &mut tokio::process::Command,
    verdict: &Verdict,
    _container_pid: Option<u32>,
) -> std::io::Result<()> {
    enter_on_exec(cmd, verdict, &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kakoi() -> Kakoi {
        Kakoi::start().expect("kakoi loads")
    }

    fn node(mode: NativeSecurity, can_enforce: bool) -> Node {
        Node {
            mode,
            can_enforce,
            bounds: vec!["memory", "pids"],
            memory_bytes: 68_719_476_736,
            runtime_gate: false,
            landlock_abi: 6,
        }
    }

    async fn verdict_of(c: &Confinement, volumes: &[String], node: &Node) -> Verdict {
        let resources = crate::backend::Resources::default();
        kakoi()
            .enclose(
                Asked {
                    confinement: c,
                    namespace: "",
                    volumes,
                    read_only_volumes: &[],
                    resources: &resources,
                },
                node,
            )
            .await
            .unwrap()
    }

    async fn enclosed_default() -> Verdict {
        verdict_of(
            &Confinement::default(),
            &[],
            &node(NativeSecurity::Enforce, true),
        )
        .await
    }

    #[cfg(target_os = "linux")]
    async fn enclosed_verdict() -> Verdict {
        verdict_of(
            &Confinement::default(),
            &[],
            &node(NativeSecurity::Enforce, true),
        )
        .await
    }

    #[cfg(target_os = "linux")]
    const EPERM: u32 = 0x0005_0001;
    #[cfg(target_os = "linux")]
    const ENOSYS: u32 = 0x0005_0026;
    #[cfg(target_os = "linux")]
    const ALLOW: u32 = 0x7fff_0000;
    #[cfg(target_os = "linux")]
    const KILL: u32 = 0x8000_0000;

    #[tokio::test]
    async fn every_host_effect_blue_knows_has_a_rendering_here() {
        let rendered: Vec<String> = kakoi()
            .0
            .ask("kakoi::rendered_capabilities", "{}".to_string())
            .await
            .unwrap();
        for effect in engenho_blue::Capability::host_effects() {
            assert!(
                rendered.iter().any(|row| row == effect.label()),
                "{} has no row in kakoi::capability_rendering",
                effect.label()
            );
        }
    }

    #[tokio::test]
    async fn a_pods_access_narrows_its_enclosure_and_a_bad_one_refuses_it() {
        let confined = |access: &str| Confinement {
            access: Some(access.to_string()),
            ..Confinement::default()
        };
        let volumes = ["/data".to_string(), "/cfg".to_string()];
        let on = |c: Confinement| {
            let volumes = volumes.clone();
            async move { verdict_of(&c, &volumes, &node(NativeSecurity::Enforce, true)).await }
        };
        let closed = on(confined(r#"{"effects":["filesystem"],"writes":["/data"]}"#)).await;
        assert!(closed.namespaces.iter().any(|n| n == "net"));
        assert_eq!(closed.narrowed, vec!["/cfg".to_string()]);
        let open = on(Confinement::default()).await;
        assert!(!open.namespaces.iter().any(|n| n == "net"));
        assert!(open.narrowed.is_empty());
        let unread = on(confined("not json")).await;
        assert!(unread.refused.unwrap().contains(ACCESS_ANNOTATION));
    }

    fn write_as(
        groups: &[&str],
        value: serde_json::Value,
        current: Option<serde_json::Value>,
    ) -> AdmissionRequest {
        AdmissionRequest {
            action: engenho_controllers::admission::AdmissionAction::Put,
            key: engenho_store::ResourceKey::namespaced("", "v1", "Pod", "ns", "p"),
            value: Some(value),
            current,
            user_info: engenho_types::auth::UserInfo {
                username: "someone".to_string(),
                groups: groups.iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn only_a_holder_may_write_a_root_grant_on_a_pod_or_in_a_template() {
        let rule = RootGrantAdmission::new(NativeSecurity::Enforce);
        let granted = serde_json::json!({"metadata": {"annotations": {GRANT_ANNOTATION: "why"}}});
        let cron = serde_json::json!({
            "spec": {"jobTemplate": {"spec": {"template": granted.clone()}}}
        });
        for object in [&granted, &cron] {
            let refused = rule
                .review(&write_as(&["system:authenticated"], object.clone(), None))
                .await
                .unwrap();
            assert!(matches!(refused, AdmissionDecision::Deny(_)), "{refused:?}");
            let allowed = rule
                .review(&write_as(&["system:masters"], object.clone(), None))
                .await
                .unwrap();
            assert_eq!(allowed, AdmissionDecision::Allow);
        }
        let unchanged = rule
            .review(&write_as(&[], granted.clone(), Some(granted.clone())))
            .await
            .unwrap();
        assert_eq!(unchanged, AdmissionDecision::Allow);
        let plain = rule
            .review(&write_as(&[], serde_json::json!({"metadata": {}}), None))
            .await
            .unwrap();
        assert_eq!(plain, AdmissionDecision::Allow);
    }

    #[tokio::test]
    async fn a_rule_that_failed_to_load_refuses_grants_and_nothing_else() {
        let rule = RootGrantAdmission {
            kakoi: Err("no program".to_string()),
            mode: NativeSecurity::Enforce,
        };
        let plain = rule
            .review(&write_as(&[], serde_json::json!({"metadata": {}}), None))
            .await
            .unwrap();
        assert_eq!(plain, AdmissionDecision::Allow);
        let granted = serde_json::json!({"metadata": {"annotations": {GRANT_ANNOTATION: "why"}}});
        assert!(
            rule.review(&write_as(&["system:masters"], granted, None))
                .await
                .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_mount_with_a_space_in_its_path_is_still_found() {
        let found = host_mounts(
            "36 25 8:1 / /mnt/my\\040disk rw,noatime shared:1 - ext4 /dev/sda1 rw\n\
             37 25 0:5 / /dev/shm rw shared:2 - tmpfs tmpfs rw\n",
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target.as_bytes(), b"/mnt/my disk");
        assert_eq!(mountinfo_path("/a\\134b"), b"/a\\b");
        assert_eq!(mountinfo_path("/plain"), b"/plain");
        assert_eq!(mountinfo_path("/end\\04"), b"/end\\04");
    }

    #[cfg(target_os = "linux")]
    fn run_filter(program: &[libc::sock_filter], arch: u32, nr: u32, arg0: u32) -> u32 {
        let load = |offset| match offset {
            0 => nr,
            4 => arch,
            16 => arg0,
            other => panic!("the filter reads an unknown offset {other}"),
        };
        let mut pc = 0;
        let mut a = 0;
        loop {
            let i = program[pc];
            pc += 1;
            let taken = match i.code {
                0x20 => {
                    a = load(i.k);
                    continue;
                }
                0x06 => return i.k,
                0x15 => a == i.k,
                0x25 => a > i.k,
                0x45 => a & i.k != 0,
                other => panic!("the filter holds an unknown instruction {other:#x}"),
            };
            pc += usize::from(if taken { i.jt } else { i.jf });
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_filter_answers_each_call_as_blue_declared() {
        let v = enclosed_verdict().await;
        let program = seccomp_filter(&v);
        let on = |nr, arg0| run_filter(&program, AUDIT_ARCH, nr, arg0);
        let number = |sys: libc::c_long| u32::try_from(sys).unwrap();
        assert_eq!(on(number(libc::SYS_getpid), 0), ALLOW);
        assert_eq!(on(number(libc::SYS_mount), 0), EPERM);
        assert_eq!(on(number(libc::SYS_io_uring_setup), 0), EPERM);
        assert_eq!(on(number(libc::SYS_fsopen), 0), EPERM);
        assert_eq!(on(number(libc::SYS_clone3), 0), ENOSYS);
        assert_eq!(on(0x4000_0000 | number(libc::SYS_mount), 0), ENOSYS);
        assert_eq!(on(v.newest.unwrap(), 0), ALLOW);
        assert_eq!(on(v.newest.unwrap() + 1, 0), ENOSYS);
        let clone = number(libc::SYS_clone);
        let thread = u32::try_from(libc::CLONE_VM | libc::CLONE_THREAD).unwrap();
        assert_eq!(on(clone, thread), ALLOW);
        for flag in [
            libc::CLONE_NEWNS,
            libc::CLONE_NEWUSER,
            libc::CLONE_NEWPID,
            libc::CLONE_NEWNET,
            libc::CLONE_NEWIPC,
            libc::CLONE_NEWUTS,
            libc::CLONE_NEWCGROUP,
        ] {
            assert_eq!(on(clone, u32::try_from(flag).unwrap()), EPERM, "{flag:#x}");
        }
        let socket = number(libc::SYS_socket);
        assert_eq!(on(socket, u32::try_from(libc::AF_INET).unwrap()), ALLOW);
        assert_eq!(on(socket, u32::try_from(libc::AF_UNIX).unwrap()), ALLOW);
        assert_eq!(on(socket, u32::try_from(libc::AF_ALG).unwrap()), EPERM);
        assert_eq!(on(socket, u32::try_from(libc::AF_VSOCK).unwrap()), EPERM);
        assert_eq!(run_filter(&program, 0x4000_0003, 1, 0), KILL);
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[tokio::test]
    async fn the_denied_numbers_are_the_kernels_for_their_names() {
        let mut expected: Vec<libc::c_long> = vec![
            libc::SYS_mount,
            libc::SYS_umount2,
            libc::SYS_pivot_root,
            libc::SYS_chroot,
            libc::SYS_unshare,
            libc::SYS_setns,
            libc::SYS_ptrace,
            libc::SYS_process_vm_readv,
            libc::SYS_process_vm_writev,
            libc::SYS_bpf,
            libc::SYS_perf_event_open,
            libc::SYS_kexec_load,
            libc::SYS_kexec_file_load,
            libc::SYS_init_module,
            libc::SYS_finit_module,
            libc::SYS_delete_module,
            libc::SYS_reboot,
            libc::SYS_swapon,
            libc::SYS_swapoff,
            libc::SYS_acct,
            libc::SYS_add_key,
            libc::SYS_request_key,
            libc::SYS_keyctl,
            libc::SYS_userfaultfd,
            libc::SYS_open_by_handle_at,
            libc::SYS_quotactl,
            libc::SYS_settimeofday,
            libc::SYS_clock_settime,
            libc::SYS_adjtimex,
            libc::SYS_iopl,
            libc::SYS_ioperm,
            libc::SYS_io_uring_setup,
            libc::SYS_io_uring_enter,
            libc::SYS_io_uring_register,
            libc::SYS_open_tree,
            libc::SYS_move_mount,
            libc::SYS_fsopen,
            libc::SYS_fsconfig,
            libc::SYS_fsmount,
            libc::SYS_fspick,
            libc::SYS_mount_setattr,
            libc::SYS_pidfd_getfd,
            libc::SYS_process_madvise,
            libc::SYS_quotactl_fd,
            libc::SYS_kcmp,
            libc::SYS_fanotify_init,
            libc::SYS_syslog,
            libc::SYS_clock_adjtime,
            libc::SYS_sethostname,
            libc::SYS_setdomainname,
            libc::SYS_vhangup,
            libc::SYS_lookup_dcookie,
            libc::SYS_nfsservctl,
            libc::SYS_mbind,
            libc::SYS_set_mempolicy,
            libc::SYS_migrate_pages,
            libc::SYS_move_pages,
            libc::SYS_uselib,
            libc::SYS_ustat,
            libc::SYS_sysfs,
        ];
        expected.sort_unstable();
        let mut denied: Vec<libc::c_long> = enclosed_verdict()
            .await
            .deny
            .iter()
            .map(|n| libc::c_long::from(*n))
            .collect();
        denied.sort_unstable();
        assert_eq!(denied, expected);
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_code)]
    #[tokio::test]
    async fn the_kernel_answers_a_filtered_child_as_the_filter_says() {
        let filter = seccomp_filter(&enclosed_verdict().await);
        let program = libc::sock_fprog {
            len: u16::try_from(filter.len()).unwrap(),
            filter: filter.as_ptr().cast_mut(),
        };
        let probes: [(libc::c_long, libc::c_long, i32); 4] = [
            (libc::SYS_io_uring_setup, 1, libc::EPERM),
            (libc::SYS_clone3, 0, libc::ENOSYS),
            (
                libc::SYS_clone,
                libc::c_long::from(libc::CLONE_NEWUSER),
                libc::EPERM,
            ),
            (libc::SYS_fsopen, 0, libc::EPERM),
        ];
        unsafe {
            let child = libc::fork();
            assert!(child >= 0);
            if child == 0 {
                libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
                let installed = libc::prctl(
                    libc::PR_SET_SECCOMP,
                    libc::SECCOMP_MODE_FILTER,
                    std::ptr::from_ref(&program),
                );
                if installed != 0 {
                    libc::_exit(100);
                }
                for (index, (call, arg0, errno)) in probes.iter().enumerate() {
                    let answer = libc::syscall(*call, *arg0, 0, 0, 0, 0);
                    if answer != -1 || *libc::__errno_location() != *errno {
                        libc::_exit(1 + i32::try_from(index).unwrap_or(50));
                    }
                }
                libc::_exit(0);
            }
            let mut status = 0;
            assert_eq!(libc::waitpid(child, &raw mut status, 0), child);
            assert!(libc::WIFEXITED(status), "status {status}");
            assert_eq!(libc::WEXITSTATUS(status), 0);
        }
    }

    #[tokio::test]
    async fn a_pod_that_says_nothing_is_enclosed_by_default() {
        let v = enclosed_default().await;
        assert_eq!(v.refused, None);
        assert_eq!((v.uid, v.gid), (Some(65532), Some(65532)));
        assert!(v.no_new_privs && v.clear_caps);
    }

    #[tokio::test]
    async fn root_without_a_grant_is_refused_and_with_one_starts() {
        let mut c = Confinement {
            run_as_user: Some(0),
            ..Confinement::default()
        };
        let refused = verdict_of(&c, &[], &node(NativeSecurity::Enforce, true)).await;
        assert!(refused.refused.unwrap().contains("kakoi.pleme.io/root"));
        c.root_grant = Some("drops to the unit's account".to_string());
        let granted = verdict_of(&c, &[], &node(NativeSecurity::Enforce, true)).await;
        assert_eq!(granted.refused, None);
        assert_eq!(granted.uid, Some(0));
    }

    #[test]
    fn a_pod_level_identity_reaches_a_container_that_states_none() {
        let pod = serde_json::json!({
            "metadata": {"annotations": {GRANT_ANNOTATION: "drops to its account"}},
            "spec": {"securityContext": {"runAsUser": 65532, "runAsGroup": 7, "runAsNonRoot": true}}
        });
        let c = Confinement::default().inheriting_pod(&pod);
        assert_eq!(c.run_as_user, Some(65532));
        assert_eq!(c.run_as_group, Some(7));
        assert_eq!(c.run_as_non_root, Some(true));
        assert_eq!(c.root_grant.as_deref(), Some("drops to its account"));
    }

    #[test]
    fn every_security_field_of_a_pod_reaches_the_facts() {
        let pod = serde_json::json!({
            "spec": {
                "hostPID": true,
                "hostIPC": true,
                "hostNetwork": true,
                "shareProcessNamespace": true,
                "hostUsers": false,
                "securityContext": {
                    "seccompProfile": {"type": "Localhost"},
                    "sysctls": [{"name": "kernel.shm_rmid_forced", "value": "1"}],
                    "fsGroup": 2000,
                    "supplementalGroups": [3],
                    "appArmorProfile": {"type": "RuntimeDefault"},
                    "seLinuxOptions": {"level": "s0"}
                },
                "volumes": [
                    {"name": "radio", "hostPath": {"path": "/dev/ttyUSB0", "type": "CharDevice"}},
                    {"name": "disk", "hostPath": {"path": "/dev/sdz", "type": "BlockDevice"}},
                    {"name": "data", "hostPath": {"path": "/data", "type": "Directory"}},
                    {"name": "bare", "hostPath": {"path": "/bare"}}
                ]
            }
        });
        let c = Confinement::default()
            .inheriting_pod(&pod)
            .reading_container(&serde_json::json!({
                "volumeDevices": [{"name": "b", "devicePath": "/dev/xvda"}]
            }));
        assert!(c.host_pid && c.host_ipc && c.host_network);
        assert_eq!(c.seccomp_profile.as_deref(), Some("Localhost"));
        assert_eq!(c.sysctls, vec!["kernel.shm_rmid_forced".to_string()]);
        assert_eq!(
            c.devices,
            vec!["/dev/ttyUSB0".to_string(), "/dev/sdz".to_string()]
        );
        assert_eq!(c.volume_devices, vec!["/dev/xvda".to_string()]);
        assert_eq!(
            c.not_applied,
            [
                "fsGroup",
                "supplementalGroups",
                "appArmorProfile",
                "seLinuxOptions",
                "shareProcessNamespace",
                "hostUsers"
            ]
            .map(str::to_string)
            .to_vec()
        );
        let quiet = Confinement::default().inheriting_pod(&serde_json::json!({
            "spec": {"hostUsers": true, "shareProcessNamespace": false, "hostNetwork": false,
                     "securityContext": {"supplementalGroups": []}}
        }));
        assert!(quiet.not_applied.is_empty(), "{quiet:?}");
        assert!(!quiet.host_network);
    }

    #[test]
    fn a_containers_own_profile_wins_and_its_own_fields_are_named() {
        let sc = serde_json::json!({
            "seccompProfile": {"type": "RuntimeDefault"},
            "procMount": "Unmasked",
            "seLinuxOptions": {"level": "s0"}
        });
        let pod = serde_json::json!({
            "spec": {"securityContext": {"seccompProfile": {"type": "Unconfined"}, "seLinuxOptions": {"level": "s1"}}}
        });
        let c = Confinement::from_pod_json(Some(&sc), "p", "c")
            .unwrap()
            .inheriting_pod(&pod);
        assert_eq!(c.seccomp_profile.as_deref(), Some("RuntimeDefault"));
        assert_eq!(
            c.not_applied,
            ["procMount", "seLinuxOptions"].map(str::to_string).to_vec()
        );
        let plain = Confinement::from_pod_json(
            Some(&serde_json::json!({"procMount": "Default"})),
            "p",
            "c",
        )
        .unwrap();
        assert!(plain.not_applied.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_capability_blue_names_maps_to_the_kernels_and_any_other_refuses() {
        assert_eq!(
            kept_capability("NET_BIND_SERVICE").unwrap(),
            rustix::thread::CapabilitySet::NET_BIND_SERVICE
        );
        assert!(kept_capability("SYS_ADMIN").is_err());
    }

    #[tokio::test]
    async fn a_pod_keeps_the_keepable_capability_without_a_grant() {
        let on = |cap_add: Vec<String>| async move {
            let c = Confinement {
                cap_add,
                ..Confinement::default()
            };
            verdict_of(&c, &[], &node(NativeSecurity::Enforce, true)).await
        };
        let kept = on(vec!["NET_BIND_SERVICE".to_string()]).await;
        assert_eq!(kept.refused, None);
        assert!(kept.enclosed);
        assert_eq!(kept.keep, vec!["NET_BIND_SERVICE".to_string()]);
        let refused = on(vec!["NET_RAW".to_string()]).await;
        assert!(refused.refused.unwrap().contains(GRANT_ANNOTATION));
    }

    #[tokio::test]
    async fn a_pod_with_no_limit_is_bounded_by_default_and_a_declared_one_stands() {
        let v = enclosed_default().await;
        assert_eq!(v.limits.pids_max, Some(4096));
        assert_eq!(v.limits.memory_max, Some(17_179_869_184));
        let resources = crate::backend::Resources::from_container_json(&serde_json::json!({
            "resources": {"limits": {"memory": "64Mi"}}
        }));
        let declared = kakoi()
            .enclose(
                Asked {
                    confinement: &Confinement::default(),
                    namespace: "",
                    volumes: &[],
                    read_only_volumes: &[],
                    resources: &resources,
                },
                &node(NativeSecurity::Enforce, true),
            )
            .await
            .unwrap();
        assert_eq!(declared.limits.memory_max, None);
        assert_eq!(declared.limits.pids_max, Some(4096));
        let mut blind = node(NativeSecurity::Enforce, true);
        blind.bounds.clear();
        let blind = verdict_of(&Confinement::default(), &[], &blind).await;
        assert_eq!(blind.refused, None);
        assert_eq!(blind.limits, DefaultLimits::default());
        assert!(
            blind.warnings.iter().any(|w| w.contains("pids.max")),
            "{blind:?}"
        );
        assert!(
            blind.warnings.iter().any(|w| w.contains("memory.max")),
            "{blind:?}"
        );
    }

    #[test]
    fn a_default_bound_lands_in_the_plan_and_a_declared_limit_wins() {
        use crate::cgroup::{CgroupPlan, Knob};
        let limits = DefaultLimits {
            pids_max: Some(4096),
            memory_max: Some(1 << 30),
        };
        let plan = CgroupPlan::default().with_defaults(&limits);
        assert_eq!(plan.memory_max, Some(1 << 30));
        assert!(plan.writes().contains(&(Knob::PidsMax, "4096".to_string())));
        assert!(
            plan.writes()
                .contains(&(Knob::MemorySwapMax, "0".to_string()))
        );
        let declared = CgroupPlan {
            memory_max: Some(1 << 26),
            ..CgroupPlan::default()
        }
        .with_defaults(&limits);
        assert_eq!(declared.memory_max, Some(1 << 26));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_node_states_its_memory() {
        assert!(node_memory_bytes() > 1 << 20);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn each_right_and_scope_is_the_kernels_bit() {
        let rows: [(&str, u64); 17] = [
            ("execute", 1 << 0),
            ("write-file", 1 << 1),
            ("read-file", 1 << 2),
            ("read-dir", 1 << 3),
            ("remove-dir", 1 << 4),
            ("remove-file", 1 << 5),
            ("make-char", 1 << 6),
            ("make-dir", 1 << 7),
            ("make-reg", 1 << 8),
            ("make-sock", 1 << 9),
            ("make-fifo", 1 << 10),
            ("make-block", 1 << 11),
            ("make-sym", 1 << 12),
            ("refer", 1 << 13),
            ("truncate", 1 << 14),
            ("ioctl-dev", 1 << 15),
            ("resolve-unix", 1 << 16),
        ];
        for (word, bit) in rows {
            assert_eq!(landlock_right(word).unwrap(), bit, "{word}");
        }
        assert!(landlock_right("mount").is_err());
        assert_eq!(landlock_scope("abstract-unix").unwrap(), 1);
        assert_eq!(landlock_scope("signal").unwrap(), 2);
        assert!(landlock_scope("network").is_err());
        assert_eq!(
            LANDLOCK_FILE_RIGHTS,
            [
                "execute",
                "write-file",
                "read-file",
                "truncate",
                "ioctl-dev"
            ]
            .iter()
            .fold(0, |all, word| all | landlock_right(word).unwrap())
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_seam_makes_blues_domain_into_the_kernels_calls_for_each_door() {
        let v = enclosed_default().await;
        let landlock = v.landlock.clone().expect("a domain");
        assert!(landlock.handled.iter().any(|w| w == "execute"));
        let start = Steps::of(&v, Door::Start).unwrap().landlock.unwrap();
        let join = Steps::of(&v, Door::Join).unwrap().landlock.unwrap();
        assert_eq!(start.scoped, 3);
        assert_eq!(join.scoped, 1);
        assert_eq!(start.handled, join.handled);
        assert_ne!(start.handled & (1 << 13), 0, "refer is ABI 2");
        assert_eq!(start.handled & (1 << 16), 0, "resolve-unix is ABI 9");
        let store = start
            .rules
            .iter()
            .filter(|(path, _)| path.as_bytes() == b"/nix/store")
            .fold(0, |all, (_, rights)| all | rights);
        assert_eq!(store, 1 | (1 << 2) | (1 << 3));
        let unknown = Landlock {
            handled: vec!["mount".to_string()],
            ..Landlock::default()
        };
        assert!(LandlockCalls::of(&unknown, &[]).is_err());
        let bad_scope = Landlock::default();
        assert!(LandlockCalls::of(&bad_scope, &["network".to_string()]).is_err());
    }

    #[cfg(target_os = "linux")]
    #[allow(unsafe_code)]
    #[test]
    fn the_kernel_holds_the_domain_the_seam_builds_and_takes_no_rule_it_would_refuse() {
        let abi = landlock_abi();
        if abi == 0 {
            eprintln!("this kernel has no Landlock, so this run proves nothing");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        let denied = dir.path().join("denied");
        std::fs::create_dir(&allowed).unwrap();
        std::fs::create_dir(&denied).unwrap();
        std::fs::write(allowed.join("f"), "x").unwrap();
        std::fs::write(denied.join("f"), "x").unwrap();
        let single = dir.path().join("single");
        std::fs::write(&single, "x").unwrap();
        let text = |p: &std::path::Path| p.to_string_lossy().to_string();
        let words = |list: &[&str]| list.iter().map(ToString::to_string).collect::<Vec<_>>();
        let landlock = Landlock {
            handled: words(&["read-file", "read-dir", "write-file"]),
            rules: vec![
                LandlockRule {
                    path: text(&allowed),
                    rights: words(&["read-file", "read-dir"]),
                },
                LandlockRule {
                    path: text(&single),
                    rights: words(&["read-file", "read-dir"]),
                },
                LandlockRule {
                    path: text(&dir.path().join("absent")),
                    rights: words(&["read-file"]),
                },
            ],
            ..Landlock::default()
        };
        let scopes = if abi >= 6 {
            words(&["signal"])
        } else {
            Vec::new()
        };
        let calls = LandlockCalls::of(&landlock, &scopes).unwrap();
        let path_of = |p: std::path::PathBuf| std::ffi::CString::new(text(&p)).unwrap();
        let readable = path_of(allowed.join("f"));
        let refused = path_of(denied.join("f"));
        let lone = path_of(single.clone());
        let parent = libc::pid_t::try_from(std::process::id()).unwrap();
        unsafe {
            let child = libc::fork();
            assert!(child >= 0);
            if child == 0 {
                let Ok(ruleset) = calls.ruleset() else {
                    libc::_exit(10);
                };
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                    libc::_exit(11);
                }
                if landlock_restrict(&ruleset).is_err() {
                    libc::_exit(12);
                }
                if libc::open(readable.as_ptr(), libc::O_RDONLY) < 0 {
                    libc::_exit(13);
                }
                if libc::open(lone.as_ptr(), libc::O_RDONLY) < 0 {
                    libc::_exit(14);
                }
                if libc::open(refused.as_ptr(), libc::O_RDONLY) >= 0
                    || *libc::__errno_location() != libc::EACCES
                {
                    libc::_exit(15);
                }
                if abi >= 6
                    && (libc::kill(parent, 0) == 0 || *libc::__errno_location() != libc::EPERM)
                {
                    libc::_exit(16);
                }
                libc::_exit(0);
            }
            let mut status = 0;
            assert_eq!(libc::waitpid(child, &raw mut status, 0), child);
            assert!(libc::WIFEXITED(status), "status {status}");
            assert_eq!(libc::WEXITSTATUS(status), 0);
        }
    }

    #[tokio::test]
    async fn a_kernel_without_landlock_is_a_named_weaker_state_never_a_refusal() {
        let mut bare = node(NativeSecurity::Enforce, true);
        bare.landlock_abi = 0;
        let none = verdict_of(&Confinement::default(), &[], &bare).await;
        assert_eq!(none.refused, None);
        assert!(none.enclosed);
        assert_eq!(none.landlock, None);
        assert!(
            none.warnings.iter().any(|w| w.contains("no Landlock")),
            "{none:?}"
        );
        let six = enclosed_default().await;
        assert!(six.warnings.iter().any(|w| w.contains("ABI 9")), "{six:?}");
        assert!(
            !six.warnings.iter().any(|w| w.contains("needs ABI 6")),
            "{six:?}"
        );
        let mut nine = node(NativeSecurity::Enforce, true);
        nine.landlock_abi = 9;
        let nine = verdict_of(&Confinement::default(), &[], &nine).await;
        assert!(nine.warnings.is_empty(), "{nine:?}");
    }

    #[test]
    fn this_kernels_abi_reads_as_a_number() {
        let abi = landlock_abi();
        assert!(abi < 1000, "{abi}");
        #[cfg(not(target_os = "linux"))]
        assert_eq!(abi, 0);
    }

    #[tokio::test]
    async fn a_runtime_gate_leaves_to_its_runtime_what_only_the_native_seam_cannot_apply() {
        let mut gate = node(NativeSecurity::Enforce, true);
        gate.runtime_gate = true;
        for c in [
            Confinement {
                sysctls: vec!["a.b".into()],
                ..Confinement::default()
            },
            Confinement {
                seccomp_profile: Some("Localhost".into()),
                ..Confinement::default()
            },
            Confinement {
                run_as_user: Some(100_001),
                ..Confinement::default()
            },
        ] {
            assert_eq!(verdict_of(&c, &[], &gate).await.refused, None, "{c:?}");
        }
        let root = Confinement {
            host_pid: true,
            ..Confinement::default()
        };
        assert!(verdict_of(&root, &[], &gate).await.refused.is_some());
    }

    #[tokio::test]
    async fn blue_decides_each_new_fact_the_seam_states() {
        let on = |c: Confinement| async move {
            verdict_of(&c, &[], &node(NativeSecurity::Enforce, true)).await
        };
        let refused_for = [
            (
                Confinement {
                    host_pid: true,
                    ..Confinement::default()
                },
                "hostPID",
            ),
            (
                Confinement {
                    host_ipc: true,
                    ..Confinement::default()
                },
                "hostIPC",
            ),
            (
                Confinement {
                    seccomp_profile: Some("Unconfined".into()),
                    ..Confinement::default()
                },
                "Unconfined",
            ),
            (
                Confinement {
                    seccomp_profile: Some("Localhost".into()),
                    ..Confinement::default()
                },
                "Localhost",
            ),
            (
                Confinement {
                    sysctls: vec!["a.b".into()],
                    ..Confinement::default()
                },
                "a.b",
            ),
            (
                Confinement {
                    devices: vec!["/dev/x".into()],
                    ..Confinement::default()
                },
                "/dev/x",
            ),
            (
                Confinement {
                    volume_devices: vec!["/dev/y".into()],
                    ..Confinement::default()
                },
                "/dev/y",
            ),
            (
                Confinement {
                    host_network: true,
                    access: Some(r#"{"effects":["clock"]}"#.into()),
                    ..Confinement::default()
                },
                "hostNetwork",
            ),
        ];
        for (confinement, name) in refused_for {
            let refused = on(confinement).await.refused.expect(name);
            assert!(refused.contains(name), "{refused}");
        }
        let fine = on(Confinement {
            seccomp_profile: Some("RuntimeDefault".into()),
            host_network: true,
            ..Confinement::default()
        })
        .await;
        assert_eq!(fine.refused, None);
        assert!(fine.enclosed);
        let told = on(Confinement {
            not_applied: vec!["fsGroup".into()],
            ..Confinement::default()
        })
        .await;
        assert!(
            told.warnings.iter().any(|w| w.contains("fsGroup")),
            "{told:?}"
        );
    }

    #[test]
    fn a_container_field_wins_over_its_pod_and_a_blank_grant_grants_nothing() {
        let pod = serde_json::json!({
            "metadata": {"annotations": {GRANT_ANNOTATION: "  "}},
            "spec": {"securityContext": {"runAsUser": 1}}
        });
        let c = Confinement {
            run_as_user: Some(2),
            ..Confinement::default()
        }
        .inheriting_pod(&pod);
        assert_eq!(c.run_as_user, Some(2));
        assert_eq!(c.root_grant, None);
    }

    #[tokio::test]
    async fn a_node_that_cannot_enforce_says_so_and_changes_nothing() {
        let v = verdict_of(
            &Confinement::default(),
            &[],
            &node(NativeSecurity::Enforce, false),
        )
        .await;
        assert_eq!(v.uid, None);
        assert!(!v.no_new_privs);
        assert_eq!(v.warnings.len(), 1);
    }
}
