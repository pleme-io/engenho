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
}

#[derive(Serialize)]
struct Facts<'a> {
    arch: &'a str,
    namespace: &'a str,
    volumes: &'a [String],
    mode: NativeSecurity,
    can_enforce: bool,
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
    /// Clear the child's capability sets.
    pub clear_caps: bool,
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
    pub async fn enclose(
        &self,
        confinement: &Confinement,
        namespace: &str,
        volumes: &[String],
        mode: NativeSecurity,
        can_enforce: bool,
    ) -> Result<Verdict, String> {
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
        } = confinement;
        let parsed = access
            .as_deref()
            .map(serde_json::from_str::<serde_json::Value>);
        let facts = Facts {
            arch: std::env::consts::ARCH,
            namespace,
            volumes,
            mode,
            can_enforce,
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
    let filter = seccomp_filter(&v);
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
            become_declared(&v, &filter)
        });
    }
    Ok(())
}

/// The child's last steps before `exec`, in this one order: groups, gid, uid,
/// capabilities, `no_new_privs`, the filter.
#[cfg(target_os = "linux")]
fn become_declared(v: &Verdict, filter: &[libc::sock_filter]) -> std::io::Result<()> {
    use rustix::thread::{CapabilitySet, CapabilitySets};

    if let Some(gid) = v.gid {
        rustix::thread::set_thread_groups(&[])?;
        rustix::thread::set_thread_gid(rustix::fs::Gid::from_raw(gid))?;
    }
    if let Some(uid) = v.uid {
        rustix::thread::set_thread_uid(rustix::fs::Uid::from_raw(uid))?;
    }
    if v.clear_caps {
        let none = CapabilitySet::empty();
        rustix::thread::set_capabilities(
            None,
            CapabilitySets {
                effective: none,
                permitted: none,
                inheritable: none,
            },
        )?;
    }
    if v.no_new_privs {
        rustix::thread::set_no_new_privs(true)?;
    }
    if !v.deny.is_empty() {
        let program = libc::sock_fprog {
            len: filter.len() as u16,
            filter: filter.as_ptr().cast_mut(),
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
    let filter = seccomp_filter(&v);
    #[allow(unsafe_code)]
    unsafe {
        cmd.pre_exec(move || {
            for (namespace, kind) in &joined {
                move_into_link_name_space(namespace.as_fd(), Some(*kind))?;
            }
            if v.enclosed {
                rustix::process::chdir(c"/")?;
            }
            become_declared(&v, &filter)
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

    #[cfg(target_os = "linux")]
    async fn enclosed_verdict() -> Verdict {
        kakoi()
            .enclose(
                &Confinement::default(),
                "",
                &[],
                NativeSecurity::Enforce,
                true,
            )
            .await
            .unwrap()
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
            async move {
                kakoi()
                    .enclose(&c, "", &volumes, NativeSecurity::Enforce, true)
                    .await
                    .unwrap()
            }
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
        let v = kakoi()
            .enclose(
                &Confinement::default(),
                "",
                &[],
                NativeSecurity::Enforce,
                true,
            )
            .await
            .unwrap();
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
        let refused = kakoi()
            .enclose(&c, "", &[], NativeSecurity::Enforce, true)
            .await
            .unwrap();
        assert!(refused.refused.unwrap().contains("kakoi.pleme.io/root"));
        c.root_grant = Some("drops to the unit's account".to_string());
        let granted = kakoi()
            .enclose(&c, "", &[], NativeSecurity::Enforce, true)
            .await
            .unwrap();
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
        let v = kakoi()
            .enclose(
                &Confinement::default(),
                "",
                &[],
                NativeSecurity::Enforce,
                false,
            )
            .await
            .unwrap();
        assert_eq!(v.uid, None);
        assert!(!v.no_new_privs);
        assert_eq!(v.warnings.len(), 1);
    }
}
