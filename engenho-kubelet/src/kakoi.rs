//! The enclosure of a native container, decided by the embedded `kakoi` blue
//! program (`blue/kakoi/kakoi.b`).
//!
//! Rust is the seam: it states the facts, reads the verdict, and applies it to
//! the child in [`enter_on_exec`].

use serde::{Deserialize, Serialize};

use crate::backend::Confinement;
use crate::blue_worker::BlueWorker;

/// How a node treats a native container's security.
pub use engenho_config::NativeSecurity;

const FILES: &[(&str, &str)] = &[("kakoi/kakoi.b", include_str!("../blue/kakoi/kakoi.b"))];
const ENTRY: &str = "use(\"kakoi\")\n";

/// The pod annotation that grants root; its value is the reason.
pub const GRANT_ANNOTATION: &str = "kakoi.pleme.io/root";

#[derive(Serialize)]
struct Declared<'a> {
    run_as_user: Option<i64>,
    run_as_group: Option<i64>,
    run_as_non_root: Option<bool>,
    privileged: bool,
    allow_escalation: Option<bool>,
    cap_add: &'a [String],
}

#[derive(Serialize)]
struct Facts<'a> {
    namespace: &'a str,
    volumes: &'a [String],
    mode: NativeSecurity,
    can_enforce: bool,
    grant: Option<&'a str>,
    declared: Declared<'a>,
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
        let facts = Facts {
            namespace,
            volumes,
            mode,
            can_enforce,
            grant: confinement.root_grant.as_deref(),
            declared: Declared {
                run_as_user: confinement.run_as_user,
                run_as_group: confinement.run_as_group,
                run_as_non_root: confinement.run_as_non_root,
                privileged: confinement.privileged,
                allow_escalation: confinement.allow_escalation,
                cap_add: &confinement.cap_add,
            },
        };
        let facts = serde_json::to_string(&facts).map_err(|e| e.to_string())?;
        self.0.ask("kakoi::enclose", facts).await
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
            if kernel || target.contains("\\") {
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
                target: std::ffi::CString::new(target).ok()?,
                keep,
            })
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn cstrings(paths: impl IntoIterator<Item = String>) -> Vec<std::ffi::CString> {
    paths
        .into_iter()
        .filter_map(|p| std::ffi::CString::new(p).ok())
        .collect()
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
    use rustix::thread::{CapabilitySet, CapabilitySets, UnshareFlags};

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
    let writable = cstrings(
        volumes
            .iter()
            .filter(|m| !m.read_only)
            .map(|m| m.path.clone()),
    );
    let readable = cstrings(
        volumes
            .iter()
            .filter(|m| m.read_only)
            .map(|m| m.path.clone()),
    );
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
                rustix::thread::unshare_unsafe(UnshareFlags::NEWNS)?;
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
            Ok(())
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn kakoi() -> Kakoi {
        Kakoi::start().expect("kakoi loads")
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
