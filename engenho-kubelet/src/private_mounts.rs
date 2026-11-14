//! A native pod's own mount namespace, where each of its volumes is
//! bind-mounted at its `mountPath`.
//!
//! A native container is a host process, so by default it sees the host's
//! filesystem and a volume is honoured only when its host path IS its
//! `mountPath` ([`crate::native_backend`]). A workload that reads a fixed
//! absolute path, such as `/etc/app/conf/app.conf`, then cannot run twice on
//! one node with two configurations.
//!
//! With `runtime.native_mounts` set to `per_pod` (for a pod annotated
//! [`MOUNTS_ANNOTATION`]`: private`) or `every_pod`, the container's process
//! enters a mount namespace of its own just before `exec`:
//!
//! 1. `unshare(CLONE_NEWNS)`, then `/` is made `MS_REC | MS_PRIVATE`, so
//!    nothing mounted below reaches the host or another pod;
//! 2. each volume, shallowest `mountPath` first, has its target created when
//!    missing and its source bind-mounted onto it;
//! 3. each `readOnly` volume is remounted read-only, keeping the flags its
//!    source carries (`nosuid`, `nodev`, `noexec`, the atime flags).
//!
//! The step runs first in the child's one pre-`exec` hook
//! ([`crate::kakoi::enter_on_exec`]), before the enclosure and before the
//! account changes, because mounting needs `CAP_SYS_ADMIN`. The namespace
//! belongs to the process: it ends when the process is reaped, so supervision,
//! stop and a daemon restart are what they were. A command run in the
//! container joins it ([`namespace_of`]).
//!
//! A target that does not exist is created in the host's filesystem (or in an
//! outer volume, for a nested one) as an empty directory or file, the same
//! mount point a container runtime creates in its root filesystem. Nothing is
//! mounted on the host.
//!
//! Every refusal names one pod: a node that cannot mount refuses the pods that
//! ask, and the pods that do not ask run as before.

use crate::pod_volume::{BindSource, ResolvedMount};
use std::ffi::CString;
use std::path::{Component, Path, PathBuf};

pub use engenho_config::NativeMounts;

/// The pod annotation that asks for a private mount namespace.
pub const MOUNTS_ANNOTATION: &str = "engenho.pleme.io/mounts";

/// The one value [`MOUNTS_ANNOTATION`] takes.
pub const PRIVATE: &str = "private";

const STORE: &str = "/nix/store";

/// Why a `mountPath` cannot be a bind target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountPathFault {
    /// Not absolute.
    Relative,
    /// Holds a `..` component.
    ParentComponent,
    /// It is `/`.
    Root,
    /// It is in the Nix store, lexically or through a symlink.
    InStore,
    /// Another of the container's volumes mounts at the same path.
    Collision,
}

impl std::fmt::Display for MountPathFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Relative => "it is not an absolute path",
            Self::ParentComponent => "it holds a `..` component",
            Self::Root => "it is `/`, and a volume cannot replace the root",
            Self::InStore => {
                "it is in the Nix store, and the closure is what the pod is \
                 promised to run"
            }
            Self::Collision => "another volume of this container mounts at the same path",
        })
    }
}

/// What a private mount namespace refused, for one pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The annotation names something other than [`PRIVATE`].
    UnknownAsk {
        /// What it named.
        value: String,
    },
    /// The pod asks and the node has `runtime.native_mounts: off`.
    NodeOff,
    /// A `mountPath` that cannot be a bind target.
    MountPath {
        /// The path as declared.
        mount_path: String,
        /// Why.
        fault: MountPathFault,
    },
    /// A runtime-managed named volume, which has no host path to bind.
    NamedVolume {
        /// The volume.
        volume: String,
    },
    /// A volume's source could not be created or read.
    Source {
        /// The host path.
        path: PathBuf,
        /// What the kernel said.
        detail: String,
    },
    /// This node cannot make a mount namespace.
    Unavailable {
        /// Why.
        reason: String,
    },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownAsk { value } => write!(
                f,
                "the annotation {MOUNTS_ANNOTATION} is {value:?}; the one value \
                 it takes is {PRIVATE:?}"
            ),
            Self::NodeOff => write!(
                f,
                "the pod asks for a private mount namespace ({MOUNTS_ANNOTATION}: \
                 {PRIVATE}) and this node has runtime.native_mounts: off. Set it \
                 to per_pod or every_pod, or remove the annotation."
            ),
            Self::MountPath { mount_path, fault } => {
                write!(f, "cannot mount a volume at {mount_path:?}: {fault}")
            }
            Self::NamedVolume { volume } => write!(
                f,
                "volume {volume:?} is a runtime-managed named volume, which has \
                 no host path to bind"
            ),
            Self::Source { path, detail } => {
                write!(f, "volume source {}: {detail}", path.display())
            }
            Self::Unavailable { reason } => {
                write!(
                    f,
                    "this node cannot give a pod its own mount namespace: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for Refusal {}

/// Whether a container gets a namespace of its own, from the node's setting
/// and what its pod asks.
///
/// # Errors
/// [`Refusal::UnknownAsk`] for an annotation that is not [`PRIVATE`], and
/// [`Refusal::NodeOff`] for a pod that asks on a node with it off.
pub fn wanted(node: NativeMounts, asked: Option<&str>) -> Result<bool, Refusal> {
    let asks = match asked {
        None => false,
        Some(PRIVATE) => true,
        Some(other) => {
            return Err(Refusal::UnknownAsk {
                value: other.to_string(),
            });
        }
    };
    match node {
        NativeMounts::Off if asks => Err(Refusal::NodeOff),
        NativeMounts::Off => Ok(false),
        NativeMounts::PerPod => Ok(asks),
        NativeMounts::EveryPod => Ok(true),
    }
}

/// One bind in a container's namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bind {
    /// The host path bound, the volume's subPath included.
    pub source: PathBuf,
    /// The `mountPath`, normalized.
    pub target: PathBuf,
    /// Remount it read-only.
    pub read_only: bool,
}

/// The `mountPath` as a bind target: absolute, no `..`, not `/`, not in the
/// store, trailing and doubled separators dropped.
fn target_of(mount_path: &str) -> Result<PathBuf, Refusal> {
    let refuse = |fault| Refusal::MountPath {
        mount_path: mount_path.to_string(),
        fault,
    };
    let path = Path::new(mount_path);
    if !path.is_absolute() {
        return Err(refuse(MountPathFault::Relative));
    }
    if path.components().any(|c| c == Component::ParentDir) {
        return Err(refuse(MountPathFault::ParentComponent));
    }
    let target: PathBuf = path.components().collect();
    if target == Path::new("/") {
        return Err(refuse(MountPathFault::Root));
    }
    if target.starts_with(STORE) {
        return Err(refuse(MountPathFault::InStore));
    }
    Ok(target)
}

/// The binds a container's mounts make, shallowest target first, so an outer
/// volume is in place before one mounted inside it. Pure: it reads no file.
///
/// # Errors
/// The first mount that cannot be bound, as a [`Refusal`].
pub fn plan(mounts: &[ResolvedMount]) -> Result<Vec<Bind>, Refusal> {
    let mut binds: Vec<Bind> = Vec::with_capacity(mounts.len());
    for m in mounts {
        let target = target_of(&m.mount_path)?;
        if binds.iter().any(|b| b.target == target) {
            return Err(Refusal::MountPath {
                mount_path: m.mount_path.clone(),
                fault: MountPathFault::Collision,
            });
        }
        let root = match m.source.bind_source() {
            BindSource::Path(p) => p,
            BindSource::Volume(name) => {
                return Err(Refusal::NamedVolume {
                    volume: name.to_string(),
                });
            }
        };
        let source = match m.sub_path.as_deref().map(|s| s.trim_start_matches('/')) {
            Some(sub) if !sub.is_empty() => root.join(sub),
            _ => root.to_path_buf(),
        };
        binds.push(Bind {
            source,
            target,
            read_only: m.read_only,
        });
    }
    binds.sort_by_key(|b| b.target.components().count());
    Ok(binds)
}

/// Whether `target`, as the host resolves the part of it that exists, lands in
/// the store: `/etc/x` is a store path when `/etc/x` is a symlink into it.
fn resolves_into_store(target: &Path) -> bool {
    target
        .ancestors()
        .find(|a| a.exists())
        .and_then(|a| std::fs::canonicalize(a).ok())
        .is_some_and(|real| real.starts_with(STORE))
}

/// Whether this process can make a mount namespace: Linux, holding
/// `CAP_SYS_ADMIN`.
///
/// # Errors
/// [`Refusal::Unavailable`], naming what is missing.
pub fn can_mount() -> Result<(), Refusal> {
    #[cfg(target_os = "linux")]
    {
        const CAP_SYS_ADMIN: u32 = 21;
        let status =
            std::fs::read_to_string("/proc/self/status").map_err(|e| Refusal::Unavailable {
                reason: format!("/proc/self/status: {e}"),
            })?;
        let effective = status
            .lines()
            .find_map(|l| l.strip_prefix("CapEff:"))
            .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
            .ok_or_else(|| Refusal::Unavailable {
                reason: "/proc/self/status names no CapEff".to_string(),
            })?;
        if effective & (1 << CAP_SYS_ADMIN) == 0 {
            return Err(Refusal::Unavailable {
                reason: format!(
                    "the daemon does not hold CAP_SYS_ADMIN (CapEff {effective:016x}); \
                     run it as root"
                ),
            });
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Refusal::Unavailable {
            reason: format!(
                "a mount namespace is a Linux facility and this node is {}",
                std::env::consts::OS
            ),
        })
    }
}

/// One bind, ready for a child that may not allocate.
#[derive(Debug, Clone)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct Ready {
    source: CString,
    target: CString,
    /// Every directory from the top down to the target (to its parent, for a
    /// file), created when missing.
    dirs: Vec<CString>,
    file: bool,
    read_only: bool,
}

/// A container's binds with every path the child needs already built.
#[derive(Debug, Clone)]
pub struct Prepared {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    binds: Vec<Ready>,
}

fn c_path(path: &Path) -> Result<CString, Refusal> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| Refusal::Source {
        path: path.to_path_buf(),
        detail: "the path holds a NUL byte".to_string(),
    })
}

/// Read the host for what [`plan`] cannot know: create a missing source
/// directory, as the shared-path mode does; tell a file from a directory;
/// refuse a target a symlink carries into the store.
///
/// # Errors
/// The first bind that cannot be prepared, as a [`Refusal`].
pub fn prepare(binds: &[Bind]) -> Result<Prepared, Refusal> {
    let mut ready = Vec::with_capacity(binds.len());
    for b in binds {
        if resolves_into_store(&b.target) {
            return Err(Refusal::MountPath {
                mount_path: b.target.display().to_string(),
                fault: MountPathFault::InStore,
            });
        }
        if !b.source.exists() {
            std::fs::create_dir_all(&b.source).map_err(|e| Refusal::Source {
                path: b.source.clone(),
                detail: e.to_string(),
            })?;
        }
        let file = std::fs::metadata(&b.source)
            .map_err(|e| Refusal::Source {
                path: b.source.clone(),
                detail: e.to_string(),
            })?
            .is_file();
        let deepest = if file {
            b.target.parent().unwrap_or(&b.target)
        } else {
            &b.target
        };
        let mut dirs: Vec<CString> = deepest
            .ancestors()
            .filter(|a| *a != Path::new("/"))
            .map(c_path)
            .collect::<Result<_, _>>()?;
        dirs.reverse();
        ready.push(Ready {
            source: c_path(&b.source)?,
            target: c_path(&b.target)?,
            dirs,
            file,
            read_only: b.read_only,
        });
    }
    Ok(Prepared { binds: ready })
}

#[cfg(target_os = "linux")]
impl Prepared {
    /// Enter a mount namespace of its own and bind every volume. Runs in the
    /// forked child before `exec`: it allocates nothing and makes only
    /// syscalls.
    ///
    /// # Errors
    /// The first syscall that failed.
    pub fn enter(&self) -> std::io::Result<()> {
        use rustix::fs::{Mode, OFlags, StatVfsMountFlags};
        use rustix::io::Errno;
        use rustix::mount::{MountFlags, MountPropagationFlags};

        #[allow(unsafe_code)]
        unsafe {
            rustix::thread::unshare_unsafe(rustix::thread::UnshareFlags::NEWNS)?;
        }
        rustix::mount::mount_change(
            c"/",
            MountPropagationFlags::PRIVATE | MountPropagationFlags::REC,
        )?;
        for b in &self.binds {
            for dir in &b.dirs {
                match rustix::fs::mkdir(dir.as_c_str(), Mode::from_raw_mode(0o755)) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if b.file {
                drop(rustix::fs::open(
                    b.target.as_c_str(),
                    OFlags::CREATE | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOCTTY,
                    Mode::from_raw_mode(0o644),
                )?);
            }
            rustix::mount::mount_bind_recursive(b.source.as_c_str(), b.target.as_c_str())?;
        }
        for b in self.binds.iter().filter(|b| b.read_only) {
            let has = rustix::fs::statvfs(b.target.as_c_str())?.f_flag;
            let keep = [
                (StatVfsMountFlags::NOSUID, MountFlags::NOSUID),
                (StatVfsMountFlags::NODEV, MountFlags::NODEV),
                (StatVfsMountFlags::NOEXEC, MountFlags::NOEXEC),
                (StatVfsMountFlags::NOATIME, MountFlags::NOATIME),
                (StatVfsMountFlags::NODIRATIME, MountFlags::NODIRATIME),
                (StatVfsMountFlags::RELATIME, MountFlags::RELATIME),
            ]
            .into_iter()
            .filter(|(seen, _)| has.contains(*seen))
            .fold(MountFlags::empty(), |all, (_, flag)| all | flag);
            rustix::mount::mount_remount(
                b.target.as_c_str(),
                MountFlags::BIND | MountFlags::RDONLY | keep,
                c"",
            )?;
        }
        Ok(())
    }
}

/// The mount namespace of a live container's process, for a command run in
/// it to join.
///
/// # Errors
/// The namespace file that could not be opened.
pub fn namespace_of(pid: u32) -> std::io::Result<std::fs::File> {
    std::fs::File::open(PathBuf::from("/proc").join(pid.to_string()).join("ns/mnt"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod_volume::MountSource;

    fn mount(source: &str, at: &str) -> ResolvedMount {
        ResolvedMount {
            source: MountSource::UserHostPath(PathBuf::from(source)),
            mount_path: at.to_string(),
            read_only: false,
            sub_path: None,
        }
    }

    fn fault_of(mounts: &[ResolvedMount]) -> MountPathFault {
        match plan(mounts) {
            Err(Refusal::MountPath { fault, .. }) => fault,
            other => panic!("expected a mountPath refusal, got {other:?}"),
        }
    }

    #[test]
    fn off_is_the_default_and_keeps_every_pod_on_the_host_filesystem() {
        assert_eq!(NativeMounts::default(), NativeMounts::Off);
        assert_eq!(wanted(NativeMounts::Off, None), Ok(false));
    }

    #[test]
    fn a_pod_asks_and_the_node_decides() {
        assert_eq!(wanted(NativeMounts::PerPod, None), Ok(false));
        assert_eq!(wanted(NativeMounts::PerPod, Some(PRIVATE)), Ok(true));
        assert_eq!(wanted(NativeMounts::EveryPod, None), Ok(true));
        assert_eq!(wanted(NativeMounts::EveryPod, Some(PRIVATE)), Ok(true));
    }

    #[test]
    fn an_ask_the_node_cannot_honour_refuses_that_pod_by_name() {
        assert_eq!(
            wanted(NativeMounts::Off, Some(PRIVATE)),
            Err(Refusal::NodeOff)
        );
        let shown = Refusal::NodeOff.to_string();
        assert!(shown.contains("runtime.native_mounts"), "{shown}");
        assert!(shown.contains(MOUNTS_ANNOTATION), "{shown}");
        for node in [
            NativeMounts::Off,
            NativeMounts::PerPod,
            NativeMounts::EveryPod,
        ] {
            assert_eq!(
                wanted(node, Some("yes")),
                Err(Refusal::UnknownAsk {
                    value: "yes".to_string()
                })
            );
        }
    }

    #[test]
    fn each_mount_binds_its_source_at_its_mount_path() {
        let binds = plan(&[
            mount("/var/lib/engenho/volumes/a", "/etc/app/conf"),
            mount("/srv/data", "/srv/data"),
        ])
        .unwrap();
        assert_eq!(
            binds,
            vec![
                Bind {
                    source: PathBuf::from("/srv/data"),
                    target: PathBuf::from("/srv/data"),
                    read_only: false,
                },
                Bind {
                    source: PathBuf::from("/var/lib/engenho/volumes/a"),
                    target: PathBuf::from("/etc/app/conf"),
                    read_only: false,
                },
            ]
        );
    }

    #[test]
    fn an_outer_volume_is_bound_before_one_inside_it_whatever_the_declared_order() {
        let binds = plan(&[
            mount("/v/inner", "/etc/app/conf"),
            mount("/v/outer", "/etc/app"),
            mount("/v/deeper", "/etc/app/conf/extra"),
        ])
        .unwrap();
        let order: Vec<_> = binds.iter().map(|b| b.target.clone()).collect();
        assert_eq!(
            order,
            ["/etc/app", "/etc/app/conf", "/etc/app/conf/extra"].map(PathBuf::from)
        );
    }

    #[test]
    fn binds_at_one_depth_keep_their_declared_order() {
        let binds = plan(&[mount("/v/b", "/b/x"), mount("/v/a", "/a/x")]).unwrap();
        assert_eq!(binds[0].target, PathBuf::from("/b/x"));
        assert_eq!(binds[1].target, PathBuf::from("/a/x"));
    }

    #[test]
    fn read_only_and_sub_path_are_carried() {
        let mut m = mount("/v/cm", "/etc/app/app.conf");
        m.read_only = true;
        m.sub_path = Some("/app.conf".to_string());
        let binds = plan(&[m]).unwrap();
        assert_eq!(binds[0].source, PathBuf::from("/v/cm/app.conf"));
        assert!(binds[0].read_only);
        let mut empty = mount("/v/cm", "/etc/app/conf");
        empty.sub_path = Some(String::new());
        assert_eq!(plan(&[empty]).unwrap()[0].source, PathBuf::from("/v/cm"));
    }

    #[test]
    fn a_mount_path_that_cannot_be_a_target_is_refused_by_name() {
        assert_eq!(
            fault_of(&[mount("/v", "etc/app")]),
            MountPathFault::Relative
        );
        assert_eq!(
            fault_of(&[mount("/v", "/etc/../root")]),
            MountPathFault::ParentComponent
        );
        assert_eq!(fault_of(&[mount("/v", "/")]), MountPathFault::Root);
        assert_eq!(fault_of(&[mount("/v", "//")]), MountPathFault::Root);
        assert_eq!(
            fault_of(&[mount("/v", "/nix/store")]),
            MountPathFault::InStore
        );
        assert_eq!(
            fault_of(&[mount("/v", "/nix/store/abc-x/etc")]),
            MountPathFault::InStore
        );
        let shown = plan(&[mount("/v", "etc/app")]).unwrap_err().to_string();
        assert!(shown.contains("\"etc/app\""), "{shown}");
        assert!(shown.contains("not an absolute path"), "{shown}");
    }

    #[test]
    fn a_path_beside_the_store_is_not_in_it() {
        assert!(plan(&[mount("/v", "/nix/store-like")]).is_ok());
        assert!(plan(&[mount("/v", "/nix/var/x")]).is_ok());
    }

    #[test]
    fn two_volumes_at_one_path_collide_however_the_path_is_spelled() {
        assert_eq!(
            fault_of(&[mount("/a", "/etc/app"), mount("/b", "/etc/app")]),
            MountPathFault::Collision
        );
        assert_eq!(
            fault_of(&[mount("/a", "/etc/app"), mount("/b", "/etc//app/")]),
            MountPathFault::Collision
        );
        assert!(plan(&[mount("/a", "/etc/app"), mount("/b", "/etc/app2")]).is_ok());
    }

    #[test]
    fn a_named_volume_has_nothing_to_bind() {
        let m = ResolvedMount {
            source: MountSource::NamedVolume("scratch".to_string()),
            mount_path: "/scratch".to_string(),
            read_only: false,
            sub_path: None,
        };
        assert_eq!(
            plan(&[m]),
            Err(Refusal::NamedVolume {
                volume: "scratch".to_string()
            })
        );
    }

    #[test]
    fn preparing_creates_a_missing_source_and_tells_a_file_from_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("made/here");
        let file = dir.path().join("app.conf");
        std::fs::write(&file, "x").unwrap();
        let prepared = prepare(&[
            Bind {
                source: missing.clone(),
                target: PathBuf::from("/etc/app/conf"),
                read_only: false,
            },
            Bind {
                source: file,
                target: PathBuf::from("/etc/app/app.conf"),
                read_only: true,
            },
        ])
        .unwrap();
        assert!(missing.is_dir());
        assert!(!prepared.binds[0].file);
        assert_eq!(
            prepared.binds[0].dirs,
            [c"/etc", c"/etc/app", c"/etc/app/conf"].map(CString::from)
        );
        assert!(prepared.binds[1].file);
        assert!(prepared.binds[1].read_only);
        assert_eq!(
            prepared.binds[1].dirs,
            [c"/etc", c"/etc/app"].map(CString::from)
        );
    }

    #[test]
    fn a_target_a_symlink_carries_into_the_store_is_refused() {
        if !Path::new(STORE).is_dir() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("etc-static");
        std::os::unix::fs::symlink(STORE, &link).unwrap();
        let refused = prepare(&[Bind {
            source: dir.path().to_path_buf(),
            target: link.join("x/conf"),
            read_only: false,
        }])
        .unwrap_err();
        assert!(
            matches!(
                refused,
                Refusal::MountPath {
                    fault: MountPathFault::InStore,
                    ..
                }
            ),
            "{refused:?}"
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn off_linux_no_node_can_mount() {
        let shown = can_mount().unwrap_err().to_string();
        assert!(shown.contains("Linux facility"), "{shown}");
    }
}
