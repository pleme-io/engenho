//! The privilege drop: `setgroups`, `setresgid`, `setresuid` and the ambient
//! capability dance — through rustix, with no `unsafe` in this crate.
//!
//! ## Why the thread-scoped calls are the right ones
//!
//! On Linux credentials are a property of a TASK, not of a process: glibc's
//! `setuid()` is a library that broadcasts the change to every thread. rustix
//! exposes the raw syscalls (`set_thread_res_uid` and friends), which change
//! the CALLING thread — and that is exactly what this runner wants:
//!
//! * before `exec`, the process is single-threaded and the calling thread is
//!   the process; after `execve` the new image carries the calling thread's
//!   credentials in any case;
//! * for an `ExecStartPre=` that must run as the service user, the drop
//!   happens on a dedicated thread which then spawns the child, and `fork`
//!   copies the CALLING thread's credentials — so the child is the service
//!   user while the runner's own thread stays root and can still install the
//!   next directory. No `unsafe` `pre_exec` closure, no second process.
//!
//! ## Ambient capabilities
//!
//! `AmbientCapabilities=CAP_NET_RAW` means "the unprivileged process keeps
//! this capability across `exec`". The kernel's recipe — which is what
//! systemd does too — is: `PR_SET_KEEPCAPS` so `setresuid` does not clear the
//! permitted set; drop to the user; `capset` the permitted, effective and
//! inheritable sets to exactly the wanted capabilities; raise each one in the
//! ambient set with `PR_CAP_AMBIENT_RAISE`.
//!
//! ## Off Linux
//!
//! macOS has no `setresuid`, no capabilities and no thread-scoped
//! credentials. A unit that needs a drop is REFUSED there rather than run as
//! the invoking user, because running a service as root when its unit says
//! `User=hass` is the kind of silent difference this crate exists to prevent.

use std::fmt;

use crate::capability::CapabilitySet;
use crate::identity::Account;

/// Which step of the drop failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// `prctl(PR_SET_KEEPCAPS)`.
    KeepCapabilities,
    /// `setgroups`.
    Groups,
    /// `setresgid`.
    Gid,
    /// `setresuid`.
    Uid,
    /// `capset`.
    Capabilities,
    /// `prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE)`.
    Ambient,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::KeepCapabilities => "prctl(PR_SET_KEEPCAPS)",
            Self::Groups => "setgroups",
            Self::Gid => "setresgid",
            Self::Uid => "setresuid",
            Self::Capabilities => "capset",
            Self::Ambient => "prctl(PR_CAP_AMBIENT_RAISE)",
        })
    }
}

/// Why privileges could not be dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeError {
    /// A syscall refused.
    Syscall {
        /// The step.
        step: Step,
        /// The account it was dropping to.
        user: String,
        /// What the kernel said.
        detail: String,
    },
    /// This platform has no thread-scoped credentials.
    Unsupported {
        /// The platform.
        platform: &'static str,
        /// The account the unit asked for.
        user: String,
    },
}

impl fmt::Display for PrivilegeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syscall { step, user, detail } => write!(
                f,
                "{step} while dropping to {user} failed: {detail} (unit-run must run as root to \
                 change a process's user)"
            ),
            Self::Unsupported { platform, user } => write!(
                f,
                "the unit runs as {user}, and {platform} has no thread-scoped credential \
                 syscalls: this runner refuses rather than run the service as the invoking user"
            ),
        }
    }
}

impl std::error::Error for PrivilegeError {}

/// Whether the caller is root, i.e. whether a drop can succeed at all.
#[must_use]
pub fn is_root() -> bool {
    rustix::process::geteuid().is_root()
}

/// Whether becoming `account` is a change at all.
///
/// A drop is skipped when the caller already IS that uid and gid — the case
/// a test run hits (it cannot become another user, and does not need to) and
/// the case an operator running `unit-run` as the service's own user hits.
/// It is never skipped when the ids differ, so a unit that says `User=hass`
/// either runs as hass or fails loudly.
#[must_use]
pub fn needs_drop(account: &Account) -> bool {
    let uid = rustix::process::geteuid().as_raw();
    let gid = rustix::process::getegid().as_raw();
    uid != account.uid || gid != account.gid
}

/// Set the process umask, returning the previous one.
#[must_use]
pub fn set_umask(mask: u32) -> u32 {
    let bits: rustix::fs::RawMode = mask.try_into().unwrap_or(0o022);
    let previous = rustix::process::umask(rustix::fs::Mode::from_bits_truncate(bits));
    u32::from(previous.bits())
}

/// Become `account`, keeping `ambient` across the following `exec`.
///
/// Affects the CALLING THREAD only — see the module header for why that is
/// the point rather than a limitation.
///
/// # Errors
///
/// A [`PrivilegeError`] naming the step that failed, or
/// [`PrivilegeError::Unsupported`] off Linux.
#[cfg(target_os = "linux")]
pub fn drop_to(account: &Account, ambient: &CapabilitySet) -> Result<(), PrivilegeError> {
    use rustix::process::{Gid, Uid};
    use rustix::thread::{
        CapabilitySet as RustixCaps, CapabilitySets, configure_capability_in_ambient_set,
        set_capabilities, set_keep_capabilities, set_thread_groups, set_thread_res_gid,
        set_thread_res_uid,
    };

    let fail = |step: Step| {
        move |e: rustix::io::Errno| PrivilegeError::Syscall {
            step,
            user: account.name.clone(),
            detail: e.to_string(),
        }
    };
    let keep = !ambient.is_empty();
    if keep {
        set_keep_capabilities(true).map_err(fail(Step::KeepCapabilities))?;
    }

    let groups: Vec<Gid> = account
        .groups
        .iter()
        .map(|gid| Gid::from_raw(*gid))
        .collect();
    set_thread_groups(&groups).map_err(fail(Step::Groups))?;
    let gid = Gid::from_raw(account.gid);
    set_thread_res_gid(gid, gid, gid).map_err(fail(Step::Gid))?;
    let uid = Uid::from_raw(account.uid);
    set_thread_res_uid(uid, uid, uid).map_err(fail(Step::Uid))?;

    if keep {
        // The permitted set survived the setresuid thanks to KEEPCAPS; reduce
        // it to what the unit asked for, and make those inheritable so they
        // can be raised into the ambient set.
        let wanted = RustixCaps::from_bits_retain(ambient.mask());
        set_capabilities(
            None,
            CapabilitySets {
                effective: wanted,
                permitted: wanted,
                inheritable: wanted,
            },
        )
        .map_err(fail(Step::Capabilities))?;
        for capability in ambient.members() {
            configure_capability_in_ambient_set(
                RustixCaps::from_bits_retain(capability.bit()),
                true,
            )
            .map_err(fail(Step::Ambient))?;
        }
        set_keep_capabilities(false).map_err(fail(Step::KeepCapabilities))?;
    }
    Ok(())
}

/// Become `account` — refused off Linux.
///
/// # Errors
///
/// Always [`PrivilegeError::Unsupported`].
#[cfg(not(target_os = "linux"))]
pub fn drop_to(account: &Account, _ambient: &CapabilitySet) -> Result<(), PrivilegeError> {
    Err(PrivilegeError::Unsupported {
        platform: std::env::consts::OS,
        user: account.name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> Account {
        Account {
            name: "hass".into(),
            uid: 286,
            gid: 286,
            group: "hass".into(),
            groups: vec![286],
            home: "/var/lib/hass".into(),
            shell: "/bin/nologin".into(),
        }
    }

    #[test]
    fn the_umask_round_trips() {
        let previous = set_umask(0o077);
        let ours = set_umask(previous);
        assert_eq!(ours, 0o077);
    }

    /// Off Linux the refusal is typed and names the platform — the service is
    /// never quietly run as the invoking user.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn a_drop_is_refused_where_it_cannot_be_done() {
        let err = drop_to(&account(), &CapabilitySet::default()).unwrap_err();
        assert!(matches!(err, PrivilegeError::Unsupported { .. }));
        assert!(err.to_string().contains("hass"));
    }

    /// As a non-root user the syscall refuses, and the error names the step —
    /// the positive path needs root and lives in `tests/privilege_drop.rs`.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_drop_without_root_fails_at_a_named_step() {
        if is_root() {
            return;
        }
        let err = drop_to(&account(), &CapabilitySet::default()).unwrap_err();
        assert!(
            matches!(
                err,
                PrivilegeError::Syscall {
                    step: Step::Groups | Step::Gid | Step::Uid,
                    ..
                }
            ),
            "{err}"
        );
    }
}
