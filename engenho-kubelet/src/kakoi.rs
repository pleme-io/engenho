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
        mode: NativeSecurity,
        can_enforce: bool,
    ) -> Result<Verdict, String> {
        let facts = Facts {
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

/// Arrange for the child to enter its enclosure just before `exec`.
///
/// `std` has already changed uid and gid when this runs, so a non-root child
/// lost its capabilities then; this clears what a granted root child was told
/// to give up and stops `execve` gaining privileges back.
#[cfg(target_os = "linux")]
pub fn enter_on_exec(cmd: &mut tokio::process::Command, verdict: &Verdict) {
    let no_new_privs = verdict.no_new_privs;
    let clear_caps = verdict.clear_caps;
    if !no_new_privs && !clear_caps {
        return;
    }
    #[allow(unsafe_code)]
    unsafe {
        cmd.pre_exec(move || {
            if clear_caps {
                let empty = rustix::thread::CapabilitySet::empty();
                rustix::thread::set_capabilities(
                    None,
                    rustix::thread::CapabilitySets {
                        effective: empty,
                        permitted: empty,
                        inheritable: empty,
                    },
                )?;
            }
            if no_new_privs {
                rustix::thread::set_no_new_privs(true)?;
            }
            Ok(())
        });
    }
}

/// No enclosure can be entered off Linux; [`can_enforce`] said so first.
#[cfg(not(target_os = "linux"))]
pub fn enter_on_exec(_cmd: &mut tokio::process::Command, _verdict: &Verdict) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn kakoi() -> Kakoi {
        Kakoi::start().expect("kakoi loads")
    }

    #[tokio::test]
    async fn a_pod_that_says_nothing_is_enclosed_by_default() {
        let v = kakoi()
            .enclose(&Confinement::default(), NativeSecurity::Enforce, true)
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
            .enclose(&c, NativeSecurity::Enforce, true)
            .await
            .unwrap();
        assert!(refused.refused.unwrap().contains("kakoi.pleme.io/root"));
        c.root_grant = Some("drops to the unit's account".to_string());
        let granted = kakoi()
            .enclose(&c, NativeSecurity::Enforce, true)
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
            .enclose(&Confinement::default(), NativeSecurity::Enforce, false)
            .await
            .unwrap();
        assert_eq!(v.uid, None);
        assert!(!v.no_new_privs);
        assert_eq!(v.warnings.len(), 1);
    }
}
