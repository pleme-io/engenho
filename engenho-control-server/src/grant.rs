//! Who a local caller is, and what they may do.
//!
//! The kernel says who is on the other end of the socket (`getpeereid` on
//! macOS, `SO_PEERCRED` on Linux, through tokio's `peer_cred`). That — never
//! anything in the request — is what the grant is computed from:
//!
//! | Peer | Granted |
//! |---|---|
//! | the daemon's own uid, or root | destructive |
//! | anyone else, on a [`SocketAccess::Group`] socket | the configured [`GroupTier`] |
//! | anyone else, on an owner-only socket | nothing |
//!
//! A request may lower its own authority with `Engenho-Ceiling` (an agent
//! told to observe only), never raise it. `Engenho-Actor` says what kind of
//! caller it claims to be; it is recorded, and never authority.

use engenho_config::{GroupTier, SocketAccess};
use engenho_control_types::types::{AttestedView, DeclaredView, GrantView};
use engenho_control_types::{AuthorityTier, Principal};

/// The header a caller declares itself with.
pub const ACTOR_HEADER: &str = "engenho-actor";
/// The header a caller lowers its authority with.
pub const CEILING_HEADER: &str = "engenho-ceiling";

/// What the kernel attested about a local peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCred {
    /// Its effective uid.
    pub uid: u32,
    /// Its effective gid.
    pub gid: u32,
    /// Its pid, where the platform reports one.
    pub pid: Option<i32>,
}

/// The local grant policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrantPolicy {
    daemon_euid: u32,
    access: SocketAccess,
    group_tier: GroupTier,
}

impl GrantPolicy {
    /// The policy for a daemon running as `daemon_euid`.
    #[must_use]
    pub const fn new(daemon_euid: u32, access: SocketAccess, group_tier: GroupTier) -> Self {
        Self {
            daemon_euid,
            access,
            group_tier,
        }
    }

    /// The tier `peer` is entitled to, or `None`.
    #[must_use]
    pub const fn grant(&self, peer: &PeerCred) -> Option<AuthorityTier> {
        if peer.uid == self.daemon_euid || peer.uid == 0 {
            return Some(AuthorityTier::Destructive);
        }
        match (self.access, self.group_tier) {
            (SocketAccess::Owner, _) => None,
            (SocketAccess::Group, GroupTier::Observe) => Some(AuthorityTier::Observe),
            (SocketAccess::Group, GroupTier::Mutate) => Some(AuthorityTier::Mutate),
        }
    }

    /// The access the socket was bound with.
    #[must_use]
    pub const fn access(&self) -> SocketAccess {
        self.access
    }

    /// The group members' tier.
    #[must_use]
    pub const fn group_tier(&self) -> GroupTier {
        self.group_tier
    }
}

/// What a caller declared about itself: `human`, `agent:<label>`,
/// `automation:<label>`; anything else is `unknown`.
#[must_use]
pub fn declared(header: Option<&str>) -> DeclaredView {
    match header.map(str::trim) {
        Some("human") => DeclaredView::Human,
        Some(v) => match v.split_once(':') {
            Some(("agent", label)) if !label.is_empty() => DeclaredView::Agent(label.to_owned()),
            Some(("automation", label)) if !label.is_empty() => {
                DeclaredView::Automation(label.to_owned())
            }
            _ => DeclaredView::Unknown,
        },
        None => DeclaredView::Unknown,
    }
}

/// The ceiling a caller asked for, if it named a tier.
#[must_use]
pub fn ceiling(header: Option<&str>) -> Option<AuthorityTier> {
    header.and_then(|v| v.trim().parse().ok())
}

/// Mint the principal for a local caller the policy granted `granted`.
#[must_use]
pub fn mint(
    peer: &PeerCred,
    granted: AuthorityTier,
    actor: Option<&str>,
    ceiling_header: Option<&str>,
) -> Principal {
    let effective = ceiling(ceiling_header).map_or(granted, |c| c.min(granted));
    Principal::mint(
        AttestedView::LocalUid {
            uid: peer.uid,
            gid: peer.gid,
            pid: peer.pid.unwrap_or(-1),
        },
        declared(actor),
        GrantView { granted, effective },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAEMON: u32 = 501;

    fn peer(uid: u32) -> PeerCred {
        PeerCred {
            uid,
            gid: 20,
            pid: Some(7),
        }
    }

    #[test]
    fn the_daemons_user_and_root_may_do_everything_and_others_what_the_group_allows() {
        let owner = GrantPolicy::new(DAEMON, SocketAccess::Owner, GroupTier::Mutate);
        assert_eq!(owner.grant(&peer(DAEMON)), Some(AuthorityTier::Destructive));
        assert_eq!(owner.grant(&peer(0)), Some(AuthorityTier::Destructive));
        assert_eq!(owner.grant(&peer(1000)), None);

        let group = GrantPolicy::new(DAEMON, SocketAccess::Group, GroupTier::Mutate);
        assert_eq!(group.grant(&peer(1000)), Some(AuthorityTier::Mutate));
        let observers = GrantPolicy::new(DAEMON, SocketAccess::Group, GroupTier::Observe);
        assert_eq!(observers.grant(&peer(1000)), Some(AuthorityTier::Observe));
    }

    #[test]
    fn a_ceiling_lowers_authority_and_never_raises_it() {
        let p = mint(
            &peer(DAEMON),
            AuthorityTier::Destructive,
            None,
            Some("observe"),
        );
        assert_eq!(p.effective(), AuthorityTier::Observe);
        let p = mint(
            &peer(1000),
            AuthorityTier::Observe,
            None,
            Some("destructive"),
        );
        assert_eq!(p.effective(), AuthorityTier::Observe);
        let p = mint(&peer(DAEMON), AuthorityTier::Mutate, None, Some("nonsense"));
        assert_eq!(p.effective(), AuthorityTier::Mutate);
    }

    #[test]
    fn a_declared_actor_is_parsed_and_never_trusted_for_more() {
        assert_eq!(declared(Some("human")), DeclaredView::Human);
        assert_eq!(
            declared(Some("agent:claude")),
            DeclaredView::Agent("claude".into())
        );
        assert_eq!(
            declared(Some("automation:tend")),
            DeclaredView::Automation("tend".into())
        );
        assert_eq!(declared(Some("agent:")), DeclaredView::Unknown);
        assert_eq!(declared(Some("root")), DeclaredView::Unknown);
        assert_eq!(declared(None), DeclaredView::Unknown);
    }
}
