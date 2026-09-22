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
//! A remote caller is whoever its TLS handshake proved it holds the key of:
//! a pinned client, granted the tier the server declared for that pin
//! ([`crate::pins`]).
//!
//! A request may lower its own authority with `Engenho-Ceiling` (an agent
//! told to observe only), never raise it. `Engenho-Actor` says what kind of
//! caller it claims to be; it is recorded, and never authority.

use engenho_config::{GroupTier, SocketAccess};
use engenho_control_types::types::{AttestedView, DeclaredView, GrantView, SpkiSha256};
use engenho_control_types::{AuthorityTier, Principal};

use crate::pins::Client;

/// Who is calling, as the transport proved it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Peer {
    /// On the local socket: what the kernel said.
    Local(PeerCred),
    /// On the remote listener: the pinned client whose key the handshake
    /// proved.
    Remote(Client),
}

impl Peer {
    /// What the transport attested, in the control API's words.
    ///
    /// # Errors
    ///
    /// A pin that does not spell as one (never, for a parsed pin).
    pub fn attested(&self) -> Result<AttestedView, String> {
        Ok(match self {
            Self::Local(peer) => AttestedView::LocalUid {
                uid: peer.uid,
                gid: peer.gid,
                pid: peer.pid.unwrap_or(-1),
            },
            Self::Remote(client) => AttestedView::RemotePin {
                client: client.name.clone(),
                spki: SpkiSha256::try_from(client.spki.to_string()).map_err(|e| e.to_string())?,
            },
        })
    }
}

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

    /// The tier `peer` is entitled to, or `None`: a local peer by this
    /// policy, a remote one by its pin.
    #[must_use]
    pub const fn grant_peer(&self, peer: &Peer) -> Option<AuthorityTier> {
        match peer {
            Peer::Local(local) => self.grant(local),
            Peer::Remote(client) => Some(client.tier),
        }
    }

    /// The tier a local `peer` is entitled to, or `None`.
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

/// Mint the principal for a caller the transport attested as `attested` and
/// the policy granted `granted`.
#[must_use]
pub fn mint(
    attested: AttestedView,
    granted: AuthorityTier,
    actor: Option<&str>,
    ceiling_header: Option<&str>,
) -> Principal {
    let effective = ceiling(ceiling_header).map_or(granted, |c| c.min(granted));
    Principal::mint(attested, declared(actor), GrantView { granted, effective })
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

    fn attested(uid: u32) -> AttestedView {
        Peer::Local(peer(uid)).attested().unwrap()
    }

    #[test]
    fn a_ceiling_lowers_authority_and_never_raises_it() {
        let p = mint(
            attested(DAEMON),
            AuthorityTier::Destructive,
            None,
            Some("observe"),
        );
        assert_eq!(p.effective(), AuthorityTier::Observe);
        let p = mint(
            attested(1000),
            AuthorityTier::Observe,
            None,
            Some("destructive"),
        );
        assert_eq!(p.effective(), AuthorityTier::Observe);
        let p = mint(
            attested(DAEMON),
            AuthorityTier::Mutate,
            None,
            Some("nonsense"),
        );
        assert_eq!(p.effective(), AuthorityTier::Mutate);
    }

    #[test]
    fn a_remote_peer_is_granted_its_pins_tier_whatever_the_socket_policy() {
        let key = engenho_control_types::pin::KeyMaterial::generate().unwrap();
        let remote = Peer::Remote(Client {
            name: "ryn".into(),
            spki: key.spki(),
            tier: AuthorityTier::Mutate,
        });
        let owner_only = GrantPolicy::new(DAEMON, SocketAccess::Owner, GroupTier::Observe);
        assert_eq!(owner_only.grant_peer(&remote), Some(AuthorityTier::Mutate));
        assert!(matches!(
            remote.attested().unwrap(),
            AttestedView::RemotePin { client, .. } if client == "ryn"
        ));
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
