//! Node-local listeners: the surfaces engenho binds on the node that cannot
//! yet authenticate who calls them, and the one rule that follows. They bind
//! LOOPBACK until they can authenticate (IMPROVEMENT-PLAN T4.9).
//!
//! Two listeners qualify today:
//!
//! | Listener | Port | Serves | Missing gate |
//! |---|---|---|---|
//! | kubelet HTTP surface | :10250 | container logs, `exec` inside containers | authentication |
//! | etcd v3 facade | :2379 | the whole cluster state, Secrets included (read-only) | mutual TLS |
//!
//! Upstream binds both on every interface because upstream gates both. engenho
//! has neither gate yet, so an off-loopback bind would publish unauthenticated
//! `exec` and an unauthenticated read of every Secret to the network.
//!
//! ## What is enforced, and where
//!
//! - [`LoopbackAddr`] is loopback by construction: its only constructors are
//!   [`TryFrom<SocketAddr>`] and [`FromStr`], and both refuse anything else.
//! - [`crate::RuntimeConfig::validate`] runs [`crate::RuntimeConfig::node_local_addr`]
//!   over [`NodeLocalListener::ALL`], so every path that validates (the YAML
//!   parse, the progressive fold, `discover`, and `Runtime::start`) refuses a
//!   non-loopback address with [`crate::ConfigError::NodeLocalListener`].
//! - The config FIELDS are still `String`, so a `RuntimeConfig` holding
//!   `0.0.0.0:10250` can be built in code; only `validate()` stops it. That is
//!   a parse-boundary gate, not a type. The destination is the runtime binding
//!   the [`LoopbackAddr`] this module returns instead of the raw string.
//!
//! ## Why a hostname is refused
//!
//! `localhost:10250` usually resolves to loopback, but a name is resolved when
//! it is bound, not when it is parsed, and `/etc/hosts` can point it anywhere.
//! Only an `IP:port` literal can be proven loopback here.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use thiserror::Error;

/// A listener engenho binds on the node that cannot yet authenticate its
/// callers, and so binds loopback only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeLocalListener {
    /// The kubelet HTTP surface, upstream's :10250. It serves container logs
    /// and runs `exec` inside containers. Upstream gates it behind webhook
    /// authn/authz, and engenho has neither yet.
    Kubelet,
    /// The etcd v3 facade, upstream's :2379. It is read-only, but it reads the
    /// whole cluster state, Secrets included. Upstream etcd gates it behind
    /// mutual TLS, and engenho does not have that yet.
    EtcdFacade,
}

impl NodeLocalListener {
    /// Every node-local listener. [`crate::RuntimeConfig::validate`] checks each.
    pub const ALL: [Self; 2] = [Self::Kubelet, Self::EtcdFacade];

    /// The config path that sets this listener's address.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::Kubelet => "runtime.kubelet_listen_addr",
            Self::EtcdFacade => "runtime.etcd_listen_addr",
        }
    }

    /// The gate whose absence keeps this listener on loopback.
    #[must_use]
    pub const fn missing_gate(self) -> &'static str {
        match self {
            Self::Kubelet => "authentication",
            Self::EtcdFacade => "mutual TLS",
        }
    }
}

impl fmt::Display for NodeLocalListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Kubelet => "the kubelet HTTP surface (:10250)",
            Self::EtcdFacade => "the etcd v3 facade (:2379)",
        })
    }
}

/// Why an address was refused for a node-local listener.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ListenerAddrRejection {
    /// An address literal that is not on the loopback interface: `0.0.0.0`,
    /// `[::]`, a LAN address, a tailnet address.
    #[error("{0} is not a loopback address")]
    NotLoopback(SocketAddr),
    /// Not an `IP:port` literal: a hostname, or an address with no port. A
    /// name is resolved only when it is bound, so whether it lands on loopback
    /// cannot be known here.
    #[error("{0:?} is not an IP:port literal, so it cannot be proven loopback before it is bound")]
    NotALiteral(String),
}

/// A socket address on the loopback interface.
///
/// Its only constructors refuse every other address, so holding one is the
/// proof. IPv4 `127.0.0.0/8`, IPv6 `::1`, and the IPv4-mapped form
/// `::ffff:127.0.0.0/104` are loopback. Port `0` (an OS-assigned port) is
/// accepted, because the interface, not the port, is what exposes the surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LoopbackAddr(SocketAddr);

impl LoopbackAddr {
    /// The proven-loopback socket address, ready to bind.
    #[must_use]
    pub const fn socket_addr(self) -> SocketAddr {
        self.0
    }
}

impl TryFrom<SocketAddr> for LoopbackAddr {
    type Error = ListenerAddrRejection;

    fn try_from(addr: SocketAddr) -> Result<Self, Self::Error> {
        if is_loopback(addr.ip()) {
            Ok(Self(addr))
        } else {
            Err(ListenerAddrRejection::NotLoopback(addr))
        }
    }
}

impl FromStr for LoopbackAddr {
    type Err = ListenerAddrRejection;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // The parse error is dropped on purpose: `AddrParseError` only says
        // "invalid socket address syntax", which `NotALiteral` already says.
        let addr: SocketAddr = raw
            .parse()
            .map_err(|_| ListenerAddrRejection::NotALiteral(raw.to_owned()))?;
        Self::try_from(addr)
    }
}

impl fmt::Display for LoopbackAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// `to_canonical` folds an IPv4-mapped IPv6 address to its IPv4 form first,
/// because `Ipv6Addr::is_loopback` is true only for `::1` and would refuse
/// `::ffff:127.0.0.1`, which is loopback.
fn is_loopback(ip: IpAddr) -> bool {
    ip.to_canonical().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_v4_literals_are_accepted() {
        // rio's real override, the prescribed defaults, an OS-assigned port,
        // and the far end of 127.0.0.0/8.
        for raw in [
            "127.0.0.1:10251",
            "127.0.0.1:10250",
            "127.0.0.1:2379",
            "127.0.0.1:0",
            "127.255.255.254:10250",
        ] {
            let addr: LoopbackAddr = raw.parse().unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(addr.socket_addr(), raw.parse::<SocketAddr>().unwrap());
        }
    }

    #[test]
    fn loopback_v6_literals_are_accepted() {
        for raw in ["[::1]:10250", "[::1]:2379", "[::ffff:127.0.0.1]:10250"] {
            let addr: LoopbackAddr = raw.parse().unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(addr.socket_addr(), raw.parse::<SocketAddr>().unwrap());
        }
    }

    #[test]
    fn non_loopback_literals_are_refused_with_the_address() {
        for raw in [
            "0.0.0.0:10250",
            "[::]:2379",
            "10.0.0.5:10250",
            "192.168.1.10:2379",
            "100.64.0.1:10250",
            "[fe80::1]:10250",
            "[::ffff:10.0.0.5]:10250",
        ] {
            let want: SocketAddr = raw.parse().unwrap();
            assert_eq!(
                raw.parse::<LoopbackAddr>(),
                Err(ListenerAddrRejection::NotLoopback(want)),
                "{raw} must be refused as not loopback"
            );
        }
    }

    #[test]
    fn hostnames_and_portless_addresses_are_refused_as_unprovable() {
        for raw in [
            "localhost:10250",
            "node.example:2379",
            "127.0.0.1",
            "::1",
            "",
        ] {
            assert_eq!(
                raw.parse::<LoopbackAddr>(),
                Err(ListenerAddrRejection::NotALiteral(raw.to_owned())),
                "{raw:?} must be refused as not a literal"
            );
        }
    }

    #[test]
    fn try_from_socket_addr_agrees_with_from_str() {
        let lo: SocketAddr = "127.0.0.1:10250".parse().unwrap();
        let any: SocketAddr = "0.0.0.0:10250".parse().unwrap();
        assert_eq!(
            LoopbackAddr::try_from(lo).map(LoopbackAddr::socket_addr),
            Ok(lo)
        );
        assert_eq!(
            LoopbackAddr::try_from(any),
            Err(ListenerAddrRejection::NotLoopback(any))
        );
    }
}
