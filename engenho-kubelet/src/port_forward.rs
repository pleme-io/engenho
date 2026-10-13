//! THE PORT-FORWARD PROTOCOL — upstream's WebSocket port-forward framing.
//!
//! One WebSocket carries every requested port. Port `i` (in the order the
//! query names them) owns two channels: data on `2i` and errors on `2i+1`.
//! The server's FIRST frame on each of them is the port number as two
//! little-endian bytes, which is how a client maps channels back to ports;
//! every later data frame is raw TCP payload, both ways. A port the kubelet
//! cannot dial gets a message on its error channel instead of data — the
//! session stays up for the ports that did connect.
//!
//! Like [`crate::exec_channel`], the protocol is pure functions over bytes
//! so every framing rule is testable without a socket.

/// The subprotocols upstream's WebSocket port-forward accepts. All three
/// frame identically (binary, channel byte first).
pub const SUBPROTOCOLS: [&str; 3] = [
    crate::exec_channel::SUBPROTOCOL_V4,
    crate::port_forward_spdy::STREAM_PROTOCOL,
    "channel.k8s.io",
];

/// Why a port-forward is refused before upgrading.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PortForwardRefusal {
    /// No `ports` (or `port`) parameter.
    #[error("port-forward needs at least one `ports` parameter")]
    NoPorts,
    /// A port that is not a number in 1-65535.
    #[error("invalid port {0:?}: a port is a number in 1-65535")]
    BadPort(String),
    /// More ports than the one-byte channel space can address.
    #[error("too many ports ({0}): one session addresses at most 127")]
    TooManyPorts(usize),
    /// None of the accepted subprotocols was offered.
    #[error("client offered no supported port-forward subprotocol (need one of {SUBPROTOCOLS:?})")]
    SubprotocolUnsupported,
}

/// Parse the ports a client asked for, in order.
///
/// Accepts `ports=` and `port=`, repeated or comma-separated: upstream's
/// clients have spelled it each way.
///
/// # Errors
///
/// [`PortForwardRefusal`] for an empty, malformed or oversized list.
pub fn parse_ports(raw_query: &str) -> Result<Vec<u16>, PortForwardRefusal> {
    let mut ports = Vec::new();
    for pair in raw_query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k != "ports" && k != "port" {
            continue;
        }
        for word in v.split(&[',', ' '][..]).filter(|w| !w.is_empty()) {
            let word = word.replace("%2C", ",");
            match word.parse::<u16>() {
                Ok(p) if p > 0 => ports.push(p),
                _ => return Err(PortForwardRefusal::BadPort(word)),
            }
        }
    }
    if ports.is_empty() {
        return Err(PortForwardRefusal::NoPorts);
    }
    if ports.len() > 127 {
        return Err(PortForwardRefusal::TooManyPorts(ports.len()));
    }
    Ok(ports)
}

/// Pick the subprotocol to echo from a client's offer.
#[must_use]
pub fn negotiate(offered: &str) -> Option<&'static str> {
    let offered: Vec<&str> = offered.split(',').map(str::trim).collect();
    SUBPROTOCOLS.into_iter().find(|p| offered.contains(p))
}

/// The data channel of the `index`th port.
#[must_use]
pub const fn data_channel(index: usize) -> u8 {
    (index * 2) as u8
}

/// The error channel of the `index`th port.
#[must_use]
pub const fn error_channel(index: usize) -> u8 {
    (index * 2 + 1) as u8
}

/// What a client frame addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inbound {
    /// TCP payload for the `index`th port.
    Data(usize),
    /// A frame on an error channel or past the last port; dropped.
    Ignored,
}

/// Route a client frame's channel byte.
#[must_use]
pub fn route(channel: u8, ports: usize) -> Inbound {
    let index = usize::from(channel / 2);
    if channel.is_multiple_of(2) && index < ports {
        Inbound::Data(index)
    } else {
        Inbound::Ignored
    }
}

/// The frame that opens a channel: its port, little-endian.
#[must_use]
pub fn port_frame(channel: u8, port: u16) -> Vec<u8> {
    let [lo, hi] = port.to_le_bytes();
    vec![channel, lo, hi]
}

/// A frame carrying `payload` on `channel`.
#[must_use]
pub fn frame(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(channel);
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_parse_in_order_in_every_spelling_upstream_clients_use() {
        assert_eq!(parse_ports("ports=8080"), Ok(vec![8080]));
        assert_eq!(parse_ports("ports=80,443"), Ok(vec![80, 443]));
        assert_eq!(parse_ports("ports=80&ports=443"), Ok(vec![80, 443]));
        assert_eq!(parse_ports("port=5432&x=1"), Ok(vec![5432]));
    }

    #[test]
    fn a_missing_zero_or_malformed_port_is_refused() {
        assert_eq!(parse_ports(""), Err(PortForwardRefusal::NoPorts));
        assert_eq!(
            parse_ports("ports=0"),
            Err(PortForwardRefusal::BadPort("0".into()))
        );
        assert_eq!(
            parse_ports("ports=http"),
            Err(PortForwardRefusal::BadPort("http".into()))
        );
        assert_eq!(
            parse_ports("ports=70000"),
            Err(PortForwardRefusal::BadPort("70000".into()))
        );
    }

    #[test]
    fn each_port_owns_an_even_data_and_an_odd_error_channel() {
        assert_eq!((data_channel(0), error_channel(0)), (0, 1));
        assert_eq!((data_channel(2), error_channel(2)), (4, 5));
        assert_eq!(route(4, 3), Inbound::Data(2));
        assert_eq!(route(5, 3), Inbound::Ignored);
        assert_eq!(route(6, 3), Inbound::Ignored);
    }

    #[test]
    fn the_opening_frame_is_the_port_little_endian() {
        assert_eq!(port_frame(0, 8080), vec![0, 0x90, 0x1f]);
        assert_eq!(port_frame(3, 1), vec![3, 1, 0]);
    }

    #[test]
    fn v4_is_accepted_and_v5_alone_is_not() {
        assert_eq!(
            negotiate("v4.channel.k8s.io"),
            Some(crate::exec_channel::SUBPROTOCOL_V4)
        );
        assert_eq!(negotiate("portforward.k8s.io"), Some("portforward.k8s.io"));
        assert_eq!(negotiate("v5.channel.k8s.io"), None);
    }
}
