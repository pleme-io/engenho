//! The names a Pod stream negotiates on the wire, shared by both ends of
//! engenho's relay (apiserver and kubelet). Upstream's spellings, exactly:
//! clients hardcode them.

/// The `Upgrade` token of a plain SPDY upgrade.
pub const SPDY_31: &str = "SPDY/3.1";

/// The port-forward stream protocol.
pub const PORT_FORWARD: &str = "portforward.k8s.io";

/// The WebSocket subprotocol that tunnels a SPDY port-forward session in
/// binary messages (upstream's PortForwardWebsockets, kubectl ≥ 1.31).
pub const SPDY_PORT_FORWARD_TUNNEL: &str = "SPDY/3.1+portforward.k8s.io";

/// The header a SPDY upgrade offers its stream protocols in, and the answer
/// names the chosen one in.
pub const STREAM_PROTOCOL_HEADER: &str = "x-stream-protocol-version";

/// The header a refused SPDY upgrade lists what would be accepted in.
pub const ACCEPTED_STREAM_PROTOCOLS_HEADER: &str = "x-accepted-stream-protocol-versions";
