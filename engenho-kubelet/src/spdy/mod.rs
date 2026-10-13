//! SPDY/3.1 — the multiplexing session kubectl and k9s port-forward over.
//!
//! ★ WHY A KUBELET SPEAKS A DEAD PROTOCOL. kubectl ≥ 1.31 and k9s 0.50 forward
//! ports by opening a WebSocket with subprotocol `SPDY/3.1+portforward.k8s.io`
//! and tunnelling a whole SPDY/3.1 session through its binary messages
//! (upstream's PortForwardWebsockets); when that upgrade fails they fall back
//! to a plain `Upgrade: SPDY/3.1`. Either way the session inside is SPDY, so
//! a cluster that does not speak it has no port-forward any client uses.
//!
//! This is a codec, not an HTTP/2 predecessor: frames
//! ([`frame::FrameReader`], [`frame::FrameWriter`]) and the per-direction
//! header zlib streams ([`header_zlib`]). The port-forward session built on
//! it is [`crate::port_forward_spdy`].
//!
//! ★ FLOW CONTROL IS DECODED, NOT ENFORCED. SETTINGS and WINDOW_UPDATE are
//! read as typed frames and then ignored. The only peer that exists
//! (moby/spdystream, inside client-go) never sends a WINDOW_UPDATE and never
//! honours one, so a sender that respected the 64 KiB initial window would
//! stall every transfer past 64 KiB against the one client there is.
//! Backpressure comes from the transport instead: frames are written in
//! order, and a slow reader stalls its writer.

pub mod dictionary;
pub mod frame;
pub mod header_zlib;

pub use frame::{Frame, FrameReader, FrameWriter, Headers, RstStatus, StreamId};

/// The `Upgrade` token of a plain SPDY/3.1 upgrade.
pub const UPGRADE_TOKEN: &str = engenho_types::stream_protocol::SPDY_31;

/// Why a SPDY byte stream cannot be read further.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpdyError {
    /// A frame that breaks the framing rules.
    #[error("SPDY protocol error: {0}")]
    Protocol(String),
    /// A header block that does not inflate.
    #[error("SPDY header compression: {0}")]
    Compression(String),
    /// A control frame of another SPDY version.
    #[error("SPDY version {0} is not supported (only 3)")]
    UnsupportedVersion(u16),
}
