//! THE EXEC STREAM PROTOCOL — `v5.channel.k8s.io` / `v4.channel.k8s.io`
//! framing, shared by exec and attach.
//!
//! ★ WHY THIS EXISTS AS ITS OWN MODULE. `kubectl exec`, `attach`, `cp` and
//! `port-forward` all ride a multiplexed stream, and the multiplexing is
//! the whole contract: stdin, stdout, stderr, the exit status and terminal
//! resizes share ONE connection, distinguished by a single leading byte.
//! Get that byte wrong and the client does not error — it prints the
//! command's stderr as if it were stdout, or silently treats a resize
//! message as program output. The protocol is therefore separated from the
//! transport so every framing rule is testable without a live WebSocket,
//! which is the same discipline that found a real bug in the etcd watch
//! loop when it was pulled out of `tonic::Streaming`.
//!
//! ★ WEBSOCKET, NOT SPDY, and that is a deliberate choice rather than a
//! shortcut. Upstream's original transport was SPDY/3.1 — a protocol with
//! no maintained Rust implementation, deprecated by its own authors, and
//! removed from browsers. Kubernetes 1.29+ added `v5.channel.k8s.io` over
//! WebSocket and modern kubectl negotiates it FIRST. Implementing the
//! living protocol reaches every current client; implementing the dead one
//! would be a large surface serving an ever-shrinking population.
//!
//! ★ CHANNEL NUMBERS ARE UPSTREAM'S AND ARE NOT NEGOTIABLE. They are not a
//! detail this implementation is free to choose: kubectl hard-codes them,
//! and a server that renumbered would produce a session that connects,
//! transfers bytes, and shows the user the wrong stream.

/// The channels a v5 exec stream carries.
///
/// A closed enum rather than raw bytes so an unhandled channel is a compile
/// error at the match, not a frame silently routed to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Client → server: the process's standard input.
    Stdin,
    /// Server → client: standard output.
    Stdout,
    /// Server → client: standard error.
    ///
    /// Distinct from stdout for the reason the distinction exists at all —
    /// merging them makes `kubectl exec ... 2>/dev/null` impossible and
    /// corrupts any caller piping stdout into a parser.
    Stderr,
    /// Server → client: a terminating `Status` object.
    ///
    /// This is how a NON-ZERO EXIT REACHES THE CLIENT. Without it `kubectl
    /// exec` cannot report a failing command's exit code and every failed
    /// command looks like a successful one that printed to stderr.
    Error,
    /// Client → server: a terminal resize (`{"Width":N,"Height":M}`).
    Resize,
    /// Client → server, `v5.channel.k8s.io` only: close the stream named by
    /// the payload's single byte. This is how `kubectl cp` and `exec -i`
    /// signal end of input without closing the socket they still read from.
    Close,
}

impl Channel {
    /// Upstream's wire byte.
    #[must_use]
    pub fn as_byte(self) -> u8 {
        match self {
            Self::Stdin => 0,
            Self::Stdout => 1,
            Self::Stderr => 2,
            Self::Error => 3,
            Self::Resize => 4,
            Self::Close => 255,
        }
    }

    /// Decode a wire byte.
    ///
    /// Returns `None` for anything else rather than defaulting to stdout —
    /// a frame on an unknown channel is a protocol the server does not
    /// speak, and printing it as program output would corrupt the session
    /// invisibly.
    #[must_use]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Stdin),
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            3 => Some(Self::Error),
            4 => Some(Self::Resize),
            255 => Some(Self::Close),
            _ => None,
        }
    }

    /// Is this channel one the CLIENT is allowed to send on?
    ///
    /// A client writing to stdout or the error channel is either confused
    /// or hostile; accepting it would let a client inject text into its own
    /// session's output and, worse, forge a success status.
    #[must_use]
    pub fn client_may_send(self) -> bool {
        matches!(self, Self::Stdin | Self::Resize | Self::Close)
    }
}

/// A decoded frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub channel: Channel,
    pub payload: Vec<u8>,
}

/// Why a frame could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// A zero-length message carries no channel byte.
    ///
    /// Some proxies emit empty frames as keepalives, so this is a SKIP
    /// signal for the caller rather than a session-fatal error — tearing
    /// down an exec because a proxy sent a keepalive would look like a
    /// random disconnect to the user.
    #[error("empty frame carries no channel byte")]
    Empty,
    /// The channel byte is not one this protocol defines.
    #[error("unknown channel byte {byte}")]
    UnknownChannel { byte: u8 },
    /// The client sent on a server-only channel.
    #[error("client may not send on the {channel:?} channel")]
    ClientMayNotSend { channel: Channel },
    /// A close signal on a protocol that has none (v4 and below).
    #[error("the close channel exists only in {SUBPROTOCOL_V5}")]
    CloseNotInProtocol,
}

/// Encode a server → client frame.
#[must_use]
pub fn encode(channel: Channel, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(channel.as_byte());
    out.extend_from_slice(payload);
    out
}

/// Decode a client → server frame, enforcing channel direction and what the
/// negotiated protocol defines.
///
/// # Errors
///
/// [`FrameError`] naming why the frame is not one this session speaks.
pub fn decode_from_client(bytes: &[u8], protocol: Subprotocol) -> Result<Frame, FrameError> {
    let (&b, rest) = bytes.split_first().ok_or(FrameError::Empty)?;
    let channel = Channel::from_byte(b).ok_or(FrameError::UnknownChannel { byte: b })?;
    if !channel.client_may_send() {
        return Err(FrameError::ClientMayNotSend { channel });
    }
    if channel == Channel::Close && protocol != Subprotocol::V5 {
        return Err(FrameError::CloseNotInProtocol);
    }
    Ok(Frame {
        channel,
        payload: rest.to_vec(),
    })
}

/// A terminal resize the client requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct TerminalSize {
    /// Upstream capitalises these; a lowercase rename would silently parse
    /// to zero and resize every terminal to nothing.
    #[serde(rename = "Width")]
    pub width: u16,
    #[serde(rename = "Height")]
    pub height: u16,
}

/// Parse a resize payload.
#[must_use]
pub fn parse_resize(payload: &[u8]) -> Option<TerminalSize> {
    serde_json::from_slice(payload).ok()
}

/// Render the terminating `Status` upstream sends on the error channel.
///
/// ★ A ZERO EXIT AND A NON-ZERO EXIT ARE DIFFERENT OBJECTS, not one object
/// with a different number. Upstream sends `status: "Success"` with no
/// details for 0, and `status: "Failure"` carrying a `NonZeroExitCode`
/// cause for anything else. kubectl reads the CAUSE to recover the exit
/// code, so a "Failure" without one makes `kubectl exec; echo $?` report 1
/// for every failure regardless of what actually happened.
#[must_use]
pub fn exit_status(exit_code: i32) -> serde_json::Value {
    if exit_code == 0 {
        return serde_json::json!({
            "metadata": {},
            "status": "Success",
        });
    }
    serde_json::json!({
        "metadata": {},
        "status": "Failure",
        "message": format!("command terminated with exit code {exit_code}"),
        "reason": "NonZeroExitCode",
        "details": {
            "causes": [
                { "reason": "ExitCode", "message": exit_code.to_string() }
            ]
        }
    })
}

/// The newest channel subprotocol: v4 plus the close signal.
pub const SUBPROTOCOL_V5: &str = "v5.channel.k8s.io";

/// The channel subprotocol kubectl falls back to, and the one upstream's
/// WebSocket port-forward speaks.
pub const SUBPROTOCOL_V4: &str = "v4.channel.k8s.io";

/// A channel subprotocol engenho speaks.
///
/// v4 and v5 frame every channel identically and both carry a structured
/// `Status` on the error channel; v5 adds only [`Channel::Close`]. v3 and
/// below report the exit as a bare string on the error channel, so they are
/// not accepted: answering a v3 client with v4 semantics produces a session
/// that works until the command fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subprotocol {
    /// `v5.channel.k8s.io`.
    V5,
    /// `v4.channel.k8s.io`.
    V4,
}

impl Subprotocol {
    /// The header value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V5 => SUBPROTOCOL_V5,
            Self::V4 => SUBPROTOCOL_V4,
        }
    }
}

/// Pick the subprotocol from a client's `Sec-WebSocket-Protocol` offer: v5
/// when offered, else v4, else none.
#[must_use]
pub fn negotiate(offered: &str) -> Option<Subprotocol> {
    let has = |want: &str| offered.split(',').map(str::trim).any(|p| p == want);
    if has(SUBPROTOCOL_V5) {
        Some(Subprotocol::V5)
    } else if has(SUBPROTOCOL_V4) {
        Some(Subprotocol::V4)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_channel_bytes_are_upstreams_and_round_trip() {
        // kubectl hard-codes these. A server that renumbered would produce
        // a session that connects, transfers bytes, and shows the user the
        // wrong stream.
        for (ch, byte) in [
            (Channel::Stdin, 0u8),
            (Channel::Stdout, 1),
            (Channel::Stderr, 2),
            (Channel::Error, 3),
            (Channel::Resize, 4),
            (Channel::Close, 255),
        ] {
            assert_eq!(ch.as_byte(), byte, "{ch:?}");
            assert_eq!(Channel::from_byte(byte), Some(ch));
        }
    }

    #[test]
    fn an_unknown_channel_is_refused_not_treated_as_stdout() {
        // Defaulting to stdout would print protocol bytes as program
        // output and corrupt the session invisibly.
        assert_eq!(Channel::from_byte(5), None);
        assert_eq!(
            decode_from_client(&[7, b'x'], Subprotocol::V5),
            Err(FrameError::UnknownChannel { byte: 7 })
        );
    }

    #[test]
    fn a_client_may_not_write_to_stdout_or_forge_a_status() {
        // Accepting it would let a client inject text into its own
        // session's output and, worse, forge a success status.
        for ch in [Channel::Stdout, Channel::Stderr, Channel::Error] {
            assert!(!ch.client_may_send(), "{ch:?}");
            assert_eq!(
                decode_from_client(&[ch.as_byte(), b'x'], Subprotocol::V5),
                Err(FrameError::ClientMayNotSend { channel: ch })
            );
        }
        assert!(Channel::Stdin.client_may_send());
        assert!(Channel::Resize.client_may_send());
        assert!(Channel::Close.client_may_send());
    }

    #[test]
    fn an_empty_frame_is_a_skip_not_a_fatal_error() {
        // Some proxies emit empty frames as keepalives; tearing down an
        // exec for one would look like a random disconnect to the user.
        assert_eq!(
            decode_from_client(&[], Subprotocol::V5),
            Err(FrameError::Empty)
        );
    }

    #[test]
    fn stdin_frames_decode_with_their_payload_intact() {
        // Anti-vacuity: a decoder that rejected everything would pass every
        // negative test above.
        let f = decode_from_client(&[0, b'l', b's', b'\n'], Subprotocol::V4).expect("decodes");
        assert_eq!(f.channel, Channel::Stdin);
        assert_eq!(f.payload, b"ls\n".to_vec());
    }

    #[test]
    fn encoding_prefixes_the_channel_and_preserves_the_payload_exactly() {
        assert_eq!(encode(Channel::Stdout, b"hi"), vec![1, b'h', b'i']);
        assert_eq!(encode(Channel::Stderr, b"e"), vec![2, b'e']);
        // A zero-length payload still carries its channel byte, which is
        // how a stream signals EOF on one channel without closing the
        // connection.
        assert_eq!(encode(Channel::Stdout, b""), vec![1]);
    }

    #[test]
    fn a_resize_payload_uses_upstreams_capitalised_keys() {
        // A lowercase rename would parse to zero and resize every terminal
        // to nothing — a session that connects and shows an empty screen.
        let s = parse_resize(br#"{"Width":120,"Height":40}"#).expect("parses");
        assert_eq!((s.width, s.height), (120, 40));
        assert!(parse_resize(br#"{"width":120,"height":40}"#).is_none());
    }

    #[test]
    fn a_zero_exit_and_a_failure_are_different_objects() {
        let ok = exit_status(0);
        assert_eq!(ok["status"], "Success");
        assert!(ok.get("reason").is_none(), "success carries no reason");

        let bad = exit_status(42);
        assert_eq!(bad["status"], "Failure");
        assert_eq!(bad["reason"], "NonZeroExitCode");
        // kubectl reads the CAUSE to recover the code. A Failure without
        // one makes `kubectl exec; echo $?` report 1 for every failure
        // regardless of what actually happened.
        assert_eq!(bad["details"]["causes"][0]["reason"], "ExitCode");
        assert_eq!(bad["details"]["causes"][0]["message"], "42");
    }

    #[test]
    fn v5_is_preferred_v4_is_the_fallback_and_v3_is_refused() {
        assert_eq!(negotiate("v5.channel.k8s.io"), Some(Subprotocol::V5));
        assert_eq!(
            negotiate("v4.channel.k8s.io, v5.channel.k8s.io"),
            Some(Subprotocol::V5)
        );
        assert_eq!(
            negotiate("v4.channel.k8s.io,v3.channel.k8s.io"),
            Some(Subprotocol::V4)
        );
        assert_eq!(negotiate("v3.channel.k8s.io"), None);
        assert_eq!(negotiate(""), None);
    }

    #[test]
    fn the_close_signal_exists_only_in_v5() {
        let f = decode_from_client(&[255, 0], Subprotocol::V5).expect("v5 close");
        assert_eq!(f.channel, Channel::Close);
        assert_eq!(f.payload, vec![Channel::Stdin.as_byte()]);
        assert_eq!(
            decode_from_client(&[255, 0], Subprotocol::V4),
            Err(FrameError::CloseNotInProtocol)
        );
    }
}
