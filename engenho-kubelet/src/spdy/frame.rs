//! SPDY/3.1 frames as values, and the stateful reader and writer that carry
//! them over a byte stream.
//!
//! Wire shapes (draft-mbelshe-httpbis-spdy-00 §2.2, §2.6): a control frame is
//! `1 | version:15 | type:16 | flags:8 | length:24 | payload`; a data frame is
//! `0 | stream:31 | flags:8 | length:24 | payload`. Header blocks are
//! `count:32` then `(len:32 name len:32 value)*`, compressed by the
//! connection's one zlib stream per direction ([`super::header_zlib`]).

use super::SpdyError;
use super::header_zlib::{HeaderDeflater, HeaderInflater};

/// The only version this codec speaks.
pub const VERSION: u16 = 3;

/// A stream identifier: odd when the client opened it, even for the server.
pub type StreamId = u32;

/// `FLAG_FIN`: the sender's last frame on the stream.
const FLAG_FIN: u8 = 0x01;
/// `FLAG_UNIDIRECTIONAL` on SYN_STREAM: the recipient may not send.
const FLAG_UNIDIRECTIONAL: u8 = 0x02;
/// The 31-bit stream-id mask.
const ID_MASK: u32 = 0x7fff_ffff;
/// The largest payload a 24-bit length can carry.
pub const MAX_PAYLOAD: usize = 0x00ff_ffff;

/// Control frame types.
const SYN_STREAM: u16 = 1;
const SYN_REPLY: u16 = 2;
const RST_STREAM: u16 = 3;
const SETTINGS: u16 = 4;
const PING: u16 = 6;
const GOAWAY: u16 = 7;
const HEADERS: u16 = 8;
const WINDOW_UPDATE: u16 = 9;

/// A header block: lower-case names, in order. A multi-valued header keeps
/// its NUL-separated wire form as one value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<(String, String)>);

impl Headers {
    /// No headers.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// These headers plus `name: value` (the name lower-cased, as SPDY/3
    /// requires).
    #[must_use]
    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.0.push((name.to_ascii_lowercase(), value.to_owned()));
        self
    }

    /// The first value of `name`, case-insensitively.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.split('\0').next().unwrap_or_default())
    }

    /// Every `(name, value)` pair.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }
}

/// RST_STREAM status codes (§2.6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RstStatus {
    /// 1.
    ProtocolError,
    /// 2.
    InvalidStream,
    /// 3: the stream was refused before any processing.
    RefusedStream,
    /// 5: the sender no longer wants the stream.
    Cancel,
    /// 6.
    InternalError,
    /// Any other code, kept as sent.
    Other(u32),
}

impl RstStatus {
    const fn code(self) -> u32 {
        match self {
            Self::ProtocolError => 1,
            Self::InvalidStream => 2,
            Self::RefusedStream => 3,
            Self::Cancel => 5,
            Self::InternalError => 6,
            Self::Other(c) => c,
        }
    }

    const fn of(code: u32) -> Self {
        match code {
            1 => Self::ProtocolError,
            2 => Self::InvalidStream,
            3 => Self::RefusedStream,
            5 => Self::Cancel,
            6 => Self::InternalError,
            c => Self::Other(c),
        }
    }
}

/// One SETTINGS entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting {
    /// The entry's flags byte.
    pub flags: u8,
    /// The 24-bit setting id (7 is `SETTINGS_INITIAL_WINDOW_SIZE`).
    pub id: u32,
    /// Its value.
    pub value: u32,
}

/// A frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Open a stream.
    SynStream {
        /// The new stream.
        stream: StreamId,
        /// The stream this one is associated with, or 0.
        associated: StreamId,
        /// 0 (highest) to 7.
        priority: u8,
        /// The opener sends nothing more.
        fin: bool,
        /// The recipient may not send.
        unidirectional: bool,
        /// The stream's headers.
        headers: Headers,
    },
    /// Accept a stream.
    SynReply {
        /// The accepted stream.
        stream: StreamId,
        /// The replier sends nothing more.
        fin: bool,
        /// Reply headers.
        headers: Headers,
    },
    /// Abort a stream.
    RstStream {
        /// The aborted stream.
        stream: StreamId,
        /// Why.
        status: RstStatus,
    },
    /// Connection settings.
    Settings {
        /// Its entries.
        entries: Vec<Setting>,
    },
    /// Liveness probe; the recipient echoes a peer-initiated one.
    Ping {
        /// Odd from the client, even from the server.
        id: u32,
    },
    /// The sender opens no more streams.
    GoAway {
        /// The last stream the sender processed.
        last_good: StreamId,
        /// 0 OK, 1 protocol error, 2 internal error.
        status: u32,
    },
    /// More headers on an open stream.
    Headers {
        /// The stream.
        stream: StreamId,
        /// The sender's last frame on it.
        fin: bool,
        /// The headers.
        headers: Headers,
    },
    /// Flow-control credit.
    WindowUpdate {
        /// The stream it credits.
        stream: StreamId,
        /// How many bytes.
        delta: u32,
    },
    /// Payload.
    Data {
        /// The stream.
        stream: StreamId,
        /// The sender's last frame on it.
        fin: bool,
        /// The bytes.
        data: Vec<u8>,
    },
    /// A control frame of a type this codec does not model (NOOP,
    /// CREDENTIAL), read and skipped whole.
    Unknown {
        /// Its type.
        kind: u16,
    },
}

/// Reads frames out of a byte stream that arrives in arbitrary pieces.
#[derive(Debug, Default)]
pub struct FrameReader {
    buf: Vec<u8>,
    inflater: HeaderInflater,
}

impl FrameReader {
    /// Append bytes as they arrive.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The next whole frame, or `None` until more bytes arrive.
    ///
    /// # Errors
    ///
    /// [`SpdyError`] for a frame this codec cannot accept; the connection is
    /// unusable afterwards, as the header zlib stream is out of step.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, SpdyError> {
        let Some(head) = self.buf.get(..8) else {
            return Ok(None);
        };
        let word = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
        let flags = head[4];
        let len = usize::from(head[5]) << 16 | usize::from(head[6]) << 8 | usize::from(head[7]);
        let Some(payload) = self.buf.get(8..8 + len) else {
            return Ok(None);
        };
        let payload = payload.to_vec();
        self.buf.drain(..8 + len);
        let fin = flags & FLAG_FIN != 0;
        if word & 0x8000_0000 == 0 {
            return Ok(Some(Frame::Data {
                stream: word & ID_MASK,
                fin,
                data: payload,
            }));
        }
        let version = u16::try_from((word >> 16) & 0x7fff).unwrap_or(u16::MAX);
        if version != VERSION {
            return Err(SpdyError::UnsupportedVersion(version));
        }
        let kind = u16::try_from(word & 0xffff).unwrap_or(u16::MAX);
        let p = Payload(&payload);
        let frame = match kind {
            SYN_STREAM => {
                let block = p.from(10, "SYN_STREAM")?;
                Frame::SynStream {
                    stream: p.u32_at(0, "SYN_STREAM")? & ID_MASK,
                    associated: p.u32_at(4, "SYN_STREAM")? & ID_MASK,
                    priority: p.byte(8, "SYN_STREAM")? >> 5,
                    fin,
                    unidirectional: flags & FLAG_UNIDIRECTIONAL != 0,
                    headers: self.headers(block)?,
                }
            }
            SYN_REPLY => Frame::SynReply {
                stream: p.u32_at(0, "SYN_REPLY")? & ID_MASK,
                fin,
                headers: self.headers(p.from(4, "SYN_REPLY")?)?,
            },
            HEADERS => Frame::Headers {
                stream: p.u32_at(0, "HEADERS")? & ID_MASK,
                fin,
                headers: self.headers(p.from(4, "HEADERS")?)?,
            },
            RST_STREAM => Frame::RstStream {
                stream: p.u32_at(0, "RST_STREAM")? & ID_MASK,
                status: RstStatus::of(p.u32_at(4, "RST_STREAM")?),
            },
            SETTINGS => {
                let count = p.u32_at(0, "SETTINGS")? as usize;
                let mut entries = Vec::with_capacity(count.min(64));
                for i in 0..count {
                    let at = 4 + i * 8;
                    let head = p.u32_at(at, "SETTINGS")?;
                    entries.push(Setting {
                        flags: u8::try_from(head >> 24).unwrap_or(0),
                        id: head & 0x00ff_ffff,
                        value: p.u32_at(at + 4, "SETTINGS")?,
                    });
                }
                Frame::Settings { entries }
            }
            PING => Frame::Ping {
                id: p.u32_at(0, "PING")?,
            },
            GOAWAY => Frame::GoAway {
                last_good: p.u32_at(0, "GOAWAY")? & ID_MASK,
                status: p.u32_at(4, "GOAWAY").unwrap_or(0),
            },
            WINDOW_UPDATE => Frame::WindowUpdate {
                stream: p.u32_at(0, "WINDOW_UPDATE")? & ID_MASK,
                delta: p.u32_at(4, "WINDOW_UPDATE")? & ID_MASK,
            },
            other => Frame::Unknown { kind: other },
        };
        Ok(Some(frame))
    }

    fn headers(&mut self, block: &[u8]) -> Result<Headers, SpdyError> {
        let raw = self.inflater.inflate(block)?;
        let p = Payload(&raw);
        let count = p.u32_at(0, "header block")? as usize;
        let mut at = 4;
        let mut pairs = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let name = p.string(&mut at)?;
            let value = p.string(&mut at)?;
            pairs.push((name.to_ascii_lowercase(), value));
        }
        Ok(Headers(pairs))
    }
}

/// Bounds-checked reads over a frame payload.
struct Payload<'a>(&'a [u8]);

impl<'a> Payload<'a> {
    fn short(what: &str) -> SpdyError {
        SpdyError::Protocol(format!("{what} frame is shorter than its fields"))
    }

    fn u32_at(&self, at: usize, what: &str) -> Result<u32, SpdyError> {
        self.0
            .get(at..at + 4)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes)
            .ok_or_else(|| Self::short(what))
    }

    fn byte(&self, at: usize, what: &str) -> Result<u8, SpdyError> {
        self.0.get(at).copied().ok_or_else(|| Self::short(what))
    }

    fn from(&self, at: usize, what: &str) -> Result<&'a [u8], SpdyError> {
        self.0.get(at..).ok_or_else(|| Self::short(what))
    }

    fn string(&self, at: &mut usize) -> Result<String, SpdyError> {
        let len = self.u32_at(*at, "header block")? as usize;
        let bytes = self
            .0
            .get(*at + 4..*at + 4 + len)
            .ok_or_else(|| Self::short("header block"))?;
        *at += 4 + len;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| SpdyError::Protocol("a header is not UTF-8".into()))
    }
}

/// Writes frames; owns this side's header zlib stream, so every frame of one
/// connection goes through one writer, in order.
#[derive(Debug, Default)]
pub struct FrameWriter {
    deflater: HeaderDeflater,
}

impl FrameWriter {
    /// The bytes of `frame`.
    ///
    /// A data payload longer than [`MAX_PAYLOAD`] is the caller's bug; the
    /// session never builds one (it chunks at 32 KiB).
    #[must_use]
    pub fn encode(&mut self, frame: &Frame) -> Vec<u8> {
        match frame {
            Frame::Data { stream, fin, data } => {
                let mut out = Vec::with_capacity(8 + data.len());
                out.extend_from_slice(&(stream & ID_MASK).to_be_bytes());
                push_flags_len(&mut out, u8::from(*fin) * FLAG_FIN, data.len());
                out.extend_from_slice(data);
                out
            }
            Frame::SynStream {
                stream,
                associated,
                priority,
                fin,
                unidirectional,
                headers,
            } => {
                let mut body = Vec::new();
                body.extend_from_slice(&(stream & ID_MASK).to_be_bytes());
                body.extend_from_slice(&(associated & ID_MASK).to_be_bytes());
                body.push((priority & 0x07) << 5);
                body.push(0);
                body.extend(self.deflater.deflate(&header_block(headers)));
                let flags =
                    u8::from(*fin) * FLAG_FIN | u8::from(*unidirectional) * FLAG_UNIDIRECTIONAL;
                control(SYN_STREAM, flags, &body)
            }
            Frame::SynReply {
                stream,
                fin,
                headers,
            } => self.with_headers(SYN_REPLY, *stream, *fin, headers),
            Frame::Headers {
                stream,
                fin,
                headers,
            } => self.with_headers(HEADERS, *stream, *fin, headers),
            Frame::RstStream { stream, status } => {
                control(RST_STREAM, 0, &two_words(stream & ID_MASK, status.code()))
            }
            Frame::Settings { entries } => {
                let mut body = Vec::with_capacity(4 + entries.len() * 8);
                body.extend_from_slice(&u32::try_from(entries.len()).unwrap_or(0).to_be_bytes());
                for s in entries {
                    body.extend_from_slice(
                        &(u32::from(s.flags) << 24 | (s.id & 0x00ff_ffff)).to_be_bytes(),
                    );
                    body.extend_from_slice(&s.value.to_be_bytes());
                }
                control(SETTINGS, 0, &body)
            }
            Frame::Ping { id } => control(PING, 0, &id.to_be_bytes()),
            Frame::GoAway { last_good, status } => {
                control(GOAWAY, 0, &two_words(last_good & ID_MASK, *status))
            }
            Frame::WindowUpdate { stream, delta } => control(
                WINDOW_UPDATE,
                0,
                &two_words(stream & ID_MASK, delta & ID_MASK),
            ),
            Frame::Unknown { kind } => control(*kind, 0, &[]),
        }
    }

    fn with_headers(&mut self, kind: u16, stream: StreamId, fin: bool, h: &Headers) -> Vec<u8> {
        let mut body = (stream & ID_MASK).to_be_bytes().to_vec();
        body.extend(self.deflater.deflate(&header_block(h)));
        control(kind, u8::from(fin) * FLAG_FIN, &body)
    }
}

fn header_block(h: &Headers) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&u32::try_from(h.0.len()).unwrap_or(0).to_be_bytes());
    for (n, v) in &h.0 {
        for s in [n, v] {
            out.extend_from_slice(&u32::try_from(s.len()).unwrap_or(0).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
    }
    out
}

fn two_words(a: u32, b: u32) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&a.to_be_bytes());
    out[4..].copy_from_slice(&b.to_be_bytes());
    out
}

fn push_flags_len(out: &mut Vec<u8>, flags: u8, len: usize) {
    let len = u32::try_from(len.min(MAX_PAYLOAD)).unwrap_or(0);
    out.push(flags);
    out.extend_from_slice(&len.to_be_bytes()[1..]);
}

fn control(kind: u16, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&(0x8000 | VERSION).to_be_bytes());
    out.extend_from_slice(&kind.to_be_bytes());
    push_flags_len(&mut out, flags, body.len());
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two SYN_STREAMs exactly as upstream's framer (moby/spdystream v0.5.0,
    /// the one client-go port-forward uses) writes them: error then data
    /// stream of one request, sharing one FDICT zlib stream.
    const UPSTREAM_SYN_STREAMS: &str = "800300010000003e0000000100000000000078f9e3c6a7c202253a703a2b294a4dcc851625aca9a0808614cb45e01c60616061c0c0c0c0590489c64c50f2663400000000ffff800300010000001800000003000000000000c2aa19540e2512d60b000000ffff";

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
            .collect()
    }

    #[test]
    fn upstreams_syn_streams_decode_with_the_dictionary() {
        let mut r = FrameReader::default();
        let bytes = hex(UPSTREAM_SYN_STREAMS);
        for chunk in bytes.chunks(7) {
            r.push(chunk);
        }
        for (want_id, want_type) in [(1, "error"), (3, "data")] {
            let Some(Frame::SynStream {
                stream, headers, ..
            }) = r.next_frame().expect("decodes")
            else {
                panic!("a SYN_STREAM");
            };
            assert_eq!(stream, want_id);
            assert_eq!(headers.get("streamType"), Some(want_type));
            assert_eq!(headers.get("port"), Some("8080"));
            assert_eq!(headers.get("requestid"), Some("0"));
        }
        assert_eq!(r.next_frame().expect("no error"), None);
    }

    #[test]
    fn every_frame_round_trips() {
        let frames = vec![
            Frame::SynStream {
                stream: 5,
                associated: 0,
                priority: 3,
                fin: false,
                unidirectional: true,
                headers: Headers::new().with("streamType", "data").with("port", "80"),
            },
            Frame::SynReply {
                stream: 5,
                fin: false,
                headers: Headers::new(),
            },
            Frame::Headers {
                stream: 5,
                fin: true,
                headers: Headers::new().with("x", "y"),
            },
            Frame::Data {
                stream: 5,
                fin: true,
                data: b"payload".to_vec(),
            },
            Frame::RstStream {
                stream: 5,
                status: RstStatus::Cancel,
            },
            Frame::Settings {
                entries: vec![Setting {
                    flags: 1,
                    id: 7,
                    value: 65_536,
                }],
            },
            Frame::Ping { id: 9 },
            Frame::GoAway {
                last_good: 5,
                status: 0,
            },
            Frame::WindowUpdate {
                stream: 5,
                delta: 1024,
            },
        ];
        let mut w = FrameWriter::default();
        let mut r = FrameReader::default();
        for f in &frames {
            r.push(&w.encode(f));
        }
        for f in &frames {
            assert_eq!(r.next_frame().expect("decodes").as_ref(), Some(f));
        }
    }

    #[test]
    fn a_frame_waits_for_its_last_byte() {
        let mut w = FrameWriter::default();
        let bytes = w.encode(&Frame::Ping { id: 1 });
        let mut r = FrameReader::default();
        r.push(&bytes[..bytes.len() - 1]);
        assert_eq!(r.next_frame().expect("no error"), None);
        r.push(&bytes[bytes.len() - 1..]);
        assert_eq!(
            r.next_frame().expect("decodes"),
            Some(Frame::Ping { id: 1 })
        );
    }

    #[test]
    fn spdy_2_is_refused() {
        let mut r = FrameReader::default();
        r.push(&[0x80, 0x02, 0x00, 0x06, 0, 0, 0, 4, 0, 0, 0, 1]);
        assert!(matches!(
            r.next_frame(),
            Err(SpdyError::UnsupportedVersion(2))
        ));
    }
}
