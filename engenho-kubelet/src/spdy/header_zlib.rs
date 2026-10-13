//! The two halves of SPDY/3 header compression: one zlib stream per
//! direction, shared by every header block on the connection, each block
//! ending on a sync-flush boundary.
//!
//! ★ INFLATE takes the peer's stream as it comes. Upstream's framer writes
//! FDICT and references [`HEADER_DICTIONARY`]; a stream without FDICT is
//! accepted too. miniz_oxide has no preset-dictionary call, so the dictionary
//! is placed in the output window ahead of the first block: deflate's
//! back-references cannot tell a preset dictionary from earlier output, which
//! is exactly how zlib defines one.
//!
//! ★ DEFLATE writes stored (uncompressed) blocks behind a zlib header without
//! FDICT. A header block is a few dozen bytes once per stream, so compression
//! buys nothing; what matters is that every reader accepts it. Go's zlib
//! reader only consults the dictionary when FDICT is set, and a stored block
//! is byte-aligned and complete, which is all a sync flush promises.

use miniz_oxide::inflate::TINFLStatus;
use miniz_oxide::inflate::core::{DecompressorOxide, decompress, inflate_flags};

use super::SpdyError;
use super::dictionary::HEADER_DICTIONARY;

/// The largest decompressed header block accepted: a bound on what one peer
/// frame can make this process allocate.
pub const MAX_HEADER_BLOCK: usize = 256 * 1024;

/// History deflate may reference: 32 KiB.
const WINDOW: usize = 32 * 1024;

/// The empty stored block a zlib sync flush ends with.
///
/// Load-bearing, not decoration: Go's inflater hands out a stored block's
/// bytes only when the NEXT block starts, so a header block that ends on its
/// data block alone is never delivered, and spdystream reports "compressed
/// payload size was incorrect" (measured against kubectl 1.34, 2026-10-13).
const SYNC_MARKER: [u8; 5] = [0x00, 0x00, 0x00, 0xff, 0xff];

/// The zlib FDICT flag in FLG.
const FDICT: u8 = 0x20;

/// Inflates a peer's header blocks.
pub struct HeaderInflater {
    started: bool,
    window: Vec<u8>,
    state: Box<DecompressorOxide>,
}

impl std::fmt::Debug for HeaderInflater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderInflater")
            .field("started", &self.started)
            .field("window", &self.window.len())
            .finish_non_exhaustive()
    }
}

impl Default for HeaderInflater {
    fn default() -> Self {
        Self {
            started: false,
            window: Vec::new(),
            state: Box::default(),
        }
    }
}

impl HeaderInflater {
    /// Inflate one header block.
    ///
    /// # Errors
    ///
    /// [`SpdyError::Compression`] for a malformed stream, a dictionary other
    /// than SPDY/3's, or a block larger than [`MAX_HEADER_BLOCK`].
    pub fn inflate(&mut self, mut block: &[u8]) -> Result<Vec<u8>, SpdyError> {
        if !self.started {
            block = self.read_zlib_header(block)?;
            self.started = true;
        }
        let start = self.window.len();
        let mut pos = start;
        let flags = inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF
            | inflate_flags::TINFL_FLAG_HAS_MORE_INPUT;
        loop {
            if pos - start > MAX_HEADER_BLOCK {
                return Err(SpdyError::Compression(format!(
                    "a header block inflates past {MAX_HEADER_BLOCK} bytes"
                )));
            }
            self.window.resize(pos + 4096, 0);
            let (status, read, written) =
                decompress(&mut self.state, block, &mut self.window, pos, flags);
            pos += written;
            block = &block[read..];
            if read == 0
                && written == 0
                && status == TINFLStatus::NeedsMoreInput
                && !block.is_empty()
            {
                return Err(SpdyError::Compression(
                    "the header block made no progress".into(),
                ));
            }
            match status {
                TINFLStatus::NeedsMoreInput if block.is_empty() => break,
                TINFLStatus::NeedsMoreInput | TINFLStatus::HasMoreOutput => {}
                TINFLStatus::Done => {
                    return Err(SpdyError::Compression(
                        "the peer ended its header zlib stream".into(),
                    ));
                }
                other => {
                    return Err(SpdyError::Compression(format!(
                        "malformed header block: {other:?}"
                    )));
                }
            }
        }
        self.window.truncate(pos);
        let out = self.window[start..pos].to_vec();
        if self.window.len() > 2 * WINDOW {
            let excess = self.window.len() - WINDOW;
            self.window.drain(..excess);
        }
        Ok(out)
    }

    fn read_zlib_header<'a>(&mut self, block: &'a [u8]) -> Result<&'a [u8], SpdyError> {
        let [cmf, flg, rest @ ..] = block else {
            return Err(SpdyError::Compression(
                "the first header block is shorter than a zlib header".into(),
            ));
        };
        if cmf & 0x0f != 8 || (u16::from(*cmf) << 8 | u16::from(*flg)) % 31 != 0 {
            return Err(SpdyError::Compression(format!(
                "not a zlib deflate header: {cmf:#04x} {flg:#04x}"
            )));
        }
        if flg & FDICT == 0 {
            return Ok(rest);
        }
        let [a, b, c, d, rest @ ..] = rest else {
            return Err(SpdyError::Compression(
                "truncated zlib dictionary id".into(),
            ));
        };
        let id = u32::from_be_bytes([*a, *b, *c, *d]);
        let want = adler32(&HEADER_DICTIONARY);
        if id != want {
            return Err(SpdyError::Compression(format!(
                "zlib dictionary {id:#010x} is not SPDY/3's ({want:#010x})"
            )));
        }
        self.window.extend_from_slice(&HEADER_DICTIONARY);
        Ok(rest)
    }
}

/// Deflates this side's header blocks.
#[derive(Debug, Default)]
pub struct HeaderDeflater {
    started: bool,
}

impl HeaderDeflater {
    /// Deflate one header block, ending on a sync-flush boundary.
    #[must_use]
    pub fn deflate(&mut self, block: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(block.len() + 2 + 5 * (block.len() / 0xffff + 1));
        if !self.started {
            out.extend_from_slice(&[0x78, 0x01]);
            self.started = true;
        }
        for chunk in block.chunks(0xffff) {
            let len = u16::try_from(chunk.len()).unwrap_or(u16::MAX);
            out.push(0x00);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&(!len).to_le_bytes());
            out.extend_from_slice(chunk);
        }
        out.extend_from_slice(&SYNC_MARKER);
        out
    }
}

fn adler32(data: &[u8]) -> u32 {
    let (mut a, mut b) = (1u32, 0u32);
    for &x in data {
        a = (a + u32::from(x)) % 65521;
        b = (b + a) % 65521;
    }
    b << 16 | a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_this_side_deflates_inflates_across_blocks() {
        let mut d = HeaderDeflater::default();
        let mut i = HeaderInflater::default();
        for block in [&b"first"[..], b"", &vec![7u8; 70_000]] {
            assert_eq!(i.inflate(&d.deflate(block)).expect("inflates"), block);
        }
    }

    #[test]
    fn every_block_ends_on_a_sync_flush_marker() {
        let mut d = HeaderDeflater::default();
        assert_eq!(
            d.deflate(&[0, 0, 0, 0]),
            [
                0x78, 0x01, 0x00, 0x04, 0x00, 0xfb, 0xff, 0, 0, 0, 0, 0x00, 0x00, 0x00, 0xff, 0xff
            ]
        );
        assert_eq!(d.deflate(&[]), SYNC_MARKER);
    }

    #[test]
    fn a_foreign_dictionary_is_refused() {
        let mut i = HeaderInflater::default();
        let err = i
            .inflate(&[0x78, 0xf9, 0, 0, 0, 1, 0x03, 0x00])
            .expect_err("refused");
        assert!(err.to_string().contains("not SPDY/3's"), "{err}");
    }

    #[test]
    fn the_dictionary_id_is_the_one_upstream_writes() {
        assert_eq!(adler32(&HEADER_DICTIONARY), 0xe3c6_a7c2);
    }
}
