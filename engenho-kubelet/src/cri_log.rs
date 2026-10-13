//! The CRI container log format, written and read.
//!
//! A CRI runtime (containerd, CRI-O) writes every container log line as one
//! record, `<RFC3339Nano> <stdout|stderr> <F|P> <message>\n`, and the kubelet
//! parses those records back to serve `kubectl logs`: `F` closes a line, `P`
//! is a fragment of a longer one (a line over [`MAX_RECORD`] bytes, or output
//! that ended without a newline). The native backend writes its logs in this
//! same format, so each line carries the time it was written, which is what
//! `timestamps=true`, `sinceSeconds` and `sinceTime` read; anything that
//! reads CRI logs reads these files unchanged.
//!
//! [`RecordEncoder`] turns raw output bytes into records. [`render`] turns
//! records back into the text `kubectl logs` prints, and says how far it
//! consumed so a follow resumes on a record boundary.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// The longest message one record carries; a longer line is split into `P`
/// records, as containerd splits at its 16 KiB buffer.
pub const MAX_RECORD: usize = 16 * 1024;

/// Which output a record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    /// The container's stdout (and its terminal, with `tty`).
    Stdout,
    /// The container's stderr.
    Stderr,
}

impl LogStream {
    /// The record's stream field.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }

    fn parse(field: &[u8]) -> Option<Self> {
        match field {
            b"stdout" => Some(Self::Stdout),
            b"stderr" => Some(Self::Stderr),
            _ => None,
        }
    }
}

/// Frames one stream's raw output into CRI records, holding an unterminated
/// line until its newline (or [`Self::finish`]) arrives.
#[derive(Debug)]
pub struct RecordEncoder {
    stream: LogStream,
    pending: Vec<u8>,
}

impl RecordEncoder {
    /// An encoder for `stream`.
    #[must_use]
    pub const fn new(stream: LogStream) -> Self {
        Self {
            stream,
            pending: Vec::new(),
        }
    }

    /// Append to `out` the records `bytes` completes, stamped `now`.
    pub fn push(&mut self, bytes: &[u8], now: SystemTime, out: &mut Vec<u8>) {
        self.pending.extend_from_slice(bytes);
        let stamp = stamp(now);
        let mut start = 0;
        while let Some(nl) = self.pending[start..].iter().position(|b| *b == b'\n') {
            let line = &self.pending[start..start + nl];
            emit_line(&stamp, self.stream, line, true, out);
            start += nl + 1;
        }
        while self.pending.len() - start >= MAX_RECORD {
            let chunk = &self.pending[start..start + MAX_RECORD];
            record(&stamp, self.stream, false, chunk, out);
            start += MAX_RECORD;
        }
        self.pending.drain(..start);
    }

    /// Append to `out` the unterminated tail, if any, as a `P` record: the
    /// output ended without a newline.
    pub fn finish(&mut self, now: SystemTime, out: &mut Vec<u8>) {
        if !self.pending.is_empty() {
            record(&stamp(now), self.stream, false, &self.pending, out);
            self.pending.clear();
        }
    }
}

fn stamp(now: SystemTime) -> String {
    engenho_types::time::system_time_to_rfc3339_nano(now)
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn emit_line(stamp: &str, stream: LogStream, line: &[u8], full: bool, out: &mut Vec<u8>) {
    let mut rest = line;
    while rest.len() > MAX_RECORD {
        record(stamp, stream, false, &rest[..MAX_RECORD], out);
        rest = &rest[MAX_RECORD..];
    }
    record(stamp, stream, full, rest, out);
}

fn record(stamp: &str, stream: LogStream, full: bool, msg: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(stamp.as_bytes());
    out.push(b' ');
    out.extend_from_slice(stream.as_str().as_bytes());
    out.extend_from_slice(if full { b" F " } else { b" P " });
    out.extend_from_slice(msg);
    out.push(b'\n');
}

/// One parsed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    /// When the runtime wrote it, as written (`RFC3339Nano`).
    pub stamp: &'a str,
    /// The parsed instant.
    pub time: SystemTime,
    /// Its stream.
    pub stream: LogStream,
    /// `true` for a `P` fragment.
    pub partial: bool,
    /// The message, without the record's newline.
    pub msg: &'a [u8],
}

/// Parse one record (without its trailing newline). `None` when the line is
/// not a CRI record.
#[must_use]
pub fn parse_record(line: &[u8]) -> Option<Record<'_>> {
    let mut fields = line.splitn(4, |b| *b == b' ');
    let stamp = std::str::from_utf8(fields.next()?).ok()?;
    let stream = LogStream::parse(fields.next()?)?;
    let partial = match fields.next()? {
        b"F" => false,
        b"P" => true,
        _ => return None,
    };
    let msg = fields.next().unwrap_or_default();
    let time = engenho_types::time::parse_rfc3339_system_time(stamp)?;
    Some(Record {
        stamp,
        time,
        stream,
        partial,
        msg,
    })
}

/// What [`render`] prints.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RenderOptions {
    /// Prefix each line with the time its first record was written.
    pub timestamps: bool,
    /// Only lines written at or after this instant.
    pub since: Option<SystemTime>,
    /// Only the last `n` lines (applied before `since`, as the kubelet does).
    pub tail: Option<u32>,
}

/// [`render`]'s answer: the text, and how many bytes of the input it
/// consumed. A follow resumes at `consumed`, which is always a record
/// boundary with no line half-printed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rendered {
    /// The log text as `kubectl logs` prints it.
    pub text: String,
    /// Bytes of the input the text accounts for.
    pub consumed: usize,
}

struct Line<'a> {
    stamp: Option<&'a str>,
    time: Option<SystemTime>,
    msg: Vec<u8>,
    terminated: bool,
}

/// Render the records in `buf`, which starts on a record boundary.
///
/// A line is printed once its `F` record has been read. While one is still
/// open (`P` fragments with no `F` yet, on either stream) nothing past the
/// point it opened is consumed, so the next read prints it whole. With
/// `ended` (the container has exited and its output is all written) an open
/// line is printed as it stands, without a newline, as upstream prints a
/// final fragment. A line that is not a CRI record is printed verbatim with
/// no timestamp; `since` never drops it, since it carries no time.
#[must_use]
pub fn render(buf: &[u8], opts: RenderOptions, ended: bool) -> Rendered {
    let mut lines: Vec<(Line<'_>, usize)> = Vec::new();
    let mut open: [Option<Line<'_>>; 2] = [None, None];
    let mut offset = 0;
    let mut safe = 0;
    while let Some(nl) = buf[offset..].iter().position(|b| *b == b'\n') {
        let raw = &buf[offset..offset + nl];
        offset += nl + 1;
        match parse_record(raw) {
            Some(r) => {
                let slot = &mut open[r.stream as usize];
                let line = slot.get_or_insert_with(|| Line {
                    stamp: Some(r.stamp),
                    time: Some(r.time),
                    msg: Vec::new(),
                    terminated: false,
                });
                line.msg.extend_from_slice(r.msg);
                if !r.partial
                    && let Some(mut done) = slot.take()
                {
                    done.terminated = true;
                    lines.push((done, offset));
                }
            }
            None => lines.push((
                Line {
                    stamp: None,
                    time: None,
                    msg: raw.to_vec(),
                    terminated: true,
                },
                offset,
            )),
        }
        if open.iter().all(Option::is_none) {
            safe = offset;
        }
    }
    if ended {
        for line in open.into_iter().flatten() {
            lines.push((line, offset));
        }
        safe = offset;
    }
    let mut kept: Vec<Line<'_>> = lines
        .into_iter()
        .filter(|(_, end)| *end <= safe)
        .map(|(l, _)| l)
        .collect();
    if let Some(n) = opts.tail {
        let drop = kept.len().saturating_sub(n as usize);
        kept.drain(..drop);
    }
    let mut text = Vec::new();
    for line in kept {
        if let (Some(since), Some(t)) = (opts.since, line.time)
            && t < since
        {
            continue;
        }
        if opts.timestamps
            && let Some(stamp) = line.stamp
        {
            text.extend_from_slice(stamp.as_bytes());
            text.push(b' ');
        }
        text.extend_from_slice(&line.msg);
        if line.terminated {
            text.push(b'\n');
        }
    }
    Rendered {
        text: String::from_utf8_lossy(&text).into_owned(),
        consumed: safe,
    }
}

/// A shared CRI log file several streams append whole records to.
pub type SharedLog = Arc<Mutex<std::fs::File>>;

/// A pipe whose write end takes one stream's raw output; a thread frames
/// what arrives into records appended to `log`, each stamped when it was
/// read. The returned receiver turns `true` once the write end has closed and
/// the last record is on disk.
///
/// # Errors
///
/// The pipe or the thread could not be created.
pub fn framed_sink(
    log: SharedLog,
    stream: LogStream,
) -> std::io::Result<(std::fs::File, tokio::sync::watch::Receiver<bool>)> {
    let (mut reader, writer) = std::io::pipe()?;
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);
    std::thread::Builder::new()
        .name(["log-", stream.as_str()].concat())
        .spawn(move || {
            let mut enc = RecordEncoder::new(stream);
            let mut buf = vec![0u8; MAX_RECORD];
            let mut out = Vec::new();
            loop {
                let n = match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                enc.push(&buf[..n], SystemTime::now(), &mut out);
                append(&log, &mut out);
            }
            enc.finish(SystemTime::now(), &mut out);
            append(&log, &mut out);
            let _ = done_tx.send(true);
        })?;
    Ok((
        std::fs::File::from(std::os::fd::OwnedFd::from(writer)),
        done_rx,
    ))
}

fn append(log: &SharedLog, out: &mut Vec<u8>) {
    if out.is_empty() {
        return;
    }
    if let Ok(mut file) = log.lock() {
        if let Err(e) = file.write_all(out) {
            tracing::warn!(error = %e, "container log write failed");
        }
    } else {
        tracing::warn!("container log lock poisoned; records dropped");
    }
    out.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_760_356_800 + secs)
    }

    fn encode(chunks: &[(&[u8], u64)], stream: LogStream) -> Vec<u8> {
        let mut enc = RecordEncoder::new(stream);
        let mut out = Vec::new();
        for (bytes, t) in chunks {
            enc.push(bytes, at(*t), &mut out);
        }
        out
    }

    #[test]
    fn lines_become_records_stamped_when_written() {
        let out = encode(&[(b"one\ntw", 0), (b"o\n", 1)], LogStream::Stdout);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "2025-10-13T12:00:00Z stdout F one\n2025-10-13T12:00:01Z stdout F two\n"
        );
    }

    #[test]
    fn a_long_line_is_split_into_partial_records() {
        let long = vec![b'x'; MAX_RECORD + 5];
        let mut line = long.clone();
        line.push(b'\n');
        let out = encode(&[(&line, 0)], LogStream::Stderr);
        let records: Vec<&[u8]> = out
            .split(|b| *b == b'\n')
            .filter(|r| !r.is_empty())
            .collect();
        assert_eq!(records.len(), 2);
        assert!(parse_record(records[0]).unwrap().partial);
        assert!(!parse_record(records[1]).unwrap().partial);
        let shown = render(&out, RenderOptions::default(), false);
        assert_eq!(shown.text.len(), long.len() + 1);
    }

    #[test]
    fn timestamps_prefix_each_line_with_its_write_time() {
        let out = encode(&[(b"hello world\n", 0), (b"bye\n", 2)], LogStream::Stdout);
        let shown = render(
            &out,
            RenderOptions {
                timestamps: true,
                ..RenderOptions::default()
            },
            false,
        );
        assert_eq!(
            shown.text,
            "2025-10-13T12:00:00Z hello world\n2025-10-13T12:00:02Z bye\n"
        );
        let plain = render(&out, RenderOptions::default(), false);
        assert_eq!(plain.text, "hello world\nbye\n");
        assert_eq!(plain.consumed, out.len());
    }

    #[test]
    fn since_drops_lines_written_before_it_and_tail_applies_first() {
        let out = encode(
            &[(b"a\n", 0), (b"b\n", 10), (b"c\n", 20)],
            LogStream::Stdout,
        );
        let since = |s, tail| RenderOptions {
            since: Some(at(s)),
            tail,
            ..RenderOptions::default()
        };
        assert_eq!(render(&out, since(10, None), false).text, "b\nc\n");
        assert_eq!(render(&out, since(21, None), false).text, "");
        assert_eq!(render(&out, since(0, Some(1)), false).text, "c\n");
    }

    #[test]
    fn an_open_line_is_not_consumed_until_it_closes_or_the_container_ends() {
        let mut enc = RecordEncoder::new(LogStream::Stdout);
        let mut out = Vec::new();
        enc.push(b"done\n", at(0), &mut out);
        let boundary = out.len();
        enc.push(b"prompt> ", at(1), &mut out);
        enc.finish(at(1), &mut out);
        let running = render(&out, RenderOptions::default(), false);
        assert_eq!(running.text, "done\n");
        assert_eq!(running.consumed, boundary);
        let ended = render(&out, RenderOptions::default(), true);
        assert_eq!(ended.text, "done\nprompt> ");
        assert_eq!(ended.consumed, out.len());
    }

    #[test]
    fn interleaved_streams_keep_their_own_lines() {
        let mut enc = RecordEncoder::new(LogStream::Stdout);
        let mut out = Vec::new();
        enc.push(&vec![b'o'; MAX_RECORD], at(0), &mut out);
        out.extend(encode(&[(b"err\n", 1)], LogStream::Stderr));
        enc.push(b"!\n", at(2), &mut out);
        let shown = render(&out, RenderOptions::default(), false);
        let mut want = "err\n".to_owned();
        want.push_str(&"o".repeat(MAX_RECORD));
        want.push_str("!\n");
        assert_eq!(shown.text, want);
    }

    #[test]
    fn a_resumed_read_prints_nothing_twice() {
        let out = encode(&[(b"a\n", 0), (b"b\n", 1)], LogStream::Stdout);
        let first = render(&out[..out.len() - 3], RenderOptions::default(), false);
        assert_eq!(first.text, "a\n");
        let second = render(&out[first.consumed..], RenderOptions::default(), false);
        assert_eq!(second.text, "b\n");
    }

    #[test]
    fn a_line_that_is_not_a_record_is_printed_verbatim() {
        let shown = render(
            b"plain text\n",
            RenderOptions {
                timestamps: true,
                since: Some(at(100)),
                tail: None,
            },
            false,
        );
        assert_eq!(shown.text, "plain text\n");
    }
}
