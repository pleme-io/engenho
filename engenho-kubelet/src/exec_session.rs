//! THE EXEC SESSION — what actually happens between an upgraded socket and
//! a process inside a container.
//!
//! [`crate::exec_channel`] owns the *framing* (which byte prefixes which
//! stream). This module owns the *session*: parsing what the client asked
//! for, deciding whether engenho can serve it, and rendering the exact
//! sequence of frames that answers it.
//!
//! ★ THE PUMP IS A PURE FUNCTION OVER FRAMES, NOT A SOCKET LOOP. Every
//! decision here — refusals, which channels are written, what the
//! terminating status says — is computed by [`plan`] and [`session_frames`],
//! which take values and return values. The route is then a thin adapter.
//! A stream protocol tested only through a live socket is tested only in
//! the shapes someone remembered to open a socket for, and its failures are
//! hangs rather than assertions.
//!
//! ★ WHAT ENGENHO CAN AND CANNOT SERVE TODAY, AND WHY THE LINE IS HERE.
//! The runtime seam is [`crate::backend::ContainerRuntime::exec`], which is
//! BATCH: it runs argv to completion and hands back a captured
//! [`crate::backend::ExecOutcome`]. That is a complete answer for
//! `kubectl exec <pod> -- <cmd>`, which is the overwhelming majority of
//! real exec traffic. It is NOT an answer for `-i` or `-t`: an interactive
//! session needs the process's stdin wired to the socket while it runs, and
//! no amount of framing produces that from a batch call.
//!
//! So `stdin=true` and `tty=true` are REFUSED, with a reason, at the start.
//! The alternative — accept the upgrade and serve batch semantics anyway —
//! gives the user a shell prompt that never echoes and never exits, which
//! reads as a broken cluster rather than a missing feature. A refusal a
//! human can read is strictly better than a hang, and it is the honest
//! statement of where the seam currently stops.

use crate::backend::ExecOutcome;
use crate::exec_channel::{Channel, encode, exit_status};

/// Why engenho will not serve this exec.
///
/// Each variant names a *capability*, not a syntax error, so the message
/// tells an operator what engenho cannot do rather than what they typed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecRefusal {
    /// No `command` parameter. Upstream requires at least one.
    #[error("no command given: exec needs at least one `command` parameter")]
    NoCommand,
    /// `stdin=true` — an interactive session.
    #[error(
        "interactive exec (stdin) is not served: the runtime seam runs a \
         command to completion and returns its output, so there is no \
         running process to write stdin to"
    )]
    StdinUnsupported,
    /// `tty=true` — a pty-backed session.
    #[error(
        "tty exec is not served: allocating a pty requires the streaming \
         runtime seam, which is not wired yet"
    )]
    TtyUnsupported,
    /// The client did not offer `v5.channel.k8s.io`.
    #[error("client offered no supported stream subprotocol (need {SUBPROTOCOL})")]
    SubprotocolUnsupported,
}

const SUBPROTOCOL: &str = crate::exec_channel::SUBPROTOCOL_V5;

/// What the client asked for, as parsed off the query string.
///
/// `command` repeats — `?command=sh&command=-c&command=ls` — which
/// `serde_urlencoded` cannot express, so this is parsed by hand rather
/// than derived. A derive here would silently keep only the LAST
/// occurrence and run `ls` with no `sh -c`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecQuery {
    pub command: Vec<String>,
    pub container: Option<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

impl ExecQuery {
    /// Parse a raw query string.
    ///
    /// Unknown parameters are ignored: upstream adds them over time and a
    /// kubelet that 400s on an unrecognised one breaks on a client upgrade.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        let mut q = Self::default();
        for pair in raw.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = percent_decode(v);
            let on = v == "true" || v == "1";
            match percent_decode(k).as_str() {
                "command" => q.command.push(v),
                "container" => q.container = Some(v),
                "stdin" | "input" => q.stdin = on,
                "stdout" | "output" => q.stdout = on,
                "stderr" | "error" => q.stderr = on,
                "tty" => q.tty = on,
                _ => {}
            }
        }
        q
    }
}

/// Decode one `application/x-www-form-urlencoded` component.
///
/// Hand-written rather than pulled from a crate: the whole grammar is
/// `+` → space and `%XX` → byte, this crate needs no other URL handling,
/// and a dependency added for fifteen lines is a dependency to keep
/// current forever. An invalid escape is kept VERBATIM rather than
/// dropped — a command word silently losing a `%` would run a different
/// program than the operator typed.
#[must_use]
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(b) => {
                        out.push(b);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A session engenho has agreed to serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecPlan {
    /// argv to run inside the container.
    pub argv: Vec<String>,
    /// Container name, when the client named one.
    pub container: Option<String>,
    /// Whether the client wants the stdout channel.
    pub want_stdout: bool,
    /// Whether the client wants the stderr channel.
    pub want_stderr: bool,
}

/// Decide whether this request can be served, and how.
///
/// # Errors
///
/// [`ExecRefusal`] naming the capability engenho does not have.
pub fn plan(q: &ExecQuery, offered_subprotocols: &str) -> Result<ExecPlan, ExecRefusal> {
    if crate::exec_channel::negotiate(offered_subprotocols).is_none() {
        return Err(ExecRefusal::SubprotocolUnsupported);
    }
    if q.command.is_empty() {
        return Err(ExecRefusal::NoCommand);
    }
    // Checked BEFORE tty on purpose: `kubectl exec -it` sets both, and
    // stdin is the more fundamental of the two — a user told "no stdin"
    // learns the real shape of the limit, where "no tty" invites them to
    // retry with -i alone and hit the same wall.
    if q.stdin {
        return Err(ExecRefusal::StdinUnsupported);
    }
    if q.tty {
        return Err(ExecRefusal::TtyUnsupported);
    }
    Ok(ExecPlan {
        argv: q.command.clone(),
        container: q.container.clone(),
        // Upstream's default when a client asks for neither is to send
        // both: a client that requested no output channel at all still
        // wants the exit code, and sending output it did not ask for is
        // harmless where withholding output it did is a silent truncation.
        want_stdout: q.stdout || !q.stderr,
        want_stderr: q.stderr || !q.stdout,
    })
}

/// The exact frames a completed exec sends, in order.
///
/// The terminating status frame is ALWAYS last and ALWAYS present — it is
/// what kubectl reads to recover the exit code, and a session that closes
/// without one makes `kubectl exec; echo $?` report a failure for a command
/// that succeeded.
#[must_use]
pub fn session_frames(plan: &ExecPlan, outcome: &ExecOutcome) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    if plan.want_stdout && !outcome.stdout.is_empty() {
        frames.push(encode(Channel::Stdout, outcome.stdout.as_bytes()));
    }
    if plan.want_stderr && !outcome.stderr.is_empty() {
        frames.push(encode(Channel::Stderr, outcome.stderr.as_bytes()));
    }
    let status = exit_status(outcome.exit_code).to_string();
    frames.push(encode(Channel::Error, status.as_bytes()));
    frames
}

/// The single error-channel frame that reports a runtime failure.
///
/// A backend that could not RUN the command is distinct from a command
/// that ran and failed: it carries no exit code, so rendering it as
/// `exit_status(1)` would tell kubectl the program ran and returned 1.
#[must_use]
pub fn backend_failure_frame(reason: &str) -> Vec<u8> {
    let status = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": reason,
        "reason": "InternalError",
    });
    encode(Channel::Error, status.to_string().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(raw: &str) -> ExecQuery {
        ExecQuery::parse(raw)
    }

    #[test]
    fn a_repeated_command_parameter_keeps_every_word_in_order() {
        // The defect a serde derive would have shipped: keeping only the
        // last occurrence turns `sh -c "ls /"` into `/`.
        let parsed = q("command=sh&command=-c&command=ls+%2F");
        assert_eq!(parsed.command, vec!["sh", "-c", "ls /"]);
    }

    #[test]
    fn percent_decoding_handles_plus_escapes_and_malformed_input() {
        assert_eq!(percent_decode("ls+%2Ftmp"), "ls /tmp");
        assert_eq!(percent_decode("%E2%9C%93"), "\u{2713}");
        // A truncated or non-hex escape is KEPT, never dropped: silently
        // deleting a character changes which program runs.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("a%zz"), "a%zz");
    }

    #[test]
    fn an_unknown_parameter_is_ignored_not_rejected() {
        let parsed = q("command=ls&someFutureFlag=true");
        assert_eq!(parsed.command, vec!["ls"]);
    }

    #[test]
    fn interactive_and_tty_are_refused_with_a_reason_each() {
        let sp = SUBPROTOCOL;
        let stdin = plan(&q("command=sh&stdin=true"), sp).unwrap_err();
        assert_eq!(stdin, ExecRefusal::StdinUnsupported);
        assert!(stdin.to_string().contains("to completion"), "{stdin}");

        let tty = plan(&q("command=sh&tty=true"), sp).unwrap_err();
        assert_eq!(tty, ExecRefusal::TtyUnsupported);
    }

    #[test]
    fn dash_it_reports_the_stdin_limit_not_the_tty_one() {
        // `kubectl exec -it` sets both. Reporting tty first would send the
        // user to retry with -i alone and hit the identical wall.
        assert_eq!(
            plan(&q("command=sh&stdin=true&tty=true"), SUBPROTOCOL).unwrap_err(),
            ExecRefusal::StdinUnsupported
        );
    }

    #[test]
    fn a_client_offering_no_v5_is_refused_before_anything_else() {
        // Refused even though the request is otherwise perfectly valid:
        // answering a v4 client with v5 error framing yields a session
        // that works until the command fails.
        assert_eq!(
            plan(&q("command=ls"), "v4.channel.k8s.io").unwrap_err(),
            ExecRefusal::SubprotocolUnsupported
        );
    }

    #[test]
    fn an_empty_command_is_refused() {
        assert_eq!(
            plan(&q("stdout=true"), SUBPROTOCOL).unwrap_err(),
            ExecRefusal::NoCommand
        );
    }

    #[test]
    fn asking_for_neither_channel_still_gets_both() {
        let p = plan(&q("command=ls"), SUBPROTOCOL).unwrap();
        assert!(p.want_stdout && p.want_stderr);
    }

    #[test]
    fn asking_for_only_stderr_suppresses_stdout() {
        let p = plan(&q("command=ls&stderr=true"), SUBPROTOCOL).unwrap();
        assert!(!p.want_stdout, "stdout was not requested");
        assert!(p.want_stderr);

        let out = session_frames(
            &p,
            &ExecOutcome {
                exit_code: 0,
                stdout: "hidden".into(),
                stderr: "shown".into(),
            },
        );
        let joined: Vec<u8> = out.concat();
        assert!(!joined.windows(6).any(|w| w == b"hidden"));
        assert!(joined.windows(5).any(|w| w == b"shown"));
    }

    #[test]
    fn the_status_frame_is_always_last_and_always_present() {
        let p = plan(&q("command=true"), SUBPROTOCOL).unwrap();
        // Even with no output at all.
        let frames = session_frames(&p, &ExecOutcome::success());
        assert_eq!(frames.len(), 1, "only the status frame");
        assert_eq!(frames[0][0], Channel::Error.as_byte());

        let frames = session_frames(
            &p,
            &ExecOutcome {
                exit_code: 0,
                stdout: "a".into(),
                stderr: "b".into(),
            },
        );
        assert_eq!(frames.len(), 3);
        assert_eq!(frames.last().unwrap()[0], Channel::Error.as_byte());
    }

    #[test]
    fn a_non_zero_exit_carries_the_code_kubectl_reads() {
        let p = plan(&q("command=false"), SUBPROTOCOL).unwrap();
        let frames = session_frames(&p, &ExecOutcome::failure(42));
        let status = frames.last().unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&status[1..]).expect("status frame is JSON");
        assert_eq!(body["status"], "Failure");
        // The CAUSE is what kubectl parses; a Failure without it makes
        // every failing command report exit 1.
        let causes = body["details"]["causes"].as_array().expect("causes");
        assert!(
            causes.iter().any(|c| c["message"] == "42"),
            "exit code reaches the cause: {body}"
        );
    }

    #[test]
    fn a_backend_failure_is_not_rendered_as_an_exit_code() {
        // The distinction that matters: "could not run it" must never look
        // like "it ran and returned 1".
        let frame = backend_failure_frame("no such container");
        let body: serde_json::Value = serde_json::from_slice(&frame[1..]).expect("JSON");
        assert_eq!(body["reason"], "InternalError");
        assert!(body.get("details").is_none(), "no exit-code cause: {body}");
        assert!(body["message"].as_str().unwrap().contains("no such"));
    }
}
