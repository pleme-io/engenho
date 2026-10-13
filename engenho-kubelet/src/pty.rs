//! A pseudo-terminal for `kubectl exec -t` and `tty: true` containers.
//!
//! ★ CONTAIN THE C. The whole pty surface is `rustix` (`openpt`, `grantpt`,
//! `unlockpt`, `ptsname`, `TIOCSWINSZ`), which speaks the syscalls directly:
//! no `-sys` crate, no bindgen, no libutil `forkpty`. The one thing a safe
//! API cannot express is code that runs in the child between `fork` and
//! `exec` — making the terminal the child's controlling terminal — so that is
//! the single `#[allow(unsafe_code)]` seam in this module, and it calls only
//! two async-signal-safe syscalls.
//!
//! ★ WHY A CONTROLLING TERMINAL AND NOT JUST THREE FDS. A shell given the
//! pty slave as stdin/stdout/stderr believes it is interactive, but without
//! `setsid` + `TIOCSCTTY` the slave is nobody's controlling terminal: `^C`
//! raises no SIGINT and a resize raises no SIGWINCH, so `top` never redraws
//! and an interrupted command cannot be interrupted. Both are what `-t` is
//! FOR.

use std::os::fd::{AsFd, OwnedFd};

use crate::exec_channel::TerminalSize;

/// A freshly opened pseudo-terminal pair.
#[derive(Debug)]
pub struct Pty {
    master: OwnedFd,
    slave: OwnedFd,
}

/// The master side, kept by the parent to resize the terminal.
#[derive(Debug)]
pub struct PtyResizer(OwnedFd);

impl PtyResizer {
    /// Set the terminal's size; the kernel raises SIGWINCH in the foreground
    /// process group.
    ///
    /// # Errors
    ///
    /// The `TIOCSWINSZ` failure, typically because the session ended.
    pub fn resize(&self, size: TerminalSize) -> std::io::Result<()> {
        rustix::termios::tcsetwinsize(
            &self.0,
            rustix::termios::Winsize {
                ws_row: size.height,
                ws_col: size.width,
                ws_xpixel: 0,
                ws_ypixel: 0,
            },
        )?;
        Ok(())
    }
}

/// The parent's three handles on a running pty: output, input, resize.
#[derive(Debug)]
pub struct PtyMaster {
    /// The terminal's output (the child's stdout and stderr, merged — that is
    /// what a terminal is).
    pub reader: std::fs::File,
    /// The terminal's input.
    pub writer: std::fs::File,
    /// Resize handle.
    pub resizer: PtyResizer,
}

impl Pty {
    /// Open a pty pair. Both ends are close-on-exec; the child receives the
    /// slave only as its stdio.
    ///
    /// # Errors
    ///
    /// Any failing step of `posix_openpt` → `grantpt` → `unlockpt` →
    /// `ptsname` → `open`.
    pub fn open() -> std::io::Result<Self> {
        use rustix::fs::{Mode, OFlags};
        use rustix::pty::OpenptFlags;
        let master = rustix::pty::openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
        rustix::pty::grantpt(&master)?;
        rustix::pty::unlockpt(&master)?;
        let name = rustix::pty::ptsname(&master, Vec::new())?;
        let slave = rustix::fs::open(
            name.as_c_str(),
            OFlags::RDWR | OFlags::NOCTTY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        rustix::io::fcntl_setfd(&master, rustix::io::FdFlags::CLOEXEC)?;
        Ok(Self { master, slave })
    }

    /// Make the slave `cmd`'s stdin, stdout, stderr and controlling terminal.
    ///
    /// # Errors
    ///
    /// Duplicating the slave descriptor failed.
    pub fn wire(&self, cmd: &mut tokio::process::Command) -> std::io::Result<()> {
        let dup = || -> std::io::Result<std::process::Stdio> {
            Ok(std::process::Stdio::from(self.slave.as_fd().try_clone_to_owned()?))
        };
        cmd.stdin(dup()?);
        cmd.stdout(dup()?);
        cmd.stderr(dup()?);
        // SAFETY: the closure runs in the forked child before exec, after std
        // has dup2'd the slave onto fds 0-2. It calls only `setsid` and the
        // `TIOCSCTTY` ioctl, both async-signal-safe, allocates nothing and
        // touches no lock. Fd 0 is open for the closure's whole run (std
        // installed it), which is what `borrow_raw` requires.
        #[allow(unsafe_code)]
        unsafe {
            cmd.pre_exec(|| {
                rustix::process::setsid()?;
                rustix::process::ioctl_tiocsctty(std::os::fd::BorrowedFd::borrow_raw(0))?;
                Ok(())
            });
        }
        Ok(())
    }

    /// Release the slave (the child holds its own copies) and hand back the
    /// master's handles.
    ///
    /// The slave MUST be closed in the parent: the master reads EOF only once
    /// every slave descriptor is gone, so a parent that kept one would never
    /// see its child's terminal close.
    ///
    /// # Errors
    ///
    /// Duplicating the master descriptor failed.
    pub fn into_master(self) -> std::io::Result<PtyMaster> {
        drop(self.slave);
        let reader = std::fs::File::from(self.master.try_clone()?);
        let writer = std::fs::File::from(self.master.try_clone()?);
        Ok(PtyMaster {
            reader,
            writer,
            resizer: PtyResizer(self.master),
        })
    }

    /// Set the initial size before the child starts, so the first frame a
    /// full-screen program draws already fits.
    ///
    /// # Errors
    ///
    /// The `TIOCSWINSZ` failure.
    pub fn resize(&self, size: TerminalSize) -> std::io::Result<()> {
        PtyResizer(self.master.try_clone()?).resize(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test]
    async fn a_child_on_the_pty_sees_a_terminal_of_the_size_it_was_given() {
        let pty = Pty::open().expect("open");
        pty.resize(TerminalSize {
            width: 91,
            height: 17,
        })
        .expect("size");
        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.args(["-c", "test -t 0 && stty size"]);
        pty.wire(&mut cmd).expect("wire");
        let mut child = cmd.spawn().expect("spawn");
        drop(cmd);
        let master = pty.into_master().expect("master");
        let mut reader = tokio::fs::File::from_std(master.reader);
        let mut out = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
            }
        }
        let status = child.wait().await.expect("wait");
        assert!(status.success(), "stdin is a tty: {status:?}");
        assert_eq!(String::from_utf8_lossy(&out).trim(), "17 91");
    }

    #[tokio::test]
    async fn input_written_to_the_master_reaches_the_child() {
        let pty = Pty::open().expect("open");
        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.args(["-c", "read line; echo got:$line"]);
        pty.wire(&mut cmd).expect("wire");
        let mut child = cmd.spawn().expect("spawn");
        drop(cmd);
        let master = pty.into_master().expect("master");
        let mut writer = tokio::fs::File::from_std(master.writer);
        writer.write_all(b"ping\n").await.expect("write");
        writer.flush().await.expect("flush");
        let mut reader = tokio::fs::File::from_std(master.reader);
        let mut out = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
            }
        }
        child.wait().await.expect("wait");
        assert!(
            String::from_utf8_lossy(&out).contains("got:ping"),
            "{}",
            String::from_utf8_lossy(&out)
        );
    }
}
