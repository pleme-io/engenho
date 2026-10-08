//! Where a host-touching component looks on disk — the seam that makes it
//! testable.
//!
//! Absolute host paths (`/var/lib`, `/etc/passwd`, `/sys/fs/cgroup`,
//! `/proc/self/cgroup`) are constants of the machine. Code that hard-codes
//! them can only be tested as root on a machine it is allowed to write to,
//! which means it is not tested. Such code resolves every path through a
//! [`HostRoot`] instead: `/` in production, a temporary directory in tests,
//! and the same code runs against both.

use std::path::{Path, PathBuf};

/// The filesystem a component acts on. `/` on a node; a temporary directory
/// in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRoot(PathBuf);

impl Default for HostRoot {
    fn default() -> Self {
        Self::system()
    }
}

impl HostRoot {
    /// The real filesystem.
    #[must_use]
    pub fn system() -> Self {
        Self(PathBuf::from("/"))
    }

    /// A filesystem rooted at `root` — for tests.
    #[must_use]
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self(root.into())
    }

    /// Where an absolute host path lands under this root.
    ///
    /// A relative path is returned unchanged: the caller owns that case.
    #[must_use]
    pub fn resolve(&self, path: &Path) -> PathBuf {
        match path.strip_prefix("/") {
            Ok(relative) => self.0.join(relative),
            Err(_) => path.to_path_buf(),
        }
    }

    /// The root itself.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absolute_path_lands_under_the_root() {
        let root = HostRoot::at("/tmp/x");
        assert_eq!(
            root.resolve(Path::new("/sys/fs/cgroup/cgroup.procs")),
            PathBuf::from("/tmp/x/sys/fs/cgroup/cgroup.procs")
        );
    }

    #[test]
    fn the_system_root_is_a_no_op() {
        assert_eq!(
            HostRoot::system().resolve(Path::new("/proc/self/cgroup")),
            PathBuf::from("/proc/self/cgroup")
        );
    }

    #[test]
    fn a_relative_path_is_left_to_the_caller() {
        assert_eq!(
            HostRoot::at("/tmp/x").resolve(Path::new("bin/sh")),
            PathBuf::from("bin/sh")
        );
    }
}
