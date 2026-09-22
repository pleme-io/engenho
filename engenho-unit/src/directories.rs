//! `StateDirectory=` and its four siblings: created, moded, owned and
//! exported as `$STATE_DIRECTORY` & co, the way systemd does it.
//!
//! systemd's rules, reproduced here:
//!
//! * the directory hangs under a fixed base — `/var/lib`, `/var/cache`,
//!   `/var/log`, `/run`, `/etc`;
//! * a nested name (`wyoming/piper`) creates its parents, which stay
//!   `root:root 0755`; only the INNERMOST directory takes the directive's
//!   mode and the service's ownership;
//! * `ConfigurationDirectory=` is the exception: it stays root-owned;
//! * an existing directory owned by someone else is chowned RECURSIVELY —
//!   the case that matters when a service changes user, and the one that
//!   leaves a service unable to write its own state if it is skipped;
//! * `RuntimeDirectory=` is removed when the service stops, so this runner
//!   recreates it EMPTY at start (`RuntimeDirectoryPreserve=yes` keeps it),
//!   which is the state a fresh systemd start would present.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::layout::{BaseDir, Layout};
use crate::unit::{RuntimePreserve, ServiceUnit};

/// One directory to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedDirectory {
    /// Which base it hangs under.
    pub base: BaseDir,
    /// The directory: created here, and exported to the service as this —
    /// the layout has already placed it under its root.
    pub path: PathBuf,
    /// Its mode.
    pub mode: u32,
    /// Its owner, when the base is owned by the service.
    pub owner: Option<(u32, u32)>,
    /// Whether an existing directory is emptied first.
    pub recreate: bool,
}

/// Why a directory could not be created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryError {
    /// The directory.
    pub path: PathBuf,
    /// What failed.
    pub what: DirectoryFailure,
    /// What the OS said.
    pub detail: String,
}

/// Which step failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryFailure {
    /// `mkdir -p`.
    Create,
    /// Emptying a runtime directory.
    Clear,
    /// `chmod`.
    Mode,
    /// `chown`.
    Owner,
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.what {
            DirectoryFailure::Create => "could not be created",
            DirectoryFailure::Clear => "could not be emptied",
            DirectoryFailure::Mode => "could not be given its mode",
            DirectoryFailure::Owner => "could not be given its owner",
        };
        write!(f, "{} {what}: {}", self.path.display(), self.detail)
    }
}

impl std::error::Error for DirectoryError {}

/// What a unit's `*Directory=` directives ask for, with nothing created yet.
#[must_use]
pub fn plan(
    unit: &ServiceUnit,
    layout: &Layout,
    owner: Option<(u32, u32)>,
) -> Vec<PlannedDirectory> {
    unit.directories
        .iter()
        .map(|spec| PlannedDirectory {
            base: spec.base,
            path: layout.base(spec.base).join(&spec.path),
            mode: unit.directory_mode(spec.base),
            owner: owner.filter(|_| spec.base.owned_by_service()),
            recreate: spec.base == BaseDir::Runtime && unit.runtime_preserve == RuntimePreserve::No,
        })
        .collect()
}

/// Create every planned directory.
///
/// # Errors
///
/// A [`DirectoryError`] naming the directory and the step that failed.
pub fn create(planned: &[PlannedDirectory]) -> Result<(), DirectoryError> {
    for directory in planned {
        let fail = |what: DirectoryFailure, detail: String| DirectoryError {
            path: directory.path.clone(),
            what,
            detail,
        };
        if directory.recreate && directory.path.exists() {
            std::fs::remove_dir_all(&directory.path)
                .map_err(|e| fail(DirectoryFailure::Clear, e.to_string()))?;
        }
        let existed = directory.path.exists();
        std::fs::create_dir_all(&directory.path)
            .map_err(|e| fail(DirectoryFailure::Create, e.to_string()))?;
        set_mode(&directory.path, directory.mode).map_err(|e| fail(DirectoryFailure::Mode, e))?;
        if let Some((uid, gid)) = directory.owner {
            let misowned = existed && !owned_by(&directory.path, uid, gid);
            chown(&directory.path, uid, gid).map_err(|e| fail(DirectoryFailure::Owner, e))?;
            if misowned {
                // systemd chowns an existing tree whose owner changed; a
                // service that cannot write its own state directory is the
                // failure this prevents.
                chown_tree(&directory.path, uid, gid).map_err(|(path, detail)| DirectoryError {
                    path,
                    what: DirectoryFailure::Owner,
                    detail,
                })?;
            }
        }
    }
    Ok(())
}

/// The `*_DIRECTORY` environment variables the planned directories export:
/// every path of a base, colon-separated, as systemd exports them.
#[must_use]
pub fn environment(planned: &[PlannedDirectory]) -> BTreeMap<String, String> {
    let mut by_base: BTreeMap<BaseDir, Vec<String>> = BTreeMap::new();
    for directory in planned {
        by_base
            .entry(directory.base)
            .or_default()
            .push(directory.path.to_string_lossy().into_owned());
    }
    by_base
        .into_iter()
        .map(|(base, paths)| (base.env_var().to_string(), paths.join(":")))
        .collect()
}

fn set_mode(path: &Path, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|e| e.to_string())
}

fn chown(path: &Path, uid: u32, gid: u32) -> Result<(), String> {
    std::os::unix::fs::chown(path, Some(uid), Some(gid)).map_err(|e| e.to_string())
}

fn owned_by(path: &Path, uid: u32, gid: u32) -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).is_ok_and(|m| m.uid() == uid && m.gid() == gid)
}

/// Chown every entry below `root`, following no symlink.
fn chown_tree(root: &Path, uid: u32, gid: u32) -> Result<(), (PathBuf, String)> {
    let entries = std::fs::read_dir(root).map_err(|e| (root.to_path_buf(), e.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|e| (root.to_path_buf(), e.to_string()))?;
        let path = entry.path();
        std::os::unix::fs::lchown(&path, Some(uid), Some(gid))
            .map_err(|e| (path.clone(), e.to_string()))?;
        let kind = entry
            .file_type()
            .map_err(|e| (path.clone(), e.to_string()))?;
        if kind.is_dir() {
            chown_tree(&path, uid, gid)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::HostRoot;
    use crate::specifier::{AccountFacts, Context, HostFacts};
    use crate::unit::UnitFile;
    use std::os::unix::fs::PermissionsExt;

    fn unit_with(text: &str) -> ServiceUnit {
        let layout = Layout::new(HostRoot::system());
        let host = HostFacts::default();
        let account = AccountFacts::root();
        let file = UnitFile::parse(Path::new("/x/t.service"), text).unwrap();
        let ctx = Context {
            unit: &file.name,
            fragment: Some(&file.path),
            account: Some(&account),
            layout: &layout,
            host: &host,
        };
        file.service(&ctx).unwrap()
    }

    #[test]
    fn every_base_is_planned_under_its_own_root_with_its_mode() {
        let unit = unit_with(
            "[Service]\nExecStart=/bin/a\n\
             StateDirectory=wyoming/piper\nStateDirectoryMode=0750\n\
             CacheDirectory=frigate frigate/model_cache\n\
             LogsDirectory=l\nRuntimeDirectory=zwave-js\nConfigurationDirectory=c\n",
        );
        let layout = Layout::new(HostRoot::at("/tmp/root"));
        let planned = plan(&unit, &layout, Some((7, 8)));
        assert_eq!(planned.len(), 6);
        assert_eq!(
            planned[0].path,
            PathBuf::from("/tmp/root/var/lib/wyoming/piper")
        );
        assert_eq!(planned[0].mode, 0o750);
        assert_eq!(planned[0].owner, Some((7, 8)));
        assert_eq!(
            planned[1].path,
            PathBuf::from("/tmp/root/var/cache/frigate")
        );
        assert_eq!(
            planned[1].mode, 0o755,
            "no CacheDirectoryMode: systemd's 0755"
        );
        assert_eq!(planned[4].path, PathBuf::from("/tmp/root/run/zwave-js"));
        assert!(planned[4].recreate, "a runtime directory is fresh at start");
        assert_eq!(
            planned[5].owner, None,
            "ConfigurationDirectory= stays root-owned, as systemd leaves it"
        );

        // The service is told the paths that were actually created.
        let env = environment(&planned);
        assert_eq!(env["STATE_DIRECTORY"], "/tmp/root/var/lib/wyoming/piper");
        assert_eq!(
            env["CACHE_DIRECTORY"],
            "/tmp/root/var/cache/frigate:/tmp/root/var/cache/frigate/model_cache"
        );
        assert_eq!(env["RUNTIME_DIRECTORY"], "/tmp/root/run/zwave-js");
        assert_eq!(env["CONFIGURATION_DIRECTORY"], "/tmp/root/etc/c");
        assert!(!env.contains_key("LOGS_DIRECTORY_MODE"));

        // On a real node the root is `/`, so they are systemd's own paths.
        let system = Layout::new(HostRoot::system());
        let planned = plan(&unit, &system, Some((7, 8)));
        assert_eq!(planned[0].path, PathBuf::from("/var/lib/wyoming/piper"));
        assert_eq!(environment(&planned)["RUNTIME_DIRECTORY"], "/run/zwave-js");
    }

    #[test]
    fn creation_makes_parents_and_modes_only_the_innermost() {
        let root = tempfile::tempdir().unwrap();
        let unit = unit_with(
            "[Service]\nExecStart=/bin/a\nStateDirectory=wyoming/piper\nStateDirectoryMode=0700\n",
        );
        let layout = Layout::new(HostRoot::at(root.path()));
        let planned = plan(&unit, &layout, None);
        create(&planned).unwrap();
        let inner = root.path().join("var/lib/wyoming/piper");
        assert!(inner.is_dir());
        assert_eq!(
            std::fs::metadata(&inner).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let parent = root.path().join("var/lib/wyoming");
        assert_ne!(
            std::fs::metadata(parent).unwrap().permissions().mode() & 0o777,
            0o700,
            "the mode applies to the innermost directory only"
        );
    }

    #[test]
    fn creating_twice_keeps_state_and_empties_only_the_runtime_directory() {
        let root = tempfile::tempdir().unwrap();
        let unit = unit_with("[Service]\nExecStart=/bin/a\nStateDirectory=s\nRuntimeDirectory=r\n");
        let layout = Layout::new(HostRoot::at(root.path()));
        let planned = plan(&unit, &layout, None);
        create(&planned).unwrap();
        std::fs::write(root.path().join("var/lib/s/keep"), "x").unwrap();
        std::fs::write(root.path().join("run/r/stale"), "x").unwrap();
        create(&planned).unwrap();
        assert!(
            root.path().join("var/lib/s/keep").exists(),
            "state survives a restart"
        );
        assert!(
            !root.path().join("run/r/stale").exists(),
            "a runtime directory is what a fresh systemd start would present: empty"
        );
    }

    #[test]
    fn preserve_yes_keeps_the_runtime_directory() {
        let root = tempfile::tempdir().unwrap();
        let unit = unit_with(
            "[Service]\nExecStart=/bin/a\nRuntimeDirectory=r\nRuntimeDirectoryPreserve=yes\n",
        );
        let layout = Layout::new(HostRoot::at(root.path()));
        let planned = plan(&unit, &layout, None);
        create(&planned).unwrap();
        std::fs::write(root.path().join("run/r/keep"), "x").unwrap();
        create(&planned).unwrap();
        assert!(root.path().join("run/r/keep").exists());
    }
}
