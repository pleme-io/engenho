use engenho_kubelet::cgroup::{CgroupFs, HostFs};
use engenho_substrate::HostRoot;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct DirectoryCgroupfs {
    host: HostFs,
    root: HostRoot,
}

impl DirectoryCgroupfs {
    pub fn at(root: &Path) -> Self {
        Self {
            host: HostFs::at(HostRoot::at(root)),
            root: HostRoot::at(root),
        }
    }
}

impl CgroupFs for DirectoryCgroupfs {
    fn read(&self, path: &Path) -> std::io::Result<String> {
        self.host.read(path)
    }
    fn write(&self, path: &Path, value: &str) -> std::io::Result<()> {
        self.host.write(path, value)
    }
    fn create_dir(&self, path: &Path) -> std::io::Result<()> {
        self.host.create_dir(path)
    }
    fn remove_dir(&self, path: &Path) -> std::io::Result<()> {
        let dir = self.root.resolve(path);
        let members = std::fs::read_to_string(dir.join("cgroup.procs")).unwrap_or_default();
        let live = members
            .split_whitespace()
            .filter_map(|pid| pid.parse::<i32>().ok())
            .filter_map(rustix::process::Pid::from_raw)
            .any(|pid| rustix::process::test_kill_process(pid).is_ok());
        let children = std::fs::read_dir(&dir)?
            .flatten()
            .any(|entry| entry.path().is_dir());
        if live || children {
            return Err(rustix::io::Errno::BUSY.into());
        }
        std::fs::remove_dir_all(dir)
    }
    fn subdirs(&self, path: &Path) -> std::io::Result<Vec<String>> {
        self.host.subdirs(path)
    }
    fn writable(&self, path: &Path) -> bool {
        self.host.writable(path)
    }
    fn delegation_marked(&self, path: &Path) -> std::io::Result<bool> {
        self.host.delegation_marked(path)
    }
    fn open_procs(&self, path: &Path) -> std::io::Result<std::fs::File> {
        self.host.open_procs(path)
    }
}

pub fn delegated_unit(root: &Path) -> PathBuf {
    let unit = root.join("sys/fs/cgroup/test.slice/engenho.service");
    std::fs::create_dir_all(&unit).unwrap();
    std::fs::create_dir_all(root.join("proc/self")).unwrap();
    std::fs::write(
        root.join("proc/self/cgroup"),
        "0::/test.slice/engenho.service\n",
    )
    .unwrap();
    std::fs::write(unit.join("cgroup.controllers"), "cpu io memory pids").unwrap();
    std::fs::write(unit.join("cgroup.subtree_control"), "").unwrap();
    std::fs::write(unit.join("cgroup.procs"), std::process::id().to_string()).unwrap();
    rustix::fs::setxattr(
        unit.as_path(),
        "user.delegate",
        b"1",
        rustix::fs::XattrFlags::empty(),
    )
    .unwrap();
    unit
}
