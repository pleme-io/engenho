use engenho_substrate::closed_enum;

closed_enum! {
    #[named]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Flavour {
        Upstream,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KubeVersion {
    major: &'static str,
    minor: &'static str,
    git_version: &'static str,
}

impl KubeVersion {
    const fn new(major: &'static str, minor: &'static str, git_version: &'static str) -> Self {
        assert!(
            spells_release(git_version.as_bytes(), major.as_bytes(), minor.as_bytes()),
            "git_version must read v<major>.<minor>.<patch>"
        );
        Self {
            major,
            minor,
            git_version,
        }
    }

    #[must_use]
    pub const fn major(self) -> &'static str {
        self.major
    }

    #[must_use]
    pub const fn minor(self) -> &'static str {
        self.minor
    }

    #[must_use]
    pub const fn git_version(self) -> &'static str {
        self.git_version
    }
}

const fn spells_release(git: &[u8], major: &[u8], minor: &[u8]) -> bool {
    if git.is_empty() || git[0] != b'v' {
        return false;
    }
    let Some(at) = digits_then(git, 1, major, b'.') else {
        return false;
    };
    let Some(at) = digits_then(git, at, minor, b'.') else {
        return false;
    };
    if at >= git.len() {
        return false;
    }
    let mut i = at;
    while i < git.len() {
        if !git[i].is_ascii_digit() {
            return false;
        }
        i += 1;
    }
    true
}

const fn digits_then(git: &[u8], at: usize, part: &[u8], sep: u8) -> Option<usize> {
    if part.is_empty() || at + part.len() >= git.len() {
        return None;
    }
    let mut i = 0;
    while i < part.len() {
        if !part[i].is_ascii_digit() || git[at + i] != part[i] {
            return None;
        }
        i += 1;
    }
    if git[at + part.len()] == sep {
        Some(at + part.len() + 1)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApiFace {
    version: KubeVersion,
    flavour: Flavour,
}

impl ApiFace {
    pub const VENDORED: Self = Self {
        version: KubeVersion::new("1", "34", "v1.34.0"),
        flavour: Flavour::Upstream,
    };

    pub const REGISTRY: &'static [Self] = &[Self::VENDORED];

    #[must_use]
    pub const fn version(self) -> KubeVersion {
        self.version
    }

    #[must_use]
    pub const fn flavour(self) -> Flavour {
        self.flavour
    }
}

impl Default for ApiFace {
    fn default() -> Self {
        Self::VENDORED
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiFace, Flavour, KubeVersion, spells_release};

    #[test]
    fn the_default_face_is_the_vendored_surface() {
        assert_eq!(ApiFace::default(), ApiFace::VENDORED);
        assert_eq!(ApiFace::VENDORED.version().git_version(), "v1.34.0");
        assert_eq!(ApiFace::VENDORED.version().major(), "1");
        assert_eq!(ApiFace::VENDORED.version().minor(), "34");
        assert_eq!(ApiFace::VENDORED.flavour(), Flavour::Upstream);
        assert_eq!(Flavour::Upstream.name(), "Upstream");
    }

    #[test]
    fn the_crate_constants_are_the_vendored_face() {
        assert_eq!(
            crate::KUBE_VERSION,
            ApiFace::VENDORED.version().git_version()
        );
        assert_eq!(
            crate::KUBE_VERSION_MAJOR,
            ApiFace::VENDORED.version().major()
        );
        assert_eq!(
            crate::KUBE_VERSION_MINOR,
            ApiFace::VENDORED.version().minor()
        );
    }

    #[test]
    fn the_registry_holds_the_vendored_face_once() {
        assert_eq!(
            ApiFace::REGISTRY
                .iter()
                .filter(|f| **f == ApiFace::VENDORED)
                .count(),
            1
        );
    }

    #[test]
    fn every_registered_face_has_a_vendored_openapi_surface() {
        let vendor = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/openapi");
        for face in ApiFace::REGISTRY {
            let dir = vendor.join(face.version().git_version());
            assert!(
                dir.is_dir(),
                "{face:?} names {} but no schemas are vendored there",
                dir.display()
            );
        }
    }

    #[test]
    fn a_version_spells_its_own_components() {
        for (git, major, minor) in [
            ("v1.34.0", "1", "34"),
            ("v1.34.12", "1", "34"),
            ("v2.0.3", "2", "0"),
        ] {
            assert!(
                spells_release(git.as_bytes(), major.as_bytes(), minor.as_bytes()),
                "{git} {major} {minor}"
            );
            let v = KubeVersion::new(major, minor, git);
            assert_eq!([v.major(), v.minor(), v.git_version()], [major, minor, git]);
        }
    }

    #[test]
    fn a_version_that_disagrees_with_its_components_is_refused() {
        for (git, major, minor) in [
            ("v1.35.0", "1", "34"),
            ("v1.34", "1", "34"),
            ("v1.34.", "1", "34"),
            ("1.34.0", "1", "34"),
            ("v1.34.0-rc1", "1", "34"),
            ("v11.34.0", "1", "34"),
            ("v1.340.0", "1", "34"),
            ("v1.34.0", "", "34"),
            ("v.34.0", "", "34"),
            ("", "1", "34"),
        ] {
            assert!(
                !spells_release(git.as_bytes(), major.as_bytes(), minor.as_bytes()),
                "{git:?} {major:?} {minor:?} was accepted"
            );
        }
        assert!(
            std::panic::catch_unwind(|| KubeVersion::new("1", "34", "v1.35.0")).is_err(),
            "a disagreeing version was built"
        );
    }
}
