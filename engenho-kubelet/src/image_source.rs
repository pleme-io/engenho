//! Where a container's executable bytes come from.
//!
//! ## Why this is a type and not a string
//!
//! `ContainerSpec::image` is a `String`, and every backend so far has handed
//! it straight to a Linux container runtime. A backend that runs workloads as
//! NATIVE host processes cannot do that: there is no Linux userland to unpack
//! an OCI layer into. It needs a realised Nix closure.
//!
//! The dangerous version of this is a backend that *tries* the OCI path, fails,
//! and falls back — or worse, silently starts nothing and reports success. So
//! the distinction is made once, at parse time, and the native backend refuses
//! [`ImageSource::Oci`] with a typed error naming exactly why. A pod whose image
//! is an upstream OCI reference **cannot be placed** on a native node; it is not
//! placed-and-broken.
//!
//! That is what makes "everything is distroless, wrapped in Nix" an enforced
//! property rather than a convention someone remembers.
//!
//! ## The `nix:` form
//!
//! ```text
//! nix:/nix/store/00pjg4wzlg32ihqpcfw7jcm5q99r55vk-postgresql-16.15
//! ```
//!
//! A realised store path, absolute, under the store root. The prefix checks
//! below are not ceremony: `nix:/tmp/whatever` would name a mutable path that
//! nothing guarantees the content of, which discards the entire reason for
//! using a closure — so it is refused rather than accepted-and-hoped-about.

use std::path::{Path, PathBuf};

/// The Nix store root. A closure reference that does not live under this is
/// not content-addressed, whatever it is.
const STORE_ROOT: &str = "/nix/store/";

/// The scheme that marks a closure reference.
const NIX_SCHEME: &str = "nix:";

/// Why an image reference could not be understood.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageSourceError {
    /// The reference was empty.
    Empty,
    /// A `nix:` reference whose path is not under `/nix/store/`.
    ///
    /// Refused rather than accepted: the guarantee a closure carries is that
    /// its bytes are content-addressed and immutable. A path outside the store
    /// has neither, so honouring it would mean the type promised something the
    /// value does not have.
    NotAStorePath { path: String },
    /// A `nix:` reference containing `..`, which could resolve outside the
    /// store even though it starts inside it.
    TraversesUpward { path: String },
    /// A `nix:` reference naming the store root itself, with no package.
    NoPackage,
}

impl std::fmt::Display for ImageSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "image reference is empty"),
            Self::NotAStorePath { path } => write!(
                f,
                "nix:{path} is not under {STORE_ROOT}; a closure reference must \
                 name a content-addressed store path"
            ),
            Self::TraversesUpward { path } => {
                write!(
                    f,
                    "nix:{path} contains `..` and may resolve outside the store"
                )
            }
            Self::NoPackage => write!(f, "nix:{STORE_ROOT} names no package"),
        }
    }
}

impl std::error::Error for ImageSourceError {}

/// Where a container's executable bytes come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageSource {
    /// A realised Nix closure: runnable as a native host process.
    NixClosure(PathBuf),
    /// An upstream OCI reference: runnable only by a backend that has a Linux
    /// runtime underneath it.
    Oci(String),
}

impl ImageSource {
    /// Classify an image reference.
    ///
    /// # Errors
    /// [`ImageSourceError`] for a malformed `nix:` reference. An unrecognised
    /// reference is **not** an error — it is [`ImageSource::Oci`], because that
    /// is what every existing pod spec in the fleet contains and misreporting
    /// those as malformed would be a worse lie than classifying them honestly.
    pub fn parse(image: &str) -> Result<Self, ImageSourceError> {
        if image.is_empty() {
            return Err(ImageSourceError::Empty);
        }
        let Some(path) = image.strip_prefix(NIX_SCHEME) else {
            return Ok(Self::Oci(image.to_string()));
        };
        if !path.starts_with(STORE_ROOT) {
            return Err(ImageSourceError::NotAStorePath {
                path: path.to_string(),
            });
        }
        if path.split('/').any(|seg| seg == "..") {
            return Err(ImageSourceError::TraversesUpward {
                path: path.to_string(),
            });
        }
        if path.len() <= STORE_ROOT.len() {
            return Err(ImageSourceError::NoPackage);
        }
        Ok(Self::NixClosure(PathBuf::from(path)))
    }

    /// The closure path, if this is one.
    #[must_use]
    pub fn closure(&self) -> Option<&Path> {
        match self {
            Self::NixClosure(p) => Some(p.as_path()),
            Self::Oci(_) => None,
        }
    }

    /// Whether a backend with no Linux runtime can run this.
    #[must_use]
    pub const fn is_natively_runnable(&self) -> bool {
        matches!(self, Self::NixClosure(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real closure this host has, measured 2026-09-17:
    /// `postgresql_16` resolves for aarch64-darwin with `available = 1`.
    const PG: &str = "nix:/nix/store/00pjg4wzlg32ihqpcfw7jcm5q99r55vk-postgresql-16.15";

    #[test]
    fn a_store_path_is_a_natively_runnable_closure() {
        let src = ImageSource::parse(PG).expect("a realised store path must parse");
        assert!(src.is_natively_runnable());
        assert_eq!(
            src.closure().expect("closure"),
            Path::new("/nix/store/00pjg4wzlg32ihqpcfw7jcm5q99r55vk-postgresql-16.15")
        );
    }

    /// ★ The negative control, and the whole point of the type. The image ryn
    /// runs Postgres from TODAY must classify as not-natively-runnable, or the
    /// native backend would accept it and then fail at start with no Linux
    /// userland to unpack it into.
    #[test]
    fn the_oci_image_ryn_runs_today_is_not_natively_runnable() {
        let src = ImageSource::parse("docker.io/library/postgres:16-alpine")
            .expect("an OCI reference is classified, not rejected");
        assert_eq!(
            src,
            ImageSource::Oci("docker.io/library/postgres:16-alpine".to_string())
        );
        assert!(
            !src.is_natively_runnable(),
            "an OCI image must never be reported as natively runnable: a \
             backend with no Linux runtime would accept the pod and then fail \
             to start it"
        );
        assert!(src.closure().is_none());
    }

    /// A path outside the store carries none of the guarantees a closure does,
    /// so it is refused rather than accepted and hoped about.
    #[test]
    fn a_nix_reference_outside_the_store_is_refused() {
        assert_eq!(
            ImageSource::parse("nix:/tmp/postgres").expect_err("must refuse"),
            ImageSourceError::NotAStorePath {
                path: "/tmp/postgres".to_string()
            }
        );
    }

    /// Starting inside the store is not enough — `..` can leave it again.
    #[test]
    fn a_nix_reference_that_climbs_out_of_the_store_is_refused() {
        assert_eq!(
            ImageSource::parse("nix:/nix/store/../../etc/shadow").expect_err("must refuse"),
            ImageSourceError::TraversesUpward {
                path: "/nix/store/../../etc/shadow".to_string()
            }
        );
    }

    #[test]
    fn the_bare_store_root_names_no_package() {
        assert_eq!(
            ImageSource::parse("nix:/nix/store/").expect_err("must refuse"),
            ImageSourceError::NoPackage
        );
    }

    #[test]
    fn an_empty_reference_is_refused_rather_than_treated_as_oci() {
        assert_eq!(
            ImageSource::parse("").expect_err("must refuse"),
            ImageSourceError::Empty
        );
    }

    /// Every OCI shape in use on ryn today must survive classification. If this
    /// ever started erroring, existing pods would stop being schedulable on the
    /// podman backend too — the type must be additive, not a migration gate.
    #[test]
    fn existing_oci_shapes_still_classify_cleanly() {
        for image in [
            "docker.io/library/postgres:16-alpine",
            "postgres:16-alpine",
            "ghcr.io/pleme-io/pangea-operator@sha256:abc123",
            "localhost:5000/thing:latest",
        ] {
            let src = ImageSource::parse(image).expect("must classify");
            assert!(!src.is_natively_runnable(), "{image}");
        }
    }
}
