//! WorkloadShape — typed catalog of representations the substrate
//! can render `Drv` outputs into.
//!
//! Every shape carries a deterministic "shape hash" the
//! confirmation layer uses as the receipt subject + the cache
//! address. Same Drv rendered into multiple shapes produces
//! distinct shape hashes (the hash composes the drv hash with the
//! shape identifier).
//!
//! This module ships only the typed enum + naming + hashing. The
//! actual renderers ship per-shape as they're built (OciImage
//! first, NixClosure next, etc.).

use serde::{Deserialize, Serialize};

/// The shapes the substrate can materialize.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadShape {
    /// OCI image — the universal container format. Produces a
    /// tarball + manifest registries can serve.
    OciImage,
    /// NixOS closure — a directory tree of /nix/store paths +
    /// activation script. The native Nix representation.
    NixClosure,
    /// qcow2 disk image — a bootable VM disk.
    Qcow2,
    /// WebAssembly bundle — a single .wasm file (+ optional WIT).
    Wasm,
    /// Static binary — single-file executable for a target triple.
    StaticBinary {
        /// LLVM target triple, e.g. "x86_64-unknown-linux-musl".
        triple: String,
    },
    /// Helm chart — packaged as a `.tgz` ready for OCI push.
    HelmChart,
    /// Reserved for substrate-internal use; consumers should not
    /// match on this variant.
    Custom {
        /// Free-form shape identifier.
        name: String,
    },
}

impl WorkloadShape {
    /// Project a [`Realisation`] into its native shape. Every
    /// Realisation IS a NixClosure by definition (it's the output
    /// of a Drv); upper layers re-render via [`ShapeRenderer`] for
    /// other shapes.
    ///
    /// Returns `None` if the realisation has no NAR hash (cache
    /// only carries the path; the NAR hasn't been fetched yet).
    #[must_use]
    pub fn from_realisation(_r: &crate::derivation::Realisation) -> Self {
        WorkloadShape::NixClosure
    }

    /// Stable identifier (snake_case) used as a tag in receipts
    /// + cache keys. Mirrors the serde rename — keeps consistency
    /// between in-process matching + on-wire receipts.
    #[must_use]
    pub fn tag(&self) -> String {
        match self {
            Self::OciImage => "oci_image".into(),
            Self::NixClosure => "nix_closure".into(),
            Self::Qcow2 => "qcow2".into(),
            Self::Wasm => "wasm".into(),
            Self::StaticBinary { triple } => format!("static_binary:{triple}"),
            Self::HelmChart => "helm_chart".into(),
            Self::Custom { name } => format!("custom:{name}"),
        }
    }

    /// Compute the deterministic shape hash: BLAKE3 of
    /// `{shape_tag}|{drv_hash_hex}`. Two nodes rendering the same
    /// Drv into the same Shape produce the same shape hash.
    #[must_use]
    pub fn shape_hash(&self, drv_hash_hex: &str) -> [u8; 32] {
        let composed = format!("{}|{drv_hash_hex}", self.tag());
        *blake3::hash(composed.as_bytes()).as_bytes()
    }

    /// File extension typical consumers serve this shape under.
    /// Returns None for shapes with no canonical extension.
    #[must_use]
    pub fn file_extension(&self) -> Option<&'static str> {
        match self {
            Self::OciImage => Some("tar"),
            Self::NixClosure => None, // a directory, not a file
            Self::Qcow2 => Some("qcow2"),
            Self::Wasm => Some("wasm"),
            Self::StaticBinary { .. } => None,  // no canonical extension
            Self::HelmChart => Some("tgz"),
            Self::Custom { .. } => None,
        }
    }

    /// MIME / OCI media type the shape is served as, when one
    /// exists. None = no canonical media type.
    #[must_use]
    pub fn media_type(&self) -> Option<&'static str> {
        match self {
            Self::OciImage => Some("application/vnd.oci.image.manifest.v1+json"),
            Self::Wasm => Some("application/wasm"),
            Self::HelmChart => Some("application/vnd.cncf.helm.chart.content.v1.tar+gzip"),
            _ => None,
        }
    }
}

impl std::fmt::Display for WorkloadShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.tag())
    }
}

/// Trait every shape renderer implements. Substrate ships the
/// trait; concrete impls live in per-shape crates / modules as
/// they're built (kept lean here — no impls yet).
#[async_trait::async_trait]
pub trait ShapeRenderer: Send + Sync {
    /// Backend identifier for telemetry.
    fn name(&self) -> &'static str;

    /// The shape this renderer produces.
    fn shape(&self) -> WorkloadShape;

    /// Materialize the drv into this shape. Returns the rendered
    /// artifact bytes + a content hash the caller can present as
    /// the receipt's `evidence_hash`.
    ///
    /// # Errors
    /// Renderer-specific failures via [`ShapeError`].
    async fn render(
        &self,
        drv: &crate::derivation::Drv,
    ) -> Result<RenderedArtifact, ShapeError>;
}

/// Rendered output.
#[derive(Clone, Debug)]
pub struct RenderedArtifact {
    /// Shape the renderer produced.
    pub shape: WorkloadShape,
    /// Bytes the consumer can serve / store / publish.
    pub bytes: Vec<u8>,
    /// BLAKE3 over `bytes` — the canonical receipt evidence hash.
    pub evidence_hash: [u8; 32],
}

impl RenderedArtifact {
    /// Build from bytes; computes evidence_hash.
    #[must_use]
    pub fn from_bytes(shape: WorkloadShape, bytes: Vec<u8>) -> Self {
        let evidence_hash = *blake3::hash(&bytes).as_bytes();
        Self {
            shape,
            bytes,
            evidence_hash,
        }
    }
}

/// Shape renderer errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ShapeError {
    /// Renderer backend failure.
    #[error("backend: {0}")]
    Backend(String),
    /// Drv structurally invalid for this shape.
    #[error("invalid drv: {0}")]
    InvalidDrv(String),
    /// Shape isn't supported by this renderer.
    #[error("unsupported shape: {0}")]
    UnsupportedShape(String),
}

impl ShapeError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
            Self::InvalidDrv(_) => "invalid_drv",
            Self::UnsupportedShape(_) => "unsupported_shape",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_stable_snake_case() {
        assert_eq!(WorkloadShape::OciImage.tag(), "oci_image");
        assert_eq!(WorkloadShape::NixClosure.tag(), "nix_closure");
        assert_eq!(WorkloadShape::Qcow2.tag(), "qcow2");
        assert_eq!(WorkloadShape::Wasm.tag(), "wasm");
        assert_eq!(WorkloadShape::HelmChart.tag(), "helm_chart");
    }

    #[test]
    fn parametric_shapes_include_param_in_tag() {
        let s = WorkloadShape::StaticBinary {
            triple: "x86_64-unknown-linux-musl".into(),
        };
        assert_eq!(s.tag(), "static_binary:x86_64-unknown-linux-musl");
        let c = WorkloadShape::Custom {
            name: "openraft-state".into(),
        };
        assert_eq!(c.tag(), "custom:openraft-state");
    }

    #[test]
    fn display_matches_tag() {
        let s = WorkloadShape::OciImage;
        assert_eq!(format!("{s}"), s.tag());
    }

    #[test]
    fn shape_hash_is_deterministic() {
        let s = WorkloadShape::OciImage;
        let h1 = s.shape_hash("abc123");
        let h2 = s.shape_hash("abc123");
        assert_eq!(h1, h2);
    }

    #[test]
    fn shape_hash_diverges_per_shape() {
        let drv_hash = "abc123";
        let oci = WorkloadShape::OciImage.shape_hash(drv_hash);
        let qcow = WorkloadShape::Qcow2.shape_hash(drv_hash);
        assert_ne!(oci, qcow);
    }

    #[test]
    fn shape_hash_diverges_per_drv() {
        let s = WorkloadShape::OciImage;
        let h1 = s.shape_hash("abc");
        let h2 = s.shape_hash("def");
        assert_ne!(h1, h2);
    }

    #[test]
    fn file_extension_correctness() {
        assert_eq!(WorkloadShape::OciImage.file_extension(), Some("tar"));
        assert_eq!(WorkloadShape::Qcow2.file_extension(), Some("qcow2"));
        assert_eq!(WorkloadShape::Wasm.file_extension(), Some("wasm"));
        assert_eq!(WorkloadShape::HelmChart.file_extension(), Some("tgz"));
        assert_eq!(WorkloadShape::NixClosure.file_extension(), None);
        assert_eq!(
            WorkloadShape::StaticBinary {
                triple: "x".into()
            }
            .file_extension(),
            None
        );
    }

    #[test]
    fn media_type_correctness() {
        assert_eq!(
            WorkloadShape::OciImage.media_type(),
            Some("application/vnd.oci.image.manifest.v1+json")
        );
        assert_eq!(WorkloadShape::Wasm.media_type(), Some("application/wasm"));
        assert_eq!(WorkloadShape::Qcow2.media_type(), None);
        assert_eq!(WorkloadShape::NixClosure.media_type(), None);
    }

    #[test]
    fn shape_round_trips_via_serde() {
        for shape in [
            WorkloadShape::OciImage,
            WorkloadShape::NixClosure,
            WorkloadShape::Qcow2,
            WorkloadShape::Wasm,
            WorkloadShape::StaticBinary {
                triple: "aarch64-apple-darwin".into(),
            },
            WorkloadShape::HelmChart,
            WorkloadShape::Custom {
                name: "custom-thing".into(),
            },
        ] {
            let bytes = serde_json::to_vec(&shape).unwrap();
            let back: WorkloadShape = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(back, shape);
        }
    }

    #[test]
    fn rendered_artifact_from_bytes_computes_hash() {
        let a = RenderedArtifact::from_bytes(WorkloadShape::Wasm, b"hello".to_vec());
        assert_eq!(a.bytes, b"hello");
        assert_eq!(a.evidence_hash, *blake3::hash(b"hello").as_bytes());
        assert_eq!(a.shape, WorkloadShape::Wasm);
    }

    #[test]
    fn from_realisation_projects_to_nix_closure() {
        use crate::derivation::{DrvHash, OutputPath, Realisation};
        let r = Realisation {
            drv_hash: DrvHash::from_bytes(b"d"),
            output_name: "out".into(),
            output_path: OutputPath::new("/nix/store/a-out"),
            nar_hash: None,
        };
        assert_eq!(
            WorkloadShape::from_realisation(&r),
            WorkloadShape::NixClosure
        );
    }

    #[test]
    fn shape_error_kinds_are_stable() {
        assert_eq!(ShapeError::Backend("x".into()).kind(), "backend");
        assert_eq!(ShapeError::InvalidDrv("x".into()).kind(), "invalid_drv");
        assert_eq!(
            ShapeError::UnsupportedShape("x".into()).kind(),
            "unsupported_shape"
        );
    }
}
