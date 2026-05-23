//! OciImageRenderer — `ShapeRenderer` impl that wraps a typed
//! `CommandRunner` to invoke `skopeo` for OCI image materialization.
//!
//! ## Wire shape
//!
//! For each `Drv` to render:
//!   1. Compose the source reference (typically a Nix output path
//!      converted to a local docker-archive URI).
//!   2. Compose the destination reference (oci-archive or daemon
//!      target).
//!   3. Invoke `skopeo copy --src-... --dest-... src dst`.
//!   4. Read the destination bytes as the rendered artifact.
//!
//! ## Test-friendly
//!
//! `FakeCommandRunner` pins the skopeo response so the renderer is
//! testable without a real skopeo binary. The renderer is generic
//! over the `CommandRunner` trait; production wires `RealCommandRunner`,
//! tests wire `FakeCommandRunner`.
//!
//! ## Synthetic artifact in test mode
//!
//! When the operator provides no destination-bytes accessor, the
//! renderer falls back to a synthetic artifact whose bytes are
//! BLAKE3(drv_hash + skopeo_invocation_bytes). Tests don't need
//! real OCI bytes to verify the dispatch path.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::command_runner::{CommandRequest, CommandRunner};
use crate::derivation::Drv;
use crate::shape::{RenderedArtifact, ShapeError, ShapeRenderer, WorkloadShape};

crate::async_closure_type! {
    /// Operator-supplied source-reference generator. Given a Drv,
    /// returns the URI skopeo should read from (e.g.
    /// "docker-archive:/nix/store/{hash}-image.tar").
    pub type OciSourceRef = sync (&Drv) -> String;
}

crate::async_closure_type! {
    /// Operator-supplied destination-reference generator. Given a Drv,
    /// returns the URI skopeo should write to (e.g.
    /// "oci-archive:/tmp/{drv_hash_hex}-oci.tar").
    pub type OciDestRef = sync (&Drv) -> String;
}

crate::async_closure_type! {
    /// Optional accessor that reads the destination bytes back after
    /// a successful skopeo run. Production wires this to `std::fs::read`
    /// on the dest path; absence yields a synthetic artifact.
    pub type OciDestReader = sync (&Drv) -> Option<Vec<u8>>;
}

/// Renderer parameterized by a CommandRunner + the three ref/reader
/// closures.
pub struct OciImageRenderer {
    name: &'static str,
    runner: Arc<dyn CommandRunner>,
    source_ref: OciSourceRef,
    dest_ref: OciDestRef,
    dest_reader: Option<OciDestReader>,
    /// Path to the skopeo binary; default "skopeo".
    binary: String,
    /// Extra skopeo flags (operator-controlled).
    extra_flags: Vec<String>,
}

impl OciImageRenderer {
    /// New renderer with all closures + default binary `skopeo`.
    #[must_use]
    pub fn new(
        name: &'static str,
        runner: Arc<dyn CommandRunner>,
        source_ref: OciSourceRef,
        dest_ref: OciDestRef,
    ) -> Self {
        Self {
            name,
            runner,
            source_ref,
            dest_ref,
            dest_reader: None,
            binary: "skopeo".into(),
            extra_flags: Vec::new(),
        }
    }

    /// Convenience with name `"oci-image"`.
    #[must_use]
    pub fn default_named(
        runner: Arc<dyn CommandRunner>,
        source_ref: OciSourceRef,
        dest_ref: OciDestRef,
    ) -> Self {
        Self::new("oci-image", runner, source_ref, dest_ref)
    }

    /// Override the skopeo binary path.
    #[must_use]
    pub fn with_binary(mut self, binary: impl Into<String>) -> Self {
        self.binary = binary.into();
        self
    }

    /// Add extra skopeo flags (e.g. `["--insecure-policy"]`).
    #[must_use]
    pub fn with_extra_flags(mut self, flags: Vec<String>) -> Self {
        self.extra_flags = flags;
        self
    }

    /// Attach a destination-bytes reader.
    #[must_use]
    pub fn with_dest_reader(mut self, reader: OciDestReader) -> Self {
        self.dest_reader = Some(reader);
        self
    }

    /// Pure helper: compose the typed CommandRequest for a Drv.
    /// Exposed so tests can assert what would be invoked without
    /// driving the full async path.
    #[must_use]
    pub fn build_request(&self, drv: &Drv) -> CommandRequest {
        let src = (self.source_ref)(drv);
        let dst = (self.dest_ref)(drv);
        let mut args = vec!["copy".to_string()];
        args.extend(self.extra_flags.iter().cloned());
        args.push(src);
        args.push(dst);
        CommandRequest::new(self.binary.clone(), args)
    }

    /// Convenience: default source-ref closure that maps the drv
    /// hash into `docker-archive:/nix/store/{hash}-image.tar`.
    /// Operators wire their own when the convention differs.
    #[must_use]
    pub fn default_source_ref() -> OciSourceRef {
        Arc::new(|drv: &Drv| {
            format!(
                "docker-archive:/nix/store/{}-image.tar",
                drv.drv_hash.to_hex()
            )
        })
    }

    /// Convenience: default dest-ref closure that writes to
    /// `oci-archive:/tmp/{hash}-oci.tar`.
    #[must_use]
    pub fn default_dest_ref() -> OciDestRef {
        Arc::new(|drv: &Drv| format!("oci-archive:/tmp/{}-oci.tar", drv.drv_hash.to_hex()))
    }

    /// Convenience: default dest-reader closure that strips the
    /// `oci-archive:` prefix and reads the file.
    #[must_use]
    pub fn default_dest_reader() -> OciDestReader {
        Arc::new(|drv: &Drv| {
            let path: PathBuf = format!("/tmp/{}-oci.tar", drv.drv_hash.to_hex()).into();
            std::fs::read(&path).ok()
        })
    }
}

#[async_trait]
impl ShapeRenderer for OciImageRenderer {
    fn name(&self) -> &'static str {
        self.name
    }

    fn shape(&self) -> WorkloadShape {
        WorkloadShape::OciImage
    }

    async fn render(&self, drv: &Drv) -> Result<RenderedArtifact, ShapeError> {
        let req = self.build_request(drv);
        let resp = self
            .runner
            .run(&req)
            .await
            .map_err(|e| ShapeError::Backend(format!("skopeo: {e}")))?;
        if !resp.is_success() {
            return Err(ShapeError::Backend(format!(
                "skopeo exit {:?}: {}",
                resp.exit_code,
                String::from_utf8_lossy(&resp.stderr)
            )));
        }
        // Read the destination if a reader is configured; else
        // synthesize bytes from the drv hash + invocation, so the
        // dispatch path is testable without real OCI artifacts.
        let bytes = if let Some(reader) = &self.dest_reader {
            match reader(drv) {
                Some(b) => b,
                None => {
                    return Err(ShapeError::Backend(format!(
                        "dest_reader returned None for drv {}",
                        drv.drv_hash
                    )));
                }
            }
        } else {
            let mut composed = drv.drv_hash.to_hex().into_bytes();
            for arg in &req.args {
                composed.extend_from_slice(arg.as_bytes());
            }
            composed
        };
        Ok(RenderedArtifact::from_bytes(WorkloadShape::OciImage, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_runner::{CommandResponse, FakeCommandRunner};
    use crate::derivation::DrvHash;

    fn d(tag: &[u8]) -> Drv {
        Drv::synthetic(DrvHash::from_bytes(tag), "x86_64-linux")
    }

    fn src_ref() -> OciSourceRef {
        OciImageRenderer::default_source_ref()
    }

    fn dst_ref() -> OciDestRef {
        OciImageRenderer::default_dest_ref()
    }

    #[tokio::test]
    async fn build_request_composes_skopeo_copy_command() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref());
        let req = renderer.build_request(&d(b"x"));
        assert_eq!(req.program, "skopeo");
        assert_eq!(req.args[0], "copy");
        assert!(req.args[req.args.len() - 2].starts_with("docker-archive:"));
        assert!(req.args[req.args.len() - 1].starts_with("oci-archive:"));
    }

    #[tokio::test]
    async fn build_request_inserts_extra_flags_between_copy_and_args() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref())
            .with_extra_flags(vec!["--insecure-policy".into(), "--quiet".into()]);
        let req = renderer.build_request(&d(b"x"));
        assert_eq!(req.args[0], "copy");
        assert_eq!(req.args[1], "--insecure-policy");
        assert_eq!(req.args[2], "--quiet");
    }

    #[tokio::test]
    async fn with_binary_overrides_program() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref())
            .with_binary("/usr/local/bin/skopeo");
        let req = renderer.build_request(&d(b"x"));
        assert_eq!(req.program, "/usr/local/bin/skopeo");
    }

    #[tokio::test]
    async fn render_dispatches_through_runner() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner.clone(), src_ref(), dst_ref());
        let _ = renderer.render(&d(b"x")).await.unwrap();
        let calls = runner.invocations().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].program, "skopeo");
    }

    #[tokio::test]
    async fn render_returns_synthetic_bytes_when_no_dest_reader() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref());
        let a = renderer.render(&d(b"x")).await.unwrap();
        // Synthetic bytes contain the drv hash hex.
        let hex = DrvHash::from_bytes(b"x").to_hex();
        assert!(String::from_utf8_lossy(&a.bytes).contains(&hex));
        assert_eq!(a.shape, WorkloadShape::OciImage);
    }

    #[tokio::test]
    async fn render_faithful_across_nodes() {
        let runner1 = Arc::new(FakeCommandRunner::new());
        let runner2 = Arc::new(FakeCommandRunner::new());
        let renderer1 = OciImageRenderer::default_named(runner1, src_ref(), dst_ref());
        let renderer2 = OciImageRenderer::default_named(runner2, src_ref(), dst_ref());
        let a1 = renderer1.render(&d(b"x")).await.unwrap();
        let a2 = renderer2.render(&d(b"x")).await.unwrap();
        // Same drv → same evidence (different nodes converge).
        assert_eq!(a1.evidence_hash, a2.evidence_hash);
    }

    #[tokio::test]
    async fn render_propagates_skopeo_failure() {
        let runner = Arc::new(FakeCommandRunner::new());
        runner
            .set_default(CommandResponse {
                exit_code: Some(127),
                stdout: Vec::new(),
                stderr: b"skopeo: command not found".to_vec(),
            })
            .await;
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref());
        let err = renderer.render(&d(b"x")).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn render_uses_pinned_response_when_runner_pins_command() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner.clone(), src_ref(), dst_ref());
        // Pin the exact skopeo invocation the renderer will build.
        let expected_req = renderer.build_request(&d(b"x"));
        runner
            .pin(
                expected_req.program.clone(),
                expected_req.args.clone(),
                CommandResponse {
                    exit_code: Some(0),
                    stdout: b"pinned-stdout".to_vec(),
                    stderr: Vec::new(),
                },
            )
            .await;
        renderer.render(&d(b"x")).await.unwrap();
        assert_eq!(runner.call_count().await, 1);
    }

    #[tokio::test]
    async fn dest_reader_none_surfaces_as_backend_error() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref())
            .with_dest_reader(Arc::new(|_| None));
        let err = renderer.render(&d(b"x")).await.unwrap_err();
        assert_eq!(err.kind(), "backend");
    }

    #[tokio::test]
    async fn dest_reader_returns_bytes_as_evidence() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref())
            .with_dest_reader(Arc::new(|_| Some(b"real-oci-bytes".to_vec())));
        let a = renderer.render(&d(b"x")).await.unwrap();
        assert_eq!(a.bytes, b"real-oci-bytes");
        assert_eq!(a.evidence_hash, *blake3::hash(b"real-oci-bytes").as_bytes());
    }

    #[tokio::test]
    async fn shape_is_oci_image() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref());
        assert_eq!(renderer.shape(), WorkloadShape::OciImage);
    }

    #[tokio::test]
    async fn name_passes_through_constructor() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::new("custom-oci", runner, src_ref(), dst_ref());
        assert_eq!(renderer.name(), "custom-oci");
    }

    #[tokio::test]
    async fn default_name_is_oci_image() {
        let runner = Arc::new(FakeCommandRunner::new());
        let renderer = OciImageRenderer::default_named(runner, src_ref(), dst_ref());
        assert_eq!(renderer.name(), "oci-image");
    }

    #[test]
    fn default_source_ref_format() {
        let s = OciImageRenderer::default_source_ref();
        let url = s(&d(b"x"));
        let hex = DrvHash::from_bytes(b"x").to_hex();
        assert_eq!(url, format!("docker-archive:/nix/store/{hex}-image.tar"));
    }

    #[test]
    fn default_dest_ref_format() {
        let s = OciImageRenderer::default_dest_ref();
        let url = s(&d(b"x"));
        let hex = DrvHash::from_bytes(b"x").to_hex();
        assert_eq!(url, format!("oci-archive:/tmp/{hex}-oci.tar"));
    }
}
