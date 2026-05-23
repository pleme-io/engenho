//! Property: OciImageRenderer build_request semantics + builder chain.

use engenho_substrate::{Drv, FakeCommandRunner, OciDestRef, OciImageRenderer, OciSourceRef};
use engenho_substrate_props::helpers::sample_drv as drv;
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;
use std::sync::Arc;

fn fixed_source(uri: &'static str) -> OciSourceRef {
    Arc::new(move |_: &Drv| uri.to_string())
}

fn fixed_dest(uri: &'static str) -> OciDestRef {
    Arc::new(move |_: &Drv| uri.to_string())
}

proptest_with_env! {
    /// build_request always starts with "copy" as the first arg.
    #[test]
    fn build_request_starts_with_copy(hash_b in any::<u8>()) {
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            fixed_source("docker-archive:/in.tar"),
            fixed_dest("oci-archive:/out.tar"),
        );
        let req = r.build_request(&drv(hash_b));
        assert_eq!(req.args.first().map(String::as_str), Some("copy"));
    }

    /// build_request includes source + dest refs as the last two args.
    #[test]
    fn build_request_includes_src_and_dst(hash_b in any::<u8>()) {
        let src = "docker-archive:/myinput.tar";
        let dst = "oci-archive:/myoutput.tar";
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            fixed_source(src),
            fixed_dest(dst),
        );
        let req = r.build_request(&drv(hash_b));
        assert!(req.args.contains(&src.to_string()));
        assert!(req.args.contains(&dst.to_string()));
        // Position invariant: src comes before dst in the argv.
        let src_pos = req.args.iter().position(|a| a == src).unwrap();
        let dst_pos = req.args.iter().position(|a| a == dst).unwrap();
        assert!(src_pos < dst_pos, "source should come before dest");
    }

    /// Default binary is "skopeo".
    #[test]
    fn default_binary_is_skopeo(hash_b in any::<u8>()) {
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            fixed_source("docker-archive:/in.tar"),
            fixed_dest("oci-archive:/out.tar"),
        );
        let req = r.build_request(&drv(hash_b));
        assert_eq!(req.program, "skopeo");
    }

    /// with_binary overrides the default skopeo binary.
    #[test]
    fn with_binary_overrides_default(hash_b in any::<u8>(), binary in "[a-z][a-z0-9_-]{0,20}") {
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            fixed_source("docker-archive:/in.tar"),
            fixed_dest("oci-archive:/out.tar"),
        )
        .with_binary(&binary);
        let req = r.build_request(&drv(hash_b));
        assert_eq!(req.program, binary);
    }

    /// with_extra_flags adds flags between "copy" and src/dst.
    #[test]
    fn with_extra_flags_adds_in_order(
        hash_b in any::<u8>(),
        flags in proptest::collection::vec("[a-z-]{1,16}", 0..5),
    ) {
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            fixed_source("docker-archive:/in.tar"),
            fixed_dest("oci-archive:/out.tar"),
        )
        .with_extra_flags(flags.clone());
        let req = r.build_request(&drv(hash_b));
        // args = ["copy", flags..., src, dst] — total length = 1 + flags.len() + 2
        assert_eq!(req.args.len(), 1 + flags.len() + 2);
        for (i, flag) in flags.iter().enumerate() {
            assert_eq!(req.args[i + 1], *flag);
        }
    }

    /// default_source_ref includes the drv hash in hex.
    #[test]
    fn default_source_ref_includes_hash_hex(hash_b in any::<u8>()) {
        let src = OciImageRenderer::default_source_ref();
        let d = drv(hash_b);
        let uri = src(&d);
        let hash_hex = d.drv_hash.to_hex();
        assert!(uri.contains(&hash_hex), "uri {uri} missing hash {hash_hex}");
        assert!(uri.starts_with("docker-archive:"));
    }

    /// default_dest_ref includes the drv hash in hex + oci-archive prefix.
    #[test]
    fn default_dest_ref_includes_hash_hex(hash_b in any::<u8>()) {
        let dst = OciImageRenderer::default_dest_ref();
        let d = drv(hash_b);
        let uri = dst(&d);
        let hash_hex = d.drv_hash.to_hex();
        assert!(uri.contains(&hash_hex));
        assert!(uri.starts_with("oci-archive:"));
    }

    /// build_request is deterministic — same drv → same CommandRequest.
    #[test]
    fn build_request_deterministic(hash_b in any::<u8>()) {
        let r = OciImageRenderer::default_named(
            Arc::new(FakeCommandRunner::new()),
            OciImageRenderer::default_source_ref(),
            OciImageRenderer::default_dest_ref(),
        );
        let d = drv(hash_b);
        let req1 = r.build_request(&d);
        let req2 = r.build_request(&d);
        assert_eq!(req1, req2);
    }
}
