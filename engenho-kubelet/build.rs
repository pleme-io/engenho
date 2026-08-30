//! Compile the vendored CRI v1 protos into a gRPC CLIENT.
//!
//! Client only: engenho's kubelet CALLS a container runtime, it does not
//! implement one. Generating the server traits would ship dead code and
//! invite someone to implement the wrong side of the seam.
//!
//! Uses protox — a pure-Rust protobuf compiler — so no `protoc` is needed
//! on the build host, the same route `engenho-kube-proto` and
//! `engenho-etcd` already take.
//!
//! The proto is upstream's own, from `kubernetes/cri-api@release-1.34`, and
//! is NOT reconstructed. The only edit is removing `gogoproto` options,
//! which are Go codegen hints with no effect on the wire encoding.

use std::path::PathBuf;

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    // Under substrate's lockfile-builder CARGO_MANIFEST_DIR is the WORKSPACE
    // ROOT, not this crate — the same two-layout problem the other proto
    // crates document, handled the same way.
    let here = manifest.join("vendor/proto");
    let root = if here.is_dir() {
        here
    } else {
        manifest.join("engenho-kubelet/vendor/proto")
    };
    println!("cargo:rerun-if-changed={}", root.display());

    let files = [root.join("runtime/v1/api.proto")];
    let descriptors = protox::compile(&files, [&root]).expect("vendored CRI protos must compile");

    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir(&out)
        .compile_fds(descriptors)
        .expect("tonic + prost codegen for the CRI protos");
}
