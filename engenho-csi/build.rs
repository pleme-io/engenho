//! Compile the vendored CSI + plugin-registration protos.
//!
//! `protox` (pure Rust) so no `protoc` is needed on the build host — the
//! same route `engenho-kube-proto` and `engenho-etcd` take.
//!
//! ★ CLIENTS ARE THE ARCHITECTURE; SERVERS ARE THE TEST INSTRUMENT.
//! engenho is the kubelet in this relationship: it CALLS a driver's
//! Identity / Controller / Node services and CALLS a plugin's Registration
//! service. Nothing in engenho serves them.
//!
//! The server traits are generated anyway, and for one reason worth stating
//! rather than leaving to be inferred: they let the tests stand up a REAL
//! CSI driver — a real tonic server on a real unix socket, speaking the
//! real wire format — instead of a mock of our own client. A CSI client
//! tested only against a hand-written double proves that our encoder agrees
//! with our decoder, which is exactly the thing that cannot fail. Against a
//! generated server it proves the wire is right.

use std::path::PathBuf;

fn main() {
    // Under real cargo this is the crate dir; under substrate's
    // lockfile-builder it is the WORKSPACE ROOT. Same two-layout problem
    // engenho-etcd documents, solved the same way.
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let here = manifest.join("vendor/proto");
    let root = if here.is_dir() {
        here
    } else {
        manifest.join("engenho-csi/vendor/proto")
    };

    println!("cargo:rerun-if-changed={}", root.display());

    let files = [
        root.join("csi/csi.proto"),
        root.join("pluginregistration/api.proto"),
    ];

    let descriptors = protox::compile(&files, [&root]).expect("vendored CSI protos must compile");

    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out)
        .compile_fds(descriptors)
        .expect("tonic + prost codegen for the CSI protos");
}
