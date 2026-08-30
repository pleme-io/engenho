//! Compile the vendored etcd v3 protos into Rust types.
//!
//! Uses `protox` — a pure-Rust protobuf compiler — so no `protoc` binary is
//! needed on the build host. engenho's build hosts do not ship one, which is
//! why `engenho-kube-proto` already takes this route; this mirrors it.
//!
//! The protos under `vendor/proto/` are upstream's own, fetched from
//! `etcd-io/etcd@release-3.5` and NOT reconstructed. The only edit applied
//! was stripping `gogoproto` and `google.api.http` options, which are Go
//! codegen and grpc-gateway hints with no effect on the wire encoding.
//! Every field number and type is upstream's byte for byte.

use std::path::PathBuf;

fn main() {
    // Under real cargo this is the crate dir. Under substrate's
    // lockfile-builder it is the WORKSPACE ROOT — the same two-layout
    // problem engenho-kube-proto documents, solved the same way.
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let here = manifest.join("vendor/proto");
    let root = if here.is_dir() {
        here
    } else {
        manifest.join("engenho-etcd/vendor/proto")
    };

    println!("cargo:rerun-if-changed={}", root.display());

    let files = [
        root.join("etcd/api/mvccpb/kv.proto"),
        root.join("etcd/api/authpb/auth.proto"),
        root.join("etcd/api/etcdserverpb/rpc.proto"),
    ];

    let descriptors = protox::compile(&files, [&root]).expect("vendored etcd protos must compile");

    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    // tonic-build wraps prost codegen and additionally emits the SERVICE
    // traits — `KvServer`, `WatchServer`, `MaintenanceServer`. Servers only:
    // engenho answers etcd requests, it never issues them, so generating
    // clients would ship dead code.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir(&out)
        .compile_fds(descriptors)
        .expect("tonic + prost codegen for the etcd protos");
}
