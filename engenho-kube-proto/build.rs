//! Build-time protobuf descriptor compilation.
//!
//! Compiles the vendored go-to-protobuf `generated.proto` set
//! (`vendor/proto/`) into a single `FileDescriptorSet` using `protox` —
//! a pure-Rust protobuf compiler that needs NO `protoc` binary on the
//! build host (engenho's build hosts do not ship protoc). The encoded
//! descriptor set is written to `$OUT_DIR/k8s_descriptors.bin` and
//! `include_bytes!`d by the crate at compile time, then loaded into a
//! `prost_reflect::DescriptorPool` at first use.
//!
//! The descriptor set carries every field's `json_name` (== the K8s
//! camelCase JSON key, because go-to-protobuf already emits camelCase
//! proto field names), which is what drives the
//! `DynamicMessage <-> serde_json::Value` bridge in `src/lib.rs`. No
//! hand-maintained field-name map exists or is needed.

use std::path::PathBuf;

use prost_reflect::prost::Message;

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_root = manifest_dir.join("vendor/proto");

    // The eight vendored files, referenced by their import path relative
    // to the include root. protox resolves the `import "..."` graph from
    // the same include root, so every transitive import is satisfied.
    let files = [
        "k8s.io/api/core/v1/generated.proto",
        "k8s.io/api/apps/v1/generated.proto",
        "k8s.io/api/rbac/v1/generated.proto",
        "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
        "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
        "k8s.io/apimachinery/pkg/api/resource/generated.proto",
    ];

    // Rebuild only when a vendored proto or this script changes.
    println!("cargo:rerun-if-changed=build.rs");
    for f in &files {
        println!(
            "cargo:rerun-if-changed={}",
            proto_root.join(f).display()
        );
    }

    // Compile to a FileDescriptorSet (pure-Rust; no protoc).
    let file_descriptor_set = protox::compile(files, [&proto_root])
        .expect("protox: compile vendored k8s .proto set to FileDescriptorSet");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let bin_path = out_dir.join("k8s_descriptors.bin");
    std::fs::write(&bin_path, file_descriptor_set.encode_to_vec())
        .expect("write k8s_descriptors.bin");
}
