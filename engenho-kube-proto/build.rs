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
    // ── Locating the vendored protos across TWO build layouts ─────────
    // Under real cargo, CARGO_MANIFEST_DIR is THIS crate's directory and
    // `vendor/proto` sits directly beneath it.
    //
    // Under substrate's lockfile-builder it is the WORKSPACE ROOT. That
    // is deliberate, not a bug: every member is built with
    // `src = workspaceSrc` so an `include_str!("../../x")` reaching a
    // workspace-root file still resolves, and `libPath`/`build_script`
    // are prefixed with the member path to compensate
    // (substrate/lib/build/rust/lockfile-builder.nix:281).
    // CARGO_MANIFEST_DIR is NOT among the prefixed values, so joining a
    // path onto it resolves one level too high.
    //
    // The failure that causes is actively misleading: protox reports
    // "file '...' is not in any include path" — which reads as a missing
    // vendored file — when the files are present and merely one
    // directory down. So try both layouts and, on failure, name every
    // candidate rather than let protox describe it.
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let candidates = [
        manifest_dir.join("vendor/proto"),
        manifest_dir.join("engenho-kube-proto/vendor/proto"),
    ];
    let proto_root = candidates
        .iter()
        .find(|p| p.join("k8s.io").is_dir())
        .unwrap_or_else(|| {
            let tried = candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join("\n  ");
            panic!(
                "engenho-kube-proto: vendored k8s protos not found.\n\
                 CARGO_MANIFEST_DIR = {}\n  tried:\n  {tried}",
                manifest_dir.display()
            )
        })
        .clone();

    // The eight vendored files, referenced by their import path relative
    // to the include root. protox resolves the `import "..."` graph from
    // the same include root, so every transitive import is satisfied.
    let files = [
        "k8s.io/api/core/v1/generated.proto",
        "k8s.io/api/apps/v1/generated.proto",
        "k8s.io/api/rbac/v1/generated.proto",
        "k8s.io/api/authorization/v1/generated.proto",
        "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
        "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
        "k8s.io/apimachinery/pkg/api/resource/generated.proto",
    ];

    // Rebuild only when a vendored proto or this script changes.
    println!("cargo:rerun-if-changed=build.rs");
    for f in &files {
        println!("cargo:rerun-if-changed={}", proto_root.join(f).display());
    }

    // Compile to a FileDescriptorSet (pure-Rust; no protoc).
    let file_descriptor_set = protox::compile(files, [&proto_root])
        .expect("protox: compile vendored k8s .proto set to FileDescriptorSet");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let bin_path = out_dir.join("k8s_descriptors.bin");
    std::fs::write(&bin_path, file_descriptor_set.encode_to_vec())
        .expect("write k8s_descriptors.bin");
}
