use std::path::{Path, PathBuf};

use engenho_kube_codegen::{emit_blue_kinds, schemas};

fn workspace() -> PathBuf {
    let here = Path::new(env!("CARGO_MANIFEST_DIR"));
    if here.join("engenho-types").is_dir() {
        here.to_path_buf()
    } else {
        here.join("..")
    }
}

fn emitted() -> String {
    let schema = workspace().join("engenho-types/vendor/openapi/v1.34.0");
    emit_blue_kinds(&schemas::load_schemas(&schema).expect("vendored schemas load"))
        .expect("blue kinds emit")
}

#[test]
fn the_committed_blue_kinds_are_what_the_vendored_schema_generates() {
    let golden = workspace().join("engenho-types/blue/kinds.b");
    let committed = std::fs::read_to_string(&golden).unwrap_or_default();
    assert!(
        committed == emitted(),
        "{} is stale: run engenho-kube-codegen --schema engenho-types/vendor/openapi/v1.34.0 --output engenho-types/src/generated_v1_34 --blue engenho-types/blue/kinds.b",
        golden.display()
    );
}

#[test]
fn every_selected_kind_is_a_record_with_a_catalog_row() {
    let src = emitted();
    for record in [
        "config_map",
        "secret",
        "service",
        "service_account",
        "deployment",
        "network_policy",
        "role",
        "role_binding",
    ] {
        assert!(src.contains(&format!("\nrecord {record}(")), "{record}");
        assert!(
            src.contains(&format!("    {record}: {{")),
            "{record} catalog row"
        );
    }
}

#[test]
fn required_fields_are_typed_and_envelope_fields_are_not_fields() {
    let src = emitted();
    assert!(src.contains("record role_binding("));
    assert!(src.contains("role_ref: role_ref"));
    assert!(src.contains("containers: List(container)"));
    assert!(!src.contains("record deployment(api_version"));
}

#[test]
fn a_changed_schema_changes_the_records() {
    let schema = workspace().join("engenho-types/vendor/openapi/v1.34.0");
    let mut s = schemas::load_schemas(&schema).expect("vendored schemas load");
    let before = emit_blue_kinds(&s).expect("emit");
    s.get_mut("io.k8s.api.core.v1.ConfigMap")
        .expect("ConfigMap schema")
        .properties
        .insert("extraData".into(), serde_json::json!({"type": "string"}));
    let after = emit_blue_kinds(&s).expect("emit");
    assert_ne!(before, after);
    assert!(after.contains("extra_data"));
}
