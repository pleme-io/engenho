//! The compiled-in fixtures are well formed.

use std::collections::BTreeSet;

use engenho_oracle::Vector;

#[test]
fn every_table_loads_and_every_row_names_its_upstream_line() {
    for vector in Vector::ALL {
        let table = vector.load();
        assert!(!table.cases.is_empty(), "{vector} has no rows");
        for case in &table.cases {
            let reference = case.upstream_ref.as_deref().unwrap_or_default();
            assert!(
                !reference.is_empty(),
                "{vector}: {} names no upstream line",
                case.name
            );
        }
    }
}

#[test]
fn every_row_has_a_kind() {
    for vector in Vector::ALL {
        let table = vector.load();
        assert!(
            !table.kinds().contains("<no kind>"),
            "{vector} has a row without the field its kinds are keyed by"
        );
    }
}

#[test]
fn every_vector_file_has_a_variant() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("vectors");
    let on_disk: BTreeSet<String> = std::fs::read_dir(&dir)
        .expect("vectors/ is readable")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .map(|p| p.file_stem().expect("stem").to_string_lossy().into_owned())
        .collect();
    let declared: BTreeSet<String> = Vector::ALL.iter().map(|v| v.stem().to_owned()).collect();
    assert_eq!(on_disk, declared, "vectors/*.json and Vector::ALL disagree");
    for stem in &declared {
        assert!(
            dir.join(stem).with_extension("md").exists(),
            "vectors/{stem}.json has no .md"
        );
    }
}
