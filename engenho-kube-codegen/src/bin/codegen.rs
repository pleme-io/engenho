//! `engenho-kube-codegen` — emit typed kinds from vendored OpenAPI v3.
//!
//! Two modes:
//!
//!   * **Default** — emit each kind's `.rs` file under `--output`,
//!     overwriting any existing source. Used to regenerate after
//!     changes to `catalog.rs` or after a new K8s minor is vendored.
//!
//!   * **`--check`** — re-emit + diff against the on-disk source.
//!     Exits non-zero on drift. Used by CI to enforce the determinism
//!     contract (theory/ENGENHO.md §VI.1).

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use engenho_kube_codegen::{
    KIND_CATALOG, KindEntry, OpenApiDoc, SchemaView, emit_catalog, emit_kind_typed, emit_module,
    emit_shared_module, shared_substructs,
};

#[derive(Parser, Debug)]
#[command(name = "engenho-kube-codegen", version, about)]
struct Args {
    /// Path to the vendored OpenAPI v3 schema directory containing
    /// `api__v1_openapi.json`, `apis__apps__v1_openapi.json`,
    /// `apis__rbac.authorization.k8s.io__v1_openapi.json`.
    #[arg(long)]
    schema: PathBuf,

    /// Output directory under engenho-types/src. The generator
    /// creates one subdirectory per module (`core_v1/`, `apps_v1/`,
    /// `rbac_v1/`) + a top-level `mod.rs` listing them.
    #[arg(long)]
    output: PathBuf,

    /// Don't write anything; diff against existing source instead.
    /// Exit non-zero on drift. Used by CI.
    #[arg(long)]
    check: bool,
}

fn schema_path_for_group(schema_dir: &std::path::Path, group: &str) -> PathBuf {
    let fname = match group {
        "" => "api__v1_openapi.json",
        "apps" => "apis__apps__v1_openapi.json",
        "rbac.authorization.k8s.io" => "apis__rbac.authorization.k8s.io__v1_openapi.json",
        other => panic!(
            "no vendored OpenAPI schema for group {other:?} — extend codegen::schema_path_for_group"
        ),
    };
    schema_dir.join(fname)
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Group catalog entries by module (core_v1, apps_v1, rbac_v1).
    // BTreeMap so emission order is deterministic across runs.
    let mut by_module: BTreeMap<&'static str, Vec<&KindEntry>> = BTreeMap::new();
    for e in KIND_CATALOG {
        by_module.entry(e.module).or_default().push(e);
    }

    // Cache loaded OpenAPI docs per group so we don't re-parse.
    let mut docs: BTreeMap<&'static str, OpenApiDoc> = BTreeMap::new();
    for entry in KIND_CATALOG {
        if !docs.contains_key(entry.group) {
            let path = schema_path_for_group(&args.schema, entry.group);
            let doc = OpenApiDoc::load(&path)
                .with_context(|| format!("load schema for group {:?}", entry.group))?;
            docs.insert(entry.group, doc);
        }
    }

    // Merge every group's components.schemas into one map so the typed
    // emitter can resolve $refs across groups (e.g. apps Deployment →
    // core PodTemplateSpec). BTreeMap → deterministic; first writer wins
    // (identical schema keys across files carry identical bodies).
    let mut schemas: BTreeMap<String, SchemaView> = BTreeMap::new();
    for doc in docs.values() {
        for (key, shape) in &doc.components.schemas {
            schemas.entry(key.clone()).or_insert_with(|| SchemaView {
                description: shape.description.clone(),
                properties: shape.properties.clone(),
                required: shape.required.clone(),
            });
        }
    }

    // Per-module emission. Each module gets `<module>/{kind}.rs` files
    // + a `<module>/mod.rs` that re-exports them.
    let mut drift = false;

    // Emit the shared sub-struct module (types.rs) once — every kind's $ref
    // closure, globally deduplicated into one canonical set.
    {
        let kind_keys: Vec<&str> = KIND_CATALOG.iter().map(|e| e.openapi_key).collect();
        let kind_names: Vec<&str> = KIND_CATALOG.iter().map(|e| e.kind).collect();
        let shared = shared_substructs(&kind_keys, &kind_names, &schemas);
        let shared_src = emit_shared_module(&shared, KIND_CATALOG);
        let shared_target = args.output.join("types.rs");
        if args.check {
            let existing = std::fs::read_to_string(&shared_target).unwrap_or_default();
            if existing != shared_src {
                eprintln!("DRIFT: {}", shared_target.display());
                drift = true;
            }
        } else {
            std::fs::create_dir_all(&args.output)
                .with_context(|| format!("create {}", args.output.display()))?;
            std::fs::write(&shared_target, shared_src)
                .with_context(|| format!("write {}", shared_target.display()))?;
        }
    }

    for (module, entries) in &by_module {
        let module_dir = args.output.join(module);
        if !args.check {
            std::fs::create_dir_all(&module_dir)
                .with_context(|| format!("create {}", module_dir.display()))?;
        }

        // Emit each kind (typed: walks properties + transitive $ref closure).
        for entry in entries {
            let view = schemas.get(entry.openapi_key).ok_or_else(|| {
                anyhow::anyhow!("kind {:?} not in merged schemas", entry.openapi_key)
            })?;
            let rust = emit_kind_typed(entry, view, &schemas);
            let target = module_dir.join(format!("{}.rs", entry.kind.to_lowercase()));
            if args.check {
                let existing = std::fs::read_to_string(&target).unwrap_or_default();
                if existing != rust {
                    eprintln!("DRIFT: {}", target.display());
                    drift = true;
                }
            } else {
                std::fs::write(&target, rust)
                    .with_context(|| format!("write {}", target.display()))?;
            }
        }

        // Emit module-level mod.rs.
        let mod_rs = emit_module(module, entries);
        let mod_target = module_dir.join("mod.rs");
        if args.check {
            let existing = std::fs::read_to_string(&mod_target).unwrap_or_default();
            if existing != mod_rs {
                eprintln!("DRIFT: {}", mod_target.display());
                drift = true;
            }
        } else {
            std::fs::write(&mod_target, mod_rs)
                .with_context(|| format!("write {}", mod_target.display()))?;
        }
    }

    // Runtime-iterable catalog (one ResourceDescriptor per KIND_CATALOG
    // entry). The single most important generated artifact for M0.1 group
    // routing/discovery — drives handler construction, routing keys, and
    // discovery from ONE source. Plural = entry.resource verbatim.
    {
        let catalog_src = emit_catalog(KIND_CATALOG);
        let catalog_target = args.output.join("catalog.rs");
        if args.check {
            let existing = std::fs::read_to_string(&catalog_target).unwrap_or_default();
            if existing != catalog_src {
                eprintln!("DRIFT: {}", catalog_target.display());
                drift = true;
            }
        } else {
            std::fs::create_dir_all(&args.output)
                .with_context(|| format!("create {}", args.output.display()))?;
            std::fs::write(&catalog_target, catalog_src)
                .with_context(|| format!("write {}", catalog_target.display()))?;
        }
    }

    // Top-level `lib.rs`-includable mod.rs.
    let top_mod = {
        let mut s = String::from(
            "//! GENERATED — engenho-kube-codegen — every K8s kind we currently emit.\n\n",
        );
        for module in by_module.keys() {
            s.push_str(&format!("pub mod {};\n", module));
        }
        // Shared sub-structs module + flat re-export.
        s.push_str("pub mod types;\npub use types::*;\n");
        // Runtime-iterable resource catalog + flat re-export.
        s.push_str("pub mod catalog;\npub use catalog::*;\n");
        s
    };
    let top_target = args.output.join("mod.rs");
    if args.check {
        let existing = std::fs::read_to_string(&top_target).unwrap_or_default();
        if existing != top_mod {
            eprintln!("DRIFT: {}", top_target.display());
            drift = true;
        }
    } else {
        std::fs::create_dir_all(&args.output)
            .with_context(|| format!("create {}", args.output.display()))?;
        std::fs::write(&top_target, top_mod)
            .with_context(|| format!("write {}", top_target.display()))?;
    }

    if args.check && drift {
        anyhow::bail!("generated source is stale — re-run without --check to regenerate");
    }
    eprintln!(
        "engenho-kube-codegen: {} kinds across {} modules",
        KIND_CATALOG.len(),
        by_module.len(),
    );
    Ok(())
}
