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

use engenho_kube_codegen::{KIND_CATALOG, KindEntry, OpenApiDoc, emit_kind, emit_module};

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

    // Per-module emission. Each module gets `<module>/{kind}.rs` files
    // + a `<module>/mod.rs` that re-exports them.
    let mut drift = false;
    for (module, entries) in &by_module {
        let module_dir = args.output.join(module);
        if !args.check {
            std::fs::create_dir_all(&module_dir)
                .with_context(|| format!("create {}", module_dir.display()))?;
        }

        // Emit each kind.
        for entry in entries {
            let doc = docs.get(entry.group).expect("doc loaded above");
            let shape = doc.shape(entry.openapi_key)?;
            let rust = emit_kind(entry, shape);
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

    // Top-level `lib.rs`-includable mod.rs.
    let top_mod = {
        let mut s = String::from(
            "//! GENERATED — engenho-kube-codegen — every K8s kind we currently emit.\n\n",
        );
        for module in by_module.keys() {
            s.push_str(&format!("pub mod {};\n", module));
        }
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
