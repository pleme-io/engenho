//! `engenho-kube-codegen` — emit typed kinds from vendored `OpenAPI` v3.
//!
//! Two modes:
//!
//!   * **Default** — emit each kind's `.rs` file under `--output`,
//!     overwriting any existing source. Used to regenerate after
//!     changes to `catalog.rs` or after a new K8s minor is vendored.
//!
//!   * **`--check`** — re-emit + diff against the on-disk source.
//!     Exits non-zero on drift. Used by CI to enforce the determinism
//!     contract.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

use engenho_kube_codegen::{
    KIND_CATALOG, KindEntry, emit_blue_kinds, emit_catalog, emit_kind_typed, emit_module,
    emit_shared_module, format_source, schemas, shared_substructs,
};

#[derive(Parser, Debug)]
#[command(name = "engenho-kube-codegen", version, about)]
struct Args {
    /// Path to the vendored `OpenAPI` v3 schema directory containing
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

    #[arg(long)]
    blue: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Group catalog entries by module (core_v1, apps_v1, rbac_v1).
    // BTreeMap so emission order is deterministic across runs.
    //
    // OPAQUE kinds (no vendored OpenAPI schema, no typed struct — e.g.
    // `apiextensions.k8s.io/v1.CustomResourceDefinition`, stored as plain
    // JSON) are EXCLUDED from per-module struct emission here. They have no
    // `module` / `openapi_key`, so they only appear as a `RESOURCE_CATALOG`
    // row (emitted below over the FULL `KIND_CATALOG`). Skipping them keeps
    // `schema_path_for_group` from panicking on a group with no vendored
    // schema + keeps the merged-schemas lookup from failing.
    let mut by_module: BTreeMap<&'static str, Vec<&KindEntry>> = BTreeMap::new();
    for e in KIND_CATALOG {
        if e.opaque {
            continue;
        }
        by_module.entry(e.module).or_default().push(e);
    }

    let docs = schemas::load_docs(&args.schema)?;
    let schemas = schemas::merge(&docs);

    // Per-module emission. Each module gets `<module>/{kind}.rs` files
    // + a `<module>/mod.rs` that re-exports them.
    let mut drift = false;

    // Emit the shared sub-struct module (types.rs) once — every kind's $ref
    // closure, globally deduplicated into one canonical set.
    {
        // Opaque kinds have no schema → no shared sub-structs to seed.
        let kind_keys: Vec<&str> = KIND_CATALOG
            .iter()
            .filter(|e| !e.opaque)
            .map(|e| e.openapi_key)
            .collect();
        let kind_names: Vec<&str> = KIND_CATALOG
            .iter()
            .filter(|e| !e.opaque)
            .map(|e| e.kind)
            .collect();
        let shared = shared_substructs(&kind_keys, &kind_names, &schemas);
        let shared_src = emit_shared_module(&shared, KIND_CATALOG);
        let shared_src = format_source(&shared_src)?;
        settle(
            &args.output.join("types.rs"),
            &shared_src,
            args.check,
            &mut drift,
        )?;
    }

    for (module, entries) in &by_module {
        let module_dir = args.output.join(module);

        // Emit each kind (typed: walks properties + transitive $ref closure).
        for entry in entries {
            let view = schemas.get(entry.openapi_key).ok_or_else(|| {
                anyhow::anyhow!("kind {:?} not in merged schemas", entry.openapi_key)
            })?;
            let rust = emit_kind_typed(entry, view, &schemas);
            let rust = format_source(&rust)?;
            let target = module_dir.join(format!("{}.rs", entry.kind.to_lowercase()));
            settle(&target, &rust, args.check, &mut drift)?;
        }

        // Emit module-level mod.rs.
        let mod_rs = emit_module(module, entries);
        let mod_rs = format_source(&mod_rs)?;
        settle(&module_dir.join("mod.rs"), &mod_rs, args.check, &mut drift)?;
    }

    // Runtime-iterable catalog (one ResourceDescriptor per KIND_CATALOG
    // entry). The single most important generated artifact for M0.1 group
    // routing/discovery — drives handler construction, routing keys, and
    // discovery from ONE source. Plural = entry.resource verbatim.
    {
        let catalog_src = emit_catalog(KIND_CATALOG);
        let catalog_src = format_source(&catalog_src)?;
        settle(
            &args.output.join("catalog.rs"),
            &catalog_src,
            args.check,
            &mut drift,
        )?;
    }

    // Top-level `lib.rs`-includable mod.rs.
    let top_mod = {
        let mut s = String::from(
            "//! GENERATED — engenho-kube-codegen — every K8s kind we currently emit.\n\n",
        );
        for module in by_module.keys() {
            let _ = writeln!(s, "pub mod {module};");
        }
        // Shared sub-structs module + flat re-export.
        s.push_str("pub mod types;\npub use types::*;\n");
        // Runtime-iterable resource catalog + flat re-export.
        s.push_str("pub mod catalog;\npub use catalog::*;\n");
        s
    };
    settle(
        &args.output.join("mod.rs"),
        &top_mod,
        args.check,
        &mut drift,
    )?;

    if let Some(blue_target) = &args.blue {
        let blue_src = emit_blue_kinds(&schemas)?;
        if args.check {
            let existing = std::fs::read_to_string(blue_target).unwrap_or_default();
            if existing != blue_src {
                eprintln!("DRIFT: {}", blue_target.display());
                drift = true;
            }
        } else {
            if let Some(dir) = blue_target.parent() {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("create {}", dir.display()))?;
            }
            std::fs::write(blue_target, blue_src)
                .with_context(|| format!("write {}", blue_target.display()))?;
        }
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

fn settle(target: &Path, src: &str, check: bool, drift: &mut bool) -> Result<()> {
    if check {
        let existing = std::fs::read_to_string(target).unwrap_or_default();
        if existing != src {
            eprintln!("DRIFT: {}", target.display());
            *drift = true;
        }
        return Ok(());
    }
    if let Some(dir) = target.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    std::fs::write(target, src).with_context(|| format!("write {}", target.display()))
}
