//! `engenho-cluster-config-render` — typed-config → on-disk artifacts.
//!
//! Reads a YAML [`ClusterConfig`] from `--input`, validates, and writes:
//!
//! * `<output-dir>/config.yaml`       — `/etc/rancher/k3s/config.yaml`
//! * `<output-dir>/server-args.txt`   — newline-separated extra cmdline flags
//! * `<output-dir>/manifests/<name>.yaml` — one per [`Manifest`]
//!
//! Used by nixos-k3s-vm at NixOS build time (via `runCommand`) so each
//! VM image bakes its artifacts deterministically. Re-running with the
//! same input produces byte-identical outputs (the renderer is a pure
//! function of [`ClusterConfig`]).

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use engenho_cluster_config::ClusterConfig;

#[derive(Parser, Debug)]
#[command(name = "engenho-cluster-config-render", version, about)]
struct Args {
    /// Path to the YAML ClusterConfig.
    #[arg(long)]
    input: PathBuf,

    /// Directory to write artifacts. Created if absent.
    #[arg(long)]
    output_dir: PathBuf,

    /// Emit JSON to stdout summarising what was written (paths + bytes).
    /// Useful for nix consumers to assert the expected set landed.
    #[arg(long)]
    summary_json: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.input)
        .with_context(|| format!("read {}", args.input.display()))?;
    let cfg = ClusterConfig::from_yaml_str(&raw)
        .with_context(|| format!("parse + validate {}", args.input.display()))?;

    std::fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create {}", args.output_dir.display()))?;
    let manifests_dir = args.output_dir.join("manifests");
    std::fs::create_dir_all(&manifests_dir)
        .with_context(|| format!("create {}", manifests_dir.display()))?;

    // config.yaml
    let config_yaml = cfg.render_k3s_config_yaml();
    let config_path = args.output_dir.join("config.yaml");
    std::fs::write(&config_path, &config_yaml)
        .with_context(|| format!("write {}", config_path.display()))?;

    // server-args.txt
    let args_text = cfg.render_k3s_server_args().join("\n") + "\n";
    let args_path = args.output_dir.join("server-args.txt");
    std::fs::write(&args_path, &args_text)
        .with_context(|| format!("write {}", args_path.display()))?;

    // manifests/*.yaml
    let manifests = cfg.render_bootstrap_manifests();
    let mut manifest_paths = Vec::with_capacity(manifests.len());
    for m in &manifests {
        let p = manifests_dir.join(&m.filename);
        std::fs::write(&p, &m.body)
            .with_context(|| format!("write {}", p.display()))?;
        manifest_paths.push(p);
    }

    if args.summary_json {
        let summary = serde_json::json!({
            "config_yaml":  config_path,
            "server_args":  args_path,
            "manifests":    manifest_paths,
            "cluster_name": cfg.cluster_name,
            "node_ip":      cfg.node_ip,
        });
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        eprintln!("wrote config + {} args + {} manifests to {}",
            cfg.render_k3s_server_args().len(),
            manifests.len(),
            args.output_dir.display(),
        );
    }
    Ok(())
}
