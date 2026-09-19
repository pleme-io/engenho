//! Headroom census for the scheduler's capacity ledger (plan T1.5; the
//! seed of T0.10's `NodeLedger` check).
//!
//! ```text
//! kubectl get nodes -o json    > nodes.json
//! kubectl get pods -A -o json  > pods.json
//! cargo run -p engenho-scheduler --example ledger_headroom -- nodes.json pods.json
//! ```
//!
//! Runs the SAME [`NodeLedger::seed`] the scheduler runs at the top of every
//! tick, then prints each bound pod's charge and each node's signed headroom.
//! Read-only. Exits non-zero when any node is already charged past its
//! allocatable, which is the condition under which the ledger must not ship.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use engenho_scheduler::{NodeLedger, holds_capacity, pod_requests};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
enum CensusError {
    #[error("usage: ledger_headroom <nodes.json> <pods.json>")]
    Usage,
    #[error("reading {}: {source}", path.display())]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parsing {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("{}: not a List with an `items` array", path.display())]
    NotAList { path: PathBuf },
    #[error("{count} node(s) are already charged past their allocatable")]
    Overcommitted { count: usize },
}

fn items(path: &Path) -> Result<Vec<Value>, CensusError> {
    let raw = std::fs::read(path).map_err(|source| CensusError::Read {
        path: path.to_owned(),
        source,
    })?;
    let list: Value = serde_json::from_slice(&raw).map_err(|source| CensusError::Parse {
        path: path.to_owned(),
        source,
    })?;
    match list {
        Value::Object(mut map) => match map.remove("items") {
            Some(Value::Array(items)) => Ok(items),
            _ => Err(CensusError::NotAList {
                path: path.to_owned(),
            }),
        },
        _ => Err(CensusError::NotAList {
            path: path.to_owned(),
        }),
    }
}

fn text<'a>(v: &'a Value, pointer: &str) -> &'a str {
    v.pointer(pointer).and_then(Value::as_str).unwrap_or("-")
}

fn census() -> Result<(), CensusError> {
    let mut args = std::env::args_os().skip(1);
    let (Some(nodes), Some(pods), None) = (args.next(), args.next(), args.next()) else {
        return Err(CensusError::Usage);
    };
    let nodes = items(Path::new(&nodes))?;
    let pods = items(Path::new(&pods))?;

    for pod in &pods {
        let req = pod_requests(pod);
        println!(
            "pod {}/{} node={} phase={} hold={:?} cpu={}m memory={}B malformed={}",
            text(pod, "/metadata/namespace"),
            text(pod, "/metadata/name"),
            text(pod, "/spec/nodeName"),
            text(pod, "/status/phase"),
            holds_capacity(pod),
            req.cpu_milli,
            req.mem_milli / 1000,
            req.unparseable,
        );
    }

    let ledger = NodeLedger::seed(&nodes, &pods);
    let mut overcommitted = 0;
    for row in ledger.headroom() {
        println!("node {row}");
        if row.is_overcommitted() {
            overcommitted += 1;
        }
    }
    if overcommitted == 0 {
        Ok(())
    } else {
        Err(CensusError::Overcommitted {
            count: overcommitted,
        })
    }
}

fn main() -> ExitCode {
    match census() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
