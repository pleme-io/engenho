//! NODE CLUSTERS — the per-node, baremetal engenho control plane.
//!
//! ★ WHY THIS MODULE EXISTS. `KikaiClusterReader` sources every answer from
//! kikai's VM registry: `~/.config/kikai/clusters.yaml` for the roster,
//! `vm.pid` for liveness, `snapshots/auto.vmstate` for recoverability, and
//! `~/.kube/configs/<CLUSTER-NAME>` for the API. Every one of those is a VM
//! fact, and engenho does not run in a VM on darwin — it runs the whole
//! control plane as a native process on the host.
//!
//! The consequence, measured on cid 2026-08-29: the operator's live cluster
//! `engenho-cid-f3c36831` was UNADDRESSABLE. `cluster_status` on the only
//! name the reader knew (`engenho-local`) reported `VM: fail — no vm.pid`
//! and `API: fail — kubeconfig absent` while the cluster was serving
//! happily on `127.0.0.1:6443`, and asking for the real name was refused
//! with `legal: ["engenho-local"]` — a refusal that was correct about the
//! question and actively misleading about the world.
//!
//! ★ THE SPLIT THIS ENCODES: THE PATH IS STABLE, THE NAME IS NOT.
//! engenho names its cluster `engenho-<node>-<blake3-8>`, and that hash
//! cannot be computed ahead of time — Nix offers md5/sha1/sha256/sha512 and
//! no blake3, which is why `modules/shared/engenho-kubeconfig.nix` in the
//! nix repo pins the PATH and lets the NAME live inside the file, and why
//! banken is wired with `--sole-context-of <path>` rather than
//! `--context <name>`. This module is the same decision in the same shape:
//! resolve the roster BY READING the kubeconfig, never by spelling a name.
//!
//! ★ SOLE-CONTEXT, AND WHY ZERO AND TWO ARE BOTH REFUSALS. engenho
//! publishes exactly one context into this file. Zero means the control
//! plane has not bootstrapped and there is nothing to name; two or more
//! means the file is not the one this module thinks it is, and picking
//! either would serve an operator a cluster they did not ask for under a
//! name they did not choose — the precise failure banken's `--sole-context-of`
//! refusal exists to prevent (demonstrated 2026-08-28, when a context-less
//! launch landed on an EKS estate whose SSO token had expired).

use std::path::{Path, PathBuf};

use engenho_kube_client::config::Kubeconfig;

/// The node's own engenho cluster: the name it calls itself, and the
/// kubeconfig that named it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCluster {
    /// `engenho-<node>-<blake3-8>`, read from the kubeconfig's sole context.
    pub name: String,
    /// The stable path — `~/.kube/configs/engenho`, NOT `.../<name>`.
    pub kubeconfig: PathBuf,
    /// API server URL from the context's cluster entry, for the status row.
    pub server: String,
}

/// Why discovery found nothing. Not an error type: a node with no engenho
/// cluster is an ordinary, expected state (a VM-only workstation, a machine
/// before first bootstrap), and it must not turn every unrelated MCP call
/// into a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoNodeCluster {
    /// No kubeconfig at the stable path — not bootstrapped.
    Absent,
    /// Present but unreadable or unparseable.
    Unreadable(String),
    /// Present with a context count that cannot be resolved to one cluster.
    NotSoleContext(usize),
}

/// The stable, per-node kubeconfig path. A CONSTANT, deliberately: the
/// whole point is that this does not vary with the cluster's name.
pub fn kubeconfig_path(home: &Path) -> PathBuf {
    home.join(".kube/configs/engenho")
}

/// Resolve the node's engenho cluster, or say precisely why there isn't one.
pub fn discover(home: &Path) -> Result<NodeCluster, NoNodeCluster> {
    let path = kubeconfig_path(home);
    if !path.exists() {
        return Err(NoNodeCluster::Absent);
    }
    let kc = Kubeconfig::load(&path).map_err(|e| NoNodeCluster::Unreadable(e.to_string()))?;
    resolve(&kc, path)
}

/// The pure half — separated from the filesystem so every branch is
/// testable without a kubeconfig on disk.
pub fn resolve(kc: &Kubeconfig, path: PathBuf) -> Result<NodeCluster, NoNodeCluster> {
    if kc.contexts.len() != 1 {
        return Err(NoNodeCluster::NotSoleContext(kc.contexts.len()));
    }
    let ctx = &kc.contexts[0];
    // The CLUSTER name, not the CONTEXT name. They are conventionally equal
    // here, but the API is addressed by cluster and conflating the two would
    // make the reader's roster disagree with the object it then serves.
    let cluster_name = ctx.context.cluster.clone();
    let server = kc
        .clusters
        .iter()
        .find(|c| c.name == cluster_name)
        .map(|c| c.cluster.server.clone())
        .unwrap_or_default();
    Ok(NodeCluster {
        name: cluster_name,
        kubeconfig: path,
        server,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_kube_client::config::{Cluster, Context, NamedCluster, NamedContext};

    fn kc(contexts: Vec<(&str, &str)>, clusters: Vec<(&str, &str)>) -> Kubeconfig {
        Kubeconfig {
            api_version: "v1".into(),
            kind: "Config".into(),
            current_context: contexts
                .first()
                .map(|c| c.0.to_string())
                .unwrap_or_default(),
            clusters: clusters
                .into_iter()
                .map(|(name, server)| NamedCluster {
                    name: name.into(),
                    cluster: Cluster {
                        server: server.into(),
                        certificate_authority_data: None,
                        certificate_authority: None,
                        insecure_skip_tls_verify: false,
                    },
                })
                .collect(),
            users: vec![],
            contexts: contexts
                .into_iter()
                .map(|(name, cluster)| NamedContext {
                    name: name.into(),
                    context: Context {
                        cluster: cluster.into(),
                        user: "engenho-admin".into(),
                        namespace: None,
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn resolves_the_per_node_name_from_the_file_not_the_path() {
        // The whole point: the path says `engenho`, the cluster is called
        // `engenho-cid-f3c36831`, and only the file knows that.
        let k = kc(
            vec![("engenho-cid-f3c36831", "engenho-cid-f3c36831")],
            vec![("engenho-cid-f3c36831", "https://127.0.0.1:6443")],
        );
        let n = resolve(&k, PathBuf::from("/h/.kube/configs/engenho")).expect("resolves");
        assert_eq!(n.name, "engenho-cid-f3c36831");
        assert_eq!(n.server, "https://127.0.0.1:6443");
        assert!(
            n.kubeconfig.ends_with("configs/engenho"),
            "the stable path is kept, never rebuilt from the name"
        );
    }

    #[test]
    fn the_cluster_name_wins_over_the_context_name() {
        // Conventionally equal, so a reader that took `ctx.name` would pass
        // every realistic fixture and still be wrong.
        let k = kc(
            vec![("shorthand", "engenho-cid-f3c36831")],
            vec![("engenho-cid-f3c36831", "https://127.0.0.1:6443")],
        );
        let n = resolve(&k, PathBuf::from("/h/.kube/configs/engenho")).expect("resolves");
        assert_eq!(n.name, "engenho-cid-f3c36831");
    }

    #[test]
    fn zero_and_many_contexts_are_both_refused() {
        let empty = kc(vec![], vec![]);
        assert_eq!(
            resolve(&empty, PathBuf::from("/h/x")),
            Err(NoNodeCluster::NotSoleContext(0))
        );
        let two = kc(
            vec![("a", "a"), ("b", "b")],
            vec![("a", "https://a:6443"), ("b", "https://b:6443")],
        );
        assert_eq!(
            resolve(&two, PathBuf::from("/h/x")),
            Err(NoNodeCluster::NotSoleContext(2))
        );
    }

    #[test]
    fn a_context_naming_an_absent_cluster_still_resolves_a_name() {
        // A malformed file must not cost the operator the ROSTER entry —
        // the name is what makes the cluster addressable at all, and an
        // empty server merely makes the status row say so.
        let k = kc(vec![("c", "engenho-cid-deadbeef")], vec![]);
        let n = resolve(&k, PathBuf::from("/h/x")).expect("resolves");
        assert_eq!(n.name, "engenho-cid-deadbeef");
        assert_eq!(n.server, "");
    }

    #[test]
    fn an_absent_kubeconfig_is_absent_not_an_error() {
        let dir = std::env::temp_dir().join(format!("engenho-node-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        assert_eq!(discover(&dir), Err(NoNodeCluster::Absent));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
