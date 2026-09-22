//! T5.1 — the shipped closure gate.
//!
//! engenho ships three packages: the `engenho` daemon, `engenho-mcp` and
//! `engenho-cluster-config-render`. Every workspace crate is either linked
//! into one of them or it is not, and this file declares which ([`SHIPPED`],
//! [`UNSHIPPED`], with a reason each). The test computes the closure the
//! manifests actually give — normal dependencies, default features, feature
//! activation followed through optional dependencies — and holds it equal to
//! the declaration. So a crate cannot ENTER the shipped binary (a new
//! dependency edge) or LEAVE it (an edge made optional) without a change to
//! this catalog in the same commit.
//!
//! ## What it reads
//!
//! The manifests, as cargo reads them: `cargo metadata --no-deps` (no
//! resolution, no network) gives each member's declared dependencies with
//! workspace inheritance already applied. Dev and build dependencies are not
//! linked into a binary, so they do not count. Target-specific dependencies
//! count whatever their target: the binaries ship to Linux and macOS, and a
//! crate linked on either is shipped.
//!
//! ## The crates.io edges it does read
//!
//! The walk keeps the crates.io dependencies a workspace crate declares, so
//! two named crates can be held out of the shipped binaries: `async-nats`
//! (T5.2 — engenho's fabric is `in_binary`, so the NATS client stays behind
//! `teia-nats`) and, from engenho-substrate-core, `tokio` and `serde_yaml`
//! (T5.6 — the core is the tokio-free half of the substrate carve).
//!
//! ## Tier, stated plainly
//!
//! A CI gate, not a type: a closure change is caught by this test, not made
//! impossible. It reads `--no-deps` metadata, so it follows edges DECLARED
//! BY WORKSPACE CRATES — a crates.io crate that pulls `async-nats` in
//! transitively is invisible here, and `cargo tree` is the check for that.
//! Nothing in the closure does today, which is why the gate is worth having
//! at this reach.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use serde_json::Value;

/// The packages engenho ships a binary of.
const ROOTS: [&str; 3] = ["engenho", "engenho-mcp", "engenho-cluster-config-render"];

/// Every workspace crate linked into a shipped binary.
const SHIPPED: &[&str] = &[
    "engenho",
    "engenho-apiserver",
    "engenho-cluster-config",
    "engenho-cluster-config-render",
    "engenho-cni",
    "engenho-config",
    "engenho-controllers",
    "engenho-csi",
    "engenho-etcd",
    "engenho-kube-client",
    "engenho-kube-proto",
    "engenho-kubelet",
    "engenho-mcp",
    "engenho-runtime",
    "engenho-scheduler",
    "engenho-serve",
    "engenho-store",
    "engenho-substrate",
    "engenho-substrate-core",
    "engenho-types",
];

/// Every workspace crate in no shipped binary, and why it is in the
/// workspace at all.
const UNSHIPPED: &[(&str, &str)] = &[
    (
        "engenho-substrate-incubator",
        "substrate modules no shipped crate depends on (the T5.6 carve)",
    ),
    (
        "engenho-teia",
        "the NATS fabric: optional since T5.2, behind engenho-store's teia-nats \
         feature, which is off by default",
    ),
    (
        "engenho-diff",
        "the differential test harness against a live k3s reference",
    ),
    (
        "engenho-fonte",
        "the source-of-truth reconciler; wired to revoada, not the daemon",
    ),
    (
        "engenho-fonte-cli",
        "fonte's loop over a mock universe (`--features mock-universe`)",
    ),
    (
        "engenho-kube-codegen",
        "the OpenAPI-to-Rust generator whose output engenho-types checks in",
    ),
    (
        "engenho-oracle",
        "upstream behaviour tables and their harness; a dev-dependency",
    ),
    (
        "engenho-revoada",
        "the multi-node distribution layer; the daemon does not link it",
    ),
    (
        "engenho-substrate-props",
        "substrate's property tests; a dev-dependency",
    ),
    (
        "engenho-sui-typescape",
        "the sui value bridge; reached only through fonte",
    ),
];

/// One workspace member, as its manifest declares it.
#[derive(Debug)]
struct Package {
    /// `[features]`: each feature and what it enables.
    features: BTreeMap<String, Vec<String>>,
    /// Its normal (linked) dependencies: workspace members and crates.io
    /// packages alike. Only members have a [`Package`] of their own, so the
    /// walk follows workspace edges and records crates.io ones as leaves.
    deps: Vec<Dep>,
}

/// A normal dependency edge, to a workspace member or a crates.io package.
#[derive(Debug)]
struct Dep {
    /// The depended-on package's name.
    package: String,
    /// The name the manifest refers to it by (its rename, or its name).
    key: String,
    optional: bool,
    default_features: bool,
    features: Vec<String>,
}

/// The workspace's members from `cargo metadata --no-deps` output.
fn members(metadata: &Value) -> BTreeMap<String, Package> {
    let packages = metadata["packages"]
        .as_array()
        .expect("cargo metadata lists packages");
    packages
        .iter()
        .map(|p| {
            let name = p["name"].as_str().expect("a package has a name").to_owned();
            let features = p["features"]
                .as_object()
                .map(|f| {
                    f.iter()
                        .map(|(k, v)| {
                            let items = v
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect();
                            (k.clone(), items)
                        })
                        .collect()
                })
                .unwrap_or_default();
            let deps = p["dependencies"]
                .as_array()
                .into_iter()
                .flatten()
                // `kind` is null for a normal dependency, "dev" or "build"
                // otherwise; only a normal one is linked into the binary.
                .filter(|d| d["kind"].is_null())
                .filter_map(|d| {
                    let package = d["name"].as_str()?;
                    Some(Dep {
                        package: package.to_owned(),
                        key: d["rename"].as_str().unwrap_or(package).to_owned(),
                        optional: d["optional"].as_bool().unwrap_or(false),
                        default_features: d["uses_default_features"].as_bool().unwrap_or(true),
                        features: d["features"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect(),
                    })
                })
                .collect();
            (name, Package { features, deps })
        })
        .collect()
}

/// The features a root carries when nothing asks for more.
const DEFAULT: &[&str] = &["default"];

/// Everything linked into `roots` built with default features: workspace
/// members and the crates.io packages they name, normal edges, optional ones
/// only when a feature activates them, followed to a fixed point.
fn closure(members: &BTreeMap<String, Package>, roots: &[&str]) -> BTreeSet<String> {
    let seeded: Vec<(&str, &[&str])> = roots.iter().map(|r| (*r, DEFAULT)).collect();
    closure_from(members, &seeded)
}

/// [`closure`], with each root's own feature set spelled out — the way to
/// ask what an off-by-default feature would pull in.
fn closure_from(
    members: &BTreeMap<String, Package>,
    roots: &[(&str, &[&str])],
) -> BTreeSet<String> {
    // Enabled features per active package; a package is active once it has
    // an entry.
    let mut enabled: BTreeMap<String, BTreeSet<String>> = roots
        .iter()
        .map(|(name, features)| {
            (
                (*name).to_owned(),
                features.iter().map(|f| (*f).to_owned()).collect(),
            )
        })
        .collect();
    loop {
        let before = enabled.clone();
        for (name, features) in &before {
            let Some(package) = members.get(name) else {
                continue;
            };
            // Close this package's own features and collect what they turn
            // on in its dependencies.
            let mut own = features.clone();
            let mut activated: BTreeSet<String> = BTreeSet::new();
            // `dep/feature` asks: a weak one (`dep?/feature`) applies only if
            // the dependency is active for another reason.
            let mut dep_features: Vec<(String, String)> = Vec::new();
            let mut queue: Vec<String> = own.iter().cloned().collect();
            while let Some(feature) = queue.pop() {
                for item in package.features.get(&feature).into_iter().flatten() {
                    if let Some(dep) = item.strip_prefix("dep:") {
                        activated.insert(dep.to_owned());
                    } else if let Some((dep, dep_feature)) = item.split_once('/') {
                        let weak = dep.strip_suffix('?');
                        if weak.is_none() {
                            activated.insert(dep.to_owned());
                        }
                        let dep = weak.unwrap_or(dep);
                        dep_features.push((dep.to_owned(), dep_feature.to_owned()));
                    } else if package.features.contains_key(item) {
                        if own.insert(item.clone()) {
                            queue.push(item.clone());
                        }
                    } else {
                        // An optional dependency's implicit feature.
                        activated.insert(item.clone());
                    }
                }
            }
            let entry = enabled.entry(name.clone()).or_default();
            entry.extend(own);
            for dep in &package.deps {
                if dep.optional && !activated.contains(&dep.key) {
                    continue;
                }
                let target = enabled.entry(dep.package.clone()).or_default();
                if dep.default_features {
                    target.insert("default".to_owned());
                }
                target.extend(dep.features.iter().cloned());
                target.extend(
                    dep_features
                        .iter()
                        .filter(|(key, _)| *key == dep.key)
                        .map(|(_, feature)| feature.clone()),
                );
            }
        }
        if enabled == before {
            return enabled.into_keys().collect();
        }
    }
}

/// `cargo metadata --no-deps` for this workspace.
fn workspace_metadata() -> Value {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/../Cargo.toml");
    let out = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
            manifest,
        ])
        .output()
        .expect("run cargo metadata");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("cargo metadata prints JSON")
}

fn set(names: impl IntoIterator<Item = impl Into<String>>) -> BTreeSet<String> {
    names.into_iter().map(Into::into).collect()
}

/// [`closure`] restricted to workspace members. The catalog above classifies
/// workspace crates; the crates.io packages the walk also reports are the
/// subject of their own tests further down.
fn workspace_closure(members: &BTreeMap<String, Package>, roots: &[&str]) -> BTreeSet<String> {
    closure(members, roots)
        .into_iter()
        .filter(|name| members.contains_key(name))
        .collect()
}

/// THE GATE. The crates linked into the shipped binaries are exactly the
/// declared shipped set: a crate entering or leaving fails here until the
/// catalog above says so.
#[test]
fn the_shipped_closure_is_the_declared_one() {
    let members = members(&workspace_metadata());
    let linked = workspace_closure(&members, &ROOTS);
    let declared = set(SHIPPED.iter().copied());
    let entered: Vec<&String> = linked.difference(&declared).collect();
    let left: Vec<&String> = declared.difference(&linked).collect();
    assert!(
        entered.is_empty() && left.is_empty(),
        "the shipped closure changed without a catalog change: \
         entered {entered:?} (move them to SHIPPED, or cut the edge), \
         left {left:?} (move them to UNSHIPPED with a reason)"
    );
}

/// The positive control: the closure reaches crates the daemon links only
/// through others (controllers via the runtime and the apiserver), and it
/// does not reach the distribution layer. A closure computation gone blind
/// would pass the gate above only if the catalog were blind too.
#[test]
fn the_closure_reaches_through_the_runtime_and_stops_at_revoada() {
    let members = members(&workspace_metadata());
    let linked = workspace_closure(&members, &ROOTS);
    for through in ["engenho-controllers", "engenho-store", "engenho-kubelet"] {
        assert!(linked.contains(through), "{through} is not in {linked:?}");
    }
    assert!(!linked.contains("engenho-revoada"), "{linked:?}");
}

/// Every workspace member is classified exactly once, and every unshipped
/// crate says why it is here. A new crate fails until it is declared.
#[test]
fn every_workspace_member_is_declared_shipped_or_not() {
    let members = members(&workspace_metadata());
    let shipped = set(SHIPPED.iter().copied());
    let unshipped = set(UNSHIPPED.iter().map(|(name, _)| *name));
    assert!(
        shipped.is_disjoint(&unshipped),
        "declared both: {:?}",
        shipped.intersection(&unshipped).collect::<Vec<_>>()
    );
    let declared: BTreeSet<String> = shipped.union(&unshipped).cloned().collect();
    let actual: BTreeSet<String> = members.keys().cloned().collect();
    assert_eq!(declared, actual, "the catalog and the workspace disagree");
    for (name, why) in UNSHIPPED {
        assert!(!why.trim().is_empty(), "{name} gives no reason");
    }
}

// ── the crates.io edges the closure must not carry ───────────────────────

/// The NATS client. engenho's fabric is `in_binary`, so a NATS server would
/// be a second process on every node (docs/IMPROVEMENT-PLAN.md §5.1).
const NATS_CLIENT: &str = "async-nats";

/// engenho-store's feature, off by default, that switches the NATS Raft
/// transport on and with it engenho-teia's client.
const NATS_FEATURE: &str = "teia-nats";

/// What engenho-substrate-core must not carry, and why. `tokio`: the core's
/// whole promise is that no async runtime is reachable from it (T5.6).
/// `serde_yaml`: parsing a config format is engenho-config's job, and the
/// core sits below it.
const CORE_MUST_NOT_CARRY: [&str; 2] = ["tokio", "serde_yaml"];

/// T5.2's integration half: no shipped binary links the NATS client.
///
/// engenho-teia's own fence (`engenho-teia/tests/nats_fence.rs`) pins teia's
/// manifest and says in as many words that it cannot see this — whether the
/// binary's closure is free of `async-nats` also depends on engenho-store
/// keeping engenho-teia optional, and on nothing in the closure turning
/// `teia-nats` on. That edge is what this test owns.
#[test]
fn no_shipped_binary_links_the_nats_client() {
    let members = members(&workspace_metadata());
    let linked = closure(&members, &ROOTS);
    assert!(
        !linked.contains(NATS_CLIENT),
        "a shipped binary reaches {NATS_CLIENT}: something in the closure \
         turned engenho-store's `{NATS_FEATURE}` on, or took a direct edge. \
         engenho's fabric is in_binary; the NATS client stays behind that \
         feature"
    );
}

/// The positive control for the test above, which could go green two vacuous
/// ways: a walk blind to crates.io edges altogether, or one blind to the
/// feature that carries this edge. So both are checked — the daemon's own
/// `tokio` is in the closure, and the same walk from engenho-store with
/// `teia-nats` on does arrive at the client.
#[test]
fn the_closure_sees_crates_io_edges_and_teia_nats_carries_the_client() {
    let members = members(&workspace_metadata());
    let linked = closure(&members, &ROOTS);
    assert!(
        linked.contains("tokio"),
        "the closure reports no crates.io edge at all, so \
         no_shipped_binary_links_the_nats_client would pass on any manifest"
    );
    let with_nats = closure_from(&members, &[("engenho-store", &["default", NATS_FEATURE])]);
    assert!(
        with_nats.contains(NATS_CLIENT),
        "`{NATS_FEATURE}` no longer reaches {NATS_CLIENT} (it reaches \
         {with_nats:?}); the fence moved, and the test above now proves \
         nothing"
    );
}

/// T5.1's core clause: nothing engenho-substrate-core links — directly, or
/// through another workspace crate — is an async runtime or a YAML parser.
///
/// The crate's own `tests/tokio_free.rs` bans those names in its manifest,
/// rename included. This asks the other half: the core's manifest could stay
/// clean and still carry `tokio` by taking one edge to a workspace crate
/// that has it. Direct ban there, transitive ban here.
#[test]
fn the_substrate_core_closure_carries_neither_a_runtime_nor_yaml() {
    let members = members(&workspace_metadata());
    let linked = closure(&members, &["engenho-substrate-core"]);
    let carried: Vec<&str> = CORE_MUST_NOT_CARRY
        .iter()
        .copied()
        .filter(|c| linked.contains(*c))
        .collect();
    assert!(
        carried.is_empty(),
        "engenho-substrate-core reaches {carried:?} (it links {linked:?}); \
         the module that needs it belongs in engenho-substrate, the leaf"
    );
}

/// The positive control for the core's carve: the same walk from the leaf
/// does carry the runtime the core refuses, and does re-export the core. So
/// the core's empty answer is the carve holding, not a walk that sees
/// nothing from a root with few edges.
#[test]
fn the_substrate_leaf_carries_the_runtime_the_core_refuses() {
    let members = members(&workspace_metadata());
    let leaf = closure(&members, &["engenho-substrate"]);
    assert!(
        leaf.contains("tokio"),
        "engenho-substrate no longer links tokio, so the core's clean answer \
         is unfalsifiable: {leaf:?}"
    );
    assert!(
        leaf.contains("engenho-substrate-core"),
        "the leaf no longer re-exports the core: {leaf:?}"
    );
}

// ── the closure computation, on manifests built to exercise it ───────────

fn manifest(packages: &Value) -> BTreeMap<String, Package> {
    members(&serde_json::json!({ "packages": packages }))
}

fn dep(name: &str) -> Value {
    serde_json::json!({
        "name": name, "rename": null, "kind": null, "optional": false,
        "uses_default_features": true, "features": []
    })
}

/// Normal edges are followed transitively; dev and build edges are not.
#[test]
fn the_closure_follows_normal_edges_only() {
    let mut dev = dep("d");
    dev["kind"] = "dev".into();
    let mut build = dep("b");
    build["kind"] = "build".into();
    let m = manifest(&serde_json::json!([
        {"name": "root", "features": {}, "dependencies": [dep("a"), dev, build]},
        {"name": "a", "features": {}, "dependencies": [dep("c")]},
        {"name": "c", "features": {}, "dependencies": []},
        {"name": "d", "features": {}, "dependencies": []},
        {"name": "b", "features": {}, "dependencies": []},
    ]));
    assert_eq!(closure(&m, &["root"]), set(["root", "a", "c"]));
}

/// An optional dependency is linked only when a feature in force activates
/// it: through the default feature, through a feature a dependent asks for,
/// or through `dep/feature`.
#[test]
fn an_optional_dependency_is_linked_only_when_activated() {
    let mut off = dep("off");
    off["optional"] = true.into();
    let mut by_default = dep("by-default");
    by_default["optional"] = true.into();
    let mut asked = dep("mid");
    asked["features"] = serde_json::json!(["extra"]);
    let mut inner = dep("inner");
    inner["optional"] = true.into();
    let mut renamed = dep("real-name");
    renamed["rename"] = "alias".into();
    renamed["optional"] = true.into();
    let m = manifest(&serde_json::json!([
        {"name": "root",
         "features": {"default": ["dep:by-default", "alias/x"], "unused": ["dep:off"]},
         "dependencies": [off, by_default, asked, renamed]},
        {"name": "off", "features": {}, "dependencies": []},
        {"name": "by-default", "features": {}, "dependencies": []},
        {"name": "mid", "features": {"extra": ["dep:inner"]}, "dependencies": [inner]},
        {"name": "inner", "features": {}, "dependencies": []},
        {"name": "real-name", "features": {"x": []}, "dependencies": []},
    ]));
    assert_eq!(
        closure(&m, &["root"]),
        set(["root", "by-default", "mid", "inner", "real-name"])
    );
}

/// A weak `dep?/feature` turns the feature on only in a dependency already
/// linked for another reason; it never links the dependency itself.
#[test]
fn a_weak_dependency_feature_links_nothing_by_itself() {
    let packages = |default: Value| {
        let mut weak = dep("weak");
        weak["optional"] = true.into();
        let mut inner = dep("inner");
        inner["optional"] = true.into();
        manifest(&serde_json::json!([
            {"name": "root", "features": {"default": default}, "dependencies": [weak]},
            {"name": "weak", "features": {"extra": ["dep:inner"]}, "dependencies": [inner]},
            {"name": "inner", "features": {}, "dependencies": []},
        ]))
    };
    assert_eq!(
        closure(&packages(serde_json::json!(["weak?/extra"])), &["root"]),
        set(["root"])
    );
    assert_eq!(
        closure(
            &packages(serde_json::json!(["weak?/extra", "dep:weak"])),
            &["root"]
        ),
        set(["root", "weak", "inner"])
    );
}
