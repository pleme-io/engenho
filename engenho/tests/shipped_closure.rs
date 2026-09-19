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
//! ## Tier, stated plainly
//!
//! A CI gate, not a type: a closure change is caught by this test, not made
//! impossible. It follows workspace crates only; which crates.io crates a
//! shipped crate pulls in is not checked here.

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
    "engenho-store",
    "engenho-substrate",
    "engenho-teia",
    "engenho-types",
];

/// Every workspace crate in no shipped binary, and why it is in the
/// workspace at all.
const UNSHIPPED: &[(&str, &str)] = &[
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
    /// Its normal (linked) dependencies on other workspace members.
    deps: Vec<Dep>,
}

/// A normal dependency edge to another workspace member.
#[derive(Debug)]
struct Dep {
    /// The member's package name.
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
    let names: BTreeSet<&str> = packages.iter().filter_map(|p| p["name"].as_str()).collect();
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
                    names.contains(package).then(|| Dep {
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

/// The workspace members linked into `roots` built with default features:
/// normal edges, optional ones only when a feature activates them, followed
/// to a fixed point.
fn closure(members: &BTreeMap<String, Package>, roots: &[&str]) -> BTreeSet<String> {
    // Enabled features per active package; a package is active once it has
    // an entry.
    let mut enabled: BTreeMap<String, BTreeSet<String>> = roots
        .iter()
        .map(|r| ((*r).to_owned(), BTreeSet::from(["default".to_owned()])))
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

/// THE GATE. The crates linked into the shipped binaries are exactly the
/// declared shipped set: a crate entering or leaving fails here until the
/// catalog above says so.
#[test]
fn the_shipped_closure_is_the_declared_one() {
    let members = members(&workspace_metadata());
    let linked = closure(&members, &ROOTS);
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
    let linked = closure(&members, &ROOTS);
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
