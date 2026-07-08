//! Selector parity — `?labelSelector=` (the FULL grammar) + `?fieldSelector=`
//! against a seeded set of ConfigMaps, diffing the RETURNED ITEM SETS.
//!
//! Four ConfigMaps are seeded in a fresh namespace, each carrying a shared
//! `probe=cmset` label (so the oracle's auto-created `kube-root-ca.crt` — which
//! engenho's namespace lacks — is scoped OUT of every list) plus test labels:
//!
//!   cmA {tier=web, env=prod}   cmB {tier=api, env=prod}
//!   cmC {tier=web, env=dev}    cmD {}            (no tier / no env)
//!
//! Every label list is ANDed with `probe=cmset`. The grammar exercised:
//!   equality `k=v` · set `k in (a,b)` / `k notin (a)` · exists `k` /
//!   not-exists `!k` · inequality `k!=v` · a multi-clause AND · the two core
//!   field selectors `metadata.name=` / `metadata.namespace=`.
//!
//! Fail-loud on an unreachable oracle; RATCHET against [`KNOWN_DIVERGENCES`].

mod common;

use engenho_diff::{Operation, volatile_meta};

/// selector baseline.
///
/// The full label grammar + the two core field selectors run to PARITY (the
/// item sets match). The ONE recorded divergence is an EXOTIC field selector:
///
///   * `StatusCodeDiff:list_selector/exotic-field:GET` — `?fieldSelector=
///     status.phase=Running` on ConfigMaps. k3s returns 400 `field label not
///     supported: status.phase` (a ConfigMap has no such field); engenho
///     returns 200 with an empty list (it filters unsupported field keys out
///     rather than rejecting them). Closing this needs per-kind field-selector
///     REGISTRATION (the set of selectable fields per kind) engenho does not
///     yet carry — gated on that milestone.
const KNOWN_DIVERGENCES: &[&str] = &["StatusCodeDiff:list_selector/exotic-field:GET"];

#[tokio::test(flavor = "multi_thread")]
async fn selector_parity() {
    let engenho = common::boot_engenho(&["ConfigMap", "Namespace"]).await;
    let k3s = common::load_oracle().await;

    let norm = volatile_meta();
    let suffix = common::unique_suffix();
    let ns = {
        let mut s = String::from("engenho-diff-sel-");
        s.push_str(&suffix);
        s
    };

    // Seed (not diffed): namespace + 4 labeled ConfigMaps.
    let mut setup = vec![Operation::create_namespace(&ns)];
    setup.push(Operation::create_labeled_configmap(
        &ns,
        "cma",
        &[("probe", "cmset"), ("tier", "web"), ("env", "prod")],
    ));
    setup.push(Operation::create_labeled_configmap(
        &ns,
        "cmb",
        &[("probe", "cmset"), ("tier", "api"), ("env", "prod")],
    ));
    setup.push(Operation::create_labeled_configmap(
        &ns,
        "cmc",
        &[("probe", "cmset"), ("tier", "web"), ("env", "dev")],
    ));
    setup.push(Operation::create_labeled_configmap(
        &ns,
        "cmd",
        &[("probe", "cmset")],
    ));
    for op in &setup {
        common::exec_only(op, &engenho, &k3s).await;
    }

    // Every label list is ANDed with `probe=cmset` to exclude kube-root-ca.crt.
    let l = |sel: &str| {
        let mut s = String::from("probe=cmset,");
        s.push_str(sel);
        s
    };
    let ops = [
        // equality: tier=web → {cma, cmc}
        Operation::list_selector(&ns, "configmaps", "eq", Some(&l("tier=web")), None),
        // set-in: tier in (web,api) → {cma, cmb, cmc}
        Operation::list_selector(&ns, "configmaps", "in", Some(&l("tier in (web,api)")), None),
        // set-notin: tier notin (web) → {cmb, cmd}  (cmd lacks tier ⇒ included)
        Operation::list_selector(
            &ns,
            "configmaps",
            "notin",
            Some(&l("tier notin (web)")),
            None,
        ),
        // exists: tier → {cma, cmb, cmc}
        Operation::list_selector(&ns, "configmaps", "exists", Some(&l("tier")), None),
        // not-exists: !tier → {cmd}
        Operation::list_selector(&ns, "configmaps", "not-exists", Some(&l("!tier")), None),
        // inequality: tier!=web → {cmb, cmd}  (cmd lacks tier ⇒ included)
        Operation::list_selector(&ns, "configmaps", "ne", Some(&l("tier!=web")), None),
        // multi-clause AND: tier in (web),env=prod → {cma}
        Operation::list_selector(
            &ns,
            "configmaps",
            "multi",
            Some(&l("tier in (web),env=prod")),
            None,
        ),
        // field selector metadata.name (name isolates; no probe needed) → {cma}
        Operation::list_selector(
            &ns,
            "configmaps",
            "field-name",
            None,
            Some("metadata.name=cma"),
        ),
        // field selector metadata.namespace (probe-scoped) → {cma..cmd}
        Operation::list_selector(
            &ns,
            "configmaps",
            "field-ns",
            Some("probe=cmset"),
            Some(&{
                let mut s = String::from("metadata.namespace=");
                s.push_str(&ns);
                s
            }),
        ),
        // EXOTIC field selector — engenho 200+empty, k3s 400 (baselined).
        Operation::list_selector(
            &ns,
            "configmaps",
            "exotic-field",
            None,
            Some("status.phase=Running"),
        ),
    ];

    let mut findings = Vec::new();
    for op in &ops {
        findings.push(common::run_one(op, &engenho, &k3s, &norm).await);
    }

    common::cleanup(&k3s, &Operation::delete_namespace(&ns).path).await;

    common::print_report("SELECTOR", &findings);
    engenho.shutdown().await.expect("engenho shuts down");
    common::assert_ratchet(&findings, KNOWN_DIVERGENCES);
}
