//! The harness is closed in both directions: every row is claimed, and every
//! exemption is live. Each rule has one row here that breaks it.

use engenho_oracle::{Answer, Case, Deviation, Failure, OutOfScope, Table, Vector, run};
use serde_json::{Value, json};

/// Three rows over two kinds. Kinds come from `input.kind` in the prober table.
fn table() -> Table {
    let json = json!({"cases": [
        {"name": "a1", "input": {"kind": "alpha", "x": 1}, "expected": {"y": 2, "z": 3}, "upstream_ref": "a.go:1"},
        {"name": "a2", "input": {"kind": "alpha", "x": 5}, "expected": {"y": 6, "z": 7}, "upstream_ref": "a.go:2"},
        {"name": "b1", "input": {"kind": "beta"}, "expected": [1, 2], "upstream_ref": "b.go:1"},
    ]});
    Table::parse(Vector::ProberResults, &json.to_string()).expect("synthetic table parses")
}

fn kind(case: &Case) -> &str {
    case.input["kind"].as_str().unwrap_or_default()
}

/// An adapter that reproduces upstream exactly.
fn faithful(case: &Case) -> Answer {
    Answer::Checked(case.expected.clone())
}

fn failures(result: Result<engenho_oracle::Report, engenho_oracle::Failures>) -> Vec<Failure> {
    result.expect_err("the run should fail").failures
}

const NONE: &[OutOfScope] = &[];
const NO_DEV: &[Deviation] = &[];

#[test]
fn a_faithful_adapter_passes() {
    let report = run(&table(), NONE, NO_DEV, faithful).expect("passes");
    assert_eq!(
        (
            report.checked,
            report.agree,
            report.deviations,
            report.out_of_scope
        ),
        (3, 3, 0, 0)
    );
    assert!(report.unchecked_keys.is_empty());
}

#[test]
fn a_wrong_value_fails() {
    let got = failures(run(&table(), NONE, NO_DEV, |c: &Case| {
        if c.name == "a2" {
            Answer::Checked(json!({"y": 6, "z": 0}))
        } else {
            faithful(c)
        }
    }));
    assert!(
        matches!(&got[..], [Failure::Disagrees { case, .. }] if case == "a2"),
        "{got:?}"
    );
}

#[test]
fn a_wrong_whole_value_fails() {
    let got = failures(run(&table(), NONE, NO_DEV, |c: &Case| {
        if c.name == "b1" {
            Answer::Checked(json!([1]))
        } else {
            faithful(c)
        }
    }));
    assert!(
        matches!(&got[..], [Failure::Disagrees { case, .. }] if case == "b1"),
        "{got:?}"
    );
}

#[test]
fn a_subset_of_keys_passes_and_the_rest_is_reported() {
    let report = run(&table(), NONE, NO_DEV, |c: &Case| {
        if kind(c) == "alpha" {
            Answer::Checked(json!({"y": c.expected["y"]}))
        } else {
            faithful(c)
        }
    })
    .expect("passes");
    let unchecked: Vec<&str> = report.unchecked_keys["alpha"]
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(unchecked, ["z"]);
}

#[test]
fn an_unshaped_answer_fails() {
    for bad in [json!({}), json!({"y": 2, "extra": 1}), json!(2)] {
        let got = failures(run(&table(), NONE, NO_DEV, |c: &Case| {
            if c.name == "a1" {
                Answer::Checked(bad.clone())
            } else {
                faithful(c)
            }
        }));
        assert!(
            matches!(&got[..], [Failure::Unshaped { case, .. }] if case == "a1"),
            "{bad}: {got:?}"
        );
    }
}

#[test]
fn an_unclaimed_row_fails() {
    let got = failures(run(&table(), NONE, NO_DEV, |c: &Case| {
        if kind(c) == "beta" {
            Answer::NotChecked
        } else {
            faithful(c)
        }
    }));
    assert!(
        matches!(&got[..], [Failure::Unclaimed { case, kind }] if case == "b1" && kind == "beta"),
        "{got:?}"
    );
}

#[test]
fn an_out_of_scope_kind_claims_its_rows() {
    let skip = [OutOfScope {
        kind: "beta",
        why: "no counterpart",
    }];
    let report = run(&table(), &skip, NO_DEV, |c: &Case| {
        if kind(c) == "beta" {
            Answer::NotChecked
        } else {
            faithful(c)
        }
    })
    .expect("passes");
    assert_eq!((report.checked, report.out_of_scope), (2, 1));
}

#[test]
fn a_stale_out_of_scope_kind_fails() {
    let skip = [OutOfScope {
        kind: "gamma",
        why: "no such kind",
    }];
    let got = failures(run(&table(), &skip, NO_DEV, faithful));
    assert!(
        matches!(&got[..], [Failure::StaleOutOfScope { kind: "gamma" }]),
        "{got:?}"
    );
}

#[test]
fn checking_an_out_of_scope_kind_fails() {
    let skip = [OutOfScope {
        kind: "beta",
        why: "said out of scope",
    }];
    let got = failures(run(&table(), &skip, NO_DEV, faithful));
    assert!(
        matches!(&got[..], [Failure::CheckedOutOfScope { case, kind: "beta" }] if case == "b1"),
        "{got:?}"
    );
}

#[test]
fn a_declared_deviation_passes_while_it_disagrees() {
    let dev = [Deviation {
        case: "a2",
        why: "engenho differs on purpose",
    }];
    let report = run(&table(), NONE, &dev, |c: &Case| {
        if c.name == "a2" {
            Answer::Checked(json!({"y": 0}))
        } else {
            faithful(c)
        }
    })
    .expect("passes");
    assert_eq!((report.agree, report.deviations), (2, 1));
}

#[test]
fn a_deviation_that_now_agrees_fails() {
    let dev = [Deviation {
        case: "a2",
        why: "was different once",
    }];
    let got = failures(run(&table(), NONE, &dev, faithful));
    assert!(
        matches!(&got[..], [Failure::DeviationNowAgrees { case: "a2" }]),
        "{got:?}"
    );
}

#[test]
fn a_deviation_on_a_missing_or_unchecked_row_fails() {
    let dev = [Deviation {
        case: "zz",
        why: "typo",
    }];
    let got = failures(run(&table(), NONE, &dev, faithful));
    assert!(
        matches!(&got[..], [Failure::DeviationNoSuchRow { case: "zz" }]),
        "{got:?}"
    );

    let skip = [OutOfScope {
        kind: "beta",
        why: "no counterpart",
    }];
    let dev = [Deviation {
        case: "b1",
        why: "not checked",
    }];
    let got = failures(run(&table(), &skip, &dev, |c: &Case| {
        if kind(c) == "beta" {
            Answer::NotChecked
        } else {
            faithful(c)
        }
    }));
    assert!(
        matches!(&got[..], [Failure::DeviationNotChecked { case: "b1" }]),
        "{got:?}"
    );
}

#[test]
fn checking_nothing_fails() {
    let skip = [
        OutOfScope {
            kind: "alpha",
            why: "x",
        },
        OutOfScope {
            kind: "beta",
            why: "y",
        },
    ];
    let got = failures(run(&table(), &skip, NO_DEV, |_: &Case| Answer::NotChecked));
    assert!(matches!(&got[..], [Failure::Vacuous]), "{got:?}");
}

#[test]
fn every_failure_is_reported_not_just_the_first() {
    let skip = [OutOfScope {
        kind: "gamma",
        why: "stale",
    }];
    let got = failures(run(&table(), &skip, NO_DEV, |c: &Case| {
        match c.name.as_str() {
            "a1" => Answer::Checked(json!({"y": 0})),
            "b1" => Answer::NotChecked,
            _ => faithful(c),
        }
    }));
    assert_eq!(got.len(), 3, "{got:?}");
}

#[test]
fn duplicate_row_names_are_rejected() {
    let json = json!({"cases": [
        {"name": "same", "input": {}, "expected": {}},
        {"name": "same", "input": {}, "expected": {}},
    ]});
    let err = Table::parse(Vector::ProberResults, &json.to_string()).expect_err("rejected");
    assert!(err.to_string().contains("two rows named"), "{err}");
    let _: Value = json;
}
